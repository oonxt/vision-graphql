//! SQL generation from IR.

use crate::ast::{Count, Field, Operation, QueryArgs, RootField, Val};
use crate::dialect::{
    escape_string_literal, json_kind, pg_type_name, quote_ident, Dialect, JsonKind,
};
use crate::error::{Error, Result};
use crate::plan::MutationPlan;
use crate::schema::{ColumnType, Schema, Table};
use crate::types::{Bind, BindSpec, Inputs};
use std::fmt::Write as _;

/// Render an [`Operation`] into a single SQL statement plus its parameter list.
///
/// The parameters come back as [`BindSpec`]s rather than [`Bind`]s: an operation
/// lowered symbolically still has variables in it, and those only become values
/// once a request supplies them. Everything the operation *does* pin down is
/// converted here, so literal type errors surface at render time either way.
/// Use [`render_now`] when the operation is already fully literal.
#[tracing::instrument(level = "trace", skip_all)]
pub fn render(
    op: &Operation,
    schema: &Schema,
    dialect: Dialect,
) -> Result<(String, Vec<BindSpec>)> {
    match render_any(op, schema, dialect)? {
        Rendered::Statement { sql, specs } => Ok((sql, specs)),
        Rendered::Plan(_) => Err(Error::Unsupported {
            message: format!(
                "a mutation on {dialect:?} renders as a sequence of statements, not one; \
                 use render_any"
            ),
        }),
    }
}

/// What an operation renders to: one statement, or — a mutation on a backend
/// without data-modifying CTEs — a [`MutationPlan`] of several.
#[derive(Debug, Clone)]
pub enum Rendered {
    Statement { sql: String, specs: Vec<BindSpec> },
    Plan(Box<MutationPlan>),
}

impl Rendered {
    /// The SQL, or for a plan its statements one per line.
    pub fn sql(&self) -> &str {
        match self {
            Rendered::Statement { sql, .. } => sql,
            Rendered::Plan(p) => p.text(),
        }
    }

    /// Every parameter specification, in the order the statements bind them.
    pub(crate) fn specs(&self) -> Box<dyn Iterator<Item = &BindSpec> + '_> {
        match self {
            Rendered::Statement { specs, .. } => Box::new(specs.iter()),
            Rendered::Plan(p) => Box::new(p.specs()),
        }
    }
}

/// [`render`], for every operation. A dialect that writes in one statement
/// renders a mutation as [`Rendered::Statement`]; one that cannot renders it
/// as a [`Rendered::Plan`] — see [`crate::plan`].
pub fn render_any(op: &Operation, schema: &Schema, dialect: Dialect) -> Result<Rendered> {
    let mut ctx = RenderCtx::new(dialect);
    match op {
        Operation::Query(roots) => render_query(roots, schema, &mut ctx)?,
        Operation::Mutation(fields) if dialect.mutations_by_plan() => {
            use crate::ast::MutationField;
            crate::ast::ensure_unique_root_aliases(fields.iter().map(MutationField::alias))?;
            check_mutable(fields, schema)?;
            return Ok(Rendered::Plan(Box::new(crate::plan::build(
                fields, schema, dialect,
            )?)));
        }
        Operation::Mutation(fields) => render_mutation(fields, schema, &mut ctx)?,
    }
    Ok(Rendered::Statement {
        sql: ctx.sql,
        specs: ctx.binds,
    })
}

/// Render and immediately resolve every parameter against `inputs`.
#[tracing::instrument(level = "trace", skip_all)]
pub fn render_now(
    op: &Operation,
    schema: &Schema,
    inputs: &Inputs<'_>,
    dialect: Dialect,
) -> Result<(String, Vec<Bind>)> {
    let (sql, specs) = render(op, schema, dialect)?;
    let binds = crate::types::resolve_binds(&specs, inputs)?;
    Ok((sql, binds))
}

pub(crate) struct RenderCtx {
    /// Whose SQL this is. Not defaulted: a context built without saying is a
    /// PostgreSQL statement handed to whichever backend is listening.
    pub(crate) dialect: Dialect,
    pub(crate) sql: String,
    pub(crate) binds: Vec<BindSpec>,
    alias_counter: usize,
    /// Maps target-table-name → CTE alias for INSERT CTEs emitted in this
    /// statement. Used by nested-returning render to decide whether to read
    /// from the CTE (when source was just inserted here) or from the real
    /// table (Phase 1 behavior).
    inserted_ctes: std::collections::HashMap<String, String>,
    /// Umbrella CTE alias (e.g., "m0") for the mutation field currently
    /// being rendered in render_mutation_output_for. Used to filter which
    /// entries of inserted_ctes are visible to returning-subquery lookup,
    /// preventing cross-field bleed in multi-field mutation blocks.
    current_mutation_cte: Option<String>,
    /// Names of scope-check guard CTEs emitted by scoped inserts (at every
    /// nesting level). The final SELECT cross-joins them so PostgreSQL is
    /// forced to evaluate each guard's abort-on-violation CASE.
    scope_check_ctes: Vec<String>,
}

impl RenderCtx {
    pub(crate) fn new(dialect: Dialect) -> Self {
        Self {
            dialect,
            sql: String::new(),
            binds: Vec::new(),
            alias_counter: 0,
            inserted_ctes: Default::default(),
            current_mutation_cte: None,
            scope_check_ctes: Vec::new(),
        }
    }

    pub(crate) fn next_alias(&mut self, prefix: &str) -> String {
        let a = format!("{prefix}{}", self.alias_counter);
        self.alias_counter += 1;
        a
    }

    /// Append a scalar parameter; returns its 1-based placeholder number.
    pub(crate) fn push_scalar(
        &mut self,
        val: &Val,
        pg: &ColumnType,
        path: impl FnOnce() -> String,
    ) -> Result<usize> {
        self.binds.push(BindSpec::scalar(val.clone(), pg, path)?);
        Ok(self.binds.len())
    }

    /// Append a scalar in a comparison position, where a null is refused. See
    /// [`BindSpec::comparison`].
    pub(crate) fn push_comparison(
        &mut self,
        val: &Val,
        pg: &ColumnType,
        path: impl FnOnce() -> String,
    ) -> Result<usize> {
        self.binds
            .push(BindSpec::comparison(val.clone(), pg, path)?);
        Ok(self.binds.len())
    }

    /// Append an `_in` / `_nin` list parameter.
    fn push_array(
        &mut self,
        val: &Val,
        pg: &ColumnType,
        path: impl FnOnce() -> String,
    ) -> Result<usize> {
        self.binds.push(BindSpec::array(val.clone(), pg, path)?);
        Ok(self.binds.len())
    }

    /// Append an `_in` / `_nin` list parameter that may be null. See
    /// [`BindSpec::optional_array`].
    fn push_optional_array(
        &mut self,
        val: &Val,
        pg: &ColumnType,
        path: impl FnOnce() -> String,
    ) -> Result<usize> {
        self.binds
            .push(BindSpec::optional_array(val.clone(), pg, path)?);
        Ok(self.binds.len())
    }

    /// Append the list operand of a comparison operator. See
    /// [`BindSpec::operand_list`].
    fn push_operand_list(
        &mut self,
        val: &Val,
        pg: &ColumnType,
        optional: bool,
        path: impl FnOnce() -> String,
    ) -> Result<usize> {
        self.binds
            .push(BindSpec::operand_list(val.clone(), pg, optional, path)?);
        Ok(self.binds.len())
    }

    /// Append a parameter the renderer determined on its own.
    pub(crate) fn push_fixed(&mut self, bind: Bind) -> usize {
        self.binds.push(BindSpec::Fixed(bind));
        self.binds.len()
    }

    /// Append a `limit` / `offset` supplied as a variable.
    fn push_count(&mut self, count: &Count, path: impl FnOnce() -> String) -> usize {
        self.binds.push(BindSpec::Count {
            val: count.clone(),
            path: path(),
        });
        self.binds.len()
    }
}

fn render_query(roots: &[RootField], schema: &Schema, ctx: &mut RenderCtx) -> Result<()> {
    // Checked here and not only in the parser: the typed builder never goes
    // near the parser, and two roots sharing a key mean the second silently
    // overwrites the first in the decoded response.
    crate::ast::ensure_unique_root_aliases(roots.iter().map(|r| r.alias.as_str()))?;
    write!(ctx.sql, "SELECT {}", ctx.dialect.json_object_open()).unwrap();
    for (i, root) in roots.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        write!(ctx.sql, "'{}', ", escape_string_literal(&root.alias)).unwrap();
        render_root(root, schema, ctx)?;
    }
    ctx.sql.push_str(") AS result");
    Ok(())
}

fn render_root(root: &RootField, schema: &Schema, ctx: &mut RenderCtx) -> Result<()> {
    // Answered while lowering; it rides along as a bound parameter so a document
    // mixing introspection with data is still one statement, and so the JSON
    // never has to be escaped into the SQL text.
    if let crate::ast::RootBody::Introspection(value) = &root.body {
        let n = ctx.push_fixed(crate::types::Bind::Text(value.to_string()));
        write!(ctx.sql, "{}", ctx.dialect.json_param(n)).unwrap();
        return Ok(());
    }
    let table = schema.table(&root.table).ok_or_else(|| Error::Validate {
        path: root.alias.clone(),
        message: format!("unknown table '{}'", root.table),
    })?;
    match &root.body {
        crate::ast::RootBody::List { selection } => {
            render_list(root, selection, table, schema, ctx)
        }
        crate::ast::RootBody::Aggregate {
            ops,
            nodes,
            typenames,
            nodes_limit,
        } => render_aggregate(
            root,
            ops,
            nodes.as_deref(),
            typenames,
            nodes_limit.as_ref(),
            table,
            schema,
            ctx,
        ),
        crate::ast::RootBody::ByPk { pk, selection } => {
            render_by_pk(root, pk, selection, table, schema, ctx)
        }
        // Returned above, before the table lookup this arm sits behind.
        crate::ast::RootBody::Introspection(_) => unreachable!("handled at the top of render_root"),
    }
}

fn render_list(
    root: &RootField,
    selection: &[Field],
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let inner_alias = ctx.next_alias("t");
    let row_alias = ctx.next_alias("r");
    let shape = row_shape(selection, table, ctx.dialect);
    write!(
        ctx.sql,
        "{}",
        ctx.dialect.rows_list_open(&row_alias, &shape)
    )
    .unwrap();
    render_inner_select(root, selection, table, &inner_alias, schema, ctx)?;
    ctx.sql.push_str(") ");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push(')');
    Ok(())
}

/// The response keys a row selection produces, with what each holds — the
/// order of the inner select's output columns, which is what a dialect that
/// builds the row's JSON object by hand ([`Dialect::rows_list_open`]) needs.
/// A column the table does not have is `Plain` here; the inner select refuses
/// it with the error that names it.
fn row_shape(selection: &[Field], table: &Table, dialect: Dialect) -> Vec<(String, JsonKind)> {
    if !dialect.builds_row_objects() {
        return Vec::new();
    }
    selection
        .iter()
        .map(|f| match f {
            Field::Typename { alias } => (alias.clone(), JsonKind::Plain),
            Field::Column { column, alias } => (
                alias.clone(),
                table
                    .find_column(column)
                    .map(|c| json_kind(&c.ty))
                    .unwrap_or(JsonKind::Plain),
            ),
            Field::JsonPath { alias, .. }
            | Field::Relation { alias, .. }
            | Field::RelationAggregate { alias, .. } => (alias.clone(), JsonKind::Json),
        })
        .collect()
}

/// Refuse two selection fields answering to one response key.
///
/// The parser merges duplicates (or refuses the unmergeable) before they get
/// here, but the typed builder does not — and both `AS "key"` output columns
/// and `json_build_object` entries keep the last duplicate silently when the
/// row decodes.
pub(crate) fn ensure_unique_selection_keys(fields: &[Field], path: &str) -> Result<()> {
    let mut seen: Vec<&str> = Vec::with_capacity(fields.len());
    for f in fields {
        let key = match f {
            Field::Column { alias, .. }
            | Field::JsonPath { alias, .. }
            | Field::Typename { alias }
            | Field::Relation { alias, .. }
            | Field::RelationAggregate { alias, .. } => alias.as_str(),
        };
        if seen.contains(&key) {
            return Err(Error::Validate {
                path: format!("{path}.{key}"),
                message: format!("two fields both answer to '{key}'; give one of them an alias"),
            });
        }
        seen.push(key);
    }
    Ok(())
}

fn render_inner_select(
    root: &RootField,
    selection: &[Field],
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    ensure_unique_selection_keys(selection, &root.alias)?;
    let windowed = !root.args.distinct_on.is_empty() && ctx.dialect.distinct_on_by_window();
    ctx.sql.push_str("SELECT ");
    if !root.args.distinct_on.is_empty() && !windowed {
        ctx.sql.push_str("DISTINCT ON (");
        render_distinct_on_columns(&root.args.distinct_on, table, table_alias, &root.alias, ctx)?;
        ctx.sql.push_str(") ");
    }
    for (i, field) in selection.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match field {
            Field::Typename { alias } => render_typename_select(table, alias, ctx),
            Field::Column { column, alias } => {
                let col = table.find_column(column).ok_or_else(|| Error::Validate {
                    path: format!("{}.{}", root.alias, alias),
                    message: format!("unknown column '{column}' on '{}'", root.table),
                })?;
                // quote_ident on the alias too: parser aliases are GraphQL
                // Names, but builder aliases are arbitrary strings, and a
                // quote in one must not escape the identifier.
                write!(
                    ctx.sql,
                    "{table_alias}.{} AS {}",
                    quote_ident(&col.physical_name),
                    quote_ident(alias)
                )
                .unwrap();
            }
            Field::JsonPath {
                column,
                alias,
                path,
            } => {
                let col = table.find_column(column).ok_or_else(|| Error::Validate {
                    path: format!("{}.{}", root.alias, alias),
                    message: format!("unknown column '{column}' on '{}'", root.table),
                })?;
                let err_path = format!("{}.{}", root.alias, alias);
                let expr = render_json_path_expr(table_alias, col, path, &err_path, ctx)?;
                write!(ctx.sql, "{expr} AS {}", quote_ident(alias)).unwrap();
            }
            Field::Relation {
                name,
                alias,
                args,
                selection,
            } => {
                render_relation_field(
                    name,
                    alias,
                    args,
                    selection,
                    table,
                    table_alias,
                    schema,
                    &root.alias,
                    ctx,
                )?;
            }
            Field::RelationAggregate {
                name,
                alias,
                args,
                ops,
                nodes,
                typenames,
                nodes_limit,
            } => {
                render_relation_aggregate_field(
                    name,
                    alias,
                    args,
                    ops,
                    nodes.as_deref(),
                    typenames,
                    nodes_limit.as_ref(),
                    table,
                    table_alias,
                    schema,
                    &root.alias,
                    ctx,
                )?;
            }
        }
    }
    if windowed {
        // No `DISTINCT ON`: number the rows within each distinct group in
        // the order asked for and keep the first. The derived table carries
        // every column of the table under the same alias, so the selection,
        // the correlated subqueries in it and the outer ORDER BY render
        // exactly as they do without the window. The `WHERE` goes inside,
        // where it filters before numbering, as `DISTINCT ON` filters before
        // choosing.
        write!(
            ctx.sql,
            " FROM (SELECT {table_alias}.*, row_number() OVER (PARTITION BY "
        )
        .unwrap();
        render_distinct_on_columns(&root.args.distinct_on, table, table_alias, &root.alias, ctx)?;
        // The same ORDER BY orders the window and the outer rows. Rendered
        // once and copied: rendering it twice would push its binds twice.
        let ob_start = ctx.sql.len();
        render_order_by(&root.args, table, table_alias, schema, ctx)?;
        let order_by = ctx.sql[ob_start..].to_string();
        write!(
            ctx.sql,
            ") AS {DISTINCT_ROW_NUMBER} FROM {}.{} {table_alias}",
            quote_ident(&table.physical_schema),
            quote_ident(&table.physical_name),
        )
        .unwrap();
        render_where(&root.args, table, table_alias, schema, ctx)?;
        write!(
            ctx.sql,
            ") {table_alias} WHERE {table_alias}.{DISTINCT_ROW_NUMBER} = 1"
        )
        .unwrap();
        ctx.sql.push_str(&order_by);
    } else {
        write!(
            ctx.sql,
            " FROM {}.{} {table_alias}",
            quote_ident(&table.physical_schema),
            quote_ident(&table.physical_name),
        )
        .unwrap();
        render_where(&root.args, table, table_alias, schema, ctx)?;
        render_order_by(&root.args, table, table_alias, schema, ctx)?;
    }
    render_limit_offset(&root.args, &root.alias, ctx);
    Ok(())
}

/// The column the window form of `distinct_on` numbers rows by. Quoted and
/// prefixed so it cannot be a column of the table.
const DISTINCT_ROW_NUMBER: &str = "\"__vision_graphql_rn\"";

/// The `distinct_on` columns, comma-separated and qualified.
fn render_distinct_on_columns(
    distinct_on: &[String],
    table: &Table,
    table_alias: &str,
    path: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    for (i, col_name) in distinct_on.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
            path: format!("{path}.distinct_on.{col_name}"),
            message: format!("unknown column '{col_name}' on '{}'", table.exposed_name),
        })?;
        write!(ctx.sql, "{table_alias}.{}", quote_ident(&col.physical_name)).unwrap();
    }
    Ok(())
}

fn render_where(
    args: &QueryArgs,
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let Some(expr) = args.where_.as_ref() else {
        return Ok(());
    };
    ctx.sql.push_str(" WHERE ");
    render_bool_expr(expr, table, table_alias, schema, ctx)?;
    Ok(())
}

/// The operator's name in a document, for error messages.
/// Refuse a comparison the schema does not publish for the column's type.
///
/// The same predicate the type system builds the comparison inputs from
/// (`type_system::cmp_applies`), asked at the one point both entry points pass
/// through. Without it the builder accepted `_gt` over `jsonb` — an ordering
/// PostgreSQL will evaluate and nobody should depend on — while `__schema`
/// said no such operator existed.
fn check_cmp_applies(
    op: crate::ast::CmpOp,
    col: &crate::schema::Column,
    dialect: Dialect,
) -> Result<()> {
    crate::type_system::check_cmp(
        op,
        &col.ty,
        dialect,
        || format!("where.{}", col.exposed_name),
        &format!("'{}'", col.exposed_name),
    )
}

/// `lhs <op> operand`, with the operand bound as the operator types it
/// ([`CmpOp::operand`](crate::ast::CmpOp::operand)) rather than as `lhs`.
/// The one place a comparison binds its operand, so that `_has_key` cannot
/// be bound as `jsonb` on one path and as text on another. A null operand is
/// refused, as it is in every comparison.
fn render_cmp(
    lhs: &str,
    lhs_ty: &ColumnType,
    op: crate::ast::CmpOp,
    value: &Val,
    path: impl FnOnce() -> String,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::CmpOperand;
    let rhs = match op.operand(lhs_ty) {
        CmpOperand::Scalar(ty) => {
            let n = ctx.push_comparison(value, &ty, path)?;
            ctx.dialect.param(n, &ty).to_string()
        }
        CmpOperand::List(ty) => {
            let n = ctx.push_operand_list(value, &ty, false, path)?;
            ctx.dialect.list_param(n, &ty).to_string()
        }
    };
    write!(ctx.sql, "{}", ctx.dialect.compare(lhs, op, rhs)).unwrap();
    Ok(())
}

pub(crate) fn render_bool_expr(
    expr: &crate::ast::BoolExpr,
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::BoolExpr;
    match expr {
        BoolExpr::And(parts) => render_bool_list(parts, "AND", table, table_alias, schema, ctx),
        BoolExpr::Or(parts) => {
            refuse_optional_under(parts, "`_or`")?;
            render_bool_list(parts, "OR", table, table_alias, schema, ctx)
        }
        BoolExpr::Not(inner) => {
            refuse_optional_under(std::iter::once(inner.as_ref()), "`_not`")?;
            ctx.sql.push_str("(NOT ");
            render_bool_expr(inner, table, table_alias, schema, ctx)?;
            ctx.sql.push(')');
            Ok(())
        }
        BoolExpr::Compare { column, op, value } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            check_cmp_applies(*op, col, ctx.dialect)?;
            let lhs = format!("{table_alias}.{}", quote_ident(&col.physical_name));
            render_cmp(&lhs, &col.ty, *op, value, || format!("where.{column}"), ctx)
        }
        BoolExpr::Optional(inner) => render_optional(inner, table, Some(table_alias), schema, ctx),
        BoolExpr::Const(b) => {
            ctx.sql.push_str(if *b { "TRUE" } else { "FALSE" });
            Ok(())
        }
        BoolExpr::ValueCompare {
            left,
            op,
            right,
            pg,
        } => render_value_compare(left, *op, right, pg, ctx),
        BoolExpr::ValueInList {
            value,
            values,
            pg,
            negated,
        } => render_value_in_list(value, values, pg, *negated, ctx),
        BoolExpr::IsNull { column, is_null } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            let qualified = format!("{table_alias}.{}", quote_ident(&col.physical_name));
            render_is_null(&qualified, column, is_null, ctx)
        }
        BoolExpr::InList {
            column,
            values,
            negated,
        } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            check_cmp_applies(crate::ast::CmpOp::Eq, col, ctx.dialect)?;
            if is_empty_literal_list(values) {
                ctx.sql.push_str(if *negated { "TRUE" } else { "FALSE" });
                return Ok(());
            }
            let n = ctx.push_array(values, &col.ty, || format!("where.{column}"))?;
            let lhs = format!("{table_alias}.{}", quote_ident(&col.physical_name));
            write!(
                ctx.sql,
                "{}",
                ctx.dialect.in_list(&lhs, n, &col.ty, *negated)
            )
            .unwrap();
            Ok(())
        }
        BoolExpr::Relation { name, inner } => {
            let rel = table.find_relation(name).ok_or_else(|| Error::Validate {
                path: format!("where.{name}"),
                message: format!("unknown relation '{name}' on '{}'", table.exposed_name),
            })?;
            let target = schema
                .table(&rel.target_table)
                .ok_or_else(|| Error::Validate {
                    path: format!("where.{name}"),
                    message: format!("relation target table '{}' missing", rel.target_table),
                })?;
            let remote_alias = ctx.next_alias("e");
            ctx.sql.push_str("EXISTS (SELECT 1 FROM ");
            write!(
                ctx.sql,
                "{}.{} {remote_alias}",
                quote_ident(&target.physical_schema),
                quote_ident(&target.physical_name),
            )
            .unwrap();
            ctx.sql.push_str(" WHERE ");
            for (i, (local_col, remote_col)) in rel.mapping.iter().enumerate() {
                if i > 0 {
                    ctx.sql.push_str(" AND ");
                }
                let l = table
                    .find_column(local_col)
                    .ok_or_else(|| Error::Validate {
                        path: format!("where.{name}"),
                        message: format!("relation mapping: unknown local column '{local_col}'"),
                    })?;
                let r = target
                    .find_column(remote_col)
                    .ok_or_else(|| Error::Validate {
                        path: format!("where.{name}"),
                        message: format!("relation mapping: unknown remote column '{remote_col}'"),
                    })?;
                write!(
                    ctx.sql,
                    "{remote_alias}.{} = {table_alias}.{}",
                    quote_ident(&r.physical_name),
                    quote_ident(&l.physical_name),
                )
                .unwrap();
            }
            ctx.sql.push_str(" AND ");
            render_bool_expr(inner, target, &remote_alias, schema, ctx)?;
            ctx.sql.push(')');
            Ok(())
        }
    }
}

fn render_bool_list(
    parts: &[crate::ast::BoolExpr],
    joiner: &str,
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    if parts.is_empty() {
        ctx.sql
            .push_str(if joiner == "AND" { "TRUE" } else { "FALSE" });
        return Ok(());
    }
    ctx.sql.push('(');
    for (i, p) in parts.iter().enumerate() {
        if i > 0 {
            write!(ctx.sql, " {joiner} ").unwrap();
        }
        render_bool_expr(p, table, table_alias, schema, ctx)?;
    }
    ctx.sql.push(')');
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn render_relation_subquery(
    name: &str,
    alias: &str,
    args: &QueryArgs,
    selection: &[Field],
    parent_table: &Table,
    parent_alias: &str,
    schema: &Schema,
    parent_path: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    ensure_unique_selection_keys(selection, &format!("{parent_path}.{alias}"))?;
    let rel = parent_table
        .find_relation(name)
        .ok_or_else(|| Error::Validate {
            path: format!("{parent_path}.{alias}"),
            message: format!(
                "unknown relation '{name}' on '{}'",
                parent_table.exposed_name
            ),
        })?;
    let target = schema
        .table(&rel.target_table)
        .ok_or_else(|| Error::Validate {
            path: format!("{parent_path}.{alias}"),
            message: format!("relation target table '{}' missing", rel.target_table),
        })?;

    let remote_alias = ctx.next_alias("t");
    let row_alias = ctx.next_alias("r");

    let shape = row_shape(selection, target, ctx.dialect);
    match rel.kind {
        crate::schema::RelKind::Array => {
            write!(
                ctx.sql,
                "{}",
                ctx.dialect.rows_list_open(&row_alias, &shape)
            )
            .unwrap();
        }
        crate::schema::RelKind::Object => {
            write!(
                ctx.sql,
                "{}",
                ctx.dialect.row_object_open(&row_alias, &shape)
            )
            .unwrap();
        }
    }

    let rel_path = format!("{parent_path}.{alias}");
    let distinct = !args.distinct_on.is_empty();
    let windowed = distinct && ctx.dialect.distinct_on_by_window();
    ctx.sql.push_str("SELECT ");
    if distinct && !windowed {
        ctx.sql.push_str("DISTINCT ON (");
        render_distinct_on_columns(&args.distinct_on, target, &remote_alias, &rel_path, ctx)?;
        ctx.sql.push_str(") ");
    }
    for (i, field) in selection.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match field {
            Field::Typename { alias: fa } => render_typename_select(target, fa, ctx),
            Field::Column { column, alias: fa } => {
                let col = target.find_column(column).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.{alias}.{fa}"),
                    message: format!("unknown column '{column}' on '{}'", target.exposed_name),
                })?;
                write!(
                    ctx.sql,
                    "{remote_alias}.{} AS {}",
                    quote_ident(&col.physical_name),
                    quote_ident(fa)
                )
                .unwrap();
            }
            Field::JsonPath {
                column,
                alias: fa,
                path,
            } => {
                let col = target.find_column(column).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.{alias}.{fa}"),
                    message: format!("unknown column '{column}' on '{}'", target.exposed_name),
                })?;
                let err_path = format!("{parent_path}.{alias}.{fa}");
                let expr = render_json_path_expr(&remote_alias, col, path, &err_path, ctx)?;
                write!(ctx.sql, "{expr} AS {}", quote_ident(fa)).unwrap();
            }
            Field::Relation {
                name: cname,
                alias: ca,
                args: cargs,
                selection: csel,
            } => {
                render_relation_field(
                    cname,
                    ca,
                    cargs,
                    csel,
                    target,
                    &remote_alias,
                    schema,
                    &format!("{parent_path}.{alias}"),
                    ctx,
                )?;
            }
            Field::RelationAggregate {
                name: cname,
                alias: ca,
                args: cargs,
                ops,
                nodes,
                typenames,
                nodes_limit,
            } => {
                render_relation_aggregate_field(
                    cname,
                    ca,
                    cargs,
                    ops,
                    nodes.as_deref(),
                    typenames,
                    nodes_limit.as_ref(),
                    target,
                    &remote_alias,
                    schema,
                    &format!("{parent_path}.{alias}"),
                    ctx,
                )?;
            }
        }
    }
    let visible_cte = match (
        ctx.inserted_ctes.get(&rel.target_table),
        ctx.current_mutation_cte.as_deref(),
    ) {
        (Some(cte_alias), Some(prefix))
            if cte_alias == prefix || cte_alias.starts_with(&format!("{prefix}_")) =>
        {
            Some(cte_alias.clone())
        }
        _ => None,
    };

    let source = match visible_cte {
        Some(cte_alias) => cte_alias,
        None => format!(
            "{}.{}",
            quote_ident(&target.physical_schema),
            quote_ident(&target.physical_name)
        ),
    };
    let ob_path = format!("{rel_path}.order_by");
    if windowed {
        // As at the root (see render_inner_select): number the rows within
        // each distinct group and keep the first. The derived table is
        // correlated to the parent row — SQLite allows that in FROM — so the
        // numbering is per parent, as `DISTINCT ON` inside the correlated
        // subquery is on PostgreSQL.
        write!(
            ctx.sql,
            " FROM (SELECT {remote_alias}.*, row_number() OVER (PARTITION BY "
        )
        .unwrap();
        render_distinct_on_columns(&args.distinct_on, target, &remote_alias, &rel_path, ctx)?;
        let ob_start = ctx.sql.len();
        render_relation_order_by(args, target, &remote_alias, schema, &ob_path, ctx)?;
        let order_by = ctx.sql[ob_start..].to_string();
        write!(
            ctx.sql,
            ") AS {DISTINCT_ROW_NUMBER} FROM {source} {remote_alias}"
        )
        .unwrap();
        render_relation_where(
            args,
            rel,
            parent_table,
            parent_alias,
            target,
            &remote_alias,
            schema,
            &rel_path,
            ctx,
        )?;
        write!(
            ctx.sql,
            ") {remote_alias} WHERE {remote_alias}.{DISTINCT_ROW_NUMBER} = 1"
        )
        .unwrap();
        ctx.sql.push_str(&order_by);
    } else {
        write!(ctx.sql, " FROM {source} {remote_alias}").unwrap();
        render_relation_where(
            args,
            rel,
            parent_table,
            parent_alias,
            target,
            &remote_alias,
            schema,
            &rel_path,
            ctx,
        )?;
        render_relation_order_by(args, target, &remote_alias, schema, &ob_path, ctx)?;
    }

    if let Some(limit) = args.limit.as_ref() {
        render_count(limit, "LIMIT", &format!("{parent_path}.{alias}.limit"), ctx);
    } else if matches!(rel.kind, crate::schema::RelKind::Object) {
        ctx.sql.push_str(" LIMIT 1");
    } else if args.offset.is_some() && ctx.dialect.offset_needs_limit() {
        ctx.sql.push_str(" LIMIT -1");
    }
    if let Some(offset) = args.offset.as_ref() {
        render_count(
            offset,
            "OFFSET",
            &format!("{parent_path}.{alias}.offset"),
            ctx,
        );
    }

    ctx.sql.push_str(") ");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push(')');

    Ok(())
}

/// A relation subquery's WHERE: the correlation to the parent row, then the
/// relation's own `where`.
#[allow(clippy::too_many_arguments)]
fn render_relation_where(
    args: &QueryArgs,
    rel: &crate::schema::Relation,
    parent_table: &Table,
    parent_alias: &str,
    target: &Table,
    remote_alias: &str,
    schema: &Schema,
    rel_path: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    ctx.sql.push_str(" WHERE ");
    for (i, (local_col, remote_col)) in rel.mapping.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(" AND ");
        }
        let l = parent_table
            .find_column(local_col)
            .ok_or_else(|| Error::Validate {
                path: rel_path.to_string(),
                message: format!(
                    "relation mapping: unknown local column '{local_col}' on '{}'",
                    parent_table.exposed_name
                ),
            })?;
        let r = target
            .find_column(remote_col)
            .ok_or_else(|| Error::Validate {
                path: rel_path.to_string(),
                message: format!(
                    "relation mapping: unknown remote column '{remote_col}' on '{}'",
                    target.exposed_name
                ),
            })?;
        write!(
            ctx.sql,
            "{remote_alias}.{} = {parent_alias}.{}",
            quote_ident(&r.physical_name),
            quote_ident(&l.physical_name),
        )
        .unwrap();
    }
    if let Some(expr) = args.where_.as_ref() {
        ctx.sql.push_str(" AND ");
        render_bool_expr(expr, target, remote_alias, schema, ctx)?;
    }
    Ok(())
}

/// `ORDER BY` for a relation subquery: the `distinct_on` columns first, as
/// at the root, then the relation's own terms.
fn render_relation_order_by(
    args: &QueryArgs,
    target: &Table,
    remote_alias: &str,
    schema: &Schema,
    ob_path: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let prefix = distinct_order_prefix(args);
    if prefix.is_empty() && args.order_by.is_empty() {
        return Ok(());
    }
    ctx.sql.push_str(" ORDER BY ");
    for (i, ob) in prefix.iter().chain(args.order_by.iter()).enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        render_order_by_expr(ob, target, remote_alias, schema, ob_path, ctx)?;
        render_order_dir(ob, ctx);
    }
    Ok(())
}

/// A `_aggregate` field in a SELECT list: the subquery plus its response key.
#[allow(clippy::too_many_arguments)]
fn render_relation_aggregate_field(
    name: &str,
    alias: &str,
    args: &QueryArgs,
    ops: &[crate::ast::AggSelect],
    nodes: Option<&[Field]>,
    typenames: &[String],
    nodes_limit: Option<&crate::ast::Count>,
    parent_table: &Table,
    parent_alias: &str,
    schema: &Schema,
    parent_path: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    render_relation_aggregate(
        name,
        alias,
        args,
        ops,
        nodes,
        typenames,
        nodes_limit,
        parent_table,
        parent_alias,
        schema,
        parent_path,
        ctx,
    )?;
    write!(ctx.sql, " AS {}", quote_ident(alias)).unwrap();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn render_relation_field(
    name: &str,
    alias: &str,
    args: &QueryArgs,
    selection: &[Field],
    parent_table: &Table,
    parent_alias: &str,
    schema: &Schema,
    parent_path: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    render_relation_subquery(
        name,
        alias,
        args,
        selection,
        parent_table,
        parent_alias,
        schema,
        parent_path,
        ctx,
    )?;
    write!(ctx.sql, " AS {}", quote_ident(alias)).unwrap();
    Ok(())
}

/// The expression an ORDER BY term sorts on.
///
/// A plain column renders as `alias."col"`. A term that walks object relations
/// renders as a correlated scalar subquery, e.g. ordering `experiments` by
/// `{sample: {collected_at: asc}}`:
///
/// ```sql
/// (SELECT ob0."collected_at" FROM "public"."samples" AS ob0
///   WHERE ob0."id" = e0."sample_id" LIMIT 1)
/// ```
///
/// A correlated subquery is used rather than a JOIN so the row multiplicity of
/// the surrounding query is untouched — object relations are 1:1, so LIMIT 1 is
/// exact, and NULL (no matching row) sorts as PostgreSQL's default.
///
/// Each hop's scope predicate (`OrderByHop::filter`, injected by `apply_scope`)
/// is ANDed into the subquery's WHERE, so a scoped caller sorts only by rows it
/// could have read. A row filtered out by scope contributes no row to the
/// subquery, so the term evaluates to NULL — the same as no related row at all,
/// which is exactly what the caller is entitled to know.
/// `ASC` / `DESC`, plus an explicit `NULLS FIRST|LAST` when the caller asked for
/// one. Omitting it leaves PostgreSQL's default, which is asymmetric:
/// `ASC` sorts NULLs last, `DESC` sorts them first — so `DESC NULLS LAST` has to
/// be requested, it is not what plain `desc` gives you.
fn render_order_dir(ob: &crate::ast::OrderBy, ctx: &mut RenderCtx) {
    ctx.sql
        .push_str(ctx.dialect.order_dir(ob.direction, ob.nulls));
}

fn render_order_by_expr(
    ob: &crate::ast::OrderBy,
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    path_ctx: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    if ob.path.is_empty() {
        let col = table
            .find_column(&ob.column)
            .ok_or_else(|| Error::Validate {
                path: format!("{path_ctx}.{}", ob.column),
                message: format!("unknown column '{}' on '{}'", ob.column, table.exposed_name),
            })?;
        write!(ctx.sql, "{table_alias}.{}", quote_ident(&col.physical_name)).unwrap();
        return Ok(());
    }

    struct Hop<'a> {
        alias: String,
        target: &'a Table,
        qualified: String,
        /// Join conditions tying this hop to the previous one (or, for the
        /// first hop, to the outer row).
        conds: Vec<String>,
        filter: Option<&'a crate::ast::BoolExpr>,
    }

    // Walk the path first: the FROM/JOIN text needs the leaf alias, which is
    // only known at the end, and the SQL must be emitted in a single forward
    // pass so bind placeholders stay in step with the binds render_bool_expr
    // pushes.
    let mut hops: Vec<Hop> = Vec::with_capacity(ob.path.len());
    let mut cur = table;
    let mut cur_alias = table_alias.to_string();

    for hop in &ob.path {
        let rel_name = &hop.relation;
        let rel = cur.find_relation(rel_name).ok_or_else(|| Error::Validate {
            path: format!("{path_ctx}.{rel_name}"),
            message: format!("unknown relation '{rel_name}' on '{}'", cur.exposed_name),
        })?;
        if rel.kind != crate::schema::RelKind::Object {
            return Err(Error::Validate {
                path: format!("{path_ctx}.{rel_name}"),
                message: format!(
                    "cannot order by array relation '{rel_name}'; only object relations are supported"
                ),
            });
        }
        let target = schema
            .table(&rel.target_table)
            .ok_or_else(|| Error::Validate {
                path: format!("{path_ctx}.{rel_name}"),
                message: format!("relation target table '{}' missing", rel.target_table),
            })?;

        let a = ctx.next_alias("ob");
        let mut conds = Vec::new();
        for (local, remote) in &rel.mapping {
            let lcol = cur.find_column(local).ok_or_else(|| Error::Validate {
                path: format!("{path_ctx}.{rel_name}"),
                message: format!("unknown column '{local}' on '{}'", cur.exposed_name),
            })?;
            let rcol = target.find_column(remote).ok_or_else(|| Error::Validate {
                path: format!("{path_ctx}.{rel_name}"),
                message: format!("unknown column '{remote}' on '{}'", target.exposed_name),
            })?;
            conds.push(format!(
                "{a}.{} = {cur_alias}.{}",
                quote_ident(&rcol.physical_name),
                quote_ident(&lcol.physical_name)
            ));
        }

        hops.push(Hop {
            alias: a.clone(),
            target,
            qualified: format!(
                "{}.{}",
                quote_ident(&target.physical_schema),
                quote_ident(&target.physical_name)
            ),
            conds,
            filter: hop.filter.as_ref(),
        });

        cur = target;
        cur_alias = a;
    }

    let col = cur.find_column(&ob.column).ok_or_else(|| Error::Validate {
        path: format!("{path_ctx}.{}", ob.column),
        message: format!("unknown column '{}' on '{}'", ob.column, cur.exposed_name),
    })?;

    let first = &hops[0];
    let leaf = hops.last().expect("path is non-empty");
    write!(
        ctx.sql,
        "(SELECT {}.{} FROM {} AS {}",
        leaf.alias,
        quote_ident(&col.physical_name),
        first.qualified,
        first.alias
    )
    .unwrap();
    // Only the first hop correlates to the outer query; the rest are joins.
    for h in &hops[1..] {
        write!(
            ctx.sql,
            " JOIN {} AS {} ON {}",
            h.qualified,
            h.alias,
            h.conds.join(" AND ")
        )
        .unwrap();
    }
    write!(ctx.sql, " WHERE {}", first.conds.join(" AND ")).unwrap();
    for h in &hops {
        if let Some(f) = h.filter {
            ctx.sql.push_str(" AND ");
            render_bool_expr(f, h.target, &h.alias, schema, ctx)?;
        }
    }
    ctx.sql.push_str(" LIMIT 1)");
    Ok(())
}

/// The `distinct_on` columns the ORDER BY has to start with, as order terms —
/// those the document did not already order by. `DISTINCT ON` demands the
/// leading order terms be the distinct columns, and the window form partitions
/// by them, so either way they come first.
fn distinct_order_prefix(args: &QueryArgs) -> Vec<crate::ast::OrderBy> {
    args.distinct_on
        .iter()
        .filter(|d| {
            !args
                .order_by
                .iter()
                .any(|ob| ob.path.is_empty() && ob.column == **d)
        })
        .map(|d| crate::ast::OrderBy::column(d.clone(), crate::ast::OrderDir::Asc))
        .collect()
}

fn render_order_by(
    args: &QueryArgs,
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let prefix = distinct_order_prefix(args);
    if prefix.is_empty() && args.order_by.is_empty() {
        return Ok(());
    }
    ctx.sql.push_str(" ORDER BY ");
    let mut first = true;
    for ob in prefix.iter().chain(args.order_by.iter()) {
        if !first {
            ctx.sql.push_str(", ");
        }
        first = false;
        render_order_by_expr(ob, table, table_alias, schema, "order_by", ctx)?;
        render_order_dir(ob, ctx);
    }
    Ok(())
}

fn render_limit_offset(args: &QueryArgs, path: &str, ctx: &mut RenderCtx) {
    if let Some(limit) = args.limit.as_ref() {
        render_count(limit, "LIMIT", &format!("{path}.limit"), ctx);
    } else if args.offset.is_some() && ctx.dialect.offset_needs_limit() {
        ctx.sql.push_str(" LIMIT -1");
    }
    if let Some(offset) = args.offset.as_ref() {
        render_count(offset, "OFFSET", &format!("{path}.offset"), ctx);
    }
}

/// A literal count renders inline — it is part of the query text, so it is as
/// fixed as the rest of the SQL. A variable becomes a bind, which is what keeps
/// `limit: $n` from producing a different statement per page size. A literal the
/// caller asked to be bound ([`Count::Bound`]) does the same for the same
/// reason, at the cost of no longer showing in the rendered SQL.
fn render_count(count: &Count, keyword: &str, path: &str, ctx: &mut RenderCtx) {
    match count {
        Count::Lit(n) => write!(ctx.sql, " {keyword} {n}").unwrap(),
        Count::Bound(n) => {
            let i = ctx.push_fixed(crate::types::Bind::Int8(*n as i64));
            write!(ctx.sql, " {keyword} {}", ctx.dialect.count_param(i)).unwrap();
        }
        Count::Var { .. } => {
            let n = ctx.push_count(count, || path.to_string());
            write!(ctx.sql, " {keyword} {}", ctx.dialect.count_param(n)).unwrap();
        }
    }
}

/// [`BoolExpr::Optional`]: the comparison when its operand has a value, `TRUE`
/// when it is null.
///
/// A literal operand is decided here — a null renders `TRUE`, anything else
/// renders exactly as it would unwrapped, so the eager path's SQL is untouched
/// by the wrapper. A variable is decided per request without changing the
/// statement: `($n IS NULL OR column = $n)`, one placeholder used twice. The
/// null case is decided by the parameter, not by the planner seeing a
/// constant, so this form is what the compiled path needs and the only one
/// that keeps one statement for both requests.
///
/// The column is resolved before the null shortcut: whether the document is
/// valid must not depend on which value this request sent. It is also why a
/// dropped comparison is still a comparison with a null operand rather than
/// no predicate — the scope rewrite has to see the column.
///
/// `alias` is `None` for the unaliased contexts (`render_bool_expr_no_alias`).
fn render_optional(
    inner: &crate::ast::BoolExpr,
    table: &Table,
    alias: Option<&str>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::BoolExpr;
    let (column, operand) = match inner {
        BoolExpr::Compare { column, value, .. } => (column, value),
        BoolExpr::InList { column, values, .. } => (column, values),
        BoolExpr::IsNull { column, is_null } => (column, is_null),
        // The lowering only ever wraps a comparison; the typed builder could
        // wrap anything. There is no reading of "optional" for a conjunction
        // that is not a guess, so it is refused rather than guessed.
        other => {
            return Err(Error::Validate {
                path: "where".into(),
                message: format!(
                    "Optional wraps a single comparison (`_eq`, `_in`, `_is_null`, …), not {}",
                    match other {
                        BoolExpr::And(_) => "`_and`",
                        BoolExpr::Or(_) => "`_or`",
                        BoolExpr::Not(_) => "`_not`",
                        BoolExpr::Relation { .. } => "a relation predicate",
                        BoolExpr::Optional(_) => "another Optional",
                        BoolExpr::Const(_) => "a constant",
                        BoolExpr::ValueCompare { .. } | BoolExpr::ValueInList { .. } => {
                            "a column-less comparison"
                        }
                        BoolExpr::Compare { .. }
                        | BoolExpr::InList { .. }
                        | BoolExpr::IsNull { .. } => unreachable!(),
                    }
                ),
            })
        }
    };
    let col = table.find_column(column).ok_or_else(|| Error::Validate {
        path: format!("where.{column}"),
        message: format!("unknown column '{column}' on '{}'", table.exposed_name),
    })?;
    match inner {
        BoolExpr::Compare { op, .. } => check_cmp_applies(*op, col, ctx.dialect)?,
        BoolExpr::InList { .. } => check_cmp_applies(crate::ast::CmpOp::Eq, col, ctx.dialect)?,
        _ => {}
    }
    let render_plain = |ctx: &mut RenderCtx| match alias {
        Some(a) => render_bool_expr(inner, table, a, schema, ctx),
        None => render_bool_expr_no_alias(inner, table, schema, ctx),
    };
    match operand.as_lit() {
        Some(v) if v.is_null() => {
            ctx.sql.push_str("TRUE");
            return Ok(());
        }
        Some(_) => return render_plain(ctx),
        None => {}
    }
    let qualified = match alias {
        Some(a) => format!("{a}.{}", quote_ident(&col.physical_name)),
        None => quote_ident(&col.physical_name),
    };
    let path = || format!("where.{column}");
    match inner {
        BoolExpr::Compare { op, value, .. } => {
            use crate::ast::CmpOperand;
            let p = match op.operand(&col.ty) {
                CmpOperand::Scalar(ty) => {
                    let n = ctx.push_scalar(value, &ty, path)?;
                    ctx.dialect.param(n, &ty).to_string()
                }
                CmpOperand::List(ty) => {
                    let n = ctx.push_operand_list(value, &ty, true, path)?;
                    ctx.dialect.list_param(n, &ty).to_string()
                }
            };
            write!(
                ctx.sql,
                "({p} IS NULL OR {})",
                ctx.dialect.compare(&qualified, *op, &p)
            )
            .unwrap();
        }
        BoolExpr::InList {
            values, negated, ..
        } => {
            if values.is_lit() {
                // A composite with no variables left: same as a literal list.
                return render_plain(ctx);
            }
            let n = ctx.push_optional_array(values, &col.ty, path)?;
            write!(
                ctx.sql,
                "({} IS NULL OR {})",
                ctx.dialect.list_param(n, &col.ty),
                ctx.dialect.in_list(&qualified, n, &col.ty, *negated)
            )
            .unwrap();
        }
        BoolExpr::IsNull { is_null, .. } => {
            let n = ctx.push_scalar(is_null, &ColumnType::Bool, path)?;
            let p = ctx.dialect.bool_param(n);
            write!(ctx.sql, "({p} IS NULL OR ({qualified} IS NULL) = {p})").unwrap();
        }
        _ => unreachable!("matched above"),
    }
    Ok(())
}

/// [`BoolExpr::ValueCompare`]: `$n::pg op $m::pg`. No table is involved, so
/// one renderer serves the aliased and the unaliased forms alike.
///
/// The operator is checked against `pg` the way a column comparison's is
/// checked against the column's type: `_like` over an integer would otherwise
/// be a cast error from PostgreSQL on the first request, blamed on the
/// principal.
fn render_value_compare(
    left: &Val,
    op: crate::ast::CmpOp,
    right: &Val,
    pg: &ColumnType,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let path = || crate::ast::value_leaf_path([left, right]);
    crate::type_system::check_cmp(op, pg, ctx.dialect, path, &value_subject(pg))?;
    let l = ctx.push_comparison(left, pg, path)?;
    let lhs = ctx.dialect.param(l, pg).to_string();
    render_cmp(&lhs, pg, op, right, path, ctx)
}

/// [`BoolExpr::ValueInList`]: `$n::pg = ANY($m::pg[])`, or `<> ALL` when
/// negated. An empty literal list collapses as a column `_in` does — after
/// the value has been checked, so that a null against an empty list is
/// refused as a null in any comparison is, not answered.
fn render_value_in_list(
    value: &Val,
    values: &Val,
    pg: &ColumnType,
    negated: bool,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let path = || crate::ast::value_leaf_path([value, values]);
    crate::type_system::check_cmp(
        crate::ast::CmpOp::Eq,
        pg,
        ctx.dialect,
        path,
        &value_subject(pg),
    )?;
    if is_empty_literal_list(values) {
        BindSpec::comparison(value.clone(), pg, path)?;
        ctx.sql.push_str(if negated { "TRUE" } else { "FALSE" });
        return Ok(());
    }
    let v = ctx.push_comparison(value, pg, path)?;
    let list = ctx.push_array(values, pg, path)?;
    let lhs = ctx.dialect.param(v, pg);
    write!(ctx.sql, "{}", ctx.dialect.in_list(&lhs, list, pg, negated)).unwrap();
    Ok(())
}

/// What a column-less comparison is "on", for its refusal message.
pub(crate) fn value_subject(pg: &ColumnType) -> String {
    format!("a {} value", pg_type_name(pg))
}

/// [`BoolExpr::Optional`] under `_or` or `_not` is refused: a dropped
/// comparison is `TRUE`, which under `_or` admits every row and under `_not`
/// none — neither is what leaving a filter out means. The lowering refuses
/// the document before it gets here; this is for the typed builder, which
/// can put an `Optional` anywhere. Walks through `_and` and relation
/// predicates, as the lowering's own check does.
fn refuse_optional_under<'a>(
    parts: impl IntoIterator<Item = &'a crate::ast::BoolExpr>,
    what: &str,
) -> Result<()> {
    use crate::ast::BoolExpr;
    for part in parts {
        match part {
            BoolExpr::Optional(_) => {
                return Err(Error::Validate {
                    path: "where".into(),
                    message: format!(
                        "an Optional comparison cannot apply under {what}: a dropped \
                         comparison is TRUE, which would admit every row there or none"
                    ),
                })
            }
            BoolExpr::And(inner) => refuse_optional_under(inner, what)?,
            BoolExpr::Relation { inner, .. } => {
                refuse_optional_under(std::iter::once(inner.as_ref()), what)?
            }
            _ => {}
        }
    }
    Ok(())
}

/// [`BoolExpr::IsNull`] on an already-resolved, already-quoted column.
///
/// A literal decides the predicate here: `IS NULL` or `IS NOT NULL`, as a
/// reader expects to see it. A variable cannot, so it binds — `(col IS NULL)
/// = $n::boolean` — the same statement for both requests, which is what lets
/// `_is_null: $b` compile without the document bounding `$b`. A null is
/// refused as a null comparison is: neither predicate is what it asks for.
fn render_is_null(qualified: &str, column: &str, is_null: &Val, ctx: &mut RenderCtx) -> Result<()> {
    match is_null.as_lit() {
        Some(serde_json::Value::Bool(true)) => write!(ctx.sql, "{qualified} IS NULL").unwrap(),
        Some(serde_json::Value::Bool(false)) => write!(ctx.sql, "{qualified} IS NOT NULL").unwrap(),
        Some(v) if v.is_null() => {
            return Err(crate::types::null_comparison(&format!("where.{column}")));
        }
        Some(_) => {
            return Err(Error::Validate {
                path: format!("where.{column}"),
                message: "expected boolean".into(),
            });
        }
        None => {
            let n =
                ctx.push_comparison(is_null, &ColumnType::Bool, || format!("where.{column}"))?;
            write!(
                ctx.sql,
                "({qualified} IS NULL) = {}",
                ctx.dialect.bool_param(n)
            )
            .unwrap();
        }
    }
    Ok(())
}

/// An `_in` list that is literally empty, and so can collapse to TRUE/FALSE.
/// A list that only *might* be empty at request time keeps the `= ANY` form:
/// `x = ANY('{}')` is already false and `x <> ALL('{}')` already true, so the
/// collapse is an optimisation, not a semantic requirement.
fn is_empty_literal_list(values: &Val) -> bool {
    match values {
        Val::Array(items) => items.is_empty(),
        Val::Lit(v) => v.as_array().is_some_and(|a| a.is_empty()),
        _ => false,
    }
}

/// Render a JSON/JSONB path read as `<table_alias>."col" #> $N::text[]`, pushing
/// the path components as a single `text[]` bind. The result preserves the
/// column's json/jsonb type, so it nests inside the surrounding `json_agg`/
/// `row_to_json` unchanged.
///
/// The column type is validated here (not only in the parser) because the typed
/// builder API constructs [`Field::JsonPath`] without going through the parser.
fn render_json_path_expr(
    table_alias: &str,
    col: &crate::schema::Column,
    path: &[String],
    err_path: &str,
    ctx: &mut RenderCtx,
) -> Result<String> {
    use crate::schema::ColumnType;
    if !matches!(col.ty, ColumnType::Json | ColumnType::Jsonb) {
        return Err(Error::Validate {
            path: err_path.into(),
            message: format!(
                "path read requires a json/jsonb column, but '{}' is not",
                col.exposed_name
            ),
        });
    }
    let n = ctx.push_fixed(ctx.dialect.json_path_bind(path));
    let col_sql = format!("{table_alias}.{}", quote_ident(&col.physical_name));
    let expr = ctx.dialect.json_path(&col_sql, n).to_string();
    Ok(expr)
}

/// `__typename` in a SELECT list: a literal of the type this selection set
/// belongs to. Cast to text so `row_to_json` sees a string rather than an
/// `unknown`-typed constant.
/// `__typename` beside `affected_rows` / `returning`: the mutation-response
/// type, which is not the row type the `returning` selection reads.
fn render_response_typenames(names: &[String], table: &Table, ctx: &mut RenderCtx) {
    for alias in names {
        write!(
            ctx.sql,
            ", '{}', {}",
            escape_string_literal(alias),
            ctx.dialect
                .text_literal(&crate::type_names::mutation_response(table))
        )
        .unwrap();
    }
}

fn render_typename_select(table: &Table, alias: &str, ctx: &mut RenderCtx) {
    write!(
        ctx.sql,
        "{} AS {}",
        ctx.dialect.text_literal(crate::type_names::row(table)),
        quote_ident(alias)
    )
    .unwrap();
}

fn render_by_pk(
    root: &RootField,
    pk: &[(String, Val)],
    selection: &[Field],
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    ensure_unique_selection_keys(selection, &root.alias)?;
    let inner_alias = ctx.next_alias("t");
    let row_alias = ctx.next_alias("r");
    let shape = row_shape(selection, table, ctx.dialect);
    write!(
        ctx.sql,
        "{}",
        ctx.dialect.row_object_open(&row_alias, &shape)
    )
    .unwrap();
    ctx.sql.push_str("SELECT ");
    for (i, field) in selection.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match field {
            Field::Typename { alias } => render_typename_select(table, alias, ctx),
            Field::Column { column, alias } => {
                let col = table.find_column(column).ok_or_else(|| Error::Validate {
                    path: format!("{}.{}", root.alias, alias),
                    message: format!("unknown column '{column}' on '{}'", table.exposed_name),
                })?;
                write!(
                    ctx.sql,
                    "{inner_alias}.{} AS {}",
                    quote_ident(&col.physical_name),
                    quote_ident(alias)
                )
                .unwrap();
            }
            Field::JsonPath {
                column,
                alias,
                path,
            } => {
                let col = table.find_column(column).ok_or_else(|| Error::Validate {
                    path: format!("{}.{}", root.alias, alias),
                    message: format!("unknown column '{column}' on '{}'", table.exposed_name),
                })?;
                let err_path = format!("{}.{}", root.alias, alias);
                let expr = render_json_path_expr(&inner_alias, col, path, &err_path, ctx)?;
                write!(ctx.sql, "{expr} AS {}", quote_ident(alias)).unwrap();
            }
            Field::Relation {
                name,
                alias,
                args,
                selection,
            } => {
                render_relation_field(
                    name,
                    alias,
                    args,
                    selection,
                    table,
                    &inner_alias,
                    schema,
                    &root.alias,
                    ctx,
                )?;
            }
            Field::RelationAggregate {
                name,
                alias,
                args,
                ops,
                nodes,
                typenames,
                nodes_limit,
            } => {
                render_relation_aggregate_field(
                    name,
                    alias,
                    args,
                    ops,
                    nodes.as_deref(),
                    typenames,
                    nodes_limit.as_ref(),
                    table,
                    &inner_alias,
                    schema,
                    &root.alias,
                    ctx,
                )?;
            }
        }
    }
    write!(
        ctx.sql,
        " FROM {}.{} {inner_alias} WHERE ",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    for (i, (col_name, value)) in pk.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(" AND ");
        }
        let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
            path: format!("{}.pk.{col_name}", root.alias),
            message: format!("unknown column '{col_name}' on '{}'", table.exposed_name),
        })?;
        let n = ctx.push_comparison(value, &col.ty, || format!("{}.pk.{col_name}", root.alias))?;
        let ph = ctx.dialect.param(n, &col.ty);
        write!(
            ctx.sql,
            "{inner_alias}.{} = {ph}",
            quote_ident(&col.physical_name)
        )
        .unwrap();
    }
    // by_pk has no `where` argument in the source language, but the scope
    // rewrite injects predicates here; honor them on top of the PK match.
    if let Some(expr) = root.args.where_.as_ref() {
        ctx.sql.push_str(" AND ");
        render_bool_expr(expr, table, &inner_alias, schema, ctx)?;
    }
    ctx.sql.push_str(" LIMIT 1) ");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push(')');
    Ok(())
}

/// Reject any mutation that would write to a read-only table.
///
/// The parser refuses to *derive* mutation roots for a read-only table, but the
/// builder API (`Mutation::insert(..)` → `Engine::run`) constructs the AST
/// directly and never passes through the parser. The renderer is the one choke
/// point both paths share, so the guard has to live here as well — a read-only
/// table is a property of the schema, and no way of reaching the renderer may
/// write to one.
pub(crate) fn check_mutable(fields: &[crate::ast::MutationField], schema: &Schema) -> Result<()> {
    use crate::ast::{InsertObject, MutationField};

    fn deny(table: &Table, alias: &str) -> Error {
        Error::Validate {
            path: alias.to_string(),
            message: format!(
                "table '{}' is read-only; mutations are not available",
                table.exposed_name
            ),
        }
    }

    fn check_table(name: &str, alias: &str, schema: &Schema) -> Result<()> {
        let t = schema.table(name).ok_or_else(|| Error::Validate {
            path: alias.to_string(),
            message: format!("unknown table '{name}'"),
        })?;
        if t.read_only {
            return Err(deny(t, alias));
        }
        Ok(())
    }

    // A nested insert reaches a table without naming a root field, so recurse.
    fn check_nested(obj: &InsertObject, alias: &str, schema: &Schema) -> Result<()> {
        for na in obj.nested_arrays.values() {
            check_table(&na.table, alias, schema)?;
            for row in &na.rows {
                check_nested(row, alias, schema)?;
            }
        }
        for no in obj.nested_objects.values() {
            check_table(&no.table, alias, schema)?;
            check_nested(&no.row, alias, schema)?;
        }
        Ok(())
    }

    for mf in fields {
        let table = match mf {
            MutationField::Insert { table, .. }
            | MutationField::Update { table, .. }
            | MutationField::UpdateByPk { table, .. }
            | MutationField::Delete { table, .. }
            | MutationField::DeleteByPk { table, .. } => table,
        };
        check_table(table, mf.alias(), schema)?;
        if let MutationField::Insert { objects, .. } = mf {
            for obj in objects {
                check_nested(obj, mf.alias(), schema)?;
            }
        }
    }
    Ok(())
}

fn render_mutation(
    fields: &[crate::ast::MutationField],
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::MutationField;
    // See render_query: the builder path has no other duplicate-key guard.
    crate::ast::ensure_unique_root_aliases(fields.iter().map(MutationField::alias))?;
    check_mutable(fields, schema)?;
    ctx.sql.push_str("WITH ");
    for (i, mf) in fields.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let cte = format!("m{i}");
        match mf {
            MutationField::Insert {
                table,
                objects,
                on_conflict,
                scope_check,
                ..
            } => {
                render_insert_cte(
                    &cte,
                    table,
                    objects,
                    on_conflict.as_ref(),
                    scope_check.as_ref(),
                    schema,
                    ctx,
                )?;
            }
            MutationField::Update {
                table,
                where_,
                set,
                scope_check,
                ..
            } => {
                render_update_cte(&cte, table, where_, set, scope_check.as_ref(), schema, ctx)?;
            }
            MutationField::UpdateByPk {
                table,
                pk,
                set,
                scope,
                ..
            } => {
                render_update_by_pk_cte(&cte, table, pk, set, scope.as_ref(), schema, ctx)?;
            }
            MutationField::Delete { table, where_, .. } => {
                render_delete_cte(&cte, table, where_, schema, ctx)?;
            }
            MutationField::DeleteByPk {
                table, pk, scope, ..
            } => {
                render_delete_by_pk_cte(&cte, table, pk, scope.as_ref(), schema, ctx)?;
            }
        }
    }
    ctx.sql.push(' ');
    write!(ctx.sql, "SELECT {}", ctx.dialect.json_object_open()).unwrap();
    for (i, mf) in fields.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let cte = format!("m{i}");
        render_mutation_output_for(mf, &cte, schema, ctx)?;
    }
    ctx.sql.push_str(") AS result");
    // Cross-join the 1-row guard CTEs (from scoped inserts at every nesting
    // level) and reference each guard's `ok` in WHERE. The WHERE reference is
    // essential: without it PostgreSQL prunes the unused `ok` column and never
    // evaluates the error-raising CASE, so the guard would silently do nothing.
    // On a passing insert every `ok` is 0 and the row is kept; on a violation
    // the CASE's ELSE cast fails while WHERE is evaluated, aborting the
    // statement.
    if !ctx.scope_check_ctes.is_empty() {
        ctx.sql.push_str(" FROM ");
        ctx.sql.push_str(&ctx.scope_check_ctes.join(", "));
        ctx.sql.push_str(" WHERE ");
        let guards = std::mem::take(&mut ctx.scope_check_ctes);
        for (i, chk) in guards.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(" AND ");
            }
            write!(ctx.sql, "{chk}.ok = 0").unwrap();
        }
    }
    Ok(())
}

/// Emit `, {cte}_chk AS (...)` — a single-row guard CTE that aggregates `check`
/// over the rows written into `{cte}` (inserted or updated). If every row
/// satisfies it the CASE yields 0; otherwise it casts a diagnostic string to
/// `integer`, raising an error that aborts the statement. The `count(*)` inside
/// the cast argument keeps PostgreSQL from constant-folding (and prematurely
/// raising) the ELSE branch at plan time — it is only evaluated when a row
/// actually violates. `action` names the operation in the diagnostic (e.g.
/// "inserted", "modified"). The guard CTE name is recorded in
/// `ctx.scope_check_ctes` so the final SELECT references it.
fn emit_scope_guard(
    cte: &str,
    table: &Table,
    check: &crate::ast::BoolExpr,
    action: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let chk = format!("{cte}_chk");
    write!(ctx.sql, ", {chk} AS (SELECT CASE WHEN coalesce(bool_and(").unwrap();
    render_bool_expr(check, table, cte, schema, ctx)?;
    write!(
        ctx.sql,
        "), true) THEN 0 ELSE CAST('vision_graphql: scope check violation on \"{}\" (' || count(*)::text || ' rows) {action} outside scope' AS integer) END AS ok FROM {cte})",
        table.exposed_name
    )
    .unwrap();
    ctx.scope_check_ctes.push(chk);
    Ok(())
}

fn render_insert_cte(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    on_conflict: Option<&crate::ast::OnConflict>,
    scope_check: Option<&crate::ast::BoolExpr>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    // Top-level: parent ordinals are just 1..=N.
    let parent_ords: Vec<i64> = (1..=objects.len() as i64).collect();
    render_insert_cte_recursive(
        cte,
        table_name,
        objects,
        &parent_ords,
        on_conflict,
        scope_check,
        None,
        false, // top-level: NOT nested
        0,
        schema,
        ctx,
    )
}

#[allow(clippy::too_many_arguments)]
fn render_insert_cte_recursive(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    parent_ords: &[i64],
    on_conflict: Option<&crate::ast::OnConflict>,
    scope_check: Option<&crate::ast::BoolExpr>,
    parent_link: Option<(&str, &crate::schema::Relation, &crate::schema::Table)>,
    is_nested_cte: bool,
    depth: usize,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use std::collections::BTreeSet;

    debug_assert_eq!(objects.len(), parent_ords.len());

    // The unconditional bound on insert-tree nesting, at the point every
    // entry point passes: the walk in ExecutionLimits short-circuits when the
    // limits are unbounded, and a stack overflow here aborts the process.
    if depth > crate::limits::DEFAULT_MAX_DEPTH {
        return Err(Error::Validate {
            path: "objects".into(),
            message: format!(
                "nested inserts nest deeper than the limit of {}",
                crate::limits::DEFAULT_MAX_DEPTH
            ),
        });
    }

    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;

    if objects.is_empty() {
        // Nothing to insert at this level — emit a no-op CTE so later CTEs
        // can still reference {cte} without type errors.
        write!(
            ctx.sql,
            "{cte} AS (SELECT * FROM {}.{} WHERE FALSE)",
            quote_ident(&table.physical_schema),
            quote_ident(&table.physical_name),
        )
        .unwrap();
        ctx.inserted_ctes
            .insert(table_name.to_string(), cte.to_string());
        // Also emit a no-op _ord so callers that JOIN against it don't break.
        write!(
            ctx.sql,
            ", {cte}_ord AS (SELECT *, 0::bigint AS ord FROM {cte})"
        )
        .unwrap();
        return Ok(());
    }

    // 1. Collect parent columns.
    let mut col_set: BTreeSet<String> = BTreeSet::new();
    for obj in objects {
        for k in obj.columns.keys() {
            col_set.insert(k.clone());
        }
    }
    let cols: Vec<String> = col_set.into_iter().collect();

    // 2. Emit object-relation CTE chains BEFORE the parent input/insert.
    //    Batch-uniform rule (enforced at parse): if any row has `nested_objects[k]`,
    //    all rows do. Collect the rows and recursively emit each.
    let mut object_rel_names: Vec<String> = Vec::new();
    if let Some(first) = objects.first() {
        for k in first.nested_objects.keys() {
            object_rel_names.push(k.clone());
        }
    }

    for rel_name in &object_rel_names {
        let rel = table
            .find_relation(rel_name)
            .ok_or_else(|| Error::Validate {
                path: cte.into(),
                message: format!("unknown relation '{rel_name}' on '{}'", table.exposed_name),
            })?;
        // Gather the N object-rows (one per parent row), in parent ord order.
        let child_rows: Vec<crate::ast::InsertObject> = objects
            .iter()
            .map(|o| {
                o.nested_objects
                    .get(rel_name)
                    .expect("batch-uniform guarantees presence")
                    .row
                    .clone()
            })
            .collect();
        // Object-relation child uses parent ordinals as its own ordinals (1:1).
        let child_ords: Vec<i64> = parent_ords.to_vec();
        // Read on_conflict from objects[0]'s nested_objects[rel_name] — the
        // GraphQL input attaches one wrapper per relation per parent row,
        // and the batch-uniform rule means all rows have the same shape.
        let child_on_conflict = objects
            .first()
            .and_then(|o| o.nested_objects.get(rel_name))
            .and_then(|noi| noi.on_conflict.clone());
        let child_scope_check = objects
            .first()
            .and_then(|o| o.nested_objects.get(rel_name))
            .and_then(|noi| noi.scope_check.clone());
        let child_cte = format!("{cte}_{rel_name}");
        render_insert_cte_recursive(
            &child_cte,
            &rel.target_table,
            &child_rows,
            &child_ords,
            child_on_conflict.as_ref(),
            child_scope_check.as_ref(),
            None, // NOT a child-of-parent; this is a prerequisite insert
            true, // this is a nested CTE
            depth + 1,
            schema,
            ctx,
        )?;
        ctx.sql.push_str(", ");
    }

    // 3. Emit the parent's `{cte}_input` VALUES CTE with ord + column values.
    let input_cte = format!("{cte}_input");
    let ord_col_name = if parent_link.is_some() {
        "parent_ord"
    } else {
        "ord"
    };

    write!(ctx.sql, "{input_cte} AS (SELECT * FROM (VALUES ").unwrap();
    for (r, obj) in objects.iter().enumerate() {
        if r > 0 {
            ctx.sql.push_str(", ");
        }
        ctx.sql.push('(');
        write!(ctx.sql, "{}", parent_ords[r]).unwrap();
        for exposed in &cols {
            ctx.sql.push_str(", ");
            let col = table
                .find_column(exposed)
                .expect("column should exist — validated at parse");
            match obj.columns.get(exposed) {
                None => write!(ctx.sql, "{}", ctx.dialect.null_of(&col.ty)).unwrap(),
                Some(v) => {
                    let n =
                        ctx.push_scalar(v, &col.ty, || format!("{cte}.objects[{r}].{exposed}"))?;
                    write!(ctx.sql, "{}", ctx.dialect.param(n, &col.ty)).unwrap();
                }
            }
        }
        ctx.sql.push(')');
    }
    write!(ctx.sql, ") AS t({ord_col_name}").unwrap();
    for exposed in &cols {
        write!(ctx.sql, ", {}", quote_ident(exposed)).unwrap();
    }
    ctx.sql.push_str(")), ");

    // 4. Emit the parent INSERT CTE. Column list = parent columns +
    //    FK columns from parent_link (array-child case) + FK columns
    //    from each object_rel in object_rel_names.
    write!(
        ctx.sql,
        "{cte} AS (INSERT INTO {}.{} (",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();

    let mut first = true;
    for exposed in &cols {
        if !first {
            ctx.sql.push_str(", ");
        }
        first = false;
        let col = table.find_column(exposed).unwrap();
        ctx.sql.push_str(&quote_ident(&col.physical_name));
    }
    // FK columns from parent_link (Phase 2's array-child case).
    if let Some((_, rel, _)) = parent_link {
        for (_, child_col) in &rel.mapping {
            if !first {
                ctx.sql.push_str(", ");
            }
            first = false;
            let col = table
                .find_column(child_col)
                .ok_or_else(|| Error::Validate {
                    path: cte.into(),
                    message: format!(
                        "mapped FK column '{child_col}' missing on '{}'",
                        table.exposed_name
                    ),
                })?;
            ctx.sql.push_str(&quote_ident(&col.physical_name));
        }
    }
    // FK columns from object relations (Phase 3A).
    for rel_name in &object_rel_names {
        let rel = table.find_relation(rel_name).unwrap();
        for (parent_fk_col, _) in &rel.mapping {
            if !first {
                ctx.sql.push_str(", ");
            }
            first = false;
            let col = table
                .find_column(parent_fk_col)
                .ok_or_else(|| Error::Validate {
                    path: cte.into(),
                    message: format!(
                        "mapped FK column '{parent_fk_col}' missing on '{}'",
                        table.exposed_name
                    ),
                })?;
            ctx.sql.push_str(&quote_ident(&col.physical_name));
        }
    }
    ctx.sql.push(')');

    // SELECT source.
    ctx.sql.push_str(" SELECT ");
    let mut first_sel = true;
    for exposed in &cols {
        if !first_sel {
            ctx.sql.push_str(", ");
        }
        first_sel = false;
        write!(ctx.sql, "c.{}", quote_ident(exposed)).unwrap();
    }
    // FK from parent_link (array-child case).
    if let Some((_, rel, parent_table)) = parent_link {
        for (parent_col, _) in &rel.mapping {
            if !first_sel {
                ctx.sql.push_str(", ");
            }
            first_sel = false;
            let pcol = parent_table
                .find_column(parent_col)
                .ok_or_else(|| Error::Validate {
                    path: cte.into(),
                    message: format!(
                        "mapped parent column '{parent_col}' missing on '{}'",
                        parent_table.exposed_name
                    ),
                })?;
            write!(ctx.sql, "p.{}", quote_ident(&pcol.physical_name)).unwrap();
        }
    }
    // FK from each object relation (Phase 3A). Alias for each object-ord join
    // is `o_{rel_name}` — unique per object relation.
    for rel_name in &object_rel_names {
        let rel = table.find_relation(rel_name).unwrap();
        let obj_target = schema
            .table(&rel.target_table)
            .ok_or_else(|| Error::Validate {
                path: cte.into(),
                message: format!("object-relation target '{}' missing", rel.target_table),
            })?;
        for (_, target_col) in &rel.mapping {
            if !first_sel {
                ctx.sql.push_str(", ");
            }
            first_sel = false;
            let tcol = obj_target
                .find_column(target_col)
                .ok_or_else(|| Error::Validate {
                    path: cte.into(),
                    message: format!(
                        "mapped target column '{target_col}' missing on '{}'",
                        obj_target.exposed_name
                    ),
                })?;
            write!(ctx.sql, "o_{rel_name}.{}", quote_ident(&tcol.physical_name)).unwrap();
        }
    }

    // FROM clause. Base is the input CTE. Add JOINs for parent_link
    // (Phase 2) and each object relation (Phase 3A).
    write!(ctx.sql, " FROM {input_cte} c").unwrap();

    if let Some((parent_ord_cte_alias, _rel, _parent_table)) = parent_link {
        write!(
            ctx.sql,
            " JOIN {parent_ord_cte_alias} p ON p.ord = c.parent_ord"
        )
        .unwrap();
    }

    for rel_name in &object_rel_names {
        let obj_ord_cte = format!("{cte}_{rel_name}_ord");
        write!(
            ctx.sql,
            " JOIN {obj_ord_cte} o_{rel_name} ON o_{rel_name}.ord = c.ord"
        )
        .unwrap();
    }

    // For top-level and object-relation inserts (not child inserts), explicit
    // ORDER BY input ord so PG preserves input order through RETURNING. This
    // keeps the downstream ROW_NUMBER() OVER () correlation robust. Child
    // inserts don't need it — their correlation is via the JOIN not order.
    if parent_link.is_none() {
        ctx.sql.push_str(" ORDER BY c.ord");
    }

    if let Some(oc) = on_conflict {
        render_on_conflict(oc, table, scope_check, is_nested_cte, schema, ctx)?;
    }
    ctx.sql.push_str(" RETURNING *)");

    // 5. Track this CTE for returning-visibility lookup.
    ctx.inserted_ctes
        .insert(table_name.to_string(), cte.to_string());

    // 6. Always emit `{cte}_ord` so any consumer (array-children or object-relation
    //    parents) can JOIN against it.
    write!(
        ctx.sql,
        ", {cte}_ord AS (SELECT *, ROW_NUMBER() OVER () AS ord FROM {cte})"
    )
    .unwrap();

    // 6b. Scoped insert: emit this level's abort-on-violation guard so every
    //     row just inserted into {cte} must satisfy the table's scope check.
    if let Some(check) = scope_check {
        emit_scope_guard(cte, table, check, "inserted", schema, ctx)?;
    }

    // 7. For each nested array relation, emit the child chain.
    let any_nested_arrays = objects.iter().any(|o| !o.nested_arrays.is_empty());
    if any_nested_arrays {
        use std::collections::BTreeMap;
        let mut per_relation: BTreeMap<&str, (Vec<i64>, Vec<crate::ast::InsertObject>)> =
            BTreeMap::new();

        for (parent_ord_val, obj) in parent_ords.iter().zip(objects.iter()) {
            for (rel_name, nested) in &obj.nested_arrays {
                let entry = per_relation
                    .entry(rel_name.as_str())
                    .or_insert_with(|| (Vec::new(), Vec::new()));
                for child in &nested.rows {
                    entry.0.push(*parent_ord_val);
                    entry.1.push(child.clone());
                }
            }
        }

        for (rel_name, (child_ords, child_rows)) in per_relation {
            let rel = table
                .find_relation(rel_name)
                .ok_or_else(|| Error::Validate {
                    path: cte.into(),
                    message: format!("unknown relation '{rel_name}' on '{}'", table.exposed_name),
                })?;
            // Find the first parent row that has this array relation; read its on_conflict.
            // Array relations can be present in some parent rows and absent in others
            // (unlike object relations which are batch-uniform), so we scan all parents.
            let child_on_conflict = objects
                .iter()
                .find_map(|o| o.nested_arrays.get(rel_name))
                .and_then(|nai| nai.on_conflict.clone());
            // Scope check for this nested target table (same for every parent's
            // wrapper since it keys on the target table).
            let child_scope_check = objects
                .iter()
                .find_map(|o| o.nested_arrays.get(rel_name))
                .and_then(|nai| nai.scope_check.clone());
            let child_cte = format!("{cte}_{rel_name}");
            let parent_ord_cte_name = format!("{cte}_ord");
            ctx.sql.push_str(", ");
            render_insert_cte_recursive(
                &child_cte,
                &rel.target_table,
                &child_rows,
                &child_ords,
                child_on_conflict.as_ref(),
                child_scope_check.as_ref(),
                Some((&parent_ord_cte_name, rel, table)),
                true, // this is a nested CTE
                depth + 1,
                schema,
                ctx,
            )?;
        }
    }

    Ok(())
}

fn render_on_conflict(
    oc: &crate::ast::OnConflict,
    table: &Table,
    scope_check: Option<&crate::ast::BoolExpr>,
    nested_context: bool,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    write!(
        ctx.sql,
        " ON CONFLICT ON CONSTRAINT {} ",
        quote_ident(&oc.constraint)
    )
    .unwrap();
    if oc.update_columns.is_empty() {
        if nested_context {
            // Rewrite DO NOTHING → DO UPDATE SET pk = table.pk (a true no-op
            // referencing the existing row's value; NOT EXCLUDED.pk which
            // would change the value to the proposed sequence id) so RETURNING
            // includes conflict rows and the downstream ROW_NUMBER() ord
            // correlation stays 1:1 with input.
            let pk_name = table.primary_key.first().ok_or_else(|| Error::Validate {
                path: "on_conflict".into(),
                message: format!(
                    "nested DO NOTHING on-conflict requires a primary key on table '{}'",
                    table.exposed_name
                ),
            })?;
            let pk_col = table.find_column(pk_name).ok_or_else(|| Error::Validate {
                path: "on_conflict".into(),
                message: format!(
                    "primary key column '{pk_name}' missing on '{}'",
                    table.exposed_name
                ),
            })?;
            // Reference the table's own column (not EXCLUDED) so the update
            // is a true no-op: the existing PK value is preserved. Using
            // EXCLUDED.pk would set it to the new-row's serial value instead.
            write!(
                ctx.sql,
                "DO UPDATE SET {pk_phys} = {tbl}.{pk_phys}",
                pk_phys = quote_ident(&pk_col.physical_name),
                tbl = quote_ident(&table.physical_name),
            )
            .unwrap();
        } else {
            ctx.sql.push_str("DO NOTHING");
        }
    } else {
        ctx.sql.push_str("DO UPDATE SET ");
        for (i, exposed) in oc.update_columns.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(", ");
            }
            let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
                path: format!("on_conflict.update_columns.{exposed}"),
                message: format!("unknown column '{exposed}' on '{}'", table.exposed_name),
            })?;
            write!(
                ctx.sql,
                "{} = EXCLUDED.{}",
                quote_ident(&col.physical_name),
                quote_ident(&col.physical_name),
            )
            .unwrap();
        }
        // Combine the user's optional DO UPDATE WHERE with the scope predicate.
        // In a DO UPDATE these reference the *existing* (target) row, so the
        // scope predicate acts as a pre-image filter: a conflicting row outside
        // scope fails the WHERE and is skipped rather than overwritten. (The
        // post-insert guard still checks the resulting row, covering the
        // post-image.) Columns are qualified with the target table's name: the
        // insert's `INSERT … SELECT … FROM c` keeps the source relation `c` (and
        // `excluded`) in scope here, so a bare column would be ambiguous.
        let tref = quote_ident(&table.physical_name);
        match (oc.where_.as_ref(), scope_check) {
            (Some(user), Some(scope)) => {
                ctx.sql.push_str(" WHERE (");
                render_bool_expr(user, table, &tref, schema, ctx)?;
                ctx.sql.push_str(") AND (");
                render_bool_expr(scope, table, &tref, schema, ctx)?;
                ctx.sql.push(')');
            }
            (Some(user), None) => {
                ctx.sql.push_str(" WHERE ");
                render_bool_expr(user, table, &tref, schema, ctx)?;
            }
            (None, Some(scope)) => {
                ctx.sql.push_str(" WHERE ");
                render_bool_expr(scope, table, &tref, schema, ctx)?;
            }
            (None, None) => {}
        }
    }
    Ok(())
}

fn render_update_cte(
    cte: &str,
    table_name: &str,
    where_: &crate::ast::BoolExpr,
    set: &std::collections::BTreeMap<String, Val>,
    scope_check: Option<&crate::ast::BoolExpr>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;
    write!(
        ctx.sql,
        "{cte} AS (UPDATE {}.{} SET ",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    render_set_clause(table, set, cte, ctx)?;
    ctx.sql.push_str(" WHERE ");
    render_bool_expr_no_alias(where_, table, schema, ctx)?;
    ctx.sql.push_str(" RETURNING *)");
    if let Some(check) = scope_check {
        emit_scope_guard(cte, table, check, "modified", schema, ctx)?;
    }
    Ok(())
}

fn render_update_by_pk_cte(
    cte: &str,
    table_name: &str,
    pk: &[(String, Val)],
    set: &std::collections::BTreeMap<String, Val>,
    scope: Option<&crate::ast::BoolExpr>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;
    write!(
        ctx.sql,
        "{cte} AS (UPDATE {}.{} SET ",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    render_set_clause(table, set, cte, ctx)?;
    ctx.sql.push_str(" WHERE ");
    render_pk_predicate(table, pk, scope, cte, None, schema, ctx)?;
    ctx.sql.push_str(" RETURNING *)");
    if let Some(check) = scope {
        emit_scope_guard(cte, table, check, "modified", schema, ctx)?;
    }
    Ok(())
}

fn render_delete_cte(
    cte: &str,
    table_name: &str,
    where_: &crate::ast::BoolExpr,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;
    write!(
        ctx.sql,
        "{cte} AS (DELETE FROM {}.{} WHERE ",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    render_bool_expr_no_alias(where_, table, schema, ctx)?;
    ctx.sql.push_str(" RETURNING *)");
    Ok(())
}

fn render_delete_by_pk_cte(
    cte: &str,
    table_name: &str,
    pk: &[(String, Val)],
    scope: Option<&crate::ast::BoolExpr>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;
    write!(
        ctx.sql,
        "{cte} AS (DELETE FROM {}.{} WHERE ",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    render_pk_predicate(table, pk, scope, cte, None, schema, ctx)?;
    ctx.sql.push_str(" RETURNING *)");
    Ok(())
}

/// `col = $n, …` for an update's `_set`, shared by the one-statement renderer
/// and the plan builder so a rule about how a value binds lands in both.
pub(crate) fn render_set_clause(
    table: &Table,
    set: &std::collections::BTreeMap<String, Val>,
    path: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    for (i, (exposed, value)) in set.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
            path: format!("{path}._set.{exposed}"),
            message: format!("unknown column '{exposed}'"),
        })?;
        let n = ctx.push_scalar(value, &col.ty, || format!("{path}._set.{exposed}"))?;
        write!(
            ctx.sql,
            "{} = {}",
            quote_ident(&col.physical_name),
            ctx.dialect.param(n, &col.ty)
        )
        .unwrap();
    }
    Ok(())
}

/// `pk = $n AND … [AND (scope)]` for the `_by_pk` forms, shared as
/// [`render_set_clause`] is. Unqualified without `alias`, as `UPDATE` and
/// `DELETE` want it; qualified with one for a read.
pub(crate) fn render_pk_predicate(
    table: &Table,
    pk: &[(String, Val)],
    scope: Option<&crate::ast::BoolExpr>,
    path: &str,
    alias: Option<&str>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    for (i, (col_name, value)) in pk.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(" AND ");
        }
        let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
            path: format!("{path}.pk.{col_name}"),
            message: format!("unknown column '{col_name}'"),
        })?;
        // A primary key is never null, so a null here matches nothing either.
        let n = ctx.push_comparison(value, &col.ty, || format!("{path}.pk.{col_name}"))?;
        if let Some(a) = alias {
            write!(ctx.sql, "{a}.").unwrap();
        }
        write!(
            ctx.sql,
            "{} = {}",
            quote_ident(&col.physical_name),
            ctx.dialect.param(n, &col.ty)
        )
        .unwrap();
    }
    if let Some(expr) = scope {
        ctx.sql.push_str(" AND (");
        match alias {
            Some(a) => render_bool_expr(expr, table, a, schema, ctx)?,
            None => render_bool_expr_no_alias(expr, table, schema, ctx)?,
        }
        ctx.sql.push(')');
    }
    Ok(())
}

fn render_mutation_output_for(
    mf: &crate::ast::MutationField,
    cte: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let prev = ctx.current_mutation_cte.replace(cte.to_string());
    let result = render_mutation_output_for_inner(mf, cte, schema, ctx);
    ctx.current_mutation_cte = prev;
    result
}

fn render_mutation_output_for_inner(
    mf: &crate::ast::MutationField,
    cte: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::MutationField;
    match mf {
        MutationField::Insert {
            alias,
            table,
            returning,
            response_typenames,
            one,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(ctx.sql, "'{}', ", escape_string_literal(alias)).unwrap();
            if *one {
                ctx.sql.push_str("(SELECT ");
                if returning.is_empty() {
                    ctx.sql.push_str(ctx.dialect.empty_json_object());
                } else {
                    render_json_build_object_for_nodes(returning, cte, tbl, alias, schema, ctx)?;
                }
                write!(ctx.sql, " FROM {cte} LIMIT 1)").unwrap();
            } else {
                ctx.sql.push_str(ctx.dialect.json_object_open());
                // affected_rows sums the parent CTE with every child CTE that
                // was emitted under it.
                ctx.sql.push_str("'affected_rows', (");
                // Gather all CTEs whose aliases start with the umbrella
                // `{cte}` (the parent) or `{cte}_` (the children at any level).
                // Use ctx.inserted_ctes for this — its values are all the
                // CTE aliases.
                let mut matching: Vec<&String> = ctx
                    .inserted_ctes
                    .values()
                    .filter(|v| v.as_str() == cte || v.starts_with(&format!("{cte}_")))
                    .collect();
                matching.sort();
                for (i, c) in matching.iter().enumerate() {
                    if i > 0 {
                        ctx.sql.push_str(" + ");
                    }
                    write!(ctx.sql, "(SELECT count(*) FROM {c})").unwrap();
                }
                if matching.is_empty() {
                    // Defensive — should never happen; means render_insert_cte
                    // didn't record the parent CTE. Fall back to bare count.
                    write!(ctx.sql, "SELECT count(*) FROM {cte}").unwrap();
                }
                ctx.sql.push(')');

                if !returning.is_empty() {
                    write!(
                        ctx.sql,
                        ", 'returning', (SELECT {}",
                        ctx.dialect.json_agg_open()
                    )
                    .unwrap();
                    render_json_build_object_for_nodes(returning, cte, tbl, alias, schema, ctx)?;
                    write!(ctx.sql, "{} FROM {cte})", ctx.dialect.json_agg_close()).unwrap();
                } else {
                    write!(ctx.sql, ", 'returning', {}", ctx.dialect.empty_json_array()).unwrap();
                }
                render_response_typenames(response_typenames, tbl, ctx);
                ctx.sql.push(')');
            }
        }
        MutationField::Update {
            alias,
            table,
            returning,
            response_typenames,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(
                ctx.sql,
                "'{}', {}",
                escape_string_literal(alias),
                ctx.dialect.json_object_open()
            )
            .unwrap();
            write!(ctx.sql, "'affected_rows', (SELECT count(*) FROM {cte})").unwrap();
            if !returning.is_empty() {
                write!(
                    ctx.sql,
                    ", 'returning', (SELECT {}",
                    ctx.dialect.json_agg_open()
                )
                .unwrap();
                render_json_build_object_for_nodes(returning, cte, tbl, alias, schema, ctx)?;
                write!(ctx.sql, "{} FROM {cte})", ctx.dialect.json_agg_close()).unwrap();
            } else {
                write!(ctx.sql, ", 'returning', {}", ctx.dialect.empty_json_array()).unwrap();
            }
            render_response_typenames(response_typenames, tbl, ctx);
            ctx.sql.push(')');
        }
        MutationField::UpdateByPk {
            alias,
            table,
            selection,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(ctx.sql, "'{}', (SELECT ", escape_string_literal(alias)).unwrap();
            if selection.is_empty() {
                ctx.sql.push_str(ctx.dialect.empty_json_object());
            } else {
                render_json_build_object_for_nodes(selection, cte, tbl, alias, schema, ctx)?;
            }
            write!(ctx.sql, " FROM {cte} LIMIT 1)").unwrap();
        }
        MutationField::Delete {
            alias,
            table,
            returning,
            response_typenames,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(
                ctx.sql,
                "'{}', {}",
                escape_string_literal(alias),
                ctx.dialect.json_object_open()
            )
            .unwrap();
            write!(ctx.sql, "'affected_rows', (SELECT count(*) FROM {cte})").unwrap();
            if !returning.is_empty() {
                write!(
                    ctx.sql,
                    ", 'returning', (SELECT {}",
                    ctx.dialect.json_agg_open()
                )
                .unwrap();
                render_json_build_object_for_nodes(returning, cte, tbl, alias, schema, ctx)?;
                write!(ctx.sql, "{} FROM {cte})", ctx.dialect.json_agg_close()).unwrap();
            } else {
                write!(ctx.sql, ", 'returning', {}", ctx.dialect.empty_json_array()).unwrap();
            }
            render_response_typenames(response_typenames, tbl, ctx);
            ctx.sql.push(')');
        }
        MutationField::DeleteByPk {
            alias,
            table,
            selection,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(ctx.sql, "'{}', (SELECT ", escape_string_literal(alias)).unwrap();
            if selection.is_empty() {
                ctx.sql.push_str(ctx.dialect.empty_json_object());
            } else {
                render_json_build_object_for_nodes(selection, cte, tbl, alias, schema, ctx)?;
            }
            write!(ctx.sql, " FROM {cte} LIMIT 1)").unwrap();
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn render_aggregate(
    root: &RootField,
    ops: &[crate::ast::AggSelect],
    nodes: Option<&[Field]>,
    typenames: &[String],
    nodes_limit: Option<&crate::ast::Count>,
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    render_aggregate_object(
        &root.args,
        ops,
        nodes,
        typenames,
        nodes_limit,
        table,
        &root.alias,
        None,
        schema,
        ctx,
    )
}

/// One `_aggregate` answer, at a root or on a row.
///
/// `correlation` is what distinguishes them: at a root the source is the whole
/// table, and on a row it is the rows of an array relation, which needs the
/// join back to the parent. Everything above that — which keys the object
/// carries, whether the source is read at all — is the same question in both
/// places, so it is answered once.
#[allow(clippy::too_many_arguments)]
fn render_aggregate_object(
    args: &QueryArgs,
    ops: &[crate::ast::AggSelect],
    nodes: Option<&[Field]>,
    typenames: &[String],
    nodes_limit: Option<&crate::ast::Count>,
    table: &Table,
    path: &str,
    correlation: Option<&Correlation<'_>>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    // The parser refuses both of these with a document path; the typed builder
    // never goes near the parser, so the checks that keep a plausible wrong
    // answer from going out have to live here too.
    if !args.distinct_on.is_empty() {
        return Err(Error::Validate {
            path: format!("{path}.distinct_on"),
            message: "an aggregate cannot take 'distinct_on'; \
                      use `count(columns: [\u{2026}], distinct: true)` to count distinct values"
                .into(),
        });
    }
    let mut seen: Vec<&str> = Vec::new();
    for sel in ops {
        if seen.contains(&sel.alias.as_str()) {
            return Err(Error::Validate {
                path: format!("{path}.aggregate.{}", sel.alias),
                message: format!(
                    "two aggregate fields both answer to '{}'; give one of them an alias",
                    sel.alias
                ),
            });
        }
        seen.push(sel.alias.as_str());
    }
    let inner_alias = ctx.next_alias("t");

    // Whether anything in the projection actually reads the source rows. Type
    // names do not, and neither does an empty `aggregate`. Without something
    // that collapses — an aggregate function, or the `json_agg` over `nodes` —
    // selecting FROM the source would yield one row per source row inside a
    // scalar subquery, which Postgres rejects outright at two rows and answers
    // with null at zero. So when nothing reads the rows, do not read them.
    // `nodes` on a source of its own does not need the shared one.
    let needs_source = (nodes.is_some() && nodes_limit.is_none())
        || ops
            .iter()
            .any(|s| !matches!(s.op, crate::ast::AggOp::Typename));

    write!(ctx.sql, "(SELECT {}", ctx.dialect.json_object_open()).unwrap();
    let mut first = true;
    for alias in typenames {
        if !first {
            ctx.sql.push_str(", ");
        }
        first = false;
        write!(
            ctx.sql,
            "'{}', {}",
            escape_string_literal(alias),
            ctx.dialect
                .text_literal(&crate::type_names::aggregate(table))
        )
        .unwrap();
    }
    // Only when the document asked for it: an `aggregate` key in a response to
    // `{ nodes { … } }` is a field the caller did not select.
    if !ops.is_empty() {
        if !first {
            ctx.sql.push_str(", ");
        }
        first = false;
        write!(ctx.sql, "'aggregate', {}", ctx.dialect.json_object_open()).unwrap();
        for (i, op) in ops.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(", ");
            }
            render_agg_op(op, &inner_alias, table, ctx)?;
        }
        ctx.sql.push(')');
    }

    if let Some(node_fields) = nodes {
        if !first {
            ctx.sql.push_str(", ");
        }
        match nodes_limit {
            // A cap meant for `nodes` alone cannot ride on the shared source —
            // the same `LIMIT` would decide what `count` counted — so `nodes`
            // reads its own.
            Some(limit) => {
                let node_alias = ctx.next_alias("t");
                let mut node_args = args.clone();
                node_args.limit = Some(limit.clone());
                write!(ctx.sql, "'nodes', (SELECT {}", ctx.dialect.json_agg_open()).unwrap();
                render_json_build_object_for_nodes(
                    node_fields,
                    &node_alias,
                    table,
                    path,
                    schema,
                    ctx,
                )?;
                write!(ctx.sql, "{} FROM (", ctx.dialect.json_agg_close()).unwrap();
                render_aggregate_source(
                    &node_args,
                    &[],
                    nodes,
                    table,
                    path,
                    correlation,
                    schema,
                    ctx,
                )?;
                write!(ctx.sql, ") {node_alias})").unwrap();
            }
            None => {
                write!(ctx.sql, "'nodes', {}", ctx.dialect.json_agg_open()).unwrap();
                render_json_build_object_for_nodes(
                    node_fields,
                    &inner_alias,
                    table,
                    path,
                    schema,
                    ctx,
                )?;
                ctx.sql.push_str(ctx.dialect.json_agg_close());
            }
        }
    }

    ctx.sql.push(')');
    if needs_source {
        ctx.sql.push_str(" FROM (");
        render_aggregate_source(args, ops, nodes, table, path, correlation, schema, ctx)?;
        ctx.sql.push_str(") ");
        ctx.sql.push_str(&inner_alias);
    }
    ctx.sql.push(')');
    Ok(())
}

/// The join that ties a relation aggregate's source back to the row it hangs
/// from.
struct Correlation<'a> {
    rel: &'a crate::schema::Relation,
    parent: &'a Table,
    parent_alias: &'a str,
}

/// A `_aggregate` field on a row: the aggregate object, over the rows of an
/// array relation, correlated to the row it hangs from.
#[allow(clippy::too_many_arguments)]
fn render_relation_aggregate(
    name: &str,
    alias: &str,
    args: &QueryArgs,
    ops: &[crate::ast::AggSelect],
    nodes: Option<&[Field]>,
    typenames: &[String],
    nodes_limit: Option<&crate::ast::Count>,
    parent_table: &Table,
    parent_alias: &str,
    schema: &Schema,
    parent_path: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let path = format!("{parent_path}.{alias}");
    let rel = parent_table
        .find_relation(name)
        .ok_or_else(|| Error::Validate {
            path: path.clone(),
            message: format!(
                "unknown relation '{name}' on '{}'",
                parent_table.exposed_name
            ),
        })?;
    let target = schema
        .table(&rel.target_table)
        .ok_or_else(|| Error::Validate {
            path: path.clone(),
            message: format!("relation target table '{}' missing", rel.target_table),
        })?;
    let correlation = Correlation {
        rel,
        parent: parent_table,
        parent_alias,
    };
    render_aggregate_object(
        args,
        ops,
        nodes,
        typenames,
        nodes_limit,
        target,
        &path,
        Some(&correlation),
        schema,
        ctx,
    )
}

fn render_agg_op(
    sel: &crate::ast::AggSelect,
    table_alias: &str,
    table: &Table,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::AggOp;
    let key = escape_string_literal(&sel.alias);
    match &sel.op {
        AggOp::Count { columns, distinct } => {
            // The parser refuses this pairing; the builder can still construct
            // it, and dropping the DISTINCT silently answers a different
            // question than the one asked.
            if *distinct && columns.is_empty() {
                return Err(Error::Validate {
                    path: format!("aggregate.{}.distinct", sel.alias),
                    message: "'distinct' needs 'columns' to be distinct on".into(),
                });
            }
            if columns.len() > 1 && !ctx.dialect.counts_tuples() {
                return Err(Error::Unsupported {
                    message: format!(
                        "count over several columns is not available on {:?}: count one \
                         column, or count(*)",
                        ctx.dialect
                    ),
                });
            }
            write!(ctx.sql, "'{key}', count(").unwrap();
            if columns.is_empty() {
                ctx.sql.push('*');
            } else {
                if *distinct {
                    ctx.sql.push_str("DISTINCT ");
                }
                // More than one column counts the tuple: a row constructor is
                // NULL only when every field is, which is the reading that makes
                // `count(DISTINCT (a, b))` mean distinct pairs.
                if columns.len() > 1 {
                    ctx.sql.push('(');
                }
                for (i, exposed) in columns.iter().enumerate() {
                    if i > 0 {
                        ctx.sql.push_str(", ");
                    }
                    let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
                        path: format!("aggregate.{}", sel.alias),
                        message: format!("unknown column '{exposed}' on '{}'", table.exposed_name),
                    })?;
                    write!(ctx.sql, "{table_alias}.{}", quote_ident(&col.physical_name)).unwrap();
                }
                if columns.len() > 1 {
                    ctx.sql.push(')');
                }
            }
            ctx.sql.push(')');
            Ok(())
        }
        AggOp::Func { func, fields } => {
            render_agg_func(&key, *func, fields, table_alias, table, ctx)
        }
        AggOp::Typename => {
            write!(
                ctx.sql,
                "'{key}', {}",
                ctx.dialect
                    .text_literal(&crate::type_names::aggregate_fields(table))
            )
            .unwrap();
            Ok(())
        }
    }
}

fn render_agg_func(
    key: &str,
    func: crate::ast::AggFunc,
    fields: &[crate::ast::AggField],
    table_alias: &str,
    table: &Table,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::AggField;
    let pg_func = func.name();
    if !ctx.dialect.supports_agg(func) {
        return Err(Error::Unsupported {
            message: format!("'{pg_func}' is not available on {:?}", ctx.dialect),
        });
    }
    // Same rule the parser applies: a function with no column to apply to
    // would render `{}` with the function never called — an answer that looks
    // complete and contains nothing.
    if !fields.iter().any(|f| matches!(f, AggField::Column(_))) {
        return Err(Error::Validate {
            path: format!("aggregate.{key}"),
            message: format!("'{pg_func}' needs at least one column"),
        });
    }
    // One key per alias inside the group too — the parser merges duplicates,
    // the builder has no pass that would.
    let mut seen: Vec<&str> = Vec::with_capacity(fields.len());
    for f in fields {
        let k = match f {
            AggField::Column(c) => c.alias.as_str(),
            AggField::Typename { alias } => alias.as_str(),
        };
        if seen.contains(&k) {
            return Err(Error::Validate {
                path: format!("aggregate.{key}.{k}"),
                message: format!("two fields both answer to '{k}'; give one of them an alias"),
            });
        }
        seen.push(k);
    }
    write!(ctx.sql, "'{key}', {}", ctx.dialect.json_object_open()).unwrap();
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match f {
            AggField::Typename { alias } => {
                write!(
                    ctx.sql,
                    "'{}', {}",
                    escape_string_literal(alias),
                    ctx.dialect
                        .text_literal(&crate::type_names::agg_op_fields(table, pg_func))
                )
                .unwrap();
            }
            AggField::Column(c) => {
                let col = table
                    .find_column(&c.column)
                    .ok_or_else(|| Error::Validate {
                        path: format!("aggregate.{key}.{}", c.alias),
                        message: format!(
                            "unknown column '{}' on '{}'",
                            c.column, table.exposed_name
                        ),
                    })?;
                // The parser refuses this earlier with a document path, but the
                // typed builder never goes near the parser — this is the one
                // point both entry points pass through, so the check that keeps
                // "function sum(text) does not exist" from being PostgreSQL's
                // answer has to live here too.
                if !crate::type_system::applies(func, &col.ty) {
                    return Err(Error::Validate {
                        path: format!("aggregate.{key}.{}", c.alias),
                        message: format!(
                            "'{pg_func}' does not apply to '{}': {}",
                            col.exposed_name,
                            crate::type_system::why_inapplicable(func, &col.ty)
                        ),
                    });
                }
                write!(
                    ctx.sql,
                    "'{}', {pg_func}({table_alias}.{})",
                    escape_string_literal(&c.alias),
                    quote_ident(&col.physical_name)
                )
                .unwrap();
            }
        }
    }
    ctx.sql.push(')');
    Ok(())
}

pub(crate) fn render_json_build_object_for_nodes(
    fields: &[Field],
    table_alias: &str,
    table: &Table,
    parent_path: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    ensure_unique_selection_keys(fields, parent_path)?;
    ctx.sql.push_str(ctx.dialect.json_object_open());
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match f {
            Field::Typename { alias } => {
                write!(
                    ctx.sql,
                    "'{}', {}",
                    escape_string_literal(alias),
                    ctx.dialect.text_literal(crate::type_names::row(table))
                )
                .unwrap();
            }
            Field::Column { column, alias } => {
                let col = table.find_column(column).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.nodes.{alias}"),
                    message: format!("unknown column '{column}' on '{}'", table.exposed_name),
                })?;
                // Escaped like every other response key: on the parser path a
                // GraphQL Name cannot carry a quote, but these aliases also
                // arrive from the typed builder, where they are arbitrary
                // strings — unescaped, one apostrophe breaks the statement and
                // a crafted one rewrites it.
                let value = format!("{table_alias}.{}", quote_ident(&col.physical_name));
                write!(
                    ctx.sql,
                    "'{}', {}",
                    escape_string_literal(alias),
                    ctx.dialect.value_as_json(value, json_kind(&col.ty))
                )
                .unwrap();
            }
            Field::JsonPath {
                column,
                alias,
                path,
            } => {
                let col = table.find_column(column).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.nodes.{alias}"),
                    message: format!("unknown column '{column}' on '{}'", table.exposed_name),
                })?;
                let err_path = format!("{parent_path}.nodes.{alias}");
                let expr = render_json_path_expr(table_alias, col, path, &err_path, ctx)?;
                write!(
                    ctx.sql,
                    "'{}', {}",
                    escape_string_literal(alias),
                    ctx.dialect.value_as_json(expr, JsonKind::Json)
                )
                .unwrap();
            }
            Field::Relation {
                name,
                alias: rel_alias,
                args,
                selection,
            } => {
                write!(ctx.sql, "'{}', ", escape_string_literal(rel_alias)).unwrap();
                render_relation_subquery(
                    name,
                    rel_alias,
                    args,
                    selection,
                    table,
                    table_alias,
                    schema,
                    parent_path,
                    ctx,
                )?;
            }
            Field::RelationAggregate {
                name,
                alias: rel_alias,
                args,
                ops,
                nodes,
                typenames,
                nodes_limit,
            } => {
                write!(ctx.sql, "'{}', ", escape_string_literal(rel_alias)).unwrap();
                render_relation_aggregate(
                    name,
                    rel_alias,
                    args,
                    ops,
                    nodes.as_deref(),
                    typenames,
                    nodes_limit.as_ref(),
                    table,
                    table_alias,
                    schema,
                    parent_path,
                    ctx,
                )?;
            }
        }
    }
    ctx.sql.push(')');
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn render_aggregate_source(
    args: &QueryArgs,
    ops: &[crate::ast::AggSelect],
    nodes: Option<&[Field]>,
    table: &Table,
    path: &str,
    correlation: Option<&Correlation<'_>>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use std::collections::BTreeSet;

    let mut cols_needed: BTreeSet<String> = BTreeSet::new();
    for sel in ops {
        // `count(columns: …)` reads columns too, so the inner select has to
        // project them just like sum/avg/max/min do.
        for c in sel.op.columns_read() {
            let col = table.find_column(c).ok_or_else(|| Error::Validate {
                path: format!("{path}.aggregate"),
                message: format!("unknown column '{c}' on '{}'", table.exposed_name),
            })?;
            cols_needed.insert(col.physical_name.clone());
        }
    }
    if let Some(fields) = nodes {
        for f in fields {
            let column = match f {
                Field::Column { column, .. } | Field::JsonPath { column, .. } => column,
                // A relation under `nodes` correlates back to this source on
                // the relation's local columns, so those must be projected —
                // the subquery reads them off the source alias, not the table.
                Field::Relation { name, .. } | Field::RelationAggregate { name, .. } => {
                    let rel = table.find_relation(name).ok_or_else(|| Error::Validate {
                        path: format!("{path}.nodes"),
                        message: format!("unknown relation '{name}' on '{}'", table.exposed_name),
                    })?;
                    for (local, _) in &rel.mapping {
                        let col = table.find_column(local).ok_or_else(|| Error::Validate {
                            path: format!("{path}.nodes"),
                            message: format!(
                                "relation mapping: unknown local column '{local}' on '{}'",
                                table.exposed_name
                            ),
                        })?;
                        cols_needed.insert(col.physical_name.clone());
                    }
                    continue;
                }
                Field::Typename { .. } => continue,
            };
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("{path}.nodes"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            cols_needed.insert(col.physical_name.clone());
        }
    }

    ctx.sql.push_str("SELECT ");
    if cols_needed.is_empty() {
        ctx.sql.push('1');
    } else {
        let mut first = true;
        for c in &cols_needed {
            if !first {
                ctx.sql.push_str(", ");
            }
            first = false;
            ctx.sql.push_str(&quote_ident(c));
        }
    }
    // Alias the source so the where clause goes through the standard
    // renderer, which supports EXISTS relation filters (needed both for
    // user-written relation filters and for scope-injected predicates).
    let src_alias = ctx.next_alias("s");
    // Inside a mutation, a relation reads the CTE holding the rows this
    // statement just wrote — and an aggregate over that relation has to read
    // the same thing, or one response reports a row under `posts` and `count: 0`
    // under `posts_aggregate`.
    let visible_cte = correlation.and_then(|c| {
        match (
            ctx.inserted_ctes.get(&c.rel.target_table),
            ctx.current_mutation_cte.as_deref(),
        ) {
            (Some(cte_alias), Some(prefix))
                if cte_alias == prefix || cte_alias.starts_with(&format!("{prefix}_")) =>
            {
                Some(cte_alias.clone())
            }
            _ => None,
        }
    });
    match visible_cte {
        Some(cte_alias) => write!(ctx.sql, " FROM {cte_alias} {src_alias}").unwrap(),
        None => write!(
            ctx.sql,
            " FROM {}.{} {src_alias}",
            quote_ident(&table.physical_schema),
            quote_ident(&table.physical_name),
        )
        .unwrap(),
    }

    // The correlation and the user's `where` are both filters on the same
    // source, so they are ANDed; the correlation goes first because it is the
    // one that makes the subquery a per-row question at all.
    let mut filtered = false;
    if let Some(c) = correlation {
        ctx.sql.push_str(" WHERE ");
        filtered = true;
        for (i, (local_col, remote_col)) in c.rel.mapping.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(" AND ");
            }
            let l = c
                .parent
                .find_column(local_col)
                .ok_or_else(|| Error::Validate {
                    path: path.to_string(),
                    message: format!(
                        "relation mapping: unknown local column '{local_col}' on '{}'",
                        c.parent.exposed_name
                    ),
                })?;
            let r = table
                .find_column(remote_col)
                .ok_or_else(|| Error::Validate {
                    path: path.to_string(),
                    message: format!(
                        "relation mapping: unknown remote column '{remote_col}' on '{}'",
                        table.exposed_name
                    ),
                })?;
            write!(
                ctx.sql,
                "{src_alias}.{} = {}.{}",
                quote_ident(&r.physical_name),
                c.parent_alias,
                quote_ident(&l.physical_name),
            )
            .unwrap();
        }
    }
    if let Some(expr) = args.where_.as_ref() {
        ctx.sql.push_str(if filtered { " AND " } else { " WHERE " });
        render_bool_expr(expr, table, &src_alias, schema, ctx)?;
    }
    // The same renderer the row list uses — a hand-rolled loop here read only
    // `ob.column`, so an `order_by` through a relation silently sorted by the
    // source table's column of that name, and `NULLS FIRST|LAST` never
    // rendered at all.
    if !args.order_by.is_empty() {
        ctx.sql.push_str(" ORDER BY ");
        for (i, ob) in args.order_by.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(", ");
            }
            render_order_by_expr(
                ob,
                table,
                &src_alias,
                schema,
                &format!("{path}.order_by"),
                ctx,
            )?;
            render_order_dir(ob, ctx);
        }
    }
    render_limit_offset(args, path, ctx);
    Ok(())
}

#[allow(clippy::only_used_in_recursion)]
pub(crate) fn render_bool_expr_no_alias(
    expr: &crate::ast::BoolExpr,
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::BoolExpr;
    match expr {
        BoolExpr::And(parts) => {
            if parts.is_empty() {
                ctx.sql.push_str("TRUE");
                return Ok(());
            }
            ctx.sql.push('(');
            for (i, p) in parts.iter().enumerate() {
                if i > 0 {
                    ctx.sql.push_str(" AND ");
                }
                render_bool_expr_no_alias(p, table, schema, ctx)?;
            }
            ctx.sql.push(')');
            Ok(())
        }
        BoolExpr::Or(parts) => {
            refuse_optional_under(parts, "`_or`")?;
            if parts.is_empty() {
                ctx.sql.push_str("FALSE");
                return Ok(());
            }
            ctx.sql.push('(');
            for (i, p) in parts.iter().enumerate() {
                if i > 0 {
                    ctx.sql.push_str(" OR ");
                }
                render_bool_expr_no_alias(p, table, schema, ctx)?;
            }
            ctx.sql.push(')');
            Ok(())
        }
        BoolExpr::Not(inner) => {
            refuse_optional_under(std::iter::once(inner.as_ref()), "`_not`")?;
            ctx.sql.push_str("(NOT ");
            render_bool_expr_no_alias(inner, table, schema, ctx)?;
            ctx.sql.push(')');
            Ok(())
        }
        BoolExpr::Compare { column, op, value } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            check_cmp_applies(*op, col, ctx.dialect)?;
            let lhs = quote_ident(&col.physical_name);
            render_cmp(&lhs, &col.ty, *op, value, || format!("where.{column}"), ctx)
        }
        BoolExpr::Optional(inner) => render_optional(inner, table, None, schema, ctx),
        BoolExpr::Const(b) => {
            ctx.sql.push_str(if *b { "TRUE" } else { "FALSE" });
            Ok(())
        }
        BoolExpr::ValueCompare {
            left,
            op,
            right,
            pg,
        } => render_value_compare(left, *op, right, pg, ctx),
        BoolExpr::ValueInList {
            value,
            values,
            pg,
            negated,
        } => render_value_in_list(value, values, pg, *negated, ctx),
        BoolExpr::IsNull { column, is_null } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            render_is_null(&quote_ident(&col.physical_name), column, is_null, ctx)
        }
        BoolExpr::InList {
            column,
            values,
            negated,
        } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            check_cmp_applies(crate::ast::CmpOp::Eq, col, ctx.dialect)?;
            if is_empty_literal_list(values) {
                ctx.sql.push_str(if *negated { "TRUE" } else { "FALSE" });
                return Ok(());
            }
            let n = ctx.push_array(values, &col.ty, || format!("where.{column}"))?;
            let lhs = quote_ident(&col.physical_name);
            write!(
                ctx.sql,
                "{}",
                ctx.dialect.in_list(&lhs, n, &col.ty, *negated)
            )
            .unwrap();
            Ok(())
        }
        BoolExpr::Relation { name, inner } => {
            // No table alias here (UPDATE/DELETE/ON CONFLICT target the table by
            // name), so correlate the EXISTS back to it via the table's physical
            // name rather than an alias.
            let rel = table.find_relation(name).ok_or_else(|| Error::Validate {
                path: format!("where.{name}"),
                message: format!("unknown relation '{name}' on '{}'", table.exposed_name),
            })?;
            let target = schema
                .table(&rel.target_table)
                .ok_or_else(|| Error::Validate {
                    path: format!("where.{name}"),
                    message: format!("relation target table '{}' missing", rel.target_table),
                })?;
            let remote_alias = ctx.next_alias("e");
            write!(
                ctx.sql,
                "EXISTS (SELECT 1 FROM {}.{} {remote_alias} WHERE ",
                quote_ident(&target.physical_schema),
                quote_ident(&target.physical_name),
            )
            .unwrap();
            for (i, (local_col, remote_col)) in rel.mapping.iter().enumerate() {
                if i > 0 {
                    ctx.sql.push_str(" AND ");
                }
                let l = table
                    .find_column(local_col)
                    .ok_or_else(|| Error::Validate {
                        path: format!("where.{name}"),
                        message: format!("relation mapping: unknown local column '{local_col}'"),
                    })?;
                let r = target
                    .find_column(remote_col)
                    .ok_or_else(|| Error::Validate {
                        path: format!("where.{name}"),
                        message: format!("relation mapping: unknown remote column '{remote_col}'"),
                    })?;
                write!(
                    ctx.sql,
                    "{remote_alias}.{} = {}.{}",
                    quote_ident(&r.physical_name),
                    quote_ident(&table.physical_name),
                    quote_ident(&l.physical_name),
                )
                .unwrap();
            }
            ctx.sql.push_str(" AND ");
            render_bool_expr(inner, target, &remote_alias, schema, ctx)?;
            ctx.sql.push(')');
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Field, Operation, QueryArgs, RootBody, RootField};
    use crate::schema::{ColumnType, Schema, Table};

    /// Tests build fully literal operations, so rendering can resolve the
    /// parameters straight away — the shape every caller of `Engine::query`
    /// sees.
    fn render(op: &Operation, schema: &Schema) -> Result<(String, Vec<Bind>)> {
        render_now(op, schema, &Inputs::none(), Dialect::Postgres)
    }

    fn users_schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("name", "name", ColumnType::Text, true),
            )
            .build()
    }

    #[test]
    fn render_plain_list() {
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs::default(),
            body: RootBody::List {
                selection: vec![
                    Field::Column {
                        column: "id".into(),
                        alias: "id".into(),
                    },
                    Field::Column {
                        column: "name".into(),
                        alias: "name".into(),
                    },
                ],
            },
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert!(binds.is_empty());
    }

    fn docs_schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("docs", "public", "docs")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("data", "data", ColumnType::Jsonb, true)
                    .column("meta", "meta", ColumnType::Json, true)
                    .column("name", "name", ColumnType::Text, true),
            )
            .build()
    }

    #[test]
    fn json_has_no_equality_on_any_path() {
        use crate::ast::{BoolExpr, CmpOp};
        let list = |where_: BoolExpr| {
            Operation::Query(vec![RootField {
                table: "docs".into(),
                alias: "docs".into(),
                args: QueryArgs {
                    where_: Some(where_),
                    ..Default::default()
                },
                body: RootBody::List {
                    selection: vec![Field::Column {
                        column: "id".into(),
                        alias: "id".into(),
                    }],
                },
            }])
        };
        let eq = BoolExpr::Compare {
            column: "meta".into(),
            op: CmpOp::Eq,
            value: serde_json::json!({}).into(),
        };
        let in_ = BoolExpr::InList {
            column: "meta".into(),
            values: serde_json::json!([{}]).into(),
            negated: false,
        };
        let value = BoolExpr::ValueCompare {
            left: serde_json::json!({}).into(),
            op: CmpOp::Eq,
            right: serde_json::json!({}).into(),
            pg: ColumnType::Json,
        };
        for where_ in [eq.clone(), in_.clone(), value] {
            let err = render(&list(where_), &docs_schema()).unwrap_err();
            assert!(
                matches!(&err, Error::Validate { message, .. } if message.contains("json has no equality")),
                "{err:?}"
            );
        }
        // The mutation renderer answers the same.
        for where_ in [eq, in_] {
            let op = Operation::Mutation(vec![crate::ast::MutationField::Delete {
                alias: "d".into(),
                table: "docs".into(),
                where_,
                returning: vec![],
                response_typenames: vec![],
            }]);
            let err = render(&op, &docs_schema()).unwrap_err();
            assert!(
                matches!(&err, Error::Validate { message, .. } if message.contains("json has no equality")),
                "{err:?}"
            );
        }
        // jsonb compares.
        let ok = list(BoolExpr::Compare {
            column: "data".into(),
            op: CmpOp::Eq,
            value: serde_json::json!({}).into(),
        });
        render(&ok, &docs_schema()).unwrap();
    }

    #[test]
    fn render_json_path_extract() {
        let op = Operation::Query(vec![RootField {
            table: "docs".into(),
            alias: "docs".into(),
            args: QueryArgs::default(),
            body: RootBody::List {
                selection: vec![Field::JsonPath {
                    column: "data".into(),
                    alias: "abundance".into(),
                    path: vec!["a".into(), "b".into()],
                }],
            },
        }]);
        let (sql, binds) = render(&op, &docs_schema()).unwrap();
        assert!(
            sql.contains(r#""data" #> $1::text[] AS "abundance""#),
            "unexpected SQL: {sql}"
        );
        assert_eq!(binds.len(), 1);
        assert_eq!(
            binds[0],
            Bind::TextArray(vec![Some("a".into()), Some("b".into())])
        );
    }

    #[test]
    fn render_json_path_on_plain_json_column() {
        let op = Operation::Query(vec![RootField {
            table: "docs".into(),
            alias: "docs".into(),
            args: QueryArgs::default(),
            body: RootBody::List {
                selection: vec![Field::JsonPath {
                    column: "meta".into(),
                    alias: "tags".into(),
                    path: vec!["tags".into()],
                }],
            },
        }]);
        let (sql, _) = render(&op, &docs_schema()).unwrap();
        assert!(sql.contains(r#""meta" #> $1::text[] AS "tags""#));
    }

    #[test]
    fn render_json_path_rejects_non_json_column() {
        // The builder API bypasses the parser, so the renderer must reject a
        // path read on a non-json/jsonb column itself.
        let op = Operation::Query(vec![RootField {
            table: "docs".into(),
            alias: "docs".into(),
            args: QueryArgs::default(),
            body: RootBody::List {
                selection: vec![Field::JsonPath {
                    column: "name".into(),
                    alias: "oops".into(),
                    path: vec!["x".into()],
                }],
            },
        }]);
        let err = render(&op, &docs_schema()).unwrap_err();
        assert!(
            matches!(&err, Error::Validate { message, .. } if message.contains("json/jsonb")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn render_where_eq_int() {
        use crate::ast::{BoolExpr, CmpOp};
        use serde_json::json;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs {
                where_: Some(BoolExpr::Compare {
                    column: "id".into(),
                    op: CmpOp::Eq,
                    value: json!(42).into(),
                }),
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                }],
            },
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
        assert!(matches!(binds[0], crate::types::Bind::Int4(42)));
    }

    fn roles_schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", ColumnType::Int4, false)
                    .column(
                        "role",
                        "role",
                        ColumnType::Enum {
                            schema: "public".into(),
                            name: "role_type".into(),
                        },
                        false,
                    )
                    .column("birthday", "birthday", ColumnType::Date, true),
            )
            .build()
    }

    #[test]
    fn render_where_eq_enum() {
        use crate::ast::{BoolExpr, CmpOp};
        use serde_json::json;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs {
                where_: Some(BoolExpr::Compare {
                    column: "role".into(),
                    op: CmpOp::Eq,
                    value: json!("admin").into(),
                }),
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                }],
            },
        }]);
        let (sql, binds) = render(&op, &roles_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], crate::types::Bind::Text(s) if s == "admin"));
    }

    #[test]
    fn render_where_in_enum_list() {
        use crate::ast::BoolExpr;
        use serde_json::json;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs {
                where_: Some(BoolExpr::InList {
                    column: "role".into(),
                    values: json!(["admin", "staff"]).into(),
                    negated: false,
                }),
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                }],
            },
        }]);
        let (sql, binds) = render(&op, &roles_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], crate::types::Bind::TextArray(v) if v.len() == 2));
    }

    #[test]
    fn render_where_gte_date() {
        use crate::ast::{BoolExpr, CmpOp};
        use serde_json::json;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs {
                where_: Some(BoolExpr::Compare {
                    column: "birthday".into(),
                    op: CmpOp::Gte,
                    value: json!("2000-01-01").into(),
                }),
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                }],
            },
        }]);
        let (sql, binds) = render(&op, &roles_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
        assert!(matches!(&binds[0], crate::types::Bind::Text(s) if s == "2000-01-01"));
    }

    #[test]
    fn render_where_and_of_ops() {
        use crate::ast::{BoolExpr, CmpOp};
        use serde_json::json;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs {
                where_: Some(BoolExpr::And(vec![
                    BoolExpr::Compare {
                        column: "id".into(),
                        op: CmpOp::Gt,
                        value: json!(1).into(),
                    },
                    BoolExpr::Compare {
                        column: "name".into(),
                        op: CmpOp::Neq,
                        value: json!("bob").into(),
                    },
                ])),
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                }],
            },
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 2);
    }

    #[test]
    fn render_order_limit_offset() {
        use crate::ast::{OrderBy, OrderDir};

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs {
                order_by: vec![
                    OrderBy::column("name", OrderDir::Asc),
                    OrderBy::column("id", OrderDir::Desc),
                ],
                limit: Some(Count::Lit(10)),
                offset: Some(Count::Lit(5)),
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                }],
            },
        }]);
        let (sql, _binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }

    fn users_posts_schema() -> Schema {
        use crate::schema::Relation;
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("name", "name", ColumnType::Text, true)
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("title", "title", ColumnType::Text, false)
                    .column("user_id", "user_id", ColumnType::Int4, false)
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .build()
    }

    // ── ORDER BY through object relations ──────────────────────────────────

    /// `posts(order_by: {user: {name: asc}})` — sort posts by their author's name.
    /// Renders as a correlated scalar subquery so the row multiplicity of the
    /// outer query is untouched.
    #[test]
    fn order_by_object_relation_renders_correlated_subquery() {
        let schema = users_posts_schema();
        let op = crate::parser::parse_and_lower(
            "query { posts(order_by: {user: {name: asc}}) { id } }",
            &serde_json::json!({}),
            None,
            &schema,
        )
        .expect("parse");
        let (sql, _binds) = render(&op, &schema).expect("render");

        assert!(
            sql.contains("ORDER BY (SELECT"),
            "order_by through a relation must render a subquery, got: {sql}"
        );
        assert!(
            sql.contains(r#"FROM "public"."users""#),
            "subquery must select from the related table, got: {sql}"
        );
        assert!(
            sql.contains("LIMIT 1)"),
            "object relation is 1:1, subquery must be bounded, got: {sql}"
        );
        assert!(sql.contains(" ASC"), "direction must survive, got: {sql}");
    }

    /// The correlation must tie the subquery to the outer row, not to a constant.
    #[test]
    fn order_by_object_relation_correlates_to_outer_row() {
        let schema = users_posts_schema();
        let op = crate::parser::parse_and_lower(
            "query { posts(order_by: {user: {name: desc}}) { id } }",
            &serde_json::json!({}),
            None,
            &schema,
        )
        .expect("parse");
        let (sql, _) = render(&op, &schema).expect("render");

        // posts.user maps ("user_id" -> "id"): subquery.users.id = outer.posts.user_id
        let has_correlation = sql.contains(r#"."id" = "#) && sql.contains(r#"."user_id""#);
        assert!(
            has_correlation,
            "subquery must correlate users.id to the outer posts.user_id, got: {sql}"
        );
        assert!(sql.contains(" DESC"), "direction must survive, got: {sql}");
    }

    /// Own columns must keep rendering as a plain column reference — no subquery.
    #[test]
    fn order_by_own_column_stays_plain() {
        let schema = users_posts_schema();
        let op = crate::parser::parse_and_lower(
            "query { posts(order_by: {title: asc}) { id } }",
            &serde_json::json!({}),
            None,
            &schema,
        )
        .expect("parse");
        let (sql, _) = render(&op, &schema).expect("render");

        assert!(sql.contains(r#""title" ASC"#), "got: {sql}");
        assert!(
            !sql.contains("ORDER BY (SELECT"),
            "a plain column must not become a subquery, got: {sql}"
        );
    }

    /// Ordering through an array relation needs an aggregate (Hasura's
    /// `posts_aggregate: {count: desc}`), which is not implemented. It must be a
    /// clear error rather than silently sorting by something arbitrary.
    #[test]
    fn order_by_array_relation_is_rejected() {
        let schema = users_posts_schema();
        let err = crate::parser::parse_and_lower(
            "query { users(order_by: {posts: {title: asc}}) { id } }",
            &serde_json::json!({}),
            None,
            &schema,
        )
        .expect_err("ordering by an array relation must not be accepted");
        let msg = err.to_string();
        assert!(
            msg.contains("array relation"),
            "error should name the cause, got: {msg}"
        );
    }

    /// A relation-qualified order_by on a nested relation field, not just the root.
    #[test]
    fn order_by_object_relation_inside_nested_field() {
        let schema = users_posts_schema();
        let op = crate::parser::parse_and_lower(
            "query { users { id posts(order_by: {user: {name: asc}}) { title } } }",
            &serde_json::json!({}),
            None,
            &schema,
        )
        .expect("parse");
        let (sql, _) = render(&op, &schema).expect("render");
        assert!(
            sql.contains("ORDER BY (SELECT"),
            "nested relation field must support relation-qualified order_by, got: {sql}"
        );
    }

    // ── NULL placement in ORDER BY ─────────────────────────────────────────

    /// PostgreSQL's default is asymmetric: ASC sorts NULLs last, DESC sorts them
    /// first. `desc_nulls_last` is therefore a distinct thing you must ask for —
    /// this is the case the old raw SQL (`ORDER BY abundance DESC NULLS LAST`)
    /// needed and plain `desc` silently would not have given.
    #[test]
    fn desc_nulls_last_is_not_the_same_as_desc() {
        let schema = users_schema();
        let plain = crate::parser::parse_and_lower(
            "query { users(order_by: {name: desc}) { id } }",
            &serde_json::json!({}),
            None,
            &schema,
        )
        .unwrap();
        let (plain_sql, _) = render(&plain, &schema).unwrap();

        let pinned = crate::parser::parse_and_lower(
            "query { users(order_by: {name: desc_nulls_last}) { id } }",
            &serde_json::json!({}),
            None,
            &schema,
        )
        .unwrap();
        let (pinned_sql, _) = render(&pinned, &schema).unwrap();

        assert!(plain_sql.contains(" DESC"), "got: {plain_sql}");
        assert!(
            !plain_sql.contains("NULLS"),
            "plain desc must not pin NULLs, got: {plain_sql}"
        );
        assert!(pinned_sql.contains(" DESC NULLS LAST"), "got: {pinned_sql}");
    }

    #[test]
    fn all_four_nulls_variants_render() {
        let schema = users_schema();
        for (token, expect) in [
            ("asc_nulls_first", " ASC NULLS FIRST"),
            ("asc_nulls_last", " ASC NULLS LAST"),
            ("desc_nulls_first", " DESC NULLS FIRST"),
            ("desc_nulls_last", " DESC NULLS LAST"),
        ] {
            let op = crate::parser::parse_and_lower(
                &format!("query {{ users(order_by: {{name: {token}}}) {{ id }} }}"),
                &serde_json::json!({}),
                None,
                &schema,
            )
            .unwrap();
            let (sql, _) = render(&op, &schema).unwrap();
            assert!(
                sql.contains(expect),
                "{token} -> 期望 `{expect}`，得到: {sql}"
            );
        }
    }

    /// NULL placement must survive the relation-qualified (correlated subquery) path.
    #[test]
    fn nulls_order_survives_relation_qualified_order_by() {
        let schema = users_posts_schema();
        let op = crate::parser::parse_and_lower(
            "query { posts(order_by: {user: {name: desc_nulls_last}}) { id } }",
            &serde_json::json!({}),
            None,
            &schema,
        )
        .unwrap();
        let (sql, _) = render(&op, &schema).unwrap();
        assert!(sql.contains("ORDER BY (SELECT"), "got: {sql}");
        assert!(
            sql.contains(") DESC NULLS LAST"),
            "关联排序也必须带上 NULLS LAST，got: {sql}"
        );
    }

    #[test]
    fn unknown_direction_names_the_valid_tokens() {
        let schema = users_schema();
        let err = crate::parser::parse_and_lower(
            "query { users(order_by: {name: sideways}) { id } }",
            &serde_json::json!({}),
            None,
            &schema,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("desc_nulls_last"),
            "报错应列出合法取值，got: {msg}"
        );
    }

    #[test]
    fn render_insert_array_with_returning() {
        use crate::ast::{InsertObject, MutationField};
        use std::collections::BTreeMap;

        let mut columns = BTreeMap::new();
        columns.insert("name".to_string(), serde_json::json!("alice").into());
        let op = Operation::Mutation(vec![MutationField::Insert {
            response_typenames: Vec::new(),
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested_arrays: BTreeMap::new(),
                nested_objects: BTreeMap::new(),
            }],
            on_conflict: None,
            returning: vec![Field::Column {
                column: "id".into(),
                alias: "id".into(),
            }],
            one: false,
            scope_check: None,
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
    }

    #[test]
    fn render_insert_one() {
        use crate::ast::{InsertObject, MutationField};
        use std::collections::BTreeMap;

        let mut columns = BTreeMap::new();
        columns.insert("name".to_string(), serde_json::json!("alice").into());
        let op = Operation::Mutation(vec![MutationField::Insert {
            response_typenames: Vec::new(),
            alias: "insert_users_one".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested_arrays: BTreeMap::new(),
                nested_objects: BTreeMap::new(),
            }],
            on_conflict: None,
            returning: vec![Field::Column {
                column: "id".into(),
                alias: "id".into(),
            }],
            one: true,
            scope_check: None,
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_insert_scope_check_emits_guard_cte() {
        use crate::ast::{BoolExpr, CmpOp, InsertObject, MutationField};
        use std::collections::BTreeMap;

        let mut columns = BTreeMap::new();
        columns.insert("name".to_string(), serde_json::json!("alice").into());
        let op = Operation::Mutation(vec![MutationField::Insert {
            response_typenames: Vec::new(),
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested_arrays: BTreeMap::new(),
                nested_objects: BTreeMap::new(),
            }],
            on_conflict: None,
            returning: vec![Field::Column {
                column: "id".into(),
                alias: "id".into(),
            }],
            one: false,
            scope_check: Some(BoolExpr::Compare {
                column: "name".into(),
                op: CmpOp::Eq,
                value: serde_json::json!("alice").into(),
            }),
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        assert!(
            sql.contains("m0_chk AS (SELECT CASE WHEN coalesce(bool_and("),
            "guard CTE: {sql}"
        );
        assert!(
            sql.contains("count(*)::text"),
            "non-constant cast defers folding: {sql}"
        );
        assert!(
            sql.trim_end().ends_with("FROM m0_chk WHERE m0_chk.ok = 0"),
            "final SELECT must reference the guard's ok in WHERE so PG evaluates it: {sql}"
        );
    }

    #[test]
    fn render_insert_without_scope_check_has_no_guard() {
        use crate::ast::{InsertObject, MutationField};
        use std::collections::BTreeMap;

        let mut columns = BTreeMap::new();
        columns.insert("name".to_string(), serde_json::json!("alice").into());
        let op = Operation::Mutation(vec![MutationField::Insert {
            response_typenames: Vec::new(),
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested_arrays: BTreeMap::new(),
                nested_objects: BTreeMap::new(),
            }],
            on_conflict: None,
            returning: Vec::new(),
            one: false,
            scope_check: None,
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        assert!(!sql.contains("_chk"), "no guard when unscoped: {sql}");
    }

    #[test]
    fn render_update_with_scope_check_emits_guard_cte() {
        use crate::ast::{BoolExpr, CmpOp, MutationField};
        use std::collections::BTreeMap;

        let mut set = BTreeMap::new();
        set.insert("name".to_string(), serde_json::json!("bob").into());
        let scope = BoolExpr::Compare {
            column: "name".into(),
            op: CmpOp::Eq,
            value: serde_json::json!("alice").into(),
        };
        let op = Operation::Mutation(vec![MutationField::Update {
            response_typenames: Vec::new(),
            alias: "update_users".into(),
            table: "users".into(),
            // where already carries the AND-ed scope (as apply_scope leaves it).
            where_: BoolExpr::And(vec![
                BoolExpr::Compare {
                    column: "id".into(),
                    op: CmpOp::Gt,
                    value: serde_json::json!(0).into(),
                },
                scope.clone(),
            ]),
            set,
            returning: Vec::new(),
            scope_check: Some(scope),
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        assert!(
            sql.contains("m0_chk AS (SELECT CASE WHEN coalesce(bool_and("),
            "post-update guard CTE: {sql}"
        );
        assert!(
            sql.contains("modified outside scope"),
            "guard diagnostic names the update: {sql}"
        );
        assert!(
            sql.trim_end().ends_with("FROM m0_chk WHERE m0_chk.ok = 0"),
            "final SELECT references the guard's ok: {sql}"
        );
    }

    #[test]
    fn render_update_without_scope_check_has_no_guard() {
        use crate::ast::{BoolExpr, CmpOp, MutationField};
        use std::collections::BTreeMap;

        let mut set = BTreeMap::new();
        set.insert("name".to_string(), serde_json::json!("bob").into());
        let op = Operation::Mutation(vec![MutationField::Update {
            response_typenames: Vec::new(),
            alias: "update_users".into(),
            table: "users".into(),
            where_: BoolExpr::Compare {
                column: "id".into(),
                op: CmpOp::Gt,
                value: serde_json::json!(0).into(),
            },
            set,
            returning: Vec::new(),
            scope_check: None,
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        assert!(!sql.contains("_chk"), "no guard when unscoped: {sql}");
    }

    #[test]
    fn render_upsert_injects_scope_into_do_update_where() {
        use crate::ast::{BoolExpr, CmpOp, InsertObject, MutationField, OnConflict};
        use std::collections::BTreeMap;

        let mut columns = BTreeMap::new();
        columns.insert("id".to_string(), serde_json::json!(1).into());
        columns.insert("name".to_string(), serde_json::json!("alice").into());
        let op = Operation::Mutation(vec![MutationField::Insert {
            response_typenames: Vec::new(),
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested_arrays: BTreeMap::new(),
                nested_objects: BTreeMap::new(),
            }],
            on_conflict: Some(OnConflict {
                constraint: "users_pkey".into(),
                update_columns: vec!["name".into()],
                where_: None,
            }),
            returning: Vec::new(),
            one: false,
            scope_check: Some(BoolExpr::Compare {
                column: "name".into(),
                op: CmpOp::Eq,
                value: serde_json::json!("alice").into(),
            }),
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        // The DO UPDATE WHERE applies the scope predicate to the EXISTING row,
        // so a conflicting foreign row is skipped, not overwritten.
        assert!(
            sql.contains("DO UPDATE SET") && sql.contains("WHERE \"users\".\"name\" = $"),
            "scope predicate gates DO UPDATE on the qualified pre-image row: {sql}"
        );
        // And the post-insert guard still checks the resulting row.
        assert!(
            sql.contains("m0_chk AS (SELECT CASE WHEN coalesce(bool_and("),
            "post-insert guard still present: {sql}"
        );
    }

    #[test]
    fn render_update_by_pk_appends_scope_predicate() {
        use crate::ast::{BoolExpr, CmpOp, MutationField};
        use std::collections::BTreeMap;

        let mut set = BTreeMap::new();
        set.insert("name".to_string(), serde_json::json!("bob").into());
        let op = Operation::Mutation(vec![MutationField::UpdateByPk {
            alias: "update_users_by_pk".into(),
            table: "users".into(),
            pk: vec![("id".into(), serde_json::json!(1).into())],
            set,
            selection: vec![Field::Column {
                column: "id".into(),
                alias: "id".into(),
            }],
            scope: Some(BoolExpr::Compare {
                column: "name".into(),
                op: CmpOp::Eq,
                value: serde_json::json!("alice").into(),
            }),
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        // PK match AND scope, all before RETURNING.
        let where_pos = sql.find("WHERE").expect("has where");
        let ret_pos = sql.find("RETURNING").expect("has returning");
        let clause = &sql[where_pos..ret_pos];
        assert!(clause.contains("\"id\" = $"), "PK match present: {clause}");
        assert!(
            clause.contains(" AND (\"name\" = $"),
            "scope ANDed onto PK match: {clause}"
        );
        // by_pk also re-checks the scope predicate as a post-update guard so an
        // in-scope row cannot be moved out of scope.
        assert!(
            sql.contains("m0_chk AS (SELECT CASE WHEN coalesce(bool_and("),
            "post-update guard CTE: {sql}"
        );
        assert!(
            sql.trim_end().ends_with("FROM m0_chk WHERE m0_chk.ok = 0"),
            "final SELECT references the guard's ok: {sql}"
        );
        // set value + pk value + scope filter value + scope guard value
        assert_eq!(binds.len(), 4);
    }

    #[test]
    fn render_delete_by_pk_without_scope_has_no_extra_and() {
        use crate::ast::MutationField;

        let op = Operation::Mutation(vec![MutationField::DeleteByPk {
            alias: "delete_users_by_pk".into(),
            table: "users".into(),
            pk: vec![("id".into(), serde_json::json!(1).into())],
            selection: vec![Field::Column {
                column: "id".into(),
                alias: "id".into(),
            }],
            scope: None,
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        let where_pos = sql.find("WHERE").expect("has where");
        let ret_pos = sql.find("RETURNING").expect("has returning");
        assert!(
            !sql[where_pos..ret_pos].contains(" AND "),
            "unscoped: bare PK match"
        );
        assert_eq!(binds.len(), 1);
    }

    #[test]
    fn render_distinct_on_auto_prepends_order_by() {
        use crate::ast::RootBody;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs {
                distinct_on: vec!["name".into()],
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                }],
            },
        }]);
        let (sql, _binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_by_pk_single_col() {
        use crate::ast::RootBody;
        use serde_json::json;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users_by_pk".into(),
            args: QueryArgs::default(),
            body: RootBody::ByPk {
                pk: vec![("id".into(), json!(7).into())],
                selection: vec![Field::Column {
                    column: "name".into(),
                    alias: "name".into(),
                }],
            },
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
    }

    #[test]
    fn render_aggregate_count_and_sum() {
        use crate::ast::{AggOp, AggSelect, RootBody};

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users_aggregate".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                typenames: Vec::new(),
                nodes_limit: None,
                ops: vec![
                    AggSelect {
                        alias: "count".into(),
                        op: AggOp::count(),
                    },
                    AggSelect {
                        alias: "sum".into(),
                        op: AggOp::Func {
                            func: crate::ast::AggFunc::Sum,
                            fields: vec![crate::ast::AggField::column("id")],
                        },
                    },
                ],
                nodes: Some(vec![Field::Column {
                    column: "name".into(),
                    alias: "name".into(),
                }]),
            },
        }]);
        let (sql, _binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_count_distinct_and_aliases() {
        use crate::ast::{AggOp, AggSelect, RootBody};

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users_aggregate".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                typenames: Vec::new(),
                nodes_limit: None,
                ops: vec![
                    AggSelect {
                        alias: "total".into(),
                        op: AggOp::count(),
                    },
                    AggSelect {
                        alias: "names".into(),
                        op: AggOp::Count {
                            columns: vec!["name".into()],
                            distinct: true,
                        },
                    },
                    AggSelect {
                        alias: "pairs".into(),
                        op: AggOp::Count {
                            columns: vec!["id".into(), "name".into()],
                            distinct: true,
                        },
                    },
                    AggSelect {
                        alias: "highest".into(),
                        op: AggOp::Func {
                            func: crate::ast::AggFunc::Max,
                            fields: vec![crate::ast::AggField::Column(crate::ast::AggCol {
                                alias: "newest".into(),
                                column: "id".into(),
                            })],
                        },
                    },
                ],
                nodes: None,
            },
        }]);
        let (sql, _binds) = render(&op, &users_schema()).unwrap();
        assert!(sql.contains("'total', count(*)"), "{sql}");
        assert!(
            sql.contains(r#"'names', count(DISTINCT t0."name")"#),
            "{sql}"
        );
        assert!(
            sql.contains(r#"'pairs', count(DISTINCT (t0."id", t0."name"))"#),
            "{sql}"
        );
        assert!(
            sql.contains(r#"'highest', json_build_object('newest', max(t0."id"))"#),
            "{sql}"
        );
        // The columns count() reads must be projected by the inner select.
        assert!(sql.contains(r#"SELECT "id", "name" FROM"#), "{sql}");
    }

    #[test]
    fn render_typename_is_a_literal_of_the_type_name() {
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs::default(),
            body: crate::ast::RootBody::List {
                selection: vec![
                    Field::Typename {
                        alias: "__typename".into(),
                    },
                    Field::Column {
                        column: "id".into(),
                        alias: "id".into(),
                    },
                ],
            },
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        assert!(sql.contains(r#"'users'::text AS "__typename""#), "{sql}");
        // A literal, not a bind: it comes from the schema, not the request.
        assert!(binds.is_empty(), "{binds:?}");
    }

    #[test]
    fn aggregate_key_is_omitted_when_only_nodes_were_asked_for() {
        use crate::ast::RootBody;
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users_aggregate".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                nodes_limit: None,
                ops: Vec::new(),
                nodes: Some(vec![Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                }]),
                typenames: Vec::new(),
            },
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        assert!(!sql.contains("'aggregate'"), "{sql}");
        assert!(sql.contains("'nodes'"), "{sql}");
    }

    /// Nothing in the projection reads a row, so the statement must not read
    /// any: a scalar subquery over an unaggregated source errors at two rows and
    /// answers null at zero.
    #[test]
    fn a_typename_only_aggregate_reads_no_rows() {
        use crate::ast::{AggOp, AggSelect, RootBody};
        for body in [
            RootBody::Aggregate {
                nodes_limit: None,
                ops: Vec::new(),
                nodes: None,
                typenames: vec!["__typename".into()],
            },
            RootBody::Aggregate {
                nodes_limit: None,
                ops: vec![AggSelect {
                    alias: "__typename".into(),
                    op: AggOp::Typename,
                }],
                nodes: None,
                typenames: Vec::new(),
            },
        ] {
            let op = Operation::Query(vec![RootField {
                table: "users".into(),
                alias: "users_aggregate".into(),
                args: QueryArgs::default(),
                body,
            }]);
            let (sql, _) = render(&op, &users_schema()).unwrap();
            assert!(!sql.contains("FROM"), "{sql}");
        }
    }

    /// …but as soon as something does aggregate, the source comes back.
    #[test]
    fn an_aggregate_with_a_function_still_reads_rows() {
        use crate::ast::{AggOp, AggSelect, RootBody};
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users_aggregate".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                nodes_limit: None,
                ops: vec![
                    AggSelect {
                        alias: "__typename".into(),
                        op: AggOp::Typename,
                    },
                    AggSelect {
                        alias: "count".into(),
                        op: AggOp::count(),
                    },
                ],
                nodes: None,
                typenames: Vec::new(),
            },
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        assert!(sql.contains("count(*)"), "{sql}");
        assert!(sql.contains(r#"FROM "public"."users""#), "{sql}");
    }

    #[test]
    fn render_aggregate_no_nodes() {
        use crate::ast::{AggOp, AggSelect, RootBody};

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users_aggregate".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                typenames: Vec::new(),
                nodes_limit: None,
                ops: vec![AggSelect {
                    alias: "count".into(),
                    op: AggOp::count(),
                }],
                nodes: None,
            },
        }]);
        let (sql, _binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_where_relation_exists() {
        use crate::ast::{BoolExpr, CmpOp};
        use serde_json::json;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs {
                where_: Some(BoolExpr::Relation {
                    name: "posts".into(),
                    inner: Box::new(BoolExpr::Compare {
                        column: "title".into(),
                        op: CmpOp::Eq,
                        value: json!("hello").into(),
                    }),
                }),
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                }],
            },
        }]);
        let (sql, binds) = render(&op, &users_posts_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
    }

    #[test]
    fn render_object_relation() {
        let op = Operation::Query(vec![RootField {
            table: "posts".into(),
            alias: "posts".into(),
            args: QueryArgs::default(),
            body: RootBody::List {
                selection: vec![
                    Field::Column {
                        column: "title".into(),
                        alias: "title".into(),
                    },
                    Field::Relation {
                        name: "user".into(),
                        alias: "user".into(),
                        args: QueryArgs::default(),
                        selection: vec![Field::Column {
                            column: "name".into(),
                            alias: "name".into(),
                        }],
                    },
                ],
            },
        }]);
        let (sql, _binds) = render(&op, &users_posts_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_array_relation() {
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs::default(),
            body: RootBody::List {
                selection: vec![
                    Field::Column {
                        column: "id".into(),
                        alias: "id".into(),
                    },
                    Field::Relation {
                        name: "posts".into(),
                        alias: "posts".into(),
                        args: QueryArgs::default(),
                        selection: vec![Field::Column {
                            column: "title".into(),
                            alias: "title".into(),
                        }],
                    },
                ],
            },
        }]);
        let (sql, binds) = render(&op, &users_posts_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert!(binds.is_empty());
    }

    #[test]
    fn render_insert_array_with_nested_relation_returning() {
        use crate::ast::{InsertObject, MutationField};
        use crate::schema::Relation;
        use std::collections::BTreeMap;

        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("name", "name", ColumnType::Text, true)
                    .primary_key(&["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("title", "title", ColumnType::Text, false)
                    .column("user_id", "user_id", ColumnType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build();

        let mut columns = BTreeMap::new();
        columns.insert("name".to_string(), serde_json::json!("alice").into());
        let op = Operation::Mutation(vec![MutationField::Insert {
            response_typenames: Vec::new(),
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested_arrays: BTreeMap::new(),
                nested_objects: BTreeMap::new(),
            }],
            on_conflict: None,
            returning: vec![
                Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                },
                Field::Relation {
                    name: "posts".into(),
                    alias: "posts".into(),
                    args: QueryArgs::default(),
                    selection: vec![Field::Column {
                        column: "title".into(),
                        alias: "title".into(),
                    }],
                },
            ],
            one: false,
            scope_check: None,
        }]);

        let (sql, _binds) = render(&op, &schema).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_insert_with_nested_children() {
        use crate::ast::{InsertObject, MutationField, NestedArrayInsert};
        use crate::schema::Relation;
        use std::collections::BTreeMap;

        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("name", "name", ColumnType::Text, true)
                    .primary_key(&["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("title", "title", ColumnType::Text, false)
                    .column("user_id", "user_id", ColumnType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build();

        let mut parent_cols = BTreeMap::new();
        parent_cols.insert("name".into(), serde_json::json!("alice").into());

        let mut child_cols = BTreeMap::new();
        child_cols.insert("title".into(), serde_json::json!("p1").into());

        let mut nested_arrays = BTreeMap::new();
        nested_arrays.insert(
            "posts".into(),
            NestedArrayInsert {
                table: "posts".into(),
                rows: vec![InsertObject {
                    columns: child_cols,
                    nested_arrays: BTreeMap::new(),
                    nested_objects: BTreeMap::new(),
                }],
                on_conflict: None,
                scope_check: None,
            },
        );

        let op = Operation::Mutation(vec![MutationField::Insert {
            response_typenames: Vec::new(),
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns: parent_cols,
                nested_arrays,
                nested_objects: BTreeMap::new(),
            }],
            on_conflict: None,
            returning: vec![
                Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                },
                Field::Relation {
                    name: "posts".into(),
                    alias: "posts".into(),
                    args: QueryArgs::default(),
                    selection: vec![Field::Column {
                        column: "title".into(),
                        alias: "title".into(),
                    }],
                },
            ],
            one: false,
            scope_check: None,
        }]);

        let (sql, _binds) = render(&op, &schema).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_insert_with_nested_object() {
        use crate::ast::{InsertObject, MutationField, NestedObjectInsert};
        use crate::schema::Relation;
        use std::collections::BTreeMap;

        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("name", "name", ColumnType::Text, true)
                    .primary_key(&["id"]),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("title", "title", ColumnType::Text, false)
                    .column("user_id", "user_id", ColumnType::Int4, false)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .build();

        let mut parent_cols = BTreeMap::new();
        parent_cols.insert("title".into(), serde_json::json!("p1").into());

        let mut child_cols = BTreeMap::new();
        child_cols.insert("name".into(), serde_json::json!("alice").into());

        let mut nested_objects = BTreeMap::new();
        nested_objects.insert(
            "user".into(),
            NestedObjectInsert {
                table: "users".into(),
                row: InsertObject {
                    columns: child_cols,
                    nested_arrays: BTreeMap::new(),
                    nested_objects: BTreeMap::new(),
                },
                on_conflict: None,
                scope_check: None,
            },
        );

        let op = Operation::Mutation(vec![MutationField::Insert {
            response_typenames: Vec::new(),
            alias: "insert_posts".into(),
            table: "posts".into(),
            objects: vec![InsertObject {
                columns: parent_cols,
                nested_arrays: BTreeMap::new(),
                nested_objects,
            }],
            on_conflict: None,
            returning: vec![
                Field::Column {
                    column: "title".into(),
                    alias: "title".into(),
                },
                Field::Relation {
                    name: "user".into(),
                    alias: "user".into(),
                    args: QueryArgs::default(),
                    selection: vec![Field::Column {
                        column: "name".into(),
                        alias: "name".into(),
                    }],
                },
            ],
            one: false,
            scope_check: None,
        }]);

        let (sql, _binds) = render(&op, &schema).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_nested_on_conflict_do_nothing_rewrite() {
        use crate::ast::{InsertObject, MutationField, NestedObjectInsert, OnConflict};
        use crate::schema::Relation;
        use std::collections::BTreeMap;

        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("name", "name", ColumnType::Text, false)
                    .primary_key(&["id"]),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("title", "title", ColumnType::Text, false)
                    .column("user_id", "user_id", ColumnType::Int4, false)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .build();

        let mut parent_cols = BTreeMap::new();
        parent_cols.insert("title".into(), serde_json::json!("p1").into());

        let mut child_cols = BTreeMap::new();
        child_cols.insert("name".into(), serde_json::json!("alice").into());

        let mut nested_objects = BTreeMap::new();
        nested_objects.insert(
            "user".into(),
            NestedObjectInsert {
                table: "users".into(),
                row: InsertObject {
                    columns: child_cols,
                    nested_arrays: BTreeMap::new(),
                    nested_objects: BTreeMap::new(),
                },
                on_conflict: Some(OnConflict {
                    constraint: "users_name_key".into(),
                    update_columns: vec![],
                    where_: None,
                }),
                scope_check: None,
            },
        );

        let op = Operation::Mutation(vec![MutationField::Insert {
            response_typenames: Vec::new(),
            alias: "insert_posts".into(),
            table: "posts".into(),
            objects: vec![InsertObject {
                columns: parent_cols,
                nested_arrays: BTreeMap::new(),
                nested_objects,
            }],
            on_conflict: None,
            returning: vec![Field::Column {
                column: "title".into(),
                alias: "title".into(),
            }],
            one: false,
            scope_check: None,
        }]);

        let (sql, _binds) = render(&op, &schema).unwrap();
        insta::assert_snapshot!(sql);
    }

    /// Builder aliases are arbitrary strings, unlike parser aliases (GraphQL
    /// Names). Every position one reaches must escape it.
    #[test]
    fn builder_aliases_cannot_break_out_of_sql_strings() {
        use crate::ast::{AggOp, AggSelect, RootBody};
        let schema = users_schema();
        let hostile = "x', (SELECT usename FROM pg_user LIMIT 1)) --";
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "agg".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                ops: vec![AggSelect {
                    alias: "count".into(),
                    op: AggOp::count(),
                }],
                nodes: Some(vec![Field::Column {
                    column: "name".into(),
                    alias: hostile.into(),
                }]),
                typenames: Vec::new(),
                nodes_limit: None,
            },
        }]);
        let (sql, _) = render(&op, &schema).unwrap();
        assert!(
            sql.contains("'x'', (SELECT usename FROM pg_user LIMIT 1)) --'"),
            "the alias must land as one escaped literal, got: {sql}"
        );
    }

    #[test]
    fn builder_alias_cannot_break_out_of_an_identifier() {
        let schema = users_schema();
        let hostile = r#"a" FROM pg_user; --"#;
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs::default(),
            body: RootBody::List {
                selection: vec![Field::Column {
                    column: "name".into(),
                    alias: hostile.into(),
                }],
            },
        }]);
        let (sql, _) = render(&op, &schema).unwrap();
        assert!(
            sql.contains(r#"AS "a"" FROM pg_user; --""#),
            "the alias must land as one quoted identifier, got: {sql}"
        );
    }

    #[test]
    fn two_builder_roots_answering_to_one_key_are_refused() {
        // The parser has always refused this; the builder rendered both and
        // the first result silently vanished behind the second.
        let schema = users_schema();
        let root = || RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs::default(),
            body: RootBody::List {
                selection: vec![Field::Column {
                    column: "id".into(),
                    alias: "id".into(),
                }],
            },
        };
        let err = render(&Operation::Query(vec![root(), root()]), &schema).unwrap_err();
        assert!(format!("{err}").contains("both answer to 'users'"), "{err}");
    }

    #[test]
    fn builder_aggregate_refuses_distinct_on() {
        use crate::ast::{AggOp, AggSelect, RootBody};
        let schema = users_schema();
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "agg".into(),
            args: QueryArgs {
                distinct_on: vec!["name".into()],
                ..QueryArgs::default()
            },
            body: RootBody::Aggregate {
                ops: vec![AggSelect {
                    alias: "count".into(),
                    op: AggOp::count(),
                }],
                nodes: None,
                typenames: Vec::new(),
                nodes_limit: None,
            },
        }]);
        let err = render(&op, &schema).unwrap_err();
        assert!(
            format!("{err}").contains("cannot take 'distinct_on'"),
            "{err}"
        );
    }

    fn aggregate_root(ops: Vec<crate::ast::AggSelect>) -> Operation {
        Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "agg".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                ops,
                nodes: None,
                typenames: Vec::new(),
                nodes_limit: None,
            },
        }])
    }

    #[test]
    fn builder_count_distinct_without_columns_is_refused() {
        use crate::ast::{AggOp, AggSelect};
        // Dropping the DISTINCT answered a different question; the parser
        // refuses the pairing and the renderer must too.
        let op = aggregate_root(vec![AggSelect {
            alias: "count".into(),
            op: AggOp::Count {
                columns: Vec::new(),
                distinct: true,
            },
        }]);
        let err = render(&op, &users_schema()).unwrap_err();
        assert!(
            format!("{err}").contains("'distinct' needs 'columns'"),
            "{err}"
        );
    }

    #[test]
    fn builder_agg_func_without_columns_is_refused() {
        use crate::ast::{AggFunc, AggOp, AggSelect};
        // `{}` with the function never applied looks complete and contains
        // nothing.
        let op = aggregate_root(vec![AggSelect {
            alias: "sum".into(),
            op: AggOp::Func {
                func: AggFunc::Sum,
                fields: Vec::new(),
            },
        }]);
        let err = render(&op, &users_schema()).unwrap_err();
        assert!(
            format!("{err}").contains("needs at least one column"),
            "{err}"
        );
    }

    #[test]
    fn builder_duplicate_aggregate_keys_are_refused() {
        use crate::ast::{AggOp, AggSelect};
        let op = aggregate_root(vec![
            AggSelect {
                alias: "count".into(),
                op: AggOp::count(),
            },
            AggSelect {
                alias: "count".into(),
                op: AggOp::Count {
                    columns: vec!["name".into()],
                    distinct: false,
                },
            },
        ]);
        let err = render(&op, &users_schema()).unwrap_err();
        assert!(format!("{err}").contains("both answer to 'count'"), "{err}");
    }

    /// Publish and implement must agree in both directions: `__schema` never
    /// offered `_gt` on jsonb or `_like` on int, so the renderer must not
    /// quietly accept them from the builder.
    #[test]
    fn unpublished_comparison_operators_are_refused() {
        use crate::ast::{BoolExpr, CmpOp};
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("data", "data", ColumnType::Jsonb, true),
            )
            .build();
        let compare = |column: &str, op| {
            Operation::Query(vec![RootField {
                table: "users".into(),
                alias: "users".into(),
                args: QueryArgs {
                    where_: Some(BoolExpr::Compare {
                        column: column.into(),
                        op,
                        value: crate::ast::Val::Lit(serde_json::json!(1)),
                    }),
                    ..QueryArgs::default()
                },
                body: RootBody::List {
                    selection: vec![Field::Column {
                        column: "id".into(),
                        alias: "id".into(),
                    }],
                },
            }])
        };
        let err = render(&compare("data", CmpOp::Gt), &schema).unwrap_err();
        assert!(format!("{err}").contains("'_gt' does not apply"), "{err}");
        let err = render(&compare("id", CmpOp::Like), &schema).unwrap_err();
        assert!(format!("{err}").contains("'_like' does not apply"), "{err}");
        // The published ones still render.
        render(&compare("id", CmpOp::Gt), &schema).unwrap();
        // The jsonb family is jsonb's alone.
        let err = render(&compare("id", CmpOp::HasKey), &schema).unwrap_err();
        assert!(
            format!("{err}").contains("'_has_key' does not apply"),
            "{err}"
        );
    }

    /// The operators, and the operand each binds: a key is text and a key
    /// list a text array, whatever the column is. Semantics are pinned in
    /// `tests/integration_jsonb_operators.rs`; this catches a swapped symbol.
    #[test]
    fn jsonb_operators_render_with_their_operand_types() {
        use crate::ast::{BoolExpr, CmpOp};
        use serde_json::json;
        let schema = Schema::builder()
            .table(
                Table::new("d", "public", "d")
                    .column("id", "id", ColumnType::Int4, false)
                    .column("extra", "extra", ColumnType::Jsonb, true),
            )
            .build();
        for (op, value, want) in [
            (CmpOp::Contains, json!({"a": 1}), r#""extra" @> $1::jsonb"#),
            (
                CmpOp::ContainedIn,
                json!({"a": 1}),
                r#""extra" <@ $1::jsonb"#,
            ),
            (CmpOp::HasKey, json!("a"), r#""extra" ? $1::text"#),
            (CmpOp::HasKeysAny, json!(["a"]), r#""extra" ?| $1::text[]"#),
            (CmpOp::HasKeysAll, json!(["a"]), r#""extra" ?& $1::text[]"#),
        ] {
            let op_ = Operation::Query(vec![RootField {
                table: "d".into(),
                alias: "d".into(),
                args: QueryArgs {
                    where_: Some(BoolExpr::Compare {
                        column: "extra".into(),
                        op,
                        value: crate::ast::Val::Lit(value),
                    }),
                    ..QueryArgs::default()
                },
                body: RootBody::List {
                    selection: vec![Field::Column {
                        column: "id".into(),
                        alias: "id".into(),
                    }],
                },
            }]);
            let (sql, _) = render(&op_, &schema).unwrap();
            assert!(sql.contains(want), "{op:?}: {sql}");
        }
    }

    #[test]
    fn builder_duplicate_selection_keys_are_refused() {
        use crate::ast::{AggOp, AggSelect, RootBody};
        // The parser merges duplicates before rendering; the builder has no
        // such pass, and json_build_object / AS-column duplicates silently
        // last-win on decode.
        let schema = users_schema();
        let dup = || {
            vec![
                Field::Column {
                    column: "id".into(),
                    alias: "x".into(),
                },
                Field::Column {
                    column: "name".into(),
                    alias: "x".into(),
                },
            ]
        };
        let list = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs::default(),
            body: RootBody::List { selection: dup() },
        }]);
        let err = render(&list, &schema).unwrap_err();
        assert!(format!("{err}").contains("both answer to 'x'"), "{err}");

        let nodes = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "agg".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                ops: vec![AggSelect {
                    alias: "count".into(),
                    op: AggOp::count(),
                }],
                nodes: Some(dup()),
                typenames: Vec::new(),
                nodes_limit: None,
            },
        }]);
        let err = render(&nodes, &schema).unwrap_err();
        assert!(format!("{err}").contains("both answer to 'x'"), "{err}");
    }

    #[test]
    fn aggregate_order_by_walks_the_relation_path() {
        // The hand-rolled ORDER BY read only `ob.column`, so ordering
        // `posts_aggregate` by `{user: {name: asc}}` sorted by a column of
        // `posts` — silently, whenever the name existed on both tables.
        let schema = users_posts_schema();
        let op = crate::parser::parse_and_lower(
            "{ posts_aggregate(order_by: {user: {name: desc_nulls_last}}, limit: 2) { nodes { id } } }",
            &serde_json::json!({}),
            None,
            &schema,
        )
        .unwrap();
        let (sql, _) = render(&op, &schema).unwrap();
        assert!(
            sql.contains("ORDER BY (SELECT"),
            "must correlate through the relation, got: {sql}"
        );
        assert!(sql.contains(") DESC NULLS LAST"), "got: {sql}");
    }

    /// The SQLite dialect, pinned as text: a compiled statement is reused by
    /// its SQL, so the shape must not drift without someone noticing. The
    /// semantics are checked against a real SQLite in `tests/sqlite_read.rs`;
    /// these say what that SQL is.
    mod sqlite {
        use crate::dialect::Dialect;
        use crate::parser::{lower_with, parse_document, Bindings};
        use crate::schema::{ColumnType, Relation, Schema, Table};

        fn schema() -> Schema {
            Schema::builder()
                .dialect(Dialect::Sqlite)
                .table(
                    Table::new("users", "main", "users")
                        .column("id", "id", ColumnType::Int8, false)
                        .column("name", "name", ColumnType::Text, false)
                        .column("active", "active", ColumnType::Bool, false)
                        .column("meta", "meta", ColumnType::Json, true)
                        .column("score", "score", ColumnType::Float8, true)
                        .primary_key(&["id"])
                        .relation("posts", Relation::array("posts").on([("id", "user_id")])),
                )
                .table(
                    Table::new("posts", "main", "posts")
                        .column("id", "id", ColumnType::Int8, false)
                        .column("user_id", "user_id", ColumnType::Int8, false)
                        .column("title", "title", ColumnType::Text, false)
                        .column("views", "views", ColumnType::Int8, false)
                        .column("published", "published", ColumnType::Bool, true)
                        .primary_key(&["id"])
                        .relation("user", Relation::object("users").on([("user_id", "id")])),
                )
                .build()
        }

        fn render(source: &str) -> (String, Vec<crate::types::BindSpec>) {
            let doc = parse_document(source).unwrap();
            let op = lower_with(&doc, Bindings::symbolic(), None, &schema()).unwrap();
            super::super::render(&op, &schema(), Dialect::Sqlite).unwrap()
        }

        #[test]
        fn nested_list_with_the_kinds_wrapped_and_a_bound_list() {
            let (sql, binds) = render(
                r#"query($ids: [bigint!]!, $q: String!) {
                    users(where: {id: {_in: $ids}, name: {_ilike: $q}}, order_by: {score: asc}, offset: 2) {
                        id name active meta first: meta(path: "tags.0")
                        posts(order_by: {title: desc}, limit: 3) { title published user { name } }
                    }
                }"#,
            );
            insta::assert_snapshot!(sql);
            assert_eq!(binds.len(), 3);
        }

        #[test]
        fn distinct_on_is_a_window_over_a_derived_table() {
            let (sql, _) = render(
                "{ posts(distinct_on: [user_id], where: {views: {_gt: 1}}, order_by: [{user_id: asc}, {views: desc}], limit: 5) { user_id title published } }",
            );
            insta::assert_snapshot!(sql);
        }

        #[test]
        fn aggregate_with_nodes_and_an_optional_list() {
            let (sql, _) = render(
                "query($ids: [bigint!] @optional) { posts_aggregate(where: {id: {_in: $ids}}) { aggregate { count sum { views } max { title } } nodes { title published } } }",
            );
            insta::assert_snapshot!(sql);
        }

        #[test]
        fn by_pk_and_typename() {
            let (sql, _) = render("{ users_by_pk(id: 1) { __typename name active } }");
            insta::assert_snapshot!(sql);
        }

        #[test]
        fn distinct_on_inside_a_relation() {
            let source = "{ users { name posts(distinct_on: [user_id], where: {views: {_gt: 1}}, order_by: {views: desc}, limit: 2) { title } } }";
            let (sql, _) = render(source);
            insta::assert_snapshot!("sqlite_relation_distinct_on", sql);
            // And the PostgreSQL form, which lives in the same function.
            let doc = parse_document(source).unwrap();
            let op = lower_with(&doc, Bindings::symbolic(), None, &schema()).unwrap();
            let (sql, _) = super::super::render(&op, &schema(), Dialect::Postgres).unwrap();
            insta::assert_snapshot!("postgres_relation_distinct_on", sql);
        }
    }
}
