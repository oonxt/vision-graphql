//! SQL generation from IR.

use crate::ast::{Field, Operation, QueryArgs, RootField};
use crate::error::{Error, Result};
use crate::schema::{Schema, Table};
use crate::types::Bind;
use std::fmt::Write as _;

/// Render an [`Operation`] into a single SQL statement plus bound parameters.
#[tracing::instrument(level = "trace", skip_all)]
pub fn render(op: &Operation, schema: &Schema) -> Result<(String, Vec<Bind>)> {
    let mut ctx = RenderCtx::default();
    match op {
        Operation::Query(roots) => render_query(roots, schema, &mut ctx),
        Operation::Mutation(fields) => render_mutation(fields, schema, &mut ctx),
    }?;
    Ok((ctx.sql, ctx.binds))
}

#[derive(Default)]
struct RenderCtx {
    sql: String,
    binds: Vec<Bind>,
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
}

impl RenderCtx {
    fn next_alias(&mut self, prefix: &str) -> String {
        let a = format!("{prefix}{}", self.alias_counter);
        self.alias_counter += 1;
        a
    }
}

fn render_query(roots: &[RootField], schema: &Schema, ctx: &mut RenderCtx) -> Result<()> {
    ctx.sql.push_str("SELECT json_build_object(");
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
    let table = schema.table(&root.table).ok_or_else(|| Error::Validate {
        path: root.alias.clone(),
        message: format!("unknown table '{}'", root.table),
    })?;
    match &root.body {
        crate::ast::RootBody::List { selection } => {
            render_list(root, selection, table, schema, ctx)
        }
        crate::ast::RootBody::Aggregate { ops, nodes } => {
            render_aggregate(root, ops, nodes.as_deref(), table, schema, ctx)
        }
        crate::ast::RootBody::ByPk { pk, selection } => {
            render_by_pk(root, pk, selection, table, schema, ctx)
        }
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
    ctx.sql.push_str("(SELECT coalesce(json_agg(row_to_json(");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push_str(")), '[]'::json) FROM (");
    render_inner_select(root, selection, table, &inner_alias, schema, ctx)?;
    ctx.sql.push_str(") ");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push(')');
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
    ctx.sql.push_str("SELECT ");
    if !root.args.distinct_on.is_empty() {
        ctx.sql.push_str("DISTINCT ON (");
        for (i, col_name) in root.args.distinct_on.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(", ");
            }
            let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
                path: format!("{}.distinct_on.{col_name}", root.alias),
                message: format!("unknown column '{col_name}' on '{}'", root.table),
            })?;
            write!(ctx.sql, "{table_alias}.{}", quote_ident(&col.physical_name)).unwrap();
        }
        ctx.sql.push_str(") ");
    }
    for (i, field) in selection.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match field {
            Field::Column { physical, alias } => {
                let col = table.find_column(physical).ok_or_else(|| Error::Validate {
                    path: format!("{}.{}", root.alias, alias),
                    message: format!("unknown column '{physical}' on '{}'", root.table),
                })?;
                write!(
                    ctx.sql,
                    r#"{table_alias}.{} AS "{}""#,
                    quote_ident(&col.physical_name),
                    alias
                )
                .unwrap();
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
        }
    }
    write!(
        ctx.sql,
        " FROM {}.{} {table_alias}",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    render_where(&root.args, table, table_alias, schema, ctx)?;
    render_order_by(&root.args, table, table_alias, ctx)?;
    render_limit_offset(&root.args, ctx);
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

fn render_bool_expr(
    expr: &crate::ast::BoolExpr,
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::{BoolExpr, CmpOp};
    match expr {
        BoolExpr::And(parts) => render_bool_list(parts, "AND", table, table_alias, schema, ctx),
        BoolExpr::Or(parts) => render_bool_list(parts, "OR", table, table_alias, schema, ctx),
        BoolExpr::Not(inner) => {
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
            let bind =
                crate::types::json_to_bind(value, &col.pg_type).map_err(|e| Error::Validate {
                    path: format!("where.{column}"),
                    message: format!("{e}"),
                })?;
            ctx.binds.push(bind);
            let placeholder = format!("${}", ctx.binds.len());
            let op_str = match op {
                CmpOp::Eq => "=",
                CmpOp::Neq => "<>",
                CmpOp::Gt => ">",
                CmpOp::Gte => ">=",
                CmpOp::Lt => "<",
                CmpOp::Lte => "<=",
                CmpOp::Like => "LIKE",
                CmpOp::ILike => "ILIKE",
                CmpOp::NLike => "NOT LIKE",
                CmpOp::NILike => "NOT ILIKE",
            };
            write!(
                ctx.sql,
                "{table_alias}.{} {op_str} {placeholder}",
                quote_ident(&col.physical_name)
            )
            .unwrap();
            Ok(())
        }
        BoolExpr::IsNull { column, negated } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            let pred = if *negated { "IS NOT NULL" } else { "IS NULL" };
            write!(
                ctx.sql,
                "{table_alias}.{} {pred}",
                quote_ident(&col.physical_name)
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

    match rel.kind {
        crate::schema::RelKind::Array => {
            ctx.sql.push_str("(SELECT coalesce(json_agg(row_to_json(");
            ctx.sql.push_str(&row_alias);
            ctx.sql.push_str(")), '[]'::json) FROM (");
        }
        crate::schema::RelKind::Object => {
            ctx.sql.push_str("(SELECT row_to_json(");
            ctx.sql.push_str(&row_alias);
            ctx.sql.push_str(") FROM (");
        }
    }

    ctx.sql.push_str("SELECT ");
    for (i, field) in selection.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match field {
            Field::Column {
                physical,
                alias: fa,
            } => {
                let col = target
                    .find_column(physical)
                    .ok_or_else(|| Error::Validate {
                        path: format!("{parent_path}.{alias}.{fa}"),
                        message: format!(
                            "unknown column '{physical}' on '{}'",
                            target.exposed_name
                        ),
                    })?;
                write!(
                    ctx.sql,
                    r#"{remote_alias}.{} AS "{}""#,
                    quote_ident(&col.physical_name),
                    fa
                )
                .unwrap();
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

    if let Some(cte_alias) = visible_cte {
        write!(ctx.sql, " FROM {cte_alias} {remote_alias}").unwrap();
    } else {
        write!(
            ctx.sql,
            " FROM {}.{} {remote_alias}",
            quote_ident(&target.physical_schema),
            quote_ident(&target.physical_name),
        )
        .unwrap();
    }

    ctx.sql.push_str(" WHERE ");
    for (i, (local_col, remote_col)) in rel.mapping.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(" AND ");
        }
        let l = parent_table
            .find_column(local_col)
            .ok_or_else(|| Error::Validate {
                path: format!("{parent_path}.{alias}"),
                message: format!(
                    "relation mapping: unknown local column '{local_col}' on '{}'",
                    parent_table.exposed_name
                ),
            })?;
        let r = target
            .find_column(remote_col)
            .ok_or_else(|| Error::Validate {
                path: format!("{parent_path}.{alias}"),
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
        render_bool_expr(expr, target, &remote_alias, schema, ctx)?;
    }

    if !args.order_by.is_empty() {
        ctx.sql.push_str(" ORDER BY ");
        for (i, ob) in args.order_by.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(", ");
            }
            let col = target
                .find_column(&ob.column)
                .ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.{alias}.order_by.{}", ob.column),
                    message: format!(
                        "unknown column '{}' on '{}'",
                        ob.column, target.exposed_name
                    ),
                })?;
            let dir = match ob.direction {
                crate::ast::OrderDir::Asc => "ASC",
                crate::ast::OrderDir::Desc => "DESC",
            };
            write!(
                ctx.sql,
                "{remote_alias}.{} {dir}",
                quote_ident(&col.physical_name),
            )
            .unwrap();
        }
    }

    if let Some(n) = args.limit {
        write!(ctx.sql, " LIMIT {n}").unwrap();
    } else if matches!(rel.kind, crate::schema::RelKind::Object) {
        ctx.sql.push_str(" LIMIT 1");
    }
    if let Some(n) = args.offset {
        write!(ctx.sql, " OFFSET {n}").unwrap();
    }

    ctx.sql.push_str(") ");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push(')');

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
    write!(ctx.sql, r#" AS "{alias}""#).unwrap();
    Ok(())
}

fn render_order_by(
    args: &QueryArgs,
    table: &Table,
    table_alias: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let mut prefix: Vec<(String, crate::ast::OrderDir)> = Vec::new();
    for d in &args.distinct_on {
        let already = args.order_by.iter().any(|ob| ob.column == *d);
        if !already {
            prefix.push((d.clone(), crate::ast::OrderDir::Asc));
        }
    }
    if prefix.is_empty() && args.order_by.is_empty() {
        return Ok(());
    }
    ctx.sql.push_str(" ORDER BY ");
    let mut first = true;
    for (col_name, dir) in prefix.iter().map(|(c, d)| (c.as_str(), *d)).chain(
        args.order_by
            .iter()
            .map(|ob| (ob.column.as_str(), ob.direction)),
    ) {
        if !first {
            ctx.sql.push_str(", ");
        }
        first = false;
        let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
            path: format!("order_by.{col_name}"),
            message: format!("unknown column '{col_name}' on '{}'", table.exposed_name),
        })?;
        let dir_s = match dir {
            crate::ast::OrderDir::Asc => "ASC",
            crate::ast::OrderDir::Desc => "DESC",
        };
        write!(
            ctx.sql,
            "{table_alias}.{} {dir_s}",
            quote_ident(&col.physical_name)
        )
        .unwrap();
    }
    Ok(())
}

fn render_limit_offset(args: &QueryArgs, ctx: &mut RenderCtx) {
    if let Some(n) = args.limit {
        write!(ctx.sql, " LIMIT {n}").unwrap();
    }
    if let Some(n) = args.offset {
        write!(ctx.sql, " OFFSET {n}").unwrap();
    }
}

/// Return the PostgreSQL type keyword used in a cast expression (`$1::type`)
/// for a given schema PgType.
fn pg_type_cast(pg: &crate::schema::PgType) -> &'static str {
    use crate::schema::PgType;
    match pg {
        PgType::Bool => "bool",
        PgType::Int4 => "int4",
        PgType::Int8 => "int8",
        PgType::Float4 => "float4",
        PgType::Float8 => "float8",
        PgType::Text => "text",
        PgType::Varchar => "varchar",
        PgType::Uuid => "uuid",
        PgType::Numeric => "numeric",
        PgType::Timestamp => "timestamp",
        PgType::TimestampTz => "timestamptz",
        PgType::Jsonb => "jsonb",
    }
}

fn quote_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

fn escape_string_literal(s: &str) -> String {
    s.replace('\'', "''")
}

fn render_by_pk(
    root: &RootField,
    pk: &[(String, serde_json::Value)],
    selection: &[Field],
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let inner_alias = ctx.next_alias("t");
    let row_alias = ctx.next_alias("r");
    ctx.sql.push_str("(SELECT row_to_json(");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push_str(") FROM (SELECT ");
    for (i, field) in selection.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match field {
            Field::Column { physical, alias } => {
                let col = table.find_column(physical).ok_or_else(|| Error::Validate {
                    path: format!("{}.{}", root.alias, alias),
                    message: format!("unknown column '{physical}' on '{}'", table.exposed_name),
                })?;
                write!(
                    ctx.sql,
                    r#"{inner_alias}.{} AS "{}""#,
                    quote_ident(&col.physical_name),
                    alias
                )
                .unwrap();
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
        let bind =
            crate::types::json_to_bind(value, &col.pg_type).map_err(|e| Error::Validate {
                path: format!("{}.pk.{col_name}", root.alias),
                message: format!("{e}"),
            })?;
        ctx.binds.push(bind);
        let ph = format!("${}", ctx.binds.len());
        write!(
            ctx.sql,
            "{inner_alias}.{} = {ph}",
            quote_ident(&col.physical_name)
        )
        .unwrap();
    }
    ctx.sql.push_str(" LIMIT 1) ");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push(')');
    Ok(())
}

fn render_mutation(
    fields: &[crate::ast::MutationField],
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::MutationField;
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
                ..
            } => {
                render_insert_cte(&cte, table, objects, on_conflict.as_ref(), schema, ctx)?;
            }
            MutationField::Update {
                table, where_, set, ..
            } => {
                render_update_cte(&cte, table, where_, set, schema, ctx)?;
            }
            MutationField::UpdateByPk { table, pk, set, .. } => {
                render_update_by_pk_cte(&cte, table, pk, set, schema, ctx)?;
            }
            MutationField::Delete { table, where_, .. } => {
                render_delete_cte(&cte, table, where_, schema, ctx)?;
            }
            MutationField::DeleteByPk { table, pk, .. } => {
                render_delete_by_pk_cte(&cte, table, pk, schema, ctx)?;
            }
        }
    }
    ctx.sql.push(' ');
    ctx.sql.push_str("SELECT json_build_object(");
    for (i, mf) in fields.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let cte = format!("m{i}");
        render_mutation_output_for(mf, &cte, schema, ctx)?;
    }
    ctx.sql.push_str(") AS result");
    Ok(())
}

fn render_insert_cte(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    on_conflict: Option<&crate::ast::OnConflict>,
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
        None,
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
    // Some((parent_ord_cte_alias, relation, parent_table))
    // when this call is a child insert.
    parent_link: Option<(&str, &crate::schema::Relation, &crate::schema::Table)>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use std::collections::BTreeSet;

    debug_assert_eq!(objects.len(), parent_ords.len());

    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;

    if objects.is_empty() {
        // Nothing to insert at this level — emit a no-op CTE so later CTEs
        // can still reference {cte} without type errors. Use a SELECT of an
        // empty, correctly-typed row set.
        write!(
            ctx.sql,
            "{cte} AS (SELECT * FROM {}.{} WHERE FALSE)",
            quote_ident(&table.physical_schema),
            quote_ident(&table.physical_name),
        )
        .unwrap();
        ctx.inserted_ctes.insert(table_name.to_string(), cte.to_string());
        return Ok(());
    }

    // 1. Collect all columns appearing in any row.
    let mut col_set: BTreeSet<String> = BTreeSet::new();
    for obj in objects {
        for k in obj.columns.keys() {
            col_set.insert(k.clone());
        }
    }
    let cols: Vec<String> = col_set.into_iter().collect();

    // 2. Emit the `{cte}_input` VALUES CTE with the ord column and each column value.
    let input_cte = format!("{cte}_input");
    let ord_col_name = if parent_link.is_some() { "parent_ord" } else { "ord" };

    write!(ctx.sql, "{input_cte} AS (SELECT * FROM (VALUES ").unwrap();
    for (r, obj) in objects.iter().enumerate() {
        if r > 0 {
            ctx.sql.push_str(", ");
        }
        ctx.sql.push('(');
        // First column: the ordinal.
        write!(ctx.sql, "{}", parent_ords[r]).unwrap();
        // Remaining columns: each value (or NULL cast to the correct type).
        for exposed in &cols {
            ctx.sql.push_str(", ");
            let col = table
                .find_column(exposed)
                .expect("column should exist — validated at parse");
            let cast = pg_type_cast(&col.pg_type);
            match obj.columns.get(exposed) {
                None => write!(ctx.sql, "NULL::{cast}").unwrap(),
                Some(v) => {
                    let bind = crate::types::json_to_bind(v, &col.pg_type).map_err(|e| {
                        Error::Validate {
                            path: format!("{cte}.objects[{r}].{exposed}"),
                            message: format!("{e}"),
                        }
                    })?;
                    ctx.binds.push(bind);
                    write!(ctx.sql, "${}::{cast}", ctx.binds.len()).unwrap();
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

    // 3. Emit the actual INSERT CTE.
    //    If this is a child, we INSERT (<columns>, <fk_cols_from_parent>)
    //    SELECT <cols>, p.<parent_pk_cols> FROM {input_cte} c JOIN {parent_ord_cte} p ON p.ord = c.parent_ord.
    //    If this is top-level, we INSERT (<columns>)
    //    SELECT <cols> FROM {input_cte} ORDER BY ord.
    write!(
        ctx.sql,
        "{cte} AS (INSERT INTO {}.{} (",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();

    // Physical column list for INSERT target.
    let mut first = true;
    for exposed in &cols {
        if !first {
            ctx.sql.push_str(", ");
        }
        first = false;
        let col = table.find_column(exposed).unwrap();
        ctx.sql.push_str(&quote_ident(&col.physical_name));
    }
    // Add FK columns when this is a child insert.
    if let Some((_, rel, _)) = parent_link {
        for (_, child_col) in &rel.mapping {
            if !first {
                ctx.sql.push_str(", ");
            }
            first = false;
            let col = table.find_column(child_col).ok_or_else(|| Error::Validate {
                path: cte.into(),
                message: format!("mapped FK column '{child_col}' missing on '{}'", table.exposed_name),
            })?;
            ctx.sql.push_str(&quote_ident(&col.physical_name));
        }
    }
    ctx.sql.push(')');

    // SELECT source.
    match parent_link {
        None => {
            ctx.sql.push_str(" SELECT ");
            let mut first_sel = true;
            for exposed in &cols {
                if !first_sel {
                    ctx.sql.push_str(", ");
                }
                first_sel = false;
                ctx.sql.push_str(&quote_ident(exposed));
            }
            write!(ctx.sql, " FROM {input_cte} ORDER BY ord").unwrap();
        }
        Some((parent_ord_cte_alias, rel, parent_table)) => {
            ctx.sql.push_str(" SELECT ");
            let mut first_sel = true;
            for exposed in &cols {
                if !first_sel {
                    ctx.sql.push_str(", ");
                }
                first_sel = false;
                write!(ctx.sql, "c.{}", quote_ident(exposed)).unwrap();
            }
            // FK columns come from the parent ord CTE.
            for (parent_col, _) in &rel.mapping {
                if !first_sel {
                    ctx.sql.push_str(", ");
                }
                first_sel = false;
                let pcol = parent_table.find_column(parent_col).ok_or_else(|| Error::Validate {
                    path: cte.into(),
                    message: format!(
                        "mapped parent column '{parent_col}' missing on '{}'",
                        parent_table.exposed_name
                    ),
                })?;
                write!(ctx.sql, "p.{}", quote_ident(&pcol.physical_name)).unwrap();
            }
            write!(
                ctx.sql,
                " FROM {input_cte} c JOIN {parent_ord_cte_alias} p ON p.ord = c.parent_ord"
            )
            .unwrap();
        }
    }

    if let Some(oc) = on_conflict {
        render_on_conflict(oc, table, schema, ctx)?;
    }
    ctx.sql.push_str(" RETURNING *)");

    // Track this CTE for returning-visibility lookup.
    ctx.inserted_ctes.insert(table_name.to_string(), cte.to_string());

    // 4. For each nested array relation, emit the parent-ord CTE first
    //    (because children need to JOIN against it), then the child chain.
    let any_nested_arrays = objects.iter().any(|o| !o.nested_arrays.is_empty());
    if any_nested_arrays {
        write!(
            ctx.sql,
            ", {cte}_ord AS (SELECT *, ROW_NUMBER() OVER () AS ord FROM {cte})"
        )
        .unwrap();

        // Group children by relation name across all parent objects, tracking
        // which parent_ord each child row belongs to.
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
            let rel = table.find_relation(rel_name).ok_or_else(|| Error::Validate {
                path: cte.into(),
                message: format!("unknown relation '{rel_name}' on '{}'", table.exposed_name),
            })?;
            // Child CTE alias: `{cte}_{rel_name}`.
            let child_cte = format!("{cte}_{rel_name}");
            let parent_ord_cte_name = format!("{cte}_ord");
            ctx.sql.push_str(", ");
            render_insert_cte_recursive(
                &child_cte,
                &rel.target_table,
                &child_rows,
                &child_ords,
                None, // nested children don't carry their own on_conflict in Phase 2
                Some((&parent_ord_cte_name, rel, table)),
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
        ctx.sql.push_str("DO NOTHING");
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
        if let Some(expr) = oc.where_.as_ref() {
            ctx.sql.push_str(" WHERE ");
            render_bool_expr_no_alias(expr, table, schema, ctx)?;
        }
    }
    Ok(())
}

fn render_update_cte(
    cte: &str,
    table_name: &str,
    where_: &crate::ast::BoolExpr,
    set: &std::collections::BTreeMap<String, serde_json::Value>,
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
    for (i, (exposed, value)) in set.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
            path: format!("{cte}._set.{exposed}"),
            message: format!("unknown column '{exposed}'"),
        })?;
        let bind =
            crate::types::json_to_bind(value, &col.pg_type).map_err(|e| Error::Validate {
                path: format!("{cte}._set.{exposed}"),
                message: format!("{e}"),
            })?;
        ctx.binds.push(bind);
        write!(
            ctx.sql,
            "{} = ${}",
            quote_ident(&col.physical_name),
            ctx.binds.len()
        )
        .unwrap();
    }
    ctx.sql.push_str(" WHERE ");
    render_bool_expr_no_alias(where_, table, schema, ctx)?;
    ctx.sql.push_str(" RETURNING *)");
    Ok(())
}

fn render_update_by_pk_cte(
    cte: &str,
    table_name: &str,
    pk: &[(String, serde_json::Value)],
    set: &std::collections::BTreeMap<String, serde_json::Value>,
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
    for (i, (exposed, value)) in set.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
            path: format!("{cte}._set.{exposed}"),
            message: format!("unknown column '{exposed}'"),
        })?;
        let bind =
            crate::types::json_to_bind(value, &col.pg_type).map_err(|e| Error::Validate {
                path: format!("{cte}._set.{exposed}"),
                message: format!("{e}"),
            })?;
        ctx.binds.push(bind);
        write!(
            ctx.sql,
            "{} = ${}",
            quote_ident(&col.physical_name),
            ctx.binds.len()
        )
        .unwrap();
    }
    ctx.sql.push_str(" WHERE ");
    for (i, (col_name, value)) in pk.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(" AND ");
        }
        let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
            path: format!("{cte}.pk.{col_name}"),
            message: format!("unknown column '{col_name}'"),
        })?;
        let bind =
            crate::types::json_to_bind(value, &col.pg_type).map_err(|e| Error::Validate {
                path: format!("{cte}.pk.{col_name}"),
                message: format!("{e}"),
            })?;
        ctx.binds.push(bind);
        write!(
            ctx.sql,
            "{} = ${}",
            quote_ident(&col.physical_name),
            ctx.binds.len()
        )
        .unwrap();
    }
    ctx.sql.push_str(" RETURNING *)");
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
    pk: &[(String, serde_json::Value)],
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
    for (i, (col_name, value)) in pk.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(" AND ");
        }
        let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
            path: format!("{cte}.pk.{col_name}"),
            message: format!("unknown column '{col_name}'"),
        })?;
        let bind =
            crate::types::json_to_bind(value, &col.pg_type).map_err(|e| Error::Validate {
                path: format!("{cte}.pk.{col_name}"),
                message: format!("{e}"),
            })?;
        ctx.binds.push(bind);
        write!(
            ctx.sql,
            "{} = ${}",
            quote_ident(&col.physical_name),
            ctx.binds.len()
        )
        .unwrap();
    }
    ctx.sql.push_str(" RETURNING *)");
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
                    ctx.sql.push_str("'{}'::json");
                } else {
                    render_json_build_object_for_nodes(returning, cte, tbl, alias, schema, ctx)?;
                }
                write!(ctx.sql, " FROM {cte} LIMIT 1)").unwrap();
            } else {
                ctx.sql.push_str("json_build_object(");
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
                    .filter(|v| {
                        v.as_str() == cte || v.starts_with(&format!("{cte}_"))
                    })
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
                    ctx.sql
                        .push_str(", 'returning', (SELECT coalesce(json_agg(");
                    render_json_build_object_for_nodes(returning, cte, tbl, alias, schema, ctx)?;
                    write!(ctx.sql, "), '[]'::json) FROM {cte})").unwrap();
                } else {
                    ctx.sql.push_str(", 'returning', '[]'::json");
                }
                ctx.sql.push(')');
            }
        }
        MutationField::Update {
            alias,
            table,
            returning,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(
                ctx.sql,
                "'{}', json_build_object(",
                escape_string_literal(alias)
            )
            .unwrap();
            write!(ctx.sql, "'affected_rows', (SELECT count(*) FROM {cte})").unwrap();
            if !returning.is_empty() {
                ctx.sql
                    .push_str(", 'returning', (SELECT coalesce(json_agg(");
                render_json_build_object_for_nodes(returning, cte, tbl, alias, schema, ctx)?;
                write!(ctx.sql, "), '[]'::json) FROM {cte})").unwrap();
            } else {
                ctx.sql.push_str(", 'returning', '[]'::json");
            }
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
                ctx.sql.push_str("'{}'::json");
            } else {
                render_json_build_object_for_nodes(selection, cte, tbl, alias, schema, ctx)?;
            }
            write!(ctx.sql, " FROM {cte} LIMIT 1)").unwrap();
        }
        MutationField::Delete {
            alias,
            table,
            returning,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(
                ctx.sql,
                "'{}', json_build_object(",
                escape_string_literal(alias)
            )
            .unwrap();
            write!(ctx.sql, "'affected_rows', (SELECT count(*) FROM {cte})").unwrap();
            if !returning.is_empty() {
                ctx.sql
                    .push_str(", 'returning', (SELECT coalesce(json_agg(");
                render_json_build_object_for_nodes(returning, cte, tbl, alias, schema, ctx)?;
                write!(ctx.sql, "), '[]'::json) FROM {cte})").unwrap();
            } else {
                ctx.sql.push_str(", 'returning', '[]'::json");
            }
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
                ctx.sql.push_str("'{}'::json");
            } else {
                render_json_build_object_for_nodes(selection, cte, tbl, alias, schema, ctx)?;
            }
            write!(ctx.sql, " FROM {cte} LIMIT 1)").unwrap();
        }
    }
    Ok(())
}

fn render_aggregate(
    root: &RootField,
    ops: &[crate::ast::AggOp],
    nodes: Option<&[Field]>,
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let inner_alias = ctx.next_alias("t");

    ctx.sql.push_str("(SELECT json_build_object(");
    ctx.sql.push_str("'aggregate', json_build_object(");
    for (i, op) in ops.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        render_agg_op(op, &inner_alias, table, ctx)?;
    }
    ctx.sql.push(')');

    if let Some(node_fields) = nodes {
        ctx.sql.push_str(", 'nodes', coalesce(json_agg(");
        render_json_build_object_for_nodes(node_fields, &inner_alias, table, &root.alias, schema, ctx)?;
        ctx.sql.push_str("), '[]'::json)");
    }

    ctx.sql.push_str(") FROM (");
    render_aggregate_source(root, ops, nodes, table, schema, ctx)?;
    ctx.sql.push_str(") ");
    ctx.sql.push_str(&inner_alias);
    ctx.sql.push(')');
    Ok(())
}

fn render_agg_op(
    op: &crate::ast::AggOp,
    table_alias: &str,
    table: &Table,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::AggOp;
    match op {
        AggOp::Count => {
            ctx.sql.push_str("'count', count(*)");
            Ok(())
        }
        AggOp::Sum { columns } => render_agg_func("sum", "sum", columns, table_alias, table, ctx),
        AggOp::Avg { columns } => render_agg_func("avg", "avg", columns, table_alias, table, ctx),
        AggOp::Max { columns } => render_agg_func("max", "max", columns, table_alias, table, ctx),
        AggOp::Min { columns } => render_agg_func("min", "min", columns, table_alias, table, ctx),
    }
}

fn render_agg_func(
    key: &str,
    pg_func: &str,
    columns: &[String],
    table_alias: &str,
    table: &Table,
    ctx: &mut RenderCtx,
) -> Result<()> {
    write!(ctx.sql, "'{key}', json_build_object(").unwrap();
    for (i, col_exposed) in columns.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let col = table
            .find_column(col_exposed)
            .ok_or_else(|| Error::Validate {
                path: format!("aggregate.{key}.{col_exposed}"),
                message: format!("unknown column '{col_exposed}' on '{}'", table.exposed_name),
            })?;
        write!(
            ctx.sql,
            "'{col_exposed}', {pg_func}({table_alias}.{})",
            quote_ident(&col.physical_name)
        )
        .unwrap();
    }
    ctx.sql.push(')');
    Ok(())
}

fn render_json_build_object_for_nodes(
    fields: &[Field],
    table_alias: &str,
    table: &Table,
    parent_path: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    ctx.sql.push_str("json_build_object(");
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match f {
            Field::Column { physical, alias } => {
                let col = table.find_column(physical).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.nodes.{alias}"),
                    message: format!("unknown column '{physical}' on '{}'", table.exposed_name),
                })?;
                write!(
                    ctx.sql,
                    "'{alias}', {table_alias}.{}",
                    quote_ident(&col.physical_name)
                )
                .unwrap();
            }
            Field::Relation {
                name,
                alias: rel_alias,
                args,
                selection,
            } => {
                write!(ctx.sql, "'{rel_alias}', ").unwrap();
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
        }
    }
    ctx.sql.push(')');
    Ok(())
}

fn render_aggregate_source(
    root: &RootField,
    ops: &[crate::ast::AggOp],
    nodes: Option<&[Field]>,
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::AggOp;
    use std::collections::BTreeSet;

    let mut cols_needed: BTreeSet<String> = BTreeSet::new();
    for op in ops {
        let columns = match op {
            AggOp::Count => continue,
            AggOp::Sum { columns }
            | AggOp::Avg { columns }
            | AggOp::Max { columns }
            | AggOp::Min { columns } => columns,
        };
        for c in columns {
            let col = table.find_column(c).ok_or_else(|| Error::Validate {
                path: format!("{}.aggregate", root.alias),
                message: format!("unknown column '{c}' on '{}'", table.exposed_name),
            })?;
            cols_needed.insert(col.physical_name.clone());
        }
    }
    if let Some(fields) = nodes {
        for f in fields {
            if let Field::Column { physical, .. } = f {
                let col = table.find_column(physical).ok_or_else(|| Error::Validate {
                    path: format!("{}.nodes", root.alias),
                    message: format!("unknown column '{physical}' on '{}'", table.exposed_name),
                })?;
                cols_needed.insert(col.physical_name.clone());
            }
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
    write!(
        ctx.sql,
        " FROM {}.{}",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();

    if let Some(expr) = root.args.where_.as_ref() {
        ctx.sql.push_str(" WHERE ");
        render_bool_expr_no_alias(expr, table, schema, ctx)?;
    }
    if !root.args.order_by.is_empty() {
        ctx.sql.push_str(" ORDER BY ");
        for (i, ob) in root.args.order_by.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(", ");
            }
            let col = table
                .find_column(&ob.column)
                .ok_or_else(|| Error::Validate {
                    path: format!("{}.order_by.{}", root.alias, ob.column),
                    message: format!("unknown column '{}' on '{}'", ob.column, table.exposed_name),
                })?;
            let dir = match ob.direction {
                crate::ast::OrderDir::Asc => "ASC",
                crate::ast::OrderDir::Desc => "DESC",
            };
            write!(ctx.sql, "{} {dir}", quote_ident(&col.physical_name)).unwrap();
        }
    }
    if let Some(n) = root.args.limit {
        write!(ctx.sql, " LIMIT {n}").unwrap();
    }
    if let Some(n) = root.args.offset {
        write!(ctx.sql, " OFFSET {n}").unwrap();
    }
    Ok(())
}

#[allow(clippy::only_used_in_recursion)]
fn render_bool_expr_no_alias(
    expr: &crate::ast::BoolExpr,
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::{BoolExpr, CmpOp};
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
            let bind =
                crate::types::json_to_bind(value, &col.pg_type).map_err(|e| Error::Validate {
                    path: format!("where.{column}"),
                    message: format!("{e}"),
                })?;
            ctx.binds.push(bind);
            let placeholder = format!("${}", ctx.binds.len());
            let op_str = match op {
                CmpOp::Eq => "=",
                CmpOp::Neq => "<>",
                CmpOp::Gt => ">",
                CmpOp::Gte => ">=",
                CmpOp::Lt => "<",
                CmpOp::Lte => "<=",
                CmpOp::Like => "LIKE",
                CmpOp::ILike => "ILIKE",
                CmpOp::NLike => "NOT LIKE",
                CmpOp::NILike => "NOT ILIKE",
            };
            write!(
                ctx.sql,
                "{} {op_str} {placeholder}",
                quote_ident(&col.physical_name)
            )
            .unwrap();
            Ok(())
        }
        BoolExpr::IsNull { column, negated } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            let pred = if *negated { "IS NOT NULL" } else { "IS NULL" };
            write!(ctx.sql, "{} {pred}", quote_ident(&col.physical_name)).unwrap();
            Ok(())
        }
        BoolExpr::Relation { .. } => Err(Error::Validate {
            path: "where".into(),
            message: "relation filters not supported inside aggregate source".into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Field, Operation, QueryArgs, RootBody, RootField};
    use crate::schema::{PgType, Schema, Table};

    fn users_schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true),
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
                        physical: "id".into(),
                        alias: "id".into(),
                    },
                    Field::Column {
                        physical: "name".into(),
                        alias: "name".into(),
                    },
                ],
            },
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert!(binds.is_empty());
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
                    value: json!(42),
                }),
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    physical: "id".into(),
                    alias: "id".into(),
                }],
            },
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
        assert!(matches!(binds[0], crate::types::Bind::Int4(42)));
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
                        value: json!(1),
                    },
                    BoolExpr::Compare {
                        column: "name".into(),
                        op: CmpOp::Neq,
                        value: json!("bob"),
                    },
                ])),
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    physical: "id".into(),
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
                    OrderBy {
                        column: "name".into(),
                        direction: OrderDir::Asc,
                    },
                    OrderBy {
                        column: "id".into(),
                        direction: OrderDir::Desc,
                    },
                ],
                limit: Some(10),
                offset: Some(5),
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    physical: "id".into(),
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
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .build()
    }

    #[test]
    fn render_insert_array_with_returning() {
        use crate::ast::{InsertObject, MutationField};
        use std::collections::BTreeMap;

        let mut columns = BTreeMap::new();
        columns.insert("name".to_string(), serde_json::json!("alice"));
        let op = Operation::Mutation(vec![MutationField::Insert {
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested_arrays: BTreeMap::new(),
                nested_objects: BTreeMap::new(),
            }],
            on_conflict: None,
            returning: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
            one: false,
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
        columns.insert("name".to_string(), serde_json::json!("alice"));
        let op = Operation::Mutation(vec![MutationField::Insert {
            alias: "insert_users_one".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested_arrays: BTreeMap::new(),
                nested_objects: BTreeMap::new(),
            }],
            on_conflict: None,
            returning: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
            one: true,
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
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
                    physical: "id".into(),
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
                pk: vec![("id".into(), json!(7))],
                selection: vec![Field::Column {
                    physical: "name".into(),
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
        use crate::ast::{AggOp, RootBody};

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users_aggregate".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                ops: vec![
                    AggOp::Count,
                    AggOp::Sum {
                        columns: vec!["id".into()],
                    },
                ],
                nodes: Some(vec![Field::Column {
                    physical: "name".into(),
                    alias: "name".into(),
                }]),
            },
        }]);
        let (sql, _binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_aggregate_no_nodes() {
        use crate::ast::{AggOp, RootBody};

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users_aggregate".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                ops: vec![AggOp::Count],
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
                        value: json!("hello"),
                    }),
                }),
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    physical: "id".into(),
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
                        physical: "title".into(),
                        alias: "title".into(),
                    },
                    Field::Relation {
                        name: "user".into(),
                        alias: "user".into(),
                        args: QueryArgs::default(),
                        selection: vec![Field::Column {
                            physical: "name".into(),
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
                        physical: "id".into(),
                        alias: "id".into(),
                    },
                    Field::Relation {
                        name: "posts".into(),
                        alias: "posts".into(),
                        args: QueryArgs::default(),
                        selection: vec![Field::Column {
                            physical: "title".into(),
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
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build();

        let mut columns = BTreeMap::new();
        columns.insert("name".to_string(), serde_json::json!("alice"));
        let op = Operation::Mutation(vec![MutationField::Insert {
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
                    physical: "id".into(),
                    alias: "id".into(),
                },
                Field::Relation {
                    name: "posts".into(),
                    alias: "posts".into(),
                    args: QueryArgs::default(),
                    selection: vec![Field::Column {
                        physical: "title".into(),
                        alias: "title".into(),
                    }],
                },
            ],
            one: false,
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
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build();

        let mut parent_cols = BTreeMap::new();
        parent_cols.insert("name".into(), serde_json::json!("alice"));

        let mut child_cols = BTreeMap::new();
        child_cols.insert("title".into(), serde_json::json!("p1"));

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
            },
        );

        let op = Operation::Mutation(vec![MutationField::Insert {
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
                    physical: "id".into(),
                    alias: "id".into(),
                },
                Field::Relation {
                    name: "posts".into(),
                    alias: "posts".into(),
                    args: QueryArgs::default(),
                    selection: vec![Field::Column {
                        physical: "title".into(),
                        alias: "title".into(),
                    }],
                },
            ],
            one: false,
        }]);

        let (sql, _binds) = render(&op, &schema).unwrap();
        insta::assert_snapshot!(sql);
    }
}
