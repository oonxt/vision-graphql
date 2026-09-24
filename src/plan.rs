//! A mutation as a sequence of statements, for a backend with no
//! data-modifying CTEs.
//!
//! On PostgreSQL a mutation is one statement: every insert, update and delete
//! is a CTE, nested inserts join the CTE of their parent, the scope guards
//! are CTEs that raise, and the final SELECT assembles the response — all in
//! one snapshot, atomic by construction. SQLite allows no DML inside a CTE
//! and no `RETURNING` in a subquery, so the same mutation here is a
//! [`MutationPlan`]: statements run one after another on one connection,
//! inside a transaction the executor opens, each one's `RETURNING` rows
//! captured for the ones after it, and the response assembled from those
//! captures in Rust.
//!
//! # What is the same
//!
//! - Every row an insert writes is checked against the scope predicate
//!   after the write, at every nesting level; a violation fails the whole
//!   mutation and nothing of it is kept. The same for the rows an update
//!   leaves. (The check is a `SELECT count(*)` over the rows just written,
//!   where PostgreSQL's is an aggregate that raises; SQLite has no way to
//!   raise from SQL, so the executor does.)
//! - `affected_rows` counts every row written under the field, nested rows
//!   included; `returning` is the parent rows, in the order they were
//!   given, with the full selection — nested relations included, read after
//!   the writes.
//! - A nested `on_conflict` with no `update_columns` is a no-op update
//!   rather than `DO NOTHING`, so the row is returned and its key can be
//!   the child's foreign key.
//!
//! # What differs
//!
//! - **Statements see each other.** A later field of the same mutation sees
//!   the rows an earlier one wrote, and `returning { relation }` sees every
//!   related row in the table, not only those this mutation inserted; on
//!   PostgreSQL every CTE reads the snapshot the statement began with. The
//!   README's *Backends* table records this.
//! - **Rows are identified by `rowid`.** A `WITHOUT ROWID` table cannot be
//!   written through this engine (SQLite reports `no such column: rowid`).
//! - **A deleted row's `returning`** is built from what `DELETE … RETURNING`
//!   gave back: columns, `__typename` and JSON path reads. A relation of a
//!   row that no longer exists is refused.
//! - **One statement per object.** SQLite does not promise the order of
//!   `RETURNING` rows for a multi-row insert, and a child needs its own
//!   parent's key, so each object is its own `INSERT`. A column an object
//!   leaves out gets the column's default, where the one-statement form
//!   writes an explicit NULL for a column another object in the batch set.

// The plan is rendered whatever the features — a Schema can say Sqlite without
// the driver being compiled in — but only the SQLite executor reads it.
#![cfg_attr(not(feature = "sqlite"), allow(dead_code))]

use crate::ast::{BoolExpr, Field, InsertObject, MutationField, OnConflict, Val};
use crate::dialect::{escape_string_literal, json_kind, quote_ident, Dialect, JsonKind};
use crate::error::{Error, Result};
use crate::schema::{ColumnType, Relation, Schema, Table};
use crate::sql::{
    ensure_unique_selection_keys, render_bool_expr, render_bool_expr_no_alias,
    render_json_build_object_for_nodes, RenderCtx,
};
use crate::types::{Bind, BindSpec, NullOf};
use std::fmt::Write as _;

/// The key the captured row JSON carries its `rowid` under. Two underscores:
/// not a name a column can be exposed as through the GraphQL path, and
/// distinct from every exposed name the builder path could pick short of
/// choosing this one.
pub(crate) const ROWID_KEY: &str = "__rowid";

/// A mutation rendered as statements to run in order. See the module docs.
#[derive(Debug, Clone)]
pub struct MutationPlan {
    pub(crate) steps: Vec<Step>,
    pub(crate) fields: Vec<FieldResponse>,
    /// How many captures the steps fill, so the executor can size its store.
    pub(crate) captures: usize,
    /// Every statement, one per line, for [`CompiledQuery::sql`] and for
    /// tests; not what is executed.
    ///
    /// [`CompiledQuery::sql`]: crate::CompiledQuery::sql
    pub(crate) text: String,
}

impl MutationPlan {
    /// The statements, one per line.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Every bind spec of every statement, for [`CompiledQuery::variables`].
    ///
    /// [`CompiledQuery::variables`]: crate::CompiledQuery::variables
    pub(crate) fn specs(&self) -> impl Iterator<Item = &BindSpec> {
        let in_steps = self.steps.iter().flat_map(|s| match s {
            Step::Write { binds, .. } | Step::Check { binds, .. } => binds.iter(),
        });
        let in_selects = self.fields.iter().flat_map(|f| f.select_binds());
        in_steps.chain(in_selects).filter_map(|b| match b {
            PlanBind::Spec(s) => Some(s),
            _ => None,
        })
    }
}

/// One statement of the plan.
#[derive(Debug, Clone)]
pub(crate) enum Step {
    /// An `INSERT`, `UPDATE` or `DELETE` whose `RETURNING` is one JSON
    /// object per affected row — every column under its exposed name, plus
    /// the rowid — stored under `capture`.
    Write {
        sql: String,
        binds: Vec<PlanBind>,
        capture: usize,
    },
    /// A scope guard: `SELECT count(*)` of the rows a write left outside the
    /// scope. Any count but zero fails the mutation.
    Check {
        sql: String,
        binds: Vec<PlanBind>,
        table: String,
        action: &'static str,
    },
}

/// A parameter of a plan statement.
#[derive(Debug, Clone)]
pub(crate) enum PlanBind {
    /// Resolved from the request, as any statement's parameter is.
    Spec(BindSpec),
    /// A column of the one row captured by `capture`: a parent's key for its
    /// child's foreign key, a nested object's key for the row that points
    /// at it. Bound as the column's type.
    Captured {
        capture: usize,
        column: String,
        ty: ColumnType,
    },
    /// The rowids of every row captured by these steps, in order, as a JSON
    /// list for `json_each`.
    Rowids(Vec<usize>),
}

/// How one mutation field's response is assembled after the steps ran.
#[derive(Debug, Clone)]
pub(crate) struct FieldResponse {
    pub(crate) alias: String,
    pub(crate) shape: ResponseShape,
}

impl FieldResponse {
    fn select_binds(&self) -> impl Iterator<Item = &PlanBind> {
        let select = match &self.shape {
            ResponseShape::Batch { returning, .. } | ResponseShape::One { returning, .. } => {
                returning.as_ref()
            }
            ResponseShape::Deleted { .. } => None,
        };
        select.into_iter().flat_map(|s| s.binds.iter())
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ResponseShape {
    /// `{affected_rows, returning, __typename…}`.
    Batch {
        /// Every write under the field, nested ones included: what
        /// `affected_rows` counts.
        captures: Vec<usize>,
        /// The parent rows' selection, or `None` for `returning: []`.
        returning: Option<Select>,
        typenames: Vec<(String, String)>,
    },
    /// One row's selection, or null when the write touched no row. `None`
    /// selection is `{}`: the row exists, nothing was asked of it.
    One {
        capture: usize,
        returning: Option<Select>,
    },
    /// Rows that no longer exist: the selection is read off the captured
    /// rows.
    Deleted {
        captures: Vec<usize>,
        fields: Vec<CapturedField>,
        typenames: Vec<(String, String)>,
        one: bool,
    },
}

/// A statement that reads the rows a write captured — by rowid, in capture
/// order — and returns their selection as JSON: an array, or one object /
/// no row for `one`.
#[derive(Debug, Clone)]
pub(crate) struct Select {
    pub(crate) sql: String,
    pub(crate) binds: Vec<PlanBind>,
}

/// A field of a deleted row's `returning`, answered from the captured row.
#[derive(Debug, Clone)]
pub(crate) enum CapturedField {
    Column {
        key: String,
        column: String,
    },
    Typename {
        key: String,
        name: String,
    },
    JsonPath {
        key: String,
        column: String,
        path: Vec<String>,
    },
}

/// One statement under construction: a render context plus the binds that
/// are not request parameters.
struct Stmt {
    ctx: RenderCtx,
    overrides: Vec<(usize, PlanBind)>,
}

impl Stmt {
    fn new(dialect: Dialect) -> Self {
        Stmt {
            ctx: RenderCtx::new(dialect),
            overrides: Vec::new(),
        }
    }

    /// A placeholder for a value the executor supplies from a capture.
    fn captured(&mut self, bind: PlanBind, of: NullOf) -> usize {
        let n = self.ctx.push_fixed(Bind::Null(of));
        self.overrides.push((n - 1, bind));
        n
    }

    fn finish(self) -> (String, Vec<PlanBind>) {
        let Stmt { ctx, overrides } = self;
        let mut binds: Vec<PlanBind> = ctx.binds.into_iter().map(PlanBind::Spec).collect();
        for (i, b) in overrides {
            binds[i] = b;
        }
        (ctx.sql, binds)
    }
}

struct Builder<'a> {
    schema: &'a Schema,
    dialect: Dialect,
    steps: Vec<Step>,
    captures: usize,
}

/// Render a mutation as a plan. The IR is already scoped and limited; this
/// reads `scope_check` / `scope` from it as the one-statement renderer does.
pub(crate) fn build(
    fields: &[MutationField],
    schema: &Schema,
    dialect: Dialect,
) -> Result<MutationPlan> {
    let mut b = Builder {
        schema,
        dialect,
        steps: Vec::new(),
        captures: 0,
    };
    let mut responses = Vec::with_capacity(fields.len());
    for mf in fields {
        responses.push(b.field(mf)?);
    }
    let mut text = String::new();
    for step in &b.steps {
        match step {
            Step::Write { sql, .. } | Step::Check { sql, .. } => {
                text.push_str(sql);
                text.push('\n');
            }
        }
    }
    for f in &responses {
        if let ResponseShape::Batch {
            returning: Some(s), ..
        }
        | ResponseShape::One {
            returning: Some(s), ..
        } = &f.shape
        {
            text.push_str(&s.sql);
            text.push('\n');
        }
    }
    Ok(MutationPlan {
        steps: b.steps,
        fields: responses,
        captures: b.captures,
        text,
    })
}

impl<'a> Builder<'a> {
    /// A table of the schema, borrowed from the schema rather than from the
    /// builder, so holding one does not lock the builder.
    fn table(&self, name: &str, path: &str) -> Result<&'a Table> {
        self.schema
            .table(name)
            .map(|t| &**t)
            .ok_or_else(|| Error::Validate {
                path: path.to_string(),
                message: format!("unknown table '{name}'"),
            })
    }

    fn capture(&mut self) -> usize {
        let c = self.captures;
        self.captures += 1;
        c
    }

    fn qualified(table: &Table) -> String {
        format!(
            "{}.{}",
            quote_ident(&table.physical_schema),
            quote_ident(&table.physical_name)
        )
    }

    /// `RETURNING json_object(...)`: the rowid and every column, each as the
    /// JSON it should be.
    fn returning_row(&self, table: &Table, sql: &mut String) {
        write!(sql, " RETURNING json_object('{ROWID_KEY}', rowid").unwrap();
        for col in table.columns() {
            write!(
                sql,
                ", '{}', {}",
                escape_string_literal(&col.exposed_name),
                self.dialect
                    .value_as_json(quote_ident(&col.physical_name), json_kind(&col.ty))
            )
            .unwrap();
        }
        sql.push(')');
    }

    fn field(&mut self, mf: &MutationField) -> Result<FieldResponse> {
        let alias = mf.alias().to_string();
        let shape = match mf {
            MutationField::Insert {
                table,
                objects,
                on_conflict,
                returning,
                response_typenames,
                one,
                scope_check,
                ..
            } => {
                let t = self.table(table, &alias)?;
                let mut all = Vec::new();
                let mut parents = Vec::new();
                for (i, obj) in objects.iter().enumerate() {
                    let cap = self.insert_object(
                        t,
                        obj,
                        on_conflict.as_ref(),
                        scope_check.as_ref(),
                        None,
                        &format!("{alias}.objects[{i}]"),
                        0,
                        &mut all,
                    )?;
                    parents.push(cap);
                }
                let select = if returning.is_empty() {
                    None
                } else {
                    Some(self.select_rows(t, returning, &parents, *one, &alias)?)
                };
                if *one {
                    // The lowering makes `insert_<t>_one` exactly one object.
                    let capture = parents.first().copied().ok_or_else(|| Error::Validate {
                        path: alias.clone(),
                        message: "insert_one takes exactly one object".into(),
                    })?;
                    ResponseShape::One {
                        capture,
                        returning: select,
                    }
                } else {
                    ResponseShape::Batch {
                        captures: all,
                        returning: select,
                        typenames: self.typenames(response_typenames, t),
                    }
                }
            }
            MutationField::Update {
                table,
                where_,
                set,
                returning,
                response_typenames,
                scope_check,
                ..
            } => {
                let t = self.table(table, &alias)?;
                let mut s = Stmt::new(self.dialect);
                write!(s.ctx.sql, "UPDATE {} SET ", Self::qualified(t)).unwrap();
                self.render_set(t, set, &alias, &mut s)?;
                s.ctx.sql.push_str(" WHERE ");
                render_bool_expr_no_alias(where_, t, self.schema, &mut s.ctx)?;
                self.returning_row(t, &mut s.ctx.sql);
                let cap = self.capture();
                let (sql, binds) = s.finish();
                self.steps.push(Step::Write {
                    sql,
                    binds,
                    capture: cap,
                });
                if let Some(check) = scope_check {
                    self.check(t, check, cap, "modified")?;
                }
                let select = if returning.is_empty() {
                    None
                } else {
                    Some(self.select_rows(t, returning, &[cap], false, &alias)?)
                };
                ResponseShape::Batch {
                    captures: vec![cap],
                    returning: select,
                    typenames: self.typenames(response_typenames, t),
                }
            }
            MutationField::UpdateByPk {
                table,
                pk,
                set,
                selection,
                scope,
                ..
            } => {
                let t = self.table(table, &alias)?;
                let mut s = Stmt::new(self.dialect);
                write!(s.ctx.sql, "UPDATE {} SET ", Self::qualified(t)).unwrap();
                self.render_set(t, set, &alias, &mut s)?;
                s.ctx.sql.push_str(" WHERE ");
                self.render_pk_match(t, pk, scope.as_ref(), &alias, &mut s)?;
                self.returning_row(t, &mut s.ctx.sql);
                let cap = self.capture();
                let (sql, binds) = s.finish();
                self.steps.push(Step::Write {
                    sql,
                    binds,
                    capture: cap,
                });
                if let Some(check) = scope {
                    self.check(t, check, cap, "modified")?;
                }
                let select = if selection.is_empty() {
                    None
                } else {
                    Some(self.select_rows(t, selection, &[cap], true, &alias)?)
                };
                ResponseShape::One {
                    capture: cap,
                    returning: select,
                }
            }
            MutationField::Delete {
                table,
                where_,
                returning,
                response_typenames,
                ..
            } => {
                let t = self.table(table, &alias)?;
                let mut s = Stmt::new(self.dialect);
                write!(s.ctx.sql, "DELETE FROM {} WHERE ", Self::qualified(t)).unwrap();
                render_bool_expr_no_alias(where_, t, self.schema, &mut s.ctx)?;
                self.returning_row(t, &mut s.ctx.sql);
                let cap = self.capture();
                let (sql, binds) = s.finish();
                self.steps.push(Step::Write {
                    sql,
                    binds,
                    capture: cap,
                });
                ResponseShape::Deleted {
                    captures: vec![cap],
                    fields: self.captured_fields(t, returning, &alias)?,
                    typenames: self.typenames(response_typenames, t),
                    one: false,
                }
            }
            MutationField::DeleteByPk {
                table,
                pk,
                selection,
                scope,
                ..
            } => {
                let t = self.table(table, &alias)?;
                let mut s = Stmt::new(self.dialect);
                write!(s.ctx.sql, "DELETE FROM {} WHERE ", Self::qualified(t)).unwrap();
                self.render_pk_match(t, pk, scope.as_ref(), &alias, &mut s)?;
                self.returning_row(t, &mut s.ctx.sql);
                let cap = self.capture();
                let (sql, binds) = s.finish();
                self.steps.push(Step::Write {
                    sql,
                    binds,
                    capture: cap,
                });
                ResponseShape::Deleted {
                    captures: vec![cap],
                    fields: self.captured_fields(t, selection, &alias)?,
                    typenames: Vec::new(),
                    one: true,
                }
            }
        };
        Ok(FieldResponse { alias, shape })
    }

    fn typenames(&self, aliases: &[String], table: &Table) -> Vec<(String, String)> {
        aliases
            .iter()
            .map(|a| (a.clone(), crate::type_names::mutation_response(table)))
            .collect()
    }

    /// `col = $n, …` for an update's `_set`.
    fn render_set(
        &self,
        table: &Table,
        set: &std::collections::BTreeMap<String, Val>,
        path: &str,
        s: &mut Stmt,
    ) -> Result<()> {
        for (i, (exposed, value)) in set.iter().enumerate() {
            if i > 0 {
                s.ctx.sql.push_str(", ");
            }
            let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
                path: format!("{path}._set.{exposed}"),
                message: format!("unknown column '{exposed}'"),
            })?;
            let n = s
                .ctx
                .push_scalar(value, &col.ty, || format!("{path}._set.{exposed}"))?;
            write!(
                s.ctx.sql,
                "{} = {}",
                quote_ident(&col.physical_name),
                self.dialect.param(n, &col.ty)
            )
            .unwrap();
        }
        Ok(())
    }

    /// `pk = $n AND … [AND (scope)]` for the `_by_pk` forms.
    fn render_pk_match(
        &self,
        table: &Table,
        pk: &[(String, Val)],
        scope: Option<&BoolExpr>,
        path: &str,
        s: &mut Stmt,
    ) -> Result<()> {
        for (i, (col_name, value)) in pk.iter().enumerate() {
            if i > 0 {
                s.ctx.sql.push_str(" AND ");
            }
            let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
                path: format!("{path}.pk.{col_name}"),
                message: format!("unknown column '{col_name}'"),
            })?;
            // A primary key is never null, so a null here matches nothing.
            let n = s
                .ctx
                .push_comparison(value, &col.ty, || format!("{path}.pk.{col_name}"))?;
            write!(
                s.ctx.sql,
                "{} = {}",
                quote_ident(&col.physical_name),
                self.dialect.param(n, &col.ty)
            )
            .unwrap();
        }
        if let Some(expr) = scope {
            s.ctx.sql.push_str(" AND (");
            render_bool_expr_no_alias(expr, table, self.schema, &mut s.ctx)?;
            s.ctx.sql.push(')');
        }
        Ok(())
    }

    /// The scope guard over the rows `capture` holds.
    fn check(
        &mut self,
        table: &Table,
        check: &BoolExpr,
        capture: usize,
        action: &'static str,
    ) -> Result<()> {
        let mut s = Stmt::new(self.dialect);
        let t = "t";
        let n = s.captured(PlanBind::Rowids(vec![capture]), NullOf::Int8Array);
        write!(
            s.ctx.sql,
            "SELECT count(*) FROM {} {t} WHERE {t}.rowid IN (SELECT value FROM json_each(?{n})) AND NOT (",
            Self::qualified(table)
        )
        .unwrap();
        render_bool_expr(check, table, t, self.schema, &mut s.ctx)?;
        s.ctx.sql.push(')');
        let (sql, binds) = s.finish();
        self.steps.push(Step::Check {
            sql,
            binds,
            table: table.exposed_name.clone(),
            action,
        });
        Ok(())
    }

    /// One `INSERT` for `obj`, after the rows it points at and before the
    /// rows that point at it. Returns the capture holding its row.
    #[allow(clippy::too_many_arguments)]
    fn insert_object(
        &mut self,
        table: &Table,
        obj: &InsertObject,
        on_conflict: Option<&OnConflict>,
        scope_check: Option<&BoolExpr>,
        parent: Option<(usize, &Relation, &Table)>,
        path: &str,
        depth: usize,
        all: &mut Vec<usize>,
    ) -> Result<usize> {
        if depth > crate::limits::DEFAULT_MAX_DEPTH {
            return Err(Error::Validate {
                path: path.to_string(),
                message: format!(
                    "nested inserts nest deeper than the limit of {}",
                    crate::limits::DEFAULT_MAX_DEPTH
                ),
            });
        }

        // Object relations first: this row's foreign keys are their keys.
        let mut from_objects: Vec<(String, PlanBind)> = Vec::new();
        for (rel_name, noi) in &obj.nested_objects {
            let rel = table
                .find_relation(rel_name)
                .ok_or_else(|| Error::Validate {
                    path: path.to_string(),
                    message: format!("unknown relation '{rel_name}' on '{}'", table.exposed_name),
                })?;
            let target = self.table(&rel.target_table, path)?;
            let cap = self.insert_object(
                target,
                &noi.row,
                noi.on_conflict.as_ref(),
                noi.scope_check.as_ref(),
                None,
                &format!("{path}.{rel_name}"),
                depth + 1,
                all,
            )?;
            for (parent_col, target_col) in &rel.mapping {
                let tcol = target
                    .find_column(target_col)
                    .ok_or_else(|| Error::Validate {
                        path: path.to_string(),
                        message: format!(
                            "mapped target column '{target_col}' missing on '{}'",
                            target.exposed_name
                        ),
                    })?;
                from_objects.push((
                    parent_col.clone(),
                    PlanBind::Captured {
                        capture: cap,
                        column: target_col.clone(),
                        ty: tcol.ty.clone(),
                    },
                ));
            }
        }

        let nested = parent.is_some() || !obj.nested_arrays.is_empty() || depth > 0;
        let mut s = Stmt::new(self.dialect);
        write!(s.ctx.sql, "INSERT INTO {} (", Self::qualified(table)).unwrap();
        let mut values = String::new();
        let mut first = true;
        let mut sep = |sql: &mut String, values: &mut String| {
            if !first {
                sql.push_str(", ");
                values.push_str(", ");
            }
            first = false;
        };
        for (exposed, v) in &obj.columns {
            let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
                path: format!("{path}.{exposed}"),
                message: format!("unknown column '{exposed}' on '{}'", table.exposed_name),
            })?;
            sep(&mut s.ctx.sql, &mut values);
            s.ctx.sql.push_str(&quote_ident(&col.physical_name));
            let n = s
                .ctx
                .push_scalar(v, &col.ty, || format!("{path}.{exposed}"))?;
            write!(values, "{}", self.dialect.param(n, &col.ty)).unwrap();
        }
        if let Some((parent_cap, rel, parent_table)) = parent {
            for (parent_col, child_col) in &rel.mapping {
                let ccol = table
                    .find_column(child_col)
                    .ok_or_else(|| Error::Validate {
                        path: path.to_string(),
                        message: format!(
                            "mapped FK column '{child_col}' missing on '{}'",
                            table.exposed_name
                        ),
                    })?;
                let pcol = parent_table
                    .find_column(parent_col)
                    .ok_or_else(|| Error::Validate {
                        path: path.to_string(),
                        message: format!(
                            "mapped parent column '{parent_col}' missing on '{}'",
                            parent_table.exposed_name
                        ),
                    })?;
                sep(&mut s.ctx.sql, &mut values);
                s.ctx.sql.push_str(&quote_ident(&ccol.physical_name));
                let n = s.captured(
                    PlanBind::Captured {
                        capture: parent_cap,
                        column: parent_col.clone(),
                        ty: pcol.ty.clone(),
                    },
                    NullOf::scalar(&pcol.ty),
                );
                write!(values, "{}", self.dialect.param(n, &ccol.ty)).unwrap();
            }
        }
        for (parent_col, bind) in from_objects {
            let col = table
                .find_column(&parent_col)
                .ok_or_else(|| Error::Validate {
                    path: path.to_string(),
                    message: format!(
                        "mapped FK column '{parent_col}' missing on '{}'",
                        table.exposed_name
                    ),
                })?;
            let of = match &bind {
                PlanBind::Captured { ty, .. } => NullOf::scalar(ty),
                _ => unreachable!("object-relation keys are captured"),
            };
            sep(&mut s.ctx.sql, &mut values);
            s.ctx.sql.push_str(&quote_ident(&col.physical_name));
            let n = s.captured(bind, of);
            write!(values, "{}", self.dialect.param(n, &col.ty)).unwrap();
        }
        if first {
            // Nothing set at all: a row of defaults.
            s.ctx.sql.truncate(s.ctx.sql.len() - 2);
            s.ctx.sql.push_str(" DEFAULT VALUES");
        } else {
            write!(s.ctx.sql, ") VALUES ({values})").unwrap();
        }
        if let Some(oc) = on_conflict {
            self.render_on_conflict(oc, table, scope_check, nested, path, &mut s)?;
        }
        self.returning_row(table, &mut s.ctx.sql);
        let cap = self.capture();
        all.push(cap);
        let (sql, binds) = s.finish();
        self.steps.push(Step::Write {
            sql,
            binds,
            capture: cap,
        });
        if let Some(check) = scope_check {
            self.check(table, check, cap, "inserted")?;
        }

        // Then the rows that point at this one.
        for (rel_name, nai) in &obj.nested_arrays {
            let rel = table
                .find_relation(rel_name)
                .ok_or_else(|| Error::Validate {
                    path: path.to_string(),
                    message: format!("unknown relation '{rel_name}' on '{}'", table.exposed_name),
                })?;
            let target = self.table(&rel.target_table, path)?;
            for (i, row) in nai.rows.iter().enumerate() {
                self.insert_object(
                    target,
                    row,
                    nai.on_conflict.as_ref(),
                    nai.scope_check.as_ref(),
                    Some((cap, rel, table)),
                    &format!("{path}.{rel_name}[{i}]"),
                    depth + 1,
                    all,
                )?;
            }
        }
        Ok(cap)
    }

    /// `ON CONFLICT (cols) DO UPDATE … | DO NOTHING`. The constraint is named
    /// as on PostgreSQL and resolved to its columns, which is what SQLite
    /// takes; a nested `DO NOTHING` becomes a no-op update so the row comes
    /// back for its dependants, as the one-statement form does.
    fn render_on_conflict(
        &self,
        oc: &OnConflict,
        table: &Table,
        scope_check: Option<&BoolExpr>,
        nested: bool,
        path: &str,
        s: &mut Stmt,
    ) -> Result<()> {
        let cols = table
            .unique_constraints
            .get(&oc.constraint)
            .or_else(|| table.unique_indexes.get(&oc.constraint))
            .ok_or_else(|| Error::Validate {
                path: format!("{path}.on_conflict.constraint"),
                message: format!(
                    "unknown constraint '{}' on '{}'",
                    oc.constraint, table.exposed_name
                ),
            })?;
        s.ctx.sql.push_str(" ON CONFLICT (");
        for (i, c) in cols.iter().enumerate() {
            if i > 0 {
                s.ctx.sql.push_str(", ");
            }
            s.ctx.sql.push_str(&quote_ident(c));
        }
        s.ctx.sql.push_str(") ");
        let tref = quote_ident(&table.physical_name);
        if oc.update_columns.is_empty() {
            if nested {
                let pk_name = table.primary_key.first().ok_or_else(|| Error::Validate {
                    path: format!("{path}.on_conflict"),
                    message: format!(
                        "nested DO NOTHING on-conflict requires a primary key on table '{}'",
                        table.exposed_name
                    ),
                })?;
                let pk_col = table.find_column(pk_name).ok_or_else(|| Error::Validate {
                    path: format!("{path}.on_conflict"),
                    message: format!(
                        "primary key column '{pk_name}' missing on '{}'",
                        table.exposed_name
                    ),
                })?;
                write!(
                    s.ctx.sql,
                    "DO UPDATE SET {pk} = {tref}.{pk}",
                    pk = quote_ident(&pk_col.physical_name)
                )
                .unwrap();
            } else {
                s.ctx.sql.push_str("DO NOTHING");
            }
            return Ok(());
        }
        s.ctx.sql.push_str("DO UPDATE SET ");
        for (i, exposed) in oc.update_columns.iter().enumerate() {
            if i > 0 {
                s.ctx.sql.push_str(", ");
            }
            let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
                path: format!("{path}.on_conflict.update_columns.{exposed}"),
                message: format!("unknown column '{exposed}' on '{}'", table.exposed_name),
            })?;
            write!(
                s.ctx.sql,
                "{} = excluded.{}",
                quote_ident(&col.physical_name),
                quote_ident(&col.physical_name)
            )
            .unwrap();
        }
        match (oc.where_.as_ref(), scope_check) {
            (Some(user), Some(scope)) => {
                s.ctx.sql.push_str(" WHERE (");
                render_bool_expr(user, table, &tref, self.schema, &mut s.ctx)?;
                s.ctx.sql.push_str(") AND (");
                render_bool_expr(scope, table, &tref, self.schema, &mut s.ctx)?;
                s.ctx.sql.push(')');
            }
            (Some(expr), None) | (None, Some(expr)) => {
                s.ctx.sql.push_str(" WHERE ");
                render_bool_expr(expr, table, &tref, self.schema, &mut s.ctx)?;
            }
            (None, None) => {}
        }
        Ok(())
    }

    /// The selection of the rows `captures` hold, read back by rowid in
    /// capture order. An array, or for `one` the first object.
    fn select_rows(
        &self,
        table: &Table,
        fields: &[Field],
        captures: &[usize],
        one: bool,
        path: &str,
    ) -> Result<Select> {
        let mut s = Stmt::new(self.dialect);
        let n = s.captured(PlanBind::Rowids(captures.to_vec()), NullOf::Int8Array);
        // Taken from the context, not spelled: the relation subqueries in the
        // selection take their aliases from the same counter.
        let alias = s.ctx.next_alias("t");
        let alias = alias.as_str();
        s.ctx.sql.push_str("SELECT ");
        if !one {
            s.ctx.sql.push_str(self.dialect.json_agg_open());
        }
        render_json_build_object_for_nodes(fields, alias, table, path, self.schema, &mut s.ctx)?;
        if !one {
            s.ctx.sql.push_str(self.dialect.json_agg_close());
        }
        // The join to json_each orders the rows as they were captured; a
        // derived table keeps that order for the aggregate, as elsewhere.
        write!(
            s.ctx.sql,
            " FROM (SELECT x.* FROM {} x JOIN json_each(?{n}) je ON x.rowid = je.value ORDER BY je.key) {alias}",
            Self::qualified(table)
        )
        .unwrap();
        if one {
            s.ctx.sql.push_str(" LIMIT 1");
        }
        let (sql, binds) = s.finish();
        Ok(Select { sql, binds })
    }

    /// What a deleted row's `returning` can answer from the captured row.
    fn captured_fields(
        &self,
        table: &Table,
        fields: &[Field],
        path: &str,
    ) -> Result<Vec<CapturedField>> {
        ensure_unique_selection_keys(fields, path)?;
        fields
            .iter()
            .map(|f| match f {
                Field::Column { column, alias } => {
                    table.find_column(column).ok_or_else(|| Error::Validate {
                        path: format!("{path}.{alias}"),
                        message: format!("unknown column '{column}' on '{}'", table.exposed_name),
                    })?;
                    Ok(CapturedField::Column {
                        key: alias.clone(),
                        column: column.clone(),
                    })
                }
                Field::Typename { alias } => Ok(CapturedField::Typename {
                    key: alias.clone(),
                    name: crate::type_names::row(table).to_string(),
                }),
                Field::JsonPath {
                    column,
                    alias,
                    path: jpath,
                } => {
                    let col = table.find_column(column).ok_or_else(|| Error::Validate {
                        path: format!("{path}.{alias}"),
                        message: format!("unknown column '{column}' on '{}'", table.exposed_name),
                    })?;
                    if !matches!(json_kind(&col.ty), JsonKind::Json) {
                        return Err(Error::Validate {
                            path: format!("{path}.{alias}"),
                            message: format!(
                                "path read requires a json/jsonb column, but '{}' is not",
                                col.exposed_name
                            ),
                        });
                    }
                    Ok(CapturedField::JsonPath {
                        key: alias.clone(),
                        column: column.clone(),
                        path: jpath.clone(),
                    })
                }
                Field::Relation { alias, .. } | Field::RelationAggregate { alias, .. } => {
                    Err(Error::Unsupported {
                        message: format!(
                            "{path}.{alias}: a relation in a delete's returning is not available \
                             on {:?}; the rows are gone by the time it would be read",
                            self.dialect
                        ),
                    })
                }
            })
            .collect()
    }
}

/// Walk a JSON value along a path the way PostgreSQL's `#>` does: a
/// component indexes an array when it is a number, and is a key otherwise.
pub(crate) fn json_path_get<'a>(
    value: &'a serde_json::Value,
    path: &[String],
) -> &'a serde_json::Value {
    use serde_json::Value;
    let mut cur = value;
    for comp in path {
        cur = match cur {
            Value::Object(m) => m.get(comp).unwrap_or(&Value::Null),
            Value::Array(a) => comp
                .parse::<usize>()
                .ok()
                .and_then(|i| a.get(i))
                .unwrap_or(&Value::Null),
            _ => &Value::Null,
        };
    }
    cur
}

#[cfg(test)]
mod tests {
    use super::*;
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
                    .primary_key(&["id"])
                    .unique_constraint("users_pkey", &["id"])
                    .unique_constraint("users_name_key", &["name"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "main", "posts")
                    .column("id", "id", ColumnType::Int8, false)
                    .column("user_id", "user_id", ColumnType::Int8, false)
                    .column("title", "title", ColumnType::Text, false)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .build()
    }

    fn plan(source: &str) -> MutationPlan {
        let doc = parse_document(source).unwrap();
        let op = lower_with(&doc, Bindings::symbolic(), None, &schema()).unwrap();
        let crate::ast::Operation::Mutation(fields) = op else {
            panic!("a mutation")
        };
        build(&fields, &schema(), Dialect::Sqlite).unwrap()
    }

    #[test]
    fn nested_insert_is_one_statement_per_object_in_dependency_order() {
        let p = plan(
            r#"mutation($n: String!) {
                insert_users(objects: [
                    {name: "first", posts: {data: [{title: "a"}, {title: "b"}]}},
                    {name: "solo"}
                ]) { affected_rows returning { id posts(where: {title: {_eq: $n}}) { title } } }
            }"#,
        );
        insta::assert_snapshot!(p.text());
        assert_eq!(p.captures, 4);
        let ResponseShape::Batch { captures, .. } = &p.fields[0].shape else {
            panic!("batch")
        };
        assert_eq!(captures, &[0, 1, 2, 3]);
        // The children bind their parent's key from its capture.
        let Step::Write { binds, .. } = &p.steps[1] else {
            panic!("write")
        };
        assert!(binds.iter().any(|b| matches!(
            b,
            PlanBind::Captured { capture: 0, column, .. } if column == "id"
        )));
    }

    #[test]
    fn object_relation_insert_comes_first_and_feeds_the_foreign_key() {
        let p = plan(
            r#"mutation { insert_posts_one(object: {title: "t", user: {data: {name: "u"}, on_conflict: {constraint: users_name_key, update_columns: []}}}) { id } }"#,
        );
        insta::assert_snapshot!(p.text());
        assert!(
            matches!(&p.steps[0], Step::Write { sql, .. } if sql.starts_with("INSERT INTO \"main\".\"users\""))
        );
        assert!(
            matches!(&p.steps[0], Step::Write { sql, .. } if sql.contains("DO UPDATE SET \"id\" = \"users\".\"id\""))
        );
    }

    #[test]
    fn update_and_delete_with_scope() {
        let mut op = {
            let doc = parse_document(
                r#"mutation { update_users(where: {active: {_eq: true}}, _set: {name: "x"}) { affected_rows returning { name } } delete_posts(where: {id: {_eq: 1}}) { affected_rows returning { title } } }"#,
            )
            .unwrap();
            lower_with(&doc, Bindings::symbolic(), None, &schema()).unwrap()
        };
        let scope = crate::scope::ScopeSet::new()
            .allow(
                "users",
                BoolExpr::Compare {
                    column: "id".into(),
                    op: crate::ast::CmpOp::Eq,
                    value: serde_json::json!(1).into(),
                },
            )
            .allow("posts", BoolExpr::Const(true));
        crate::scope::apply_scope(&mut op, &scope, &schema()).unwrap();
        let crate::ast::Operation::Mutation(fields) = op else {
            panic!("a mutation")
        };
        let p = build(&fields, &schema(), Dialect::Sqlite).unwrap();
        insta::assert_snapshot!(p.text());
        assert!(p.steps.iter().any(|s| matches!(
            s,
            Step::Check {
                action: "modified",
                ..
            }
        )));
    }

    #[test]
    fn a_relation_in_a_delete_returning_is_refused() {
        let doc = parse_document(
            "mutation { delete_users(where: {id: {_eq: 1}}) { returning { posts { title } } } }",
        )
        .unwrap();
        let op = lower_with(&doc, Bindings::symbolic(), None, &schema()).unwrap();
        let crate::ast::Operation::Mutation(fields) = op else {
            panic!("a mutation")
        };
        let err = build(&fields, &schema(), Dialect::Sqlite).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    }

    #[test]
    fn json_paths_walk_like_postgres() {
        let v = serde_json::json!({"tags": ["a", "b"], "n": {"k": 2}, "0": "zero"});
        let p = |s: &[&str]| {
            json_path_get(&v, &s.iter().map(|c| c.to_string()).collect::<Vec<_>>()).clone()
        };
        assert_eq!(p(&["tags", "0"]), serde_json::json!("a"));
        assert_eq!(p(&["n", "k"]), serde_json::json!(2));
        assert_eq!(p(&["0"]), serde_json::json!("zero"));
        assert_eq!(p(&["missing"]), serde_json::Value::Null);
    }
}
