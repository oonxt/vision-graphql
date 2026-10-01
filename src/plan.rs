//! A mutation as a sequence of statements, for a backend with no
//! data-modifying CTEs.
//!
//! On PostgreSQL a mutation is one statement: every insert, update and delete
//! is a CTE, nested inserts join the CTE of their parent, the scope guards
//! are CTEs that raise, and the final SELECT assembles the response — all in
//! one snapshot, atomic by construction. SQLite and MySQL allow no DML inside
//! a CTE (and MySQL no `RETURNING` at all), so the same mutation here is a
//! [`MutationPlan`]: statements run one after another on one connection,
//! inside a transaction (or, inside a caller's transaction, a savepoint) the
//! executor opens and closes, each write's rows captured for the statements
//! after it, and the response assembled from those captures in Rust.
//!
//! # What is the same
//!
//! - Every row an insert writes is checked against the scope predicate
//!   after the write, at every nesting level; a violation fails the whole
//!   mutation and nothing of it is kept. The same for the rows an update
//!   leaves. (The check is a `SELECT count(*)` over the rows just written,
//!   where PostgreSQL's is an aggregate that raises; neither database has a
//!   way to raise from SQL, so the executor does.)
//! - `affected_rows` counts every row written under the field, nested rows
//!   included; `returning` is the parent rows, in the order they were
//!   given, with the full selection — nested relations included, read after
//!   the writes. A deleted row's `returning` is read just before the delete,
//!   relations included, as PostgreSQL reads it from the snapshot.
//! - A parent that inserted no row (`DO NOTHING` on a conflict) gets no
//!   children: a statement whose parent capture is empty is skipped, as the
//!   join to an empty parent CTE inserts nothing.
//! - A *nested* `on_conflict` with no `update_columns` is a no-op update
//!   rather than `DO NOTHING`, so the row is returned and its key can be
//!   the child's foreign key. At the top level it is `DO NOTHING`, as on
//!   PostgreSQL: nothing returned, nothing counted, no children.
//!
//! # What differs
//!
//! - **Statements see each other.** A later field of the same mutation sees
//!   the rows an earlier one wrote, and `returning { relation }` sees every
//!   related row in the table, not only those this mutation inserted; on
//!   PostgreSQL every CTE reads the snapshot the statement began with. The
//!   README's *Backends* table records this.
//! - **One statement per object.** SQLite does not promise the order of
//!   `RETURNING` rows for a multi-row insert, MySQL returns none, and a
//!   child needs its own parent's key, so each object is its own `INSERT`.
//!   A column an object leaves out gets the column's default, where the
//!   one-statement form writes an explicit NULL for a column another object
//!   in the batch set.
//!
//! # How a row is identified
//!
//! On SQLite by `rowid`: every write returns it (`RETURNING`), the guard,
//! the read-back and a child's foreign key take it from there. A `WITHOUT
//! ROWID` table cannot be written (SQLite reports `no such column: rowid`),
//! and an object with no values at all cannot take `on_conflict`: SQLite's
//! `DEFAULT VALUES` admits no upsert clause.
//!
//! On MySQL by primary key, which a table written through the engine must
//! therefore have. A write returns nothing, so an insert is followed by a
//! read of the row it wrote — by the key the object supplied, the key its
//! parent lent it, or `LAST_INSERT_ID()` for a single integer key it left
//! out (any other key it leaves out is refused: MySQL cannot report a key
//! it generated otherwise). An update selects the keys of the rows it will
//! touch first (`FOR UPDATE`), then updates those rows by key, then reads
//! them back; a delete counts what it removed. `on_conflict` is `ON
//! DUPLICATE KEY UPDATE`, which MySQL fires on *any* unique key, not only
//! the one named; a top-level `DO NOTHING` is an insert conditional on no
//! row with the named constraint's values existing; and an `on_conflict`
//! `where` is refused, because MySQL has nothing that leaves a conflicting
//! row unreturned when the condition fails.

// The plan is rendered whatever the features — a Schema can say Sqlite or
// MySql without the driver being compiled in — but only those executors
// read it.
#![cfg_attr(not(any(feature = "sqlite", feature = "mysql")), allow(dead_code))]

use crate::ast::{BoolExpr, Field, InsertObject, MutationField, OnConflict, Val};
use crate::dialect::{
    anonymise_placeholders, escape_string_literal, json_kind, quote_ident, Dialect, ORDER_NUMBER,
};
use crate::error::{Error, Result};
use crate::schema::{Column, ColumnType, Relation, Schema, Table};
use crate::sql::{
    mapped_column, relation_named, render_bool_expr, render_bool_expr_no_alias,
    render_conflict_action, render_json_build_object_for_nodes, render_pk_predicate,
    render_set_clause, RenderCtx,
};
use crate::types::{Bind, BindSpec, NullOf};
use std::fmt::Write as _;

/// The key the captured row JSON carries its `rowid` under, on SQLite. Two
/// underscores: not a name a column can be exposed as through the GraphQL
/// path, and distinct from every exposed name the builder path could pick
/// short of choosing this one.
pub(crate) const ROWID_KEY: &str = "__rowid";

/// The key the captured row JSON carries its primary key under, on MySQL:
/// a JSON array of the key's values, one element per key column.
pub(crate) const KEY_KEY: &str = "__key";

/// A mutation rendered as statements to run in order. See the module docs.
#[derive(Debug, Clone)]
pub struct MutationPlan {
    pub(crate) steps: Vec<Step>,
    pub(crate) fields: Vec<FieldResponse>,
    /// How many captures the writes fill, so the executor can size its store.
    pub(crate) captures: usize,
    /// How many values the reads fill.
    pub(crate) reads: usize,
    /// Every statement, one per line, for [`CompiledQuery::sql`] and for
    /// tests; not what is executed.
    ///
    /// [`CompiledQuery::sql`]: crate::CompiledQuery::sql
    pub(crate) text: String,
    /// The response's key order, for a dialect that
    /// [reorders keys](Dialect::reorders_keys); `None` elsewhere. The
    /// executor assembles the response itself, in this order already; what
    /// this puts back is the order inside each `returning` row.
    pub(crate) keys: Option<crate::sql::KeyOrder>,
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
            Step::Write { binds, rows, .. } => {
                let readback = match rows {
                    RowsFrom::Readback { binds, .. } => binds.iter(),
                    RowsFrom::Statement | RowsFrom::Count => [].iter(),
                };
                binds.iter().chain(readback)
            }
            Step::Check { binds, .. } | Step::Read { binds, .. } => binds.iter().chain([].iter()),
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
    /// An `INSERT`, `UPDATE`, `DELETE` — or, on MySQL, the `SELECT … FOR
    /// UPDATE` that picks the rows an update will touch — whose rows land
    /// under `capture` as one JSON object per row: the row's identity
    /// ([`ROWID_KEY`] or [`KEY_KEY`]) and, for a row that stays, every
    /// column under its exposed name. How the rows are obtained is `rows`.
    ///
    /// Skipped, leaving the capture empty, when a [`PlanBind::Captured`] it
    /// binds refers to an empty capture: its parent inserted nothing.
    Write {
        sql: String,
        binds: Vec<PlanBind>,
        capture: usize,
        rows: RowsFrom,
    },
    /// A scope guard: `SELECT count(*)` of the rows a write left outside the
    /// scope. Any count but zero fails the mutation.
    Check {
        sql: String,
        binds: Vec<PlanBind>,
        table: String,
        action: &'static str,
    },
    /// A read the response needs from before a write: a delete's
    /// `returning`, taken while the rows still exist. One JSON value, stored
    /// under `into`.
    Read {
        sql: String,
        binds: Vec<PlanBind>,
        into: usize,
    },
}

/// Where a [`Step::Write`]'s captured rows come from.
#[derive(Debug, Clone)]
pub(crate) enum RowsFrom {
    /// The statement returns them: a `RETURNING`, or a `SELECT`.
    Statement,
    /// The statement returns nothing, and the rows are not needed beyond
    /// their number: the capture holds one placeholder per affected row.
    Count,
    /// The statement returns nothing; when it affected a row, `sql` reads
    /// it back — by the key the write knows, or by
    /// [`PlanBind::LastInsertId`].
    Readback { sql: String, binds: Vec<PlanBind> },
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
    /// The identities of every row captured by these steps, in order, as a
    /// list: rowids on SQLite, key arrays on MySQL.
    Keys(Vec<usize>),
    /// `LAST_INSERT_ID()` of the write this read-back follows (MySQL).
    LastInsertId,
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
    /// Rows that no longer exist: the selection was read before the delete
    /// ([`Step::Read`]), the count comes from the delete.
    Deleted {
        capture: usize,
        read: Option<usize>,
        typenames: Vec<(String, String)>,
        one: bool,
    },
}

/// A statement that reads the rows a write captured — by identity, in
/// capture order — and returns their selection as JSON: an array, or one
/// object / no row for `one`.
#[derive(Debug, Clone)]
pub(crate) struct Select {
    pub(crate) sql: String,
    pub(crate) binds: Vec<PlanBind>,
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

    /// A placeholder for a value that comes from wherever `source` says: the
    /// request, or a capture.
    fn bind_from(&mut self, source: &KeyBind, ty: &ColumnType, path: &str) -> Result<usize> {
        match source {
            KeyBind::Val(v) => self.ctx.push_scalar(v, ty, || path.to_string()),
            KeyBind::Plan(bind) => Ok(self.captured(bind.clone(), NullOf::scalar(ty))),
        }
    }

    fn finish(self) -> (String, Vec<PlanBind>) {
        let Stmt { ctx, overrides } = self;
        let mut binds: Vec<PlanBind> = ctx.binds.into_iter().map(PlanBind::Spec).collect();
        for (i, b) in overrides {
            binds[i] = b;
        }
        if ctx.dialect.anonymous_placeholders() {
            anonymise_placeholders(&ctx.sql, &binds)
        } else {
            (ctx.sql, binds)
        }
    }
}

/// Where an inserted column's value comes from, kept so the read-back and
/// the conditional insert can bind the same value again.
#[derive(Clone)]
enum KeyBind {
    Val(Val),
    Plan(PlanBind),
}

/// What picks the rows a delete removes, for the read that precedes it.
enum Picks<'a> {
    Where(&'a BoolExpr),
    Pk(&'a [(String, Val)], Option<&'a BoolExpr>),
}

struct Builder<'a> {
    schema: &'a Schema,
    dialect: Dialect,
    steps: Vec<Step>,
    captures: usize,
    reads: usize,
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
        reads: 0,
    };
    let mut responses = Vec::with_capacity(fields.len());
    for mf in fields {
        responses.push(b.field(mf)?);
    }
    let mut text = String::new();
    for step in &b.steps {
        match step {
            Step::Write { sql, rows, .. } => {
                text.push_str(sql);
                text.push('\n');
                if let RowsFrom::Readback { sql, .. } = rows {
                    text.push_str(sql);
                    text.push('\n');
                }
            }
            Step::Check { sql, .. } | Step::Read { sql, .. } => {
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
    let keys = dialect
        .reorders_keys()
        .then(|| key_order_of_mutation(fields, schema));
    Ok(MutationPlan {
        steps: b.steps,
        fields: responses,
        captures: b.captures,
        reads: b.reads,
        text,
        keys,
    })
}

/// The [`KeyOrder`](crate::sql::KeyOrder) of a mutation's response: per
/// field, what the executor assembles — `affected_rows`, `returning`, the
/// type names — or one row, with the rows in selection order.
fn key_order_of_mutation(fields: &[MutationField], schema: &Schema) -> crate::sql::KeyOrder {
    use crate::sql::{key_order_of_fields, KeyOrder};
    let rows = |table: &str, selection: &[Field]| match schema.table(table) {
        Some(t) => key_order_of_fields(selection, t, schema),
        None => KeyOrder::Any,
    };
    let batch = |table: &str, returning: &[Field], typenames: &[String]| {
        let mut keys = vec![
            ("affected_rows".to_string(), KeyOrder::Any),
            (
                "returning".to_string(),
                KeyOrder::List(Box::new(rows(table, returning))),
            ),
        ];
        keys.extend(typenames.iter().map(|t| (t.clone(), KeyOrder::Any)));
        KeyOrder::Object(keys)
    };
    KeyOrder::Object(
        fields
            .iter()
            .map(|mf| {
                let order = match mf {
                    MutationField::Insert {
                        table,
                        returning,
                        response_typenames,
                        one: true,
                        ..
                    } => {
                        let _ = response_typenames;
                        rows(table, returning)
                    }
                    MutationField::Insert {
                        table,
                        returning,
                        response_typenames,
                        ..
                    }
                    | MutationField::Update {
                        table,
                        returning,
                        response_typenames,
                        ..
                    }
                    | MutationField::Delete {
                        table,
                        returning,
                        response_typenames,
                        ..
                    } => batch(table, returning, response_typenames),
                    MutationField::UpdateByPk {
                        table, selection, ..
                    }
                    | MutationField::DeleteByPk {
                        table, selection, ..
                    } => rows(table, selection),
                };
                (mf.alias().to_string(), order)
            })
            .collect(),
    )
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

    fn read_slot(&mut self) -> usize {
        let r = self.reads;
        self.reads += 1;
        r
    }

    fn qualified(&self, table: &Table) -> String {
        format!(
            "{}.{}",
            quote_ident(&table.physical_schema, self.dialect),
            quote_ident(&table.physical_name, self.dialect)
        )
    }

    /// Whether rows are identified by primary key rather than by rowid.
    fn by_key(&self) -> bool {
        match self.dialect {
            Dialect::Postgres | Dialect::Sqlite => false,
            Dialect::MySql => true,
        }
    }

    /// The columns a row of `table` is identified by on a dialect that
    /// identifies by key; a table without a primary key cannot be written
    /// there, and says so.
    fn key_columns(&self, table: &'a Table, path: &str) -> Result<Vec<&'a Column>> {
        if table.primary_key.is_empty() {
            return Err(Error::Unsupported {
                message: format!(
                    "{path}: a mutation on {:?} identifies rows by primary key, and '{}' has none",
                    self.dialect, table.exposed_name
                ),
            });
        }
        table
            .primary_key
            .iter()
            .map(|pk| {
                table.find_column(pk).ok_or_else(|| Error::Validate {
                    path: path.to_string(),
                    message: format!(
                        "primary key column '{pk}' missing on '{}'",
                        table.exposed_name
                    ),
                })
            })
            .collect()
    }

    /// `JSON_OBJECT('__key', JSON_ARRAY(k1, k2)` — left open for the columns
    /// that may follow; the caller closes it. How a MySQL statement spells a
    /// row's identity.
    fn key_object(&self, alias: Option<&str>, keys: &[&Column]) -> String {
        let mut s = format!("JSON_OBJECT('{KEY_KEY}', JSON_ARRAY(");
        for (i, k) in keys.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            if let Some(a) = alias {
                s.push_str(a);
                s.push('.');
            }
            s.push_str(&quote_ident(&k.physical_name, self.dialect));
        }
        s.push(')');
        s
    }

    /// `', col', value…` for every column, as the JSON it should be.
    fn columns_as_json(&self, table: &Table, alias: Option<&str>, sql: &mut String) {
        for col in table.columns() {
            let value = match alias {
                Some(a) => format!("{a}.{}", quote_ident(&col.physical_name, self.dialect)),
                None => quote_ident(&col.physical_name, self.dialect),
            };
            write!(
                sql,
                ", '{}', {}",
                escape_string_literal(&col.exposed_name, self.dialect),
                self.dialect.value_as_json(value, json_kind(&col.ty))
            )
            .unwrap();
        }
    }

    /// `RETURNING json_object(...)` (SQLite): the rowid and every column,
    /// each as the JSON it should be.
    fn returning_row(&self, table: &Table, sql: &mut String) {
        write!(sql, " RETURNING json_object('{ROWID_KEY}', rowid").unwrap();
        self.columns_as_json(table, None, sql);
        sql.push(')');
    }

    /// `RETURNING json_object('__rowid', rowid)`: all a delete has to say.
    fn returning_rowid(sql: &mut String) {
        write!(sql, " RETURNING json_object('{ROWID_KEY}', rowid)").unwrap();
    }

    /// The predicate picking the rows whose identities are bound as
    /// parameter `n`: `rowid IN (json_each)` or `(k1, k2) IN (JSON_TABLE)`.
    fn keys_predicate(&self, alias: Option<&str>, n: usize, keys: &[&Column]) -> String {
        let col = |name: &str| match alias {
            Some(a) => format!("{a}.{}", quote_ident(name, self.dialect)),
            None => quote_ident(name, self.dialect),
        };
        match self.dialect {
            Dialect::Postgres | Dialect::Sqlite => {
                let rowid = match alias {
                    Some(a) => format!("{a}.rowid"),
                    None => "rowid".to_string(),
                };
                format!("{rowid} IN (SELECT value FROM json_each(?{n}))")
            }
            Dialect::MySql => {
                let lhs: Vec<String> = keys.iter().map(|k| col(&k.physical_name)).collect();
                let cols: Vec<String> = (0..keys.len()).map(|i| format!("jt.k{i}")).collect();
                format!(
                    "({}) IN (SELECT {} FROM {})",
                    lhs.join(", "),
                    cols.join(", "),
                    self.key_table(n, keys)
                )
            }
        }
    }

    /// `JSON_TABLE(?n, …) AS jt` reading a bound list of key arrays as one
    /// typed column per key column, numbered `ord` in list order.
    fn key_table(&self, n: usize, keys: &[&Column]) -> String {
        let mut s = format!("JSON_TABLE(?{n}, '$[*]' COLUMNS (ord FOR ORDINALITY");
        for (i, k) in keys.iter().enumerate() {
            write!(
                s,
                ", k{i} {} PATH '$[{i}]'",
                crate::dialect::mysql_json_table_type(&k.ty)
            )
            .unwrap();
        }
        s.push_str(")) AS jt");
        s
    }

    /// The order rows of `alias` are read back in when nothing else says:
    /// rowid, or the primary key.
    fn row_order(&self, alias: &str, keys: &[&Column]) -> String {
        match self.dialect {
            Dialect::Postgres | Dialect::Sqlite => format!("{alias}.rowid"),
            Dialect::MySql => keys
                .iter()
                .map(|k| format!("{alias}.{}", quote_ident(&k.physical_name, self.dialect)))
                .collect::<Vec<_>>()
                .join(", "),
        }
    }

    fn push_write(&mut self, s: Stmt, rows: RowsFrom) -> usize {
        let cap = self.capture();
        let (sql, binds) = s.finish();
        self.steps.push(Step::Write {
            sql,
            binds,
            capture: cap,
            rows,
        });
        cap
    }

    /// A write that returns its rows (SQLite `RETURNING`, or a `SELECT`).
    fn write(&mut self, s: Stmt) -> usize {
        self.push_write(s, RowsFrom::Statement)
    }

    /// The rows an update on MySQL will touch, captured by key before the
    /// update and locked until the transaction ends.
    fn pick_for_update(
        &mut self,
        table: &'a Table,
        pred: impl FnOnce(&str, &mut RenderCtx) -> Result<()>,
        path: &str,
    ) -> Result<(usize, Vec<&'a Column>)> {
        let keys = self.key_columns(table, path)?;
        let mut s = Stmt::new(self.dialect);
        let x = s.ctx.next_alias("t");
        write!(
            s.ctx.sql,
            "SELECT {}) FROM {} {x} WHERE ",
            self.key_object(Some(&x), &keys),
            self.qualified(table)
        )
        .unwrap();
        pred(&x, &mut s.ctx)?;
        // `OF`: only this table's rows, not those of a table a relation
        // filter read on the way, as PostgreSQL's UPDATE locks only its
        // target rows.
        write!(s.ctx.sql, " FOR UPDATE OF {x}").unwrap();
        let cap = self.write(s);
        Ok((cap, keys))
    }

    /// Refuse a `_set` on a key column on a dialect that identifies rows by
    /// key: the rows are picked by key before the update, and guarded, read
    /// back and returned by that key after it — a key that changed under
    /// them would make every one of those a miss, silently.
    fn refuse_key_change(
        &self,
        table: &Table,
        set: &std::collections::BTreeMap<String, Val>,
        path: &str,
    ) -> Result<()> {
        if let Some(col) = table.primary_key.iter().find(|pk| set.contains_key(*pk)) {
            return Err(Error::Unsupported {
                message: format!(
                    "{path}._set.{col}: a primary key column cannot be updated on {:?}, which \
                     identifies the rows of an update by their key",
                    self.dialect
                ),
            });
        }
        Ok(())
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
                let cap = if self.by_key() {
                    self.refuse_key_change(t, set, &alias)?;
                    let schema = self.schema;
                    let (cap, keys) = self.pick_for_update(
                        t,
                        |x, ctx| render_bool_expr(where_, t, x, schema, ctx),
                        &alias,
                    )?;
                    self.update_by_keys(t, set, cap, &keys, &alias)?;
                    cap
                } else {
                    let mut s = Stmt::new(self.dialect);
                    write!(s.ctx.sql, "UPDATE {} SET ", self.qualified(t)).unwrap();
                    render_set_clause(t, set, &alias, &mut s.ctx)?;
                    s.ctx.sql.push_str(" WHERE ");
                    render_bool_expr_no_alias(where_, t, self.schema, &mut s.ctx)?;
                    self.returning_row(t, &mut s.ctx.sql);
                    self.write(s)
                };
                if let Some(check) = scope_check {
                    self.check(t, check, cap, "modified", &alias)?;
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
                let cap = if self.by_key() {
                    self.refuse_key_change(t, set, &alias)?;
                    let schema = self.schema;
                    let (cap, keys) = self.pick_for_update(
                        t,
                        |x, ctx| {
                            render_pk_predicate(t, pk, scope.as_ref(), &alias, Some(x), schema, ctx)
                        },
                        &alias,
                    )?;
                    self.update_by_keys(t, set, cap, &keys, &alias)?;
                    cap
                } else {
                    let mut s = Stmt::new(self.dialect);
                    write!(s.ctx.sql, "UPDATE {} SET ", self.qualified(t)).unwrap();
                    render_set_clause(t, set, &alias, &mut s.ctx)?;
                    s.ctx.sql.push_str(" WHERE ");
                    render_pk_predicate(
                        t,
                        pk,
                        scope.as_ref(),
                        &alias,
                        None,
                        self.schema,
                        &mut s.ctx,
                    )?;
                    self.returning_row(t, &mut s.ctx.sql);
                    self.write(s)
                };
                if let Some(check) = scope {
                    self.check(t, check, cap, "modified", &alias)?;
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
                let read = if returning.is_empty() {
                    None
                } else {
                    Some(self.read_before_delete(
                        t,
                        returning,
                        Picks::Where(where_),
                        false,
                        &alias,
                    )?)
                };
                let cap = if self.by_key() {
                    let schema = self.schema;
                    let (cap, keys) = self.pick_for_update(
                        t,
                        |x, ctx| render_bool_expr(where_, t, x, schema, ctx),
                        &alias,
                    )?;
                    self.delete_by_keys(t, cap, &keys);
                    cap
                } else {
                    let mut s = Stmt::new(self.dialect);
                    write!(s.ctx.sql, "DELETE FROM {} WHERE ", self.qualified(t)).unwrap();
                    render_bool_expr_no_alias(where_, t, self.schema, &mut s.ctx)?;
                    self.delete(s)
                };
                ResponseShape::Deleted {
                    capture: cap,
                    read,
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
                let read = if selection.is_empty() {
                    None
                } else {
                    Some(self.read_before_delete(
                        t,
                        selection,
                        Picks::Pk(pk, scope.as_ref()),
                        true,
                        &alias,
                    )?)
                };
                let cap = if self.by_key() {
                    let schema = self.schema;
                    let (cap, keys) = self.pick_for_update(
                        t,
                        |x, ctx| {
                            render_pk_predicate(t, pk, scope.as_ref(), &alias, Some(x), schema, ctx)
                        },
                        &alias,
                    )?;
                    self.delete_by_keys(t, cap, &keys);
                    cap
                } else {
                    let mut s = Stmt::new(self.dialect);
                    write!(s.ctx.sql, "DELETE FROM {} WHERE ", self.qualified(t)).unwrap();
                    render_pk_predicate(
                        t,
                        pk,
                        scope.as_ref(),
                        &alias,
                        None,
                        self.schema,
                        &mut s.ctx,
                    )?;
                    self.delete(s)
                };
                ResponseShape::Deleted {
                    capture: cap,
                    read,
                    typenames: Vec::new(),
                    one: true,
                }
            }
        };
        Ok(FieldResponse { alias, shape })
    }

    /// A `DELETE … RETURNING rowid` (SQLite): the rows, counted.
    fn delete(&mut self, mut s: Stmt) -> usize {
        Self::returning_rowid(&mut s.ctx.sql);
        self.write(s)
    }

    /// `DELETE … WHERE <the keys captured under `cap`>` (MySQL). The rows
    /// were picked and locked already, so the count is theirs; and a
    /// `where` that reaches this table through a relation filter would put
    /// the target in its own FROM, which MySQL refuses (error 1093), where
    /// a list of keys is just a list.
    fn delete_by_keys(&mut self, table: &Table, cap: usize, keys: &[&Column]) {
        let mut s = Stmt::new(self.dialect);
        let n = s.captured(PlanBind::Keys(vec![cap]), NullOf::Text);
        write!(
            s.ctx.sql,
            "DELETE FROM {} WHERE {}",
            self.qualified(table),
            self.keys_predicate(None, n, keys)
        )
        .unwrap();
        self.push_write(s, RowsFrom::Count);
    }

    /// `UPDATE … WHERE <the keys captured under `cap`>` (MySQL): the rows
    /// were picked and locked already, so the count is theirs.
    fn update_by_keys(
        &mut self,
        table: &Table,
        set: &std::collections::BTreeMap<String, Val>,
        cap: usize,
        keys: &[&Column],
        path: &str,
    ) -> Result<()> {
        let mut s = Stmt::new(self.dialect);
        write!(s.ctx.sql, "UPDATE {} SET ", self.qualified(table)).unwrap();
        render_set_clause(table, set, path, &mut s.ctx)?;
        let n = s.captured(PlanBind::Keys(vec![cap]), NullOf::Text);
        write!(s.ctx.sql, " WHERE {}", self.keys_predicate(None, n, keys)).unwrap();
        self.push_write(s, RowsFrom::Count);
        Ok(())
    }

    fn typenames(&self, aliases: &[String], table: &Table) -> Vec<(String, String)> {
        aliases
            .iter()
            .map(|a| (a.clone(), crate::type_names::mutation_response(table)))
            .collect()
    }

    /// The scope guard over the rows `capture` holds.
    fn check(
        &mut self,
        table: &'a Table,
        check: &BoolExpr,
        capture: usize,
        action: &'static str,
        path: &str,
    ) -> Result<()> {
        let keys = if self.by_key() {
            self.key_columns(table, path)?
        } else {
            Vec::new()
        };
        let mut s = Stmt::new(self.dialect);
        let t = s.ctx.next_alias("t");
        let n = s.captured(PlanBind::Keys(vec![capture]), self.keys_null_of());
        write!(
            s.ctx.sql,
            "SELECT count(*) FROM {} {t} WHERE {} AND NOT (",
            self.qualified(table),
            self.keys_predicate(Some(&t), n, &keys)
        )
        .unwrap();
        render_bool_expr(check, table, &t, self.schema, &mut s.ctx)?;
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

    /// What a [`PlanBind::Keys`] placeholder is declared as: a list of
    /// integers, or JSON text.
    fn keys_null_of(&self) -> NullOf {
        match self.dialect {
            Dialect::Postgres | Dialect::Sqlite => NullOf::Int8Array,
            Dialect::MySql => NullOf::Text,
        }
    }

    /// One `INSERT` for `obj`, after the rows it points at and before the
    /// rows that point at it. Returns the capture holding its row.
    #[allow(clippy::too_many_arguments)]
    fn insert_object(
        &mut self,
        table: &'a Table,
        obj: &InsertObject,
        on_conflict: Option<&OnConflict>,
        scope_check: Option<&BoolExpr>,
        parent: Option<(usize, &Relation, &Table)>,
        path: &str,
        depth: usize,
        all: &mut Vec<usize>,
    ) -> Result<usize> {
        crate::limits::check_insert_depth(depth, path)?;

        // Object relations first: this row's foreign keys are their keys.
        let mut from_objects: Vec<(String, PlanBind)> = Vec::new();
        for (rel_name, noi) in &obj.nested_objects {
            let rel = relation_named(table, rel_name, path)?;
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
                let tcol = mapped_column(target, target_col, "target", path)?;
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

        // Nested as the one-statement renderer means it: any row but a
        // top-level object — a child, or a row an object relation points at.
        let nested = parent.is_some() || depth > 0;

        // Every column the row sets, with where its value comes from.
        let mut values: Vec<(&Column, KeyBind)> = Vec::new();
        for (exposed, v) in &obj.columns {
            let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
                path: format!("{path}.{exposed}"),
                message: format!("unknown column '{exposed}' on '{}'", table.exposed_name),
            })?;
            values.push((col, KeyBind::Val(v.clone())));
        }
        if let Some((parent_cap, rel, parent_table)) = parent {
            for (parent_col, child_col) in &rel.mapping {
                let ccol = mapped_column(table, child_col, "FK", path)?;
                let pcol = mapped_column(parent_table, parent_col, "parent", path)?;
                values.push((
                    ccol,
                    KeyBind::Plan(PlanBind::Captured {
                        capture: parent_cap,
                        column: parent_col.clone(),
                        ty: pcol.ty.clone(),
                    }),
                ));
            }
        }
        for (parent_col, bind) in from_objects {
            let col = mapped_column(table, &parent_col, "FK", path)?;
            values.push((col, KeyBind::Plan(bind)));
        }

        // How the row will be found again, on a dialect that has to.
        let key = if self.by_key() {
            Some(self.insert_key(table, &values, path)?)
        } else {
            None
        };

        let mut s = Stmt::new(self.dialect);
        write!(s.ctx.sql, "INSERT INTO {} (", self.qualified(table)).unwrap();
        let mut placeholders = String::new();
        for (i, (col, source)) in values.iter().enumerate() {
            if i > 0 {
                s.ctx.sql.push_str(", ");
                placeholders.push_str(", ");
            }
            s.ctx
                .sql
                .push_str(&quote_ident(&col.physical_name, self.dialect));
            let n = s.bind_from(source, &col.ty, &format!("{path}.{}", col.exposed_name))?;
            write!(placeholders, "{}", self.dialect.param(n, &col.ty)).unwrap();
        }
        // A top-level DO NOTHING on MySQL is an insert conditional on no
        // conflicting row: the one spelling that inserts nothing, counts
        // nothing and returns nothing, as PostgreSQL's does. (INSERT IGNORE
        // would also ignore a foreign key it cannot satisfy.) The probe reads
        // the table whole, scope or no scope — deliberately: the conflict is
        // with any row, and PostgreSQL's DO NOTHING tells the caller the same
        // thing through `affected_rows` (0: a row with these values exists
        // somewhere). A scoped probe would insert into the conflict instead
        // and report a duplicate key, which says it louder.
        let conditional = match on_conflict {
            Some(oc) if self.by_key() && oc.update_columns.is_empty() && !nested => Some(oc),
            _ => None,
        };
        if values.is_empty() {
            // Nothing set at all: a row of defaults. SQLite's grammar admits
            // no upsert clause after DEFAULT VALUES, and there is no other
            // spelling of "a row of defaults" to hang one on; MySQL's
            // `() VALUES ()` takes one, and the conditional form needs a
            // value list.
            if on_conflict.is_some() && (!self.by_key() || conditional.is_some()) {
                return Err(Error::Unsupported {
                    message: format!(
                        "{path}: an object with no values cannot take on_conflict on {:?}",
                        self.dialect
                    ),
                });
            }
            s.ctx.sql.truncate(s.ctx.sql.len() - 2);
            s.ctx.sql.push_str(self.dialect.insert_defaults());
        } else if let Some(oc) = conditional {
            write!(
                s.ctx.sql,
                ") SELECT {placeholders} FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM {} WHERE ",
                self.qualified(table)
            )
            .unwrap();
            let cols = self.conflict_columns(oc, table, path)?;
            for (i, col) in cols.iter().enumerate() {
                if i > 0 {
                    s.ctx.sql.push_str(" AND ");
                }
                // A conflict column the object does not set is its default,
                // which the conditional cannot see; `= NULL` is never true,
                // so the row goes in and a conflicting default is the
                // database's error to raise.
                let source = values
                    .iter()
                    .find(|(c, _)| c.exposed_name == col.exposed_name)
                    .map(|(_, source)| source.clone())
                    .unwrap_or(KeyBind::Val(Val::Lit(serde_json::Value::Null)));
                let n = s.bind_from(&source, &col.ty, &format!("{path}.{}", col.exposed_name))?;
                write!(
                    s.ctx.sql,
                    "{} = {}",
                    quote_ident(&col.physical_name, self.dialect),
                    self.dialect.param(n, &col.ty)
                )
                .unwrap();
            }
            s.ctx.sql.push(')');
        } else {
            write!(s.ctx.sql, ") VALUES ({placeholders})").unwrap();
            if let Some(oc) = on_conflict {
                match self.dialect {
                    Dialect::Postgres | Dialect::Sqlite => {
                        self.render_on_conflict(oc, table, scope_check, nested, path, &mut s)?
                    }
                    Dialect::MySql => self.render_on_duplicate_key(
                        oc,
                        table,
                        key.as_deref().expect("keyed on MySQL"),
                        path,
                        &mut s,
                    )?,
                }
            }
        }

        let cap = match key {
            None => {
                self.returning_row(table, &mut s.ctx.sql);
                self.write(s)
            }
            Some(key) => {
                // An upsert may have touched the row the named constraint
                // finds rather than the row the object's key names: read
                // back by either, and by the constraint's columns only when
                // the object set every one of them (a column it did not set
                // is NULL, which conflicts with nothing).
                let by_constraint = match (on_conflict, conditional) {
                    (Some(oc), None) => {
                        let cols = self.conflict_columns(oc, table, path)?;
                        let sources: Option<Vec<(&Column, KeyBind)>> = cols
                            .iter()
                            .map(|c| {
                                values
                                    .iter()
                                    .find(|(v, _)| v.exposed_name == c.exposed_name)
                                    .map(|(_, src)| (*c, src.clone()))
                            })
                            .collect();
                        sources.filter(|s| {
                            !s.iter().all(|(c, _)| {
                                key.iter().any(|(k, _)| k.exposed_name == c.exposed_name)
                            })
                        })
                    }
                    _ => None,
                };
                let readback = self.readback(table, &key, by_constraint.as_deref(), path)?;
                self.push_write(s, readback)
            }
        };
        all.push(cap);
        if let Some(check) = scope_check {
            self.check(table, check, cap, "inserted", path)?;
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

    /// Where each key column of the row about to be inserted gets its
    /// value: what the object set, what a parent lent, or — for a single
    /// integer key the object left out — `LAST_INSERT_ID()`. Anything else
    /// left out is refused: MySQL cannot report a key it generated
    /// otherwise, and reading the row back by a guess would be a wrong row.
    fn insert_key(
        &self,
        table: &'a Table,
        values: &[(&Column, KeyBind)],
        path: &str,
    ) -> Result<Vec<(&'a Column, KeyBind)>> {
        let keys = self.key_columns(table, path)?;
        keys.iter()
            .map(|k| {
                if let Some((_, source)) = values
                    .iter()
                    .find(|(c, _)| c.exposed_name == k.exposed_name)
                {
                    return Ok((*k, source.clone()));
                }
                let integer =
                    matches!(k.ty, ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8);
                if keys.len() == 1 && integer {
                    return Ok((*k, KeyBind::Plan(PlanBind::LastInsertId)));
                }
                Err(Error::Unsupported {
                    message: format!(
                        "{path}: supply '{}' — on {:?} a generated key can only be read back \
                         when it is a single AUTO_INCREMENT column",
                        k.exposed_name, self.dialect
                    ),
                })
            })
            .collect()
    }

    /// The read of the row an insert wrote, by its key — or, for an upsert,
    /// by the named constraint's columns as well. Two rows answering (the
    /// object conflicted on both keys, and MySQL updated one of them where
    /// PostgreSQL would have refused the insert) is the executor's error.
    fn readback(
        &self,
        table: &Table,
        key: &[(&Column, KeyBind)],
        or_by: Option<&[(&Column, KeyBind)]>,
        path: &str,
    ) -> Result<RowsFrom> {
        let mut s = Stmt::new(self.dialect);
        let keys: Vec<&Column> = key.iter().map(|(k, _)| *k).collect();
        write!(s.ctx.sql, "SELECT {}", self.key_object(None, &keys)).unwrap();
        self.columns_as_json(table, None, &mut s.ctx.sql);
        write!(s.ctx.sql, ") FROM {} WHERE ", self.qualified(table)).unwrap();
        let equalities = |s: &mut Stmt, cols: &[(&Column, KeyBind)]| -> Result<()> {
            s.ctx.sql.push('(');
            for (i, (k, source)) in cols.iter().enumerate() {
                if i > 0 {
                    s.ctx.sql.push_str(" AND ");
                }
                let n = s.bind_from(source, &k.ty, &format!("{path}.{}", k.exposed_name))?;
                write!(
                    s.ctx.sql,
                    "{} = {}",
                    quote_ident(&k.physical_name, self.dialect),
                    self.dialect.param(n, &k.ty)
                )
                .unwrap();
            }
            s.ctx.sql.push(')');
            Ok(())
        };
        equalities(&mut s, key)?;
        if let Some(cols) = or_by {
            s.ctx.sql.push_str(" OR ");
            equalities(&mut s, cols)?;
        }
        let (sql, binds) = s.finish();
        Ok(RowsFrom::Readback { sql, binds })
    }

    /// The columns of the constraint `on_conflict` names, by exposed name.
    fn conflict_columns(
        &self,
        oc: &OnConflict,
        table: &'a Table,
        path: &str,
    ) -> Result<Vec<&'a Column>> {
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
        cols.iter()
            .map(|c| {
                // Constraint columns are recorded by exposed name (the type
                // system resolves them the same way); the statement wants
                // the physical one. A constraint over a hidden column has no
                // exposed name to resolve, and PostgreSQL's `ON CONSTRAINT`
                // sidesteps that where this cannot.
                table.find_column(c).ok_or_else(|| Error::Validate {
                    path: format!("{path}.on_conflict.constraint"),
                    message: format!(
                        "constraint '{}' covers column '{c}', which '{}' does not expose",
                        oc.constraint, table.exposed_name
                    ),
                })
            })
            .collect()
    }

    /// `ON CONFLICT (cols) DO UPDATE … | DO NOTHING` (SQLite). The
    /// constraint is named as on PostgreSQL and resolved to its columns,
    /// which is what SQLite takes; a nested `DO NOTHING` becomes a no-op
    /// update so the row comes back for its dependants, as the
    /// one-statement form does.
    fn render_on_conflict(
        &self,
        oc: &OnConflict,
        table: &'a Table,
        scope_check: Option<&BoolExpr>,
        nested: bool,
        path: &str,
        s: &mut Stmt,
    ) -> Result<()> {
        let cols = self.conflict_columns(oc, table, path)?;
        s.ctx.sql.push_str(" ON CONFLICT (");
        for (i, col) in cols.iter().enumerate() {
            if i > 0 {
                s.ctx.sql.push_str(", ");
            }
            s.ctx
                .sql
                .push_str(&quote_ident(&col.physical_name, self.dialect));
        }
        s.ctx.sql.push_str(") ");
        render_conflict_action(
            oc,
            table,
            scope_check,
            nested,
            &format!("{path}.on_conflict"),
            "excluded",
            self.schema,
            &mut s.ctx,
        )
    }

    /// `AS new ON DUPLICATE KEY UPDATE …` (MySQL), for an upsert or a nested
    /// no-op. The constraint is checked to exist, and otherwise unused:
    /// MySQL fires this on any unique key. A key the object left out is set
    /// to `LAST_INSERT_ID(key)` so that the read-back finds the existing
    /// row when that is the one the write touched. An `on_conflict` `where`
    /// is refused: `c = IF(cond, new.c, c)` would keep the row as it was
    /// but still return and count it, where PostgreSQL returns nothing.
    fn render_on_duplicate_key(
        &self,
        oc: &OnConflict,
        table: &'a Table,
        key: &[(&Column, KeyBind)],
        path: &str,
        s: &mut Stmt,
    ) -> Result<()> {
        self.conflict_columns(oc, table, path)?;
        if oc.where_.is_some() {
            return Err(Error::Unsupported {
                message: format!(
                    "{path}.on_conflict.where: a conditional upsert is not available on {:?}, \
                     which cannot leave a conflicting row unreturned when the condition fails",
                    self.dialect
                ),
            });
        }
        s.ctx.sql.push_str(" AS new ON DUPLICATE KEY UPDATE ");
        // Qualified by table: beside the `new` row alias a bare column name
        // is ambiguous to MySQL.
        let tref = quote_ident(&table.physical_name, self.dialect);
        let mut first = true;
        for (k, source) in key {
            if matches!(source, KeyBind::Plan(PlanBind::LastInsertId)) {
                let pk = quote_ident(&k.physical_name, self.dialect);
                write!(s.ctx.sql, "{tref}.{pk} = LAST_INSERT_ID({tref}.{pk})").unwrap();
                first = false;
            }
        }
        for exposed in &oc.update_columns {
            let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
                path: format!("{path}.on_conflict.update_columns.{exposed}"),
                message: format!("unknown column '{exposed}' on '{}'", table.exposed_name),
            })?;
            if !first {
                s.ctx.sql.push_str(", ");
            }
            first = false;
            let c = quote_ident(&col.physical_name, self.dialect);
            write!(s.ctx.sql, "{tref}.{c} = new.{c}").unwrap();
        }
        if first {
            // A nested no-op with its key supplied: the row is left as it
            // is, and read back by that key.
            let (k, _) = key.first().expect("a key has a column");
            let pk = quote_ident(&k.physical_name, self.dialect);
            write!(s.ctx.sql, "{tref}.{pk} = {tref}.{pk}").unwrap();
        }
        Ok(())
    }

    /// The selection of the rows `captures` hold, read back by identity in
    /// capture order. An array, or for `one` the first object.
    fn select_rows(
        &self,
        table: &'a Table,
        fields: &[Field],
        captures: &[usize],
        one: bool,
        path: &str,
    ) -> Result<Select> {
        let keys = if self.by_key() {
            self.key_columns(table, path)?
        } else {
            Vec::new()
        };
        let mut s = Stmt::new(self.dialect);
        let n = s.captured(PlanBind::Keys(captures.to_vec()), self.keys_null_of());
        // Taken from the context, not spelled: the relation subqueries in the
        // selection take their aliases from the same counter.
        let alias = s.ctx.next_alias("t");
        s.ctx.sql.push_str("SELECT ");
        if !one {
            s.ctx.sql.push_str(self.dialect.nodes_list_open());
        }
        render_json_build_object_for_nodes(fields, &alias, table, path, self.schema, &mut s.ctx)?;
        if one {
            s.ctx.sql.push_str(" FROM (");
        } else {
            write!(s.ctx.sql, "{}", self.dialect.nodes_list_mid(&alias)).unwrap();
        }
        // The join to the bound list orders the rows as they were captured;
        // the derived table keeps that order for the aggregate, or carries
        // it as a number for a window to order by.
        match self.dialect {
            Dialect::Postgres | Dialect::Sqlite => write!(
                s.ctx.sql,
                "SELECT x.* FROM {} x JOIN json_each(?{n}) je ON x.rowid = je.value ORDER BY je.key",
                self.qualified(table)
            )
            .unwrap(),
            Dialect::MySql => {
                write!(
                    s.ctx.sql,
                    "SELECT x.*, jt.ord AS {ORDER_NUMBER} FROM {} x JOIN {} ON ",
                    self.qualified(table),
                    self.key_table(n, &keys)
                )
                .unwrap();
                for (i, k) in keys.iter().enumerate() {
                    if i > 0 {
                        s.ctx.sql.push_str(" AND ");
                    }
                    write!(
                        s.ctx.sql,
                        "x.{} = jt.k{i}",
                        quote_ident(&k.physical_name, self.dialect)
                    )
                    .unwrap();
                }
                s.ctx.sql.push_str(" ORDER BY jt.ord");
            }
        }
        if one {
            write!(s.ctx.sql, ") {alias} LIMIT 1").unwrap();
        } else {
            write!(s.ctx.sql, "{}", self.dialect.nodes_list_close(&alias)).unwrap();
        }
        let (sql, binds) = s.finish();
        Ok(Select { sql, binds })
    }

    /// The selection of the rows a delete is about to remove, read while
    /// they exist — relations included, as PostgreSQL reads them from the
    /// statement's snapshot. Returns the read slot the executor fills.
    fn read_before_delete(
        &mut self,
        table: &'a Table,
        fields: &[Field],
        picks: Picks<'_>,
        one: bool,
        path: &str,
    ) -> Result<usize> {
        let keys = if self.by_key() {
            self.key_columns(table, path)?
        } else {
            Vec::new()
        };
        let mut s = Stmt::new(self.dialect);
        let inner = s.ctx.next_alias("t");
        let outer = s.ctx.next_alias("t");
        s.ctx.sql.push_str("SELECT ");
        if !one {
            s.ctx.sql.push_str(self.dialect.nodes_list_open());
        }
        render_json_build_object_for_nodes(fields, &outer, table, path, self.schema, &mut s.ctx)?;
        if one {
            s.ctx.sql.push_str(" FROM (");
        } else {
            write!(s.ctx.sql, "{}", self.dialect.nodes_list_mid(&outer)).unwrap();
        }
        let order = self.row_order(&inner, &keys);
        write!(s.ctx.sql, "SELECT {inner}.*").unwrap();
        if self.dialect.numbers_rows_for_order() {
            write!(
                s.ctx.sql,
                ", ROW_NUMBER() OVER (ORDER BY {order}) AS {ORDER_NUMBER}"
            )
            .unwrap();
        }
        write!(s.ctx.sql, " FROM {} {inner} WHERE ", self.qualified(table)).unwrap();
        match picks {
            Picks::Where(expr) => render_bool_expr(expr, table, &inner, self.schema, &mut s.ctx)?,
            Picks::Pk(pk, scope) => render_pk_predicate(
                table,
                pk,
                scope,
                path,
                Some(&inner),
                self.schema,
                &mut s.ctx,
            )?,
        }
        write!(s.ctx.sql, " ORDER BY {order}").unwrap();
        if one {
            write!(s.ctx.sql, ") {outer} LIMIT 1").unwrap();
        } else {
            write!(s.ctx.sql, "{}", self.dialect.nodes_list_close(&outer)).unwrap();
        }
        let into = self.read_slot();
        let (sql, binds) = s.finish();
        self.steps.push(Step::Read { sql, binds, into });
        Ok(into)
    }
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

    fn fields_of(source: &str, schema: &Schema) -> Vec<MutationField> {
        let doc = parse_document(source).unwrap();
        let op = lower_with(&doc, Bindings::symbolic(), None, schema).unwrap();
        let crate::ast::Operation::Mutation(fields) = op else {
            panic!("a mutation")
        };
        fields
    }

    fn plan(source: &str) -> MutationPlan {
        build(&fields_of(source, &schema()), &schema(), Dialect::Sqlite).unwrap()
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
    fn a_top_level_do_nothing_is_do_nothing() {
        let p = plan(
            r#"mutation { insert_users(objects: [{name: "u", posts: {data: [{title: "t"}]}}], on_conflict: {constraint: users_name_key, update_columns: []}) { affected_rows } }"#,
        );
        assert!(
            matches!(&p.steps[0], Step::Write { sql, .. } if sql.contains("DO NOTHING")),
            "{}",
            p.text()
        );
    }

    #[test]
    fn on_conflict_names_physical_columns() {
        let schema = Schema::builder()
            .dialect(Dialect::Sqlite)
            .table(
                Table::new("users", "main", "users")
                    .column("id", "id", ColumnType::Int8, false)
                    .column("userName", "user_name", ColumnType::Text, false)
                    .primary_key(&["id"])
                    .unique_constraint("users_name_key", &["userName"]),
            )
            .build();
        let fields = fields_of(
            r#"mutation { insert_users(objects: [{userName: "u"}], on_conflict: {constraint: users_name_key, update_columns: [userName]}) { affected_rows } }"#,
            &schema,
        );
        let p = build(&fields, &schema, Dialect::Sqlite).unwrap();
        assert!(
            p.text().contains(
                r#"ON CONFLICT ("user_name") DO UPDATE SET "user_name" = excluded."user_name""#
            ),
            "{}",
            p.text()
        );
    }

    #[test]
    fn a_row_of_defaults_takes_no_on_conflict() {
        // The lowering refuses an empty object; the builder does not.
        use crate::builder::{IntoOperation, Mutation};
        let op = Mutation::insert("users", vec![Default::default()])
            .on_conflict(OnConflict {
                constraint: "users_pkey".into(),
                update_columns: Vec::new(),
                where_: None,
            })
            .into_operation();
        let crate::ast::Operation::Mutation(fields) = op else {
            panic!("a mutation")
        };
        let err = build(&fields, &schema(), Dialect::Sqlite).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    }

    #[test]
    fn update_and_delete_with_scope() {
        let mut op = {
            let doc = parse_document(
                r#"mutation { update_users(where: {active: {_eq: true}}, _set: {name: "x"}) { affected_rows returning { name } } delete_posts(where: {id: {_eq: 1}}) { affected_rows returning { title user { name } } } }"#,
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
        // The delete's returning is read before the delete, relation included.
        let read_at = p
            .steps
            .iter()
            .position(|s| matches!(s, Step::Read { .. }))
            .unwrap();
        let delete_at = p
            .steps
            .iter()
            .position(|s| matches!(s, Step::Write { sql, .. } if sql.starts_with("DELETE")))
            .unwrap();
        assert!(read_at < delete_at);
    }
}
