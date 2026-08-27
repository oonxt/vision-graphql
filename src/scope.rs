//! Scoped execution: mechanically AND a per-table predicate into every table
//! access point of an operation before it is rendered to SQL.
//!
//! A [`ScopeSet`] maps exposed table names to a [`TableScope`]. Obtain a
//! scoped handle via [`crate::Engine::scoped`]; every query it executes is
//! rewritten so that each table access — root selects, `_by_pk`, aggregates,
//! relation subqueries at any depth, and `EXISTS` relation filters inside
//! `where` — carries the table's predicate. Tables without an entry are
//! denied: the scope set must spell out everything the caller may touch.
//!
//! Scope predicates are policy supplied by trusted application code; they are
//! injected as-is and are NOT themselves re-scoped (a predicate may reference
//! a relation to a table the caller cannot query directly).
//!
//! Scoped `delete` (and its `_by_pk` form) injects the predicate as a *filter*:
//! it is AND-ed into the statement's `WHERE`, so a scoped caller can only remove
//! rows the predicate already lets them see. A `_by_pk` row failing the
//! predicate simply does not match and the mutation returns null.
//!
//! Scoped `update` (and its `_by_pk` form) injects the predicate as *both* a
//! pre-image filter and a post-update check. The filter is AND-ed into the
//! `WHERE` (so only in-scope rows are touched); the check is a guard CTE the
//! renderer emits over the updated rows, so a caller cannot move a row *out* of
//! scope (e.g. reassign an owning column). A violation aborts the statement.
//!
//! Scoped `insert` injects the predicate as a post-insert *check*: every
//! inserted row must satisfy it or the whole statement aborts (the renderer
//! emits a guard CTE that errors on violation). Nested inserts are enforced at
//! every level — each nested target table must be in the scope set, and its
//! rows are checked against its own predicate; a violation anywhere aborts the
//! whole (atomic) statement. An `insert` with `on_conflict … update_columns`
//! (upsert) additionally injects the predicate into the `DO UPDATE … WHERE` as
//! a pre-image filter, so a conflicting row outside scope is skipped rather than
//! overwritten; the post-insert check still applies to the resulting row.

use std::collections::{BTreeSet, HashMap};

use crate::ast::{BoolExpr, Field, MutationField, Operation, RootBody};
use crate::error::{Error, Result};
use crate::schema::{Schema, Table};

/// Access rule for one table inside a [`ScopeSet`].
#[derive(Debug, Clone)]
pub enum TableScope {
    /// Access allowed; the predicate is AND-ed into every access point.
    /// Columns are exposed names on the target table, exactly as in a
    /// user-written `where`.
    Allow(BoolExpr),
    /// Access allowed with no additional predicate (public/lookup tables).
    Unrestricted,
    /// Access refused. Equivalent to omitting the table — listing it makes
    /// the intent explicit and survives "did we forget this table?" review.
    Deny,
}

/// Which columns of a table a scoped caller may touch.
///
/// The two forms differ in what a migration does to them, which is the whole
/// choice: [`Only`] is an allowlist, so a column added tomorrow is invisible
/// until someone names it; [`Except`] is a denylist, so that column is visible
/// to every caller the moment it exists. Prefer [`Only`] wherever the set is
/// knowable — a schema grows by columns nobody thought about, and that is
/// precisely the case a denylist gets wrong.
///
/// One set covers reading and writing alike: a column a caller may not see is
/// not one it may set either.
///
/// [`Only`]: ColumnScope::Only
/// [`Except`]: ColumnScope::Except
#[derive(Debug, Clone)]
pub enum ColumnScope {
    /// Only these columns, by exposed name.
    Only(BTreeSet<String>),
    /// Every column except these.
    Except(BTreeSet<String>),
}

impl ColumnScope {
    fn admits(&self, column: &str) -> bool {
        match self {
            ColumnScope::Only(cols) => cols.contains(column),
            ColumnScope::Except(cols) => !cols.contains(column),
        }
    }
}

/// Per-table access rules for one scoped execution context.
///
/// Typically built once per request from the authenticated principal and
/// passed to [`crate::Engine::scoped`].
#[derive(Debug, Clone, Default)]
pub struct ScopeSet {
    tables: HashMap<String, TableScope>,
    /// Per-table column rules. Absent means every column of that table, which
    /// keeps column restriction orthogonal to row restriction: a table can be
    /// `unrestricted` for rows and still withhold a column, and the other way
    /// round.
    columns: HashMap<String, ColumnScope>,
}

impl ScopeSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow `table`, AND-ing `expr` into every access of it.
    pub fn allow(mut self, table: impl Into<String>, expr: BoolExpr) -> Self {
        self.tables.insert(table.into(), TableScope::Allow(expr));
        self
    }

    /// Allow `table` without an additional predicate.
    pub fn unrestricted(mut self, table: impl Into<String>) -> Self {
        self.tables.insert(table.into(), TableScope::Unrestricted);
        self
    }

    /// Restrict `table` to these columns and no others.
    ///
    /// Fail-closed against a growing schema: a column added later is not
    /// admitted until it is named here. See [`ColumnScope`].
    ///
    /// Replaces any previous rule for the same table. The two forms are
    /// alternative spellings of one rule, not layers — `.columns(t, [a, b])`
    /// followed by `.hide_columns(t, [c])` leaves the *denylist*, admitting
    /// everything but `c`, which is not what the pair reads like.
    pub fn columns<I, S>(mut self, table: impl Into<String>, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns.insert(
            table.into(),
            ColumnScope::Only(columns.into_iter().map(Into::into).collect()),
        );
        self
    }

    /// Withhold these columns of `table`, admitting the rest — including any
    /// added later, which is what makes [`ScopeSet::columns`] the safer form.
    ///
    /// Replaces any previous rule for the same table; see [`ScopeSet::columns`].
    pub fn hide_columns<I, S>(mut self, table: impl Into<String>, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns.insert(
            table.into(),
            ColumnScope::Except(columns.into_iter().map(Into::into).collect()),
        );
        self
    }

    /// The column rule for `table`, if it has one.
    pub fn column_scope(&self, table: &str) -> Option<&ColumnScope> {
        self.columns.get(table)
    }

    /// Explicitly refuse access to `table`.
    pub fn deny(mut self, table: impl Into<String>) -> Self {
        self.tables.insert(table.into(), TableScope::Deny);
        self
    }

    pub fn get(&self, table: &str) -> Option<&TableScope> {
        self.tables.get(table)
    }

    /// Exposed table names this set has explicit entries for.
    pub fn tables(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }
}

/// Resolve the predicate to inject for `table`: `Ok(Some(expr))` to AND in,
/// `Ok(None)` for unrestricted, `Err` when denied or absent (fail-closed).
/// Columns an aggregate function reads.
fn agg_columns(op: &crate::ast::AggOp) -> Vec<&str> {
    use crate::ast::{AggField, AggOp};
    match op {
        AggOp::Count { columns, .. } => columns.iter().map(String::as_str).collect(),
        AggOp::Sum { fields }
        | AggOp::Avg { fields }
        | AggOp::Max { fields }
        | AggOp::Min { fields } => fields
            .iter()
            .filter_map(|f| match f {
                AggField::Column(c) => Some(c.column.as_str()),
                AggField::Typename { .. } => None,
            })
            .collect(),
        AggOp::Typename => Vec::new(),
    }
}

/// Refuse a column the scope does not admit.
///
/// Refusing rather than dropping it from the selection: a response missing a
/// field the document asked for is a wrong answer wearing the shape of a right
/// one, and a caller comparing against a column it may not read learns its
/// contents from which rows come back either way.
fn check_column(scope: &ScopeSet, table: &Table, column: &str) -> Result<()> {
    match scope.column_scope(&table.exposed_name) {
        Some(rule) if !rule.admits(column) => Err(Error::ScopeColumnDenied {
            table: table.exposed_name.clone(),
            column: column.to_string(),
        }),
        _ => Ok(()),
    }
}

/// Every column a selection reads directly. Relations recurse elsewhere, each
/// against its own target table.
fn check_selection(fields: &[Field], table: &Table, scope: &ScopeSet) -> Result<()> {
    for f in fields {
        match f {
            Field::Column { column, .. } | Field::JsonPath { column, .. } => {
                check_column(scope, table, column)?;
            }
            Field::Typename { .. } | Field::Relation { .. } => {}
        }
    }
    Ok(())
}

/// `distinct_on` reads its columns as surely as a selection does — the rows
/// that come back are chosen by their values.
fn check_args_columns(args: &crate::ast::QueryArgs, table: &Table, scope: &ScopeSet) -> Result<()> {
    for c in &args.distinct_on {
        check_column(scope, table, c)?;
    }
    Ok(())
}

fn resolve(scope: &ScopeSet, table: &str) -> Result<Option<BoolExpr>> {
    match scope.get(table) {
        Some(TableScope::Allow(expr)) => Ok(Some(expr.clone())),
        Some(TableScope::Unrestricted) => Ok(None),
        Some(TableScope::Deny) | None => Err(Error::ScopeDenied {
            table: table.to_string(),
        }),
    }
}

/// AND `new_term` into an optional where slot (same shape as the builder's
/// `merge_and`: flatten into an existing top-level `And`).
fn merge_and_into(slot: &mut Option<BoolExpr>, new_term: BoolExpr) {
    *slot = Some(match slot.take() {
        None => new_term,
        Some(BoolExpr::And(mut parts)) => {
            parts.push(new_term);
            BoolExpr::And(parts)
        }
        Some(other) => BoolExpr::And(vec![other, new_term]),
    });
}

/// AND `new_term` into a required where slot (update/delete carry a
/// non-optional `where_`). Flattens into an existing top-level `And`.
fn and_in(slot: &mut BoolExpr, new_term: BoolExpr) {
    let cur = std::mem::replace(slot, BoolExpr::And(Vec::new()));
    *slot = match cur {
        BoolExpr::And(mut parts) => {
            parts.push(new_term);
            BoolExpr::And(parts)
        }
        other => BoolExpr::And(vec![other, new_term]),
    };
}

/// Look up `table` in the schema, mapping absence to a validation error keyed
/// on `path` (the mutation's response alias).
fn lookup_table<'s>(schema: &'s Schema, table: &str, path: &str) -> Result<&'s Table> {
    schema
        .table(table)
        .map(|t| &**t)
        .ok_or_else(|| Error::Validate {
            path: path.to_string(),
            message: format!("unknown table '{table}'"),
        })
}

/// Rewrite `op` in place so every table access point carries its scope
/// predicate. Errors fail the whole operation before any SQL is built.
pub(crate) fn apply_scope(op: &mut Operation, scope: &ScopeSet, schema: &Schema) -> Result<()> {
    match op {
        Operation::Query(roots) => {
            for root in roots {
                scope_root(root, scope, schema)?;
            }
        }
        Operation::Mutation(fields) => {
            for mf in fields {
                scope_mutation(mf, scope, schema)?;
            }
        }
    }
    Ok(())
}

/// Rewrite one query root field so its table — and every nested relation it
/// reaches — carries the scope predicate.
fn scope_root(root: &mut crate::ast::RootField, scope: &ScopeSet, schema: &Schema) -> Result<()> {
    // Introspection describes the schema, not rows, so a scope has nothing to
    // restrict here — and it names no table, so the lookup below would fail.
    // What a scoped caller may *read* is still decided per row by the
    // predicates on the data roots; the schema is the same for everyone, which
    // is why enabling introspection is a deployment decision rather than a
    // per-principal one.
    if matches!(root.body, RootBody::Introspection(_)) {
        return Ok(());
    }
    let table = lookup_table(schema, &root.table, &root.alias)?;
    // Scope EXISTS targets inside the user-written where FIRST, so the
    // predicate we inject afterwards is never itself re-scoped.
    if let Some(w) = root.args.where_.as_mut() {
        scope_bool_expr(w, table, scope, schema)?;
    }
    if let Some(expr) = resolve(scope, &root.table)? {
        merge_and_into(&mut root.args.where_, expr);
    }
    scope_order_by(&mut root.args, table, scope, schema)?;
    check_args_columns(&root.args, table, scope)?;
    match &mut root.body {
        RootBody::List { selection } => {
            scope_fields(selection, table, scope, schema)?;
        }
        RootBody::ByPk { pk, selection } => {
            // A key column can be restricted like any other, and `_by_pk` reads
            // it by matching on a value the caller supplied.
            for (column, _) in pk.iter() {
                check_column(scope, table, column)?;
            }
            scope_fields(selection, table, scope, schema)?;
        }
        RootBody::Aggregate { ops, nodes, .. } => {
            // `max { salary }` answers a question about a column as surely as
            // selecting it does, and over few enough rows it answers it exactly.
            for sel in ops.iter() {
                for column in agg_columns(&sel.op) {
                    check_column(scope, table, column)?;
                }
            }
            if let Some(fields) = nodes.as_mut() {
                scope_fields(fields, table, scope, schema)?;
            }
        }
        // Returned above.
        RootBody::Introspection(_) => {}
    }
    Ok(())
}

/// Rewrite one mutation field so it can only touch in-scope rows.
///
/// `update`/`delete` AND the predicate into their `WHERE`; `update` also stashes
/// it in `scope_check` so the renderer emits a post-update guard (no row may be
/// moved out of scope). The `_by_pk` forms stash the predicate in their `scope`
/// slot, which the renderer appends onto the PK match and — for `update_by_pk` —
/// re-checks as a post-update guard. An `insert` stashes the predicate in its
/// `scope_check` slot for the renderer's post-insert guard (and its upsert
/// `DO UPDATE … WHERE` pre-image filter), recursing into nested inserts so every
/// level's target table is resolved and checked. Relation fields in `returning`/
/// selection are scoped like any query selection.
fn scope_mutation(mf: &mut MutationField, scope: &ScopeSet, schema: &Schema) -> Result<()> {
    match mf {
        MutationField::Insert {
            alias,
            table,
            objects,
            on_conflict,
            returning,
            scope_check,
            ..
        } => {
            let t = lookup_table(schema, table, alias)?;
            *scope_check = resolve(scope, table)?;
            scope_fields(returning, t, scope, schema)?;
            check_on_conflict(on_conflict.as_mut(), t, scope, schema)?;
            // Recurse through nested inserts, resolving each nested target
            // table's check. An absent/denied nested table fails closed here,
            // before any SQL is built.
            for obj in objects.iter_mut() {
                check_insert_columns(obj, t, scope, schema)?;
                scope_insert_object(obj, scope)?;
            }
            Ok(())
        }
        MutationField::Update {
            alias,
            table,
            where_,
            set,
            returning,
            scope_check,
            ..
        } => {
            let t = lookup_table(schema, table, alias)?;
            // Scope EXISTS targets in the user-written where before injecting.
            scope_bool_expr(where_, t, scope, schema)?;
            scope_fields(returning, t, scope, schema)?;
            // A column the caller may not read is not one it may write.
            for column in set.keys() {
                check_column(scope, t, column)?;
            }
            let pred = resolve(scope, table)?;
            // Same predicate twice: pre-image filter (AND-ed into the WHERE) and
            // post-update check (the renderer's guard CTE). The filter restricts
            // which rows may be touched; the check forbids moving a row out of
            // scope.
            *scope_check = pred.clone();
            if let Some(expr) = pred {
                and_in(where_, expr);
            }
            Ok(())
        }
        MutationField::Delete {
            alias,
            table,
            where_,
            returning,
            ..
        } => {
            let t = lookup_table(schema, table, alias)?;
            scope_bool_expr(where_, t, scope, schema)?;
            scope_fields(returning, t, scope, schema)?;
            if let Some(expr) = resolve(scope, table)? {
                and_in(where_, expr);
            }
            Ok(())
        }
        MutationField::UpdateByPk {
            alias,
            table,
            pk,
            set,
            selection,
            scope: slot,
        } => {
            let t = lookup_table(schema, table, alias)?;
            scope_fields(selection, t, scope, schema)?;
            for (column, _) in pk.iter() {
                check_column(scope, t, column)?;
            }
            for column in set.keys() {
                check_column(scope, t, column)?;
            }
            *slot = resolve(scope, table)?;
            Ok(())
        }
        MutationField::DeleteByPk {
            alias,
            table,
            pk,
            selection,
            scope: slot,
        } => {
            let t = lookup_table(schema, table, alias)?;
            scope_fields(selection, t, scope, schema)?;
            for (column, _) in pk.iter() {
                check_column(scope, t, column)?;
            }
            *slot = resolve(scope, table)?;
            Ok(())
        }
    }
}

/// Resolve and stash the scope check for every nested insert reachable from
/// `obj`, recursively. Each nested target table must be in the scope set; an
/// absent/denied one fails closed (`Error::ScopeDenied`). The predicate itself
/// is policy and is not re-scoped.
/// Columns written by one inserted row, and by every row nested under it.
///
/// Each level is checked against its own target table: a caller allowed to
/// write `posts.title` is not thereby allowed to write `users.role` through a
/// nested insert.
fn check_insert_columns(
    obj: &mut crate::ast::InsertObject,
    table: &Table,
    scope: &ScopeSet,
    schema: &Schema,
) -> Result<()> {
    for column in obj.columns.keys() {
        check_column(scope, table, column)?;
    }
    for nested in obj.nested_arrays.values_mut() {
        let target = lookup_table(schema, &nested.table, &table.exposed_name)?;
        check_on_conflict(nested.on_conflict.as_mut(), target, scope, schema)?;
        for row in nested.rows.iter_mut() {
            check_insert_columns(row, target, scope, schema)?;
        }
    }
    for nested in obj.nested_objects.values_mut() {
        let target = lookup_table(schema, &nested.table, &table.exposed_name)?;
        check_on_conflict(nested.on_conflict.as_mut(), target, scope, schema)?;
        check_insert_columns(&mut nested.row, target, scope, schema)?;
    }
    Ok(())
}

/// An `on_conflict` block, wherever it appears.
///
/// `update_columns` writes them and `where` reads them, so both answer to the
/// column rules — and the `where` is a user-written predicate like any other,
/// so its relation targets need the row scope injected. A nested block is the
/// same block: it was only the top-level one that was being checked, which let
/// `posts: { data: […], on_conflict: { update_columns: [secret] } }` write a
/// column the caller could not so much as read.
fn check_on_conflict(
    oc: Option<&mut crate::ast::OnConflict>,
    table: &Table,
    scope: &ScopeSet,
    schema: &Schema,
) -> Result<()> {
    let Some(oc) = oc else {
        return Ok(());
    };
    for column in &oc.update_columns {
        check_column(scope, table, column)?;
    }
    if let Some(w) = oc.where_.as_mut() {
        scope_bool_expr(w, table, scope, schema)?;
    }
    Ok(())
}

fn scope_insert_object(obj: &mut crate::ast::InsertObject, scope: &ScopeSet) -> Result<()> {
    for nai in obj.nested_arrays.values_mut() {
        nai.scope_check = resolve(scope, &nai.table)?;
        for row in nai.rows.iter_mut() {
            scope_insert_object(row, scope)?;
        }
    }
    for noi in obj.nested_objects.values_mut() {
        noi.scope_check = resolve(scope, &noi.table)?;
        scope_insert_object(&mut noi.row, scope)?;
    }
    Ok(())
}

/// Scope every relation field in a selection, recursively.
fn scope_fields(
    fields: &mut [Field],
    parent: &Table,
    scope: &ScopeSet,
    schema: &Schema,
) -> Result<()> {
    check_selection(fields, parent, scope)?;
    for field in fields {
        let Field::Relation {
            name,
            args,
            selection,
            ..
        } = field
        else {
            continue;
        };
        let rel = parent.find_relation(name).ok_or_else(|| Error::Validate {
            path: format!("{}.{name}", parent.exposed_name),
            message: format!("unknown relation '{name}' on '{}'", parent.exposed_name),
        })?;
        let target = schema
            .table(&rel.target_table)
            .ok_or_else(|| Error::Validate {
                path: format!("{}.{name}", parent.exposed_name),
                message: format!("unknown table '{}'", rel.target_table),
            })?;
        if let Some(w) = args.where_.as_mut() {
            scope_bool_expr(w, target, scope, schema)?;
        }
        if let Some(expr) = resolve(scope, &rel.target_table)? {
            merge_and_into(&mut args.where_, expr);
        }
        scope_order_by(args, target, scope, schema)?;
        check_args_columns(args, target, scope)?;
        scope_fields(selection, target, scope, schema)?;
    }
    Ok(())
}

/// Resolve the scope of every table an `order_by` walks through.
///
/// Ordering through an object relation reads the target table exactly as
/// selecting it does — the renderer emits a correlated subquery over it — so it
/// resolves the same way, and fail-closed for the same reason: a caller that may
/// not read a table must not be able to sort by it either. Sorting is not a
/// weaker form of access than reading; the resulting row order is a function of
/// the hidden column, and for a low-cardinality column it discloses that column
/// outright.
///
/// A restricted (rather than denied) target keeps working: its predicate is
/// stashed on the hop and ANDed into the subquery, so rows the caller cannot see
/// sort as NULL instead of by their real value.
fn scope_order_by(
    args: &mut crate::ast::QueryArgs,
    table: &Table,
    scope: &ScopeSet,
    schema: &Schema,
) -> Result<()> {
    for ob in &mut args.order_by {
        let mut cur = table;
        for hop in &mut ob.path {
            let rel = cur
                .find_relation(&hop.relation)
                .ok_or_else(|| Error::Validate {
                    path: format!("{}.order_by.{}", cur.exposed_name, hop.relation),
                    message: format!(
                        "unknown relation '{}' on '{}'",
                        hop.relation, cur.exposed_name
                    ),
                })?;
            let target = schema
                .table(&rel.target_table)
                .ok_or_else(|| Error::Validate {
                    path: format!("{}.order_by.{}", cur.exposed_name, hop.relation),
                    message: format!("unknown table '{}'", rel.target_table),
                })?;
            hop.filter = resolve(scope, &rel.target_table)?;
            cur = target;
        }
        // Sorting by a column reads it: the row order is a function of its
        // values, and for a low-cardinality column it discloses them outright.
        check_column(scope, cur, &ob.column)?;
    }
    Ok(())
}

/// Scope the targets of `EXISTS` relation filters inside a user-written
/// boolean expression. Only user-written expressions are walked; injected
/// scope predicates are policy and pass through untouched.
fn scope_bool_expr(
    expr: &mut BoolExpr,
    table: &Table,
    scope: &ScopeSet,
    schema: &Schema,
) -> Result<()> {
    match expr {
        BoolExpr::And(parts) | BoolExpr::Or(parts) => {
            for p in parts {
                scope_bool_expr(p, table, scope, schema)?;
            }
            Ok(())
        }
        BoolExpr::Not(inner) => scope_bool_expr(inner, table, scope, schema),
        BoolExpr::Relation { name, inner } => {
            let rel = table.find_relation(name).ok_or_else(|| Error::Validate {
                path: format!("{}.where.{name}", table.exposed_name),
                message: format!("unknown relation '{name}' on '{}'", table.exposed_name),
            })?;
            let target = schema
                .table(&rel.target_table)
                .ok_or_else(|| Error::Validate {
                    path: format!("{}.where.{name}", table.exposed_name),
                    message: format!("unknown table '{}'", rel.target_table),
                })?;
            scope_bool_expr(inner, target, scope, schema)?;
            if let Some(scope_expr) = resolve(scope, &rel.target_table)? {
                let user_inner = std::mem::replace(inner.as_mut(), BoolExpr::And(Vec::new()));
                *inner.as_mut() = BoolExpr::And(vec![user_inner, scope_expr]);
            }
            Ok(())
        }
        // Filtering on a column reads it — which rows come back is a function
        // of its values, so a caller that may not select it may not compare
        // against it either.
        BoolExpr::Compare { column, .. }
        | BoolExpr::IsNull { column, .. }
        | BoolExpr::InList { column, .. } => check_column(scope, table, column),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{CmpOp, QueryArgs, RootField};
    use crate::schema::{PgType, Relation, Table};
    use serde_json::json;

    fn schema() -> Schema {
        Schema::builder()
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
                    .column("user_id", "user_id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .primary_key(&["id"]),
            )
            .build()
    }

    fn owner(col: &str, v: i64) -> BoolExpr {
        BoolExpr::Compare {
            column: col.into(),
            op: CmpOp::Eq,
            value: json!(v).into(),
        }
    }

    fn list_root(table: &str, selection: Vec<Field>) -> RootField {
        RootField {
            table: table.into(),
            alias: table.into(),
            args: QueryArgs::default(),
            body: RootBody::List { selection },
        }
    }

    #[test]
    fn injects_predicate_into_root_where() {
        let mut op = Operation::Query(vec![list_root(
            "posts",
            vec![Field::Column {
                column: "id".into(),
                alias: "id".into(),
            }],
        )]);
        let scope = ScopeSet::new().allow("posts", owner("user_id", 7));
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Query(roots) = op else {
            unreachable!()
        };
        match roots[0].args.where_.as_ref().unwrap() {
            BoolExpr::Compare { column, .. } => assert_eq!(column, "user_id"),
            other => panic!("expected injected compare, got {other:?}"),
        }
    }

    #[test]
    fn absent_table_is_denied() {
        let mut op = Operation::Query(vec![list_root("posts", Vec::new())]);
        let scope = ScopeSet::new(); // empty: nothing reachable
        let err = apply_scope(&mut op, &scope, &schema()).unwrap_err();
        assert!(matches!(err, Error::ScopeDenied { table } if table == "posts"));
    }

    #[test]
    fn relation_selection_gets_target_scope() {
        let mut op = Operation::Query(vec![list_root(
            "users",
            vec![Field::Relation {
                name: "posts".into(),
                alias: "posts".into(),
                args: QueryArgs::default(),
                selection: vec![Field::Column {
                    column: "title".into(),
                    alias: "title".into(),
                }],
            }],
        )]);
        let scope = ScopeSet::new()
            .unrestricted("users")
            .allow("posts", owner("user_id", 7));
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Query(roots) = op else {
            unreachable!()
        };
        let RootBody::List { selection } = &roots[0].body else {
            unreachable!()
        };
        let Field::Relation { args, .. } = &selection[0] else {
            unreachable!()
        };
        assert!(args.where_.is_some(), "relation where must carry scope");
    }

    #[test]
    fn exists_filter_target_is_scoped() {
        let mut root = list_root("users", Vec::new());
        root.args.where_ = Some(BoolExpr::Relation {
            name: "posts".into(),
            inner: Box::new(owner("id", 1)),
        });
        let mut op = Operation::Query(vec![root]);
        let scope = ScopeSet::new()
            .unrestricted("users")
            .allow("posts", owner("user_id", 7));
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Query(roots) = op else {
            unreachable!()
        };
        let Some(BoolExpr::Relation { inner, .. }) = roots[0].args.where_.as_ref() else {
            panic!("expected relation filter to survive");
        };
        assert!(
            matches!(inner.as_ref(), BoolExpr::And(parts) if parts.len() == 2),
            "EXISTS inner must be (user AND scope), got {inner:?}"
        );
    }

    #[test]
    fn exists_filter_on_denied_table_errors() {
        let mut root = list_root("users", Vec::new());
        root.args.where_ = Some(BoolExpr::Relation {
            name: "posts".into(),
            inner: Box::new(owner("id", 1)),
        });
        let mut op = Operation::Query(vec![root]);
        let scope = ScopeSet::new().unrestricted("users").deny("posts");
        let err = apply_scope(&mut op, &scope, &schema()).unwrap_err();
        assert!(matches!(err, Error::ScopeDenied { table } if table == "posts"));
    }

    fn insert(table: &str, objects: Vec<crate::ast::InsertObject>) -> MutationField {
        MutationField::Insert {
            response_typenames: Vec::new(),
            alias: format!("insert_{table}"),
            table: table.into(),
            objects,
            on_conflict: None,
            returning: Vec::new(),
            one: false,
            scope_check: None,
        }
    }

    #[test]
    fn flat_insert_gets_scope_check() {
        let mut op = Operation::Mutation(vec![insert(
            "posts",
            vec![crate::ast::InsertObject::default()],
        )]);
        let scope = ScopeSet::new().allow("posts", owner("user_id", 7));
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Mutation(fields) = op else {
            unreachable!()
        };
        let MutationField::Insert { scope_check, .. } = &fields[0] else {
            unreachable!()
        };
        assert!(
            matches!(scope_check, Some(BoolExpr::Compare { column, .. }) if column == "user_id"),
            "flat insert must carry the table's check, got {scope_check:?}"
        );
    }

    #[test]
    fn unrestricted_insert_has_no_check() {
        let mut op = Operation::Mutation(vec![insert(
            "posts",
            vec![crate::ast::InsertObject::default()],
        )]);
        let scope = ScopeSet::new().unrestricted("posts");
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Mutation(fields) = op else {
            unreachable!()
        };
        let MutationField::Insert { scope_check, .. } = &fields[0] else {
            unreachable!()
        };
        assert!(scope_check.is_none(), "unrestricted table needs no check");
    }

    #[test]
    fn insert_on_denied_table_errors() {
        let mut op = Operation::Mutation(vec![insert(
            "posts",
            vec![crate::ast::InsertObject::default()],
        )]);
        let scope = ScopeSet::new(); // posts absent → denied
        let err = apply_scope(&mut op, &scope, &schema()).unwrap_err();
        assert!(matches!(err, Error::ScopeDenied { table } if table == "posts"));
    }

    fn nested_posts_parent() -> crate::ast::InsertObject {
        let mut parent = crate::ast::InsertObject::default();
        parent.nested_arrays.insert(
            "posts".into(),
            crate::ast::NestedArrayInsert {
                table: "posts".into(),
                rows: vec![crate::ast::InsertObject::default()],
                on_conflict: None,
                scope_check: None,
            },
        );
        parent
    }

    #[test]
    fn nested_insert_gets_per_level_check() {
        let mut op = Operation::Mutation(vec![insert("users", vec![nested_posts_parent()])]);
        let scope = ScopeSet::new()
            .unrestricted("users")
            .allow("posts", owner("user_id", 7));
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Mutation(fields) = op else {
            unreachable!()
        };
        let MutationField::Insert { objects, .. } = &fields[0] else {
            unreachable!()
        };
        let nai = objects[0].nested_arrays.get("posts").unwrap();
        assert!(
            matches!(&nai.scope_check, Some(BoolExpr::Compare { column, .. }) if column == "user_id"),
            "nested level must carry its table's check, got {:?}",
            nai.scope_check
        );
    }

    #[test]
    fn nested_insert_denied_target_fails_closed() {
        let mut op = Operation::Mutation(vec![insert("users", vec![nested_posts_parent()])]);
        // users allowed, but the nested target `posts` is absent → denied.
        let scope = ScopeSet::new().unrestricted("users");
        let err = apply_scope(&mut op, &scope, &schema()).unwrap_err();
        assert!(matches!(err, Error::ScopeDenied { table } if table == "posts"));
    }

    #[test]
    fn update_where_gets_scope_anded_in() {
        let mut op = Operation::Mutation(vec![MutationField::Update {
            response_typenames: Vec::new(),
            alias: "update_posts".into(),
            table: "posts".into(),
            where_: owner("id", 1),
            set: std::collections::BTreeMap::new(),
            returning: Vec::new(),
            scope_check: None,
        }]);
        let scope = ScopeSet::new().allow("posts", owner("user_id", 7));
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Mutation(fields) = op else {
            unreachable!()
        };
        let MutationField::Update {
            where_,
            scope_check,
            ..
        } = &fields[0]
        else {
            unreachable!()
        };
        assert!(
            matches!(where_, BoolExpr::And(parts) if parts.len() == 2),
            "update where must be (user AND scope), got {where_:?}"
        );
        assert!(
            matches!(scope_check, Some(BoolExpr::Compare { column, .. }) if column == "user_id"),
            "update must also stash the predicate as a post-update check, got {scope_check:?}"
        );
    }

    #[test]
    fn unrestricted_update_has_no_check() {
        let mut op = Operation::Mutation(vec![MutationField::Update {
            response_typenames: Vec::new(),
            alias: "update_posts".into(),
            table: "posts".into(),
            where_: owner("id", 1),
            set: std::collections::BTreeMap::new(),
            returning: Vec::new(),
            scope_check: None,
        }]);
        let scope = ScopeSet::new().unrestricted("posts");
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Mutation(fields) = op else {
            unreachable!()
        };
        let MutationField::Update { scope_check, .. } = &fields[0] else {
            unreachable!()
        };
        assert!(
            scope_check.is_none(),
            "unrestricted update needs no post-update check"
        );
    }

    #[test]
    fn delete_on_denied_table_errors() {
        let mut op = Operation::Mutation(vec![MutationField::Delete {
            response_typenames: Vec::new(),
            alias: "delete_posts".into(),
            table: "posts".into(),
            where_: owner("id", 1),
            returning: Vec::new(),
        }]);
        let scope = ScopeSet::new(); // posts absent: denied
        let err = apply_scope(&mut op, &scope, &schema()).unwrap_err();
        assert!(matches!(err, Error::ScopeDenied { table } if table == "posts"));
    }

    #[test]
    fn update_by_pk_fills_scope_slot() {
        let mut op = Operation::Mutation(vec![MutationField::UpdateByPk {
            alias: "update_posts_by_pk".into(),
            table: "posts".into(),
            pk: vec![("id".into(), json!(1).into())],
            set: std::collections::BTreeMap::new(),
            selection: Vec::new(),
            scope: None,
        }]);
        let scope = ScopeSet::new().allow("posts", owner("user_id", 7));
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Mutation(fields) = op else {
            unreachable!()
        };
        let MutationField::UpdateByPk { scope, .. } = &fields[0] else {
            unreachable!()
        };
        assert!(
            matches!(scope, Some(BoolExpr::Compare { column, .. }) if column == "user_id"),
            "by_pk scope slot must carry the predicate, got {scope:?}"
        );
    }

    #[test]
    fn unrestricted_by_pk_leaves_scope_empty() {
        let mut op = Operation::Mutation(vec![MutationField::DeleteByPk {
            alias: "delete_posts_by_pk".into(),
            table: "posts".into(),
            pk: vec![("id".into(), json!(1).into())],
            selection: Vec::new(),
            scope: None,
        }]);
        let scope = ScopeSet::new().unrestricted("posts");
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Mutation(fields) = op else {
            unreachable!()
        };
        let MutationField::DeleteByPk { scope, .. } = &fields[0] else {
            unreachable!()
        };
        assert!(scope.is_none(), "unrestricted table needs no predicate");
    }

    #[test]
    fn scope_predicate_is_not_rescoped() {
        // posts' scope references the user relation; users itself is NOT in
        // the scope set. Injection must still succeed: policy predicates are
        // trusted and never re-scoped.
        let schema = Schema::builder()
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build();
        let mut op = Operation::Query(vec![list_root("posts", Vec::new())]);
        let scope = ScopeSet::new().allow(
            "posts",
            BoolExpr::Relation {
                name: "user".into(),
                inner: Box::new(owner("id", 7)),
            },
        );
        apply_scope(&mut op, &scope, &schema).unwrap();
        let Operation::Query(roots) = op else {
            unreachable!()
        };
        assert!(matches!(
            roots[0].args.where_.as_ref().unwrap(),
            BoolExpr::Relation { .. }
        ));
    }
}

#[cfg(test)]
mod column_tests {
    use super::*;
    use crate::ast::CmpOp;
    use crate::schema::{PgType, Relation, Table};
    use serde_json::json;

    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .column("salary", "salary", PgType::Int4, true)
                    .primary_key(&["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .column("draft", "draft", PgType::Bool, false)
                    .primary_key(&["id"]),
            )
            .build()
    }

    /// `salary` is not in the set; everything else on `users` is.
    fn scope() -> ScopeSet {
        ScopeSet::new()
            .unrestricted("users")
            .unrestricted("posts")
            .columns("users", ["id", "name"])
    }

    fn lower(q: &str) -> Operation {
        crate::parser::parse_and_lower(q, &json!({}), None, &schema()).unwrap()
    }

    fn apply(q: &str) -> Result<Operation> {
        let mut op = lower(q);
        apply_scope(&mut op, &scope(), &schema())?;
        Ok(op)
    }

    fn denied(q: &str) -> String {
        let err = apply(q).unwrap_err();
        assert!(
            matches!(err, Error::ScopeColumnDenied { .. }),
            "expected a column denial, got {err:?}"
        );
        format!("{err}")
    }

    #[test]
    fn an_admitted_column_passes_everywhere() {
        apply("{ users { id name posts { id draft } } }").unwrap();
        apply("{ users(where: {name: {_eq: \"a\"}}, order_by: [{name: asc}]) { id } }").unwrap();
        apply("{ users_by_pk(id: 1) { name } }").unwrap();
    }

    #[test]
    fn selecting_a_withheld_column_is_refused() {
        let msg = denied("{ users { id salary } }");
        assert!(msg.contains("salary"), "{msg}");
        // Refused, not dropped: a response missing a field the document asked
        // for is a wrong answer shaped like a right one.
        denied("{ users_by_pk(id: 1) { salary } }");
        denied("{ users_aggregate { nodes { salary } } }");
    }

    /// Reading is not the only way to learn a column's value.
    #[test]
    fn a_withheld_column_cannot_be_filtered_sorted_or_counted_on() {
        denied("{ users(where: {salary: {_gt: 100}}) { id } }");
        denied("{ users(where: {salary: {_is_null: true}}) { id } }");
        denied("{ users(where: {salary: {_in: [1, 2]}}) { id } }");
        denied("{ users(where: {_or: [{salary: {_gt: 1}}]}) { id } }");
        denied("{ users(order_by: [{salary: desc}]) { id } }");
        denied("{ users(distinct_on: [salary]) { id } }");
        denied("{ users_aggregate { aggregate { max { salary } } } }");
        denied("{ users_aggregate { aggregate { count(columns: [salary]) } } }");
    }

    #[test]
    fn a_withheld_column_cannot_be_written() {
        denied(r#"mutation { insert_users(objects: [{salary: 1}]) { affected_rows } }"#);
        denied(
            r#"mutation { update_users(where: {id: {_eq: 1}}, _set: {salary: 1}) {
                 affected_rows } }"#,
        );
        denied(r#"mutation { update_users_by_pk(pk_columns: {id: 1}, _set: {salary: 1}) { id } }"#);
        denied(r#"mutation { insert_users(objects: [{name: "a"}]) { returning { salary } } }"#);
    }

    /// Each level of a nested insert answers to its own table's rules.
    #[test]
    fn nested_insert_columns_answer_to_their_own_table() {
        let scope = ScopeSet::new()
            .unrestricted("users")
            .unrestricted("posts")
            .columns("users", ["id", "name"])
            .columns("posts", ["id", "user_id"]);
        let mut op = lower(
            r#"mutation { insert_users(objects: [{name: "a", posts: {data: [{draft: true}]}}]) {
                 affected_rows } }"#,
        );
        let err = apply_scope(&mut op, &scope, &schema()).unwrap_err();
        assert!(
            format!("{err}").contains("draft"),
            "the child's column, against the child's rules: {err}"
        );
    }

    /// The rules follow the relation: `posts` is unrestricted here, and reading
    /// it through `users` does not borrow `users`' restriction.
    #[test]
    fn a_relations_columns_are_the_targets() {
        apply("{ users { id posts { draft } } }").unwrap();
        let scope = ScopeSet::new()
            .unrestricted("users")
            .unrestricted("posts")
            .columns("posts", ["id"]);
        let mut op = lower("{ users { id posts { draft } } }");
        let err = apply_scope(&mut op, &scope, &schema()).unwrap_err();
        assert!(format!("{err}").contains("draft"), "{err}");
    }

    /// The `physical` field held a physical name while every reader looked it
    /// up as an exposed one — so a scope rule naming the exposed column
    /// compared against the wrong string, in both directions.
    #[test]
    fn a_column_whose_names_differ_is_matched_by_its_exposed_name() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("salary", "salary_cents", PgType::Int4, true)
                    .primary_key(&["id"]),
            )
            .build();
        let lower = |q: &str| crate::parser::parse_and_lower(q, &json!({}), None, &schema).unwrap();

        // Withheld stays withheld…
        let scope = ScopeSet::new()
            .unrestricted("users")
            .hide_columns("users", ["salary"]);
        let mut op = lower("{ users { salary } }");
        let err = apply_scope(&mut op, &scope, &schema).unwrap_err();
        assert!(matches!(err, Error::ScopeColumnDenied { .. }), "{err:?}");

        // …and admitted stays admitted.
        let scope = ScopeSet::new()
            .unrestricted("users")
            .columns("users", ["id", "salary"]);
        let mut op = lower("{ users { id salary } }");
        apply_scope(&mut op, &scope, &schema).unwrap();
    }

    /// A nested block's `on_conflict` is the same block as a top-level one, and
    /// writes a column just as directly.
    #[test]
    fn a_nested_on_conflict_answers_to_the_column_rules() {
        let scope = ScopeSet::new()
            .unrestricted("users")
            .unrestricted("posts")
            .columns("posts", ["id", "user_id"]);
        let mut op = lower(
            r#"mutation { insert_users(objects: [{name: "a", posts: {
                 data: [{id: 1}],
                 on_conflict: {constraint: "posts_pkey", update_columns: ["draft"]}
               }}]) { affected_rows } }"#,
        );
        let err = apply_scope(&mut op, &scope, &schema()).unwrap_err();
        assert!(format!("{err}").contains("draft"), "{err}");
    }

    /// …and its `where` is a user-written predicate: its columns are read, and
    /// its relation targets need the row scope injected like anywhere else.
    #[test]
    fn a_nested_on_conflict_where_is_checked_and_scoped() {
        let scope = ScopeSet::new()
            .unrestricted("users")
            .unrestricted("posts")
            .columns("posts", ["id", "user_id"]);
        let mut op = lower(
            r#"mutation { insert_users(objects: [{name: "a", posts: {
                 data: [{id: 1}],
                 on_conflict: {constraint: "posts_pkey", update_columns: ["id"],
                               where: {draft: {_eq: true}}}
               }}]) { affected_rows } }"#,
        );
        let err = apply_scope(&mut op, &scope, &schema()).unwrap_err();
        assert!(format!("{err}").contains("draft"), "{err}");
    }

    #[test]
    fn hide_columns_is_the_complement_and_admits_what_it_does_not_name() {
        let scope = ScopeSet::new()
            .unrestricted("users")
            .hide_columns("users", ["salary"]);
        let mut op = lower("{ users { id name } }");
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let mut op = lower("{ users { salary } }");
        assert!(apply_scope(&mut op, &scope, &schema()).is_err());
    }

    #[test]
    fn a_table_with_no_column_rule_admits_every_column() {
        let scope = ScopeSet::new().unrestricted("users").unrestricted("posts");
        let mut op = lower("{ users { id name salary } }");
        apply_scope(&mut op, &scope, &schema()).unwrap();
    }

    #[test]
    fn row_and_column_rules_are_independent() {
        // Restricted rows, restricted columns, on a table that is `allow`ed
        // rather than `unrestricted`.
        let scope = ScopeSet::new()
            .allow(
                "users",
                BoolExpr::Compare {
                    column: "id".into(),
                    op: CmpOp::Eq,
                    value: json!(7).into(),
                },
            )
            .columns("users", ["id", "name"]);
        let mut op = lower("{ users { id name } }");
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Query(roots) = &op else {
            panic!()
        };
        assert!(
            roots[0].args.where_.is_some(),
            "the row predicate still lands"
        );

        let mut op = lower("{ users { salary } }");
        assert!(apply_scope(&mut op, &scope, &schema()).is_err());
    }
}
