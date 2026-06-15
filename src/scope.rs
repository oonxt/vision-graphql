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
//! Scoped `update` and `delete` (including their `_by_pk` forms) inject the
//! predicate as a *filter*: it is AND-ed into the statement's `WHERE`, so a
//! scoped caller can only modify rows the predicate already lets them see. A
//! `_by_pk` row failing the predicate simply does not match and the mutation
//! returns null.
//!
//! Scoped `insert` injects the predicate as a post-insert *check*: every
//! inserted row must satisfy it or the whole statement aborts (the renderer
//! emits a guard CTE that errors on violation). Nested inserts are enforced at
//! every level — each nested target table must be in the scope set, and its
//! rows are checked against its own predicate; a violation anywhere aborts the
//! whole (atomic) statement.

use std::collections::HashMap;

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

/// Per-table access rules for one scoped execution context.
///
/// Typically built once per request from the authenticated principal and
/// passed to [`crate::Engine::scoped`].
#[derive(Debug, Clone, Default)]
pub struct ScopeSet {
    tables: HashMap<String, TableScope>,
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
    let table = lookup_table(schema, &root.table, &root.alias)?;
    // Scope EXISTS targets inside the user-written where FIRST, so the
    // predicate we inject afterwards is never itself re-scoped.
    if let Some(w) = root.args.where_.as_mut() {
        scope_bool_expr(w, table, scope, schema)?;
    }
    if let Some(expr) = resolve(scope, &root.table)? {
        merge_and_into(&mut root.args.where_, expr);
    }
    match &mut root.body {
        RootBody::List { selection } | RootBody::ByPk { selection, .. } => {
            scope_fields(selection, table, scope, schema)?;
        }
        RootBody::Aggregate { nodes, .. } => {
            if let Some(fields) = nodes.as_mut() {
                scope_fields(fields, table, scope, schema)?;
            }
        }
    }
    Ok(())
}

/// Rewrite one mutation field so it can only touch in-scope rows.
///
/// `update`/`delete` AND the predicate into their `WHERE`; the `_by_pk` forms
/// stash it in their `scope` slot for the renderer to append onto the PK
/// match. An `insert` stashes the predicate in its `scope_check` slot for the
/// renderer's post-insert guard, recursing into nested inserts so every level's
/// target table is resolved and checked. Relation fields in `returning`/
/// selection are scoped like any query selection.
fn scope_mutation(mf: &mut MutationField, scope: &ScopeSet, schema: &Schema) -> Result<()> {
    match mf {
        MutationField::Insert {
            alias,
            table,
            objects,
            returning,
            scope_check,
            ..
        } => {
            let t = lookup_table(schema, table, alias)?;
            *scope_check = resolve(scope, table)?;
            scope_fields(returning, t, scope, schema)?;
            // Recurse through nested inserts, resolving each nested target
            // table's check. An absent/denied nested table fails closed here,
            // before any SQL is built.
            for obj in objects.iter_mut() {
                scope_insert_object(obj, scope)?;
            }
            Ok(())
        }
        MutationField::Update {
            alias,
            table,
            where_,
            returning,
            ..
        } => {
            let t = lookup_table(schema, table, alias)?;
            // Scope EXISTS targets in the user-written where before injecting.
            scope_bool_expr(where_, t, scope, schema)?;
            scope_fields(returning, t, scope, schema)?;
            if let Some(expr) = resolve(scope, table)? {
                and_in(where_, expr);
            }
            Ok(())
        }
        MutationField::Delete {
            alias,
            table,
            where_,
            returning,
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
            selection,
            scope: slot,
            ..
        } => {
            let t = lookup_table(schema, table, alias)?;
            scope_fields(selection, t, scope, schema)?;
            *slot = resolve(scope, table)?;
            Ok(())
        }
        MutationField::DeleteByPk {
            alias,
            table,
            selection,
            scope: slot,
            ..
        } => {
            let t = lookup_table(schema, table, alias)?;
            scope_fields(selection, t, scope, schema)?;
            *slot = resolve(scope, table)?;
            Ok(())
        }
    }
}

/// Resolve and stash the scope check for every nested insert reachable from
/// `obj`, recursively. Each nested target table must be in the scope set; an
/// absent/denied one fails closed (`Error::ScopeDenied`). The predicate itself
/// is policy and is not re-scoped.
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
        scope_fields(selection, target, scope, schema)?;
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
        BoolExpr::Compare { .. } | BoolExpr::IsNull { .. } | BoolExpr::InList { .. } => Ok(()),
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
            value: json!(v),
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
                physical: "id".into(),
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
                    physical: "title".into(),
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
            alias: "update_posts".into(),
            table: "posts".into(),
            where_: owner("id", 1),
            set: std::collections::BTreeMap::new(),
            returning: Vec::new(),
        }]);
        let scope = ScopeSet::new().allow("posts", owner("user_id", 7));
        apply_scope(&mut op, &scope, &schema()).unwrap();
        let Operation::Mutation(fields) = op else {
            unreachable!()
        };
        let MutationField::Update { where_, .. } = &fields[0] else {
            unreachable!()
        };
        assert!(
            matches!(where_, BoolExpr::And(parts) if parts.len() == 2),
            "update where must be (user AND scope), got {where_:?}"
        );
    }

    #[test]
    fn delete_on_denied_table_errors() {
        let mut op = Operation::Mutation(vec![MutationField::Delete {
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
            pk: vec![("id".into(), json!(1))],
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
            pk: vec![("id".into(), json!(1))],
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
