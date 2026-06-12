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
//! Mutations are currently rejected in scoped execution (fail-closed); scoped
//! mutation support is tracked separately.

use std::collections::HashMap;

use crate::ast::{BoolExpr, Field, Operation, RootBody};
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

/// Rewrite `op` in place so every table access point carries its scope
/// predicate. Errors fail the whole operation before any SQL is built.
pub(crate) fn apply_scope(op: &mut Operation, scope: &ScopeSet, schema: &Schema) -> Result<()> {
    let roots = match op {
        Operation::Mutation(_) => {
            return Err(Error::Scope(
                "mutations are not supported in scoped execution".into(),
            ));
        }
        Operation::Query(roots) => roots,
    };
    for root in roots {
        let table = schema.table(&root.table).ok_or_else(|| Error::Validate {
            path: root.alias.clone(),
            message: format!("unknown table '{}'", root.table),
        })?;
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

    #[test]
    fn mutations_are_rejected() {
        let mut op = Operation::Mutation(Vec::new());
        let scope = ScopeSet::new();
        let err = apply_scope(&mut op, &scope, &schema()).unwrap_err();
        assert!(matches!(err, Error::Scope(_)));
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
