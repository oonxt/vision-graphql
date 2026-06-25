//! Scope policy templates: build once, validate against the schema, then bind a
//! [`Principal`] per request to get a [`ScopeSet`].
//!
//! A [`ScopePolicy`] is the static *shape* of an access policy — which tables
//! are reachable and under which predicate — with the per-principal values left
//! as parameters. Build and [`validate`](ScopePolicyBuilder::validate) it once
//! (its lookups against the schema happen here, not per request); then call
//! [`bind`](ScopePolicy::bind) on each request to substitute the principal and
//! obtain a concrete [`ScopeSet`] for [`crate::Engine::scoped`].
//!
//! ```
//! use vision_graphql::predicate::{col, principal};
//! use vision_graphql::policy::ScopePolicy;
//! # use vision_graphql::schema::{Schema, Table, PgType, Relation};
//! # let schema = Schema::builder()
//! #     .table(Table::new("orders", "public", "orders")
//! #         .column("id", "id", PgType::Int4, false)
//! #         .column("user_id", "user_id", PgType::Int4, false)
//! #         .primary_key(&["id"]))
//! #     .build();
//! let policy = ScopePolicy::builder()
//!     .allow("orders", col("user_id").eq(principal()))
//!     .validate(&schema)
//!     .unwrap();
//!
//! // per request:
//! let scope = policy.bind_value(7).unwrap();
//! # let _ = scope;
//! ```

use std::collections::HashMap;

use serde_json::Value;

use crate::error::{Error, Result};
use crate::predicate::{Principal, ScopeExpr};
use crate::schema::{Schema, Table};
use crate::scope::ScopeSet;

/// Per-table rule in a [`ScopePolicy`], the templated counterpart of
/// [`crate::TableScope`].
#[derive(Debug, Clone)]
pub enum ScopeRule {
    /// Access allowed under this predicate template.
    Allow(ScopeExpr),
    /// Access allowed with no predicate (public/lookup tables).
    Unrestricted,
    /// Access explicitly refused.
    Deny,
}

/// A validated, reusable access policy. Build via [`ScopePolicy::builder`] or
/// [`ScopePolicy::from_toml`](crate::scope_config); [`bind`](Self::bind) per
/// request.
#[derive(Debug, Clone, Default)]
pub struct ScopePolicy {
    tables: HashMap<String, ScopeRule>,
}

/// Builder for [`ScopePolicy`]. Mirrors [`crate::ScopeSet`]'s surface but takes
/// predicate *templates* and must be [`validate`](Self::validate)d.
#[derive(Debug, Clone, Default)]
pub struct ScopePolicyBuilder {
    tables: HashMap<String, ScopeRule>,
}

impl ScopePolicy {
    pub fn builder() -> ScopePolicyBuilder {
        ScopePolicyBuilder::default()
    }

    /// Substitute `principal` into every rule, producing a concrete
    /// [`ScopeSet`]. Cheap: a tree-walk + clone, no parsing or schema lookups.
    /// Errors only when a rule references a parameter the principal omits.
    pub fn bind(&self, principal: &Principal) -> Result<ScopeSet> {
        let mut set = ScopeSet::new();
        for (table, rule) in &self.tables {
            set = match rule {
                ScopeRule::Allow(expr) => set.allow(table.clone(), expr.resolve(principal)?),
                ScopeRule::Unrestricted => set.unrestricted(table.clone()),
                ScopeRule::Deny => set.deny(table.clone()),
            };
        }
        Ok(set)
    }

    /// Convenience for single-key scopes: binds the `principal` parameter to
    /// `value`. Equivalent to `bind(&Principal::new().set("principal", value))`.
    pub fn bind_value(&self, value: impl Into<Value>) -> Result<ScopeSet> {
        self.bind(&Principal::new().set("principal", value))
    }

    /// Exposed table names this policy has explicit rules for.
    pub fn tables(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }
}

impl ScopePolicyBuilder {
    /// Allow `table` under `expr`.
    pub fn allow(mut self, table: impl Into<String>, expr: ScopeExpr) -> Self {
        self.tables.insert(table.into(), ScopeRule::Allow(expr));
        self
    }

    /// Allow `table` with no predicate.
    pub fn unrestricted(mut self, table: impl Into<String>) -> Self {
        self.tables.insert(table.into(), ScopeRule::Unrestricted);
        self
    }

    /// Explicitly refuse `table`.
    pub fn deny(mut self, table: impl Into<String>) -> Self {
        self.tables.insert(table.into(), ScopeRule::Deny);
        self
    }

    /// Insert a pre-built rule (used by the TOML loader).
    pub fn rule(mut self, table: impl Into<String>, rule: ScopeRule) -> Self {
        self.tables.insert(table.into(), rule);
        self
    }

    /// Validate every rule against `schema` — each table exists, and within an
    /// `Allow` template every column resolves on its (relation-walked) table and
    /// every relation resolves to a known target. This is the same set of checks
    /// [`crate::scope`] runs per request, hoisted to build time. On success the
    /// policy is frozen and [`bind`](ScopePolicy::bind) cannot fail on shape.
    pub fn validate(self, schema: &Schema) -> Result<ScopePolicy> {
        for (table_name, rule) in &self.tables {
            let table = schema.table(table_name).ok_or_else(|| Error::Validate {
                path: format!("scope.{table_name}"),
                message: format!("unknown table '{table_name}'"),
            })?;
            if let ScopeRule::Allow(expr) = rule {
                validate_expr(expr, table, schema, &format!("scope.{table_name}"))?;
            }
        }
        Ok(ScopePolicy {
            tables: self.tables,
        })
    }
}

/// Walk a template against `table`, checking every column and relation exists.
fn validate_expr(expr: &ScopeExpr, table: &Table, schema: &Schema, path: &str) -> Result<()> {
    match expr {
        ScopeExpr::And(parts) | ScopeExpr::Or(parts) => {
            for p in parts {
                validate_expr(p, table, schema, path)?;
            }
            Ok(())
        }
        ScopeExpr::Not(inner) => validate_expr(inner, table, schema, path),
        ScopeExpr::Relation { name, inner } => {
            let rel = table.find_relation(name).ok_or_else(|| Error::Validate {
                path: format!("{path}.{name}"),
                message: format!("unknown relation '{name}' on '{}'", table.exposed_name),
            })?;
            let target = schema
                .table(&rel.target_table)
                .ok_or_else(|| Error::Validate {
                    path: format!("{path}.{name}"),
                    message: format!("unknown table '{}'", rel.target_table),
                })?;
            validate_expr(inner, target, schema, &format!("{path}.{name}"))
        }
        ScopeExpr::Compare { column, .. }
        | ScopeExpr::IsNull { column, .. }
        | ScopeExpr::InList { column, .. } => {
            table.find_column(column).map(|_| ()).ok_or_else(|| Error::Validate {
                path: format!("{path}.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::{col, principal, rel};
    use crate::schema::{PgType, Relation, Schema, Table};
    use crate::TableScope;

    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"])
                    .relation("orders", Relation::array("orders").on([("id", "user_id")])),
            )
            .table(
                Table::new("orders", "public", "orders")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .build()
    }

    #[test]
    fn validate_then_bind_produces_scope_set() {
        let policy = ScopePolicy::builder()
            .allow("orders", col("user_id").eq(principal()))
            .unrestricted("users")
            .validate(&schema())
            .unwrap();
        let set = policy.bind_value(7).unwrap();
        assert!(matches!(set.get("orders"), Some(TableScope::Allow(_))));
        assert!(matches!(set.get("users"), Some(TableScope::Unrestricted)));
    }

    #[test]
    fn validate_rejects_unknown_column() {
        let err = ScopePolicy::builder()
            .allow("orders", col("nope").eq(principal()))
            .validate(&schema())
            .unwrap_err();
        assert!(matches!(err, Error::Validate { .. }));
    }

    #[test]
    fn validate_rejects_unknown_relation() {
        let err = ScopePolicy::builder()
            .allow("orders", rel("ghost", col("id").eq(1)))
            .validate(&schema())
            .unwrap_err();
        assert!(matches!(err, Error::Validate { .. }));
    }

    #[test]
    fn validate_rejects_unknown_table() {
        let err = ScopePolicy::builder()
            .unrestricted("ghosts")
            .validate(&schema())
            .unwrap_err();
        assert!(matches!(err, Error::Validate { .. }));
    }

    #[test]
    fn bind_missing_param_errors() {
        let policy = ScopePolicy::builder()
            .allow("orders", col("user_id").eq(crate::predicate::param("tenant")))
            .validate(&schema())
            .unwrap();
        let err = policy.bind_value(7).unwrap_err();
        assert!(matches!(err, Error::Validate { .. }));
    }
}
