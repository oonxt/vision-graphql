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

use crate::ast::{CmpOp, Val};
use crate::error::{Error, Result};
use crate::predicate::{is_param_ref, Operand, Principal, ScopeExpr};
use crate::schema::{PgType, Schema, Table};
use crate::scope::{ColumnScope, ScopeSet};
use crate::types::BindSpec;

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
    columns: HashMap<String, ColumnScope>,
}

/// Builder for [`ScopePolicy`]. Mirrors [`crate::ScopeSet`]'s surface but takes
/// predicate *templates* and must be [`validate`](Self::validate)d.
#[derive(Debug, Clone, Default)]
pub struct ScopePolicyBuilder {
    tables: HashMap<String, ScopeRule>,
    columns: HashMap<String, ColumnScope>,
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
        // Column rules carry no parameters — which columns a role may touch is
        // part of the shape, not of the principal — so they are copied across
        // rather than resolved.
        set = self.apply_columns(set);
        Ok(set)
    }

    fn apply_columns(&self, mut set: ScopeSet) -> ScopeSet {
        for (table, rule) in &self.columns {
            set = match rule {
                ColumnScope::Only(cols) => set.columns(table.clone(), cols.iter().cloned()),
                ColumnScope::Except(cols) => set.hide_columns(table.clone(), cols.iter().cloned()),
            };
        }
        set
    }

    /// A [`ScopeSet`] with every rule lowered but no principal substituted:
    /// parameters stay as [`crate::ast::Val::ScopeParam`] for the request to
    /// fill in.
    ///
    /// This is what [`crate::Engine::compile`] applies, so a compiled statement
    /// carries the policy's predicates while still serving every principal.
    /// Binding a principal at compile time instead would mint one statement per
    /// tenant.
    pub fn symbolic(&self) -> ScopeSet {
        let mut set = ScopeSet::new();
        for (table, rule) in &self.tables {
            set = match rule {
                ScopeRule::Allow(expr) => set.allow(table.clone(), expr.symbolic()),
                ScopeRule::Unrestricted => set.unrestricted(table.clone()),
                ScopeRule::Deny => set.deny(table.clone()),
            };
        }
        // Carried here too, or a query compiled against this policy would be
        // checked for rows and not for columns — which is the path a scoped
        // endpoint is most likely to be using.
        self.apply_columns(set)
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

    /// The rule for `table`, if this policy has one.
    ///
    /// What a host composing two policies reads: the predicate one source
    /// gave a table, to AND another's into it. Without this the two could
    /// only be chosen between, since a policy's rules were otherwise sealed
    /// once validated.
    pub fn rule(&self, table: &str) -> Option<&ScopeRule> {
        self.tables.get(table)
    }

    /// The column rule for `table`, if this policy has one.
    pub fn column_scope(&self, table: &str) -> Option<&ColumnScope> {
        self.columns.get(table)
    }

    /// Reopen a validated policy as a builder carrying every rule it had, to
    /// add to or AND into — a TOML policy loaded at startup and a rule set
    /// derived elsewhere, joined on one table with
    /// [`and_allow`](ScopePolicyBuilder::and_allow). The result must be
    /// validated again: what is added has not been checked.
    pub fn into_builder(self) -> ScopePolicyBuilder {
        ScopePolicyBuilder {
            tables: self.tables,
            columns: self.columns,
        }
    }

    /// Every parameter name this policy's rules reference, spelled as it
    /// resolves (`"principal"`, `"claim.school_id"`), sorted.
    ///
    /// What [`bind`](Self::bind) will look up on the principal — so a host can
    /// check a policy against the parameters it binds once, when the policy is
    /// built, rather than discover a reference it never binds on the first
    /// request that fails closed. A dotted name is satisfied by a parameter
    /// bound under its first segment (`claim.school_id` by an object bound as
    /// `claim`) or under the whole name verbatim, nothing in between — that is
    /// [`Principal::get`]'s rule, so compare the segment before the first dot
    /// or the whole name. For the same question asked of TOML text before
    /// there is a schema to validate it against, see
    /// [`referenced_params`](crate::scope_config::referenced_params).
    pub fn params(&self) -> std::collections::BTreeSet<String> {
        let mut out = std::collections::BTreeSet::new();
        for rule in self.tables.values() {
            if let ScopeRule::Allow(expr) = rule {
                expr.collect_params(&mut out);
            }
        }
        out
    }
}

impl ScopePolicyBuilder {
    /// Allow `table` under `expr`, replacing any rule the table had. Two
    /// `allow`s on one table keep the second; to require both, see
    /// [`and_allow`](Self::and_allow).
    pub fn allow(mut self, table: impl Into<String>, expr: ScopeExpr) -> Self {
        self.tables.insert(table.into(), ScopeRule::Allow(expr));
        self
    }

    /// Allow `table` under `expr` *and* whatever rule it already has.
    ///
    /// Two sources of policy on one table — a resource's TOML and an
    /// authorization model's rules, say — must both be satisfied, and neither
    /// should have to know the other's predicate. The rule the table had is
    /// read as the truth value it stands for: an existing `allow` becomes
    /// `and([existing, expr])` (flattened into an existing conjunction);
    /// `unrestricted` is `TRUE`, so the result is `expr` alone; `deny` is
    /// `FALSE`, so it stays `deny`.
    ///
    /// A table with no rule yet gets `expr`. In a finished policy an absent
    /// table is denied, but in a builder it is a rule not yet written, and
    /// the second source is as entitled to write it as the first was — so
    /// this is the one case where `and_allow` admits what was not admitted
    /// before. A source that means to refuse a table says `deny`, which
    /// nothing composed on top of it can undo.
    pub fn and_allow(mut self, table: impl Into<String>, expr: ScopeExpr) -> Self {
        let table = table.into();
        let rule = match self.tables.remove(&table) {
            None | Some(ScopeRule::Unrestricted) => ScopeRule::Allow(expr),
            Some(ScopeRule::Allow(ScopeExpr::And(mut parts))) => {
                parts.push(expr);
                ScopeRule::Allow(ScopeExpr::And(parts))
            }
            Some(ScopeRule::Allow(existing)) => {
                ScopeRule::Allow(ScopeExpr::And(vec![existing, expr]))
            }
            Some(ScopeRule::Deny) => ScopeRule::Deny,
        };
        self.tables.insert(table, rule);
        self
    }

    /// Allow `table` with no predicate.
    pub fn unrestricted(mut self, table: impl Into<String>) -> Self {
        self.tables.insert(table.into(), ScopeRule::Unrestricted);
        self
    }

    /// Restrict `table` to these columns and no others. See [`ColumnScope`].
    ///
    /// Replaces any previous rule for the same table — the two forms are
    /// alternative spellings of one rule, not layers.
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

    /// Withhold these columns of `table`, admitting the rest. Replaces any
    /// previous rule for the same table; see [`ScopePolicyBuilder::columns`].
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
        // Column names are validated too: a typo in an allowlist is the
        // dangerous direction — `columns(["naem"])` would withhold `name` and
        // look like it was doing its job — and in a denylist it is worse, since
        // `hide_columns(["passwrod_hash"])` hides nothing at all.
        for (table_name, rule) in &self.columns {
            let table = schema.table(table_name).ok_or_else(|| Error::Validate {
                path: format!("scope.{table_name}"),
                message: format!("unknown table '{table_name}'"),
            })?;
            let named = match rule {
                ColumnScope::Only(cols) | ColumnScope::Except(cols) => cols,
            };
            for column in named {
                if table.find_column(column).is_none() {
                    return Err(Error::Validate {
                        path: format!("scope.{table_name}.columns"),
                        message: format!("unknown column '{column}' on '{}'", table.exposed_name),
                    });
                }
            }
        }
        Ok(ScopePolicy {
            tables: self.tables,
            columns: self.columns,
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
        | ScopeExpr::InList { column, .. }
        | ScopeExpr::InSet { column, .. } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("{path}.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            let path = format!("{path}.{column}");
            let subject = format!("'{}'", col.exposed_name);
            match expr {
                ScopeExpr::Compare { op, value, .. } => {
                    crate::type_system::check_cmp(*op, &col.pg_type, || path.clone(), &subject)?;
                    validate_scalar(value, &col.pg_type, &path)
                }
                ScopeExpr::InList { values, .. } => {
                    crate::type_system::check_cmp(
                        CmpOp::Eq,
                        &col.pg_type,
                        || path.clone(),
                        &subject,
                    )?;
                    values
                        .iter()
                        .try_for_each(|v| validate_scalar(v, &col.pg_type, &path))
                }
                ScopeExpr::InSet { set, .. } => {
                    crate::type_system::check_cmp(
                        CmpOp::Eq,
                        &col.pg_type,
                        || path.clone(),
                        &subject,
                    )?;
                    validate_list(set, &col.pg_type, &path)
                }
                _ => Ok(()),
            }
        }
        ScopeExpr::Const(_) => Ok(()),
        // No column to look up; the type is the leaf's own. The checks are
        // the renderer's, run once here instead of on the first request.
        ScopeExpr::ValueCompare {
            left,
            op,
            right,
            ty,
        } => {
            let path = leaf_path(path, [left, right]);
            crate::type_system::check_cmp(
                *op,
                ty,
                || path.clone(),
                &crate::sql::value_subject(ty),
            )?;
            validate_scalar(left, ty, &path)?;
            validate_scalar(right, ty, &path)
        }
        ScopeExpr::ValueInSet { value, set, ty, .. } => {
            let path = leaf_path(path, [value, set]);
            crate::type_system::check_cmp(
                CmpOp::Eq,
                ty,
                || path.clone(),
                &crate::sql::value_subject(ty),
            )?;
            validate_scalar(value, ty, &path)?;
            validate_list(set, ty, &path)
        }
    }
}

/// The path a column-less leaf reports under, named for its parameter where
/// it has one — the same rule the renderer applies, so a policy-time and a
/// request-time refusal of the same leaf read alike.
fn leaf_path<'a>(path: &str, operands: impl IntoIterator<Item = &'a Operand>) -> String {
    let symbolic: Vec<Val> = operands.into_iter().map(Operand::symbolic).collect();
    let leaf = crate::ast::value_leaf_path(&symbolic);
    format!("{path}.{}", leaf.trim_start_matches("where."))
}

/// A name the grammar refuses can never be looked up, so failing it here —
/// once, on the policy — beats failing it per request with a message that
/// blames the principal.
fn validate_param_ref(operand: &Operand, path: &str) -> Result<()> {
    if let Operand::Param(name) = operand {
        if !is_param_ref(name) {
            return Err(Error::Validate {
                path: path.to_string(),
                message: format!(
                    "'{name}' is not a parameter reference; expected \
                     `name` or `name.field` (identifiers: \
                     [A-Za-z_][A-Za-z0-9_]*)"
                ),
            });
        }
    }
    Ok(())
}

/// An operand in a comparison position, checked as the renderer will check
/// it: a parameter must be well-formed, and a literal must bind as `ty` and
/// not be null. Going through [`BindSpec`] rather than restating its rules
/// keeps `validate` and rendering one judgement; a parameter comes back as a
/// deferred spec, which is exactly "checked per request".
fn validate_scalar(operand: &Operand, ty: &PgType, path: &str) -> Result<()> {
    validate_param_ref(operand, path)?;
    BindSpec::comparison(operand.symbolic(), ty, || path.to_string()).map(drop)
}

/// An operand standing for a whole list; see [`validate_scalar`].
fn validate_list(operand: &Operand, ty: &PgType, path: &str) -> Result<()> {
    validate_param_ref(operand, path)?;
    BindSpec::array(operand.symbolic(), ty, || path.to_string()).map(drop)
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
                    .column("meta", "meta", PgType::Json, true)
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
    fn validate_rejects_malformed_param_name() {
        // The DSL takes any string; the grammar is enforced where the TOML
        // loader enforces it too, so both entry points agree.
        for bad in ["claim.", "a..b", "tenant id", "tenant-id"] {
            let err = ScopePolicy::builder()
                .allow("orders", col("user_id").eq(crate::predicate::param(bad)))
                .validate(&schema())
                .unwrap_err();
            assert!(
                matches!(&err, Error::Validate { path, message }
                    if path == "scope.orders.user_id" && message.contains(bad)),
                "{bad}: {err:?}"
            );
        }
        // Inside `_in` lists as well.
        let err = ScopePolicy::builder()
            .allow(
                "orders",
                col("user_id").in_([
                    crate::predicate::param("ok"),
                    crate::predicate::param("not ok"),
                ]),
            )
            .validate(&schema())
            .unwrap_err();
        assert!(matches!(err, Error::Validate { .. }), "{err:?}");
        // Dotted references are well-formed.
        ScopePolicy::builder()
            .allow(
                "orders",
                col("user_id").eq(crate::predicate::param("claim.user_id")),
            )
            .validate(&schema())
            .unwrap();
    }

    #[test]
    fn params_lists_every_reference_once() {
        let policy = ScopePolicy::builder()
            .allow(
                "orders",
                crate::predicate::and([
                    col("user_id").eq(crate::predicate::param("claim.user_id")),
                    rel(
                        "user",
                        col("id").in_([principal(), crate::predicate::param("other")]),
                    ),
                    col("id").is_not_null(),
                ]),
            )
            .allow("users", col("id").eq(principal()))
            .validate(&schema())
            .unwrap();
        assert_eq!(
            policy.params().into_iter().collect::<Vec<_>>(),
            vec!["claim.user_id", "other", "principal"]
        );
        // Unrestricted and deny rules reference nothing.
        let policy = ScopePolicy::builder()
            .unrestricted("users")
            .validate(&schema())
            .unwrap();
        assert!(policy.params().is_empty());
    }

    #[test]
    fn bind_missing_param_errors() {
        let policy = ScopePolicy::builder()
            .allow(
                "orders",
                col("user_id").eq(crate::predicate::param("tenant")),
            )
            .validate(&schema())
            .unwrap();
        let err = policy.bind_value(7).unwrap_err();
        assert!(matches!(err, Error::Validate { .. }));
    }

    #[test]
    fn and_allow_requires_both_and_deny_absorbs() {
        use crate::predicate::and;
        let owner = || col("user_id").eq(principal());
        let extra = || col("id").gt(0);
        // Allow ∧ expr → And of the two; a third joins the same conjunction
        // rather than nesting, so a rule set added one at a time stays flat.
        let b = ScopePolicy::builder()
            .allow("orders", owner())
            .and_allow("orders", extra())
            .and_allow("orders", extra());
        assert!(matches!(
            b.tables.get("orders"),
            Some(ScopeRule::Allow(ScopeExpr::And(parts))) if parts.len() == 3
        ));
        // Unrestricted ∧ expr → expr; nothing ∧ expr → expr.
        let b = ScopePolicy::builder()
            .unrestricted("orders")
            .and_allow("orders", extra())
            .and_allow("users", extra());
        assert!(matches!(
            b.tables.get("orders"),
            Some(ScopeRule::Allow(ScopeExpr::Compare { .. }))
        ));
        assert!(matches!(
            b.tables.get("users"),
            Some(ScopeRule::Allow(ScopeExpr::Compare { .. }))
        ));
        // Deny ∧ expr → deny.
        let b = ScopePolicy::builder()
            .deny("orders")
            .and_allow("orders", extra());
        assert!(matches!(b.tables.get("orders"), Some(ScopeRule::Deny)));
        // A plain `allow` still replaces, so the two are distinguishable.
        let b = ScopePolicy::builder()
            .allow("orders", and([owner(), extra()]))
            .allow("orders", extra());
        assert!(matches!(
            b.tables.get("orders"),
            Some(ScopeRule::Allow(ScopeExpr::Compare { .. }))
        ));
    }

    #[test]
    fn a_validated_policy_reopens_with_its_rules_and_columns() {
        let policy = ScopePolicy::builder()
            .allow("orders", col("user_id").eq(principal()))
            .columns("orders", ["id"])
            .unrestricted("users")
            .validate(&schema())
            .unwrap();
        assert!(matches!(policy.rule("orders"), Some(ScopeRule::Allow(_))));
        assert!(matches!(
            policy.rule("users"),
            Some(ScopeRule::Unrestricted)
        ));
        assert!(policy.rule("ghosts").is_none());
        assert!(matches!(
            policy.column_scope("orders"),
            Some(ColumnScope::Only(_))
        ));
        let again = policy
            .clone()
            .into_builder()
            .and_allow("orders", col("id").gt(0))
            .validate(&schema())
            .unwrap();
        assert!(matches!(
            again.rule("orders"),
            Some(ScopeRule::Allow(ScopeExpr::And(parts))) if parts.len() == 2
        ));
        assert!(matches!(again.rule("users"), Some(ScopeRule::Unrestricted)));
        assert!(matches!(
            again.column_scope("orders"),
            Some(ColumnScope::Only(_))
        ));
        // What was added is validated like anything else.
        let err = policy
            .into_builder()
            .and_allow("orders", col("nope").eq(1))
            .validate(&schema())
            .unwrap_err();
        assert!(matches!(err, Error::Validate { .. }));
    }

    #[test]
    fn typed_leaves_are_checked_when_the_policy_is_built() {
        use crate::predicate::{constant, param, typed};
        let ok = ScopePolicy::builder()
            .allow(
                "orders",
                crate::predicate::or([
                    typed(param("role"), PgType::Text).eq("admin"),
                    typed("vip", PgType::Text).in_set(param("tiers")),
                    typed(param("level"), PgType::Int4).gte(3),
                    col("user_id").in_set(param("ids")),
                    constant(false),
                ]),
            )
            .validate(&schema())
            .unwrap();
        assert_eq!(
            ok.params().into_iter().collect::<Vec<_>>(),
            ["ids", "level", "role", "tiers"]
        );
        let refused = [
            // An operator the type does not have.
            typed(param("level"), PgType::Int4).like("adm%"),
            // json has no equality; jsonb would.
            typed(param("claims"), PgType::Json).eq(serde_json::json!({"admin": true})),
            typed(param("claims"), PgType::Json).in_set(serde_json::json!([])),
            col("meta").eq(serde_json::json!({})),
            col("meta").in_set(param("metas")),
            // A column-side literal set that is not a list, or is mistyped.
            col("user_id").in_set("seven"),
            col("user_id").in_set(serde_json::json!(["seven"])),
            // A column-side literal that does not bind as the column's type.
            col("user_id").eq("seven"),
            col("user_id").in_(["seven"]),
            // A literal that does not bind as the type.
            typed(param("level"), PgType::Int4).eq("three"),
            // A null: comparing against it matches nothing.
            typed(param("role"), PgType::Text).eq(serde_json::Value::Null),
            // A parameter name the grammar refuses.
            typed(param("ro le"), PgType::Text).eq("admin"),
            // A literal set with a member of the wrong type.
            typed("vip", PgType::Text).in_set(serde_json::json!(["vip", 1])),
            // A literal "set" that is not a list.
            typed("vip", PgType::Text).in_set("vip"),
            col("user_id").in_set(param("not ok")),
        ];
        for expr in refused {
            let err = ScopePolicy::builder()
                .allow("orders", expr.clone())
                .validate(&schema())
                .unwrap_err();
            assert!(matches!(err, Error::Validate { .. }), "{expr:?}: {err:?}");
        }
        // A refusal names the parameter of the leaf it is about, as the
        // renderer's would, so the two read alike.
        let err = ScopePolicy::builder()
            .allow("orders", typed(param("level"), PgType::Int4).eq("three"))
            .validate(&schema())
            .unwrap_err();
        assert!(
            matches!(&err, Error::Validate { path, .. } if path == "scope.orders.$level"),
            "{err:?}"
        );
        // jsonb keeps its equality.
        ScopePolicy::builder()
            .allow(
                "orders",
                typed(param("claims"), PgType::Jsonb).eq(serde_json::json!({"admin": true})),
            )
            .validate(&schema())
            .unwrap();
    }
}
