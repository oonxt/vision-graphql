//! Declarative TOML scope policy.
//!
//! Mirrors the schema [`config`](crate::schema::config) overlay: a policy is a
//! table of per-table rules. Each table sets exactly one of `where` (a predicate
//! using the same object syntax as a query `where`), `unrestricted = true`, or
//! `deny = true`. In a `where` value position, a string `"$name"` is a parameter
//! reference resolved at bind time (`"$principal"` is the conventional default);
//! `$$` escapes a literal leading `$`.
//!
//! ```toml
//! [tables.users]
//! where = { id = { _eq = "$principal" } }
//!
//! [tables.orders]
//! where = { user_id = { _eq = "$principal" } }
//!
//! [tables.samples]
//! where = { order = { user_id = { _eq = "$principal" } } }   # relation chain
//!
//! [tables.adverts]
//! unrestricted = true
//!
//! [tables.secrets]
//! deny = true
//! ```
//!
//! The result is an ordinary [`ScopePolicy`], validated against the schema and
//! bound per request exactly like a programmatically built one.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use crate::ast::BoolExpr;
use crate::error::{Error, Result};
use crate::parser::lower_where;
use crate::policy::{ScopePolicy, ScopeRule};
use crate::predicate::{Operand, ScopeExpr};
use crate::schema::Schema;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeConfig {
    #[serde(default)]
    tables: BTreeMap<String, TableRule>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableRule {
    #[serde(default, rename = "where")]
    where_: Option<toml::Value>,
    #[serde(default)]
    unrestricted: bool,
    #[serde(default)]
    deny: bool,
}

impl ScopePolicy {
    /// Parse and validate a TOML scope policy against `schema`. See the
    /// [`scope_config`](crate::scope_config) module docs for the format.
    pub fn from_toml(source: &str, schema: &Schema) -> Result<ScopePolicy> {
        parse(source, schema)
    }
}

/// Parse and validate a TOML scope policy against `schema`.
pub fn parse(source: &str, schema: &Schema) -> Result<ScopePolicy> {
    let cfg: ScopeConfig =
        toml::from_str(source).map_err(|e| Error::Scope(format!("TOML parse error: {e}")))?;
    let mut builder = ScopePolicy::builder();
    for (table_name, tr) in &cfg.tables {
        builder = builder.rule(table_name.clone(), build_rule(table_name, tr, schema)?);
    }
    builder.validate(schema)
}

fn build_rule(table_name: &str, tr: &TableRule, schema: &Schema) -> Result<ScopeRule> {
    let set = tr.where_.is_some() as u8 + tr.unrestricted as u8 + tr.deny as u8;
    if set != 1 {
        return Err(Error::Scope(format!(
            "tables.{table_name}: set exactly one of 'where', 'unrestricted', 'deny'"
        )));
    }
    if tr.unrestricted {
        return Ok(ScopeRule::Unrestricted);
    }
    if tr.deny {
        return Ok(ScopeRule::Deny);
    }
    let where_toml = tr.where_.as_ref().expect("checked exactly-one above");
    let table = schema.table(table_name).ok_or_else(|| {
        Error::Scope(format!("tables.{table_name}: unknown table '{table_name}'"))
    })?;
    let json = toml_to_json(where_toml);
    let lowered = lower_where(&json, table, schema, &format!("scope.{table_name}.where"))?;
    Ok(ScopeRule::Allow(to_template(lowered)))
}

/// Convert a `toml::Value` into the `serde_json::Value` shape `lower_where`
/// expects. TOML tables/arrays/scalars map one-to-one; datetimes stringify.
fn toml_to_json(v: &toml::Value) -> Value {
    match v {
        toml::Value::String(s) => Value::String(s.clone()),
        toml::Value::Integer(i) => Value::from(*i),
        toml::Value::Float(f) => Value::from(*f),
        toml::Value::Boolean(b) => Value::from(*b),
        toml::Value::Datetime(d) => Value::String(d.to_string()),
        toml::Value::Array(a) => Value::Array(a.iter().map(toml_to_json).collect()),
        toml::Value::Table(t) => {
            Value::Object(t.iter().map(|(k, v)| (k.clone(), toml_to_json(v))).collect())
        }
    }
}

/// Lift a lowered `BoolExpr` (whose value leaves are concrete JSON) into a
/// `ScopeExpr` template, rewriting `"$name"` string leaves to parameters.
fn to_template(expr: BoolExpr) -> ScopeExpr {
    match expr {
        BoolExpr::And(parts) => ScopeExpr::And(parts.into_iter().map(to_template).collect()),
        BoolExpr::Or(parts) => ScopeExpr::Or(parts.into_iter().map(to_template).collect()),
        BoolExpr::Not(inner) => ScopeExpr::Not(Box::new(to_template(*inner))),
        BoolExpr::Relation { name, inner } => ScopeExpr::Relation {
            name,
            inner: Box::new(to_template(*inner)),
        },
        BoolExpr::Compare { column, op, value } => ScopeExpr::Compare {
            column,
            op,
            value: to_operand(value),
        },
        BoolExpr::IsNull { column, negated } => ScopeExpr::IsNull { column, negated },
        BoolExpr::InList {
            column,
            values,
            negated,
        } => ScopeExpr::InList {
            column,
            values: values.into_iter().map(to_operand).collect(),
            negated,
        },
    }
}

/// Map one lowered value leaf to an [`Operand`]: `"$name"` → a parameter, `$$…`
/// → an unescaped literal, anything else → the literal itself.
fn to_operand(v: Value) -> Operand {
    if let Value::String(s) = &v {
        if let Some(rest) = s.strip_prefix("$$") {
            return Operand::Lit(Value::String(format!("${rest}")));
        }
        if let Some(name) = s.strip_prefix('$') {
            if is_ident(name) {
                return Operand::Param(name.to_string());
            }
        }
    }
    Operand::Lit(v)
}

/// `[A-Za-z_][A-Za-z0-9_]*`.
fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::Principal;
    use crate::schema::{PgType, Relation, Table};

    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("orders", "public", "orders")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .table(
                Table::new("adverts", "public", "adverts")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build()
    }

    #[test]
    fn parses_where_unrestricted_and_deny() {
        let toml = r#"
            [tables.orders]
            where = { user_id = { _eq = "$principal" } }

            [tables.adverts]
            unrestricted = true

            [tables.users]
            deny = true
        "#;
        let policy = parse(toml, &schema()).unwrap();
        // Binds the principal placeholder to a concrete value.
        let set = policy.bind_value(7).unwrap();
        let crate::TableScope::Allow(expr) = set.get("orders").unwrap() else {
            panic!("orders should be Allow");
        };
        let BoolExpr::Compare { column, value, .. } = expr else {
            panic!("expected compare, got {expr:?}");
        };
        assert_eq!(column, "user_id");
        assert_eq!(*value, serde_json::json!(7));
        assert!(matches!(
            set.get("adverts"),
            Some(crate::TableScope::Unrestricted)
        ));
        assert!(matches!(set.get("users"), Some(crate::TableScope::Deny)));
    }

    #[test]
    fn relation_chain_and_named_param() {
        let toml = r#"
            [tables.orders]
            where = { user = { id = { _eq = "$account" } } }
        "#;
        let policy = parse(toml, &schema()).unwrap();
        let set = policy
            .bind(&Principal::new().set("account", 99))
            .unwrap();
        let crate::TableScope::Allow(BoolExpr::Relation { inner, .. }) = set.get("orders").unwrap()
        else {
            panic!("expected relation");
        };
        let BoolExpr::Compare { value, .. } = inner.as_ref() else {
            panic!("expected compare");
        };
        assert_eq!(*value, serde_json::json!(99));
    }

    #[test]
    fn dollar_escape_is_literal() {
        let toml = r#"
            [tables.orders]
            where = { title = { _eq = "$$literal" } }
        "#;
        let policy = parse(toml, &schema()).unwrap();
        // No params needed: the value is a literal "$literal".
        let set = policy.bind(&Principal::new()).unwrap();
        let crate::TableScope::Allow(BoolExpr::Compare { value, .. }) = set.get("orders").unwrap()
        else {
            panic!("expected compare");
        };
        assert_eq!(*value, serde_json::json!("$literal"));
    }

    #[test]
    fn requires_exactly_one_rule() {
        let toml = r#"
            [tables.orders]
            where = { user_id = { _eq = "$principal" } }
            deny = true
        "#;
        let err = parse(toml, &schema()).unwrap_err();
        assert!(matches!(err, Error::Scope(_)));
    }

    #[test]
    fn unknown_column_rejected_at_parse() {
        let toml = r#"
            [tables.orders]
            where = { nope = { _eq = "$principal" } }
        "#;
        let err = parse(toml, &schema()).unwrap_err();
        assert!(matches!(err, Error::Validate { .. }));
    }
}
