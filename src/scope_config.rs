//! Declarative TOML scope policy.
//!
//! Mirrors the schema [`config`](crate::schema::config) overlay: a policy is a
//! table of per-table rules. Each table sets exactly one of `where` (a predicate
//! using the same object syntax as a query `where`), `unrestricted = true`, or
//! `deny = true`. In a `where` value position, a string `"$name"` is a parameter
//! reference resolved at bind time (`"$principal"` is the conventional default),
//! and `"$name.field"` reads a field of an object-valued parameter, any depth
//! down. `$$` escapes a literal leading `$`. Any other string starting with `$`
//! is refused when the policy loads — it would otherwise be a literal that
//! matches nothing, which reads as a policy that works.
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
//! [tables.courses]
//! where = { school_id = { _eq = "$claim.school_id" } }       # field of an object parameter
//!
//! [tables.adverts]
//! unrestricted = true
//!
//! [tables.secrets]
//! deny = true
//! ```
//!
//! The result is an ordinary [`ScopePolicy`], validated against the schema and
//! bound per request exactly like a programmatically built one. The query
//! `where` syntax has no spelling for a column-less condition
//! ([`typed`](crate::predicate::typed)), a constant
//! ([`constant`](crate::predicate::constant)) or a whole list as one parameter
//! ([`in_set`](crate::predicate::Col::in_set)); a policy that needs them is
//! built in code, or loaded here and extended with
//! [`ScopePolicy::into_builder`].
//!
//! [`referenced_params`] reads the same text without a schema and reports which
//! parameter names it references, for a host that wants to check a policy
//! against the parameters it will bind before it has a schema to validate the
//! policy against.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;

use crate::ast::{BoolExpr, Val};
use crate::error::{Error, Result};
use crate::parser::{lower_where, Names};
use crate::policy::{ScopePolicy, ScopeRule};
use crate::predicate::{is_param_ref, Operand, ScopeExpr};
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
    let cfg = read_config(source)?;
    let mut builder = ScopePolicy::builder();
    for (table_name, tr) in &cfg.tables {
        builder = builder.rule(table_name.clone(), build_rule(table_name, tr, schema)?);
    }
    builder.validate(schema)
}

fn read_config(source: &str) -> Result<ScopeConfig> {
    toml::from_str(source).map_err(|e| Error::Scope(format!("TOML parse error: {e}")))
}

/// The `where` of a table rule, or `None` for `unrestricted` / `deny` — after
/// checking that exactly one of the three is set.
fn where_of<'a>(table_name: &str, tr: &'a TableRule) -> Result<Option<&'a toml::Value>> {
    let set = tr.where_.is_some() as u8 + tr.unrestricted as u8 + tr.deny as u8;
    if set != 1 {
        return Err(Error::Scope(format!(
            "tables.{table_name}: set exactly one of 'where', 'unrestricted', 'deny'"
        )));
    }
    Ok(tr.where_.as_ref())
}

fn build_rule(table_name: &str, tr: &TableRule, schema: &Schema) -> Result<ScopeRule> {
    let Some(where_toml) = where_of(table_name, tr)? else {
        return Ok(if tr.deny {
            ScopeRule::Deny
        } else {
            ScopeRule::Unrestricted
        });
    };
    let table = schema.table(table_name).ok_or_else(|| {
        Error::Scope(format!("tables.{table_name}: unknown table '{table_name}'"))
    })?;
    let lowered = lower_rule(where_toml, Names::Schema { table, schema }, table_name)?;
    Ok(ScopeRule::Allow(lowered))
}

/// One table's `where`, through the query `where` lowering and into a
/// template. The TOML has no GraphQL variables in it, so the binding mode is
/// moot; what varies is where names resolve — see [`Names`].
fn lower_rule(where_toml: &toml::Value, names: Names<'_>, table_name: &str) -> Result<ScopeExpr> {
    let json = toml_to_json(where_toml);
    let path = format!("scope.{table_name}.where");
    let lowered = lower_where(
        &crate::parser::json_to_gql(&json),
        names,
        crate::parser::Bindings::eager(&Value::Null),
        &path,
    )?;
    to_template(lowered, &path)
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
        toml::Value::Table(t) => Value::Object(
            t.iter()
                .map(|(k, v)| (k.clone(), toml_to_json(v)))
                .collect(),
        ),
    }
}

/// Lift a lowered `BoolExpr` (whose value leaves are concrete JSON) into a
/// `ScopeExpr` template, rewriting `"$name"` string leaves to parameters.
fn to_template(expr: BoolExpr, path: &str) -> Result<ScopeExpr> {
    Ok(match expr {
        BoolExpr::And(parts) => ScopeExpr::And(
            parts
                .into_iter()
                .map(|p| to_template(p, path))
                .collect::<Result<_>>()?,
        ),
        BoolExpr::Or(parts) => ScopeExpr::Or(
            parts
                .into_iter()
                .map(|p| to_template(p, path))
                .collect::<Result<_>>()?,
        ),
        BoolExpr::Not(inner) => ScopeExpr::Not(Box::new(to_template(*inner, path)?)),
        BoolExpr::Relation { name, inner } => {
            let inner = Box::new(to_template(*inner, &format!("{path}.{name}"))?);
            ScopeExpr::Relation { name, inner }
        }
        BoolExpr::Compare { column, op, value } => {
            let value = to_operand(value, &format!("{path}.{column}"))?;
            ScopeExpr::Compare { column, op, value }
        }
        // A policy's `_is_null` is a literal: a parameter there would make the
        // predicate's shape depend on the principal, which a template cannot
        // hold. The lowering has already refused a `"$name"` string here.
        BoolExpr::IsNull { column, is_null } => match is_null.as_lit().and_then(Value::as_bool) {
            Some(b) => ScopeExpr::IsNull {
                column,
                negated: !b,
            },
            None => {
                return Err(Error::Scope(format!(
                    "{path}.{column}._is_null: expected true or false"
                )))
            }
        },
        // Only an `@optional` GraphQL variable produces one, and a TOML policy
        // has no variables.
        BoolExpr::Optional(_) => {
            return Err(Error::Scope(format!(
                "{path}: internal: optional comparison in a TOML policy"
            )))
        }
        // The `where` lowering never produces these: a document has no
        // spelling for a constant or a column-less comparison.
        BoolExpr::Const(_) | BoolExpr::ValueCompare { .. } | BoolExpr::ValueInList { .. } => {
            return Err(Error::Scope(format!(
                "{path}: internal: column-less predicate in a TOML policy"
            )))
        }
        BoolExpr::InList {
            column,
            values,
            negated,
        } => {
            let values = val_to_operands(values, &format!("{path}.{column}"))?;
            ScopeExpr::InList {
                column,
                values,
                negated,
            }
        }
    })
}

/// Map a lowered `_in` list to operands. Lowering has already refused a
/// non-list here.
fn val_to_operands(v: Val, path: &str) -> Result<Vec<Operand>> {
    match v {
        Val::Lit(Value::Array(items)) => items
            .into_iter()
            .map(|i| to_operand(Val::Lit(i), path))
            .collect(),
        other => Err(non_literal(path, &other)),
    }
}

/// Map one lowered value leaf to an [`Operand`]: `"$name"` / `"$name.field"` →
/// a parameter, `$$…` → an unescaped literal, anything else → the literal
/// itself.
///
/// A `$` that is neither is an error, not a literal. Letting it through as the
/// string it is spelled with was how `"$claim.school_id"` once became
/// `school_id = '$claim.school_id'`: a predicate that matches no row, behind a
/// 200 — the policy looked like it was working. The same goes for a mistyped
/// name. Someone who wants a literal starting with `$` has `$$`.
fn to_operand(v: Val, path: &str) -> Result<Operand> {
    let Val::Lit(v) = v else {
        return Err(non_literal(path, &v));
    };
    if let Value::String(s) = &v {
        if let Some(name) = param_ref(s, path)? {
            return Ok(Operand::Param(name.to_string()));
        }
    }
    // A string that is not a reference is a literal, `$$` unescaped. A
    // json/jsonb column takes an object or array literal, whose strings get
    // the same treatment: `$$` unescapes, and a `$…` is refused — an operand
    // is one literal or one parameter, so a reference inside a composite has
    // nothing to resolve it and would otherwise ship as text.
    Ok(Operand::Lit(unescape_composite(v, path)?))
}

/// The parameter a string in an operand position references, or `None` for a
/// literal (a plain string, or `$$…`). The one reading of a leading `$`.
fn param_ref<'a>(s: &'a str, path: &str) -> Result<Option<&'a str>> {
    if s.starts_with("$$") {
        return Ok(None);
    }
    let Some(name) = s.strip_prefix('$') else {
        return Ok(None);
    };
    if is_param_ref(name) {
        return Ok(Some(name));
    }
    Err(Error::Scope(format!(
        "{path}: '{s}' is not a parameter reference; expected `$name` or \
         `$name.field` (identifiers: [A-Za-z_][A-Za-z0-9_]*), or `$$` for \
         a literal '$'"
    )))
}

fn unescape_composite(v: Value, path: &str) -> Result<Value> {
    Ok(match v {
        Value::String(s) => {
            if let Some(rest) = s.strip_prefix("$$") {
                Value::String(format!("${rest}"))
            } else if s.starts_with('$') {
                return Err(Error::Scope(format!(
                    "{path}: '{s}' inside an object or array literal: a parameter \
                     reference cannot be part of a literal; use `$$` for a literal '$'"
                )));
            } else {
                Value::String(s)
            }
        }
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|i| unescape_composite(i, path))
                .collect::<Result<_>>()?,
        ),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, i)| Ok((k, unescape_composite(i, path)?)))
                .collect::<Result<_>>()?,
        ),
        other => other,
    })
}

/// Lowering a TOML predicate under eager bindings yields literals only — there
/// are no variables to leave symbolic and no principal yet. Should something
/// else arrive, refusing beats inventing a value: an empty `_nin` list renders
/// as `TRUE`, which would make the rule an unrestricted table.
fn non_literal(path: &str, v: &Val) -> Error {
    Error::Scope(format!(
        "{path}: internal: non-literal value {v:?} in a TOML policy"
    ))
}

/// The parameter names a TOML policy references, without a schema.
///
/// Returns every distinct `$name` / `$name.field` reference in `source`, spelled
/// as it resolves (`"claim.school_id"`), in sorted order. The reading is the
/// loader's own: the same lowering [`ScopePolicy::from_toml`] runs, with the
/// keys of each `where` resolved by syntax instead of against a schema (a key
/// whose value is an object of comparison operators is a column, any other
/// object is a relation). Everything that needs no schema is checked as the
/// loader checks it — the reference grammar, one rule per table, known
/// operators, booleans under `_is_null`, lists under `_in` — with the same
/// errors. What is *not* checked is what needs the schema: whether a table,
/// column or relation exists, or whether an operator applies to a column's
/// type. So a policy that fails here fails `from_toml` too, and a policy that
/// passes here can still fail there on a schema check.
///
/// This exists for a host that validates a policy when it is saved, before it
/// has a schema to validate against, and wants to refuse a reference it will
/// never bind (`$claim.schools_id` for a project that declares `school_id`). A
/// reference like that is well-formed, so [`ScopePolicy::from_toml`] accepts
/// it, and only the host knows the set it binds. Without this the host would
/// have to parse the policy itself and carry its own copy of the reference
/// grammar — which is how a policy comes to pass the host's check and mean
/// something else to the engine.
///
/// A dotted name is satisfied by a parameter bound under its first segment
/// (`claim.school_id` by an object bound as `claim`), or under the whole
/// dotted name verbatim; nothing in between. That is [`Principal::get`]'s
/// rule, so a host checking these names against the set it binds should
/// compare the segment before the first dot, or the whole name.
///
/// [`Principal::get`]: crate::predicate::Principal::get
pub fn referenced_params(source: &str) -> Result<BTreeSet<String>> {
    let cfg = read_config(source)?;
    let mut out = BTreeSet::new();
    for (table_name, tr) in &cfg.tables {
        if let Some(where_toml) = where_of(table_name, tr)? {
            lower_rule(where_toml, Names::Syntactic, table_name)?.collect_params(&mut out);
        }
    }
    Ok(out)
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
        let set = policy.bind(&Principal::new().set("account", 99)).unwrap();
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
    fn dotted_reference_reads_a_field_of_an_object_param() {
        let toml = r#"
            [tables.orders]
            where = { user_id = { _eq = "$claim.user_id" } }
        "#;
        let policy = parse(toml, &schema()).unwrap();
        let claim = serde_json::json!({"user_id": 5, "role": "staff"});
        let set = policy.bind(&Principal::new().set("claim", claim)).unwrap();
        let crate::TableScope::Allow(BoolExpr::Compare { value, .. }) = set.get("orders").unwrap()
        else {
            panic!("expected compare");
        };
        assert_eq!(*value, serde_json::json!(5));

        // The field missing from the bound object is a bind error, not a null.
        let err = policy
            .bind(&Principal::new().set("claim", serde_json::json!({"role": "staff"})))
            .unwrap_err();
        assert!(matches!(err, Error::Validate { .. }), "{err:?}");
    }

    #[test]
    fn malformed_reference_is_refused_at_parse_not_matched_as_text() {
        // Each of these used to load as the literal string it is spelled with,
        // producing a predicate that matches nothing and looks like it works.
        for bad in [
            "$",
            "$1st",
            "$claim.",
            "$.school_id",
            "$claim..school_id",
            "$claim school",
            "$claim-id",
            "${claim}",
        ] {
            let toml = format!(
                r#"
                    [tables.orders]
                    where = {{ title = {{ _eq = "{bad}" }} }}
                "#
            );
            let err = parse(&toml, &schema()).unwrap_err();
            let Error::Scope(msg) = &err else {
                panic!("{bad}: expected Error::Scope, got {err:?}");
            };
            assert!(
                msg.starts_with("scope.orders.where.title:") && msg.contains(bad),
                "{bad}: {msg}"
            );
        }

        // Inside an object or array literal (a json/jsonb column) too: an
        // operand is one literal or one parameter, so a reference in there has
        // nothing to resolve it and must not ship as text. `$$` still escapes.
        let jsonb = Schema::builder()
            .table(
                Table::new("docs", "public", "docs")
                    .column("id", "id", PgType::Int4, false)
                    .column("meta", "meta", PgType::Jsonb, false)
                    .primary_key(&["id"]),
            )
            .build();
        for bad in [
            r#"{ owner = "$principal" }"#,
            r#"["$principal"]"#,
            r#"{ a = { b = ["x", "$p"] } }"#,
        ] {
            let toml = format!("[tables.docs]\nwhere = {{ meta = {{ _eq = {bad} }} }}");
            let err = parse(&toml, &jsonb).unwrap_err();
            assert!(
                matches!(&err, Error::Scope(m) if m.starts_with("scope.docs.where.meta:")),
                "{bad}: {err:?}"
            );
        }
        let err = parse(
            r#"
                [tables.docs]
                where = { meta = { _in = [{ owner = "$principal" }] } }
            "#,
            &jsonb,
        )
        .unwrap_err();
        assert!(matches!(err, Error::Scope(_)), "{err:?}");
        let policy = parse(
            r#"
                [tables.docs]
                where = { meta = { _eq = { price = "$$5", tags = ["$$x"] } } }
            "#,
            &jsonb,
        )
        .unwrap();
        let set = policy.bind(&Principal::new()).unwrap();
        let crate::TableScope::Allow(BoolExpr::Compare { value, .. }) = set.get("docs").unwrap()
        else {
            panic!("expected compare");
        };
        assert_eq!(*value, serde_json::json!({"price": "$5", "tags": ["$x"]}));

        // Inside `_in` lists and relation chains too — every value position
        // goes through the same check.
        let err = parse(
            r#"
                [tables.orders]
                where = { user = { id = { _in = [1, "$acc ount"] } } }
            "#,
            &schema(),
        )
        .unwrap_err();
        assert!(
            matches!(&err, Error::Scope(m) if m.starts_with("scope.orders.where.user.id:")),
            "{err:?}"
        );
    }

    #[test]
    fn referenced_params_reports_every_reference_without_a_schema() {
        // Tables and columns that no schema here knows: the walk is syntactic.
        let toml = r#"
            [tables.courses]
            where = { _and = [
                { school_id = { _eq = "$claim.school_id" } },
                { _or = [
                    { owner_id = { _eq = "$principal" } },
                    { school = { region = { tenant = { _in = ["$tenant", "$$literal", 3] } } } },
                ] },
                { _not = { status = { _in = ["archived"] } } },
                { deleted_at = { _is_null = true } },
                { code = { _like = "$$%" } },
            ] }

            [tables.adverts]
            unrestricted = true

            [tables.secrets]
            deny = true
        "#;
        let params = referenced_params(toml).unwrap();
        assert_eq!(
            params.into_iter().collect::<Vec<_>>(),
            vec!["claim.school_id", "principal", "tenant"]
        );

        // The same reference in two places is reported once.
        let params = referenced_params(
            r#"
                [tables.a]
                where = { x = { _eq = "$p" } }
                [tables.b]
                where = { y = { _eq = "$p" }, z = { _neq = "$p" } }
            "#,
        )
        .unwrap();
        assert_eq!(params.into_iter().collect::<Vec<_>>(), vec!["p"]);
    }

    #[test]
    fn referenced_params_agrees_with_the_loader() {
        // Every policy here goes through both readers. They must refuse the
        // same policies — a host gating saves on `referenced_params` must not
        // refuse what the engine would run, nor pass what it would refuse for
        // a reason no schema is needed to see — and report the same set when
        // they accept. The schema has what trips a walk keyed on spelling: a
        // column and a relation whose names start with `_`, and a jsonb
        // column whose operands are objects.
        let schema = Schema::builder()
            .table(
                Table::new("orders", "public", "orders")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("_deleted", "_deleted", PgType::Bool, false)
                    .column("meta", "meta", PgType::Jsonb, true)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")]))
                    .relation("_owner", Relation::object("users").on([("user_id", "id")])),
            )
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("role", "role", PgType::Text, false)
                    .primary_key(&["id"]),
            )
            .build();

        let accepted: &[(&str, &[&str])] = &[
            (
                r#"[tables.orders]
                   where = { _deleted = { _eq = false }, id = { _eq = "$p" } }"#,
                &["p"],
            ),
            (
                r#"[tables.orders]
                   where = { _owner = { id = { _eq = "$claim.user_id" } } }"#,
                &["claim.user_id"],
            ),
            (
                r#"[tables.orders]
                   where = { user = { _and = [{ id = { _eq = "$a" } }, { role = { _neq = "$$x" } }] } }"#,
                &["a"],
            ),
            (
                r#"[tables.orders]
                   where = { _or = [
                       { user = { id = { _in = [1, "$t"] } } },
                       { _not = { title = { _like = "$$%" } } },
                       { title = { _neq = "$$l" } },
                       { title = { _is_null = true } },
                       { meta = { _eq = { price = "$$5", tags = ["$$x"] } } },
                   ] }
                   [tables.users]
                   where = { id = { _eq = "$principal" } }"#,
                &["principal", "t"],
            ),
            (
                r#"[tables.orders]
                   unrestricted = true
                   [tables.users]
                   deny = true"#,
                &[],
            ),
        ];
        for (toml, expect) in accepted {
            let loaded = parse(toml, &schema)
                .unwrap_or_else(|e| panic!("loader refused {toml}: {e}"))
                .params();
            let syntactic =
                referenced_params(toml).unwrap_or_else(|e| panic!("syntactic refused {toml}: {e}"));
            assert_eq!(loaded, syntactic, "{toml}");
            assert_eq!(syntactic.into_iter().collect::<Vec<_>>(), *expect, "{toml}");
        }

        let refused = [
            // The reference grammar.
            r#"[tables.orders]
               where = { title = { _eq = "$claim." } }"#,
            r#"[tables.orders]
               where = { _and = [ { _not = { user = { id = { _in = [1, "$acc ount"] } } } } ] }"#,
            r#"[tables.orders]
               where = { meta = { _eq = { owner = "$principal" } } }"#,
            r#"[tables.orders]
               where = { meta = { _in = [{ owner = "$principal" }] } }"#,
            // Shape.
            r#"[tables.orders]
               where = { title = { _regex = "$p" } }"#,
            r#"[tables.orders]
               where = { title = { _is_null = "$p" } }"#,
            r#"[tables.orders]
               where = { title = { _in = "$p" } }"#,
            r#"[tables.orders]
               where = { _and = { title = { _eq = "$p" } } }"#,
            r#"[tables.orders]
               where = { _eq = "$p" }"#,
            r#"[tables.orders]
               where = { _not = { _eq = "$p" } }"#,
            r#"[tables.orders]
               where = { user = { _and = [{ _eq = "$p" }] } }"#,
            r#"[tables.orders]
               where = { user_id = { _eq = "$principal" } }
               deny = true"#,
            r#"[tables.orders]
               nope = true"#,
        ];
        for toml in refused {
            let loaded = parse(toml, &schema)
                .err()
                .unwrap_or_else(|| panic!("loader accepted {toml}"));
            let syntactic = referenced_params(toml)
                .err()
                .unwrap_or_else(|| panic!("syntactic accepted {toml}"));
            assert_eq!(
                std::mem::discriminant(&loaded),
                std::mem::discriminant(&syntactic),
                "{toml}\n  loader: {loaded}\n  syntactic: {syntactic}"
            );
            // A reference error is the same error, position included.
            if let Error::Scope(_) = loaded {
                assert_eq!(format!("{loaded}"), format!("{syntactic}"), "{toml}");
            }
        }
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
