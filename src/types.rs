//! PostgreSQL ↔ JSON type mapping, and the deferred form of a bound parameter.

use crate::ast::Val;
use crate::error::{Error, Result};
use crate::predicate::Principal;
use crate::schema::PgType;
use serde_json::Value;

/// Everything a rendered statement needs to turn its [`BindSpec`]s into
/// [`Bind`]s: this request's GraphQL variables and its principal.
#[derive(Debug, Clone, Copy)]
pub struct Inputs<'a> {
    variables: Option<&'a Value>,
    defaults: Option<&'a serde_json::Map<String, Value>>,
    principal: Option<&'a Principal>,
}

/// The empty variables object, so `Inputs::none()` can hand out a `&Value`.
static NO_VARIABLES: Value = Value::Null;

impl<'a> Inputs<'a> {
    /// No variables and no principal — the shape an eagerly-lowered operation
    /// needs, since every value in it is already a literal.
    pub fn none() -> Self {
        Inputs {
            variables: None,
            defaults: None,
            principal: None,
        }
    }

    /// Variables for this request. `variables` should be a JSON object.
    pub fn variables(variables: &'a Value) -> Self {
        Inputs {
            variables: Some(variables),
            defaults: None,
            principal: None,
        }
    }

    /// Attach the defaults an operation declared for its variables
    /// (`query($n: Int = 10)`).
    ///
    /// The eager path applies these while lowering, but a compiled statement is
    /// lowered before any request exists, so it carries them and they land
    /// here. A variable the request actually supplied always wins, including
    /// when it supplied null.
    pub fn with_defaults(mut self, defaults: &'a serde_json::Map<String, Value>) -> Self {
        self.defaults = Some(defaults);
        self
    }

    /// Attach the principal whose parameters back [`Val::ScopeParam`].
    pub fn with_principal(mut self, principal: &'a Principal) -> Self {
        self.principal = Some(principal);
        self
    }

    fn vars(&self) -> &'a Value {
        self.variables.unwrap_or(&NO_VARIABLES)
    }

    /// Value of GraphQL variable `name`. Unbound is an error rather than null:
    /// silently treating a missing variable as null turns a client mistake into
    /// a query that runs and returns the wrong rows.
    pub fn variable(&self, name: &str) -> Result<&'a Value> {
        self.vars()
            .get(name)
            .or_else(|| self.defaults.and_then(|d| d.get(name)))
            .ok_or_else(|| Error::Variable {
                name: name.to_string(),
                message: "not bound".into(),
            })
    }

    /// Value of scope parameter `name`, from the principal.
    pub fn scope_param(&self, name: &str) -> Result<&'a Value> {
        self.principal
            .and_then(|p| p.get(name))
            .ok_or_else(|| Error::Validate {
                path: format!("principal.{name}"),
                message: format!("scope parameter '{name}' not supplied"),
            })
    }
}

/// A bound parameter that may not be known yet.
///
/// [`crate::sql::render`] emits one of these per placeholder. Anything the query
/// text alone determines is converted right there and stored as [`Fixed`], so a
/// literal's type error surfaces when the query is compiled rather than on the
/// request that happens to run it. Only genuinely per-request values stay
/// symbolic.
///
/// [`Fixed`]: BindSpec::Fixed
#[derive(Debug, Clone)]
pub enum BindSpec {
    /// Already converted: a literal, or a value the renderer synthesised.
    Fixed(Bind),
    /// A scalar column value.
    Scalar {
        val: Val,
        pg: PgType,
        /// Error path, e.g. `where.user_id`.
        path: String,
        /// Whether a null here is refused.
        ///
        /// Set for a comparison, where SQL's answer to `col = NULL` is no rows
        /// — not "the rows whose column is null", which is what a caller
        /// writing `_eq: null` means. Answering with an empty result would be
        /// the shape of a right answer to a question that was never asked.
        ///
        /// Not set where a null is a value: `_set: {col: null}` and an inserted
        /// column mean exactly what they say.
        reject_null: bool,
    },
    /// An `_in` / `_nin` list, resolving to a JSON array.
    Array { val: Val, pg: PgType, path: String },
    /// A `limit` / `offset` supplied as a variable.
    Count {
        val: crate::ast::Count,
        path: String,
    },
}

impl BindSpec {
    /// Convert `val` now when it is a literal, so errors are caught at render
    /// time; otherwise keep it for the request that supplies the value.
    pub(crate) fn scalar(val: Val, pg: &PgType, path: impl FnOnce() -> String) -> Result<Self> {
        Self::scalar_inner(val, pg, path, false)
    }

    /// A scalar in a comparison, where a null is refused rather than compared.
    /// See [`BindSpec::Scalar::reject_null`].
    pub(crate) fn comparison(val: Val, pg: &PgType, path: impl FnOnce() -> String) -> Result<Self> {
        Self::scalar_inner(val, pg, path, true)
    }

    fn scalar_inner(
        val: Val,
        pg: &PgType,
        path: impl FnOnce() -> String,
        reject_null: bool,
    ) -> Result<Self> {
        match val.as_lit() {
            Some(v) => {
                let path = path();
                if reject_null && v.is_null() {
                    return Err(null_comparison(&path));
                }
                json_to_bind(v, pg)
                    .map(BindSpec::Fixed)
                    .map_err(|e| Error::Validate {
                        path,
                        message: format!("{e}"),
                    })
            }
            None => Ok(BindSpec::Scalar {
                val,
                pg: pg.clone(),
                path: path(),
                reject_null,
            }),
        }
    }

    /// Same as [`BindSpec::scalar`] for an `_in` / `_nin` list.
    pub(crate) fn array(val: Val, pg: &PgType, path: impl FnOnce() -> String) -> Result<Self> {
        if val.is_lit() {
            let path = path();
            let no_inputs = Inputs::none();
            let resolved = val.resolve(&no_inputs).map_err(|e| Error::Validate {
                path: path.clone(),
                message: format!("{e}"),
            })?;
            return bind_array(&resolved, pg, &path).map(BindSpec::Fixed);
        }
        Ok(BindSpec::Array {
            val,
            pg: pg.clone(),
            path: path(),
        })
    }

    /// Resolve to a concrete parameter for this request.
    pub fn resolve(&self, inputs: &Inputs<'_>) -> Result<Bind> {
        match self {
            BindSpec::Fixed(b) => Ok(b.clone()),
            BindSpec::Scalar {
                val,
                pg,
                path,
                reject_null,
            } => {
                let v = val.resolve(inputs)?;
                // A variable carries the same refusal to where its value
                // arrives: `_eq: $x` with `x` null is the same question as
                // `_eq: null`, asked one request later.
                if *reject_null && v.is_null() {
                    return Err(null_comparison(path));
                }
                json_to_bind(&v, pg).map_err(|e| Error::Validate {
                    path: path.clone(),
                    message: format!("{e}"),
                })
            }
            BindSpec::Array { val, pg, path } => {
                let v = val.resolve(inputs)?;
                bind_array(&v, pg, path)
            }
            BindSpec::Count { val, path } => {
                let n = val.resolve(inputs, path)?;
                i64::try_from(n)
                    .map(Bind::Int8)
                    .map_err(|_| Error::Validate {
                        path: path.clone(),
                        message: format!("{n} is too large"),
                    })
            }
        }
    }
}

/// The one error this refusal produces, in both places it can happen.
fn null_comparison(path: &str) -> Error {
    Error::Validate {
        path: path.to_string(),
        message: "comparing against null matches no rows, which is unlikely to be \
                  what was meant; use `_is_null` to ask whether the column is null"
            .into(),
    }
}

fn bind_array(v: &Value, pg: &PgType, path: &str) -> Result<Bind> {
    let items = v.as_array().ok_or_else(|| Error::Validate {
        path: path.to_string(),
        message: format!("expected a list, got {v}"),
    })?;
    json_to_bind_array(items, pg).map_err(|e| Error::Validate {
        path: path.to_string(),
        message: format!("{e}"),
    })
}

/// Resolve a rendered statement's parameters in placeholder order.
pub fn resolve_binds(specs: &[BindSpec], inputs: &Inputs<'_>) -> Result<Vec<Bind>> {
    specs.iter().map(|s| s.resolve(inputs)).collect()
}

/// A single bound parameter ready to pass to sqlx.
///
/// "Stringly" PostgreSQL types (uuid, numeric, timestamps, jsonb) are carried
/// as [`Bind::Text`]: the client declares the parameter as `text` and the
/// rendered SQL casts it (`$1::uuid`) so the server performs the conversion.
#[derive(Debug, Clone, PartialEq)]
pub enum Bind {
    Null,
    Bool(bool),
    Int4(i32),
    Int8(i64),
    Float8(f64),
    Text(String),
    BoolArray(Vec<Option<bool>>),
    Int4Array(Vec<Option<i32>>),
    Int8Array(Vec<Option<i64>>),
    Float8Array(Vec<Option<f64>>),
    TextArray(Vec<Option<String>>),
}

pub fn json_to_bind(v: &Value, pg: &PgType) -> Result<Bind> {
    if v.is_null() {
        return Ok(Bind::Null);
    }
    match pg {
        PgType::Bool => v
            .as_bool()
            .map(Bind::Bool)
            .ok_or_else(|| Error::TypeMap("expected Bool".into())),
        // int2 travels as int4 — sqlx has no i16 in `Bind` — but the range is
        // checked here rather than left to the cast. Whether 100000 fits a
        // smallint is knowable from the value, and a literal that cannot fit
        // should fail where the query is compiled, not on the request that
        // happens to run it.
        PgType::Int2 => v
            .as_i64()
            .and_then(|n| i16::try_from(n).ok())
            .map(|n| Bind::Int4(n as i32))
            .ok_or_else(|| {
                Error::TypeMap(format!("expected an integer in smallint range, got {v}"))
            }),
        PgType::Int4 => v
            .as_i64()
            .and_then(|n| i32::try_from(n).ok())
            .map(Bind::Int4)
            .ok_or_else(|| Error::TypeMap("expected an integer".into())),
        PgType::Int8 => v
            .as_i64()
            .map(Bind::Int8)
            .ok_or_else(|| Error::TypeMap("expected Int8".into())),
        PgType::Float4 | PgType::Float8 => v
            .as_f64()
            .map(Bind::Float8)
            .ok_or_else(|| Error::TypeMap("expected floating point".into())),
        // `numeric` is carried as text so the server does the conversion. That is
        // a reason to *accept* a string, not to refuse a number: a column that
        // reads back as `12.34` and then refuses `_gt: 10` makes every caller
        // round-trip through strings for no benefit.
        //
        // A number is rendered from the `f64` serde_json already parsed it into
        // — not from the caller's own text, which is gone by the time this runs
        // (this crate does not enable `arbitrary_precision`). So a literal with
        // more precision than a double can hold arrives here already rounded,
        // and the string form is the only way to carry one exactly. Documented
        // in the README beside the type mapping.
        PgType::Numeric => match v {
            Value::String(s) => Ok(Bind::Text(s.clone())),
            Value::Number(n) => Ok(Bind::Text(n.to_string())),
            other => Err(Error::TypeMap(format!(
                "expected a number or a string for numeric, got {other}"
            ))),
        },
        PgType::Text
        | PgType::Varchar
        | PgType::Uuid
        | PgType::Timestamp
        | PgType::TimestampTz
        | PgType::Date
        | PgType::Time
        | PgType::Enum { .. } => v
            .as_str()
            .map(|s| Bind::Text(s.to_string()))
            .ok_or_else(|| Error::TypeMap(format!("expected string for {pg:?}"))),
        PgType::Json | PgType::Jsonb => Ok(Bind::Text(v.to_string())),
    }
}

/// Convert a JSON array (from `_in` / `_nin`) into a single array bind for
/// `= ANY($n)` / `<> ALL($n)`. NULL elements are allowed and keep SQL `IN`
/// semantics (they never match).
pub fn json_to_bind_array(values: &[Value], pg: &PgType) -> Result<Bind> {
    fn collect<T>(
        values: &[Value],
        f: impl Fn(&Value) -> Option<T>,
        expected: &str,
    ) -> Result<Vec<Option<T>>> {
        values
            .iter()
            .map(|v| {
                if v.is_null() {
                    Ok(None)
                } else {
                    f(v).map(Some)
                        .ok_or_else(|| Error::TypeMap(format!("expected {expected}")))
                }
            })
            .collect()
    }
    match pg {
        PgType::Bool => collect(values, Value::as_bool, "Bool").map(Bind::BoolArray),
        PgType::Int2 => collect(
            values,
            |v| {
                v.as_i64()
                    .and_then(|n| i16::try_from(n).ok())
                    .map(i32::from)
            },
            "an integer in smallint range",
        )
        .map(Bind::Int4Array),
        PgType::Int4 => collect(
            values,
            |v| v.as_i64().and_then(|n| i32::try_from(n).ok()),
            "an integer",
        )
        .map(Bind::Int4Array),
        PgType::Int8 => collect(values, Value::as_i64, "Int8").map(Bind::Int8Array),
        PgType::Float4 | PgType::Float8 => {
            collect(values, Value::as_f64, "floating point").map(Bind::Float8Array)
        }
        // Same as the scalar case: `_in: [1, 2]` on a numeric column is the
        // natural way to write it.
        PgType::Numeric => collect(
            values,
            |v| match v {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            },
            "a number or a string",
        )
        .map(Bind::TextArray),
        PgType::Text
        | PgType::Varchar
        | PgType::Uuid
        | PgType::Timestamp
        | PgType::TimestampTz
        | PgType::Date
        | PgType::Time
        | PgType::Enum { .. } => {
            collect(values, |v| v.as_str().map(str::to_string), "string").map(Bind::TextArray)
        }
        PgType::Json | PgType::Jsonb => Ok(Bind::TextArray(
            values
                .iter()
                .map(|v| {
                    if v.is_null() {
                        None
                    } else {
                        Some(v.to_string())
                    }
                })
                .collect(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::PgType;
    use serde_json::json;

    #[test]
    fn convert_int4_value() {
        let bind = json_to_bind(&json!(42), &PgType::Int4).unwrap();
        assert!(matches!(bind, Bind::Int4(42)));
    }

    #[test]
    fn convert_text_value() {
        let bind = json_to_bind(&json!("hi"), &PgType::Text).unwrap();
        match bind {
            Bind::Text(s) => assert_eq!(s, "hi"),
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn convert_null_value() {
        let bind = json_to_bind(&json!(null), &PgType::Int4).unwrap();
        assert!(matches!(bind, Bind::Null));
    }

    /// `int2` has no bind of its own: it goes out as int4 and the cast narrows
    /// it, so an out-of-range value is refused by Postgres rather than wrapped.
    #[test]
    fn smallint_binds_through_int4_but_keeps_its_own_range() {
        assert!(matches!(
            json_to_bind(&json!(7), &PgType::Int2).unwrap(),
            Bind::Int4(7)
        ));
        // Knowable from the value, so it fails here rather than at the server.
        let err = json_to_bind(&json!(100000), &PgType::Int2).unwrap_err();
        assert!(format!("{err}").contains("smallint range"), "{err}");
        assert!(json_to_bind(&json!(100000), &PgType::Int4).is_ok());
    }

    #[test]
    fn numeric_takes_a_number_or_a_string() {
        // A numeric column reads back as a JSON number, so writing one must
        // work; the string form stays for values a float cannot hold exactly.
        for (input, expected) in [
            (json!(9.5), "9.5"),
            (json!(10), "10"),
            (json!("12.340"), "12.340"),
            (
                json!("179769313486231570000000000000000000.5"),
                "179769313486231570000000000000000000.5",
            ),
        ] {
            match json_to_bind(&input, &PgType::Numeric).unwrap() {
                Bind::Text(s) => assert_eq!(s, expected, "for {input}"),
                other => panic!("expected text, got {other:?}"),
            }
        }
        let err = json_to_bind(&json!(true), &PgType::Numeric).unwrap_err();
        assert!(format!("{err}").contains("number or a string"), "{err}");
    }

    #[test]
    fn reject_type_mismatch() {
        let err = json_to_bind(&json!("not a number"), &PgType::Int4).unwrap_err();
        assert!(format!("{err}").contains("expected an integer"));
    }
}
