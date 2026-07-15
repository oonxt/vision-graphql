//! PostgreSQL ↔ JSON type mapping.

use crate::error::{Error, Result};
use crate::schema::PgType;
use serde_json::Value;

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
        PgType::Int4 => v
            .as_i64()
            .and_then(|n| i32::try_from(n).ok())
            .map(Bind::Int4)
            .ok_or_else(|| Error::TypeMap("expected Int4".into())),
        PgType::Int8 => v
            .as_i64()
            .map(Bind::Int8)
            .ok_or_else(|| Error::TypeMap("expected Int8".into())),
        PgType::Float4 | PgType::Float8 => v
            .as_f64()
            .map(Bind::Float8)
            .ok_or_else(|| Error::TypeMap("expected floating point".into())),
        PgType::Text
        | PgType::Varchar
        | PgType::Uuid
        | PgType::Numeric
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
        PgType::Int4 => collect(
            values,
            |v| v.as_i64().and_then(|n| i32::try_from(n).ok()),
            "Int4",
        )
        .map(Bind::Int4Array),
        PgType::Int8 => collect(values, Value::as_i64, "Int8").map(Bind::Int8Array),
        PgType::Float4 | PgType::Float8 => {
            collect(values, Value::as_f64, "floating point").map(Bind::Float8Array)
        }
        PgType::Text
        | PgType::Varchar
        | PgType::Uuid
        | PgType::Numeric
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

    #[test]
    fn reject_type_mismatch() {
        let err = json_to_bind(&json!("not a number"), &PgType::Int4).unwrap_err();
        assert!(format!("{err}").contains("expected Int4"));
    }
}
