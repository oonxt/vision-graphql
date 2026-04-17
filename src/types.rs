//! PostgreSQL ↔ JSON type mapping.

use crate::error::{Error, Result};
use crate::schema::PgType;
use serde_json::Value;
use std::error::Error as StdError;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};

/// A single bound parameter ready to pass to `tokio-postgres`.
#[derive(Debug, Clone, PartialEq)]
pub enum Bind {
    Null,
    Bool(bool),
    Int4(i32),
    Int8(i64),
    Float8(f64),
    Text(String),
}

impl ToSql for Bind {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn StdError + Sync + Send>> {
        match self {
            Bind::Null => Ok(IsNull::Yes),
            Bind::Bool(v) => v.to_sql(ty, out),
            Bind::Int4(v) => v.to_sql(ty, out),
            Bind::Int8(v) => v.to_sql(ty, out),
            Bind::Float8(v) => v.to_sql(ty, out),
            Bind::Text(v) => v.as_str().to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
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
        | PgType::TimestampTz => v
            .as_str()
            .map(|s| Bind::Text(s.to_string()))
            .ok_or_else(|| Error::TypeMap(format!("expected string for {pg:?}"))),
        PgType::Jsonb => Ok(Bind::Text(v.to_string())),
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
