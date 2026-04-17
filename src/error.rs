//! Error types for vision-graphql.

use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("validation error at {path}: {message}")]
    Validate { path: String, message: String },

    #[error("variable error: {name}: {message}")]
    Variable { name: String, message: String },

    #[error("type mapping: {0}")]
    TypeMap(String),

    #[error("database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    #[error("pool error: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),

    #[error("result decoding: {0}")]
    Decode(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_error_displays_path_and_message() {
        let e = Error::Validate {
            path: "users.where.id._eq".into(),
            message: "expected integer, got string".into(),
        };
        let s = format!("{e}");
        assert!(s.contains("users.where.id._eq"));
        assert!(s.contains("expected integer"));
    }
}
