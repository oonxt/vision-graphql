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
    Database(#[from] sqlx::Error),

    #[error("result decoding: {0}")]
    Decode(String),

    #[error("scope: table '{table}' is not accessible in scoped execution")]
    ScopeDenied { table: String },

    #[error("scope: {0}")]
    Scope(String),
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
