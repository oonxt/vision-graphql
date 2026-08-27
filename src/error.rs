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

    /// A column the scope does not admit. Separate from [`Error::ScopeDenied`]
    /// so an endpoint can tell "you may not read this table at all" from "not
    /// this column of it", which are different things to report.
    #[error("scope: column '{column}' on '{table}' is not accessible in scoped execution")]
    ScopeColumnDenied { table: String, column: String },

    #[error("scope: {0}")]
    Scope(String),

    /// The document was rejected before parsing, by [`crate::limits::ParseLimits`].
    /// Distinct from [`Error::Parse`] so an endpoint can answer "too large /
    /// too deep" differently from "syntactically invalid".
    #[error("query rejected: {message}")]
    Limit { message: String },

    /// The query cannot be lowered without its variable values, so it cannot be
    /// compiled once and reused. See [`crate::Engine::compile`].
    #[error("not compilable at {path}: {message}")]
    NotCompilable { path: String, message: String },
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
