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

/// A stable, machine-readable classification of an [`Error`].
///
/// The string is the contract, not the variant: it goes in `extensions.code`
/// and a client may branch on it, so it outlives any reshuffling of the enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// The document could not be parsed.
    ParseFailed,
    /// The document was refused before parsing — too large, or too deeply
    /// nested. See [`crate::limits::ParseLimits`].
    DocumentRejected,
    /// The request asks for something the schema does not offer, or offers
    /// differently: an unknown column, a value of the wrong type, a null in a
    /// comparison.
    ValidationFailed,
    /// A variable the document declares and the request did not supply.
    VariableMissing,
    /// The scope in force does not admit this table or column.
    ScopeDenied,
    /// The request costs more than [`crate::limits::ExecutionLimits`] allows.
    LimitExceeded,
    /// The query cannot be compiled ahead of its variables.
    NotCompilable,
    /// PostgreSQL refused the statement.
    DatabaseError,
    /// Something inside the engine — a schema that does not hold together, a
    /// result that would not decode. Not the caller's doing.
    Internal,
}

impl ErrorCode {
    /// The wire form.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::ParseFailed => "PARSE_FAILED",
            ErrorCode::DocumentRejected => "DOCUMENT_REJECTED",
            ErrorCode::ValidationFailed => "VALIDATION_FAILED",
            ErrorCode::VariableMissing => "VARIABLE_MISSING",
            ErrorCode::ScopeDenied => "SCOPE_DENIED",
            ErrorCode::LimitExceeded => "LIMIT_EXCEEDED",
            ErrorCode::NotCompilable => "NOT_COMPILABLE",
            ErrorCode::DatabaseError => "DATABASE_ERROR",
            ErrorCode::Internal => "INTERNAL_ERROR",
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Error {
    /// How to classify this for a caller.
    pub fn code(&self) -> ErrorCode {
        match self {
            Error::Parse(_) => ErrorCode::ParseFailed,
            Error::Limit { .. } => ErrorCode::DocumentRejected,
            Error::Validate { .. } | Error::TypeMap(_) => ErrorCode::ValidationFailed,
            Error::Variable { .. } => ErrorCode::VariableMissing,
            Error::ScopeDenied { .. } | Error::ScopeColumnDenied { .. } | Error::Scope(_) => {
                ErrorCode::ScopeDenied
            }
            Error::NotCompilable { .. } => ErrorCode::NotCompilable,
            Error::Database(_) => ErrorCode::DatabaseError,
            Error::Schema(_) | Error::Decode(_) => ErrorCode::Internal,
        }
    }

    /// The message meant for whoever sent the request.
    ///
    /// Most of them are the full one: a validation error names a column the
    /// document already named, and withholding it would only make the client
    /// guess. Two are not.
    ///
    /// A database error is reduced to its SQLSTATE. PostgreSQL's message text
    /// carries table names, constraint names, and sometimes a source file and
    /// line from inside the server — none of which the client asked for, and
    /// some of which describes structure the schema deliberately does not
    /// expose. The full text stays available through [`std::fmt::Display`] for
    /// the log, which is where it belongs.
    ///
    /// An internal error says so and nothing else, for the same reason.
    pub fn client_message(&self) -> String {
        match self {
            Error::Database(e) => match sqlstate(e) {
                Some(code) => format!("the database refused the statement (SQLSTATE {code})"),
                None => "the database refused the statement".into(),
            },
            Error::Schema(_) | Error::Decode(_) => {
                "the engine could not complete this request".into()
            }
            other => other.to_string(),
        }
    }

    /// Where in the request this went wrong, as this crate spells it —
    /// `users.where.id`, `m0.objects[0].price`.
    ///
    /// Not the GraphQL `path`, which is a position in the *response*: every
    /// error here is a request error, raised before any data exists, so there
    /// is no response position to name. It travels in `extensions` instead.
    pub fn position(&self) -> Option<&str> {
        match self {
            Error::Validate { path, .. } | Error::NotCompilable { path, .. } => Some(path),
            Error::Variable { name, .. } => Some(name),
            _ => None,
        }
    }

    /// One GraphQL error object: `message`, and `extensions` carrying the code,
    /// the position when there is one, and the SQLSTATE when the database is
    /// what refused.
    pub fn to_graphql_error(&self) -> serde_json::Value {
        let mut extensions = serde_json::Map::new();
        extensions.insert(
            "code".into(),
            serde_json::Value::String(self.code().as_str().into()),
        );
        if let Some(p) = self.position() {
            extensions.insert("path".into(), serde_json::Value::String(p.into()));
        }
        if let Error::Database(e) = self {
            if let Some(code) = sqlstate(e) {
                extensions.insert("sqlstate".into(), serde_json::Value::String(code));
            }
        }
        serde_json::json!({
            "message": self.client_message(),
            "extensions": serde_json::Value::Object(extensions),
        })
    }

    /// The whole response body for a failed request: `{"errors": [ … ]}`.
    ///
    /// No `data` key, and one error rather than several — deliberately. This
    /// engine renders one statement per request and runs it whole, so there is
    /// no partial success to report and no second error to collect: the first
    /// thing that goes wrong is the only thing that happens.
    pub fn to_graphql_response(&self) -> serde_json::Value {
        serde_json::json!({ "errors": [self.to_graphql_error()] })
    }
}

/// The five-character SQLSTATE PostgreSQL answered with, if it answered.
fn sqlstate(e: &sqlx::Error) -> Option<String> {
    match e {
        sqlx::Error::Database(db) => db.code().map(|c| c.into_owned()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_validation_error_reaches_the_client_whole() {
        // It names a column the document already named; withholding it would
        // only make the caller guess.
        let e = Error::Validate {
            path: "users.where.id".into(),
            message: "expected integer, got string".into(),
        };
        let g = e.to_graphql_error();
        assert!(g["message"].as_str().unwrap().contains("expected integer"));
        assert_eq!(g["extensions"]["code"], "VALIDATION_FAILED");
        assert_eq!(g["extensions"]["path"], "users.where.id");
    }

    #[test]
    fn an_internal_error_says_only_that() {
        for e in [
            Error::Schema("table 'users' has two columns named id".into()),
            Error::Decode("invalid type: string, expected i64 at line 1".into()),
        ] {
            let g = e.to_graphql_error();
            assert_eq!(g["extensions"]["code"], "INTERNAL_ERROR");
            let msg = g["message"].as_str().unwrap();
            assert!(msg.contains("could not complete"), "{msg}");
            assert!(!msg.contains("users"), "internals must not travel: {msg}");
            assert!(
                !msg.contains("expected i64"),
                "internals must not travel: {msg}"
            );
            // …while the log still gets everything.
            assert!(format!("{e}").contains("users") || format!("{e}").contains("expected i64"));
        }
    }

    #[test]
    fn the_response_body_carries_no_data_key() {
        // Every error here is a request error: one statement per request, run
        // whole, so there is never partial data to report alongside.
        let e = Error::Limit {
            message: "document nests deeper than the limit of 64".into(),
        };
        let body = e.to_graphql_response();
        assert!(body.get("data").is_none(), "{body}");
        assert_eq!(body["errors"].as_array().unwrap().len(), 1);
        assert_eq!(body["errors"][0]["extensions"]["code"], "DOCUMENT_REJECTED");
    }

    #[test]
    fn codes_are_distinct_per_kind() {
        use std::collections::BTreeSet;
        let codes: BTreeSet<&str> = [
            Error::Parse("x".into()).code(),
            Error::Limit {
                message: "x".into(),
            }
            .code(),
            Error::Validate {
                path: "p".into(),
                message: "m".into(),
            }
            .code(),
            Error::Variable {
                name: "n".into(),
                message: "m".into(),
            }
            .code(),
            Error::ScopeDenied { table: "t".into() }.code(),
            Error::NotCompilable {
                path: "p".into(),
                message: "m".into(),
            }
            .code(),
            Error::Schema("x".into()).code(),
        ]
        .iter()
        .map(|c| c.as_str())
        .collect();
        assert_eq!(codes.len(), 7, "each kind answers to its own code");
    }

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
