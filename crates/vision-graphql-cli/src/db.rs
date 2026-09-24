//! One connection per URL scheme, and one introspection over both.
//!
//! The CLI's three database commands differ in what they do with what
//! introspection found, not in how they find it. This is the one place that
//! decides which database a URL names and asks it for its tables.

use anyhow::{bail, Context, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::sqlite::SqlitePoolOptions;
use vision_graphql::schema::introspect::{introspect_schemas, IntrospectedDb};
use vision_graphql::Dialect;

use crate::render::redact_url;

/// A pool on whichever database the URL named. Lazy: nothing connects until
/// introspection asks, so a bad URL fails there with a message that names
/// the operation.
pub enum Source {
    Postgres(sqlx::PgPool),
    Sqlite(sqlx::SqlitePool),
}

/// What introspection found, whichever database it was.
pub struct Found {
    pub db: IntrospectedDb,
    pub dialect: Dialect,
}

pub fn is_sqlite(url: &str) -> bool {
    url.starts_with("sqlite:")
}

pub fn connect(url: &str) -> Result<Source> {
    if is_sqlite(url) {
        // The pragmas the engine relies on, as a host would set them; the
        // file must exist — a typo in the path must not create an empty
        // database and report it as having no tables.
        let opts = vision_graphql::sqlite::connect_options(url)
            .with_context(|| format!("parsing connection URL {}", redact_url(url)))?;
        Ok(Source::Sqlite(
            SqlitePoolOptions::new()
                .max_connections(1)
                .connect_lazy_with(opts),
        ))
    } else {
        let opts: PgConnectOptions = url
            .parse()
            .with_context(|| format!("parsing connection URL {}", redact_url(url)))?;
        Ok(Source::Postgres(
            PgPoolOptions::new()
                .max_connections(2)
                .connect_lazy_with(opts),
        ))
    }
}

/// Introspect `source`. `schemas` is PostgreSQL's `--schema`; a SQLite file
/// has one schema, `main`, and any other request is refused rather than
/// ignored.
pub async fn introspect(source: &Source, schemas: &[String], url: &str) -> Result<Found> {
    match source {
        Source::Postgres(pool) => {
            let schemas: Vec<&str> = schemas.iter().map(String::as_str).collect();
            let db = introspect_schemas(pool, &schemas)
                .await
                .with_context(|| format!("introspect failed against {}", redact_url(url)))?;
            Ok(Found {
                db,
                dialect: Dialect::Postgres,
            })
        }
        Source::Sqlite(pool) => {
            let default = schemas.len() == 1 && matches!(schemas[0].as_str(), "public" | "main");
            if !default {
                bail!(
                    "--schema does not apply to SQLite: a database file has one schema, \
                     and every table is introspected"
                );
            }
            let found = vision_graphql::schema::introspect_sqlite::introspect(pool)
                .await
                .with_context(|| format!("introspect failed against {}", redact_url(url)))?;
            for table in &found.loosely_typed {
                tracing::warn!(
                    target: "vision_gql",
                    table = %table,
                    "table is not STRICT: SQLite does not enforce its declared column types, \
                     so the types generated here are what the writer chose to honour"
                );
            }
            Ok(Found {
                db: found.db,
                dialect: Dialect::Sqlite,
            })
        }
    }
}
