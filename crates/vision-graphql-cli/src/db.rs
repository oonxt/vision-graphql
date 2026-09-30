//! One connection per URL scheme, and one introspection over all of them.
//!
//! The CLI's three database commands differ in what they do with what
//! introspection found, not in how they find it. This is the one place that
//! decides which database a URL names and asks it for its tables.

use anyhow::{bail, Context, Result};
#[cfg(feature = "mysql")]
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::sqlite::SqlitePoolOptions;
use vision_graphql::schema::introspect::{introspect_schemas, IntrospectedDb};
use vision_graphql::schema::merge::build_from_introspection;
use vision_graphql::schema::SchemaBuilder;
use vision_graphql::Dialect;

use crate::render::redact_url;

/// A pool on whichever database the URL named. Lazy: nothing connects until
/// introspection asks, so a bad URL fails there with a message that names
/// the operation.
pub struct Source {
    pool: Pool,
    /// The URL with its password removed, for messages.
    pub redacted: String,
}

enum Pool {
    Postgres(sqlx::PgPool),
    Sqlite(sqlx::SqlitePool),
    #[cfg(feature = "mysql")]
    MySql(sqlx::MySqlPool),
}

/// What introspection found, whichever database it was.
pub struct Found {
    pub db: IntrospectedDb,
    pub dialect: Dialect,
    /// SQLite tables that are not `STRICT`; empty for the others.
    pub loosely_typed: Vec<String>,
}

impl Found {
    /// The schema builder the engine would build from this — the same one
    /// `Schema::introspect` / `Schema::introspect_sqlite` /
    /// `Schema::introspect_mysql` return — so what the CLI derives (SDL,
    /// warnings) is what the engine publishes.
    pub fn into_builder(self) -> SchemaBuilder {
        build_from_introspection(self.db)
            .dialect(self.dialect)
            .loosely_typed(&self.loosely_typed)
    }
}

fn is_sqlite(url: &str) -> bool {
    url.starts_with("sqlite:")
}

fn is_mysql(url: &str) -> bool {
    url.starts_with("mysql:")
}

pub fn connect(url: &str) -> Result<Source> {
    let redacted = redact_url(url);
    let pool = if is_sqlite(url) {
        // Introspection only reads, and a typo in the path must not create
        // an empty database and report it as having no tables — whatever
        // mode the URL carries, this opens read-only and creates nothing.
        // An in-memory database has nothing to introspect.
        if url.contains(":memory:") {
            bail!("an in-memory SQLite database has no tables to introspect");
        }
        let opts = vision_graphql::sqlite::connect_options(url)
            .with_context(|| format!("parsing connection URL {redacted}"))?
            .read_only(true)
            .create_if_missing(false);
        Pool::Sqlite(
            SqlitePoolOptions::new()
                .max_connections(1)
                .connect_lazy_with(opts),
        )
    } else if is_mysql(url) {
        #[cfg(not(feature = "mysql"))]
        bail!("this vision-gql was built without MySQL support (the `mysql` feature)");
        #[cfg(feature = "mysql")]
        {
            let opts = vision_graphql::mysql::connect_options(url)
                .with_context(|| format!("parsing connection URL {redacted}"))?;
            Pool::MySql(
                MySqlPoolOptions::new()
                    .max_connections(2)
                    .connect_lazy_with(opts),
            )
        }
    } else {
        let opts: PgConnectOptions = url
            .parse()
            .with_context(|| format!("parsing connection URL {redacted}"))?;
        Pool::Postgres(
            PgPoolOptions::new()
                .max_connections(2)
                .connect_lazy_with(opts),
        )
    };
    Ok(Source { pool, redacted })
}

impl Source {
    /// Introspect. `schemas` is `--schema` as given — `None` when it was not:
    /// PostgreSQL then reads `public`; SQLite, whose file has one schema, and
    /// MySQL, which reads the database the URL names, refuse any value rather
    /// than ignoring it.
    pub async fn introspect(&self, schemas: Option<&[String]>) -> Result<Found> {
        match &self.pool {
            Pool::Postgres(pool) => {
                let schemas: Vec<&str> = match schemas {
                    Some(s) => s.iter().map(String::as_str).collect(),
                    None => vec!["public"],
                };
                let db = introspect_schemas(pool, &schemas)
                    .await
                    .with_context(|| format!("introspect failed against {}", self.redacted))?;
                Ok(Found {
                    db,
                    dialect: Dialect::Postgres,
                    loosely_typed: Vec::new(),
                })
            }
            Pool::Sqlite(pool) => {
                if schemas.is_some() {
                    bail!(
                        "--schema does not apply to SQLite: a database file has one schema, \
                         and every table is introspected"
                    );
                }
                let found = vision_graphql::schema::introspect_sqlite::introspect(pool)
                    .await
                    .with_context(|| format!("introspect failed against {}", self.redacted))?;
                Ok(Found {
                    db: found.db,
                    dialect: Dialect::Sqlite,
                    loosely_typed: found.loosely_typed,
                })
            }
            #[cfg(feature = "mysql")]
            Pool::MySql(pool) => {
                if schemas.is_some() {
                    bail!(
                        "--schema does not apply to MySQL: the database the URL names is \
                         introspected (mysql://user:pass@host/name)"
                    );
                }
                let db = vision_graphql::schema::introspect_mysql::introspect(pool)
                    .await
                    .with_context(|| format!("introspect failed against {}", self.redacted))?;
                Ok(Found {
                    db,
                    dialect: Dialect::MySql,
                    loosely_typed: Vec::new(),
                })
            }
        }
    }
}
