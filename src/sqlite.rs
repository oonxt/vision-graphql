//! Connecting to SQLite the way the engine's SQL assumes.
//!
//! Two things SQLite decides per connection, not per database, and decides
//! differently from PostgreSQL by default:
//!
//! - **`LIKE` is case-insensitive** (for ASCII) unless `PRAGMA
//!   case_sensitive_like` is on. The engine renders `_like` as `LIKE` and
//!   `_ilike` as `lower() LIKE lower()`; with the pragma off, both are
//!   case-insensitive and `_like` answers a question it was not asked.
//! - **Foreign keys are not enforced** unless `PRAGMA foreign_keys` is on.
//!   The schema's relations are derived from them; a row that violates one
//!   makes an object relation come back null where the schema promised a row.
//!
//! [`connect_options`] sets both on every connection of a pool.
//! [`verify`] checks a pool that was built some other way, and
//! [`Schema::introspect_sqlite`](crate::Schema::introspect_sqlite) runs it
//! first, so a misconfigured pool is refused before it serves anything. A
//! version below 3.44 is refused too: `json_group_array(… ORDER BY …)`, which
//! nested `order_by` renders to, arrived there.

use crate::error::{Error, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use sqlx::Row;
use std::str::FromStr;

/// The oldest SQLite the rendered SQL runs on.
pub const MIN_VERSION: (u32, u32, u32) = (3, 44, 0);

/// Connection options for `url` (`sqlite://path`, `sqlite::memory:`, …)
/// with the pragmas the engine relies on. Hand the result to
/// `SqlitePoolOptions::connect_with`.
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let pool = sqlx::sqlite::SqlitePoolOptions::new()
///     .connect_with(vision_graphql::sqlite::connect_options("sqlite://app.db")?)
///     .await?;
/// # Ok(()) }
/// ```
pub fn connect_options(url: &str) -> Result<SqliteConnectOptions> {
    let opts = SqliteConnectOptions::from_str(url)?;
    Ok(opts.foreign_keys(true).pragma("case_sensitive_like", "ON"))
}

/// Check that `pool`'s connections behave the way the rendered SQL assumes.
/// See the module docs for what is checked and why.
pub async fn verify(pool: &SqlitePool) -> Result<()> {
    let row = sqlx::query(
        "SELECT sqlite_version() AS v, ('a' LIKE 'A') AS folds, \
         (SELECT foreign_keys FROM pragma_foreign_keys) AS fks",
    )
    .fetch_one(pool)
    .await?;
    let version: String = row.try_get("v")?;
    let folds: i64 = row.try_get("folds")?;
    let fks: i64 = row.try_get("fks")?;
    if parse_version(&version) < Some(MIN_VERSION) {
        return Err(Error::Schema(format!(
            "SQLite {version} is too old: {}.{}.{} or later is needed",
            MIN_VERSION.0, MIN_VERSION.1, MIN_VERSION.2
        )));
    }
    if folds != 0 {
        return Err(Error::Schema(
            "SQLite LIKE is case-insensitive on this pool; connect with \
             vision_graphql::sqlite::connect_options, or set PRAGMA case_sensitive_like = ON \
             on every connection"
                .into(),
        ));
    }
    if fks != 1 {
        return Err(Error::Schema(
            "SQLite foreign keys are not enforced on this pool; connect with \
             vision_graphql::sqlite::connect_options, or set PRAGMA foreign_keys = ON on \
             every connection"
                .into(),
        ));
    }
    Ok(())
}

fn parse_version(v: &str) -> Option<(u32, u32, u32)> {
    let mut it = v.split('.').map(|p| p.parse::<u32>().ok());
    Some((it.next()??, it.next()??, it.next().flatten().unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_compare_numerically() {
        assert_eq!(parse_version("3.51.3"), Some((3, 51, 3)));
        assert_eq!(parse_version("3.9"), Some((3, 9, 0)));
        assert!(parse_version("3.9") < Some(MIN_VERSION));
        assert!(parse_version("3.44.0") >= Some(MIN_VERSION));
        assert_eq!(parse_version("x"), None);
    }
}
