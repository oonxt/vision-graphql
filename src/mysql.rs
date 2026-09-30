//! Connecting to MySQL the way the engine's SQL assumes.
//!
//! What MySQL decides per server or per session, and the rendered SQL
//! depends on:
//!
//! - **The version.** The window form of `JSON_ARRAYAGG`, `JSON_TABLE`, the
//!   `AS new` alias of an upsert: 8.0.19 is where the last of them arrived.
//!   MariaDB is refused outright — its JSON is text, and it has none of the
//!   three.
//! - **`sql_mode`.** The renderer writes a backslash in a string literal as
//!   `\\`; under `NO_BACKSLASH_ESCAPES` that is two characters, and a type
//!   name or a response key with a backslash in it comes back doubled.
//! - **Time zone conversion.** A `TIMESTAMP` is read back in the session's
//!   zone and rendered in UTC with `CONVERT_TZ`, which answers NULL — for
//!   every row, silently — when the zone is a name and the server's time
//!   zone tables were never loaded.
//!
//! [`connect_options`] parses a URL into options that set nothing beyond
//! what sqlx sets on its own (`utf8mb4`, `time_zone = '+00:00'`); it is
//! here so a caller has one place to start from. [`verify`] checks a pool,
//! and [`Schema::introspect_mysql`](crate::Schema::introspect_mysql) runs
//! it first, so a server the SQL would misbehave on is refused before it
//! serves anything; an [`Engine`](crate::Engine) runs it once before the
//! first statement on its own pool.

use crate::error::{Error, Result};
use sqlx::mysql::{MySqlConnectOptions, MySqlPool};
use sqlx::Row;
use std::str::FromStr;

/// The oldest MySQL the rendered SQL runs on.
pub const MIN_VERSION: (u32, u32, u32) = (8, 0, 19);

/// Connection options for `url` (`mysql://user:pass@host/db`). Hand the
/// result to `MySqlPoolOptions::connect_with`.
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let pool = sqlx::mysql::MySqlPoolOptions::new()
///     .connect_with(vision_graphql::mysql::connect_options("mysql://root:root@127.0.0.1/app")?)
///     .await?;
/// # Ok(()) }
/// ```
pub fn connect_options(url: &str) -> Result<MySqlConnectOptions> {
    Ok(MySqlConnectOptions::from_str(url)?)
}

/// Check that `pool`'s server and sessions behave the way the rendered SQL
/// assumes. See the module docs for what is checked and why.
pub async fn verify(pool: &MySqlPool) -> Result<()> {
    let row = sqlx::query(
        "SELECT VERSION() AS v, @@session.sql_mode AS m, \
         (CONVERT_TZ(NOW(), @@session.time_zone, '+00:00') IS NOT NULL) AS tz",
    )
    .fetch_one(pool)
    .await?;
    let version: String = row.try_get("v")?;
    let sql_mode: String = row.try_get("m")?;
    let tz_converts: i64 = row.try_get("tz")?;
    if version.to_ascii_lowercase().contains("mariadb") {
        return Err(Error::Schema(format!(
            "MariaDB ({version}) is not MySQL: its JSON is text, and it has no JSON_TABLE; \
             the MySQL backend does not run on it"
        )));
    }
    if parse_version(&version) < Some(MIN_VERSION) {
        return Err(Error::Schema(format!(
            "MySQL {version} is too old: {}.{}.{} or later is needed",
            MIN_VERSION.0, MIN_VERSION.1, MIN_VERSION.2
        )));
    }
    if sql_mode
        .split(',')
        .any(|m| m.trim().eq_ignore_ascii_case("NO_BACKSLASH_ESCAPES"))
    {
        return Err(Error::Schema(
            "MySQL sql_mode NO_BACKSLASH_ESCAPES is set on this pool; the rendered SQL escapes \
             a backslash in a literal as \\\\, which that mode reads as two"
                .into(),
        ));
    }
    if tz_converts != 1 {
        return Err(Error::Schema(
            "MySQL cannot convert this session's time zone to UTC (CONVERT_TZ returns NULL): \
             load the server's time zone tables, or set time_zone to an offset such as \
             '+00:00' on every connection"
                .into(),
        ));
    }
    Ok(())
}

/// `8.4.11`, or `8.0.19-log` and the like — the number before the first
/// thing that is not a number.
fn parse_version(v: &str) -> Option<(u32, u32, u32)> {
    let head: String = v
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let mut it = head.split('.').map(|p| p.parse::<u32>().ok());
    Some((it.next()??, it.next()??, it.next().flatten().unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_compare_numerically() {
        assert_eq!(parse_version("8.4.11"), Some((8, 4, 11)));
        assert_eq!(parse_version("8.0.19-log"), Some((8, 0, 19)));
        assert_eq!(parse_version("8.0.19-0ubuntu0.22.04.1"), Some((8, 0, 19)));
        assert!(parse_version("8.0.18") < Some(MIN_VERSION));
        assert!(parse_version("8.0.19") >= Some(MIN_VERSION));
        assert!(parse_version("5.7.44") < Some(MIN_VERSION));
        assert!(parse_version("9.1.0") >= Some(MIN_VERSION));
        assert_eq!(parse_version("x"), None);
    }
}
