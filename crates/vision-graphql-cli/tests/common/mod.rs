//! A database for a test.
//!
//! A copy of `tests/common/mod.rs` in the library crate. A test helper is not
//! part of either crate's public surface, and shipping one so the other could
//! use it would be worse than the duplication.
//!
//! **Set `TEST_DATABASE_URL` and this costs milliseconds.** Every test then gets
//! a fresh database on one server — `CREATE DATABASE` is a template copy — and
//! no container is started at all. The full suite runs in about thirteen
//! seconds instead of a minute, and Docker is never in the way. CI does this
//! with a service container:
//!
//! ```text
//! docker run --rm -d -e POSTGRES_PASSWORD=postgres -p 55432:5432 postgres:17.4-alpine
//! export TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/postgres
//! ```
//!
//! Without it, each test starts a container of its own through testcontainers
//! and drops it when it finishes. That is what the tests always did; the start
//! is retried now, because a hundred of them arriving together is how Docker
//! ends up answering one with `PortNotExposed` — a failure that reads as a
//! flaky test and is not one.
//!
//! The container is owned by the [`TestDb`] the test holds, not by a `static`:
//! a `static` never drops, and a hundred containers that nobody removes make
//! the *next* run slower than the one that leaked them.

#![allow(dead_code)] // each test binary uses a different part of this

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sqlx::postgres::{PgPool, PgPoolOptions};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::sync::OnceCell;

/// The image every test runs against. One place, so a version bump is one edit.
pub const PG_IMAGE_TAG: &str = "17.4-alpine";

/// A database, and whatever has to stay alive for it to answer.
///
/// Hold it for as long as the test needs the database. Dropping it removes the
/// container, when there is one.
pub struct TestDb {
    pub pool: PgPool,
    pub url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

/// The shared server's URL, when one was given. Read once; there is nothing to
/// keep alive.
static SHARED: OnceCell<Option<String>> = OnceCell::const_new();
static NEXT_DB: AtomicU32 = AtomicU32::new(0);

async fn shared_admin_url() -> Option<&'static String> {
    SHARED
        .get_or_init(|| async { std::env::var("TEST_DATABASE_URL").ok() })
        .await
        .as_ref()
}

/// Start the container, retried.
///
/// Every test binary that has no `TEST_DATABASE_URL` starts one of these, and
/// `cargo test` runs the binaries in parallel — two dozen `docker create` calls
/// arriving together, which Docker answers with a timeout often enough to
/// matter. Retrying is cheaper than the alternatives; pointing
/// `TEST_DATABASE_URL` at a server that already exists avoids the question.
async fn start_container() -> ContainerAsync<Postgres> {
    let mut last = String::new();
    for attempt in 0..6 {
        match Postgres::default().with_tag(PG_IMAGE_TAG).start().await {
            Ok(c) => return c,
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(Duration::from_millis(500 * (attempt + 1))).await;
            }
        }
    }
    panic!(
        "could not start postgres after several tries: {last}\n\
         Set TEST_DATABASE_URL to a running PostgreSQL to skip containers entirely."
    );
}

/// The mapped port, retried.
///
/// Docker can report the port as not yet exposed for a moment after the
/// container is running, and answering that with a failed test would put the
/// original flake back one level down.
async fn host_port(container: &ContainerAsync<Postgres>) -> u16 {
    let mut last = None;
    for attempt in 0..20 {
        match container.get_host_port_ipv4(5432).await {
            Ok(p) => return p,
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(50 * (attempt + 1))).await;
            }
        }
    }
    panic!("container port never appeared: {last:?}");
}

/// A fresh, empty database for this test.
pub async fn fresh_db() -> TestDb {
    match shared_admin_url().await {
        // A server that already exists: take a database on it.
        Some(admin_url) => {
            let (pool, url) = create_database(admin_url).await;
            TestDb {
                pool,
                url,
                _container: None,
            }
        }
        // Otherwise one server for this test, removed when it drops.
        None => {
            let container = start_container().await;
            let port = host_port(&container).await;
            let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to the test database");
            TestDb {
                pool,
                url,
                _container: Some(container),
            }
        }
    }
}

/// A new database on `admin_url`, and the URL that reaches it.
///
/// The name is unique per process and per call, so two tests — in one binary or
/// in twenty running at once — never share one.
async fn create_database(admin_url: &str) -> (PgPool, String) {
    let name = format!(
        "t{}_{}",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await
        .expect("connect to the maintenance database");
    // `CREATE DATABASE` takes no parameters, so the name has to be in the text.
    // It is ours — a pid and a counter — and quoted regardless, which is what
    // `AssertSqlSafe` is asserting here.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(r#"CREATE DATABASE "{name}""#)))
        .execute(&admin)
        .await
        .expect("create the test database");
    admin.close().await;

    let url = swap_database(admin_url, &name);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to the test database");
    (pool, url)
}

/// Replace the database in a connection URL, keeping everything else.
///
/// Not unit-tested here on purpose: this module is compiled into every test
/// binary, so a test in it runs twenty-five times and says nothing new. Every
/// integration test that connects exercises it.
fn swap_database(url: &str, database: &str) -> String {
    match url.rsplit_once('/') {
        // Keep any query string: `?sslmode=require` belongs to the server, not
        // to the database.
        Some((prefix, rest)) => match rest.split_once('?') {
            Some((_, query)) => format!("{prefix}/{database}?{query}"),
            None => format!("{prefix}/{database}"),
        },
        None => format!("{url}/{database}"),
    }
}
