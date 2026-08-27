//! A database for a test.
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
/// container, when there is one — and on a shared server, the database itself:
/// a long-lived server otherwise accretes a hundred databases per run, and the
/// `t{pid}_…` names would eventually collide with a leaked one when the OS
/// reuses a pid.
pub struct TestDb {
    pub pool: PgPool,
    pub url: String,
    /// The name of the database created on a shared server, so `Drop` can
    /// remove it. `None` on the container path — the container takes the
    /// database with it.
    pub db_name: Option<String>,
    admin_url: Option<String>,
    _container: Option<ContainerAsync<Postgres>>,
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let (Some(name), Some(admin)) = (self.db_name.take(), self.admin_url.take()) else {
            return;
        };
        // Drop is synchronous and the ambient runtime may be single-threaded
        // (`#[tokio::test]`'s default), where blocking on it deadlocks — so a
        // throwaway thread and runtime, joined so the removal has actually
        // happened when drop returns. WITH (FORCE) severs this TestDb's own
        // pool, which still holds connections at this point.
        let outcome = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build a runtime to drop the test database");
            rt.block_on(async {
                let admin = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&admin)
                    .await?;
                sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    r#"DROP DATABASE "{name}" WITH (FORCE)"#
                )))
                .execute(&admin)
                .await?;
                admin.close().await;
                Ok::<_, sqlx::Error>(())
            })
        })
        .join();
        // Complain but do not panic: a panic here during a failing test's
        // unwind would abort the process and eat the failure that matters.
        if let Ok(Err(e)) = outcome {
            eprintln!("warning: test database was not removed: {e}");
        }
    }
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
            let (pool, url, name) = create_database(admin_url).await;
            TestDb {
                pool,
                url,
                db_name: Some(name),
                admin_url: Some(admin_url.clone()),
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
                db_name: None,
                admin_url: None,
                _container: Some(container),
            }
        }
    }
}

/// A new database on `admin_url`, and the URL that reaches it.
///
/// The name is unique per process and per call — a pid and a counter — plus a
/// per-run timestamp, because a pid alone comes back: the OS reuses them, and
/// on a server where an interrupted run left its databases behind (`Drop`
/// removes them, but `kill -9` outruns any Drop) the reused pid recomputes an
/// existing name and `CREATE DATABASE` panics on it.
async fn create_database(admin_url: &str) -> (PgPool, String, String) {
    static RUN: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let run = RUN.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is past 1970")
            .as_nanos() as u64
    });
    let name = format!(
        "t{}_{:x}_{}",
        std::process::id(),
        run,
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
    (pool, url, name)
}

/// Replace the database in a connection URL, keeping everything else.
///
/// The path has to be looked for *after* the authority: a URL with no database
/// segment — `postgres://u:p@host:5432`, valid, sqlx defaults the database —
/// has its last `/` inside `://`, and splitting there hands the database name
/// the authority's place. Tested in `integration_harness.rs`, once — this
/// module is compiled into every test binary, and a test here would run
/// twenty-five times and say nothing new.
pub fn swap_database(url: &str, database: &str) -> String {
    // Keep any query string: `?sslmode=require` belongs to the server, not to
    // the database — and it can appear with no path at all.
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (url, None),
    };
    let authority_start = base.find("://").map(|i| i + 3).unwrap_or(0);
    let server = match base[authority_start..].find('/') {
        Some(i) => &base[..authority_start + i],
        None => base,
    };
    match query {
        Some(query) => format!("{server}/{database}?{query}"),
        None => format!("{server}/{database}"),
    }
}
