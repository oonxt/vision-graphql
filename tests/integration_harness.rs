//! The test harness's own regressions. One binary runs these once — the
//! `common` module is compiled into every test binary, so a test *inside* it
//! would run twenty-five times and say nothing new.

mod common;

/// Every URL shape `TEST_DATABASE_URL` is allowed to take. The one with no
/// database segment is the regression: sqlx defaults the database there, so the
/// URL is valid — and its last `/` is the one inside `://`, which a bare
/// `rsplit_once('/')` once split at, handing the database name the authority's
/// place and every test a "could not resolve host" far from the cause.
#[test]
fn swap_database_replaces_only_the_database() {
    for (url, want) in [
        (
            "postgres://u:p@h:5432/postgres",
            "postgres://u:p@h:5432/db1",
        ),
        (
            "postgres://u:p@h:5432/postgres?sslmode=require",
            "postgres://u:p@h:5432/db1?sslmode=require",
        ),
        ("postgres://u:p@h:5432", "postgres://u:p@h:5432/db1"),
        (
            "postgres://u:p@h:5432?sslmode=require",
            "postgres://u:p@h:5432/db1?sslmode=require",
        ),
    ] {
        assert_eq!(common::swap_database(url, "db1"), want, "from {url}");
    }
}

/// On a shared server, the database a test took goes away with its `TestDb`.
/// Leaked ones once accumulated by the hundred, and a reused pid recomputing a
/// leaked name panicked `CREATE DATABASE` — a hard failure that read as flaky.
#[tokio::test]
async fn dropping_a_testdb_drops_its_database_on_a_shared_server() {
    // Only meaningful against a shared server; on the container path the
    // container itself is the cleanup, and there is no server left to ask.
    let Ok(admin_url) = std::env::var("TEST_DATABASE_URL") else {
        return;
    };
    let db = common::fresh_db().await;
    let name = db
        .db_name
        .clone()
        .expect("the shared path names its database");
    // Drop joins the removal thread, so the database is gone when this returns.
    drop(db);

    let admin = sqlx::PgPool::connect(&admin_url).await.expect("admin");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(&name)
            .fetch_one(&admin)
            .await
            .expect("ask pg_database");
    assert!(!exists, "database {name} should have gone with its TestDb");
}
