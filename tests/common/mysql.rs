//! A MySQL database for a test; the MySQL twin of the PostgreSQL harness
//! in the parent module.
//!
//! **Set `MYSQL_TEST_URL` and this costs milliseconds per test.** Every test
//! then gets a fresh database on one server. Without it, each test starts a
//! `mysql:8.4` container through testcontainers, which works and costs a
//! minute per binary. CI sets the variable against a service container.

use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions};
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::sync::OnceCell;

pub const MYSQL_IMAGE_TAG: &str = "8.4";

/// A database, and whatever has to stay alive for it to answer.
pub struct TestDb {
    pub pool: MySqlPool,
    /// The URL that reaches this database, for a process of its own (the
    /// CLI).
    pub url: String,
    db_name: Option<String>,
    admin_url: Option<String>,
    _container: Option<ContainerAsync<Mysql>>,
}
impl Drop for TestDb {
    fn drop(&mut self) {
        let (Some(name), Some(admin)) = (self.db_name.take(), self.admin_url.take()) else {
            return;
        };
        let outcome = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build a runtime to drop the test database");
            rt.block_on(async {
                let admin = MySqlPoolOptions::new()
                    .max_connections(1)
                    .connect(&admin)
                    .await?;
                sqlx::raw_sql(sqlx::AssertSqlSafe(format!("DROP DATABASE `{name}`")))
                    .execute(&admin)
                    .await?;
                admin.close().await;
                Ok::<_, sqlx::Error>(())
            })
        })
        .join();
        if let Ok(Err(e)) = outcome {
            eprintln!("warning: test database was not removed: {e}");
        }
    }
}

static SHARED: OnceCell<Option<String>> = OnceCell::const_new();
static NEXT_DB: AtomicU32 = AtomicU32::new(0);

async fn shared_admin_url() -> Option<&'static String> {
    SHARED
        .get_or_init(|| async { std::env::var("MYSQL_TEST_URL").ok() })
        .await
        .as_ref()
}

async fn start_container() -> ContainerAsync<Mysql> {
    let mut last = String::new();
    for attempt in 0..6 {
        match Mysql::default().with_tag(MYSQL_IMAGE_TAG).start().await {
            Ok(c) => return c,
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(Duration::from_millis(500 * (attempt + 1))).await;
            }
        }
    }
    panic!(
        "could not start mysql after several tries: {last}\n\
         Set MYSQL_TEST_URL to a running MySQL to skip containers entirely."
    );
}

pub async fn fresh_db() -> TestDb {
    match shared_admin_url().await {
        Some(admin_url) => {
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
                run % 0xffff_ffff,
                NEXT_DB.fetch_add(1, Ordering::Relaxed)
            );
            let admin = MySqlPoolOptions::new()
                .max_connections(1)
                .connect(admin_url)
                .await
                .expect("connect to MYSQL_TEST_URL");
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE DATABASE `{name}`")))
                .execute(&admin)
                .await
                .expect("create the test database");
            admin.close().await;
            let opts = MySqlConnectOptions::from_str(admin_url)
                .expect("parse MYSQL_TEST_URL")
                .database(&name);
            let pool = MySqlPoolOptions::new()
                .max_connections(4)
                .connect_with(opts)
                .await
                .expect("connect to the test database");
            // The admin URL's path is its database; this one's is `name`.
            let url = match admin_url.rfind('/') {
                Some(i) if i > "mysql://".len() => format!("{}/{name}", &admin_url[..i]),
                _ => format!("{admin_url}/{name}"),
            };
            TestDb {
                pool,
                url,
                db_name: Some(name),
                admin_url: Some(admin_url.clone()),
                _container: None,
            }
        }
        None => {
            let container = start_container().await;
            let mut port = None;
            for attempt in 0..20 {
                match container.get_host_port_ipv4(3306).await {
                    Ok(p) => {
                        port = Some(p);
                        break;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(50 * (attempt + 1))).await,
                }
            }
            let port = port.expect("container port never appeared");
            let url = format!("mysql://root@127.0.0.1:{port}/test");
            let pool = MySqlPoolOptions::new()
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

/// Run `ddl`, one statement at a time.
pub async fn run_ddl(pool: &MySqlPool, ddl: &str) {
    for stmt in ddl.split(";\n").map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::raw_sql(sqlx::AssertSqlSafe(stmt.to_string()))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }
}
