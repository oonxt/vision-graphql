use std::process::Command;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};

async fn boot_pg() -> (
    String, // DATABASE_URL
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .expect("start pg");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    // Seed a tiny schema.
    let cfg: tokio_postgres::Config = url.parse().unwrap();
    let (client, conn) = cfg.connect(tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(async move { let _ = conn.await; });
    client
        .batch_execute(
            r#"
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                secret TEXT
            );
            CREATE TABLE posts (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                user_id INT NOT NULL REFERENCES users(id)
            );
            CREATE TABLE audit_log (
                id SERIAL PRIMARY KEY,
                msg TEXT NOT NULL
            );
            "#,
        )
        .await
        .expect("seed");

    (url, container)
}

#[tokio::test(flavor = "multi_thread")]
async fn generate_to_stdout_includes_all_tables() {
    let (url, _c) = boot_pg().await;
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args(["generate", "--url", &url])
        .output()
        .expect("run cli");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(s.contains("# ── public.users ─"));
    assert!(s.contains("# ── public.posts ─"));
    assert!(s.contains("# ── public.audit_log ─"));
}

#[tokio::test(flavor = "multi_thread")]
async fn generate_ignore_tables_filters() {
    let (url, _c) = boot_pg().await;
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args(["generate", "--url", &url, "--ignore-tables", "audit_*"])
        .output()
        .expect("run cli");
    assert!(out.status.success());
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(s.contains("public.users"));
    assert!(!s.contains("audit_log"));
}

fn write_temp_toml(name: &str, contents: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("vision-gql-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join(name);
    std::fs::write(&p, contents).unwrap();
    p
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_clean_overlay_exits_zero() {
    let (url, _c) = boot_pg().await;
    let p = write_temp_toml(
        "clean.toml",
        r#"
        [tables.users]
        expose_as = "profiles"
        hide_columns = ["secret"]
        "#,
    );
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args(["diff", "--url", &url, "--config", p.to_str().unwrap()])
        .output()
        .expect("run cli");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8(out.stdout).unwrap().contains("OK"));
    let _ = std::fs::remove_file(&p);
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_stale_hide_column_exits_one() {
    let (url, _c) = boot_pg().await;
    let p = write_temp_toml(
        "stale.toml",
        r#"
        [tables.users]
        hide_columns = ["password_hash"]
        "#,
    );
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args(["diff", "--url", &url, "--config", p.to_str().unwrap()])
        .output()
        .expect("run cli");
    assert_eq!(out.status.code(), Some(1));
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(s.contains("password_hash"));
    let _ = std::fs::remove_file(&p);
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_json_format_exits_one_with_machine_output() {
    let (url, _c) = boot_pg().await;
    let p = write_temp_toml(
        "stale_json.toml",
        r#"
        [tables.users]
        hide_columns = ["password_hash"]
        "#,
    );
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args([
            "diff",
            "--url",
            &url,
            "--config",
            p.to_str().unwrap(),
            "--format",
            "json",
        ])
        .output()
        .expect("run cli");
    assert_eq!(out.status.code(), Some(1));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["missing_columns"][0]["column"], "password_hash");
    let _ = std::fs::remove_file(&p);
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_ignore_tables_filters_findings() {
    let (url, _c) = boot_pg().await;
    let p = write_temp_toml(
        "ignored.toml",
        r#"
        [tables.users]
        hide_columns = ["password_hash"]
        "#,
    );
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args([
            "diff",
            "--url",
            &url,
            "--config",
            p.to_str().unwrap(),
            "--ignore-tables",
            "users",
        ])
        .output()
        .expect("run cli");
    assert!(out.status.success(), "ignored entry should disappear");
    let _ = std::fs::remove_file(&p);
}

#[tokio::test(flavor = "multi_thread")]
async fn generate_force_required_to_overwrite() {
    let (url, _c) = boot_pg().await;
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let dir = std::env::temp_dir().join(format!("vision-gql-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("schema.toml");

    let out = Command::new(bin)
        .args(["generate", "--url", &url, "-o", path.to_str().unwrap()])
        .output()
        .expect("run cli");
    assert!(out.status.success(), "first write must succeed");

    let out = Command::new(bin)
        .args(["generate", "--url", &url, "-o", path.to_str().unwrap()])
        .output()
        .expect("run cli");
    assert_eq!(out.status.code(), Some(2), "second write without --force must exit 2");

    let out = Command::new(bin)
        .args(["generate", "--url", &url, "-o", path.to_str().unwrap(), "--force"])
        .output()
        .expect("run cli");
    assert!(out.status.success(), "with --force must succeed");

    let _ = std::fs::remove_dir_all(&dir);
}
