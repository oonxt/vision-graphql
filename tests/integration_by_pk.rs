use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .primary_key(&["id"]),
        )
        .build()
}

async fn setup() -> (
    Engine,
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .expect("start pg");
    let host_port = container.get_host_port_ipv4(5432).await.expect("port");
    let mut cfg = Config::new();
    cfg.host = Some("127.0.0.1".into());
    cfg.port = Some(host_port);
    cfg.user = Some("postgres".into());
    cfg.password = Some("postgres".into());
    cfg.dbname = Some("postgres".into());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).expect("pool");
    {
        let client = pool.get().await.expect("client");
        client
            .batch_execute(
                r#"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL
                );
                INSERT INTO users (name) VALUES ('alice'), ('bob'), ('cara');
                "#,
            )
            .await
            .expect("seed");
    }
    let engine = Engine::new(pool, schema());
    (engine, container)
}

#[tokio::test]
async fn by_pk_returns_object() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query("query { users_by_pk(id: 2) { id name } }", None)
        .await
        .expect("query ok");
    assert_eq!(v["users_by_pk"]["id"], json!(2));
    assert_eq!(v["users_by_pk"]["name"], json!("bob"));
}

#[tokio::test]
async fn by_pk_missing_row_returns_null() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query("query { users_by_pk(id: 9999) { id } }", None)
        .await
        .expect("query ok");
    assert!(v["users_by_pk"].is_null());
}

#[tokio::test]
async fn by_pk_with_variable() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query Q($uid: Int!) { users_by_pk(id: $uid) { name } }",
            Some(json!({"uid": 1})),
        )
        .await
        .expect("query ok");
    assert_eq!(v["users_by_pk"]["name"], json!("alice"));
}
