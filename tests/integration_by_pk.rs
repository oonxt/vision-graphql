use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
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
    let url = format!("postgres://postgres:postgres@127.0.0.1:{host_port}/postgres");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("pool");
    sqlx::raw_sql(
        r#"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL
                );
                INSERT INTO users (name) VALUES ('alice'), ('bob'), ('cara');
                "#,
    )
    .execute(&pool)
    .await
    .expect("seed");
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
