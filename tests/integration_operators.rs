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
                .column("name", "name", PgType::Text, true)
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
                    name TEXT
                );
                INSERT INTO users (name) VALUES ('alice'), ('bob'), ('carol'), (NULL);
                "#,
    )
    .execute(&pool)
    .await
    .expect("seed");
    (Engine::new(pool, schema()), container)
}

#[tokio::test]
async fn in_operator_matches_multiple_values() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_in: ["alice", "bob"]}}) { name } }"#,
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn like_matches_pattern() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_like: "a%"}}) { name } }"#,
            None,
        )
        .await
        .expect("query ok");
    let arr = v["users"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], json!("alice"));
}

#[tokio::test]
async fn ilike_case_insensitive() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_ilike: "ALICE"}}) { name } }"#,
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn is_null_filter() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_is_null: true}}) { id } }"#,
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn is_not_null_filter() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_is_null: false}}) { id } }"#,
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn named_fragment_works_against_db() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"
            fragment UF on users { id name }
            query { users(where: {name: {_eq: "alice"}}) { ...UF } }
            "#,
            None,
        )
        .await
        .expect("query ok");
    let arr = v["users"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], json!("alice"));
}
