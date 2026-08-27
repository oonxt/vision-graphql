use serde_json::{json, Value};
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

mod common;

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

async fn setup() -> (Engine, common::TestDb) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
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
    (Engine::new(pool, schema()), db)
}

#[tokio::test]
async fn in_operator_matches_multiple_values() {
    let (engine, _db) = setup().await;
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
async fn nin_operator_excludes_values_and_nulls() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_nin: ["alice"]}}) { name } }"#,
            None,
        )
        .await
        .expect("query ok");
    // bob + carol; the NULL-name row never matches, same as SQL NOT IN.
    assert_eq!(v["users"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn empty_in_list_matches_nothing() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_in: []}}) { name } }"#,
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn like_matches_pattern() {
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
