use serde_json::{json, Value};
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

mod common;

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

async fn setup() -> (Engine, common::TestDb) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
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
    (engine, db)
}

#[tokio::test]
async fn by_pk_returns_object() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query("query { users_by_pk(id: 2) { id name } }", None)
        .await
        .expect("query ok");
    assert_eq!(v["users_by_pk"]["id"], json!(2));
    assert_eq!(v["users_by_pk"]["name"], json!("bob"));
}

#[tokio::test]
async fn by_pk_missing_row_returns_null() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query("query { users_by_pk(id: 9999) { id } }", None)
        .await
        .expect("query ok");
    assert!(v["users_by_pk"].is_null());
}

#[tokio::test]
async fn by_pk_with_variable() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query(
            "query Q($uid: Int!) { users_by_pk(id: $uid) { name } }",
            Some(json!({"uid": 1})),
        )
        .await
        .expect("query ok");
    assert_eq!(v["users_by_pk"]["name"], json!("alice"));
}
