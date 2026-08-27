use serde_json::{json, Value};
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

mod common;

fn users_schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, true)
                .column("active", "active", PgType::Bool, false)
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
                    name TEXT,
                    active BOOL NOT NULL
                );
                INSERT INTO users (name, active) VALUES
                    ('alice', TRUE),
                    ('bob',   FALSE),
                    ('cara',  TRUE);
                "#,
    )
    .execute(&pool)
    .await
    .expect("seed");

    let engine = Engine::new(pool, users_schema());
    (engine, db)
}

#[tokio::test]
async fn plain_list_returns_all_rows() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query("query { users { id name } }", None)
        .await
        .expect("query ok");
    let users = v.get("users").and_then(Value::as_array).expect("array");
    assert_eq!(users.len(), 3);
    assert!(users.iter().any(|u| u["name"] == json!("alice")));
}

#[tokio::test]
async fn where_eq_with_variable() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query(
            "query Q($n: String!) { users(where: {name: {_eq: $n}}) { id name } }",
            Some(json!({"n": "bob"})),
        )
        .await
        .expect("query ok");
    let users = v.get("users").and_then(Value::as_array).expect("array");
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["name"], json!("bob"));
}

#[tokio::test]
async fn order_by_limit_offset() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query(
            "query { users(order_by: [{name: desc}], limit: 2, offset: 1) { name } }",
            None,
        )
        .await
        .expect("query ok");
    let users = v.get("users").and_then(Value::as_array).expect("array");
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["name"], json!("bob"));
    assert_eq!(users[1]["name"], json!("alice"));
}

#[tokio::test]
async fn sql_injection_attempt_is_bound_safely() {
    let (engine, _db) = setup().await;
    let nasty = "'); DROP TABLE users; --";
    let v: Value = engine
        .query(
            "query Q($n: String!) { users(where: {name: {_eq: $n}}) { id } }",
            Some(json!({"n": nasty})),
        )
        .await
        .expect("query ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 0);

    let v2 = engine
        .query("query { users { id } }", None)
        .await
        .expect("second query ok");
    assert_eq!(v2["users"].as_array().unwrap().len(), 3);
}

/// The third field of a GraphQL request body. A client that ships one document
/// holding every operation it might send picks one per request by name; without
/// this the document could only be run through `compile_with`.
#[tokio::test]
async fn an_operation_can_be_named() {
    let (engine, _db) = setup().await;
    const DOC: &str = r#"
        query Everyone { users { id } }
        query OnlyActive { users(where: {active: {_eq: true}}) { id name } }
        mutation Rename($id: Int!) {
          update_users_by_pk(pk_columns: {id: $id}, _set: {name: "renamed"}) { name }
        }
    "#;

    // Without a name, a document holding several says so rather than guessing.
    let err = engine.query(DOC, None).await.unwrap_err();
    assert!(
        format!("{err}").contains("operation_name required"),
        "{err}"
    );

    let all = engine
        .query_with(DOC, None, Some("Everyone"))
        .await
        .unwrap();
    assert_eq!(all["users"].as_array().unwrap().len(), 3);

    let active = engine
        .query_with(DOC, None, Some("OnlyActive"))
        .await
        .unwrap();
    assert_eq!(active["users"].as_array().unwrap().len(), 2);

    // Mutations too, and the variables travel with the operation.
    let renamed = engine
        .query_with(DOC, Some(json!({"id": 1})), Some("Rename"))
        .await
        .unwrap();
    assert_eq!(renamed["update_users_by_pk"]["name"], "renamed");

    // A name nobody defined is an error, not a fallback to the first operation.
    let err = engine
        .query_with(DOC, None, Some("Nonexistent"))
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("Nonexistent"), "{err}");

    // One operation and a name that matches it still works.
    let one = engine
        .query_with("query Solo { users { id } }", None, Some("Solo"))
        .await
        .unwrap();
    assert_eq!(one["users"].as_array().unwrap().len(), 3);

    // …and one operation with a name that does *not* match is still an error.
    // Running it anyway would answer a question about `Solo` when the client
    // asked about something else — the same silent substitution a multi-op
    // document refuses.
    let err = engine
        .query_with("query Solo { users { id } }", None, Some("Other"))
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("Other"), "{err}");

    // An anonymous operation cannot be picked by name either.
    let err = engine
        .query_with("{ users { id } }", None, Some("Anything"))
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("Anything"), "{err}");
}
