use serde_json::json;
use vision_graphql::predicate::{col, principal, Principal};
use vision_graphql::{Engine, QueryRegistry, Schema, ScopePolicy};

mod common;

async fn setup() -> (Engine, Schema) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"CREATE TABLE users (id SERIAL PRIMARY KEY, owner INT NOT NULL, name TEXT);
           INSERT INTO users (owner, name) VALUES (1, 'a'), (1, 'b'), (2, 'c');"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    let schema = Schema::introspect(&pool).await.unwrap().build();
    let engine = Engine::new(
        pool.clone(),
        Schema::introspect(&pool).await.unwrap().build(),
    );
    (engine, schema)
}

#[tokio::test]
async fn a_registry_compiled_at_startup_serves_requests_by_key() {
    let (engine, _schema) = setup().await;
    let registry = QueryRegistry::compile_all(
        &engine,
        [
            ("list", "query($n: Int!) { users(limit: $n) { id name } }"),
            ("one", "query($id: Int!) { users_by_pk(id: $id) { name } }"),
        ],
    )
    .unwrap();

    let data = engine
        .execute(registry.require("list").unwrap(), Some(json!({"n": 2})))
        .await
        .unwrap();
    assert_eq!(data["users"].as_array().unwrap().len(), 2);

    let data = engine
        .execute(registry.require("one").unwrap(), Some(json!({"id": 3})))
        .await
        .unwrap();
    assert_eq!(data["users_by_pk"]["name"], "c");

    // A key nobody registered never reaches the database.
    let err = registry.require("../../etc/passwd").unwrap_err();
    assert!(format!("{err}").contains("no query is registered"), "{err}");
}

#[tokio::test]
async fn a_scoped_registry_serves_every_principal_from_one_statement() {
    let (engine, schema) = setup().await;
    let policy = ScopePolicy::builder()
        .allow("users", col("owner").eq(principal()))
        .validate(&schema)
        .unwrap();
    let registry =
        QueryRegistry::compile_all_scoped(&engine, [("mine", "{ users { id name } }")], &policy)
            .unwrap();

    let q = registry.require("mine").unwrap();
    let mine = engine
        .execute_scoped(q, None, &Principal::new().set("principal", 1))
        .await
        .unwrap();
    assert_eq!(mine["users"].as_array().unwrap().len(), 2);

    let theirs = engine
        .execute_scoped(q, None, &Principal::new().set("principal", 2))
        .await
        .unwrap();
    assert_eq!(theirs["users"].as_array().unwrap().len(), 1);

    // The same statement served both; the principal decided the rows.
    assert!(q.is_scoped());
    let err = engine.execute(q, None).await.unwrap_err();
    assert!(
        format!("{err}").contains("compiled against a policy"),
        "{err}"
    );
}
