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
                .column("age", "age", PgType::Int4, true)
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
                    name TEXT NOT NULL UNIQUE,
                    age INT
                );
                "#,
            )
            .await
            .expect("seed");
    }
    let engine = Engine::new(pool, schema());
    (engine, container)
}

#[tokio::test]
async fn insert_array_returns_affected_rows_and_returning() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation { insert_users(objects: [{name: "alice", age: 30}, {name: "bob"}]) { affected_rows returning { id name } } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(2));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|r| r["name"] == json!("alice")));
}

#[tokio::test]
async fn insert_one_returns_single_object() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation { insert_users_one(object: {name: "cara"}) { id name } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let one = &v["insert_users_one"];
    assert_eq!(one["name"], json!("cara"));
    assert!(one["id"].is_number());
}

#[tokio::test]
async fn insert_with_on_conflict_do_update() {
    let (engine, _c) = setup().await;
    let _ = engine
        .query(
            r#"mutation { insert_users_one(object: {name: "dup", age: 1}) { id } }"#,
            None,
        )
        .await
        .expect("seed ok");
    let v: Value = engine
        .query(
            r#"mutation { insert_users(
                objects: [{name: "dup", age: 99}],
                on_conflict: {constraint: "users_name_key", update_columns: ["age"]}
            ) { affected_rows returning { name age } } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let ret = &v["insert_users"]["returning"];
    assert_eq!(ret[0]["name"], json!("dup"));
    assert_eq!(ret[0]["age"], json!(99));
}

#[tokio::test]
async fn update_by_where_affected_rows_and_returning() {
    let (engine, _c) = setup().await;
    let _ = engine
        .query(
            r#"mutation { insert_users(objects: [{name: "u1"}, {name: "u2"}]) { affected_rows } }"#,
            None,
        )
        .await
        .expect("seed ok");
    let v: Value = engine
        .query(
            r#"mutation { update_users(where: {name: {_eq: "u1"}}, _set: {age: 99}) { affected_rows returning { name age } } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["update_users"]["affected_rows"], json!(1));
    assert_eq!(v["update_users"]["returning"][0]["age"], json!(99));
}

#[tokio::test]
async fn update_by_pk_returns_object() {
    let (engine, _c) = setup().await;
    let seed: Value = engine
        .query(
            r#"mutation { insert_users_one(object: {name: "pk_user"}) { id } }"#,
            None,
        )
        .await
        .expect("seed ok");
    let id = seed["insert_users_one"]["id"].as_i64().unwrap();
    let v: Value = engine
        .query(
            &format!(
                r#"mutation {{ update_users_by_pk(pk_columns: {{id: {id}}}, _set: {{age: 42}}) {{ id name age }} }}"#
            ),
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["update_users_by_pk"]["age"], json!(42));
}

#[tokio::test]
async fn update_by_pk_missing_row_returns_null() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation { update_users_by_pk(pk_columns: {id: 99999}, _set: {age: 1}) { id } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert!(v["update_users_by_pk"].is_null());
}

#[tokio::test]
async fn delete_by_where() {
    let (engine, _c) = setup().await;
    let _ = engine
        .query(
            r#"mutation { insert_users(objects: [{name: "d1"}, {name: "d2"}]) { affected_rows } }"#,
            None,
        )
        .await
        .expect("seed ok");
    let v: Value = engine
        .query(
            r#"mutation { delete_users(where: {name: {_eq: "d1"}}) { affected_rows returning { name } } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["delete_users"]["affected_rows"], json!(1));
    assert_eq!(v["delete_users"]["returning"][0]["name"], json!("d1"));
}

#[tokio::test]
async fn delete_by_pk_missing_returns_null() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(r#"mutation { delete_users_by_pk(id: 99999) { id } }"#, None)
        .await
        .expect("mutation ok");
    assert!(v["delete_users_by_pk"].is_null());
}
