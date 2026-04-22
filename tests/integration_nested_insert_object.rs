use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::Engine;

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("organizations", "public", "organizations")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .primary_key(&["id"]),
        )
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .column("organization_id", "organization_id", PgType::Int4, true)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")]))
                .relation(
                    "organization",
                    Relation::object("organizations").on([("organization_id", "id")]),
                ),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .primary_key(&["id"])
                .relation("user", Relation::object("users").on([("user_id", "id")]))
                .relation("comments", Relation::array("comments").on([("id", "post_id")])),
        )
        .table(
            Table::new("comments", "public", "comments")
                .column("id", "id", PgType::Int4, false)
                .column("body", "body", PgType::Text, false)
                .column("post_id", "post_id", PgType::Int4, false)
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
                CREATE TABLE organizations (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL
                );
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    organization_id INT REFERENCES organizations(id)
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id)
                );
                CREATE TABLE comments (
                    id SERIAL PRIMARY KEY,
                    body TEXT NOT NULL,
                    post_id INT NOT NULL REFERENCES posts(id)
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
async fn insert_post_with_nested_user() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [
                   { title: "p1", user: { data: { name: "alice" } } }
                 ]) {
                   affected_rows
                   returning { title user { name } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_posts"]["affected_rows"], json!(2));
    let rows = v["insert_posts"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["title"], json!("p1"));
    assert_eq!(rows[0]["user"]["name"], json!("alice"));
}

#[tokio::test]
async fn nested_object_missing_data_key_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{ title: "t", user: {} }]) {
                   affected_rows
                 }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(msg.contains("'data'"), "error was: {msg}");
}

#[tokio::test]
async fn nested_object_array_data_is_error() {
    let (engine, _c) = setup().await;
    // object-relation 'data' must be an object, not an array.
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [
                   { title: "t", user: { data: [{ name: "x" }] } }
                 ]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("must be a single object, not an array"),
        "error was: {msg}"
    );
}

#[tokio::test]
async fn nested_object_fk_and_nested_both_set_is_error() {
    let (engine, _c) = setup().await;
    // Can't both set user_id AND provide a nested user.
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [
                   { title: "t", user_id: 99, user: { data: { name: "x" } } }
                 ]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("populated from the nested object"),
        "error was: {msg}"
    );
}

#[tokio::test]
async fn nested_object_mixed_batch_is_error() {
    let (engine, _c) = setup().await;
    // Row 1 uses nested `user`, row 2 uses explicit user_id — rejected.
    // Note: user_id=99 doesn't exist, but the parser should reject BEFORE executing SQL.
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [
                   { title: "p1", user: { data: { name: "alice" } } },
                   { title: "p2", user_id: 99 }
                 ]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("must be uniform"),
        "error was: {msg}"
    );
}

#[tokio::test]
async fn insert_batch_with_nested_users() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [
                   { title: "p1", user: { data: { name: "alice" } } },
                   { title: "p2", user: { data: { name: "bob"   } } }
                 ]) {
                   affected_rows
                   returning { title user { name } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_posts"]["affected_rows"], json!(4));
    let rows = v["insert_posts"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 2);

    let p1 = rows.iter().find(|r| r["title"] == json!("p1")).expect("p1");
    assert_eq!(p1["user"]["name"], json!("alice"));

    let p2 = rows.iter().find(|r| r["title"] == json!("p2")).expect("p2");
    assert_eq!(p2["user"]["name"], json!("bob"));
}

#[tokio::test]
async fn insert_post_with_nested_user_and_comments() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "p1",
                   user:     { data: { name: "alice" } },
                   comments: { data: [{ body: "c1" }, { body: "c2" }] }
                 }]) {
                   affected_rows
                   returning {
                     title
                     user { name }
                     comments(order_by: [{ id: asc }]) { body }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_posts"]["affected_rows"], json!(4));
    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("p1"));
    assert_eq!(row["user"]["name"], json!("alice"));
    let bodies: Vec<_> = row["comments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["body"].clone())
        .collect();
    assert_eq!(bodies, vec![json!("c1"), json!("c2")]);
}
