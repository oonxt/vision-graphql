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
                .column("email", "email", PgType::Text, true)
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
                    name TEXT NOT NULL CONSTRAINT organizations_name_key UNIQUE
                );
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL CONSTRAINT users_name_key UNIQUE,
                    email TEXT,
                    organization_id INT REFERENCES organizations(id)
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL CONSTRAINT posts_title_key UNIQUE,
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
async fn nested_object_on_conflict_do_nothing_links_to_existing() {
    let (engine, _c) = setup().await;

    let seeded: Value = engine
        .query(
            r#"mutation {
                 insert_users_one(object: { name: "alice", email: "old@e.com" }) { id }
               }"#,
            None,
        )
        .await
        .expect("seed ok");
    let alice_id = seeded["insert_users_one"]["id"].as_i64().unwrap();

    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "p1",
                   user: {
                     data: { name: "alice", email: "ignored@e.com" },
                     on_conflict: { constraint: "users_name_key", update_columns: [] }
                   }
                 }]) {
                   affected_rows
                   returning { title user { id email } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");

    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("p1"));
    assert_eq!(row["user"]["id"].as_i64().unwrap(), alice_id);
    // Email unchanged — DO NOTHING means existing row wins.
    assert_eq!(row["user"]["email"], json!("old@e.com"));
}

#[tokio::test]
async fn nested_wrapper_unknown_key_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "t",
                   user: { data: { name: "alice" }, foo: "bar" }
                 }]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("'data' and 'on_conflict'"),
        "error should mention both supported keys; was: {msg}"
    );
}

#[tokio::test]
async fn nested_on_conflict_missing_constraint_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "t",
                   user: {
                     data: { name: "alice" },
                     on_conflict: { update_columns: [] }
                   }
                 }]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("'constraint'"),
        "error should mention missing constraint; was: {msg}"
    );
}

#[tokio::test]
async fn nested_object_on_conflict_do_update_updates_existing() {
    let (engine, _c) = setup().await;

    let seeded: Value = engine
        .query(
            r#"mutation {
                 insert_users_one(object: { name: "alice", email: "old@e.com" }) { id }
               }"#,
            None,
        )
        .await
        .expect("seed ok");
    let alice_id = seeded["insert_users_one"]["id"].as_i64().unwrap();

    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "p1",
                   user: {
                     data: { name: "alice", email: "new@e.com" },
                     on_conflict: { constraint: "users_name_key", update_columns: ["email"] }
                   }
                 }]) {
                   returning { title user { id email } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("p1"));
    assert_eq!(row["user"]["id"].as_i64().unwrap(), alice_id);
    assert_eq!(row["user"]["email"], json!("new@e.com"));
}
