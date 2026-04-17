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
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")])),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .column("published", "published", PgType::Bool, false)
                .primary_key(&["id"])
                .relation("user", Relation::object("users").on([("user_id", "id")])),
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
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id),
                    published BOOL NOT NULL
                );
                INSERT INTO users (name) VALUES ('alice'), ('bob');
                INSERT INTO posts (title, user_id, published) VALUES
                    ('a1', 1, TRUE),
                    ('a2', 1, FALSE),
                    ('b1', 2, TRUE);
                "#,
            )
            .await
            .expect("seed");
    }

    let engine = Engine::new(pool, schema());
    (engine, container)
}

#[tokio::test]
async fn array_relation_returns_nested_rows() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { users(order_by: [{id: asc}]) { name posts(order_by: [{id: asc}]) { title } } }",
            None,
        )
        .await
        .expect("query ok");
    let users = v["users"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["name"], json!("alice"));
    assert_eq!(users[0]["posts"].as_array().unwrap().len(), 2);
    assert_eq!(users[0]["posts"][0]["title"], json!("a1"));
    assert_eq!(users[1]["posts"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn object_relation_returns_single_nested_row() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { posts(order_by: [{id: asc}]) { title user { name } } }",
            None,
        )
        .await
        .expect("query ok");
    let posts = v["posts"].as_array().unwrap();
    assert_eq!(posts.len(), 3);
    assert_eq!(posts[0]["user"]["name"], json!("alice"));
    assert_eq!(posts[2]["user"]["name"], json!("bob"));
}

#[tokio::test]
async fn nested_relation_args_limit_and_filter() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { users(order_by: [{id: asc}]) { name posts(where: {published: {_eq: true}}, limit: 5) { title } } }",
            None,
        )
        .await
        .expect("query ok");
    let alice_posts = v["users"][0]["posts"].as_array().unwrap();
    assert_eq!(alice_posts.len(), 1);
    assert_eq!(alice_posts[0]["title"], json!("a1"));
}

#[tokio::test]
async fn where_relation_exists_filter() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {posts: {title: {_eq: "a1"}}}) { name } }"#,
            None,
        )
        .await
        .expect("query ok");
    let users = v["users"].as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["name"], json!("alice"));
}
