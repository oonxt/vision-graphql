use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::ast::OrderDir;
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::{Engine, Mutation, Query};

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .column("age", "age", PgType::Int4, true)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")])),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
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
                    name TEXT NOT NULL,
                    age INT
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id)
                );
                INSERT INTO users (name, age) VALUES ('alice', 30), ('bob', 20);
                INSERT INTO posts (title, user_id) VALUES ('p1', 1), ('p2', 2);
                "#,
            )
            .await
            .expect("seed");
    }
    (Engine::new(pool, schema()), container)
}

#[tokio::test]
async fn builder_query_with_relation() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .run(
            Query::from("users")
                .select(&["name"])
                .with_relation("posts", Query::from("posts").select(&["title"]))
                .order_by("id", OrderDir::Asc),
        )
        .await
        .expect("run ok");
    let users = v["users"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["posts"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn builder_aggregate() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .run(Query::aggregate("users").count().sum(&["age"]))
        .await
        .expect("run ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["count"], json!(2));
    assert_eq!(v["users_aggregate"]["aggregate"]["sum"]["age"], json!(50));
}

#[tokio::test]
async fn builder_by_pk() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .run(Query::by_pk("users", &[("id", json!(1))]).select(&["name"]))
        .await
        .expect("run ok");
    assert_eq!(v["users_by_pk"]["name"], json!("alice"));
}

#[tokio::test]
async fn builder_insert_and_update() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .run(Mutation::insert_one("users", [("name", json!("cara"))]).returning(&["id", "name"]))
        .await
        .expect("insert ok");
    let id = v["insert_users_one"]["id"].as_i64().unwrap();

    let v: Value = engine
        .run(
            Mutation::update_by_pk("users", &[("id", json!(id as i32))])
                .set("age", json!(99))
                .select(&["age"]),
        )
        .await
        .expect("update ok");
    assert_eq!(v["update_users_by_pk"]["age"], json!(99));
}

#[tokio::test]
async fn builder_delete_by_where() {
    let (engine, _c) = setup().await;
    let _ = engine
        .run(Mutation::delete("posts").where_eq("user_id", 1))
        .await
        .expect("delete posts ok");
    let v: Value = engine
        .run(
            Mutation::delete("users")
                .where_eq("name", "alice")
                .returning(&["id"]),
        )
        .await
        .expect("delete ok");
    assert_eq!(v["delete_users"]["affected_rows"], json!(1));
}
