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
                .column("name", "name", PgType::Text, false)
                .column("score", "score", PgType::Int4, false)
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
                    name TEXT NOT NULL,
                    score INT NOT NULL
                );
                INSERT INTO users (name, score) VALUES
                    ('alice', 10),
                    ('bob',   20),
                    ('cara',  30);
                "#,
    )
    .execute(&pool)
    .await
    .expect("seed");

    let engine = Engine::new(pool, schema());
    (engine, container)
}

#[tokio::test]
async fn aggregate_count_returns_row_count() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query("query { users_aggregate { aggregate { count } } }", None)
        .await
        .expect("query ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["count"], json!(3));
}

#[tokio::test]
async fn aggregate_sum_and_avg() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { users_aggregate { aggregate { sum { score } avg { score } } } }",
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["sum"]["score"], json!(60));
    assert_eq!(
        v["users_aggregate"]["aggregate"]["avg"]["score"],
        json!(20.0)
    );
}

#[tokio::test]
async fn aggregate_with_nodes() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { users_aggregate(where: {score: {_gte: 20}}) { aggregate { count } nodes { name } } }",
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["count"], json!(2));
    let nodes = v["users_aggregate"]["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);
}

#[tokio::test]
async fn aggregate_max_min() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { users_aggregate { aggregate { max { score } min { score } } } }",
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["max"]["score"], json!(30));
    assert_eq!(v["users_aggregate"]["aggregate"]["min"]["score"], json!(10));
}
