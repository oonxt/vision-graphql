//! Integration coverage for "stringly" PostgreSQL types: uuid, numeric,
//! timestamptz, and jsonb. These are encoded as text binds and rely on
//! explicit casts so the server converts them.

use serde_json::json;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

const UUID_A: &str = "11111111-1111-1111-1111-111111111111";
const UUID_B: &str = "22222222-2222-2222-2222-222222222222";

fn events_schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("events", "public", "events")
                .column("id", "id", PgType::Int4, false)
                .column("ext_id", "ext_id", PgType::Uuid, false)
                .column("amount", "amount", PgType::Numeric, true)
                .column("created_at", "created_at", PgType::TimestampTz, false)
                .column("meta", "meta", PgType::Jsonb, true)
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

    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        r#"
        CREATE TABLE events (
            id SERIAL PRIMARY KEY,
            ext_id UUID NOT NULL,
            amount NUMERIC,
            created_at TIMESTAMPTZ NOT NULL,
            meta JSONB
        );
        INSERT INTO events (ext_id, amount, created_at, meta) VALUES
            ('{UUID_A}', 12.50, '2026-01-01T00:00:00Z', '{{"k": 1}}'),
            ('{UUID_B}', 99.99, '2026-03-01T00:00:00Z', NULL);
        "#
    )))
    .execute(&pool)
    .await
    .expect("seed");

    (Engine::new(pool, events_schema()), container)
}

#[tokio::test]
async fn filter_by_uuid_eq() {
    let (engine, _c) = setup().await;
    let res = engine
        .query(
            r#"query { events(where: {ext_id: {_eq: "11111111-1111-1111-1111-111111111111"}}) { id ext_id } }"#,
            None,
        )
        .await
        .expect("uuid _eq filter should work");
    assert_eq!(res, json!({"events": [{"id": 1, "ext_id": UUID_A}]}));
}

#[tokio::test]
async fn filter_by_timestamptz_gt() {
    let (engine, _c) = setup().await;
    let res = engine
        .query(
            r#"query { events(where: {created_at: {_gt: "2026-02-01T00:00:00Z"}}) { id } }"#,
            None,
        )
        .await
        .expect("timestamptz _gt filter should work");
    assert_eq!(res, json!({"events": [{"id": 2}]}));
}

#[tokio::test]
async fn filter_by_numeric_gt() {
    let (engine, _c) = setup().await;
    let res = engine
        .query(
            r#"query { events(where: {amount: {_gt: "50"}}) { id } }"#,
            None,
        )
        .await
        .expect("numeric _gt filter should work");
    assert_eq!(res, json!({"events": [{"id": 2}]}));
}

#[tokio::test]
async fn insert_with_stringly_types() {
    let (engine, _c) = setup().await;
    let res = engine
        .query(
            r#"
            mutation {
                insert_events(objects: [{
                    ext_id: "33333333-3333-3333-3333-333333333333",
                    amount: "7.25",
                    created_at: "2026-06-01T12:00:00Z",
                    meta: {tags: ["a", "b"]}
                }]) {
                    affected_rows
                    returning { id ext_id amount meta }
                }
            }
            "#,
            None,
        )
        .await
        .expect("insert with uuid/numeric/timestamptz/jsonb should work");
    assert_eq!(res["insert_events"]["affected_rows"], json!(1));
    assert_eq!(
        res["insert_events"]["returning"][0]["ext_id"],
        json!("33333333-3333-3333-3333-333333333333")
    );
    assert_eq!(
        res["insert_events"]["returning"][0]["meta"],
        json!({"tags": ["a", "b"]})
    );
}

#[tokio::test]
async fn filter_uuid_in_list() {
    let (engine, _c) = setup().await;
    let res = engine
        .query(
            &format!(r#"query {{ events(where: {{ext_id: {{_in: ["{UUID_A}", "{UUID_B}"]}}}}, order_by: {{id: asc}}) {{ id }} }}"#),
            None,
        )
        .await
        .expect("uuid _in filter should work");
    assert_eq!(res, json!({"events": [{"id": 1}, {"id": 2}]}));
}
