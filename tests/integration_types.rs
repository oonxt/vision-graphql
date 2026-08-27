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
async fn read_jsonb_subkey_with_path_and_alias() {
    let (engine, _c) = setup().await;
    let res = engine
        .query(
            r#"query { events(where: {id: {_eq: 1}}) { id abundance: meta(path: "k") } }"#,
            None,
        )
        .await
        .expect("jsonb path read should work");
    // meta = {"k": 1}; `meta #> '{k}'` returns the jsonb value 1, renamed to `abundance`.
    assert_eq!(res, json!({"events": [{"id": 1, "abundance": 1}]}));
}

#[tokio::test]
async fn read_jsonb_nested_path_cascades_and_indexes() {
    let (engine, _c) = setup().await;
    engine
        .query(
            r#"
            mutation {
                insert_events(objects: [{
                    ext_id: "44444444-4444-4444-4444-444444444444",
                    created_at: "2026-07-01T00:00:00Z",
                    meta: {a: {b: [10, 20, 30]}}
                }]) { affected_rows }
            }
            "#,
            None,
        )
        .await
        .expect("insert nested jsonb");

    let res = engine
        .query(
            r#"query {
                events(where: {ext_id: {_eq: "44444444-4444-4444-4444-444444444444"}}) {
                    inner: meta(path: "a.b")
                    second: meta(path: "a.b.1")
                }
            }"#,
            None,
        )
        .await
        .expect("nested jsonb path read should work");
    // `#> '{a,b}'` keeps the jsonb array structure; `#> '{a,b,1}'` indexes it.
    assert_eq!(
        res,
        json!({"events": [{"inner": [10, 20, 30], "second": 20}]})
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

/// `smallint` used to vanish from the schema with only a log line — the column
/// was simply absent, and a query naming it was told it did not exist. Same for
/// `character(n)`.
#[tokio::test]
async fn smallint_and_char_are_readable_and_writable() {
    let container = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .expect("start pg");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
    sqlx::raw_sql(
        r#"CREATE TABLE items (
               id SERIAL PRIMARY KEY,
               qty SMALLINT NOT NULL,
               code CHAR(4),
               weird BYTEA
           );
           INSERT INTO items (qty, code) VALUES (3, 'ab  '), (9, 'cd  ');"#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let introspected = vision_graphql::Schema::introspect(&pool).await.unwrap();
    let engine = vision_graphql::Engine::new(
        pool.clone(),
        vision_graphql::Schema::introspect(&pool)
            .await
            .unwrap()
            .build(),
    );
    let _ = introspected;

    let v = engine
        .query("{ items(where: {qty: {_gt: 5}}) { id qty code } }", None)
        .await
        .unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 1);
    assert_eq!(v["items"][0]["qty"], 9);
    assert_eq!(v["items"][0]["code"], "cd  ");

    // Writing one too, and `_in` over the same type.
    let v = engine
        .query(
            r#"mutation { insert_items(objects: [{qty: 7, code: "ef"}]) {
                 returning { qty code } } }"#,
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["insert_items"]["returning"][0]["qty"], 7);

    let v = engine
        .query("{ items(where: {qty: {_in: [3, 7]}}) { qty } }", None)
        .await
        .unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 2);
}

/// A type with no mapping is still dropped — but it is recorded now, so
/// `vision-gql diff` can say so instead of leaving the hole invisible.
#[tokio::test]
async fn an_unmappable_column_is_recorded_rather_than_only_logged() {
    let container = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .expect("start pg");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE items (id SERIAL PRIMARY KEY, tags TEXT[], blob BYTEA, span INTERVAL);",
    )
    .execute(&pool)
    .await
    .unwrap();

    let db = vision_graphql::schema::introspect::introspect(&pool)
        .await
        .unwrap();
    let mut skipped: Vec<(&str, &str)> = db
        .skipped_columns
        .iter()
        .map(|c| (c.column.as_str(), c.data_type.as_str()))
        .collect();
    skipped.sort();
    assert_eq!(
        skipped,
        vec![("blob", "bytea"), ("span", "interval"), ("tags", "ARRAY")]
    );
}

/// A `numeric` column reads back as a JSON number, so a filter written with one
/// has to work: requiring `_gt: "10"` made every caller round-trip through
/// strings for no benefit.
#[tokio::test]
async fn numeric_accepts_the_json_numbers_it_returns() {
    let container = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .expect("start pg");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
    sqlx::raw_sql(
        r#"CREATE TABLE items (id SERIAL PRIMARY KEY, price NUMERIC(12,2));
           INSERT INTO items (price) VALUES (12.34), (99.99);"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    let engine = vision_graphql::Engine::new(
        pool.clone(),
        vision_graphql::Schema::introspect(&pool)
            .await
            .unwrap()
            .build(),
    );

    let v = engine
        .query("{ items(where: {price: {_gt: 50}}) { price } }", None)
        .await
        .unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 1);
    assert_eq!(v["items"][0]["price"], 99.99);

    let v = engine
        .query(
            r#"mutation { insert_items(objects: [{price: 5.25}]) { returning { price } } }"#,
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["insert_items"]["returning"][0]["price"], 5.25);

    // The string form still works, and is what a value a float cannot hold
    // exactly must use.
    let v = engine
        .query(
            r#"mutation { insert_items(objects: [{price: "0.10"}]) { returning { price } } }"#,
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["insert_items"]["returning"][0]["price"], 0.10);
}
