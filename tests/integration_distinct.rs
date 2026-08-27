use serde_json::Value;
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

mod common;

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("events", "public", "events")
                .column("id", "id", PgType::Int4, false)
                .column("kind", "kind", PgType::Text, false)
                .column("ts", "ts", PgType::Int8, false)
                .primary_key(&["id"]),
        )
        .build()
}

async fn setup() -> (Engine, common::TestDb) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"
                CREATE TABLE events (
                    id SERIAL PRIMARY KEY,
                    kind TEXT NOT NULL,
                    ts BIGINT NOT NULL
                );
                INSERT INTO events (kind, ts) VALUES
                    ('click', 10),
                    ('click', 20),
                    ('view',  15),
                    ('view',  25);
                "#,
    )
    .execute(&pool)
    .await
    .expect("seed");
    let engine = Engine::new(pool, schema());
    (engine, db)
}

#[tokio::test]
async fn distinct_on_kind_returns_one_per_kind() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query("query { events(distinct_on: [kind]) { kind ts } }", None)
        .await
        .expect("query ok");
    let rows = v["events"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    let kinds: Vec<&str> = rows.iter().map(|r| r["kind"].as_str().unwrap()).collect();
    assert!(kinds.contains(&"click"));
    assert!(kinds.contains(&"view"));
}
