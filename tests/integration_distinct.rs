use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::Value;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

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
            .await
            .expect("seed");
    }
    let engine = Engine::new(pool, schema());
    (engine, container)
}

#[tokio::test]
async fn distinct_on_kind_returns_one_per_kind() {
    let (engine, _c) = setup().await;
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
