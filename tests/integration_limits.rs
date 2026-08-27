use serde_json::json;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use vision_graphql::limits::ExecutionLimits;
use vision_graphql::{Engine, Query, Schema};

async fn boot() -> (
    String,
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
) {
    let c = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .unwrap();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&url)
        .await
        .unwrap();
    sqlx::raw_sql(
        r#"CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
           CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INT NOT NULL REFERENCES users(id));
           INSERT INTO users (name) SELECT 'u' || g FROM generate_series(1, 50) g;
           INSERT INTO posts (user_id) SELECT (g % 50) + 1 FROM generate_series(1, 200) g;
           CREATE VIEW slow AS SELECT 1::int AS id, pg_sleep(3) IS NULL AS done;"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
    (url, c)
}

async fn engine(url: &str, limits: ExecutionLimits) -> Engine {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(url)
        .await
        .unwrap();
    let schema = Schema::introspect(&pool).await.unwrap().build();
    Engine::new(pool, schema).with_limits(limits)
}

#[tokio::test]
async fn default_limit_bounds_a_query_that_asked_for_nothing() {
    let (url, _c) = boot().await;
    let e = engine(&url, ExecutionLimits::new().default_limit(10)).await;

    let v = e.query("{ users { id } }", None).await.unwrap();
    assert_eq!(
        v["users"].as_array().unwrap().len(),
        10,
        "root list is capped"
    );

    let v = e
        .query("{ users(limit: 3) { id posts { id } } }", None)
        .await
        .unwrap();
    assert_eq!(
        v["users"].as_array().unwrap().len(),
        3,
        "an explicit limit wins"
    );
    for u in v["users"].as_array().unwrap() {
        assert!(
            u["posts"].as_array().unwrap().len() <= 10,
            "relations are capped too"
        );
    }

    // The typed builder bypasses the parser entirely, so this is the path that
    // would go unbounded if the check lived anywhere but the IR.
    let v = e.run(Query::from("users").select(&["id"])).await.unwrap();
    assert_eq!(v["users"].as_array().unwrap().len(), 10);

    // A count still counts every row: capping an aggregate would change the
    // answer rather than the cost.
    let v = e
        .query("{ users_aggregate { aggregate { count } } }", None)
        .await
        .unwrap();
    assert_eq!(v["users_aggregate"]["aggregate"]["count"], 50);
}

#[tokio::test]
async fn ceilings_are_refused_rather_than_silently_clamped() {
    let (url, _c) = boot().await;
    let e = engine(
        &url,
        ExecutionLimits::new()
            .max_limit(20)
            .max_table_reads(3)
            .max_relation_depth(1),
    )
    .await;

    let err = e
        .query("{ users(limit: 500) { id } }", None)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("over the limit of 20"), "{err}");

    // Through a variable, checked when it resolves — including on a compiled
    // statement, which was rendered before any value existed.
    let compiled = e
        .compile("query($n: Int) { users(limit: $n) { id } }")
        .unwrap();
    let err = e
        .execute(&compiled, Some(json!({"n": 500})))
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("over the limit of 20"), "{err}");
    let ok = e.execute(&compiled, Some(json!({"n": 2}))).await.unwrap();
    assert_eq!(ok["users"].as_array().unwrap().len(), 2);

    let err = e
        .query(
            "{ users { a: posts { id } b: posts { id } c: posts { id } } }",
            None,
        )
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("table positions"), "{err}");

    let err = e
        .query("{ users { posts { user { id } } } }", None)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("deeper than the limit"), "{err}");
}

/// The engine renders one statement per request, so a `statement_timeout` set
/// on the connection governs every query it sends — no per-request round trip,
/// and nothing for the engine itself to implement. This is the test that says so.
#[tokio::test]
async fn a_connection_level_statement_timeout_governs_engine_queries() {
    let (url, _c) = boot().await;
    let opts: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_with(opts.options([("statement_timeout", "300ms")]))
        .await
        .unwrap();
    let schema = Schema::introspect(&pool).await.unwrap().build();
    let e = Engine::new(pool, schema);

    // `slow` sleeps three seconds; the timeout is 300ms.
    let err = e.query("{ slow { id } }", None).await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("statement timeout") || msg.contains("57014"),
        "expected a timeout, got {msg}"
    );

    // The connection is still usable afterwards.
    let v = e.query("{ users(limit: 1) { id } }", None).await.unwrap();
    assert_eq!(v["users"].as_array().unwrap().len(), 1);
}
