//! Views and materialized views as query targets.
//!
//! Two things make them different from a base table, and both are handled in the
//! schema layer rather than the renderer — which needs no view awareness at all,
//! since it only ever emits `FROM "schema"."name"`:
//!
//! * Materialized views are a Postgres extension and are absent from
//!   `information_schema` entirely, so they need a `pg_catalog` pass.
//! * Neither kind has constraints, so introspection finds no primary key. The
//!   config overlay is where a logical one is declared, which is what makes
//!   `_by_pk` available.

use serde_json::Value;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use vision_graphql::schema::Schema;
use vision_graphql::Engine;

async fn pool() -> (
    sqlx::PgPool,
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .expect("start pg");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
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
            active BOOL NOT NULL
        );
        INSERT INTO users (name, active) VALUES ('alice', TRUE), ('bob', TRUE), ('carol', FALSE);

        CREATE VIEW active_users AS
            SELECT id, name, active FROM users WHERE active;

        -- Deliberately types that carry a modifier: `format_type` renders these
        -- as `character varying(50)`, `numeric(10,2)` and `timestamp(3) with
        -- time zone`, and for the datetime one the modifier lands in the MIDDLE
        -- of the name, so trimming a suffix is not enough to map the type.
        CREATE MATERIALIZED VIEW mv_user_stats AS
            SELECT u.id,
                   u.name::varchar(50)            AS label,
                   (u.id * 1.5)::numeric(10,2)    AS score,
                   now()::timestamp(3) with time zone AS seen_at
            FROM users u;
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed");
    (pool, container)
}

async fn schema_with_config(pool: &sqlx::PgPool) -> Schema {
    Schema::introspect(pool)
        .await
        .expect("introspect")
        .load_config("tests/fixtures/views.toml")
        .expect("load toml")
        .build()
}

/// Materialized views are absent from `information_schema`, so before the
/// `pg_catalog` pass they were invisible: querying one failed with
/// "unknown root field".
#[tokio::test]
async fn materialized_view_is_introspected_and_queryable() {
    let (pool, _c) = pool().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();

    let mv = schema
        .table("mv_user_stats")
        .expect("a materialized view must be introspected");
    assert!(
        mv.read_only,
        "Postgres cannot write a materialized view at all; it must be read-only"
    );

    let engine = Engine::new(pool, schema);
    let v: Value = engine
        .query(
            "query { mv_user_stats(order_by: [{id: asc}]) { id label score } }",
            None,
        )
        .await
        .expect("select from a materialized view");
    let rows = v["mv_user_stats"].as_array().expect("rows");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["label"], "alice");
}

/// The columns above all carry a type modifier. `numeric(10,2)` and
/// `character varying(50)` put it at the end, but `timestamp(3) with time zone`
/// puts it in the middle — if the modifier is not stripped, the type fails to
/// map and the column is silently dropped from the schema.
#[tokio::test]
async fn materialized_view_columns_with_type_modifiers_are_mapped() {
    let (pool, _c) = pool().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    let mv = schema.table("mv_user_stats").expect("matview");

    for col in ["id", "label", "score", "seen_at"] {
        assert!(
            mv.find_column(col).is_some(),
            "column '{col}' was dropped — its type modifier was not stripped"
        );
    }

    // And the mapped types actually round-trip through a query.
    let engine = Engine::new(pool, schema);
    let v: Value = engine
        .query(
            "query { mv_user_stats(where: {id: {_eq: 2}}) { label score seen_at } }",
            None,
        )
        .await
        .expect("query the modifier-typed columns");
    let row = &v["mv_user_stats"][0];
    assert_eq!(row["label"], "bob");
    assert_eq!(row["score"], 3.0, "numeric(10,2) must survive");
    assert!(!row["seen_at"].is_null(), "timestamptz must survive");
}

/// A materialized view cannot be written by Postgres under any circumstances,
/// so it must never be handed mutation roots.
#[tokio::test]
async fn materialized_view_rejects_writes() {
    let (pool, _c) = pool().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    let engine = Engine::new(pool, schema);

    let msg = engine
        .query(
            r#"mutation { insert_mv_user_stats(objects: [{id: 9, label: "x"}]) { affected_rows } }"#,
            None,
        )
        .await
        .expect_err("a materialized view must reject writes")
        .to_string();
    assert!(msg.contains("read-only"), "got: {msg}");
}

/// Without a declared PK, `_by_pk` on a view is unavailable — a view has no
/// constraints for introspection to find.
#[tokio::test]
async fn by_pk_on_a_view_is_unavailable_until_declared() {
    let (pool, _c) = pool().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    assert!(
        schema
            .table("active_users")
            .expect("view")
            .primary_key
            .is_empty(),
        "introspection cannot find a PK on a view"
    );

    let engine = Engine::new(pool, schema);
    let msg = engine
        .query("query { active_users_by_pk(id: 1) { name } }", None)
        .await
        .expect_err("no PK declared, so no _by_pk")
        .to_string();
    assert!(msg.contains("no primary key"), "got: {msg}");
}

/// Declaring the logical PK in config is what turns `_by_pk` on — for both a
/// plain view and a materialized one.
#[tokio::test]
async fn config_primary_key_enables_by_pk_on_views() {
    let (pool, _c) = pool().await;
    let schema = schema_with_config(&pool).await;

    assert_eq!(
        schema.table("active_users").expect("view").primary_key,
        ["id"]
    );
    assert_eq!(
        schema.table("mv_user_stats").expect("matview").primary_key,
        ["id"]
    );

    let engine = Engine::new(pool, schema);

    let v: Value = engine
        .query("query { active_users_by_pk(id: 2) { name } }", None)
        .await
        .expect("_by_pk on a view with a declared PK");
    assert_eq!(v["active_users_by_pk"]["name"], "bob");

    let v2: Value = engine
        .query("query { mv_user_stats_by_pk(id: 3) { label } }", None)
        .await
        .expect("_by_pk on a materialized view with a declared PK");
    assert_eq!(v2["mv_user_stats_by_pk"]["label"], "carol");

    // A row the view filters out is simply not there, PK or not.
    let v3: Value = engine
        .query("query { active_users_by_pk(id: 3) { name } }", None)
        .await
        .expect("_by_pk for a row outside the view");
    assert!(
        v3["active_users_by_pk"].is_null(),
        "carol is inactive, the view does not contain her: {v3}"
    );
}

/// Declaring a PK must not make a read-only relation writable — the two knobs
/// are independent, and `update_<view>_by_pk` stays shut.
#[tokio::test]
async fn declaring_a_pk_does_not_make_a_view_writable() {
    let (pool, _c) = pool().await;
    let schema = schema_with_config(&pool).await;
    let engine = Engine::new(pool.clone(), schema);

    for m in [
        r#"mutation { update_active_users_by_pk(id: 1, _set: {name: "X"}) { id } }"#,
        r#"mutation { delete_active_users_by_pk(id: 1) { id } }"#,
    ] {
        let msg = engine
            .query(m, None)
            .await
            .expect_err("a PK does not unfreeze a view")
            .to_string();
        assert!(msg.contains("read-only"), "got: {msg}");
    }

    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM users ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read users");
    assert_eq!(names, ["alice", "bob", "carol"]);
}
