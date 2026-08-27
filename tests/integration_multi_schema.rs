//! Introspecting more than one Postgres schema.
//!
//! The engine has always rendered two-part names (`"schema"."table"`), so the
//! renderer needs nothing new here — what was missing was introspection, which
//! only ever looked at `public`, and a naming rule for the moment two schemas
//! both contain a table called `orders`.

use serde_json::Value;
use vision_graphql::schema::config::{ConfigOverlay, TableOverlay};
use vision_graphql::schema::Schema;
use vision_graphql::Engine;

mod common;

async fn setup_pool() -> (sqlx::PgPool, common::TestDb) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();

    sqlx::raw_sql(
        r#"
        CREATE SCHEMA app;
        CREATE SCHEMA audit;
        CREATE SCHEMA archive;

        CREATE TABLE audit.actors (
            id   SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
        -- A foreign key that crosses schemas.
        CREATE TABLE app.orders (
            id       SERIAL PRIMARY KEY,
            total    INT NOT NULL,
            actor_id INT NOT NULL REFERENCES audit.actors(id)
        );
        -- Competes with app.orders for the exposed name `orders`.
        CREATE TABLE audit.orders (
            id   SERIAL PRIMARY KEY,
            note TEXT NOT NULL
        );
        -- Same shape as app.orders, different rows: the overlay repoint target.
        CREATE TABLE archive.orders (
            id       SERIAL PRIMARY KEY,
            total    INT NOT NULL,
            actor_id INT NOT NULL
        );

        INSERT INTO audit.actors (name) VALUES ('alice'), ('bob');
        INSERT INTO app.orders (total, actor_id) VALUES (10, 1), (20, 2), (30, 1);
        INSERT INTO audit.orders (note) VALUES ('an audit row');
        INSERT INTO archive.orders (total, actor_id) VALUES (999, 1);
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed");
    (pool, db)
}

#[tokio::test]
async fn first_schema_owns_bare_names_the_rest_are_prefixed() {
    let (pool, _db) = setup_pool().await;
    let schema = Schema::introspect_schemas(&pool, &["app", "audit"])
        .await
        .expect("introspect")
        .build();

    let orders = schema.table("orders").expect("app.orders keeps `orders`");
    assert_eq!(orders.physical_schema, "app");
    assert!(orders.find_column("total").is_some());

    let audit_orders = schema
        .table("audit_orders")
        .expect("audit.orders is exposed prefixed");
    assert_eq!(audit_orders.physical_schema, "audit");
    assert_eq!(audit_orders.physical_name, "orders");
    assert!(audit_orders.find_column("note").is_some());

    assert!(schema.table("audit_actors").is_some());
}

/// The payoff: an FK across schemas reads exactly like a same-schema one, in a
/// single SQL statement.
#[tokio::test]
async fn cross_schema_relation_queries_end_to_end() {
    let (pool, _db) = setup_pool().await;
    let schema = Schema::introspect_schemas(&pool, &["app", "audit"])
        .await
        .expect("introspect")
        .build();
    let engine = Engine::new(pool, schema);

    // Object relation: order -> actor, walking app -> audit.
    let v: Value = engine
        .query(
            "query { orders(order_by: {total: asc}) { total audit_actor { name } } }",
            None,
        )
        .await
        .expect("query ok");
    let orders = v["orders"].as_array().expect("orders array");
    assert_eq!(orders.len(), 3);
    assert_eq!(orders[0]["total"], 10);
    assert_eq!(orders[0]["audit_actor"]["name"], "alice");
    assert_eq!(orders[1]["audit_actor"]["name"], "bob");

    // Array relation the other way: actor -> orders, walking audit -> app.
    let v: Value = engine
        .query(
            "query { audit_actors(order_by: {id: asc}) { name orders { total } } }",
            None,
        )
        .await
        .expect("query ok");
    let actors = v["audit_actors"].as_array().expect("actors array");
    assert_eq!(actors.len(), 2);
    assert_eq!(actors[0]["name"], "alice");
    assert_eq!(
        actors[0]["orders"].as_array().unwrap().len(),
        2,
        "alice has two orders in app.orders"
    );
    assert_eq!(actors[1]["orders"].as_array().unwrap().len(), 1);
}

/// A relation filter crosses schemas too — it renders as an `EXISTS` subquery
/// against the other schema's table.
#[tokio::test]
async fn cross_schema_relation_filter_works() {
    let (pool, _db) = setup_pool().await;
    let schema = Schema::introspect_schemas(&pool, &["app", "audit"])
        .await
        .expect("introspect")
        .build();
    let engine = Engine::new(pool, schema);

    let v: Value = engine
        .query(
            r#"query { orders(where: {audit_actor: {name: {_eq: "bob"}}}) { total } }"#,
            None,
        )
        .await
        .expect("query ok");
    let orders = v["orders"].as_array().expect("orders array");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0]["total"], 20);
}

/// Only what was asked for gets introspected — and an FK reaching into a schema
/// that was left out produces no relation, since there would be no table in the
/// schema for it to name.
#[tokio::test]
async fn unlisted_schemas_are_not_introspected() {
    let (pool, _db) = setup_pool().await;
    let schema = Schema::introspect_schemas(&pool, &["app"])
        .await
        .expect("introspect")
        .build();

    assert!(schema.table("orders").is_some());
    assert!(schema.table("audit_actors").is_none());
    assert!(schema.table("audit_orders").is_none());
    assert!(
        schema
            .table("orders")
            .unwrap()
            .find_relation("audit_actor")
            .is_none(),
        "the FK target is outside the introspected set"
    );
}

/// `public` alone still behaves exactly as it did: bare names, no prefixes, and
/// nothing from the other schemas leaks in.
#[tokio::test]
async fn default_introspect_still_sees_only_public() {
    let (pool, _db) = setup_pool().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();

    assert!(schema.table("orders").is_none());
    assert!(schema.table("app_orders").is_none());
    assert!(schema.table("audit_actors").is_none());
}

/// The overlay's `schema` moves an exposed table to a different physical schema
/// without changing its columns or its name.
#[tokio::test]
async fn overlay_schema_repoints_reads_to_another_schema() {
    let (pool, _db) = setup_pool().await;

    let mut cfg = ConfigOverlay::default();
    cfg.tables.insert(
        "orders".into(),
        TableOverlay {
            schema: Some("archive".into()),
            ..Default::default()
        },
    );
    let schema = Schema::introspect_schemas(&pool, &["app", "audit"])
        .await
        .expect("introspect")
        .apply_config(&cfg)
        .build();

    assert_eq!(schema.table("orders").unwrap().physical_schema, "archive");

    let engine = Engine::new(pool, schema);
    let v: Value = engine
        .query("query { orders { total } }", None)
        .await
        .expect("query ok");
    let orders = v["orders"].as_array().expect("orders array");
    assert_eq!(orders.len(), 1, "archive.orders holds a single row");
    assert_eq!(
        orders[0]["total"], 999,
        "rows came from archive.orders, not app.orders"
    );
}
