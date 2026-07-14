//! Read-only tables: no mutation roots, no nested-insert target.
//!
//! Mutation roots are derived from the exposed table name by prefix, so anything
//! in the schema is writable unless it says otherwise — and introspection puts
//! views in the schema, since they have columns in `information_schema` like any
//! other relation. Postgres auto-updates a *simple* view straight through to its
//! base table, so before this guard existed `insert_active_users` did not fail:
//! it wrote a row into `users`. These tests pin that shut.

use serde_json::Value;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use vision_graphql::schema::Schema;
use vision_graphql::Engine;

async fn pool_with_views() -> (
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
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            user_id INT NOT NULL REFERENCES users(id),
            title TEXT NOT NULL
        );
        INSERT INTO users (name, active) VALUES ('alice', TRUE), ('carol', FALSE);
        INSERT INTO orders (user_id, title) VALUES (1, 'a-1');

        -- Simple view: Postgres treats this as AUTO-UPDATABLE, so writes to it
        -- land in `users` unless something stops them.
        CREATE VIEW active_users AS
            SELECT id, name, active FROM users WHERE active;

        -- Same shape, but the config overlay marks it writable again.
        CREATE VIEW writable_users AS
            SELECT id, name, active FROM users WHERE active;
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed");
    (pool, container)
}

async fn user_names(pool: &sqlx::PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT name FROM users ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("read users")
}

/// The hole this closes: every write aimed at an introspected view must be
/// rejected, and the base table behind it must be untouched.
#[tokio::test]
async fn introspected_view_rejects_every_write() {
    let (pool, _c) = pool_with_views().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    let view = schema.table("active_users").expect("view is introspected");
    assert!(view.read_only, "a VIEW must be introspected as read-only");

    let engine = Engine::new(pool.clone(), schema);
    for m in [
        r#"mutation { insert_active_users(objects: [{name: "mallory", active: true}]) { affected_rows } }"#,
        r#"mutation { insert_active_users_one(object: {name: "mallory", active: true}) { id } }"#,
        r#"mutation { update_active_users(where: {name: {_eq: "alice"}}, _set: {name: "ALICE"}) { affected_rows } }"#,
        r#"mutation { delete_active_users(where: {name: {_eq: "alice"}}) { affected_rows } }"#,
    ] {
        let err = engine
            .query(m, None)
            .await
            .expect_err("a write to a view must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("read-only"),
            "error must name the cause, got: {msg}"
        );
    }

    // The point of the guard: nothing reached the base table.
    assert_eq!(
        user_names(&pool).await,
        ["alice", "carol"],
        "no write may reach the table behind the view"
    );
}

/// `_by_pk` writes go through a different parser arm — they must be guarded too.
/// The view has no PK, so this also pins the order of the two checks: read-only
/// is reported before "no primary key", because it is the more fundamental fact.
#[tokio::test]
async fn view_by_pk_writes_are_rejected_as_read_only() {
    let (pool, _c) = pool_with_views().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    let engine = Engine::new(pool.clone(), schema);

    for m in [
        r#"mutation { update_active_users_by_pk(id: 1, _set: {name: "X"}) { id } }"#,
        r#"mutation { delete_active_users_by_pk(id: 1) { id } }"#,
    ] {
        let msg = engine
            .query(m, None)
            .await
            .expect_err("by_pk write to a view must be rejected")
            .to_string();
        assert!(msg.contains("read-only"), "got: {msg}");
    }
    assert_eq!(user_names(&pool).await, ["alice", "carol"]);
}

/// Read-only is about writes only — the view stays fully queryable.
#[tokio::test]
async fn read_only_view_is_still_fully_queryable() {
    let (pool, _c) = pool_with_views().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    let engine = Engine::new(pool, schema);

    let v: Value = engine
        .query(
            "query { active_users(order_by: [{name: asc}]) { id name } }",
            None,
        )
        .await
        .expect("a read-only view still selects");
    assert_eq!(v["active_users"][0]["name"], "alice");
    assert_eq!(
        v["active_users"].as_array().expect("array").len(),
        1,
        "carol is inactive, the view filters her out"
    );

    let agg: Value = engine
        .query(
            "query { active_users_aggregate { aggregate { count } } }",
            None,
        )
        .await
        .expect("a read-only view still aggregates");
    assert_eq!(agg["active_users_aggregate"]["aggregate"]["count"], 1);
}

/// Base tables are untouched by all this: they stay writable.
#[tokio::test]
async fn base_table_stays_writable() {
    let (pool, _c) = pool_with_views().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    assert!(
        !schema.table("users").expect("users").read_only,
        "a BASE TABLE must not be marked read-only"
    );

    let engine = Engine::new(pool.clone(), schema);
    let v: Value = engine
        .query(
            r#"mutation { insert_users(objects: [{name: "dave", active: true}]) { affected_rows } }"#,
            None,
        )
        .await
        .expect("base table is writable");
    assert_eq!(v["insert_users"]["affected_rows"], 1);
    assert_eq!(user_names(&pool).await, ["alice", "carol", "dave"]);
}

/// The config overlay overrides introspection in both directions: a view fronted
/// by INSTEAD OF triggers is genuinely writable, and a base table can be frozen.
#[tokio::test]
async fn config_overrides_read_only_in_both_directions() {
    let (pool, _c) = pool_with_views().await;
    let schema = Schema::introspect(&pool)
        .await
        .expect("introspect")
        .load_config("tests/fixtures/read_only.toml")
        .expect("load toml")
        .build();

    assert!(
        !schema.table("writable_users").expect("view").read_only,
        "config must be able to unfreeze a view"
    );
    assert!(
        schema.table("orders").expect("base table").read_only,
        "config must be able to freeze a base table"
    );
    assert!(
        schema.table("active_users").expect("view").read_only,
        "a view the config says nothing about keeps what introspection found"
    );

    let engine = Engine::new(pool.clone(), schema);

    // The frozen base table now rejects writes.
    let msg = engine
        .query(
            r#"mutation { delete_orders(where: {id: {_eq: 1}}) { affected_rows } }"#,
            None,
        )
        .await
        .expect_err("a frozen base table must reject writes")
        .to_string();
    assert!(msg.contains("read-only"), "got: {msg}");

    // The unfrozen view accepts them — and Postgres auto-updates it through to
    // `users`, which is exactly the behavior the default guard exists to prevent.
    let v: Value = engine
        .query(
            r#"mutation { insert_writable_users(objects: [{name: "dave", active: true}]) { affected_rows } }"#,
            None,
        )
        .await
        .expect("an unfrozen view is writable");
    assert_eq!(v["insert_writable_users"]["affected_rows"], 1);
    assert_eq!(user_names(&pool).await, ["alice", "carol", "dave"]);
}

/// A nested insert reaches a table without ever naming a root field, so the
/// guard has to cover that path too — otherwise `insert_users` with a nested
/// `orders` block writes into a table the schema says is frozen.
#[tokio::test]
async fn nested_insert_into_read_only_target_is_rejected() {
    let (pool, _c) = pool_with_views().await;
    let schema = Schema::introspect(&pool)
        .await
        .expect("introspect")
        .load_config("tests/fixtures/read_only.toml") // freezes `orders`
        .expect("load toml")
        .build();
    let engine = Engine::new(pool.clone(), schema);

    let msg = engine
        .query(
            r#"mutation {
                 insert_users(objects: [
                   {name: "dave", active: true, orders: {data: [{title: "nested"}]}}
                 ]) { affected_rows }
               }"#,
            None,
        )
        .await
        .expect_err("a nested insert into a frozen table must be rejected")
        .to_string();
    assert!(msg.contains("read-only"), "got: {msg}");

    // The whole mutation must have been rejected before any SQL ran: the parent
    // row must not exist either.
    assert_eq!(
        user_names(&pool).await,
        ["alice", "carol"],
        "the parent insert must not slip through when the nested target is frozen"
    );
    let orders: i64 = sqlx::query_scalar("SELECT count(*) FROM orders")
        .fetch_one(&pool)
        .await
        .expect("count orders");
    assert_eq!(orders, 1, "no nested row may be written");
}

/// The builder API constructs the AST directly and never passes through the
/// parser, so the parser's guard alone is not enough — the renderer is the one
/// choke point both paths share. Before the renderer check existed, this wrote a
/// row straight into `users` through the view.
#[tokio::test]
async fn builder_path_cannot_write_to_a_read_only_table() {
    use vision_graphql::Mutation;

    let (pool, _c) = pool_with_views().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    let engine = Engine::new(pool.clone(), schema);

    let msg = engine
        .run(Mutation::insert_one(
            "active_users",
            [
                ("name", serde_json::json!("mallory")),
                ("active", serde_json::json!(true)),
            ],
        ))
        .await
        .expect_err("the builder must not be able to write to a read-only table")
        .to_string();
    assert!(msg.contains("read-only"), "got: {msg}");

    assert_eq!(
        user_names(&pool).await,
        ["alice", "carol"],
        "the builder path must not reach the table behind the view"
    );
}
