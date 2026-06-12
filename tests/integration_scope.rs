//! Scoped execution end-to-end: every table access point must carry the
//! scope predicate, and anything outside the ScopeSet must fail closed.
//!
//! Schema models the ownership-chain shape scoping exists for:
//! users ←(user_id) orders ←(order_id) samples, plus a public lookup table.

use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use vision_graphql::ast::{BoolExpr, CmpOp};
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::{Engine, Error, Query, ScopeSet};

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, true)
                .primary_key(&["id"])
                .relation("orders", Relation::array("orders").on([("id", "user_id")])),
        )
        .table(
            Table::new("orders", "public", "orders")
                .column("id", "id", PgType::Int4, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .primary_key(&["id"])
                .relation("user", Relation::object("users").on([("user_id", "id")]))
                .relation("samples", Relation::array("samples").on([("id", "order_id")])),
        )
        .table(
            Table::new("samples", "public", "samples")
                .column("id", "id", PgType::Int4, false)
                .column("order_id", "order_id", PgType::Int4, false)
                .column("serial", "serial", PgType::Text, false)
                .primary_key(&["id"])
                .relation("order", Relation::object("orders").on([("order_id", "id")])),
        )
        .table(
            Table::new("adverts", "public", "adverts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
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
        CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            user_id INT NOT NULL REFERENCES users(id),
            title TEXT NOT NULL
        );
        CREATE TABLE samples (
            id SERIAL PRIMARY KEY,
            order_id INT NOT NULL REFERENCES orders(id),
            serial TEXT NOT NULL
        );
        CREATE TABLE adverts (id SERIAL PRIMARY KEY, title TEXT NOT NULL);
        INSERT INTO users (name) VALUES ('alice'), ('bob');
        INSERT INTO orders (user_id, title) VALUES
            (1, 'a-order-1'), (1, 'a-order-2'), (2, 'b-order-1');
        INSERT INTO samples (order_id, serial) VALUES
            (1, 'S-A1'), (2, 'S-A2'), (3, 'S-B1');
        INSERT INTO adverts (title) VALUES ('ad-1'), ('ad-2');
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed");

    let engine = Engine::new(pool, schema());
    (engine, container)
}

fn eq(column: &str, v: i64) -> BoolExpr {
    BoolExpr::Compare {
        column: column.into(),
        op: CmpOp::Eq,
        value: json!(v),
    }
}

/// The scope of "user N": own row, own orders, samples reachable via the
/// order chain, plus the public adverts table. Everything else is absent →
/// denied.
fn user_scope(user_id: i64) -> ScopeSet {
    ScopeSet::new()
        .allow("users", eq("id", user_id))
        .allow("orders", eq("user_id", user_id))
        .allow(
            "samples",
            BoolExpr::Relation {
                name: "order".into(),
                inner: Box::new(eq("user_id", user_id)),
            },
        )
        .unrestricted("adverts")
}

#[tokio::test]
async fn root_select_is_filtered_to_owner() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .scoped(user_scope(1))
        .query("query { orders { id title } }", None)
        .await
        .expect("query ok");
    let orders = v["orders"].as_array().expect("array");
    assert_eq!(orders.len(), 2, "alice sees exactly her 2 orders");
    assert!(orders.iter().all(|o| o["title"].as_str().unwrap().starts_with("a-")));
}

#[tokio::test]
async fn relation_path_scope_filters_via_exists_chain() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .scoped(user_scope(2))
        .query("query { samples { serial } }", None)
        .await
        .expect("query ok");
    let samples = v["samples"].as_array().expect("array");
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0]["serial"], json!("S-B1"));
}

#[tokio::test]
async fn nested_relation_selection_is_scoped_per_level() {
    let (engine, _c) = setup().await;
    // users root is scoped to self; nested orders/samples each carry their
    // own predicate too (defense in depth at every access point).
    let v: Value = engine
        .scoped(user_scope(1))
        .query(
            "query { users { id orders { title samples { serial } } } }",
            None,
        )
        .await
        .expect("query ok");
    let users = v["users"].as_array().expect("array");
    assert_eq!(users.len(), 1, "only self visible");
    let orders = users[0]["orders"].as_array().expect("orders");
    assert_eq!(orders.len(), 2);
    let all_serials: Vec<&str> = orders
        .iter()
        .flat_map(|o| o["samples"].as_array().unwrap())
        .map(|s| s["serial"].as_str().unwrap())
        .collect();
    assert_eq!(all_serials, vec!["S-A1", "S-A2"]);
}

#[tokio::test]
async fn by_pk_outside_scope_returns_null() {
    let (engine, _c) = setup().await;
    let scoped = engine.scoped(user_scope(1));
    let own: Value = scoped
        .query("query { orders_by_pk(id: 1) { id } }", None)
        .await
        .expect("query ok");
    assert_eq!(own["orders_by_pk"]["id"], json!(1));

    // order 3 belongs to bob: PK exists, scope filters it out → null.
    let theirs: Value = scoped
        .query("query { orders_by_pk(id: 3) { id } }", None)
        .await
        .expect("query ok");
    assert!(theirs["orders_by_pk"].is_null(), "IDOR probe must see null");
}

#[tokio::test]
async fn aggregate_count_respects_scope() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .scoped(user_scope(1))
        .query(
            "query { samples_aggregate { aggregate { count } } }",
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(
        v["samples_aggregate"]["aggregate"]["count"],
        json!(2),
        "count leaks rows if the aggregate source is not scoped"
    );
}

#[tokio::test]
async fn exists_filter_target_is_scoped() {
    let (engine, _c) = setup().await;
    // bob probes "which users have orders" — the EXISTS target (orders) is
    // scoped to bob's rows, so alice (who has orders, but not bob's) is out.
    let v: Value = engine
        .scoped(user_scope(2))
        .query(
            r#"query { users(where: {orders: {id: {_gt: 0}}}) { id } }"#,
            None,
        )
        .await
        .expect("query ok");
    let users = v["users"].as_array().expect("array");
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["id"], json!(2));
}

#[tokio::test]
async fn unlisted_table_is_denied() {
    let (engine, _c) = setup().await;
    let scope = ScopeSet::new().unrestricted("adverts");
    let err = engine
        .scoped(scope)
        .query("query { orders { id } }", None)
        .await
        .expect_err("orders absent from scope must be denied");
    assert!(matches!(err, Error::ScopeDenied { ref table } if table == "orders"));
}

#[tokio::test]
async fn unrestricted_table_passes_through() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .scoped(user_scope(1))
        .query("query { adverts { id title } }", None)
        .await
        .expect("query ok");
    assert_eq!(v["adverts"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn mutation_is_rejected_fail_closed() {
    let (engine, _c) = setup().await;
    let err = engine
        .scoped(user_scope(1))
        .query(
            r#"mutation { insert_orders(objects: [{user_id: 1, title: "x"}]) { affected_rows } }"#,
            None,
        )
        .await
        .expect_err("mutations unsupported in scoped mode");
    assert!(matches!(err, Error::Scope(_)));
}

#[tokio::test]
async fn builder_path_and_run_as_are_scoped() {
    #[derive(serde::Deserialize)]
    struct Order {
        id: i64,
    }
    let (engine, _c) = setup().await;
    let orders: Vec<Order> = engine
        .scoped(user_scope(2))
        .run_as(Query::from("orders").select(&["id"]))
        .await
        .expect("run_as ok");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].id, 3);
}

#[tokio::test]
async fn transaction_cannot_escape_scope() {
    let (engine, _c) = setup().await;
    let scoped = engine.scoped(user_scope(1));
    let (own_count, denied) = scoped
        .transaction(async |tx| {
            let v = tx.query("query { orders { id } }", None).await?;
            let count = v["orders"].as_array().unwrap().len();
            let denied = tx
                .query("query { samples { id } }", None)
                .await
                .is_ok();
            Ok::<_, Error>((count, denied))
        })
        .await
        .expect("tx ok");
    assert_eq!(own_count, 2);
    assert!(denied, "samples IS in user scope — sanity check the tx path");

    // and a genuinely denied table inside a tx:
    let err = scoped
        .transaction(async |tx| tx.query("query { users { id } }", None).await)
        .await;
    assert!(err.is_ok(), "users in scope");
    let err = engine
        .scoped(ScopeSet::new())
        .transaction(async |tx| tx.query("query { users { id } }", None).await)
        .await
        .expect_err("empty scope denies inside tx too");
    assert!(matches!(err, Error::ScopeDenied { .. }));
}
