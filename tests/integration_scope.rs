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
use vision_graphql::{Engine, Error, Mutation, Query, ScopeSet};

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
                .relation(
                    "samples",
                    Relation::array("samples").on([("id", "order_id")]),
                ),
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
    assert!(orders
        .iter()
        .all(|o| o["title"].as_str().unwrap().starts_with("a-")));
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
        .query("query { samples_aggregate { aggregate { count } } }", None)
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
async fn scoped_insert_array_in_scope_succeeds() {
    let (engine, _c) = setup().await;
    // The array insert form (insert_orders) is scoped just like insert_one:
    // an in-scope row passes the post-insert guard.
    let v: Value = engine
        .scoped(user_scope(1))
        .query(
            r#"mutation { insert_orders(objects: [{user_id: 1, title: "x"}]) { affected_rows } }"#,
            None,
        )
        .await
        .expect("in-scope array insert ok");
    assert_eq!(v["insert_orders"]["affected_rows"], json!(1));
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
            let denied = tx.query("query { samples { id } }", None).await.is_ok();
            Ok::<_, Error>((count, denied))
        })
        .await
        .expect("tx ok");
    assert_eq!(own_count, 2);
    assert!(
        denied,
        "samples IS in user scope — sanity check the tx path"
    );

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

// ===== Scoped mutations: update/delete inject the predicate as a filter, so a
// scoped caller can only touch rows already in their scope. Insert stays
// fail-closed. =====

#[tokio::test]
async fn scoped_update_only_touches_owned_rows() {
    let (engine, _c) = setup().await;
    // alice (user 1) tries to retitle order 3 (bob's). The scope predicate
    // (user_id = 1) AND-s onto her where, so zero rows match.
    let v: Value = engine
        .scoped(user_scope(1))
        .run(
            Mutation::update("orders")
                .where_eq("id", 3)
                .set("title", json!("hijacked"))
                .returning(&["id"]),
        )
        .await
        .expect("update ok");
    assert_eq!(v["update_orders"]["affected_rows"], json!(0));

    // bob's order is untouched.
    let title: Value = engine
        .run(Query::by_pk("orders", &[("id", json!(3))]).select(&["title"]))
        .await
        .expect("read ok");
    assert_eq!(title["orders_by_pk"]["title"], json!("b-order-1"));

    // alice CAN update her own order.
    let v: Value = engine
        .scoped(user_scope(1))
        .run(
            Mutation::update("orders")
                .where_eq("id", 1)
                .set("title", json!("a-order-1-edited"))
                .returning(&["id", "title"]),
        )
        .await
        .expect("update ok");
    assert_eq!(v["update_orders"]["affected_rows"], json!(1));
    assert_eq!(
        v["update_orders"]["returning"][0]["title"],
        json!("a-order-1-edited")
    );
}

#[tokio::test]
async fn scoped_update_by_pk_returns_null_for_foreign_row() {
    let (engine, _c) = setup().await;
    // by_pk on bob's order: PK matches but scope predicate does not → null.
    let v: Value = engine
        .scoped(user_scope(1))
        .run(
            Mutation::update_by_pk("orders", &[("id", json!(3))])
                .set("title", json!("nope"))
                .select(&["id"]),
        )
        .await
        .expect("update_by_pk ok");
    assert_eq!(v["update_orders_by_pk"], Value::Null);

    // own row by_pk succeeds.
    let v: Value = engine
        .scoped(user_scope(1))
        .run(
            Mutation::update_by_pk("orders", &[("id", json!(2))])
                .set("title", json!("a-order-2-edited"))
                .select(&["id", "title"]),
        )
        .await
        .expect("update_by_pk ok");
    assert_eq!(v["update_orders_by_pk"]["title"], json!("a-order-2-edited"));
}

#[tokio::test]
async fn scoped_delete_cannot_remove_foreign_rows() {
    let (engine, _c) = setup().await;
    // alice deletes "all" samples she can reach via a broad predicate. samples'
    // scope is a Relation (reachable via her orders), so this also exercises a
    // Relation predicate inside a DELETE WHERE; only her 2 samples are eligible.
    // samples is a leaf table (nothing FK-references it).
    let v: Value = engine
        .scoped(user_scope(1))
        .run(
            Mutation::delete("samples")
                .where_expr(BoolExpr::Compare {
                    column: "id".into(),
                    op: CmpOp::Gt,
                    value: json!(0),
                })
                .returning(&["id"]),
        )
        .await
        .expect("delete ok");
    assert_eq!(
        v["delete_samples"]["affected_rows"],
        json!(2),
        "only alice's 2 samples"
    );

    // bob's sample survives.
    let remaining: Value = engine
        .run(Query::from("samples").select(&["serial"]))
        .await
        .expect("read ok");
    let rows = remaining["samples"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["serial"], json!("S-B1"));
}

#[tokio::test]
async fn scoped_delete_by_pk_on_denied_table_fails_closed() {
    let (engine, _c) = setup().await;
    // samples is reachable, but a table absent from the set is denied outright.
    let scope = ScopeSet::new().allow("orders", eq("user_id", 1)); // samples absent
    let err = engine
        .scoped(scope)
        .run(Mutation::delete_by_pk("samples", &[("id", json!(1))]).select(&["id"]))
        .await
        .expect_err("denied");
    assert!(matches!(err, Error::ScopeDenied { table } if table == "samples"));
}

#[tokio::test]
async fn scoped_insert_in_scope_succeeds() {
    let (engine, _c) = setup().await;
    // alice inserts an order owned by herself — satisfies the scope check.
    let v: Value = engine
        .scoped(user_scope(1))
        .run(
            Mutation::insert_one("orders", [("user_id", json!(1)), ("title", json!("a-new"))])
                .returning(&["user_id", "title"]),
        )
        .await
        .expect("in-scope insert ok");
    assert_eq!(v["insert_orders_one"]["title"], json!("a-new"));
    assert_eq!(v["insert_orders_one"]["user_id"], json!(1));
}

#[tokio::test]
async fn scoped_insert_outside_scope_aborts() {
    let (engine, _c) = setup().await;
    // alice tries to insert an order owned by bob (user_id 2). The post-insert
    // check fails → the whole statement aborts and nothing is committed.
    let err = engine
        .scoped(user_scope(1))
        .run(Mutation::insert_one(
            "orders",
            [("user_id", json!(2)), ("title", json!("forged"))],
        ))
        .await
        .expect_err("out-of-scope insert must abort");
    // Surfaces as a DB error from the deliberate failed cast.
    assert!(
        matches!(&err, Error::Database(_)),
        "expected DB-level abort, got {err:?}"
    );

    // Nothing was written: bob still has exactly his one seeded order.
    let bob: Value = engine
        .run(Query::from("orders").where_eq("user_id", 2).select(&["id"]))
        .await
        .expect("read ok");
    assert_eq!(bob["orders"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn scoped_insert_on_denied_table_fails_closed() {
    let (engine, _c) = setup().await;
    // samples is absent from this set → denied before any SQL runs.
    let scope = ScopeSet::new().allow("orders", eq("user_id", 1));
    let err = engine
        .scoped(scope)
        .run(Mutation::insert_one(
            "samples",
            [("order_id", json!(1)), ("serial", json!("X"))],
        ))
        .await
        .expect_err("denied table");
    assert!(matches!(err, Error::ScopeDenied { table } if table == "samples"));
}

/// Parent `users` is unrestricted (a fresh user can be created); nested
/// `orders` must be tagged `ok-*`. Models "may create any user, but only
/// orders within my tenant prefix". Deterministic because the check is on a
/// column the child sets directly, not the derived FK.
fn nested_insert_scope() -> ScopeSet {
    ScopeSet::new().unrestricted("users").allow(
        "orders",
        BoolExpr::Compare {
            column: "title".into(),
            op: CmpOp::Like,
            value: json!("ok-%"),
        },
    )
}

#[tokio::test]
async fn scoped_nested_insert_in_scope_succeeds() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .scoped(nested_insert_scope())
        .query(
            r#"mutation { insert_users_one(object: {name: "carol", orders: {data: [{title: "ok-1"}]}}) { id name } }"#,
            None,
        )
        .await
        .expect("in-scope nested insert ok");
    assert_eq!(v["insert_users_one"]["name"], json!("carol"));

    // Both the parent and the child were committed.
    let orders: Value = engine
        .run(
            Query::from("orders")
                .where_eq("title", "ok-1")
                .select(&["title"]),
        )
        .await
        .expect("read ok");
    assert_eq!(orders["orders"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn scoped_nested_insert_child_violation_aborts_everything() {
    let (engine, _c) = setup().await;
    // The child order is tagged outside the allowed prefix → the nested guard
    // aborts the whole statement; neither the user nor the order is committed.
    let err = engine
        .scoped(nested_insert_scope())
        .query(
            r#"mutation { insert_users_one(object: {name: "mallory", orders: {data: [{title: "smuggled"}]}}) { id } }"#,
            None,
        )
        .await
        .expect_err("child out of scope must abort");
    assert!(
        matches!(&err, Error::Database(_)),
        "expected DB abort, got {err:?}"
    );

    // Atomic: the parent user was rolled back too.
    let users: Value = engine
        .run(
            Query::from("users")
                .where_eq("name", "mallory")
                .select(&["id"]),
        )
        .await
        .expect("read ok");
    assert!(
        users["users"].as_array().unwrap().is_empty(),
        "parent insert must roll back with the failed child"
    );
}

// ===== Post-update check: a scoped caller may not move a row *out* of their
// scope (e.g. reassign the owning column). The scope predicate is enforced both
// as a pre-image filter (which rows can be touched) and a post-update guard
// (the result must stay in scope). =====

#[tokio::test]
async fn scoped_update_cannot_move_row_out_of_scope() {
    let (engine, _c) = setup().await;
    // alice owns order 1. She tries to reassign it to bob (user_id 2). The
    // pre-image filter (user_id = 1) matches her own row, but the post-update
    // guard sees the new row with user_id = 2 → violation → whole stmt aborts.
    let err = engine
        .scoped(user_scope(1))
        .run(
            Mutation::update("orders")
                .where_eq("id", 1)
                .set("user_id", json!(2))
                .returning(&["id"]),
        )
        .await
        .expect_err("moving a row out of scope must abort");
    assert!(
        matches!(&err, Error::Database(_)),
        "expected DB-level abort, got {err:?}"
    );

    // Nothing changed: order 1 still belongs to alice.
    let owner: Value = engine
        .run(Query::by_pk("orders", &[("id", json!(1))]).select(&["user_id"]))
        .await
        .expect("read ok");
    assert_eq!(owner["orders_by_pk"]["user_id"], json!(1), "row untouched");

    // An in-scope update (changing a non-owning column) still works.
    let v: Value = engine
        .scoped(user_scope(1))
        .run(
            Mutation::update("orders")
                .where_eq("id", 1)
                .set("title", json!("a-order-1-edited"))
                .returning(&["id"]),
        )
        .await
        .expect("in-scope update ok");
    assert_eq!(v["update_orders"]["affected_rows"], json!(1));
}

#[tokio::test]
async fn scoped_update_by_pk_cannot_move_row_out_of_scope() {
    let (engine, _c) = setup().await;
    // alice's own order 2, reassigned to bob via _by_pk: PK + pre-image filter
    // match, but the post-update guard rejects the out-of-scope result.
    let err = engine
        .scoped(user_scope(1))
        .run(
            Mutation::update_by_pk("orders", &[("id", json!(2))])
                .set("user_id", json!(2))
                .select(&["id"]),
        )
        .await
        .expect_err("by_pk move out of scope must abort");
    assert!(
        matches!(&err, Error::Database(_)),
        "expected DB-level abort, got {err:?}"
    );

    let owner: Value = engine
        .run(Query::by_pk("orders", &[("id", json!(2))]).select(&["user_id"]))
        .await
        .expect("read ok");
    assert_eq!(owner["orders_by_pk"]["user_id"], json!(1), "row untouched");
}

// ===== Upsert pre-image: a scoped insert with on_conflict do_update injects the
// scope predicate into the DO UPDATE WHERE, so a conflicting row outside scope
// is skipped (not overwritten) rather than stolen. =====

#[tokio::test]
async fn scoped_upsert_cannot_overwrite_foreign_row() {
    let (engine, _c) = setup().await;
    // bob's order 3 exists. alice upserts on the orders pkey, trying to take it
    // over by setting user_id = 1 and a new title. The DO UPDATE WHERE applies
    // her scope (user_id = 1) to the EXISTING row (still user_id = 2) → the
    // conflict row is skipped, nothing is updated, and bob keeps his order.
    let v: Value = engine
        .scoped(user_scope(1))
        .query(
            r#"mutation {
                 insert_orders(
                   objects: [{id: 3, user_id: 1, title: "stolen"}],
                   on_conflict: {constraint: "orders_pkey", update_columns: ["user_id", "title"]}
                 ) { affected_rows }
               }"#,
            None,
        )
        .await
        .expect("upsert runs without error");
    assert_eq!(
        v["insert_orders"]["affected_rows"], json!(0),
        "foreign conflict row is skipped, not overwritten"
    );

    // bob's order 3 is intact.
    let order: Value = engine
        .run(Query::by_pk("orders", &[("id", json!(3))]).select(&["user_id", "title"]))
        .await
        .expect("read ok");
    assert_eq!(order["orders_by_pk"]["user_id"], json!(2), "still bob's");
    assert_eq!(order["orders_by_pk"]["title"], json!("b-order-1"));
}

#[tokio::test]
async fn scoped_nested_insert_denied_target_fails_closed() {
    let (engine, _c) = setup().await;
    // Parent allowed, but the nested target table `orders` is absent from the
    // set → denied at rewrite time, before any SQL runs.
    let scope = ScopeSet::new().unrestricted("users");
    let err = engine
        .scoped(scope)
        .query(
            r#"mutation { insert_users_one(object: {name: "nina", orders: {data: [{title: "x"}]}}) { id } }"#,
            None,
        )
        .await
        .expect_err("denied nested target");
    assert!(matches!(err, Error::ScopeDenied { table } if table == "orders"));
}
