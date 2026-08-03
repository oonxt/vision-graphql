//! Compile-once / execute-many against a real database.
//!
//! The unit tests in `src/compiled.rs` prove the SQL and the parameter recipe
//! are right; these prove the rows that come back are, and that the guards
//! around scoped execution hold where it matters.

use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use vision_graphql::predicate::{col, principal, Principal};
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::{Engine, Error, ScopePolicy};

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
                .relation("user", Relation::object("users").on([("user_id", "id")])),
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
        INSERT INTO users (name) VALUES ('alice'), ('bob');
        INSERT INTO orders (user_id, title) VALUES
            (1, 'a-1'), (1, 'a-2'), (2, 'b-1');
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed");

    (Engine::new(pool, schema()), container)
}

fn titles(data: &Value, key: &str) -> Vec<String> {
    data[key]
        .as_array()
        .expect("array")
        .iter()
        .map(|r| r["title"].as_str().expect("title").to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn one_statement_serves_every_variable_value() {
    let (engine, _c) = setup().await;
    let q = engine
        .compile("query($id: Int!) { orders(where: {user_id: {_eq: $id}}, order_by: {id: asc}) { title } }")
        .expect("compile");
    assert_eq!(q.variables(), vec!["id".to_string()]);

    let a = engine.execute(&q, Some(json!({"id": 1}))).await.unwrap();
    assert_eq!(titles(&a, "orders"), ["a-1", "a-2"]);

    let b = engine.execute(&q, Some(json!({"id": 2}))).await.unwrap();
    assert_eq!(titles(&b, "orders"), ["b-1"]);

    // Same handle, so provably the same SQL for both.
    assert!(q.sql().contains("$1"), "{}", q.sql());
}

#[tokio::test(flavor = "multi_thread")]
async fn compiled_matches_the_per_request_path() {
    let (engine, _c) = setup().await;
    let source =
        "query($id: Int!) { orders(where: {user_id: {_eq: $id}}, order_by: {id: asc}) { title } }";
    let vars = json!({"id": 1});

    let compiled = engine.compile(source).expect("compile");
    let via_compile = engine.execute(&compiled, Some(vars.clone())).await.unwrap();
    let via_query = engine.query(source, Some(vars)).await.unwrap();
    assert_eq!(via_compile, via_query);
}

#[tokio::test(flavor = "multi_thread")]
async fn variable_list_and_limit_execute() {
    let (engine, _c) = setup().await;
    let q = engine
        .compile(
            "query($ids: [Int!], $n: Int!) {
                 orders(where: {id: {_in: $ids}}, order_by: {id: asc}, limit: $n) { title }
             }",
        )
        .expect("compile");

    let data = engine
        .execute(&q, Some(json!({"ids": [1, 2, 3], "n": 2})))
        .await
        .unwrap();
    assert_eq!(titles(&data, "orders"), ["a-1", "a-2"]);

    let data = engine
        .execute(&q, Some(json!({"ids": [3], "n": 10})))
        .await
        .unwrap();
    assert_eq!(titles(&data, "orders"), ["b-1"]);

    // An empty list is a value, not a different statement.
    let data = engine
        .execute(&q, Some(json!({"ids": [], "n": 10})))
        .await
        .unwrap();
    assert!(titles(&data, "orders").is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn one_statement_serves_every_principal() {
    let (engine, _c) = setup().await;
    let policy = ScopePolicy::builder()
        .allow("orders", col("user_id").eq(principal()))
        .validate(&schema())
        .expect("policy");

    let q = engine
        .compile_scoped("{ orders(order_by: {id: asc}) { title } }", &policy)
        .expect("compile");
    assert!(q.is_scoped());

    let alice = engine
        .execute_scoped(&q, None, &Principal::new().set("principal", 1))
        .await
        .unwrap();
    assert_eq!(titles(&alice, "orders"), ["a-1", "a-2"]);

    let bob = engine
        .execute_scoped(&q, None, &Principal::new().set("principal", 2))
        .await
        .unwrap();
    assert_eq!(titles(&bob, "orders"), ["b-1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn scope_still_binds_when_the_query_has_its_own_filter() {
    let (engine, _c) = setup().await;
    let policy = ScopePolicy::builder()
        .allow("orders", col("user_id").eq(principal()))
        .validate(&schema())
        .expect("policy");
    let q = engine
        .compile_scoped(
            "query($t: String!) { orders(where: {title: {_eq: $t}}) { title } }",
            &policy,
        )
        .expect("compile");

    // bob asking for one of alice's rows gets nothing, not the row.
    let out = engine
        .execute_scoped(
            &q,
            Some(json!({"t": "a-1"})),
            &Principal::new().set("principal", 2),
        )
        .await
        .unwrap();
    assert!(titles(&out, "orders").is_empty());

    let out = engine
        .execute_scoped(
            &q,
            Some(json!({"t": "a-1"})),
            &Principal::new().set("principal", 1),
        )
        .await
        .unwrap();
    assert_eq!(titles(&out, "orders"), ["a-1"]);
}

/// `apply_scope` turns a scoped update into a pre-image filter *and* a
/// post-update check. Both must survive compilation with the principal still
/// deferred, or a compiled scoped mutation would write outside its scope.
#[tokio::test(flavor = "multi_thread")]
async fn a_scoped_compiled_update_still_filters_by_principal() {
    let (engine, _c) = setup().await;
    let policy = ScopePolicy::builder()
        .allow("orders", col("user_id").eq(principal()))
        .validate(&schema())
        .expect("policy");
    let q = engine
        .compile_scoped(
            "mutation($id: Int!, $t: String!) {
                 update_orders(where: {id: {_eq: $id}}, _set: {title: $t}) {
                     affected_rows
                     returning { title }
                 }
             }",
            &policy,
        )
        .expect("compile");

    // bob (2) trying to rename alice's order 1: matched by the query's own
    // filter, excluded by the scope predicate.
    let out = engine
        .execute_scoped(
            &q,
            Some(json!({"id": 1, "t": "hijacked"})),
            &Principal::new().set("principal", 2),
        )
        .await
        .unwrap();
    assert_eq!(out["update_orders"]["affected_rows"], json!(0));

    // alice (1) may rename her own.
    let out = engine
        .execute_scoped(
            &q,
            Some(json!({"id": 1, "t": "renamed"})),
            &Principal::new().set("principal", 1),
        )
        .await
        .unwrap();
    assert_eq!(out["update_orders"]["affected_rows"], json!(1));

    // And the row really did not change under bob.
    let check = engine
        .query("{ orders(where: {id: {_eq: 1}}) { title } }", None)
        .await
        .unwrap();
    assert_eq!(titles(&check, "orders"), ["renamed"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scoped_compiled_delete_still_filters_by_principal() {
    let (engine, _c) = setup().await;
    let policy = ScopePolicy::builder()
        .allow("orders", col("user_id").eq(principal()))
        .validate(&schema())
        .expect("policy");
    let q = engine
        .compile_scoped(
            "mutation($id: Int!) { delete_orders(where: {id: {_eq: $id}}) { affected_rows } }",
            &policy,
        )
        .expect("compile");

    let out = engine
        .execute_scoped(
            &q,
            Some(json!({"id": 3})),
            &Principal::new().set("principal", 1),
        )
        .await
        .unwrap();
    assert_eq!(
        out["delete_orders"]["affected_rows"],
        json!(0),
        "alice must not delete bob's order"
    );

    let out = engine
        .execute_scoped(
            &q,
            Some(json!({"id": 3})),
            &Principal::new().set("principal", 2),
        )
        .await
        .unwrap();
    assert_eq!(out["delete_orders"]["affected_rows"], json!(1));
}

/// Scope predicates are injected at *every* table access point, nested
/// relations included. Compiling must not drop the nested one.
#[tokio::test(flavor = "multi_thread")]
async fn a_scoped_compiled_nested_relation_is_filtered_too() {
    let (engine, _c) = setup().await;
    let policy = ScopePolicy::builder()
        .unrestricted("users")
        .allow("orders", col("user_id").eq(principal()))
        .validate(&schema())
        .expect("policy");
    let q = engine
        .compile_scoped(
            "{ users(order_by: {id: asc}) { name orders(order_by: {id: asc}) { title } } }",
            &policy,
        )
        .expect("compile");

    let out = engine
        .execute_scoped(&q, None, &Principal::new().set("principal", 1))
        .await
        .unwrap();
    let users = out["users"].as_array().expect("users");
    // Both users are visible (public table), but only alice's orders hang off them.
    assert_eq!(users.len(), 2);
    assert_eq!(titles(&users[0], "orders"), ["a-1", "a-2"]);
    assert!(titles(&users[1], "orders").is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scoped_statement_cannot_be_run_unscoped() {
    let (engine, _c) = setup().await;
    let policy = ScopePolicy::builder()
        .allow("orders", col("user_id").eq(principal()))
        .validate(&schema())
        .expect("policy");
    let q = engine
        .compile_scoped("{ orders { title } }", &policy)
        .expect("compile");

    let err = engine.execute(&q, None).await.unwrap_err();
    assert!(matches!(err, Error::Scope(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unscoped_statement_cannot_be_run_with_a_principal() {
    let (engine, _c) = setup().await;
    // Passing a principal to a statement that carries no predicate would look
    // like it restricted the query while returning every row.
    let q = engine.compile("{ orders { title } }").expect("compile");
    let err = engine
        .execute_scoped(&q, None, &Principal::new().set("principal", 1))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Scope(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_outside_the_policy_fails_at_compile_time() {
    let (engine, _c) = setup().await;
    let policy = ScopePolicy::builder()
        .allow("orders", col("user_id").eq(principal()))
        .validate(&schema())
        .expect("policy");
    let err = engine
        .compile_scoped("{ users { name } }", &policy)
        .unwrap_err();
    assert!(matches!(err, Error::ScopeDenied { .. }), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn update_and_delete_compile_and_run() {
    let (engine, _c) = setup().await;
    let update = engine
        .compile(
            "mutation($id: Int!, $t: String!) {
                 update_orders(where: {id: {_eq: $id}}, _set: {title: $t}) {
                     affected_rows
                     returning { title }
                 }
             }",
        )
        .expect("compile update");
    let out = engine
        .execute(&update, Some(json!({"id": 1, "t": "renamed"})))
        .await
        .unwrap();
    assert_eq!(out["update_orders"]["affected_rows"], json!(1));
    assert_eq!(
        out["update_orders"]["returning"][0]["title"],
        json!("renamed")
    );

    let delete = engine
        .compile("mutation($id: Int!) { delete_orders(where: {id: {_eq: $id}}) { affected_rows } }")
        .expect("compile delete");
    let out = engine
        .execute(&delete, Some(json!({"id": 3})))
        .await
        .unwrap();
    assert_eq!(out["delete_orders"]["affected_rows"], json!(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn by_pk_compiles_and_deserializes() {
    #[derive(serde::Deserialize, PartialEq, Debug)]
    struct Order {
        title: String,
    }
    let (engine, _c) = setup().await;
    let q = engine
        .compile("query($id: Int!) { orders_by_pk(id: $id) { title } }")
        .expect("compile");
    let got: Option<Order> = engine.execute_as(&q, Some(json!({"id": 2}))).await.unwrap();
    assert_eq!(
        got,
        Some(Order {
            title: "a-2".into()
        })
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_uncompilable_query_says_so_and_still_runs_per_request() {
    let (engine, _c) = setup().await;
    let source = "query($w: orders_bool_exp) { orders(where: $w) { title } }";
    let err = engine.compile(source).unwrap_err();
    assert!(matches!(err, Error::NotCompilable { .. }), "{err:?}");

    let data = engine
        .query(source, Some(json!({"w": {"id": {"_eq": 3}}})))
        .await
        .unwrap();
    assert_eq!(titles(&data, "orders"), ["b-1"]);
}
