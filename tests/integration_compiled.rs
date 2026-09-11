//! Compile-once / execute-many against a real database.
//!
//! The unit tests in `src/compiled.rs` prove the SQL and the parameter recipe
//! are right; these prove the rows that come back are, and that the guards
//! around scoped execution hold where it matters.

use serde_json::{json, Value};
use vision_graphql::predicate::{col, principal, Principal};
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::{Engine, Error, ScopePolicy};

mod common;

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
                .column("ref", "ref", PgType::Uuid, true)
                .column("qty", "qty", PgType::Int4, true)
                .primary_key(&["id"])
                .relation("user", Relation::object("users").on([("user_id", "id")])),
        )
        .build()
}

async fn setup() -> (Engine, common::TestDb) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();

    sqlx::raw_sql(
        r#"
        CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            user_id INT NOT NULL REFERENCES users(id),
            title TEXT NOT NULL,
            ref UUID,
            qty INT
        );
        INSERT INTO users (name) VALUES ('alice'), ('bob');
        INSERT INTO orders (user_id, title, ref) VALUES
            (1, 'a-1', '00000000-0000-0000-0000-000000000001'),
            (1, 'a-2', NULL),
            (2, 'b-1', '00000000-0000-0000-0000-000000000003');
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed");

    (Engine::new(pool, schema()), db)
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
    let source = "query($w: orders_bool_exp) { orders(where: $w) { title } }";
    let err = engine.compile(source).unwrap_err();
    assert!(matches!(err, Error::NotCompilable { .. }), "{err:?}");

    let data = engine
        .query(source, Some(json!({"w": {"id": {"_eq": 3}}})))
        .await
        .unwrap();
    assert_eq!(titles(&data, "orders"), ["b-1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn choices_pick_the_ordering_the_request_asks_for() {
    let (engine, _db) = setup().await;
    let source = r#"query($sort: [orders_order_by!]! @choices(values: [[{title: asc}], [{title: desc}], [{id: desc}]])) {
        orders(order_by: $sort) { title }
    }"#;
    let q = engine.compile(source).expect("compile");
    assert_eq!(q.shape_count(), 3);

    for (sort, expect) in [
        (json!([{"title": "asc"}]), ["a-1", "a-2", "b-1"]),
        (json!([{"title": "desc"}]), ["b-1", "a-2", "a-1"]),
        (json!([{"id": "desc"}]), ["b-1", "a-2", "a-1"]),
    ] {
        let vars = json!({"sort": sort});
        let compiled = engine.execute(&q, Some(vars.clone())).await.unwrap();
        assert_eq!(titles(&compiled, "orders"), expect, "{vars}");
        // The same document answers the same under the per-request path.
        let eager = engine.query(source, Some(vars)).await.unwrap();
        assert_eq!(compiled, eager);
    }

    // A value the document never offered: refused on both paths, not sorted
    // some other way.
    let vars = json!({"sort": [{"id": "asc"}]});
    let err = engine.execute(&q, Some(vars.clone())).await.unwrap_err();
    assert!(
        matches!(&err, Error::Variable { name, message } if name == "sort" && message.contains("@choices")),
        "{err:?}"
    );
    let err = engine.query(source, Some(vars)).await.unwrap_err();
    assert!(
        matches!(&err, Error::Variable { name, .. } if name == "sort"),
        "{err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn choices_and_a_policy_compose_per_shape() {
    let (engine, _db) = setup().await;
    let policy = ScopePolicy::builder()
        .allow("orders", col("user_id").eq(principal()))
        .validate(&schema())
        .expect("policy");
    let q = engine
        .compile_scoped(
            r#"query($sort: [orders_order_by!]! @choices(values: [[{id: asc}], [{id: desc}]]),
                     $null: Boolean! @choices(values: [true, false])) {
                 orders(order_by: $sort, where: {ref: {_is_null: $null}}) { title }
               }"#,
            &policy,
        )
        .expect("compile");
    assert_eq!(q.shape_count(), 4);
    for (_, sql) in q.shapes() {
        assert!(
            sql.contains("user_id"),
            "scope predicate missing from a shape: {sql}"
        );
    }

    let alice = Principal::new().set("principal", 1);
    let bob = Principal::new().set("principal", 2);
    let data = engine
        .execute_scoped(
            &q,
            Some(json!({"sort": [{"id": "desc"}], "null": false})),
            &alice,
        )
        .await
        .unwrap();
    assert_eq!(titles(&data, "orders"), ["a-1"]);
    let data = engine
        .execute_scoped(
            &q,
            Some(json!({"sort": [{"id": "desc"}], "null": true})),
            &alice,
        )
        .await
        .unwrap();
    assert_eq!(titles(&data, "orders"), ["a-2"]);
    let data = engine
        .execute_scoped(
            &q,
            Some(json!({"sort": [{"id": "asc"}], "null": false})),
            &bob,
        )
        .await
        .unwrap();
    assert_eq!(titles(&data, "orders"), ["b-1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn optional_filters_apply_when_supplied_and_drop_when_null() {
    let (engine, _db) = setup().await;
    let source = r#"query($t: String @optional, $ids: [Int!] @optional, $not: [Int!] @optional, $ref: uuid @optional) {
        orders(where: {title: {_ilike: $t}, id: {_in: $ids, _nin: $not}, ref: {_eq: $ref}}, order_by: {id: asc}) { title }
    }"#;
    let q = engine.compile(source).expect("compile");
    assert_eq!(q.shape_count(), 1);

    let cases: Vec<(Value, Vec<&str>)> = vec![
        (
            json!({"t": null, "ids": null, "not": null, "ref": null}),
            vec!["a-1", "a-2", "b-1"],
        ),
        (
            json!({"t": "a-%", "ids": null, "not": null, "ref": null}),
            vec!["a-1", "a-2"],
        ),
        (
            json!({"t": null, "ids": [3], "not": null, "ref": null}),
            vec!["b-1"],
        ),
        (
            json!({"t": null, "ids": [], "not": null, "ref": null}),
            vec![],
        ),
        (
            json!({"t": null, "ids": null, "not": [1, 3], "ref": null}),
            vec!["a-2"],
        ),
        (
            json!({"t": null, "ids": null, "not": [], "ref": null}),
            vec!["a-1", "a-2", "b-1"],
        ),
        (
            json!({"t": null, "ids": null, "not": null, "ref": "00000000-0000-0000-0000-000000000003"}),
            vec!["b-1"],
        ),
        (
            json!({"t": "%1", "ids": [1, 3], "not": [3], "ref": "00000000-0000-0000-0000-000000000001"}),
            vec!["a-1"],
        ),
    ];
    for (vars, expect) in cases {
        let compiled = engine.execute(&q, Some(vars.clone())).await.unwrap();
        assert_eq!(titles(&compiled, "orders"), expect, "{vars}");
        let eager = engine.query(source, Some(vars)).await.unwrap();
        assert_eq!(compiled, eager);
    }

    // Leaving a variable out is not the same as passing null.
    let err = engine
        .execute(&q, Some(json!({"t": null, "ids": null, "not": null})))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::Variable { name, .. } if name == "ref"),
        "{err:?}"
    );
}

/// The three-state filter from the field report: a list endpoint whose
/// `roots` argument is "only top-level", "only nested", or unset. It was two
/// documents; it is one statement.
#[tokio::test(flavor = "multi_thread")]
async fn an_optional_is_null_is_three_states_in_one_statement() {
    let (engine, _db) = setup().await;
    let source = r#"query($roots: Boolean @optional = null) {
        orders(where: {ref: {_is_null: $roots}}, order_by: {id: asc}) { title }
    }"#;
    let q = engine.compile(source).expect("compile");
    assert_eq!(q.shape_count(), 1);

    let cases: Vec<(Value, Vec<&str>)> = vec![
        (json!({"roots": true}), vec!["a-2"]),
        (json!({"roots": false}), vec!["a-1", "b-1"]),
        (json!({"roots": null}), vec!["a-1", "a-2", "b-1"]),
        (json!({}), vec!["a-1", "a-2", "b-1"]),
    ];
    for (vars, expect) in cases {
        let compiled = engine.execute(&q, Some(vars.clone())).await.unwrap();
        assert_eq!(titles(&compiled, "orders"), expect, "{vars}");
        let eager = engine.query(source, Some(vars)).await.unwrap();
        assert_eq!(compiled, eager);
    }

    // Without @optional the variable still binds — one statement — but a
    // null is refused rather than matching nothing.
    let source = "query($b: Boolean!) { orders(where: {ref: {_is_null: $b}}, order_by: {id: asc}) { title } }";
    let q = engine.compile(source).expect("compile");
    let rows = engine.execute(&q, Some(json!({"b": true}))).await.unwrap();
    assert_eq!(titles(&rows, "orders"), vec!["a-2"]);
    let rows = engine.execute(&q, Some(json!({"b": false}))).await.unwrap();
    assert_eq!(titles(&rows, "orders"), vec!["a-1", "b-1"]);
    let err = engine
        .execute(&q, Some(json!({"b": null})))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::Validate { message, .. } if message.contains("@optional")),
        "{err:?}"
    );
}

/// `@choices` bounds what the request may send; `@optional` adds null. One
/// shape per value and one with the comparison dropped, on both paths.
#[tokio::test(flavor = "multi_thread")]
async fn a_choices_variable_that_is_also_optional_has_a_shape_with_the_filter_dropped() {
    let (engine, _db) = setup().await;
    let source = r#"query($t: String @choices(values: ["a-1", "b-1"]) @optional = null) {
        orders(where: {title: {_eq: $t}}, order_by: {id: asc}) { title }
    }"#;
    let q = engine.compile(source).expect("compile");
    assert_eq!(q.shape_count(), 3);

    let cases: Vec<(Value, Vec<&str>)> = vec![
        (json!({"t": "a-1"}), vec!["a-1"]),
        (json!({"t": "b-1"}), vec!["b-1"]),
        (json!({"t": null}), vec!["a-1", "a-2", "b-1"]),
        // The default is null: the request that says nothing gets no filter.
        (json!({}), vec!["a-1", "a-2", "b-1"]),
    ];
    for (vars, expect) in cases {
        let compiled = engine.execute(&q, Some(vars.clone())).await.unwrap();
        assert_eq!(titles(&compiled, "orders"), expect, "{vars}");
        let eager = engine.query(source, Some(vars)).await.unwrap();
        assert_eq!(compiled, eager);
    }

    // Null is the one value @optional adds; the list still bounds the rest —
    // a title that exists but was not declared is refused, not answered.
    for vars in [json!({"t": "a-2"}), json!({"t": 1})] {
        let err = engine.execute(&q, Some(vars.clone())).await.unwrap_err();
        assert!(
            matches!(&err, Error::Variable { name, .. } if name == "t"),
            "{vars}: {err:?}"
        );
        let err = engine.query(source, Some(vars.clone())).await.unwrap_err();
        assert!(
            matches!(&err, Error::Variable { name, .. } if name == "t"),
            "{vars} (eager): {err:?}"
        );
    }
}

/// Compiled statements run inside `Engine::transaction` like text and builder
/// operations do — the host whose documents are all compiled statements can
/// make several of them atomic without leaving the policy behind.
#[tokio::test(flavor = "multi_thread")]
async fn compiled_statements_run_inside_a_transaction() {
    let (engine, _db) = setup().await;
    let policy = ScopePolicy::builder()
        .allow("orders", col("user_id").eq(principal()))
        .validate(&schema())
        .expect("policy");
    let alice = Principal::new().set("principal", 1);
    let bob = Principal::new().set("principal", 2);

    // Rebuild alice's orders: delete, then insert — one transaction.
    let delete = engine
        .compile_scoped(
            "mutation { delete_orders(where: {}) { affected_rows } }",
            &policy,
        )
        .expect("compile delete");
    // Written out: an insert's rows are its shape, so they cannot be
    // variables (see the crate docs). One compiled statement per row here.
    let insert = |title: &str| {
        engine
            .compile_scoped(
                &format!(
                    r#"mutation {{ insert_orders_one(object: {{user_id: 1, title: "{title}"}}) {{ title }} }}"#
                ),
                &policy,
            )
            .expect("compile insert")
    };
    let (a3, a4, a5) = (insert("a-3"), insert("a-4"), insert("a-5"));
    let list = engine
        .compile("{ orders(order_by: {title: asc}) { title } }")
        .expect("compile list");

    // Rolled back: the delete inside is undone with the closure's Err.
    let err = engine
        .transaction(async |tx| {
            let d = tx.execute_scoped(&delete, None, &alice).await?;
            assert_eq!(d["delete_orders"]["affected_rows"], 2);
            let seen = tx.execute(&list, None).await?;
            assert_eq!(titles(&seen, "orders"), ["b-1"], "deleted inside the tx");
            Err::<(), _>(Error::Validate {
                path: "test".into(),
                message: "abort".into(),
            })
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Validate { .. }), "{err:?}");
    let after = engine.execute(&list, None).await.unwrap();
    assert_eq!(titles(&after, "orders"), ["a-1", "a-2", "b-1"]);

    // Committed: both statements land together, each under alice's scope.
    let out: Value = engine
        .transaction(async |tx| {
            tx.execute_scoped(&delete, None, &alice).await?;
            tx.execute_scoped(&a3, None, &alice).await?;
            let inserted: serde_json::Map<String, Value> =
                tx.execute_scoped_as(&a4, None, &alice).await?;
            assert_eq!(inserted["title"], "a-4");
            tx.execute(&list, None).await
        })
        .await
        .unwrap();
    assert_eq!(titles(&out, "orders"), ["a-3", "a-4", "b-1"]);

    // Scope holds inside the transaction: bob's delete touches nothing of
    // alice's, and bob cannot insert a row for alice — the post-insert check
    // fails the statement from inside Postgres (a `Database` error, as on the
    // pool), and with it the transaction. The row state below is the proof;
    // the error's text is log-only.
    let bobs = engine
        .transaction(async |tx| {
            let d = tx.execute_scoped(&delete, None, &bob).await?;
            Ok::<_, Error>(d["delete_orders"]["affected_rows"].as_i64().unwrap())
        })
        .await
        .unwrap();
    assert_eq!(bobs, 1, "bob deletes bob's row only");
    let err = engine
        .transaction(async |tx| tx.execute_scoped(&a5, None, &bob).await)
        .await
        .unwrap_err();
    assert!(matches!(&err, Error::Database(_)), "{err:?}");
    let after = engine.execute(&list, None).await.unwrap();
    assert_eq!(titles(&after, "orders"), ["a-3", "a-4"]);

    // The pairing guard is the same one the pool applies.
    let err = engine
        .transaction(async |tx| tx.execute(&delete, None).await)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::Scope(m) if m.contains("execute_scoped")),
        "{err:?}"
    );
    let err = engine
        .transaction(async |tx| tx.execute_scoped(&list, None, &alice).await)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::Scope(m) if m.contains("compile_scoped")),
        "{err:?}"
    );
}

/// The host holds the transaction; the engine runs its statements in it.
/// What the field report's second shape needs: native SQL and policy-bound
/// statements on one connection the host begins, times out and commits.
#[tokio::test(flavor = "multi_thread")]
async fn a_host_holding_its_own_transaction_runs_scoped_statements_in_it() {
    let (engine, db) = setup().await;
    let pool = db.pool.clone();
    let policy = ScopePolicy::builder()
        .allow("orders", col("user_id").eq(principal()))
        .validate(&schema())
        .expect("policy");
    let alice = Principal::new().set("principal", 1);
    let bob = Principal::new().set("principal", 2);
    let delete = engine
        .compile_scoped(
            "mutation { delete_orders(where: {}) { affected_rows } }",
            &policy,
        )
        .expect("compile delete");
    let a3 = engine
        .compile_scoped(
            r#"mutation { insert_orders_one(object: {user_id: 1, title: "a-3"}) { title } }"#,
            &policy,
        )
        .expect("compile insert");
    let list = engine
        .compile("{ orders(order_by: {title: asc}) { title } }")
        .expect("compile list");

    // Rolled back by the host. The engine's statements see the host's
    // uncommitted native insert — same connection — and the pool sees none
    // of it.
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("INSERT INTO orders (user_id, title) VALUES (1, 'raw-1')")
        .execute(&mut *tx)
        .await
        .expect("native insert");
    let d = engine
        .execute_scoped_on(&mut *tx, &delete, None, &alice)
        .await
        .unwrap();
    assert_eq!(d["delete_orders"]["affected_rows"], 3, "a-1, a-2 and raw-1");
    engine
        .execute_scoped_on(&mut *tx, &a3, None, &alice)
        .await
        .unwrap();
    let inside = engine.execute_on(&mut *tx, &list, None).await.unwrap();
    assert_eq!(titles(&inside, "orders"), ["a-3", "b-1"]);
    let outside = engine.execute(&list, None).await.unwrap();
    assert_eq!(titles(&outside, "orders"), ["a-1", "a-2", "b-1"]);
    tx.rollback().await.expect("rollback");
    let after = engine.execute(&list, None).await.unwrap();
    assert_eq!(titles(&after, "orders"), ["a-1", "a-2", "b-1"]);

    // Committed by the host, with the scoped text surface on the same
    // connection: bob's delete under his set touches only his row.
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("INSERT INTO orders (user_id, title) VALUES (2, 'raw-2')")
        .execute(&mut *tx)
        .await
        .expect("native insert");
    let bobs = engine
        .scoped(policy.bind(&bob).expect("bind"))
        .query_on(
            &mut *tx,
            "mutation { delete_orders(where: {}) { affected_rows } }",
            None,
        )
        .await
        .unwrap();
    assert_eq!(bobs["delete_orders"]["affected_rows"], 2, "b-1 and raw-2");
    let inserted: serde_json::Map<String, Value> = engine
        .execute_scoped_as_on(&mut *tx, &a3, None, &alice)
        .await
        .unwrap();
    assert_eq!(inserted["title"], "a-3");
    let seen = engine
        .query_on(
            &mut *tx,
            "{ orders(order_by: {title: asc}) { title } }",
            None,
        )
        .await
        .unwrap();
    assert_eq!(titles(&seen, "orders"), ["a-1", "a-2", "a-3"]);
    tx.commit().await.expect("commit");
    let after = engine.execute(&list, None).await.unwrap();
    assert_eq!(titles(&after, "orders"), ["a-1", "a-2", "a-3"]);

    // The guards are the pool's: pairing, and the set's denial.
    let mut tx = pool.begin().await.expect("begin");
    let err = engine
        .execute_on(&mut *tx, &delete, None)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::Scope(m) if m.contains("execute_scoped")),
        "{err:?}"
    );
    let err = engine
        .execute_scoped_on(&mut *tx, &list, None, &alice)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::Scope(m) if m.contains("compile_scoped")),
        "{err:?}"
    );
    let err = engine
        .scoped(vision_graphql::ScopeSet::new())
        .query_on(&mut *tx, "{ orders { id } }", None)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ScopeDenied { .. }), "{err:?}");
    // bob cannot insert a row for alice through the host's connection either.
    let err = engine
        .execute_scoped_on(&mut *tx, &a3, None, &bob)
        .await
        .unwrap_err();
    assert!(matches!(&err, Error::Database(_)), "{err:?}");
    tx.rollback().await.expect("rollback");
    let after = engine.execute(&list, None).await.unwrap();
    assert_eq!(titles(&after, "orders"), ["a-1", "a-2", "a-3"]);
}

/// A statement is prepared on the connection with the types of the request
/// that first ran it and reused, by SQL text, for every later one. A null
/// used to go out as text whatever the column, so a compiled statement first
/// run with a null and then with a number failed on the second request — on
/// that connection only, which read as a flaky test rather than a bug.
#[tokio::test(flavor = "multi_thread")]
async fn a_null_first_run_does_not_fix_the_parameter_type_for_later_ones() {
    let (_engine, db) = setup().await;
    // One connection, so the second execution provably reuses the first's
    // prepared statement.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&db.url)
        .await
        .expect("connect");
    let engine = Engine::new(pool, schema());

    let q = engine
        .compile(
            "mutation($qty: Int, $ids: [Int!] @optional) {
                 update_orders(where: {id: {_in: $ids}}, _set: {qty: $qty}) { affected_rows }
             }",
        )
        .expect("compile");
    let data = engine
        .execute(&q, Some(json!({"qty": null, "ids": null})))
        .await
        .unwrap();
    assert_eq!(data["update_orders"]["affected_rows"], 3);
    let data = engine
        .execute(&q, Some(json!({"qty": 5, "ids": [1, 2]})))
        .await
        .unwrap();
    assert_eq!(data["update_orders"]["affected_rows"], 2);

    let q = engine
        .compile("query($ids: [Int!] @optional) { orders(where: {id: {_in: $ids}}, order_by: {id: asc}) { qty } }")
        .expect("compile");
    let data = engine
        .execute(&q, Some(json!({"ids": null})))
        .await
        .unwrap();
    assert_eq!(
        data["orders"],
        json!([{"qty": 5}, {"qty": 5}, {"qty": null}])
    );
    let data = engine.execute(&q, Some(json!({"ids": [3]}))).await.unwrap();
    assert_eq!(data["orders"], json!([{"qty": null}]));
}
