//! Mutations on SQLite, against a real in-memory database.
//!
//! A mutation here is a sequence of statements in one transaction (see
//! `vision_graphql::plan`); what these tests pin is that the sequence answers
//! as PostgreSQL's one statement does — the same `affected_rows`, the same
//! `returning`, the same scope guard — and that a failure part-way leaves
//! nothing behind.

#![cfg(feature = "sqlite")]

use serde_json::{json, Value};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use vision_graphql::ast::{BoolExpr, CmpOp};
use vision_graphql::sqlite::connect_options;
use vision_graphql::{Engine, Error, Mutation, Schema, ScopeSet};

const DDL: &str = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    active BOOLEAN NOT NULL DEFAULT 1,
    meta JSON,
    score REAL DEFAULT 0
);
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    title TEXT NOT NULL,
    views INT NOT NULL DEFAULT 0
);
CREATE TABLE tags (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL REFERENCES posts,
    label TEXT NOT NULL
);
CREATE TABLE keyed (k TEXT PRIMARY KEY, n INT) WITHOUT ROWID;
CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1;
INSERT INTO users (id, name, active, meta) VALUES (1, 'Ann', 1, '{"tags":["a"]}'), (2, 'bob', 0, NULL);
INSERT INTO posts (id, user_id, title, views) VALUES (1, 1, 'zeta', 10), (2, 1, 'alpha', 30), (3, 2, 'mid', 20);
INSERT INTO tags (id, post_id, label) VALUES (1, 2, 'x');
"#;

async fn pool() -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options("sqlite::memory:").unwrap())
        .await
        .unwrap();
    sqlx::raw_sql(DDL).execute(&pool).await.unwrap();
    pool
}

async fn engine() -> Engine<sqlx::Sqlite> {
    let pool = pool().await;
    let schema = Schema::introspect_sqlite(&pool).await.unwrap().build();
    Engine::new(pool, schema)
}

async fn q(e: &Engine<sqlx::Sqlite>, src: &str) -> Value {
    e.query(src, None).await.unwrap()
}

async fn count(e: &Engine<sqlx::Sqlite>, table: &str) -> i64 {
    let v = q(
        e,
        &format!("{{ {table}_aggregate {{ aggregate {{ count }} }} }}"),
    )
    .await;
    v[format!("{table}_aggregate")]["aggregate"]["count"]
        .as_i64()
        .unwrap()
}

#[tokio::test]
async fn insert_returns_the_rows_with_their_types_in_order() {
    let e = engine().await;
    let v = q(
        &e,
        r#"mutation { insert_users(objects: [{name: "Cara", active: false, meta: {k: [1, 2]}}, {name: "Dan"}]) {
            affected_rows __typename
            returning { id name active meta score first: meta(path: "k.0") }
        } }"#,
    )
    .await;
    assert_eq!(
        v["insert_users"],
        json!({
            "affected_rows": 2,
            "__typename": "users_mutation_response",
            "returning": [
                {"id": 3, "name": "Cara", "active": false, "meta": {"k": [1, 2]}, "score": 0.0, "first": 1},
                {"id": 4, "name": "Dan", "active": true, "meta": null, "score": 0.0, "first": null}
            ]
        })
    );
    assert_eq!(count(&e, "users").await, 4);
    // An empty batch is refused by the lowering, on every backend.
    let err = e
        .query(
            "mutation { insert_users(objects: []) { affected_rows } }",
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Validate { .. }), "{err}");
}

#[tokio::test]
async fn insert_one_and_a_row_of_defaults() {
    let e = engine().await;
    let v = q(
        &e,
        r#"mutation { insert_users_one(object: {name: "Eve"}) { id name active } }"#,
    )
    .await;
    assert_eq!(
        v["insert_users_one"],
        json!({"id": 3, "name": "Eve", "active": true})
    );
    let v = q(
        &e,
        r#"mutation { insert_users_one(object: {name: "Fay"}) { __typename } }"#,
    )
    .await;
    assert_eq!(v["insert_users_one"], json!({"__typename": "users"}));
}

#[tokio::test]
async fn nested_inserts_in_both_directions() {
    let e = engine().await;
    // Array relation: children get the parent's key; affected_rows counts them.
    let v = q(
        &e,
        r#"mutation { insert_users(objects: [
            {name: "Cara", posts: {data: [{title: "c1", tags: {data: [{label: "t1"}, {label: "t2"}]}}, {title: "c2"}]}},
            {name: "Dan", posts: {data: [{title: "d1"}]}}
        ]) { affected_rows returning { name posts(order_by: {title: asc}) { title user_id tags { label } } } } }"#,
    )
    .await;
    assert_eq!(v["insert_users"]["affected_rows"], json!(7));
    assert_eq!(
        v["insert_users"]["returning"],
        json!([
            {"name": "Cara", "posts": [
                {"title": "c1", "user_id": 3, "tags": [{"label": "t1"}, {"label": "t2"}]},
                {"title": "c2", "user_id": 3, "tags": []}
            ]},
            {"name": "Dan", "posts": [{"title": "d1", "user_id": 4, "tags": []}]}
        ])
    );
    // Object relation: the pointed-at row goes first and lends its key.
    let v = q(
        &e,
        r#"mutation { insert_posts_one(object: {title: "by new", user: {data: {name: "Gus"}}}) { title user { name } } }"#,
    )
    .await;
    assert_eq!(
        v["insert_posts_one"],
        json!({"title": "by new", "user": {"name": "Gus"}})
    );
    // An object relation that hits an existing row on a no-op upsert still
    // lends that row's key.
    let v = q(
        &e,
        r#"mutation { insert_posts_one(object: {title: "for Ann", user: {data: {name: "Ann"}, on_conflict: {constraint: sqlite_autoindex_users_1, update_columns: []}}}) { user_id user { name } } }"#,
    )
    .await;
    assert_eq!(
        v["insert_posts_one"],
        json!({"user_id": 1, "user": {"name": "Ann"}})
    );
    assert_eq!(count(&e, "users").await, 5);
}

#[tokio::test]
async fn on_conflict_by_constraint_name_and_by_pkey() {
    let e = engine().await;
    let v = q(
        &e,
        r#"mutation { insert_users(objects: [{name: "Ann", active: false}], on_conflict: {constraint: sqlite_autoindex_users_1, update_columns: [active]}) { affected_rows returning { id active } } }"#,
    )
    .await;
    assert_eq!(
        v["insert_users"],
        json!({"affected_rows": 1, "returning": [{"id": 1, "active": false}]})
    );
    // DO NOTHING at the top level: no row, nothing returned.
    let v = q(
        &e,
        r#"mutation { insert_users(objects: [{id: 1, name: "Ann"}], on_conflict: {constraint: users_pkey, update_columns: []}) { affected_rows returning { id } } }"#,
    )
    .await;
    assert_eq!(
        v["insert_users"],
        json!({"affected_rows": 0, "returning": []})
    );
    let v = q(
        &e,
        r#"mutation { insert_users_one(object: {id: 2, name: "bob"}, on_conflict: {constraint: users_pkey, update_columns: []}) { id } }"#,
    )
    .await;
    assert_eq!(v["insert_users_one"], Value::Null);
    // The update's own WHERE.
    let v = q(
        &e,
        r#"mutation { insert_users(objects: [{name: "bob", score: 5}], on_conflict: {constraint: sqlite_autoindex_users_1, update_columns: [score], where: {active: {_eq: true}}}) { affected_rows } }"#,
    )
    .await;
    assert_eq!(v["insert_users"]["affected_rows"], json!(0));
    assert_eq!(count(&e, "users").await, 2);
}

#[tokio::test]
async fn update_and_update_by_pk() {
    let e = engine().await;
    let v = q(
        &e,
        r#"mutation { update_posts(where: {user_id: {_eq: 1}}, _set: {views: 0}) { affected_rows returning { title views user { name } } } }"#,
    )
    .await;
    assert_eq!(v["update_posts"]["affected_rows"], json!(2));
    assert_eq!(
        v["update_posts"]["returning"],
        json!([{"title": "zeta", "views": 0, "user": {"name": "Ann"}}, {"title": "alpha", "views": 0, "user": {"name": "Ann"}}])
    );
    let v = q(&e, r#"mutation { update_users_by_pk(pk_columns: {id: 2}, _set: {active: true, meta: {x: 1}}) { name active meta } }"#).await;
    assert_eq!(
        v["update_users_by_pk"],
        json!({"name": "bob", "active": true, "meta": {"x": 1}})
    );
    let v = q(
        &e,
        r#"mutation { update_users_by_pk(pk_columns: {id: 99}, _set: {active: true}) { name } }"#,
    )
    .await;
    assert_eq!(v["update_users_by_pk"], Value::Null);
    let v = q(&e, r#"mutation { update_users(where: {id: {_eq: 99}}, _set: {active: true}) { affected_rows returning { id } } }"#).await;
    assert_eq!(
        v["update_users"],
        json!({"affected_rows": 0, "returning": []})
    );
}

#[tokio::test]
async fn delete_returns_what_the_rows_were() {
    let e = engine().await;
    let v = q(
        &e,
        r#"mutation { delete_posts(where: {views: {_lte: 20}}) { affected_rows __typename returning { id title __typename } } }"#,
    )
    .await;
    assert_eq!(
        v["delete_posts"],
        json!({"affected_rows": 2, "__typename": "posts_mutation_response",
               "returning": [{"id": 1, "title": "zeta", "__typename": "posts"}, {"id": 3, "title": "mid", "__typename": "posts"}]})
    );
    assert_eq!(count(&e, "posts").await, 1);
    // A row something still points at cannot go: the database says so, and
    // the plan's transaction takes the refusal back to nothing.
    let err = e
        .query(
            r#"mutation { delete_posts(where: {id: {_eq: 2}}) { affected_rows } }"#,
            None,
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("FOREIGN KEY"), "{err}");
    assert_eq!(count(&e, "posts").await, 1);
    let v = q(
        &e,
        r#"mutation { delete_users_by_pk(id: 2) { name active first: meta(path: "tags.0") } }"#,
    )
    .await;
    assert_eq!(
        v["delete_users_by_pk"],
        json!({"name": "bob", "active": false, "first": null})
    );
    let v = q(&e, r#"mutation { delete_users_by_pk(id: 2) { name } }"#).await;
    assert_eq!(v["delete_users_by_pk"], Value::Null);
    // A relation of a row that is gone cannot be read.
    let err = e
        .query(
            r#"mutation { delete_users(where: {id: {_eq: 1}}) { returning { posts { title } } } }"#,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    assert_eq!(count(&e, "users").await, 1);
}

#[tokio::test]
async fn a_failure_part_way_leaves_nothing_behind() {
    let e = engine().await;
    // The second object violates NOT NULL on posts.title after the first
    // user and its post were written.
    let err = e
        .query(
            r#"mutation { insert_users(objects: [{name: "Cara", posts: {data: [{title: "ok"}]}}, {name: "Dan", posts: {data: [{views: 1}]}}]) { affected_rows } }"#,
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Database(_) | Error::Validate { .. }),
        "{err}"
    );
    assert_eq!(count(&e, "users").await, 2);
    assert_eq!(count(&e, "posts").await, 3);
}

#[tokio::test]
async fn later_fields_see_earlier_writes() {
    // The documented divergence: PostgreSQL's CTEs share one snapshot, so
    // the delete there would count the rows as they were.
    let e = engine().await;
    let v = q(
        &e,
        r#"mutation {
            a: insert_posts(objects: [{user_id: 2, title: "late"}]) { affected_rows }
            b: delete_posts(where: {user_id: {_eq: 2}}) { affected_rows }
        }"#,
    )
    .await;
    assert_eq!(v["a"]["affected_rows"], json!(1));
    assert_eq!(v["b"]["affected_rows"], json!(2));
}

fn scope_of(user: i64) -> ScopeSet {
    let eq = |column: &str| BoolExpr::Compare {
        column: column.into(),
        op: CmpOp::Eq,
        value: json!(user).into(),
    };
    ScopeSet::new()
        .allow("users", eq("id"))
        .allow("posts", eq("user_id"))
}

#[tokio::test]
async fn scope_guards_every_write() {
    let e = engine().await;
    let scoped = e.scoped(scope_of(1));
    // In scope: fine, and returning is scoped too.
    let v = scoped
        .query(r#"mutation { insert_posts(objects: [{user_id: 1, title: "mine"}]) { affected_rows returning { user { name } } } }"#, None)
        .await
        .unwrap();
    assert_eq!(
        v["insert_posts"]["returning"],
        json!([{"user": {"name": "Ann"}}])
    );
    // Out of scope: refused after the write, and the write is undone — the
    // first, in-scope object too.
    let err = scoped
        .query(r#"mutation { insert_posts(objects: [{user_id: 1, title: "ok"}, {user_id: 2, title: "not mine"}]) { affected_rows } }"#, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("scope check violation"), "{err}");
    assert_eq!(count(&e, "posts").await, 4);
    // A nested child out of scope fails the whole thing.
    let err = scoped
        .query(r#"mutation { insert_users(objects: [{name: "Zed", posts: {data: [{title: "x"}]}}]) { affected_rows } }"#, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("scope check violation"), "{err}");
    assert_eq!(count(&e, "users").await, 2);
    // An update may not move a row out of scope, and only sees its own rows.
    let err = scoped
        .query(r#"mutation { update_posts(where: {id: {_eq: 1}}, _set: {user_id: 2}) { affected_rows } }"#, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("scope check violation"), "{err}");
    let v = scoped
        .query(r#"mutation { update_posts(where: {views: {_gte: 0}}, _set: {views: 1}) { affected_rows } }"#, None)
        .await
        .unwrap();
    assert_eq!(v["update_posts"]["affected_rows"], json!(3));
    let v = scoped
        .query(
            r#"mutation { update_posts_by_pk(pk_columns: {id: 3}, _set: {views: 9}) { id } }"#,
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["update_posts_by_pk"], Value::Null);
    // Deletes are filtered: post 3 is user 2's and stays.
    let v = scoped
        .query(
            r#"mutation { delete_posts(where: {id: {_gt: 2}}) { affected_rows } }"#,
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["delete_posts"]["affected_rows"], json!(1));
    assert_eq!(count(&e, "posts").await, 3);
    let v = scoped
        .query(r#"mutation { delete_posts_by_pk(id: 3) { id } }"#, None)
        .await
        .unwrap();
    assert_eq!(v["delete_posts_by_pk"], Value::Null);
    let err = scoped
        .query(
            r#"mutation { delete_tags(where: {id: {_eq: 1}}) { affected_rows } }"#,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ScopeDenied { .. }), "{err}");
}

#[tokio::test]
async fn compiled_persisted_builder_and_transactions() {
    let e = engine().await;
    // Compiled once, run with different variables: the plan's statements
    // are fixed, their parameters are not.
    let compiled = e
        .compile(r#"mutation($ids: [bigint!]!, $v: bigint!) { update_posts(where: {id: {_in: $ids}}, _set: {views: $v}) { affected_rows returning { id views } } insert_users_one(object: {name: "Cara"}) { id } }"#)
        .unwrap();
    assert!(compiled.sql().contains("INSERT INTO"), "{}", compiled.sql());
    let mut vars = compiled.variables();
    vars.sort();
    assert_eq!(vars, vec!["ids", "v"]);
    let v = e
        .execute(&compiled, Some(json!({"ids": [1, 2], "v": 7})))
        .await
        .unwrap();
    assert_eq!(
        v["update_posts"],
        json!({"affected_rows": 2, "returning": [{"id": 1, "views": 7}, {"id": 2, "views": 7}]})
    );
    assert_eq!(v["insert_users_one"]["id"], json!(3));
    // The second run's insert conflicts; the whole plan is undone, updates
    // included.
    let err = e
        .execute(&compiled, Some(json!({"ids": [3], "v": 1})))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("UNIQUE"), "{err}");
    let v = q(&e, "{ posts_by_pk(id: 3) { views } }").await;
    assert_eq!(v["posts_by_pk"]["views"], json!(20));

    let reg = vision_graphql::QueryRegistry::compile_all(
        &e,
        [(
            "del",
            "mutation($id: bigint!) { delete_posts_by_pk(id: $id) { title } }",
        )],
    )
    .unwrap();
    let v = e
        .execute(reg.require("del").unwrap(), Some(json!({"id": 3})))
        .await
        .unwrap();
    assert_eq!(v["delete_posts_by_pk"], json!({"title": "mid"}));

    let v = e
        .run(Mutation::insert(
            "posts",
            vec![[
                ("user_id".to_string(), json!(1)),
                ("title".to_string(), json!("built")),
            ]
            .into_iter()
            .collect()],
        ))
        .await
        .unwrap();
    assert_eq!(v["insert_posts"]["affected_rows"], json!(1));

    // The engine's transaction: rolled back on Err, every statement of the
    // plan with it.
    let err = e
        .transaction(async |tx| {
            tx.query(
                r#"mutation { insert_users_one(object: {name: "Temp"}) { id } }"#,
                None,
            )
            .await?;
            Err::<(), _>(Error::Schema("abort".into()))
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("abort"));
    assert_eq!(count(&e, "users").await, 3);

    // A caller's transaction through the _on twins: the plan is a savepoint
    // in it.
    let mut tx = e.pool().begin().await.unwrap();
    let v = e
        .query_on(
            &mut *tx,
            r#"mutation { insert_users_one(object: {name: "Held"}) { id } }"#,
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["insert_users_one"]["id"], json!(4));
    tx.rollback().await.unwrap();
    assert_eq!(count(&e, "users").await, 3);
    let v = e
        .query_on(
            e.pool(),
            r#"mutation { insert_users_one(object: {name: "Pooled"}) { id } }"#,
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["insert_users_one"]["id"], json!(4));
}

#[tokio::test]
async fn a_without_rowid_table_is_refused_loudly_and_a_view_is_read_only() {
    let e = engine().await;
    let err = e
        .query(
            r#"mutation { insert_keyed_one(object: {k: "a", n: 1}) { k } }"#,
            None,
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("rowid"), "{err}");
    let err = e
        .run(Mutation::insert(
            "active_users",
            vec![[("name".to_string(), json!("x"))].into_iter().collect()],
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("read-only"), "{err}");
}
