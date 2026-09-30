//! Mutations on MySQL, against a real MySQL. Set `MYSQL_TEST_URL` as for
//! `tests/mysql_read.rs`.
//!
//! A mutation here is a sequence of statements in one transaction (see
//! `vision_graphql::plan`), with rows identified by primary key and read
//! back after each write, MySQL having no `RETURNING`. What these tests pin
//! is that the sequence answers as PostgreSQL's one statement does — the
//! same `affected_rows`, the same `returning`, the same scope guard — and
//! that a failure part-way leaves nothing behind.

#![cfg(feature = "mysql")]

mod common;

use common::mysql::{fresh_db, run_ddl, TestDb};
use serde_json::{json, Value};
use vision_graphql::ast::{BoolExpr, CmpOp};
use vision_graphql::{Engine, Error, Mutation, Schema, ScopeSet};

const DDL: &str = r#"
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    meta JSON,
    score DOUBLE DEFAULT 0,
    seen_at TIMESTAMP(6) NULL,
    UNIQUE KEY users_name_key (name)
);
CREATE TABLE posts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    title VARCHAR(100) NOT NULL,
    views INT NOT NULL DEFAULT 0,
    CONSTRAINT posts_user_fk FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE TABLE tags (
    id INT AUTO_INCREMENT PRIMARY KEY,
    post_id INT NOT NULL,
    label VARCHAR(20) NOT NULL,
    CONSTRAINT tags_post_fk FOREIGN KEY (post_id) REFERENCES posts(id)
);
CREATE TABLE memberships (user_id INT NOT NULL, post_id INT NOT NULL, role VARCHAR(10), PRIMARY KEY (user_id, post_id));
CREATE TABLE tokens (id CHAR(36) NOT NULL DEFAULT (UUID()) PRIMARY KEY, note VARCHAR(20));
CREATE TABLE log (line VARCHAR(50));
CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1;
INSERT INTO users (id, name, active, meta, seen_at) VALUES (1, 'Ann', 1, '{"tags":["a"]}', '2026-01-01 00:00:00'), (2, 'bob', 0, NULL, NULL);
INSERT INTO posts (id, user_id, title, views) VALUES (1, 1, 'zeta', 10), (2, 1, 'alpha', 30), (3, 2, 'mid', 20);
INSERT INTO tags (id, post_id, label) VALUES (1, 2, 'x');
"#;

async fn engine() -> (Engine<sqlx::MySql>, TestDb) {
    let db = fresh_db().await;
    run_ddl(&db.pool, DDL).await;
    let schema = Schema::introspect_mysql(&db.pool).await.unwrap().build();
    (Engine::new(db.pool.clone(), schema), db)
}

async fn q(e: &Engine<sqlx::MySql>, src: &str) -> Value {
    e.query(src, None)
        .await
        .unwrap_or_else(|err| panic!("{src}: {err}"))
}

async fn count(e: &Engine<sqlx::MySql>, table: &str) -> i64 {
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
    let (e, _db) = engine().await;
    let v = q(
        &e,
        r#"mutation { insert_users(objects: [{name: "Cara", active: false, meta: {k: [1, 2]}, seen_at: "2026-02-03T04:05:06+00:00"}, {name: "Dan"}]) {
            affected_rows __typename
            returning { id name active meta score seen_at first: meta(path: "k.0") }
        } }"#,
    )
    .await;
    assert_eq!(
        v["insert_users"],
        json!({
            "affected_rows": 2,
            "__typename": "users_mutation_response",
            "returning": [
                {"id": 3, "name": "Cara", "active": false, "meta": {"k": [1, 2]}, "score": 0.0,
                 "seen_at": "2026-02-03T04:05:06+00:00", "first": 1},
                {"id": 4, "name": "Dan", "active": true, "meta": null, "score": 0.0,
                 "seen_at": null, "first": null}
            ]
        })
    );
    // The response's keys are in selection order, at every level.
    assert!(
        serde_json::to_string(&v["insert_users"]["returning"][0])
            .unwrap()
            .starts_with(r#"{"id":3,"name":"Cara","active":false,"meta":"#),
        "{v}"
    );
    assert_eq!(count(&e, "users").await, 4);
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
async fn insert_one_a_row_of_defaults_and_a_supplied_key() {
    let (e, _db) = engine().await;
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
    // A key the object supplies is the key it is read back by.
    let v = q(
        &e,
        r#"mutation { insert_users_one(object: {id: 50, name: "Gus"}) { id name } }"#,
    )
    .await;
    assert_eq!(v["insert_users_one"], json!({"id": 50, "name": "Gus"}));
    // A row of defaults: `INSERT INTO t () VALUES ()`.
    let v = e
        .run(Mutation::insert("log", vec![Default::default()]))
        .await;
    // `log` has no primary key, so the write is refused before it runs.
    let err = v.unwrap_err();
    assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    assert!(err.to_string().contains("primary key"), "{err}");
    let v = e
        .run(Mutation::insert("tokens", vec![Default::default()]))
        .await
        .unwrap_err();
    // A generated key that is not AUTO_INCREMENT cannot be read back.
    assert!(matches!(v, Error::Unsupported { .. }), "{v}");
    assert!(v.to_string().contains("supply 'id'"), "{v}");
    let v = q(
        &e,
        r#"mutation { insert_tokens_one(object: {id: "0d1f4b3a-0000-4000-8000-000000000001", note: "n"}) { id note } }"#,
    )
    .await;
    assert_eq!(
        v["insert_tokens_one"],
        json!({"id": "0d1f4b3a-0000-4000-8000-000000000001", "note": "n"})
    );
}

#[tokio::test]
async fn nested_inserts_in_both_directions() {
    let (e, _db) = engine().await;
    let v = q(
        &e,
        r#"mutation { insert_users(objects: [
            {name: "Cara", posts: {data: [{title: "c1", tags: {data: [{label: "t1"}, {label: "t2"}]}}, {title: "c2"}]}},
            {name: "Dan", posts: {data: [{title: "d1"}]}}
        ]) { affected_rows returning { name posts(order_by: {title: asc}) { title user_id tags(order_by: {label: asc}) { label } } } } }"#,
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
    // lends that row's key — read back through LAST_INSERT_ID(id).
    let v = q(
        &e,
        r#"mutation { insert_posts_one(object: {title: "for Ann", user: {data: {name: "Ann"}, on_conflict: {constraint: users_name_key, update_columns: []}}}) { user_id user { name } } }"#,
    )
    .await;
    assert_eq!(
        v["insert_posts_one"],
        json!({"user_id": 1, "user": {"name": "Ann"}})
    );
    // The same with the key supplied: a no-op update on the key itself,
    // which MySQL reports as a row found (sqlx asks for found rows, not
    // changed rows), so the row is read back and lent.
    let v = q(
        &e,
        r#"mutation { insert_posts_one(object: {title: "for Ann again", user: {data: {id: 1, name: "Ann"}, on_conflict: {constraint: users_pkey, update_columns: []}}}) { user_id user { name } } }"#,
    )
    .await;
    assert_eq!(
        v["insert_posts_one"],
        json!({"user_id": 1, "user": {"name": "Ann"}})
    );
    // And a nested upsert that updates: the row is read back by the key
    // the object supplied.
    let v = q(
        &e,
        r#"mutation { insert_posts_one(object: {title: "for bob", user: {data: {id: 2, name: "bob", active: true}, on_conflict: {constraint: users_pkey, update_columns: [active]}}}) { user_id user { name active } } }"#,
    )
    .await;
    assert_eq!(
        v["insert_posts_one"],
        json!({"user_id": 2, "user": {"name": "bob", "active": true}})
    );
    assert_eq!(count(&e, "users").await, 5);
    // A composite key: the parent lends one half, the object gives the other.
    let v = q(
        &e,
        r#"mutation { insert_memberships(objects: [{user_id: 1, post_id: 3, role: "r"}, {user_id: 2, post_id: 3}]) { affected_rows returning { user_id post_id role } } }"#,
    )
    .await;
    assert_eq!(
        v["insert_memberships"],
        json!({"affected_rows": 2, "returning": [{"user_id": 1, "post_id": 3, "role": "r"}, {"user_id": 2, "post_id": 3, "role": null}]})
    );
    let v = q(
        &e,
        r#"mutation { update_memberships(where: {post_id: {_eq: 3}}, _set: {role: "x"}) { affected_rows returning { user_id role } } }"#,
    )
    .await;
    assert_eq!(
        v["update_memberships"],
        json!({"affected_rows": 2, "returning": [{"user_id": 1, "role": "x"}, {"user_id": 2, "role": "x"}]})
    );
    let v = q(
        &e,
        r#"mutation { delete_memberships_by_pk(user_id: 1, post_id: 3) { role } }"#,
    )
    .await;
    assert_eq!(v["delete_memberships_by_pk"], json!({"role": "x"}));
}

#[tokio::test]
async fn on_conflict_by_constraint_name_and_by_pkey() {
    let (e, _db) = engine().await;
    let v = q(
        &e,
        r#"mutation { insert_users(objects: [{name: "Ann", active: false}], on_conflict: {constraint: users_name_key, update_columns: [active]}) { affected_rows returning { id active } } }"#,
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
    // A DO NOTHING that finds no row inserts, counts and returns it. (Not
    // by id: MySQL hands out an AUTO_INCREMENT value to the upsert above
    // before finding the duplicate, and never hands it out again.)
    let v = q(
        &e,
        r#"mutation { insert_users(objects: [{name: "Cara"}], on_conflict: {constraint: users_name_key, update_columns: []}) { affected_rows returning { name } } }"#,
    )
    .await;
    assert_eq!(
        v["insert_users"],
        json!({"affected_rows": 1, "returning": [{"name": "Cara"}]})
    );
    // The update's own WHERE has no MySQL spelling that keeps the row out
    // of the response when it fails: refused, not approximated.
    let err = e
        .query(
            r#"mutation { insert_users(objects: [{name: "bob", score: 5}], on_conflict: {constraint: users_name_key, update_columns: [score], where: {active: {_eq: true}}}) { affected_rows } }"#,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    let err = e
        .query(
            r#"mutation { insert_users(objects: [{name: "x"}], on_conflict: {constraint: nope, update_columns: []}) { affected_rows } }"#,
            None,
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("nope"), "{err}");
    assert_eq!(count(&e, "users").await, 3);
}

#[tokio::test]
async fn a_key_cannot_change_under_an_update() {
    let (e, _db) = engine().await;
    // The rows are picked, guarded and read back by key; a key that moved
    // would make every one of those a miss. Refused, not missed.
    for src in [
        r#"mutation { update_posts(where: {id: {_eq: 1}}, _set: {id: 99}) { affected_rows } }"#,
        r#"mutation { update_posts_by_pk(pk_columns: {id: 1}, _set: {id: 99, views: 1}) { id } }"#,
    ] {
        let err = e.query(src, None).await.unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{src}: {err}");
        assert!(err.to_string().contains("_set.id"), "{src}: {err}");
    }
    let v = q(&e, "{ posts_by_pk(id: 1) { views } }").await;
    assert_eq!(v["posts_by_pk"]["views"], json!(10));
}

#[tokio::test]
async fn an_upsert_reads_back_the_row_the_constraint_found() {
    let (e, _db) = engine().await;
    // The object names a key of its own, but conflicts on the constraint:
    // MySQL updates Ann (id 1), and that is the row read back.
    let v = q(
        &e,
        r#"mutation { insert_users(objects: [{id: 5, name: "Ann", active: false}], on_conflict: {constraint: users_name_key, update_columns: [active]}) { affected_rows returning { id name active } } }"#,
    )
    .await;
    assert_eq!(
        v["insert_users"],
        json!({"affected_rows": 1, "returning": [{"id": 1, "name": "Ann", "active": false}]})
    );
    assert_eq!(count(&e, "users").await, 2);
    // And nested: the pointing row gets the key of the row that was found.
    let v = q(
        &e,
        r#"mutation { insert_posts_one(object: {title: "t", user: {data: {id: 7, name: "bob"}, on_conflict: {constraint: users_name_key, update_columns: []}}}) { user_id } }"#,
    )
    .await;
    assert_eq!(v["insert_posts_one"], json!({"user_id": 2}));
    // Conflicting on two keys at once — id 2 is bob's, the name is Ann's —
    // MySQL updates one of them; PostgreSQL would refuse. Refused here too,
    // after the fact, and undone.
    let err = e
        .query(
            r#"mutation { insert_users(objects: [{id: 2, name: "Ann", active: false}], on_conflict: {constraint: users_name_key, update_columns: [active]}) { affected_rows } }"#,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    assert!(err.to_string().contains("different unique keys"), "{err}");
    let v = q(&e, "{ users(order_by: {id: asc}) { id active } }").await;
    assert_eq!(
        v["users"],
        json!([{"id": 1, "active": false}, {"id": 2, "active": false}])
    );
}

#[tokio::test]
async fn a_delete_may_filter_through_its_own_table() {
    let (e, _db) = engine().await;
    // MySQL refuses a DELETE whose subquery reads the target table (error
    // 1093); the rows are picked by key first, so the filter is a SELECT's.
    let v = q(
        &e,
        r#"mutation { delete_tags(where: {post: {tags: {label: {_eq: "x"}}}}) { affected_rows returning { label } } }"#,
    )
    .await;
    assert_eq!(
        v["delete_tags"],
        json!({"affected_rows": 1, "returning": [{"label": "x"}]})
    );
    assert_eq!(count(&e, "tags").await, 0);
}

#[tokio::test]
async fn update_and_update_by_pk() {
    let (e, _db) = engine().await;
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
    // A row set to what it already is still counts: the rows are picked
    // before the update, as PostgreSQL counts matched rows.
    let v = q(
        &e,
        r#"mutation { update_posts(where: {user_id: {_eq: 1}}, _set: {views: 0}) { affected_rows } }"#,
    )
    .await;
    assert_eq!(v["update_posts"]["affected_rows"], json!(2));
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
    let (e, _db) = engine().await;
    let v = q(
        &e,
        r#"mutation { delete_posts(where: {views: {_lte: 20}}) { affected_rows __typename returning { id title __typename user { name } } } }"#,
    )
    .await;
    assert_eq!(
        v["delete_posts"],
        json!({"affected_rows": 2, "__typename": "posts_mutation_response",
               "returning": [{"id": 1, "title": "zeta", "__typename": "posts", "user": {"name": "Ann"}},
                             {"id": 3, "title": "mid", "__typename": "posts", "user": {"name": "bob"}}]})
    );
    assert_eq!(count(&e, "posts").await, 1);
    let v = q(&e, r#"mutation { delete_users_by_pk(id: 2) { name active first: meta(path: "tags.0") posts { title } } }"#).await;
    assert_eq!(
        v["delete_users_by_pk"],
        json!({"name": "bob", "active": false, "first": null, "posts": []})
    );
    let v = q(&e, r#"mutation { delete_users_by_pk(id: 2) { name } }"#).await;
    assert_eq!(v["delete_users_by_pk"], Value::Null);
    let v = q(
        &e,
        r#"mutation { delete_users(where: {id: {_eq: 2}}) { affected_rows returning { name } } }"#,
    )
    .await;
    assert_eq!(
        v["delete_users"],
        json!({"affected_rows": 0, "returning": []})
    );
    let err = e
        .query(
            r#"mutation { delete_posts(where: {id: {_eq: 2}}) { affected_rows } }"#,
            None,
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("foreign key"),
        "{err}"
    );
    assert_eq!(count(&e, "posts").await, 1);
    assert_eq!(count(&e, "users").await, 1);
}

#[tokio::test]
async fn a_conflict_that_inserts_nothing_inserts_no_children_either() {
    let (e, _db) = engine().await;
    let v = q(
        &e,
        r#"mutation { insert_users(objects: [{name: "Ann", posts: {data: [{title: "orphan"}]}}], on_conflict: {constraint: users_name_key, update_columns: []}) { affected_rows returning { id } } }"#,
    )
    .await;
    assert_eq!(
        v["insert_users"],
        json!({"affected_rows": 0, "returning": []})
    );
    assert_eq!(count(&e, "posts").await, 3);
    let err = e
        .run(
            Mutation::insert("users", vec![Default::default()]).on_conflict(
                vision_graphql::ast::OnConflict {
                    constraint: "users_pkey".into(),
                    update_columns: Vec::new(),
                    where_: None,
                },
            ),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unsupported { .. }), "{err}");
}

#[tokio::test]
async fn a_plan_inside_the_engines_transaction_is_a_savepoint() {
    let (e, _db) = engine().await;
    let n = e
        .transaction(async |tx| {
            tx.query(r#"mutation { insert_users_one(object: {name: "Kept"}) { id } }"#, None)
                .await?;
            let err = tx
                .query(
                    r#"mutation { insert_users(objects: [{name: "Gone", posts: {data: [{title: "ok"}]}}, {name: "Ann"}]) { affected_rows } }"#,
                    None,
                )
                .await
                .unwrap_err();
            assert!(err.to_string().contains("Duplicate"), "{err}");
            let v = tx.query("{ users_aggregate { aggregate { count } } }", None).await?;
            Ok(v["users_aggregate"]["aggregate"]["count"].as_i64().unwrap())
        })
        .await
        .unwrap();
    assert_eq!(n, 3);
    assert_eq!(count(&e, "users").await, 3);
    assert_eq!(count(&e, "posts").await, 3);
}

#[tokio::test]
async fn a_failure_part_way_leaves_nothing_behind() {
    let (e, _db) = engine().await;
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
    let (e, _db) = engine().await;
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
    let (e, _db) = engine().await;
    let scoped = e.scoped(scope_of(1));
    let v = scoped
        .query(r#"mutation { insert_posts(objects: [{user_id: 1, title: "mine"}]) { affected_rows returning { user { name } } } }"#, None)
        .await
        .unwrap();
    assert_eq!(
        v["insert_posts"]["returning"],
        json!([{"user": {"name": "Ann"}}])
    );
    let err = scoped
        .query(r#"mutation { insert_posts(objects: [{user_id: 1, title: "ok"}, {user_id: 2, title: "not mine"}]) { affected_rows } }"#, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("scope check violation"), "{err}");
    assert!(
        matches!(&err, Error::ScopeViolation { table, rows: 1, .. } if table == "posts"),
        "{err:?}"
    );
    assert_eq!(err.code(), vision_graphql::ErrorCode::ScopeDenied);
    assert_eq!(count(&e, "posts").await, 4);
    let err = scoped
        .query(r#"mutation { insert_users(objects: [{name: "Zed", posts: {data: [{title: "x"}]}}]) { affected_rows } }"#, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("scope check violation"), "{err}");
    assert_eq!(count(&e, "users").await, 2);
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
    let (e, _db) = engine().await;
    let compiled = e
        .compile(r#"mutation($ids: [Int!]!, $v: Int!) { update_posts(where: {id: {_in: $ids}}, _set: {views: $v}) { affected_rows returning { id views } } insert_users_one(object: {name: "Cara"}) { name } }"#)
        .unwrap();
    assert!(compiled.sql().contains("INSERT INTO"), "{}", compiled.sql());
    assert!(!compiled.sql().contains("?1"), "{}", compiled.sql());
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
    assert_eq!(v["insert_users_one"]["name"], json!("Cara"));
    // The second run's insert conflicts; the whole plan is undone, updates
    // included.
    let err = e
        .execute(&compiled, Some(json!({"ids": [3], "v": 1})))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Duplicate"), "{err}");
    let v = q(&e, "{ posts_by_pk(id: 3) { views } }").await;
    assert_eq!(v["posts_by_pk"]["views"], json!(20));

    let reg = vision_graphql::QueryRegistry::compile_all(
        &e,
        [(
            "del",
            "mutation($id: Int!) { delete_posts_by_pk(id: $id) { title } }",
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
    // plan with it. (Auto-increment values are not given back on MySQL, so
    // what is asserted is the rows, not their ids.)
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
            r#"mutation { insert_users_one(object: {name: "Held"}) { name } }"#,
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["insert_users_one"]["name"], json!("Held"));
    tx.rollback().await.unwrap();
    assert_eq!(count(&e, "users").await, 3);
    let v = e
        .query_on(
            e.pool(),
            r#"mutation { insert_users_one(object: {name: "Pooled"}) { name } }"#,
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["insert_users_one"]["name"], json!("Pooled"));
    assert_eq!(count(&e, "users").await, 4);
}

#[tokio::test]
async fn a_view_is_read_only_and_what_is_published_is_implemented() {
    let (e, _db) = engine().await;
    let err = e
        .run(Mutation::insert(
            "active_users",
            vec![[("name".to_string(), json!("x"))].into_iter().collect()],
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("read-only"), "{err}");
    let ts = e.schema().type_system();
    assert!(ts.mutation_root().is_some());
    let sdl = vision_graphql::sdl::render(ts);
    assert!(sdl.contains("insert_users"), "{sdl}");
    // A table without a primary key gets its mutation fields — the schema
    // is dialect-neutral there — and every one is refused on use: rows are
    // found again by key, and there is none.
    for src in [
        r#"mutation { insert_log(objects: [{line: "a"}]) { affected_rows } }"#,
        r#"mutation { update_log(where: {line: {_eq: "a"}}, _set: {line: "b"}) { affected_rows } }"#,
        r#"mutation { delete_log(where: {line: {_eq: "a"}}) { affected_rows } }"#,
    ] {
        let err = e.query(src, None).await.unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{src}: {err}");
        assert!(err.to_string().contains("primary key"), "{src}: {err}");
    }
}
