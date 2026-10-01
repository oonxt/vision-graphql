//! The MySQL backend, read-only, against a real MySQL. Set `MYSQL_TEST_URL`
//! to a server and every test takes a database of its own on it; see
//! `tests/common/mysql.rs`.
//!
//! Every assertion here is about the *response*, not the SQL text: the things
//! MySQL does differently by default are semantic — booleans as `0`/`1`,
//! timestamps spelled its own way, NULLs sorting first, a `LIKE` that ignores
//! case, an aggregation that drops the order it was given, an object whose
//! keys come back sorted — and a rendered string looks right either way.

#![cfg(feature = "mysql")]

mod common;

use common::mysql::{fresh_db, run_ddl, TestDb};
use serde_json::{json, Value};
use vision_graphql::ast::{BoolExpr, CmpOp};
use vision_graphql::{Dialect, Engine, Error, Query, Schema, ScopeSet};

const DDL: &str = r#"
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    score DECIMAL(10,2),
    meta JSON,
    created_at DATETIME(6),
    seen_at TIMESTAMP(6) NULL,
    opens_at TIME(3),
    big BIGINT UNSIGNED,
    picture BLOB,
    flags SET('a','b'),
    UNIQUE KEY users_name_key (name)
);
CREATE TABLE posts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    title VARCHAR(100) NOT NULL,
    views INT NOT NULL DEFAULT 0,
    published TINYINT(1),
    CONSTRAINT posts_user_fk FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE TABLE tags (
    id INT AUTO_INCREMENT PRIMARY KEY,
    post_id INT NOT NULL,
    label VARCHAR(20) NOT NULL,
    CONSTRAINT tags_post_fk FOREIGN KEY (post_id) REFERENCES posts(id)
);
CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1;
INSERT INTO users (id, name, active, score, meta, created_at, seen_at, opens_at, big) VALUES
    (1, 'Ann',  1, 9.50, '{"tags":["a","b"],"n":{"k":2}}', '2026-01-01 00:00:00', '2026-01-01 00:00:00', '09:00:00.5', 18446744073709551615),
    (2, 'bob',  0, NULL, NULL,                              '2026-01-02 00:00:00.5', NULL, '09:00:00', NULL),
    (3, 'Cara', 1, 7.25, '{"tags":[]}',                     NULL,                    NULL, NULL, 7);
CREATE TABLE ev (id INT NOT NULL, tag VARBINARY(16) NOT NULL, n INT, PRIMARY KEY (id, tag), UNIQUE KEY ev_n_tag (n, tag), UNIQUE KEY ev_n (n));
INSERT INTO posts (id, user_id, title, views, published) VALUES
    (1, 1, 'zeta',  10, 1),
    (2, 1, 'alpha', 30, 0),
    (3, 2, 'mid',   20, NULL),
    (4, 3, 'omega', 30, 1);
INSERT INTO tags (id, post_id, label) VALUES (1, 2, 'x'), (2, 2, 'y'), (3, 4, 'z');
"#;

async fn db() -> TestDb {
    let db = fresh_db().await;
    run_ddl(&db.pool, DDL).await;
    db
}

async fn engine() -> (Engine<sqlx::MySql>, TestDb) {
    let db = db().await;
    let schema = Schema::introspect_mysql(&db.pool).await.unwrap().build();
    (Engine::new(db.pool.clone(), schema), db)
}

async fn q(engine: &Engine<sqlx::MySql>, src: &str) -> Value {
    engine
        .query(src, None)
        .await
        .unwrap_or_else(|e| panic!("{src}: {e}"))
}

fn names(v: &Value) -> Vec<String> {
    v["users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["name"].as_str().unwrap().to_string())
        .collect()
}

fn titles_of(v: &Value, key: &str) -> Vec<String> {
    v[key]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["title"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn introspection_reads_tables_types_keys_and_relations() {
    use vision_graphql::schema::ColumnType;
    let db = db().await;
    let schema = Schema::introspect_mysql(&db.pool).await.unwrap().build();
    assert_eq!(schema.dialect(), Dialect::MySql);
    let names: Vec<&str> = schema.tables().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["active_users", "ev", "posts", "tags", "users"]);

    let users = schema.table("users").unwrap();
    assert_eq!(users.primary_key, ["id"]);
    let ty = |c: &str| users.find_column(c).unwrap().ty.clone();
    assert_eq!(ty("id"), ColumnType::Int4);
    assert_eq!(ty("active"), ColumnType::Bool);
    assert_eq!(ty("score"), ColumnType::Numeric);
    assert_eq!(ty("meta"), ColumnType::Jsonb);
    assert_eq!(ty("created_at"), ColumnType::Timestamp);
    assert_eq!(ty("seen_at"), ColumnType::TimestampTz);
    assert_eq!(ty("big"), ColumnType::Int8);
    assert_eq!(ty("opens_at"), ColumnType::Time);
    // A key over a column the engine leaves out is no key: `ev` has no
    // primary key here and no `_by_pk`, and only the unique key whose
    // columns are all in the schema.
    let ev = schema.table("ev").unwrap();
    assert!(ev.primary_key.is_empty());
    assert!(ev.find_column("tag").is_none());
    assert_eq!(ev.unique_constraints.keys().collect::<Vec<_>>(), ["ev_n"]);
    assert!(!vision_graphql::sdl::render(schema.type_system()).contains("ev_by_pk"));
    // BLOB and SET have no mapping and are left out — findably.
    assert!(users.find_column("picture").is_none());
    assert!(users.find_column("flags").is_none());
    assert!(users.find_relation("posts").is_some());
    assert!(users.unique_constraints.contains_key("users_name_key"));
    assert!(users.unique_constraints.contains_key("users_pkey"));
    let posts = schema.table("posts").unwrap();
    assert!(posts.find_relation("user").is_some());
    let tags = schema.table("tags").unwrap();
    let rel = tags.find_relation("post").unwrap();
    assert_eq!(rel.mapping, vec![("post_id".to_string(), "id".to_string())]);
    assert!(schema.table("active_users").unwrap().read_only);

    let found = vision_graphql::schema::introspect_mysql::introspect(&db.pool)
        .await
        .unwrap();
    let skipped: Vec<(String, String)> = found
        .skipped_columns
        .iter()
        .map(|s| (s.column.clone(), s.data_type.clone()))
        .collect();
    assert_eq!(
        skipped,
        [
            ("tag".to_string(), "varbinary(16)".to_string()),
            ("picture".to_string(), "blob".to_string()),
            ("flags".to_string(), "set('a','b')".to_string())
        ]
    );
}

#[tokio::test]
async fn scalars_come_back_with_their_json_types() {
    let (e, _db) = engine().await;
    let v = q(
        &e,
        "{ users(order_by: {id: asc}) { id name active score meta created_at seen_at opens_at big } }",
    )
    .await;
    // Times as PostgreSQL's JSON spells them: `T`, the fraction only when
    // there is one and without trailing zeros, UTC with its offset.
    assert_eq!(
        v["users"],
        json!([
            {"id": 1, "name": "Ann", "active": true, "score": 9.5,
             "meta": {"tags": ["a", "b"], "n": {"k": 2}},
             "created_at": "2026-01-01T00:00:00", "seen_at": "2026-01-01T00:00:00+00:00",
             "opens_at": "09:00:00.5", "big": 18446744073709551615u64},
            {"id": 2, "name": "bob", "active": false, "score": null,
             "meta": null, "created_at": "2026-01-02T00:00:00.5", "seen_at": null,
             "opens_at": "09:00:00", "big": null},
            {"id": 3, "name": "Cara", "active": true, "score": 7.25,
             "meta": {"tags": []}, "created_at": null, "seen_at": null, "opens_at": null, "big": 7}
        ])
    );
}

/// MySQL's `JSON_OBJECT` sorts keys; the response is in selection order at
/// every level, as PostgreSQL's is. Compared as text: `serde_json::Value`
/// equality does not see key order. Inside a JSON column the keys are the
/// column's business — MySQL stores them in its own order, as `jsonb` does
/// — and an `_aggregate` answer puts `aggregate` before `nodes` whatever the
/// document did, as the PostgreSQL renderer does.
#[tokio::test]
async fn response_keys_follow_the_selection() {
    let (e, _db) = engine().await;
    let v = q(
        &e,
        r#"{ users(where: {id: {_eq: 1}}) { name id meta posts(order_by: {id: asc}, limit: 1) { title id user { name id } } }
             n: users_aggregate { nodes { name id } aggregate { count max { id } } }
             one: users_by_pk(id: 2) { name id } }"#,
    )
    .await;
    assert_eq!(
        serde_json::to_string(&v).unwrap(),
        r#"{"users":[{"name":"Ann","id":1,"meta":{"n":{"k":2},"tags":["a","b"]},"posts":[{"title":"zeta","id":1,"user":{"name":"Ann","id":1}}]}],"n":{"aggregate":{"count":3,"max":{"id":3}},"nodes":[{"name":"Ann","id":1},{"name":"bob","id":2},{"name":"Cara","id":3}]},"one":{"name":"bob","id":2}}"#
    );
}

#[tokio::test]
async fn nested_relations_keep_their_json_and_their_order() {
    let (e, _db) = engine().await;
    let v = q(
        &e,
        r#"{ users(order_by: {id: asc}) {
              name
              posts(order_by: {title: desc}) { title published tags(order_by: {label: desc}) { label } }
           } }"#,
    )
    .await;
    assert_eq!(
        v["users"],
        json!([
            {"name": "Ann", "posts": [
                {"title": "zeta", "published": true, "tags": []},
                {"title": "alpha", "published": false, "tags": [{"label": "y"}, {"label": "x"}]}
            ]},
            {"name": "bob", "posts": [{"title": "mid", "published": null, "tags": []}]},
            {"name": "Cara", "posts": [{"title": "omega", "published": true, "tags": [{"label": "z"}]}]}
        ])
    );
    // Without a limit, where MySQL's aggregate would drop the order.
    let v = q(
        &e,
        "{ posts(order_by: {views: desc_nulls_last}) { title } }",
    )
    .await;
    assert_eq!(titles_of(&v, "posts"), ["alpha", "omega", "mid", "zeta"]);
    let v = q(
        &e,
        "{ posts(where: {id: {_eq: 3}}) { title user { name active meta } } }",
    )
    .await;
    assert_eq!(
        v["posts"],
        json!([{"title": "mid", "user": {"name": "bob", "active": false, "meta": null}}])
    );
}

#[tokio::test]
async fn by_pk_and_typename() {
    let (e, _db) = engine().await;
    let v = q(&e, "{ users_by_pk(id: 2) { __typename name active } }").await;
    assert_eq!(
        v["users_by_pk"],
        json!({"__typename": "users", "name": "bob", "active": false})
    );
    let v = q(&e, "{ users_by_pk(id: 99) { name } }").await;
    assert_eq!(v["users_by_pk"], Value::Null);
}

#[tokio::test]
async fn nulls_sort_as_they_do_on_postgres() {
    let (e, _db) = engine().await;
    // ASC: nulls last. MySQL's own default would put bob first.
    let v = q(&e, "{ users(order_by: {score: asc}) { name } }").await;
    assert_eq!(names(&v), ["Cara", "Ann", "bob"]);
    let v = q(&e, "{ users(order_by: {score: desc}) { name } }").await;
    assert_eq!(names(&v), ["bob", "Ann", "Cara"]);
    let v = q(&e, "{ users(order_by: {score: desc_nulls_last}) { name } }").await;
    assert_eq!(names(&v), ["Ann", "Cara", "bob"]);
    let v = q(&e, "{ users(order_by: {score: asc_nulls_first}) { name } }").await;
    assert_eq!(names(&v), ["bob", "Cara", "Ann"]);
}

#[tokio::test]
async fn limit_and_a_bare_offset() {
    let (e, _db) = engine().await;
    let v = q(
        &e,
        "{ users(order_by: {id: asc}, limit: 1, offset: 1) { name } }",
    )
    .await;
    assert_eq!(v["users"], json!([{"name": "bob"}]));
    let v = q(&e, "{ users(order_by: {id: asc}, offset: 2) { name } }").await;
    assert_eq!(v["users"], json!([{"name": "Cara"}]));
    let v = q(
        &e,
        "{ users(order_by: {id: asc}) { posts(order_by: {id: asc}, offset: 1) { title } } }",
    )
    .await;
    assert_eq!(v["users"][0]["posts"], json!([{"title": "alpha"}]));
}

#[tokio::test]
async fn comparison_operators() {
    let (e, _db) = engine().await;
    let v = q(&e, "{ users(where: {score: {_gt: 8}}) { name } }").await;
    assert_eq!(names(&v), ["Ann"]);
    // A decimal compares as a decimal, not as a double.
    let v = q(&e, "{ users(where: {score: {_eq: 7.25}}) { name } }").await;
    assert_eq!(names(&v), ["Cara"]);
    let v = q(&e, "{ users(where: {active: {_eq: false}}) { name } }").await;
    assert_eq!(names(&v), ["bob"]);
    let v = q(&e, "{ users(where: {score: {_is_null: true}}) { name } }").await;
    assert_eq!(names(&v), ["bob"]);
    let v = q(
        &e,
        "{ users(where: {id: {_in: [3, 1]}}, order_by: {id: asc}) { name } }",
    )
    .await;
    assert_eq!(names(&v), ["Ann", "Cara"]);
    let v = q(&e, "{ users(where: {id: {_nin: [3, 1]}}) { name } }").await;
    assert_eq!(names(&v), ["bob"]);
    let v = q(&e, "{ users(where: {id: {_in: []}}) { name } }").await;
    assert!(names(&v).is_empty());
    let v = q(
        &e,
        "{ users(where: {name: {_in: [\"bob\", \"nobody\"]}}) { name } }",
    )
    .await;
    assert_eq!(names(&v), ["bob"]);
    let v = q(&e, "{ users(where: {score: {_in: [7.25, 1]}}) { name } }").await;
    assert_eq!(names(&v), ["Cara"]);
    let v = q(&e, "{ users(where: {_or: [{name: {_eq: \"Ann\"}}, {name: {_eq: \"bob\"}}]}, order_by: {id: asc}) { name } }").await;
    assert_eq!(names(&v), ["Ann", "bob"]);
    let v = q(
        &e,
        "{ users(where: {posts: {views: {_gte: 30}}}, order_by: {id: asc}) { name } }",
    )
    .await;
    assert_eq!(names(&v), ["Ann", "Cara"]);
    // Timestamps, in ISO 8601 as they come back.
    let v = q(
        &e,
        "{ users(where: {created_at: {_gte: \"2026-01-02T00:00:00\"}}) { name } }",
    )
    .await;
    assert_eq!(names(&v), ["bob"]);
    let v = q(
        &e,
        "{ users(where: {created_at: {_in: [\"2026-01-01T00:00:00\", \"2026-01-02T00:00:00.500000\"]}}, order_by: {id: asc}) { name } }",
    )
    .await;
    assert_eq!(names(&v), ["Ann", "bob"]);
    let v = q(
        &e,
        "{ users(where: {seen_at: {_lt: \"2026-01-01T00:00:01+00:00\"}}) { name } }",
    )
    .await;
    assert_eq!(names(&v), ["Ann"]);
    // A JSON column compares structurally, key order aside.
    let v = q(
        &e,
        "{ users(where: {meta: {_eq: {n: {k: 2}, tags: [\"a\", \"b\"]}}}) { name } }",
    )
    .await;
    assert_eq!(names(&v), ["Ann"]);
    let v = q(&e, "{ users(where: {meta: {_in: [{tags: []}]}}) { name } }").await;
    assert_eq!(names(&v), ["Cara"]);
}

#[tokio::test]
async fn like_is_case_sensitive_and_ilike_is_not() {
    let (e, _db) = engine().await;
    // MySQL's default collation would match all three here.
    let v = q(
        &e,
        "{ users(where: {name: {_like: \"%a%\"}}, order_by: {id: asc}) { name } }",
    )
    .await;
    assert_eq!(names(&v), ["Cara"]);
    let v = q(
        &e,
        "{ users(where: {name: {_ilike: \"%a%\"}}, order_by: {id: asc}) { name } }",
    )
    .await;
    assert_eq!(names(&v), ["Ann", "Cara"]);
    let v = q(
        &e,
        "{ users(where: {name: {_nlike: \"%a%\"}}, order_by: {id: asc}) { name } }",
    )
    .await;
    assert_eq!(names(&v), ["Ann", "bob"]);
    let v = q(&e, "{ users(where: {name: {_nilike: \"%a%\"}}) { name } }").await;
    assert_eq!(names(&v), ["bob"]);
    // `\` escapes the wildcard, as on PostgreSQL.
    let v = q(
        &e,
        "{ users(where: {name: {_like: \"%\\\\%%\"}}) { name } }",
    )
    .await;
    assert!(names(&v).is_empty());
}

#[tokio::test]
async fn aggregates() {
    let (e, _db) = engine().await;
    let v = q(
        &e,
        r#"{ posts_aggregate(where: {views: {_gte: 20}}, order_by: {title: asc}) {
              aggregate { count sum { views } avg { views } max { title } min { views } stddev_pop { views } }
              nodes { title }
           } }"#,
    )
    .await;
    assert_eq!(v["posts_aggregate"]["aggregate"]["count"], json!(3));
    assert_eq!(v["posts_aggregate"]["aggregate"]["sum"]["views"], json!(80));
    assert_eq!(v["posts_aggregate"]["aggregate"]["min"]["views"], json!(20));
    assert_eq!(
        v["posts_aggregate"]["aggregate"]["max"]["title"],
        json!("omega")
    );
    let avg = v["posts_aggregate"]["aggregate"]["avg"]["views"]
        .as_f64()
        .unwrap();
    assert!((avg - 80.0 / 3.0).abs() < 1e-3);
    assert!(v["posts_aggregate"]["aggregate"]["stddev_pop"]["views"]
        .as_f64()
        .is_some());
    // `nodes` in the order asked for, from a source of their own.
    assert_eq!(
        titles_of(&v["posts_aggregate"], "nodes"),
        ["alpha", "mid", "omega"]
    );

    let v = q(&e, "{ posts_aggregate(where: {views: {_gt: 1000}}) { aggregate { count sum { views } } nodes { id } } }").await;
    assert_eq!(v["posts_aggregate"]["aggregate"]["count"], json!(0));
    assert_eq!(
        v["posts_aggregate"]["aggregate"]["sum"]["views"],
        Value::Null
    );
    assert_eq!(v["posts_aggregate"]["nodes"], json!([]));

    let v = q(
        &e,
        "{ users(order_by: {id: asc}) { name posts_aggregate(order_by: {id: asc}) { aggregate { count } nodes { published } } } }",
    )
    .await;
    assert_eq!(
        v["users"][0]["posts_aggregate"]["aggregate"]["count"],
        json!(2)
    );
    assert_eq!(
        v["users"][0]["posts_aggregate"]["nodes"],
        json!([{"published": true}, {"published": false}])
    );
    assert_eq!(
        v["users"][1]["posts_aggregate"]["nodes"],
        json!([{"published": null}])
    );
    // `max` of a timestamp is spelled as the column is.
    let v = q(
        &e,
        "{ users_aggregate { aggregate { max { created_at seen_at } } } }",
    )
    .await;
    assert_eq!(
        v["users_aggregate"]["aggregate"]["max"],
        json!({"created_at": "2026-01-02T00:00:00.5", "seen_at": "2026-01-01T00:00:00+00:00"})
    );
}

#[tokio::test]
async fn counting_several_columns_needs_distinct() {
    let (e, _db) = engine().await;
    let err = e
        .query(
            "{ posts_aggregate { aggregate { count(columns: [title, views]) } } }",
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    let v = q(
        &e,
        "{ posts_aggregate { aggregate { count(columns: [user_id, views], distinct: true) } } }",
    )
    .await;
    assert_eq!(v["posts_aggregate"]["aggregate"]["count"], json!(4));
    let v = q(
        &e,
        "{ posts_aggregate { aggregate { count(columns: [views], distinct: true) } } }",
    )
    .await;
    assert_eq!(v["posts_aggregate"]["aggregate"]["count"], json!(3));
    let v = q(
        &e,
        "{ posts_aggregate { aggregate { count(columns: [published]) } } }",
    )
    .await;
    assert_eq!(v["posts_aggregate"]["aggregate"]["count"], json!(3));
}

#[tokio::test]
async fn distinct_on_keeps_the_first_row_of_each_group() {
    let (e, _db) = engine().await;
    let v = q(
        &e,
        "{ posts(distinct_on: [user_id], order_by: [{user_id: asc}, {views: desc}]) { user_id title tags(order_by: {label: asc}) { label } } }",
    )
    .await;
    assert_eq!(
        v["posts"],
        json!([
            {"user_id": 1, "title": "alpha", "tags": [{"label": "x"}, {"label": "y"}]},
            {"user_id": 2, "title": "mid", "tags": []},
            {"user_id": 3, "title": "omega", "tags": [{"label": "z"}]}
        ])
    );
    let v = q(
        &e,
        "{ posts(distinct_on: [user_id], where: {views: {_lt: 30}}, order_by: [{user_id: asc}, {views: desc}], limit: 1) { user_id published } }",
    )
    .await;
    assert_eq!(v["posts"], json!([{"user_id": 1, "published": true}]));
    // Inside a relation.
    let v = q(
        &e,
        "{ users(order_by: {id: asc}) { name posts(distinct_on: [user_id], order_by: [{user_id: asc}, {views: desc}]) { title } } }",
    )
    .await;
    assert_eq!(
        v["users"],
        json!([
            {"name": "Ann", "posts": [{"title": "alpha"}]},
            {"name": "bob", "posts": [{"title": "mid"}]},
            {"name": "Cara", "posts": [{"title": "omega"}]}
        ])
    );
}

#[tokio::test]
async fn json_path_reads_and_operators() {
    let (e, _db) = engine().await;
    let v = q(
        &e,
        r#"{ users(order_by: {id: asc}) { first: meta(path: "tags.0") k: meta(path: "n.k") tags: meta(path: "tags") } }"#,
    )
    .await;
    assert_eq!(
        v["users"],
        json!([
            {"first": "a", "k": 2, "tags": ["a", "b"]},
            {"first": null, "k": null, "tags": null},
            {"first": null, "k": null, "tags": []}
        ])
    );

    // The jsonb operators, on the fixture the PostgreSQL suite uses, with
    // its answers.
    run_ddl(
        &e.pool().clone(),
        r#"CREATE TABLE departments (id INT PRIMARY KEY, name TEXT NOT NULL, extra JSON);
INSERT INTO departments VALUES
  (1, 'cardio', '{"is_mdt": true, "tags": ["a", "b"], "level": 2}'),
  (2, 'neuro',  '{"is_mdt": false, "tags": ["b"]}'),
  (3, 'ortho',  '{"note": "x"}'),
  (4, 'admin',  NULL),
  (5, 'labs',   '["a", "b"]'),
  (6, 'deep',   '[["a"], {"k": "a"}, "b"]');
"#,
    )
    .await;
    let schema = Schema::introspect_mysql(e.pool()).await.unwrap().build();
    let e = Engine::new(e.pool().clone(), schema);
    let cases: &[(&str, &[i64])] = &[
        (r#"{extra: {_contains: {is_mdt: true}}}"#, &[1]),
        (r#"{extra: {_contains: {tags: ["b"]}}}"#, &[1, 2]),
        (
            r#"{extra: {_contained_in: {is_mdt: false, tags: ["b"], note: "x"}}}"#,
            &[2, 3],
        ),
        (r#"{extra: {_has_key: "tags"}}"#, &[1, 2]),
        // A key test on an array is about its top-level string elements:
        // the "a" nested inside row 6 does not count, as it would not for
        // PostgreSQL's `?`.
        (r#"{extra: {_has_key: "a"}}"#, &[5]),
        (r#"{extra: {_has_key: "b"}}"#, &[5, 6]),
        (r#"{extra: {_has_keys_any: ["note", "level"]}}"#, &[1, 3]),
        (r#"{extra: {_has_keys_any: ["a"]}}"#, &[5]),
        (r#"{extra: {_has_keys_all: ["is_mdt", "tags"]}}"#, &[1, 2]),
        (r#"{extra: {_has_keys_all: ["a", "b"]}}"#, &[5]),
        (r#"{extra: {_has_keys_all: ["b"]}}"#, &[5, 6]),
        (r#"{extra: {_has_keys_any: []}}"#, &[]),
        (r#"{extra: {_has_keys_all: []}}"#, &[1, 2, 3, 5, 6]),
        (
            r#"{_and: [{extra: {_has_key: "tags"}}, {_not: {extra: {_contains: {is_mdt: true}}}}], name: {_like: "n%"}}"#,
            &[2],
        ),
        (
            r#"{_or: [{extra: {_has_key: "note"}}, {extra: {_contains: {level: 2}}}]}"#,
            &[1, 3],
        ),
    ];
    for (where_, want) in cases {
        let src = format!("{{ departments(where: {where_}, order_by: {{id: asc}}) {{ id }} }}");
        let v = q(&e, &src).await;
        let ids: Vec<i64> = v["departments"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, *want, "{where_}");
    }
    // And from variables, compiled: the operands bind.
    let compiled = e
        .compile(
            r#"query($doc: jsonb!, $key: String!, $keys: [String!]!) {
                a: departments(where: {extra: {_contains: $doc}}, order_by: {id: asc}) { id }
                b: departments(where: {extra: {_has_key: $key}}, order_by: {id: asc}) { id }
                c: departments(where: {extra: {_has_keys_all: $keys}}, order_by: {id: asc}) { id }
            }"#,
        )
        .unwrap();
    let v = e
        .execute(
            &compiled,
            Some(json!({"doc": {"tags": ["b"]}, "key": "note", "keys": ["is_mdt", "tags"]})),
        )
        .await
        .unwrap();
    assert_eq!(
        v,
        json!({"a": [{"id": 1}, {"id": 2}], "b": [{"id": 3}], "c": [{"id": 1}, {"id": 2}]})
    );
}

#[tokio::test]
async fn variables_compiled_statements_and_transactions() {
    let (e, _db) = engine().await;
    let compiled = e
        .compile("query($ids: [Int!]!, $min: numeric) { users(where: {id: {_in: $ids}, score: {_gte: $min}}, order_by: {id: asc}) { name } }")
        .unwrap();
    let v = e
        .execute(&compiled, Some(json!({"ids": [1, 2, 3], "min": 8})))
        .await
        .unwrap();
    assert_eq!(v["users"], json!([{"name": "Ann"}]));
    let v = e
        .execute(&compiled, Some(json!({"ids": [3], "min": 0})))
        .await
        .unwrap();
    assert_eq!(v["users"], json!([{"name": "Cara"}]));
    let v = e
        .execute(&compiled, Some(json!({"ids": [], "min": 0})))
        .await
        .unwrap();
    assert_eq!(v["users"], json!([]));

    // The optional forms: one placeholder used twice on PostgreSQL, bound
    // twice here. Null means no filter.
    let compiled = e
        .compile("query($ids: [Int!] @optional, $min: numeric @optional) { users(where: {id: {_in: $ids}, score: {_gte: $min}}, order_by: {id: asc}) { id } }")
        .unwrap();
    let v = e
        .execute(&compiled, Some(json!({"ids": null, "min": null})))
        .await
        .unwrap();
    assert_eq!(v["users"], json!([{"id": 1}, {"id": 2}, {"id": 3}]));
    let v = e
        .execute(&compiled, Some(json!({"ids": [2, 3], "min": null})))
        .await
        .unwrap();
    assert_eq!(v["users"], json!([{"id": 2}, {"id": 3}]));
    let v = e
        .execute(&compiled, Some(json!({"ids": null, "min": 8})))
        .await
        .unwrap();
    assert_eq!(v["users"], json!([{"id": 1}]));

    let n = e
        .transaction(async |tx| {
            let v = tx
                .query("{ users_aggregate { aggregate { count } } }", None)
                .await?;
            Ok(v["users_aggregate"]["aggregate"]["count"].as_i64().unwrap())
        })
        .await
        .unwrap();
    assert_eq!(n, 3);

    // The `_on` twins, on a connection the caller holds.
    let mut conn = e.pool().acquire().await.unwrap();
    let v = e
        .query_on(&mut *conn, "{ users_by_pk(id: 3) { name } }", None)
        .await
        .unwrap();
    assert_eq!(v["users_by_pk"]["name"], json!("Cara"));

    let v = e
        .run(Query::from("posts").select(&["title"]).limit(1))
        .await
        .unwrap();
    assert_eq!(v["posts"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn scope_holds() {
    let (e, _db) = engine().await;
    let scope = ScopeSet::new()
        .allow(
            "users",
            BoolExpr::Compare {
                column: "id".into(),
                op: CmpOp::Eq,
                value: json!(1).into(),
            },
        )
        .allow(
            "posts",
            BoolExpr::Compare {
                column: "user_id".into(),
                op: CmpOp::Eq,
                value: json!(1).into(),
            },
        );
    let scoped = e.scoped(scope);
    let v = scoped
        .query(
            "{ users { name posts(order_by: {id: asc}) { title } } }",
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        v["users"],
        json!([{"name": "Ann", "posts": [{"title": "zeta"}, {"title": "alpha"}]}])
    );
    let v = scoped
        .query("{ users_by_pk(id: 2) { name } }", None)
        .await
        .unwrap();
    assert_eq!(v["users_by_pk"], Value::Null);
    let err = scoped.query("{ tags { label } }", None).await.unwrap_err();
    assert!(matches!(err, Error::ScopeDenied { .. }), "{err}");
}

#[tokio::test]
async fn what_is_published_is_what_is_implemented() {
    let (e, _db) = engine().await;
    let ts = e.schema().type_system();
    assert!(ts.mutation_root().is_some());
    let sdl = vision_graphql::sdl::render(ts);
    assert!(sdl.contains("insert_users"), "{sdl}");
    assert!(sdl.contains("stddev"), "{sdl}");
    assert!(sdl.contains("_ilike"), "{sdl}");
    assert!(sdl.contains("_has_key"), "{sdl}");
    assert!(sdl.contains("distinct_on"), "{sdl}");
    // What the type system cannot withhold is refused when reached: a count
    // of several columns without `distinct` has no MySQL spelling.
    let err = e
        .query(
            "{ posts_aggregate { aggregate { count(columns: [title, views]) } } }",
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unsupported { .. }), "{err}");
}

#[tokio::test]
#[should_panic(expected = "the schema describes Postgres but the engine runs on MySql")]
async fn a_schema_of_the_other_dialect_is_refused() {
    let db = db().await;
    let schema = Schema::builder().build();
    let _ = Engine::new(db.pool.clone(), schema);
}

/// `order_by` through an object relation: a `LEFT JOIN` when a key pins the
/// target row, and — under `distinct_on`'s window form — the correlated
/// subquery. Every row stays, and a scope-hidden row sorts as NULL.
#[tokio::test]
async fn order_by_through_a_relation() {
    let (e, _db) = engine().await;
    // MySQL's default collation ignores case: Cara > bob > Ann.
    let v = q(
        &e,
        "{ posts(order_by: [{user: {name: desc}}, {id: asc}]) { title } }",
    )
    .await;
    assert_eq!(titles_of(&v, "posts"), ["omega", "mid", "zeta", "alpha"]);

    let v = q(
        &e,
        "{ posts_aggregate(order_by: [{user: {name: asc}}, {id: asc}], limit: 3) { aggregate { count } nodes { title } } }",
    )
    .await;
    assert_eq!(v["posts_aggregate"]["aggregate"]["count"], json!(3));
    assert_eq!(
        titles_of(&v["posts_aggregate"], "nodes"),
        ["zeta", "alpha", "mid"]
    );

    let v = q(
        &e,
        "{ posts(distinct_on: [views], order_by: [{views: asc}, {user: {name: desc}}]) { title } }",
    )
    .await;
    assert_eq!(titles_of(&v, "posts"), ["zeta", "mid", "omega"]);

    // Scope on the joined hop is in its ON: bob's post stays, sorted as NULL.
    let scoped = e.scoped(ScopeSet::new().unrestricted("posts").allow(
        "users",
        BoolExpr::Compare {
            column: "id".into(),
            op: CmpOp::Neq,
            value: json!(2).into(),
        },
    ));
    let v = scoped
        .query(
            "{ posts(order_by: [{user: {name: desc_nulls_last}}, {id: asc}]) { title } }",
            None,
        )
        .await
        .unwrap();
    assert_eq!(titles_of(&v, "posts"), ["omega", "zeta", "alpha", "mid"]);
}

#[tokio::test]
async fn a_persisted_unsupported_query_keeps_its_code() {
    let (e, _db) = engine().await;
    let err = vision_graphql::QueryRegistry::compile_all(
        &e,
        [(
            "m",
            "{ posts_aggregate { aggregate { count(columns: [title, views]) } } }",
        )],
    )
    .err()
    .unwrap();
    assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    assert_eq!(err.code(), vision_graphql::ErrorCode::Unsupported);
}

/// A list with `limit` / `offset` picks its page in a derived table before
/// the projection runs — MySQL evaluates the select list before its filesort,
/// so without it every relation subquery ran for every matching row. What the
/// page holds is decided inside it: the order through a relation,
/// `distinct_on`, and the scope predicate; and the page's rows are still
/// numbered for the aggregation above them.
#[tokio::test]
async fn a_page_is_picked_before_the_projection() {
    let (e, _db) = engine().await;
    let v = q(
        &e,
        "{ users(order_by: {id: desc}, limit: 1, offset: 2) { name posts(order_by: {views: desc}, offset: 1) { title tags { label } } } }",
    )
    .await;
    assert_eq!(
        v["users"],
        json!([{"name": "Ann", "posts": [{"title": "zeta", "tags": []}]}])
    );
    // Through the pinned relation: the collation ignores case, Cara > bob > Ann.
    let v = q(
        &e,
        "{ posts(order_by: [{user: {name: desc}}, {id: asc}], limit: 2, offset: 1) { title } }",
    )
    .await;
    assert_eq!(titles_of(&v, "posts"), ["mid", "zeta"]);
    // The distinct rows are paged: one post per user, the second user's.
    let v = q(
        &e,
        "{ posts(distinct_on: [user_id], order_by: [{user_id: asc}, {views: desc}], limit: 1, offset: 1) { title } }",
    )
    .await;
    assert_eq!(titles_of(&v, "posts"), ["mid"]);
    // The scope predicate is inside the page: a hidden row is not skipped
    // over by the offset.
    let scoped = e.scoped(ScopeSet::new().allow(
        "users",
        BoolExpr::Compare {
            column: "id".into(),
            op: CmpOp::Neq,
            value: json!(1).into(),
        },
    ));
    let v = scoped
        .query(
            "{ users(order_by: {id: asc}, limit: 1, offset: 1) { name } }",
            None,
        )
        .await
        .unwrap();
    assert_eq!(names(&v), ["Cara"]);
}

/// The `order_by` term through a relation is rendered once, in the page's
/// derived table, and *copied* into the `ORDER BY` beside it. On MySQL the
/// placeholders are anonymised by position, so a bind inside the copied
/// text — the scope predicate on an unpinned hop — rests on every mention
/// binding its own value. `first_tag` is an object relation onto a column
/// with no unique index, so it stays a correlated subquery.
#[tokio::test]
async fn a_bind_inside_a_copied_order_term_binds_in_place() {
    let db = db().await;
    let overlay = vision_graphql::schema::config::parse(
        r#"
        [[tables.posts.relations]]
        name = "first_tag"
        kind = "object"
        target = "tags"
        mapping = [["id", "post_id"]]
        "#,
    )
    .unwrap();
    let schema = Schema::introspect_mysql(&db.pool)
        .await
        .unwrap()
        .apply_config(&overlay)
        .build();
    let e = Engine::new(db.pool.clone(), schema);
    let scoped = e.scoped(ScopeSet::new().unrestricted("posts").allow(
        "tags",
        BoolExpr::Compare {
            column: "label".into(),
            op: CmpOp::Neq,
            value: json!("z").into(),
        },
    ));
    let v = scoped
        .query(
            "{ posts(where: {views: {_gt: 5}}, order_by: [{first_tag: {label: desc_nulls_last}}, {id: asc}], limit: 2, offset: 1) { title } }",
            None,
        )
        .await
        .unwrap();
    assert_eq!(titles_of(&v, "posts"), ["zeta", "mid"]);
    let v = scoped
        .query(
            "{ posts(distinct_on: [user_id], where: {views: {_gt: 5}}, order_by: [{user_id: asc}, {first_tag: {label: desc_nulls_last}}], limit: 2, offset: 1) { title } }",
            None,
        )
        .await
        .unwrap();
    assert_eq!(titles_of(&v, "posts"), ["mid", "omega"]);
}
