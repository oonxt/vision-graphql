//! The SQLite backend, read-only, against a real SQLite.
//!
//! In memory and in process — no container, no server — so this suite is
//! cheap enough to run on every change. Every assertion here is about the
//! *response*, not the SQL text: the things SQLite gets wrong by default are
//! semantic (booleans as `0`/`1`, JSON as escaped strings, NULLs sorting
//! first, case-insensitive `LIKE`), and a rendered string looks right either
//! way.

#![cfg(feature = "sqlite")]

use serde_json::{json, Value};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use vision_graphql::ast::{BoolExpr, CmpOp};
use vision_graphql::sqlite::connect_options;
use vision_graphql::{Dialect, Engine, Error, Query, Schema, ScopeSet};

const DDL: &str = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT 1,
    score REAL,
    meta JSON,
    created_at DATETIME,
    price NUMERIC,
    picture BLOB
);
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    title TEXT NOT NULL,
    views INT NOT NULL DEFAULT 0,
    published BOOLEAN
);
CREATE TABLE tags (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL REFERENCES posts,
    label TEXT NOT NULL
);
CREATE TABLE counters (id INTEGER PRIMARY KEY, n INT) STRICT;
CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1;
CREATE UNIQUE INDEX users_name_key ON users(name);

INSERT INTO users (id, name, active, score, meta, created_at, price) VALUES
    (1, 'Ann',  1, 9.5,  '{"tags":["a","b"],"n":{"k":2}}', '2026-01-01T00:00:00Z', 1.5),
    (2, 'bob',  0, NULL, NULL,                              '2026-01-02T00:00:00Z', 2.5),
    (3, 'Cara', 1, 7.25, '{"tags":[]}',                     NULL,                   NULL);
INSERT INTO posts (id, user_id, title, views, published) VALUES
    (1, 1, 'zeta',  10, 1),
    (2, 1, 'alpha', 30, 0),
    (3, 2, 'mid',   20, NULL),
    (4, 3, 'omega', 30, 1);
INSERT INTO tags (id, post_id, label) VALUES (1, 2, 'x'), (2, 2, 'y'), (3, 4, 'z');
INSERT INTO counters (id, n) VALUES (1, 1);
"#;

/// One connection: an in-memory database lives in its connection, and a
/// second connection would open a second, empty one.
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

async fn q(engine: &Engine<sqlx::Sqlite>, src: &str) -> Value {
    engine.query(src, None).await.unwrap()
}

#[tokio::test]
async fn introspection_reads_tables_types_keys_and_relations() {
    let pool = pool().await;
    let sb = Schema::introspect_sqlite(&pool).await.unwrap();
    let schema = sb.build();
    assert_eq!(schema.dialect(), Dialect::Sqlite);
    let names: Vec<&str> = schema.tables().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        ["active_users", "counters", "posts", "tags", "users"]
    );

    let users = schema.table("users").unwrap();
    assert_eq!(users.primary_key, ["id"]);
    // NUMERIC and BLOB have no mapping and are left out — findably.
    assert!(users.find_column("price").is_none());
    assert!(users.find_column("picture").is_none());
    assert!(users.find_relation("posts").is_some());
    assert!(users.unique_indexes.contains_key("users_name_key"));
    let posts = schema.table("posts").unwrap();
    assert!(posts.find_relation("user").is_some());
    // `REFERENCES posts` named no column: resolved to the primary key.
    let tags = schema.table("tags").unwrap();
    let rel = tags.find_relation("post").unwrap();
    assert_eq!(rel.mapping, vec![("post_id".to_string(), "id".to_string())]);
    assert!(schema.table("active_users").unwrap().read_only);

    // Non-STRICT tables are said so, once each; the STRICT one and the view
    // are not.
    let loose: Vec<String> = schema
        .warnings()
        .iter()
        .filter_map(|w| match w {
            vision_graphql::SchemaWarning::LooselyTypedTable { table } => Some(table.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(loose, ["posts", "tags", "users"]);
}

#[tokio::test]
async fn introspection_records_what_it_skipped() {
    let pool = pool().await;
    let found = vision_graphql::schema::introspect_sqlite::introspect(&pool)
        .await
        .unwrap();
    let skipped: Vec<(String, String)> = found
        .db
        .skipped_columns
        .iter()
        .map(|s| (s.column.clone(), s.data_type.clone()))
        .collect();
    assert_eq!(
        skipped,
        [
            ("price".to_string(), "NUMERIC".to_string()),
            ("picture".to_string(), "BLOB".to_string())
        ]
    );
}

#[tokio::test]
async fn scalars_come_back_with_their_json_types() {
    let e = engine().await;
    let v = q(
        &e,
        "{ users(order_by: {id: asc}) { id name active score meta created_at } }",
    )
    .await;
    assert_eq!(
        v["users"],
        json!([
            {"id": 1, "name": "Ann", "active": true, "score": 9.5,
             "meta": {"tags": ["a", "b"], "n": {"k": 2}}, "created_at": "2026-01-01T00:00:00Z"},
            {"id": 2, "name": "bob", "active": false, "score": null,
             "meta": null, "created_at": "2026-01-02T00:00:00Z"},
            {"id": 3, "name": "Cara", "active": true, "score": 7.25,
             "meta": {"tags": []}, "created_at": null}
        ])
    );
}

#[tokio::test]
async fn nested_relations_keep_their_json_and_their_order() {
    let e = engine().await;
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
    let e = engine().await;
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
    let e = engine().await;
    // ASC: nulls last. SQLite's own default would put bob first.
    let v = q(&e, "{ users(order_by: {score: asc}) { name } }").await;
    assert_eq!(
        v["users"],
        json!([{"name": "Cara"}, {"name": "Ann"}, {"name": "bob"}])
    );
    // DESC: nulls first.
    let v = q(&e, "{ users(order_by: {score: desc}) { name } }").await;
    assert_eq!(
        v["users"],
        json!([{"name": "bob"}, {"name": "Ann"}, {"name": "Cara"}])
    );
    // Asked for explicitly, it is what was asked.
    let v = q(&e, "{ users(order_by: {score: desc_nulls_last}) { name } }").await;
    assert_eq!(
        v["users"],
        json!([{"name": "Ann"}, {"name": "Cara"}, {"name": "bob"}])
    );
}

#[tokio::test]
async fn limit_and_a_bare_offset() {
    let e = engine().await;
    let v = q(
        &e,
        "{ users(order_by: {id: asc}, limit: 1, offset: 1) { name } }",
    )
    .await;
    assert_eq!(v["users"], json!([{"name": "bob"}]));
    // OFFSET without LIMIT is not SQLite grammar; the renderer supplies one.
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
    let e = engine().await;
    let names = |v: &Value| -> Vec<String> {
        v["users"]
            .as_array()
            .unwrap()
            .iter()
            .map(|u| u["name"].as_str().unwrap().to_string())
            .collect()
    };
    let v = q(&e, "{ users(where: {score: {_gt: 8}}) { name } }").await;
    assert_eq!(names(&v), ["Ann"]);
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
    let v = q(&e, "{ users(where: {_or: [{name: {_eq: \"Ann\"}}, {name: {_eq: \"bob\"}}]}, order_by: {id: asc}) { name } }").await;
    assert_eq!(names(&v), ["Ann", "bob"]);
    let v = q(
        &e,
        "{ users(where: {posts: {views: {_gte: 30}}}, order_by: {id: asc}) { name } }",
    )
    .await;
    assert_eq!(names(&v), ["Ann", "Cara"]);
}

#[tokio::test]
async fn like_is_case_sensitive_and_ilike_is_not() {
    let e = engine().await;
    let names = |v: &Value| -> Vec<String> {
        v["users"]
            .as_array()
            .unwrap()
            .iter()
            .map(|u| u["name"].as_str().unwrap().to_string())
            .collect()
    };
    // SQLite's default LIKE would match all three here.
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
    let e = engine().await;
    let v = q(
        &e,
        r#"{ posts_aggregate(where: {views: {_gte: 20}}) {
              aggregate { count sum { views } avg { views } max { title } min { views } }
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
    assert!((avg - 80.0 / 3.0).abs() < 1e-9);
    assert_eq!(v["posts_aggregate"]["nodes"].as_array().unwrap().len(), 3);

    let v = q(&e, "{ posts_aggregate(where: {views: {_gt: 1000}}) { aggregate { count sum { views } } nodes { id } } }").await;
    assert_eq!(v["posts_aggregate"]["aggregate"]["count"], json!(0));
    assert_eq!(
        v["posts_aggregate"]["aggregate"]["sum"]["views"],
        Value::Null
    );
    assert_eq!(v["posts_aggregate"]["nodes"], json!([]));

    let v = q(
        &e,
        "{ users(order_by: {id: asc}) { name posts_aggregate { aggregate { count } nodes { published } } } }",
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
}

#[tokio::test]
async fn distinct_on_keeps_the_first_row_of_each_group() {
    let e = engine().await;
    let v = q(
        &e,
        "{ posts(distinct_on: [user_id], order_by: [{user_id: asc}, {views: desc}]) { user_id title tags { label } } }",
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
    // With a filter, a limit and the JSON kinds through the derived table.
    let v = q(
        &e,
        "{ posts(distinct_on: [user_id], where: {views: {_lt: 30}}, order_by: [{user_id: asc}, {views: desc}], limit: 1) { user_id published } }",
    )
    .await;
    assert_eq!(v["posts"], json!([{"user_id": 1, "published": true}]));
}

#[tokio::test]
async fn json_path_reads() {
    let e = engine().await;
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
}

#[tokio::test]
async fn variables_compiled_statements_and_transactions() {
    let e = engine().await;
    let compiled = e
        .compile("query($ids: [bigint!]!, $min: Float) { users(where: {id: {_in: $ids}, score: {_gte: $min}}, order_by: {id: asc}) { name } }")
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

    // The optional list: null means no filter.
    let compiled = e
        .compile("query($ids: [bigint!] @optional) { users(where: {id: {_in: $ids}}, order_by: {id: asc}) { id } }")
        .unwrap();
    let v = e
        .execute(&compiled, Some(json!({"ids": null})))
        .await
        .unwrap();
    assert_eq!(v["users"], json!([{"id": 1}, {"id": 2}, {"id": 3}]));
    let v = e
        .execute(&compiled, Some(json!({"ids": [2]})))
        .await
        .unwrap();
    assert_eq!(v["users"], json!([{"id": 2}]));

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

    let v = e
        .run(Query::from("posts").select(&["title"]).limit(1))
        .await
        .unwrap();
    assert_eq!(v["posts"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn scope_holds() {
    let e = engine().await;
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
        .query("{ users { name posts { title } } }", None)
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
    let e = engine().await;
    let ts = e.schema().type_system();
    assert!(ts.mutation_root().is_some());
    let sdl = vision_graphql::sdl::render(ts);
    assert!(!sdl.contains("stddev"), "{sdl}");
    assert!(sdl.contains("_ilike"), "{sdl}");
    assert!(sdl.contains("distinct_on"), "{sdl}");
    // And refused when reached anyway.
    let err = e
        .query(
            "{ posts_aggregate { aggregate { stddev { views } } } }",
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unsupported { .. }), "{err}");
}

#[tokio::test]
async fn a_pool_without_the_pragmas_is_refused() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let err = match Schema::introspect_sqlite(&pool).await {
        Ok(_) => panic!("a pool with SQLite's default LIKE was accepted"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("case-insensitive"), "{err}");
}

#[tokio::test]
#[should_panic(expected = "the schema describes Postgres but the engine runs on Sqlite")]
async fn a_schema_of_the_other_dialect_is_refused() {
    let pool = pool().await;
    let schema = Schema::builder().build();
    let _ = Engine::new(pool, schema);
}

#[tokio::test]
async fn distinct_on_inside_a_relation() {
    let e = engine().await;
    // Without distinct_on Ann has two posts; with it, the highest-viewed one.
    let v = q(
        &e,
        "{ users(order_by: {id: asc}) { name posts(distinct_on: [user_id], order_by: [{user_id: asc}, {views: desc}]) { title tags { label } } } }",
    )
    .await;
    assert_eq!(
        v["users"],
        json!([
            {"name": "Ann", "posts": [{"title": "alpha", "tags": [{"label": "x"}, {"label": "y"}]}]},
            {"name": "bob", "posts": [{"title": "mid", "tags": []}]},
            {"name": "Cara", "posts": [{"title": "omega", "tags": [{"label": "z"}]}]}
        ])
    );
    // The relation's own filter applies before the numbering.
    let v = q(
        &e,
        "{ users(where: {id: {_eq: 1}}) { posts(distinct_on: [user_id], where: {views: {_lt: 30}}) { title } } }",
    )
    .await;
    assert_eq!(v["users"][0]["posts"], json!([{"title": "zeta"}]));
}

#[tokio::test]
async fn counting_several_columns_is_refused_and_one_is_counted() {
    let e = engine().await;
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
async fn foreign_keys_find_their_table_whatever_the_case() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options("sqlite::memory:").unwrap())
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE Users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users, title TEXT);
         CREATE TABLE likes (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES USERS(id));
         INSERT INTO Users VALUES (1, 'Ann'); INSERT INTO posts VALUES (1, 1, 'p'); INSERT INTO likes VALUES (1, 1);",
    )
    .execute(&pool)
    .await
    .unwrap();
    let schema = Schema::introspect_sqlite(&pool).await.unwrap().build();
    assert!(schema
        .table("posts")
        .unwrap()
        .find_relation("User")
        .is_some());
    assert!(schema
        .table("likes")
        .unwrap()
        .find_relation("User")
        .is_some());
    let e = Engine::new(pool, schema);
    let v = q(&e, "{ posts { User { name } } likes { User { name } } }").await;
    assert_eq!(v["posts"][0]["User"]["name"], json!("Ann"));
    assert_eq!(v["likes"][0]["User"]["name"], json!("Ann"));
}

#[tokio::test]
async fn a_hand_built_schema_still_gets_its_pool_checked() {
    use vision_graphql::schema::{ColumnType, Table};
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO users VALUES (1, 'Ann');")
        .execute(&pool)
        .await
        .unwrap();
    let schema = Schema::builder()
        .dialect(Dialect::Sqlite)
        .table(
            Table::new("users", "main", "users")
                .column("id", "id", ColumnType::Int8, false)
                .column("name", "name", ColumnType::Text, true)
                .primary_key(&["id"]),
        )
        .build();
    let e = Engine::new(pool, schema);
    let err = e
        .query("{ users(where: {name: {_like: \"a%\"}}) { name } }", None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("case-insensitive"), "{err}");
    let err = e
        .transaction(async |tx| tx.query("{ users { name } }", None).await)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("case-insensitive"), "{err}");
}

#[tokio::test]
async fn a_value_the_declared_type_does_not_admit_is_an_error_not_a_null() {
    let e = engine().await;
    // The table is not STRICT, so this goes in.
    sqlx::raw_sql("INSERT INTO users (id, name, active) VALUES (4, 'Dan', 'true');")
        .execute(&pool_of(&e))
        .await
        .unwrap();
    let err = e
        .query("{ users(where: {id: {_eq: 4}}) { active } }", None)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Decode(_)), "{err}");
    assert!(err.to_string().contains("BOOLEAN"), "{err}");
    // A column that is not asked for does not get in the way.
    let v = q(&e, "{ users(where: {id: {_eq: 4}}) { name } }").await;
    assert_eq!(v["users"], json!([{"name": "Dan"}]));
}

/// The engine's own pool, for a test that has to write around the engine.
fn pool_of(e: &Engine<sqlx::Sqlite>) -> SqlitePool {
    e.pool().clone()
}

#[tokio::test]
async fn a_persisted_unsupported_query_keeps_its_code() {
    let e = engine().await;
    let err = vision_graphql::QueryRegistry::compile_all(
        &e,
        [(
            "m",
            "{ posts_aggregate { aggregate { stddev { views } } } }",
        )],
    )
    .err()
    .unwrap();
    assert!(matches!(err, Error::Unsupported { .. }), "{err}");
    assert_eq!(err.code(), vision_graphql::ErrorCode::Unsupported);
}

/// SQLite compares JSON as text: `json()` and `jsonb()` keep key order, so
/// `{"a":1,"b":2}` and `{"b":2,"a":1}` differ there and are equal to
/// PostgreSQL's `jsonb`. No comparison on a JSON column is published — only
/// `_is_null` is left — and each is refused as unsupported, not as a mistake
/// in the document, when reached anyway: from a document, from the builder,
/// and in a scope policy.
#[tokio::test]
async fn json_columns_publish_no_comparison_but_is_null() {
    let pool = pool().await;
    sqlx::raw_sql(
        "CREATE TABLE depts (id INTEGER PRIMARY KEY, extra JSONB, doc JSON);
         INSERT INTO depts VALUES (1, '{\"b\": 2, \"a\": 1}', '{\"b\": 2, \"a\": 1}'),
                                  (2, NULL, NULL);
         CREATE TABLE members (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES depts, extra JSONB);",
    )
    .execute(&pool)
    .await
    .unwrap();
    let schema = Schema::introspect_sqlite(&pool).await.unwrap().build();
    let sdl = vision_graphql::sdl::render(schema.type_system());
    for input in ["input jsonb_comparison_exp", "input json_comparison_exp"] {
        let block = sdl
            .split(input)
            .nth(1)
            .and_then(|rest| rest.split('}').next())
            .unwrap_or_else(|| panic!("{input} missing"));
        let fields: Vec<&str> = block
            .lines()
            .filter_map(|l| l.trim().split(':').next())
            .filter(|f| !f.is_empty() && *f != "{")
            .collect();
        assert_eq!(fields, ["_is_null"], "{input}: {block}");
    }
    let policy = vision_graphql::ScopePolicy::from_toml(
        r#"
        [tables.depts]
        where = { extra = { _eq = { a = 1 } } }
        "#,
        &schema,
    )
    .unwrap_err();
    assert!(matches!(policy, Error::Unsupported { .. }), "{policy}");

    let e = Engine::new(pool, schema);
    for where_ in [
        // Equal on PostgreSQL, where jsonb ignores key order; the text differs.
        r#"{extra: {_eq: {a: 1, b: 2}}}"#,
        r#"{extra: {_neq: {a: 1, b: 2}}}"#,
        r#"{extra: {_in: [{a: 1, b: 2}]}}"#,
        r#"{extra: {_nin: [{a: 1, b: 2}]}}"#,
        r#"{extra: {_contains: {a: 1}}}"#,
        r#"{extra: {_has_key: "a"}}"#,
        r#"{extra: {_has_keys_all: ["a"]}}"#,
    ] {
        let err = e
            .query(&format!("{{ depts(where: {where_}) {{ id }} }}"), None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{where_}: {err}");
    }
    for op in [CmpOp::Eq, CmpOp::HasKey] {
        let err = e
            .run(
                Query::from("depts")
                    .select(&["id"])
                    .where_cmp("extra", op, json!("a")),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{op:?}: {err}");
    }
    // `json` has no equality on either backend: a mistake in the document.
    let err = e
        .query("{ depts(where: {doc: {_eq: {a: 1}}}) { id } }", None)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Validate { .. }), "{err}");
    let v = q(&e, "{ depts(where: {extra: {_is_null: false}}) { id } }").await;
    assert_eq!(v["depts"], json!([{"id": 1}]));

    // Grouping is equality too: `distinct_on` and `count(distinct: true)`
    // would split one jsonb value spelled two ways. Counting values stays.
    for src in [
        "{ depts(distinct_on: [extra]) { id } }",
        "{ depts(distinct_on: [doc]) { id } }",
        "{ depts { members(distinct_on: [extra]) { id } } }",
        "{ depts_aggregate { aggregate { count(columns: [extra], distinct: true) } } }",
        "{ depts_aggregate { aggregate { count(columns: [doc], distinct: true) } } }",
    ] {
        let err = e.query(src, None).await.unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{src}: {err}");
    }
    let v = q(
        &e,
        "{ depts_aggregate { aggregate { count(columns: [extra]) } } }",
    )
    .await;
    assert_eq!(v["depts_aggregate"]["aggregate"]["count"], json!(1));
}

fn titles_of(v: &Value, key: &str) -> Vec<String> {
    v[key]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["title"].as_str().unwrap().to_string())
        .collect()
}

/// `order_by` through an object relation: a `LEFT JOIN` when a key pins the
/// target row, and — under `distinct_on`'s window form — the correlated
/// subquery. Every row stays, and a scope-hidden row sorts as NULL.
#[tokio::test]
async fn order_by_through_a_relation() {
    let e = engine().await;
    // users.id is the key: joined. bob > Cara > Ann under binary collation.
    let v = q(
        &e,
        "{ posts(order_by: [{user: {name: desc}}, {id: asc}]) { title } }",
    )
    .await;
    assert_eq!(titles_of(&v, "posts"), ["mid", "omega", "zeta", "alpha"]);

    let v = q(
        &e,
        "{ posts_aggregate(order_by: [{user: {name: asc}}, {id: asc}], limit: 3) { aggregate { count } nodes { title } } }",
    )
    .await;
    assert_eq!(v["posts_aggregate"]["aggregate"]["count"], json!(3));
    assert_eq!(
        titles_of(&v["posts_aggregate"], "nodes"),
        ["zeta", "alpha", "omega"]
    );

    // The window form of distinct_on keeps the relation term a subquery.
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

/// A view keyed by the overlay's primary key is joined; unkeyed, it is the
/// subquery. The same answer either way.
#[tokio::test]
async fn order_by_through_a_view_keyed_or_not() {
    use vision_graphql::schema::{ColumnType, Relation, Table};
    for keyed in [true, false] {
        let mut view = Table::new("active_users", "main", "active_users")
            .column("id", "id", ColumnType::Int8, true)
            .column("name", "name", ColumnType::Text, true);
        if keyed {
            view = view.primary_key(&["id"]);
        }
        let schema = Schema::builder()
            .dialect(Dialect::Sqlite)
            .table(
                Table::new("posts", "main", "posts")
                    .column("id", "id", ColumnType::Int8, false)
                    .column("user_id", "user_id", ColumnType::Int8, false)
                    .column("title", "title", ColumnType::Text, false)
                    .primary_key(&["id"])
                    .relation(
                        "author",
                        Relation::object("active_users").on([("user_id", "id")]),
                    ),
            )
            .table(view)
            .build();
        let e = Engine::new(pool().await, schema);
        let v = q(
            &e,
            "{ posts(order_by: [{author: {name: asc_nulls_last}}, {id: asc}]) { title } }",
        )
        .await;
        // bob is inactive: no row in the view, so his post sorts last.
        assert_eq!(
            titles_of(&v, "posts"),
            ["zeta", "alpha", "omega", "mid"],
            "keyed: {keyed}"
        );
    }
}

/// The case that asked for it: an application keeping its own bookkeeping
/// table (a migration ledger) in the one namespace SQLite has. `sqlite_` is
/// reserved, so no name keeps it out of introspection; `retain_tables` is how
/// it leaves the schema — with the relation a user table has into it, and
/// with nothing a request or a scope policy can still reach it through.
#[tokio::test]
async fn retain_tables_removes_a_table_and_every_way_into_it() {
    const LEDGER: &str = r#"
    CREATE TABLE _vision_schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT);
    CREATE TABLE release_notes (
        id INTEGER PRIMARY KEY,
        version TEXT REFERENCES _vision_schema_migrations(version),
        body TEXT
    );
    INSERT INTO _vision_schema_migrations VALUES ('0001', '2026-09-28');
    INSERT INTO release_notes VALUES (1, '0001', 'first');
    "#;
    let pool = pool().await;
    sqlx::raw_sql(LEDGER).execute(&pool).await.unwrap();

    let before = Schema::introspect_sqlite(&pool).await.unwrap().build();
    assert!(before.table("_vision_schema_migrations").is_some());
    let (into_ledger, _) = before
        .table("release_notes")
        .unwrap()
        .relations()
        .find(|(_, r)| r.target_table == "_vision_schema_migrations")
        .expect("the foreign key derives a relation into the ledger");
    let into_ledger = into_ledger.clone();

    let schema = Schema::introspect_sqlite(&pool)
        .await
        .unwrap()
        .retain_tables(|t| t != "_vision_schema_migrations")
        .build();
    assert!(schema.table("_vision_schema_migrations").is_none());
    let notes = schema.table("release_notes").unwrap();
    assert!(
        notes.find_relation(&into_ledger).is_none(),
        "a relation into a removed table is removed with it"
    );
    assert!(notes.find_column("version").is_some(), "the column stays");

    let sdl = vision_graphql::sdl::render(schema.type_system());
    assert!(!sdl.contains("_vision_schema_migrations"), "{sdl}");

    // A scope policy naming the table, or walking the relation into it, is a
    // build error, not a rule that silently guards nothing.
    let err = vision_graphql::ScopePolicy::from_toml(
        r#"
        [tables._vision_schema_migrations]
        unrestricted = true
        "#,
        &schema,
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown table"), "{err}");
    let err = vision_graphql::ScopePolicy::from_toml(
        &format!(
            r#"
            [tables.release_notes]
            where = {{ {into_ledger} = {{ version = {{ _eq = "0001" }} }} }}
            "#
        ),
        &schema,
    )
    .unwrap_err();
    assert!(err.to_string().contains(&into_ledger), "{err}");

    let e = Engine::new(pool, schema);
    for doc in [
        "{ _vision_schema_migrations { version } }".to_string(),
        r#"mutation { insert__vision_schema_migrations(objects: [{version: "0002"}]) { affected_rows } }"#
            .to_string(),
        format!("{{ release_notes {{ id {into_ledger} {{ version }} }} }}"),
        format!("{{ release_notes(where: {{{into_ledger}: {{version: {{_eq: \"0001\"}}}}}}) {{ id }} }}"),
    ] {
        assert!(e.query(&doc, None).await.is_err(), "{doc}");
    }
    assert_eq!(
        q(&e, "{ release_notes { id version } }").await,
        json!({"release_notes": [{"id": 1, "version": "0001"}]})
    );
}
