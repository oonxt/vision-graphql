use serde_json::{json, Value};
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

mod common;

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .column("score", "score", PgType::Int4, false)
                .primary_key(&["id"]),
        )
        .build()
}

async fn setup() -> (Engine, common::TestDb) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();

    sqlx::raw_sql(
        r#"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    score INT NOT NULL
                );
                INSERT INTO users (name, score) VALUES
                    ('alice', 10),
                    ('bob',   20),
                    ('cara',  30);
                "#,
    )
    .execute(&pool)
    .await
    .expect("seed");

    let engine = Engine::new(pool, schema());
    (engine, db)
}

#[tokio::test]
async fn aggregate_count_returns_row_count() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query("query { users_aggregate { aggregate { count } } }", None)
        .await
        .expect("query ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["count"], json!(3));
}

#[tokio::test]
async fn aggregate_sum_and_avg() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query(
            "query { users_aggregate { aggregate { sum { score } avg { score } } } }",
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["sum"]["score"], json!(60));
    assert_eq!(
        v["users_aggregate"]["aggregate"]["avg"]["score"],
        json!(20.0)
    );
}

#[tokio::test]
async fn aggregate_with_nodes() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query(
            "query { users_aggregate(where: {score: {_gte: 20}}) { aggregate { count } nodes { name } } }",
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["count"], json!(2));
    let nodes = v["users_aggregate"]["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);
}

#[tokio::test]
async fn aggregate_max_min() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query(
            "query { users_aggregate { aggregate { max { score } min { score } } } }",
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["max"]["score"], json!(30));
    assert_eq!(v["users_aggregate"]["aggregate"]["min"]["score"], json!(10));
}

/// `count(columns:)` and `count(distinct:)` against a real server: the row
/// constructor is unusual enough SQL to be worth executing rather than
/// asserting on the rendered string, and these are the semantics the AST
/// documents.
#[tokio::test]
async fn count_over_columns_and_distinct() {
    let (engine, _db) = setup().await;
    let v = engine
        .query(
            r#"{ users_aggregate { aggregate {
                   all: count
                   scores: count(columns: [score])
                   distinct_scores: count(columns: [score], distinct: true)
                   pairs: count(columns: [name, score], distinct: true)
                 } } }"#,
            None,
        )
        .await
        .expect("count with columns must be valid SQL");
    let agg = &v["users_aggregate"]["aggregate"];
    assert_eq!(
        agg["all"], agg["scores"],
        "no score is null, so both count every row"
    );
    assert!(
        agg["distinct_scores"].as_i64().unwrap() <= agg["all"].as_i64().unwrap(),
        "{agg}"
    );
    assert!(
        agg["pairs"].as_i64().unwrap() >= agg["distinct_scores"].as_i64().unwrap(),
        "{agg}"
    );
}

/// A selection that reads no rows must not read any: before this, it rendered a
/// scalar subquery over an unaggregated source, which errors at two rows.
#[tokio::test]
async fn typename_only_aggregate_runs() {
    let (engine, _db) = setup().await;
    let v = engine
        .query(
            "{ users_aggregate { __typename aggregate { __typename } } }",
            None,
        )
        .await
        .expect("a typename-only aggregate must not read rows");
    assert_eq!(v["users_aggregate"]["__typename"], "users_aggregate");
    assert_eq!(
        v["users_aggregate"]["aggregate"]["__typename"],
        "users_aggregate_fields"
    );
}

/// The count a paginated list needs: how many rows are behind the page, asked
/// per parent row rather than per table.
#[tokio::test]
async fn a_relation_can_be_aggregated() {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"CREATE TABLE authors (id SERIAL PRIMARY KEY, name TEXT);
           CREATE TABLE posts (
               id SERIAL PRIMARY KEY,
               author_id INT NOT NULL REFERENCES authors(id),
               score INT NOT NULL,
               draft BOOL NOT NULL DEFAULT false
           );
           INSERT INTO authors (name) VALUES ('alice'), ('bob'), ('cara');
           INSERT INTO posts (author_id, score, draft) VALUES
             (1, 10, false), (1, 20, false), (1, 5, true),
             (2, 7, false);"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    let engine = Engine::new(
        pool.clone(),
        Schema::introspect(&pool).await.unwrap().build(),
    );

    let v = engine
        .query(
            r#"{ authors(order_by: [{id: asc}]) {
                   name
                   posts_aggregate { aggregate { count max { score } } }
                 } }"#,
            None,
        )
        .await
        .unwrap();
    let authors = v["authors"].as_array().unwrap();
    assert_eq!(authors[0]["posts_aggregate"]["aggregate"]["count"], 3);
    assert_eq!(
        authors[0]["posts_aggregate"]["aggregate"]["max"]["score"],
        20
    );
    assert_eq!(authors[1]["posts_aggregate"]["aggregate"]["count"], 1);
    // An author with no posts counts zero rather than going missing.
    assert_eq!(authors[2]["posts_aggregate"]["aggregate"]["count"], 0);

    // Arguments apply to the aggregated set, and a page can be counted beside
    // the rows it shows.
    let v = engine
        .query(
            r#"{ authors(where: {id: {_eq: 1}}) {
                   posts(limit: 1, order_by: [{score: desc}]) { score }
                   published: posts_aggregate(where: {draft: {_eq: false}}) {
                     aggregate { count }
                   }
                   posts_aggregate { aggregate { count } nodes { score } }
                 } }"#,
            None,
        )
        .await
        .unwrap();
    let a = &v["authors"][0];
    assert_eq!(a["posts"].as_array().unwrap().len(), 1);
    assert_eq!(a["posts"][0]["score"], 20);
    assert_eq!(a["published"]["aggregate"]["count"], 2);
    assert_eq!(a["posts_aggregate"]["aggregate"]["count"], 3);
    assert_eq!(a["posts_aggregate"]["nodes"].as_array().unwrap().len(), 3);

    // An object relation is one row; there is nothing to aggregate.
    let err = engine
        .query(
            "{ posts { author_aggregate { aggregate { count } } } }",
            None,
        )
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("nothing to aggregate"), "{err}");
}

/// Counting a table is reading it. A scope that denies the table must deny the
/// count, or the number answers the question the rows were refused.
#[tokio::test]
async fn a_relation_aggregate_obeys_the_scope() {
    use vision_graphql::predicate::{col, principal};
    use vision_graphql::ScopePolicy;

    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"CREATE TABLE authors (id SERIAL PRIMARY KEY, name TEXT);
           CREATE TABLE posts (
               id SERIAL PRIMARY KEY,
               author_id INT NOT NULL REFERENCES authors(id),
               owner INT NOT NULL
           );
           INSERT INTO authors (name) VALUES ('alice');
           INSERT INTO posts (author_id, owner) VALUES (1, 1), (1, 1), (1, 2);"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    let schema = Schema::introspect(&pool).await.unwrap().build();
    let engine = Engine::new(
        pool.clone(),
        Schema::introspect(&pool).await.unwrap().build(),
    );

    // The row predicate reaches the count: principal 1 owns two of the three.
    let policy = ScopePolicy::builder()
        .unrestricted("authors")
        .allow("posts", col("owner").eq(principal()))
        .validate(&schema)
        .unwrap();
    let scoped = engine.scoped(policy.bind_value(1).unwrap());
    let v = scoped
        .query(
            "{ authors { posts_aggregate { aggregate { count } } } }",
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["authors"][0]["posts_aggregate"]["aggregate"]["count"], 2);

    // A table left out of the set is denied for counting as much as for reading.
    let policy = ScopePolicy::builder()
        .unrestricted("authors")
        .validate(&schema)
        .unwrap();
    let scoped = engine.scoped(policy.bind_value(1).unwrap());
    let err = scoped
        .query(
            "{ authors { posts_aggregate { aggregate { count } } } }",
            None,
        )
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("posts"), "{err}");

    // …and a column it withholds cannot be aggregated either.
    let policy = ScopePolicy::builder()
        .unrestricted("authors")
        .unrestricted("posts")
        .columns("posts", ["id", "author_id"])
        .validate(&schema)
        .unwrap();
    let scoped = engine.scoped(policy.bind_value(1).unwrap());
    let err = scoped
        .query(
            "{ authors { posts_aggregate { aggregate { max { owner } } } } }",
            None,
        )
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("owner"), "{err}");
}

/// `aggregate` and `nodes` share a source, so a `LIMIT` on it decides what
/// `count` counts. A default the caller never asked for must not do that.
#[tokio::test]
async fn a_default_limit_bounds_nodes_and_leaves_the_count_true() {
    use vision_graphql::limits::ExecutionLimits;

    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"CREATE TABLE authors (id SERIAL PRIMARY KEY);
           CREATE TABLE posts (id SERIAL PRIMARY KEY, author_id INT NOT NULL REFERENCES authors(id));
           INSERT INTO authors DEFAULT VALUES;
           INSERT INTO posts (author_id) SELECT 1 FROM generate_series(1, 30);"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    let engine = Engine::new(
        pool.clone(),
        Schema::introspect(&pool).await.unwrap().build(),
    )
    .with_limits(ExecutionLimits::new().default_limit(10));

    for q in [
        "{ posts_aggregate { aggregate { count } nodes { id } } }",
        "{ authors { posts_aggregate { aggregate { count } nodes { id } } } }",
    ] {
        let v = engine.query(q, None).await.unwrap();
        let agg = if q.starts_with("{ posts") {
            &v["posts_aggregate"]
        } else {
            &v["authors"][0]["posts_aggregate"]
        };
        assert_eq!(
            agg["aggregate"]["count"], 30,
            "the count sees every row: {q}"
        );
        assert_eq!(
            agg["nodes"].as_array().unwrap().len(),
            10,
            "and the rows are bounded: {q}"
        );
    }

    // A limit the caller wrote applies to both: they asked about that many rows.
    let v = engine
        .query(
            "{ posts_aggregate(limit: 5) { aggregate { count } nodes { id } } }",
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["posts_aggregate"]["aggregate"]["count"], 5);
    assert_eq!(v["posts_aggregate"]["nodes"].as_array().unwrap().len(), 5);
}

/// A relation in `returning` reads the rows this statement just wrote. An
/// aggregate over that relation has to read the same ones, or one response
/// reports a row and counts none.
#[tokio::test]
async fn a_relation_aggregate_in_returning_sees_the_rows_just_inserted() {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"CREATE TABLE authors (id SERIAL PRIMARY KEY, name TEXT);
           CREATE TABLE posts (
               id SERIAL PRIMARY KEY,
               author_id INT NOT NULL REFERENCES authors(id),
               title TEXT
           );"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    let engine = Engine::new(
        pool.clone(),
        Schema::introspect(&pool).await.unwrap().build(),
    );

    let v = engine
        .query(
            r#"mutation { insert_authors(objects: [
                 {name: "alice", posts: {data: [{title: "a"}, {title: "b"}]}}
               ]) { returning {
                     name
                     posts { id }
                     posts_aggregate { aggregate { count } }
               } } }"#,
            None,
        )
        .await
        .unwrap();
    let a = &v["insert_authors"]["returning"][0];
    assert_eq!(a["posts"].as_array().unwrap().len(), 2);
    assert_eq!(
        a["posts_aggregate"]["aggregate"]["count"], 2,
        "the count must see what `posts` sees: {a}"
    );
}

/// Every per-column aggregate, run for real — and the type each one is
/// published as, checked against what PostgreSQL actually answers with.
#[tokio::test]
async fn every_aggregate_function_runs_and_is_published_as_its_result_type() {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"CREATE TABLE m (
               id SERIAL PRIMARY KEY,
               small SMALLINT NOT NULL,
               whole INT NOT NULL,
               big BIGINT NOT NULL,
               approx DOUBLE PRECISION NOT NULL,
               exact NUMERIC(12,2) NOT NULL,
               label TEXT NOT NULL
           );
           INSERT INTO m (small, whole, big, approx, exact, label) VALUES
             (1, 10, 100, 1.5, 1.25, 'a'),
             (2, 20, 200, 2.5, 2.25, 'b'),
             (3, 30, 300, 3.5, 3.25, 'c');"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    let schema = Schema::introspect(&pool).await.unwrap().build();
    let engine = Engine::new(
        pool.clone(),
        Schema::introspect(&pool).await.unwrap().build(),
    );

    let v = engine
        .query(
            r#"{ m_aggregate { aggregate {
                   sum { whole } avg { whole }
                   max { whole label } min { whole label }
                   stddev { whole } stddev_pop { whole } stddev_samp { whole }
                   variance { whole } var_pop { whole } var_samp { whole }
                 } } }"#,
            None,
        )
        .await
        .expect("every function must be valid SQL");
    let a = &v["m_aggregate"]["aggregate"];
    assert_eq!(a["sum"]["whole"], 60);
    assert_eq!(a["avg"]["whole"], 20.0);
    assert_eq!(a["max"]["label"], "c");
    assert_eq!(a["min"]["whole"], 10);
    assert_eq!(a["stddev"]["whole"], 10.0);
    // A float comparison, so a tolerance rather than a literal.
    let var_pop = a["var_pop"]["whole"].as_f64().unwrap();
    assert!((var_pop - 200.0 / 3.0).abs() < 1e-9, "{var_pop}");
    let stddev_pop = a["stddev_pop"]["whole"].as_f64().unwrap();
    assert!((stddev_pop - var_pop.sqrt()).abs() < 1e-9, "{stddev_pop}");

    // `max`/`min` order anything; the rest are arithmetic and are not offered
    // on a text column.
    let err = engine
        .query("{ m_aggregate { aggregate { sum { label } } } }", None)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("label"), "{err}");

    // What the type system publishes has to be what PostgreSQL answers with.
    let ts = schema.type_system();
    let published = |group: &str, column: &str| -> String {
        let vision_graphql::type_system::TypeDef::Object { fields, .. } =
            ts.get(&format!("m_{group}_fields")).expect(group)
        else {
            panic!("{group} should be an object")
        };
        let f = fields.iter().find(|f| f.name == column).expect(column);
        f.ty.base_name().to_string()
    };
    for (group, column, expected) in [
        ("sum", "small", "bigint"),
        ("sum", "whole", "bigint"),
        ("sum", "big", "numeric"),
        ("sum", "approx", "Float"),
        ("sum", "exact", "numeric"),
        ("avg", "whole", "numeric"),
        ("avg", "approx", "Float"),
        ("stddev", "whole", "numeric"),
        ("var_samp", "approx", "Float"),
        ("max", "whole", "Int"),
        ("max", "label", "String"),
    ] {
        assert_eq!(published(group, column), expected, "{group} of {column}");
    }

    // …and PostgreSQL agrees.
    for (func, column, pg_type) in [
        ("sum", "whole", "bigint"),
        ("sum", "big", "numeric"),
        ("avg", "whole", "numeric"),
        ("stddev", "whole", "numeric"),
        ("var_samp", "approx", "double precision"),
    ] {
        let actual: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT pg_typeof({func}({column}))::text FROM m"
        )))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(actual, pg_type, "{func}({column})");
    }
}

/// `boolean`, `uuid` and enums order — `_gt` works on every one — but
/// PostgreSQL defines no `max`/`min` aggregate over them: publishing those
/// fields published a query whose only possible answer was "function
/// max(boolean) does not exist", from the database, at request time.
#[tokio::test]
async fn max_min_are_not_offered_where_postgres_has_none() {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
           CREATE TABLE t (
               id SERIAL PRIMARY KEY,
               label TEXT NOT NULL,
               day DATE NOT NULL,
               flag BOOLEAN NOT NULL,
               token UUID NOT NULL DEFAULT gen_random_uuid(),
               state mood NOT NULL
           );
           INSERT INTO t (label, day, flag, state) VALUES
             ('a', '2024-01-01', true, 'sad'),
             ('b', '2024-06-01', false, 'happy');"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    let schema = Schema::introspect(&pool).await.unwrap().build();
    let engine = Engine::new(
        pool.clone(),
        Schema::introspect(&pool).await.unwrap().build(),
    );

    // What stays: max over text and dates, run for real.
    let v = engine
        .query("{ t_aggregate { aggregate { max { label day } } } }", None)
        .await
        .expect("max over text and date is PostgreSQL's own");
    assert_eq!(v["t_aggregate"]["aggregate"]["max"]["label"], "b");
    assert_eq!(v["t_aggregate"]["aggregate"]["max"]["day"], "2024-06-01");

    // What goes: the schema stops publishing them…
    let ts = schema.type_system();
    let vision_graphql::type_system::TypeDef::Object { fields, .. } = ts
        .get("t_max_fields")
        .expect("max group exists for label/day")
    else {
        panic!("t_max_fields should be an object");
    };
    for absent in ["flag", "token", "state"] {
        assert!(
            !fields.iter().any(|f| f.name == absent),
            "PostgreSQL has no max over '{absent}'"
        );
    }

    // …and lowering refuses what it never published, with the reason, rather
    // than letting the database answer with an opaque error.
    for column in ["flag", "token", "state"] {
        let err = engine
            .query(
                &format!("{{ t_aggregate {{ aggregate {{ max {{ {column} }} }} }} }}"),
                None,
            )
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("does not apply") && msg.contains(column),
            "{msg}"
        );
    }
}

fn schema_with_posts() -> Schema {
    use vision_graphql::schema::Relation;
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .column("score", "score", PgType::Int4, false)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")])),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .primary_key(&["id"])
                .relation("user", Relation::object("users").on([("user_id", "id")])),
        )
        .build()
}

async fn setup_with_posts() -> (Engine, common::TestDb) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    score INT NOT NULL
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL REFERENCES users(id),
                    name TEXT NOT NULL
                );
                INSERT INTO users (name, score) VALUES
                    ('alice', 10),
                    ('bob',   20),
                    ('cara',  30);
                -- posts.name orders OPPOSITE to the author's name, so a sort
                -- that reads the wrong table gives itself away.
                INSERT INTO posts (user_id, name) VALUES
                    (1, 'z-post'),
                    (2, 'm-post'),
                    (3, 'a-post');
                "#,
    )
    .execute(&pool)
    .await
    .expect("seed");
    let engine = Engine::new(pool, schema_with_posts());
    (engine, db)
}

/// `nodes` publishes the full row type, relations included; the lowering used
/// to refuse them, and the renderer's shared source did not project the join
/// columns a relation correlates on.
#[tokio::test]
async fn aggregate_nodes_carry_relations() {
    let (engine, _db) = setup_with_posts().await;
    let v: Value = engine
        .query(
            "{ users_aggregate { aggregate { count } nodes {
                name posts { name } posts_aggregate { aggregate { count } }
            } } }",
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["count"], json!(3));
    let nodes = v["users_aggregate"]["nodes"].as_array().expect("nodes");
    assert_eq!(nodes.len(), 3);
    let alice = nodes.iter().find(|n| n["name"] == "alice").expect("alice");
    assert_eq!(alice["posts"], json!([{"name": "z-post"}]));
    assert_eq!(alice["posts_aggregate"]["aggregate"]["count"], json!(1));
}

/// The same shape when `nodes` reads a source of its own (an injected default
/// limit) — the other arm of the renderer.
#[tokio::test]
async fn aggregate_nodes_carry_relations_under_a_default_limit() {
    use vision_graphql::limits::ExecutionLimits;
    let (engine, _db) = setup_with_posts().await;
    let engine = engine.with_limits(ExecutionLimits::new().default_limit(2));
    let v: Value = engine
        .query(
            "{ users_aggregate { aggregate { count } nodes { name posts { name } } } }",
            None,
        )
        .await
        .expect("query ok");
    // The cap lands on `nodes` alone; `count` still answers for every row.
    assert_eq!(v["users_aggregate"]["aggregate"]["count"], json!(3));
    let nodes = v["users_aggregate"]["nodes"].as_array().expect("nodes");
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0]["posts"], json!([{"name": "z-post"}]));
}

/// `order_by` through a relation must sort by the *target's* column. The old
/// renderer read only the column name, so `{user: {name: asc}}` silently
/// sorted by `posts.name` — which is seeded to order the opposite way.
#[tokio::test]
async fn aggregate_order_by_through_a_relation_sorts_by_the_target_table() {
    let (engine, _db) = setup_with_posts().await;
    let v: Value = engine
        .query(
            "{ posts_aggregate(order_by: {user: {name: asc}}, limit: 2) { nodes { name } } }",
            None,
        )
        .await
        .expect("query ok");
    // alice's post first, then bob's — post names would give a-post, m-post.
    assert_eq!(
        v["posts_aggregate"]["nodes"],
        json!([{"name": "z-post"}, {"name": "m-post"}])
    );
}

/// A builder alias is an arbitrary string; it must come back as the response
/// key, not as SQL.
#[tokio::test]
async fn a_builder_alias_with_a_quote_round_trips() {
    use vision_graphql::ast::{AggOp, AggSelect, Field, Operation, QueryArgs, RootBody, RootField};
    let (engine, _db) = setup_with_posts().await;
    let hostile = "o'brien', (SELECT 1)) --";
    let op = Operation::Query(vec![RootField {
        table: "users".into(),
        alias: "agg".into(),
        args: QueryArgs::default(),
        body: RootBody::Aggregate {
            ops: vec![AggSelect {
                alias: "count".into(),
                op: AggOp::count(),
            }],
            nodes: Some(vec![Field::Column {
                column: "name".into(),
                alias: hostile.into(),
            }]),
            typenames: Vec::new(),
            nodes_limit: None,
        },
    }]);
    let v: Value = engine.run(op).await.expect("run ok");
    assert_eq!(v["agg"]["aggregate"]["count"], json!(3));
    let nodes = v["agg"]["nodes"].as_array().expect("nodes");
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[0][hostile], json!("alice"));
}
