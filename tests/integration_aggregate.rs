use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

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
    (engine, container)
}

#[tokio::test]
async fn aggregate_count_returns_row_count() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query("query { users_aggregate { aggregate { count } } }", None)
        .await
        .expect("query ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["count"], json!(3));
}

#[tokio::test]
async fn aggregate_sum_and_avg() {
    let (engine, _c) = setup().await;
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
    let (engine, _c) = setup().await;
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
    let (engine, _c) = setup().await;
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
    let (engine, _c) = setup().await;
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
    let (engine, _c) = setup().await;
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
    let c = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .unwrap();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
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

    let c = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .unwrap();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
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

    let c = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .unwrap();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
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
    let c = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .unwrap();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
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
