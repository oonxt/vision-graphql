//! `order_by` through object relations, executed against a real Postgres.
//!
//! The unit tests in `src/sql.rs` assert on the rendered SQL string; these
//! assert the database accepts that SQL and sorts rows the way the query asked.

use serde_json::Value;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::Engine;

/// posts → user → team: two object-relation hops, so the multi-hop JOIN branch
/// of `render_order_by_expr` is exercised, not just the single correlated hop.
fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("teams", "public", "teams")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .primary_key(&["id"]),
        )
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .column("team_id", "team_id", PgType::Int4, true)
                .primary_key(&["id"])
                .relation("team", Relation::object("teams").on([("team_id", "id")])),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, true)
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
        CREATE TABLE teams (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            team_id INT REFERENCES teams(id)
        );
        CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            user_id INT REFERENCES users(id)
        );
        INSERT INTO teams (name) VALUES ('zeta'), ('alpha');
        -- alice is on team zeta, bob on team alpha
        INSERT INTO users (name, team_id) VALUES ('alice', 1), ('bob', 2);
        INSERT INTO posts (title, user_id) VALUES
            ('a1', 1),
            ('a2', 1),
            ('b1', 2),
            ('orphan', NULL);
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed");

    let engine = Engine::new(pool, schema());
    (engine, container)
}

fn titles(v: &Value) -> Vec<String> {
    v["posts"]
        .as_array()
        .expect("posts array")
        .iter()
        .map(|p| p["title"].as_str().expect("title").to_string())
        .collect()
}

/// One hop: sort posts by their author's name.
#[tokio::test]
async fn order_by_object_relation_one_hop() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { posts(where: {user_id: {_is_null: false}}, \
             order_by: [{user: {name: desc}}, {id: asc}]) { title } }",
            None,
        )
        .await
        .expect("query runs against postgres");

    // bob > alice descending, so bob's post leads; ties broken by id.
    assert_eq!(titles(&v), ["b1", "a1", "a2"]);
}

/// Two hops: sort posts by the *team of the author*. This is the JOIN branch —
/// only the first hop correlates to the outer row, the rest are joins.
#[tokio::test]
async fn order_by_object_relation_two_hops() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { posts(where: {user_id: {_is_null: false}}, \
             order_by: [{user: {team: {name: asc}}}, {id: asc}]) { title } }",
            None,
        )
        .await
        .expect("two-hop order_by runs against postgres");

    // alpha (bob) sorts before zeta (alice).
    assert_eq!(titles(&v), ["b1", "a1", "a2"]);
}

/// A row whose object relation has no match must not be dropped — the ORDER BY
/// is a scalar subquery, not a join, so multiplicity of the outer query is
/// untouched and the missing side sorts as NULL.
#[tokio::test]
async fn order_by_object_relation_keeps_rows_with_no_related_row() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { posts(order_by: [{user: {name: asc}}, {id: asc}]) { title } }",
            None,
        )
        .await
        .expect("query runs against postgres");

    let got = titles(&v);
    assert_eq!(
        got.len(),
        4,
        "the orphan post must survive an order_by through its empty relation, got: {got:?}"
    );
    // Postgres sorts NULL last by default under ASC.
    assert_eq!(got.last().expect("non-empty"), "orphan");
}
