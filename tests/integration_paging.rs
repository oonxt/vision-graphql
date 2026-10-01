//! A list with `limit` / `offset` picks its page before the projection runs,
//! executed against a real Postgres.
//!
//! The one-level form put the relation subqueries in the select list of the
//! query that carried `OFFSET`, and PostgreSQL evaluates that select list for
//! every row it then skips: a page at offset 5000 with four relations ran
//! their subqueries 5050 times each. The unit tests in `src/sql.rs` pin the
//! shape; these pin what the shape is for — the plan's loop counts — and
//! that the page is the same page under every argument that decides it.

use serde_json::{json, Value};
use vision_graphql::ast::{BoolExpr, CmpOp};
use vision_graphql::schema::{ColumnType, Relation, Schema, Table};
use vision_graphql::{Engine, ScopeSet};

mod common;

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", ColumnType::Int4, false)
                .column("name", "name", ColumnType::Text, false)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")]))
                // `name` has no unique constraint, so this hop is not pinned
                // and an `order_by` through it stays a correlated subquery.
                .relation("namesake", Relation::object("users").on([("name", "name")])),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", ColumnType::Int4, false)
                .column("title", "title", ColumnType::Text, false)
                .column("user_id", "user_id", ColumnType::Int4, false)
                .primary_key(&["id"])
                .relation("user", Relation::object("users").on([("user_id", "id")])),
        )
        .build()
}

/// Ten users, `u01`..`u10`; user `n` has `n % 3 + 1` posts titled
/// `u<n>-p<k>`.
async fn setup() -> (Engine, common::TestDb) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"
        CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            user_id INT NOT NULL REFERENCES users(id)
        );
        INSERT INTO users SELECT n, 'u' || lpad(n::text, 2, '0') FROM generate_series(1, 10) n;
        INSERT INTO posts (title, user_id)
            SELECT 'u' || lpad(n::text, 2, '0') || '-p' || k, n
            FROM generate_series(1, 10) n, generate_series(1, n % 3 + 1) k;
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed");
    (Engine::new(pool, schema()), db)
}

fn names(v: &Value, key: &str) -> Vec<String> {
    v[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key} is a list: {v}"))
        .iter()
        .map(|row| row["name"].as_str().expect("name").to_string())
        .collect()
}

fn titles(v: &Value) -> Vec<String> {
    v.as_array()
        .expect("list")
        .iter()
        .map(|p| p["title"].as_str().expect("title").to_string())
        .collect()
}

/// The largest `Actual Loops` of any plan node reading `relation`.
fn loops_over(plan: &Value, relation: &str) -> u64 {
    let mut most = 0;
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        match node {
            Value::Array(items) => stack.extend(items),
            Value::Object(map) => {
                if map.get("Relation Name").and_then(Value::as_str) == Some(relation) {
                    most = most.max(map["Actual Loops"].as_u64().unwrap_or(0));
                }
                stack.extend(map.values());
            }
            _ => {}
        }
    }
    most
}

/// The relation subqueries run once per row of the page, not once per row
/// `OFFSET` skipped: with `limit: 2, offset: 3`, `posts` is read twice.
#[tokio::test]
async fn the_projection_runs_over_the_page_only() {
    let (engine, _db) = setup().await;
    let source = "{ users(order_by: {id: asc}, limit: 2, offset: 3) { id name posts(order_by: {id: asc}) { title } } }";
    let compiled = engine.compile(source).expect("compiles");
    assert!(
        compiled.variables().is_empty(),
        "no binds, so the statement can be explained as it is"
    );
    // The statement is the engine's own, with no variables in it.
    let plan: Value = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "EXPLAIN (ANALYZE, FORMAT JSON) {}",
        compiled.sql()
    )))
    .fetch_one(engine.pool())
    .await
    .expect("explain");
    assert_eq!(
        loops_over(&plan, "posts"),
        2,
        "posts must be read once per page row; plan: {plan}"
    );

    let v = engine.query(source, None).await.expect("runs");
    assert_eq!(names(&v, "users"), ["u04", "u05"]);
    assert_eq!(titles(&v["users"][0]["posts"]), ["u04-p1", "u04-p2"]);
    assert_eq!(
        titles(&v["users"][1]["posts"]),
        ["u05-p1", "u05-p2", "u05-p3"]
    );
}

/// A relation paged on its own picks its page per parent row, and its
/// nested relations run over that page.
#[tokio::test]
async fn a_relation_pages_per_parent_row() {
    let (engine, _db) = setup().await;
    let v = engine
        .query(
            "{ users(where: {id: {_in: [2, 5]}}, order_by: {id: asc}) { \
               posts(order_by: {id: desc}, limit: 1, offset: 1) { title user { name } } } }",
            None,
        )
        .await
        .expect("runs");
    assert_eq!(
        v["users"],
        json!([
            {"posts": [{"title": "u02-p2", "user": {"name": "u02"}}]},
            {"posts": [{"title": "u05-p2", "user": {"name": "u05"}}]},
        ])
    );
}

/// The scope predicate is inside the page: a row the caller may not read is
/// not counted by `OFFSET`, so the page starts where the visible rows do.
#[tokio::test]
async fn the_page_counts_visible_rows_only() {
    let (engine, _db) = setup().await;
    let scope = ScopeSet::new().unrestricted("posts").allow(
        "users",
        BoolExpr::Compare {
            column: "id".into(),
            op: CmpOp::Gt,
            value: json!(5).into(),
        },
    );
    let v = engine
        .scoped(scope)
        .query(
            "{ users(order_by: {id: asc}, limit: 2, offset: 1) { name } }",
            None,
        )
        .await
        .expect("scoped page runs");
    assert_eq!(names(&v, "users"), ["u07", "u08"]);
}

/// `order_by` through a relation decides which rows are in the page, so it
/// is evaluated inside it and carried out: the pinned hop as its join, the
/// unpinned one as its subquery. The page is the slice of the whole order.
#[tokio::test]
async fn a_relation_order_decides_the_page() {
    let (engine, _db) = setup().await;
    let whole = engine
        .query(
            "{ posts(order_by: [{user: {name: desc}}, {id: asc}]) { title } }",
            None,
        )
        .await
        .expect("whole list");
    let page = engine
        .query(
            "{ posts(order_by: [{user: {name: desc}}, {id: asc}], limit: 3, offset: 4) { title } }",
            None,
        )
        .await
        .expect("page through a pinned hop");
    assert_eq!(titles(&page["posts"]), titles(&whole["posts"])[4..7]);

    let whole = engine
        .query(
            "{ users(order_by: [{namesake: {name: desc}}, {id: asc}]) { name } }",
            None,
        )
        .await
        .expect("whole list");
    let page = engine
        .query(
            "{ users(order_by: [{namesake: {name: desc}}, {id: asc}], limit: 3, offset: 2) { name } }",
            None,
        )
        .await
        .expect("page through an unpinned hop");
    assert_eq!(names(&page, "users"), names(&whole, "users")[2..5]);
}

/// `distinct_on` goes into the page too: the page is a slice of the distinct
/// rows, chosen by the order that follows.
#[tokio::test]
async fn distinct_on_is_applied_before_the_page() {
    let (engine, _db) = setup().await;
    let v = engine
        .query(
            "{ posts(distinct_on: [user_id], order_by: [{user_id: asc}, {id: desc}], limit: 2, offset: 1) { title } }",
            None,
        )
        .await
        .expect("distinct page runs");
    assert_eq!(titles(&v["posts"]), ["u02-p3", "u03-p1"]);
}

/// A bound `limit` / `offset` compiles to one statement and the page moves
/// with the values.
#[tokio::test]
async fn a_compiled_page_moves_with_its_variables() {
    let (engine, _db) = setup().await;
    let q = engine
        .compile("query($n: Int!, $o: Int!) { users(order_by: {id: asc}, limit: $n, offset: $o) { name posts(order_by: {id: asc}) { title } } }")
        .expect("compiles");
    let first = engine
        .execute(&q, Some(json!({"n": 2, "o": 0})))
        .await
        .expect("first page");
    let third = engine
        .execute(&q, Some(json!({"n": 2, "o": 4})))
        .await
        .expect("third page");
    assert_eq!(names(&first, "users"), ["u01", "u02"]);
    assert_eq!(names(&third, "users"), ["u05", "u06"]);
    assert_eq!(titles(&third["users"][1]["posts"]), ["u06-p1"]);
}

/// A bind inside the copied order term — the scope predicate on the
/// unpinned hop — and one in the `where` either side of it: each reaches the
/// database as its own value, however many times the text mentions it.
#[tokio::test]
async fn a_bind_inside_a_copied_order_term_binds_in_place() {
    let (engine, _db) = setup().await;
    // The scope hides users 8..10 as namesakes (and as roots); below it the
    // namesake is the row itself, so the order is by name descending.
    let scope = ScopeSet::new().allow(
        "users",
        BoolExpr::Compare {
            column: "id".into(),
            op: CmpOp::Lt,
            value: json!(8).into(),
        },
    );
    let v = engine
        .scoped(scope)
        .query(
            "{ users(where: {id: {_gt: 1}}, order_by: [{namesake: {name: desc}}, {id: asc}], limit: 3, offset: 2) { name } }",
            None,
        )
        .await
        .expect("scoped page through an unpinned hop runs");
    assert_eq!(names(&v, "users"), ["u05", "u04", "u03"]);
}
