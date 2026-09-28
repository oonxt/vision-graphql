//! `order_by` through object relations, executed against a real Postgres.
//!
//! The unit tests in `src/sql.rs` assert on the rendered SQL string; these
//! assert the database accepts that SQL and sorts rows the way the query asked.

use serde_json::{json, Value};
use vision_graphql::ast::{BoolExpr, CmpOp};
use vision_graphql::schema::{ColumnType, Relation, Schema, Table};
use vision_graphql::{Engine, ScopeSet};

mod common;

/// posts → user → team: two object-relation hops. Every hop is keyed, so these
/// run the `LEFT JOIN` rendering; the view tests at the bottom also run the
/// correlated subquery a relation without a key falls back to.
fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("teams", "public", "teams")
                .column("id", "id", ColumnType::Int4, false)
                .column("name", "name", ColumnType::Text, false)
                .primary_key(&["id"]),
        )
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", ColumnType::Int4, false)
                .column("name", "name", ColumnType::Text, false)
                .column("team_id", "team_id", ColumnType::Int4, true)
                .primary_key(&["id"])
                .relation("team", Relation::object("teams").on([("team_id", "id")])),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", ColumnType::Int4, false)
                .column("title", "title", ColumnType::Text, false)
                .column("user_id", "user_id", ColumnType::Int4, true)
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
    (engine, db)
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
    let (engine, _db) = setup().await;
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
    let (engine, _db) = setup().await;
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

/// A row whose object relation has no match must not be dropped — the join is
/// a LEFT one, so the outer row stays and the missing side sorts as NULL.
#[tokio::test]
async fn order_by_object_relation_keeps_rows_with_no_related_row() {
    let (engine, _db) = setup().await;
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

/// Scope on the second hop goes into that join's `ON`: a caller who may not
/// read team alpha still gets bob's post, sorted as if bob had no team — not
/// dropped, as a predicate in the outer `WHERE` would do.
#[tokio::test]
async fn scope_on_a_joined_hop_nulls_the_sort_key_and_keeps_the_row() {
    let (engine, _db) = setup().await;
    let scope = ScopeSet::new()
        .unrestricted("posts")
        .unrestricted("users")
        .allow(
            "teams",
            BoolExpr::Compare {
                column: "name".into(),
                op: CmpOp::Eq,
                value: json!("zeta").into(),
            },
        );
    let v: Value = engine
        .scoped(scope)
        .query(
            "query { posts(where: {user_id: {_is_null: false}}, \
             order_by: [{user: {team: {name: asc_nulls_first}}}, {id: asc}]) { title } }",
            None,
        )
        .await
        .expect("scoped two-hop order_by runs");
    // alpha is invisible: b1 sorts as NULL (first), not by 'alpha'.
    assert_eq!(titles(&v), ["b1", "a1", "a2"]);
}

/// `distinct_on` with a joined term after the distinct columns: `DISTINCT ON`
/// still keeps one row per group, chosen by the order that follows.
#[tokio::test]
async fn distinct_on_with_a_joined_term() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query(
            "query { posts(where: {user_id: {_is_null: false}}, distinct_on: [user_id], \
             order_by: [{user_id: asc}, {user: {name: asc}}, {id: desc}]) { title } }",
            None,
        )
        .await
        .expect("distinct_on with a relation term runs");
    assert_eq!(titles(&v), ["a2", "b1"]);
}

/// An aggregate's nodes order through the relation too, and its count is over
/// the rows, not the rows times the join.
#[tokio::test]
async fn aggregate_orders_through_the_join() {
    let (engine, _db) = setup().await;
    let v: Value = engine
        .query(
            "query { posts_aggregate(where: {user_id: {_is_null: false}}, \
               order_by: [{user: {name: desc}}, {id: asc}], limit: 2) { \
               aggregate { count } nodes { id title } } }",
            None,
        )
        .await
        .expect("aggregate ordered through a relation runs");
    let nodes: Vec<&str> = v["posts_aggregate"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["title"].as_str().unwrap())
        .collect();
    assert_eq!(nodes, ["b1", "a1"]);
    assert_eq!(v["posts_aggregate"]["aggregate"]["count"], 2);
}

// ===== An aggregate view — the shape that made the per-row subquery cost
// 30 s. A view has no constraints, so only the overlay's primary_key pins it;
// without one the subquery stays, and both must answer alike. =====

fn staff_schema(keyed_view: bool) -> Schema {
    let mut view = Table::new("staff_bonuses", "public", "staff_bonuses")
        .column("staff_id", "staff_id", ColumnType::Int4, true)
        .column("total_amount", "total_amount", ColumnType::Int8, true);
    if keyed_view {
        view = view.primary_key(&["staff_id"]);
    }
    Schema::builder()
        .table(
            Table::new("staffs", "public", "staffs")
                .column("id", "id", ColumnType::Int4, false)
                .column("name", "name", ColumnType::Text, false)
                .primary_key(&["id"])
                .relation(
                    "bonus",
                    Relation::object("staff_bonuses").on([("id", "staff_id")]),
                ),
        )
        .table(view)
        .build()
}

async fn staff_setup(keyed_view: bool) -> (Engine, common::TestDb) {
    let db = common::fresh_db().await;
    sqlx::raw_sql(
        r#"
        CREATE TABLE staffs (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE bonuses (id SERIAL PRIMARY KEY, staff_id INT NOT NULL, amount INT NOT NULL);
        CREATE VIEW staff_bonuses AS
            SELECT staff_id, sum(amount)::int8 AS total_amount FROM bonuses GROUP BY staff_id;
        INSERT INTO staffs (name) VALUES ('ann'), ('ben'), ('cat'), ('dan');
        -- ann 30, ben 5, cat none, dan 12: several base rows per staff, so a
        -- join to the base table rather than the view would repeat staff.
        INSERT INTO bonuses (staff_id, amount) VALUES
            (1, 10), (1, 20), (2, 5), (4, 4), (4, 8);
        "#,
    )
    .execute(&db.pool)
    .await
    .expect("seed");
    (Engine::new(db.pool.clone(), staff_schema(keyed_view)), db)
}

async fn staff_names_by_bonus(engine: &Engine) -> Vec<String> {
    let v: Value = engine
        .query(
            "query { staffs(order_by: [{bonus: {total_amount: desc_nulls_last}}, {id: asc}]) { name } }",
            None,
        )
        .await
        .expect("order_by through the view runs");
    v["staffs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap().to_string())
        .collect()
}

/// Keyed by the overlay: joined once. Every staff exactly once, in bonus
/// order, the one with no bonus last.
#[tokio::test]
async fn order_by_a_keyed_aggregate_view() {
    let (engine, _db) = staff_setup(true).await;
    assert_eq!(
        staff_names_by_bonus(&engine).await,
        ["ann", "dan", "ben", "cat"]
    );
}

/// Not keyed: the subquery fallback answers the same.
#[tokio::test]
async fn order_by_an_unkeyed_aggregate_view() {
    let (engine, _db) = staff_setup(false).await;
    assert_eq!(
        staff_names_by_bonus(&engine).await,
        ["ann", "dan", "ben", "cat"]
    );
}
