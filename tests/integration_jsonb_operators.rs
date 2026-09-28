//! `jsonb` containment and key tests in `where`: `_contains`, `_contained_in`,
//! `_has_key`, `_has_keys_any`, `_has_keys_all`. Checked against PostgreSQL's
//! own answers rather than the rendered text: `?` on an array matching a
//! string element, and `?&` over an empty list matching everything, are the
//! database's rules, not ours.

use serde_json::{json, Value};
use vision_graphql::schema::{ColumnType, Schema, Table};
use vision_graphql::{Engine, Error, ScopePolicy};

mod common;

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("departments", "public", "departments")
                .column("id", "id", ColumnType::Int4, false)
                .column("name", "name", ColumnType::Text, false)
                .column("extra", "extra", ColumnType::Jsonb, true)
                .primary_key(&["id"]),
        )
        .build()
}

async fn setup() -> (Engine, common::TestDb) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"
        CREATE TABLE departments (id INT PRIMARY KEY, name TEXT NOT NULL, extra JSONB);
        INSERT INTO departments VALUES
          (1, 'cardio', '{"is_mdt": true, "tags": ["a", "b"], "level": 2}'),
          (2, 'neuro',  '{"is_mdt": false, "tags": ["b"]}'),
          (3, 'ortho',  '{"note": "x"}'),
          (4, 'admin',  NULL),
          (5, 'labs',   '["a", "b"]');
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed");
    (Engine::new(pool, schema()), db)
}

fn ids(v: &Value) -> Vec<i64> {
    v["departments"]
        .as_array()
        .expect("array")
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect()
}

async fn filter(engine: &Engine, where_: &str) -> Vec<i64> {
    let q = format!("{{ departments(where: {where_}, order_by: {{id: asc}}) {{ id }} }}");
    ids(&engine
        .query(&q, None)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}")))
}

#[tokio::test]
async fn each_operator_answers_as_postgres_does() {
    let (engine, _db) = setup().await;
    let cases: &[(&str, &[i64])] = &[
        (r#"{extra: {_contains: {is_mdt: true}}}"#, &[1]),
        // Containment recurses: an array contains the elements it is given.
        (r#"{extra: {_contains: {tags: ["b"]}}}"#, &[1, 2]),
        (
            r#"{extra: {_contained_in: {is_mdt: false, tags: ["b"], note: "x"}}}"#,
            &[2, 3],
        ),
        (r#"{extra: {_has_key: "tags"}}"#, &[1, 2]),
        // `?` on a top-level array matches a string element.
        (r#"{extra: {_has_key: "a"}}"#, &[5]),
        (r#"{extra: {_has_keys_any: ["note", "level"]}}"#, &[1, 3]),
        (r#"{extra: {_has_keys_all: ["is_mdt", "tags"]}}"#, &[1, 2]),
        // Over no keys, "any" holds for none and "all" for every non-null row.
        (r#"{extra: {_has_keys_any: []}}"#, &[]),
        (r#"{extra: {_has_keys_all: []}}"#, &[1, 2, 3, 5]),
    ];
    for (where_, want) in cases {
        assert_eq!(filter(&engine, where_).await, *want, "{where_}");
    }
}

/// What the request was for: a key in `extra` combined with other conditions
/// in one query, rather than an id list fetched first.
#[tokio::test]
async fn composes_with_other_conditions() {
    let (engine, _db) = setup().await;
    let got = filter(
        &engine,
        r#"{_and: [{extra: {_has_key: "tags"}}, {_not: {extra: {_contains: {is_mdt: true}}}}], name: {_like: "n%"}}"#,
    )
    .await;
    assert_eq!(got, [2]);
    let got = filter(
        &engine,
        r#"{_or: [{extra: {_has_key: "note"}}, {extra: {_contains: {level: 2}}}]}"#,
    )
    .await;
    assert_eq!(got, [1, 3]);
}

#[tokio::test]
async fn operands_bind_from_variables_on_both_paths() {
    let (engine, _db) = setup().await;
    let source = r#"query($doc: jsonb!, $key: String!, $keys: [String!]!) {
        a: departments(where: {extra: {_contains: $doc}}, order_by: {id: asc}) { id }
        b: departments(where: {extra: {_has_key: $key}}, order_by: {id: asc}) { id }
        c: departments(where: {extra: {_has_keys_all: $keys}}, order_by: {id: asc}) { id }
    }"#;
    let vars = json!({"doc": {"tags": ["a"]}, "key": "note", "keys": ["tags", "level"]});
    let want = json!({"a": [{"id": 1}], "b": [{"id": 3}], "c": [{"id": 1}]});

    let eager = engine.query(source, Some(vars.clone())).await.unwrap();
    assert_eq!(eager, want);
    // The compiled path is the one where the value is not known at render
    // time, and the key's type has to come from the operator, not the column.
    let compiled = engine.compile(source).expect("compile");
    assert_eq!(engine.execute(&compiled, Some(vars)).await.unwrap(), want);
    let other = json!({"doc": {"is_mdt": false}, "key": "a", "keys": []});
    assert_eq!(
        engine.execute(&compiled, Some(other)).await.unwrap(),
        json!({"a": [{"id": 2}], "b": [{"id": 5}], "c": [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 5}]})
    );
}

#[tokio::test]
async fn an_optional_operand_left_out_drops_the_filter() {
    let (engine, _db) = setup().await;
    let source = r#"query($key: String @optional, $keys: [String!] @optional) {
        departments(where: {extra: {_has_key: $key, _has_keys_any: $keys}}, order_by: {id: asc}) { id }
    }"#;
    let compiled = engine.compile(source).expect("compile");
    let run = |vars: Value| {
        let (engine, compiled) = (&engine, &compiled);
        async move { ids(&engine.execute(compiled, Some(vars)).await.unwrap()) }
    };
    assert_eq!(
        run(json!({"key": null, "keys": null})).await,
        [1, 2, 3, 4, 5]
    );
    assert_eq!(run(json!({"key": "tags", "keys": null})).await, [1, 2]);
    assert_eq!(
        run(json!({"key": null, "keys": ["note", "level"]})).await,
        [1, 3]
    );
    assert_eq!(
        run(json!({"key": "tags", "keys": ["note", "level"]})).await,
        [1]
    );
}

#[tokio::test]
async fn misuse_is_refused() {
    let (engine, _db) = setup().await;
    for (where_, needle) in [
        // Not jsonb: the operators are published on jsonb columns only.
        (r#"{name: {_has_key: "a"}}"#, "does not apply"),
        (r#"{name: {_contains: "a"}}"#, "does not apply"),
        // A key is a string, whatever the column holds.
        (r#"{extra: {_has_key: 1}}"#, "where.extra"),
        (r#"{extra: {_has_keys_any: "a"}}"#, "where.extra"),
        // A null operand compares against nothing, as for every operator.
        (r#"{extra: {_contains: null}}"#, "null"),
    ] {
        let q = format!("{{ departments(where: {where_}) {{ id }} }}");
        let err = engine.query(&q, None).await.unwrap_err();
        assert!(
            matches!(err, Error::Validate { .. }) && err.to_string().contains(needle),
            "{where_}: {err}"
        );
    }
}

#[tokio::test]
async fn a_scope_policy_can_filter_on_a_key() {
    let (engine, _db) = setup().await;
    let toml = r#"
        [tables.departments]
        where = { extra = { _has_key = "$principal" } }
    "#;
    let policy = ScopePolicy::from_toml(toml, &schema()).expect("policy");
    let v = engine
        .scoped(policy.bind_value("tags").unwrap())
        .query("{ departments(order_by: {id: asc}) { id } }", None)
        .await
        .unwrap();
    assert_eq!(ids(&v), [1, 2]);
    // The operand is checked as the key it is when the policy is built, not
    // as the jsonb the column holds.
    let err = ScopePolicy::from_toml(
        r#"
        [tables.departments]
        where = { extra = { _has_key = 1 } }
        "#,
        &schema(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("extra"), "{err}");
}

#[tokio::test]
async fn published_with_the_operand_types_they_take() {
    let (engine, _db) = setup().await;
    let sdl = vision_graphql::sdl::render(engine.schema().type_system());
    let block = sdl
        .split("input jsonb_comparison_exp")
        .nth(1)
        .and_then(|rest| rest.split('}').next())
        .expect("jsonb_comparison_exp");
    for field in [
        "_contains: jsonb",
        "_contained_in: jsonb",
        "_has_key: String",
        "_has_keys_any: [String!]",
        "_has_keys_all: [String!]",
    ] {
        assert!(block.contains(field), "{field} missing from {block}");
    }
    let text = sdl
        .split("input String_comparison_exp")
        .nth(1)
        .and_then(|rest| rest.split('}').next())
        .unwrap();
    assert!(!text.contains("_has_key"), "{text}");
}
