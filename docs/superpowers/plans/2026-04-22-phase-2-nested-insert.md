# Phase 2: Nested One-to-Many Insert — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable `insert_*` and `insert_*_one` mutations to accept Hasura-style nested array-relation input (e.g. `posts: { data: [...] }`) and render a single SQL statement of chained data-modifying CTEs that atomically insert parent + children, with `returning` visibility of freshly-inserted children.

**Architecture:** AST gets a recursive `InsertObject` type replacing the flat `BTreeMap` on `MutationField::Insert.objects`. Parser recursively descends into array-kind relations, rejecting object relations with a clear "not yet supported" error. Renderer emits one INSERT CTE per (parent, relation-path), using ordinality-tagged `INSERT ... SELECT ... JOIN` to correlate children to parents. A new `inserted_ctes: HashMap<TableName, CteAlias>` render-context lets nested `returning` subqueries read from the child CTE name instead of the real table, bypassing PostgreSQL's same-statement visibility barrier.

**Tech Stack:** Rust, tokio-postgres, async-graphql-parser, serde_json, insta, testcontainers-modules (Postgres 17.4).

---

## File Structure

**Modify:**
- `src/ast.rs` — new `InsertObject` + `NestedArrayInsert` types; `MutationField::Insert.objects: Vec<InsertObject>`.
- `src/parser.rs` — rework `json_object_to_map` (rename to `parse_insert_object`) to recurse into nested relations; update `parse_insert_args` to produce `Vec<InsertObject>`.
- `src/sql.rs` — refactor `render_insert_cte` to take `&[InsertObject]`; emit child CTEs for each nested relation path; add `inserted_ctes` to `RenderCtx`; update `render_relation_subquery` to read from CTE when source table is in `inserted_ctes`; update `render_mutation_output_for` for `affected_rows` summation.
- `src/error.rs` — no changes needed (reuse `Validate`).
- `README.md` — add subsection on nested insert.
- `src/snapshots/` — one new SQL snapshot locking CTE shape.

**Create:**
- `tests/integration_nested_insert.rs` — all new integration tests live here, isolated from `integration_mutation.rs` so the larger 3-table fixture doesn't pollute the existing mutation tests.

**Do not touch:**
- `src/executor.rs`, `src/engine.rs` — parameter binding path already works with arbitrary `Bind` params; no changes needed.
- `src/builder.rs` — programmatic builders can be updated in a follow-up; mutation DSL goes through the parser, not the builder.

---

## Task 1: Failing integration test — single parent with one nested child

**Files:**
- Create: `tests/integration_nested_insert.rs`

- [ ] **Step 1: Create the test file with fixture and single failing test**

Create `tests/integration_nested_insert.rs` with this content:

```rust
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::Engine;

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")])),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .column("published", "published", PgType::Bool, true)
                .primary_key(&["id"])
                .relation("user", Relation::object("users").on([("user_id", "id")]))
                .relation("comments", Relation::array("comments").on([("id", "post_id")])),
        )
        .table(
            Table::new("comments", "public", "comments")
                .column("id", "id", PgType::Int4, false)
                .column("body", "body", PgType::Text, false)
                .column("post_id", "post_id", PgType::Int4, false)
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
    let mut cfg = Config::new();
    cfg.host = Some("127.0.0.1".into());
    cfg.port = Some(host_port);
    cfg.user = Some("postgres".into());
    cfg.password = Some("postgres".into());
    cfg.dbname = Some("postgres".into());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).expect("pool");
    {
        let client = pool.get().await.expect("client");
        client
            .batch_execute(
                r#"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id),
                    published BOOL
                );
                CREATE TABLE comments (
                    id SERIAL PRIMARY KEY,
                    body TEXT NOT NULL,
                    post_id INT NOT NULL REFERENCES posts(id)
                );
                "#,
            )
            .await
            .expect("seed");
    }
    let engine = Engine::new(pool, schema());
    (engine, container)
}

#[tokio::test]
async fn insert_one_parent_with_one_child() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [
                   { name: "alice", posts: { data: [{ title: "p1" }] } }
                 ]) {
                   affected_rows
                   returning { id name posts { title } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(2));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], json!("alice"));
    let posts = rows[0]["posts"].as_array().unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0]["title"], json!("p1"));
}
```

- [ ] **Step 2: Run and confirm the failure mode**

Run: `cargo test --test integration_nested_insert insert_one_parent_with_one_child -- --nocapture`

Expected: FAIL with a parse/validation error — today's `json_object_to_map` at `src/parser.rs:513-517` rejects `posts` as an unknown column. The exact message should be `unknown column 'posts' on 'users'`.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert.rs
git commit -m "test: failing single-parent nested insert"
```

---

## Task 2: Add `InsertObject` + `NestedArrayInsert` AST types, migrate `MutationField::Insert.objects`

**Files:**
- Modify: `src/ast.rs:116-157`
- Modify: `src/parser.rs:271-289` (two `MutationField::Insert` constructor sites)
- Modify: `src/sql.rs:727` (`render_insert_cte` signature and body)
- Modify: `src/sql.rs:690-697` (the call site in `render_mutation`)

**Context:** Purely a type migration. Existing `objects: Vec<BTreeMap<String, serde_json::Value>>` becomes `objects: Vec<InsertObject>` with `InsertObject.nested` always empty until Task 3 populates it. All existing Phase 1 tests (66 integration + 71 unit = 137 total on main's latest `cargo test`) must still pass after this task.

- [ ] **Step 1: Add new types to `src/ast.rs`**

In `src/ast.rs`, just after the `OnConflict` struct (around line 165), add:

```rust
/// One row being inserted. Carries regular column values plus any
/// nested array-relation inserts that should happen as children.
#[derive(Debug, Clone, Default)]
pub struct InsertObject {
    /// `{ exposed_column -> value }` for this parent row.
    pub columns: std::collections::BTreeMap<String, serde_json::Value>,
    /// Nested array-relation inserts, keyed by the parent-side relation name.
    /// Each value carries the rows to insert as children of *this* parent row.
    pub nested: std::collections::BTreeMap<String, NestedArrayInsert>,
}

/// A nested `posts: { data: [...] }` block attached to one parent row.
#[derive(Debug, Clone)]
pub struct NestedArrayInsert {
    /// Target table name (resolved from the parent relation's `target_table`).
    pub table: String,
    /// Rows to insert as children. Each element is itself an `InsertObject`,
    /// so this recurses arbitrarily deep.
    pub rows: Vec<InsertObject>,
}
```

- [ ] **Step 2: Change `MutationField::Insert.objects` field type**

In `src/ast.rs`, locate the `Insert` variant (around line 119-129). Change:

```rust
    Insert {
        alias: String,
        table: String,
        /// Each inner map is `{ exposed_column -> value }` for one row to insert.
        objects: Vec<std::collections::BTreeMap<String, serde_json::Value>>,
        on_conflict: Option<OnConflict>,
        returning: Vec<Field>,
        one: bool,
    },
```

to:

```rust
    Insert {
        alias: String,
        table: String,
        /// Each element is one parent row with its optional nested children.
        objects: Vec<InsertObject>,
        on_conflict: Option<OnConflict>,
        returning: Vec<Field>,
        one: bool,
    },
```

- [ ] **Step 3: Update `parse_insert_args` to produce `Vec<InsertObject>`**

In `src/parser.rs`, find `parse_insert_args` (around line 435). The function currently returns `Vec<std::collections::BTreeMap<String, serde_json::Value>>`. Change the return type and update construction so each `BTreeMap` becomes an `InsertObject { columns: <the map>, nested: BTreeMap::new() }`:

Change this signature and body structure (line 435-499):

```rust
fn parse_insert_args(
    args: &[(Positioned<Name>, Positioned<GqlValue>)],
    table: &Table,
    schema: &Schema,
    vars: &Value,
    parent_path: &str,
    single: bool,
) -> Result<(Vec<crate::ast::InsertObject>, Option<crate::ast::OnConflict>)> {
    use std::collections::BTreeMap;
    let mut objects: Vec<crate::ast::InsertObject> = Vec::new();
    let mut on_conflict: Option<crate::ast::OnConflict> = None;

    for (name_p, value_p) in args {
        let aname = name_p.node.as_str();
        let v = &value_p.node;
        match aname {
            "object" if single => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.object"))?;
                let obj = json_object_to_map(&json, table, &format!("{parent_path}.object"))?;
                objects.push(crate::ast::InsertObject {
                    columns: obj,
                    nested: BTreeMap::new(),
                });
            }
            "objects" if !single => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.objects"))?;
                let arr = json.as_array().ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.objects"),
                    message: "expected array".into(),
                })?;
                for (i, item) in arr.iter().enumerate() {
                    let obj = json_object_to_map(
                        item,
                        table,
                        &format!("{parent_path}.objects[{i}]"),
                    )?;
                    objects.push(crate::ast::InsertObject {
                        columns: obj,
                        nested: BTreeMap::new(),
                    });
                }
            }
            "on_conflict" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.on_conflict"))?;
                on_conflict = Some(parse_on_conflict(
                    &json,
                    table,
                    &format!("{parent_path}.on_conflict"),
                )?);
            }
            other => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.{other}"),
                    message: format!("unknown argument '{other}'"),
                });
            }
        }
    }
    if objects.is_empty() {
        return Err(Error::Validate {
            path: parent_path.into(),
            message: if single {
                "missing required argument 'object'".into()
            } else {
                "missing required argument 'objects'".into()
            },
        });
    }
    Ok((objects, on_conflict))
}
```

(`json_object_to_map` stays unchanged for now — Task 3 will rework it.)

- [ ] **Step 4: Update `render_insert_cte` signature to take `&[InsertObject]`**

In `src/sql.rs`, find `render_insert_cte` (around line 727). Change the signature from:

```rust
fn render_insert_cte(
    cte: &str,
    table_name: &str,
    objects: &[std::collections::BTreeMap<String, serde_json::Value>],
    on_conflict: Option<&crate::ast::OnConflict>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
```

to:

```rust
fn render_insert_cte(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    on_conflict: Option<&crate::ast::OnConflict>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
```

Then inside the function, the loop over `objects` currently does `for (r, obj) in objects.iter().enumerate() { ... for k in obj.keys() { ... } ... obj.get(exposed) ... }`. Replace every direct `obj.keys()` / `obj.get()` with `obj.columns.keys()` / `obj.columns.get()`. Specifically:

- Line 742-746 (collecting column set): change `for k in obj.keys()` to `for k in obj.columns.keys()`.
- Line 777 (getting a value): change `let value = obj.get(exposed);` to `let value = obj.columns.get(exposed);`.

Nothing else in `render_insert_cte` changes at this task.

- [ ] **Step 5: Compile-check and run full test suite**

Run: `cargo check`

Expected: clean build. If the call site in `render_mutation` (line 696) needs adjustment, it shouldn't — it just forwards `objects` by reference.

Run: `cargo test`

Expected: all existing tests (137 total) still pass. The only newly-failing test should be `insert_one_parent_with_one_child` from Task 1, with the SAME error message (`unknown column 'posts'`) — Task 2 hasn't taught the parser anything yet.

- [ ] **Step 6: Commit**

```bash
git add src/ast.rs src/parser.rs src/sql.rs
git commit -m "refactor: introduce InsertObject AST for Phase 2 nested insert"
```

---

## Task 3: Parser accepts nested array-relation input

**Files:**
- Modify: `src/parser.rs:501-528` (`json_object_to_map` → rename to `parse_insert_object`; add recursion)
- Modify: `src/parser.rs:435-499` (`parse_insert_args` calls the new recursive helper)

**Context:** The parser must now walk the input JSON per key and decide: is this key a column, an array relation, or an object relation? For array relations, recurse into `data: [...]`. For object relations, emit a "not yet supported" error per spec (Phase 3 territory). For unknown keys, keep the existing error.

- [ ] **Step 1: Rename and extend `json_object_to_map` to `parse_insert_object`**

In `src/parser.rs`, replace the entire `json_object_to_map` function (line 501-528) with:

```rust
fn parse_insert_object(
    json: &Value,
    table: &Table,
    schema: &Schema,
    path: &str,
) -> Result<crate::ast::InsertObject> {
    use std::collections::BTreeMap;
    let obj = json.as_object().ok_or_else(|| Error::Validate {
        path: path.into(),
        message: "expected object".into(),
    })?;

    let mut columns: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut nested: BTreeMap<String, crate::ast::NestedArrayInsert> = BTreeMap::new();

    for (k, v) in obj {
        // Try column first.
        if table.find_column(k).is_some() {
            columns.insert(k.clone(), v.clone());
            continue;
        }

        // Try relation.
        if let Some(rel) = table.find_relation(k) {
            match rel.kind {
                crate::schema::RelKind::Array => {
                    let target = schema
                        .table(&rel.target_table)
                        .ok_or_else(|| Error::Validate {
                            path: format!("{path}.{k}"),
                            message: format!(
                                "relation target table '{}' missing",
                                rel.target_table
                            ),
                        })?;

                    // Validate shape: `{ data: [...] }`
                    let wrapper = v.as_object().ok_or_else(|| Error::Validate {
                        path: format!("{path}.{k}"),
                        message: "nested array insert expects object with 'data' key".into(),
                    })?;
                    let data = wrapper.get("data").ok_or_else(|| Error::Validate {
                        path: format!("{path}.{k}"),
                        message: "missing required key 'data' in nested array insert".into(),
                    })?;
                    let data_arr = data.as_array().ok_or_else(|| Error::Validate {
                        path: format!("{path}.{k}.data"),
                        message: "expected array".into(),
                    })?;

                    // Reject any extra keys in the wrapper (e.g. stray on_conflict, which is Phase 3).
                    for other_k in wrapper.keys() {
                        if other_k != "data" {
                            return Err(Error::Validate {
                                path: format!("{path}.{k}.{other_k}"),
                                message: format!(
                                    "unknown key '{other_k}' in nested array insert; only 'data' is supported"
                                ),
                            });
                        }
                    }

                    // Recurse into each child row.
                    let mut rows = Vec::with_capacity(data_arr.len());
                    for (i, item) in data_arr.iter().enumerate() {
                        let child = parse_insert_object(
                            item,
                            target,
                            schema,
                            &format!("{path}.{k}.data[{i}]"),
                        )?;

                        // Reject child input that sets the FK column(s) that the engine
                        // will supply from the parent.
                        for (_parent_col, child_fk_col) in &rel.mapping {
                            if child.columns.contains_key(child_fk_col) {
                                return Err(Error::Validate {
                                    path: format!("{path}.{k}.data[{i}].{child_fk_col}"),
                                    message: format!(
                                        "column '{child_fk_col}' is populated from the parent; must not appear in nested child input"
                                    ),
                                });
                            }
                        }

                        rows.push(child);
                    }

                    nested.insert(
                        k.clone(),
                        crate::ast::NestedArrayInsert {
                            table: rel.target_table.clone(),
                            rows,
                        },
                    );
                    continue;
                }
                crate::schema::RelKind::Object => {
                    return Err(Error::Validate {
                        path: format!("{path}.{k}"),
                        message: format!(
                            "object-relation nested insert for '{k}' is not yet supported; use a separate mutation"
                        ),
                    });
                }
            }
        }

        return Err(Error::Validate {
            path: format!("{path}.{k}"),
            message: format!("unknown column '{k}' on '{}'", table.exposed_name),
        });
    }

    if columns.is_empty() && nested.is_empty() {
        return Err(Error::Validate {
            path: path.into(),
            message: "insert row must set at least one column or nested relation".into(),
        });
    }

    Ok(crate::ast::InsertObject { columns, nested })
}
```

- [ ] **Step 2: Update `parse_insert_args` to call the new helper**

In `src/parser.rs`, inside `parse_insert_args` (line 435-499), replace both calls to `json_object_to_map` with `parse_insert_object`. The two call sites are inside the `"object" if single =>` arm and the `"objects" if !single =>` arm.

At `src/parser.rs` inside the `"object" if single =>` branch, change:

```rust
                let obj = json_object_to_map(&json, table, &format!("{parent_path}.object"))?;
                objects.push(crate::ast::InsertObject {
                    columns: obj,
                    nested: BTreeMap::new(),
                });
```

to:

```rust
                let obj = parse_insert_object(&json, table, schema, &format!("{parent_path}.object"))?;
                objects.push(obj);
```

At the `"objects" if !single =>` branch, change:

```rust
                    let obj = json_object_to_map(
                        item,
                        table,
                        &format!("{parent_path}.objects[{i}]"),
                    )?;
                    objects.push(crate::ast::InsertObject {
                        columns: obj,
                        nested: BTreeMap::new(),
                    });
```

to:

```rust
                    let obj = parse_insert_object(
                        item,
                        table,
                        schema,
                        &format!("{parent_path}.objects[{i}]"),
                    )?;
                    objects.push(obj);
```

- [ ] **Step 3: Compile and run full tests**

Run: `cargo check`

Expected: clean build. If a residual call to `json_object_to_map` remains, the compiler will point at it.

Run: `cargo test`

Expected: all 137 existing tests still pass. `insert_one_parent_with_one_child` still fails — but with a DIFFERENT error now. Instead of a parse error, the failure will come from the renderer when it processes the non-empty `nested` map. The exact new failure depends on Task 4's starting state; expect a panic or malformed SQL since Task 4 hasn't taught the renderer to handle nested yet.

Run specifically: `cargo test --test integration_nested_insert insert_one_parent_with_one_child -- --nocapture 2>&1 | tail -30`

Expected: the error path changes from a parser validation error to either a renderer-side failure or a silent drop (parent inserts, no child inserts). Either way is fine for intermediate TDD state — Task 5 finishes the fix.

- [ ] **Step 4: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): accept Hasura-style nested array-relation insert input"
```

---

## Task 4: Parser-only unit validation tests

**Files:**
- Modify: `tests/integration_nested_insert.rs`

**Context:** Before wiring the renderer, add tests that exercise the *parser's* error paths via the engine. They don't need Docker to reveal parse bugs, but will spin the container anyway since they call `engine.query()`. Keeping them here means they live with the rest of the Phase 2 suite.

- [ ] **Step 1: Append four validation tests**

Append to `tests/integration_nested_insert.rs`:

```rust
#[tokio::test]
async fn nested_insert_missing_data_key_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{ name: "x", posts: {} }]) {
                   affected_rows
                 }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(msg.contains("'data'"), "error was: {msg}");
}

#[tokio::test]
async fn nested_insert_non_array_data_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{ name: "x", posts: { data: {} } }]) {
                   affected_rows
                 }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(msg.contains("expected array"), "error was: {msg}");
}

#[tokio::test]
async fn nested_insert_child_fk_column_rejected() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_users(objects: [
                   { name: "x", posts: { data: [{ title: "t", user_id: 99 }] } }
                 ]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(msg.contains("populated from the parent"), "error was: {msg}");
}

#[tokio::test]
async fn nested_insert_object_relation_rejected() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [
                   { title: "t", user: { data: { name: "x" } } }
                 ]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("object-relation nested insert for 'user'"),
        "error was: {msg}"
    );
}
```

- [ ] **Step 2: Run and verify all four PASS**

Run: `cargo test --test integration_nested_insert -- nested_insert_missing nested_insert_non_array nested_insert_child_fk nested_insert_object_relation`

Expected: 4/4 PASS. Each one hits a parser error path added in Task 3 and returns before any SQL rendering.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert.rs
git commit -m "test: nested insert parser validation errors"
```

---

## Task 5: Renderer emits child CTEs for single-level nested insert

**Files:**
- Modify: `src/sql.rs` — `render_insert_cte` gets rewritten to emit parent + child CTEs in one call; `render_mutation` loop stays the same (one `m<i>` CTE alias per top-level mutation field, but nested-insert now generates multiple PG-level CTEs under that umbrella alias).
- Modify: `src/sql.rs` — `RenderCtx` gets an `inserted_ctes: HashMap<String, String>` field (table-name → CTE alias).
- Modify: `src/sql.rs` — `render_mutation_output_for` builds the umbrella CTE name off the *parent* table's CTE, and populates `affected_rows` as a sum across all CTEs emitted for this field.

**Context:** This is the meat of Phase 2. The CTE shape we emit (verbatim spec example):

```sql
WITH
  p0_input AS (
    SELECT * FROM (VALUES (1, $1), (2, $2)) AS t(ord, name)
  ),
  m0 AS (
    INSERT INTO "public"."users" ("name")
    SELECT name FROM p0_input ORDER BY ord
    RETURNING *
  ),
  m0_ord AS (
    SELECT *, ROW_NUMBER() OVER () AS ord FROM m0
  ),
  p1_input AS (
    SELECT * FROM (VALUES (1, $3), (1, $4), (2, $5)) AS t(parent_ord, title)
  ),
  m1 AS (
    INSERT INTO "public"."posts" ("title", "user_id")
    SELECT c.title, p.id
    FROM p1_input c
    JOIN m0_ord p ON p.ord = c.parent_ord
    RETURNING *
  )
SELECT ...
```

Key design decisions:
1. The top-level `render_mutation` loop emits `m0`, `m1`, … as top-level aliases. Phase 1 had one CTE per mutation field (`m0` for the whole `insert_users`). In Phase 2, `render_insert_cte` is responsible for emitting the full parent-input + parent-insert + ord-numbered + child-input + child-insert chain and may append multiple CTEs to `ctx.sql`. For downstream use, the *parent* CTE takes the umbrella alias passed in (`m<i>`), and nested child CTEs get auto-generated aliases prefixed with that umbrella (e.g. `m0_posts`, `m0_comments`). This keeps the top-level `json_build_object` key (`insert_users`) pointing at the right summary.
2. We need to pass info about what child CTEs we emitted back to the caller so that (a) `affected_rows` can sum them, and (b) the nested `returning` render can read from the CTE instead of the real table. The cleanest vehicle is `RenderCtx.inserted_ctes`, a hash map from *table name* (e.g., `"posts"`) to *CTE alias* (e.g., `"m0_posts"`).
3. Use inline `VALUES` for the input CTEs, with every value parameterized through `ctx.binds` — same pattern as today's `render_insert_cte`. No JSONB unnest; keeps the Bind-based param layer untouched.

Implementation will touch ~120 lines in `src/sql.rs`. This task is the largest in the plan.

- [ ] **Step 1: Add `inserted_ctes` to `RenderCtx`**

In `src/sql.rs`, update the `RenderCtx` struct (line 22):

```rust
#[derive(Default)]
struct RenderCtx {
    sql: String,
    binds: Vec<Bind>,
    alias_counter: usize,
    /// Maps target-table-name → CTE alias for INSERT CTEs emitted in this
    /// statement. Used by nested-returning render to decide whether to read
    /// from the CTE (when source was just inserted here) or from the real
    /// table (Phase 1 behavior).
    inserted_ctes: std::collections::HashMap<String, String>,
}
```

- [ ] **Step 2: Replace the body of `render_insert_cte` with the nested-capable renderer**

Replace the entire `render_insert_cte` function (line 727-804 currently) with:

```rust
fn render_insert_cte(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    on_conflict: Option<&crate::ast::OnConflict>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    render_insert_cte_recursive(
        cte,
        table_name,
        objects,
        on_conflict,
        None, // no parent_ord_cte — this is the top-level parent insert
        schema,
        ctx,
    )
}

/// Emits:
///   [parent_ord_cte provided ⇒ child_input_cte +] insert_cte
///   for this level, then recurses into each nested array relation.
///
/// When `parent_ord_cte` is `Some((name, parent_table))`, the INSERT at this
/// level is a child insert that joins against that parent's ordinality CTE.
/// Each child row remembers the 1-based ordinal of the parent row it belongs
/// to, via a `parent_ord` column in the input VALUES.
fn render_insert_cte_recursive(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    on_conflict: Option<&crate::ast::OnConflict>,
    parent_ord_cte: Option<(&str, &crate::schema::Relation, &crate::schema::Table)>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use std::collections::BTreeSet;

    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;

    // 1. Collect the set of columns appearing across all rows.
    let mut col_set: BTreeSet<String> = BTreeSet::new();
    for obj in objects {
        for k in obj.columns.keys() {
            col_set.insert(k.clone());
        }
    }
    let cols: Vec<String> = col_set.into_iter().collect();

    // 2. Emit input CTE with inline VALUES.
    //    Top-level parent: `VALUES (ord, <col1>, <col2>, ...)` with ord = 1..N.
    //    Child level:      `VALUES (parent_ord, <col1>, <col2>, ...)` where parent_ord
    //                      is the 1-based ordinal of the parent row this child belongs to.
    let input_cte = format!("{cte}_input");
    let ord_col = if parent_ord_cte.is_some() {
        "parent_ord"
    } else {
        "ord"
    };

    write!(ctx.sql, "{input_cte} AS (SELECT * FROM (VALUES ").unwrap();

    // Flatten rows into a (parent_ord_value, &InsertObject) stream.
    // For top-level, parent_ord_value == the row's 1-based index.
    // For child level, parent_ord_value comes from the row's position inside
    // its parent's `rows` — but actually the caller knows this: we flatten in
    // Task 5's child-loop below. Here we only handle the already-flattened
    // `objects: &[InsertObject]`. The caller ensures each element has a
    // correct parent_ord attached — but at this layer we don't have it.
    //
    // Redesign: `objects` alone is insufficient for child levels. We need an
    // extra "parent_ord per object" slice. Rather than complicate this fn,
    // we accept a parallel `parent_ords: Option<&[i64]>`.
    //
    // See Step 3 for the final signature.
    todo!("reworked in Step 3");
}
```

**Note:** Step 2 sketches the first-draft design, but we immediately see the parent-ord parameter needs to be passed in. Don't commit Step 2 yet — proceed to Step 3 which gives the final signature and body. This split is intentional so the executor can follow the reasoning.

- [ ] **Step 3: Final signature and full body of `render_insert_cte_recursive`**

Replace the `todo!()` scaffold from Step 2 with the full implementation. Use this final design — `render_insert_cte` stays the public entry, `render_insert_cte_recursive` carries the extra plumbing:

```rust
fn render_insert_cte(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    on_conflict: Option<&crate::ast::OnConflict>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    // Top-level: parent ordinals are just 1..=N.
    let parent_ords: Vec<i64> = (1..=objects.len() as i64).collect();
    render_insert_cte_recursive(
        cte,
        table_name,
        objects,
        &parent_ords,
        on_conflict,
        None,
        schema,
        ctx,
    )
}

#[allow(clippy::too_many_arguments)]
fn render_insert_cte_recursive(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    parent_ords: &[i64],
    on_conflict: Option<&crate::ast::OnConflict>,
    // Some((parent_ord_cte_alias, relation, parent_table))
    // when this call is a child insert.
    parent_link: Option<(&str, &crate::schema::Relation, &crate::schema::Table)>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use std::collections::BTreeSet;

    debug_assert_eq!(objects.len(), parent_ords.len());

    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;

    if objects.is_empty() {
        // Nothing to insert at this level — emit a no-op CTE so later CTEs
        // can still reference {cte} without type errors. Use a SELECT of an
        // empty, correctly-typed row set.
        write!(
            ctx.sql,
            "{cte} AS (SELECT * FROM {}.{} WHERE FALSE)",
            quote_ident(&table.physical_schema),
            quote_ident(&table.physical_name),
        )
        .unwrap();
        ctx.inserted_ctes.insert(table_name.to_string(), cte.to_string());
        return Ok(());
    }

    // 1. Collect all columns appearing in any row.
    let mut col_set: BTreeSet<String> = BTreeSet::new();
    for obj in objects {
        for k in obj.columns.keys() {
            col_set.insert(k.clone());
        }
    }
    let cols: Vec<String> = col_set.into_iter().collect();

    // 2. Emit the `{cte}_input` VALUES CTE with the ord column and each column value.
    let input_cte = format!("{cte}_input");
    let ord_col_name = if parent_link.is_some() { "parent_ord" } else { "ord" };

    write!(ctx.sql, "{input_cte} AS (SELECT * FROM (VALUES ").unwrap();
    for (r, obj) in objects.iter().enumerate() {
        if r > 0 {
            ctx.sql.push_str(", ");
        }
        ctx.sql.push('(');
        // First column: the ordinal.
        write!(ctx.sql, "{}", parent_ords[r]).unwrap();
        // Remaining columns: each value (or DEFAULT).
        for exposed in &cols {
            ctx.sql.push_str(", ");
            let col = table
                .find_column(exposed)
                .expect("column should exist — validated at parse");
            match obj.columns.get(exposed) {
                None => ctx.sql.push_str("DEFAULT"),
                Some(v) => {
                    let bind = crate::types::json_to_bind(v, &col.pg_type).map_err(|e| {
                        Error::Validate {
                            path: format!("{cte}.objects[{r}].{exposed}"),
                            message: format!("{e}"),
                        }
                    })?;
                    ctx.binds.push(bind);
                    write!(ctx.sql, "${}", ctx.binds.len()).unwrap();
                }
            }
        }
        ctx.sql.push(')');
    }
    write!(ctx.sql, ") AS t({ord_col_name}").unwrap();
    for exposed in &cols {
        write!(ctx.sql, ", {}", quote_ident(exposed)).unwrap();
    }
    ctx.sql.push_str(")), ");

    // 3. Emit the actual INSERT CTE.
    //    If this is a child, we INSERT (<columns>, <fk_cols_from_parent>)
    //    SELECT <cols>, p.<parent_pk_cols> FROM {input_cte} c JOIN {parent_ord_cte} p ON p.ord = c.parent_ord.
    //    If this is top-level, we INSERT (<columns>)
    //    SELECT <cols> FROM {input_cte} ORDER BY ord.
    write!(
        ctx.sql,
        "{cte} AS (INSERT INTO {}.{} (",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();

    // Physical column list for INSERT target.
    let mut first = true;
    for exposed in &cols {
        if !first {
            ctx.sql.push_str(", ");
        }
        first = false;
        let col = table.find_column(exposed).unwrap();
        ctx.sql.push_str(&quote_ident(&col.physical_name));
    }
    // Add FK columns when this is a child insert.
    if let Some((_, rel, _)) = parent_link {
        for (_, child_col) in &rel.mapping {
            if !first {
                ctx.sql.push_str(", ");
            }
            first = false;
            let col = table.find_column(child_col).ok_or_else(|| Error::Validate {
                path: cte.into(),
                message: format!("mapped FK column '{child_col}' missing on '{}'", table.exposed_name),
            })?;
            ctx.sql.push_str(&quote_ident(&col.physical_name));
        }
    }
    ctx.sql.push(')');

    // SELECT source.
    match parent_link {
        None => {
            ctx.sql.push_str(" SELECT ");
            let mut first_sel = true;
            for exposed in &cols {
                if !first_sel {
                    ctx.sql.push_str(", ");
                }
                first_sel = false;
                ctx.sql.push_str(&quote_ident(exposed));
            }
            write!(ctx.sql, " FROM {input_cte} ORDER BY ord").unwrap();
        }
        Some((parent_ord_cte_alias, rel, parent_table)) => {
            ctx.sql.push_str(" SELECT ");
            let mut first_sel = true;
            for exposed in &cols {
                if !first_sel {
                    ctx.sql.push_str(", ");
                }
                first_sel = false;
                write!(ctx.sql, "c.{}", quote_ident(exposed)).unwrap();
            }
            // FK columns come from the parent ord CTE.
            for (parent_col, _) in &rel.mapping {
                if !first_sel {
                    ctx.sql.push_str(", ");
                }
                first_sel = false;
                let pcol = parent_table.find_column(parent_col).ok_or_else(|| Error::Validate {
                    path: cte.into(),
                    message: format!(
                        "mapped parent column '{parent_col}' missing on '{}'",
                        parent_table.exposed_name
                    ),
                })?;
                write!(ctx.sql, "p.{}", quote_ident(&pcol.physical_name)).unwrap();
            }
            write!(
                ctx.sql,
                " FROM {input_cte} c JOIN {parent_ord_cte_alias} p ON p.ord = c.parent_ord"
            )
            .unwrap();
        }
    }

    if let Some(oc) = on_conflict {
        render_on_conflict(oc, table, schema, ctx)?;
    }
    ctx.sql.push_str(" RETURNING *)");

    // Track this CTE for returning-visibility lookup.
    ctx.inserted_ctes.insert(table_name.to_string(), cte.to_string());

    // 4. For each nested array relation we need to recurse into, emit the
    //    parent-ord CTE first (because children need to JOIN against it),
    //    then the child chain.
    //
    //    We only emit `{cte}_ord` if at least one child needs it; it's cheap
    //    to always emit though, and simplifies reasoning.
    let any_nested = objects.iter().any(|o| !o.nested.is_empty());
    if any_nested {
        write!(
            ctx.sql,
            ", {cte}_ord AS (SELECT *, ROW_NUMBER() OVER () AS ord FROM {cte})"
        )
        .unwrap();

        // Group children by relation name across all parent objects, tracking
        // which parent_ord each child row belongs to.
        use std::collections::BTreeMap;
        let mut per_relation: BTreeMap<&str, (Vec<i64>, Vec<crate::ast::InsertObject>)> =
            BTreeMap::new();

        for (parent_ord_val, obj) in parent_ords.iter().zip(objects.iter()) {
            for (rel_name, nested) in &obj.nested {
                let entry = per_relation
                    .entry(rel_name.as_str())
                    .or_insert_with(|| (Vec::new(), Vec::new()));
                for child in &nested.rows {
                    entry.0.push(*parent_ord_val);
                    entry.1.push(child.clone());
                }
            }
        }

        for (rel_name, (child_ords, child_rows)) in per_relation {
            let rel = table.find_relation(rel_name).ok_or_else(|| Error::Validate {
                path: cte.into(),
                message: format!("unknown relation '{rel_name}' on '{}'", table.exposed_name),
            })?;
            // Child CTE alias: `{cte}_{rel_name}`.
            let child_cte = format!("{cte}_{rel_name}");
            let parent_ord_cte_name = format!("{cte}_ord");
            ctx.sql.push_str(", ");
            render_insert_cte_recursive(
                &child_cte,
                &rel.target_table,
                &child_rows,
                &child_ords,
                None, // nested children don't carry their own on_conflict in Phase 2
                Some((&parent_ord_cte_name, rel, table)),
                schema,
                ctx,
            )?;
        }
    }

    Ok(())
}
```

Required imports at the top of `src/sql.rs` if not already present:
- `use std::fmt::Write;` — already there.
- `crate::schema::Relation` and `crate::schema::Table` are already referenced by `render_relation_subquery` — nothing new.

- [ ] **Step 4: Update `affected_rows` in `render_mutation_output_for`**

In `src/sql.rs`, find the Insert arm of `render_mutation_output_for` (line 1036-1062). The current line:

```rust
                write!(ctx.sql, "'affected_rows', (SELECT count(*) FROM {cte})").unwrap();
```

is wrong for Phase 2 because `{cte}` only counts the parent rows. Replace the Insert arm entirely with:

```rust
        MutationField::Insert {
            alias,
            table,
            returning,
            one,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(ctx.sql, "'{}', ", escape_string_literal(alias)).unwrap();
            if *one {
                ctx.sql.push_str("(SELECT ");
                if returning.is_empty() {
                    ctx.sql.push_str("'{}'::json");
                } else {
                    render_json_build_object_for_nodes(returning, cte, tbl, alias, schema, ctx)?;
                }
                write!(ctx.sql, " FROM {cte} LIMIT 1)").unwrap();
            } else {
                ctx.sql.push_str("json_build_object(");
                // affected_rows sums the parent CTE with every child CTE that
                // was emitted under it.
                ctx.sql.push_str("'affected_rows', (");
                // Gather all CTEs whose aliases start with the umbrella
                // `{cte}` (the parent) or `{cte}_` (the children at any level).
                // Use ctx.inserted_ctes for this — its values are all the
                // CTE aliases.
                let mut matching: Vec<&String> = ctx
                    .inserted_ctes
                    .values()
                    .filter(|v| v.as_str() == cte || v.starts_with(&format!("{cte}_")))
                    .collect();
                matching.sort();
                for (i, c) in matching.iter().enumerate() {
                    if i > 0 {
                        ctx.sql.push_str(" + ");
                    }
                    write!(ctx.sql, "(SELECT count(*) FROM {c})").unwrap();
                }
                if matching.is_empty() {
                    // Defensive — should never happen; means render_insert_cte
                    // didn't record the parent CTE. Fall back to bare count.
                    write!(ctx.sql, "SELECT count(*) FROM {cte}").unwrap();
                }
                ctx.sql.push(')');

                if !returning.is_empty() {
                    ctx.sql
                        .push_str(", 'returning', (SELECT coalesce(json_agg(");
                    render_json_build_object_for_nodes(returning, cte, tbl, alias, schema, ctx)?;
                    write!(ctx.sql, "), '[]'::json) FROM {cte})").unwrap();
                } else {
                    ctx.sql.push_str(", 'returning', '[]'::json");
                }
                ctx.sql.push(')');
            }
        }
```

- [ ] **Step 5: Compile and run the Task-1 test**

Run: `cargo check`

Expected: clean build. If there are mismatches (e.g., `Relation` not imported, missing `debug_assert_eq!`), fix in place.

Run: `cargo test --test integration_nested_insert insert_one_parent_with_one_child -- --nocapture 2>&1 | tail -30`

Expected behavior depends on whether the nested returning already reads from the CTE. At this point it doesn't — the parent's `returning { posts { title } }` still reads from the real `"public"."posts"` table (Phase 1 behavior), which PG's snapshot hides from the just-inserted rows. So the test likely fails with `assertion failed: posts.len() == 1` (got 0) even though `affected_rows == 2` is correct.

That's the expected intermediate state. Task 6 flips `returning` to read from the CTE.

If instead the test fails with a SQL error, something in render is wrong — debug the rendered SQL by adding `println!` before `execute`, or by enabling tracing. Do NOT commit broken render.

- [ ] **Step 6: Commit**

```bash
git add src/sql.rs
git commit -m "feat(sql): emit child CTEs with ordinality correlation for nested insert"
```

---

## Task 6: Nested `returning` reads from child CTE when target was inserted

**Files:**
- Modify: `src/sql.rs` — `render_relation_subquery` (around line 315) learns to prefer `ctx.inserted_ctes` over the real table when the target table was just nested-inserted in this statement.

**Context:** `render_relation_subquery` currently always emits `FROM "schema"."table" <alias>`. For the returning of a nested-insert parent, the real table does not (yet) hold the freshly-inserted rows — they're only visible via the child CTE's name. We check `ctx.inserted_ctes` to decide the `FROM` source.

- [ ] **Step 1: Update `render_relation_subquery`'s FROM clause**

Find the line in `src/sql.rs` (inside `render_relation_subquery`, around line 405-410) that writes:

```rust
    write!(
        ctx.sql,
        " FROM {}.{} {remote_alias}",
        quote_ident(&target.physical_schema),
        quote_ident(&target.physical_name),
    )
    .unwrap();
```

Replace with:

```rust
    if let Some(cte_alias) = ctx.inserted_ctes.get(&rel.target_table).cloned() {
        write!(ctx.sql, " FROM {cte_alias} {remote_alias}").unwrap();
    } else {
        write!(
            ctx.sql,
            " FROM {}.{} {remote_alias}",
            quote_ident(&target.physical_schema),
            quote_ident(&target.physical_name),
        )
        .unwrap();
    }
```

The `.cloned()` above sidesteps a borrow conflict with `ctx.sql` being mutated after the lookup.

- [ ] **Step 2: Run the Task-1 test**

Run: `cargo test --test integration_nested_insert insert_one_parent_with_one_child -- --nocapture`

Expected: PASS. `affected_rows == 2`, `returning[0].posts` contains `{ title: "p1" }`.

If it still fails with `posts.len() == 0`, dump the rendered SQL (insert a `tracing::debug!` or print before executing) and check:
- Is `m0_posts` in the rendered SQL? (It should be.)
- Does the returning subquery read `FROM m0_posts t0` or `FROM "public"."posts" t0`? (After this step, it should be the former.)
- Is `t0."user_id" = m0."id"` correct direction? (For `Relation::array("posts").on([("id", "user_id")])`, `id` is the parent-side, `user_id` is child-side, so `t0."user_id" = m0."id"` is right.)

- [ ] **Step 3: Run full test suite to confirm no regressions on Phase 1 tests**

Run: `cargo test`

Expected: everything GREEN. Phase 1's Task-1 test (`insert_array_returning_with_nested_relation` in `integration_mutation.rs`) must still pass — in that test, the `posts` relation is NOT nested-inserted (the mutation only inserts `users`), so `ctx.inserted_ctes` does NOT contain `posts`, and the real-table path is taken, correctly returning `[]` for the freshly-inserted user.

- [ ] **Step 4: Commit**

```bash
git add src/sql.rs
git commit -m "feat(sql): nested returning reads from child CTE after nested insert"
```

---

## Task 7: Multi-parent correlation test

**Files:**
- Modify: `tests/integration_nested_insert.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_insert_multi_parent_correlation() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [
                   { name: "u1", posts: { data: [{ title: "u1-p1" }, { title: "u1-p2" }] } },
                   { name: "u2", posts: { data: [{ title: "u2-p1" }] } }
                 ]) {
                   affected_rows
                   returning {
                     name
                     posts(order_by: [{ id: asc }]) { title }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(5));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 2);

    let u1 = rows.iter().find(|r| r["name"] == json!("u1")).expect("u1");
    let u1_titles: Vec<_> = u1["posts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["title"].clone())
        .collect();
    assert_eq!(u1_titles, vec![json!("u1-p1"), json!("u1-p2")]);

    let u2 = rows.iter().find(|r| r["name"] == json!("u2")).expect("u2");
    let u2_titles: Vec<_> = u2["posts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["title"].clone())
        .collect();
    assert_eq!(u2_titles, vec![json!("u2-p1")]);
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_nested_insert nested_insert_multi_parent_correlation`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert.rs
git commit -m "test: multi-parent nested insert correlation"
```

---

## Task 8: Correlation stress test (5+ parents)

**Files:**
- Modify: `tests/integration_nested_insert.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_insert_correlation_stress() {
    let (engine, _c) = setup().await;

    // 5 parents, each with a unique marker title on their child so we can
    // verify correlation by FK round-trip.
    let mutation = r#"mutation {
        insert_users(objects: [
          { name: "a", posts: { data: [{ title: "a-child" }] } },
          { name: "b", posts: { data: [{ title: "b-child" }] } },
          { name: "c", posts: { data: [{ title: "c-child" }] } },
          { name: "d", posts: { data: [{ title: "d-child" }] } },
          { name: "e", posts: { data: [{ title: "e-child" }] } }
        ]) {
          affected_rows
          returning { id name }
        }
      }"#;
    let v: Value = engine.query(mutation, None).await.expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(10));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 5);

    // For each parent, query the real DB and confirm their child's title
    // prefix matches their name.
    for r in rows {
        let name = r["name"].as_str().unwrap().to_string();
        let id = r["id"].as_i64().unwrap();

        let v2: Value = engine
            .query(
                &format!(
                    r#"query {{ posts(where: {{ user_id: {{_eq: {id} }} }}) {{ title }} }}"#
                ),
                None,
            )
            .await
            .expect("lookup ok");
        let titles: Vec<_> = v2["posts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["title"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(titles, vec![format!("{name}-child")]);
    }
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_nested_insert nested_insert_correlation_stress`

Expected: PASS. Every parent's child has the right prefix, confirming correlation is stable for 5 parents.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert.rs
git commit -m "test: 5-parent correlation stress for nested insert"
```

---

## Task 9: Multi-level nesting (users → posts → comments)

**Files:**
- Modify: `tests/integration_nested_insert.rs`

**Context:** The `comments` table is already in the fixture from Task 1. This test inserts one user with one post with two comments, and asserts all three levels landed atomically.

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_insert_three_levels() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "a",
                   posts: {
                     data: [{
                       title: "p1",
                       comments: { data: [{ body: "c1" }, { body: "c2" }] }
                     }]
                   }
                 }]) {
                   affected_rows
                   returning {
                     name
                     posts {
                       title
                       comments(order_by: [{ id: asc }]) { body }
                     }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(4));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let posts = rows[0]["posts"].as_array().unwrap();
    assert_eq!(posts.len(), 1);
    let comments = posts[0]["comments"].as_array().unwrap();
    assert_eq!(comments.len(), 2);
    assert_eq!(comments[0]["body"], json!("c1"));
    assert_eq!(comments[1]["body"], json!("c2"));
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_nested_insert nested_insert_three_levels`

Expected: PASS. If it fails with "unknown relation 'comments'" — check that the `posts` table definition in the `schema()` function (Task 1) includes `.relation("comments", Relation::array("comments").on([("id", "post_id")]))`.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert.rs
git commit -m "test: three-level nested insert (users→posts→comments)"
```

---

## Task 10: Sibling array relations on the same parent

**Files:**
- Modify: `tests/integration_nested_insert.rs` — extend fixture to add a `reactions` table on users, then add the sibling test.

**Context:** We need a second array relation on `users` alongside `posts`. Cheapest: a `reactions` table with `user_id` FK. This also doubles as the fixture for Task 11 (non-nested sibling relation stays reading from real table).

- [ ] **Step 1: Extend the fixture schema**

In `tests/integration_nested_insert.rs`'s `schema()` function, add a `reactions` table and its relation back to users:

```rust
fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")]))
                .relation("reactions", Relation::array("reactions").on([("id", "user_id")])),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .column("published", "published", PgType::Bool, true)
                .primary_key(&["id"])
                .relation("user", Relation::object("users").on([("user_id", "id")]))
                .relation("comments", Relation::array("comments").on([("id", "post_id")])),
        )
        .table(
            Table::new("comments", "public", "comments")
                .column("id", "id", PgType::Int4, false)
                .column("body", "body", PgType::Text, false)
                .column("post_id", "post_id", PgType::Int4, false)
                .primary_key(&["id"]),
        )
        .table(
            Table::new("reactions", "public", "reactions")
                .column("id", "id", PgType::Int4, false)
                .column("kind", "kind", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .primary_key(&["id"]),
        )
        .build()
}
```

And the `batch_execute` SQL in `setup()` — add a `reactions` table create:

```rust
                CREATE TABLE reactions (
                    id SERIAL PRIMARY KEY,
                    kind TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id)
                );
```

Append this CREATE TABLE after the `comments` CREATE TABLE statement in the existing `batch_execute` string.

- [ ] **Step 2: Append the sibling-relations test**

```rust
#[tokio::test]
async fn nested_insert_sibling_array_relations() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "a",
                   posts:     { data: [{ title: "p1" }] },
                   reactions: { data: [{ kind: "like" }, { kind: "wow" }] }
                 }]) {
                   affected_rows
                   returning {
                     name
                     posts     { title }
                     reactions(order_by: [{ id: asc }]) { kind }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(4));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows[0]["posts"].as_array().unwrap().len(), 1);
    let kinds: Vec<_> = rows[0]["reactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["kind"].clone())
        .collect();
    assert_eq!(kinds, vec![json!("like"), json!("wow")]);
}
```

- [ ] **Step 3: Run and verify PASS**

Run: `cargo test --test integration_nested_insert nested_insert_sibling_array_relations`

Expected: PASS. Also re-run the earlier Phase 2 tests to confirm the fixture extension didn't break them:

Run: `cargo test --test integration_nested_insert`

Expected: all previously-added Phase 2 tests still green.

- [ ] **Step 4: Commit**

```bash
git add tests/integration_nested_insert.rs
git commit -m "test: sibling array relations in nested insert"
```

---

## Task 11: Non-nested sibling relation returns `[]` correctly

**Files:**
- Modify: `tests/integration_nested_insert.rs`

**Context:** Locks in the "read from real table for relations NOT in `inserted_ctes`" branch. Insert a user with nested `posts`, but ask the `returning` to also select `reactions` (which were NOT nested-inserted). `reactions` must return an empty array.

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_insert_unrelated_sibling_returns_empty() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "a",
                   posts: { data: [{ title: "p1" }] }
                 }]) {
                   returning {
                     name
                     posts     { title }
                     reactions { kind }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    // posts were nested-inserted → visible via CTE.
    assert_eq!(rows[0]["posts"].as_array().unwrap().len(), 1);
    // reactions were not touched → real-table read returns [] for this fresh user.
    assert_eq!(rows[0]["reactions"], json!([]));
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_nested_insert nested_insert_unrelated_sibling_returns_empty`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert.rs
git commit -m "test: non-nested sibling returns [] from real table"
```

---

## Task 12: Empty nested `data: []`

**Files:**
- Modify: `tests/integration_nested_insert.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_insert_empty_children_array() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "a",
                   posts: { data: [] }
                 }]) {
                   affected_rows
                   returning { name posts { title } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(1));
    assert_eq!(v["insert_users"]["returning"][0]["name"], json!("a"));
    assert_eq!(v["insert_users"]["returning"][0]["posts"], json!([]));
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_nested_insert nested_insert_empty_children_array`

Expected: PASS. `affected_rows` is 1 (just the parent — no child CTE row-count contributes since the no-op child CTE in `render_insert_cte_recursive` Step 3 above produces zero rows).

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert.rs
git commit -m "test: empty nested data array inserts parent only"
```

---

## Task 13: `insert_*_one` with nested children

**Files:**
- Modify: `tests/integration_nested_insert.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_insert_one_with_children() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users_one(object: {
                   name: "solo",
                   posts: { data: [{ title: "p1" }, { title: "p2" }] }
                 }) {
                   id
                   name
                   posts(order_by: [{ id: asc }]) { title }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let one = &v["insert_users_one"];
    assert_eq!(one["name"], json!("solo"));
    let titles: Vec<_> = one["posts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["title"].clone())
        .collect();
    assert_eq!(titles, vec![json!("p1"), json!("p2")]);
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_nested_insert nested_insert_one_with_children`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert.rs
git commit -m "test: insert_*_one with nested children"
```

---

## Task 14: Atomic rollback on child failure

**Files:**
- Modify: `tests/integration_nested_insert.rs`

**Context:** Provoke a child-side failure (NOT NULL violation on `comments.body`) and verify the parent insert was rolled back — no orphan `users` or `posts` row survives.

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_insert_rolls_back_on_child_failure() {
    let (engine, _c) = setup().await;

    // `body: null` violates NOT NULL on comments.body. The whole mutation
    // should fail atomically — no users, posts, or comments should persist.
    let err = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "rb",
                   posts: { data: [{
                     title: "t",
                     comments: { data: [{ body: null }] }
                   }] }
                 }]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected DB error");
    let _ = err; // inspect not needed; PG returns a NOT NULL violation

    // Independent query to verify no `rb` user landed.
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_eq: "rb"}}) { id } }"#,
            None,
        )
        .await
        .expect("lookup ok");
    assert_eq!(v["users"], json!([]));
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_nested_insert nested_insert_rolls_back_on_child_failure`

Expected: PASS. The mutation returns an error; the follow-up query finds no `rb` user.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert.rs
git commit -m "test: atomic rollback on nested child failure"
```

---

## Task 15: SQL snapshot for the canonical CTE shape

**Files:**
- Modify: `src/sql.rs` — inside `mod tests` block (around line 1485-2000). Append at the end of the mod, just before its closing `}`.

**Context:** Lock in the exact SQL shape for a single parent + single nested child. If the CTE chain ever changes unintentionally, the snapshot diffs. This is the one Phase 2 snapshot in the plan.

- [ ] **Step 1: Append the snapshot test**

```rust
    #[test]
    fn render_insert_with_nested_children() {
        use crate::ast::{InsertObject, MutationField, NestedArrayInsert};
        use crate::schema::Relation;
        use std::collections::BTreeMap;

        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build();

        let mut parent_cols = BTreeMap::new();
        parent_cols.insert("name".into(), serde_json::json!("alice"));

        let mut child_cols = BTreeMap::new();
        child_cols.insert("title".into(), serde_json::json!("p1"));

        let mut nested = BTreeMap::new();
        nested.insert(
            "posts".into(),
            NestedArrayInsert {
                table: "posts".into(),
                rows: vec![InsertObject {
                    columns: child_cols,
                    nested: BTreeMap::new(),
                }],
            },
        );

        let op = Operation::Mutation(vec![MutationField::Insert {
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns: parent_cols,
                nested,
            }],
            on_conflict: None,
            returning: vec![
                Field::Column {
                    physical: "id".into(),
                    alias: "id".into(),
                },
                Field::Relation {
                    name: "posts".into(),
                    alias: "posts".into(),
                    args: QueryArgs::default(),
                    selection: vec![Field::Column {
                        physical: "title".into(),
                        alias: "title".into(),
                    }],
                },
            ],
            one: false,
        }]);

        let (sql, _binds) = render(&op, &schema).unwrap();
        insta::assert_snapshot!(sql);
    }
```

- [ ] **Step 2: Run, review the snapshot, accept it**

Run: `cargo test --lib render_insert_with_nested_children`

Expected on first run: `.snap.new` file created at `src/snapshots/vision_graphql__sql__tests__render_insert_with_nested_children.snap.new`.

Inspect it:

```
cat src/snapshots/vision_graphql__sql__tests__render_insert_with_nested_children.snap.new
```

The rendered SQL must contain these substrings in order (line breaks may differ):
- `m0_input AS (SELECT * FROM (VALUES (1,` — parent input with ordinality
- `AS t(ord, "name")` — alias of the input VALUES
- `m0 AS (INSERT INTO "public"."users" ("name") SELECT "name" FROM m0_input ORDER BY ord` — parent insert
- `m0_ord AS (SELECT *, ROW_NUMBER() OVER () AS ord FROM m0)` — ord CTE
- `m0_posts_input AS (SELECT * FROM (VALUES (1,` — child input (parent_ord = 1 for the single child)
- `AS t(parent_ord, "title")`
- `m0_posts AS (INSERT INTO "public"."posts" ("title", "user_id") SELECT c."title", p."id" FROM m0_posts_input c JOIN m0_ord p ON p.ord = c.parent_ord RETURNING *)`
- `(SELECT count(*) FROM m0) + (SELECT count(*) FROM m0_posts)` — summed affected_rows
- `'posts', (SELECT coalesce(json_agg(row_to_json(` — returning relation subquery
- `FROM m0_posts t0 WHERE t0."user_id" = m0."id"` — returning subquery reads from CTE

If any of those substrings is missing, STOP — something is wrong with the render. Debug before accepting.

If they're all present, accept:

```
cargo insta accept
```

Re-run:

```
cargo test --lib render_insert_with_nested_children
```

Expected: PASS.

- [ ] **Step 3: Run full lib test suite to catch snapshot regressions elsewhere**

Run: `cargo test --lib`

Expected: all 72+ lib tests pass (71 from main + this new one).

- [ ] **Step 4: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "test: snapshot for nested-insert CTE chain"
```

---

## Task 16: README update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a nested-insert subsection**

`grep -n "^##" README.md` to find section headings. After the existing Phase 1 "Mutations" section (added by commit `733e379`), append a new subsection. Locate the right insertion point — find the `## Mutations` heading and add after the existing content within that section but before the next `##` heading.

Add this content:

```markdown
### Nested one-to-many insert

Array relations can be inserted alongside their parent in a single atomic
mutation. The input uses Hasura's `{ data: [...] }` shape so that Phase 3
can add `on_conflict` as a sibling without a breaking change.

```graphql
mutation {
  insert_users(objects: [
    {
      name: "alice",
      posts: { data: [
        { title: "p1" },
        { title: "p2", published: true }
      ]}
    }
  ]) {
    affected_rows          # includes parents + every descendant
    returning {
      id
      name
      posts { title }      # sees freshly-inserted children
    }
  }
}
```

Nesting is arbitrary-depth (e.g. users → posts → comments). Object-relation
nested insert (e.g. `insert_posts(objects: [{ title, user: { data: {...} } }])`)
is not yet supported — use a separate mutation for now.
```

Adjust heading level if the file uses a different convention (check existing `## Mutations` vs `### Nested relations in returning`).

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document Phase 2 nested one-to-many insert"
```

---

## Task 17: Full-suite final verification

**Files:**
- None.

- [ ] **Step 1: Run everything**

Run: `cargo test`

Expected: every test passes. Phase 1 tests (66 integration + 71 lib = 137 before Phase 2) still green, Phase 2 adds:
- 11 integration tests in `integration_nested_insert.rs` (one per Task 1, 4×4, 7, 8, 9, 10, 11, 12, 13, 14 — count them: Task 1 adds 1, Task 4 adds 4, Tasks 7-14 add 1 each = 1+4+8 = 13)
- 1 snapshot test (Task 15)

Total expected: ~137 + 14 = 151 tests.

- [ ] **Step 2: No extra commit — this is a verification gate**

If the suite is green, Phase 2 is done. If anything fails, fix it and amend the task's commit (only if the failing commit is HEAD; otherwise create a follow-up fix commit).

---

## Self-Review

**1. Spec coverage:**
- § Input API (Hasura-shape with `data:` wrapper) → Task 3.
- § Scope semantics: array-only, arbitrary depth, siblings, empty `data: []`, atomic → Tasks 3, 5, 9, 10, 12, 14.
- § Object-relation rejection → Task 4.
- § FK-column-in-child rejection → Task 4.
- § `affected_rows` sums all CTEs → Task 5 Step 4.
- § Returning sees children via CTE read → Task 6.
- § Sibling non-nested relation still reads real table → Task 11.
- § Correlation strategy (ordinality) → Task 5 Step 3 with stress test in Task 8.
- § Testing plan items 1-14 → Tasks 1, 4, 7, 8, 9, 10, 11, 12, 13, 14. Item 4 (validation-missing-data) Task 4. Item 12 (object-relation rejected) Task 4. Items 6 (empty nested array) Task 12, 7 (atomic rollback) Task 14, 8 (insert_one) Task 13, 9 (returning sees children) Task 1, 10 (non-nested sibling) Task 11.
- § SQL snapshot → Task 15.
- § README → Task 16.

**2. Placeholder scan:** One `todo!()` appears in Task 5 Step 2 — this is intentional scaffolding the plan openly documents and Step 3 immediately replaces. Not a placeholder in the plan-failure sense.

**3. Type consistency:**
- `InsertObject { columns, nested }` — used in ast.rs (Task 2), parser.rs (Task 3), sql.rs render (Task 5), snapshot test (Task 15). Consistent.
- `NestedArrayInsert { table, rows }` — defined Task 2, consumed Task 3 parser and Task 5 renderer, tested in snapshot (Task 15). Consistent.
- `inserted_ctes: HashMap<String, String>` — defined Task 5 Step 1, written in Task 5 Step 3, read in Task 5 Step 4 (affected_rows) and Task 6 (relation subquery). Consistent.
- `render_insert_cte_recursive` signature — defined Task 5 Step 3, called from `render_insert_cte` in Task 5 Step 3 and recursively in Task 5 Step 3's `any_nested` block. Consistent.
- Parent-ord CTE naming: `{cte}_ord` used in Task 5 Step 3 (emission) and referenced in recursive call's `Some((&parent_ord_cte_name, rel, table))`. Consistent.
- Child-CTE naming: `{cte}_{rel_name}` used in Task 5 Step 3 and snapshot verification in Task 15 Step 2 (`m0_posts_input`, `m0_posts`). Consistent.

**4. Verification:** Every task has a runnable command with expected output. TDD cycle (RED → green → refactor) is preserved within Tasks 1-6; Tasks 7-14 are additive post-green tests; Task 15 locks shape.

Fix applied: the `todo!()` in Task 5 Step 2 is explicitly flagged with a note telling the executor not to commit that scaffold — Step 3 supersedes.
