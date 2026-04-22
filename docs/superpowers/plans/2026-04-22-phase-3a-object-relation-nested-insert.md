# Phase 3A: Object-Relation Nested Insert — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `insert_*` / `insert_*_one` to accept `<relname>: { data: {...} }` nested input for `RelKind::Object` (many-to-one) relations. The engine inserts the referenced entity first, then the parent row with the new entity's PK as FK value — all in one atomic SQL statement.

**Architecture:** AST splits the current `InsertObject.nested` field into `nested_arrays` (Phase 2) + `nested_objects` (new). Parser removes the "object relation not yet supported" error and recurses into `{ data: <object> }`. `parse_insert_args` enforces a batch-uniform rule: all rows in a batch either use a given object relation or none do. Renderer changes `render_insert_cte_recursive` to: (1) always emit `{cte}_ord` (was conditional) so any CTE can be joined against, and (2) emit object-relation CTE chains BEFORE the parent INSERT, threading FK columns into the parent INSERT's SELECT via additional JOINs on the object CTE's ord.

**Tech Stack:** Rust, tokio-postgres, async-graphql-parser, serde_json, insta, testcontainers-modules (Postgres 17.4).

---

## File Structure

**Modify:**
- `src/ast.rs` — rename `InsertObject.nested` → `nested_arrays`; add `nested_objects: BTreeMap<String, NestedObjectInsert>` + `NestedObjectInsert` type.
- `src/parser.rs` — `parse_insert_object` now handles `RelKind::Object` instead of rejecting; `parse_insert_args` enforces batch-uniform rule.
- `src/sql.rs` — `render_insert_cte_recursive` always emits `{cte}_ord`; emits object-relation CTEs before parent INSERT and JOINs them in the INSERT SELECT.
- `src/snapshots/*.snap` — 3 Phase-2 snapshots regenerate because `{cte}_ord` is now always emitted (semantically unchanged: extra CTE is a no-op when unused).
- `README.md` — add subsection on object-relation nested insert.

**Create:**
- `tests/integration_nested_insert_object.rs` — all Phase 3A integration tests with a fresh fixture that adds an `organizations` table (for two-level object nesting).
- `src/snapshots/vision_graphql__sql__tests__render_insert_with_nested_object.snap` — one new snapshot for Phase 3A's canonical CTE shape.

**Do not touch:**
- `src/executor.rs`, `src/engine.rs`, `src/builder.rs` — unaffected by Phase 3A.
- `tests/integration_nested_insert.rs` — existing Phase 2 tests use GraphQL syntax only, no AST field references; unchanged.

---

## Task 1: Failing integration test — single parent with nested object

**Files:**
- Create: `tests/integration_nested_insert_object.rs`

- [ ] **Step 1: Create the test file with fixture and one failing test**

Create `tests/integration_nested_insert_object.rs`:

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
            Table::new("organizations", "public", "organizations")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .primary_key(&["id"]),
        )
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .column("organization_id", "organization_id", PgType::Int4, true)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")]))
                .relation(
                    "organization",
                    Relation::object("organizations").on([("organization_id", "id")]),
                ),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
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
                CREATE TABLE organizations (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL
                );
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    organization_id INT REFERENCES organizations(id)
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id)
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
async fn insert_post_with_nested_user() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [
                   { title: "p1", user: { data: { name: "alice" } } }
                 ]) {
                   affected_rows
                   returning { title user { name } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_posts"]["affected_rows"], json!(2));
    let rows = v["insert_posts"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["title"], json!("p1"));
    assert_eq!(rows[0]["user"]["name"], json!("alice"));
}
```

- [ ] **Step 2: Run and confirm the failure mode**

Run: `cargo test --test integration_nested_insert_object insert_post_with_nested_user -- --nocapture`

Expected: FAIL with a validation error from `src/parser.rs:606-613` (the current "object-relation nested insert for 'user' is not yet supported; use a separate mutation" branch). Exact message should contain `object-relation nested insert for 'user' is not yet supported`.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert_object.rs
git commit -m "test: failing nested object-relation insert"
```

---

## Task 2: Rename `nested` → `nested_arrays`; add `nested_objects` + `NestedObjectInsert`

**Files:**
- Modify: `src/ast.rs:166-185` — struct definitions
- Modify: `src/parser.rs:513, 588-594` — call sites
- Modify: `src/sql.rs:978, 993, 997` — call sites (iteration over `obj.nested`)
- Modify: `src/sql.rs` — 4 test-construction sites: `1914`, `1940`, `2157`, `2211-2228`
- Modify: 3 Phase-2 snapshots will regenerate (see Task 6 notes for why this is safe)

**Context:** Pure type migration — no behavior change in this task. Task 3 will populate `nested_objects`; Task 6 will use it.

- [ ] **Step 1: Update `src/ast.rs`**

Replace the `InsertObject` and `NestedArrayInsert` block at `src/ast.rs:166-185` with:

```rust
/// One row being inserted. Carries regular column values, any nested
/// array-relation inserts (children), and any nested object-relation inserts
/// (a single related entity per parent row).
#[derive(Debug, Clone, Default)]
pub struct InsertObject {
    /// `{ exposed_column -> value }` for this parent row.
    pub columns: std::collections::BTreeMap<String, serde_json::Value>,
    /// Array-relation (one-to-many) nested inserts, keyed by the parent-side
    /// relation name. Each value carries the rows to insert as children of
    /// *this* parent row.
    pub nested_arrays: std::collections::BTreeMap<String, NestedArrayInsert>,
    /// Object-relation (many-to-one) nested inserts, keyed by the parent-side
    /// relation name. Each value carries the single row whose PK becomes the
    /// parent row's FK. The engine inserts this row FIRST, before the parent.
    pub nested_objects: std::collections::BTreeMap<String, NestedObjectInsert>,
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

/// A nested `user: { data: {...} }` block attached to one parent row.
/// Exactly one row — object relations reference exactly one entity.
#[derive(Debug, Clone)]
pub struct NestedObjectInsert {
    /// Target table name (resolved from the parent relation's `target_table`).
    pub table: String,
    /// The row to insert. The engine inserts this BEFORE the parent row
    /// and uses its PK as the parent's FK.
    pub row: InsertObject,
}
```

- [ ] **Step 2: Update `src/parser.rs` references**

At `src/parser.rs:513`, change:

```rust
    let mut nested: BTreeMap<String, crate::ast::NestedArrayInsert> = BTreeMap::new();
```

to:

```rust
    let mut nested_arrays: BTreeMap<String, crate::ast::NestedArrayInsert> = BTreeMap::new();
    let nested_objects: BTreeMap<String, crate::ast::NestedObjectInsert> = BTreeMap::new();
```

(The `let` is `let` not `let mut` for `nested_objects` because Task 2 doesn't populate it; Task 3 will change it to `let mut`.)

At `src/parser.rs:588-594` (the `nested.insert(...)` call inside the Array arm), change:

```rust
                    nested.insert(
                        k.clone(),
                        crate::ast::NestedArrayInsert {
                            table: rel.target_table.clone(),
                            rows,
                        },
                    );
```

to:

```rust
                    nested_arrays.insert(
                        k.clone(),
                        crate::ast::NestedArrayInsert {
                            table: rel.target_table.clone(),
                            rows,
                        },
                    );
```

At the end of the function where the result is constructed, change the final `if columns.is_empty() && nested.is_empty()` check and the `Ok(...)` return. Find this block near `src/parser.rs:615-621`:

```rust
    if columns.is_empty() && nested.is_empty() {
        return Err(Error::Validate {
            path: path.into(),
            message: "insert row must set at least one column or nested relation".into(),
        });
    }

    Ok(crate::ast::InsertObject { columns, nested })
```

Change to:

```rust
    if columns.is_empty() && nested_arrays.is_empty() && nested_objects.is_empty() {
        return Err(Error::Validate {
            path: path.into(),
            message: "insert row must set at least one column or nested relation".into(),
        });
    }

    Ok(crate::ast::InsertObject {
        columns,
        nested_arrays,
        nested_objects,
    })
```

- [ ] **Step 3: Update `src/sql.rs` renderer references**

At `src/sql.rs:978`, change:

```rust
    let any_nested = objects.iter().any(|o| !o.nested.is_empty());
```

to:

```rust
    let any_nested_arrays = objects.iter().any(|o| !o.nested_arrays.is_empty());
```

At `src/sql.rs:981` (inside the same `if any_nested {` block — which becomes `if any_nested_arrays {`), update the `if` condition to use the new variable name.

At `src/sql.rs:993`, change:

```rust
            for (rel_name, nested) in &obj.nested {
```

to:

```rust
            for (rel_name, nested) in &obj.nested_arrays {
```

The inner `&nested.rows` access (line 997) stays the same because `nested` here is the loop variable bound to `NestedArrayInsert` value — field name unchanged.

- [ ] **Step 4: Update `src/sql.rs` snapshot-test construction sites**

At `src/sql.rs:1914` (inside `render_insert_array_with_returning` test), change:

```rust
        let op = Operation::Mutation(vec![MutationField::Insert {
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested: BTreeMap::new(),
            }],
```

to:

```rust
        let op = Operation::Mutation(vec![MutationField::Insert {
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested_arrays: BTreeMap::new(),
                nested_objects: BTreeMap::new(),
            }],
```

At `src/sql.rs:1940` (inside `render_insert_one` test), same rename: `nested: BTreeMap::new()` → `nested_arrays: BTreeMap::new(), nested_objects: BTreeMap::new()`.

At `src/sql.rs:2157` (inside `render_insert_array_with_nested_relation_returning`), same rename.

At `src/sql.rs:2211-2228` (inside `render_insert_with_nested_children`) — the larger block:

```rust
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
```

Change to:

```rust
        let mut nested_arrays = BTreeMap::new();
        nested_arrays.insert(
            "posts".into(),
            NestedArrayInsert {
                table: "posts".into(),
                rows: vec![InsertObject {
                    columns: child_cols,
                    nested_arrays: BTreeMap::new(),
                    nested_objects: BTreeMap::new(),
                }],
            },
        );

        let op = Operation::Mutation(vec![MutationField::Insert {
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns: parent_cols,
                nested_arrays,
                nested_objects: BTreeMap::new(),
            }],
```

- [ ] **Step 5: Compile-check and run tests**

Run: `cargo check`

Expected: clean build. The `let` (non-`mut`) for `nested_objects` in `parse_insert_object` will generate an "unused variable" warning — ignore for now, Task 3 fixes it.

Run: `cargo test`

Expected: all 131 existing tests still pass. Task-1's failing test (`insert_post_with_nested_user`) still fails with the same error (`object-relation nested insert for 'user' is not yet supported`) because Task 2 hasn't changed parser behavior.

No snapshots should regenerate at this task — we only renamed AST field names; the SQL render path is unchanged.

- [ ] **Step 6: Commit**

```bash
git add src/ast.rs src/parser.rs src/sql.rs
git commit -m "refactor: split InsertObject.nested into nested_arrays + nested_objects"
```

---

## Task 3: Parser handles `RelKind::Object` — recurse into `{ data: <object> }`

**Files:**
- Modify: `src/parser.rs:500-622` — `parse_insert_object` function

**Context:** Remove the "not yet supported" branch. Validate `{ data: <single object> }` shape, recurse into the inner object, reject FK-column-also-set conflict.

- [ ] **Step 1: Rewrite the RelKind::Object arm**

In `src/parser.rs`, locate `parse_insert_object`. Change the `let mut nested_arrays` / `let nested_objects` lines (added in Task 2) to both be `let mut`:

```rust
    let mut nested_arrays: BTreeMap<String, crate::ast::NestedArrayInsert> = BTreeMap::new();
    let mut nested_objects: BTreeMap<String, crate::ast::NestedObjectInsert> = BTreeMap::new();
```

Then find the `RelKind::Object => { ... "not yet supported" ... }` branch (near line 606). Replace the entire branch with:

```rust
                crate::schema::RelKind::Object => {
                    let target = schema
                        .table(&rel.target_table)
                        .ok_or_else(|| Error::Validate {
                            path: format!("{path}.{k}"),
                            message: format!(
                                "relation target table '{}' missing",
                                rel.target_table
                            ),
                        })?;

                    // Validate shape: `{ data: <object> }`
                    let wrapper = v.as_object().ok_or_else(|| Error::Validate {
                        path: format!("{path}.{k}"),
                        message: "nested object insert expects object with 'data' key".into(),
                    })?;
                    let data = wrapper.get("data").ok_or_else(|| Error::Validate {
                        path: format!("{path}.{k}"),
                        message: "missing required key 'data' in nested object insert".into(),
                    })?;

                    // `data` must be a single object, not an array.
                    if data.is_array() {
                        return Err(Error::Validate {
                            path: format!("{path}.{k}.data"),
                            message: "object-relation 'data' must be a single object, not an array".into(),
                        });
                    }
                    if !data.is_object() {
                        return Err(Error::Validate {
                            path: format!("{path}.{k}.data"),
                            message: "object-relation 'data' must be an object".into(),
                        });
                    }

                    // Reject extra keys in the wrapper (leaves room for Phase 3B on_conflict).
                    for other_k in wrapper.keys() {
                        if other_k != "data" {
                            return Err(Error::Validate {
                                path: format!("{path}.{k}.{other_k}"),
                                message: format!(
                                    "unknown key '{other_k}' in nested object insert; only 'data' is supported"
                                ),
                            });
                        }
                    }

                    // Reject FK-column-also-set conflict: the parent row must not
                    // specify the mapped FK column when it's also providing nested
                    // object data.
                    for (parent_fk_col, _) in &rel.mapping {
                        if columns.contains_key(parent_fk_col) {
                            return Err(Error::Validate {
                                path: format!("{path}.{k}"),
                                message: format!(
                                    "column '{parent_fk_col}' is populated from the nested object; must not also appear in the parent row"
                                ),
                            });
                        }
                    }

                    // Recurse into the inner object.
                    let child = parse_insert_object(
                        data,
                        target,
                        schema,
                        &format!("{path}.{k}.data"),
                    )?;

                    nested_objects.insert(
                        k.clone(),
                        crate::ast::NestedObjectInsert {
                            table: rel.target_table.clone(),
                            row: child,
                        },
                    );
                    continue;
                }
```

- [ ] **Step 2: Compile and run Task-1's test**

Run: `cargo check`

Expected: clean build. The "unused variable" warning for `nested_objects` is gone now because we write to it.

Run: `cargo test --test integration_nested_insert_object insert_post_with_nested_user -- --nocapture`

Expected: FAIL, but with a DIFFERENT error than before. The parser now accepts the nested input and builds an AST with populated `nested_objects`. The renderer (Task 6) doesn't yet handle it, so the failure will be either:
- A runtime SQL error from PG about missing column `user_id` (the parent row doesn't set it, and the renderer doesn't fill it from the nested object yet), OR
- An assertion mismatch on `affected_rows` or the `returning` payload.

Either is acceptable. Do NOT try to fix the renderer in this task.

- [ ] **Step 3: Confirm Phase 2 parser-error test still passes**

One of the Phase 2 validation tests (`nested_insert_object_relation_rejected`) asserts that object-relation input gets rejected with "object-relation nested insert for 'user' is not yet supported". After Task 3, that test will FAIL because the parser no longer rejects. We need to update it.

Open `tests/integration_nested_insert.rs` and find `nested_insert_object_relation_rejected` (around line 187-203). The Phase 2 test will break. We don't want to lose coverage — the test should be UPDATED to verify a DIFFERENT error path that still makes sense under Phase 3A (e.g., the FK-column-also-set conflict, or mis-shaped `data`), or REMOVED because the behavior it tested is gone.

Decision: DELETE the `nested_insert_object_relation_rejected` test. Phase 3A replaces its premise. We'll add fresh Phase 3A validation tests in Task 5 that cover the new error paths (mixed batch, FK conflict, non-object data).

Delete the entire function `nested_insert_object_relation_rejected` (lines ~187-203 in `tests/integration_nested_insert.rs`).

- [ ] **Step 4: Run full test suite**

Run: `cargo test`

Expected: all Phase 1 and Phase 2 tests still pass (one fewer test total: 130 instead of 131, because we deleted `nested_insert_object_relation_rejected`). Task-1's `insert_post_with_nested_user` still fails — but not as a parse rejection.

- [ ] **Step 5: Commit**

```bash
git add src/parser.rs tests/integration_nested_insert.rs
git commit -m "feat(parser): accept nested object-relation insert input"
```

---

## Task 4: Batch-uniform rule enforcement

**Files:**
- Modify: `src/parser.rs:435-498` — `parse_insert_args`

**Context:** If any row in `objects: [...]` uses `user: { data: {...} }`, all rows must. Mixed nested-vs-direct-FK → parse error. This prevents the SQL renderer from having to handle a mix of nested and direct-FK parent rows — cleaner MVP.

- [ ] **Step 1: Add the uniform check after the objects loop**

In `src/parser.rs`, inside `parse_insert_args`, find the block at the end just before the `if objects.is_empty()` check. Add the uniform check there.

Current tail (approx line 484-495):

```rust
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

Insert the uniform check BEFORE the `Ok` return, after the `is_empty` check:

```rust
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

    // Batch-uniform rule for nested_objects: every row must have the same
    // set of object-relation keys (either all rows nest a given relation,
    // or none do). Mixed is rejected to keep the renderer's JOIN clean.
    if objects.len() > 1 {
        let first_keys: std::collections::BTreeSet<&str> = objects[0]
            .nested_objects
            .keys()
            .map(|s| s.as_str())
            .collect();
        for (i, obj) in objects.iter().enumerate().skip(1) {
            let these: std::collections::BTreeSet<&str> = obj
                .nested_objects
                .keys()
                .map(|s| s.as_str())
                .collect();
            if these != first_keys {
                // Find a specific offender for the error message.
                let missing: Vec<&&str> = first_keys.difference(&these).collect();
                let extra: Vec<&&str> = these.difference(&first_keys).collect();
                let detail = if !missing.is_empty() {
                    format!("row 0 nests '{}' but row {i} does not", missing[0])
                } else {
                    format!("row {i} nests '{}' but row 0 does not", extra[0])
                };
                return Err(Error::Validate {
                    path: format!("{parent_path}.objects"),
                    message: format!(
                        "nested object-relation usage must be uniform across all rows in the batch: {detail}"
                    ),
                });
            }
        }
    }

    Ok((objects, on_conflict))
}
```

- [ ] **Step 2: Compile and run existing tests**

Run: `cargo check`

Expected: clean build.

Run: `cargo test`

Expected: all existing tests still pass. Task-1's test still fails (same reason as Task 3). The uniform check is a no-op for single-row batches and for all-uniform batches.

- [ ] **Step 3: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): enforce batch-uniform rule for nested object relations"
```

---

## Task 5: Parser validation tests for Phase 3A

**Files:**
- Modify: `tests/integration_nested_insert_object.rs`

**Context:** Four tests for the new Phase 3A error paths, mirroring Phase 2's style.

- [ ] **Step 1: Append the four tests**

Append to `tests/integration_nested_insert_object.rs`:

```rust
#[tokio::test]
async fn nested_object_missing_data_key_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{ title: "t", user: {} }]) {
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
async fn nested_object_array_data_is_error() {
    let (engine, _c) = setup().await;
    // object-relation 'data' must be an object, not an array.
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [
                   { title: "t", user: { data: [{ name: "x" }] } }
                 ]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("must be a single object, not an array"),
        "error was: {msg}"
    );
}

#[tokio::test]
async fn nested_object_fk_and_nested_both_set_is_error() {
    let (engine, _c) = setup().await;
    // Can't both set user_id AND provide a nested user.
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [
                   { title: "t", user_id: 99, user: { data: { name: "x" } } }
                 ]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("populated from the nested object"),
        "error was: {msg}"
    );
}

#[tokio::test]
async fn nested_object_mixed_batch_is_error() {
    let (engine, _c) = setup().await;
    // Row 1 uses nested `user`, row 2 uses explicit user_id — rejected.
    // Note: this mutation would also fail the FK constraint since user_id=99
    // doesn't exist, but the parser should reject BEFORE executing any SQL.
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [
                   { title: "p1", user: { data: { name: "alice" } } },
                   { title: "p2", user_id: 99 }
                 ]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("must be uniform"),
        "error was: {msg}"
    );
}
```

- [ ] **Step 2: Run the four tests**

Run: `cargo test --test integration_nested_insert_object -- nested_object_missing nested_object_array nested_object_fk nested_object_mixed`

Expected: 4/4 PASS. Each hits a parser error path added in Task 3 or 4.

Task-1's test (`insert_post_with_nested_user`) still fails — Task 6 is next.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert_object.rs
git commit -m "test: nested object-relation parser validation"
```

---

## Task 6: Renderer emits object-relation CTEs before parent INSERT

**Files:**
- Modify: `src/sql.rs:796-1027` — `render_insert_cte_recursive`

**Context:** The largest task. Three changes:
1. **Always emit `{cte}_ord`** (remove the `any_nested_arrays` guard). Now any CTE can be JOINed against — for array-children (existing) or for object-relation parents (new).
2. **Emit object-relation CTE chains BEFORE the parent INSERT.** Each object relation present in the batch gets its own CTE chain via a recursive call. Naming: `{cte}_{rel_name}` for the object-CTE (e.g., `m0_user`).
3. **Parent INSERT SELECT JOINs against each object-relation ord CTE** to pull FK values. FK columns get added to the parent's INSERT column list.

Also: the `any_nested_arrays` guard stays removed (merged into "always emit `_ord`"), so three Phase-2 snapshots regenerate.

- [ ] **Step 1: Restructure `render_insert_cte_recursive`**

The new structure, from top to bottom:

1. Empty-objects no-op case (unchanged)
2. Collect parent columns (unchanged)
3. **NEW:** For each object-relation present in the batch, recursively emit its CTE chain
4. Emit `{cte}_input` (unchanged)
5. Emit `{cte}` INSERT (modified: includes object-relation FK columns in target list; SELECT source now JOINs against each object-relation ord CTE)
6. Store in `inserted_ctes` (unchanged)
7. **MODIFIED:** Unconditionally emit `{cte}_ord` (was under `if any_nested_arrays`)
8. For each array-relation, recursively emit child chain (unchanged)

Replace the entire `render_insert_cte_recursive` function body with the new version. In `src/sql.rs`, find the function (starts around line 796) and replace its body through line 1026 with this full implementation:

```rust
#[allow(clippy::too_many_arguments)]
fn render_insert_cte_recursive(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    parent_ords: &[i64],
    on_conflict: Option<&crate::ast::OnConflict>,
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
        // can still reference {cte} without type errors.
        write!(
            ctx.sql,
            "{cte} AS (SELECT * FROM {}.{} WHERE FALSE)",
            quote_ident(&table.physical_schema),
            quote_ident(&table.physical_name),
        )
        .unwrap();
        ctx.inserted_ctes.insert(table_name.to_string(), cte.to_string());
        // Also emit a no-op _ord so callers that JOIN against it don't break.
        write!(
            ctx.sql,
            ", {cte}_ord AS (SELECT *, 0::bigint AS ord FROM {cte})"
        )
        .unwrap();
        return Ok(());
    }

    // 1. Collect parent columns.
    let mut col_set: BTreeSet<String> = BTreeSet::new();
    for obj in objects {
        for k in obj.columns.keys() {
            col_set.insert(k.clone());
        }
    }
    let cols: Vec<String> = col_set.into_iter().collect();

    // 2. Emit object-relation CTE chains BEFORE the parent input/insert.
    //    Batch-uniform rule (enforced at parse): if any row has `nested_objects[k]`,
    //    all rows do. Collect the rows and recursively emit each.
    let mut object_rel_names: Vec<String> = Vec::new();
    if let Some(first) = objects.first() {
        for k in first.nested_objects.keys() {
            object_rel_names.push(k.clone());
        }
    }

    for rel_name in &object_rel_names {
        let rel = table.find_relation(rel_name).ok_or_else(|| Error::Validate {
            path: cte.into(),
            message: format!(
                "unknown relation '{rel_name}' on '{}'",
                table.exposed_name
            ),
        })?;
        // Gather the N object-rows (one per parent row), in parent ord order.
        let child_rows: Vec<crate::ast::InsertObject> = objects
            .iter()
            .map(|o| {
                o.nested_objects
                    .get(rel_name)
                    .expect("batch-uniform guarantees presence")
                    .row
                    .clone()
            })
            .collect();
        // Object-relation child uses parent ordinals as its own ordinals (1:1).
        let child_ords: Vec<i64> = parent_ords.to_vec();
        let child_cte = format!("{cte}_{rel_name}");
        render_insert_cte_recursive(
            &child_cte,
            &rel.target_table,
            &child_rows,
            &child_ords,
            None, // object-relation children don't carry on_conflict in Phase 3A
            None, // NOT a child-of-parent; this is a prerequisite insert
            schema,
            ctx,
        )?;
        ctx.sql.push_str(", ");
    }

    // 3. Emit the parent's `{cte}_input` VALUES CTE with ord + column values.
    let input_cte = format!("{cte}_input");
    let ord_col_name = if parent_link.is_some() { "parent_ord" } else { "ord" };

    write!(ctx.sql, "{input_cte} AS (SELECT * FROM (VALUES ").unwrap();
    for (r, obj) in objects.iter().enumerate() {
        if r > 0 {
            ctx.sql.push_str(", ");
        }
        ctx.sql.push('(');
        write!(ctx.sql, "{}", parent_ords[r]).unwrap();
        for exposed in &cols {
            ctx.sql.push_str(", ");
            let col = table
                .find_column(exposed)
                .expect("column should exist — validated at parse");
            let cast = pg_type_cast(&col.pg_type);
            match obj.columns.get(exposed) {
                None => write!(ctx.sql, "NULL::{cast}").unwrap(),
                Some(v) => {
                    let bind = crate::types::json_to_bind(v, &col.pg_type).map_err(|e| {
                        Error::Validate {
                            path: format!("{cte}.objects[{r}].{exposed}"),
                            message: format!("{e}"),
                        }
                    })?;
                    ctx.binds.push(bind);
                    write!(ctx.sql, "${}::{cast}", ctx.binds.len()).unwrap();
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

    // 4. Emit the parent INSERT CTE. Column list = parent columns +
    //    FK columns from parent_link (array-child case) + FK columns
    //    from each object_rel in object_rel_names.
    write!(
        ctx.sql,
        "{cte} AS (INSERT INTO {}.{} (",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();

    let mut first = true;
    for exposed in &cols {
        if !first {
            ctx.sql.push_str(", ");
        }
        first = false;
        let col = table.find_column(exposed).unwrap();
        ctx.sql.push_str(&quote_ident(&col.physical_name));
    }
    // FK columns from parent_link (Phase 2's array-child case).
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
    // FK columns from object relations (Phase 3A).
    for rel_name in &object_rel_names {
        let rel = table.find_relation(rel_name).unwrap();
        for (parent_fk_col, _) in &rel.mapping {
            if !first {
                ctx.sql.push_str(", ");
            }
            first = false;
            let col = table.find_column(parent_fk_col).ok_or_else(|| Error::Validate {
                path: cte.into(),
                message: format!("mapped FK column '{parent_fk_col}' missing on '{}'", table.exposed_name),
            })?;
            ctx.sql.push_str(&quote_ident(&col.physical_name));
        }
    }
    ctx.sql.push(')');

    // SELECT source.
    ctx.sql.push_str(" SELECT ");
    let mut first_sel = true;
    for exposed in &cols {
        if !first_sel {
            ctx.sql.push_str(", ");
        }
        first_sel = false;
        write!(ctx.sql, "c.{}", quote_ident(exposed)).unwrap();
    }
    // FK from parent_link (array-child case).
    if let Some((_, rel, parent_table)) = parent_link {
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
    }
    // FK from each object relation (Phase 3A). Alias for each object-ord join
    // is `o_{rel_name}` — unique per object relation.
    for rel_name in &object_rel_names {
        let rel = table.find_relation(rel_name).unwrap();
        let obj_target = schema.table(&rel.target_table).ok_or_else(|| Error::Validate {
            path: cte.into(),
            message: format!("object-relation target '{}' missing", rel.target_table),
        })?;
        for (_, target_col) in &rel.mapping {
            if !first_sel {
                ctx.sql.push_str(", ");
            }
            first_sel = false;
            let tcol = obj_target.find_column(target_col).ok_or_else(|| Error::Validate {
                path: cte.into(),
                message: format!(
                    "mapped target column '{target_col}' missing on '{}'",
                    obj_target.exposed_name
                ),
            })?;
            write!(
                ctx.sql,
                "o_{rel_name}.{}",
                quote_ident(&tcol.physical_name)
            )
            .unwrap();
        }
    }

    // FROM clause. Base is the input CTE. Add JOINs for parent_link
    // (Phase 2) and each object relation (Phase 3A).
    write!(ctx.sql, " FROM {input_cte} c").unwrap();

    if let Some((parent_ord_cte_alias, _rel, _parent_table)) = parent_link {
        write!(
            ctx.sql,
            " JOIN {parent_ord_cte_alias} p ON p.ord = c.parent_ord"
        )
        .unwrap();
    }

    for rel_name in &object_rel_names {
        let obj_ord_cte = format!("{cte}_{rel_name}_ord");
        write!(
            ctx.sql,
            " JOIN {obj_ord_cte} o_{rel_name} ON o_{rel_name}.ord = c.ord"
        )
        .unwrap();
    }

    if let Some(oc) = on_conflict {
        render_on_conflict(oc, table, schema, ctx)?;
    }
    ctx.sql.push_str(" RETURNING *)");

    // 5. Track this CTE for returning-visibility lookup.
    ctx.inserted_ctes.insert(table_name.to_string(), cte.to_string());

    // 6. Always emit `{cte}_ord` so any consumer (array-children or object-relation
    //    parents) can JOIN against it.
    write!(
        ctx.sql,
        ", {cte}_ord AS (SELECT *, ROW_NUMBER() OVER () AS ord FROM {cte})"
    )
    .unwrap();

    // 7. For each nested array relation, emit the child chain.
    let any_nested_arrays = objects.iter().any(|o| !o.nested_arrays.is_empty());
    if any_nested_arrays {
        use std::collections::BTreeMap;
        let mut per_relation: BTreeMap<&str, (Vec<i64>, Vec<crate::ast::InsertObject>)> =
            BTreeMap::new();

        for (parent_ord_val, obj) in parent_ords.iter().zip(objects.iter()) {
            for (rel_name, nested) in &obj.nested_arrays {
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
            let child_cte = format!("{cte}_{rel_name}");
            let parent_ord_cte_name = format!("{cte}_ord");
            ctx.sql.push_str(", ");
            render_insert_cte_recursive(
                &child_cte,
                &rel.target_table,
                &child_rows,
                &child_ords,
                None,
                Some((&parent_ord_cte_name, rel, table)),
                schema,
                ctx,
            )?;
        }
    }

    Ok(())
}
```

Key observations about this rewrite:
- Object relations render via recursive call with `parent_link = None` (top-level style). They don't JOIN against a parent ord — they ARE the parents, inserted first.
- Parent's INSERT SELECT uses CTE-derived aliases: `c` for the input, `p` for the parent_link ord (Phase 2 case), `o_{rel_name}` for each object-relation ord (Phase 3A case).
- `{cte}_ord` is now emitted unconditionally — removes the Phase 2 `any_nested` guard.
- No-op CTE case (empty `objects`) also emits a dummy `{cte}_ord` so consumers don't break.
- Object-relation cleanup happens naturally: when `m0_user`'s recursive call finishes, it emitted `m0_user_ord`. Then the parent's SELECT JOINs `m0_user_ord o_user ON o_user.ord = c.ord`.

- [ ] **Step 2: Compile-check**

Run: `cargo check`

Expected: clean build. If anything fails, the most likely issues are:
- Missing use statement for `crate::ast::InsertObject` (unlikely — already there)
- Typo on variable names in the loop

Debug and fix.

- [ ] **Step 3: Run Task-1's test**

Run: `cargo test --test integration_nested_insert_object insert_post_with_nested_user -- --nocapture`

Expected: PASS. `affected_rows == 2`, `rows[0]["title"] == "p1"`, `rows[0]["user"]["name"] == "alice"`.

If it fails, debug using the rendered SQL. Enable tracing: `RUST_LOG=debug cargo test --test integration_nested_insert_object insert_post_with_nested_user -- --nocapture 2>&1 | grep "executing"`.

The expected SQL shape (approximately):
```sql
WITH
  m0_user_input AS (SELECT * FROM (VALUES (1, $1::text)) AS t(ord, "name")),
  m0_user AS (INSERT INTO "public"."users" ("name") SELECT "name" FROM m0_user_input ORDER BY ord RETURNING *),
  m0_user_ord AS (SELECT *, ROW_NUMBER() OVER () AS ord FROM m0_user),
  m0_input AS (SELECT * FROM (VALUES (1, $2::text)) AS t(ord, "title")),
  m0 AS (INSERT INTO "public"."posts" ("title", "user_id") SELECT c."title", o_user."id" FROM m0_input c JOIN m0_user_ord o_user ON o_user.ord = c.ord RETURNING *),
  m0_ord AS (SELECT *, ROW_NUMBER() OVER () AS ord FROM m0)
SELECT json_build_object(...)
```

- [ ] **Step 4: Run the full test suite — expect 3 Phase 2 snapshots to regenerate**

Run: `cargo test`

Expected: unit-test failures on 3 snapshots because `{cte}_ord` is now always emitted:
- `render_insert_array_with_returning` — now emits `m0_ord` (previously omitted because no nested children)
- `render_insert_one` — similarly
- `render_insert_array_with_nested_relation_returning` — unchanged (already had `m0_ord` before, since Phase 1 was before `{cte}_ord` existed)

Actually, scrub the third entry from the "regenerate" list. `render_insert_array_with_nested_relation_returning` had no nested insert (the Phase 1 test demonstrates nested returning with non-nested-insert data), so it should also NOW emit the new `m0_ord`. Regen it too.

Run: `cargo test --lib 2>&1 | grep -E "^test .* FAILED|^test result"`

Expected: 3 snapshot tests report FAILED.

Run: `cargo insta review` — inspect each diff. The ONLY change per snapshot should be the addition of a trailing `, m0_ord AS (SELECT *, ROW_NUMBER() OVER () AS ord FROM m0)` CTE. Accept all three:

```
cargo insta accept
```

Re-run full suite:

```
cargo test
```

Expected: all tests pass, including Task-1 test (`insert_post_with_nested_user`). Count: previous 130 (after deleting `nested_insert_object_relation_rejected` in Task 3) + 1 (Task-1 test) + 4 (Task 5 tests) = 135 tests, all green.

- [ ] **Step 5: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "feat(sql): emit object-relation CTEs before parent INSERT with FK JOINs"
```

---

## Task 7: Multi-parent object-relation batch test

**Files:**
- Modify: `tests/integration_nested_insert_object.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn insert_batch_with_nested_users() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [
                   { title: "p1", user: { data: { name: "alice" } } },
                   { title: "p2", user: { data: { name: "bob"   } } }
                 ]) {
                   affected_rows
                   returning { title user { name } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_posts"]["affected_rows"], json!(4));
    let rows = v["insert_posts"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 2);

    let p1 = rows.iter().find(|r| r["title"] == json!("p1")).expect("p1");
    assert_eq!(p1["user"]["name"], json!("alice"));

    let p2 = rows.iter().find(|r| r["title"] == json!("p2")).expect("p2");
    assert_eq!(p2["user"]["name"], json!("bob"));
}
```

- [ ] **Step 2: Run — PASS**

Run: `cargo test --test integration_nested_insert_object insert_batch_with_nested_users`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert_object.rs
git commit -m "test: batch nested object-relation insert"
```

---

## Task 8: Sibling object + array relations on same parent

**Files:**
- Modify: `tests/integration_nested_insert_object.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn insert_post_with_nested_user_and_comments() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "p1",
                   user:     { data: { name: "alice" } },
                   comments: { data: [{ body: "c1" }, { body: "c2" }] }
                 }]) {
                   affected_rows
                   returning {
                     title
                     user { name }
                     comments(order_by: [{ id: asc }]) { body }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_posts"]["affected_rows"], json!(4));
    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("p1"));
    assert_eq!(row["user"]["name"], json!("alice"));
    let bodies: Vec<_> = row["comments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["body"].clone())
        .collect();
    assert_eq!(bodies, vec![json!("c1"), json!("c2")]);
}
```

- [ ] **Step 2: Run — PASS**

Run: `cargo test --test integration_nested_insert_object insert_post_with_nested_user_and_comments`

Expected: PASS. Confirms object (user) emitted before parent, array (comments) after.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert_object.rs
git commit -m "test: sibling object + array nested relations"
```

---

## Task 9: Two-level object nesting — post → user → organization

**Files:**
- Modify: `tests/integration_nested_insert_object.rs`

**Context:** Fixture already has `organizations` table and `users.organization` object relation (defined in Task 1). This test exercises them.

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn insert_post_with_two_level_object_nesting() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "p1",
                   user: { data: {
                     name: "alice",
                     organization: { data: { name: "acme" } }
                   } }
                 }]) {
                   affected_rows
                   returning {
                     title
                     user { name organization { name } }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_posts"]["affected_rows"], json!(3));
    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("p1"));
    assert_eq!(row["user"]["name"], json!("alice"));
    assert_eq!(row["user"]["organization"]["name"], json!("acme"));
}
```

- [ ] **Step 2: Run — PASS**

Run: `cargo test --test integration_nested_insert_object insert_post_with_two_level_object_nesting`

Expected: PASS. Object-relation recursion through two levels (post → user → org).

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert_object.rs
git commit -m "test: two-level object-relation nesting"
```

---

## Task 10: Correlation stress test (5 posts with 5 distinct users)

**Files:**
- Modify: `tests/integration_nested_insert_object.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_object_correlation_stress() {
    let (engine, _c) = setup().await;
    let mutation = r#"mutation {
        insert_posts(objects: [
          { title: "post-a", user: { data: { name: "user-a" } } },
          { title: "post-b", user: { data: { name: "user-b" } } },
          { title: "post-c", user: { data: { name: "user-c" } } },
          { title: "post-d", user: { data: { name: "user-d" } } },
          { title: "post-e", user: { data: { name: "user-e" } } }
        ]) {
          affected_rows
          returning { title user_id }
        }
      }"#;
    let v: Value = engine.query(mutation, None).await.expect("mutation ok");
    assert_eq!(v["insert_posts"]["affected_rows"], json!(10));

    let rows = v["insert_posts"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 5);

    // For each post, round-trip the DB to confirm its user_id points at the
    // correct user by name (e.g., post-a → user-a).
    for r in rows {
        let title = r["title"].as_str().unwrap().to_string();
        let user_id = r["user_id"].as_i64().unwrap();
        let expected_user_name = title.replace("post-", "user-");

        let v2: Value = engine
            .query(
                &format!(
                    r#"query {{ users(where: {{ id: {{_eq: {user_id} }} }}) {{ name }} }}"#
                ),
                None,
            )
            .await
            .expect("lookup ok");
        assert_eq!(
            v2["users"][0]["name"].as_str().unwrap(),
            expected_user_name,
            "post {title} should correlate with user {expected_user_name}"
        );
    }
}
```

- [ ] **Step 2: Run — PASS**

Run: `cargo test --test integration_nested_insert_object nested_object_correlation_stress`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert_object.rs
git commit -m "test: 5-parent correlation stress for nested object insert"
```

---

## Task 11: `insert_*_one` with nested object

**Files:**
- Modify: `tests/integration_nested_insert_object.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn insert_post_one_with_nested_user() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts_one(object: {
                   title: "solo",
                   user: { data: { name: "solo-user" } }
                 }) {
                   title
                   user { name }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let one = &v["insert_posts_one"];
    assert_eq!(one["title"], json!("solo"));
    assert_eq!(one["user"]["name"], json!("solo-user"));
}
```

- [ ] **Step 2: Run — PASS**

Run: `cargo test --test integration_nested_insert_object insert_post_one_with_nested_user`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert_object.rs
git commit -m "test: insert_*_one with nested object"
```

---

## Task 12: Atomic rollback when nested object insert fails

**Files:**
- Modify: `tests/integration_nested_insert_object.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_object_rolls_back_on_parent_failure() {
    let (engine, _c) = setup().await;

    // Violate NOT NULL on users.name (set it null). Whole mutation must fail;
    // neither the post nor any user should persist.
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "rb",
                   user: { data: { name: null } }
                 }]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected DB error");
    let _ = err;

    // Verify no orphan post with title="rb".
    let v: Value = engine
        .query(
            r#"query { posts(where: { title: {_eq: "rb"} }) { id } }"#,
            None,
        )
        .await
        .expect("lookup ok");
    assert_eq!(v["posts"], json!([]));
}
```

- [ ] **Step 2: Run — PASS**

Run: `cargo test --test integration_nested_insert_object nested_object_rolls_back_on_parent_failure`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_insert_object.rs
git commit -m "test: atomic rollback on nested object failure"
```

---

## Task 13: SQL snapshot — canonical Phase 3A CTE chain

**Files:**
- Modify: `src/sql.rs` — append to `mod tests`

- [ ] **Step 1: Append the snapshot test at the end of `mod tests`**

Find `mod tests` in `src/sql.rs` (around line 1485). At the end (just before the closing `}` of the mod), append:

```rust
    #[test]
    fn render_insert_with_nested_object() {
        use crate::ast::{InsertObject, MutationField, NestedObjectInsert};
        use crate::schema::Relation;
        use std::collections::BTreeMap;

        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"]),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .build();

        let mut parent_cols = BTreeMap::new();
        parent_cols.insert("title".into(), serde_json::json!("p1"));

        let mut child_cols = BTreeMap::new();
        child_cols.insert("name".into(), serde_json::json!("alice"));

        let mut nested_objects = BTreeMap::new();
        nested_objects.insert(
            "user".into(),
            NestedObjectInsert {
                table: "users".into(),
                row: InsertObject {
                    columns: child_cols,
                    nested_arrays: BTreeMap::new(),
                    nested_objects: BTreeMap::new(),
                },
            },
        );

        let op = Operation::Mutation(vec![MutationField::Insert {
            alias: "insert_posts".into(),
            table: "posts".into(),
            objects: vec![InsertObject {
                columns: parent_cols,
                nested_arrays: BTreeMap::new(),
                nested_objects,
            }],
            on_conflict: None,
            returning: vec![
                Field::Column {
                    physical: "title".into(),
                    alias: "title".into(),
                },
                Field::Relation {
                    name: "user".into(),
                    alias: "user".into(),
                    args: QueryArgs::default(),
                    selection: vec![Field::Column {
                        physical: "name".into(),
                        alias: "name".into(),
                    }],
                },
            ],
            one: false,
        }]);

        let (sql, _binds) = render(&op, &schema).unwrap();
        insta::assert_snapshot!(sql);
    }
```

- [ ] **Step 2: Run and verify the snapshot**

Run: `cargo test --lib render_insert_with_nested_object`

Expected first run: an `.snap.new` file appears at `src/snapshots/vision_graphql__sql__tests__render_insert_with_nested_object.snap.new`.

Inspect:

```
cat src/snapshots/vision_graphql__sql__tests__render_insert_with_nested_object.snap.new
```

Expected substrings (order matters):
- `m0_user_input AS (SELECT * FROM (VALUES (1, $1::text)) AS t(ord, "name"))`
- `m0_user AS (INSERT INTO "public"."users" ("name") SELECT "name" FROM m0_user_input ORDER BY ord RETURNING *)`
- `m0_user_ord AS (SELECT *, ROW_NUMBER() OVER () AS ord FROM m0_user)`
- `m0_input AS (SELECT * FROM (VALUES (1, $2::text)) AS t(ord, "title"))`
- `m0 AS (INSERT INTO "public"."posts" ("title", "user_id") SELECT c."title", o_user."id" FROM m0_input c JOIN m0_user_ord o_user ON o_user.ord = c.ord RETURNING *)`
- `m0_ord AS (SELECT *, ROW_NUMBER() OVER () AS ord FROM m0)`
- `'affected_rows', ((SELECT count(*) FROM m0_user) + (SELECT count(*) FROM m0))` — sums user + post CTEs
- `'user', (SELECT row_to_json(` — nested returning object-relation
- `FROM m0_user t0 WHERE t0."id" = m0."user_id"` — returning reads from m0_user CTE (via `inserted_ctes` + `current_mutation_cte` from Phase 2)

Accept:

```
cargo insta accept
```

- [ ] **Step 3: Re-run to verify**

Run: `cargo test --lib render_insert_with_nested_object`

Expected: PASS.

Also run the full lib suite to make sure nothing else shifted:

```
cargo test --lib
```

Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "test: snapshot for nested object-relation CTE chain"
```

---

## Task 14: README update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add subsection**

`grep -n "^##\|^###" README.md` — find the existing `## Mutations` section, and the `### Nested one-to-many insert` subsection added in Phase 2.

Insert the new subsection AFTER `### Nested one-to-many insert` and BEFORE the next `##` heading:

```markdown
### Nested many-to-one insert

Object relations can be inserted alongside their parent in the same mutation.
The new entity is inserted first, and its PK is used as the parent's FK:

```graphql
mutation {
  insert_posts(objects: [
    { title: "p1", user: { data: { name: "alice" } } },
    { title: "p2", user: { data: { name: "bob"   } } }
  ]) {
    affected_rows            # 4: 2 users + 2 posts
    returning {
      title
      user { name }          # reads from the freshly-inserted users CTE
    }
  }
}
```

Combines freely with one-to-many nesting — a parent can carry both object and
array children in one row. Object-relation recursion also works arbitrarily
deep (e.g. post → user → organization).

**Batch-uniform constraint:** within a single `objects: [...]`, either every
row uses `<rel>: { data: {...} }` for a given object relation, or no row does.
Mixed usage is rejected; split into two mutation fields instead.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document Phase 3A nested object-relation insert"
```

---

## Task 15: Full-suite verification

**Files:**
- None.

- [ ] **Step 1: Run everything**

```
cargo test
```

Expected counts (approximate):
- 72+ lib tests (71 from Phase 2 + 1 new Phase 3A snapshot)
- Phase 1/2 integration suites unchanged except for the one deleted test (`nested_insert_object_relation_rejected`)
- `integration_nested_insert_object` — 11 tests (Task 1 = 1, Task 5 = 4, Task 7-12 = 6)

Total ~146 tests, all green.

- [ ] **Step 2: No commit — verification gate**

---

## Self-Review

**1. Spec coverage:**
- § Input API (Hasura `{ data: <object> }`) → Task 3.
- § Arbitrary depth → Task 9 (2-level) confirms recursion.
- § Sibling with array relations → Task 8.
- § FK-column-conflict rejected → Task 3 (added to `RelKind::Object` branch), exercised by Task 5's `nested_object_fk_and_nested_both_set_is_error`.
- § Batch-uniform rule → Task 4, exercised by Task 5's `nested_object_mixed_batch_is_error`.
- § `affected_rows` sums object + array + parent → Task 6's affected_rows summation already sums all CTEs matching the umbrella prefix; the 4/5/10 affected-row assertions in Tasks 7-10 confirm.
- § `returning` reads from object CTE → Task 6's parent INSERT JOINs against `m0_user_ord`, and the existing Phase-2 `inserted_ctes` + `current_mutation_cte` filter correctly scope the returning read.
- § Testing plan items 1-12 → Tasks 1, 5 (4 validations), 7, 8, 9, 10, 11, 12.
- § SQL snapshot → Task 13.
- § README → Task 14.
- § No mixed (only uniform) → Task 4 + Task 5's `nested_object_mixed_batch_is_error`.

**2. Placeholder scan:** no `TBD`, no vague "handle edge case", no reference to undefined types. Task 3 uses `NestedObjectInsert { table, row }` which is defined in Task 2.

**3. Type consistency:**
- `NestedObjectInsert { table: String, row: InsertObject }` — defined Task 2 Step 1, constructed Task 3 Step 1, consumed Task 6 Step 1 (`.row.clone()`), constructed in snapshot test Task 13 Step 1.
- `nested_objects: BTreeMap<String, NestedObjectInsert>` — defined Task 2, populated Task 3, read Task 6.
- `nested_arrays: BTreeMap<String, NestedArrayInsert>` — renamed from `nested` in Task 2, used in Task 6's array-children path.
- `{cte}_ord` — emitted unconditionally in Task 6 Step 1; consumed by both array-child JOINs (unchanged Phase 2 behavior) and object-relation parent JOINs (Task 6 new behavior).
- `o_{rel_name}` SELECT alias for object-ord JOIN — appears in Task 6's SELECT source and FROM clauses consistently.

**4. Spec-driven test coverage check:** all 12 spec test items map to Tasks 1, 5 (4 tests), 7, 8, 9, 10, 11, 12 — accounting. Tasks 5 items: `nested_object_missing_data`, `_array_data`, `_fk_and_nested_both_set`, `_mixed_batch` → spec items 11, "not-an-object" variant (spec says `data: []` should fail), 10, 9. Task 5's `_array_data` test covers spec item 11 (`user: { data: [] }` specifically — the plan uses `data: [{...}]` to trip the "single object not array" check; spec's `data: []` empty array variant is also caught by the same error). Leaves spec item 12 (empty object) — not testable via GraphQL syntax without a parse error from the underlying "insert row must set at least one column or nested relation", but that rule's existence is guaranteed by Task 2's `if columns.is_empty() && nested_arrays.is_empty() && nested_objects.is_empty()` check. That check is exercised by existing Phase 2 tests — no new test needed. Note: spec item 12 is covered indirectly; OK.

Fix applied inline: the plan's Task 1 fixture has `organizations` and `users.organization` relation already wired; Task 9's two-level test uses it. No fixture gap.

---
