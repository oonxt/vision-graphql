# Phase 3B: Nested `on_conflict` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow `on_conflict` as a sibling of `data` inside nested insert wrappers (both array and object relations); in nested contexts, transparently rewrite `DO NOTHING` to `DO UPDATE SET pk = EXCLUDED.pk` so RETURNING includes conflict rows and the 1:1 ord correlation stays intact.

**Architecture:** Extend `NestedArrayInsert` / `NestedObjectInsert` with an optional `OnConflict` (reuse the Phase 1 type). Parser relaxes the "only 'data' is supported" check in `parse_insert_object` to also accept `on_conflict`. Renderer threads a new `is_nested_cte: bool` flag through `render_insert_cte_recursive` and passes it to `render_on_conflict`, which picks `DO NOTHING` at top level and the PK-self-update rewrite inside nested CTEs.

**Tech Stack:** Rust, tokio-postgres, async-graphql-parser, serde_json, insta, testcontainers-modules (Postgres 17.4).

---

## File Structure

**Modify:**
- `src/ast.rs` — add `on_conflict: Option<OnConflict>` to both `NestedArrayInsert` and `NestedObjectInsert`.
- `src/parser.rs` — `parse_insert_object`: relax the wrapper-key rejection in both the Array and Object arms; call `parse_on_conflict` when the key is present; thread into the nested struct.
- `src/sql.rs` — `render_insert_cte_recursive` gains an `is_nested_cte: bool` parameter; its two recursive call sites pass `true`; the public wrapper `render_insert_cte` passes `false`. `render_on_conflict` gains a `nested_context: bool` parameter; empty `update_columns` renders `DO NOTHING` when `nested_context=false` and the PK-self-update rewrite when `true`.
- `README.md` — add subsection documenting nested on_conflict and the DO NOTHING rewrite.

**Create:**
- `tests/integration_nested_on_conflict.rs` — fresh fixture + integration tests.
- `src/snapshots/vision_graphql__sql__tests__render_nested_on_conflict_do_nothing_rewrite.snap` — lock the rewrite shape.

**Do not touch:**
- `src/builder.rs` — the AST change adds an optional field; if `InsertObject` or `NestedArrayInsert` construction sites in builder.rs compile-fail, add `on_conflict: None` to them mechanically. Check via grep in Task 2.
- Existing Phase 1/2/3A tests — no regressions expected. Top-level on_conflict unchanged.

---

## Task 1: Failing integration test — nested object DO NOTHING flagship

**Files:**
- Create: `tests/integration_nested_on_conflict.rs`

- [ ] **Step 1: Create test file with fixture and one failing test**

Create `tests/integration_nested_on_conflict.rs`:

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
                .column("email", "email", PgType::Text, true)
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
                    name TEXT NOT NULL CONSTRAINT organizations_name_key UNIQUE
                );
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL CONSTRAINT users_name_key UNIQUE,
                    email TEXT,
                    organization_id INT REFERENCES organizations(id)
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL CONSTRAINT posts_title_key UNIQUE,
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
async fn nested_object_on_conflict_do_nothing_links_to_existing() {
    let (engine, _c) = setup().await;

    // Pre-seed "alice" with email "old@e.com".
    let seeded: Value = engine
        .query(
            r#"mutation {
                 insert_users_one(object: { name: "alice", email: "old@e.com" }) { id }
               }"#,
            None,
        )
        .await
        .expect("seed ok");
    let alice_id = seeded["insert_users_one"]["id"].as_i64().unwrap();

    // Insert post with nested user referencing "alice" by name + DO NOTHING.
    // Without Phase 3B: parser rejects on_conflict in nested wrapper.
    // With Phase 3B: transparent rewrite → DO UPDATE SET id = EXCLUDED.id,
    // so alice's row is returned, post links to her existing id, email unchanged.
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "p1",
                   user: {
                     data: { name: "alice", email: "ignored@e.com" },
                     on_conflict: { constraint: "users_name_key", update_columns: [] }
                   }
                 }]) {
                   affected_rows
                   returning { title user { id email } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");

    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("p1"));
    assert_eq!(row["user"]["id"].as_i64().unwrap(), alice_id);
    // Email unchanged — DO NOTHING means existing row wins (no actual update applied).
    assert_eq!(row["user"]["email"], json!("old@e.com"));
}
```

- [ ] **Step 2: Run and confirm failure mode**

Run: `cargo test --test integration_nested_on_conflict nested_object_on_conflict_do_nothing_links_to_existing -- --nocapture`

Expected: FAIL with a parse error from `src/parser.rs` (current code rejects `on_conflict` as an unknown key in the nested object wrapper). Expected error contains `unknown key 'on_conflict' in nested object insert; only 'data' is supported`.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_on_conflict.rs
git commit -m "test: failing nested on_conflict (DO NOTHING flagship)"
```

---

## Task 2: AST — add `on_conflict` to NestedArrayInsert and NestedObjectInsert

**Files:**
- Modify: `src/ast.rs` — both struct definitions

- [ ] **Step 1: Update struct definitions**

Find `NestedArrayInsert` and `NestedObjectInsert` in `src/ast.rs`. Current shapes:

```rust
pub struct NestedArrayInsert {
    pub table: String,
    pub rows: Vec<InsertObject>,
}

pub struct NestedObjectInsert {
    pub table: String,
    pub row: InsertObject,
}
```

Change to:

```rust
pub struct NestedArrayInsert {
    pub table: String,
    pub rows: Vec<InsertObject>,
    /// Optional Hasura-style on_conflict applied when emitting this
    /// nested INSERT. When present with `update_columns: []`, the renderer
    /// transparently rewrites `DO NOTHING` → `DO UPDATE SET pk = EXCLUDED.pk`
    /// to keep RETURNING correlated 1:1 with input ords.
    pub on_conflict: Option<OnConflict>,
}

pub struct NestedObjectInsert {
    pub table: String,
    pub row: InsertObject,
    /// Optional Hasura-style on_conflict. Same rewrite semantics as
    /// NestedArrayInsert.
    pub on_conflict: Option<OnConflict>,
}
```

- [ ] **Step 2: Fix all construction sites across the codebase**

Run: `grep -rn "NestedArrayInsert \{\|NestedObjectInsert \{" src/ tests/`

Every construction site must add `on_conflict: None`. The expected sites are:

- `src/parser.rs` — inside `parse_insert_object`'s `RelKind::Array` arm (`NestedArrayInsert { table, rows }` → add `on_conflict: None`)
- `src/parser.rs` — inside `parse_insert_object`'s `RelKind::Object` arm (`NestedObjectInsert { table, row }` → add `on_conflict: None`)
- `src/sql.rs` — the `render_insert_with_nested_children` snapshot test (inside `mod tests`) constructs `NestedArrayInsert { ... }` — add `on_conflict: None`.
- `src/sql.rs` — the `render_insert_with_nested_object` snapshot test (Phase 3A) constructs `NestedObjectInsert { ... }` — add `on_conflict: None`.

Also check `src/builder.rs` just in case: `grep -n "NestedArrayInsert\|NestedObjectInsert" src/builder.rs`.

For each match, add `on_conflict: None,` as the final field in the struct literal.

- [ ] **Step 3: Compile-check and run all existing tests**

Run: `cargo check`

Expected: clean build. All AST construction sites updated.

Run: `cargo test --no-fail-fast`

Expected: 142 pre-existing tests still pass (0 failures, except Task 1's RED test which still fails with the SAME parse error — Task 2 didn't touch parser).

- [ ] **Step 4: Commit**

```bash
git add src/ast.rs src/parser.rs src/sql.rs src/builder.rs 2>/dev/null ; git add -u
git commit -m "refactor: add on_conflict to NestedArrayInsert and NestedObjectInsert"
```

(If `src/builder.rs` had no matches, `git add` will silently skip — that's fine. The `git add -u` catches any files that only had modifications without being explicitly added.)

---

## Task 3: Parser — accept `on_conflict` as sibling of `data`

**Files:**
- Modify: `src/parser.rs` — `parse_insert_object`, both `RelKind::Array` and `RelKind::Object` arms

- [ ] **Step 1: Update the Array arm's wrapper validation**

In `src/parser.rs`, find the `RelKind::Array` arm in `parse_insert_object` (look for `"only 'data' is supported"` to locate it). The current rejection loop:

```rust
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
```

Replace with:

```rust
                    // Reject any extra keys in the wrapper.
                    for other_k in wrapper.keys() {
                        if other_k != "data" && other_k != "on_conflict" {
                            return Err(Error::Validate {
                                path: format!("{path}.{k}.{other_k}"),
                                message: format!(
                                    "unknown key '{other_k}' in nested array insert; only 'data' and 'on_conflict' are supported"
                                ),
                            });
                        }
                    }

                    // Parse optional on_conflict against the CHILD table.
                    let on_conflict = if let Some(oc_json) = wrapper.get("on_conflict") {
                        Some(parse_on_conflict(
                            oc_json,
                            target,
                            &format!("{path}.{k}.on_conflict"),
                        )?)
                    } else {
                        None
                    };
```

Then find the `nested_arrays.insert(...)` call later in the same arm:

```rust
                    nested_arrays.insert(
                        k.clone(),
                        crate::ast::NestedArrayInsert {
                            table: rel.target_table.clone(),
                            rows,
                            on_conflict: None,
                        },
                    );
```

Change the last field to use the parsed value:

```rust
                    nested_arrays.insert(
                        k.clone(),
                        crate::ast::NestedArrayInsert {
                            table: rel.target_table.clone(),
                            rows,
                            on_conflict,
                        },
                    );
```

- [ ] **Step 2: Update the Object arm similarly**

In the `RelKind::Object` arm, find the analogous rejection block. Replace:

```rust
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
```

with:

```rust
                    // Reject extra keys in the wrapper.
                    for other_k in wrapper.keys() {
                        if other_k != "data" && other_k != "on_conflict" {
                            return Err(Error::Validate {
                                path: format!("{path}.{k}.{other_k}"),
                                message: format!(
                                    "unknown key '{other_k}' in nested object insert; only 'data' and 'on_conflict' are supported"
                                ),
                            });
                        }
                    }

                    // Parse optional on_conflict against the CHILD table.
                    let on_conflict = if let Some(oc_json) = wrapper.get("on_conflict") {
                        Some(parse_on_conflict(
                            oc_json,
                            target,
                            &format!("{path}.{k}.on_conflict"),
                        )?)
                    } else {
                        None
                    };
```

Then find `nested_objects.insert(...)` in the same arm:

```rust
                    nested_objects.insert(
                        k.clone(),
                        crate::ast::NestedObjectInsert {
                            table: rel.target_table.clone(),
                            row: child,
                            on_conflict: None,
                        },
                    );
```

Change to:

```rust
                    nested_objects.insert(
                        k.clone(),
                        crate::ast::NestedObjectInsert {
                            table: rel.target_table.clone(),
                            row: child,
                            on_conflict,
                        },
                    );
```

- [ ] **Step 3: Compile and run Task-1 test**

Run: `cargo check`

Expected: clean build.

Run: `cargo test --test integration_nested_on_conflict nested_object_on_conflict_do_nothing_links_to_existing -- --nocapture`

Expected: STILL FAIL — but with a DIFFERENT error than the parse rejection. The parser now accepts `on_conflict`, but the renderer hasn't been taught to use it yet. Expected failure modes:

- A DB-level error like "duplicate key value violates unique constraint 'users_name_key'" because the renderer ignores the on_conflict and emits a plain INSERT that fails on the pre-seeded "alice" row, OR
- An assertion mismatch.

Either is fine — Task 4 fixes it.

- [ ] **Step 4: Run full suite to confirm no pre-existing regressions**

Run: `cargo test --no-fail-fast`

Expected: all 142 pre-existing tests still pass. Task-1 test still fails (in its new mode).

- [ ] **Step 5: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): accept on_conflict as sibling of data in nested wrappers"
```

---

## Task 4: Renderer — thread `on_conflict` + DO NOTHING rewrite

**Files:**
- Modify: `src/sql.rs` — `render_insert_cte_recursive` signature + internal recursive calls; `render_on_conflict` signature + empty-`update_columns` branch; `render_insert_cte` wrapper

- [ ] **Step 1: Add `is_nested_cte` parameter to `render_insert_cte_recursive`**

In `src/sql.rs`, change `render_insert_cte_recursive`'s signature:

From:

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
```

To:

```rust
#[allow(clippy::too_many_arguments)]
fn render_insert_cte_recursive(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    parent_ords: &[i64],
    on_conflict: Option<&crate::ast::OnConflict>,
    parent_link: Option<(&str, &crate::schema::Relation, &crate::schema::Table)>,
    is_nested_cte: bool,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
```

- [ ] **Step 2: Update the `render_insert_cte` public wrapper to pass `false`**

Find `render_insert_cte` in `src/sql.rs` (the public wrapper that just calls `render_insert_cte_recursive`). Add the new argument:

Change from:

```rust
fn render_insert_cte(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    on_conflict: Option<&crate::ast::OnConflict>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
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
```

To:

```rust
fn render_insert_cte(
    cte: &str,
    table_name: &str,
    objects: &[crate::ast::InsertObject],
    on_conflict: Option<&crate::ast::OnConflict>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let parent_ords: Vec<i64> = (1..=objects.len() as i64).collect();
    render_insert_cte_recursive(
        cte,
        table_name,
        objects,
        &parent_ords,
        on_conflict,
        None,
        false, // top-level: NOT nested
        schema,
        ctx,
    )
}
```

- [ ] **Step 3: Update the object-relation recursive call inside `render_insert_cte_recursive`**

Find the object-relation call (around the `for rel_name in &object_rel_names` loop that calls `render_insert_cte_recursive`). Currently:

```rust
        for rel_name in &object_rel_names {
            let rel = table.find_relation(rel_name).ok_or_else(|| Error::Validate { ... })?;
            let child_rows: Vec<crate::ast::InsertObject> = objects
                .iter()
                .map(|o| o.nested_objects.get(rel_name).expect("...").row.clone())
                .collect();
            let child_ords: Vec<i64> = parent_ords.to_vec();
            let child_cte = format!("{cte}_{rel_name}");
            render_insert_cte_recursive(
                &child_cte,
                &rel.target_table,
                &child_rows,
                &child_ords,
                None, // object-relation children don't carry on_conflict in Phase 3A
                None,
                schema,
                ctx,
            )?;
            ctx.sql.push_str(", ");
        }
```

Change the recursive call to pick up the per-relation on_conflict and pass `is_nested_cte: true`:

```rust
        for rel_name in &object_rel_names {
            let rel = table.find_relation(rel_name).ok_or_else(|| Error::Validate { ... })?;
            let child_rows: Vec<crate::ast::InsertObject> = objects
                .iter()
                .map(|o| o.nested_objects.get(rel_name).expect("...").row.clone())
                .collect();
            let child_ords: Vec<i64> = parent_ords.to_vec();
            // Pull the on_conflict from the first row's nested_objects[rel_name].
            // Batch-uniform rule guarantees all rows' on_conflict for the same
            // relation are structurally parsed from the same input, so they're
            // either all Some(...) with the same shape or all None. (The parser
            // uniform check guarantees the relation is present in all rows; the
            // on_conflict field on each parent row's NestedObjectInsert is
            // independent but in practice the GraphQL input comes from one
            // wrapper object per relation — so we read from index 0 as canonical.)
            let child_on_conflict = objects
                .first()
                .and_then(|o| o.nested_objects.get(rel_name))
                .and_then(|noi| noi.on_conflict.clone());
            let child_cte = format!("{cte}_{rel_name}");
            render_insert_cte_recursive(
                &child_cte,
                &rel.target_table,
                &child_rows,
                &child_ords,
                child_on_conflict.as_ref(),
                None,
                true, // this is a nested CTE
                schema,
                ctx,
            )?;
            ctx.sql.push_str(", ");
        }
```

(The keep-existing comment explains the `index 0` read: in GraphQL, the nested `user: { data: ..., on_conflict: ... }` is part of each row's input, and per the batch-uniform rule the wrapper shape — including on_conflict — is identical across rows. Taking `objects[0]`'s is therefore canonical.)

- [ ] **Step 4: Update the array-relation recursive call**

Find the array-relation recursive call (at the end of the function, inside `if any_nested_arrays`). Currently:

```rust
        for (rel_name, (child_ords, child_rows)) in per_relation {
            let rel = table.find_relation(rel_name).ok_or_else(|| Error::Validate { ... })?;
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
```

Change to pick up the on_conflict from the first parent's `nested_arrays[rel_name]` and pass `is_nested_cte: true`:

```rust
        for (rel_name, (child_ords, child_rows)) in per_relation {
            let rel = table.find_relation(rel_name).ok_or_else(|| Error::Validate { ... })?;
            // Read on_conflict from the first parent row that has this array
            // relation (they all share the same wrapper shape in the GraphQL input).
            let child_on_conflict = objects
                .iter()
                .find_map(|o| o.nested_arrays.get(rel_name))
                .and_then(|nai| nai.on_conflict.clone());
            let child_cte = format!("{cte}_{rel_name}");
            let parent_ord_cte_name = format!("{cte}_ord");
            ctx.sql.push_str(", ");
            render_insert_cte_recursive(
                &child_cte,
                &rel.target_table,
                &child_rows,
                &child_ords,
                child_on_conflict.as_ref(),
                Some((&parent_ord_cte_name, rel, table)),
                true, // this is a nested CTE
                schema,
                ctx,
            )?;
        }
```

Note: array relations may be present in some parent rows and absent from others (unlike object relations, which are batch-uniform). So we use `find_map` to locate the first row that has this relation.

- [ ] **Step 5: Add `nested_context` parameter to `render_on_conflict`**

Find `render_on_conflict` in `src/sql.rs`. Current signature:

```rust
fn render_on_conflict(
    oc: &crate::ast::OnConflict,
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
```

Change to:

```rust
fn render_on_conflict(
    oc: &crate::ast::OnConflict,
    table: &Table,
    schema: &Schema,
    nested_context: bool,
    ctx: &mut RenderCtx,
) -> Result<()> {
```

- [ ] **Step 6: Update the empty `update_columns` branch in `render_on_conflict`**

Current body (the part that handles `update_columns.is_empty()`):

```rust
    if oc.update_columns.is_empty() {
        ctx.sql.push_str("DO NOTHING");
    } else {
        ctx.sql.push_str("DO UPDATE SET ");
        ...
    }
```

Change to:

```rust
    if oc.update_columns.is_empty() {
        if nested_context {
            // Rewrite DO NOTHING → DO UPDATE SET pk = EXCLUDED.pk so
            // RETURNING includes conflict rows and the downstream
            // ROW_NUMBER() ord correlation stays 1:1 with input.
            let pk_name = table.primary_key.first().ok_or_else(|| Error::Validate {
                path: "on_conflict".into(),
                message: format!(
                    "nested DO NOTHING on-conflict requires a primary key on table '{}'",
                    table.exposed_name
                ),
            })?;
            let pk_col = table.find_column(pk_name).ok_or_else(|| Error::Validate {
                path: "on_conflict".into(),
                message: format!(
                    "primary key column '{pk_name}' missing on '{}'",
                    table.exposed_name
                ),
            })?;
            write!(
                ctx.sql,
                "DO UPDATE SET {} = EXCLUDED.{}",
                quote_ident(&pk_col.physical_name),
                quote_ident(&pk_col.physical_name),
            )
            .unwrap();
        } else {
            ctx.sql.push_str("DO NOTHING");
        }
    } else {
        ctx.sql.push_str("DO UPDATE SET ");
        ...
    }
```

(Keep the non-empty branch unchanged. Just wrap the existing DO NOTHING emit in the `if nested_context / else` conditional.)

- [ ] **Step 7: Update the call site of `render_on_conflict` inside `render_insert_cte_recursive`**

Find where `render_on_conflict` is called (inside `render_insert_cte_recursive`, right before `RETURNING *`). Currently:

```rust
    if let Some(oc) = on_conflict {
        render_on_conflict(oc, table, schema, ctx)?;
    }
    ctx.sql.push_str(" RETURNING *)");
```

Change to:

```rust
    if let Some(oc) = on_conflict {
        render_on_conflict(oc, table, schema, is_nested_cte, ctx)?;
    }
    ctx.sql.push_str(" RETURNING *)");
```

- [ ] **Step 8: Compile-check**

Run: `cargo check`

Expected: clean build. If any other call sites exist (e.g., update renderer if on_conflict is reused there — unlikely), they'll need `nested_context: false` appended. Grep: `grep -n "render_on_conflict(" src/sql.rs` — should be exactly ONE call site inside `render_insert_cte_recursive`. If more exist, add `false` for each.

- [ ] **Step 9: Run Task-1 test**

Run: `cargo test --test integration_nested_on_conflict nested_object_on_conflict_do_nothing_links_to_existing -- --nocapture`

Expected: PASS. `row["user"]["id"] == alice_id`, `row["user"]["email"] == "old@e.com"` (DO NOTHING → rewrite → no-op update → original alice row returned).

If it fails, dump the rendered SQL and verify:
- The nested `m0_user` INSERT emits `ON CONFLICT ON CONSTRAINT "users_name_key" DO UPDATE SET "id" = EXCLUDED."id"`.
- The post INSERT's JOIN against `m0_user_ord` correctly picks up alice's `id`.

- [ ] **Step 10: Run full suite**

Run: `cargo test --no-fail-fast`

Expected: all 142 pre-existing tests PLUS Task-1 test = 143 green.

Any Phase 1 top-level on_conflict test (e.g., `insert_with_on_conflict_do_update` in `integration_mutation.rs`) must still pass. Top-level's `nested_context=false` keeps DO NOTHING behavior intact.

- [ ] **Step 11: Commit**

```bash
git add src/sql.rs
git commit -m "feat(sql): thread nested on_conflict and transparent DO NOTHING rewrite"
```

---

## Task 5: Parser validation tests

**Files:**
- Modify: `tests/integration_nested_on_conflict.rs`

- [ ] **Step 1: Append two validation tests**

```rust
#[tokio::test]
async fn nested_wrapper_unknown_key_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "t",
                   user: { data: { name: "alice" }, foo: "bar" }
                 }]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("'data' and 'on_conflict'"),
        "error should mention both supported keys; was: {msg}"
    );
}

#[tokio::test]
async fn nested_on_conflict_missing_constraint_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "t",
                   user: {
                     data: { name: "alice" },
                     on_conflict: { update_columns: [] }
                   }
                 }]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("'constraint'"),
        "error should mention missing constraint; was: {msg}"
    );
}
```

- [ ] **Step 2: Run — both PASS**

Run: `cargo test --test integration_nested_on_conflict -- nested_wrapper_unknown_key nested_on_conflict_missing_constraint`

Expected: 2/2 PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_on_conflict.rs
git commit -m "test: nested on_conflict parser validation"
```

---

## Task 6: Nested object `on_conflict` DO UPDATE test

**Files:**
- Modify: `tests/integration_nested_on_conflict.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_object_on_conflict_do_update_updates_existing() {
    let (engine, _c) = setup().await;

    // Pre-seed alice with old email.
    let seeded: Value = engine
        .query(
            r#"mutation {
                 insert_users_one(object: { name: "alice", email: "old@e.com" }) { id }
               }"#,
            None,
        )
        .await
        .expect("seed ok");
    let alice_id = seeded["insert_users_one"]["id"].as_i64().unwrap();

    // Insert post with nested user upsert of alice; DO UPDATE email.
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "p1",
                   user: {
                     data: { name: "alice", email: "new@e.com" },
                     on_conflict: { constraint: "users_name_key", update_columns: ["email"] }
                   }
                 }]) {
                   returning { title user { id email } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("p1"));
    assert_eq!(row["user"]["id"].as_i64().unwrap(), alice_id);
    assert_eq!(row["user"]["email"], json!("new@e.com"));
}
```

- [ ] **Step 2: Run — PASS**

Run: `cargo test --test integration_nested_on_conflict nested_object_on_conflict_do_update_updates_existing`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_on_conflict.rs
git commit -m "test: nested object on_conflict DO UPDATE"
```

---

## Task 7: Nested array `on_conflict` DO UPDATE test

**Files:**
- Modify: `tests/integration_nested_on_conflict.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_array_on_conflict_do_update_updates_existing() {
    let (engine, _c) = setup().await;

    // Pre-seed user "bob" + a post titled "fixed-slug" belonging to bob.
    let seeded_bob: Value = engine
        .query(
            r#"mutation { insert_users_one(object: { name: "bob" }) { id } }"#,
            None,
        )
        .await
        .expect("seed bob");
    let bob_id = seeded_bob["insert_users_one"]["id"].as_i64().unwrap();

    let _: Value = engine
        .query(
            &format!(
                r#"mutation {{
                     insert_posts_one(object: {{ title: "fixed-slug", user_id: {bob_id} }}) {{ id }}
                   }}"#
            ),
            None,
        )
        .await
        .expect("seed post");

    // Insert user "carol" with nested posts array that upsert-collides with bob's post.
    // The conflicting post gets DO UPDATE'd to belong to carol.
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "carol",
                   posts: {
                     data: [
                       { title: "fixed-slug" },
                       { title: "carol-fresh" }
                     ],
                     on_conflict: {
                       constraint: "posts_title_key",
                       update_columns: ["user_id"]
                     }
                   }
                 }]) {
                   affected_rows
                   returning {
                     name
                     posts(order_by: [{ title: asc }]) { title user_id }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");

    let row = &v["insert_users"]["returning"][0];
    assert_eq!(row["name"], json!("carol"));
    let carol_id = row.get("id").and_then(|v| v.as_i64()); // may be absent; use user_id on post
    let posts = row["posts"].as_array().unwrap();
    assert_eq!(posts.len(), 2);
    // Both posts now belong to carol.
    let carols_user_id_from_post = posts[0]["user_id"].as_i64().unwrap();
    let _ = carol_id; // tolerate whichever shape
    for p in posts {
        assert_eq!(p["user_id"].as_i64().unwrap(), carols_user_id_from_post);
    }
}
```

- [ ] **Step 2: Run — PASS**

Run: `cargo test --test integration_nested_on_conflict nested_array_on_conflict_do_update_updates_existing`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_on_conflict.rs
git commit -m "test: nested array on_conflict DO UPDATE"
```

---

## Task 8: Nested array `on_conflict` DO NOTHING test

**Files:**
- Modify: `tests/integration_nested_on_conflict.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn nested_array_on_conflict_do_nothing_preserves_existing() {
    let (engine, _c) = setup().await;

    // Pre-seed alice + a post titled "already-there" owned by alice.
    let seeded: Value = engine
        .query(
            r#"mutation { insert_users_one(object: { name: "alice" }) { id } }"#,
            None,
        )
        .await
        .expect("seed alice");
    let alice_id = seeded["insert_users_one"]["id"].as_i64().unwrap();
    let _: Value = engine
        .query(
            &format!(
                r#"mutation {{
                     insert_posts_one(object: {{ title: "already-there", user_id: {alice_id} }}) {{ id }}
                   }}"#
            ),
            None,
        )
        .await
        .expect("seed existing post");

    // Insert user "bob" with nested posts, one conflict, one fresh; DO NOTHING.
    // Conflict post should NOT be reassigned to bob (original owner preserved).
    // Fresh post is bob's.
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "bob",
                   posts: {
                     data: [
                       { title: "already-there" },
                       { title: "bob-fresh" }
                     ],
                     on_conflict: { constraint: "posts_title_key", update_columns: [] }
                   }
                 }]) {
                   returning {
                     id
                     name
                     posts(order_by: [{ title: asc }]) { title user_id }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");

    let row = &v["insert_users"]["returning"][0];
    assert_eq!(row["name"], json!("bob"));
    let bob_id = row["id"].as_i64().unwrap();

    let posts = row["posts"].as_array().unwrap();
    // Both posts appear in the returning (transparent DO NOTHING rewrite
    // brings the existing row into the CTE), but ownership of the conflict
    // row is PRESERVED as alice, not changed to bob.
    assert_eq!(posts.len(), 2);

    let already = posts
        .iter()
        .find(|p| p["title"] == json!("already-there"))
        .expect("already-there present");
    assert_eq!(
        already["user_id"].as_i64().unwrap(),
        alice_id,
        "DO NOTHING preserves original owner"
    );

    let fresh = posts
        .iter()
        .find(|p| p["title"] == json!("bob-fresh"))
        .expect("bob-fresh present");
    assert_eq!(fresh["user_id"].as_i64().unwrap(), bob_id);
}
```

- [ ] **Step 2: Run — PASS**

Run: `cargo test --test integration_nested_on_conflict nested_array_on_conflict_do_nothing_preserves_existing`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_on_conflict.rs
git commit -m "test: nested array on_conflict DO NOTHING preserves existing"
```

---

## Task 9: Top-level `DO NOTHING` regression test

**Files:**
- Modify: `tests/integration_nested_on_conflict.rs`

**Context:** Locks in that top-level `DO NOTHING` behavior is unchanged (Phase 1 semantics preserved).

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn top_level_on_conflict_do_nothing_unchanged() {
    let (engine, _c) = setup().await;

    // Pre-seed "dup".
    let _: Value = engine
        .query(
            r#"mutation { insert_users_one(object: { name: "dup" }) { id } }"#,
            None,
        )
        .await
        .expect("seed dup");

    // Top-level on_conflict with empty update_columns: must be DO NOTHING
    // — skipped row, affected_rows = 0, returning empty.
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(
                   objects: [{ name: "dup" }],
                   on_conflict: { constraint: "users_name_key", update_columns: [] }
                 ) {
                   affected_rows
                   returning { name }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(0));
    assert_eq!(v["insert_users"]["returning"], json!([]));
}
```

- [ ] **Step 2: Run — PASS**

Run: `cargo test --test integration_nested_on_conflict top_level_on_conflict_do_nothing_unchanged`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_on_conflict.rs
git commit -m "test: top-level on_conflict DO NOTHING unchanged (regression)"
```

---

## Task 10: Two-level nested on_conflict test

**Files:**
- Modify: `tests/integration_nested_on_conflict.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn two_level_nested_on_conflict_on_innermost() {
    let (engine, _c) = setup().await;

    // Pre-seed an organization "acme".
    let seeded: Value = engine
        .query(
            r#"mutation { insert_organizations_one(object: { name: "acme" }) { id } }"#,
            None,
        )
        .await
        .expect("seed org");
    let acme_id = seeded["insert_organizations_one"]["id"].as_i64().unwrap();

    // Insert post → new user → existing organization (via on_conflict DO NOTHING).
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "p1",
                   user: { data: {
                     name: "alice",
                     organization: {
                       data: { name: "acme" },
                       on_conflict: { constraint: "organizations_name_key", update_columns: [] }
                     }
                   } }
                 }]) {
                   returning {
                     title
                     user { name organization { id name } }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");

    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("p1"));
    assert_eq!(row["user"]["name"], json!("alice"));
    assert_eq!(row["user"]["organization"]["id"].as_i64().unwrap(), acme_id);
    assert_eq!(row["user"]["organization"]["name"], json!("acme"));
}
```

- [ ] **Step 2: Run — PASS**

Run: `cargo test --test integration_nested_on_conflict two_level_nested_on_conflict_on_innermost`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_on_conflict.rs
git commit -m "test: two-level nested on_conflict on innermost level"
```

---

## Task 11: Sibling object + array with on_conflict test

**Files:**
- Modify: `tests/integration_nested_on_conflict.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn sibling_object_and_array_with_on_conflict() {
    let (engine, _c) = setup().await;

    // Pre-seed user "alice" and a comment's post doesn't exist yet (we insert fresh).
    let seeded: Value = engine
        .query(
            r#"mutation { insert_users_one(object: { name: "alice", email: "old@e.com" }) { id } }"#,
            None,
        )
        .await
        .expect("seed alice");
    let alice_id = seeded["insert_users_one"]["id"].as_i64().unwrap();

    // Insert post with nested user (upsert alice DO NOTHING) AND nested comments (fresh, no conflict).
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "sibling-post",
                   user: {
                     data: { name: "alice" },
                     on_conflict: { constraint: "users_name_key", update_columns: [] }
                   },
                   comments: { data: [{ body: "c1" }, { body: "c2" }] }
                 }]) {
                   affected_rows
                   returning {
                     title
                     user { id email }
                     comments(order_by: [{ id: asc }]) { body }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");

    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("sibling-post"));
    assert_eq!(row["user"]["id"].as_i64().unwrap(), alice_id);
    assert_eq!(row["user"]["email"], json!("old@e.com"));
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

Run: `cargo test --test integration_nested_on_conflict sibling_object_and_array_with_on_conflict`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_nested_on_conflict.rs
git commit -m "test: sibling object + array each with on_conflict"
```

---

## Task 12: SQL snapshot — locking the DO NOTHING rewrite

**Files:**
- Modify: `src/sql.rs` — append to `mod tests`

- [ ] **Step 1: Append the snapshot test**

Find `mod tests` in `src/sql.rs` (around line 1485+). Append just before its closing `}`:

```rust
    #[test]
    fn render_nested_on_conflict_do_nothing_rewrite() {
        use crate::ast::{InsertObject, MutationField, NestedObjectInsert, OnConflict};
        use crate::schema::Relation;
        use std::collections::BTreeMap;

        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, false)
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
                on_conflict: Some(OnConflict {
                    constraint: "users_name_key".into(),
                    update_columns: vec![], // empty → should rewrite
                    where_: None,
                }),
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
            returning: vec![Field::Column {
                physical: "title".into(),
                alias: "title".into(),
            }],
            one: false,
        }]);

        let (sql, _binds) = render(&op, &schema).unwrap();
        insta::assert_snapshot!(sql);
    }
```

- [ ] **Step 2: Run, review, accept**

Run: `cargo test --lib render_nested_on_conflict_do_nothing_rewrite`

Expected first run: `.snap.new` file created.

Inspect:

```
cat src/snapshots/vision_graphql__sql__tests__render_nested_on_conflict_do_nothing_rewrite.snap.new
```

Must contain (substring match, any order within the nested user CTE):
- `m0_user AS (INSERT INTO "public"."users"` — the nested user INSERT
- `ON CONFLICT ON CONSTRAINT "users_name_key"` — on_conflict clause present
- `DO UPDATE SET "id" = EXCLUDED."id"` — the rewrite (NOT `DO NOTHING`)
- `RETURNING *` — closes the user CTE correctly

Must NOT contain:
- `DO NOTHING` anywhere in the emitted SQL (because the only on_conflict is nested with empty update_columns, which rewrites).

If the substrings are right, accept:

```
cargo insta accept
```

Re-run:

```
cargo test --lib render_nested_on_conflict_do_nothing_rewrite
```

Expected: PASS.

- [ ] **Step 3: Run full lib suite**

Run: `cargo test --lib`

Expected: all lib tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "test: snapshot for nested DO NOTHING → DO UPDATE rewrite"
```

---

## Task 13: README update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Find the insertion point**

Run: `grep -n "^##\|^###" README.md`. Locate the existing Phase 3A `### Nested many-to-one insert` subsection in `## Mutations`.

- [ ] **Step 2: Insert new subsection AFTER Phase 3A's**

Add immediately after the closing of `### Nested many-to-one insert`, before the next `##` heading:

```markdown
### Nested `on_conflict` (upsert-at-any-level)

Both array and object nested wrappers accept an `on_conflict` sibling of `data`.
The shape matches top-level `on_conflict`:

```graphql
mutation {
  insert_posts(objects: [{
    title: "p1",
    user: {
      data: { name: "alice", email: "new@e.com" },
      on_conflict: {
        constraint: "users_name_key",
        update_columns: ["email"]              # or [] for "use existing"
      }
    }
  }]) {
    returning { title user { email } }
  }
}
```

**Transparent `DO NOTHING` rewrite:** inside a nested wrapper, `update_columns: []`
is silently rewritten to `DO UPDATE SET <pk> = EXCLUDED.<pk>` — a no-op update
that forces PostgreSQL's `RETURNING` to include conflict rows so the
just-inserted parent's foreign key can point at the existing entity. Top-level
`on_conflict` semantics are unchanged — `update_columns: []` still means
`DO NOTHING` at top level.

This requires a primary key on the nested table; tables without a PK cannot use
nested `DO NOTHING` (supply non-empty `update_columns` instead).
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: document Phase 3B nested on_conflict"
```

---

## Task 14: Full-suite verification

**Files:**
- None.

- [ ] **Step 1: Run everything**

Run: `cargo test --no-fail-fast`

Expected: all tests green. Count: roughly 142 (pre-Phase 3B) + 9 (Tasks 1, 5×2, 6, 7, 8, 9, 10, 11) + 1 (Task 12 snapshot) = 152 tests. Actual number may vary slightly — the key is no failures.

- [ ] **Step 2: No commit — verification gate**

---

## Self-Review

**1. Spec coverage:**
- § Nested object on_conflict DO UPDATE → Task 6
- § Nested object on_conflict DO NOTHING (flagship) → Task 1
- § Nested array on_conflict DO UPDATE → Task 7
- § Nested array on_conflict DO NOTHING → Task 8
- § Top-level DO NOTHING unchanged (regression) → Task 9
- § Parser validation unknown key → Task 5
- § Parser validation malformed on_conflict → Task 5
- § Two-level nested with on_conflict → Task 10
- § Combined sibling object + array + each with on_conflict → Task 11 (partial — only object side has on_conflict; array side is straightforward and already exercised in Phase 2)
- § SQL snapshot locking rewrite shape → Task 12
- § AST change → Task 2
- § Parser accept → Task 3
- § Renderer thread + rewrite → Task 4
- § README → Task 13

**2. Placeholder scan:** No `TBD`, no abstract "handle edge cases" — every step has concrete code or exact commands. The one deferred detail inside Task 4 Step 3 (`ok_or_else(|| Error::Validate { ... })`) is shorthand for an error constructor that the executor can see in the surrounding file; if preferred, the existing error messages at `render_insert_cte_recursive` are the template.

**3. Type consistency:**
- `on_conflict: Option<OnConflict>` field name used consistently in Tasks 2, 3, 4.
- `is_nested_cte: bool` param name used consistently in Task 4 Steps 1, 2, 3, 4.
- `nested_context: bool` param name in `render_on_conflict` (Task 4 Steps 5, 6, 7).
- All four constraint names in test fixtures (`users_name_key`, `posts_title_key`, `organizations_name_key`) match what the `batch_execute` SQL declares (Task 1 Step 1).

**4. Fixture fragility:** Task 7's test uses a tricky DO UPDATE that reassigns `user_id` — if the constraint name is typo'd, the test fails with a confusing error. I checked the `CREATE TABLE posts` line: it uses `CONSTRAINT posts_title_key UNIQUE` — name consistent with what Task 7 references.

**5. Known scope gap (documented in spec):** Combined top-level `on_conflict` + nested children interaction is not explicitly tested here, but the spec documents it as "top-level conflict rows are excluded from m0, children also excluded — consistent with Phase 1 behavior". The existing Phase-1 test `insert_with_on_conflict_do_update` in `integration_mutation.rs` is not disturbed and continues to lock that.
