# Nested Relation Support in Mutation `returning` / `selection` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow GraphQL mutations (`insert_*`, `insert_*_one`, `update_*`, `update_*_by_pk`, `delete_*`, `delete_*_by_pk`) to select nested relation fields inside their `returning` / selection blocks, the same way SELECT queries already do.

**Architecture:** The SELECT-side parser (`lower_selection_set`) and SQL renderer (`render_relation_field`) already handle nested relations end-to-end. The mutation path today uses a column-only helper (`lower_selection_columns_only`) and a column-only renderer branch in `render_json_build_object_for_nodes`. Fix is two-fold: (1) make the mutation parser call `lower_selection_set` (threading `schema`/`vars`/`fragments` through `lower_mutation_field` and `parse_returning`), and (2) make `render_json_build_object_for_nodes` dispatch `Field::Relation` to the existing `render_relation_field`. Mutation CTE semantics (`FROM m0`) stay unchanged — the relation subquery joins against the CTE alias just like it joins against a table alias in SELECT.

**Tech Stack:** Rust, tokio-postgres, async-graphql-parser, insta snapshots, testcontainers-modules (Postgres).

---

## File Structure

**Modify only — no new files:**

- `src/parser.rs` — thread `fragments` through `lower_mutation_field`; change all 6 mutation selection-parse sites to use `lower_selection_set` (keeps `lower_selection_columns_only` intact for its remaining aggregate-nodes callers).
- `src/sql.rs` — add `Field::Relation` arm to `render_json_build_object_for_nodes` that delegates to `render_relation_field`.
- `tests/integration_mutation.rs` — add nested-returning tests for all 6 mutation variants.
- `src/sql.rs` (its `#[cfg(test)]` module) — add one SQL snapshot test showing the rendered shape.
- `README.md` — one-line mention that mutation returning now supports nested relations.

---

## Task 1: Add failing integration test for nested returning on `insert_*` (array form)

**Files:**
- Modify: `tests/integration_mutation.rs` (append a new `#[tokio::test]`)

**Context:** This file currently has flat-column tests like `insert_array_returns_affected_rows_and_returning` at lines 61-74. The test fixture in this file uses `users(id, name, age)`. We need a second related table (`posts`) so we can exercise a nested relation in `returning`. Check the existing `setup()` helper at the top of `tests/integration_mutation.rs` and the schema builder it uses — if `posts` is not yet present in this file's fixture, add it alongside `users` in the same setup, mirroring the fixture at `tests/integration_nested.rs:9-28`. Reuse the setup used by other mutation tests rather than creating a new fixture.

- [ ] **Step 1: Read current mutation test setup**

Run: `grep -n "fn setup" tests/integration_mutation.rs` and read the returned function (expect ~lines 1-60). Also open `tests/integration_nested.rs:9-28` for the schema pattern that includes `posts`. If the mutation-test setup only has `users`, extend it: add a `posts` table with columns `id INT PK, title TEXT, user_id INT, published BOOL` and the reciprocal relations (`users.posts` array, `posts.user` object), and add a `CREATE TABLE posts` + a seed insert for alice/bob's posts in the container bootstrap SQL. Keep changes minimal — if `posts` already exists, skip schema edits.

- [ ] **Step 2: Write the failing test**

Append to `tests/integration_mutation.rs`:

```rust
#[tokio::test]
async fn insert_array_returning_with_nested_relation() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{name: "dora"}]) {
                   affected_rows
                   returning {
                     id
                     name
                     posts(order_by: [{id: asc}]) { title }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(1));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], json!("dora"));
    // Newly-inserted user has no posts yet — must be an empty array, not null or missing.
    assert_eq!(rows[0]["posts"], json!([]));
}
```

- [ ] **Step 3: Run it and confirm it fails**

Run: `cargo test --test integration_mutation insert_array_returning_with_nested_relation -- --nocapture`

Expected: FAIL with a validation error like `unknown column 'posts' on 'users'` coming from `src/parser.rs:1231-1234` (`lower_selection_columns_only`).

- [ ] **Step 4: Commit**

```bash
git add tests/integration_mutation.rs
git commit -m "test: failing nested returning for insert_*"
```

---

## Task 2: Thread `fragments` into `lower_mutation_field` and `parse_returning`

**Files:**
- Modify: `src/parser.rs:247-253` (`lower_mutation_field` signature)
- Modify: `src/parser.rs` wherever `lower_mutation_field` is called (inside `lower_mutation` near line 241)
- Modify: `src/parser.rs:671-699` (`parse_returning` signature)

**Context:** `lower_mutation_field` today takes `(name, alias, field, schema, vars)`. `lower_selection_set` (parser.rs:701-708) requires `fragments: &Fragments<'_>` as well. `lower_mutation` (parser.rs:202-206) already has `fragments` in scope, so we just plumb it through. `parse_returning` (parser.rs:671) also needs `schema`, `vars`, `fragments`.

- [ ] **Step 1: Update `lower_mutation_field` signature**

Edit `src/parser.rs` at the function starting on line 247. Change:

```rust
fn lower_mutation_field(
    name: &str,
    alias: &str,
    field: &GqlField,
    schema: &Schema,
    vars: &Value,
) -> Result<crate::ast::MutationField> {
```

to:

```rust
fn lower_mutation_field(
    name: &str,
    alias: &str,
    field: &GqlField,
    schema: &Schema,
    vars: &Value,
    fragments: &Fragments<'_>,
) -> Result<crate::ast::MutationField> {
```

- [ ] **Step 2: Update the single caller of `lower_mutation_field`**

Find it in `lower_mutation` (parser.rs ~line 235-245). Grep: `grep -n "lower_mutation_field(" src/parser.rs`. Append `, fragments` to the call so it becomes `lower_mutation_field(name, alias, field, schema, vars, fragments)`.

- [ ] **Step 3: Update `parse_returning` signature**

Edit `src/parser.rs:671-674`. Change:

```rust
fn parse_returning(set: &SelectionSet, table: &Table, parent_path: &str) -> Result<Vec<Field>> {
```

to:

```rust
fn parse_returning(
    set: &SelectionSet,
    table: &Table,
    schema: &Schema,
    vars: &Value,
    fragments: &Fragments<'_>,
    parent_path: &str,
) -> Result<Vec<Field>> {
```

- [ ] **Step 4: Update the 3 callers of `parse_returning`** (insert_*, update_*, delete_*)

At `src/parser.rs:281`, change:

```rust
            let returning = parse_returning(&field.selection_set.node, table, alias)?;
```

to:

```rust
            let returning = parse_returning(&field.selection_set.node, table, schema, vars, fragments, alias)?;
```

Repeat the same substitution at `src/parser.rs:324` and `src/parser.rs:399`.

- [ ] **Step 5: Compile-check**

Run: `cargo check`

Expected: clean build. If `Fragments` is not imported in scope, add `use` or reference by full path — check parser.rs top imports; `Fragments` is used elsewhere in the file so it's already in scope.

- [ ] **Step 6: Commit**

```bash
git add src/parser.rs
git commit -m "refactor: thread schema/vars/fragments through mutation selection parsers"
```

---

## Task 3: Switch mutation selection parsing to `lower_selection_set`

**Files:**
- Modify: `src/parser.rs:684-688` (inside `parse_returning`)
- Modify: `src/parser.rs:262-263` (insert_*_one)
- Modify: `src/parser.rs:307-308` (update_*_by_pk)
- Modify: `src/parser.rs:361-362` (delete_*_by_pk)

**Context:** `lower_selection_columns_only` (parser.rs:1212) rejects relations. `lower_selection_set` (parser.rs:701) handles both columns and relations. The signature is `(set, table, schema, vars, fragments, parent_path)`. We swap the call at all 4 mutation selection sites. `lower_selection_columns_only` remains — it's still used for aggregate-node contexts where relations are genuinely not supported.

- [ ] **Step 1: Replace the call inside `parse_returning`**

At `src/parser.rs:684-688`, change:

```rust
            "returning" => {
                returning = lower_selection_columns_only(
                    &field.selection_set.node,
                    table,
                    &format!("{parent_path}.returning"),
                )?;
            }
```

to:

```rust
            "returning" => {
                returning = lower_selection_set(
                    &field.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
                    &format!("{parent_path}.returning"),
                )?;
            }
```

- [ ] **Step 2: Replace the insert_*_one call site**

At `src/parser.rs:262-263`, change:

```rust
                let returning =
                    lower_selection_columns_only(&field.selection_set.node, table, alias)?;
```

to:

```rust
                let returning = lower_selection_set(
                    &field.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
                    alias,
                )?;
```

- [ ] **Step 3: Replace the update_*_by_pk call site**

At `src/parser.rs:307-308`, change:

```rust
                let selection =
                    lower_selection_columns_only(&field.selection_set.node, table, alias)?;
```

to:

```rust
                let selection = lower_selection_set(
                    &field.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
                    alias,
                )?;
```

- [ ] **Step 4: Replace the delete_*_by_pk call site**

At `src/parser.rs:361-362`, apply the same substitution as Step 3.

- [ ] **Step 5: Compile-check**

Run: `cargo check`

Expected: clean build.

- [ ] **Step 6: Run the Task-1 test — it should STILL fail, now on the render side**

Run: `cargo test --test integration_mutation insert_array_returning_with_nested_relation -- --nocapture`

Expected: FAIL with validation message containing `relations inside aggregate nodes not yet supported` (from `src/sql.rs:1245-1249`). This confirms the parser now accepts the relation and the error moved into the renderer — exactly what we want before Task 4.

- [ ] **Step 7: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): accept nested relation fields in mutation returning/selection"
```

---

## Task 4: Make `render_json_build_object_for_nodes` dispatch `Field::Relation`

**Files:**
- Modify: `src/sql.rs:1220-1255` (`render_json_build_object_for_nodes`)

**Context:** This helper is called by every mutation render arm (insert/insert_one/update/update_by_pk/delete/delete_by_pk). The current `Field::Relation` branch returns a validation error. Replace that branch with a delegation to `render_relation_field` (sql.rs:314), which already handles nested `json_agg` / `row_to_json` wrapping and works against any SQL source alias (the mutation CTE alias `m0` works here exactly like a table alias in SELECT).

The relation-field renderer writes its own `'alias', (subquery)` pair; `render_json_build_object_for_nodes` must NOT also emit the `'alias', ` prefix for the Relation arm. Keep the `Column` arm as-is.

- [ ] **Step 1: Update the `Field::Relation` arm**

In `src/sql.rs`, locate `render_json_build_object_for_nodes` (around line 1220). Replace the current Relation arm:

```rust
            Field::Relation { .. } => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.nodes"),
                    message: "relations inside aggregate nodes not yet supported".into(),
                });
            }
```

with:

```rust
            Field::Relation {
                name,
                alias: rel_alias,
                args,
                selection,
            } => {
                render_relation_field(
                    name,
                    rel_alias,
                    args,
                    selection,
                    table,
                    table_alias,
                    schema,
                    parent_path,
                    ctx,
                )?;
            }
```

- [ ] **Step 2: Verify `render_relation_field`'s output shape matches**

Open `src/sql.rs:314` and confirm that `render_relation_field` emits its own `'<alias>', (SELECT ...)` pair — i.e., includes the JSON key. If it does NOT (e.g. only emits the subquery), prepend the key manually: `write!(ctx.sql, "'{rel_alias}', ").unwrap();` before the call. Read the function body (first ~60 lines starting at 314) to confirm before committing this task.

If `render_relation_field` takes `schema` as a parameter, `render_json_build_object_for_nodes` must also accept `schema` — check its current signature. If it does not already take `schema`, thread it in: change the helper's signature to `fn render_json_build_object_for_nodes(fields: &[Field], table_alias: &str, table: &Table, parent_path: &str, schema: &Schema, ctx: &mut RenderCtx)` and update all 6 call sites in `render_mutation_output_for` (sql.rs:1009-1132) to pass `schema`. The mutation render context already has `schema` in scope (see `src/sql.rs:1009-1013` where it's used for `schema.table(table)`).

- [ ] **Step 3: Compile-check**

Run: `cargo check`

Expected: clean build.

- [ ] **Step 4: Run the Task-1 test — it should now PASS**

Run: `cargo test --test integration_mutation insert_array_returning_with_nested_relation -- --nocapture`

Expected: PASS. `posts` comes back as `[]` for the newly-inserted user.

- [ ] **Step 5: Run the full test suite to catch regressions**

Run: `cargo test`

Expected: everything green. Snapshot tests for existing mutations should still pass since their returning clauses only use `Field::Column`.

- [ ] **Step 6: Commit**

```bash
git add src/sql.rs
git commit -m "feat(sql): render nested relation fields in mutation returning"
```

---

## Task 5: Integration test — nested returning for `insert_*_one`

**Files:**
- Modify: `tests/integration_mutation.rs`

- [ ] **Step 1: Write the test**

```rust
#[tokio::test]
async fn insert_one_returning_with_nested_relation() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users_one(object: {name: "eve"}) {
                   id
                   name
                   posts { title }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let one = &v["insert_users_one"];
    assert_eq!(one["name"], json!("eve"));
    assert_eq!(one["posts"], json!([]));
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_mutation insert_one_returning_with_nested_relation`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_mutation.rs
git commit -m "test: nested returning on insert_*_one"
```

---

## Task 6: Integration test — nested returning for `update_*`

**Files:**
- Modify: `tests/integration_mutation.rs`

**Context:** Alice already has posts from the Task-1 fixture. We update her name and select her existing posts in the returning clause.

- [ ] **Step 1: Write the test**

```rust
#[tokio::test]
async fn update_returning_with_nested_relation() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 update_users(
                   where: {name: {_eq: "alice"}},
                   _set: {name: "alice2"}
                 ) {
                   affected_rows
                   returning {
                     name
                     posts(order_by: [{id: asc}]) { title }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["update_users"]["affected_rows"], json!(1));
    let rows = v["update_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], json!("alice2"));
    let titles: Vec<_> = rows[0]["posts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["title"].clone())
        .collect();
    assert_eq!(titles, vec![json!("a1"), json!("a2")]);
}
```

Note: if `alice`'s seeded posts in the test fixture are titled differently, adjust the expected titles to match the actual seed data. Check the `setup()` SQL seed.

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_mutation update_returning_with_nested_relation`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_mutation.rs
git commit -m "test: nested returning on update_*"
```

---

## Task 7: Integration test — nested selection for `update_*_by_pk`

**Files:**
- Modify: `tests/integration_mutation.rs`

- [ ] **Step 1: Write the test**

```rust
#[tokio::test]
async fn update_by_pk_selection_with_nested_relation() {
    let (engine, _c) = setup().await;

    // Look up alice's id first so the test doesn't depend on a hard-coded PK.
    let v0: Value = engine
        .query(
            r#"query { users(where: {name: {_eq: "alice"}}) { id } }"#,
            None,
        )
        .await
        .expect("lookup ok");
    let alice_id = v0["users"][0]["id"].as_i64().unwrap();

    let mutation = format!(
        r#"mutation {{
             update_users_by_pk(
               pk_columns: {{id: {alice_id}}},
               _set: {{name: "alice3"}}
             ) {{
               id
               name
               posts(order_by: [{{id: asc}}]) {{ title }}
             }}
           }}"#
    );
    let v: Value = engine.query(&mutation, None).await.expect("mutation ok");
    let one = &v["update_users_by_pk"];
    assert_eq!(one["name"], json!("alice3"));
    assert!(one["posts"].as_array().unwrap().len() >= 1);
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_mutation update_by_pk_selection_with_nested_relation`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_mutation.rs
git commit -m "test: nested selection on update_*_by_pk"
```

---

## Task 8: Integration test — nested returning for `delete_*`

**Files:**
- Modify: `tests/integration_mutation.rs`

**Context:** Delete a user that has no posts (insert one first) to avoid FK cascade complications.

- [ ] **Step 1: Write the test**

```rust
#[tokio::test]
async fn delete_returning_with_nested_relation() {
    let (engine, _c) = setup().await;
    // Seed a user with no posts.
    engine
        .query(
            r#"mutation { insert_users_one(object: {name: "tmp"}) { id } }"#,
            None,
        )
        .await
        .expect("seed ok");

    let v: Value = engine
        .query(
            r#"mutation {
                 delete_users(where: {name: {_eq: "tmp"}}) {
                   affected_rows
                   returning {
                     name
                     posts { title }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["delete_users"]["affected_rows"], json!(1));
    let rows = v["delete_users"]["returning"].as_array().unwrap();
    assert_eq!(rows[0]["name"], json!("tmp"));
    assert_eq!(rows[0]["posts"], json!([]));
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_mutation delete_returning_with_nested_relation`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_mutation.rs
git commit -m "test: nested returning on delete_*"
```

---

## Task 9: Integration test — nested selection for `delete_*_by_pk`

**Files:**
- Modify: `tests/integration_mutation.rs`

- [ ] **Step 1: Write the test**

```rust
#[tokio::test]
async fn delete_by_pk_selection_with_nested_relation() {
    let (engine, _c) = setup().await;
    let seeded: Value = engine
        .query(
            r#"mutation { insert_users_one(object: {name: "tmp2"}) { id } }"#,
            None,
        )
        .await
        .expect("seed ok");
    let id = seeded["insert_users_one"]["id"].as_i64().unwrap();

    let mutation = format!(
        r#"mutation {{
             delete_users_by_pk(id: {id}) {{
               name
               posts {{ title }}
             }}
           }}"#
    );
    let v: Value = engine.query(&mutation, None).await.expect("mutation ok");
    let one = &v["delete_users_by_pk"];
    assert_eq!(one["name"], json!("tmp2"));
    assert_eq!(one["posts"], json!([]));
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_mutation delete_by_pk_selection_with_nested_relation`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_mutation.rs
git commit -m "test: nested selection on delete_*_by_pk"
```

---

## Task 10: SQL snapshot test for rendered nested returning

**Files:**
- Modify: `src/sql.rs` (its `#[cfg(test)] mod tests` block; search for `mod tests` or `#[test] fn render_insert_array_with_returning` to find existing snapshot tests)

**Context:** The existing snapshot `vision_graphql__sql__tests__render_insert_array_with_returning.snap` shows the format. Add one new snapshot proving the nested relation renders as a `json_agg(SELECT ...)` subquery inside the top-level `json_build_object`. Keep the fixture schema small: `users` + `posts` with a single array relation.

- [ ] **Step 1: Find an existing snapshot test to mirror**

Run: `grep -n "render_insert_array_with_returning\|#\[test\] fn render_" src/sql.rs`. Open the matching test and study its shape — schema construction, AST construction, the `render_*` call, and the `insta::assert_snapshot!(sql)` call.

- [ ] **Step 2: Add the new snapshot test**

Append inside the same `mod tests` block:

```rust
#[test]
fn render_insert_array_with_nested_relation_returning() {
    let schema = Schema::builder()
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
                .primary_key(&["id"]),
        )
        .build();

    let ast = Operation::Mutation(vec![MutationField::Insert {
        alias: "insert_users".into(),
        table: "users".into(),
        objects: vec![{
            let mut m = std::collections::BTreeMap::new();
            m.insert("name".into(), serde_json::json!("alice"));
            m
        }],
        on_conflict: None,
        returning: vec![
            Field::Column { physical: "id".into(), alias: "id".into() },
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

    let (sql, _params) = render(&ast, &schema).expect("render ok");
    insta::assert_snapshot!(sql);
}
```

The exact imports/types to use (`Operation`, `MutationField`, `QueryArgs`, `Field`, `render`, `Schema`, `Table`, `Relation`, `PgType`) should match what the neighboring snapshot tests import. If the types in this file are named differently, mirror the neighboring test exactly.

- [ ] **Step 3: Run — snapshot will be created on first run**

Run: `cargo test --lib render_insert_array_with_nested_relation_returning`

Expected: insta creates a `.snap.new` file. Review it with `cargo insta review` or by opening the file directly. Confirm the SQL contains:
- `WITH m0 AS (INSERT INTO "public"."users" ... RETURNING *)`
- A top-level `json_build_object('insert_users', ...)`
- Inside `returning`: `json_build_object('id', m0."id", 'posts', (SELECT ... json_agg ... FROM "public"."posts" ... WHERE ... user_id = m0."id"))`

- [ ] **Step 4: Accept the snapshot**

Run: `cargo insta accept` (or rename `.snap.new` to `.snap`).

- [ ] **Step 5: Run again to verify**

Run: `cargo test --lib render_insert_array_with_nested_relation_returning`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "test: snapshot for nested relation in mutation returning"
```

---

## Task 11: README update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Find the mutation example or feature list**

Run: `grep -n -i "mutation\|returning" README.md`. Locate the section that describes mutations.

- [ ] **Step 2: Add a one-line note and a small example**

Append (or insert near the existing mutation example) a paragraph roughly like:

```markdown
#### Nested relations in `returning`

Mutation `returning` / `by_pk` selection blocks support nested relation fields,
the same way SELECT queries do:

```graphql
mutation {
  insert_users(objects: [{ name: "alice" }]) {
    affected_rows
    returning {
      id
      name
      posts(order_by: [{ id: asc }]) { title }
    }
  }
}
```
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: document nested relations in mutation returning"
```

---

## Self-Review Notes

- **Spec coverage:** all 6 mutation variants have dedicated tests (Tasks 1, 5, 6, 7, 8, 9). Both parser and renderer are covered (Tasks 2-4). SQL shape is locked in by snapshot (Task 10). Docs updated (Task 11).
- **No placeholders:** every task contains concrete code. The only discovery step is Task 4 Step 2's check of `render_relation_field`'s output shape — this is unavoidable because the function body wasn't dumped during planning; the plan instructs the executor what to do in each branch of the finding.
- **Type consistency:** `Field` / `MutationField` / `QueryArgs` / `Schema` / `Table` / `Relation` / `PgType` names are used uniformly throughout. `lower_selection_set` signature (`set, table, schema, vars, fragments, parent_path`) is used consistently at every call site in Task 3.
- **Out of scope (deferred to a separate plan):** nested INPUT for insert/update (Hasura-style `posts: { data: [...] }`), `on_conflict` for nested writes, many-to-many nested writes. README clearly frames this phase as returning-side only.
