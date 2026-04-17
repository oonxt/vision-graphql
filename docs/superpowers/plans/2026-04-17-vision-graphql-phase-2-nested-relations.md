# Vision-GraphQL Phase 2 — Nested Relations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add object/array nested selections (`users { posts { title } }`) and EXISTS-based relation filters (`where: { posts: { published: {_eq: true} } }`) to the Phase 1 engine.

**Architecture:** Extend `Schema::Table` with a `Relation` type (object/array, target exposed-table name, column-name mapping). Add `Field::Relation` and `BoolExpr::Relation` IR variants that carry only the relation name (SQL generator resolves target + mapping from schema). SQL generator emits nested `json_agg`/`row_to_json` subqueries for selections and `EXISTS` subqueries for filters. All relation traversal is recursive using the same `render_inner_select` pipeline.

**Tech Stack:** No new deps. Same crates as Phase 1.

**Out of scope (deferred):** `order_by` on cross-table columns (needs `LEFT JOIN` coordination or aggregate subqueries); nested aggregates; computed fields; `via` / through-table relations (Phase 5).

---

## File Structure

All changes in existing files:

```
src/schema.rs   # add RelKind, Relation, Table::relation() builder
src/ast.rs      # extend Field and BoolExpr with Relation variants
src/sql.rs      # extend render pipeline to walk relations and EXISTS
src/parser.rs   # dispatch nested selections and relation filters
tests/integration_nested.rs   # new integration test file
```

---

### Task 1: Schema — Relation types

**Files:**
- Modify: `src/schema.rs`

- [ ] **Step 1: Write failing test**

Append to the `tests` mod at the bottom of `src/schema.rs`:

```rust
    #[test]
    fn build_users_posts_relations() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"])
                    .relation(
                        "posts",
                        Relation::array("posts").on([("id", "user_id")]),
                    ),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"])
                    .relation(
                        "user",
                        Relation::object("users").on([("user_id", "id")]),
                    ),
            )
            .build();

        let users = schema.table("users").unwrap();
        let rel = users.find_relation("posts").unwrap();
        assert_eq!(rel.kind, RelKind::Array);
        assert_eq!(rel.target_table, "posts");
        assert_eq!(rel.mapping, vec![("id".into(), "user_id".into())]);

        let posts = schema.table("posts").unwrap();
        let rel = posts.find_relation("user").unwrap();
        assert_eq!(rel.kind, RelKind::Object);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib schema::tests::build_users_posts_relations`
Expected: compile error — `Relation` / `RelKind` / `find_relation` / `Table::relation` not defined.

- [ ] **Step 3: Add Relation to src/schema.rs**

Immediately after the `Column` struct (before `pub struct Table`), insert:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelKind {
    Object,
    Array,
}

#[derive(Debug, Clone)]
pub struct Relation {
    pub kind: RelKind,
    pub target_table: String,
    /// `(local_exposed_column, remote_exposed_column)` pairs. Join condition is
    /// AND of equalities across all pairs.
    pub mapping: Vec<(String, String)>,
}

impl Relation {
    pub fn object(target: &str) -> RelationBuilder {
        RelationBuilder {
            kind: RelKind::Object,
            target: target.into(),
            mapping: Vec::new(),
        }
    }

    pub fn array(target: &str) -> RelationBuilder {
        RelationBuilder {
            kind: RelKind::Array,
            target: target.into(),
            mapping: Vec::new(),
        }
    }
}

pub struct RelationBuilder {
    kind: RelKind,
    target: String,
    mapping: Vec<(String, String)>,
}

impl RelationBuilder {
    pub fn on<I, A, B>(mut self, pairs: I) -> Relation
    where
        I: IntoIterator<Item = (A, B)>,
        A: Into<String>,
        B: Into<String>,
    {
        self.mapping = pairs
            .into_iter()
            .map(|(a, b)| (a.into(), b.into()))
            .collect();
        Relation {
            kind: self.kind,
            target_table: self.target,
            mapping: self.mapping,
        }
    }
}
```

- [ ] **Step 4: Extend Table to hold relations**

In the existing `pub struct Table`, add one field after `primary_key`:

```rust
    relations_by_name: HashMap<String, Relation>,
```

In `impl Table::new`, add one line after `primary_key: Vec::new(),`:

```rust
            relations_by_name: HashMap::new(),
```

Append a new builder method inside `impl Table`, after `primary_key`:

```rust
    pub fn relation(mut self, name: &str, rel: Relation) -> Self {
        self.relations_by_name.insert(name.into(), rel);
        self
    }

    pub fn find_relation(&self, name: &str) -> Option<&Relation> {
        self.relations_by_name.get(name)
    }
```

- [ ] **Step 5: Run test**

Run: `cargo test --lib schema::tests`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/schema.rs
git commit -m "feat(schema): add Relation with Object/Array kinds and column mapping"
```

---

### Task 2: IR — Field::Relation and BoolExpr::Relation

**Files:**
- Modify: `src/ast.rs`

- [ ] **Step 1: Write failing test**

Append to the existing `tests` mod in `src/ast.rs`:

```rust
    #[test]
    fn build_field_relation() {
        let f = Field::Relation {
            name: "posts".into(),
            alias: "posts".into(),
            args: QueryArgs::default(),
            selection: vec![Field::Column {
                physical: "title".into(),
                alias: "title".into(),
            }],
        };
        match f {
            Field::Relation { name, selection, .. } => {
                assert_eq!(name, "posts");
                assert_eq!(selection.len(), 1);
            }
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn build_bool_expr_relation() {
        use serde_json::json;
        let e = BoolExpr::Relation {
            name: "posts".into(),
            inner: Box::new(BoolExpr::Compare {
                column: "published".into(),
                op: CmpOp::Eq,
                value: json!(true),
            }),
        };
        match e {
            BoolExpr::Relation { name, .. } => assert_eq!(name, "posts"),
            _ => panic!("expected Relation"),
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib ast::tests`
Expected: compile error — `Field::Relation` / `BoolExpr::Relation` missing.

- [ ] **Step 3: Extend Field**

In `src/ast.rs`, replace the existing `pub enum Field` with:

```rust
#[derive(Debug, Clone)]
pub enum Field {
    Column {
        physical: String,
        alias: String,
    },
    Relation {
        /// Name of the relation on the parent table (resolved via schema at render).
        name: String,
        alias: String,
        args: QueryArgs,
        selection: Vec<Field>,
    },
}
```

- [ ] **Step 4: Extend BoolExpr**

In `src/ast.rs`, replace the existing `pub enum BoolExpr` with:

```rust
#[derive(Debug, Clone)]
pub enum BoolExpr {
    And(Vec<BoolExpr>),
    Or(Vec<BoolExpr>),
    Not(Box<BoolExpr>),
    Compare {
        column: String,
        op: CmpOp,
        value: Value,
    },
    /// Match rows where the named relation has at least one matching row.
    Relation {
        name: String,
        inner: Box<BoolExpr>,
    },
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test --lib ast::tests`
Expected: PASS (4 tests).

- [ ] **Step 6: Make the codebase compile again**

Adding new variants to `Field` and `BoolExpr` breaks exhaustive matches in `src/sql.rs`. Add stub arms now; Tasks 3 and 5 replace them with real implementations.

In `src/sql.rs`, locate the `match field` inside `render_inner_select`. Add after the existing `Field::Column { .. }` arm:

```rust
            Field::Relation { .. } => {
                return Err(Error::Validate {
                    path: root.alias.clone(),
                    message: "Field::Relation not yet implemented".into(),
                });
            }
```

In `src/sql.rs`, locate the `match expr` inside `render_bool_expr`. Add after the existing arms:

```rust
        BoolExpr::Relation { .. } => Err(Error::Validate {
            path: "where".into(),
            message: "BoolExpr::Relation not yet implemented".into(),
        }),
```

`src/parser.rs` is unaffected — it constructs `Field` and `BoolExpr` values but does not match on their variants.

- [ ] **Step 7: Run lib tests**

Run: `cargo test --lib`
Expected: PASS — all Phase 1 tests plus the two new ast tests.

- [ ] **Step 8: Commit**

```bash
git add src/ast.rs src/sql.rs
git commit -m "feat(ast): Field::Relation and BoolExpr::Relation variants"
```

---

### Task 3: SQL — render Field::Relation (array)

**Files:**
- Modify: `src/sql.rs`

- [ ] **Step 1: Write failing test**

Append to the `tests` mod in `src/sql.rs`:

```rust
    fn users_posts_schema() -> Schema {
        use crate::schema::Relation;
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .relation(
                        "posts",
                        Relation::array("posts").on([("id", "user_id")]),
                    ),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("user_id", "user_id", PgType::Int4, false),
            )
            .build()
    }

    #[test]
    fn render_array_relation() {
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            kind: RootKind::List,
            args: QueryArgs::default(),
            selection: vec![
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
        }]);
        let (sql, binds) = render(&op, &users_posts_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert!(binds.is_empty());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib sql::tests::render_array_relation`
Expected: FAIL — `Validate { message: "Field::Relation not yet implemented" }`.

- [ ] **Step 3: Implement nested relation rendering**

In `src/sql.rs`, replace the temporary `Field::Relation { .. } => { ... }` arm inside `render_inner_select` with a real call, and add a helper function. Concretely, replace the arm with:

```rust
            Field::Relation {
                name,
                alias,
                args,
                selection,
            } => {
                render_relation_field(
                    name,
                    alias,
                    args,
                    selection,
                    table,
                    table_alias,
                    schema,
                    &root.alias,
                    ctx,
                )?;
            }
```

This requires `render_inner_select` to have `schema: &Schema` in scope. The function currently takes it indirectly through its caller — pass `schema` explicitly.

Walk the call chain. Change `render_inner_select` signature to:

```rust
fn render_inner_select(
    root: &RootField,
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
```

Update its body accordingly (no other changes to existing logic). Update `render_list` to call `render_inner_select(root, table, &inner_alias, schema, ctx)?;` — add `schema` parameter passing. Update `render_root` to pass `schema` to `render_list(root, table, schema, ctx)`.

Actually `render_list` already receives `_schema: &Schema`. Rename that to `schema` and pass it through. The full updated `render_list` is:

```rust
fn render_list(
    root: &RootField,
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let inner_alias = ctx.next_alias("t");
    let row_alias = ctx.next_alias("r");
    ctx.sql.push_str("(SELECT coalesce(json_agg(row_to_json(");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push_str(")), '[]'::json) FROM (");
    render_inner_select(root, table, &inner_alias, schema, ctx)?;
    ctx.sql.push_str(") ");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push(')');
    Ok(())
}
```

Now implement the helper. Append after `render_limit_offset`:

```rust
#[allow(clippy::too_many_arguments)]
fn render_relation_field(
    name: &str,
    alias: &str,
    args: &QueryArgs,
    selection: &[Field],
    parent_table: &Table,
    parent_alias: &str,
    schema: &Schema,
    parent_path: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let rel = parent_table.find_relation(name).ok_or_else(|| Error::Validate {
        path: format!("{parent_path}.{alias}"),
        message: format!(
            "unknown relation '{name}' on '{}'",
            parent_table.exposed_name
        ),
    })?;
    let target = schema.table(&rel.target_table).ok_or_else(|| Error::Validate {
        path: format!("{parent_path}.{alias}"),
        message: format!("relation target table '{}' missing", rel.target_table),
    })?;

    let remote_alias = ctx.next_alias("t");
    let row_alias = ctx.next_alias("r");

    match rel.kind {
        crate::schema::RelKind::Array => {
            ctx.sql.push_str("(SELECT coalesce(json_agg(row_to_json(");
            ctx.sql.push_str(&row_alias);
            ctx.sql.push_str(")), '[]'::json) FROM (");
        }
        crate::schema::RelKind::Object => {
            ctx.sql.push_str("(SELECT row_to_json(");
            ctx.sql.push_str(&row_alias);
            ctx.sql.push_str(") FROM (");
        }
    }

    // Inner SELECT body
    ctx.sql.push_str("SELECT ");
    for (i, field) in selection.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match field {
            Field::Column { physical, alias: fa } => {
                let col = target.find_column(physical).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.{alias}.{fa}"),
                    message: format!(
                        "unknown column '{physical}' on '{}'",
                        target.exposed_name
                    ),
                })?;
                write!(
                    ctx.sql,
                    r#"{remote_alias}.{} AS "{}""#,
                    quote_ident(&col.physical_name),
                    fa
                )
                .unwrap();
            }
            Field::Relation {
                name: cname,
                alias: ca,
                args: cargs,
                selection: csel,
            } => {
                render_relation_field(
                    cname,
                    ca,
                    cargs,
                    csel,
                    target,
                    &remote_alias,
                    schema,
                    &format!("{parent_path}.{alias}"),
                    ctx,
                )?;
            }
        }
    }
    write!(
        ctx.sql,
        " FROM {}.{} {remote_alias}",
        quote_ident(&target.physical_schema),
        quote_ident(&target.physical_name),
    )
    .unwrap();

    // WHERE: join condition plus user-supplied filter
    ctx.sql.push_str(" WHERE ");
    for (i, (local_col, remote_col)) in rel.mapping.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(" AND ");
        }
        let l = parent_table.find_column(local_col).ok_or_else(|| Error::Validate {
            path: format!("{parent_path}.{alias}"),
            message: format!(
                "relation mapping: unknown local column '{local_col}' on '{}'",
                parent_table.exposed_name
            ),
        })?;
        let r = target.find_column(remote_col).ok_or_else(|| Error::Validate {
            path: format!("{parent_path}.{alias}"),
            message: format!(
                "relation mapping: unknown remote column '{remote_col}' on '{}'",
                target.exposed_name
            ),
        })?;
        write!(
            ctx.sql,
            "{remote_alias}.{} = {parent_alias}.{}",
            quote_ident(&r.physical_name),
            quote_ident(&l.physical_name),
        )
        .unwrap();
    }
    if let Some(expr) = args.where_.as_ref() {
        ctx.sql.push_str(" AND ");
        render_bool_expr(expr, target, &remote_alias, schema, ctx)?;
    }

    // ORDER BY (Task 4 wires relation order_by; for now user columns only).
    if !args.order_by.is_empty() {
        ctx.sql.push_str(" ORDER BY ");
        for (i, ob) in args.order_by.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(", ");
            }
            let col = target
                .find_column(&ob.column)
                .ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.{alias}.order_by.{}", ob.column),
                    message: format!(
                        "unknown column '{}' on '{}'",
                        ob.column, target.exposed_name
                    ),
                })?;
            let dir = match ob.direction {
                crate::ast::OrderDir::Asc => "ASC",
                crate::ast::OrderDir::Desc => "DESC",
            };
            write!(
                ctx.sql,
                "{remote_alias}.{} {dir}",
                quote_ident(&col.physical_name),
            )
            .unwrap();
        }
    }

    if let Some(n) = args.limit {
        write!(ctx.sql, " LIMIT {n}").unwrap();
    } else if matches!(rel.kind, crate::schema::RelKind::Object) {
        ctx.sql.push_str(" LIMIT 1");
    }
    if let Some(n) = args.offset {
        write!(ctx.sql, " OFFSET {n}").unwrap();
    }

    ctx.sql.push_str(") ");
    ctx.sql.push_str(&row_alias);
    write!(ctx.sql, r#") AS "{alias}""#).unwrap();

    Ok(())
}
```

This helper uses `render_bool_expr(expr, target, &remote_alias, schema, ctx)?;` — the `render_bool_expr` signature currently does not take `schema`. Change its signature now:

```rust
fn render_bool_expr(
    expr: &crate::ast::BoolExpr,
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()>
```

Do the same for `render_bool_list`:

```rust
fn render_bool_list(
    parts: &[crate::ast::BoolExpr],
    joiner: &str,
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()>
```

Recursive calls inside `render_bool_expr`/`render_bool_list` pass `schema` through.

Update `render_where` similarly:

```rust
fn render_where(
    args: &QueryArgs,
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let Some(expr) = args.where_.as_ref() else {
        return Ok(());
    };
    ctx.sql.push_str(" WHERE ");
    render_bool_expr(expr, table, table_alias, schema, ctx)?;
    Ok(())
}
```

And update the call in `render_inner_select`: `render_where(&root.args, table, table_alias, schema, ctx)?;`.

The leftover `BoolExpr::Relation { .. } =>` arm in `render_bool_expr` stays as the "not yet implemented" error stub — Task 5 will replace it.

Remove the temporary `Field::Relation { .. } => { return Err(...); }` arm added in Task 2 — it's now replaced by the real implementation.

- [ ] **Step 4: Accept snapshot**

Run: `INSTA_UPDATE=always cargo test --lib sql::tests`
Expected: all snapshots update, new `render_array_relation.snap` approved.

Inspect: `cat src/snapshots/vision_graphql__sql__tests__render_array_relation.snap`

Expected SQL skeleton:
```
SELECT json_build_object('users', (SELECT coalesce(json_agg(row_to_json(r1)), '[]'::json) FROM (SELECT t0."id" AS "id", (SELECT coalesce(json_agg(row_to_json(r3)), '[]'::json) FROM (SELECT t2."title" AS "title" FROM "public"."posts" t2 WHERE t2."user_id" = t0."id") r3) AS "posts" FROM "public"."users" t0) r1)) AS result
```

- [ ] **Step 5: Re-run all lib tests**

Run: `cargo test --lib`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "feat(sql): render array relation fields with json_agg subquery"
```

---

### Task 4: SQL — render Field::Relation (object)

The general helper from Task 3 already branches on `RelKind::Object` — this task only adds a snapshot test to lock in object semantics and confirm the `LIMIT 1` default.

**Files:**
- Modify: `src/sql.rs`

- [ ] **Step 1: Add test**

Append to the `tests` mod in `src/sql.rs`:

```rust
    #[test]
    fn render_object_relation() {
        let op = Operation::Query(vec![RootField {
            table: "posts".into(),
            alias: "posts".into(),
            kind: RootKind::List,
            args: QueryArgs::default(),
            selection: vec![
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
        }]);
        let (sql, _binds) = render(&op, &users_posts_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }
```

- [ ] **Step 2: Extend schema helper**

The `users_posts_schema` helper in the tests mod only defined the `users.posts` relation. Add the reverse. Replace the helper with:

```rust
    fn users_posts_schema() -> Schema {
        use crate::schema::Relation;
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .relation(
                        "posts",
                        Relation::array("posts").on([("id", "user_id")]),
                    ),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .relation(
                        "user",
                        Relation::object("users").on([("user_id", "id")]),
                    ),
            )
            .build()
    }
```

- [ ] **Step 3: Accept new snapshot**

Run: `INSTA_UPDATE=always cargo test --lib sql::tests::render_object_relation`
Expected: new snapshot approved.

Inspect: `cat src/snapshots/vision_graphql__sql__tests__render_object_relation.snap`

Expected contains: `(SELECT row_to_json(...) FROM (... LIMIT 1) ...) AS "user"`.

- [ ] **Step 4: Run all lib tests**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "test(sql): snapshot for object relation with LIMIT 1"
```

---

### Task 5: SQL — render BoolExpr::Relation (EXISTS)

**Files:**
- Modify: `src/sql.rs`

- [ ] **Step 1: Write failing test**

Append to the `tests` mod in `src/sql.rs`:

```rust
    #[test]
    fn render_where_relation_exists() {
        use crate::ast::{BoolExpr, CmpOp};
        use serde_json::json;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            kind: RootKind::List,
            args: QueryArgs {
                where_: Some(BoolExpr::Relation {
                    name: "posts".into(),
                    inner: Box::new(BoolExpr::Compare {
                        column: "title".into(),
                        op: CmpOp::Eq,
                        value: json!("hello"),
                    }),
                }),
                ..Default::default()
            },
            selection: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
        }]);
        let (sql, binds) = render(&op, &users_posts_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib sql::tests::render_where_relation_exists`
Expected: FAIL with "BoolExpr::Relation not yet implemented".

- [ ] **Step 3: Implement EXISTS rendering**

In `src/sql.rs`, find the `BoolExpr::Relation { .. }` arm inside `render_bool_expr` (the one added in Task 2 as a stub) and replace it with:

```rust
        BoolExpr::Relation { name, inner } => {
            let rel = table.find_relation(name).ok_or_else(|| Error::Validate {
                path: format!("where.{name}"),
                message: format!("unknown relation '{name}' on '{}'", table.exposed_name),
            })?;
            let target = schema.table(&rel.target_table).ok_or_else(|| Error::Validate {
                path: format!("where.{name}"),
                message: format!("relation target table '{}' missing", rel.target_table),
            })?;
            let remote_alias = ctx.next_alias("e");
            ctx.sql.push_str("EXISTS (SELECT 1 FROM ");
            write!(
                ctx.sql,
                "{}.{} {remote_alias}",
                quote_ident(&target.physical_schema),
                quote_ident(&target.physical_name),
            )
            .unwrap();
            ctx.sql.push_str(" WHERE ");
            for (i, (local_col, remote_col)) in rel.mapping.iter().enumerate() {
                if i > 0 {
                    ctx.sql.push_str(" AND ");
                }
                let l = table.find_column(local_col).ok_or_else(|| Error::Validate {
                    path: format!("where.{name}"),
                    message: format!(
                        "relation mapping: unknown local column '{local_col}'"
                    ),
                })?;
                let r = target.find_column(remote_col).ok_or_else(|| Error::Validate {
                    path: format!("where.{name}"),
                    message: format!(
                        "relation mapping: unknown remote column '{remote_col}'"
                    ),
                })?;
                write!(
                    ctx.sql,
                    "{remote_alias}.{} = {table_alias}.{}",
                    quote_ident(&r.physical_name),
                    quote_ident(&l.physical_name),
                )
                .unwrap();
            }
            ctx.sql.push_str(" AND ");
            render_bool_expr(inner, target, &remote_alias, schema, ctx)?;
            ctx.sql.push(')');
            Ok(())
        }
```

- [ ] **Step 4: Accept snapshot**

Run: `INSTA_UPDATE=always cargo test --lib sql::tests::render_where_relation_exists`
Expected: snapshot accepted.

Inspect: `cat src/snapshots/vision_graphql__sql__tests__render_where_relation_exists.snap`

Expected contains: `EXISTS (SELECT 1 FROM "public"."posts" e2 WHERE e2."user_id" = t0."id" AND e2."title" = $1)`.

- [ ] **Step 5: Full lib suite**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "feat(sql): EXISTS subquery rendering for BoolExpr::Relation"
```

---

### Task 6: Parser — dispatch nested selections

**Files:**
- Modify: `src/parser.rs`

- [ ] **Step 1: Write failing test**

Append to the `tests` mod in `src/parser.rs`. First extend the helper:

```rust
    fn schema_with_relations() -> Schema {
        use crate::schema::Relation;
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .relation(
                        "posts",
                        Relation::array("posts").on([("id", "user_id")]),
                    ),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .relation(
                        "user",
                        Relation::object("users").on([("user_id", "id")]),
                    ),
            )
            .build()
    }

    #[test]
    fn parse_nested_array_relation() {
        let op = parse_and_lower(
            "query { users { id posts(limit: 3) { title } } }",
            &json!({}),
            None,
            &schema_with_relations(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        assert_eq!(roots[0].selection.len(), 2);
        match &roots[0].selection[1] {
            Field::Relation { name, args, selection, .. } => {
                assert_eq!(name, "posts");
                assert_eq!(args.limit, Some(3));
                assert_eq!(selection.len(), 1);
            }
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn parse_nested_object_relation() {
        let op = parse_and_lower(
            "query { posts { title user { name } } }",
            &json!({}),
            None,
            &schema_with_relations(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match &roots[0].selection[1] {
            Field::Relation { name, .. } => assert_eq!(name, "user"),
            _ => panic!("expected Relation"),
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib parser::tests::parse_nested_array_relation`
Expected: FAIL — field treated as column.

- [ ] **Step 3: Dispatch in lower_selection_set**

In `src/parser.rs`, replace `lower_selection_set` entirely with:

```rust
fn lower_selection_set(
    set: &SelectionSet,
    table: &Table,
    schema: &Schema,
    vars: &Value,
    parent_path: &str,
) -> Result<Vec<Field>> {
    let mut out = Vec::new();
    for sel in &set.items {
        match &sel.node {
            Selection::Field(f) => {
                let field = &f.node;
                let name = field.name.node.as_str();
                let alias = field
                    .alias
                    .as_ref()
                    .map(|a| a.node.as_str().to_string())
                    .unwrap_or_else(|| name.to_string());

                if let Some(rel) = table.find_relation(name) {
                    let target = schema.table(&rel.target_table).ok_or_else(|| {
                        Error::Validate {
                            path: format!("{parent_path}.{alias}"),
                            message: format!(
                                "relation target table '{}' missing",
                                rel.target_table
                            ),
                        }
                    })?;
                    let args = lower_args(
                        &field.arguments,
                        target,
                        schema,
                        vars,
                        &format!("{parent_path}.{alias}"),
                    )?;
                    let selection = lower_selection_set(
                        &field.selection_set.node,
                        target,
                        schema,
                        vars,
                        &format!("{parent_path}.{alias}"),
                    )?;
                    out.push(Field::Relation {
                        name: name.to_string(),
                        alias,
                        args,
                        selection,
                    });
                    continue;
                }

                let col = table.find_column(name).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.{alias}"),
                    message: format!("unknown column '{name}' on '{}'", table.exposed_name),
                })?;
                out.push(Field::Column {
                    physical: col.physical_name.clone(),
                    alias,
                });
            }
            _ => {
                return Err(Error::Parse(
                    "fragments are not supported in Phase 2".into(),
                ))
            }
        }
    }
    Ok(out)
}
```

The signature now takes `schema` and `vars`. Update callers:

In `lower_query`, change the call to:

```rust
                let selection = lower_selection_set(
                    &field.selection_set.node,
                    table,
                    schema,
                    vars,
                    &alias,
                )?;
```

In `lower_args` signature, add `schema: &Schema`:

```rust
fn lower_args(
    args: &[(Positioned<Name>, Positioned<GqlValue>)],
    table: &Table,
    schema: &Schema,
    vars: &Value,
    parent_path: &str,
) -> Result<QueryArgs> {
```

Update the existing call in `lower_query`:

```rust
                let args = lower_args(&field.arguments, table, schema, vars, &alias)?;
```

Inside `lower_args`, the `"where"` arm currently calls `lower_where(&json, table, ...)`. Change `lower_where` to accept `schema` so it can recurse into relation filters (wired in Task 7). Add a parameter (unused by Task 6 code paths):

```rust
fn lower_where(
    json: &Value,
    table: &Table,
    schema: &Schema,
    path: &str,
) -> Result<BoolExpr> {
```

Update all existing calls inside `lower_where` to pass `schema` recursively. The existing `_and`/`_or` recursions become:

```rust
                    .map(|(i, x)| lower_where(x, table, schema, &format!("{path}._and[{i}]")))
```

Update the caller site in `lower_args`:

```rust
            "where" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.where"))?;
                out.where_ = Some(lower_where(&json, table, schema, &format!("{parent_path}.where"))?);
            }
```

- [ ] **Step 4: Run parser tests**

Run: `cargo test --lib parser::tests`
Expected: all PASS (existing 8 + 2 new = 10).

- [ ] **Step 5: Full lib suite**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): dispatch nested selections to Field::Relation"
```

---

### Task 7: Parser — relation filters in where

**Files:**
- Modify: `src/parser.rs`

- [ ] **Step 1: Write failing test**

Append to the `tests` mod in `src/parser.rs`:

```rust
    #[test]
    fn parse_where_relation_exists() {
        let op = parse_and_lower(
            r#"query { users(where: {posts: {title: {_eq: "hello"}}}) { id } }"#,
            &json!({}),
            None,
            &schema_with_relations(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::Relation { name, inner } => {
                assert_eq!(name, "posts");
                match inner.as_ref() {
                    crate::ast::BoolExpr::Compare { column, .. } => {
                        assert_eq!(column, "title");
                    }
                    _ => panic!("expected Compare"),
                }
            }
            _ => panic!("expected Relation"),
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib parser::tests::parse_where_relation_exists`
Expected: FAIL with "unknown column 'posts'".

- [ ] **Step 3: Detect relations inside lower_where**

In `src/parser.rs`, inside `lower_where`, replace the `col_name => { ... }` arm (the default case that currently looks up a column) with:

```rust
            col_name => {
                if let Some(rel) = table.find_relation(col_name) {
                    let target = schema.table(&rel.target_table).ok_or_else(|| {
                        Error::Validate {
                            path: format!("{path}.{col_name}"),
                            message: format!(
                                "relation target table '{}' missing",
                                rel.target_table
                            ),
                        }
                    })?;
                    let inner =
                        lower_where(v, target, schema, &format!("{path}.{col_name}"))?;
                    parts.push(BoolExpr::Relation {
                        name: col_name.to_string(),
                        inner: Box::new(inner),
                    });
                    continue;
                }

                let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
                    path: format!("{path}.{col_name}"),
                    message: format!(
                        "unknown column '{col_name}' on '{}'",
                        table.exposed_name
                    ),
                })?;
                let op_obj = v.as_object().ok_or_else(|| Error::Validate {
                    path: format!("{path}.{col_name}"),
                    message: "expected operator object".into(),
                })?;
                for (op_name, op_val) in op_obj {
                    let op = match op_name.as_str() {
                        "_eq" => CmpOp::Eq,
                        "_neq" => CmpOp::Neq,
                        "_gt" => CmpOp::Gt,
                        "_gte" => CmpOp::Gte,
                        "_lt" => CmpOp::Lt,
                        "_lte" => CmpOp::Lte,
                        other => {
                            return Err(Error::Validate {
                                path: format!("{path}.{col_name}"),
                                message: format!("unsupported operator '{other}'"),
                            })
                        }
                    };
                    parts.push(BoolExpr::Compare {
                        column: col.exposed_name.clone(),
                        op,
                        value: op_val.clone(),
                    });
                }
            }
```

The `continue` is inside a `for (k, v) in obj` loop — it advances to the next key.

- [ ] **Step 4: Run parser tests**

Run: `cargo test --lib parser::tests`
Expected: PASS (all 11).

- [ ] **Step 5: Full lib suite**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): detect relation names in where as BoolExpr::Relation"
```

---

### Task 8: Integration test — nested selections

**Files:**
- Create: `tests/integration_nested.rs`

- [ ] **Step 1: Write test file**

Create `tests/integration_nested.rs`:

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
                .relation(
                    "posts",
                    Relation::array("posts").on([("id", "user_id")]),
                ),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .column("published", "published", PgType::Bool, false)
                .primary_key(&["id"])
                .relation(
                    "user",
                    Relation::object("users").on([("user_id", "id")]),
                ),
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
                    published BOOL NOT NULL
                );
                INSERT INTO users (name) VALUES ('alice'), ('bob');
                INSERT INTO posts (title, user_id, published) VALUES
                    ('a1', 1, TRUE),
                    ('a2', 1, FALSE),
                    ('b1', 2, TRUE);
                "#,
            )
            .await
            .expect("seed");
    }

    let engine = Engine::new(pool, schema());
    (engine, container)
}

#[tokio::test]
async fn array_relation_returns_nested_rows() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { users(order_by: [{id: asc}]) { name posts(order_by: [{id: asc}]) { title } } }",
            None,
        )
        .await
        .expect("query ok");
    let users = v["users"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["name"], json!("alice"));
    assert_eq!(users[0]["posts"].as_array().unwrap().len(), 2);
    assert_eq!(users[0]["posts"][0]["title"], json!("a1"));
    assert_eq!(users[1]["posts"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn object_relation_returns_single_nested_row() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { posts(order_by: [{id: asc}]) { title user { name } } }",
            None,
        )
        .await
        .expect("query ok");
    let posts = v["posts"].as_array().unwrap();
    assert_eq!(posts.len(), 3);
    assert_eq!(posts[0]["user"]["name"], json!("alice"));
    assert_eq!(posts[2]["user"]["name"], json!("bob"));
}

#[tokio::test]
async fn nested_relation_args_limit_and_filter() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { users(order_by: [{id: asc}]) { name posts(where: {published: {_eq: true}}, limit: 5) { title } } }",
            None,
        )
        .await
        .expect("query ok");
    let alice_posts = v["users"][0]["posts"].as_array().unwrap();
    assert_eq!(alice_posts.len(), 1);
    assert_eq!(alice_posts[0]["title"], json!("a1"));
}

#[tokio::test]
async fn where_relation_exists_filter() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {posts: {title: {_eq: "a1"}}}) { name } }"#,
            None,
        )
        .await
        .expect("query ok");
    let users = v["users"].as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["name"], json!("alice"));
}
```

- [ ] **Step 2: Expose `Relation` at crate root**

The test imports `vision_graphql::schema::Relation` — already public. No change needed unless `cargo test --test integration_nested` surfaces visibility errors, in which case add `pub use schema::Relation;` to `src/lib.rs`.

- [ ] **Step 3: Run integration tests**

Run: `cargo test --test integration_nested -- --test-threads=1`
Expected: 4 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/integration_nested.rs
# Plus any src/lib.rs re-export change
git add src/lib.rs 2>/dev/null || true
git commit -m "test: e2e integration tests for nested relations and EXISTS filter"
```

---

### Task 9: Verify + tag Phase 2

**Files:** none

- [ ] **Step 1: Full test suite**

Run: `cargo test`
Expected: all unit + integration tests pass.

- [ ] **Step 2: Lint**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Format**

Run: `cargo fmt --check`
Expected: clean. Run `cargo fmt` and commit if not.

- [ ] **Step 4: Tag**

```bash
git tag -a phase-2-nested-relations -m "Phase 2: object/array relations and EXISTS filters"
```

- [ ] **Step 5: Phase 2 done**

Engine now supports:
- `users { posts { title } }` (array relation, list of nested objects)
- `posts { user { name } }` (object relation, single nested object)
- Nested relation args: `posts(where, order_by, limit, offset)`
- Relation filters: `users(where: { posts: { title: {_eq: ...} } })`

Phase 3 next: aggregates (`users_aggregate`), `_by_pk`, `distinct_on`.
