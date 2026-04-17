# Vision-GraphQL Phase 3 — Aggregates, By-PK, Distinct-On Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three Hasura-style root field kinds — aggregates (`users_aggregate`), primary-key lookups (`users_by_pk(id: ...)`), and `distinct_on` modifier — on top of the Phase 2 engine.

**Architecture:** Refactor `RootField` into a `body: RootBody` enum with `List`, `Aggregate`, and `ByPk` variants so each kind carries only the data it needs. Aggregate rendering uses a single outer `json_build_object` over an inner subquery shared between summary functions (`count`, `sum`, `avg`, `max`, `min`) and explicit `json_build_object` per nodes row (to avoid leaking aggregate-only columns). By-PK uses `row_to_json` + `LIMIT 1` and returns `null` when no row matches. `distinct_on` becomes a new `QueryArgs` field rendered as PG's `SELECT DISTINCT ON (...)` with auto-prepended `ORDER BY` columns.

**Tech Stack:** No new deps.

**Out of scope:** `nodes_aggregate` inside aggregate (aggregate-over-aggregate); cross-relation `order_by` with aggregates; permissions.

---

## File Structure

All in existing files:

```
src/ast.rs      # RootBody enum + AggOp + AggregateSelection
src/sql.rs      # Render each RootBody variant
src/parser.rs   # Detect root field suffixes (_aggregate, _by_pk); parse aggregate/pk args
tests/integration_aggregate.rs    # new
tests/integration_by_pk.rs        # new
tests/integration_distinct.rs     # new
```

---

### Task 1: Refactor RootField to RootBody enum

**Files:**
- Modify: `src/ast.rs`
- Modify: `src/sql.rs`
- Modify: `src/parser.rs`

Pure refactor; all existing behaviour preserved.

- [ ] **Step 1: Replace RootField/RootKind in src/ast.rs**

Replace the existing `pub struct RootField` and `pub enum RootKind` with:

```rust
#[derive(Debug, Clone)]
pub struct RootField {
    pub table: String,
    pub alias: String,
    pub args: QueryArgs,
    pub body: RootBody,
}

#[derive(Debug, Clone)]
pub enum RootBody {
    List {
        selection: Vec<Field>,
    },
}
```

Also update the `build_simple_root_field` test in the same file's `tests` mod. Replace its `let root = RootField { ... }` block with:

```rust
        let root = RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs::default(),
            body: RootBody::List {
                selection: vec![
                    Field::Column {
                        physical: "id".into(),
                        alias: "id".into(),
                    },
                    Field::Column {
                        physical: "name".into(),
                        alias: "name".into(),
                    },
                ],
            },
        };
        assert_eq!(root.table, "users");
        match root.body {
            RootBody::List { selection } => assert_eq!(selection.len(), 2),
        }
```

- [ ] **Step 2: Update src/sql.rs**

In `render_root`, replace the body:

```rust
fn render_root(root: &RootField, schema: &Schema, ctx: &mut RenderCtx) -> Result<()> {
    let table = schema.table(&root.table).ok_or_else(|| Error::Validate {
        path: root.alias.clone(),
        message: format!("unknown table '{}'", root.table),
    })?;
    match &root.body {
        crate::ast::RootBody::List { selection } => {
            render_list(root, selection, table, schema, ctx)
        }
    }
}
```

Update `render_list` and `render_inner_select` to take `selection: &[Field]` explicitly instead of reading `root.selection`:

```rust
fn render_list(
    root: &RootField,
    selection: &[Field],
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let inner_alias = ctx.next_alias("t");
    let row_alias = ctx.next_alias("r");
    ctx.sql.push_str("(SELECT coalesce(json_agg(row_to_json(");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push_str(")), '[]'::json) FROM (");
    render_inner_select(root, selection, table, &inner_alias, schema, ctx)?;
    ctx.sql.push_str(") ");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push(')');
    Ok(())
}

fn render_inner_select(
    root: &RootField,
    selection: &[Field],
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    ctx.sql.push_str("SELECT ");
    for (i, field) in selection.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match field {
            Field::Column { physical, alias } => {
                let col = table.find_column(physical).ok_or_else(|| Error::Validate {
                    path: format!("{}.{}", root.alias, alias),
                    message: format!("unknown column '{physical}' on '{}'", root.table),
                })?;
                write!(
                    ctx.sql,
                    r#"{table_alias}.{} AS "{}""#,
                    quote_ident(&col.physical_name),
                    alias
                )
                .unwrap();
            }
            Field::Relation { name, alias, args, selection } => {
                render_relation_field(
                    name, alias, args, selection, table, table_alias,
                    schema, &root.alias, ctx,
                )?;
            }
        }
    }
    write!(
        ctx.sql,
        " FROM {}.{} {table_alias}",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    render_where(&root.args, table, table_alias, schema, ctx)?;
    render_order_by(&root.args, table, table_alias, ctx)?;
    render_limit_offset(&root.args, ctx);
    Ok(())
}
```

All tests under `src/sql.rs` currently construct `RootField { kind, selection, ... }`. Replace each construction with `RootField { body: RootBody::List { selection: vec![...] }, ... }` and drop `kind`. Concretely, the pattern to search for is:

```rust
RootField {
    table: ...,
    alias: ...,
    kind: RootKind::List,
    args: ...,
    selection: vec![ ... ],
}
```

becomes:

```rust
RootField {
    table: ...,
    alias: ...,
    args: ...,
    body: RootBody::List {
        selection: vec![ ... ],
    },
}
```

Also remove the `use` of `RootKind` in test imports.

At the top of `src/sql.rs` test module, update the `use super::*;` block so `RootBody` is imported. Since `RootBody` is in `crate::ast`, add:

```rust
    use crate::ast::{Field, Operation, QueryArgs, RootBody, RootField};
```

(drop `RootKind`).

Each test body replaces `kind: RootKind::List,` and its `selection: vec![...]` line with `body: RootBody::List { selection: vec![...] },`.

- [ ] **Step 3: Update src/parser.rs**

In `lower_query`, replace the `RootField` construction. Find:

```rust
                roots.push(RootField {
                    table: name.to_string(),
                    alias,
                    kind: RootKind::List,
                    args,
                    selection,
                });
```

Replace with:

```rust
                roots.push(RootField {
                    table: name.to_string(),
                    alias,
                    args,
                    body: crate::ast::RootBody::List { selection },
                });
```

Remove `RootKind` from the `use crate::ast::{...}` import at the top.

Existing parser tests (`parse_plain_list` et al.) still call `parse_and_lower` and match on the result. They don't construct `RootField` directly, so they need no changes beyond any `RootKind::List` references. Search and replace:

```rust
assert!(matches!(roots[0].kind, RootKind::List));
```

becomes:

```rust
assert!(matches!(
    roots[0].body,
    crate::ast::RootBody::List { .. }
));
```

- [ ] **Step 4: Build and test**

Run: `cargo build 2>&1 | tail -30`
Expected: compiles; if any `RootKind` or `kind:` reference remains, fix.

Run: `cargo test --lib`
Expected: all existing tests pass (23 or more).

Run: `cargo test`
Expected: all integration tests still pass (8 from Phase 1+2).

- [ ] **Step 5: Commit**

```bash
git add src/ast.rs src/sql.rs src/parser.rs
git commit -m "refactor(ast): collapse RootKind/selection into RootBody enum"
```

---

### Task 2: IR — AggOp and RootBody::Aggregate

**Files:**
- Modify: `src/ast.rs`

- [ ] **Step 1: Extend RootBody and add AggOp**

In `src/ast.rs`, replace the `RootBody` enum with:

```rust
#[derive(Debug, Clone)]
pub enum RootBody {
    List {
        selection: Vec<Field>,
    },
    Aggregate {
        ops: Vec<AggOp>,
        nodes: Option<Vec<Field>>,
    },
}

#[derive(Debug, Clone)]
pub enum AggOp {
    Count,
    Sum { columns: Vec<String> },
    Avg { columns: Vec<String> },
    Max { columns: Vec<String> },
    Min { columns: Vec<String> },
}
```

- [ ] **Step 2: Write a smoke test**

Append to the `tests` mod in `src/ast.rs`:

```rust
    #[test]
    fn build_aggregate_root() {
        let body = RootBody::Aggregate {
            ops: vec![
                AggOp::Count,
                AggOp::Sum {
                    columns: vec!["age".into()],
                },
            ],
            nodes: Some(vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }]),
        };
        match body {
            RootBody::Aggregate { ops, nodes } => {
                assert_eq!(ops.len(), 2);
                assert!(nodes.is_some());
            }
            _ => panic!("expected Aggregate"),
        }
    }
```

- [ ] **Step 3: Run test**

Run: `cargo test --lib ast::tests::build_aggregate_root`
Expected: PASS.

- [ ] **Step 4: Make sql.rs compile**

`render_root` now needs an arm for `RootBody::Aggregate`. Add a stub that's replaced in Task 3:

In `src/sql.rs`, update `render_root`:

```rust
    match &root.body {
        crate::ast::RootBody::List { selection } => {
            render_list(root, selection, table, schema, ctx)
        }
        crate::ast::RootBody::Aggregate { .. } => Err(Error::Validate {
            path: root.alias.clone(),
            message: "Aggregate not yet implemented".into(),
        }),
    }
```

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/ast.rs src/sql.rs
git commit -m "feat(ast): RootBody::Aggregate with AggOp variants"
```

---

### Task 3: SQL — render RootBody::Aggregate

**Files:**
- Modify: `src/sql.rs`

- [ ] **Step 1: Write failing test**

Append to the `tests` mod in `src/sql.rs`:

```rust
    #[test]
    fn render_aggregate_count_and_sum() {
        use crate::ast::{AggOp, RootBody};

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users_aggregate".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                ops: vec![
                    AggOp::Count,
                    AggOp::Sum {
                        columns: vec!["id".into()],
                    },
                ],
                nodes: Some(vec![Field::Column {
                    physical: "name".into(),
                    alias: "name".into(),
                }]),
            },
        }]);
        let (sql, _binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_aggregate_no_nodes() {
        use crate::ast::{AggOp, RootBody};

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users_aggregate".into(),
            args: QueryArgs::default(),
            body: RootBody::Aggregate {
                ops: vec![AggOp::Count],
                nodes: None,
            },
        }]);
        let (sql, _binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib sql::tests::render_aggregate_count_and_sum`
Expected: FAIL with "Aggregate not yet implemented".

- [ ] **Step 3: Implement render_aggregate**

In `src/sql.rs`, replace the stub arm in `render_root`:

```rust
        crate::ast::RootBody::Aggregate { ops, nodes } => {
            render_aggregate(root, ops, nodes.as_deref(), table, schema, ctx)
        }
```

Append a new helper at the bottom of the file (before the `#[cfg(test)]` block):

```rust
fn render_aggregate(
    root: &RootField,
    ops: &[crate::ast::AggOp],
    nodes: Option<&[Field]>,
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let inner_alias = ctx.next_alias("t");

    ctx.sql.push_str("(SELECT json_build_object(");

    // 'aggregate' key (always present in Hasura shape; empty object if no ops).
    ctx.sql.push_str("'aggregate', json_build_object(");
    for (i, op) in ops.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        render_agg_op(op, &inner_alias, table, ctx)?;
    }
    ctx.sql.push(')');

    // Optional 'nodes' key.
    if let Some(node_fields) = nodes {
        ctx.sql.push_str(", 'nodes', coalesce(json_agg(");
        render_json_build_object_for_nodes(node_fields, &inner_alias, table, &root.alias, ctx)?;
        ctx.sql.push_str("), '[]'::json)");
    }

    ctx.sql.push_str(") FROM (");
    // Inner subquery: select all columns needed by aggregates or nodes.
    render_aggregate_source(root, ops, nodes, table, &inner_alias, schema, ctx)?;
    ctx.sql.push_str(") ");
    ctx.sql.push_str(&inner_alias);
    ctx.sql.push(')');
    Ok(())
}

fn render_agg_op(
    op: &crate::ast::AggOp,
    table_alias: &str,
    table: &Table,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::AggOp;
    match op {
        AggOp::Count => {
            ctx.sql.push_str("'count', count(*)");
            Ok(())
        }
        AggOp::Sum { columns } => render_agg_func("sum", "sum", columns, table_alias, table, ctx),
        AggOp::Avg { columns } => render_agg_func("avg", "avg", columns, table_alias, table, ctx),
        AggOp::Max { columns } => render_agg_func("max", "max", columns, table_alias, table, ctx),
        AggOp::Min { columns } => render_agg_func("min", "min", columns, table_alias, table, ctx),
    }
}

fn render_agg_func(
    key: &str,
    pg_func: &str,
    columns: &[String],
    table_alias: &str,
    table: &Table,
    ctx: &mut RenderCtx,
) -> Result<()> {
    write!(ctx.sql, "'{key}', json_build_object(").unwrap();
    for (i, col_exposed) in columns.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let col = table.find_column(col_exposed).ok_or_else(|| Error::Validate {
            path: format!("aggregate.{key}.{col_exposed}"),
            message: format!(
                "unknown column '{col_exposed}' on '{}'",
                table.exposed_name
            ),
        })?;
        write!(
            ctx.sql,
            "'{col_exposed}', {pg_func}({table_alias}.{})",
            quote_ident(&col.physical_name)
        )
        .unwrap();
    }
    ctx.sql.push(')');
    Ok(())
}

fn render_json_build_object_for_nodes(
    fields: &[Field],
    table_alias: &str,
    table: &Table,
    parent_path: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    ctx.sql.push_str("json_build_object(");
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match f {
            Field::Column { physical, alias } => {
                let col = table.find_column(physical).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.nodes.{alias}"),
                    message: format!(
                        "unknown column '{physical}' on '{}'",
                        table.exposed_name
                    ),
                })?;
                write!(
                    ctx.sql,
                    "'{alias}', {table_alias}.{}",
                    quote_ident(&col.physical_name)
                )
                .unwrap();
            }
            Field::Relation { .. } => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.nodes"),
                    message: "relations inside aggregate nodes not yet supported".into(),
                });
            }
        }
    }
    ctx.sql.push(')');
    Ok(())
}

fn render_aggregate_source(
    root: &RootField,
    ops: &[crate::ast::AggOp],
    nodes: Option<&[Field]>,
    table: &Table,
    table_alias: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::AggOp;
    use std::collections::BTreeSet;

    // Collect physical column names required by aggregates (count doesn't need any).
    let mut cols_needed: BTreeSet<String> = BTreeSet::new();
    for op in ops {
        let (AggOp::Sum { columns } | AggOp::Avg { columns } | AggOp::Max { columns } | AggOp::Min { columns }) = op else {
            continue;
        };
        for c in columns {
            let col = table.find_column(c).ok_or_else(|| Error::Validate {
                path: format!("{}.aggregate", root.alias),
                message: format!("unknown column '{c}' on '{}'", table.exposed_name),
            })?;
            cols_needed.insert(col.physical_name.clone());
        }
    }
    // Plus columns referenced by nodes, if any.
    if let Some(fields) = nodes {
        for f in fields {
            if let Field::Column { physical, .. } = f {
                let col = table.find_column(physical).ok_or_else(|| Error::Validate {
                    path: format!("{}.nodes", root.alias),
                    message: format!("unknown column '{physical}' on '{}'", table.exposed_name),
                })?;
                cols_needed.insert(col.physical_name.clone());
            }
        }
    }
    // If nothing needed (pure count), still need at least one column so PG doesn't reject SELECT ..
    if cols_needed.is_empty() {
        cols_needed.insert("1".into());
    }

    ctx.sql.push_str("SELECT ");
    let mut first = true;
    for c in &cols_needed {
        if !first {
            ctx.sql.push_str(", ");
        }
        first = false;
        if c == "1" {
            // literal 1 doesn't need quoting
            ctx.sql.push('1');
        } else {
            ctx.sql.push_str(&quote_ident(c));
        }
    }
    write!(
        ctx.sql,
        " FROM {}.{}",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();

    // Apply user args to the inner source (where/order_by/limit/offset).
    // NB: we render WHERE against the physical table without an alias because
    // the inner SELECT has none; it gets aliased in the outer FROM.
    // To share render_where we temporarily add a dummy alias — simpler: inline.
    if let Some(expr) = root.args.where_.as_ref() {
        ctx.sql.push_str(" WHERE ");
        render_bool_expr_no_alias(expr, table, schema, ctx)?;
    }
    if !root.args.order_by.is_empty() {
        ctx.sql.push_str(" ORDER BY ");
        for (i, ob) in root.args.order_by.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(", ");
            }
            let col = table
                .find_column(&ob.column)
                .ok_or_else(|| Error::Validate {
                    path: format!("{}.order_by.{}", root.alias, ob.column),
                    message: format!(
                        "unknown column '{}' on '{}'",
                        ob.column, table.exposed_name
                    ),
                })?;
            let dir = match ob.direction {
                crate::ast::OrderDir::Asc => "ASC",
                crate::ast::OrderDir::Desc => "DESC",
            };
            write!(ctx.sql, "{} {dir}", quote_ident(&col.physical_name)).unwrap();
        }
    }
    if let Some(n) = root.args.limit {
        write!(ctx.sql, " LIMIT {n}").unwrap();
    }
    if let Some(n) = root.args.offset {
        write!(ctx.sql, " OFFSET {n}").unwrap();
    }
    // Suppress unused-var lint when table_alias isn't consumed.
    let _ = table_alias;
    Ok(())
}

/// Same as render_bool_expr but emits column references without a table alias prefix.
fn render_bool_expr_no_alias(
    expr: &crate::ast::BoolExpr,
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::{BoolExpr, CmpOp};
    match expr {
        BoolExpr::And(parts) => {
            if parts.is_empty() {
                ctx.sql.push_str("TRUE");
                return Ok(());
            }
            ctx.sql.push('(');
            for (i, p) in parts.iter().enumerate() {
                if i > 0 {
                    ctx.sql.push_str(" AND ");
                }
                render_bool_expr_no_alias(p, table, schema, ctx)?;
            }
            ctx.sql.push(')');
            Ok(())
        }
        BoolExpr::Or(parts) => {
            if parts.is_empty() {
                ctx.sql.push_str("FALSE");
                return Ok(());
            }
            ctx.sql.push('(');
            for (i, p) in parts.iter().enumerate() {
                if i > 0 {
                    ctx.sql.push_str(" OR ");
                }
                render_bool_expr_no_alias(p, table, schema, ctx)?;
            }
            ctx.sql.push(')');
            Ok(())
        }
        BoolExpr::Not(inner) => {
            ctx.sql.push_str("(NOT ");
            render_bool_expr_no_alias(inner, table, schema, ctx)?;
            ctx.sql.push(')');
            Ok(())
        }
        BoolExpr::Compare { column, op, value } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            let bind =
                crate::types::json_to_bind(value, &col.pg_type).map_err(|e| Error::Validate {
                    path: format!("where.{column}"),
                    message: format!("{e}"),
                })?;
            ctx.binds.push(bind);
            let placeholder = format!("${}", ctx.binds.len());
            let op_str = match op {
                CmpOp::Eq => "=",
                CmpOp::Neq => "<>",
                CmpOp::Gt => ">",
                CmpOp::Gte => ">=",
                CmpOp::Lt => "<",
                CmpOp::Lte => "<=",
            };
            write!(
                ctx.sql,
                "{} {op_str} {placeholder}",
                quote_ident(&col.physical_name)
            )
            .unwrap();
            Ok(())
        }
        BoolExpr::Relation { .. } => Err(Error::Validate {
            path: "where".into(),
            message: "relation filters not supported inside aggregate source".into(),
        }),
    }
}
```

- [ ] **Step 4: Accept snapshots**

Run: `INSTA_UPDATE=always cargo test --lib sql::tests`
Expected: snapshots accepted.

Inspect: `cat src/snapshots/vision_graphql__sql__tests__render_aggregate_count_and_sum.snap`

Expected shape (substrings):
- `json_build_object('aggregate', json_build_object('count', count(*), 'sum', json_build_object('id', sum(...))), 'nodes', coalesce(json_agg(json_build_object('name', ...)), '[]'::json))`
- Inner: `SELECT "id", "name" FROM "public"."users"`

- [ ] **Step 5: Full lib tests**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "feat(sql): render RootBody::Aggregate with count/sum/avg/max/min"
```

---

### Task 4: Parser — `_aggregate` root field

**Files:**
- Modify: `src/parser.rs`

- [ ] **Step 1: Write failing test**

Append to the `tests` mod in `src/parser.rs`:

```rust
    #[test]
    fn parse_aggregate_basic() {
        let op = parse_and_lower(
            "query { users_aggregate { aggregate { count, sum { id } } nodes { id } } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        assert_eq!(roots[0].table, "users");
        match &roots[0].body {
            crate::ast::RootBody::Aggregate { ops, nodes } => {
                assert_eq!(ops.len(), 2);
                matches!(ops[0], crate::ast::AggOp::Count);
                match &ops[1] {
                    crate::ast::AggOp::Sum { columns } => assert_eq!(columns, &vec!["id".to_string()]),
                    _ => panic!("expected Sum"),
                }
                assert!(nodes.as_ref().map(|n| n.len()).unwrap_or(0) == 1);
            }
            _ => panic!("expected Aggregate"),
        }
    }

    #[test]
    fn parse_aggregate_count_only() {
        let op = parse_and_lower(
            "query { users_aggregate(where: {id: {_gt: 0}}) { aggregate { count } } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match &roots[0].body {
            crate::ast::RootBody::Aggregate { ops, nodes } => {
                assert_eq!(ops.len(), 1);
                assert!(nodes.is_none());
            }
            _ => panic!("expected Aggregate"),
        }
        assert!(roots[0].args.where_.is_some());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib parser::tests::parse_aggregate_basic`
Expected: FAIL — root field `users_aggregate` not found.

- [ ] **Step 3: Detect _aggregate suffix and lower aggregate body**

In `src/parser.rs`, replace the relevant block of `lower_query`. Find the existing `let table = schema.table(name).ok_or_else(...)` and the subsequent body construction. Replace the whole `Selection::Field(f) => { ... }` arm body with:

```rust
            Selection::Field(f) => {
                let field = &f.node;
                let name = field.name.node.as_str();
                let alias = field
                    .alias
                    .as_ref()
                    .map(|a| a.node.as_str().to_string())
                    .unwrap_or_else(|| name.to_string());

                // Aggregate root: "<table>_aggregate"
                if let Some(base_name) = name.strip_suffix("_aggregate") {
                    if let Some(table) = schema.table(base_name) {
                        let args = lower_args(&field.arguments, table, schema, vars, &alias)?;
                        let (ops, nodes) = lower_aggregate_selection(
                            &field.selection_set.node,
                            table,
                            &alias,
                        )?;
                        roots.push(RootField {
                            table: base_name.to_string(),
                            alias,
                            args,
                            body: crate::ast::RootBody::Aggregate { ops, nodes },
                        });
                        continue;
                    }
                }

                // Plain list root: "<table>"
                let table = schema.table(name).ok_or_else(|| Error::Validate {
                    path: alias.clone(),
                    message: format!("unknown root field '{name}'"),
                })?;
                let args = lower_args(&field.arguments, table, schema, vars, &alias)?;
                let selection =
                    lower_selection_set(&field.selection_set.node, table, schema, vars, &alias)?;

                roots.push(RootField {
                    table: name.to_string(),
                    alias,
                    args,
                    body: crate::ast::RootBody::List { selection },
                });
            }
```

Now add the aggregate selection lowering helper. Append at the end of `src/parser.rs` (before the `#[cfg(test)]`):

```rust
fn lower_aggregate_selection(
    set: &SelectionSet,
    table: &Table,
    parent_path: &str,
) -> Result<(Vec<crate::ast::AggOp>, Option<Vec<Field>>)> {
    let mut ops: Vec<crate::ast::AggOp> = Vec::new();
    let mut nodes: Option<Vec<Field>> = None;

    for sel in &set.items {
        let Selection::Field(f) = &sel.node else {
            return Err(Error::Parse(
                "fragments are not supported in Phase 3".into(),
            ));
        };
        let field = &f.node;
        let key = field.name.node.as_str();
        match key {
            "aggregate" => {
                // selection set contains: count | sum { col col } | avg | max | min
                for s in &field.selection_set.node.items {
                    let Selection::Field(sf) = &s.node else {
                        return Err(Error::Parse(
                            "fragments not supported inside aggregate".into(),
                        ));
                    };
                    let sf = &sf.node;
                    let op_name = sf.name.node.as_str();
                    match op_name {
                        "count" => {
                            ops.push(crate::ast::AggOp::Count);
                        }
                        "sum" | "avg" | "max" | "min" => {
                            let mut columns = Vec::new();
                            for cs in &sf.selection_set.node.items {
                                let Selection::Field(cf) = &cs.node else {
                                    return Err(Error::Parse(
                                        "fragments not supported inside aggregate".into(),
                                    ));
                                };
                                let cname = cf.node.name.node.as_str();
                                let col = table.find_column(cname).ok_or_else(|| {
                                    Error::Validate {
                                        path: format!("{parent_path}.aggregate.{op_name}.{cname}"),
                                        message: format!(
                                            "unknown column '{cname}' on '{}'",
                                            table.exposed_name
                                        ),
                                    }
                                })?;
                                columns.push(col.exposed_name.clone());
                            }
                            let op = match op_name {
                                "sum" => crate::ast::AggOp::Sum { columns },
                                "avg" => crate::ast::AggOp::Avg { columns },
                                "max" => crate::ast::AggOp::Max { columns },
                                "min" => crate::ast::AggOp::Min { columns },
                                _ => unreachable!(),
                            };
                            ops.push(op);
                        }
                        other => {
                            return Err(Error::Validate {
                                path: format!("{parent_path}.aggregate.{other}"),
                                message: format!("unsupported aggregate '{other}'"),
                            });
                        }
                    }
                }
            }
            "nodes" => {
                let fields = lower_selection_columns_only(
                    &field.selection_set.node,
                    table,
                    &format!("{parent_path}.nodes"),
                )?;
                nodes = Some(fields);
            }
            other => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.{other}"),
                    message: format!("unknown aggregate subfield '{other}'"),
                });
            }
        }
    }
    Ok((ops, nodes))
}

fn lower_selection_columns_only(
    set: &SelectionSet,
    table: &Table,
    parent_path: &str,
) -> Result<Vec<Field>> {
    let mut out = Vec::new();
    for sel in &set.items {
        let Selection::Field(f) = &sel.node else {
            return Err(Error::Parse(
                "fragments not supported inside aggregate nodes".into(),
            ));
        };
        let field = &f.node;
        let name = field.name.node.as_str();
        let alias = field
            .alias
            .as_ref()
            .map(|a| a.node.as_str().to_string())
            .unwrap_or_else(|| name.to_string());
        let col = table.find_column(name).ok_or_else(|| Error::Validate {
            path: format!("{parent_path}.{alias}"),
            message: format!("unknown column '{name}' on '{}'", table.exposed_name),
        })?;
        out.push(Field::Column {
            physical: col.physical_name.clone(),
            alias,
        });
    }
    Ok(out)
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib parser::tests`
Expected: all PASS (existing + 2 new).

- [ ] **Step 5: Full lib suite**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): lower users_aggregate root field to RootBody::Aggregate"
```

---

### Task 5: Integration test — aggregates

**Files:**
- Create: `tests/integration_aggregate.rs`

- [ ] **Step 1: Write the integration test**

Create `tests/integration_aggregate.rs`:

```rust
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
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
                    name TEXT NOT NULL,
                    score INT NOT NULL
                );
                INSERT INTO users (name, score) VALUES
                    ('alice', 10),
                    ('bob',   20),
                    ('cara',  30);
                "#,
            )
            .await
            .expect("seed");
    }

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
    assert_eq!(v["users_aggregate"]["aggregate"]["avg"]["score"], json!(20.0));
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
```

- [ ] **Step 2: Run**

Run: `cargo test --test integration_aggregate -- --test-threads=1`
Expected: 4 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_aggregate.rs
git commit -m "test: e2e integration tests for aggregates"
```

---

### Task 6: IR + SQL — RootBody::ByPk

**Files:**
- Modify: `src/ast.rs`
- Modify: `src/sql.rs`

- [ ] **Step 1: Extend RootBody**

In `src/ast.rs`, replace the `RootBody` enum with:

```rust
#[derive(Debug, Clone)]
pub enum RootBody {
    List {
        selection: Vec<Field>,
    },
    Aggregate {
        ops: Vec<AggOp>,
        nodes: Option<Vec<Field>>,
    },
    ByPk {
        /// `(exposed_column, value)` pairs. All PK columns must be present.
        pk: Vec<(String, serde_json::Value)>,
        selection: Vec<Field>,
    },
}
```

- [ ] **Step 2: Write failing SQL test**

Append to the `tests` mod in `src/sql.rs`:

```rust
    #[test]
    fn render_by_pk_single_col() {
        use crate::ast::RootBody;
        use serde_json::json;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users_by_pk".into(),
            args: QueryArgs::default(),
            body: RootBody::ByPk {
                pk: vec![("id".into(), json!(7))],
                selection: vec![Field::Column {
                    physical: "name".into(),
                    alias: "name".into(),
                }],
            },
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
    }
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test --lib sql::tests::render_by_pk_single_col`
Expected: FAIL — missing ByPk arm or stub.

- [ ] **Step 4: Implement render for ByPk**

In `src/sql.rs`, update `render_root` to:

```rust
    match &root.body {
        crate::ast::RootBody::List { selection } => {
            render_list(root, selection, table, schema, ctx)
        }
        crate::ast::RootBody::Aggregate { ops, nodes } => {
            render_aggregate(root, ops, nodes.as_deref(), table, schema, ctx)
        }
        crate::ast::RootBody::ByPk { pk, selection } => {
            render_by_pk(root, pk, selection, table, schema, ctx)
        }
    }
```

Append helper:

```rust
fn render_by_pk(
    root: &RootField,
    pk: &[(String, serde_json::Value)],
    selection: &[Field],
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let inner_alias = ctx.next_alias("t");
    let row_alias = ctx.next_alias("r");
    ctx.sql.push_str("(SELECT row_to_json(");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push_str(") FROM (SELECT ");
    for (i, field) in selection.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match field {
            Field::Column { physical, alias } => {
                let col = table.find_column(physical).ok_or_else(|| Error::Validate {
                    path: format!("{}.{}", root.alias, alias),
                    message: format!("unknown column '{physical}' on '{}'", table.exposed_name),
                })?;
                write!(
                    ctx.sql,
                    r#"{inner_alias}.{} AS "{}""#,
                    quote_ident(&col.physical_name),
                    alias
                )
                .unwrap();
            }
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
                    &inner_alias,
                    schema,
                    &root.alias,
                    ctx,
                )?;
            }
        }
    }
    write!(
        ctx.sql,
        " FROM {}.{} {inner_alias} WHERE ",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    for (i, (col_name, value)) in pk.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(" AND ");
        }
        let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
            path: format!("{}.pk.{col_name}", root.alias),
            message: format!("unknown column '{col_name}' on '{}'", table.exposed_name),
        })?;
        let bind = crate::types::json_to_bind(value, &col.pg_type).map_err(|e| {
            Error::Validate {
                path: format!("{}.pk.{col_name}", root.alias),
                message: format!("{e}"),
            }
        })?;
        ctx.binds.push(bind);
        let ph = format!("${}", ctx.binds.len());
        write!(
            ctx.sql,
            "{inner_alias}.{} = {ph}",
            quote_ident(&col.physical_name)
        )
        .unwrap();
    }
    ctx.sql.push_str(" LIMIT 1) ");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push(')');
    Ok(())
}
```

- [ ] **Step 5: Accept snapshot**

Run: `INSTA_UPDATE=always cargo test --lib sql::tests::render_by_pk_single_col`
Expected: snapshot accepted.

Inspect: `cat src/snapshots/vision_graphql__sql__tests__render_by_pk_single_col.snap`

Expected: contains `row_to_json(r...)`, `FROM "public"."users" t... WHERE t....id = $1 LIMIT 1`.

- [ ] **Step 6: Full lib suite**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/ast.rs src/sql.rs src/snapshots/
git commit -m "feat(sql): render RootBody::ByPk with row_to_json and LIMIT 1"
```

---

### Task 7: Parser — `_by_pk` root field

**Files:**
- Modify: `src/parser.rs`

- [ ] **Step 1: Write failing test**

Append to the `tests` mod in `src/parser.rs`:

```rust
    #[test]
    fn parse_by_pk_single_col() {
        let op = parse_and_lower(
            "query { users_by_pk(id: 7) { id name } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        assert_eq!(roots[0].table, "users");
        match &roots[0].body {
            crate::ast::RootBody::ByPk { pk, selection } => {
                assert_eq!(pk.len(), 1);
                assert_eq!(pk[0].0, "id");
                assert_eq!(pk[0].1, json!(7));
                assert_eq!(selection.len(), 2);
            }
            _ => panic!("expected ByPk"),
        }
    }

    #[test]
    fn parse_by_pk_with_variable() {
        let op = parse_and_lower(
            "query Q($uid: Int!) { users_by_pk(id: $uid) { name } }",
            &json!({"uid": 42}),
            Some("Q"),
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match &roots[0].body {
            crate::ast::RootBody::ByPk { pk, .. } => {
                assert_eq!(pk[0].1, json!(42));
            }
            _ => panic!("expected ByPk"),
        }
    }

    #[test]
    fn parse_by_pk_missing_required_pk_errors() {
        let err = parse_and_lower(
            "query { users_by_pk { name } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("required primary key"));
    }
```

Before running, update the `schema()` helper inside the `tests` mod to include a primary key:

```rust
    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"]),
            )
            .build()
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib parser::tests::parse_by_pk_single_col`
Expected: FAIL.

- [ ] **Step 3: Detect _by_pk suffix**

In `src/parser.rs` `lower_query`, inside the `Selection::Field` arm, insert another suffix check before the "plain list" block. The full ordered logic becomes:

1. `_aggregate` suffix → Aggregate
2. `_by_pk` suffix → ByPk
3. Else → List

Concretely, insert this between the aggregate block and the list block:

```rust
                // By-PK root: "<table>_by_pk"
                if let Some(base_name) = name.strip_suffix("_by_pk") {
                    if let Some(table) = schema.table(base_name) {
                        // Parse arguments: each argument must match a PK column.
                        if table.primary_key.is_empty() {
                            return Err(Error::Validate {
                                path: alias.clone(),
                                message: format!(
                                    "table '{}' has no primary key; _by_pk not available",
                                    table.exposed_name
                                ),
                            });
                        }
                        let mut pk: Vec<(String, serde_json::Value)> = Vec::new();
                        for pk_col in &table.primary_key {
                            let found = field.arguments.iter().find(|(n, _)| n.node.as_str() == pk_col);
                            let (_, value_p) = found.ok_or_else(|| Error::Validate {
                                path: alias.clone(),
                                message: format!(
                                    "required primary key argument '{pk_col}' missing"
                                ),
                            })?;
                            let json = gql_to_json(
                                &value_p.node,
                                vars,
                                &format!("{alias}.{pk_col}"),
                            )?;
                            pk.push((pk_col.clone(), json));
                        }
                        let selection = lower_selection_set(
                            &field.selection_set.node,
                            table,
                            schema,
                            vars,
                            &alias,
                        )?;
                        roots.push(RootField {
                            table: base_name.to_string(),
                            alias,
                            args: QueryArgs::default(),
                            body: crate::ast::RootBody::ByPk { pk, selection },
                        });
                        continue;
                    }
                }
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib parser::tests`
Expected: PASS (all aggregate + ByPk tests plus existing).

- [ ] **Step 5: Full lib suite**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): lower users_by_pk(id: ...) to RootBody::ByPk"
```

---

### Task 8: Integration test — by_pk

**Files:**
- Create: `tests/integration_by_pk.rs`

- [ ] **Step 1: Write integration test**

Create `tests/integration_by_pk.rs`:

```rust
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
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
                INSERT INTO users (name) VALUES ('alice'), ('bob'), ('cara');
                "#,
            )
            .await
            .expect("seed");
    }
    let engine = Engine::new(pool, schema());
    (engine, container)
}

#[tokio::test]
async fn by_pk_returns_object() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query("query { users_by_pk(id: 2) { id name } }", None)
        .await
        .expect("query ok");
    assert_eq!(v["users_by_pk"]["id"], json!(2));
    assert_eq!(v["users_by_pk"]["name"], json!("bob"));
}

#[tokio::test]
async fn by_pk_missing_row_returns_null() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query("query { users_by_pk(id: 9999) { id } }", None)
        .await
        .expect("query ok");
    assert!(v["users_by_pk"].is_null());
}

#[tokio::test]
async fn by_pk_with_variable() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query Q($uid: Int!) { users_by_pk(id: $uid) { name } }",
            Some(json!({"uid": 1})),
        )
        .await
        .expect("query ok");
    assert_eq!(v["users_by_pk"]["name"], json!("alice"));
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test integration_by_pk -- --test-threads=1`
Expected: 3 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_by_pk.rs
git commit -m "test: e2e integration tests for _by_pk lookup"
```

---

### Task 9: distinct_on end-to-end

**Files:**
- Modify: `src/ast.rs`
- Modify: `src/sql.rs`
- Modify: `src/parser.rs`
- Create: `tests/integration_distinct.rs`

- [ ] **Step 1: Extend QueryArgs**

In `src/ast.rs`, replace `pub struct QueryArgs` with:

```rust
#[derive(Debug, Clone, Default)]
pub struct QueryArgs {
    pub where_: Option<BoolExpr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub distinct_on: Vec<String>,
}
```

- [ ] **Step 2: Parser — parse distinct_on argument**

In `src/parser.rs`, inside `lower_args` match, add a new arm before the `_ => ...`:

```rust
            "distinct_on" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.distinct_on"))?;
                let items = match &json {
                    Value::Array(xs) => xs.clone(),
                    single => vec![single.clone()],
                };
                let mut cols = Vec::new();
                for (i, item) in items.iter().enumerate() {
                    let s = item.as_str().ok_or_else(|| Error::Validate {
                        path: format!("{parent_path}.distinct_on[{i}]"),
                        message: "expected column name (enum or string)".into(),
                    })?;
                    if table.find_column(s).is_none() {
                        return Err(Error::Validate {
                            path: format!("{parent_path}.distinct_on[{i}]"),
                            message: format!(
                                "unknown column '{s}' on '{}'",
                                table.exposed_name
                            ),
                        });
                    }
                    cols.push(s.to_string());
                }
                out.distinct_on = cols;
            }
```

- [ ] **Step 3: SQL — render DISTINCT ON in plain SELECT**

In `src/sql.rs`, modify `render_inner_select`. Locate the `ctx.sql.push_str("SELECT ");` line and replace with:

```rust
    ctx.sql.push_str("SELECT ");
    if !root.args.distinct_on.is_empty() {
        ctx.sql.push_str("DISTINCT ON (");
        for (i, col_name) in root.args.distinct_on.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(", ");
            }
            let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
                path: format!("{}.distinct_on.{col_name}", root.alias),
                message: format!("unknown column '{col_name}' on '{}'", root.table),
            })?;
            write!(
                ctx.sql,
                "{table_alias}.{}",
                quote_ident(&col.physical_name)
            )
            .unwrap();
        }
        ctx.sql.push_str(") ");
    }
```

Also update `render_order_by` to auto-prepend the `distinct_on` columns when they are not already at the start of `order_by`. Replace the body of `render_order_by` with:

```rust
fn render_order_by(
    args: &QueryArgs,
    table: &Table,
    table_alias: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let mut prefix: Vec<(String, crate::ast::OrderDir)> = Vec::new();
    for d in &args.distinct_on {
        let already = args.order_by.iter().any(|ob| ob.column == *d);
        if !already {
            prefix.push((d.clone(), crate::ast::OrderDir::Asc));
        }
    }
    if prefix.is_empty() && args.order_by.is_empty() {
        return Ok(());
    }
    ctx.sql.push_str(" ORDER BY ");
    let mut first = true;
    for (col_name, dir) in prefix
        .iter()
        .map(|(c, d)| (c.as_str(), *d))
        .chain(args.order_by.iter().map(|ob| (ob.column.as_str(), ob.direction)))
    {
        if !first {
            ctx.sql.push_str(", ");
        }
        first = false;
        let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
            path: format!("order_by.{col_name}"),
            message: format!("unknown column '{col_name}' on '{}'", table.exposed_name),
        })?;
        let dir_s = match dir {
            crate::ast::OrderDir::Asc => "ASC",
            crate::ast::OrderDir::Desc => "DESC",
        };
        write!(
            ctx.sql,
            "{table_alias}.{} {dir_s}",
            quote_ident(&col.physical_name)
        )
        .unwrap();
    }
    Ok(())
}
```

- [ ] **Step 4: SQL — add snapshot test**

Append to the `tests` mod in `src/sql.rs`:

```rust
    #[test]
    fn render_distinct_on_auto_prepends_order_by() {
        use crate::ast::RootBody;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs {
                distinct_on: vec!["name".into()],
                ..Default::default()
            },
            body: RootBody::List {
                selection: vec![Field::Column {
                    physical: "id".into(),
                    alias: "id".into(),
                }],
            },
        }]);
        let (sql, _binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }
```

- [ ] **Step 5: Accept snapshot + run tests**

Run: `INSTA_UPDATE=always cargo test --lib`
Expected: all pass; new `render_distinct_on_auto_prepends_order_by.snap` created.

Expected snapshot contains: `SELECT DISTINCT ON (t0."name")` and `ORDER BY t0."name" ASC`.

- [ ] **Step 6: Parser smoke test**

Append to `src/parser.rs` tests:

```rust
    #[test]
    fn parse_distinct_on_list() {
        let op = parse_and_lower(
            "query { users(distinct_on: [name]) { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        assert_eq!(roots[0].args.distinct_on, vec!["name".to_string()]);
    }
```

Run: `cargo test --lib parser::tests::parse_distinct_on_list`
Expected: PASS.

- [ ] **Step 7: Integration test**

Create `tests/integration_distinct.rs`:

```rust
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("events", "public", "events")
                .column("id", "id", PgType::Int4, false)
                .column("kind", "kind", PgType::Text, false)
                .column("ts", "ts", PgType::Int8, false)
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
                CREATE TABLE events (
                    id SERIAL PRIMARY KEY,
                    kind TEXT NOT NULL,
                    ts BIGINT NOT NULL
                );
                INSERT INTO events (kind, ts) VALUES
                    ('click', 10),
                    ('click', 20),
                    ('view',  15),
                    ('view',  25);
                "#,
            )
            .await
            .expect("seed");
    }
    let engine = Engine::new(pool, schema());
    (engine, container)
}

#[tokio::test]
async fn distinct_on_kind_returns_one_per_kind() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            "query { events(distinct_on: [kind]) { kind ts } }",
            None,
        )
        .await
        .expect("query ok");
    let rows = v["events"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    let kinds: Vec<&str> = rows.iter().map(|r| r["kind"].as_str().unwrap()).collect();
    assert!(kinds.contains(&"click"));
    assert!(kinds.contains(&"view"));
}
```

- [ ] **Step 8: Run**

Run: `cargo test --test integration_distinct -- --test-threads=1`
Expected: 1 test PASS.

- [ ] **Step 9: Commit**

```bash
git add src/ast.rs src/sql.rs src/parser.rs src/snapshots/ tests/integration_distinct.rs
git commit -m "feat: distinct_on argument with auto-prepended ORDER BY"
```

---

### Task 10: Verify + tag Phase 3

- [ ] **Step 1: Full test suite**

Run: `cargo test`
Expected: all unit + integration tests pass.

- [ ] **Step 2: Lint**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Format**

Run: `cargo fmt --check`
Expected: clean. Else `cargo fmt` and commit.

- [ ] **Step 4: Tag**

```bash
git tag -a phase-3-aggregates-bypk-distinct -m "Phase 3: aggregates, _by_pk, distinct_on"
```

- [ ] **Step 5: Phase 3 done**

Engine now supports:
- `users_aggregate { aggregate { count, sum { ... }, avg { ... }, max { ... }, min { ... } }, nodes { ... } }`
- `users_by_pk(id: 1) { ... }` returning object or `null`
- `users(distinct_on: [col]) { ... }` with auto-prepended ORDER BY

Phase 4 next: mutations (insert / insert_one / update / update_by_pk / delete / delete_by_pk / on_conflict / returning).
