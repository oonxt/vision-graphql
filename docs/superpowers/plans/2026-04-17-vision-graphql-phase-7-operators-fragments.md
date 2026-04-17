# Vision-GraphQL Phase 7 — More Operators + GraphQL Fragments

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support the remaining Hasura-style comparison operators (`_in`, `_nin`, `_like`, `_ilike`, `_nlike`, `_nilike`, `_is_null`) and GraphQL fragments (named `fragment X on T { ... }` + inline `... on T { ... }`) so the parser accepts real-world queries.

**Architecture:** Add new `CmpOp` variants for `LIKE`/`ILIKE` family, keep `_in`/`_nin` as expansions to `OR`/`AND` chains of `=`/`<>` (no new Bind types needed). Add `BoolExpr::IsNull { column, negated }` for `_is_null`. Parser inlines fragment spreads and inline fragments during lowering — type conditions are recorded but not enforced in MVP. Builder gains `where_in`, `where_like`, `where_is_null` conveniences.

**Tech Stack:** No new deps.

**Out of scope:**
- Type-condition enforcement on fragments (e.g. polymorphism via unions)
- `_gin`, `_contains`, `_contained_in` JSON operators
- `_regex` / `_iregex`

---

## File Structure

```
src/ast.rs        # extend CmpOp, add BoolExpr::IsNull
src/sql.rs        # render new ops
src/parser.rs     # lower new ops + fragments
src/builder.rs    # where_in, where_like, where_is_null
tests/integration_operators.rs   # new
```

---

### Task 1: IR extensions

**Files:**
- Modify: `src/ast.rs`

- [ ] **Step 1: Extend CmpOp and BoolExpr**

In `src/ast.rs`, replace `pub enum CmpOp` with:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    ILike,
    NLike,
    NILike,
}
```

Replace the `BoolExpr` enum — add `IsNull`:

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
    IsNull {
        column: String,
        negated: bool,
    },
    Relation {
        name: String,
        inner: Box<BoolExpr>,
    },
}
```

- [ ] **Step 2: Smoke test**

Append to the `tests` mod in `src/ast.rs`:

```rust
    #[test]
    fn build_is_null_expression() {
        let e = BoolExpr::IsNull {
            column: "name".into(),
            negated: false,
        };
        assert!(matches!(e, BoolExpr::IsNull { .. }));
    }
```

- [ ] **Step 3: Run**

Run: `cargo test --lib ast::tests`
Expected: pass.

- [ ] **Step 4: Make the codebase compile**

Adding variants to `CmpOp` and `BoolExpr` breaks exhaustive matches. In `src/sql.rs`, extend the `op_str` `match op { ... }` inside both `render_bool_expr` and `render_bool_expr_no_alias` to include the new `CmpOp` variants:

```rust
            let op_str = match op {
                CmpOp::Eq => "=",
                CmpOp::Neq => "<>",
                CmpOp::Gt => ">",
                CmpOp::Gte => ">=",
                CmpOp::Lt => "<",
                CmpOp::Lte => "<=",
                CmpOp::Like => "LIKE",
                CmpOp::ILike => "ILIKE",
                CmpOp::NLike => "NOT LIKE",
                CmpOp::NILike => "NOT ILIKE",
            };
```

(Same change in both locations — search for `CmpOp::Eq => "="`.)

In `src/sql.rs`, `render_bool_expr` and `render_bool_expr_no_alias` also need a `BoolExpr::IsNull { .. }` arm. Add before the existing `BoolExpr::Relation` arm in both:

```rust
        BoolExpr::IsNull { column, negated } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            let pred = if *negated { "IS NOT NULL" } else { "IS NULL" };
            write!(
                ctx.sql,
                "{table_alias}.{} {pred}",
                quote_ident(&col.physical_name)
            )
            .unwrap();
            Ok(())
        }
```

Note: `render_bool_expr_no_alias` version omits the `{table_alias}.` prefix:

```rust
        BoolExpr::IsNull { column, negated } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            let pred = if *negated { "IS NOT NULL" } else { "IS NULL" };
            write!(ctx.sql, "{} {pred}", quote_ident(&col.physical_name)).unwrap();
            Ok(())
        }
```

- [ ] **Step 5: Run full suite**

Run: `cargo test --lib`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/ast.rs src/sql.rs
git commit -m "feat(ast,sql): LIKE/ILIKE/NLIKE/NILIKE + IsNull variants"
```

---

### Task 2: Parser — new comparison operators + _is_null

**Files:**
- Modify: `src/parser.rs`

- [ ] **Step 1: Write failing tests**

Append to the `tests` mod in `src/parser.rs`:

```rust
    #[test]
    fn parse_where_like() {
        let op = parse_and_lower(
            r#"query { users(where: {name: {_like: "a%"}}) { id } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::Compare { op, .. } => {
                assert!(matches!(op, crate::ast::CmpOp::Like));
            }
            _ => panic!("expected Compare"),
        }
    }

    #[test]
    fn parse_where_is_null() {
        let op = parse_and_lower(
            r#"query { users(where: {name: {_is_null: true}}) { id } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::IsNull { column, negated } => {
                assert_eq!(column, "name");
                assert!(!negated);
            }
            _ => panic!("expected IsNull"),
        }
    }

    #[test]
    fn parse_where_is_not_null() {
        let op = parse_and_lower(
            r#"query { users(where: {name: {_is_null: false}}) { id } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::IsNull { negated, .. } => assert!(negated),
            _ => panic!("expected IsNull"),
        }
    }

    #[test]
    fn parse_where_in_expands_to_or() {
        let op = parse_and_lower(
            r#"query { users(where: {id: {_in: [1, 2, 3]}}) { id } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::Or(parts) => assert_eq!(parts.len(), 3),
            _ => panic!("expected Or chain"),
        }
    }
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test --lib parser::tests::parse_where_like`
Expected: FAIL — operator not supported.

- [ ] **Step 3: Extend operator lowering**

In `src/parser.rs` `lower_where`, locate the `match op_name.as_str()` block inside the column-name arm. Replace it with:

```rust
                for (op_name, op_val) in op_obj {
                    match op_name.as_str() {
                        "_eq" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Eq,
                            value: op_val.clone(),
                        }),
                        "_neq" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Neq,
                            value: op_val.clone(),
                        }),
                        "_gt" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Gt,
                            value: op_val.clone(),
                        }),
                        "_gte" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Gte,
                            value: op_val.clone(),
                        }),
                        "_lt" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Lt,
                            value: op_val.clone(),
                        }),
                        "_lte" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Lte,
                            value: op_val.clone(),
                        }),
                        "_like" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Like,
                            value: op_val.clone(),
                        }),
                        "_ilike" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::ILike,
                            value: op_val.clone(),
                        }),
                        "_nlike" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::NLike,
                            value: op_val.clone(),
                        }),
                        "_nilike" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::NILike,
                            value: op_val.clone(),
                        }),
                        "_is_null" => {
                            let b = op_val.as_bool().ok_or_else(|| Error::Validate {
                                path: format!("{path}.{col_name}._is_null"),
                                message: "expected boolean".into(),
                            })?;
                            parts.push(BoolExpr::IsNull {
                                column: col.exposed_name.clone(),
                                negated: !b,
                            });
                        }
                        "_in" => {
                            let arr = op_val.as_array().ok_or_else(|| Error::Validate {
                                path: format!("{path}.{col_name}._in"),
                                message: "expected array".into(),
                            })?;
                            let inner: Vec<BoolExpr> = arr
                                .iter()
                                .map(|v| BoolExpr::Compare {
                                    column: col.exposed_name.clone(),
                                    op: CmpOp::Eq,
                                    value: v.clone(),
                                })
                                .collect();
                            parts.push(BoolExpr::Or(inner));
                        }
                        "_nin" => {
                            let arr = op_val.as_array().ok_or_else(|| Error::Validate {
                                path: format!("{path}.{col_name}._nin"),
                                message: "expected array".into(),
                            })?;
                            let inner: Vec<BoolExpr> = arr
                                .iter()
                                .map(|v| BoolExpr::Compare {
                                    column: col.exposed_name.clone(),
                                    op: CmpOp::Neq,
                                    value: v.clone(),
                                })
                                .collect();
                            parts.push(BoolExpr::And(inner));
                        }
                        other => {
                            return Err(Error::Validate {
                                path: format!("{path}.{col_name}"),
                                message: format!("unsupported operator '{other}'"),
                            });
                        }
                    }
                }
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib parser::tests`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): LIKE/ILIKE/NLIKE/NILIKE + _in/_nin/_is_null"
```

---

### Task 3: Parser — GraphQL fragments

**Files:**
- Modify: `src/parser.rs`

- [ ] **Step 1: Write failing test**

Append to tests mod:

```rust
    #[test]
    fn parse_named_fragment() {
        let op = parse_and_lower(
            r#"
            fragment UserFields on users { id name }
            query { users { ...UserFields } }
            "#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!("expected List");
        };
        assert_eq!(selection.len(), 2);
    }

    #[test]
    fn parse_inline_fragment() {
        let op = parse_and_lower(
            r#"query { users { ... on users { id name } } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!("expected List");
        };
        assert_eq!(selection.len(), 2);
    }

    #[test]
    fn parse_unknown_fragment_errors() {
        let err = parse_and_lower(
            r#"query { users { ...MissingFragment } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("MissingFragment"));
    }
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --lib parser::tests::parse_named_fragment`
Expected: FAIL — fragments still rejected.

- [ ] **Step 3: Resolve fragments during lowering**

We need access to the full `ExecutableDocument`'s fragment map when lowering selection sets. Refactor:

1. Introduce a `LowerCtx` struct that carries `schema`, `vars`, and `fragments`.
2. Pass it through `lower_query` / `lower_selection_set` / `lower_mutation*`.
3. Expand `FragmentSpread` and `InlineFragment` by recursing into their inner selection set.

In `src/parser.rs`, near the top (after imports), add:

```rust
use async_graphql_parser::types::{FragmentDefinition, TypeCondition};
use std::collections::HashMap;

struct LowerCtx<'a> {
    schema: &'a Schema,
    vars: &'a Value,
    fragments: HashMap<String, &'a FragmentDefinition>,
}
```

Update `parse_and_lower` to build the fragment map:

```rust
pub fn parse_and_lower(
    source: &str,
    variables: &Value,
    operation_name: Option<&str>,
    schema: &Schema,
) -> Result<Operation> {
    let doc = parse_query(source).map_err(|e| Error::Parse(e.to_string()))?;
    let mut fragments: HashMap<String, &FragmentDefinition> = HashMap::new();
    for (name, def) in &doc.fragments {
        fragments.insert(name.as_str().to_string(), &def.node);
    }
    let op = pick_operation(&doc, operation_name)?;
    let ctx = LowerCtx {
        schema,
        vars: variables,
        fragments,
    };
    match op.ty {
        OperationType::Query => lower_query(op.selection_set, &ctx),
        OperationType::Mutation => lower_mutation(op.selection_set, &ctx),
        OperationType::Subscription => {
            Err(Error::Parse("subscriptions are not supported".into()))
        }
    }
}
```

Now propagate `&LowerCtx` into the other lowering functions. For each `lower_query` / `lower_mutation` / `lower_selection_set` / `lower_args` / `lower_where` / `lower_mutation_field` / `parse_insert_args` / `parse_update_args` / `parse_update_by_pk_args` / `parse_on_conflict` / `lower_aggregate_selection` / `lower_selection_columns_only`, change the parameter list to accept `ctx: &LowerCtx` instead of `schema` and `vars` separately where they're currently passed. This is mechanical.

Minimal change strategy: **keep existing signatures; add `fragments` as an extra parameter only on the walker functions that expand selection sets**. Specifically, change these four functions' signatures:

```rust
fn lower_query(
    set: &SelectionSet,
    schema: &Schema,
    vars: &Value,
    fragments: &HashMap<String, &FragmentDefinition>,
) -> Result<Operation>;

fn lower_mutation(
    set: &SelectionSet,
    schema: &Schema,
    vars: &Value,
    fragments: &HashMap<String, &FragmentDefinition>,
) -> Result<Operation>;

fn lower_selection_set(
    set: &SelectionSet,
    table: &Table,
    schema: &Schema,
    vars: &Value,
    fragments: &HashMap<String, &FragmentDefinition>,
    parent_path: &str,
) -> Result<Vec<Field>>;

fn lower_aggregate_selection(
    set: &SelectionSet,
    table: &Table,
    fragments: &HashMap<String, &FragmentDefinition>,
    parent_path: &str,
) -> Result<(Vec<crate::ast::AggOp>, Option<Vec<Field>>)>;

fn lower_selection_columns_only(
    set: &SelectionSet,
    table: &Table,
    fragments: &HashMap<String, &FragmentDefinition>,
    parent_path: &str,
) -> Result<Vec<Field>>;
```

Update `parse_and_lower` to pass `&fragments` directly (skip the `LowerCtx` struct from the earlier sketch; plain-argument passing is simpler):

```rust
pub fn parse_and_lower(
    source: &str,
    variables: &Value,
    operation_name: Option<&str>,
    schema: &Schema,
) -> Result<Operation> {
    let doc = parse_query(source).map_err(|e| Error::Parse(e.to_string()))?;
    let mut fragments: HashMap<String, &FragmentDefinition> = HashMap::new();
    for (name, def) in &doc.fragments {
        fragments.insert(name.as_str().to_string(), &def.node);
    }
    let op = pick_operation(&doc, operation_name)?;
    match op.ty {
        OperationType::Query => lower_query(op.selection_set, schema, variables, &fragments),
        OperationType::Mutation => lower_mutation(op.selection_set, schema, variables, &fragments),
        OperationType::Subscription => Err(Error::Parse("subscriptions are not supported".into())),
    }
}
```

All existing callers (internal recursion) need updating. Use `cargo build` to find each call site and propagate the extra parameter.

Replace the fragment-rejecting arms in both walkers with real expansion. In `lower_query`:

```rust
    for sel in &set.items {
        match &sel.node {
            Selection::Field(f) => {
                // ... existing code unchanged, except lower_selection_set/lower_args
                // call signatures need to pass `fragments`
            }
            Selection::FragmentSpread(fs) => {
                let name = fs.node.fragment_name.node.as_str();
                let frag = fragments.get(name).ok_or_else(|| Error::Validate {
                    path: "query".into(),
                    message: format!("unknown fragment '{name}'"),
                })?;
                // Recurse into fragment's selection set, inline its roots.
                let inner = lower_query(&frag.selection_set.node, schema, vars, fragments)?;
                if let Operation::Query(mut inner_roots) = inner {
                    roots.append(&mut inner_roots);
                }
            }
            Selection::InlineFragment(ifr) => {
                let inner = lower_query(&ifr.node.selection_set.node, schema, vars, fragments)?;
                if let Operation::Query(mut inner_roots) = inner {
                    roots.append(&mut inner_roots);
                }
            }
        }
    }
```

Similarly in `lower_selection_set`:

```rust
    for sel in &set.items {
        match &sel.node {
            Selection::Field(f) => {
                // ... existing field handling
            }
            Selection::FragmentSpread(fs) => {
                let name = fs.node.fragment_name.node.as_str();
                let frag = fragments.get(name).ok_or_else(|| Error::Validate {
                    path: parent_path.into(),
                    message: format!("unknown fragment '{name}'"),
                })?;
                let mut inner = lower_selection_set(
                    &frag.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
                    parent_path,
                )?;
                out.append(&mut inner);
            }
            Selection::InlineFragment(ifr) => {
                let mut inner = lower_selection_set(
                    &ifr.node.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
                    parent_path,
                )?;
                out.append(&mut inner);
            }
        }
    }
```

`lower_selection_columns_only` and `lower_aggregate_selection` get the same fragment-expansion logic (delete the `fragments not supported inside ...` errors and call recursively).

The `TypeCondition` import isn't needed for MVP — fragments are applied without type-checking. Drop that from the import if unused.

**Keep the existing helper functions' signatures unchanged** for `lower_args`, `lower_where`, `parse_insert_args`, `parse_update_args`, `parse_update_by_pk_args`, `parse_on_conflict`, `parse_returning`, `lower_mutation_field` — none of them walk fragment spreads, so they don't need the `fragments` parameter.

- [ ] **Step 4: Compile iteratively**

Run: `cargo build 2>&1 | head -40`

Expect compile errors about missing `fragments` parameter on call sites. Walk each error and add `&fragments` (inside `parse_and_lower`) or `fragments` (inside recursive callers) to the call.

Key call sites to update:
- `lower_query` → `lower_selection_set(...)` → add `fragments` arg
- `lower_query` → `lower_args(...)` → unchanged
- `lower_mutation` → `lower_mutation_field(...)` → unchanged
- `lower_mutation_field` → `lower_selection_columns_only(...)` → add `fragments` arg
- `lower_mutation_field` → `parse_returning(...)` (which calls `lower_selection_columns_only`) — update `parse_returning` to take `fragments` and forward
- `lower_selection_set` → recursive self call → forward `fragments`
- `lower_selection_set` → `lower_args(...)` → unchanged

Minimal cascade: also update `parse_returning`:

```rust
fn parse_returning(
    set: &SelectionSet,
    table: &Table,
    fragments: &HashMap<String, &FragmentDefinition>,
    parent_path: &str,
) -> Result<Vec<Field>> {
    // ...existing body, but change the call to:
    // lower_selection_columns_only(&field.selection_set.node, table, fragments, &format!("{parent_path}.returning"))
}
```

And update all `parse_returning(...)` call sites inside `lower_mutation_field` to pass `fragments`.

- [ ] **Step 5: Run fragment tests**

Run: `cargo test --lib parser::tests`
Expected: all pass including 3 new fragment tests.

- [ ] **Step 6: Run full lib**

Run: `cargo test --lib`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): expand named and inline GraphQL fragments"
```

---

### Task 4: Builder conveniences

**Files:**
- Modify: `src/builder.rs`

- [ ] **Step 1: Write failing tests**

Append to the `tests` mod in `src/builder.rs`:

```rust
    #[test]
    fn query_builder_where_in() {
        let rf = Query::from("users").where_in("id", &[json!(1), json!(2)]).build();
        match rf.args.where_.as_ref().unwrap() {
            BoolExpr::Or(parts) => assert_eq!(parts.len(), 2),
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn query_builder_where_like() {
        let rf = Query::from("users")
            .where_like("name", "a%")
            .build();
        match rf.args.where_.as_ref().unwrap() {
            BoolExpr::Compare { op, .. } => assert!(matches!(op, CmpOp::Like)),
            _ => panic!("expected Compare"),
        }
    }

    #[test]
    fn query_builder_where_is_null() {
        let rf = Query::from("users").where_is_null("name").build();
        match rf.args.where_.as_ref().unwrap() {
            BoolExpr::IsNull { negated, .. } => assert!(!negated),
            _ => panic!("expected IsNull"),
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib builder::tests::query_builder_where_in`
Expected: FAIL.

- [ ] **Step 3: Add convenience methods**

In `impl QueryBuilder`, before the `build` method, append:

```rust
    pub fn where_in(mut self, col: impl Into<String>, values: &[Value]) -> Self {
        let column = col.into();
        let parts: Vec<BoolExpr> = values
            .iter()
            .map(|v| BoolExpr::Compare {
                column: column.clone(),
                op: CmpOp::Eq,
                value: v.clone(),
            })
            .collect();
        self.args.where_ = Some(merge_and(self.args.where_.take(), BoolExpr::Or(parts)));
        self
    }

    pub fn where_like(self, col: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.where_cmp(col, CmpOp::Like, Value::String(pattern.into()))
    }

    pub fn where_ilike(self, col: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.where_cmp(col, CmpOp::ILike, Value::String(pattern.into()))
    }

    pub fn where_is_null(mut self, col: impl Into<String>) -> Self {
        let e = BoolExpr::IsNull {
            column: col.into(),
            negated: false,
        };
        self.args.where_ = Some(merge_and(self.args.where_.take(), e));
        self
    }

    pub fn where_is_not_null(mut self, col: impl Into<String>) -> Self {
        let e = BoolExpr::IsNull {
            column: col.into(),
            negated: true,
        };
        self.args.where_ = Some(merge_and(self.args.where_.take(), e));
        self
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib builder::tests`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/builder.rs
git commit -m "feat(builder): where_in / where_like / where_is_null conveniences"
```

---

### Task 5: Integration tests

**Files:**
- Create: `tests/integration_operators.rs`

- [ ] **Step 1: Write test file**

Create `tests/integration_operators.rs`:

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
                .column("name", "name", PgType::Text, true)
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
                    name TEXT
                );
                INSERT INTO users (name) VALUES ('alice'), ('bob'), ('carol'), (NULL);
                "#,
            )
            .await
            .expect("seed");
    }
    (Engine::new(pool, schema()), container)
}

#[tokio::test]
async fn in_operator_matches_multiple_values() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_in: ["alice", "bob"]}}) { name } }"#,
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn like_matches_pattern() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_like: "a%"}}) { name } }"#,
            None,
        )
        .await
        .expect("query ok");
    let arr = v["users"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], json!("alice"));
}

#[tokio::test]
async fn ilike_case_insensitive() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_ilike: "ALICE"}}) { name } }"#,
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn is_null_filter() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_is_null: true}}) { id } }"#,
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn is_not_null_filter() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_is_null: false}}) { id } }"#,
            None,
        )
        .await
        .expect("query ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn named_fragment_works_against_db() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"
            fragment UF on users { id name }
            query { users(where: {name: {_eq: "alice"}}) { ...UF } }
            "#,
            None,
        )
        .await
        .expect("query ok");
    let arr = v["users"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], json!("alice"));
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test integration_operators -- --test-threads=1`
Expected: 6 PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_operators.rs
git commit -m "test: e2e operators (_in/_like/_ilike/_is_null) + fragments"
```

---

### Task 6: Verify + tag Phase 7

- [ ] **Step 1: Full suite**

Run: `cargo test`
Expected: all pass.

- [ ] **Step 2: Lint**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Format**

Run: `cargo fmt --check`
Expected: clean (else `cargo fmt` + commit).

- [ ] **Step 4: Tag**

```bash
git tag -a phase-7-operators-fragments -m "Phase 7: more operators + GraphQL fragments"
```

- [ ] **Step 5: Done**

Parser now accepts the common Hasura operators and fragment references. Builder exposes ergonomic `where_in` / `where_like` / `where_is_null`.
