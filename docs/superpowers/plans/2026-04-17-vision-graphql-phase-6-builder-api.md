# Vision-GraphQL Phase 6 — Rust Builder API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose an ergonomic builder API for constructing queries and mutations in Rust without GraphQL strings. Both paths (string + builder) feed the same IR, so the existing SQL generator and executor just work.

**Architecture:** A new `src/builder.rs` module provides `QueryBuilder`, `AggregateBuilder`, `ByPkBuilder`, and `MutationBuilder`. Each assembles an `Operation` via a chain of typed methods and exposes `.build()` to produce the IR. An `IntoOperation` trait lets `Engine::run` accept any builder or a raw `Operation`. Runtime validation against the schema happens when the engine runs the operation — same code path as parsed GraphQL.

**Tech Stack:** No new deps.

**Out of scope:**
- Compile-time schema types (would require a proc-macro, phase 7+)
- Query reuse / prepared builder caching
- `_inc` / `_append` / `_prepend` column mutation ops

---

## File Structure

```
src/builder.rs         # new: QueryBuilder, AggregateBuilder, ByPkBuilder, MutationBuilder, IntoOperation
src/lib.rs             # pub use builder::*
src/engine.rs          # add Engine::run<O: IntoOperation>
tests/integration_builder.rs   # new: e2e builder → engine → PG
src/builder/tests.rs   # unit tests for IR equivalence with parser
```

Single file `builder.rs` keeps the API surface discoverable; submodule split can happen later if it grows.

---

### Task 1: IntoOperation trait + Engine::run

**Files:**
- Create: `src/builder.rs`
- Modify: `src/engine.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Create src/builder.rs with IntoOperation**

Write `src/builder.rs`:

```rust
//! Ergonomic builder API for constructing queries and mutations.
//!
//! Both the GraphQL parser and these builders produce the same IR,
//! so the engine runs them through the same pipeline.

use crate::ast::{Operation, RootField};

/// Anything that can be turned into an [`Operation`] for `Engine::run`.
pub trait IntoOperation {
    fn into_operation(self) -> Operation;
}

impl IntoOperation for Operation {
    fn into_operation(self) -> Operation {
        self
    }
}

impl IntoOperation for RootField {
    fn into_operation(self) -> Operation {
        Operation::Query(vec![self])
    }
}
```

- [ ] **Step 2: Add module declaration to lib.rs**

Edit `src/lib.rs`. After the existing `pub mod` lines, add:

```rust
pub mod builder;
```

Inside the re-exports block, add:

```rust
pub use builder::IntoOperation;
```

- [ ] **Step 3: Add Engine::run**

In `src/engine.rs`, append to `impl Engine`:

```rust
    /// Execute any [`IntoOperation`] (builders, raw `RootField`, or `Operation`).
    pub async fn run(&self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        let operation = op.into_operation();
        let (sql, binds) = render(&operation, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing");
        crate::executor::execute(&self.pool, &sql, &binds).await
    }
```

- [ ] **Step 4: Compile + run existing tests**

Run: `cargo build`
Expected: clean compile.

Run: `cargo test --lib`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/builder.rs src/engine.rs src/lib.rs
git commit -m "feat(builder): IntoOperation trait and Engine::run entry point"
```

---

### Task 2: QueryBuilder for list selects

**Files:**
- Modify: `src/builder.rs`

- [ ] **Step 1: Write failing test**

Append to `src/builder.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Field, RootBody};

    #[test]
    fn query_builder_basic_shape() {
        let rf = Query::from("users")
            .select(&["id", "name"])
            .where_eq("active", true)
            .limit(10)
            .build();
        assert_eq!(rf.table, "users");
        assert_eq!(rf.alias, "users");
        assert_eq!(rf.args.limit, Some(10));
        let RootBody::List { selection } = &rf.body else {
            panic!("expected List");
        };
        assert_eq!(selection.len(), 2);
        match &selection[0] {
            Field::Column { physical, alias } => {
                assert_eq!(physical, "id");
                assert_eq!(alias, "id");
            }
            _ => panic!("expected Column"),
        }
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --lib builder::tests::query_builder_basic_shape`
Expected: FAIL — `Query` / `QueryBuilder` missing.

- [ ] **Step 3: Implement Query + QueryBuilder**

Append to `src/builder.rs` (before the `#[cfg(test)]`):

```rust
use crate::ast::{BoolExpr, CmpOp, Field, OrderBy, OrderDir, QueryArgs, RootBody};
use serde_json::Value;

/// Entry point for query builders.
pub struct Query;

impl Query {
    /// Start a list-select builder for `table` (exposed name).
    pub fn from(table: impl Into<String>) -> QueryBuilder {
        let t: String = table.into();
        QueryBuilder {
            table: t.clone(),
            alias: t,
            args: QueryArgs::default(),
            selection: Vec::new(),
        }
    }
}

pub struct QueryBuilder {
    table: String,
    alias: String,
    args: QueryArgs,
    selection: Vec<Field>,
}

impl QueryBuilder {
    /// Set the output alias (defaults to the table name).
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = alias.into();
        self
    }

    /// Replace the current selection with this column list.
    pub fn select(mut self, cols: &[&str]) -> Self {
        self.selection = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    /// Add a single column to the selection.
    pub fn column(mut self, col: impl Into<String>) -> Self {
        let c: String = col.into();
        self.selection.push(Field::Column {
            physical: c.clone(),
            alias: c,
        });
        self
    }

    /// Set the entire `where` expression, replacing any previous filter.
    pub fn where_expr(mut self, expr: BoolExpr) -> Self {
        self.args.where_ = Some(expr);
        self
    }

    /// Add an AND-combined `col = value` filter.
    pub fn where_eq(self, col: impl Into<String>, value: impl Into<Value>) -> Self {
        self.where_cmp(col, CmpOp::Eq, value.into())
    }

    /// Add an AND-combined comparison filter.
    pub fn where_cmp(
        mut self,
        col: impl Into<String>,
        op: CmpOp,
        value: Value,
    ) -> Self {
        let cmp = BoolExpr::Compare {
            column: col.into(),
            op,
            value,
        };
        self.args.where_ = Some(match self.args.where_.take() {
            None => cmp,
            Some(BoolExpr::And(mut parts)) => {
                parts.push(cmp);
                BoolExpr::And(parts)
            }
            Some(other) => BoolExpr::And(vec![other, cmp]),
        });
        self
    }

    pub fn order_by(mut self, col: impl Into<String>, direction: OrderDir) -> Self {
        self.args.order_by.push(OrderBy {
            column: col.into(),
            direction,
        });
        self
    }

    pub fn limit(mut self, n: u64) -> Self {
        self.args.limit = Some(n);
        self
    }

    pub fn offset(mut self, n: u64) -> Self {
        self.args.offset = Some(n);
        self
    }

    pub fn distinct_on(mut self, cols: &[&str]) -> Self {
        self.args.distinct_on = cols.iter().map(|s| (*s).to_string()).collect();
        self
    }

    pub fn build(self) -> RootField {
        RootField {
            table: self.table,
            alias: self.alias,
            args: self.args,
            body: RootBody::List {
                selection: self.selection,
            },
        }
    }
}

impl IntoOperation for QueryBuilder {
    fn into_operation(self) -> Operation {
        Operation::Query(vec![self.build()])
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib builder`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/builder.rs
git commit -m "feat(builder): QueryBuilder for list selects"
```

---

### Task 3: Nested relations in QueryBuilder

**Files:**
- Modify: `src/builder.rs`

- [ ] **Step 1: Write failing test**

Append to the `tests` mod:

```rust
    #[test]
    fn query_builder_nested_relation() {
        let rf = Query::from("users")
            .select(&["id"])
            .with_relation(
                "posts",
                Query::from("posts").select(&["title"]).limit(5),
            )
            .build();
        let RootBody::List { selection } = &rf.body else {
            panic!("expected List");
        };
        assert_eq!(selection.len(), 2);
        match &selection[1] {
            Field::Relation {
                name,
                args,
                selection,
                ..
            } => {
                assert_eq!(name, "posts");
                assert_eq!(args.limit, Some(5));
                assert_eq!(selection.len(), 1);
            }
            _ => panic!("expected Relation"),
        }
    }
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --lib builder::tests::query_builder_nested_relation`
Expected: FAIL.

- [ ] **Step 3: Add with_relation method**

In `impl QueryBuilder`, append:

```rust
    /// Nest a related-table builder into the current selection.
    pub fn with_relation(mut self, name: impl Into<String>, nested: QueryBuilder) -> Self {
        let n: String = name.into();
        self.selection.push(Field::Relation {
            name: n.clone(),
            alias: n,
            args: nested.args,
            selection: nested.selection,
        });
        self
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib builder`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/builder.rs
git commit -m "feat(builder): with_relation for nested selections"
```

---

### Task 4: AggregateBuilder and ByPkBuilder

**Files:**
- Modify: `src/builder.rs`

- [ ] **Step 1: Write failing tests**

Append to tests mod:

```rust
    #[test]
    fn aggregate_builder_count_sum() {
        use crate::ast::AggOp;
        let rf = Query::aggregate("users").count().sum(&["age"]).build();
        assert_eq!(rf.alias, "users_aggregate");
        let RootBody::Aggregate { ops, nodes } = &rf.body else {
            panic!("expected Aggregate");
        };
        assert_eq!(ops.len(), 2);
        assert!(matches!(ops[0], AggOp::Count));
        assert!(nodes.is_none());
    }

    #[test]
    fn aggregate_builder_with_nodes() {
        let rf = Query::aggregate("users")
            .count()
            .nodes(&["id"])
            .build();
        let RootBody::Aggregate { nodes, .. } = &rf.body else {
            panic!("expected Aggregate");
        };
        assert_eq!(nodes.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn by_pk_builder() {
        use serde_json::json;
        let rf = Query::by_pk("users", &[("id", json!(7))])
            .select(&["name"])
            .build();
        assert_eq!(rf.alias, "users_by_pk");
        let RootBody::ByPk { pk, selection } = &rf.body else {
            panic!("expected ByPk");
        };
        assert_eq!(pk.len(), 1);
        assert_eq!(pk[0].0, "id");
        assert_eq!(pk[0].1, json!(7));
        assert_eq!(selection.len(), 1);
    }
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test --lib builder::tests::aggregate_builder_count_sum`
Expected: FAIL.

- [ ] **Step 3: Implement AggregateBuilder and ByPkBuilder**

Extend `impl Query`:

```rust
impl Query {
    /// Start an aggregate builder (`<table>_aggregate` root field).
    pub fn aggregate(table: impl Into<String>) -> AggregateBuilder {
        let t: String = table.into();
        AggregateBuilder {
            table: t.clone(),
            alias: format!("{t}_aggregate"),
            args: QueryArgs::default(),
            ops: Vec::new(),
            nodes: None,
        }
    }

    /// Start a by-primary-key builder (`<table>_by_pk` root field).
    pub fn by_pk(
        table: impl Into<String>,
        pk: &[(impl Into<String> + Clone, Value)],
    ) -> ByPkBuilder {
        let t: String = table.into();
        ByPkBuilder {
            table: t.clone(),
            alias: format!("{t}_by_pk"),
            pk: pk
                .iter()
                .map(|(k, v)| (k.clone().into(), v.clone()))
                .collect(),
            selection: Vec::new(),
        }
    }
}

pub struct AggregateBuilder {
    table: String,
    alias: String,
    args: QueryArgs,
    ops: Vec<crate::ast::AggOp>,
    nodes: Option<Vec<Field>>,
}

impl AggregateBuilder {
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = alias.into();
        self
    }

    pub fn where_expr(mut self, expr: BoolExpr) -> Self {
        self.args.where_ = Some(expr);
        self
    }

    pub fn where_eq(mut self, col: impl Into<String>, value: impl Into<Value>) -> Self {
        let cmp = BoolExpr::Compare {
            column: col.into(),
            op: CmpOp::Eq,
            value: value.into(),
        };
        self.args.where_ = Some(match self.args.where_.take() {
            None => cmp,
            Some(BoolExpr::And(mut parts)) => {
                parts.push(cmp);
                BoolExpr::And(parts)
            }
            Some(other) => BoolExpr::And(vec![other, cmp]),
        });
        self
    }

    pub fn count(mut self) -> Self {
        self.ops.push(crate::ast::AggOp::Count);
        self
    }

    pub fn sum(mut self, cols: &[&str]) -> Self {
        self.ops.push(crate::ast::AggOp::Sum {
            columns: cols.iter().map(|s| (*s).to_string()).collect(),
        });
        self
    }

    pub fn avg(mut self, cols: &[&str]) -> Self {
        self.ops.push(crate::ast::AggOp::Avg {
            columns: cols.iter().map(|s| (*s).to_string()).collect(),
        });
        self
    }

    pub fn max(mut self, cols: &[&str]) -> Self {
        self.ops.push(crate::ast::AggOp::Max {
            columns: cols.iter().map(|s| (*s).to_string()).collect(),
        });
        self
    }

    pub fn min(mut self, cols: &[&str]) -> Self {
        self.ops.push(crate::ast::AggOp::Min {
            columns: cols.iter().map(|s| (*s).to_string()).collect(),
        });
        self
    }

    pub fn nodes(mut self, cols: &[&str]) -> Self {
        self.nodes = Some(
            cols.iter()
                .map(|c| Field::Column {
                    physical: (*c).to_string(),
                    alias: (*c).to_string(),
                })
                .collect(),
        );
        self
    }

    pub fn build(self) -> RootField {
        RootField {
            table: self.table,
            alias: self.alias,
            args: self.args,
            body: RootBody::Aggregate {
                ops: self.ops,
                nodes: self.nodes,
            },
        }
    }
}

impl IntoOperation for AggregateBuilder {
    fn into_operation(self) -> Operation {
        Operation::Query(vec![self.build()])
    }
}

pub struct ByPkBuilder {
    table: String,
    alias: String,
    pk: Vec<(String, Value)>,
    selection: Vec<Field>,
}

impl ByPkBuilder {
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = alias.into();
        self
    }

    pub fn select(mut self, cols: &[&str]) -> Self {
        self.selection = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn with_relation(mut self, name: impl Into<String>, nested: QueryBuilder) -> Self {
        let n: String = name.into();
        self.selection.push(Field::Relation {
            name: n.clone(),
            alias: n,
            args: nested.args,
            selection: nested.selection,
        });
        self
    }

    pub fn build(self) -> RootField {
        RootField {
            table: self.table,
            alias: self.alias,
            args: QueryArgs::default(),
            body: RootBody::ByPk {
                pk: self.pk,
                selection: self.selection,
            },
        }
    }
}

impl IntoOperation for ByPkBuilder {
    fn into_operation(self) -> Operation {
        Operation::Query(vec![self.build()])
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib builder`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/builder.rs
git commit -m "feat(builder): AggregateBuilder and ByPkBuilder"
```

---

### Task 5: Mutation builders

**Files:**
- Modify: `src/builder.rs`

- [ ] **Step 1: Write failing tests**

Append to tests mod:

```rust
    #[test]
    fn mutation_insert_one() {
        use serde_json::json;
        let op = Mutation::insert_one(
            "users",
            [("name", json!("alice"))],
        )
        .returning(&["id"])
        .into_operation();
        match op {
            Operation::Mutation(fields) => match &fields[0] {
                crate::ast::MutationField::Insert { objects, one, .. } => {
                    assert_eq!(objects.len(), 1);
                    assert!(one);
                }
                _ => panic!("expected Insert"),
            },
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn mutation_update_by_where() {
        use serde_json::json;
        let op = Mutation::update("users")
            .where_eq("id", 1)
            .set("name", json!("z"))
            .returning(&["name"])
            .into_operation();
        match op {
            Operation::Mutation(fields) => match &fields[0] {
                crate::ast::MutationField::Update { set, returning, .. } => {
                    assert!(set.contains_key("name"));
                    assert_eq!(returning.len(), 1);
                }
                _ => panic!("expected Update"),
            },
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn mutation_delete_by_pk() {
        use serde_json::json;
        let op = Mutation::delete_by_pk("users", &[("id", json!(5))])
            .select(&["id"])
            .into_operation();
        match op {
            Operation::Mutation(fields) => match &fields[0] {
                crate::ast::MutationField::DeleteByPk { pk, selection, .. } => {
                    assert_eq!(pk.len(), 1);
                    assert_eq!(selection.len(), 1);
                }
                _ => panic!("expected DeleteByPk"),
            },
            _ => panic!("expected Mutation"),
        }
    }
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test --lib builder::tests::mutation_insert_one`
Expected: FAIL.

- [ ] **Step 3: Implement Mutation + builders**

Append to `src/builder.rs`:

```rust
use crate::ast::{MutationField, OnConflict};
use std::collections::BTreeMap;

/// Entry point for mutation builders.
pub struct Mutation;

impl Mutation {
    pub fn insert(
        table: impl Into<String>,
        objects: Vec<BTreeMap<String, Value>>,
    ) -> InsertBuilder {
        let t: String = table.into();
        InsertBuilder {
            alias: format!("insert_{t}"),
            table: t,
            objects,
            on_conflict: None,
            returning: Vec::new(),
            one: false,
        }
    }

    pub fn insert_one<I, K>(table: impl Into<String>, obj: I) -> InsertBuilder
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let t: String = table.into();
        let map: BTreeMap<String, Value> = obj
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect();
        InsertBuilder {
            alias: format!("insert_{t}_one"),
            table: t,
            objects: vec![map],
            on_conflict: None,
            returning: Vec::new(),
            one: true,
        }
    }

    pub fn update(table: impl Into<String>) -> UpdateBuilder {
        let t: String = table.into();
        UpdateBuilder {
            alias: format!("update_{t}"),
            table: t,
            where_: None,
            set: BTreeMap::new(),
            returning: Vec::new(),
        }
    }

    pub fn update_by_pk(
        table: impl Into<String>,
        pk: &[(impl Into<String> + Clone, Value)],
    ) -> UpdateByPkBuilder {
        let t: String = table.into();
        UpdateByPkBuilder {
            alias: format!("update_{t}_by_pk"),
            table: t,
            pk: pk
                .iter()
                .map(|(k, v)| (k.clone().into(), v.clone()))
                .collect(),
            set: BTreeMap::new(),
            selection: Vec::new(),
        }
    }

    pub fn delete(table: impl Into<String>) -> DeleteBuilder {
        let t: String = table.into();
        DeleteBuilder {
            alias: format!("delete_{t}"),
            table: t,
            where_: None,
            returning: Vec::new(),
        }
    }

    pub fn delete_by_pk(
        table: impl Into<String>,
        pk: &[(impl Into<String> + Clone, Value)],
    ) -> DeleteByPkBuilder {
        let t: String = table.into();
        DeleteByPkBuilder {
            alias: format!("delete_{t}_by_pk"),
            table: t,
            pk: pk
                .iter()
                .map(|(k, v)| (k.clone().into(), v.clone()))
                .collect(),
            selection: Vec::new(),
        }
    }
}

pub struct InsertBuilder {
    alias: String,
    table: String,
    objects: Vec<BTreeMap<String, Value>>,
    on_conflict: Option<OnConflict>,
    returning: Vec<Field>,
    one: bool,
}

impl InsertBuilder {
    pub fn alias(mut self, a: impl Into<String>) -> Self {
        self.alias = a.into();
        self
    }

    pub fn on_conflict(mut self, oc: OnConflict) -> Self {
        self.on_conflict = Some(oc);
        self
    }

    pub fn returning(mut self, cols: &[&str]) -> Self {
        self.returning = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn build(self) -> MutationField {
        MutationField::Insert {
            alias: self.alias,
            table: self.table,
            objects: self.objects,
            on_conflict: self.on_conflict,
            returning: self.returning,
            one: self.one,
        }
    }
}

impl IntoOperation for InsertBuilder {
    fn into_operation(self) -> Operation {
        Operation::Mutation(vec![self.build()])
    }
}

pub struct UpdateBuilder {
    alias: String,
    table: String,
    where_: Option<BoolExpr>,
    set: BTreeMap<String, Value>,
    returning: Vec<Field>,
}

impl UpdateBuilder {
    pub fn alias(mut self, a: impl Into<String>) -> Self {
        self.alias = a.into();
        self
    }

    pub fn where_expr(mut self, expr: BoolExpr) -> Self {
        self.where_ = Some(expr);
        self
    }

    pub fn where_eq(mut self, col: impl Into<String>, value: impl Into<Value>) -> Self {
        let cmp = BoolExpr::Compare {
            column: col.into(),
            op: CmpOp::Eq,
            value: value.into(),
        };
        self.where_ = Some(match self.where_.take() {
            None => cmp,
            Some(BoolExpr::And(mut parts)) => {
                parts.push(cmp);
                BoolExpr::And(parts)
            }
            Some(other) => BoolExpr::And(vec![other, cmp]),
        });
        self
    }

    pub fn set(mut self, col: impl Into<String>, value: Value) -> Self {
        self.set.insert(col.into(), value);
        self
    }

    pub fn returning(mut self, cols: &[&str]) -> Self {
        self.returning = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn build(self) -> MutationField {
        MutationField::Update {
            alias: self.alias,
            table: self.table,
            where_: self
                .where_
                .expect("update builder: where clause required"),
            set: self.set,
            returning: self.returning,
        }
    }
}

impl IntoOperation for UpdateBuilder {
    fn into_operation(self) -> Operation {
        Operation::Mutation(vec![self.build()])
    }
}

pub struct UpdateByPkBuilder {
    alias: String,
    table: String,
    pk: Vec<(String, Value)>,
    set: BTreeMap<String, Value>,
    selection: Vec<Field>,
}

impl UpdateByPkBuilder {
    pub fn set(mut self, col: impl Into<String>, value: Value) -> Self {
        self.set.insert(col.into(), value);
        self
    }

    pub fn select(mut self, cols: &[&str]) -> Self {
        self.selection = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn build(self) -> MutationField {
        MutationField::UpdateByPk {
            alias: self.alias,
            table: self.table,
            pk: self.pk,
            set: self.set,
            selection: self.selection,
        }
    }
}

impl IntoOperation for UpdateByPkBuilder {
    fn into_operation(self) -> Operation {
        Operation::Mutation(vec![self.build()])
    }
}

pub struct DeleteBuilder {
    alias: String,
    table: String,
    where_: Option<BoolExpr>,
    returning: Vec<Field>,
}

impl DeleteBuilder {
    pub fn where_expr(mut self, expr: BoolExpr) -> Self {
        self.where_ = Some(expr);
        self
    }

    pub fn where_eq(mut self, col: impl Into<String>, value: impl Into<Value>) -> Self {
        let cmp = BoolExpr::Compare {
            column: col.into(),
            op: CmpOp::Eq,
            value: value.into(),
        };
        self.where_ = Some(match self.where_.take() {
            None => cmp,
            Some(BoolExpr::And(mut parts)) => {
                parts.push(cmp);
                BoolExpr::And(parts)
            }
            Some(other) => BoolExpr::And(vec![other, cmp]),
        });
        self
    }

    pub fn returning(mut self, cols: &[&str]) -> Self {
        self.returning = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn build(self) -> MutationField {
        MutationField::Delete {
            alias: self.alias,
            table: self.table,
            where_: self
                .where_
                .expect("delete builder: where clause required"),
            returning: self.returning,
        }
    }
}

impl IntoOperation for DeleteBuilder {
    fn into_operation(self) -> Operation {
        Operation::Mutation(vec![self.build()])
    }
}

pub struct DeleteByPkBuilder {
    alias: String,
    table: String,
    pk: Vec<(String, Value)>,
    selection: Vec<Field>,
}

impl DeleteByPkBuilder {
    pub fn select(mut self, cols: &[&str]) -> Self {
        self.selection = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn build(self) -> MutationField {
        MutationField::DeleteByPk {
            alias: self.alias,
            table: self.table,
            pk: self.pk,
            selection: self.selection,
        }
    }
}

impl IntoOperation for DeleteByPkBuilder {
    fn into_operation(self) -> Operation {
        Operation::Mutation(vec![self.build()])
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib builder`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/builder.rs
git commit -m "feat(builder): Insert/Update/Delete mutation builders"
```

---

### Task 6: Re-exports in lib.rs

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Add crate-level re-exports**

Update `src/lib.rs` to expose builders at the crate root:

```rust
pub use builder::{
    AggregateBuilder, ByPkBuilder, DeleteBuilder, DeleteByPkBuilder, InsertBuilder,
    IntoOperation, Mutation, Query, QueryBuilder, UpdateBuilder, UpdateByPkBuilder,
};
```

(Append to the existing re-export block near the bottom of `src/lib.rs`.)

- [ ] **Step 2: Build**

Run: `cargo build`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add src/lib.rs
git commit -m "feat(builder): re-export builder types at crate root"
```

---

### Task 7: Integration test — builders against live PG

**Files:**
- Create: `tests/integration_builder.rs`

- [ ] **Step 1: Write test file**

Create `tests/integration_builder.rs`:

```rust
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::{Engine, Mutation, Query};

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .column("age", "age", PgType::Int4, true)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")])),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .primary_key(&["id"])
                .relation("user", Relation::object("users").on([("user_id", "id")])),
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
                    age INT
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id)
                );
                INSERT INTO users (name, age) VALUES ('alice', 30), ('bob', 20);
                INSERT INTO posts (title, user_id) VALUES ('p1', 1), ('p2', 2);
                "#,
            )
            .await
            .expect("seed");
    }
    (Engine::new(pool, schema()), container)
}

#[tokio::test]
async fn builder_query_with_relation() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .run(
            Query::from("users")
                .select(&["name"])
                .with_relation("posts", Query::from("posts").select(&["title"]))
                .order_by("id", vision_graphql::ast::OrderDir::Asc),
        )
        .await
        .expect("run ok");
    let users = v["users"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["posts"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn builder_aggregate() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .run(Query::aggregate("users").count().sum(&["age"]))
        .await
        .expect("run ok");
    assert_eq!(v["users_aggregate"]["aggregate"]["count"], json!(2));
    assert_eq!(v["users_aggregate"]["aggregate"]["sum"]["age"], json!(50));
}

#[tokio::test]
async fn builder_by_pk() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .run(Query::by_pk("users", &[("id", json!(1))]).select(&["name"]))
        .await
        .expect("run ok");
    assert_eq!(v["users_by_pk"]["name"], json!("alice"));
}

#[tokio::test]
async fn builder_insert_and_update() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .run(
            Mutation::insert_one("users", [("name", json!("cara"))])
                .returning(&["id", "name"]),
        )
        .await
        .expect("insert ok");
    let id = v["insert_users_one"]["id"].as_i64().unwrap();

    let v: Value = engine
        .run(
            Mutation::update_by_pk("users", &[("id", json!(id as i32))])
                .set("age", json!(99))
                .select(&["age"]),
        )
        .await
        .expect("update ok");
    assert_eq!(v["update_users_by_pk"]["age"], json!(99));
}

#[tokio::test]
async fn builder_delete_by_where() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .run(
            Mutation::delete("users")
                .where_eq("name", "alice")
                .returning(&["id"]),
        )
        .await
        .expect("delete ok");
    assert_eq!(v["delete_users"]["affected_rows"], json!(0)); // posts FK prevents it
    // The delete actually fails at DB level due to FK; expected as error. Re-run against a table without FKs:
    // We accept either a 0 affected_rows or a DB error — robust test replaces with a posts delete first
    let _ = v;
}
```

Note: the last test case needs adjustment — delete from `users` where posts reference will fail with FK violation. Rewrite:

Replace the `builder_delete_by_where` function with:

```rust
#[tokio::test]
async fn builder_delete_by_where() {
    let (engine, _c) = setup().await;
    // Delete all posts first to avoid FK violation
    let _ = engine
        .run(Mutation::delete("posts").where_eq("user_id", 1))
        .await
        .expect("delete posts ok");
    let v: Value = engine
        .run(
            Mutation::delete("users")
                .where_eq("name", "alice")
                .returning(&["id"]),
        )
        .await
        .expect("delete ok");
    assert_eq!(v["delete_users"]["affected_rows"], json!(1));
}
```

`ast::OrderDir` is used in `builder_query_with_relation`; `vision_graphql::ast` is already a `pub mod`.

- [ ] **Step 2: Run**

Run: `cargo test --test integration_builder -- --test-threads=1`
Expected: 5 PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_builder.rs
git commit -m "test: e2e builder API for queries and mutations"
```

---

### Task 8: Verify + tag Phase 6

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
git tag -a phase-6-builder-api -m "Phase 6: Rust builder API"
```

- [ ] **Step 5: Done**

Builder API is available at the crate root. Users can chain `Query::from("users")…build()` or pass builders directly to `engine.run(...)`.
