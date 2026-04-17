# Vision-GraphQL Phase 1 — Core SELECT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a working end-to-end vertical slice: `engine.query(graphql_string, variables)` executes a single-table SELECT (optionally with `_eq` filter, `order_by`, `limit`, `offset`) against PostgreSQL and returns a `serde_json::Value` in Hasura's data shape.

**Architecture:** Library-only Rust crate. GraphQL string → `async-graphql-parser` → IR → SQL+binds (using PG `json_agg`/`row_to_json`) → `deadpool-postgres` execution → `serde_json::Value`. Schema is constructed manually in tests for this phase; introspection comes in Phase 5.

**Tech Stack:** Rust, tokio, deadpool-postgres, tokio-postgres, async-graphql-parser, serde_json, thiserror, tracing. Dev: insta (snapshot), testcontainers-modules (postgres), pretty_assertions.

**Out of scope for this phase:** Nested relations, aggregates, mutations, `on_conflict`, schema introspection, TOML config, Rust builder API, fragments, all comparison operators beyond `_eq`/`_neq`/`_gt`/`_gte`/`_lt`/`_lte`.

---

## File Structure

```
vision-graphql/
  Cargo.toml
  .gitignore
  src/
    lib.rs          # module declarations, public re-exports
    error.rs        # Error enum
    ast.rs          # IR types
    schema.rs       # Schema/Table/Column/PgType (stub; manual construction only)
    types.rs        # PG type mapping helpers (minimum)
    sql.rs          # IR → SQL+binds generator
    parser.rs       # GraphQL string → IR
    executor.rs     # deadpool-postgres runner
    engine.rs       # Engine (public API)
  tests/
    integration_select.rs   # end-to-end against testcontainers PG
```

---

### Task 1: Project scaffolding

**Files:**
- Create: `Cargo.toml`
- Create: `.gitignore`
- Create: `src/lib.rs`
- Create: `src/error.rs`
- Create: `src/ast.rs`
- Create: `src/schema.rs`
- Create: `src/types.rs`
- Create: `src/sql.rs`
- Create: `src/parser.rs`
- Create: `src/executor.rs`
- Create: `src/engine.rs`

- [ ] **Step 1: Write Cargo.toml**

```toml
[package]
name = "vision-graphql"
version = "0.1.0"
edition = "2021"
description = "Hasura-style ORM for PostgreSQL in Rust"
license = "MIT OR Apache-2.0"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tokio-postgres = "0.7"
deadpool-postgres = "0.14"
async-graphql-parser = "7"
async-graphql-value = "7"
serde = { version = "1", features = ["derive"] }
serde_json = { version = "1", features = ["preserve_order"] }
thiserror = "2"
tracing = "0.1"
bytes = "1"

[dev-dependencies]
insta = { version = "1", features = ["json"] }
testcontainers = "0.23"
testcontainers-modules = { version = "0.11", features = ["postgres"] }
pretty_assertions = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "test-util"] }
```

- [ ] **Step 2: Write .gitignore**

```
/target
Cargo.lock
.DS_Store
```

(Library crates conventionally do not commit `Cargo.lock`.)

- [ ] **Step 3: Write empty module files**

`src/error.rs`:
```rust
//! Error types for vision-graphql.
```

`src/ast.rs`:
```rust
//! Intermediate representation for queries.
```

`src/schema.rs`:
```rust
//! Schema data structures.
```

`src/types.rs`:
```rust
//! PostgreSQL ↔ JSON type mapping.
```

`src/sql.rs`:
```rust
//! SQL generation from IR.
```

`src/parser.rs`:
```rust
//! GraphQL string → IR.
```

`src/executor.rs`:
```rust
//! Execute generated SQL against PostgreSQL.
```

`src/engine.rs`:
```rust
//! Public engine API.
```

- [ ] **Step 4: Write src/lib.rs**

```rust
//! # vision-graphql
//!
//! A Hasura-style ORM for PostgreSQL. Accepts GraphQL query strings and
//! returns `serde_json::Value` in Hasura's data shape.

pub mod ast;
pub mod engine;
pub mod error;
pub mod executor;
pub mod parser;
pub mod schema;
pub mod sql;
pub mod types;

pub use engine::Engine;
pub use error::Error;
pub use schema::Schema;
```

- [ ] **Step 5: Run `cargo build`**

Run: `cargo build`
Expected: compiles cleanly (warnings about unused modules are acceptable).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml .gitignore src/
git commit -m "feat: scaffold vision-graphql crate structure"
```

---

### Task 2: Error type

**Files:**
- Modify: `src/error.rs`
- Test: inside `src/error.rs` as `#[cfg(test)]` module

- [ ] **Step 1: Write failing test**

Append to `src/error.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_error_displays_path_and_message() {
        let e = Error::Validate {
            path: "users.where.id._eq".into(),
            message: "expected integer, got string".into(),
        };
        let s = format!("{e}");
        assert!(s.contains("users.where.id._eq"));
        assert!(s.contains("expected integer"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib error::tests -- --nocapture`
Expected: compile error — `Error` not defined.

- [ ] **Step 3: Implement Error enum**

Replace `src/error.rs` contents with:
```rust
//! Error types for vision-graphql.

use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("validation error at {path}: {message}")]
    Validate { path: String, message: String },

    #[error("variable error: {name}: {message}")]
    Variable { name: String, message: String },

    #[error("type mapping: {0}")]
    TypeMap(String),

    #[error("database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    #[error("pool error: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),

    #[error("result decoding: {0}")]
    Decode(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_error_displays_path_and_message() {
        let e = Error::Validate {
            path: "users.where.id._eq".into(),
            message: "expected integer, got string".into(),
        };
        let s = format!("{e}");
        assert!(s.contains("users.where.id._eq"));
        assert!(s.contains("expected integer"));
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib error::tests`
Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add src/error.rs
git commit -m "feat(error): add Error enum with Hasura-style path context"
```

---

### Task 3: IR types (minimum)

**Files:**
- Modify: `src/ast.rs`
- Test: inside `src/ast.rs` as `#[cfg(test)]` module

- [ ] **Step 1: Write failing test**

Append to `src/ast.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_simple_root_field() {
        let root = RootField {
            table: "users".into(),
            alias: "users".into(),
            kind: RootKind::List,
            args: QueryArgs::default(),
            selection: vec![
                Field::Column { physical: "id".into(), alias: "id".into() },
                Field::Column { physical: "name".into(), alias: "name".into() },
            ],
        };
        assert_eq!(root.table, "users");
        assert_eq!(root.selection.len(), 2);
    }

    #[test]
    fn build_where_eq_expression() {
        let expr = BoolExpr::Compare {
            column: "id".into(),
            op: CmpOp::Eq,
            value: json!(42),
        };
        match expr {
            BoolExpr::Compare { op: CmpOp::Eq, .. } => {}
            _ => panic!("unexpected variant"),
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib ast::tests`
Expected: compile error.

- [ ] **Step 3: Implement IR types**

Replace `src/ast.rs` contents with:
```rust
//! Intermediate representation for queries.

use serde_json::Value;

#[derive(Debug, Clone)]
pub enum Operation {
    Query(Vec<RootField>),
}

#[derive(Debug, Clone)]
pub struct RootField {
    pub table: String,
    pub alias: String,
    pub kind: RootKind,
    pub args: QueryArgs,
    pub selection: Vec<Field>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootKind {
    List,
}

#[derive(Debug, Clone, Default)]
pub struct QueryArgs {
    pub where_: Option<BoolExpr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub direction: OrderDir,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub enum Field {
    Column { physical: String, alias: String },
}

#[derive(Debug, Clone)]
pub enum BoolExpr {
    And(Vec<BoolExpr>),
    Or(Vec<BoolExpr>),
    Not(Box<BoolExpr>),
    Compare { column: String, op: CmpOp, value: Value },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_simple_root_field() {
        let root = RootField {
            table: "users".into(),
            alias: "users".into(),
            kind: RootKind::List,
            args: QueryArgs::default(),
            selection: vec![
                Field::Column { physical: "id".into(), alias: "id".into() },
                Field::Column { physical: "name".into(), alias: "name".into() },
            ],
        };
        assert_eq!(root.table, "users");
        assert_eq!(root.selection.len(), 2);
    }

    #[test]
    fn build_where_eq_expression() {
        let expr = BoolExpr::Compare {
            column: "id".into(),
            op: CmpOp::Eq,
            value: json!(42),
        };
        match expr {
            BoolExpr::Compare { op: CmpOp::Eq, .. } => {}
            _ => panic!("unexpected variant"),
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib ast::tests`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/ast.rs
git commit -m "feat(ast): minimum IR for Phase 1 SELECT"
```

---

### Task 4: Schema stub

**Files:**
- Modify: `src/schema.rs`
- Test: inside `src/schema.rs`

In Phase 1 the schema is constructed manually (no introspection). We just need enough types for the SQL generator and parser to validate names.

- [ ] **Step 1: Write failing test**

Append to `src/schema.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_users_schema() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"]),
            )
            .build();
        let users = schema.table("users").expect("users table");
        assert_eq!(users.physical_schema, "public");
        assert_eq!(users.physical_name, "users");
        assert!(users.find_column("id").is_some());
        assert!(users.find_column("missing").is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib schema::tests`
Expected: compile error.

- [ ] **Step 3: Implement Schema types**

Replace `src/schema.rs` contents with:
```rust
//! Schema data structures.
//!
//! In Phase 1 the schema is constructed manually via [`Schema::builder`].
//! Introspection arrives in Phase 5.

use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgType {
    Int4,
    Int8,
    Text,
    Varchar,
    Bool,
    Float4,
    Float8,
    Numeric,
    Uuid,
    Timestamp,
    TimestampTz,
    Jsonb,
}

#[derive(Debug)]
pub struct Column {
    pub exposed_name: String,
    pub physical_name: String,
    pub pg_type: PgType,
    pub nullable: bool,
}

#[derive(Debug)]
pub struct Table {
    pub exposed_name: String,
    pub physical_schema: String,
    pub physical_name: String,
    columns_by_exposed: HashMap<String, Column>,
    pub primary_key: Vec<String>,
}

impl Table {
    pub fn new(exposed: &str, schema: &str, physical: &str) -> Self {
        Self {
            exposed_name: exposed.into(),
            physical_schema: schema.into(),
            physical_name: physical.into(),
            columns_by_exposed: HashMap::new(),
            primary_key: Vec::new(),
        }
    }

    pub fn column(mut self, exposed: &str, physical: &str, pg_type: PgType, nullable: bool) -> Self {
        self.columns_by_exposed.insert(
            exposed.into(),
            Column {
                exposed_name: exposed.into(),
                physical_name: physical.into(),
                pg_type,
                nullable,
            },
        );
        self
    }

    pub fn primary_key(mut self, cols: &[&str]) -> Self {
        self.primary_key = cols.iter().map(|s| (*s).into()).collect();
        self
    }

    pub fn find_column(&self, exposed: &str) -> Option<&Column> {
        self.columns_by_exposed.get(exposed)
    }
}

#[derive(Debug)]
pub struct Schema {
    tables_by_exposed: HashMap<String, Arc<Table>>,
}

impl Schema {
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder { tables: HashMap::new() }
    }

    pub fn table(&self, exposed: &str) -> Option<&Arc<Table>> {
        self.tables_by_exposed.get(exposed)
    }
}

pub struct SchemaBuilder {
    tables: HashMap<String, Arc<Table>>,
}

impl SchemaBuilder {
    pub fn table(mut self, t: Table) -> Self {
        self.tables.insert(t.exposed_name.clone(), Arc::new(t));
        self
    }

    pub fn build(self) -> Schema {
        Schema { tables_by_exposed: self.tables }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_users_schema() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"]),
            )
            .build();
        let users = schema.table("users").expect("users table");
        assert_eq!(users.physical_schema, "public");
        assert_eq!(users.physical_name, "users");
        assert!(users.find_column("id").is_some());
        assert!(users.find_column("missing").is_none());
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib schema::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/schema.rs
git commit -m "feat(schema): manual Schema builder and PgType enum"
```

---

### Task 5: PG type helpers

**Files:**
- Modify: `src/types.rs`
- Test: inside `src/types.rs`

For Phase 1 we need two things:

1. Bind a `serde_json::Value` as a `tokio_postgres` parameter matching a target `PgType`.
2. (Decoding happens PG-side via `json_agg`; Rust only receives one JSON text result, so no per-column decoding in this phase.)

- [ ] **Step 1: Write failing test**

Append to `src/types.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::PgType;
    use serde_json::json;

    #[test]
    fn convert_int4_value() {
        let bind = json_to_bind(&json!(42), &PgType::Int4).unwrap();
        assert!(matches!(bind, Bind::Int4(42)));
    }

    #[test]
    fn convert_text_value() {
        let bind = json_to_bind(&json!("hi"), &PgType::Text).unwrap();
        match bind {
            Bind::Text(s) => assert_eq!(s, "hi"),
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn convert_null_value() {
        let bind = json_to_bind(&json!(null), &PgType::Int4).unwrap();
        assert!(matches!(bind, Bind::Null));
    }

    #[test]
    fn reject_type_mismatch() {
        let err = json_to_bind(&json!("not a number"), &PgType::Int4).unwrap_err();
        assert!(format!("{err}").contains("expected Int4"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib types::tests`
Expected: compile error.

- [ ] **Step 3: Implement types module**

Replace `src/types.rs` contents with:
```rust
//! PostgreSQL ↔ JSON type mapping.

use crate::error::{Error, Result};
use crate::schema::PgType;
use serde_json::Value;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};
use std::error::Error as StdError;

/// A single bound parameter ready to pass to `tokio-postgres`.
#[derive(Debug, Clone, PartialEq)]
pub enum Bind {
    Null,
    Bool(bool),
    Int4(i32),
    Int8(i64),
    Float8(f64),
    Text(String),
}

impl ToSql for Bind {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn StdError + Sync + Send>> {
        match self {
            Bind::Null => Ok(IsNull::Yes),
            Bind::Bool(v) => v.to_sql(ty, out),
            Bind::Int4(v) => v.to_sql(ty, out),
            Bind::Int8(v) => v.to_sql(ty, out),
            Bind::Float8(v) => v.to_sql(ty, out),
            Bind::Text(v) => v.as_str().to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

pub fn json_to_bind(v: &Value, pg: &PgType) -> Result<Bind> {
    if v.is_null() {
        return Ok(Bind::Null);
    }
    match pg {
        PgType::Bool => v
            .as_bool()
            .map(Bind::Bool)
            .ok_or_else(|| Error::TypeMap("expected Bool".into())),
        PgType::Int4 => v
            .as_i64()
            .and_then(|n| i32::try_from(n).ok())
            .map(Bind::Int4)
            .ok_or_else(|| Error::TypeMap("expected Int4".into())),
        PgType::Int8 => v
            .as_i64()
            .map(Bind::Int8)
            .ok_or_else(|| Error::TypeMap("expected Int8".into())),
        PgType::Float4 | PgType::Float8 => v
            .as_f64()
            .map(Bind::Float8)
            .ok_or_else(|| Error::TypeMap("expected floating point".into())),
        PgType::Text | PgType::Varchar | PgType::Uuid | PgType::Numeric
        | PgType::Timestamp | PgType::TimestampTz => v
            .as_str()
            .map(|s| Bind::Text(s.to_string()))
            .ok_or_else(|| Error::TypeMap(format!("expected string for {pg:?}"))),
        PgType::Jsonb => Ok(Bind::Text(v.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::PgType;
    use serde_json::json;

    #[test]
    fn convert_int4_value() {
        let bind = json_to_bind(&json!(42), &PgType::Int4).unwrap();
        assert!(matches!(bind, Bind::Int4(42)));
    }

    #[test]
    fn convert_text_value() {
        let bind = json_to_bind(&json!("hi"), &PgType::Text).unwrap();
        match bind {
            Bind::Text(s) => assert_eq!(s, "hi"),
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn convert_null_value() {
        let bind = json_to_bind(&json!(null), &PgType::Int4).unwrap();
        assert!(matches!(bind, Bind::Null));
    }

    #[test]
    fn reject_type_mismatch() {
        let err = json_to_bind(&json!("not a number"), &PgType::Int4).unwrap_err();
        assert!(format!("{err}").contains("expected Int4"));
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib types::tests`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/types.rs
git commit -m "feat(types): Bind enum with ToSql impl and json_to_bind"
```

---

### Task 6: SQL generator — plain SELECT

**Files:**
- Modify: `src/sql.rs`
- Test: inside `src/sql.rs` (insta snapshot)

This task generates the outermost `json_agg`/`row_to_json` wrapper and the inner `SELECT col, ...` for a single root field with **no WHERE, no limit, no order**.

- [ ] **Step 1: Write failing test**

Append to `src/sql.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Field, Operation, QueryArgs, RootField, RootKind};
    use crate::schema::{PgType, Schema, Table};

    fn users_schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true),
            )
            .build()
    }

    #[test]
    fn render_plain_list() {
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            kind: RootKind::List,
            args: QueryArgs::default(),
            selection: vec![
                Field::Column { physical: "id".into(), alias: "id".into() },
                Field::Column { physical: "name".into(), alias: "name".into() },
            ],
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert!(binds.is_empty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib sql::tests`
Expected: compile error — `render` not defined.

- [ ] **Step 3: Implement render for plain list**

Replace `src/sql.rs` contents with:
```rust
//! SQL generation from IR.

use crate::ast::{Field, Operation, QueryArgs, RootField, RootKind};
use crate::error::{Error, Result};
use crate::schema::{Schema, Table};
use crate::types::Bind;
use std::fmt::Write as _;

/// Render an [`Operation`] into a single SQL statement plus bound parameters.
pub fn render(op: &Operation, schema: &Schema) -> Result<(String, Vec<Bind>)> {
    let mut ctx = RenderCtx::default();
    match op {
        Operation::Query(roots) => render_query(roots, schema, &mut ctx),
    }?;
    Ok((ctx.sql, ctx.binds))
}

#[derive(Default)]
struct RenderCtx {
    sql: String,
    binds: Vec<Bind>,
    alias_counter: usize,
}

impl RenderCtx {
    fn next_alias(&mut self, prefix: &str) -> String {
        let a = format!("{prefix}{}", self.alias_counter);
        self.alias_counter += 1;
        a
    }
}

fn render_query(roots: &[RootField], schema: &Schema, ctx: &mut RenderCtx) -> Result<()> {
    // SELECT json_build_object('alias', <subselect>, ...) FROM (SELECT 1) _r
    ctx.sql.push_str("SELECT json_build_object(");
    for (i, root) in roots.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        write!(ctx.sql, "'{}', ", escape_string_literal(&root.alias)).unwrap();
        render_root(root, schema, ctx)?;
    }
    ctx.sql.push_str(") AS result");
    Ok(())
}

fn render_root(root: &RootField, schema: &Schema, ctx: &mut RenderCtx) -> Result<()> {
    let table = schema
        .table(&root.table)
        .ok_or_else(|| Error::Validate {
            path: root.alias.clone(),
            message: format!("unknown table '{}'", root.table),
        })?;
    match root.kind {
        RootKind::List => render_list(root, table, schema, ctx),
    }
}

fn render_list(
    root: &RootField,
    table: &Table,
    _schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let inner_alias = ctx.next_alias("t");
    let row_alias = ctx.next_alias("r");
    ctx.sql.push_str("(SELECT coalesce(json_agg(row_to_json(");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push_str(")), '[]'::json) FROM (");
    render_inner_select(root, table, &inner_alias, ctx)?;
    ctx.sql.push_str(") ");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push(')');
    Ok(())
}

fn render_inner_select(
    root: &RootField,
    table: &Table,
    table_alias: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    ctx.sql.push_str("SELECT ");
    for (i, field) in root.selection.iter().enumerate() {
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
        }
    }
    write!(
        ctx.sql,
        " FROM {}.{} {table_alias}",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    render_where(&root.args, table, table_alias, ctx)?;
    render_order_by(&root.args, table, table_alias, ctx)?;
    render_limit_offset(&root.args, ctx);
    Ok(())
}

fn render_where(
    _args: &QueryArgs,
    _table: &Table,
    _table_alias: &str,
    _ctx: &mut RenderCtx,
) -> Result<()> {
    // Task 7 implements this.
    Ok(())
}

fn render_order_by(
    _args: &QueryArgs,
    _table: &Table,
    _table_alias: &str,
    _ctx: &mut RenderCtx,
) -> Result<()> {
    // Task 8 implements this.
    Ok(())
}

fn render_limit_offset(_args: &QueryArgs, _ctx: &mut RenderCtx) {
    // Task 8 implements this.
}

fn quote_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

fn escape_string_literal(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Field, Operation, QueryArgs, RootField, RootKind};
    use crate::schema::{PgType, Schema, Table};

    fn users_schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true),
            )
            .build()
    }

    #[test]
    fn render_plain_list() {
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            kind: RootKind::List,
            args: QueryArgs::default(),
            selection: vec![
                Field::Column { physical: "id".into(), alias: "id".into() },
                Field::Column { physical: "name".into(), alias: "name".into() },
            ],
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert!(binds.is_empty());
    }
}
```

- [ ] **Step 4: Run test and accept the snapshot**

Run: `cargo test --lib sql::tests::render_plain_list`
Expected: FAIL — new snapshot pending.

Run: `cargo insta review` and accept.

Expected snapshot content (approximately):
```
SELECT json_build_object('users', (SELECT coalesce(json_agg(row_to_json(r1)), '[]'::json) FROM (SELECT t0."id" AS "id", t0."name" AS "name" FROM "public"."users" t0) r1)) AS result
```

- [ ] **Step 5: Re-run test**

Run: `cargo test --lib sql::tests`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "feat(sql): render plain list SELECT with json_agg wrapper"
```

---

### Task 7: SQL generator — WHERE comparison operators

**Files:**
- Modify: `src/sql.rs`
- Test: inside `src/sql.rs`

Supports `BoolExpr::Compare` with all six `CmpOp` variants, plus `And`/`Or`/`Not`.

- [ ] **Step 1: Write failing test**

Append tests to `src/sql.rs` inside the existing `tests` module:
```rust
    #[test]
    fn render_where_eq_int() {
        use crate::ast::{BoolExpr, CmpOp};
        use serde_json::json;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            kind: RootKind::List,
            args: QueryArgs {
                where_: Some(BoolExpr::Compare {
                    column: "id".into(),
                    op: CmpOp::Eq,
                    value: json!(42),
                }),
                ..Default::default()
            },
            selection: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
        assert!(matches!(binds[0], crate::types::Bind::Int4(42)));
    }

    #[test]
    fn render_where_and_of_ops() {
        use crate::ast::{BoolExpr, CmpOp};
        use serde_json::json;

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            kind: RootKind::List,
            args: QueryArgs {
                where_: Some(BoolExpr::And(vec![
                    BoolExpr::Compare {
                        column: "id".into(),
                        op: CmpOp::Gt,
                        value: json!(1),
                    },
                    BoolExpr::Compare {
                        column: "name".into(),
                        op: CmpOp::Neq,
                        value: json!("bob"),
                    },
                ])),
                ..Default::default()
            },
            selection: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 2);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib sql::tests::render_where_eq_int`
Expected: FAIL — WHERE clause missing in output.

- [ ] **Step 3: Implement WHERE rendering**

In `src/sql.rs`, replace the placeholder `render_where` function with:
```rust
fn render_where(
    args: &QueryArgs,
    table: &Table,
    table_alias: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::BoolExpr;
    let Some(expr) = args.where_.as_ref() else { return Ok(()); };
    ctx.sql.push_str(" WHERE ");
    render_bool_expr(expr, table, table_alias, ctx)?;
    Ok(())
}

fn render_bool_expr(
    expr: &crate::ast::BoolExpr,
    table: &Table,
    table_alias: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::{BoolExpr, CmpOp};
    match expr {
        BoolExpr::And(parts) => {
            render_bool_list(parts, "AND", table, table_alias, ctx)
        }
        BoolExpr::Or(parts) => {
            render_bool_list(parts, "OR", table, table_alias, ctx)
        }
        BoolExpr::Not(inner) => {
            ctx.sql.push_str("(NOT ");
            render_bool_expr(inner, table, table_alias, ctx)?;
            ctx.sql.push(')');
            Ok(())
        }
        BoolExpr::Compare { column, op, value } => {
            let col = table.find_column(column).ok_or_else(|| Error::Validate {
                path: format!("where.{column}"),
                message: format!("unknown column '{column}' on '{}'", table.exposed_name),
            })?;
            let bind = crate::types::json_to_bind(value, &col.pg_type).map_err(|e| {
                Error::Validate {
                    path: format!("where.{column}"),
                    message: format!("{e}"),
                }
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
                "{table_alias}.{} {op_str} {placeholder}",
                quote_ident(&col.physical_name)
            )
            .unwrap();
            Ok(())
        }
    }
}

fn render_bool_list(
    parts: &[crate::ast::BoolExpr],
    joiner: &str,
    table: &Table,
    table_alias: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    if parts.is_empty() {
        // Empty AND is true; empty OR is false. Match Hasura semantics.
        ctx.sql.push_str(if joiner == "AND" { "TRUE" } else { "FALSE" });
        return Ok(());
    }
    ctx.sql.push('(');
    for (i, p) in parts.iter().enumerate() {
        if i > 0 {
            write!(ctx.sql, " {joiner} ").unwrap();
        }
        render_bool_expr(p, table, table_alias, ctx)?;
    }
    ctx.sql.push(')');
    Ok(())
}
```

- [ ] **Step 4: Run tests and accept snapshots**

Run: `cargo test --lib sql::tests`
Expected: FAIL — two pending snapshots.

Run: `cargo insta review` and accept both.

- [ ] **Step 5: Re-run tests**

Run: `cargo test --lib sql::tests`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "feat(sql): WHERE with comparison ops and and/or/not"
```

---

### Task 8: SQL generator — ORDER BY, LIMIT, OFFSET

**Files:**
- Modify: `src/sql.rs`

- [ ] **Step 1: Write failing test**

Append to the `tests` module in `src/sql.rs`:
```rust
    #[test]
    fn render_order_limit_offset() {
        use crate::ast::{OrderBy, OrderDir};

        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            kind: RootKind::List,
            args: QueryArgs {
                order_by: vec![
                    OrderBy { column: "name".into(), direction: OrderDir::Asc },
                    OrderBy { column: "id".into(), direction: OrderDir::Desc },
                ],
                limit: Some(10),
                offset: Some(5),
                ..Default::default()
            },
            selection: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
        }]);
        let (sql, _binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib sql::tests::render_order_limit_offset`
Expected: FAIL — no ORDER BY / LIMIT / OFFSET in output.

- [ ] **Step 3: Implement ORDER BY and LIMIT/OFFSET**

In `src/sql.rs`, replace the placeholders for `render_order_by` and `render_limit_offset`:
```rust
fn render_order_by(
    args: &QueryArgs,
    table: &Table,
    table_alias: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    if args.order_by.is_empty() {
        return Ok(());
    }
    ctx.sql.push_str(" ORDER BY ");
    for (i, ob) in args.order_by.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let col = table.find_column(&ob.column).ok_or_else(|| Error::Validate {
            path: format!("order_by.{}", ob.column),
            message: format!("unknown column '{}' on '{}'", ob.column, table.exposed_name),
        })?;
        let dir = match ob.direction {
            crate::ast::OrderDir::Asc => "ASC",
            crate::ast::OrderDir::Desc => "DESC",
        };
        write!(
            ctx.sql,
            "{table_alias}.{} {dir}",
            quote_ident(&col.physical_name)
        )
        .unwrap();
    }
    Ok(())
}

fn render_limit_offset(args: &QueryArgs, ctx: &mut RenderCtx) {
    if let Some(n) = args.limit {
        write!(ctx.sql, " LIMIT {n}").unwrap();
    }
    if let Some(n) = args.offset {
        write!(ctx.sql, " OFFSET {n}").unwrap();
    }
}
```

`limit` and `offset` are `u64` coming from validated IR (not user-provided raw strings), so inlining them is safe.

- [ ] **Step 4: Accept snapshot**

Run: `cargo insta review` and accept.

- [ ] **Step 5: Re-run tests**

Run: `cargo test --lib sql::tests`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "feat(sql): ORDER BY, LIMIT, OFFSET"
```

---

### Task 9: GraphQL parser — simple selection set

**Files:**
- Modify: `src/parser.rs`
- Test: inside `src/parser.rs`

Parses `query { users { id name } }` into an `Operation`. No variables, no where, no fragments yet.

- [ ] **Step 1: Write failing test**

Append to `src/parser.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Field, Operation, RootKind};
    use crate::schema::{PgType, Schema, Table};
    use serde_json::json;

    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true),
            )
            .build()
    }

    #[test]
    fn parse_plain_list() {
        let op = parse_and_lower("query { users { id name } }", &json!({}), None, &schema()).unwrap();
        match op {
            Operation::Query(roots) => {
                assert_eq!(roots.len(), 1);
                assert_eq!(roots[0].table, "users");
                assert_eq!(roots[0].alias, "users");
                assert!(matches!(roots[0].kind, RootKind::List));
                assert_eq!(roots[0].selection.len(), 2);
                match &roots[0].selection[0] {
                    Field::Column { physical, alias } => {
                        assert_eq!(physical, "id");
                        assert_eq!(alias, "id");
                    }
                }
            }
        }
    }

    #[test]
    fn parse_respects_field_alias() {
        let op = parse_and_lower(
            "query { users { uid: id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match &roots[0].selection[0] {
            Field::Column { physical, alias } => {
                assert_eq!(physical, "id");
                assert_eq!(alias, "uid");
            }
        }
    }

    #[test]
    fn parse_rejects_unknown_table() {
        let err = parse_and_lower(
            "query { widgets { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("unknown root field 'widgets'"));
    }

    #[test]
    fn parse_rejects_unknown_column() {
        let err = parse_and_lower(
            "query { users { bogus } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("unknown column 'bogus'"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib parser::tests`
Expected: compile error — `parse_and_lower` not defined.

- [ ] **Step 3: Implement parse_and_lower**

Replace `src/parser.rs` contents with:
```rust
//! GraphQL string → IR.

use crate::ast::{Field, Operation, QueryArgs, RootField, RootKind};
use crate::error::{Error, Result};
use crate::schema::Schema;
use async_graphql_parser::parse_query;
use async_graphql_parser::types::{
    ExecutableDocument, OperationType, Selection, SelectionSet,
};
use serde_json::Value;

pub fn parse_and_lower(
    source: &str,
    variables: &Value,
    operation_name: Option<&str>,
    schema: &Schema,
) -> Result<Operation> {
    let doc = parse_query(source).map_err(|e| Error::Parse(e.to_string()))?;
    let op = pick_operation(&doc, operation_name)?;
    match op.ty {
        OperationType::Query => lower_query(&op.selection_set.node, schema, variables),
        OperationType::Mutation => Err(Error::Parse(
            "mutations are not supported in Phase 1".into(),
        )),
        OperationType::Subscription => Err(Error::Parse(
            "subscriptions are not supported".into(),
        )),
    }
}

struct OpInfo<'a> {
    ty: OperationType,
    selection_set: &'a async_graphql_parser::Positioned<SelectionSet>,
}

fn pick_operation<'a>(
    doc: &'a ExecutableDocument,
    name: Option<&str>,
) -> Result<OpInfo<'a>> {
    use async_graphql_parser::types::DocumentOperations;
    match (&doc.operations, name) {
        (DocumentOperations::Single(op), _) => Ok(OpInfo {
            ty: op.node.ty,
            selection_set: &op.node.selection_set,
        }),
        (DocumentOperations::Multiple(ops), Some(n)) => {
            let key = async_graphql_value::Name::new(n);
            let op = ops.get(&key).ok_or_else(|| {
                Error::Parse(format!("operation '{n}' not found"))
            })?;
            Ok(OpInfo {
                ty: op.node.ty,
                selection_set: &op.node.selection_set,
            })
        }
        (DocumentOperations::Multiple(_), None) => Err(Error::Parse(
            "document has multiple operations; operation_name required".into(),
        )),
    }
}

fn lower_query(
    set: &SelectionSet,
    schema: &Schema,
    _vars: &Value,
) -> Result<Operation> {
    let mut roots = Vec::new();
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

                // Phase 1: root field name is the exposed table name; kind is always List.
                let table = schema.table(name).ok_or_else(|| Error::Validate {
                    path: alias.clone(),
                    message: format!("unknown root field '{name}'"),
                })?;

                let selection =
                    lower_selection_set(&field.selection_set.node, table, &alias)?;

                roots.push(RootField {
                    table: name.to_string(),
                    alias,
                    kind: RootKind::List,
                    args: QueryArgs::default(),
                    selection,
                });
            }
            Selection::FragmentSpread(_) | Selection::InlineFragment(_) => {
                return Err(Error::Parse(
                    "fragments are not supported in Phase 1".into(),
                ));
            }
        }
    }
    Ok(Operation::Query(roots))
}

fn lower_selection_set(
    set: &SelectionSet,
    table: &crate::schema::Table,
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

                // Phase 1: every child field is a column.
                let col = table.find_column(name).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.{alias}"),
                    message: format!(
                        "unknown column '{name}' on '{}'",
                        table.exposed_name
                    ),
                })?;
                out.push(Field::Column {
                    physical: col.physical_name.clone(),
                    alias,
                });
            }
            Selection::FragmentSpread(_) | Selection::InlineFragment(_) => {
                return Err(Error::Parse(
                    "fragments are not supported in Phase 1".into(),
                ));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Field, Operation, RootKind};
    use crate::schema::{PgType, Schema, Table};
    use serde_json::json;

    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true),
            )
            .build()
    }

    #[test]
    fn parse_plain_list() {
        let op = parse_and_lower("query { users { id name } }", &json!({}), None, &schema()).unwrap();
        match op {
            Operation::Query(roots) => {
                assert_eq!(roots.len(), 1);
                assert_eq!(roots[0].table, "users");
                assert_eq!(roots[0].alias, "users");
                assert!(matches!(roots[0].kind, RootKind::List));
                assert_eq!(roots[0].selection.len(), 2);
                match &roots[0].selection[0] {
                    Field::Column { physical, alias } => {
                        assert_eq!(physical, "id");
                        assert_eq!(alias, "id");
                    }
                }
            }
        }
    }

    #[test]
    fn parse_respects_field_alias() {
        let op = parse_and_lower(
            "query { users { uid: id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match &roots[0].selection[0] {
            Field::Column { physical, alias } => {
                assert_eq!(physical, "id");
                assert_eq!(alias, "uid");
            }
        }
    }

    #[test]
    fn parse_rejects_unknown_table() {
        let err = parse_and_lower(
            "query { widgets { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("unknown root field 'widgets'"));
    }

    #[test]
    fn parse_rejects_unknown_column() {
        let err = parse_and_lower(
            "query { users { bogus } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("unknown column 'bogus'"));
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib parser::tests`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): lower simple GraphQL selection to IR"
```

---

### Task 10: Parser — arguments (where / order_by / limit / offset)

**Files:**
- Modify: `src/parser.rs`

Supports Hasura's argument shape, with variables resolved from the `variables` JSON.

- [ ] **Step 1: Write failing test**

Append to the `tests` module in `src/parser.rs`:
```rust
    #[test]
    fn parse_where_eq_with_variable() {
        let op = parse_and_lower(
            "query Q($uid: Int!) { users(where: {id: {_eq: $uid}}, limit: 10) { id name } }",
            &json!({"uid": 42}),
            Some("Q"),
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        let args = &roots[0].args;
        assert_eq!(args.limit, Some(10));
        match args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::Compare { column, op, value } => {
                assert_eq!(column, "id");
                assert!(matches!(op, crate::ast::CmpOp::Eq));
                assert_eq!(value, &json!(42));
            }
            _ => panic!("expected Compare"),
        }
    }

    #[test]
    fn parse_where_and_of_ops() {
        let op = parse_and_lower(
            "query { users(where: {_and: [{id: {_gt: 1}}, {name: {_neq: \"bob\"}}]}) { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::And(parts) => assert_eq!(parts.len(), 2),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn parse_order_by_list() {
        let op = parse_and_lower(
            "query { users(order_by: [{name: asc}, {id: desc}]) { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        assert_eq!(roots[0].args.order_by.len(), 2);
        assert_eq!(roots[0].args.order_by[0].column, "name");
        assert!(matches!(
            roots[0].args.order_by[0].direction,
            crate::ast::OrderDir::Asc
        ));
    }

    #[test]
    fn parse_missing_variable_errors() {
        let err = parse_and_lower(
            "query Q($uid: Int!) { users(where: {id: {_eq: $uid}}) { id } }",
            &json!({}),
            Some("Q"),
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("uid"));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib parser::tests::parse_where_eq_with_variable`
Expected: FAIL — arguments not parsed.

- [ ] **Step 3: Implement argument lowering**

Replace the body of `lower_query` in `src/parser.rs` and add helpers. The
complete replacement for `src/parser.rs` follows (overwriting the file):

```rust
//! GraphQL string → IR.

use crate::ast::{BoolExpr, CmpOp, Field, Operation, OrderBy, OrderDir, QueryArgs, RootField, RootKind};
use crate::error::{Error, Result};
use crate::schema::{Schema, Table};
use async_graphql_parser::parse_query;
use async_graphql_parser::types::{
    DocumentOperations, ExecutableDocument, OperationType, Selection, SelectionSet,
};
use async_graphql_parser::Positioned;
use async_graphql_value::{ConstValue, Name, Value as GqlValue};
use serde_json::Value;

pub fn parse_and_lower(
    source: &str,
    variables: &Value,
    operation_name: Option<&str>,
    schema: &Schema,
) -> Result<Operation> {
    let doc = parse_query(source).map_err(|e| Error::Parse(e.to_string()))?;
    let op = pick_operation(&doc, operation_name)?;
    match op.ty {
        OperationType::Query => lower_query(op.selection_set, schema, variables),
        OperationType::Mutation => Err(Error::Parse(
            "mutations are not supported in Phase 1".into(),
        )),
        OperationType::Subscription => Err(Error::Parse("subscriptions are not supported".into())),
    }
}

struct OpInfo<'a> {
    ty: OperationType,
    selection_set: &'a SelectionSet,
}

fn pick_operation<'a>(doc: &'a ExecutableDocument, name: Option<&str>) -> Result<OpInfo<'a>> {
    match (&doc.operations, name) {
        (DocumentOperations::Single(op), _) => Ok(OpInfo {
            ty: op.node.ty,
            selection_set: &op.node.selection_set.node,
        }),
        (DocumentOperations::Multiple(ops), Some(n)) => {
            let key = Name::new(n);
            let op = ops
                .get(&key)
                .ok_or_else(|| Error::Parse(format!("operation '{n}' not found")))?;
            Ok(OpInfo {
                ty: op.node.ty,
                selection_set: &op.node.selection_set.node,
            })
        }
        (DocumentOperations::Multiple(_), None) => Err(Error::Parse(
            "document has multiple operations; operation_name required".into(),
        )),
    }
}

fn lower_query(set: &SelectionSet, schema: &Schema, vars: &Value) -> Result<Operation> {
    let mut roots = Vec::new();
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
                let table = schema.table(name).ok_or_else(|| Error::Validate {
                    path: alias.clone(),
                    message: format!("unknown root field '{name}'"),
                })?;
                let args = lower_args(&field.arguments, table, vars, &alias)?;
                let selection =
                    lower_selection_set(&field.selection_set.node, table, &alias)?;

                roots.push(RootField {
                    table: name.to_string(),
                    alias,
                    kind: RootKind::List,
                    args,
                    selection,
                });
            }
            _ => {
                return Err(Error::Parse(
                    "fragments are not supported in Phase 1".into(),
                ))
            }
        }
    }
    Ok(Operation::Query(roots))
}

fn lower_selection_set(
    set: &SelectionSet,
    table: &Table,
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
                let col = table.find_column(name).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.{alias}"),
                    message: format!(
                        "unknown column '{name}' on '{}'",
                        table.exposed_name
                    ),
                })?;
                out.push(Field::Column {
                    physical: col.physical_name.clone(),
                    alias,
                });
            }
            _ => {
                return Err(Error::Parse(
                    "fragments are not supported in Phase 1".into(),
                ))
            }
        }
    }
    Ok(out)
}

fn lower_args(
    args: &[(Positioned<Name>, Positioned<GqlValue>)],
    table: &Table,
    vars: &Value,
    parent_path: &str,
) -> Result<QueryArgs> {
    let mut out = QueryArgs::default();
    for (name_p, value_p) in args {
        let name = name_p.node.as_str();
        let v = &value_p.node;
        match name {
            "where" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.where"))?;
                out.where_ = Some(lower_where(&json, table, &format!("{parent_path}.where"))?);
            }
            "order_by" => {
                out.order_by = lower_order_by(v, vars, &format!("{parent_path}.order_by"))?;
            }
            "limit" => {
                out.limit = Some(gql_u64(v, vars, &format!("{parent_path}.limit"))?);
            }
            "offset" => {
                out.offset = Some(gql_u64(v, vars, &format!("{parent_path}.offset"))?);
            }
            _ => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.{name}"),
                    message: format!("unknown argument '{name}'"),
                })
            }
        }
    }
    Ok(out)
}

fn lower_where(json: &Value, table: &Table, path: &str) -> Result<BoolExpr> {
    let obj = json.as_object().ok_or_else(|| Error::Validate {
        path: path.into(),
        message: "expected object".into(),
    })?;
    let mut parts: Vec<BoolExpr> = Vec::new();
    for (k, v) in obj {
        match k.as_str() {
            "_and" => {
                let arr = v.as_array().ok_or_else(|| Error::Validate {
                    path: format!("{path}._and"),
                    message: "expected array".into(),
                })?;
                let inner: Result<Vec<BoolExpr>> = arr
                    .iter()
                    .enumerate()
                    .map(|(i, x)| lower_where(x, table, &format!("{path}._and[{i}]")))
                    .collect();
                parts.push(BoolExpr::And(inner?));
            }
            "_or" => {
                let arr = v.as_array().ok_or_else(|| Error::Validate {
                    path: format!("{path}._or"),
                    message: "expected array".into(),
                })?;
                let inner: Result<Vec<BoolExpr>> = arr
                    .iter()
                    .enumerate()
                    .map(|(i, x)| lower_where(x, table, &format!("{path}._or[{i}]")))
                    .collect();
                parts.push(BoolExpr::Or(inner?));
            }
            "_not" => {
                parts.push(BoolExpr::Not(Box::new(lower_where(
                    v,
                    table,
                    &format!("{path}._not"),
                )?)));
            }
            col_name => {
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
        }
    }
    Ok(if parts.len() == 1 {
        parts.into_iter().next().unwrap()
    } else {
        BoolExpr::And(parts)
    })
}

fn lower_order_by(
    v: &GqlValue,
    vars: &Value,
    path: &str,
) -> Result<Vec<OrderBy>> {
    let json = gql_to_json(v, vars, path)?;
    let arr: Vec<&Value> = match &json {
        Value::Array(xs) => xs.iter().collect(),
        Value::Object(_) => vec![&json],
        _ => {
            return Err(Error::Validate {
                path: path.into(),
                message: "expected object or array".into(),
            })
        }
    };
    let mut out = Vec::new();
    for (i, item) in arr.iter().enumerate() {
        let obj = item.as_object().ok_or_else(|| Error::Validate {
            path: format!("{path}[{i}]"),
            message: "expected object".into(),
        })?;
        for (col, dir_val) in obj {
            let dir_s = dir_val.as_str().ok_or_else(|| Error::Validate {
                path: format!("{path}[{i}].{col}"),
                message: "expected 'asc' or 'desc'".into(),
            })?;
            let direction = match dir_s {
                "asc" => OrderDir::Asc,
                "desc" => OrderDir::Desc,
                other => {
                    return Err(Error::Validate {
                        path: format!("{path}[{i}].{col}"),
                        message: format!("unknown direction '{other}'"),
                    })
                }
            };
            out.push(OrderBy {
                column: col.clone(),
                direction,
            });
        }
    }
    Ok(out)
}

fn gql_u64(v: &GqlValue, vars: &Value, path: &str) -> Result<u64> {
    let json = gql_to_json(v, vars, path)?;
    json.as_u64().ok_or_else(|| Error::Validate {
        path: path.into(),
        message: "expected non-negative integer".into(),
    })
}

/// Convert a GraphQL value to JSON, resolving variable references from `vars`.
fn gql_to_json(v: &GqlValue, vars: &Value, path: &str) -> Result<Value> {
    match v {
        GqlValue::Null => Ok(Value::Null),
        GqlValue::Number(n) => serde_json::to_value(n).map_err(|e| Error::Parse(e.to_string())),
        GqlValue::String(s) => Ok(Value::String(s.clone())),
        GqlValue::Boolean(b) => Ok(Value::Bool(*b)),
        GqlValue::Enum(e) => Ok(Value::String(e.to_string())),
        GqlValue::List(xs) => {
            let mut out = Vec::with_capacity(xs.len());
            for (i, x) in xs.iter().enumerate() {
                out.push(gql_to_json(x, vars, &format!("{path}[{i}]"))?);
            }
            Ok(Value::Array(out))
        }
        GqlValue::Object(kv) => {
            let mut out = serde_json::Map::new();
            for (k, val) in kv {
                out.insert(
                    k.to_string(),
                    gql_to_json(val, vars, &format!("{path}.{k}"))?,
                );
            }
            Ok(Value::Object(out))
        }
        GqlValue::Variable(name) => {
            let nm = name.as_str();
            vars.get(nm)
                .cloned()
                .ok_or_else(|| Error::Variable {
                    name: nm.to_string(),
                    message: "not bound".into(),
                })
        }
        GqlValue::Binary(_) => Err(Error::Parse("binary literals not supported".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Field, Operation, RootKind};
    use crate::schema::{PgType, Schema, Table};
    use serde_json::json;

    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true),
            )
            .build()
    }

    #[test]
    fn parse_plain_list() {
        let op = parse_and_lower("query { users { id name } }", &json!({}), None, &schema()).unwrap();
        match op {
            Operation::Query(roots) => {
                assert_eq!(roots.len(), 1);
                assert_eq!(roots[0].table, "users");
                assert_eq!(roots[0].alias, "users");
                assert!(matches!(roots[0].kind, RootKind::List));
                assert_eq!(roots[0].selection.len(), 2);
                match &roots[0].selection[0] {
                    Field::Column { physical, alias } => {
                        assert_eq!(physical, "id");
                        assert_eq!(alias, "id");
                    }
                }
            }
        }
    }

    #[test]
    fn parse_respects_field_alias() {
        let op = parse_and_lower(
            "query { users { uid: id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match &roots[0].selection[0] {
            Field::Column { physical, alias } => {
                assert_eq!(physical, "id");
                assert_eq!(alias, "uid");
            }
        }
    }

    #[test]
    fn parse_rejects_unknown_table() {
        let err = parse_and_lower(
            "query { widgets { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("unknown root field 'widgets'"));
    }

    #[test]
    fn parse_rejects_unknown_column() {
        let err = parse_and_lower(
            "query { users { bogus } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("unknown column 'bogus'"));
    }

    #[test]
    fn parse_where_eq_with_variable() {
        let op = parse_and_lower(
            "query Q($uid: Int!) { users(where: {id: {_eq: $uid}}, limit: 10) { id name } }",
            &json!({"uid": 42}),
            Some("Q"),
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        let args = &roots[0].args;
        assert_eq!(args.limit, Some(10));
        match args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::Compare { column, op, value } => {
                assert_eq!(column, "id");
                assert!(matches!(op, crate::ast::CmpOp::Eq));
                assert_eq!(value, &json!(42));
            }
            _ => panic!("expected Compare"),
        }
    }

    #[test]
    fn parse_where_and_of_ops() {
        let op = parse_and_lower(
            "query { users(where: {_and: [{id: {_gt: 1}}, {name: {_neq: \"bob\"}}]}) { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::And(parts) => assert_eq!(parts.len(), 2),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn parse_order_by_list() {
        let op = parse_and_lower(
            "query { users(order_by: [{name: asc}, {id: desc}]) { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        assert_eq!(roots[0].args.order_by.len(), 2);
        assert_eq!(roots[0].args.order_by[0].column, "name");
        assert!(matches!(
            roots[0].args.order_by[0].direction,
            crate::ast::OrderDir::Asc
        ));
    }

    #[test]
    fn parse_missing_variable_errors() {
        let err = parse_and_lower(
            "query Q($uid: Int!) { users(where: {id: {_eq: $uid}}) { id } }",
            &json!({}),
            Some("Q"),
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("uid"));
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib parser::tests`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): where / order_by / limit / offset with variable resolution"
```

---

### Task 11: Executor

**Files:**
- Modify: `src/executor.rs`
- Test: in integration test only (Task 13). Unit-level testable logic is minimal.

- [ ] **Step 1: Write executor module**

Replace `src/executor.rs` with:
```rust
//! Execute generated SQL against PostgreSQL.

use crate::error::{Error, Result};
use crate::types::Bind;
use deadpool_postgres::Pool;
use serde_json::Value;
use tokio_postgres::types::ToSql;

/// Execute a single-statement SQL with bound parameters. The SQL is expected
/// to return exactly one row with one column named `result` containing a JSON
/// value (generated by [`crate::sql::render`]).
pub async fn execute(pool: &Pool, sql: &str, binds: &[Bind]) -> Result<Value> {
    let client = pool.get().await?;
    let stmt = client.prepare_cached(sql).await?;

    let params: Vec<&(dyn ToSql + Sync)> =
        binds.iter().map(|b| b as &(dyn ToSql + Sync)).collect();

    let row = client.query_one(&stmt, &params).await?;

    // Prefer JSONB support; we selected via json_build_object which returns `json`.
    let json: serde_json::Value = row
        .try_get::<_, serde_json::Value>(0)
        .map_err(|e| Error::Decode(e.to_string()))?;

    Ok(json)
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build`
Expected: clean compile.

- [ ] **Step 3: Commit**

```bash
git add src/executor.rs
git commit -m "feat(executor): deadpool-postgres runner for single-row JSON results"
```

---

### Task 12: Engine (public API)

**Files:**
- Modify: `src/engine.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write engine module**

Replace `src/engine.rs` with:
```rust
//! Public engine API.

use crate::error::Result;
use crate::parser::parse_and_lower;
use crate::schema::Schema;
use crate::sql::render;
use deadpool_postgres::Pool;
use serde_json::Value;
use std::sync::Arc;

pub struct Engine {
    pool: Pool,
    schema: Arc<Schema>,
}

impl Engine {
    pub fn new(pool: Pool, schema: Schema) -> Self {
        Self {
            pool,
            schema: Arc::new(schema),
        }
    }

    /// Parse a GraphQL query string, execute against PostgreSQL, return the
    /// Hasura-shaped `data` object as `serde_json::Value`.
    pub async fn query(
        &self,
        source: &str,
        variables: Option<Value>,
    ) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let op = parse_and_lower(source, &vars, None, &self.schema)?;
        let (sql, binds) = render(&op, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing");
        crate::executor::execute(&self.pool, &sql, &binds).await
    }
}
```

- [ ] **Step 2: Verify the lib re-exports**

`src/lib.rs` already re-exports `Engine`, `Error`, `Schema`. Run:

Run: `cargo build`
Expected: clean compile.

- [ ] **Step 3: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): wire parser → sql → executor"
```

---

### Task 13: End-to-end integration test

**Files:**
- Create: `tests/integration_select.rs`

- [ ] **Step 1: Write the integration test**

```rust
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

fn users_schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, true)
                .column("active", "active", PgType::Bool, false)
                .primary_key(&["id"]),
        )
        .build()
}

async fn setup() -> (Engine, impl std::future::Future<Output = ()>) {
    let container = Postgres::default().start().await.expect("start pg");
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
    let pool = cfg
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("pool");

    {
        let client = pool.get().await.expect("client");
        client
            .batch_execute(
                r#"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    active BOOL NOT NULL
                );
                INSERT INTO users (name, active) VALUES
                    ('alice', TRUE),
                    ('bob',   FALSE),
                    ('cara',  TRUE);
                "#,
            )
            .await
            .expect("seed");
    }

    let engine = Engine::new(pool, users_schema());
    // container must outlive engine; return teardown future that holds it.
    (engine, async move {
        drop(container);
    })
}

#[tokio::test]
async fn plain_list_returns_all_rows() {
    let (engine, _td) = setup().await;
    let v: Value = engine
        .query("query { users { id name } }", None)
        .await
        .expect("query ok");
    let users = v.get("users").and_then(Value::as_array).expect("array");
    assert_eq!(users.len(), 3);
    assert!(users.iter().any(|u| u["name"] == json!("alice")));
}

#[tokio::test]
async fn where_eq_with_variable() {
    let (engine, _td) = setup().await;
    let v: Value = engine
        .query(
            "query Q($n: String!) { users(where: {name: {_eq: $n}}) { id name } }",
            Some(json!({"n": "bob"})),
        )
        .await
        .expect("query ok");
    let users = v.get("users").and_then(Value::as_array).expect("array");
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["name"], json!("bob"));
}

#[tokio::test]
async fn order_by_limit_offset() {
    let (engine, _td) = setup().await;
    let v: Value = engine
        .query(
            "query { users(order_by: [{name: desc}], limit: 2, offset: 1) { name } }",
            None,
        )
        .await
        .expect("query ok");
    let users = v.get("users").and_then(Value::as_array).expect("array");
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["name"], json!("bob"));
    assert_eq!(users[1]["name"], json!("alice"));
}

#[tokio::test]
async fn sql_injection_attempt_is_bound_safely() {
    let (engine, _td) = setup().await;
    let nasty = "'); DROP TABLE users; --";
    let v: Value = engine
        .query(
            "query Q($n: String!) { users(where: {name: {_eq: $n}}) { id } }",
            Some(json!({"n": nasty})),
        )
        .await
        .expect("query ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 0);

    // users table should still exist; issue another query
    let v2 = engine
        .query("query { users { id } }", None)
        .await
        .expect("second query ok");
    assert_eq!(v2["users"].as_array().unwrap().len(), 3);
}
```

- [ ] **Step 2: Run integration tests**

Run: `cargo test --test integration_select -- --test-threads=1`
Expected: 4 tests PASS. Docker must be running.

If Docker is not available, integration tests fail to start containers. Confirm with user before running.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_select.rs
git commit -m "test: e2e integration test for Phase 1 SELECT"
```

---

### Task 14: Verify full suite and tag Phase 1 complete

**Files:** none

- [ ] **Step 1: Run full test suite**

Run: `cargo test`
Expected: all unit and integration tests pass.

- [ ] **Step 2: Run `cargo clippy`**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean (fix any warnings inline).

- [ ] **Step 3: Run `cargo fmt --check`**

Run: `cargo fmt --check`
Expected: clean. If not, run `cargo fmt` and commit.

- [ ] **Step 4: Tag Phase 1**

```bash
git tag -a phase-1-core-select -m "Phase 1: core SELECT end-to-end"
```

- [ ] **Step 5: Announce completion**

Phase 1 is complete when:
- `cargo test` passes (all unit + integration)
- `cargo clippy` passes with `-D warnings`
- `cargo fmt --check` passes
- A user can call `Engine::new(pool, schema).query(graphql_string, vars)` and get back a Hasura-shaped JSON `Value`

Move on to Phase 2 (nested relations) next.

---

## Next Phases (outline only — separate plans will be written)

- **Phase 2**: Nested selections (object + array relations), `EXISTS`-based relation filters, relation `order_by`
- **Phase 3**: Aggregates (`count`/`sum`/`avg`/`max`/`min`), `_aggregate` root field, `_by_pk` root field, `distinct_on`
- **Phase 4**: Mutations (`insert` / `insert_one` / `update` / `update_by_pk` / `delete` / `delete_by_pk` / `on_conflict` / `returning`)
- **Phase 5**: Schema introspection + TOML config + `Schema::builder` augmentation, FK auto-relations with ambiguity handling
- **Phase 6**: Rust builder API (`QueryBuilder`, `Mutation::insert`, etc.), equivalence tests with parser
- **Phase 7**: Remaining operators (`_in`, `_nin`, `_like`, `_ilike`, `_is_null`), GraphQL fragments, named operation selection polish
- **Phase 8**: `tracing` polish, basic `criterion` bench, doc-level crate README
