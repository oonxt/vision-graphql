# Vision-GraphQL Phase 8 — Tracing, Bench, Docs

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans.

**Goal:** Round out the project with structured tracing spans, a criterion micro-bench for SQL generation, crate-level documentation, and a user-facing README.

**Architecture:** Add `#[tracing::instrument]` on the engine's entry points for easy debugging. Benches live under `benches/sql_render.rs` using `criterion` — pure SQL-gen micro-bench with no PG dependency. README gives a quick start, feature matrix, and links to the design doc.

**Tech Stack:** Adds `criterion` dev-dep.

---

### Task 1: Tracing spans

**Files:**
- Modify: `src/engine.rs`
- Modify: `src/parser.rs`

- [ ] **Step 1: Instrument engine entry points**

In `src/engine.rs`, add `#[tracing::instrument(level = "debug", skip_all, fields(source = source))]` on `Engine::query` and `#[tracing::instrument(level = "debug", skip_all)]` on `Engine::run`.

Concretely replace the two method signatures:

```rust
    #[tracing::instrument(level = "debug", skip_all, fields(source))]
    pub async fn query(&self, source: &str, variables: Option<Value>) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let op = parse_and_lower(source, &vars, None, &self.schema)?;
        let (sql, binds) = render(&op, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing");
        crate::executor::execute(&self.pool, &sql, &binds).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        let operation = op.into_operation();
        let (sql, binds) = render(&operation, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing");
        crate::executor::execute(&self.pool, &sql, &binds).await
    }
```

- [ ] **Step 2: Instrument parser + sql entry points**

In `src/parser.rs`, add before `pub fn parse_and_lower`:
```rust
#[tracing::instrument(level = "trace", skip_all)]
```

In `src/sql.rs`, add before `pub fn render`:
```rust
#[tracing::instrument(level = "trace", skip_all)]
```

- [ ] **Step 3: Build**

Run: `cargo build 2>&1 | tail -5`
Expected: clean.

Run: `cargo test --lib`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add src/engine.rs src/parser.rs src/sql.rs
git commit -m "feat(tracing): instrument engine/parser/sql entry points"
```

---

### Task 2: Criterion bench for SQL generation

**Files:**
- Modify: `Cargo.toml`
- Create: `benches/sql_render.rs`

- [ ] **Step 1: Add criterion dev-dep and bench stanza**

In `Cargo.toml` under `[dev-dependencies]`, add:
```toml
criterion = { version = "0.5", features = ["html_reports"] }
```

Append a new stanza at the end of `Cargo.toml`:
```toml
[[bench]]
name = "sql_render"
harness = false
```

- [ ] **Step 2: Write the bench**

Create `benches/sql_render.rs`:

```rust
use criterion::{criterion_group, criterion_main, Criterion};
use vision_graphql::ast::{
    BoolExpr, CmpOp, Field, Operation, QueryArgs, RootBody, RootField,
};
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::sql::render;

fn sample_schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, true)
                .column("active", "active", PgType::Bool, false)
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

fn moderately_complex_query() -> Operation {
    Operation::Query(vec![RootField {
        table: "users".into(),
        alias: "users".into(),
        args: QueryArgs {
            where_: Some(BoolExpr::Compare {
                column: "active".into(),
                op: CmpOp::Eq,
                value: serde_json::json!(true),
            }),
            limit: Some(10),
            ..Default::default()
        },
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
                Field::Relation {
                    name: "posts".into(),
                    alias: "posts".into(),
                    args: QueryArgs {
                        limit: Some(5),
                        ..Default::default()
                    },
                    selection: vec![
                        Field::Column {
                            physical: "title".into(),
                            alias: "title".into(),
                        },
                    ],
                },
            ],
        },
    }])
}

fn bench_render(c: &mut Criterion) {
    let schema = sample_schema();
    let op = moderately_complex_query();
    c.bench_function("render_moderately_complex", |b| {
        b.iter(|| {
            let _ = render(&op, &schema).unwrap();
        });
    });
}

criterion_group!(benches, bench_render);
criterion_main!(benches);
```

- [ ] **Step 3: Smoke run (don't wait for full benchmark)**

Run: `cargo bench --bench sql_render -- --test`
Expected: bench compiles and `--test` mode passes quickly.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml benches/
git commit -m "bench: criterion baseline for moderately complex query SQL render"
```

---

### Task 3: Crate-level docs

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Replace the top-of-file doc comment**

Replace the `//!` block at the top of `src/lib.rs` with:

```rust
//! # vision-graphql
//!
//! A Hasura-style ORM for PostgreSQL. Accepts GraphQL query strings or
//! typed Rust builders and returns `serde_json::Value` in Hasura's data
//! shape.
//!
//! ## Quick start
//!
//! ```no_run
//! use deadpool_postgres::{Config, Runtime};
//! use tokio_postgres::NoTls;
//! use vision_graphql::{Engine, Query, Schema};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut cfg = Config::new();
//! cfg.host = Some("localhost".into());
//! cfg.dbname = Some("mydb".into());
//! let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
//!
//! // Introspect the database to build the schema.
//! let schema = Schema::introspect(&pool).await?.build();
//! let engine = Engine::new(pool, schema);
//!
//! // GraphQL string path
//! let data = engine
//!     .query("query { users { id name } }", None)
//!     .await?;
//!
//! // Builder path
//! let data = engine
//!     .run(Query::from("users").select(&["id", "name"]).limit(10))
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Features
//!
//! - List, `_by_pk`, and `_aggregate` queries
//! - Nested `Object` and `Array` relations
//! - `EXISTS`-based relation filters in `where`
//! - Full mutation surface (`insert` / `insert_one` / `update` / `update_by_pk`
//!   / `delete` / `delete_by_pk`) with `on_conflict` and `returning`
//! - Comparison operators: `_eq`, `_neq`, `_gt`, `_gte`, `_lt`, `_lte`,
//!   `_like`, `_ilike`, `_nlike`, `_nilike`, `_in`, `_nin`, `_is_null`
//! - `order_by`, `limit`, `offset`, `distinct_on`
//! - GraphQL variables and fragments (named + inline)
//! - Schema introspection plus TOML config overlays
//! - Typed builder API equivalent to the GraphQL path