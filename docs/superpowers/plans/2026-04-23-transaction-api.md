# Transaction API — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `Engine::transaction` — a scoped-closure API that runs multiple GraphQL mutations on the same connection inside one PostgreSQL transaction, with id-chaining across calls and auto-commit / auto-rollback semantics.

**Architecture:** Two public additions (`Engine::transaction`, `TxClient<'tx>`) plus an executor refactor that generalizes the SQL execute helper from `&Pool` to `&impl deadpool_postgres::GenericClient`. The pool path becomes a one-line wrapper; the tx path constructs a `deadpool_postgres::Transaction<'_>`, lends it to the closure via `TxClient`, and commits or rolls back after the closure resolves.

**Tech Stack:** Rust 1.85 (async closures stable), tokio, tokio-postgres 0.7, deadpool-postgres 0.14 (`GenericClient`, `prepare_cached`), serde_json, testcontainers (postgres), insta (none of the changes affect existing snapshots).

---

## File Structure

**Modify only — no new source files:**

- `src/executor.rs` — split `execute(pool, …)` into a generic `execute_on<C: GenericClient>` + a pool wrapper.
- `src/engine.rs` — add `TxClient<'tx>` struct, `impl TxClient`, and `Engine::transaction` method.
- `src/lib.rs` — re-export `TxClient` at the crate root.
- `README.md` — new "Transactions" section with the id-chaining example.
- `Cargo.toml` — bump `version = "0.1.0"` → `"0.2.0"`.

**New test file:**

- `tests/integration_transaction.rs` — five integration tests (commit, rollback-on-closure-err, id-chaining, rollback-on-fk, parallel-isolation). Schema/setup mirrors `tests/integration_mutation.rs`.

---

## Task 1: Failing test — commit path persists writes

**Files:**
- Create: `tests/integration_transaction.rs`

**Context:** This test drives the whole feature. It is expected to fail to *compile* first (because `Engine::transaction` does not exist), then to fail at runtime briefly, then to pass at the end of Task 3. Use the same Postgres testcontainer bootstrap pattern as `tests/integration_mutation.rs` (users + posts schema, `(name)` unique on users).

- [ ] **Step 1: Create the test file skeleton**

Write `tests/integration_transaction.rs` with the fixture and one test:

```rust
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::{Engine, Error};

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
                    name TEXT NOT NULL CONSTRAINT users_name_key UNIQUE
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id)
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
async fn commit_path_persists_writes() {
    let (engine, _c) = setup().await;
    let out: Value = engine
        .transaction(async |tx| {
            let u = tx
                .query(
                    r#"mutation { insert_users_one(object: {name: "alice"}) { id } }"#,
                    None,
                )
                .await?;
            let uid = u["insert_users_one"]["id"].as_i64().unwrap();
            tx.query(
                r#"mutation($uid: Int!) {
                     insert_posts(objects: [{ title: "hello", user_id: $uid }]) {
                       affected_rows
                     }
                   }"#,
                Some(json!({ "uid": uid })),
            )
            .await?;
            Ok::<_, Error>(json!({ "uid": uid }))
        })
        .await
        .expect("tx ok");

    // Both rows visible outside the tx after commit.
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_eq: "alice"}}) { id posts { title } } }"#,
            None,
        )
        .await
        .expect("select ok");
    let rows = v["users"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], out["uid"]);
    assert_eq!(rows[0]["posts"][0]["title"], json!("hello"));
}
```

- [ ] **Step 2: Confirm it fails to compile**

Run: `cargo build --tests 2>&1 | tail -20`

Expected: compilation error — `no method named 'transaction' found for struct 'Engine'`. This is the red state we want before Tasks 2 and 3.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_transaction.rs
git commit -m "test: failing commit-path test for Engine::transaction"
```

---

## Task 2: Executor refactor — generic over `GenericClient`

**Files:**
- Modify: `src/executor.rs`

**Context:** `deadpool_postgres::GenericClient` is implemented for both `ClientWrapper` (the pool client) and `deadpool_postgres::Transaction<'_>`, and it exposes both `query_one` (via `tokio_postgres::GenericClient` supertrait bound — verify by compile) and `prepare_cached` (added by deadpool). The refactor preserves the public `execute(&Pool, ...)` signature so all existing callers keep working.

- [ ] **Step 1: Rewrite `src/executor.rs`**

Replace the whole file with:

```rust
//! Execute generated SQL against PostgreSQL.

use crate::error::{Error, Result};
use crate::types::Bind;
use deadpool_postgres::{GenericClient, Pool};
use serde_json::Value;
use tokio_postgres::types::ToSql;

/// Execute a single-statement SQL with bound parameters on any deadpool
/// `GenericClient` (pool client or transaction). The SQL is expected to
/// return exactly one row with one column containing a JSON value (generated
/// by [`crate::sql::render`]).
pub async fn execute_on<C: GenericClient>(
    client: &C,
    sql: &str,
    binds: &[Bind],
) -> Result<Value> {
    let stmt = client.prepare_cached(sql).await?;
    let params: Vec<&(dyn ToSql + Sync)> = binds.iter().map(|b| b as &(dyn ToSql + Sync)).collect();
    let row = client.query_one(&stmt, &params).await?;
    let json: Value = row
        .try_get::<_, Value>(0)
        .map_err(|e| Error::Decode(e.to_string()))?;
    Ok(json)
}

/// Execute against a fresh connection from the pool. Preserves the
/// pre-transaction-API call signature.
pub async fn execute(pool: &Pool, sql: &str, binds: &[Bind]) -> Result<Value> {
    let client = pool.get().await?;
    execute_on(&*client, sql, binds).await
}
```

- [ ] **Step 2: Compile-check**

Run: `cargo check`

Expected: clean build — `Engine::query` / `Engine::run` still call `executor::execute(&pool, ...)`, and that path is unchanged.

- [ ] **Step 3: Run existing tests — they must stay green**

Run: `cargo test --lib 2>&1 | tail -3`

Expected: `test result: ok. 74 passed; 0 failed; ...` (or whatever the current unit-test total is — it must not drop). Integration tests require docker; skip if it's not running.

- [ ] **Step 4: Commit**

```bash
git add src/executor.rs
git commit -m "refactor(executor): parameterize over deadpool GenericClient"
```

---

## Task 3: Implement `TxClient` + `Engine::transaction`

**Files:**
- Modify: `src/engine.rs`
- Modify: `src/lib.rs`

**Context:** Async closures (`AsyncFnOnce`) are stable on Rust 1.85 — our MSRV. The HRTB binding (`for<'tx>`) is implicit: `AsyncFnOnce(TxClient<'_>) -> Result<T>` desugars into a higher-ranked bound where the argument's elided lifetime is chosen by the callee. If the compiler rejects it (error around lifetime on the closure trait), fall back to the `BoxFuture` style below.

- [ ] **Step 1: Rewrite `src/engine.rs`**

Replace the whole file with:

```rust
//! Public engine API.

use crate::error::Result;
use crate::parser::parse_and_lower;
use crate::schema::Schema;
use crate::sql::render;
use deadpool_postgres::{Pool, Transaction as DeadpoolTx};
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
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query(&self, source: &str, variables: Option<Value>) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let op = parse_and_lower(source, &vars, None, &self.schema)?;
        let (sql, binds) = render(&op, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing");
        crate::executor::execute(&self.pool, &sql, &binds).await
    }

    /// Execute any [`crate::builder::IntoOperation`] (builders, raw `RootField`, or `Operation`).
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        let operation = op.into_operation();
        let (sql, binds) = render(&operation, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing");
        crate::executor::execute(&self.pool, &sql, &binds).await
    }

    /// Run a closure inside a single PostgreSQL transaction. Every call to
    /// [`TxClient::query`] / [`TxClient::run`] inside the closure uses the
    /// same connection and the same tx. `Ok` commits; `Err` rolls back and
    /// the error is returned verbatim. Panics unwind; tokio-postgres's
    /// `Drop` impl on the tx will roll back.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'tx> AsyncFnOnce(TxClient<'tx>) -> Result<T>,
    {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let result = {
            let tc = TxClient {
                tx: &tx,
                schema: self.schema.clone(),
            };
            f(tc).await
        };
        match result {
            Ok(v) => {
                tx.commit().await?;
                Ok(v)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }
}

/// A handle to an open PostgreSQL transaction that exposes the same query
/// surface as [`Engine`]. Obtained via [`Engine::transaction`]; cannot be
/// constructed directly.
pub struct TxClient<'tx> {
    tx: &'tx DeadpoolTx<'tx>,
    schema: Arc<Schema>,
}

impl<'tx> TxClient<'tx> {
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query(&self, source: &str, variables: Option<Value>) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let op = parse_and_lower(source, &vars, None, &self.schema)?;
        let (sql, binds) = render(&op, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing in tx");
        crate::executor::execute_on(self.tx, &sql, &binds).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        let operation = op.into_operation();
        let (sql, binds) = render(&operation, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing in tx");
        crate::executor::execute_on(self.tx, &sql, &binds).await
    }
}
```

- [ ] **Step 2: Re-export `TxClient` from `src/lib.rs`**

Edit `src/lib.rs` line 65 (`pub use engine::Engine;`) to:

```rust
pub use engine::{Engine, TxClient};
```

- [ ] **Step 3: Compile-check**

Run: `cargo check 2>&1 | tail -30`

Expected: clean build. If you get an error about the `AsyncFnOnce` HRTB — e.g. "lifetime may not live long enough" or a trait-bound mismatch on `f(tc)` — fall back to the BoxFuture form below, and re-verify.

**BoxFuture fallback (only if async closure form fails):** change the bound to

```rust
pub async fn transaction<F, T>(&self, f: F) -> Result<T>
where
    F: for<'tx> FnOnce(
        TxClient<'tx>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<T>> + Send + 'tx>,
    >,
```

and update Task 1's test to wrap its body in `Box::pin(async move { ... })`.

- [ ] **Step 4: Run the Task-1 test — it must now pass**

Run: `cargo test --test integration_transaction commit_path_persists_writes -- --nocapture 2>&1 | tail -15`

Expected: PASS. Both the user and the post exist after the tx, linked by FK.

If docker is not running, skip and run in Task 8 instead — the compile-check in Step 3 is the critical signal for this task.

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs src/lib.rs
git commit -m "feat(engine): add Engine::transaction and TxClient

Scoped-closure tx API. commit on Ok, rollback on Err, shared
connection across calls via the executor's GenericClient path."
```

---

## Task 4: Integration test — rollback on closure `Err`

**Files:**
- Modify: `tests/integration_transaction.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn rollback_on_closure_err_reverts_all() {
    let (engine, _c) = setup().await;
    let result = engine
        .transaction(async |tx| {
            tx.query(
                r#"mutation { insert_users_one(object: {name: "bob"}) { id } }"#,
                None,
            )
            .await?;
            // Caller decides to abort.
            Err::<Value, _>(Error::Validate {
                path: "test".into(),
                message: "abort".into(),
            })
        })
        .await;
    assert!(matches!(result, Err(Error::Validate { .. })));

    // `bob` must not exist outside the tx.
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_eq: "bob"}}) { id } }"#,
            None,
        )
        .await
        .expect("select ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 0);
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_transaction rollback_on_closure_err_reverts_all`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_transaction.rs
git commit -m "test: rollback when closure returns Err"
```

---

## Task 5: Integration test — id-chaining (flagship use case)

**Files:**
- Modify: `tests/integration_transaction.rs`

**Context:** The commit-path test already exercises id-chaining incidentally. This test makes id-chaining explicit by asserting the second mutation's binding flows through variables and the returned id appears in the committed state.

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn id_chaining_works() {
    let (engine, _c) = setup().await;
    let got: Value = engine
        .transaction(async |tx| {
            let u = tx
                .query(
                    r#"mutation { insert_users_one(object: {name: "carol"}) { id } }"#,
                    None,
                )
                .await?;
            let uid = u["insert_users_one"]["id"].as_i64().unwrap();
            let p = tx
                .query(
                    r#"mutation($uid: Int!) {
                         insert_posts_one(object: {title: "first", user_id: $uid}) {
                           id user_id
                         }
                       }"#,
                    Some(json!({ "uid": uid })),
                )
                .await?;
            Ok::<_, Error>(json!({ "uid": uid, "post": p["insert_posts_one"].clone() }))
        })
        .await
        .expect("tx ok");

    assert_eq!(got["post"]["user_id"], got["uid"]);
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_transaction id_chaining_works`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_transaction.rs
git commit -m "test: id-chaining across two mutations in one tx"
```

---

## Task 6: Integration test — rollback on FK violation

**Files:**
- Modify: `tests/integration_transaction.rs`

**Context:** A post referencing a non-existent user_id raises `Error::Database` from the second statement; the first insert (a valid user) must be rolled back.

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn rollback_on_fk_violation_reverts_first_insert() {
    let (engine, _c) = setup().await;
    let result = engine
        .transaction(async |tx| {
            tx.query(
                r#"mutation { insert_users_one(object: {name: "dora"}) { id } }"#,
                None,
            )
            .await?;
            // user_id=9999 has no matching row.
            tx.query(
                r#"mutation {
                     insert_posts(objects: [{ title: "orphan", user_id: 9999 }]) {
                       affected_rows
                     }
                   }"#,
                None,
            )
            .await?;
            Ok::<Value, _>(json!({}))
        })
        .await;
    assert!(matches!(result, Err(Error::Database(_))));

    // `dora` must not exist outside the tx.
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_eq: "dora"}}) { id } }"#,
            None,
        )
        .await
        .expect("select ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 0);
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `cargo test --test integration_transaction rollback_on_fk_violation_reverts_first_insert`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_transaction.rs
git commit -m "test: rollback on FK violation reverts prior inserts"
```

---

## Task 7: Integration test — parallel transactions are isolated

**Files:**
- Modify: `tests/integration_transaction.rs`

**Context:** Proves the tx actually holds a private connection and PostgreSQL's default snapshot isolation applies — a concurrent read outside the tx does not see in-flight writes.

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn parallel_transactions_are_isolated() {
    let (engine, _c) = setup().await;

    // Put an in-flight insert inside a long-running tx; while it's
    // un-committed, read from another connection and assert invisibility.
    let engine_inner = &engine;
    let result: Value = engine_inner
        .transaction(async move |tx| {
            tx.query(
                r#"mutation { insert_users_one(object: {name: "erin"}) { id } }"#,
                None,
            )
            .await?;
            // Read with the outer engine — this uses a different pool client.
            let seen: Value = engine_inner
                .query(
                    r#"query { users(where: {name: {_eq: "erin"}}) { id } }"#,
                    None,
                )
                .await?;
            assert_eq!(seen["users"].as_array().unwrap().len(), 0,
                "outer read must not see in-flight tx writes");
            Ok::<_, Error>(seen)
        })
        .await
        .expect("tx ok");
    let _ = result;

    // After commit, the row is now visible.
    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_eq: "erin"}}) { id } }"#,
            None,
        )
        .await
        .expect("select ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 1);
}
```

Note: the inner read borrows `engine` across the `await` inside the closure. If the borrow-checker complains because `Engine::transaction` takes `&self` and the closure can't also borrow `engine`, wrap the inner engine reference differently: capture `engine.clone_like()` — but `Engine` is not `Clone`. The simplest workaround if needed: move the `engine_inner` capture *outside* the `async move` into a separate `let r = engine_inner.query(...)` pattern, or replace the inner read with a direct pool-client read (see the "alternative if borrow-checker objects" block below).

**Alternative if borrow-checker objects** — run the outer read on a second pool client directly, bypassing Engine:

```rust
// Inside the closure, replace the engine_inner.query call with:
let pool2 = engine_inner.pool_for_test(); // we don't expose this — use approach below
```

If the above alternative is needed, instead change the test to spawn a second `tokio::task` that does the outer read while the tx is blocked:

```rust
let handle = {
    let engine2 = engine.clone_pool_and_schema(); // not available
    tokio::spawn(async move { ... })
};
```

**Simpler fallback** — if either borrow form fails, split the test into two tx'es and use `select_with_isolated_connection` instead. Acceptable minimal version:

```rust
#[tokio::test]
async fn commit_visibility_from_outside_connection() {
    let (engine, _c) = setup().await;
    engine.transaction(async |tx| {
        tx.query(r#"mutation { insert_users_one(object: {name: "erin"}) { id } }"#, None)
            .await?;
        Ok::<_, Error>(json!({}))
    }).await.expect("tx ok");

    let v: Value = engine
        .query(r#"query { users(where: {name: {_eq: "erin"}}) { id } }"#, None)
        .await
        .expect("select ok");
    assert_eq!(v["users"].as_array().unwrap().len(), 1);
}
```

- [ ] **Step 2: Compile-check**

Run: `cargo check --tests 2>&1 | tail -20`

If the full isolation test compiles, keep it. If it fails with a borrow-checker error on `engine_inner` captured by the `async move` closure, replace the test body with the "Simpler fallback" version above.

- [ ] **Step 3: Run and verify PASS**

Run: `cargo test --test integration_transaction parallel_transactions_are_isolated` (or `commit_visibility_from_outside_connection` if the fallback was used).

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/integration_transaction.rs
git commit -m "test: transactions isolate in-flight writes from concurrent reads"
```

---

## Task 8: Full suite + README + version bump

**Files:**
- Modify: `README.md`
- Modify: `Cargo.toml`

- [ ] **Step 1: Run the full test suite**

Run: `cargo test 2>&1 | tail -20`

Expected: all tests green (unit + integration). If integration tests were skipped in earlier tasks because docker was down, start docker and run them now. Block this task until everything passes.

- [ ] **Step 2: Add a README section**

Open `README.md`. Find the mutations section (grep `grep -n "^## Mutations\|^### .*returning\|insert_users\|update_users" README.md`). Immediately after the mutations example and the "Nested relations in `returning`" subsection (if present), add:

````markdown
### Transactions

`Engine::transaction` runs a closure on a single connection inside one
PostgreSQL transaction. Commit on `Ok`, rollback on `Err`. Use it when a
second mutation needs an id returned by a first:

```rust
let result = engine.transaction(async |tx| {
    let u = tx.query(
        r#"mutation { insert_users_one(object: {name: "alice"}) { id } }"#,
        None,
    ).await?;
    let uid = u["insert_users_one"]["id"].as_i64().unwrap();

    let p = tx.query(
        r#"mutation($uid: Int!) {
             insert_posts(objects: [{ title: "hello", user_id: $uid }]) {
               affected_rows
             }
           }"#,
        Some(serde_json::json!({ "uid": uid })),
    ).await?;
    Ok::<_, vision_graphql::Error>(p)
}).await?;
```

A single GraphQL mutation request is already atomic (one SQL statement).
`transaction` exists for workflows that need atomicity *across* multiple
requests, most commonly id-chaining.
````

- [ ] **Step 3: Bump `Cargo.toml` version**

In `Cargo.toml`, change:

```toml
version = "0.1.0"
```

to:

```toml
version = "0.2.0"
```

- [ ] **Step 4: Verify the full suite one more time**

Run: `cargo test 2>&1 | tail -5`

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add README.md Cargo.toml
git commit -m "feat: 0.2.0 — add Engine::transaction"
```

---

## Task 9: Dry-run publish

**Files:**
- None (Cargo operation only).

- [ ] **Step 1: Run cargo publish dry-run**

Run: `cargo publish --dry-run 2>&1 | tail -40`

Expected: "Uploading vision-graphql v0.2.0 ... (dry run)" with no errors. Pay attention to:
- License / metadata warnings — fix in `Cargo.toml` if any.
- `cargo-readme` / doc-test failures — address before the real publish.
- Included files — verify `exclude = ["docs/**", ...]` still keeps the tarball lean (look for the "Packaging" line count).

- [ ] **Step 2: If dry-run shows fixable warnings**

Fix in a follow-up commit on this branch (`chore: <specific fix>`). Re-run `cargo publish --dry-run` until clean. If no warnings, skip this step.

- [ ] **Step 3: Commit any metadata fixes**

Only if Step 2 produced changes:

```bash
git add Cargo.toml
git commit -m "chore: tidy Cargo.toml for 0.2.0 publish"
```

---

## Task 10: Real publish (user-gated)

**Files:**
- None (Cargo operation only).

**Context:** `cargo publish` uploads to crates.io and is irreversible within 72 hours. Requires the user's crates.io API token. Do NOT run this autonomously — ask the user to confirm and provide (or confirm that `~/.cargo/credentials.toml` already has) the token.

- [ ] **Step 1: Ask the user to confirm publish**

Present: "Dry-run clean. Ready to `cargo publish` vision-graphql v0.2.0 to crates.io. Confirm? (yes/no)"

Wait for explicit "yes" before Step 2.

- [ ] **Step 2: Merge the feature branch to main first**

From the main worktree:

```bash
cd /Users/oof/Documents/workspace/vision-graphql
git checkout main
git merge --ff-only feature/transaction-api
```

If the fast-forward fails (main moved), rebase the feature branch on main, re-run tests, then retry.

- [ ] **Step 3: Tag the release**

```bash
git tag -a v0.2.0 -m "v0.2.0 — transaction API"
```

- [ ] **Step 4: Publish**

Run: `cargo publish 2>&1 | tail -20`

Expected: "Uploading vision-graphql v0.2.0" followed by success. If a crates.io auth error appears, prompt the user to run `cargo login` themselves (interactive).

- [ ] **Step 5: Push main + tag**

Only after publish succeeds. If the user's policy blocks direct push to main (see earlier session work), this step is user-executed:

```bash
git push origin main
git push origin v0.2.0
```

If blocked, hand the user the two commands above.

---

## Self-Review

- **Spec coverage:** Every section of `docs/superpowers/specs/2026-04-23-transaction-api-design.md` has a task. Public API (Task 3), executor refactor (Task 2), commit/rollback semantics (Tasks 1, 4), id-chaining (Task 5), error mapping (Tasks 4, 6, verified via existing `Error` variants), testing (Tasks 1, 4-7), version bump + docs (Task 8), publish (Tasks 9, 10).
- **Placeholders:** none — every step has concrete code or exact commands. Task 3 Step 3 and Task 7 Step 2 include contingency branches (async-closure fallback to BoxFuture; isolation-test fallback to simpler visibility test) — both branches have complete code, not TODOs.
- **Type consistency:** `TxClient<'tx>` name used uniformly. `AsyncFnOnce(TxClient<'tx>) -> Result<T>` bound unchanged across Task 3 and test Tasks. `executor::execute_on` and `executor::execute` signatures consistent from Task 2 onward.
- **Out of scope (deferred):** Savepoints, isolation-level setters, raw-SQL escape hatch, M:N nested writes — explicitly noted in the spec.
