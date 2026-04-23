# Transaction API — Design Spec

**Date:** 2026-04-23
**Status:** Approved (ready for implementation plan)

## Motivation

A single GraphQL mutation request is already atomic in vision-graphql — `render_mutation` (`src/sql.rs:723`) composes every top-level mutation field into one `WITH … SELECT` SQL statement, which PostgreSQL treats as an implicit single-statement transaction.

What is **not** atomic today is work that must span **multiple** `Engine::query` / `Engine::run` calls. The typical trigger is id-chaining: the second request needs a value returned by the first. Common cases:

- Insert a parent, then insert rows into a junction table whose FK is the parent's id (the "分解方案" workaround for M:N writes).
- Multi-step workflows combining a read check, an update, and a follow-up insert where all three must succeed or none.

Callers can dodge this today only by building their own `deadpool_postgres` tx and dropping down to raw SQL — losing the entire GraphQL→JSON pipeline. The transaction API closes that gap.

## Non-goals

- **Not** a Hasura-style M:N nested write feature. That remains deferred; transactions only make client-side decomposition safe, not declarative.
- **Not** savepoints / nested transactions.
- **Not** custom isolation levels (PostgreSQL default `READ COMMITTED` is used; advanced users can still issue `SET TRANSACTION` themselves if they really need it — see Escape Hatch).

## Public API

```rust
// src/engine.rs

impl Engine {
    /// Run a closure inside a single PostgreSQL transaction. Every call to
    /// `TxClient::query` / `TxClient::run` inside the closure executes on the
    /// same connection, in the same tx. If the closure returns `Ok`, the tx
    /// is committed. If it returns `Err` (or panics), the tx is rolled back.
    pub async fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'tx> AsyncFnOnce(TxClient<'tx>) -> Result<T>;
}

/// A handle to an open PostgreSQL transaction that exposes the same query
/// surface as [`Engine`]. Obtained via [`Engine::transaction`]; cannot be
/// constructed directly.
pub struct TxClient<'tx> { /* private */ }

impl<'tx> TxClient<'tx> {
    pub async fn query(&self, source: &str, variables: Option<Value>) -> Result<Value>;
    pub async fn run(&self, op: impl IntoOperation) -> Result<Value>;
}
```

### Usage

```rust
let posts = engine.transaction(async |tx| {
    let u = tx.query(
        r#"mutation { insert_users_one(object: {name: "alice"}) { id } }"#,
        None,
    ).await?;
    let uid = u["insert_users_one"]["id"].as_i64().unwrap();

    let p = tx.query(
        r#"mutation($uid: Int!) {
             insert_posts(objects: [
               { title: "hello", user_id: $uid, published: true }
             ]) { returning { id title } }
           }"#,
        Some(serde_json::json!({ "uid": uid })),
    ).await?;

    Ok::<_, Error>(p)
}).await?;
```

If either call returns `Err`, the tx is rolled back and the first insert is undone.

## Internals

### Lifetime shape

`tokio_postgres::Transaction<'c>` borrows from its owning `Client` for its lifetime. To make that workable:

- `Engine::transaction` acquires the pool client and opens the tx, both pinned to the function's stack frame.
- It builds `TxClient<'tx> { tx: &'tx Transaction<'tx>, schema: Arc<Schema> }`.
- It passes `TxClient` *by value* to the closure. The closure sees a `'tx`-bounded handle, so it cannot escape the borrow.
- After the closure resolves, the `TxClient` is dropped, the borrow on `tx` is released, and `Engine::transaction` calls `tx.commit()` or `tx.rollback()`.

Why HRTB (`for<'tx>`): the caller must not be able to pick `'tx`; it is invented by `Engine::transaction` and must match the tx that function opens. Without HRTB, `'tx` would bind to some outer lifetime the caller controls, defeating the whole design.

### Executor refactor

Today `executor::execute(pool: &Pool, sql, binds)` acquires the client itself and calls `client.prepare_cached`. Use `deadpool_postgres::GenericClient` as the bound — its trait exposes both `query_one` and `prepare_cached`, and `deadpool_postgres` implements it for its `ClientWrapper` **and** its `Transaction<'_>` (`deadpool-postgres-0.14.1/src/generic_client.rs:22,162,252`). That lets the tx path preserve prepare-cache reuse across multiple calls inside one closure.

```rust
// Generic over the deadpool GenericClient — both pool client and tx client impl it.
pub(crate) async fn execute_on<C: deadpool_postgres::GenericClient>(
    client: &C,
    sql: &str,
    binds: &[Bind],
) -> Result<Value>;

// Thin wrapper — preserves current `execute(&pool, …)` call sites.
pub async fn execute(pool: &Pool, sql: &str, binds: &[Bind]) -> Result<Value> {
    let client = pool.get().await?;
    execute_on(&*client, sql, binds).await
}
```

Both `Engine::query` and `TxClient::query` call `execute_on` underneath.

### Commit / rollback

```rust
let result = f(tx_client).await;
match result {
    Ok(v)  => { tx.commit().await?; Ok(v) }
    Err(e) => {
        let _ = tx.rollback().await;   // best-effort; preserve original error
        Err(e)
    }
}
```

Panic inside the closure unwinds through `Engine::transaction`; tokio_postgres' `Drop` impl on `Transaction` issues a rollback on drop. We do not catch panics.

### Escape hatch (deferred)

`TxClient::query` and `TxClient::run` only accept the GraphQL / builder pipeline, not raw SQL. Users who need `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`, `SET LOCAL lock_timeout = ...`, or any statement outside the GraphQL surface must use `deadpool-postgres` directly (bypassing `Engine::transaction`). Adding a raw-SQL escape on `TxClient` is deferred — the primary use case the API solves is id-chaining across GraphQL mutations, which raw-SQL users do not need.

## Error handling

- Pool acquisition failure: `Error::Pool` (existing).
- Tx open failure: `Error::Db` (existing, from tokio-postgres).
- Closure `Err`: returned verbatim after rollback.
- Commit failure: propagates as `Error::Db`, tx already closed.
- Rollback failure: swallowed — we prefer to surface the original closure error.

## Testing

Each test lives in a new `tests/integration_transaction.rs` using the same setup helper style as the other integration tests.

1. **commit_path_persists_writes** — tx inserts user + post, commits, a follow-up `SELECT` sees both.
2. **rollback_on_closure_err_reverts_all** — tx inserts user, closure returns `Err`; follow-up `SELECT` shows neither the user nor any partial state.
3. **id_chaining_works** — flagship use case: first `insert_users_one` returns `id`, second mutation uses `id` via a bound variable, both commit.
4. **rollback_on_second_failure** — first insert succeeds inside the tx, second insert violates FK, closure returns `Err(Error::Db(_))`, committed state shows zero rows added.
5. **parallel_transactions_are_isolated** — tx A inserts but does not yet commit; concurrently tx B selects and does not see A's row. After A commits, B (new tx) sees it. Asserts the two txs run on different connections and enforce standard snapshot isolation.

No unit tests — the transaction path has no SQL-shape changes. Snapshots unaffected.

## Version / docs

- Bump `Cargo.toml` `0.1.0 → 0.2.0` (additive new public API, SemVer minor per Cargo guidelines).
- README: new section `Transactions` with the id-chaining example above, directly under the mutations section.
- No CHANGELOG file exists today; the commit log is the history of record.

## Out of scope (deferred)

- Hasura-style M:N nested writes — revisit if transaction-based decomposition proves insufficient for real users.
- Savepoints / nested tx.
- Explicit isolation-level / lock-timeout setters.
- Raw SQL escape inside the closure.
- Multi-statement batches (`tx.batch(...)`).
