# vision-graphql

A Hasura-style ORM for PostgreSQL in Rust. Accepts GraphQL query strings (or a typed Rust builder) and returns `serde_json::Value` in Hasura's data shape. Single SQL per request via PostgreSQL's `json_agg`/`row_to_json` — no N+1.

## Quick start

```rust
use sqlx::postgres::PgPoolOptions;
use vision_graphql::{Engine, Query, Schema};

# async fn example() -> anyhow::Result<()> {
// Any sqlx PgPool works — share the one your app already has.
let pool = PgPoolOptions::new()
    .connect("postgres://localhost/mydb")
    .await?;

// Option 1: introspect the database
let schema = Schema::introspect(&pool).await?.build();
let engine = Engine::new(pool, schema);

// GraphQL string path
let data = engine
    .query(
        r#"query($id: Int!) {
             users(where: {id: {_eq: $id}}) {
               id name
               posts(limit: 5) { title }
             }
           }"#,
        Some(serde_json::json!({ "id": 1 })),
    )
    .await?;

// Builder path
let data = engine
    .run(
        Query::from("users")
            .select(&["id", "name"])
            .where_eq("active", true)
            .limit(10),
    )
    .await?;

// Typed path — unwraps the single root field and deserializes
#[derive(serde::Deserialize)]
struct User { id: i64, name: Option<String> }

let users: Vec<User> = engine
    .run_as(Query::from("users").select(&["id", "name"]))
    .await?;
# Ok(()) }
```

`run_as` unwraps the root key for you: `Query::from` → `Vec<T>`, `Query::by_pk`
→ `Option<T>`, and `insert`/`update`/`delete` → `MutationResult<T>`
(`{ affected_rows, returning }`). `query_as` deserializes the whole `data`
envelope for multi-root GraphQL strings. The untyped `query`/`run` returning
`serde_json::Value` remain for passthrough use.

## Features

| Area | Status |
|---|---|
| Select, `_by_pk`, `_aggregate` | ✓ |
| Object + Array relations | ✓ |
| `EXISTS` relation filters in `where` | ✓ |
| Mutations: `insert` / `insert_one` / `update` / `update_by_pk` / `delete` / `delete_by_pk` | ✓ |
| `on_conflict` upsert | ✓ |
| `returning` clause on mutations (with nested relations) | ✓ |
| Multi-request transactions (`Engine::transaction`) | ✓ |
| Operators: `_eq`/`_neq`/`_gt`/`_gte`/`_lt`/`_lte`/`_like`/`_ilike`/`_nlike`/`_nilike`/`_in`/`_nin`/`_is_null` | ✓ |
| `order_by` / `limit` / `offset` / `distinct_on` | ✓ |
| GraphQL variables, named + inline fragments | ✓ |
| Schema introspection | ✓ |
| PG enum / `date` / `time` columns (enum casts are schema-qualified) | ✓ |
| Enum array columns (`role_type[]`) | Not implemented (skipped at introspection) |
| TOML config overlay (`expose_as`, `hide_columns`, manual relations) | ✓ |
| Typed Rust builder API | ✓ |
| Typed results: `run_as::<T>` / `query_as::<T>` / `MutationResult<T>` | ✓ |
| Scoped execution: `Engine::scoped(ScopeSet)`, per-table predicates, deny-by-default | ✓ read queries + `delete` (incl. `_by_pk`) + `update` (filter + post-update check) + `insert` (post-insert check at every nested level, upsert pre-image filter) |
| Computed fields | Not implemented |
| Subscriptions | Not implemented |

## Architecture

```
[GraphQL string] ─┐
                  ├─→ IR (Operation) ─→ SQL + binds ─→ PostgreSQL ─→ serde_json::Value
[Rust builder]  ─┘
```

One SQL per request. All user values go through parameterized binds — there is no string interpolation of values. See `docs/superpowers/specs/2026-04-17-rust-hasura-orm-design.md` for the full design.

## Building the schema

Three layers that merge (later wins):

1. **Introspection** — `Schema::introspect(&pool).await?` queries `information_schema` and auto-derives relations from foreign keys.
2. **TOML overlay** — `.load_config("schema.toml")?` applies renames, hidden columns, and manual relations. Run `vision-gql generate` to bootstrap a starter file from a live DB.
3. **Builder** — `.table(...)` / `.relation(...)` / `.expose_as(...)` for final touches before `.build()`.

Example TOML:

```toml
[tables.users]
expose_as = "profiles"
hide_columns = ["password_hash"]

[[tables.users.relations]]
name = "followers"
kind = "array"
target = "profiles"
mapping = [["id", "followed_id"]]
```

## CLI

`vision-graphql-cli` ships a `vision-gql` binary that bootstraps and validates
overlay files against a live database.

```bash
cargo install vision-graphql-cli
vision-gql generate --url postgres://localhost/myapp > schema.toml
vision-gql diff     --url postgres://localhost/myapp --config schema.toml
vision-gql validate schema.toml
```

`generate` produces a fully-commented starter file; uncomment any stanza to
override defaults from introspection. `diff` checks the overlay's references
against the current database (exit 0 = clean, 1 = drift, 2 = error). `validate`
performs offline structural checks without a connection.

Filter what gets processed with comma-separated globs:

```bash
vision-gql generate --url $DATABASE_URL --ignore-tables 'audit_*,_temp_*'
```

Both subcommands accept `$DATABASE_URL` as the default for `--url`. TLS is
supported via rustls (`sslmode=require` in the URL); only the `public` schema
is introspected.

## Mutations

All mutation root fields (`insert_*`, `insert_*_one`, `update_*`, `update_*_by_pk`, `delete_*`, `delete_*_by_pk`) support a `returning` clause. Relation fields in `returning` work exactly like relation fields in `SELECT` queries — they expand to correlated subqueries with no N+1:

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

The same nesting is supported on `_by_pk` variants:

```graphql
mutation {
  update_users_by_pk(pk_columns: { id: 1 }, _set: { name: "bob" }) {
    id
    posts { title }
  }
}
```

### Nested one-to-many insert

Array relations can be inserted alongside their parent in a single atomic
mutation. The input uses Hasura's `{ data: [...] }` shape so that `on_conflict`
can be added as a sibling in a future release without a breaking change.

```graphql
mutation {
  insert_users(objects: [
    {
      name: "alice",
      posts: { data: [
        { title: "p1" },
        { title: "p2", published: true }
      ]}
    }
  ]) {
    affected_rows          # includes parents + every descendant
    returning {
      id
      name
      posts { title }      # sees freshly-inserted children
    }
  }
}
```

Nesting is arbitrary-depth (e.g. users → posts → comments).

### Nested many-to-one insert

Object relations can be inserted alongside their parent in the same mutation.
The new entity is inserted first, and its PK is used as the parent's FK:

```graphql
mutation {
  insert_posts(objects: [
    { title: "p1", user: { data: { name: "alice" } } },
    { title: "p2", user: { data: { name: "bob"   } } }
  ]) {
    affected_rows            # 4: 2 users + 2 posts
    returning {
      title
      user { name }          # reads from the freshly-inserted users CTE
    }
  }
}
```

Combines freely with one-to-many nesting — a parent can carry both object and
array children in one row. Object-relation recursion also works arbitrarily
deep (e.g. post → user → organization).

**Batch-uniform constraint:** within a single `objects: [...]`, either every
row uses `<rel>: { data: {...} }` for a given object relation, or no row does.
Mixed usage is rejected; split into two mutation fields instead.

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
is silently rewritten to `DO UPDATE SET <pk> = <table>.<pk>` — a no-op update
that forces PostgreSQL's `RETURNING` to include conflict rows so the
just-inserted parent's foreign key can point at the existing entity. Top-level
`on_conflict` semantics are unchanged — `update_columns: []` still means
`DO NOTHING` at top level.

This requires a primary key on the nested table; tables without a PK cannot use
nested `DO NOTHING` (supply non-empty `update_columns` instead).

## Scoped execution

`Engine::scoped(ScopeSet)` returns a handle that rewrites every query before
rendering: each table access point — root selects, `_by_pk`, aggregates,
relation subqueries at any depth, and `EXISTS` relation filters inside
`where` — gets the table's predicate AND-ed in. Tables without an entry are
denied (fail-closed), so the set must spell out everything the caller may
touch. The mechanism is policy-agnostic: how predicates are derived (RBAC,
ownership chains, …) is up to the application.

```rust
# use vision_graphql::{Engine, ScopeSet, Query};
# use vision_graphql::ast::{BoolExpr, CmpOp};
# async fn example(engine: Engine, user_id: i64) -> Result<(), vision_graphql::Error> {
let scope = ScopeSet::new()
    .allow("orders", BoolExpr::Compare {
        column: "user_id".into(),
        op: CmpOp::Eq,
        value: user_id.into(),
    })
    .allow("samples", BoolExpr::Relation {            // one-hop ownership chain
        name: "order".into(),
        inner: Box::new(BoolExpr::Compare {
            column: "user_id".into(),
            op: CmpOp::Eq,
            value: user_id.into(),
        }),
    })
    .unrestricted("adverts");                          // public lookup table

let scoped = engine.scoped(scope);
let mine = scoped.query("query { orders { id title } }", None).await?;
// `samples_aggregate`, `orders_by_pk(id: …)`, nested relations — all filtered.
// `scoped.query("query { staffs { id } }", …)` → Error::ScopeDenied.
# Ok(()) }
```

Scope predicates are trusted policy: they are injected as-is and never
re-scoped themselves. `scoped.transaction(…)` hands the closure a
`ScopedTxClient`, so the scope cannot be escaped mid-transaction.

Scoped `delete` (and its `_by_pk` form) injects the predicate as a filter — it
is AND-ed into the statement's `WHERE`, so a scoped caller can only remove rows
already in scope. A `_by_pk` row failing the predicate simply does not match, so
the mutation returns null (the same IDOR-safe behavior as a scoped `by_pk`
query). Tables absent from the `ScopeSet` are denied.

Scoped `update` (and its `_by_pk` form) enforces the predicate *twice*: as a
pre-image filter AND-ed into the `WHERE` (only in-scope rows are touched) and as
a post-update *check* — a guard CTE over the updated rows — so a caller cannot
move a row **out** of scope (e.g. reassign an owning column). A violation aborts
the whole statement; a `_by_pk` row the filter excluded leaves nothing to check
and returns null.

Scoped `insert` injects the predicate as a post-insert *check*: the renderer
wraps the insert in a guard CTE so every inserted row must satisfy the
predicate, and any violation aborts the whole statement (nothing is committed).
Nested inserts (`{ data: … }` children) are enforced at every level — each
nested target table must be in the `ScopeSet` (else `Error::ScopeDenied`), and
its rows are checked against its own predicate. Because the insert and all its
nested children render to a single atomic statement, a violation anywhere rolls
back every level. An upsert (`on_conflict` with `update_columns`) additionally
applies the predicate to the `DO UPDATE … WHERE`, so a conflicting row outside
scope is skipped rather than overwritten.

## Transactions

`Engine::transaction` runs a closure on a single connection inside one
PostgreSQL transaction. The closure returning `Ok(v)` commits; returning `Err`
rolls back. Use it when a second mutation needs an id returned by a first:

```rust
# async fn example(engine: vision_graphql::Engine) -> Result<(), vision_graphql::Error> {
use serde_json::{json, Value};
use vision_graphql::Error;

let post: Value = engine.transaction(async |tx| {
    let u = tx.query(
        r#"mutation { insert_users_one(object: {name: "alice"}) { id } }"#,
        None,
    ).await?;
    let uid = u["insert_users_one"]["id"].as_i64().unwrap();

    let p = tx.query(
        r#"mutation($uid: Int!) {
             insert_posts_one(object: {title: "hello", user_id: $uid}) { id }
           }"#,
        Some(json!({ "uid": uid })),
    ).await?;
    Ok::<_, Error>(p)
}).await?;
# let _ = post;
# Ok(()) }
```

A single GraphQL mutation request is already atomic (one SQL statement per
request). `transaction` exists for workflows that need atomicity *across*
multiple requests — most commonly id-chaining between mutations.

## License

MIT OR Apache-2.0
