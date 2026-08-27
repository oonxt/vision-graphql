# vision-graphql

A Hasura-style GraphQL-to-SQL query engine for PostgreSQL in Rust. Accepts GraphQL query strings (or a typed Rust builder) and returns `serde_json::Value` in Hasura's data shape. Single SQL per request via PostgreSQL's `json_agg`/`row_to_json` — no N+1.

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
| Aggregates on a relation (`user { posts_aggregate { … } }`) | ✓ |
| Aggregates: `count` (incl. `columns:` / `distinct:`), `sum`, `avg`, `max`, `min`, with field aliases | ✓ |
| Object + Array relations | ✓ |
| `EXISTS` relation filters in `where` | ✓ |
| Mutations: `insert` / `insert_one` / `update` / `update_by_pk` / `delete` / `delete_by_pk` | ✓ |
| `on_conflict` upsert | ✓ |
| `returning` clause on mutations (with nested relations) | ✓ |
| Multi-request transactions (`Engine::transaction`) | ✓ |
| Operators: `_eq`/`_neq`/`_gt`/`_gte`/`_lt`/`_lte`/`_like`/`_ilike`/`_nlike`/`_nilike`/`_in`/`_nin`/`_is_null` | ✓ |
| Comparing against `null` is refused, not silently empty | ✓ |
| `order_by` / `limit` / `offset` / `distinct_on` | ✓ |
| `order_by` NULL placement (`asc_nulls_last`, `desc_nulls_last`, …) | ✓ |
| Field aliases (`abundance: data`) | ✓ |
| `__typename` in every selection set | ✓ |
| Schema introspection (`__schema` / `__type`), off by default | ✓ |
| SDL export (`vision-gql sdl`, `--check` for CI) | ✓ |
| JSON/JSONB path reads (`data(path: "a.b")` → `#>`, keeps structure) | ✓ |
| GraphQL variables (incl. declared defaults, `query($n: Int = 10)`), named + inline fragments | ✓ |
| `operationName` (`query_with` / `query_as_with`, on every handle) | ✓ |
| GraphQL-shaped errors (`Error::to_graphql_response`, `Error::code`) | ✓ |
| Multiple schemas in one Schema (`Schema::introspect_schemas`), incl. cross-schema FK relations | ✓ |
| PG enum / `date` / `time` / `smallint` / `character(n)` columns (enum casts are schema-qualified) | ✓ |
| Array, `bytea`, `interval`, `inet` columns | Not mapped — left out of the schema, and reported by `vision-gql diff` |
| TOML config overlay (`expose_as`, `schema`, `hide_columns`, manual relations) | ✓ |
| Typed Rust builder API | ✓ |
| Typed results: `run_as::<T>` / `query_as::<T>` / `MutationResult<T>` | ✓ |
| Column-level scope: `ScopeSet::columns` / `hide_columns`, per role, per request | ✓ |
| Scoped execution: `Engine::scoped(ScopeSet)`, per-table predicates, deny-by-default | ✓ read queries + `delete` (incl. `_by_pk`) + `update` (filter + post-update check) + `insert` (post-insert check at every nested level, upsert pre-image filter) |
| Pre-parse limits on document size and nesting (`ParseLimits`) | ✓ |
| Execution limits: relation depth, table reads, default/max row limit (`ExecutionLimits`) | ✓ |
| Persisted queries: compile a set at startup, run by key (`QueryRegistry`) | ✓ |
| Subscriptions | Not implemented |

### Not implemented

Known gaps, so they are findable rather than discovered. Each has been looked at
and left, not forgotten.

| Gap | Notes |
|---|---|
| `distinct_on` on an `_aggregate` | The aggregate's source does not render it, so it is refused rather than dropped, and not published. `count(columns: […], distinct: true)` counts distinct values. |
| Relations inside aggregate `nodes`, fragments inside `aggregate` | Columns only. |
| `stddev` / `variance` | `count` / `sum` / `avg` / `max` / `min` only. |
| `_regex`, `_similar`, jsonb `_contains` / `_has_key`, array operators | The operators listed above are the ones the lowering implements — and the ones introspection publishes, deliberately. |
| Array, `bytea`, `interval`, `inet` columns | No type mapping: the column is left out of the schema, and `vision-gql diff` reports it. |
| PG enum values | The type is published as a named scalar, not a GraphQL enum: introspection reads the type's name but not its variants. |
| Nested insert / relation `returning` from the typed builder | The GraphQL path has both. |
| Computed fields, subscriptions | Not planned. |

## JSON/JSONB path reads

Read inside a `json`/`jsonb` column with a `path` argument. The path is a
dot-separated string of keys; numeric components index into JSON arrays (both
follow PostgreSQL `#>` semantics). The result keeps its JSON structure, so it
nests inside the response unchanged. Combine with a field alias to rename the key:

```graphql
query {
  samples {
    id
    abundance: data(path: "abundance")   # data #> '{abundance}' AS "abundance"
    first_tag: data(path: "tags.0")       # data #> '{tags,0}'
  }
}
```

Works anywhere a column is selected — top-level, nested relations, `_by_pk`,
mutation `returning`, and aggregate `nodes`. The typed builder has the parallel
`.column_path("data", "abundance", &["abundance"])`. Using `path` on a
non-`json`/`jsonb` column is a validation error.

## Architecture

```
[GraphQL string] ─→ parse ─┐
                           ├─→ IR (Operation) ─→ SQL + bind specs ─→ PostgreSQL ─→ serde_json::Value
[Rust builder]  ──────────ᐤ                            ↑
                                          variables + principal bind here
```

One SQL per request. All user values go through parameterized binds — there is no string interpolation of values. See `docs/superpowers/specs/2026-04-17-rust-hasura-orm-design.md` for the full design, and `docs/superpowers/specs/2026-08-03-compile-execute-split-design.md` for the compile/execute split and parse cache.

Two things follow from where the arrows join:

- **Parsing depends only on the query text**, so it is cached across requests
  (`ParseCache`, 256 documents by default, keyed on the full source string).
  It is ~70% of the per-request CPU: 18.9 µs → 4.0 µs on a moderately complex
  query. `Engine::with_parse_cache_capacity(pool, schema, 0)` turns it off.
- **Variables bind at the end, not during lowering**, so a query can be
  compiled once and run many times — see below.

## Compile once, execute many

`Engine::compile` does everything that does not depend on the request —
lowering, schema resolution, scope rewriting, SQL generation — and hands back a
`CompiledQuery`. `Engine::execute` supplies the variables and runs it.

```rust
# use vision_graphql::{Engine, ScopePolicy, predicate::{col, principal, Principal}};
# async fn example(engine: Engine, schema: vision_graphql::Schema) -> vision_graphql::error::Result<()> {
// once, at startup
let q = engine.compile(
    "query($ids: [Int!], $n: Int!) { users(where: {id: {_in: $ids}}, limit: $n) { id name } }",
)?;
println!("{}", q.sql());          // stable — EXPLAIN it, log it, diff it in review
assert_eq!(q.variables(), ["ids", "n"]);

// per request — 43 ns of CPU before the query hits PostgreSQL
let data = engine.execute(&q, Some(serde_json::json!({"ids": [1, 2], "n": 10}))).await?;

// with a scope policy: one statement serves every principal
let policy = ScopePolicy::builder()
    .allow("orders", col("user_id").eq(principal()))
    .validate(&schema)?;
let scoped = engine.compile_scoped("{ orders { id } }", &policy)?;
let mine = engine
    .execute_scoped(&scoped, None, &Principal::new().set("principal", 7))
    .await?;
# let _ = (data, mine); Ok(()) }
```

The policy is applied *symbolically*: the compiled SQL carries the predicates,
but which rows they admit is decided per request. A statement compiled with a
policy refuses to run without a principal, and one compiled without a policy
refuses to run with one — a principal that silently restricted nothing would be
worse than an error.

The point is not only the 18.9 µs saved. Compiling moves work that used to fail
at request time to startup: unknown columns, literal type errors, and tables
outside the policy all surface at `compile`, and the SQL becomes a stable
artifact you can review, `EXPLAIN`, or hold in an allowlist of persisted
queries.

**What cannot be compiled.** A variable that decides the *shape* of the SQL
rather than a value in it returns `Error::NotCompilable` naming the position:
`where: $w`, `order_by: $o`, `distinct_on: $d`, `_is_null: $b`, and any
variable inside an `insert` argument (a VALUES list's row count and column set
come from the argument itself, so `objects: $rows` could never compile, and
this first cut does not thread variables into written-out rows either).
Everything else compiles: comparison values, `_in` lists including `_in: $ids`,
`limit`/`offset`, `_by_pk` arguments, `update`'s `where` and `_set` values,
`delete`'s `where`. Uncompilable queries still run through `Engine::query`,
which is unchanged.

A `CompiledQuery` runs on the pool, not inside `Engine::transaction` —
mutations needing a transaction still go through `TxClient`.

## Column types

Most of the mapping is unremarkable — `integer` is `Int`, `text` is `String`.
Three things are worth knowing.

**`numeric` takes a number or a string.** It is carried to the server as text
and cast there, but that is a reason to accept a string, not to refuse a number:
a column that reads back as `12.34` and then rejects `_gt: 10` makes every
caller round-trip through strings for nothing.

Use the string form when the value needs more precision than a double holds.
A JSON number has already been parsed into an `f64` before this crate sees it,
so `12345678901234567890.12` arrives rounded and there is no way to recover what
was written; `"12345678901234567890.12"` is carried through exactly.

**A type with no mapping means the column is not there.** Arrays, `bytea`,
`interval` and `inet` have none, and introspection leaves those columns out of
the schema entirely — a query naming one is told it does not exist, and if the
dropped column belongs to a key or a foreign key, a `_by_pk` field or a whole
relation goes missing with it. That used to be a log line and nothing else;
`vision-gql diff` now reports them:

```
not exposed (no type mapping — these columns are absent from the schema):
  - items.tags (ARRAY (_text))
  - items.blob (bytea)
    2 column(s) across 2 type(s): ARRAY (_text), bytea
```

**A Postgres enum column** is exposed as a custom scalar named after the type
and bound as a string; its casts are schema-qualified.

## Building the schema

Three layers that merge (later wins):

1. **Introspection** — `Schema::introspect(&pool).await?` queries `information_schema` and auto-derives relations from foreign keys. It reads `public`; use `Schema::introspect_schemas(&pool, &["app", "audit"]).await?` for more (see below).
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

## Multiple schemas

`Schema::introspect_schemas` reads several Postgres schemas into one GraphQL
schema. Foreign keys that cross between them become ordinary relations — they
render as the same correlated subquery a same-schema relation does, so a
cross-schema read is still one SQL statement.

```rust
let schema = Schema::introspect_schemas(&pool, &["app", "audit"]).await?.build();
```

**The first schema listed owns the bare table names; every later schema is
exposed prefixed.** Given `app.orders` and `audit.orders`, that call exposes
`orders` and `audit_orders`.

The order is fixed by the call rather than inferred from what collides, so
creating a table in a later schema can never rename one that queries already
depend on. Introspecting `["public"]` alone — which is what `Schema::introspect`
does — is unaffected: nothing is prefixed.

```graphql
query {
  orders(where: {audit_actor: {name: {_eq: "bob"}}}) {
    total
    audit_actor { name }     # app.orders -> audit.actors
  }
}
```

Rename anything you don't like with `expose_as`. A foreign key pointing into a
schema you did not list is skipped, since there is no exposed table for the
relation to target.

To read a table from a different physical schema than the one it was
introspected from, use `schema` in the overlay. Only the schema qualifier moves —
columns still come from what introspection read, so the target must have the same
shape:

```toml
[tables.orders]
schema = "archive"     # renders "archive"."orders"
```

### Cross-*database* reads

A single SQL statement cannot span Postgres databases, so this is as far as the
engine goes on its own. The way to reach another database today is
`postgres_fdw`: create foreign tables in a schema (say `remote`), then include it
in `introspect_schemas`. They introspect like any other relation, but carry no
constraints, so declare a `primary_key` in the overlay if you want `_by_pk` on
them. Note that a foreign table on the inner side of a correlated subquery
pushes down poorly, and that a transaction spanning the FDW boundary is not
atomic.

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

`diff` also checks `schema` repoints: the target must exist and must carry the
columns the exposed table declares, since a repoint moves only the schema
qualifier. If the target schema was not introspected, `diff` reports the repoint
as *not checked* rather than passing it silently — add it to `--schema` to have
it verified.

Filter what gets processed with comma-separated globs:

```bash
vision-gql generate --url $DATABASE_URL --ignore-tables 'audit_*,_temp_*'
```

`--schema` selects which Postgres schemas to read (default `public`), with the
same "first one owns the bare names" rule as `Schema::introspect_schemas`. Stanza
keys in the generated file are the exposed names, so they line up with what the
overlay resolves against:

```bash
vision-gql generate --url $DATABASE_URL --schema app,audit
```

Both subcommands accept `$DATABASE_URL` as the default for `--url`. TLS is
supported via rustls (`sslmode=require` in the URL).

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

### Columns

Row rules decide *which* records a caller sees; column rules decide *what* of
each record. They are independent — a table can be `unrestricted` for rows and
still withhold a column — and they are per request, so one schema serves every
role:

```rust
# use vision_graphql::{ScopePolicy, Schema};
# use vision_graphql::predicate::{col, principal};
# fn example(schema: &Schema) -> Result<(), vision_graphql::Error> {
let policy = ScopePolicy::builder()
    .allow("staff", col("org").eq(principal()))
    .columns("staff", ["id", "org", "name"])   // and nothing else
    .validate(schema)?;                        // a typo here fails now, not later
# let _ = policy; Ok(()) }
```

`columns` is an allowlist and `hide_columns` its complement. Prefer the
allowlist: the difference is what a migration does to it. A column added
tomorrow is invisible under `columns` until someone names it, and visible to
every caller the moment it exists under `hide_columns` — which is exactly the
case a denylist gets wrong. This is also what distinguishes it from the
overlay's `hide_columns`, which is global and fixed when the schema is built.

A withheld column is refused, not omitted: a response missing a field the
document asked for is a wrong answer wearing the shape of a right one. And the
refusal covers every position that reads the column, not only the selection —
`where`, `order_by`, `distinct_on`, aggregate functions, `_by_pk` arguments,
`_set`, inserted columns at every nested level, and `on_conflict`'s
`update_columns`. Sorting by a column you may not read still tells you its
values, and filtering on it answers the question outright.

Scope predicates are trusted policy: they are injected as-is and never
re-scoped themselves. `scoped.transaction(…)` hands the closure a
`ScopedTxClient`, so the scope cannot be escaped mid-transaction.

### Building scope: `ScopePolicy`

Hand-building a `ScopeSet` from raw `BoolExpr` every request is verbose and the
shape (tables, columns, relation chains) is usually static — only the principal
varies. `ScopePolicy` captures that shape once, validates it against the schema,
and binds a principal per request:

```rust
# use vision_graphql::{Engine, ScopePolicy, Schema};
# use vision_graphql::predicate::{col, rel, principal};
# async fn example(engine: Engine, schema: &Schema, user_id: i64) -> Result<(), vision_graphql::Error> {
// once, at startup — `validate` catches typos in table/column/relation names:
let policy = ScopePolicy::builder()
    .allow("orders", col("user_id").eq(principal()))
    .allow("samples", rel("order", col("user_id").eq(principal())))  // ownership chain
    .unrestricted("adverts")
    .validate(schema)?;

// per request — a cheap tree-walk, no parsing or schema lookups:
let scoped = engine.scoped(policy.bind_value(user_id)?);
let _ = scoped.query("query { orders { id title } }", None).await?;
# Ok(()) }
```

The predicate DSL (`col`, `rel`, `and`, `or`, `not`, `param`, `principal`) builds
templates whose value slots are filled at bind time. `principal()` is the default
parameter; multi-key scopes use named params:

```rust
# use vision_graphql::{ScopePolicy, Principal};
# use vision_graphql::predicate::{col, param};
# fn example(policy: &ScopePolicy, tenant: i64, user: i64) -> Result<(), vision_graphql::Error> {
// policy: .allow("audit_log", and([col("tenant_id").eq(param("tenant_id")),
//                                   col("actor_id").eq(param("user_id"))]))
let scope = policy.bind(&Principal::new().set("tenant_id", tenant).set("user_id", user))?;
# let _ = scope; Ok(()) }
```

The same policy can be loaded from TOML (`ScopePolicy::from_toml`), where `where`
uses the query `where` object syntax and `"$name"` marks a parameter (`$$`
escapes a literal `$`):

```toml
[tables.orders]
where = { user_id = { _eq = "$principal" } }

[tables.samples]
where = { order = { user_id = { _eq = "$principal" } } }   # relation chain

[tables.adverts]
unrestricted = true
```

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

## `__typename`

Supported in every selection set: rows, nested relations, `_by_pk`, aggregate
`nodes`, the `aggregate` object and each function group inside it, mutation
`returning`, and the `{ affected_rows, returning }` wrapper itself. That breadth
is the point — Apollo and urql inject `__typename` into *every* selection set by
default, so anything less means a client that works until it touches the one
position that was missed.

```graphql
{ users { __typename id posts { __typename title } } }
```
```json
{ "users": [ { "__typename": "users", "id": 1,
               "posts": [ { "__typename": "posts", "title": "hello" } ] } ] }
```

Type names follow Hasura's scheme, so tooling generated against a Hasura
endpoint reads them unchanged: a row is the exposed table name, and the derived
types are `<table>_aggregate`, `<table>_aggregate_fields`, `<table>_sum_fields`
(and `avg` / `max` / `min`), and `<table>_mutation_response`. They render as SQL
literals — no bind, no round trip.

`__typename` is not accepted as a *root* field (`{ __typename }`), which would
name the operation root type; put it inside a field's selection set.

## Schema introspection and SDL

The GraphQL type system this engine exposes is derived from the schema —
Hasura's shape, so a client or codegen setup written against a Hasura endpoint
reads it unchanged:

```
users                         users_bool_exp        Int_comparison_exp
users_aggregate               users_order_by        order_by
users_aggregate_fields        users_insert_input    users_select_column
users_sum_fields (avg/max/min) users_set_input      users_constraint
users_mutation_response       users_on_conflict     users_pk_columns_input
```

Everything published is something the engine implements. `String_comparison_exp`
carries the operators the lowering actually lowers and not one more — no
`_regex`, no `_similar` — because a client generates code against what it is
told. A read-only table gets read types and no mutation fields. `on_conflict`
appears only where a unique constraint exists to name in it. `path` appears only
on `json`/`jsonb` columns. The directive list is empty, and a document carrying
`@include`/`@skip` is rejected rather than having it silently not happen.

### `__schema` / `__type` — off by default

```rust
# use vision_graphql::Schema;
# async fn f(pool: sqlx::PgPool) -> vision_graphql::error::Result<()> {
let schema = Schema::introspect(&pool).await?.enable_introspection().build();
# let _ = schema; Ok(()) }
```

Introspection hands the caller the whole data model — every table, column, type
and relation. That is a wider disclosure than answering data queries, so
upgrading does not turn it on. Enable it where the endpoint is internal or the
model is public anyway; leave it off where clients ship pre-generated documents.

It is answered from the schema in memory, and the JSON rides into the statement
as a bound parameter — so a document mixing introspection with data is still one
request, and a compiled introspection query works like any other.

A Postgres enum column is published as a custom scalar named after the type
rather than a GraphQL enum: introspection reads the type's name but not its
variants.

### SDL export

```bash
vision-gql sdl --url $DATABASE_URL --config schema.toml -o schema.graphql --force
vision-gql sdl --url $DATABASE_URL --config schema.toml -o schema.graphql --check
```

The exposed surface is otherwise implicit — what introspection found, minus
`hide_columns`, renamed by `expose_as`, plus what the overlay declared.
Committing the SDL turns "did that migration expose a new column" into a line in
a diff. `--check` is the CI half: exit 0 when the file matches the database, 1
when it does not, with a summary that leads with the types that appeared or went
away. Output is byte-stable for a given schema.

Unlike runtime introspection, this needs no flag: it is a build-time artifact,
not something a request can ask for. In code it is
`vision_graphql::sdl::render(schema.type_system())`.

## Aggregates

```graphql
query {
  users_aggregate(where: {active: {_eq: true}}) {
    aggregate {
      total: count                                  # count(*)
      cities: count(columns: [city], distinct: true) # count(DISTINCT city)
      oldest: max { born: birth_date }
      avg { age }
    }
    nodes { id name }
  }
}
```

An array relation carries the same field, over that row's children — which is
what a paginated list needs, since the page and its total are one request:

```graphql
{ authors {
    posts(limit: 10, order_by: [{score: desc}]) { title }
    posts_aggregate { aggregate { count } }
    published: posts_aggregate(where: {draft: {_eq: false}}) { aggregate { count } }
} }
```

It renders as a correlated subquery like any relation field, takes the same
arguments (except `distinct_on`, which the aggregate source does not render and
so does not accept), and answers `count: 0` for a parent with no children rather than
going missing. Object relations do not have one: a single row has nothing to
aggregate, and asking says so.

Counting a table is reading it, so a scope applies: the target's row predicate
lands in the aggregate's `WHERE`, a table the scope denies is denied here too,
and a withheld column cannot be summed or maxed. A number that answered the
question the rows were refused would be a hole with extra steps.

`count` takes `columns` and `distinct`; the other functions take their columns
as a selection set. Field aliases work here like anywhere else — `total: count`
answers under `total`. Anything else in an argument position is an error rather
than something quietly dropped: `count(distinct: true)` with no `columns` says
so, and a misspelled argument names itself.

## Strictness

Three rules worth knowing before pointing a client at this, all of which used to
be silent:

**A comparison against `null` is an error.** SQL's answer to `col = NULL` is no
rows — not "the rows whose column is null", which is what someone writing
`_eq: null` means. Returning an empty result would be the shape of a right
answer to a question nobody asked, so it says so instead and names `_is_null`,
which does mean it. This holds for a variable too: `_eq: $x` with `$x` null is
the same question asked one request later, and a compiled statement carries the
refusal to where the value arrives. A null stays a value where it is one —
`_set: {col: null}` and an inserted `null` are untouched.


**Unknown arguments are rejected, everywhere.** Including on `_by_pk` roots,
which read the arguments they want by name and used to leave the rest alone —
`users_by_pk(id: 1, where: {…})` now says the `where` does not belong instead of
returning the row and discarding the filter.

**Two fields cannot answer to one response key unless they ask the same
question.** Identical scalar reads collapse, which is what makes spreading a
fragment that repeats a column work. Relations merge when neither carries
arguments. Anything else — `posts` beside `posts(limit: 1)`, two root fields
both called `users` — is an error naming the key, because only one of them can
survive into the response object and the other used to vanish without a word.

## Request limits

Every document is checked against [`ParseLimits`] *before* it is parsed —
a single pass over the raw bytes, bounding total length and nesting depth.

This one guard cannot live anywhere else. Nesting an input value deeply enough
overflows the stack inside the parser, and a stack overflow in Rust aborts the
process: it is not a panic, so no `catch_unwind` at the request boundary
contains it. A ~16 KiB document takes the server down along with every request
in flight:

```graphql
{ users(where: {_not: {_not: … × 2000 … }}) { id } }
```

2000 is the depth that does it on a 2 MiB stack, which is what a tokio worker
thread gets by default; an 8 MiB main thread only moves the cliff to ~8000.
Selection-set nesting is already bounded by the parser's own recursion limit —
it is input values (`where`, `_set`, `objects`) that had no guard, which is why
the depth counted here spans `{`, `[` and `(` alike.

Defaults are 64 levels and 128 KiB, far above any hand-written or generated
query. Brackets inside string literals and `#` comments do not count. A rejected
document returns `Error::Limit`, kept separate from `Error::Parse` so an
endpoint can answer "too large" differently from "invalid syntax".

```rust
# use std::sync::Arc;
# use vision_graphql::{Engine, ParseCache, ParseLimits, Schema};
# fn example(pool: sqlx::PgPool, schema: Schema) {
let cache = ParseCache::with_limits(256, ParseLimits { max_depth: 32, max_bytes: 32 * 1024 });
let engine = Engine::with_parse_cache(pool, schema, Arc::new(cache));
# let _ = engine; }
```

`Engine::with_parse_cache` also lets several engines share one cache. Parsing
is schema-independent, so an application running an engine per role — the way
per-role column visibility is expressed today — would otherwise parse the same
document once per role.

These limits bound the *document*. What the document then costs is bounded
separately, below.

## Execution limits

`ParseLimits` bounds the text; `ExecutionLimits` bounds the statement it turns
into. Neither implies the other: a flat document nesting nothing still renders
one correlated subquery per aliased relation field, and `{ users { id } }` is
four words that reads a whole table, builds the entire result as one JSON value
in Postgres, and hands it over in one piece.

```rust
# use vision_graphql::{Engine, Schema, limits::ExecutionLimits};
# fn example(pool: sqlx::PgPool, schema: Schema) {
let engine = Engine::new(pool, schema).with_limits(
    ExecutionLimits::new()
        .max_relation_depth(6)   // { users { posts { comments { … } } } }
        .max_table_reads(40)     // every subquery, EXISTS filter and order_by hop
        .default_limit(100)      // for a row list that asked for none
        .max_limit(1000),        // ceiling on one that did
);
# let _ = engine; }
```

Everything is unset by default. A library that silently capped results would be
worse than one that did not, since the caller cannot tell a capped answer from a
complete one — so the defaults do nothing, and you set what applies where
requests come from clients.

The checks run on the lowered IR, which is the one thing both entry points
share: the typed builder never goes near the parser, so a check living there
would leave `Engine::run` unbounded. They apply to compiled statements and
scoped handles alike, and a `limit: $n` carries its ceiling to where the
variable resolves — so a statement compiled once keeps the cap it was compiled
under.

`max_table_reads` counts every position that reads a table: each root field,
each relation at any depth, each `EXISTS` filter inside a `where`, each
`order_by` hop, each nested insert. That is the number of subqueries the
statement will carry, and the thing a hundred aliases of one relation inflates
while leaving depth at 1.

`default_limit` reaches root lists, array relations, and the `nodes` of an
`_aggregate` — those rows are rows like any other. Not `_by_pk`, and not object
relations (one row by construction; a limit there would replace the `LIMIT 1`
the renderer needs).

On an aggregate it reaches `nodes` **and nothing else**: `aggregate` and `nodes`
otherwise read one source, so a cap on it would decide what `count` counted
rather than how many rows came back. `nodes` gets a source of its own when a
default applies. A limit the caller writes still applies to both — they asked
about that many rows. It is the one limit here that silently changes an answer, which
is the trade it exists to make; a client that needs to know whether more rows
exist should ask `_aggregate { count }` as its own root field.

`max_limit` reaches every position that renders a `LIMIT`, `_aggregate`
included — a ceiling one suffix could walk around would not be one.

It refuses rather than clamps. A truncated answer that looks complete is the
failure worth avoiding.

### Prepared statements and page size

A literal `limit` renders inline, which keeps the statement readable and
`EXPLAIN`-able — the point of compiling. When the number comes from a client
that is the wrong trade: `limit: 1`, `limit: 2`, `limit: 3` are three
statements, and a driver caches prepared statements per connection keyed on
their text (sqlx keeps 100), so a client paging through results evicts
everything else and leaves prepared statements accumulating server-side.
`.bind_row_counts(true)` renders `limit` and `offset` as binds instead: one
statement whatever the page size, at the cost of the number no longer showing
in `CompiledQuery::sql()`.

### Statement timeout

Set it on the connection, not per request. The engine sends one statement per
request, so a `statement_timeout` established at connection time governs every
query it makes, with no extra round trip and nothing for the engine to
implement:

```rust
# use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
# async fn example(url: &str) -> Result<(), sqlx::Error> {
let opts: PgConnectOptions = url.parse()?;
let pool = PgPoolOptions::new()
    .connect_with(opts.options([("statement_timeout", "5s")]))
    .await?;
# let _ = pool; Ok(()) }
```

## Persisted queries

The posture to prefer where clients can ship their documents: compile a set at
startup, run them by key, never parse a new document at request time.

```rust
# use vision_graphql::{Engine, QueryRegistry};
# async fn f(engine: Engine) -> vision_graphql::error::Result<()> {
// once, at startup — a failure names the key that failed
let registry = QueryRegistry::compile_all(&engine, [
    ("user-list",  "query($n: Int!) { users(limit: $n) { id name } }"),
    ("user-by-id", "query($id: Int!) { users_by_pk(id: $id) { id name } }"),
])?;

// per request
let data = engine
    .execute(registry.require("user-list")?, Some(serde_json::json!({"n": 20})))
    .await?;
# let _ = data; Ok(()) }
```

Most of what the sections above defend against stops being reachable: no new
document is parsed, so document size and nesting are moot, and the cost of each
query was fixed when it compiled. It also moves failure to startup — an unknown
column, a table outside the scope policy, a literal of the wrong type all
surface when the registry is built rather than on the request that happens to
hit that query.

`compile_all_scoped` compiles the set against a `ScopePolicy`; one statement
then serves every principal, with `execute_scoped` supplying who is asking.

The key is whatever suits the application — a name, a file path, the SHA-256 of
the document if you are implementing the persisted-query protocol clients speak.
The crate does not choose one. An unknown key is an error that names the key and
deliberately does not list the ones that do exist.

## Errors

`Error` is a Rust enum, and stays one — a library that could only hand back JSON
would be worse to program against. `to_graphql_response` produces the wire form
when you need it:

```rust
# use vision_graphql::{Engine, Error};
# async fn f(engine: Engine, source: &str) -> serde_json::Value {
match engine.query(source, None).await {
    Ok(data) => serde_json::json!({ "data": data }),
    Err(e) => {
        tracing::warn!(error = %e, "request failed");   // the whole of it
        e.to_graphql_response()                          // what the client sees
    }
}
# }
```

```json
{"errors": [{
  "message": "validation error at where.id: type mapping: expected Int4",
  "extensions": {"code": "VALIDATION_FAILED", "path": "where.id"}
}]}
```

**One error, and no `data` key.** Both are consequences of the architecture
rather than simplifications: the engine renders one statement per request and
runs it whole, so there is no partial success to report alongside, and the first
thing that goes wrong is the only thing that happens.

**`extensions.code`** is the stable classification — `VALIDATION_FAILED`,
`VARIABLE_MISSING`, `SCOPE_DENIED`, `DOCUMENT_REJECTED` (the document was too
large or too deep to look at), `LIMIT_EXCEEDED` (an ordinary document asking for
too much), `PARSE_FAILED`, `NOT_COMPILABLE`, `DATABASE_ERROR`, `INTERNAL_ERROR`
— and what an HTTP layer maps to a status. The string is the contract, not the
enum variant.

`SCOPE_DENIED` means the caller's access, and only that. A policy that would not
load, or a compiled statement run through the wrong entry point, is the host's
own mistake and comes back as `INTERNAL_ERROR`: telling a client it lacks
permission for a bug it had no part in would send it looking in the wrong place.

**What the message says depends on who caused it.** A validation error goes
back whole: it names a column the document already named, and withholding it
would only make the client guess. A *database* error does not. PostgreSQL's
message text carries table names, constraint names and sometimes a source file
and line from inside the server; the reply carries the SQLSTATE
(`23505`, `23503`, `57014`) and the full text stays in `Display` for your log.
An internal error says only that it is one.

There is no `path` in the GraphQL sense: `path` names a position in the
*response*, and every error here is raised before any data exists. The position
this crate does know — `where.id`, `m0.objects[0].price` — travels in
`extensions.path` instead.

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

MIT ([LICENSE-MIT](LICENSE-MIT)) or Apache-2.0 ([LICENSE-APACHE](LICENSE-APACHE)),
at your option.
