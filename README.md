# vision-graphql

A Hasura-style ORM for PostgreSQL in Rust. Accepts GraphQL query strings (or a typed Rust builder) and returns `serde_json::Value` in Hasura's data shape. Single SQL per request via PostgreSQL's `json_agg`/`row_to_json` — no N+1.

## Quick start

```rust
use deadpool_postgres::{Config, Runtime};
use tokio_postgres::NoTls;
use vision_graphql::{Engine, Query, Schema};

# async fn example() -> anyhow::Result<()> {
let mut cfg = Config::new();
cfg.host = Some("localhost".into());
cfg.dbname = Some("mydb".into());
let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

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
# Ok(()) }
```

## Features

| Area | Status |
|---|---|
| Select, `_by_pk`, `_aggregate` | ✓ |
| Object + Array relations | ✓ |
| `EXISTS` relation filters in `where` | ✓ |
| Mutations: `insert` / `insert_one` / `update` / `update_by_pk` / `delete` / `delete_by_pk` | ✓ |
| `on_conflict` upsert | ✓ |
| `returning` clause on mutations | ✓ |
| Operators: `_eq`/`_neq`/`_gt`/`_gte`/`_lt`/`_lte`/`_like`/`_ilike`/`_nlike`/`_nilike`/`_in`/`_nin`/`_is_null` | ✓ |
| `order_by` / `limit` / `offset` / `distinct_on` | ✓ |
| GraphQL variables, named + inline fragments | ✓ |
| Schema introspection | ✓ |
| TOML config overlay (`expose_as`, `hide_columns`, manual relations) | ✓ |
| Typed Rust builder API | ✓ |
| Row-level permissions | Not implemented |
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
2. **TOML overlay** — `.load_config("schema.toml")?` applies renames, hidden columns, and manual relations.
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

## License

MIT OR Apache-2.0
