# Vision-GraphQL: Hasura-style ORM in Rust

**Status:** Draft · **Date:** 2026-04-17

## Purpose

A Rust library that accepts Hasura-style Query/Mutation input and returns JSON,
targeting PostgreSQL only. The caller embeds it as a crate; the library handles
schema introspection, query parsing, SQL generation, execution, and JSON
assembly.

## Locked Decisions

| # | Topic | Decision |
|---|---|---|
| 1 | Delivery form | Rust library (embedded) |
| 2 | Input formats | Both GraphQL string **and** Rust builder API, collapsing to one IR |
| 3 | Database | PostgreSQL only |
| 4 | Schema source | Introspection + manual augmentation |
| 5 | Features | Select, nested select, mutations (insert/update/delete/returning), aggregation, upsert |
| 6 | Output shape | Data shape matches Hasura; errors returned as `Result<Value, Error>` (no `{data,errors}` wrapping) |
| 7 | SQL strategy | Single query per request using PG `json_agg` / `row_to_json` (Hasura-style) |
| 8 | Async/DB driver | `deadpool-postgres` (wraps `tokio-postgres`) + tokio |
| 9 | Schema extension API | Rust builder + TOML config file (builder is primary, TOML optional) |
| 10 | GraphQL parser | `async-graphql-parser` |
| 11 | Type mapping | `numeric`→string, `timestamp`→ISO 8601, `bytea`→base64, uncommon types → error in MVP |
| 12 | Variables | Fully supported on both input paths; everything becomes a PG bind parameter |
| 13 | Crate structure | Single crate, modular internal layout |

## Architecture

```
 [GraphQL string]                         ┐
        │                                 │
        ▼                                 │
  parser (async-graphql-parser)           │──→ IR (Operation)
                                          │
 [Rust builder API] ─────────────────────┘
                         │
                         ▼
              schema validator
                         │
                         ▼
             sql generator (IR → SQL + binds)
                         │
                         ▼
             executor (deadpool-postgres)
                         │
                         ▼
               Result<serde_json::Value, Error>
```

Invariants:

1. All user input collapses to one IR regardless of source path.
2. All user-provided values become PG bind parameters (`$1..$N`). Never
   concatenated into SQL text.
3. PG assembles the JSON response. Rust calls `serde_json::from_str` once on
   the returned text.

Public API sketch:

```rust
let schema = Schema::introspect(&pool).await?
    .load_config("schema.toml")?
    .build()?;

let engine = Engine::new(pool, schema);

// GraphQL string path
let json = engine.query(
    r#"query($id: Int!) { users(where: {id: {_eq: $id}}) { id name posts { title } } }"#,
    Some(json!({"id": 1})),
).await?;

// Builder path
let json = engine.run(
    Query::from("users").select(&["id", "name"]).where_eq("active", true)
).await?;
```

## Crate Layout

Single crate, internal modules:

```
vision-graphql/
  src/
    ast/          # IR types
    parser/       # GraphQL → IR
    builder/      # Rust API → IR
    schema/       # introspection + builder + TOML loader
    sql/          # IR → PG SQL with binds
    executor/     # deadpool-postgres execution + JSON decoding
    types/        # PG ↔ JSON type mapping
    error.rs
    lib.rs
  tests/          # integration tests (testcontainers)
  benches/        # criterion baselines
```

## IR (Intermediate Representation)

```rust
pub enum Operation {
    Query(Vec<RootField>),
    Mutation(Vec<MutationField>),
}

pub struct RootField {
    pub table: String,       // physical table (alias resolved)
    pub alias: String,       // JSON key in output
    pub kind: RootKind,
    pub args: QueryArgs,
    pub selection: Vec<Field>,
}

pub enum RootKind { List, Aggregate, ByPk }

pub struct QueryArgs {
    pub where_: Option<BoolExpr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub distinct_on: Vec<String>,
}

pub enum Field {
    Column(String, String),                   // (physical_col, output_alias)
    Relation {
        name: String, alias: String,
        kind: RelKind,                        // Object | Array
        args: QueryArgs,
        selection: Vec<Field>,
    },
    Aggregate {
        alias: String,
        ops: Vec<AggOp>,                      // count | sum{col} | avg{col} | max{col} | min{col}
        nodes: Option<Vec<Field>>,
    },
}

pub enum BoolExpr {
    And(Vec<BoolExpr>),
    Or(Vec<BoolExpr>),
    Not(Box<BoolExpr>),
    Compare { column: String, op: CmpOp, value: Value },
    IsNull { column: String, negated: bool },
    Relation { name: String, inner: Box<BoolExpr> },   // EXISTS subquery
}

pub enum CmpOp {
    Eq, Neq, Gt, Gte, Lt, Lte,
    In, Nin,
    Like, ILike, NLike, NILike,
}

pub enum MutationField {
    Insert {
        table: String,
        objects: Vec<HashMap<String, Value>>,
        on_conflict: Option<OnConflict>,
        returning: Vec<Field>,
    },
    Update {
        table: String,
        where_: BoolExpr,
        set: HashMap<String, Value>,
        returning: Vec<Field>,
    },
    Delete {
        table: String,
        where_: BoolExpr,
        returning: Vec<Field>,
    },
}

pub struct OnConflict {
    pub constraint: String,
    pub update_columns: Vec<String>,
    pub where_: Option<BoolExpr>,
}
```

Notes:

- `Value` is `serde_json::Value`. The SQL generator turns these into PG bind
  parameters; identifiers (table, column, direction) are never parameterized
  but come from a validated schema.
- `BoolExpr::Relation` renders as `EXISTS (SELECT 1 FROM rel_tbl ... WHERE ...)`.
- `RootKind::ByPk` renders as a query with `LIMIT 1` whose outer wrapper uses
  `row_to_json` instead of `json_agg`.

## Schema Subsystem

Three-layer merge (later wins): **introspection → TOML → builder**. Output is
an immutable `Arc<Schema>`.

```rust
pub struct Schema {
    tables: HashMap<String, Table>,
    physical: HashMap<String, String>,
}

pub struct Table {
    pub physical: QualifiedName,             // "public"."users"
    pub columns: HashMap<String, Column>,
    pub primary_key: Vec<String>,
    pub relations: HashMap<String, Relation>,
    pub unique_constraints: HashMap<String, Vec<String>>,
}

pub struct Column {
    pub physical: String,
    pub pg_type: PgType,
    pub nullable: bool,
    pub exposed: bool,                        // false = hidden from query & mutation
}

pub struct Relation {
    pub kind: RelKind,                        // Object | Array
    pub target_table: String,
    pub mapping: Vec<(String, String)>,
}
```

### Introspection

Query `information_schema.tables` / `columns` / `table_constraints` /
`key_column_usage` plus `pg_attribute`. Produce:

- All tables and columns with PG types and nullability
- Primary keys
- Unique constraints (used by `on_conflict`)
- **Base relations** auto-derived from foreign keys, named by convention:
  `posts.user_id → users.id` generates:
  - `posts.user` (Object relation → users)
  - `users.posts` (Array relation → posts)
- **Ambiguity resolution**: if a table has multiple FKs pointing to the same
  target (e.g. `posts.author_id` and `posts.editor_id` both → users),
  auto-derivation is skipped for that pair — the user must declare relations
  explicitly via TOML or builder. Emit a `tracing::warn!` listing the skipped
  pairs.

### TOML Config

```toml
[[table.users]]
expose_as = "profiles"
hide_columns = ["password_hash"]

[[table.users.relation]]
name = "followers"
kind = "array"
target = "users"
mapping = [["id", "followed_id"]]
via = "follows"

[[table.users.relation]]
name = "latest_post"
kind = "object"
target = "posts"
mapping = [["id", "user_id"]]
```

### Builder

```rust
Schema::introspect(&pool).await?
    .load_config("schema.toml")?
    .hide_column("users", "password_hash")
    .expose_as("users", "profiles")
    .relation("users", "posts", Relation::has_many("posts.user_id"))
    .build()?
```

### Validation (performed in `build()`)

- Relations reference existing tables/columns
- `on_conflict` constraint names exist
- No alias cycles
- User-defined relations override auto-derived ones with the same name
  (log a `tracing::info!`)

Failure returns `Error::Schema`.

## SQL Generation

### Select strategy

Each root field becomes a nested subquery; the outer wrapper is
`json_build_object` over all roots. A whole operation is one SQL statement.

Input:

```graphql
query {
  users(where: {active: {_eq: true}}, limit: 10) {
    id
    name
    posts(order_by: {created_at: desc}, limit: 5) {
      title
    }
  }
}
```

Output (simplified):

```sql
SELECT row_to_json(root) AS result FROM (
  SELECT
    (SELECT coalesce(json_agg(row_to_json(u)), '[]'::json) FROM (
       SELECT
         u0.id AS "id",
         u0.name AS "name",
         (SELECT coalesce(json_agg(row_to_json(p)), '[]'::json) FROM (
            SELECT p0.title AS "title"
            FROM public.posts p0
            WHERE p0.user_id = u0.id
            ORDER BY p0.created_at DESC
            LIMIT 5
         ) p) AS "posts"
       FROM public.users u0
       WHERE u0.active = $1
       LIMIT 10
     ) u) AS "users"
) root;
```

### Rendering rules

| Case | SQL fragment |
|---|---|
| Array relation subselect | `(SELECT coalesce(json_agg(row_to_json(t)), '[]'::json) FROM (...) t)` |
| Object relation subselect | `(SELECT row_to_json(t) FROM (...) t LIMIT 1)` |
| Column | `col_phys AS "exposed_alias"` |
| `_eq` / `_neq` / comparisons | `col = $N`, `col <> $N`, ... |
| `_in` | `col = ANY($N)` with PG array binding |
| Relation filter | `EXISTS (SELECT 1 FROM rel_tbl r WHERE r.fk = outer.pk AND <inner>)` |
| `order_by` on relation | `LEFT JOIN` with deterministic aliasing |
| `distinct_on` | `SELECT DISTINCT ON (a, b) ...` |
| `users_by_pk(id: ...)` | `WHERE pk = ... LIMIT 1`, wrapped in `row_to_json` |

### Aggregate

```graphql
users_aggregate(where: {active: {_eq: true}}) {
  aggregate { count, avg { age } }
  nodes { id name }
}
```

```sql
SELECT json_build_object(
  'aggregate', json_build_object(
    'count', count(*),
    'avg', json_build_object('age', avg(age))
  ),
  'nodes', coalesce(json_agg(row_to_json(n)), '[]'::json)
) FROM (
  SELECT id, name, age FROM public.users WHERE active = $1
) n;
```

### Mutation

CTE-chained, one SQL per operation:

```sql
WITH
  ins_a AS (INSERT INTO t1(...) VALUES ... ON CONFLICT ... RETURNING *),
  upd_b AS (UPDATE t2 SET ... WHERE ... RETURNING *)
SELECT json_build_object(
  'insert_t1', (SELECT json_build_object(
     'affected_rows', count(*),
     'returning', coalesce(json_agg(row_to_json(ins_a)), '[]'::json)
   ) FROM ins_a),
  'update_t2', (SELECT ... FROM upd_b)
);
```

### Parameter binding

- Generator keeps a `Vec<Value>` binds buffer; each user value is pushed and
  referenced by `$N` (N starts at 1).
- Prepared statements are cached by `deadpool-postgres`.
- Identifiers (table, column, direction) are emitted from validated schema
  names using quoted identifiers; never user-supplied text.

## Parser and Builder

### GraphQL string → IR

```rust
pub fn parse_and_lower(
    source: &str,
    variables: &Value,
    operation_name: Option<&str>,
    schema: &Schema,
) -> Result<Operation, Error>;
```

Steps:

1. `async_graphql_parser::parse_query(source)` → AST
2. Pick target operation (by `operation_name`, or the only one)
3. Walk the selection set. Match field names against the schema's root
   fields (`users`, `users_aggregate`, `users_by_pk`, `insert_users`, ...)
4. Expand argument objects into `BoolExpr` / `QueryArgs` per Hasura semantics
5. Resolve variable references (`$id`) against the `variables` JSON;
   lightweight type check against the target column's `PgType`
6. Expand fragments (`FragmentSpread`, `InlineFragment`)
7. Recurse into nested `Field::Relation` / `Field::Aggregate`

Validation runs inline. Failures surface as `Error::Validate { path, message }`
with a dotted path like `users.posts.where.id._eq`.

### Builder → IR

The builder is a typed construction shell for IR. No compile-time schema
awareness — schema is resolved at runtime, validated when `Engine::run()`
lowers the builder to `Operation`.

```rust
QueryBuilder::from("users")
    .select(&["id", "name"])
    .where_eq("active", true)
    .with_relation("posts", |b| b.select(&["title"]).limit(5))
    .limit(10)
    .build()
```

Both paths funnel through the same `validate → gen_sql → execute → parse_json`
pipeline.

## Error Handling

```rust
#[derive(thiserror::Error, Debug)]
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
```

Explicit non-goals:

- No `{data, errors}` wrapping — callers decide serialization
- No partial success — one operation is one SQL, atomic
- No localization — English messages only

Logging via `tracing`:

- `DEBUG`: generated SQL text and bind count (values omitted to avoid leaking secrets)
- `ERROR`: database failures with SQL context

## Testing Strategy

### Layer 1 — Unit (fast)

- **parser**: one happy path + several error paths per GraphQL feature
  (nested, variables, fragments, aggregate, mutation)
- **builder**: assert builder and string paths produce the **same IR** for
  equivalent input
- **sql**: `insta` snapshot tests on `(sql, binds)` tuples; any SQL diff
  requires explicit snapshot acceptance

### Layer 2 — Schema introspection (medium)

- `testcontainers` spins up PG, applies fixture DDL, asserts `Schema` shape
- TOML merging and conflict resolution

### Layer 3 — End-to-end (slow)

- `testcontainers` + fixture DDL + seed data
- Hasura-semantic contract tests: one `(input, expected JSON)` pair per
  operator, relation kind, aggregate op, mutation kind
- SQL injection smoke tests: embed `'); DROP TABLE` etc. in values; assert
  execution succeeds and schema is intact

### Commands

- `cargo test --lib` — Layer 1 only (no Docker)
- `cargo test` — full suite (Docker required)
- `#[ignore]` on the slowest fuzz tests; run nightly in CI

### Benchmarks

`benches/` with `criterion`. MVP establishes one baseline for a "moderately
complex nested query".

### TDD

Follow `superpowers:test-driven-development`: failing test first, minimal
implementation next.

## MVP Scope (explicit)

### In

- Select with `where`, `order_by`, `limit`, `offset`, `distinct_on`
- Nested select via Object and Array relations
- `users_aggregate` with `count`, `sum`, `avg`, `max`, `min`
- `users_by_pk`
- `insert_users` (array form) with `on_conflict`
- `update_users` (by `where`) and `delete_users` (by `where`)
- `returning` on all mutations (optional)
- Single-row mutation variants (`insert_users_one`, `update_users_by_pk`,
  `delete_users_by_pk`) — emit single-object result instead of array
- GraphQL `variables`
- GraphQL `fragments`
- Type mappings per §Locked Decisions #11

### Out (post-MVP)

- Row-level permissions / role-based access
- Computed fields (SQL function-backed columns)
- Subscriptions
- Transactions across multiple `Engine::run()` calls
- Remote schemas / actions
- Cross-request batching / DataLoader
- Non-PostgreSQL backends
- Partial-success error model
