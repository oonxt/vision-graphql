# Changelog

Notable changes per release. Versions before 0.13.0 are reconstructed from the
release commits; entries from 0.13.0 on are written as the work lands.

## Unreleased

Hardening for the case where a document reaches the engine from a client rather
than from the application's own source. Several of these change behaviour that
used to be silent, so read **Breaking** before upgrading.

### Fixed

- **A deeply nested input value crashed the process.** `{_not: {_not: … }}` at
  ~2000 levels — about 16 KiB of text — overflowed the stack inside the parser.
  A stack overflow aborts in Rust rather than unwinding, so no `catch_unwind` at
  the request boundary contained it: one request took the server down along with
  everything in flight. Documents are now checked against `ParseLimits` before
  parsing (default 64 levels, 128 KiB), at `parser::parse_document`, which every
  path funnels through. Selection-set nesting was already bounded by the
  parser's own recursion limit; input values were not.
- **`count` ignored its arguments.** `count(columns: [name], distinct: true)`
  rendered `count(*)` and returned a number answering a different question, with
  no error. `columns` and `distinct` are now implemented, and unknown arguments
  are rejected.
- **Aggregate fields ignored their aliases.** `total: count` answered under
  `count`, and `max { newest: id }` under `id`, handing back a response shaped
  differently from the document.
- **Two fields under one response key silently lost one of them.**
  `{ users { id } users { name } }` rendered both into one
  `json_build_object` and the second overwrote the first. Identical reads now
  collapse (so spreading a fragment that repeats a column still works),
  argument-free relations merge their selections, and anything else is an error
  naming the key.
- **`_by_pk` accepted and discarded unknown arguments.**
  `users_by_pk(id: 1, where: {…})` ran and dropped the `where`. Applies to the
  query root, `delete_*_by_pk`, and the `pk_columns` object of
  `update_*_by_pk`.
- **Variable defaults declared by an operation were ignored.**
  `query($n: Int = 10)` with `$n` unsupplied failed with "not bound" instead of
  using 10. Applied on both paths: while lowering for `Engine::query`, and at
  execute time for `Engine::compile`, which is lowered before any request
  exists. An explicitly supplied value wins, including an explicit `null`.

### Added

- `limits::ParseLimits` (`max_depth`, `max_bytes`), `ParseLimits::unbounded()`,
  and `parser::parse_document_with`.
- `ParseCache::with_limits`, `ParseCache::limits`.
- `Engine::with_parse_cache`, to set limits other than the defaults and to share
  one cache across engines — parsing is schema-independent, so an application
  running an engine per role no longer parses each document once per role.
- `AggregateBuilder::count_columns`, `count_distinct`, and `key` (response key
  for the aggregate added last).
- `CompiledQuery::defaults`.
- `QueryArgs::is_empty`, `AggOp::count()`, `AggCol::new`.

### Breaking

- `Error::Limit` is a new variant.
- `RootBody::Aggregate.ops` is now `Vec<AggSelect>` (alias plus op) rather than
  `Vec<AggOp>`; `AggOp::Count` is a struct variant carrying `columns` and
  `distinct`; `AggOp::Sum` / `Avg` / `Max` / `Min` carry `Vec<AggCol>` rather
  than `Vec<String>`. Builder method signatures are unchanged.
- Queries that previously ran now fail: unknown arguments on `_by_pk` and on
  aggregate functions, and conflicting fields under one response key. Each was
  producing a wrong answer or a truncated response before.

## 0.12.0

- Compile once, execute many: `Engine::compile` / `execute` render the SQL ahead
  of a request and bind variables per request.
- Parsed documents cached across requests (`ParseCache`).

## 0.11.1 (vision-graphql-cli)

- `diff` validates `schema` repoints.
- `write_header` uses `HEADER_PREFIX` rather than a copy of it.

## 0.11.0

- Multiple Postgres schemas in one `Schema` (`Schema::introspect_schemas`),
  including cross-schema foreign-key relations.
- CLI: `PgType::Json` handled in the type-name renderer.

## 0.10.0

- JSON/JSONB path reads: `data(path: "a.b")` → `#>`, structure preserved.
- `json` type support.

## 0.9.0

- Materialized views.
- Logical primary keys for views.
- `order_by` NULL placement: `asc_nulls_last`, `desc_nulls_first`, and friends.

## 0.8.0

- Views are read-only by default; mutations must not write through them.

## 0.7.0

- `order_by` through object relations.
- An auto-derived foreign-key relation can no longer shadow a same-named column.

## 0.6.0

- Scoped `update`: post-update check plus upsert pre-image filter.
- `ScopePolicy`: predicate DSL, template/bind, TOML config.
- Upsert `DO UPDATE … WHERE` columns qualified with the target table.

## 0.5.0

- Scoped mutations: `update`, `delete`, `insert`.
