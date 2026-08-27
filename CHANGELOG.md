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
- **A fragment cycle or a long fragment chain aborted the process.**
  `fragment F on users { id ...F }` recursed until the stack ran out, and so did
  a chain of a few hundred fragments — both invisible to `ParseLimits`, which
  counts brackets in the raw text and sees one level in either case. The
  fragment graph is now checked for cycles and chain depth before anything walks
  a selection set.
- **A `__typename`-only aggregate rendered SQL that fails at runtime.**
  `{ users_aggregate { __typename } }` selected a literal from an unaggregated
  source inside a scalar subquery: Postgres errors at two rows and answers null
  at zero. When nothing in the projection reads a row, the statement no longer
  reads any.
- **`<table>_pk_columns_input` could be published with no fields**, which is not
  a legal GraphQL type, so an SDL file carrying it would not load and
  `sdl --check` would keep certifying it. Reached by hiding a primary key
  column. Same for a table whose every column is hidden, which is now skipped
  entirely, along with any relation pointing at it.
- **Directives were read by nobody.** `field @include(if: false)` came back
  included. No directive is implemented, so a document carrying one is now
  rejected — which is also what makes the empty directive list in the
  introspection answer honest. Checked in every position the grammar allows
  one: the operation, its variable definitions, fragment definitions, and every
  selection.
- **A repeated `__typename` under one response key was written twice** into the
  same `json_build_object`: the dedup used `Vec::dedup`, which only collapses
  adjacent entries. Introspection had the mirror problem — a repeated key kept
  the first answer and silently dropped a differing one, where the data path
  errors.
- **Column and relation order was whatever the hash map felt like.** Anything
  derived from the whole schema differed run to run for reasons unrelated to the
  schema; columns now keep the order introspection read them in
  (`ordinal_position`), relations the order they were added, and
  `Schema::tables` comes out sorted.
- **Variable defaults declared by an operation were ignored.**
  `query($n: Int = 10)` with `$n` unsupplied failed with "not bound" instead of
  using 10. Applied on both paths: while lowering for `Engine::query`, and at
  execute time for `Engine::compile`, which is lowered before any request
  exists. An explicitly supplied value wins, including an explicit `null`.

### Added

- **`__typename`**, in every selection set: rows, relations, `_by_pk`, aggregate
  `nodes`, the `aggregate` object and each function group inside it, mutation
  `returning`, and the mutation-response wrapper. Apollo and urql add it to
  every selection set by default, so it previously failed with "unknown column
  '__typename'" against any of them. Type names follow Hasura's scheme
  (`users`, `users_aggregate_fields`, `users_mutation_response`, …), exposed as
  `type_names`. Not accepted as a root field; `__schema` / `__type`
  introspection is still unimplemented.
- **Schema introspection**: `__schema` and `__type`, answered in memory from a
  type system derived from the schema — the full Hasura-shaped surface, input
  types included (`<t>_bool_exp`, `<t>_order_by`, `<t>_insert_input`,
  `<t>_set_input`, `<t>_on_conflict`, `<t>_constraint`, `<t>_select_column`,
  `<scalar>_comparison_exp`, `order_by`), so GraphiQL's argument completion and
  `graphql-codegen`'s variable types work rather than merely connecting.
  **Off unless `SchemaBuilder::enable_introspection` is called**: it publishes
  the whole data model, and upgrading should not widen what a deployment
  exposes. Only what the engine implements is published — the comparison inputs
  carry exactly the operators the lowering accepts, read-only tables get no
  mutation fields, `on_conflict` appears only where a unique constraint exists.
  The answer travels as a bound parameter, so a document mixing introspection
  with data is still one statement.
- **SDL export**: `vision-gql sdl` writes the schema a database plus overlay
  exposes, with `--check` for CI (exit 1 on drift, summarised by type). Needs no
  runtime flag — it is a build-time artifact. In code:
  `vision_graphql::sdl::render(schema.type_system())`.
- `type_system::TypeSystem`, `sdl`, `introspection` modules; `Schema::type_system`,
  `Schema::introspection_enabled`, `SchemaBuilder::enable_introspection`,
  `SchemaBuilder::retain_tables`.
- `Table::unique_constraints`, `Table::unique_constraint` — introspection has
  always read these and merge threw them away; they are what `<t>_constraint` is
  built from.
- `Schema::tables`, `Schema::len`, `Schema::is_empty`, `Table::columns`,
  `Table::relations` — the schema an overlay actually produced can now be walked
  from outside the crate, which is what any SDL export, admin tooling, or test
  that asserts on exposure needs.
- `limits::ParseLimits` (`max_depth`, `max_bytes`), `ParseLimits::unbounded()`,
  and `parser::parse_document_with`.
- `ParseCache::with_limits`, `ParseCache::limits`.
- `Engine::with_parse_cache`, to set limits other than the defaults and to share
  one cache across engines — parsing is schema-independent, so an application
  running an engine per role no longer parses each document once per role.
- `AggregateBuilder::count_columns`, `count_distinct`, and `key` (response key
  for the aggregate added last).
- `CompiledQuery::defaults`.
- `Count::var`; `Count::Var` is now a struct variant carrying an optional `max`.
- `QueryArgs::is_empty`, `AggOp::count()`, `AggCol::new`, `AggField::column`.

### Breaking

- `Error::Limit` is a new variant.
- The aggregate IR changed shape: `RootBody::Aggregate.ops` is
  `Vec<AggSelect>` (a response key plus an op) rather than `Vec<AggOp>`;
  `AggOp::Count` is a struct variant carrying `columns` and `distinct`;
  `AggOp::Sum` / `Avg` / `Max` / `Min` carry `fields: Vec<AggField>` rather than
  `columns: Vec<String>`; `AggOp::Typename` is new. Builder method signatures
  are unchanged.
- `Field::Typename` is a new IR variant, and `RootBody::Aggregate` and the
  `Insert` / `Update` / `Delete` mutation fields carry the response keys asking
  for a type name.
- An `_aggregate` field selecting only `nodes` no longer returns an empty
  `aggregate` key the document did not ask for.
- Queries that previously ran now fail: unknown arguments on `_by_pk` and on
  aggregate functions, conflicting fields under one response key, and any
  document carrying a directive. Each was producing a wrong answer or a
  truncated response before.

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
