# Changelog

Notable changes per release. Versions before 0.13.0 are reconstructed from the
release commits; entries from 0.13.0 on are written as the work lands.

## Unreleased

### Fixed

- **A builder alias could rewrite the SQL.** Response keys inside an
  aggregate's `nodes` — and column aliases in every other selection — were
  interpolated into the statement unescaped. On the document path a GraphQL
  name cannot carry a quote, but `Engine::run` takes arbitrary strings: an
  apostrophe in an alias broke the statement, and a crafted one injected SQL.
  Every alias now goes through the same escaping as every other literal and
  identifier.
- **A long fragment chain crashed the process before its limit fired.** The
  chain bound was checked after recursing, on the way back up — so ~3500
  linearly chained fragments, well under the byte limit and nesting one brace
  each, overflowed the stack first, and a stack overflow aborts the process.
  The bound is now checked on the way in.
- **Nested inserts' `on_conflict` filters escaped the execution limits.** The
  cost walk read only the top-level `on_conflict.where`; a nested insert one
  level down could chain any number of relation predicates, each rendering an
  `EXISTS` subquery, with no `CostLimit` error. The walk now descends with the
  renderer — and no longer clones every nested subtree per level to do it.
- **The builder path could silently mis-aggregate.** `distinct_on` on an
  aggregate was ignored, `count_distinct` with no columns dropped its
  `DISTINCT`, an aggregate function with no columns rendered `{}` with the
  function never applied, two ops under one key overwrote each other in the
  response, and two roots sharing an alias lost the first one — each refused by
  the parser, none by the renderer, and `Engine::run` never goes near the
  parser. All five are now refused where both entry points meet.
- **`order_by` through a relation on an `_aggregate` field sorted by the wrong
  table.** The aggregate source's hand-rolled `ORDER BY` read only the column
  name, so `posts_aggregate(order_by: {user: {name: asc}})` sorted by
  `posts.name` whenever the name existed on both tables — silently — and
  errored on a schema-published query otherwise. It also never rendered
  `NULLS FIRST|LAST`. The aggregate source now uses the same `ORDER BY`
  renderer as row lists.
- **Duplicate keys inside `_aggregate` and mutation payloads silently
  overwrote each other.** `aggregate { count } aggregate { count(columns: …) }`
  answered with only the second; `nodes { id } nodes { name }` dropped `id`;
  `returning { id } returning { name }` dropped `id`. Identical requests now
  merge, conflicting ones are refused — the same rule row selections already
  had. Aliases on `aggregate`, `nodes`, `returning` and `affected_rows` are
  refused outright rather than answered under the wrong key, since the renderer
  pins those response keys.
- **A real table named like a synthesized field was unreachable.** A table
  actually called `users_aggregate` could never be queried — the root lowering
  synthesized an aggregate over `users` first — and the type system published
  both fields under one name, which is not a legal GraphQL object. The real
  table now wins at the root, in selection sets, and in what is published.
- **Aggregate `nodes` refused the relations the schema published.**
  Introspection and SDL type `nodes` as the full row type, relations included;
  the lowering accepted columns only, so a client generated from `__schema`
  was refused a query the schema promised — and the renderer supported it all
  along. `nodes` now lowers exactly like any row selection.
- **Comparison operators the schema never published were accepted.** `_gt` on
  a `jsonb` column and `_like` on an `int` were lowered and rendered — orderings
  and casts nobody should depend on — while `__schema` said no such operator
  existed. The renderer now asks the same predicate the type system publishes
  from, so the two cannot drift. This applies to scope-policy predicates too:
  a `ScopePolicy` that compares a `jsonb` column with `.gt()` (or a non-text
  column with `.like()`) was relying on that undependable ordering and is now
  refused at render, per request — check policies against this before
  upgrading.
- **An SDL description ending in `"` or `\` produced an unparseable block
  string.** Latent — descriptions are fixed templates today — but the plan of
  record is to carry table comments in them. Such a description now moves to a
  line of its own.

### Changed

- **The compile path shares the prepare pipeline.** `compile_inner` re-spelled
  scope→limits→render inline; a pass added to `prepare` would have silently
  skipped compiled and persisted statements. Both now route through one
  function, and the root-alias uniqueness check moved into rendering, where no
  entry point can miss it.

## 0.15.0 — 2026-08-27

### Added

- **`stddev`, `stddev_pop`, `stddev_samp`, `variance`, `var_pop`, `var_samp`**,
  beside the aggregates that were already there — PostgreSQL's set under
  PostgreSQL's names.

### Fixed

- **An aggregate published one type and answered with another.** `avg` of an
  `integer` was published as `Int`, where PostgreSQL answers `numeric`; `sum` of
  an `integer` as `Int`, where it answers `bigint`. A client generating code
  from the schema was generating the wrong type. The published type is now the
  one PostgreSQL returns. The value itself still travels as a JSON number, the
  way a `numeric` column always has — digits beyond double precision are
  rounded on the way out; an exact, opt-in stringified transport is a recorded
  follow-up, not a thing this release does.
- **A function that cannot apply to a column was accepted.** `sum` over a `text`
  column reached the database and came back "function sum(text) does not exist",
  while the type system had never published it — the engine accepting what the
  schema says cannot exist. Refused at lowering, with the reason.
- **`max`/`min` were published over `boolean`, `uuid` and enum columns.** Those
  types order — `_gt` works on every one — but PostgreSQL defines no `max` or
  `min` aggregate for them, so the schema was publishing a field whose only
  possible answer was "function max(boolean) does not exist", from the
  database, at request time. No longer published, and refused with the reason.
- **The aggregate refusal guarded only documents.** The typed builder
  (`Engine::run`) never goes near the parser, so `stddev` over a `text` column
  built by hand still reached the database and came back as an opaque SQL
  error. The check now also runs where both entry points meet — rendering — so
  the builder gets the same `Validate` error, with the path and the reason.

- **Two test-suite failures that looked like flaky tests and were not.** Every
  integration test started a PostgreSQL container of its own — a hundred or so
  per run — and Docker answered one of them with `PortNotExposed` about one run
  in three. And one CLI test removed a temp directory that every other CLI test
  wrote into, deleting files the tests beside it were about to read. Tests now
  take a database rather than a container: point `TEST_DATABASE_URL` at a
  PostgreSQL and the suite runs in about twelve seconds instead of a minute,
  with each test still getting a database of its own. Without it a container is
  still started per test, retried, and removed when the test ends. No library
  code changed.
- **The shared-server test path leaked what it created and mangled one URL
  shape.** Databases taken on a `TEST_DATABASE_URL` server were never dropped —
  a hundred per run, and a reused pid recomputing a leaked name made `CREATE
  DATABASE` fail hard — so a `TestDb` now drops its database when it drops, and
  the name carries a per-run timestamp. A `TEST_DATABASE_URL` without a
  database segment (valid; sqlx defaults it) had its authority overwritten by
  the database name; the path is now looked for after the authority. And two
  places dropped the `TestDb` guard while still using the database it owned,
  which on the container path removed the server mid-test. The CLI crate's copy
  of the harness is gone — it shares the library's by path, so the next fix
  lands once.

## 0.14.0 — 2026-08-27

### Added

- **Aggregates on a relation**: `authors { posts_aggregate { aggregate { count } } }`,
  the same shape the root offers, over one row's children — which is what a
  paginated list needs, since the page and its total then travel in one request.
  Array relations only: an object relation is a single row and asking says so.
  It obeys the scope, because counting a table is reading it — the target's
  predicate lands in the aggregate's `WHERE`, a denied table is denied here too,
  and a withheld column cannot be summed. It counts against `ExecutionLimits`
  as the correlated subquery it is.
- **GraphQL-shaped errors**: `Error::to_graphql_response` and
  `Error::to_graphql_error` produce the wire form, `Error::code` the stable
  classification that goes in `extensions.code` and that an HTTP layer maps to a
  status. `Error` stays a Rust enum — a library handing back only JSON would be
  worse to program against. A database error now travels as its SQLSTATE rather
  than PostgreSQL's message text, which carries table names, constraint names
  and sometimes a source position from inside the server; the full text remains
  in `Display`, for the log. An internal error says only that it is one.
- **`operationName`**: `query_with` and `query_as_with` on `Engine`, `TxClient`,
  `ScopedEngine` and `ScopedTxClient`. It is the third field of a GraphQL
  request body, and a client that ships one document holding every operation it
  might send picks one per request by name — which previously only
  `Engine::compile_with` could do, so that shape of client could not use the
  query path at all.

### Fixed

- **An injected `default_limit` capped an aggregate's `count`.** `aggregate` and
  `nodes` read one source, so the limit decided what was counted rather than how
  many rows came back — the opposite of what it is for. `nodes` now gets a
  source of its own when a default applies; a limit the caller writes still
  applies to both.
- **A relation aggregate in mutation `returning` read the base table** while the
  relation beside it read the CTE, so one response reported a row under `posts`
  and `count: 0` under `posts_aggregate`.
- **`distinct_on` on an aggregate was parsed and dropped**, leaving `count` to
  answer a different question with no error. Refused now, and no longer
  published in the type system.
- **`ErrorCode::LimitExceeded` was unreachable**: an execution-limit refusal was
  reported as `DOCUMENT_REJECTED`. `Error::CostLimit` is a new variant and the
  two now answer differently.
- **`Error::Scope` was classified as an access denial.** It carries a policy that
  would not load and a compiled statement run through the wrong entry point —
  the host's mistakes. Reported as `INTERNAL_ERROR`.
- **A named operation that matched nothing ran anyway** when the document held
  exactly one operation: the name was not looked at. Answering about the
  operation that happens to be there when the caller asked about another is the
  same silent substitution the multi-operation case already refused. Now an
  error, as the spec has it, including for an anonymous operation that has no
  name to match.

## 0.13.0 — 2026-08-27

Hardening for the case where a document reaches the engine from a client rather
than from the application's own source. Several of these change behaviour that
used to be silent, so read **Breaking** before upgrading.

### Fixed

- **`_eq: null` returned an empty result instead of saying anything.** SQL
  answers `col = NULL` with no rows, which is not what a caller writing it
  means, and an empty result is indistinguishable from a filter that legitimately
  matched nothing. Comparisons against null — including through a variable, and
  including on a compiled statement where the value arrives later — are refused
  with a message naming `_is_null`. `_by_pk(id: null)` likewise. A null is still
  a value in `_set` and in an inserted column.

- **A column whose exposed name differed from its physical one could not be
  selected at all.** The IR field was called `physical` and held the physical
  name, while every reader looked it up with `find_column`, which is keyed by
  the exposed one — so `Table::column("salary", "salary_cents", …)` rendered
  "unknown column 'salary_cents'". Renamed to `column` and filled with the
  exposed name, which is what the name now says and what every consumer wanted.
- **`on_conflict`'s constraint name was checked by nobody**, so a typo reached
  Postgres as a 42704 at request time — slipping past `Engine::compile`, whose
  point is that a query which cannot work fails at startup. Checked against the
  constraints introspection found, and the error lists them. A hand-built
  `Schema` declares none, and an empty list is not enforced.
- **A `numeric` column read back as a JSON number but refused one**, so
  `_gt: 10` was an error and every caller had to round-trip through strings.
  Numbers and strings are both accepted now.
- **`smallint` and `character(n)` columns silently vanished** from the schema,
  as any unmapped type does. Both are mapped now. What still has no mapping —
  arrays, `bytea`, `interval`, `inet` — is recorded during introspection instead
  of only logged, and `vision-gql diff` reports it: the column being absent is
  otherwise impossible to notice from the outside, and takes any key or relation
  that depended on it along with it.

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
- **`on_conflict`'s `where` was lowered against an empty schema**, so a relation
  predicate in it failed with "relation target table missing" — the wrong cause,
  and the shape was unwritable.
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

- **Every execution copied the SQL string.** `executor::execute_on` handed sqlx
  an owned `String` per request, which for a compiled statement is a copy of the
  one thing that never changes between requests. Borrowed now. The benchmarks do
  not cover this path (they stop before the database), so no number is claimed —
  only the allocation is gone.

### Added

- **`__typename`**, in every selection set: rows, relations, `_by_pk`, aggregate
  `nodes`, the `aggregate` object and each function group inside it, mutation
  `returning`, and the mutation-response wrapper. Apollo and urql add it to
  every selection set by default, so it previously failed with "unknown column
  '__typename'" against any of them. Type names follow Hasura's scheme
  (`users`, `users_aggregate_fields`, `users_mutation_response`, …), exposed as
  `type_names`. Not accepted as a root field; `__schema` / `__type`
  introspection is still unimplemented.
- **`persisted::QueryRegistry`** — compile a set of queries at startup and run
  them by key, the shape an endpoint should prefer where clients can ship their
  documents: no new document is parsed at request time, and compile failures
  surface at startup naming the key that failed rather than on the request that
  happens to hit that query. `compile_all_scoped` compiles against a
  `ScopePolicy`, so one statement serves every principal.
- **`ExecutionLimits::bind_row_counts`** — render literal `limit`/`offset` as
  bound parameters. Off by default, since rendering inline is what keeps a
  compiled statement readable and `EXPLAIN`-able. On, every page size shares one
  statement, which is what a driver's prepared-statement cache needs when the
  number comes from a client.
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
- `LICENSE-MIT` and `LICENSE-APACHE`. `Cargo.toml` had declared
  `MIT OR Apache-2.0` since the beginning with neither text in the repository.
- `PgType::Int2`; `introspect::SkippedColumn` and `IntrospectedDb::skipped_columns`.
- `Count::var`; `Count::Var` is now a struct variant carrying an optional `max`,
  and `Count::Bound` is a new variant: a literal that renders as a bind.
- `QueryArgs::is_empty`, `AggOp::count()`, `AggCol::new`, `AggField::column`.

### Breaking

- `Error::Limit` and `Error::ScopeColumnDenied` are new variants.
- The aggregate IR changed shape: `RootBody::Aggregate.ops` is
  `Vec<AggSelect>` (a response key plus an op) rather than `Vec<AggOp>`;
  `AggOp::Count` is a struct variant carrying `columns` and `distinct`;
  `AggOp::Sum` / `Avg` / `Max` / `Min` carry `fields: Vec<AggField>` rather than
  `columns: Vec<String>`; `AggOp::Typename` is new. Builder method signatures
  are unchanged.
- `Field::Column` and `Field::JsonPath` carry `column` (the exposed name) where
  they carried `physical`.
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
