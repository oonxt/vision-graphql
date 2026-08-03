# Compile / execute split & parse cache — design

**Date:** 2026-08-03
**Status:** implemented (0.12.0)
**Scope:** split a request into work that depends only on the query text and
work that depends on the request, so the first kind can be done once. Adds
`Engine::compile` / `Engine::execute` and a document parse cache. No change to
what SQL is generated or to how scope is enforced.

## Problem

Every call to `Engine::query` does the whole pipeline:

```
parse (12.6us) → lower (3.8us) → render (1.5us) → execute
```

Measured on a moderately complex query (nested relation, `where`, `order_by`,
`limit`). Two observations about that 18.9us:

1. **Parsing depends only on the text.** It is 70% of the cost and needs
   neither variables nor a principal, yet it is redone per request.
2. **Nothing else could be reused even if we wanted to**, because
   `parse_and_lower` substituted variable *values* into the IR while lowering.
   `where: {id: {_eq: $id}}` with `$id = 1` and `$id = 2` produced two different
   `Operation`s, so any cache keyed on the query text alone would be wrong —
   and a cache keyed on (text, variables) would never hit for the case that
   matters.

The perf number alone does not justify a refactor: 18.9us against a 0.3–10ms
round trip is 0.2–2% of request latency. What justifies it is that the same
change makes the SQL a *stable artifact*: something to validate at startup,
`EXPLAIN` once, review, or hold in a persisted-query allowlist. Perf is the
side effect.

## Non-goals

- No change to generated SQL. A literal `limit: 10` still renders `LIMIT 10`
  inline; only `limit: $n` becomes a bind. Every pre-existing integration test
  passes unmodified.
- No change to `apply_scope`'s enforcement model. Scope is still a pre-render
  AST rewrite injecting predicates at every table access point; only *when the
  principal's values are substituted* moves.
- No persisted-query protocol, allowlist, or wire format. This provides the
  artifact those would be built on.
- Compiled statements do not run inside `Engine::transaction`.

## Architecture

```
[GraphQL string] ─→ parse ─┐
                           ├─→ IR (Operation) ─→ SQL + Vec<BindSpec>
[Rust builder]  ──────────ᐤ                            ↑
                                          variables + principal bind here
```

The pipeline is unchanged in shape. What moved is the point where values enter:
previously during lowering, now after rendering.

### 1. Parse cache (`src/parse_cache.rs`)

`ParseCache: source → Arc<ExecutableDocument>`, held by `Engine` and shared with
every handle it spawns (`TxClient`, `ScopedEngine`, …).

Decisions worth recording:

- **Key is the full source string, not a hash.** A hash collision would execute
  a different caller's query. That is a security bug, not a cache miss, and no
  hit-rate argument outweighs it.
- **Two-generation eviction, not LRU.** Entries land in `hot`; when `hot` fills,
  it demotes wholesale to `cold` and the old `cold` is dropped. A hit in `cold`
  promotes back. Every operation is O(1) with no per-entry bookkeeping, and a
  query in steady use never falls out. The cost is looser eviction and up to
  `2 x capacity` documents retained.
- **Parsing happens outside the lock.** Concurrent misses on the same source may
  parse it more than once — wasted work, never a wrong answer — rather than
  serialising every caller behind one parse.
- **Sources over 16 KiB are never stored,** so a caller sending huge one-off
  documents cannot pin `2 x capacity` of them in memory.

### 2. Late-bound values (`ast::Val`, `ast::Count`)

```rust
enum Val { Lit(Value), Var(String), ScopeParam(String), Array(Vec<Val>), Object(Vec<(String, Val)>) }
enum Count { Lit(u64), Var(String) }
```

`Val` replaces `serde_json::Value` in every value position of the IR:
`BoolExpr::Compare.value`, `InList.values`, `RootBody::ByPk.pk`, `Update`/
`UpdateByPk`'s `set` and `pk`, `DeleteByPk.pk`, `InsertObject.columns`.

`Count` is separate from `Val` on purpose. A literal count renders *inline*
(`LIMIT 10`) exactly as before — it is part of the query text, so it is as fixed
as the rest of the SQL — while a variable becomes `LIMIT $n::int8`. That keeps
existing SQL byte-identical and still lets one statement serve every page size.

`Val::collapse()` folds a composite containing no variables back to
`Val::Lit`, so a written-out list is indistinguishable from what the old code
produced and downstream only special-cases composites that actually defer
something.

### 3. Two lowering modes (`parser::Bindings`)

```rust
enum Bindings<'a> { Eager(&'a Value), Symbolic }
```

Both modes run the *same* walker. They differ only at a variable, and only
according to what the position is:

| position | Eager | Symbolic |
|---|---|---|
| value (`_eq: $x`, `_in: $ids`, `limit: $n`, pk args, `_set` values) | substitute → `Val::Lit` | `Val::Var` |
| structural (`where: $w`, `order_by`, `distinct_on`, `_is_null: $b`, insert args) | substitute and keep walking | `Error::NotCompilable { path }` |

The enabling change is that **`lower_where` now walks the GraphQL AST rather
than pre-substituted JSON** — that is where variables still exist. The eager
path stays uniform via `structural()`, which converts a substituted variable's
JSON back into a (variable-free) GraphQL value and continues the same walk. One
implementation, two modes, no duplicated 150-line where-lowerer.

`Error::NotCompilable` names the position rather than saying "cannot compile",
because the fix is always local: move the variable to a value position or run
that query through `Engine::query`.

### 4. Rendering to a recipe (`types::BindSpec`)

`sql::render` returns `(String, Vec<BindSpec>)` instead of `(String, Vec<Bind>)`.

```rust
enum BindSpec {
    Fixed(Bind),                                  // determined by the query text
    Scalar { val: Val, pg: PgType, path: String },
    Array  { val: Val, pg: PgType, path: String },
    Count  { val: Count, path: String },
}
```

Anything the query text pins down is converted *at render time* and stored as
`Fixed`. So a literal's type error (`id: {_eq: "nope"}` on an int column)
surfaces when the query is compiled, not on whichever request happens to run it.
Only genuinely per-request values stay symbolic. `path` is carried so the error
a deferred conversion produces is identical to the one render used to produce.

`sql::render_now(op, schema, inputs)` = render + resolve, which is what every
eager caller uses; an eagerly lowered operation carries only literals, so it
passes `Inputs::none()`.

### 5. Scope compiles against a policy, not a principal

Previously: `ScopePolicy` → `resolve(&Principal)` → `ScopeSet` with concrete
values → `apply_scope` inlines them into the AST. Compiling after that point
would produce one statement *per principal* — in a multi-tenant deployment, the
cache would be useless and the memory unbounded.

So `ScopeExpr::symbolic()` lowers a policy leaving `Operand::Param(name)` as
`Val::ScopeParam(name)`, and `ScopePolicy::symbolic()` builds a `ScopeSet` of
those. `apply_scope` is unchanged — it does not care whether the `BoolExpr` it
injects holds literals or parameters. The principal binds at
`Engine::execute_scoped`.

Two guards, both tested against a live database:

- A statement compiled **with** a policy refuses to run without a principal.
- A statement compiled **without** a policy refuses to run *with* one — a
  principal that silently restricted nothing is worse than an error.

The flag is recorded explicitly rather than inferred from "did any scope
parameter survive", because a policy that only marks tables `unrestricted` has
no parameters and would otherwise be executable by either path.

## What cannot be compiled

Positions where a variable decides the *shape* of the SQL:

- `where: $w`, `order_by: $o`, `distinct_on: $d` — which predicates and clauses
  exist at all.
- `_is_null: $b` — picks between `IS NULL` and `IS NOT NULL`.
- any variable inside an `insert` argument. A VALUES list's row count and column
  set come from the argument itself, so `objects: $rows` could never compile;
  this cut also does not thread variables into written-out rows, so
  `objects: [{name: $n}]` is refused. An entirely written-out insert compiles —
  there is simply nothing left for it to defer.

Everything else compiles: comparison values, `_in` lists including `_in: $ids`,
`limit`/`offset`, `_by_pk` arguments, `update`'s `where` and `_set` values,
`delete`'s `where`.

Uncompilable queries run through `Engine::query` unchanged. Nothing regresses;
compiling is opt-in.

## Results

| path | per-request CPU |
|---|---|
| uncached (0.11 behaviour) | 18.9 us |
| parse cache only | 4.0 us |
| compiled (resolve binds only) | 43 ns |

`benches/compile_vs_query.rs`. 333 tests pass, including new integration
coverage that a compiled *scoped* update/delete still applies its pre-image
filter and post-write check, and that a compiled scoped query filters nested
relations.

## Maintenance invariant

Adding an IR value position that uses `serde_json::Value` instead of `Val`, or
lowering a structural read without `parser::structural()`, breaks `compile`
without failing any test — `Engine::query` keeps working. Same trap as scope's
pre-render rewrite: correctness lives in a pass the obvious edit site does not
mention. A new `ScopeExpr` variant needs both `resolve` and `symbolic`, or
compiled scoped queries silently lose that predicate.

## Breaking changes (0.11 → 0.12)

Source-compatible: `Engine::query` / `run` / `*_as`, the builder API, scope
policies, the CLI.

Breaking for direct IR/renderer users:

- `sql::render` returns `Vec<BindSpec>`; use `sql::render_now` for the old shape.
- `BoolExpr::Compare.value`, `InList.values`, `ByPk.pk`, `Update`/`UpdateByPk`
  `set`/`pk`, `DeleteByPk.pk`, `InsertObject.columns` now hold `Val`
  (`Val::Lit(v)` or `v.into()` for the old value).
- `QueryArgs.limit` / `offset` are `Option<Count>`.
- `parser::lower_where` takes a GraphQL value plus a `Bindings` mode.
- `Error` gained `NotCompilable`.
