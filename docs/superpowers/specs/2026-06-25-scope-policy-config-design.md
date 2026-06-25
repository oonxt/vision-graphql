# Scope policy & config — design

**Date:** 2026-06-25
**Status:** design (pre-implementation)
**Scope:** make scoped execution fast to *build per request* and possible to
*configure declaratively*, without changing the engine's enforcement path.

## Problem

Today a `ScopeSet` is hand-built per request from raw `BoolExpr`:

```rust
fn user_scope(user_id: i64) -> ScopeSet {
    ScopeSet::new()
        .allow("users",  eq("id", user_id))
        .allow("orders", eq("user_id", user_id))
        .allow("samples", BoolExpr::Relation {
            name: "order".into(),
            inner: Box::new(eq("user_id", user_id)),
        })
        .unrestricted("adverts")
}
```

Pain points:

1. **Static shape, per-request rebuild.** The policy *shape* (tables, columns,
   relation chains) is fixed; only the principal value (`user_id`) varies. Every
   request reallocates and clones the whole predicate tree.
2. **Raw `BoolExpr` is noisy.** `Relation { name, inner: Box::new(..) }` is hard
   to read, especially for deeper chains.
3. **No build-time validation.** A wrong table/column/relation name only surfaces
   at query time, inside `apply_scope`, on every request.
4. **No declarative form.** Schema is auditable via TOML overlay
   (`schema/config.rs`), but scope — a security policy — is Rust-only.

## Non-goals

- No change to `apply_scope` or the SQL renderer. The per-request artifact stays
  `ScopeSet` + concrete `BoolExpr`; everything here produces that artifact.
- No new enforcement semantics. This is purely *construction & configuration*
  ergonomics over the existing filter/check model.

## Architecture

Three layers, each producing input for the next; `ScopeSet` remains the single
artifact the engine consumes.

```
A. predicate DSL  ──►  B. ScopePolicy (template, validated once)  ──►  ScopeSet
        ▲                          ▲                               (per request,
   C. TOML config ─────────────────┘                                via .bind())
```

### Layer A — predicate DSL

Terse builders that produce a predicate *template* node, `ScopeExpr` (same shape
as `BoolExpr`, but leaf values are an `Operand`, see Layer B):

```rust
pub fn col(name: impl Into<String>) -> Col;
impl Col {
    fn eq / neq / gt / gte / lt / lte / like / ilike(self, v: impl Into<Operand>) -> ScopeExpr;
    fn is_null(self) -> ScopeExpr;  fn is_not_null(self) -> ScopeExpr;
    fn in_(self, vs: impl IntoIterator<Item = impl Into<Operand>>) -> ScopeExpr;  fn nin(..);
}
pub fn rel(name: impl Into<String>, inner: ScopeExpr) -> ScopeExpr;  // Relation/EXISTS
pub fn and(parts: impl IntoIterator<Item = ScopeExpr>) -> ScopeExpr;
pub fn or(..) -> ScopeExpr;   pub fn not(e: ScopeExpr) -> ScopeExpr;
```

The samples chain becomes:

```rust
rel("order", col("user_id").eq(principal()))
```

`ScopeExpr::literal() -> Result<BoolExpr>` resolves a placeholder-free template to
a concrete `BoolExpr`, so the DSL also serves the existing raw `ScopeSet::allow`
path (escape hatch). Lives in `src/predicate.rs`, re-exported at the crate root.

### Layer B — `ScopePolicy` template + `bind`

A policy whose principal positions are placeholders, built and validated **once**
(app lifetime), then bound cheaply per request.

There is exactly **one** placeholder concept — a named param. `principal` is just
the conventional default name; `principal()` ≡ `param("principal")` and
`bind_value(x)` ≡ `bind(Principal::new().set("principal", x))`. No separate "bare
value" variant.

```rust
pub enum Operand { Lit(serde_json::Value), Param(String) }

pub struct ScopePolicy { /* table -> Allow(ScopeExpr) | Unrestricted | Deny */ }

impl ScopePolicy {
    pub fn builder() -> ScopePolicyBuilder;        // allow/unrestricted/deny, like ScopeSet
    pub fn validate(self, schema: &Schema) -> Result<ScopePolicy>;  // build-time checks
    pub fn bind(&self, principal: &Principal) -> Result<ScopeSet>;  // per request
    pub fn bind_value(&self, v: impl Into<Value>) -> Result<ScopeSet>; // convenience: fills bare Principal
}
```

```rust
// once, at startup:
static POLICY: Lazy<ScopePolicy> = Lazy::new(|| ScopePolicy::builder()
    .allow("users",  col("id").eq(principal()))
    .allow("orders", col("user_id").eq(principal()))
    .allow("samples", rel("order", col("user_id").eq(principal())))
    .unrestricted("adverts")
    .validate(&schema).unwrap());

// per request:
let scope = POLICY.bind_value(user_id)?;   // tree-walk + clone, no parsing
engine.scoped(scope).query(..).await?;
```

- **`bind`** substitutes every `Operand::Principal`/`Param` with concrete values
  and produces today's concrete `BoolExpr`/`ScopeSet`. Errors only on a missing
  param.
- **`validate`** runs the same lookups `apply_scope` does now (table exists,
  every `Compare`/`IsNull`/`InList` column exists, every `rel(..)` resolves to a
  known relation + target table) — reusing `lookup_table` / `find_relation` /
  `find_column` from `scope.rs`. Moves point 3's failures to startup.

**Principal model.** `Principal` is a small map for named params plus an optional
bare value:

```rust
let p = Principal::new().set("tenant_id", tid).set("user_id", uid);
// col("tenant_id").eq(param("tenant_id"))  ↔  Param("tenant_id")
// col("user_id").eq(principal())           ↔  bare Principal (p's bare value)
```

`bind_value(uid)` is sugar for a Principal with only the bare value set — covers
the common single-tenant-key case.

### Layer C — declarative TOML

Mirror `schema/config.rs`. Reuse the GraphQL where lowering (`parser::lower_where`,
made `pub(crate)`) so the `where` value is the same object syntax users already
know from queries; `"$principal"` / `"$param:name"` string leaves are recognized
and rewritten to `Operand::Principal` / `Operand::Param`.

```toml
[tables.users]
where = '{ id: { _eq: "$principal" } }'

[tables.orders]
where = '{ user_id: { _eq: "$principal" } }'

[tables.samples]
where = '{ order: { user_id: { _eq: "$principal" } } }'   # relation chain

[tables.adverts]
unrestricted = true

[tables.secrets]
deny = true
```

Pipeline: parse TOML → for each table, parse the `where` string to JSON → swap
`$`-sentinel leaves for placeholders → `lower_where(json, table, schema)` →
`ScopeExpr` → assemble `ScopePolicy` → `validate(&schema)`. Output is the *same*
`ScopePolicy` as Layer B, so `bind` is identical. Auditable, hot-reloadable.

## Composition & migration

- `ScopeSet` and `apply_scope` are untouched → fully backward compatible.
- A produces `ScopeExpr`, consumed by B and C.
- C parses TOML into the same `ScopePolicy` B builds in code.
- Raw `ScopeSet::allow(table, BoolExpr)` stays as an escape hatch.

## Resolved decisions

1. **Principal API** → named-param map, `Principal::new().set(name, value)`.
   `bind_value(x)` is sugar for the param named `principal`. One concept, engine
   stays type-agnostic.
2. **`bind` fallibility** → `bind` returns `Result`; it fails only when the
   Principal is missing a param the policy references. `validate(&schema)` (run
   once at build) already covers table/column/relation existence.
3. **TOML placeholder syntax** → in a where **value position**, a string fully
   matching `^\$[A-Za-z_][A-Za-z0-9_]*$` is a param reference (`"$tenant_id"` →
   `Param("tenant_id")`). `$$` escapes a literal leading `$` (`"$$5.00"` → literal
   `"$5.00"`); a `$` elsewhere in a string (`"a$b"`) is literal. Placeholders are
   recognized only in `_eq`/`_neq`/…/`_in` value slots — never column, operator,
   or relation names. `lower_where` does not type-check values (coercion is
   deferred to render via `json_to_bind`), so placeholder strings pass through
   lowering untouched; the `BoolExpr`→`ScopeExpr` pass then rewrites the leaves.
4. **`lower_where` exposure** → made `pub(crate)`; C deliberately shares the one
   query where-syntax.

### `where` in TOML

`where` is a native TOML value (not an embedded JSON string), converted
`toml::Value` → `serde_json::Value` → `lower_where`:

```toml
[tables.orders]
where = { user_id = { _eq = "$principal" } }

[tables.audit_log.where]
_and = [
  { tenant_id = { _eq = "$tenant_id" } },
  { actor_id  = { _eq = "$user_id" } },
]
```

Each `[tables.X]` must set exactly one of `where`, `unrestricted = true`, or
`deny = true`.

## Rollout

1. Layer A (`predicate.rs`) + `ScopeExpr`/`Operand` — no behavior change, additive.
2. Layer B (`ScopePolicy`, `Principal`, `bind`, `validate`) — additive; refactor
   `apply_scope`'s lookups into shared helpers used by `validate`.
3. Layer C (`scope_config.rs`) — additive; `pub(crate) lower_where`.

Each layer ships independently behind no flag (pure addition); existing code keeps
working unchanged.
