# Phase 2: Nested One-to-Many Insert — Design

## Goal

Allow `insert_*` and `insert_*_one` mutations to nest child-array relations in their `objects` / `object` input, Hasura-style. Everything happens in a single SQL statement (chained CTEs), atomic on success/failure, and the parent's `returning` clause can select newly-inserted children.

## Scope

**In scope:**
- Array (one-to-many / `RelKind::Array`) nested insert.
- Arbitrary nesting depth (users → posts → comments → …).
- Multiple sibling array relations on the same parent.
- Empty `data: []` arrays (parent inserts, zero children).
- Atomic single-statement execution.
- `returning` visibility of newly-inserted children.
- `affected_rows` counts parents + all descendants (Hasura parity).

**Out of scope (future phases):**
- Object-relation (`RelKind::Object`, many-to-one) nested insert — child references a nested-inserted parent.
- `on_conflict` inside nested `data` blocks (upsert-on-nested).
- Many-to-many via junction tables (requires new schema metadata).
- Nested input for `update_*` / `update_*_by_pk` / `delete_*`.
- Top-level mutations remain flat for `update` and `delete`.

## Input API

Hasura-compatible. The `data:` wrapper is mandatory (not a shorthand) so `on_conflict` can be added as a sibling key in Phase 3 without a breaking change.

```graphql
mutation {
  insert_users(objects: [
    {
      name: "alice",
      posts: {
        data: [
          { title: "p1" },
          { title: "p2", published: true }
        ]
      }
    },
    {
      name: "bob",
      posts: { data: [{ title: "p3" }] }
    }
  ]) {
    affected_rows            # 5 (2 users + 3 posts)
    returning {
      id
      name
      posts(order_by: [{ id: asc }]) { title }
    }
  }
}
```

Works identically for `insert_users_one(object: { ... })`.

### Input validation rules

- A key on the `objects[i]` map is either a *column* (exposed column on the parent table) or an *array-kind relation* on the parent table (`table.find_relation(key)` with `RelKind::Array`).
- If the key resolves to an `RelKind::Object` relation (many-to-one), reject with a clear validation error indicating object-relation inserts are not yet supported (Phase 3).
- If the key is neither, reject as unknown (current behavior).
- For a relation key, the value must be an object with exactly one key `data`, whose value must be a JSON array. Missing `data` or non-array `data` is a validation error.
- The `data` array may be empty. Each element is validated recursively against the *child* table's columns and relations.
- The FK columns on the child side (those that the parent PK maps into, per the relation's `mapping`) must NOT appear in child input — they are populated by the engine from the parent's RETURNING. Including them is a validation error (conflicts with engine-supplied value).

## Output semantics

- `affected_rows` sums rows inserted across all CTEs (parent + every descendant). This matches Hasura and is easy to compute from the CTEs we already generate.
- `returning` on the parent can select any column of the parent, and any (Phase-1 supported) relation on the parent.
- **PostgreSQL data-modifying CTE visibility constraint:** sub-statements in a `WITH` block run under one snapshot and cannot see each other's effects on base tables. Concretely, a nested `returning { posts { title } }` rendered with a Phase-1-style `FROM "public"."posts"` subquery would return zero rows for freshly-inserted parents — the rows committed by the child CTE aren't yet visible to other reads of the target table.
- **Resolution:** when a `returning` relation's target table has a child `INSERT` CTE in this statement, the render must read *from the CTE name* (e.g., `FROM m1`) rather than the real table. The renderer gets a new `inserted_ctes: HashMap<TableName, CteAlias>` context so each nested-relation render can decide the correct source. For relations whose target wasn't nested-inserted, the Phase-1 real-table pattern remains correct.
- `insert_*_one` returns a single object (not `{ affected_rows, returning }`). `affected_rows` is not observable from `_one`; children are still inserted.

## Atomicity

All inserts run in a single SQL statement through chained data-modifying CTEs. Any failure (NOT NULL violation, CHECK constraint, FK violation, type coercion failure) rolls back the whole statement. The consistency guarantee is native PostgreSQL behavior — no application-level transaction management needed.

## Implementation sketch

### AST changes

Today's `MutationField::Insert.objects` is `Vec<BTreeMap<String, serde_json::Value>>` — flat. Replace with a recursive type:

```rust
pub struct InsertObject {
    /// Column-name → value for this table.
    pub columns: BTreeMap<String, serde_json::Value>,
    /// Nested array-relation inserts keyed by the parent-side relation name.
    pub nested: BTreeMap<String, NestedArrayInsert>,
}

pub struct NestedArrayInsert {
    /// Target table name, resolved from the parent relation's `target_table`.
    pub table: String,
    /// Child rows for each parent. Length of the outer `Vec` in the parent
    /// matches the number of parents; this inner `Vec` holds that parent's
    /// children.
    pub rows: Vec<InsertObject>,
}
```

`MutationField::Insert.objects` becomes `Vec<InsertObject>`.

### Parser changes

`parse_insert_args` (currently builds `objects: Vec<BTreeMap<...>>`) walks the input GraphQL object:
- For each key, first try `table.find_column` — if it resolves, treat as a column.
- Else try `table.find_relation(key)`:
  - If `RelKind::Array`: recurse into `data: [...]` → list of `InsertObject` for the target table.
  - If `RelKind::Object`: return a "not yet supported in nested insert" validation error.
- Else: unknown key.
- Validate that FK columns on the child side are absent from each child's `columns`.

### SQL generation strategy

**Correlation approach: ordinality-tagged INSERT ... SELECT.** Parents go in via a JSON/VALUES source tagged with an ordinal column; the inserted PKs are correlated back to their parent-ordinal using PostgreSQL's de-facto guarantee that `RETURNING` on a simple single-statement `INSERT ... SELECT` preserves source order.

A dedicated correlation integration test (insert batch of 5+ parents, assert each child's `user_id` matches its parent's `id`) will guard this assumption. If a future PG version breaks it, the test fires.

Generated SQL shape for the Input API example above (conceptual, one CTE per table level):

```sql
WITH
-- Tag parent input with ordinality.
p0_input AS (
  SELECT * FROM (VALUES
    (1, 'alice'),
    (2, 'bob')
  ) AS t(ord, name)
),
-- Parent insert, preserving input order.
m0 AS (
  INSERT INTO "public"."users" ("name")
  SELECT name FROM p0_input ORDER BY ord
  RETURNING *
),
-- Correlate inserted PKs with the parent ordinal.
m0_ord AS (
  SELECT m0.*, ROW_NUMBER() OVER () AS ord FROM m0
),
-- Tag child input with parent ordinal.
p1_input AS (
  SELECT * FROM (VALUES
    (1, 'p1',     NULL::bool),
    (1, 'p2',     TRUE       ),
    (2, 'p3',     NULL::bool)
  ) AS t(parent_ord, title, published)
),
-- Child insert using parent's real PK.
m1 AS (
  INSERT INTO "public"."posts" ("title", "published", "user_id")
  SELECT c.title, c.published, p.id
  FROM p1_input c
  JOIN m0_ord   p ON p.ord = c.parent_ord
  RETURNING *
)
SELECT json_build_object(
  'insert_users', json_build_object(
    'affected_rows', (SELECT count(*) FROM m0) + (SELECT count(*) FROM m1),
    'returning',     (SELECT coalesce(json_agg(json_build_object(
                         'id',    m0."id",
                         'name',  m0."name",
                         'posts', (SELECT coalesce(json_agg(row_to_json(r)), '[]'::json)
                                    FROM (SELECT t."title"
                                          FROM m1 t
                                          WHERE t."user_id" = m0."id"
                                          ORDER BY t."id" ASC) r)
                       )), '[]'::json) FROM m0)
  )
) AS result;
```

Notes:
- Child `INSERT ... SELECT ... JOIN` eliminates the need for per-parent CTEs — one CTE per table level, scales cleanly.
- The nested `posts` subquery in `returning` reads from the child CTE `m1`, not the real `posts` table, because PG's same-statement snapshot hides the just-inserted rows from base-table reads. The renderer checks its `inserted_ctes` map: `posts` has been nested-inserted → source is `m1`.
- A sibling `returning` relation whose target is *not* in `inserted_ctes` (e.g., a `reactions` relation that wasn't part of this mutation) continues to read from the real table as in Phase 1 — which legitimately returns empty for a freshly-inserted parent.
- Arbitrary depth = recurse this pattern: `m2` CTE for grandchildren joins against `m1`'s ordinal; etc.
- Multiple sibling relations on the same parent = multiple child CTEs (`m1_posts`, `m1_comments`, …) all joining back to `m0_ord`.

### Edge cases

- **Empty `data: []`**: don't emit the child CTE; parent insert proceeds alone. Simplifies SQL.
- **Parent has no PK** / **relation mapping is not on the PK**: the relation's `mapping` tells us which parent columns flow into which child columns. Use those columns from `m0` directly (not necessarily the PK).
- **Composite FKs**: mapping is `Vec<(String, String)>`; generalize the `JOIN ... ON p.col = c.col AND …` naturally.
- **Null PK**: if the parent auto-generates PKs (SERIAL/UUID), they appear in the RETURNING. If a user explicitly provides the FK-mapped column as a parent column, it flows through normally. Both work.
- **Type coercion for child columns**: the child `INSERT ... SELECT` exposes each column's type — we'll rely on PostgreSQL's normal coercion plus our existing param-binding logic.

## Testing plan

Integration tests (new file `tests/integration_nested_insert.rs` to keep the bigger mutation file focused):
1. **Happy-path single parent + children**: insert one user with two posts; assert `affected_rows == 3`, returning shows both posts.
2. **Multiple parents each with children**: insert two users, each with different child counts; assert correlation (each child's FK matches the right parent).
3. **Correlation stress test**: insert 5+ parents with varying child counts; assert every child's `user_id` matches the right parent via round-trip query.
4. **Multi-level nesting**: users → posts → comments (3-table fixture); assert all three levels insert atomically and comments correctly point at their posts.
5. **Sibling relations**: user with two array relations (e.g., `posts` and a new fixture relation `tags`); both child CTEs land rows.
6. **Empty nested array**: `posts: { data: [] }` — parent inserts, no child CTE emitted.
7. **Atomic rollback**: child has a CHECK-violating value; parent insert must not persist (use a subsequent query to verify no orphan parent).
8. **`insert_*_one` with nested**: single-parent shape.
9. **Returning sees nested children**: within the same mutation, `returning { posts { title } }` contains the just-inserted posts — locks in the CTE-read-vs-real-table-read decision.
10. **Returning unrelated relation stays correct**: parent inserted with nested posts, `returning { reactions { ... } }` (a non-nested relation) returns `[]` — proves sibling relations not in `inserted_ctes` still read from the real table.
11. **Validation: missing `data`**: `posts: { }` → parse error.
12. **Validation: `data` not array**: `posts: { data: {} }` → parse error.
13. **Validation: object relation rejected**: `insert_posts(objects: [{ title, user: { data: {...} } }])` returns "object-relation nested insert not yet supported".
14. **Validation: child supplies FK column**: `posts: { data: [{ user_id: 99, title: "x" }] }` → parse error (engine-supplied column conflict).

Unit/snapshot tests:
- One SQL snapshot locking the chained-CTE shape for the happy-path case (§ "Generated SQL shape" above as the oracle).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| PG doesn't guarantee `RETURNING` order preservation | Correlation stress test catches regressions in any PG version. Switch to per-parent CTEs only if it ever fires. |
| Parser/AST refactor breaks Phase 1 mutation tests | Refactor is additive on the `objects` field type; migrate call sites mechanically and keep Phase 1 tests green throughout TDD flow. |
| Fixture bloat in integration tests | New multi-level nesting test file isolates the larger 3-table fixture from `integration_mutation.rs`. |
| SQL text size for large batches | One CTE per table level → constant in batch size, only child-row VALUES grows. Not a concern. |

## Decomposition check

Phase 2 is a single coherent subsystem: nested-insert input + SQL generation + tests. Not decomposable further without fragmenting what must be tested end-to-end (parser accepting nested shape is useless without the renderer emitting CTEs, and vice versa). One spec, one plan.
