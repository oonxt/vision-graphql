# Phase 3A: Object-Relation Nested Insert — Design

## Goal

Allow `insert_*` and `insert_*_one` mutations to nest object-relation (many-to-one) input, Hasura-style. Each parent row carries a `<relname>: { data: {...} }` for an object relation; the engine inserts the referenced entity first and uses its PK as the parent row's FK value. Everything runs in one atomic SQL statement.

## Scope

**In scope:**
- Object relation (`RelKind::Object`) nested insert for `insert_*` and `insert_*_one`.
- Arbitrary depth (object → object → object).
- Coexistence with Phase 2's array-relation nested insert on the same parent row (`{ col, object_rel: { data: {...} }, array_rel: { data: [...] } }`).
- `returning` visibility of the newly-inserted object-related entities, via the existing `inserted_ctes` map.
- `affected_rows` counts include object-relation inserts (extends Phase 2's summation naturally).

**Out of scope (future phases):**
- Mixed usage within a batch: rows that specify `user: { data: {...} }` and rows that specify `user_id: 5` in the same `objects: [...]` call. MVP rejects this combination; users split into two mutation fields.
- `on_conflict` inside object-relation `data` (Phase 3B).
- Many-to-many via junction tables (Phase 3C).
- Nested input for `update_*` / `update_*_by_pk` / `delete_*` (Phase 3D).

## Input API

Hasura-compatible. `data:` wraps a single object (not array) because object relations reference one entity.

```graphql
mutation {
  insert_posts(objects: [
    { title: "p1", user: { data: { name: "alice" } } },
    { title: "p2", user: { data: { name: "bob"   } } }
  ]) {
    affected_rows                    # 4: 2 posts + 2 users
    returning {
      title
      user { name }                  # reads from m0_user CTE, not public.users
    }
  }
}
```

Works identically for `insert_posts_one(object: { title, user: { data: {...} } })`.

### Input validation rules

- A key on `objects[i]` is either a *column*, an *array relation* (Phase 2 path), or an *object relation* (this phase's new path).
- For an object-relation key, the value must be an object with exactly the key `data`, whose value must itself be an object (not an array). Any other shape is a validation error. Extra keys in the wrapper (e.g. `on_conflict`) are rejected with "unknown key; only 'data' is supported" until Phase 3B relaxes it.
- The inner `data` object is validated recursively against the target table (columns + nested relations).
- **FK-column conflict:** if the parent row specifies BOTH the object-relation (`user: { data: {...} }`) AND the mapped FK column (`user_id: 5`), reject with a clear error. Same principle as Phase 2's child-FK rejection: the FK must come from exactly one source.
- **Batch-uniform rule:** within a single `objects: [...]` array, either every row uses `<relname>: { data: {...} }` for a given object relation, or no row does. Partial population is a parse error.
- For `insert_*_one`, the batch is size 1 so the uniform rule is trivially satisfied.

## Output semantics

- `affected_rows` sums the parent INSERT CTE plus every object-relation CTE AND every array-relation CTE under this mutation's umbrella. Same filter as Phase 2 (prefix match on `{cte}` / `{cte}_`), no new logic needed.
- `returning` on the parent can select the object-relation field (e.g. `user { name }`). Since the target table is in `inserted_ctes` (via Phase 2's scoping fix), the subquery reads from the CTE and sees the freshly-inserted row. Correlation is `WHERE u."id" = m0."user_id"` — the parent's FK column points to the CTE row.
- For object relations, the rendered subquery additionally emits `LIMIT 1` (which `render_relation_subquery` already does for `RelKind::Object`), matching Phase 1's behavior.

## Atomicity

Single statement; all CTEs under one snapshot. Any failure at any level rolls back the entire mutation. Same guarantee as Phase 2.

## Implementation sketch

### AST changes

Split the single `nested` field (introduced in Phase 2) into two maps for clarity:

```rust
pub struct InsertObject {
    pub columns: BTreeMap<String, serde_json::Value>,
    pub nested_arrays:  BTreeMap<String, NestedArrayInsert>,   // renamed from `nested`
    pub nested_objects: BTreeMap<String, NestedObjectInsert>,  // NEW
}

pub struct NestedObjectInsert {
    pub table: String,
    pub row: InsertObject,  // exactly one row per parent
}
```

Rename affects all call sites in `parse_insert_object`, `render_insert_cte_recursive`, `integration_nested_insert.rs` fixture helpers (if any inspect the AST), and the snapshot tests in `src/sql.rs` that build these structures directly.

### Parser changes

`parse_insert_object`:
- Remove the `RelKind::Object` early-rejection branch.
- Add a sibling branch that validates `{ data: <object> }`, recurses into the object, enforces no-FK-column-also-set, and populates `nested_objects`.

`parse_insert_args`:
- After building the full `Vec<InsertObject>`, enforce the batch-uniform rule: collect the set of object-relation keys used across all rows; for each such key, verify every row in the batch has that key (not just some). If mixed, error.
  - This check runs only on object-relation keys, not array relations, because array relations are already per-row-varying (some rows can have an empty `data: []`, others can omit the key entirely; that's fine).

### SQL generation strategy

In `render_insert_cte_recursive`, BEFORE emitting `{cte}_input` and `{cte}`:

1. For each object-relation key that appears in at least one row (uniform-rule guarantees all rows have it if any do), emit a full CTE chain for that child table using the existing recursive render function. The child CTE alias is `{cte}_{relname}` (e.g., `m0_user`). Child rows' ordinals are 1..=N matching the parent rows' ordinals 1:1.
2. Emit `{cte}_{relname}_ord` with ROW_NUMBER() so the parent can JOIN against it.
3. When emitting the parent `{cte}` INSERT:
   - Include the FK columns from each object relation's `mapping` in the physical INSERT column list.
   - The SELECT source becomes a multi-way JOIN: `FROM {cte}_input i JOIN {cte}_user_ord u ON u.ord = i.ord JOIN {cte}_org_ord o ON o.ord = i.ord ...` — one JOIN per sibling object relation.
   - FK column values are pulled from the joined ord CTEs.
4. Array-relation children continue to render AFTER the parent INSERT (unchanged from Phase 2).

Canonical target SQL for a 2-row `insert_posts` with nested `user`:

```sql
WITH
  -- Object-relation parents, inserted FIRST (1:1 with parent rows).
  m0_user_input AS (SELECT * FROM (VALUES (1, $1::text), (2, $2::text)) AS t(ord, "name")),
  m0_user AS (
    INSERT INTO "public"."users" ("name")
    SELECT "name" FROM m0_user_input ORDER BY ord
    RETURNING *
  ),
  m0_user_ord AS (SELECT *, ROW_NUMBER() OVER () AS ord FROM m0_user),

  -- Parent insert, FK pulled from m0_user_ord.
  m0_input AS (SELECT * FROM (VALUES (1, $3::text), (2, $4::text)) AS t(ord, "title")),
  m0 AS (
    INSERT INTO "public"."posts" ("title", "user_id")
    SELECT i."title", u."id"
    FROM m0_input i JOIN m0_user_ord u ON u.ord = i.ord
    RETURNING *
  )
SELECT json_build_object(
  'insert_posts', json_build_object(
    'affected_rows', ((SELECT count(*) FROM m0_user) + (SELECT count(*) FROM m0)),
    'returning', (SELECT coalesce(json_agg(json_build_object(
        'title', m0."title",
        'user',  (SELECT row_to_json(r) FROM (
                   SELECT u."name" AS "name" FROM m0_user u
                   WHERE u."id" = m0."user_id" LIMIT 1
                 ) r)
    )), '[]'::json) FROM m0)
  )
) AS result;
```

### Edge cases

- **Sibling with array relations:** the object-relation chain emits first, the parent INSERT happens in the middle, then the `{cte}_ord` for array-children is emitted after the parent as in Phase 2. Unchanged machinery.
- **Deep object nesting** (object → object → object): recursive call handles it naturally. The `render_insert_cte_recursive` call for the inner object itself needs to emit any object relations it has before its own INSERT.
- **Multiple sibling object relations on one parent:** emit each object chain sequentially, then the parent JOINs against all of them in the INSERT SELECT.
- **Composite FKs:** the relation's `mapping: Vec<(parent_col, child_col)>` generalizes naturally — the JOIN condition ANDs each pair.
- **Object-inserted parent also has array-relation children** (e.g., `insert_posts(objects: [{ title, user: { data: { name, posts: { data: [...] } } } }])`): the object-relation recursion triggers its own `render_insert_cte_recursive` call, which independently emits array CTEs after its own INSERT. Arbitrary combinations of nesting work.
- **Empty object-relation data** (`user: { data: {} }`): caught by the existing "insert row must set at least one column or nested relation" error.

## Testing plan

Integration tests in a new file `tests/integration_nested_insert_object.rs` to isolate the larger fixture:

1. **Happy path single parent** — `insert_posts_one(object: { title, user: { data: {...} } })` → both rows inserted, returning shows the user.
2. **Happy path batch (uniform)** — two posts, two distinct new users; returning shows correct user per post.
3. **Sibling object + array** — post with nested user (object) and nested comments (array); all three tables inserted.
4. **Two-level object nesting** — post → user → organization (needs a 3-level fixture: `posts`/`users`/`orgs` with `users.organization_id`); assert all three levels insert and correlate correctly.
5. **Returning sees the object-inserted entity** — `returning { user { name } }` contains the newly-inserted user's name (locks in `inserted_ctes` extension to object CTEs).
6. **Correlation stress** — 5 posts with 5 different users; round-trip query confirms each post's user_id points at the right user by name.
7. **`insert_*_one` variant** — single-row shape.
8. **Atomic rollback** — nested user insert fails (e.g., NOT NULL violation on users column) → no post persists.
9. **Validation: mixed batch rejected** — row 1 uses `user: { data: {...} }`, row 2 uses `user_id: 5` → parse error.
10. **Validation: row supplies both nested AND FK column** — `{ title, user: { data: {...} }, user_id: 5 }` → parse error.
11. **Validation: `user: { data: [] }`** (array for object relation) → parse error.
12. **Validation: `user: { data: {} }`** (empty object) → caught by existing "at least one column or relation" rule.

Plus one SQL snapshot in `src/sql.rs mod tests` locking the canonical CTE ordering.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| AST rename (`nested` → `nested_arrays`) breaks Phase 2 tests / snapshots | Rename is mechanical; all call sites compile-checked. The 3 Phase-2 snapshots may regenerate once if snapshot tests construct the AST by name; verify they're semantically unchanged. |
| Batch-uniform check adds parser complexity | Implement after per-row parse loop; one set-equality check. Clear error message so users understand the MVP limitation. |
| Multi-way JOIN in the parent INSERT could get complex with many sibling object relations | Each sibling adds one JOIN; the pattern scales linearly. Not a concern for typical use. |
| Object-relation recursion depth could explode SQL size | Same concern as Phase 2 array recursion; practical limit is developer-set schema depth. Not a blocker. |

## Decomposition check

Phase 3A is a single coherent subsystem (AST rename + parser extension + renderer ordering change + tests). Not decomposable further — the AST rename affects parser and renderer in the same commit cycle, and the renderer change is only meaningful with the parser feeding it the new `nested_objects` map.
