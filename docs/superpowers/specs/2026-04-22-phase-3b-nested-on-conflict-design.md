# Phase 3B: Nested `on_conflict` (Upsert Inside Nested Data) — Design

## Goal

Allow `on_conflict` as a sibling of `data` inside nested insert wrappers (both array and object) so users can upsert at any nesting level. Preserve top-level `on_conflict` semantics (Phase 1 behavior unchanged). Transparently rewrite `DO NOTHING` to a no-op `DO UPDATE` in nested contexts so the RETURNING correlation stays 1:1 with input rows.

## Scope

**In scope:**
- `on_conflict` accepted as a sibling of `data` in array-relation wrappers: `posts: { data: [...], on_conflict: {...} }`.
- `on_conflict` accepted as a sibling of `data` in object-relation wrappers: `user: { data: {...}, on_conflict: {...} }`.
- Same `on_conflict` shape as Phase 1 top-level: `{ constraint: <name>, update_columns: [<col>...], where?: {...} }`.
- Transparent rewrite: when `update_columns: []` (user-facing `DO NOTHING`) appears inside a NESTED wrapper, the renderer emits `DO UPDATE SET <pk_col> = EXCLUDED.<pk_col>` — a no-op update that forces PostgreSQL's `RETURNING *` to include conflict rows. Top-level `DO NOTHING` is unchanged.
- `affected_rows` counts CTE row counts as today; with the no-op rewrite, conflict rows count as "affected" at nested levels. Top-level semantics (skipped rows not counted) unchanged.
- Arbitrary nesting depth preserves on_conflict at each level.

**Out of scope (future phases):**
- Many-to-many via junction tables (Phase 3C).
- Nested input for `update_*` / `update_*_by_pk` (Phase 3D).
- Changing top-level `on_conflict` semantics.
- Supporting `on_conflict` with no constraint (e.g., PG's ON CONFLICT DO NOTHING without a constraint) — we stick with Phase 1's required `constraint` field.

## Input API

Hasura-shape, mirroring the existing top-level form:

```graphql
mutation {
  insert_posts(objects: [{
    title: "p1",
    user: {
      data: { name: "alice", email: "alice@ex.com" },
      on_conflict: {
        constraint: "users_name_key",
        update_columns: []                    # upsert: link to existing alice
      }
    }
  }]) {
    affected_rows                             # 2: 1 post + 1 user (existing counted)
    returning {
      title
      user { name email }                     # existing alice's data if she preexisted
    }
  }
}
```

Works identically for array-relation on_conflict:

```graphql
insert_users(objects: [{
  name: "bob",
  posts: {
    data: [{ title: "unique-slug-1" }, { title: "existing-slug" }],
    on_conflict: {
      constraint: "posts_slug_key",
      update_columns: ["title"]
    }
  }
}])
```

### Input validation rules

- The nested wrapper object now accepts exactly `data` and optional `on_conflict`. Any other key is rejected with an updated error message listing both supported keys.
- `on_conflict` is parsed by the existing `parse_on_conflict` helper, which validates the shape (`constraint`, `update_columns`, `where?`) and the column names against the NESTED table (not the parent).
- The `constraint` name itself is not validated by the parser — PostgreSQL catches missing constraints at execution time. Same behavior as Phase 1.

## AST change

Extend `NestedArrayInsert` and `NestedObjectInsert` to carry optional on_conflict:

```rust
pub struct NestedArrayInsert {
    pub table: String,
    pub rows: Vec<InsertObject>,
    pub on_conflict: Option<OnConflict>,      // NEW
}

pub struct NestedObjectInsert {
    pub table: String,
    pub row: InsertObject,
    pub on_conflict: Option<OnConflict>,      // NEW
}
```

No new `OnConflict` type — reuse Phase 1's.

## Parser change

In `parse_insert_object` (`src/parser.rs`):

- **Array arm** (around `src/parser.rs:586-596`): replace the "only 'data' is supported" rejection loop with a two-key check. Accept `data` (existing behavior) and `on_conflict` (new: run `parse_on_conflict` against the target table). Reject any other key.
- **Object arm** (around `src/parser.rs:668-678`): same treatment.
- When constructing `NestedArrayInsert` / `NestedObjectInsert`, pass the parsed `on_conflict` (or `None`).

The error message changes from "only 'data' is supported" to "only 'data' and 'on_conflict' are supported".

## Renderer change

### Thread on_conflict through nested recursion

At the two call sites that currently hardcode `None` for nested on_conflict (`src/sql.rs:876` for object, `:1107` for array), pass the per-nested-insert `on_conflict` field instead.

### Transparent DO NOTHING rewrite

`render_on_conflict` gains a new parameter:

```rust
fn render_on_conflict(
    oc: &crate::ast::OnConflict,
    table: &Table,
    schema: &Schema,
    nested_context: bool,                    // NEW
    ctx: &mut RenderCtx,
) -> Result<()>
```

Behavior:

- `nested_context = false` (top-level): unchanged — empty `update_columns` → `DO NOTHING`.
- `nested_context = true` (inside a nested CTE): empty `update_columns` → `DO UPDATE SET <pk_col> = EXCLUDED.<pk_col>`, using the first entry of `table.primary_key`. This is a semantic no-op — the PK column gets "set" to its existing value — but forces `RETURNING *` to include conflict rows, which the downstream `ROW_NUMBER()` ord correlation depends on.

If the target table has NO primary key declared in the schema, the rewrite cannot fire safely (no anchor column to self-update). Emit a clear validation error: `nested DO NOTHING on-conflict requires a primary key on table '<name>'`. In practice every INSERT-target table we support has a PK.

Pseudocode for the new branch:

```rust
if oc.update_columns.is_empty() {
    if nested_context {
        // Rewrite DO NOTHING → DO UPDATE SET pk = EXCLUDED.pk (no-op, but forces RETURNING).
        let pk_name = table.primary_key.first().ok_or_else(|| Error::Validate {
            path: "on_conflict".into(),
            message: format!(
                "nested DO NOTHING on-conflict requires a primary key on table '{}'",
                table.exposed_name
            ),
        })?;
        let pk_col = table.find_column(pk_name).ok_or_else(|| Error::Validate {
            path: "on_conflict".into(),
            message: format!(
                "primary key column '{pk_name}' missing on '{}'",
                table.exposed_name
            ),
        })?;
        write!(
            ctx.sql,
            "DO UPDATE SET {} = EXCLUDED.{}",
            quote_ident(&pk_col.physical_name),
            quote_ident(&pk_col.physical_name),
        )
        .unwrap();
    } else {
        ctx.sql.push_str("DO NOTHING");
    }
}
```

The non-empty `update_columns` path stays unchanged.

### Call-site plumbing

Every call to `render_on_conflict` must pass `nested_context`. There are two callers in `src/sql.rs`:

1. Inside `render_insert_cte_recursive` (the one that renders the parent INSERT): the current call passes no context flag. It becomes `render_on_conflict(oc, table, schema, parent_link.is_some() || !object_rel_names.is_empty() || /* also true when this itself is an object-relation prerequisite insert */ ..., ctx)`.

   Simpler and more robust: pass `nested_context = true` whenever this recursive call was triggered from OTHER than the top-level `render_insert_cte` wrapper. We can detect that by passing a boolean down `render_insert_cte_recursive` as an extra parameter:

   ```rust
   fn render_insert_cte_recursive(
       ...,
       is_nested_cte: bool,   // NEW — top-level passes false, recursive calls pass true
       ...
   ) -> Result<()>
   ```

   `render_insert_cte` (the public entry) calls with `is_nested_cte: false`. The two internal recursive calls (object-relation prerequisite, array-relation child) pass `is_nested_cte: true`. Then `render_on_conflict` gets `is_nested_cte` verbatim.

### How this interacts with top-level on_conflict

Top-level `on_conflict` (the one on `insert_posts(objects: [...], on_conflict: {...})`) goes through `render_insert_cte_recursive` with `is_nested_cte = false` because it's the top-level call. `render_on_conflict` sees `nested_context = false`, empty `update_columns` → `DO NOTHING`. Phase 1 behavior preserved.

Top-level INSERT has another subtlety: for the RETURNING-order-based correlation to work at top level, conflict rows also need to be in m0_ord. But Phase 1 / 2 / 3A top-level inserts with `DO NOTHING` simply don't include conflict rows in RETURNING, and nothing nested depends on the top-level CTE's ord. So: if a TOP-LEVEL insert has on_conflict DO NOTHING AND has nested children (object or array), the children of the skipped parents are also skipped. This is consistent with Hasura and is the correct behavior — users who want the existing parent's children use `DO UPDATE`.

Explicitly document this in the README as the top-level interaction.

## Output semantics

- `affected_rows` counts every row in every CTE (unchanged sum logic). With the nested DO UPDATE no-op, conflict rows appear in the CTE and count toward `affected_rows`. Users asking "how many rows did my mutation touch" get a count that includes existing rows reused via conflict. This is Hasura-consistent.
- `returning` reads from the nested CTE (per Phase 2's `inserted_ctes` mechanism) and so shows either the newly-inserted row OR the existing row (with any Phase 3B DO UPDATE actually applied on it). For DO NOTHING rewrite, the returning shows the existing row's original column values.

## Edge cases

- **Composite primary key:** `table.primary_key` is `Vec<String>`. The rewrite picks the first PK column — which is sufficient for a no-op self-update. We don't need to write all PK columns because the semantic goal is only "force RETURNING to include this row".
- **Table with no primary key:** rewrite cannot fire; `render_on_conflict` returns a clear validation error. Users with PK-less tables must supply non-empty `update_columns` for nested on_conflict.
- **Nested on_conflict combined with sibling object + array:** fully supported — each nested CTE renders its own on_conflict independently. No interaction required.
- **Top-level `on_conflict` combined with nested children:** documented behavior — top-level conflict rows are excluded from m0 (because Phase 1 DO NOTHING is preserved), so their children are also excluded. Test coverage: one test verifies this stays consistent with Phase 1.

## Testing plan

New integration file `tests/integration_nested_on_conflict.rs` with a dedicated fixture:
- `users(id serial pk, name text UNIQUE, email text)` — `name` has a unique constraint `users_name_key` for `on_conflict`.
- `posts(id serial pk, title text UNIQUE, user_id int references users(id))` — `title` has a unique constraint `posts_title_key`.
- `organizations(id serial pk, name text UNIQUE)` — two-level fixture.
- `users.organization_id` nullable FK to support two-level nesting.

Tests:

1. **Nested object `on_conflict` DO UPDATE**: pre-seed "alice"; insert post with `user: { data: { name: "alice", email: "new@e.com" }, on_conflict: { constraint: "users_name_key", update_columns: ["email"] } }`. Assert post links to alice's id; alice's email is updated.
2. **Nested object `on_conflict` DO NOTHING** (flagship): pre-seed "alice" with email "old@e.com"; insert post with `user: { data: { name: "alice", email: "ignored@e.com" }, on_conflict: { constraint: "users_name_key", update_columns: [] } }`. Assert post inserted; alice's email still "old@e.com"; `returning.user.email` shows "old@e.com" (existing row returned via the transparent rewrite).
3. **Nested array `on_conflict` DO UPDATE**: pre-seed post with title "old-title"; insert user with `posts: { data: [{ title: "old-title" }, { title: "new-title" }], on_conflict: { constraint: "posts_title_key", update_columns: ["title"] } }`. Assert new-title inserted; old-title row updated (self-update).
4. **Nested array `on_conflict` DO NOTHING**: pre-seed post titled "conflict"; insert user with `posts: { data: [{ title: "conflict" }, { title: "fresh" }], on_conflict: { constraint, update_columns: [] } }`. Assert returning contains both posts (one conflict-skipped-via-rewrite, one fresh); parent correlation preserved.
5. **Top-level `DO NOTHING` unchanged (regression)**: pre-seed "dup"; `insert_users(objects: [{name: "dup"}], on_conflict: {constraint, update_columns: []})` → `affected_rows == 0`, returning empty. This is the existing Phase 1 test; include a version here to lock behavior against accidental top-level rewrite.
6. **Parser validation: unknown sibling key** — `user: { data: {...}, unknown_key: "x" }` → error message mentions both `data` and `on_conflict`.
7. **Parser validation: malformed on_conflict** — `user: { data: {...}, on_conflict: { constraint: null } }` → error from `parse_on_conflict`.
8. **Two-level nested with on_conflict**: `insert_posts(objects: [{ title, user: { data: { name, organization: { data: {...}, on_conflict: {...} } } } }])` → organization-level on_conflict applied correctly.
9. **Combined sibling object + array + each with on_conflict**: `insert_posts(objects: [{ title, user: { data: {...}, on_conflict: {...} }, comments: { data: [...], on_conflict: {...} } }])` → both upserts work, returning shows both.

Plus one SQL snapshot in `src/sql.rs` tests mod locking the DO NOTHING rewrite shape (must emit `DO UPDATE SET "id" = EXCLUDED."id"` for a nested empty-update_columns case).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| The rewrite's no-op `DO UPDATE SET pk = pk` triggers any `BEFORE UPDATE` trigger on the table | Document as a known behavior. Users who need strict "no side effects on conflict" must use top-level `on_conflict` or split the mutation. |
| Affected_rows counts existing rows on nested DO NOTHING, surprising users | Document; matches Hasura. The alternative (not counting) would require a separate CTE for conflict rows — more machinery for unclear benefit. |
| Composite-PK tables: rewrite picks first PK column only | Correct — PG only needs to "see" the row being updated. Writing more PK columns wouldn't change behavior. Documented in the renderer code. |
| Table with only a serial PK and one nullable column where `DO UPDATE SET pk = EXCLUDED.pk` is weird | Still valid SQL, no-op. Tested via the snapshot. |

## Decomposition check

Phase 3B is one coherent subsystem (AST extension + parser accept + renderer thread + rewrite). Not further decomposable without making the rewrite a follow-up task that leaves Phase 3B half-done.
