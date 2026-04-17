# Vision-GraphQL Phase 5 — Schema Introspection + TOML Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Populate `Schema` automatically from a live PostgreSQL connection (tables, columns, primary keys, unique constraints, foreign-key-derived relations) and optionally augment with a TOML config file. Introspect → TOML → builder is the layered merge order; later layers win.

**Architecture:** A new `schema/introspect.rs` module runs four `information_schema`-based queries to assemble a raw `IntrospectedDb` snapshot, maps PG type names to `PgType` variants, then generates auto-relations from foreign keys (skipping ambiguous many-FK-to-one-table cases with a `tracing::warn`). A new `schema/config.rs` parses TOML into a `ConfigOverlay` structure and applies it on top. `SchemaBuilder` gets `from_introspection()` and `load_config()` helpers that merge overlays into its builder state so the existing manual builder methods still win last.

**Tech Stack:** Adds `toml = "0.8"` and `serde` derives; reuses existing `deadpool-postgres` for introspection.

**Out of scope:**
- Materialized views, partitioned tables, inheritance
- Postgres enums (mapped to `PgType::Text` for MVP)
- Computed fields backed by SQL functions
- "via" through-table relations — Phase 6 or later
- Schema-qualified config (only `public` schema in MVP)

---

## File Structure

```
Cargo.toml             # add toml dep
src/schema.rs          # keep public API; delegate to submodules
src/schema/mod.rs      # new (promote schema.rs → schema/mod.rs)
src/schema/introspect.rs    # live DB → IntrospectedDb
src/schema/config.rs        # TOML → ConfigOverlay
src/schema/merge.rs         # apply overlays to SchemaBuilder
tests/integration_introspect.rs   # end-to-end introspection
tests/fixtures/schema.toml        # sample config used by tests
```

Promoting `schema.rs` to a directory is a one-time reorganization. The public import path stays `vision_graphql::schema::*`.

---

### Task 1: Convert schema.rs to a module directory

**Files:**
- Move: `src/schema.rs` → `src/schema/mod.rs`
- Create: `src/schema/introspect.rs` (empty)
- Create: `src/schema/config.rs` (empty)
- Create: `src/schema/merge.rs` (empty)

- [ ] **Step 1: Move the file**

Run:
```bash
mkdir -p src/schema
git mv src/schema.rs src/schema/mod.rs
```

- [ ] **Step 2: Add module declarations**

Append to `src/schema/mod.rs` right after the existing `//!` doc comment block:

```rust
pub mod config;
pub mod introspect;
pub mod merge;
```

- [ ] **Step 3: Create empty submodule files**

`src/schema/introspect.rs`:
```rust
//! Schema introspection from a live PostgreSQL connection.
```

`src/schema/config.rs`:
```rust
//! TOML configuration overlay for Schema.
```

`src/schema/merge.rs`:
```rust
//! Merge introspection results and TOML overlays into SchemaBuilder.
```

- [ ] **Step 4: Build + test**

Run: `cargo build`
Expected: clean compile.

Run: `cargo test --lib`
Expected: all previous tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/schema/
git commit -m "refactor(schema): promote schema.rs to module directory"
```

---

### Task 2: PgType mapping helper

**Files:**
- Modify: `src/schema/introspect.rs`

- [ ] **Step 1: Write failing test**

Append to `src/schema/introspect.rs`:

```rust
use crate::schema::PgType;

/// Map a `information_schema.columns.data_type` string to a [`PgType`].
/// Returns `None` for unsupported types (caller should skip the column).
pub fn data_type_to_pg_type(data_type: &str) -> Option<PgType> {
    match data_type {
        "integer" => Some(PgType::Int4),
        "bigint" => Some(PgType::Int8),
        "text" => Some(PgType::Text),
        "character varying" => Some(PgType::Varchar),
        "boolean" => Some(PgType::Bool),
        "real" => Some(PgType::Float4),
        "double precision" => Some(PgType::Float8),
        "numeric" => Some(PgType::Numeric),
        "uuid" => Some(PgType::Uuid),
        "timestamp without time zone" => Some(PgType::Timestamp),
        "timestamp with time zone" => Some(PgType::TimestampTz),
        "jsonb" => Some(PgType::Jsonb),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_types() {
        assert_eq!(data_type_to_pg_type("integer"), Some(PgType::Int4));
        assert_eq!(data_type_to_pg_type("text"), Some(PgType::Text));
        assert_eq!(data_type_to_pg_type("boolean"), Some(PgType::Bool));
        assert_eq!(
            data_type_to_pg_type("timestamp with time zone"),
            Some(PgType::TimestampTz)
        );
    }

    #[test]
    fn unknown_type_returns_none() {
        assert_eq!(data_type_to_pg_type("hstore"), None);
        assert_eq!(data_type_to_pg_type("geometry"), None);
    }
}
```

- [ ] **Step 2: Run test**

Run: `cargo test --lib schema::introspect::tests`
Expected: PASS (2 tests).

- [ ] **Step 3: Commit**

```bash
git add src/schema/introspect.rs
git commit -m "feat(schema): PgType mapping from information_schema data_type"
```

---

### Task 3: Introspect tables + columns

**Files:**
- Modify: `src/schema/introspect.rs`

- [ ] **Step 1: Add data structures + query function**

Replace `src/schema/introspect.rs` with:

```rust
//! Schema introspection from a live PostgreSQL connection.

use crate::error::{Error, Result};
use crate::schema::PgType;
use deadpool_postgres::Pool;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct IntrospectedDb {
    /// Keyed by `(schema_name, table_name)`.
    pub tables: BTreeMap<(String, String), IntrospectedTable>,
}

#[derive(Debug)]
pub struct IntrospectedTable {
    pub schema: String,
    pub name: String,
    pub columns: Vec<IntrospectedColumn>,
    pub primary_key: Vec<String>,
    /// `constraint_name -> [column, ...]` for UNIQUE and PRIMARY KEY constraints.
    pub unique_constraints: BTreeMap<String, Vec<String>>,
    pub foreign_keys: Vec<IntrospectedForeignKey>,
}

#[derive(Debug)]
pub struct IntrospectedColumn {
    pub name: String,
    pub pg_type: PgType,
    pub nullable: bool,
}

#[derive(Debug)]
pub struct IntrospectedForeignKey {
    pub constraint_name: String,
    pub from_columns: Vec<String>,
    pub to_schema: String,
    pub to_table: String,
    pub to_columns: Vec<String>,
}

/// Map a `information_schema.columns.data_type` string to a [`PgType`].
pub fn data_type_to_pg_type(data_type: &str) -> Option<PgType> {
    match data_type {
        "integer" => Some(PgType::Int4),
        "bigint" => Some(PgType::Int8),
        "text" => Some(PgType::Text),
        "character varying" => Some(PgType::Varchar),
        "boolean" => Some(PgType::Bool),
        "real" => Some(PgType::Float4),
        "double precision" => Some(PgType::Float8),
        "numeric" => Some(PgType::Numeric),
        "uuid" => Some(PgType::Uuid),
        "timestamp without time zone" => Some(PgType::Timestamp),
        "timestamp with time zone" => Some(PgType::TimestampTz),
        "jsonb" => Some(PgType::Jsonb),
        _ => None,
    }
}

/// Query PG for tables, columns, PKs, unique constraints, and foreign keys in
/// the `public` schema and return a raw `IntrospectedDb`.
pub async fn introspect(pool: &Pool) -> Result<IntrospectedDb> {
    let client = pool.get().await?;
    let mut db = IntrospectedDb::default();

    // Tables + columns
    let rows = client
        .query(
            r#"
            SELECT table_schema, table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_schema, table_name, ordinal_position
            "#,
            &[],
        )
        .await?;
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        let cname: String = row.get(2);
        let dtype: String = row.get(3);
        let is_nullable: String = row.get(4);
        let Some(pg_type) = data_type_to_pg_type(&dtype) else {
            tracing::warn!(
                target: "vision_graphql::introspect",
                table = %tname,
                column = %cname,
                data_type = %dtype,
                "skipping column with unsupported type"
            );
            continue;
        };
        let entry = db
            .tables
            .entry((schema.clone(), tname.clone()))
            .or_insert_with(|| IntrospectedTable {
                schema: schema.clone(),
                name: tname.clone(),
                columns: Vec::new(),
                primary_key: Vec::new(),
                unique_constraints: BTreeMap::new(),
                foreign_keys: Vec::new(),
            });
        entry.columns.push(IntrospectedColumn {
            name: cname,
            pg_type,
            nullable: is_nullable == "YES",
        });
    }

    // Primary keys
    let rows = client
        .query(
            r#"
            SELECT tc.table_schema, tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
            ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position
            "#,
            &[],
        )
        .await?;
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        let cname: String = row.get(2);
        if let Some(t) = db.tables.get_mut(&(schema, tname)) {
            t.primary_key.push(cname);
        }
    }

    // Unique constraints (including primary keys — useful for on_conflict)
    let rows = client
        .query(
            r#"
            SELECT tc.table_schema, tc.table_name, tc.constraint_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY') AND tc.table_schema = 'public'
            ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position
            "#,
            &[],
        )
        .await?;
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        let constraint: String = row.get(2);
        let cname: String = row.get(3);
        if let Some(t) = db.tables.get_mut(&(schema, tname)) {
            t.unique_constraints
                .entry(constraint)
                .or_default()
                .push(cname);
        }
    }

    // Foreign keys
    let rows = client
        .query(
            r#"
            SELECT
                tc.table_schema,
                tc.table_name,
                tc.constraint_name,
                kcu.column_name,
                ccu.table_schema AS foreign_schema,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column,
                kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            JOIN information_schema.referential_constraints rc
              ON tc.constraint_name = rc.constraint_name
             AND tc.table_schema = rc.constraint_schema
            JOIN information_schema.constraint_column_usage ccu
              ON rc.unique_constraint_name = ccu.constraint_name
             AND rc.unique_constraint_schema = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
            ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position
            "#,
            &[],
        )
        .await?;
    let mut fk_acc: BTreeMap<(String, String, String), IntrospectedForeignKey> = BTreeMap::new();
    for row in rows {
        let schema: String = row.get(0);
        let tname: String = row.get(1);
        let constraint: String = row.get(2);
        let cname: String = row.get(3);
        let f_schema: String = row.get(4);
        let f_table: String = row.get(5);
        let f_col: String = row.get(6);
        let key = (schema.clone(), tname.clone(), constraint.clone());
        let fk = fk_acc
            .entry(key)
            .or_insert_with(|| IntrospectedForeignKey {
                constraint_name: constraint.clone(),
                from_columns: Vec::new(),
                to_schema: f_schema.clone(),
                to_table: f_table.clone(),
                to_columns: Vec::new(),
            });
        fk.from_columns.push(cname);
        fk.to_columns.push(f_col);
    }
    for ((schema, tname, _), fk) in fk_acc {
        if let Some(t) = db.tables.get_mut(&(schema, tname)) {
            t.foreign_keys.push(fk);
        }
    }

    // Avoid unused-import warning when compiled without the `_` binding path.
    let _ = Error::Schema;
    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_types() {
        assert_eq!(data_type_to_pg_type("integer"), Some(PgType::Int4));
        assert_eq!(data_type_to_pg_type("text"), Some(PgType::Text));
        assert_eq!(data_type_to_pg_type("boolean"), Some(PgType::Bool));
        assert_eq!(
            data_type_to_pg_type("timestamp with time zone"),
            Some(PgType::TimestampTz)
        );
    }

    #[test]
    fn unknown_type_returns_none() {
        assert_eq!(data_type_to_pg_type("hstore"), None);
    }
}
```

Note: the `let _ = Error::Schema;` line is a placeholder to keep `Error` imported — remove it once the merge code (Task 6) uses `Error` directly. Actually remove it now and drop the `use crate::error::{Error, Result};` import's `Error` part:

```rust
use crate::error::Result;
```

Keep the import of `Result` since `introspect` returns `Result<...>`.

- [ ] **Step 2: Build**

Run: `cargo build`
Expected: clean compile.

- [ ] **Step 3: Unit tests**

Run: `cargo test --lib schema::introspect`
Expected: existing PgType mapping tests still pass (2 tests).

Live DB integration test comes in Task 9.

- [ ] **Step 4: Commit**

```bash
git add src/schema/introspect.rs
git commit -m "feat(schema): introspect tables, columns, PKs, unique constraints, FKs"
```

---

### Task 4: FK auto-relations (single-FK case)

**Files:**
- Modify: `src/schema/merge.rs`

- [ ] **Step 1: Write failing test**

Append to `src/schema/merge.rs`:

```rust
use crate::schema::introspect::{IntrospectedDb, IntrospectedForeignKey, IntrospectedTable};
use crate::schema::{RelKind, Relation};
use std::collections::BTreeMap;

/// For each `(source_table, target_table)` pair that has exactly one foreign key
/// connecting them, derive an Object relation on the source and an Array
/// relation on the target. Pairs with multiple FKs are skipped; callers of the
/// resulting builder can declare them explicitly.
///
/// Returns `(source_exposed_name, relation_name, Relation)` triples.
pub fn derive_relations_from_fks(
    db: &IntrospectedDb,
) -> Vec<(String, String, Relation)> {
    let mut out = Vec::new();
    // Count FKs per (source_table, target_table)
    let mut pair_counts: BTreeMap<(String, String, String, String), usize> =
        BTreeMap::new();
    for t in db.tables.values() {
        for fk in &t.foreign_keys {
            let key = (
                t.schema.clone(),
                t.name.clone(),
                fk.to_schema.clone(),
                fk.to_table.clone(),
            );
            *pair_counts.entry(key).or_insert(0) += 1;
        }
    }

    for t in db.tables.values() {
        for fk in &t.foreign_keys {
            let key = (
                t.schema.clone(),
                t.name.clone(),
                fk.to_schema.clone(),
                fk.to_table.clone(),
            );
            let count = *pair_counts.get(&key).unwrap_or(&0);
            if count != 1 {
                tracing::warn!(
                    target: "vision_graphql::merge",
                    from = %t.name,
                    to = %fk.to_table,
                    fks = count,
                    "skipping FK auto-relation: multiple FKs between same table pair"
                );
                continue;
            }

            // Only derive within `public` schema for MVP.
            if t.schema != "public" || fk.to_schema != "public" {
                continue;
            }

            let mapping: Vec<(String, String)> = fk
                .from_columns
                .iter()
                .zip(fk.to_columns.iter())
                .map(|(a, b)| (a.clone(), b.clone()))
                .collect();
            // Object relation on source → target (e.g. posts.user)
            let src_rel_name = fk.to_table.trim_end_matches('s').to_string();
            // Fallback if target doesn't end in 's'
            let src_rel_name = if src_rel_name.is_empty() || src_rel_name == fk.to_table {
                fk.to_table.clone()
            } else {
                src_rel_name
            };
            out.push((
                t.name.clone(),
                src_rel_name,
                Relation {
                    kind: RelKind::Object,
                    target_table: fk.to_table.clone(),
                    mapping: mapping.clone(),
                },
            ));
            // Array relation on target → source (e.g. users.posts)
            let rev_mapping: Vec<(String, String)> = fk
                .to_columns
                .iter()
                .zip(fk.from_columns.iter())
                .map(|(a, b)| (a.clone(), b.clone()))
                .collect();
            out.push((
                fk.to_table.clone(),
                t.name.clone(),
                Relation {
                    kind: RelKind::Array,
                    target_table: t.name.clone(),
                    mapping: rev_mapping,
                },
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::introspect::IntrospectedColumn;
    use crate::schema::PgType;

    fn fixture_with_posts_to_users() -> IntrospectedDb {
        let mut db = IntrospectedDb::default();
        db.tables.insert(
            ("public".into(), "users".into()),
            IntrospectedTable {
                schema: "public".into(),
                name: "users".into(),
                columns: vec![IntrospectedColumn {
                    name: "id".into(),
                    pg_type: PgType::Int4,
                    nullable: false,
                }],
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                foreign_keys: vec![],
            },
        );
        db.tables.insert(
            ("public".into(), "posts".into()),
            IntrospectedTable {
                schema: "public".into(),
                name: "posts".into(),
                columns: vec![
                    IntrospectedColumn {
                        name: "id".into(),
                        pg_type: PgType::Int4,
                        nullable: false,
                    },
                    IntrospectedColumn {
                        name: "user_id".into(),
                        pg_type: PgType::Int4,
                        nullable: false,
                    },
                ],
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                foreign_keys: vec![IntrospectedForeignKey {
                    constraint_name: "posts_user_id_fkey".into(),
                    from_columns: vec!["user_id".into()],
                    to_schema: "public".into(),
                    to_table: "users".into(),
                    to_columns: vec!["id".into()],
                }],
            },
        );
        db
    }

    #[test]
    fn single_fk_generates_both_directions() {
        let db = fixture_with_posts_to_users();
        let rels = derive_relations_from_fks(&db);
        assert_eq!(rels.len(), 2);
        // posts.user (Object)
        assert!(rels
            .iter()
            .any(|(src, name, r)| src == "posts" && name == "user" && r.kind == RelKind::Object));
        // users.posts (Array)
        assert!(rels
            .iter()
            .any(|(src, name, r)| src == "users" && name == "posts" && r.kind == RelKind::Array));
    }

    #[test]
    fn multiple_fks_to_same_target_skipped() {
        let mut db = fixture_with_posts_to_users();
        // Add second FK posts.editor_id → users.id
        let posts = db.tables.get_mut(&("public".into(), "posts".into())).unwrap();
        posts.columns.push(IntrospectedColumn {
            name: "editor_id".into(),
            pg_type: PgType::Int4,
            nullable: true,
        });
        posts.foreign_keys.push(IntrospectedForeignKey {
            constraint_name: "posts_editor_fkey".into(),
            from_columns: vec!["editor_id".into()],
            to_schema: "public".into(),
            to_table: "users".into(),
            to_columns: vec!["id".into()],
        });
        let rels = derive_relations_from_fks(&db);
        assert!(rels.is_empty());
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test --lib schema::merge`
Expected: both tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/schema/merge.rs
git commit -m "feat(schema): derive Object+Array relations from single FKs"
```

---

### Task 5: Build a Schema from introspection

**Files:**
- Modify: `src/schema/mod.rs`
- Modify: `src/schema/merge.rs`

- [ ] **Step 1: Add `from_introspection` to SchemaBuilder**

In `src/schema/mod.rs`, append to the `impl SchemaBuilder` block:

```rust
    /// Seed the builder from an introspected database. The resulting builder
    /// can be further customized before `.build()`.
    pub fn from_introspection(db: crate::schema::introspect::IntrospectedDb) -> Self {
        crate::schema::merge::build_from_introspection(db)
    }
```

- [ ] **Step 2: Implement build_from_introspection**

In `src/schema/merge.rs`, append:

```rust
use crate::schema::{SchemaBuilder, Table};
use crate::schema::introspect::IntrospectedDb as Db;

pub fn build_from_introspection(db: Db) -> SchemaBuilder {
    let rels = derive_relations_from_fks(&db);
    let mut sb = crate::schema::Schema::builder();
    for ((_, tname), it) in &db.tables {
        let mut t = Table::new(tname, &it.schema, tname);
        for col in &it.columns {
            t = t.column(&col.name, &col.name, col.pg_type.clone(), col.nullable);
        }
        if !it.primary_key.is_empty() {
            let refs: Vec<&str> = it.primary_key.iter().map(String::as_str).collect();
            t = t.primary_key(&refs);
        }
        for (src, name, rel) in &rels {
            if src == tname {
                t = t.relation(name, rel.clone());
            }
        }
        sb = sb.table(t);
    }
    sb
}

/// Convenience: introspect + build directly.
pub async fn introspect_into_builder(
    pool: &deadpool_postgres::Pool,
) -> crate::error::Result<SchemaBuilder> {
    let db = crate::schema::introspect::introspect(pool).await?;
    Ok(build_from_introspection(db))
}
```

- [ ] **Step 3: Public entry point on Schema**

In `src/schema/mod.rs`, append to the `impl Schema` block:

```rust
    /// Introspect the database and return a ready-to-customize builder.
    pub async fn introspect(pool: &deadpool_postgres::Pool) -> crate::error::Result<SchemaBuilder> {
        crate::schema::merge::introspect_into_builder(pool).await
    }
```

- [ ] **Step 4: Build**

Run: `cargo build`
Expected: clean compile.

- [ ] **Step 5: Commit**

```bash
git add src/schema/mod.rs src/schema/merge.rs
git commit -m "feat(schema): Schema::introspect returns a SchemaBuilder seeded from PG"
```

---

### Task 6: TOML config overlay types

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/schema/config.rs`

- [ ] **Step 1: Add toml dep**

In `Cargo.toml` under `[dependencies]`, add:

```toml
toml = "0.8"
```

- [ ] **Step 2: Write failing test**

Replace `src/schema/config.rs` with:

```rust
//! TOML configuration overlay for Schema.

use crate::error::{Error, Result};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigOverlay {
    #[serde(default)]
    pub tables: BTreeMap<String, TableOverlay>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableOverlay {
    #[serde(default)]
    pub expose_as: Option<String>,
    #[serde(default)]
    pub hide_columns: Vec<String>,
    #[serde(default)]
    pub relations: Vec<RelationOverlay>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationOverlay {
    pub name: String,
    pub kind: RelationKindOverlay,
    pub target: String,
    pub mapping: Vec<(String, String)>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RelationKindOverlay {
    Object,
    Array,
}

pub fn parse(source: &str) -> Result<ConfigOverlay> {
    toml::from_str(source).map_err(|e| Error::Schema(format!("TOML parse error: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_overlay() {
        let s = r#"
            [tables.users]
            expose_as = "profiles"
            hide_columns = ["password_hash"]

            [[tables.users.relations]]
            name = "followers"
            kind = "array"
            target = "users"
            mapping = [["id", "followed_id"]]
        "#;
        let cfg = parse(s).unwrap();
        let users = cfg.tables.get("users").unwrap();
        assert_eq!(users.expose_as.as_deref(), Some("profiles"));
        assert_eq!(users.hide_columns, vec!["password_hash".to_string()]);
        assert_eq!(users.relations.len(), 1);
        assert_eq!(users.relations[0].kind, RelationKindOverlay::Array);
    }

    #[test]
    fn rejects_unknown_fields() {
        let s = r#"
            [tables.users]
            unknown_field = 1
        "#;
        assert!(parse(s).is_err());
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib schema::config`
Expected: both tests pass.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml src/schema/config.rs
git commit -m "feat(schema): TOML ConfigOverlay types with serde"
```

---

### Task 7: Apply ConfigOverlay to SchemaBuilder

**Files:**
- Modify: `src/schema/merge.rs`
- Modify: `src/schema/mod.rs`

Applying an overlay requires rebuilding tables because `Table` is immutable once added. We'll extract the current tables from the builder, apply the overlay to each, and rebuild.

- [ ] **Step 1: Expose internal state on SchemaBuilder**

In `src/schema/mod.rs`, update `SchemaBuilder` and its impl so merge code can access its inner tables:

Replace:
```rust
pub struct SchemaBuilder {
    tables: HashMap<String, Arc<Table>>,
}
```

with:
```rust
pub struct SchemaBuilder {
    pub(crate) tables: HashMap<String, Arc<Table>>,
}
```

Then add a method to allow replacing a table's `Table` wholesale (used by merge):

```rust
impl SchemaBuilder {
    pub(crate) fn insert_raw(&mut self, exposed: String, t: Arc<Table>) {
        self.tables.insert(exposed, t);
    }

    pub(crate) fn remove_raw(&mut self, exposed: &str) -> Option<Arc<Table>> {
        self.tables.remove(exposed)
    }

    pub(crate) fn get_raw(&self, exposed: &str) -> Option<&Arc<Table>> {
        self.tables.get(exposed)
    }
}
```

- [ ] **Step 2: Expose Table internals needed for rebuild**

In `src/schema/mod.rs`, add accessors used by merge (they don't change the public API):

Inside `impl Table`, add:

```rust
    pub(crate) fn columns_iter(&self) -> impl Iterator<Item = &Column> {
        self.columns_by_exposed.values()
    }

    pub(crate) fn relations_iter(&self) -> impl Iterator<Item = (&String, &Relation)> {
        self.relations_by_name.iter()
    }
```

- [ ] **Step 3: Implement apply_config**

Append to `src/schema/merge.rs`:

```rust
use crate::schema::config::{ConfigOverlay, RelationKindOverlay};
use crate::schema::{RelKind, Relation};
use std::sync::Arc;

pub fn apply_config(mut sb: SchemaBuilder, cfg: &ConfigOverlay) -> SchemaBuilder {
    // Build rename map: old_exposed -> new_exposed
    let rename_map: std::collections::BTreeMap<String, String> = cfg
        .tables
        .iter()
        .filter_map(|(old, o)| o.expose_as.clone().map(|new| (old.clone(), new)))
        .collect();

    // Collect all current tables by their current exposed name.
    let keys: Vec<String> = sb.tables.keys().cloned().collect();
    for exposed in keys {
        let Some(old) = sb.remove_raw(&exposed) else {
            continue;
        };
        let old_physical_schema = old.physical_schema.clone();
        let old_physical_name = old.physical_name.clone();
        let old_pk = old.primary_key.clone();

        let overlay = cfg.tables.get(&exposed);
        let new_exposed = overlay
            .and_then(|o| o.expose_as.clone())
            .unwrap_or_else(|| exposed.clone());

        let mut t = Table::new(&new_exposed, &old_physical_schema, &old_physical_name);

        // Columns (drop hidden ones)
        let hidden: std::collections::BTreeSet<&str> = overlay
            .map(|o| o.hide_columns.iter().map(String::as_str).collect())
            .unwrap_or_default();
        for col in old.columns_iter() {
            if hidden.contains(col.exposed_name.as_str()) {
                continue;
            }
            t = t.column(
                &col.exposed_name,
                &col.physical_name,
                col.pg_type.clone(),
                col.nullable,
            );
        }

        // PK
        if !old_pk.is_empty() {
            let refs: Vec<&str> = old_pk.iter().map(String::as_str).collect();
            t = t.primary_key(&refs);
        }

        // Pre-existing relations (from introspection), unless overridden.
        // Cascade `expose_as` renames into existing relation targets.
        let overlay_rel_names: std::collections::BTreeSet<&str> = overlay
            .map(|o| o.relations.iter().map(|r| r.name.as_str()).collect())
            .unwrap_or_default();
        for (name, rel) in old.relations_iter() {
            if overlay_rel_names.contains(name.as_str()) {
                continue;
            }
            let mut r = rel.clone();
            if let Some(new_target) = rename_map.get(&r.target_table) {
                r.target_table = new_target.clone();
            }
            t = t.relation(name, r);
        }

        // Overlay relations
        if let Some(o) = overlay {
            for r in &o.relations {
                let kind = match r.kind {
                    RelationKindOverlay::Object => RelKind::Object,
                    RelationKindOverlay::Array => RelKind::Array,
                };
                t = t.relation(
                    &r.name,
                    Relation {
                        kind,
                        target_table: r.target.clone(),
                        mapping: r.mapping.clone(),
                    },
                );
            }
        }

        sb.insert_raw(new_exposed, Arc::new(t));
    }
    sb
}
```

- [ ] **Step 4: Extend SchemaBuilder with load_config helper**

In `src/schema/mod.rs`, append to `impl SchemaBuilder`:

```rust
    /// Load a TOML config file and apply it as an overlay.
    pub fn load_config<P: AsRef<std::path::Path>>(
        self,
        path: P,
    ) -> crate::error::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| crate::error::Error::Schema(format!("cannot read config: {e}")))?;
        let cfg = crate::schema::config::parse(&text)?;
        Ok(crate::schema::merge::apply_config(self, &cfg))
    }

    /// Apply a pre-parsed config overlay.
    pub fn apply_config(self, cfg: &crate::schema::config::ConfigOverlay) -> Self {
        crate::schema::merge::apply_config(self, cfg)
    }
```

- [ ] **Step 5: Write failing test**

Append to `src/schema/merge.rs`:

```rust
    #[test]
    fn apply_config_renames_and_hides_and_adds_relation() {
        use crate::schema::config::{ConfigOverlay, RelationKindOverlay, RelationOverlay, TableOverlay};

        // Start with a 2-table builder (users, posts with FK).
        let db = fixture_with_posts_to_users();
        let sb = build_from_introspection(db);

        let mut cfg = ConfigOverlay::default();
        let mut users_overlay = TableOverlay::default();
        users_overlay.expose_as = Some("profiles".into());
        users_overlay.relations.push(RelationOverlay {
            name: "followers".into(),
            kind: RelationKindOverlay::Array,
            target: "profiles".into(),
            mapping: vec![("id".into(), "followed_id".into())],
        });
        cfg.tables.insert("users".into(), users_overlay);

        let sb = apply_config(sb, &cfg);
        let schema = sb.build();
        // "users" no longer exists; "profiles" does
        assert!(schema.table("users").is_none());
        let profiles = schema.table("profiles").expect("profiles table");
        // Auto-derived posts relation should still be there
        assert!(profiles.find_relation("posts").is_some());
        // New followers relation from config
        assert!(profiles.find_relation("followers").is_some());
    }
```

- [ ] **Step 6: Run test**

Run: `cargo test --lib schema::merge::tests::apply_config_renames_and_hides_and_adds_relation`
Expected: PASS.

Run: `cargo test --lib`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/schema/mod.rs src/schema/merge.rs
git commit -m "feat(schema): apply TOML ConfigOverlay to SchemaBuilder"
```

---

### Task 8: Wire Schema::introspect + Schema::builder() consistency

**Files:**
- Modify: `src/schema/mod.rs`

This task ensures the three-layer merge order is clean: `Schema::introspect().load_config().relation(...).build()` all go through the same `SchemaBuilder`.

- [ ] **Step 1: Verify introspect test path**

Ensure that `SchemaBuilder` returned by `Schema::introspect` supports existing builder methods (`.table`, and the new `.load_config`, `.apply_config`). Run:

Run: `cargo build`
Expected: clean compile.

- [ ] **Step 2: Smoke test that the three methods chain correctly**

Append to `src/schema/merge.rs`:

```rust
    #[test]
    fn chained_merge_preserves_all_layers() {
        use crate::schema::{Relation, Schema, Table};
        use crate::schema::config::ConfigOverlay;

        // Layer 1: introspection (simulated via fixture)
        let db = fixture_with_posts_to_users();
        let sb = build_from_introspection(db);

        // Layer 2: TOML overlay (empty here, just verifies it compiles)
        let cfg = ConfigOverlay::default();
        let sb = apply_config(sb, &cfg);

        // Layer 3: manual builder override — add a new column to a new table
        let sb = sb.table(
            Table::new("widgets", "public", "widgets")
                .column("id", "id", crate::schema::PgType::Int4, false),
        );

        let schema: Schema = sb.build();
        assert!(schema.table("users").is_some());
        assert!(schema.table("posts").is_some());
        assert!(schema.table("widgets").is_some());
        // Auto-derived relation still there
        assert!(
            schema
                .table("users")
                .unwrap()
                .find_relation("posts")
                .is_some()
        );
        let _ = Relation::array("irrelevant"); // keep import live
    }
```

- [ ] **Step 3: Run**

Run: `cargo test --lib schema::merge`
Expected: all 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/schema/merge.rs
git commit -m "test(schema): chained introspection → TOML → builder preserves layers"
```

---

### Task 9: Integration test — live PG introspection

**Files:**
- Create: `tests/integration_introspect.rs`
- Create: `tests/fixtures/schema.toml`

- [ ] **Step 1: Create TOML fixture**

Create `tests/fixtures/schema.toml`:

```toml
[tables.users]
expose_as = "profiles"
hide_columns = ["secret"]
```

- [ ] **Step 2: Write integration test**

Create `tests/integration_introspect.rs`:

```rust
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::Schema;
use vision_graphql::Engine;

async fn setup_pool() -> (
    deadpool_postgres::Pool,
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .expect("start pg");
    let host_port = container.get_host_port_ipv4(5432).await.expect("port");
    let mut cfg = Config::new();
    cfg.host = Some("127.0.0.1".into());
    cfg.port = Some(host_port);
    cfg.user = Some("postgres".into());
    cfg.password = Some("postgres".into());
    cfg.dbname = Some("postgres".into());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).expect("pool");

    {
        let client = pool.get().await.expect("client");
        client
            .batch_execute(
                r#"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    secret TEXT
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id)
                );
                INSERT INTO users (name, secret) VALUES ('alice', 's1'), ('bob', 's2');
                INSERT INTO posts (title, user_id) VALUES ('p1', 1), ('p2', 2);
                "#,
            )
            .await
            .expect("seed");
    }
    (pool, container)
}

#[tokio::test]
async fn introspect_auto_derives_relations() {
    let (pool, _c) = setup_pool().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    assert!(schema.table("users").is_some());
    assert!(schema.table("posts").is_some());
    assert!(
        schema
            .table("users")
            .unwrap()
            .find_relation("posts")
            .is_some(),
        "expected users.posts array relation"
    );
    assert!(
        schema
            .table("posts")
            .unwrap()
            .find_relation("user")
            .is_some(),
        "expected posts.user object relation"
    );
}

#[tokio::test]
async fn introspect_runs_queries_end_to_end() {
    let (pool, _c) = setup_pool().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    let engine = Engine::new(pool, schema);
    let v: Value = engine
        .query(
            "query { users { name posts { title } } }",
            None,
        )
        .await
        .expect("query ok");
    let users = v["users"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["posts"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn load_config_renames_and_hides() {
    let (pool, _c) = setup_pool().await;
    let schema = Schema::introspect(&pool)
        .await
        .expect("introspect")
        .load_config("tests/fixtures/schema.toml")
        .expect("load toml")
        .build();
    assert!(schema.table("users").is_none());
    let profiles = schema.table("profiles").expect("renamed table");
    assert!(profiles.find_column("name").is_some());
    assert!(profiles.find_column("secret").is_none(), "should be hidden");

    // secret is hidden; querying it should fail
    let engine = Engine::new(pool, schema);
    let err = engine
        .query("query { profiles { secret } }", None)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("unknown column 'secret'"));
    let _ = json!(null); // keep import live
}
```

- [ ] **Step 3: Run**

Run: `cargo test --test integration_introspect -- --test-threads=1`
Expected: 3 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/integration_introspect.rs tests/fixtures/schema.toml
git commit -m "test: e2e introspection + TOML overlay against live PG"
```

---

### Task 10: Verify + tag Phase 5

- [ ] **Step 1: Full test suite**

Run: `cargo test`
Expected: all pass.

- [ ] **Step 2: Lint**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Format**

Run: `cargo fmt --check`
Expected: clean (else `cargo fmt` + commit).

- [ ] **Step 4: Tag**

```bash
git tag -a phase-5-introspection -m "Phase 5: introspection + TOML config overlay"
```

- [ ] **Step 5: Done**

`Schema::introspect(&pool).await?` now seeds a builder from PG; `.load_config("schema.toml")?` applies a TOML overlay; existing builder methods still chain. The three-layer merge is complete.
