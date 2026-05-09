# CLI Schema Generator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `vision-graphql-cli` crate (binary `vision-gql`) with three subcommands — `generate`, `diff`, `validate` — to bootstrap and check `schema.toml` overlays.

**Architecture:** New workspace member at `crates/vision-graphql-cli/` that depends on the existing library via path. Five pure helpers (`render`, `analyze`, `report`, `filter`, plus URL redaction in `render`) drive three thin async orchestrators. All non-DB logic is unit-testable; DB behavior is covered by testcontainers integration tests using the same pattern as the lib.

**Tech Stack:** Rust 2021, clap (derive), deadpool-postgres + tokio-postgres NoTls, globset, tracing-subscriber, time (formatting), serde_json, insta (snapshots), testcontainers.

Spec reference: `docs/superpowers/specs/2026-05-09-cli-schema-gen-design.md`.

---

## File map

**Created:**
- `crates/vision-graphql-cli/Cargo.toml`
- `crates/vision-graphql-cli/src/main.rs` — clap dispatch, exit-code mapping, tracing setup
- `crates/vision-graphql-cli/src/cmd_generate.rs` — generate orchestrator
- `crates/vision-graphql-cli/src/cmd_diff.rs` — diff orchestrator
- `crates/vision-graphql-cli/src/cmd_validate.rs` — validate orchestrator
- `crates/vision-graphql-cli/src/render.rs` — `HeaderMeta`, `redact_url`, `toml_template`
- `crates/vision-graphql-cli/src/analyze.rs` — `DiffReport`, `find_drift`
- `crates/vision-graphql-cli/src/filter.rs` — `TableFilter`
- `crates/vision-graphql-cli/src/report.rs` — text/json writers for `DiffReport`
- `crates/vision-graphql-cli/src/log_init.rs` — verbosity → tracing level
- `crates/vision-graphql-cli/tests/cli_e2e.rs` — testcontainers-backed E2E tests
- `crates/vision-graphql-cli/tests/fixtures/clean.toml`, `fixtures/stale_hide.toml`, `fixtures/dup_expose.toml` — test data

**Modified:**
- `Cargo.toml` (root) — add `[workspace]`, extend `exclude` with `crates/**`
- `README.md` — new "CLI" section + forward reference

---

## Task 1: Workspace conversion + CLI crate skeleton

**Files:**
- Modify: `Cargo.toml` (root)
- Create: `crates/vision-graphql-cli/Cargo.toml`
- Create: `crates/vision-graphql-cli/src/main.rs`

- [ ] **Step 1: Add workspace section and extend exclude in root `Cargo.toml`**

Edit the root `Cargo.toml`. After the `[package]` table's `exclude` array, change `exclude` to include `"crates/**"`, and add a new `[workspace]` table at the top of the file (above `[package]`).

Final shape (only the changed/added bits shown):

```toml
[workspace]
members = [".", "crates/vision-graphql-cli"]

[package]
name = "vision-graphql"
# ...unchanged...

exclude = [
    "docs/**",
    "tests/**",
    "benches/**",
    ".github/**",
    "*.png",
    "crates/**",
]
```

- [ ] **Step 2: Create the CLI crate manifest**

Create `crates/vision-graphql-cli/Cargo.toml`:

```toml
[package]
name = "vision-graphql-cli"
version = "0.2.0"
edition = "2021"
rust-version = "1.85"
description = "CLI to generate and validate vision-graphql schema overlays."
license = "MIT OR Apache-2.0"
repository = "https://github.com/oonxt/vision-graphql"

[[bin]]
name = "vision-gql"
path = "src/main.rs"

[dependencies]
vision-graphql      = { path = "../..", version = "0.2" }
clap                = { version = "4", features = ["derive"] }
deadpool-postgres   = "0.14"
tokio-postgres      = "0.7"
tokio               = { version = "1", features = ["rt-multi-thread", "macros"] }
toml                = "0.8"
serde               = { version = "1", features = ["derive"] }
serde_json          = "1"
anyhow              = "1"
globset             = "0.4"
tracing             = "0.1"
tracing-subscriber  = "0.3"
time                = { version = "0.3", features = ["formatting"] }

[dev-dependencies]
insta                  = "1"
testcontainers         = "0.23"
testcontainers-modules = { version = "0.11", features = ["postgres"] }
```

- [ ] **Step 3: Create a minimal `main.rs` skeleton that compiles**

Create `crates/vision-graphql-cli/src/main.rs`:

```rust
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "vision-gql", version, about = "vision-graphql schema overlay tool")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Generate a starter schema.toml from a live database.
    Generate,
    /// Validate a schema.toml against a live database.
    Diff,
    /// Validate a schema.toml's structure without connecting to a database.
    Validate,
}

fn main() {
    let _ = Cli::parse();
    eprintln!("not yet implemented");
    std::process::exit(2);
}
```

- [ ] **Step 4: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: both `vision-graphql` and `vision-graphql-cli` compile, no errors.

- [ ] **Step 5: Verify `--help` works**

Run: `cargo run -p vision-graphql-cli -- --help`
Expected: clap prints the three subcommands.

- [ ] **Step 6: Verify lib package excludes the CLI**

Run: `cargo package --list -p vision-graphql 2>&1 | grep -c '^crates/' || echo 0`
Expected output: `0`

- [ ] **Step 7: Verify existing lib tests still pass**

Run: `cargo test -p vision-graphql --lib`
Expected: all tests pass (no regression).

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/vision-graphql-cli
git commit -m "feat(cli): scaffold vision-graphql-cli workspace member"
```

---

## Task 2: `TableFilter` (pure helper, TDD)

**Files:**
- Create: `crates/vision-graphql-cli/src/filter.rs`
- Modify: `crates/vision-graphql-cli/src/main.rs` (add `mod filter;`)

- [ ] **Step 1: Add the module declaration to `main.rs`**

In `crates/vision-graphql-cli/src/main.rs`, add at the top (before `use clap...`):

```rust
mod filter;
```

- [ ] **Step 2: Write the failing tests**

Create `crates/vision-graphql-cli/src/filter.rs`:

```rust
//! Glob-based include/ignore filter for table names.

use anyhow::{Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};

/// Decides which table names should be processed.
///
/// Build with [`TableFilter::new`]. Apply with [`TableFilter::keep`].
pub struct TableFilter {
    include: Option<GlobSet>,
    ignore: Option<GlobSet>,
}

impl TableFilter {
    pub fn new(include: Option<&[String]>, ignore: Option<&[String]>) -> Result<Self> {
        Ok(Self {
            include: compile(include)?,
            ignore: compile(ignore)?,
        })
    }

    pub fn keep(&self, name: &str) -> bool {
        let included = match &self.include {
            Some(set) => set.is_match(name),
            None => true,
        };
        let ignored = matches!(&self.ignore, Some(set) if set.is_match(name));
        included && !ignored
    }
}

fn compile(patterns: Option<&[String]>) -> Result<Option<GlobSet>> {
    let Some(pats) = patterns else {
        return Ok(None);
    };
    if pats.is_empty() {
        return Ok(None);
    }
    let mut b = GlobSetBuilder::new();
    for p in pats {
        let g = Glob::new(p).with_context(|| format!("invalid glob pattern: {p}"))?;
        b.add(g);
    }
    Ok(Some(b.build().context("compiling glob set")?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).into()).collect()
    }

    #[test]
    fn empty_filter_keeps_everything() {
        let f = TableFilter::new(None, None).unwrap();
        assert!(f.keep("users"));
        assert!(f.keep("audit_log"));
    }

    #[test]
    fn include_only_restricts() {
        let f = TableFilter::new(Some(&s(&["users", "post*"])), None).unwrap();
        assert!(f.keep("users"));
        assert!(f.keep("posts"));
        assert!(f.keep("post_tags"));
        assert!(!f.keep("audit"));
    }

    #[test]
    fn ignore_only_excludes() {
        let f = TableFilter::new(None, Some(&s(&["audit_*", "_temp_*"]))).unwrap();
        assert!(f.keep("users"));
        assert!(!f.keep("audit_log"));
        assert!(!f.keep("_temp_x"));
    }

    #[test]
    fn include_then_ignore() {
        let f = TableFilter::new(Some(&s(&["*"])), Some(&s(&["audit_*"]))).unwrap();
        assert!(f.keep("users"));
        assert!(!f.keep("audit_log"));
    }

    #[test]
    fn invalid_glob_fails_construction() {
        let err = TableFilter::new(Some(&s(&["users[unclosed"])), None).unwrap_err();
        assert!(format!("{err:#}").contains("invalid glob pattern"));
    }
}
```

- [ ] **Step 3: Run tests and verify they pass**

Run: `cargo test -p vision-graphql-cli filter`
Expected: 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/vision-graphql-cli/src/filter.rs crates/vision-graphql-cli/src/main.rs
git commit -m "feat(cli): TableFilter with include/ignore globs"
```

---

## Task 3: URL redaction + `HeaderMeta`

**Files:**
- Create: `crates/vision-graphql-cli/src/render.rs`
- Modify: `crates/vision-graphql-cli/src/main.rs` (add `mod render;`)

- [ ] **Step 1: Add module declaration to `main.rs`**

Update `crates/vision-graphql-cli/src/main.rs` to add `mod render;` next to `mod filter;`.

- [ ] **Step 2: Write the failing tests**

Create `crates/vision-graphql-cli/src/render.rs`:

```rust
//! TOML template rendering and helpers (URL redaction, header metadata).

/// Strip the password component of a postgres URL so it is safe to print.
///
/// Returns the original input unchanged when it cannot be parsed as a URL.
pub fn redact_url(raw: &str) -> String {
    let scheme_sep = match raw.find("://") {
        Some(i) => i + 3,
        None => return raw.to_string(),
    };
    let (scheme, rest) = raw.split_at(scheme_sep);
    let (authority, tail) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let (userinfo, hostpart) = match authority.rfind('@') {
        Some(i) => (Some(&authority[..i]), &authority[i + 1..]),
        None => (None, authority),
    };
    match userinfo {
        Some(ui) => {
            let user = ui.split(':').next().unwrap_or("");
            if user.is_empty() {
                format!("{scheme}{hostpart}{tail}")
            } else {
                format!("{scheme}{user}@{hostpart}{tail}")
            }
        }
        None => raw.to_string(),
    }
}

/// Metadata embedded in the header of a generated schema.toml.
pub struct HeaderMeta {
    pub tool_version: String,
    pub timestamp_iso8601: String,
    pub redacted_source_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_password_keeps_user() {
        let s = redact_url("postgres://alice:supersecret@db.example.com:5432/myapp");
        assert_eq!(s, "postgres://alice@db.example.com:5432/myapp");
    }

    #[test]
    fn no_userinfo_passes_through() {
        let s = redact_url("postgres://db.example.com:5432/myapp");
        assert_eq!(s, "postgres://db.example.com:5432/myapp");
    }

    #[test]
    fn user_only_no_password() {
        let s = redact_url("postgres://alice@db.example.com/myapp");
        assert_eq!(s, "postgres://alice@db.example.com/myapp");
    }

    #[test]
    fn unparseable_returned_unchanged() {
        let s = redact_url("not-a-url");
        assert_eq!(s, "not-a-url");
    }

    #[test]
    fn no_path_redacts() {
        let s = redact_url("postgres://alice:pw@host:5432");
        assert_eq!(s, "postgres://alice@host:5432");
    }
}
```

- [ ] **Step 3: Run tests and verify they pass**

Run: `cargo test -p vision-graphql-cli render::`
Expected: 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/vision-graphql-cli/src/render.rs crates/vision-graphql-cli/src/main.rs
git commit -m "feat(cli): URL redaction and HeaderMeta scaffolding"
```

---

## Task 4: `render::toml_template` for the empty / single-table cases

**Files:**
- Modify: `crates/vision-graphql-cli/src/render.rs`

- [ ] **Step 1: Write the failing tests (snapshots)**

Append to `crates/vision-graphql-cli/src/render.rs`:

```rust
use crate::filter::TableFilter;
use vision_graphql::schema::introspect::{
    IntrospectedColumn, IntrospectedDb, IntrospectedForeignKey, IntrospectedTable,
};
use vision_graphql::schema::PgType;

const HEADER_PREFIX: &str = "# Generated by vision-gql";

/// Render a commented starter schema.toml from an introspected database.
///
/// Tables are emitted in alphabetical order. Every line outside of the input
/// content is `#`-prefixed, so the output is a no-op overlay until the user
/// uncomments specific stanzas.
pub fn toml_template(db: &IntrospectedDb, filter: &TableFilter, meta: &HeaderMeta) -> String {
    let mut out = String::new();
    write_header(&mut out, meta);

    let mut emitted_any = false;
    for ((schema, name), table) in &db.tables {
        if schema != "public" {
            continue;
        }
        if !filter.keep(name) {
            continue;
        }
        emitted_any = true;
        out.push('\n');
        write_table_stanza(&mut out, table, db);
    }
    if !emitted_any {
        out.push_str("\n# (no tables matched the filter)\n");
    }
    out
}

fn write_header(out: &mut String, meta: &HeaderMeta) {
    out.push_str(&format!(
        "# Generated by vision-gql {} on {}\n",
        meta.tool_version, meta.timestamp_iso8601
    ));
    out.push_str(&format!("# Source: {}\n", meta.redacted_source_url));
    out.push_str("# Uncomment any stanza below to override defaults from introspection.\n");
}

fn pg_type_short(t: &PgType) -> &'static str {
    match t {
        PgType::Int4 => "int4",
        PgType::Int8 => "int8",
        PgType::Text => "text",
        PgType::Varchar => "varchar",
        PgType::Bool => "bool",
        PgType::Float4 => "float4",
        PgType::Float8 => "float8",
        PgType::Numeric => "numeric",
        PgType::Uuid => "uuid",
        PgType::Timestamp => "timestamp",
        PgType::TimestampTz => "timestamptz",
        PgType::Jsonb => "jsonb",
    }
}

fn write_table_stanza(out: &mut String, t: &IntrospectedTable, _db: &IntrospectedDb) {
    out.push_str(&format!(
        "# ── {}.{} ─────────────────────────────\n",
        t.schema, t.name
    ));
    out.push_str("# columns: ");
    let mut first = true;
    for col in &t.columns {
        if !first {
            out.push_str(", ");
        }
        first = false;
        let pk = t.primary_key.iter().any(|p| p == &col.name);
        let nullable = if col.nullable { "?" } else { "" };
        let pk_marker = if pk { ", PK" } else { "" };
        out.push_str(&format!(
            "{} ({}{}{})",
            col.name,
            pg_type_short(&col.pg_type),
            nullable,
            pk_marker
        ));
    }
    out.push('\n');

    if t.foreign_keys.is_empty() {
        out.push_str("# foreign keys: (none)\n");
    } else {
        out.push_str("# foreign keys:");
        for fk in &t.foreign_keys {
            out.push_str(&format!(
                " {}({}) -> {}.{}({})",
                t.name,
                fk.from_columns.join(", "),
                fk.to_table,
                "",
                fk.to_columns.join(", "),
            ));
        }
        out.push('\n');
    }
    out.push_str("#\n");
    out.push_str(&format!("# [tables.{}]\n", t.name));
    out.push_str(&format!("# expose_as = \"{}\"\n", t.name));
    out.push_str("# hide_columns = []\n");
}

#[cfg(test)]
mod render_tests {
    use super::*;

    fn meta() -> HeaderMeta {
        HeaderMeta {
            tool_version: "0.2.0".into(),
            timestamp_iso8601: "2026-05-09T00:00:00Z".into(),
            redacted_source_url: "postgres://u@h/db".into(),
        }
    }

    #[test]
    fn empty_db_renders_only_header_and_placeholder() {
        let db = IntrospectedDb::default();
        let f = TableFilter::new(None, None).unwrap();
        let out = toml_template(&db, &f, &meta());
        assert!(out.starts_with(HEADER_PREFIX));
        assert!(out.contains("(no tables matched"));
    }

    #[test]
    fn single_table_no_fk_renders_stanza() {
        let mut db = IntrospectedDb::default();
        db.tables.insert(
            ("public".into(), "users".into()),
            IntrospectedTable {
                schema: "public".into(),
                name: "users".into(),
                columns: vec![
                    IntrospectedColumn { name: "id".into(),    pg_type: PgType::Int4, nullable: false },
                    IntrospectedColumn { name: "email".into(), pg_type: PgType::Text, nullable: true  },
                ],
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                foreign_keys: vec![],
            },
        );
        let f = TableFilter::new(None, None).unwrap();
        let out = toml_template(&db, &f, &meta());
        assert!(out.contains("# ── public.users ─"));
        assert!(out.contains("id (int4, PK)"));
        assert!(out.contains("email (text?)"));
        assert!(out.contains("# foreign keys: (none)"));
        assert!(out.contains("# [tables.users]"));
    }

    #[test]
    fn filter_excludes_non_matching_tables() {
        let mut db = IntrospectedDb::default();
        for n in ["users", "audit_log"] {
            db.tables.insert(
                ("public".into(), n.into()),
                IntrospectedTable {
                    schema: "public".into(),
                    name: n.into(),
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
        }
        let ignore = vec!["audit_*".to_string()];
        let f = TableFilter::new(None, Some(&ignore)).unwrap();
        let out = toml_template(&db, &f, &meta());
        assert!(out.contains("# ── public.users ─"));
        assert!(!out.contains("audit_log"));
    }
}
```

Note: the dummy `""` placeholder in the FK formatting (`fk.to_table, "", fk.to_columns...`) is a leftover that we'll fix in Task 5; for now the code compiles and the empty/single-table tests don't exercise FKs.

- [ ] **Step 2: Run tests and verify they pass**

Run: `cargo test -p vision-graphql-cli render`
Expected: all tests in `render_tests` pass plus the 5 from Task 3.

- [ ] **Step 3: Commit**

```bash
git add crates/vision-graphql-cli/src/render.rs
git commit -m "feat(cli): toml_template renders header and basic table stanzas"
```

---

## Task 5: `render::toml_template` with FK-derived relation stanzas

**Files:**
- Modify: `crates/vision-graphql-cli/src/render.rs`

- [ ] **Step 1: Write the failing test for the relation stanza**

Append a new test inside the existing `render_tests` module in `render.rs`:

```rust
#[test]
fn fk_emits_relation_stanzas_on_both_sides() {
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
                IntrospectedColumn { name: "id".into(),      pg_type: PgType::Int4, nullable: false },
                IntrospectedColumn { name: "user_id".into(), pg_type: PgType::Int4, nullable: false },
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
    let f = TableFilter::new(None, None).unwrap();
    let out = toml_template(&db, &f, &meta());
    // Object relation on `posts` side
    assert!(out.contains("# [[tables.posts.relations]]"));
    assert!(out.contains("# kind = \"object\""));
    assert!(out.contains("# target = \"users\""));
    // Array relation on `users` side
    assert!(out.contains("# [[tables.users.relations]]"));
    assert!(out.contains("# kind = \"array\""));
    assert!(out.contains("# target = \"posts\""));
    // FK summary line on posts
    assert!(out.contains("posts(user_id) -> users(id)"));
}
```

- [ ] **Step 2: Run test, verify it fails (no relation output yet)**

Run: `cargo test -p vision-graphql-cli render::render_tests::fk_emits_relation_stanzas_on_both_sides`
Expected: FAIL (assertions on `[[tables.posts.relations]]` and `kind = "object"` find nothing).

- [ ] **Step 3: Implement relation rendering**

Replace `write_table_stanza` in `render.rs` with:

```rust
fn write_table_stanza(out: &mut String, t: &IntrospectedTable, db: &IntrospectedDb) {
    out.push_str(&format!(
        "# ── {}.{} ─────────────────────────────\n",
        t.schema, t.name
    ));
    out.push_str("# columns: ");
    let mut first = true;
    for col in &t.columns {
        if !first {
            out.push_str(", ");
        }
        first = false;
        let pk = t.primary_key.iter().any(|p| p == &col.name);
        let nullable = if col.nullable { "?" } else { "" };
        let pk_marker = if pk { ", PK" } else { "" };
        out.push_str(&format!(
            "{} ({}{}{})",
            col.name,
            pg_type_short(&col.pg_type),
            nullable,
            pk_marker
        ));
    }
    out.push('\n');

    if t.foreign_keys.is_empty() {
        out.push_str("# foreign keys: (none)\n");
    } else {
        for fk in &t.foreign_keys {
            out.push_str(&format!(
                "# foreign keys: {}({}) -> {}({})\n",
                t.name,
                fk.from_columns.join(", "),
                fk.to_table,
                fk.to_columns.join(", "),
            ));
        }
    }
    out.push_str("#\n");
    out.push_str(&format!("# [tables.{}]\n", t.name));
    out.push_str(&format!("# expose_as = \"{}\"\n", t.name));
    out.push_str("# hide_columns = []\n");

    let derived = vision_graphql::schema::merge::derive_relations_from_fks(db);
    let mine: Vec<_> = derived.iter().filter(|(src, _, _)| src == &t.name).collect();
    for (_, rel_name, rel) in mine {
        let kind = match rel.kind {
            vision_graphql::schema::RelKind::Object => "object",
            vision_graphql::schema::RelKind::Array => "array",
        };
        out.push_str("#\n");
        out.push_str(&format!(
            "# # {} relation derived from FK\n",
            kind
        ));
        out.push_str(&format!("# [[tables.{}.relations]]\n", t.name));
        out.push_str(&format!("# name = \"{}\"\n", rel_name));
        out.push_str(&format!("# kind = \"{}\"\n", kind));
        out.push_str(&format!("# target = \"{}\"\n", rel.target_table));
        out.push_str("# mapping = [");
        let mut first = true;
        for (a, b) in &rel.mapping {
            if !first {
                out.push_str(", ");
            }
            first = false;
            out.push_str(&format!("[\"{}\", \"{}\"]", a, b));
        }
        out.push_str("]\n");
    }
}
```

- [ ] **Step 4: Verify `derive_relations_from_fks` is reachable from the CLI crate**

Run: `cargo build -p vision-graphql-cli`

Expected outcome A: builds cleanly. Skip to Step 5.

Expected outcome B: error `function 'derive_relations_from_fks' is private` or `module 'merge' is private`. In that case, modify `vision-graphql/src/schema/mod.rs` line `pub mod merge;` (it should already be `pub`; check). If `merge` is `pub mod`, then `pub fn derive_relations_from_fks` should be reachable. If not, in `vision-graphql/src/schema/merge.rs` change `pub fn derive_relations_from_fks` (it is already `pub`). Then re-run `cargo build -p vision-graphql-cli`.

- [ ] **Step 5: Run the test, verify it passes**

Run: `cargo test -p vision-graphql-cli render`
Expected: all render tests pass (including the new FK case).

- [ ] **Step 6: Commit**

```bash
git add crates/vision-graphql-cli/src/render.rs
git commit -m "feat(cli): emit FK-derived relation stanzas in template"
```

---

## Task 6: `analyze::find_drift` and `DiffReport`

**Files:**
- Create: `crates/vision-graphql-cli/src/analyze.rs`
- Modify: `crates/vision-graphql-cli/src/main.rs` (add `mod analyze;`)

- [ ] **Step 1: Add module declaration to `main.rs`**

In `main.rs`, alongside other `mod ...;` lines, add `mod analyze;`.

- [ ] **Step 2: Write failing tests + implementation skeleton**

Create `crates/vision-graphql-cli/src/analyze.rs`:

```rust
//! Validate a parsed ConfigOverlay against an introspected database.

use crate::filter::TableFilter;
use serde::Serialize;
use std::collections::BTreeMap;
use vision_graphql::schema::config::ConfigOverlay;
use vision_graphql::schema::introspect::{IntrospectedDb, IntrospectedTable};

#[derive(Debug, Serialize, Default)]
pub struct DiffReport {
    pub missing_tables: Vec<String>,
    pub missing_columns: Vec<MissingColumn>,
    pub missing_relation_targets: Vec<MissingRelTarget>,
    pub expose_as_collisions: Vec<Collision>,
}

#[derive(Debug, Serialize)]
pub struct MissingColumn {
    pub table: String,
    pub column: String,
    pub origin: ColumnOrigin,
}

#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ColumnOrigin {
    HideColumns,
    RelationLocal,
    RelationRemote,
}

#[derive(Debug, Serialize)]
pub struct MissingRelTarget {
    pub table: String,
    pub relation: String,
    pub target: String,
}

#[derive(Debug, Serialize)]
pub struct Collision {
    pub exposed_name: String,
    pub sources: Vec<String>,
}

impl DiffReport {
    pub fn is_clean(&self) -> bool {
        self.missing_tables.is_empty()
            && self.missing_columns.is_empty()
            && self.missing_relation_targets.is_empty()
            && self.expose_as_collisions.is_empty()
    }

    pub fn issue_count(&self) -> usize {
        self.missing_tables.len()
            + self.missing_columns.len()
            + self.missing_relation_targets.len()
            + self.expose_as_collisions.len()
    }
}

pub fn find_drift(cfg: &ConfigOverlay, db: &IntrospectedDb, filter: &TableFilter) -> DiffReport {
    let mut report = DiffReport::default();

    // Index physical tables for quick lookup; only `public` is in scope.
    let by_name: BTreeMap<&str, &IntrospectedTable> = db
        .tables
        .iter()
        .filter(|((schema, _), _)| schema == "public")
        .map(|((_, name), t)| (name.as_str(), t))
        .collect();

    // expose_as collisions: track all exposed names.
    let mut exposed_owners: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for name in by_name.keys() {
        exposed_owners
            .entry((*name).to_string())
            .or_default()
            .push((*name).to_string());
    }
    for (key, overlay) in &cfg.tables {
        if !filter.keep(key) {
            continue;
        }
        if let Some(new) = &overlay.expose_as {
            // Replace the original physical name's claim with the renamed one.
            exposed_owners.entry(key.clone()).and_modify(|v| {
                v.retain(|s| s != key);
            });
            exposed_owners
                .entry(new.clone())
                .or_default()
                .push(key.clone());
        }
    }
    for (exposed, sources) in &exposed_owners {
        if sources.len() > 1 {
            report.expose_as_collisions.push(Collision {
                exposed_name: exposed.clone(),
                sources: sources.clone(),
            });
        }
    }

    // Per-overlay-table checks.
    for (key, overlay) in &cfg.tables {
        if !filter.keep(key) {
            continue;
        }
        let Some(table) = by_name.get(key.as_str()) else {
            report.missing_tables.push(key.clone());
            continue;
        };
        let col_set: std::collections::BTreeSet<&str> =
            table.columns.iter().map(|c| c.name.as_str()).collect();
        for hidden in &overlay.hide_columns {
            if !col_set.contains(hidden.as_str()) {
                report.missing_columns.push(MissingColumn {
                    table: key.clone(),
                    column: hidden.clone(),
                    origin: ColumnOrigin::HideColumns,
                });
            }
        }
        for rel in &overlay.relations {
            // Resolve target: physical table name OR another overlay's expose_as.
            let target_physical = if by_name.contains_key(rel.target.as_str()) {
                Some(rel.target.clone())
            } else {
                cfg.tables
                    .iter()
                    .find(|(_, o)| o.expose_as.as_deref() == Some(rel.target.as_str()))
                    .map(|(k, _)| k.clone())
            };
            let Some(target_phys) = target_physical else {
                report.missing_relation_targets.push(MissingRelTarget {
                    table: key.clone(),
                    relation: rel.name.clone(),
                    target: rel.target.clone(),
                });
                continue;
            };
            for (local, remote) in &rel.mapping {
                if !col_set.contains(local.as_str()) {
                    report.missing_columns.push(MissingColumn {
                        table: key.clone(),
                        column: local.clone(),
                        origin: ColumnOrigin::RelationLocal,
                    });
                }
                if let Some(target_table) = by_name.get(target_phys.as_str()) {
                    let remote_set: std::collections::BTreeSet<&str> =
                        target_table.columns.iter().map(|c| c.name.as_str()).collect();
                    if !remote_set.contains(remote.as_str()) {
                        report.missing_columns.push(MissingColumn {
                            table: key.clone(),
                            column: remote.clone(),
                            origin: ColumnOrigin::RelationRemote,
                        });
                    }
                }
            }
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use vision_graphql::schema::config::{
        RelationKindOverlay, RelationOverlay, TableOverlay,
    };
    use vision_graphql::schema::introspect::{IntrospectedColumn, IntrospectedTable};
    use vision_graphql::schema::PgType;

    fn db_users_only() -> IntrospectedDb {
        let mut db = IntrospectedDb::default();
        db.tables.insert(
            ("public".into(), "users".into()),
            IntrospectedTable {
                schema: "public".into(),
                name: "users".into(),
                columns: vec![
                    IntrospectedColumn { name: "id".into(),    pg_type: PgType::Int4, nullable: false },
                    IntrospectedColumn { name: "email".into(), pg_type: PgType::Text, nullable: true  },
                ],
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                foreign_keys: vec![],
            },
        );
        db
    }

    fn no_filter() -> TableFilter {
        TableFilter::new(None, None).unwrap()
    }

    #[test]
    fn clean_overlay_against_clean_db() {
        let db = db_users_only();
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "users".into(),
            TableOverlay {
                expose_as: Some("profiles".into()),
                hide_columns: vec!["email".into()],
                relations: vec![],
            },
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert!(r.is_clean(), "expected clean, got {:?}", r);
    }

    #[test]
    fn missing_table_reported() {
        let db = db_users_only();
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "ghosts".into(),
            TableOverlay::default(),
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert_eq!(r.missing_tables, vec!["ghosts".to_string()]);
    }

    #[test]
    fn stale_hide_column_reported() {
        let db = db_users_only();
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "users".into(),
            TableOverlay {
                expose_as: None,
                hide_columns: vec!["password_hash".into()],
                relations: vec![],
            },
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert_eq!(r.missing_columns.len(), 1);
        assert_eq!(r.missing_columns[0].column, "password_hash");
        assert!(matches!(r.missing_columns[0].origin, ColumnOrigin::HideColumns));
    }

    #[test]
    fn missing_relation_target_reported() {
        let db = db_users_only();
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "users".into(),
            TableOverlay {
                expose_as: None,
                hide_columns: vec![],
                relations: vec![RelationOverlay {
                    name: "ghosts".into(),
                    kind: RelationKindOverlay::Array,
                    target: "ghost_table".into(),
                    mapping: vec![("id".into(), "user_id".into())],
                }],
            },
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert_eq!(r.missing_relation_targets.len(), 1);
        assert_eq!(r.missing_relation_targets[0].target, "ghost_table");
    }

    #[test]
    fn expose_as_collision_reported() {
        let mut db = db_users_only();
        db.tables.insert(
            ("public".into(), "profiles".into()),
            IntrospectedTable {
                schema: "public".into(),
                name: "profiles".into(),
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
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "users".into(),
            TableOverlay {
                expose_as: Some("profiles".into()),
                ..Default::default()
            },
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert_eq!(r.expose_as_collisions.len(), 1);
        assert_eq!(r.expose_as_collisions[0].exposed_name, "profiles");
    }

    #[test]
    fn filter_skips_overlay_entries() {
        let db = db_users_only();
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert("ghosts".into(), TableOverlay::default());
        let ignore = vec!["ghosts".to_string()];
        let f = TableFilter::new(None, Some(&ignore)).unwrap();
        let r = find_drift(&cfg, &db, &f);
        assert!(r.is_clean(), "ignored entry should not surface");
    }
}
```

- [ ] **Step 3: Run tests and verify they pass**

Run: `cargo test -p vision-graphql-cli analyze`
Expected: 6 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/vision-graphql-cli/src/analyze.rs crates/vision-graphql-cli/src/main.rs
git commit -m "feat(cli): analyze::find_drift overlay-vs-DB checks"
```

---

## Task 7: `report` text and JSON writers

**Files:**
- Create: `crates/vision-graphql-cli/src/report.rs`
- Modify: `crates/vision-graphql-cli/src/main.rs` (add `mod report;`)

- [ ] **Step 1: Add module declaration to `main.rs`**

Add `mod report;` to the module list in `main.rs`.

- [ ] **Step 2: Write failing tests + implementation**

Create `crates/vision-graphql-cli/src/report.rs`:

```rust
//! Format a DiffReport for human or machine consumption.

use crate::analyze::{ColumnOrigin, DiffReport};
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub enum Format {
    Text,
    Json,
}

pub fn write<W: Write>(report: &DiffReport, format: Format, out: &mut W) -> std::io::Result<()> {
    match format {
        Format::Text => write_text(report, out),
        Format::Json => write_json(report, out),
    }
}

fn write_text<W: Write>(report: &DiffReport, out: &mut W) -> std::io::Result<()> {
    if report.is_clean() {
        writeln!(out, "OK: no overlay drift detected")?;
        return Ok(());
    }
    if !report.missing_tables.is_empty() {
        writeln!(out, "missing tables (overlay references nonexistent table):")?;
        for t in &report.missing_tables {
            writeln!(out, "  - {t}")?;
        }
    }
    if !report.missing_columns.is_empty() {
        writeln!(out, "missing columns:")?;
        for c in &report.missing_columns {
            let origin = match c.origin {
                ColumnOrigin::HideColumns => "hide_columns",
                ColumnOrigin::RelationLocal => "relation.mapping local",
                ColumnOrigin::RelationRemote => "relation.mapping remote",
            };
            writeln!(out, "  - {}.{} (from {})", c.table, c.column, origin)?;
        }
    }
    if !report.missing_relation_targets.is_empty() {
        writeln!(out, "missing relation targets:")?;
        for r in &report.missing_relation_targets {
            writeln!(out, "  - {}.{}: target = {}", r.table, r.relation, r.target)?;
        }
    }
    if !report.expose_as_collisions.is_empty() {
        writeln!(out, "expose_as collisions:")?;
        for c in &report.expose_as_collisions {
            writeln!(
                out,
                "  - {} <- {}",
                c.exposed_name,
                c.sources.join(", ")
            )?;
        }
    }
    writeln!(out, "{} issues found", report.issue_count())?;
    Ok(())
}

fn write_json<W: Write>(report: &DiffReport, out: &mut W) -> std::io::Result<()> {
    serde_json::to_writer_pretty(&mut *out, report)?;
    out.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyze::{Collision, ColumnOrigin, DiffReport, MissingColumn, MissingRelTarget};

    fn dirty_report() -> DiffReport {
        DiffReport {
            missing_tables: vec!["ghosts".into()],
            missing_columns: vec![MissingColumn {
                table: "users".into(),
                column: "password_hash".into(),
                origin: ColumnOrigin::HideColumns,
            }],
            missing_relation_targets: vec![MissingRelTarget {
                table: "users".into(),
                relation: "owner".into(),
                target: "people".into(),
            }],
            expose_as_collisions: vec![Collision {
                exposed_name: "profiles".into(),
                sources: vec!["users".into(), "profiles".into()],
            }],
        }
    }

    #[test]
    fn clean_text_reports_ok() {
        let mut buf = Vec::new();
        write(&DiffReport::default(), Format::Text, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("OK"));
    }

    #[test]
    fn dirty_text_lists_each_issue() {
        let mut buf = Vec::new();
        write(&dirty_report(), Format::Text, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("missing tables"));
        assert!(s.contains("ghosts"));
        assert!(s.contains("password_hash"));
        assert!(s.contains("owner"));
        assert!(s.contains("profiles"));
        assert!(s.contains("4 issues found"));
    }

    #[test]
    fn json_round_trips() {
        let mut buf = Vec::new();
        write(&dirty_report(), Format::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["missing_tables"][0], "ghosts");
        assert_eq!(v["missing_columns"][0]["origin"], "hide_columns");
    }
}
```

- [ ] **Step 3: Run tests and verify they pass**

Run: `cargo test -p vision-graphql-cli report`
Expected: 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/vision-graphql-cli/src/report.rs crates/vision-graphql-cli/src/main.rs
git commit -m "feat(cli): text and JSON DiffReport writers"
```

---

## Task 8: Verbosity → tracing level helper

**Files:**
- Create: `crates/vision-graphql-cli/src/log_init.rs`
- Modify: `crates/vision-graphql-cli/src/main.rs` (add `mod log_init;`)

- [ ] **Step 1: Add module declaration to `main.rs`**

Add `mod log_init;` to the module list.

- [ ] **Step 2: Write tests + implementation**

Create `crates/vision-graphql-cli/src/log_init.rs`:

```rust
//! Configure tracing-subscriber from -v/-q flags.

use tracing::Level;

pub fn level_from_flags(verbose: u8, quiet: bool) -> Option<Level> {
    if quiet {
        None
    } else {
        match verbose {
            0 => Some(Level::WARN),
            1 => Some(Level::DEBUG),
            _ => Some(Level::TRACE),
        }
    }
}

pub fn install(verbose: u8, quiet: bool) {
    let Some(level) = level_from_flags(verbose, quiet) else {
        return;
    };
    let _ = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_overrides_verbose() {
        assert!(level_from_flags(2, true).is_none());
    }

    #[test]
    fn defaults_to_warn() {
        assert_eq!(level_from_flags(0, false), Some(Level::WARN));
    }

    #[test]
    fn one_v_is_debug() {
        assert_eq!(level_from_flags(1, false), Some(Level::DEBUG));
    }

    #[test]
    fn two_v_is_trace() {
        assert_eq!(level_from_flags(2, false), Some(Level::TRACE));
        assert_eq!(level_from_flags(5, false), Some(Level::TRACE));
    }
}
```

- [ ] **Step 3: Run tests and verify they pass**

Run: `cargo test -p vision-graphql-cli log_init`
Expected: 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/vision-graphql-cli/src/log_init.rs crates/vision-graphql-cli/src/main.rs
git commit -m "feat(cli): -v/-q to tracing level mapping"
```

---

## Task 9: `cmd_validate` orchestrator (offline only)

**Files:**
- Create: `crates/vision-graphql-cli/src/cmd_validate.rs`
- Modify: `crates/vision-graphql-cli/src/main.rs`

- [ ] **Step 1: Add module declaration**

Add `mod cmd_validate;` to `main.rs`.

- [ ] **Step 2: Write tests + implementation**

Create `crates/vision-graphql-cli/src/cmd_validate.rs`:

```rust
//! Offline structural validation of a schema.toml.

use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;
use std::path::Path;
use vision_graphql::schema::config::parse;

pub fn run(path: &Path) -> Result<()> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    let cfg = parse(&text)
        .with_context(|| format!("parsing {}", path.display()))?;

    let mut issues: Vec<String> = Vec::new();

    // Within-overlay expose_as collisions.
    let mut counts: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (key, overlay) in &cfg.tables {
        if let Some(new) = &overlay.expose_as {
            counts.entry(new.as_str()).or_default().push(key.as_str());
        }
    }
    for (exposed, sources) in &counts {
        if sources.len() > 1 {
            issues.push(format!(
                "expose_as collision: {} <- [{}]",
                exposed,
                sources.join(", ")
            ));
        }
    }

    // Empty mappings + structural sanity.
    for (key, overlay) in &cfg.tables {
        for rel in &overlay.relations {
            if rel.mapping.is_empty() {
                issues.push(format!(
                    "{}.{}: relation mapping must be non-empty",
                    key, rel.name
                ));
            }
        }
    }

    if issues.is_empty() {
        println!("OK: {}", path.display());
        Ok(())
    } else {
        for i in &issues {
            eprintln!("{i}");
        }
        bail!("{} structural issues found", issues.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_file(contents: &str) -> tempfile_polyfill::PathHolder {
        tempfile_polyfill::write_temp(contents)
    }

    #[test]
    fn rejects_unknown_field() {
        let f = temp_file(
            r#"
            [tables.users]
            unknown_field = 1
        "#,
        );
        let err = run(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("parsing"));
    }

    #[test]
    fn rejects_duplicate_expose_as() {
        let f = temp_file(
            r#"
            [tables.users]
            expose_as = "people"

            [tables.profiles]
            expose_as = "people"
        "#,
        );
        let err = run(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("structural issues"));
    }

    #[test]
    fn rejects_empty_mapping() {
        let f = temp_file(
            r#"
            [[tables.users.relations]]
            name = "x"
            kind = "array"
            target = "users"
            mapping = []
        "#,
        );
        let err = run(f.path()).unwrap_err();
        assert!(format!("{err:#}").contains("structural issues"));
    }

    #[test]
    fn accepts_clean_overlay() {
        let f = temp_file(
            r#"
            [tables.users]
            expose_as = "profiles"
            hide_columns = ["secret"]

            [[tables.users.relations]]
            name = "followers"
            kind = "array"
            target = "users"
            mapping = [["id", "followed_id"]]
        "#,
        );
        run(f.path()).unwrap();
    }

    /// Tiny tempfile shim so we don't depend on the `tempfile` crate.
    mod tempfile_polyfill {
        use std::io::Write;
        use std::path::{Path, PathBuf};

        pub struct PathHolder {
            pub path: PathBuf,
        }
        impl PathHolder {
            pub fn path(&self) -> &Path {
                &self.path
            }
        }
        impl Drop for PathHolder {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.path);
            }
        }

        pub fn write_temp(contents: &str) -> PathHolder {
            let mut p = std::env::temp_dir();
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            p.push(format!("vision-gql-test-{nanos}-{}.toml", std::process::id()));
            let mut f = std::fs::File::create(&p).expect("temp file");
            f.write_all(contents.as_bytes()).expect("write");
            PathHolder { path: p }
        }
    }
}
```

- [ ] **Step 3: Run tests and verify they pass**

Run: `cargo test -p vision-graphql-cli cmd_validate`
Expected: 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/vision-graphql-cli/src/cmd_validate.rs crates/vision-graphql-cli/src/main.rs
git commit -m "feat(cli): cmd_validate offline TOML structure check"
```

---

## Task 10: `cmd_generate` orchestrator + clap wiring

**Files:**
- Create: `crates/vision-graphql-cli/src/cmd_generate.rs`
- Modify: `crates/vision-graphql-cli/src/main.rs`

- [ ] **Step 1: Add module declaration**

Add `mod cmd_generate;` to `main.rs`.

- [ ] **Step 2: Implement `cmd_generate::run`**

Create `crates/vision-graphql-cli/src/cmd_generate.rs`:

```rust
//! Generate a starter schema.toml from a live database.

use anyhow::{bail, Context, Result};
use deadpool_postgres::{Config, Runtime};
use std::path::{Path, PathBuf};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio_postgres::NoTls;
use vision_graphql::schema::introspect::introspect;

use crate::filter::TableFilter;
use crate::render::{redact_url, toml_template, HeaderMeta};

pub struct Args {
    pub url: String,
    pub output: String,
    pub force: bool,
    pub include: Option<Vec<String>>,
    pub ignore: Option<Vec<String>>,
}

pub async fn run(args: Args) -> Result<()> {
    let output_target = if args.output == "-" {
        OutputTarget::Stdout
    } else {
        OutputTarget::File(PathBuf::from(&args.output))
    };

    if let OutputTarget::File(p) = &output_target {
        if p.exists() && !args.force {
            bail!(
                "refusing to overwrite {} without --force",
                p.display()
            );
        }
    }

    let pool = build_pool(&args.url)?;
    let db = introspect(&pool)
        .await
        .with_context(|| format!("introspect failed against {}", redact_url(&args.url)))?;

    let filter = TableFilter::new(args.include.as_deref(), args.ignore.as_deref())?;
    let meta = HeaderMeta {
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp_iso8601: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown".into()),
        redacted_source_url: redact_url(&args.url),
    };

    let body = toml_template(&db, &filter, &meta);

    match output_target {
        OutputTarget::Stdout => {
            print!("{body}");
        }
        OutputTarget::File(p) => {
            std::fs::write(&p, body.as_bytes())
                .with_context(|| format!("writing {}", p.display()))?;
        }
    }
    Ok(())
}

enum OutputTarget {
    Stdout,
    File(PathBuf),
}

fn build_pool(url: &str) -> Result<deadpool_postgres::Pool> {
    let cfg: tokio_postgres::Config = url
        .parse()
        .with_context(|| format!("parsing connection URL {}", redact_url(url)))?;
    let mut dp = Config::new();
    dp.host = cfg
        .get_hosts()
        .iter()
        .find_map(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => Some(s.clone()),
            _ => None,
        });
    dp.port = cfg.get_ports().first().copied();
    dp.user = cfg.get_user().map(str::to_string);
    dp.password = cfg
        .get_password()
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(str::to_string);
    dp.dbname = cfg.get_dbname().map(str::to_string);
    dp.create_pool(Some(Runtime::Tokio1), NoTls)
        .context("creating connection pool")
}

fn _path_unused(_: &Path) {} // silence unused-import warning if Path is otherwise unused
```

Note: the `_path_unused` shim keeps `Path` referenced; remove when adding more code that uses it.

- [ ] **Step 3: Wire up clap subcommands and dispatch in `main.rs`**

Replace `crates/vision-graphql-cli/src/main.rs` entirely with:

```rust
mod analyze;
mod cmd_generate;
mod cmd_validate;
mod filter;
mod log_init;
mod render;
mod report;

use anyhow::{Context, Result};
use clap::{ArgAction, Args as ClapArgs, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "vision-gql",
    version,
    about = "vision-graphql schema overlay tool"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,

    #[arg(short = 'v', long = "verbose", action = ArgAction::Count, global = true)]
    verbose: u8,

    #[arg(short = 'q', long = "quiet", global = true, conflicts_with = "verbose")]
    quiet: bool,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Generate a starter schema.toml from a live database.
    Generate(GenerateArgs),
    /// Validate a schema.toml against a live database.
    Diff(DiffArgs),
    /// Validate a schema.toml's structure without connecting to a database.
    Validate(ValidateArgs),
}

#[derive(ClapArgs, Debug)]
struct CommonDb {
    /// Postgres connection URL. Falls back to $DATABASE_URL if not set.
    #[arg(long)]
    url: Option<String>,

    /// Comma-separated globs; restrict to matching tables.
    #[arg(long, value_delimiter = ',')]
    include_tables: Option<Vec<String>>,

    /// Comma-separated globs; exclude matching tables.
    #[arg(long, value_delimiter = ',')]
    ignore_tables: Option<Vec<String>>,
}

#[derive(ClapArgs, Debug)]
struct GenerateArgs {
    #[command(flatten)]
    db: CommonDb,

    /// Output path; "-" for stdout (default).
    #[arg(short = 'o', long = "output", default_value = "-")]
    output: String,

    /// Overwrite an existing output file.
    #[arg(short = 'f', long = "force")]
    force: bool,
}

#[derive(ClapArgs, Debug)]
struct DiffArgs {
    #[command(flatten)]
    db: CommonDb,

    /// Path to the overlay TOML file.
    #[arg(long, default_value = "./schema.toml")]
    config: PathBuf,

    /// Output format.
    #[arg(long, default_value = "text", value_parser = ["text", "json"])]
    format: String,
}

#[derive(ClapArgs, Debug)]
struct ValidateArgs {
    /// Path to the overlay TOML file.
    path: PathBuf,
}

fn resolve_url(opt: Option<String>) -> Result<String> {
    opt.or_else(|| std::env::var("DATABASE_URL").ok())
        .context("no --url given and DATABASE_URL is not set")
}

fn main() {
    let cli = Cli::parse();
    log_init::install(cli.verbose, cli.quiet);
    let exit = match dispatch(cli) {
        Ok(()) => 0,
        Err(e) => {
            if e.downcast_ref::<DriftDetected>().is_some() {
                1
            } else {
                eprintln!("error: {e:#}");
                2
            }
        }
    };
    std::process::exit(exit);
}

fn dispatch(cli: Cli) -> Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting tokio runtime")?;
    rt.block_on(async {
        match cli.cmd {
            Cmd::Generate(a) => {
                let url = resolve_url(a.db.url)?;
                cmd_generate::run(cmd_generate::Args {
                    url,
                    output: a.output,
                    force: a.force,
                    include: a.db.include_tables,
                    ignore: a.db.ignore_tables,
                })
                .await
            }
            Cmd::Diff(_a) => {
                anyhow::bail!("diff not yet implemented")
            }
            Cmd::Validate(a) => cmd_validate::run(&a.path),
        }
    })
}

#[derive(Debug)]
pub struct DriftDetected;
impl std::fmt::Display for DriftDetected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "drift detected")
    }
}
impl std::error::Error for DriftDetected {}
```

- [ ] **Step 4: Verify the workspace still compiles**

Run: `cargo build -p vision-graphql-cli`
Expected: clean build.

- [ ] **Step 5: Add E2E test for `generate`**

Create `crates/vision-graphql-cli/tests/cli_e2e.rs`:

```rust
use std::process::Command;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};

async fn boot_pg() -> (
    String, // DATABASE_URL
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .expect("start pg");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    // Seed a tiny schema.
    let cfg: tokio_postgres::Config = url.parse().unwrap();
    let (client, conn) = cfg.connect(tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(async move { let _ = conn.await; });
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
            CREATE TABLE audit_log (
                id SERIAL PRIMARY KEY,
                msg TEXT NOT NULL
            );
            "#,
        )
        .await
        .expect("seed");

    (url, container)
}

#[tokio::test(flavor = "multi_thread")]
async fn generate_to_stdout_includes_all_tables() {
    let (url, _c) = boot_pg().await;
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args(["generate", "--url", &url])
        .output()
        .expect("run cli");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(s.contains("# ── public.users ─"));
    assert!(s.contains("# ── public.posts ─"));
    assert!(s.contains("# ── public.audit_log ─"));
}

#[tokio::test(flavor = "multi_thread")]
async fn generate_ignore_tables_filters() {
    let (url, _c) = boot_pg().await;
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args(["generate", "--url", &url, "--ignore-tables", "audit_*"])
        .output()
        .expect("run cli");
    assert!(out.status.success());
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(s.contains("public.users"));
    assert!(!s.contains("audit_log"));
}

#[tokio::test(flavor = "multi_thread")]
async fn generate_force_required_to_overwrite() {
    let (url, _c) = boot_pg().await;
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let dir = std::env::temp_dir().join(format!("vision-gql-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("schema.toml");

    let out = Command::new(bin)
        .args(["generate", "--url", &url, "-o", path.to_str().unwrap()])
        .output()
        .expect("run cli");
    assert!(out.status.success(), "first write must succeed");

    let out = Command::new(bin)
        .args(["generate", "--url", &url, "-o", path.to_str().unwrap()])
        .output()
        .expect("run cli");
    assert_eq!(out.status.code(), Some(2), "second write without --force must exit 2");

    let out = Command::new(bin)
        .args(["generate", "--url", &url, "-o", path.to_str().unwrap(), "--force"])
        .output()
        .expect("run cli");
    assert!(out.status.success(), "with --force must succeed");

    let _ = std::fs::remove_dir_all(&dir);
}
```

- [ ] **Step 6: Run the E2E tests and verify they pass**

Run: `cargo test -p vision-graphql-cli --test cli_e2e -- --nocapture`
Expected: 3 tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/vision-graphql-cli/src/cmd_generate.rs \
        crates/vision-graphql-cli/src/main.rs \
        crates/vision-graphql-cli/tests/cli_e2e.rs
git commit -m "feat(cli): generate subcommand wired end-to-end"
```

---

## Task 11: `cmd_diff` orchestrator + E2E

**Files:**
- Create: `crates/vision-graphql-cli/src/cmd_diff.rs`
- Modify: `crates/vision-graphql-cli/src/main.rs`
- Modify: `crates/vision-graphql-cli/tests/cli_e2e.rs`

- [ ] **Step 1: Add module declaration**

Add `mod cmd_diff;` near the other module declarations in `main.rs`.

- [ ] **Step 2: Implement `cmd_diff::run`**

Create `crates/vision-graphql-cli/src/cmd_diff.rs`:

```rust
//! Compare a schema.toml against a live database for stale references.

use anyhow::{Context, Result};
use std::path::Path;
use vision_graphql::schema::config::parse;
use vision_graphql::schema::introspect::introspect;

use crate::analyze::find_drift;
use crate::cmd_generate; // reuse build_pool
use crate::filter::TableFilter;
use crate::render::redact_url;
use crate::report::{self, Format};
use crate::DriftDetected;

pub struct Args {
    pub url: String,
    pub config: std::path::PathBuf,
    pub format: Format,
    pub include: Option<Vec<String>>,
    pub ignore: Option<Vec<String>>,
}

pub async fn run(args: Args) -> Result<()> {
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("reading {}", args.config.display()))?;
    let cfg = parse(&text)
        .with_context(|| format!("parsing {}", args.config.display()))?;

    let pool = cmd_generate::build_pool_pub(&args.url)?;
    let db = introspect(&pool)
        .await
        .with_context(|| format!("introspect failed against {}", redact_url(&args.url)))?;

    let filter = TableFilter::new(args.include.as_deref(), args.ignore.as_deref())?;
    let report = find_drift(&cfg, &db, &filter);

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    report::write(&report, args.format, &mut out)?;

    if !report.is_clean() {
        return Err(DriftDetected.into());
    }
    Ok(())
}

#[allow(dead_code)]
fn _unused(_: &Path) {}
```

- [ ] **Step 3: Promote `build_pool` to public**

In `crates/vision-graphql-cli/src/cmd_generate.rs`, rename the private `build_pool` to a public re-export so `cmd_diff` can call it. Replace the existing `fn build_pool` line with:

```rust
pub fn build_pool_pub(url: &str) -> Result<deadpool_postgres::Pool> {
```

Update the single internal caller in `cmd_generate::run` from `build_pool(&args.url)` to `build_pool_pub(&args.url)`.

- [ ] **Step 4: Wire up the diff dispatch in `main.rs`**

In `main.rs` `dispatch`, replace the `Cmd::Diff(_a)` arm with:

```rust
Cmd::Diff(a) => {
    let url = resolve_url(a.db.url)?;
    let format = match a.format.as_str() {
        "json" => report::Format::Json,
        _ => report::Format::Text,
    };
    cmd_diff::run(cmd_diff::Args {
        url,
        config: a.config,
        format,
        include: a.db.include_tables,
        ignore: a.db.ignore_tables,
    })
    .await
}
```

- [ ] **Step 5: Verify the workspace compiles**

Run: `cargo build -p vision-graphql-cli`
Expected: clean.

- [ ] **Step 6: Add E2E tests for diff**

Append to `crates/vision-graphql-cli/tests/cli_e2e.rs`:

```rust
fn write_temp_toml(name: &str, contents: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("vision-gql-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join(name);
    std::fs::write(&p, contents).unwrap();
    p
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_clean_overlay_exits_zero() {
    let (url, _c) = boot_pg().await;
    let p = write_temp_toml(
        "clean.toml",
        r#"
        [tables.users]
        expose_as = "profiles"
        hide_columns = ["secret"]
        "#,
    );
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args(["diff", "--url", &url, "--config", p.to_str().unwrap()])
        .output()
        .expect("run cli");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8(out.stdout).unwrap().contains("OK"));
    let _ = std::fs::remove_file(&p);
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_stale_hide_column_exits_one() {
    let (url, _c) = boot_pg().await;
    let p = write_temp_toml(
        "stale.toml",
        r#"
        [tables.users]
        hide_columns = ["password_hash"]
        "#,
    );
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args(["diff", "--url", &url, "--config", p.to_str().unwrap()])
        .output()
        .expect("run cli");
    assert_eq!(out.status.code(), Some(1));
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(s.contains("password_hash"));
    let _ = std::fs::remove_file(&p);
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_json_format_exits_one_with_machine_output() {
    let (url, _c) = boot_pg().await;
    let p = write_temp_toml(
        "stale_json.toml",
        r#"
        [tables.users]
        hide_columns = ["password_hash"]
        "#,
    );
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args([
            "diff",
            "--url",
            &url,
            "--config",
            p.to_str().unwrap(),
            "--format",
            "json",
        ])
        .output()
        .expect("run cli");
    assert_eq!(out.status.code(), Some(1));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["missing_columns"][0]["column"], "password_hash");
    let _ = std::fs::remove_file(&p);
}

#[tokio::test(flavor = "multi_thread")]
async fn diff_ignore_tables_filters_findings() {
    let (url, _c) = boot_pg().await;
    let p = write_temp_toml(
        "ignored.toml",
        r#"
        [tables.users]
        hide_columns = ["password_hash"]
        "#,
    );
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args([
            "diff",
            "--url",
            &url,
            "--config",
            p.to_str().unwrap(),
            "--ignore-tables",
            "users",
        ])
        .output()
        .expect("run cli");
    assert!(out.status.success(), "ignored entry should disappear");
    let _ = std::fs::remove_file(&p);
}
```

Add `serde_json` to the CLI crate's `[dev-dependencies]` if it isn't already inherited from the runtime deps. (It is already in `[dependencies]`, so no action needed.)

- [ ] **Step 7: Run the E2E tests and verify they pass**

Run: `cargo test -p vision-graphql-cli --test cli_e2e -- --nocapture`
Expected: all 7 tests pass (3 from Task 10 + 4 here).

- [ ] **Step 8: Commit**

```bash
git add crates/vision-graphql-cli/src/cmd_diff.rs \
        crates/vision-graphql-cli/src/cmd_generate.rs \
        crates/vision-graphql-cli/src/main.rs \
        crates/vision-graphql-cli/tests/cli_e2e.rs
git commit -m "feat(cli): diff subcommand wired end-to-end"
```

---

## Task 12: `validate` E2E and missing-URL E2E

**Files:**
- Modify: `crates/vision-graphql-cli/tests/cli_e2e.rs`

- [ ] **Step 1: Add E2E tests**

Append to `crates/vision-graphql-cli/tests/cli_e2e.rs`:

```rust
#[test]
fn validate_clean_exits_zero() {
    let p = write_temp_toml(
        "clean_for_validate.toml",
        r#"
        [tables.users]
        expose_as = "profiles"
        "#,
    );
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args(["validate", p.to_str().unwrap()])
        .output()
        .expect("run cli");
    assert!(out.status.success());
    let _ = std::fs::remove_file(&p);
}

#[test]
fn validate_dup_expose_as_exits_two() {
    let p = write_temp_toml(
        "dup_expose.toml",
        r#"
        [tables.users]
        expose_as = "people"

        [tables.profiles]
        expose_as = "people"
        "#,
    );
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args(["validate", p.to_str().unwrap()])
        .output()
        .expect("run cli");
    assert_eq!(out.status.code(), Some(2));
    let _ = std::fs::remove_file(&p);
}

#[test]
fn missing_url_and_env_exits_two() {
    let bin = env!("CARGO_BIN_EXE_vision-gql");
    let out = Command::new(bin)
        .args(["generate"])
        .env_remove("DATABASE_URL")
        .output()
        .expect("run cli");
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("DATABASE_URL"));
}
```

- [ ] **Step 2: Run the new tests**

Run: `cargo test -p vision-graphql-cli --test cli_e2e validate_ -- --nocapture`
Run: `cargo test -p vision-graphql-cli --test cli_e2e missing_url_ -- --nocapture`
Expected: 3 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/vision-graphql-cli/tests/cli_e2e.rs
git commit -m "test(cli): validate subcommand and missing-URL E2E"
```

---

## Task 13: README + publish smoke check

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add the CLI section to `README.md`**

Insert a new section after the existing "Building the schema" section (right before the "Mutations" section). Find the line beginning `## Mutations` in `README.md`. Immediately above it, insert:

```markdown
## CLI

`vision-graphql-cli` ships a `vision-gql` binary that bootstraps and validates
overlay files against a live database.

```bash
cargo install vision-graphql-cli
vision-gql generate --url postgres://localhost/myapp > schema.toml
vision-gql diff     --url postgres://localhost/myapp --config schema.toml
vision-gql validate schema.toml
```

`generate` produces a fully-commented starter file; uncomment any stanza to
override defaults from introspection. `diff` checks the overlay's references
against the current database (exit 0 = clean, 1 = drift, 2 = error). `validate`
performs offline structural checks without a connection.

Filter what gets processed with comma-separated globs:

```bash
vision-gql generate --url $DATABASE_URL --ignore-tables 'audit_*,_temp_*'
```

Both subcommands accept `$DATABASE_URL` as the default for `--url`. NoTls only
in this release; only the `public` schema is introspected.
```

(The opening triple-backtick of the inner code block does not collide with the doc fences here since this is plain markdown insertion. If your editor flags it, escape with explicit indentation.)

- [ ] **Step 2: Add a forward reference in the existing "Building the schema" overlay paragraph**

In `README.md`, find the line `2. **TOML overlay** — \`.load_config("schema.toml")?\` applies renames, hidden columns, and manual relations.` and replace with:

```markdown
2. **TOML overlay** — `.load_config("schema.toml")?` applies renames, hidden columns, and manual relations. Run `vision-gql generate` to bootstrap a starter file from a live DB.
```

- [ ] **Step 3: Run `cargo package` for the lib to verify nothing leaked**

Run: `cargo package --list -p vision-graphql --allow-dirty | grep -E '^(crates/|docs/)' || echo CLEAN`
Expected output: `CLEAN`.

- [ ] **Step 4: Run the full workspace test suite**

Run: `cargo test --workspace -- --include-ignored 2>&1 | tail -40`
Expected: all tests pass; no regressions in the lib crate.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs(readme): document vision-gql CLI"
```

---

## Self-review notes

Spec coverage check (each spec section → task[s]):

- Workspace layout / hybrid root-package workspace → Task 1.
- CLI crate dependencies → Task 1.
- Command surface (generate / diff / validate, all flags) → Tasks 1, 9, 10, 11.
- Filter semantics (include then ignore, glob via globset) → Task 2; Task 10/11 wire it into commands.
- Logging table (`-q` / default / `-v` / `-vv`) → Task 8; Task 10 installs it.
- `render::toml_template` signature `(&IntrospectedDb, &TableFilter, &HeaderMeta) -> String` → Tasks 4 and 5.
- Header metadata (version, ISO-8601 timestamp, redacted URL) → Task 3 (redact + struct), Task 10 (built at runtime).
- `analyze::find_drift` with all four finding categories → Task 6.
- `report::write` text + JSON → Task 7.
- `cmd_generate` / `cmd_diff` / `cmd_validate` orchestrators → Tasks 9, 10, 11.
- Exit codes (0, 1 drift, 2 other) → Task 10 (`main`'s match on `DriftDetected`).
- `cargo publish` regression mitigation (exclude `crates/**`) → Task 1 Step 1, verified Task 13 Step 3.
- Documentation (README CLI section + forward reference) → Task 13.
- Testing (unit + E2E coverage of each finding category, --force, filters, validate, log levels) → Tasks 2–9 unit, Tasks 10–12 E2E.

All spec requirements have at least one task. Type/method names cross-checked: `TableFilter::keep`, `find_drift(cfg, db, filter)`, `toml_template(db, filter, meta)`, `report::write(&report, format, &mut out)`, `cmd_generate::Args { url, output, force, include, ignore }`, `cmd_diff::Args { url, config, format, include, ignore }`, `DriftDetected` — consistent across tasks.

Plan complete and saved to `docs/superpowers/plans/2026-05-09-cli-schema-gen.md`.
