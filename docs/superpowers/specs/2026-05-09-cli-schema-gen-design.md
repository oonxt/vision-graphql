# CLI Schema Generator — Design

**Date:** 2026-05-09
**Scope:** Add a CLI tool that bootstraps a starter `schema.toml` overlay from a live PostgreSQL database, and that validates an existing overlay against the current database.

## Background

`vision-graphql` builds its `Schema` from three layers (later wins):

1. **Introspection** — `Schema::introspect(&pool)` reads `information_schema` and auto-derives tables, columns, primary keys, foreign keys.
2. **TOML overlay** — `SchemaBuilder::load_config(path)` applies `expose_as`, `hide_columns`, manual relations.
3. **Builder** — final programmatic touches before `.build()`.

Today, layer 2 has no CLI affordance. To bootstrap a starter `schema.toml`, a user must write Rust glue against `introspect.rs`. To detect drift between an overlay and a migrated database, there is no tool at all — broken overlays surface only at runtime.

## Goals

- Let users generate a complete starter `schema.toml` from a live database in one command.
- Let users / CI detect when an existing overlay references DB objects that no longer exist.
- Keep the published `vision-graphql` library tarball unchanged in size and dependencies.

## Non-goals

- Schema migrations. The CLI does not produce SQL DDL.
- Full schema diff (DB vs. introspected schema). `diff` is scoped narrowly: "is the overlay still valid against the current DB?"
- Multi-schema introspection. `introspect.rs` currently hardcodes the `public` schema; the CLI inherits that.
- TLS connections (`sslmode=require`, etc.). NoTls only in v1, matching the README's existing examples.
- Interactive REPL or `generate --merge` (preserving prior edits in an existing file). v1 always overwrites.

## Architecture

### Workspace layout

The repository becomes a hybrid root-package workspace. The library crate stays at the root; the CLI lives at `crates/vision-graphql-cli/` as a workspace member.

```
vision-graphql/
├── Cargo.toml          # [workspace] + [package] (lib unchanged)
├── src/                # lib source (unchanged)
└── crates/
    └── vision-graphql-cli/
        ├── Cargo.toml
        └── src/
            ├── main.rs
            ├── cmd_generate.rs
            ├── cmd_diff.rs
            ├── cmd_validate.rs
            ├── render.rs
            ├── analyze.rs
            ├── filter.rs
            └── report.rs
```

Root `Cargo.toml` gets:

```toml
[workspace]
members = [".", "crates/vision-graphql-cli"]
```

The lib `[package]` block stays untouched, so `cargo publish` from the root continues to publish only `vision-graphql`.

### CLI crate dependencies

```toml
[package]
name = "vision-graphql-cli"
version = "0.2.0"
edition = "2021"

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
tracing-subscriber  = { version = "0.3", features = ["env-filter"] }
time                = { version = "0.3", features = ["formatting"] }
```

`vision-graphql`'s public surface needs to expose enough for the CLI: `schema::introspect::introspect` / `IntrospectedDb` and `schema::config::parse` / `ConfigOverlay`. These are currently `pub` modules; verify their items are reachable and promote any `pub(crate)` items if needed.

### Command surface

```
vision-gql generate [--url <URL>] [--output <PATH>] [--force]
                    [--include-tables <GLOBS>] [--ignore-tables <GLOBS>]
                    [-v...] [-q]

vision-gql diff     [--url <URL>] [--config <PATH>] [--format text|json]
                    [--include-tables <GLOBS>] [--ignore-tables <GLOBS>]
                    [-v...] [-q]

vision-gql validate <PATH> [-v...] [-q]
```

Per-command flags:

- **`--url`** (`generate`, `diff`) defaults to `$DATABASE_URL`. Missing both → exit 2 with a clear message.
- **`--output`** / `-o` (`generate`) defaults to `-` (stdout). Any other value writes to that path, creating the file.
- **`--force`** / `-f` (`generate`) — required when `--output` points at an existing file. Without it, the CLI refuses to overwrite and exits 2.
- **`--config`** (`diff`) defaults to `./schema.toml`.
- **`--format`** (`diff`) is `text` (default) or `json`.
- **`--include-tables <GLOBS>`** / **`--ignore-tables <GLOBS>`** (`generate`, `diff`) — comma-separated globs. Filter semantics defined below.
- **`-v`** / **`--verbose`** (all) — stackable, sets tracing level (`-v` = debug, `-vv` = trace).
- **`-q`** / **`--quiet`** (all) — suppress all stderr tracing output. Conflicts with `-v` via clap.

Exit codes:
- `0` — success / no drift.
- `1` — drift detected (only for `diff`).
- `2` — failure (DB connect, parse error, I/O error, missing URL, refusing to overwrite without `--force`, `validate` finding structural problems).

### Filter semantics

`--include-tables` and `--ignore-tables` accept comma-separated glob patterns matched against the **physical table name** (the key under `[tables.<key>]` in the toml, equal to the introspected table name in the `public` schema).

Order of evaluation:

1. If `--include-tables` is given, restrict the working set to tables matching at least one pattern.
2. From that set, remove tables matching any `--ignore-tables` pattern.
3. If neither flag is given, the working set is all tables.

Compiled with `globset::GlobSetBuilder`. Example: `--ignore-tables 'audit_*,_temp_*,*_archive'`.

For `generate`, the filter decides which tables produce a stanza in the output. For `diff`, the filter decides which `[tables.<key>]` entries get checked — entries filtered out are silently skipped (not reported as missing). `validate` does not take filter flags.

### Logging

`tracing-subscriber::fmt()` writes to stderr. Level is computed at startup from `-v`/`-q` (clap counts `-v` occurrences):

| flags  | level    |
|--------|----------|
| `-q`   | `OFF`    |
| (none) | `WARN`   |
| `-v`   | `DEBUG`  |
| `-vv+` | `TRACE`  |

`WARN` default lets the existing `vision_graphql::introspect` warnings (e.g., "skipping column with unsupported type") reach the user without forcing them to opt in.

## Component design

### `render::toml_template(&IntrospectedDb, &TableFilter, &HeaderMeta) -> String`

Pure function. Hand-rolled writer (not `toml::to_string` — we need comments). Iterates `db.tables` filtered by `filter.keep(&physical_name)`. For each kept table, emit a section like:

```toml
# ── public.users ─────────────────────────────
# columns: id (int4, PK), name (text?), email (text)
# foreign keys: posts.user_id -> users.id (inferred relation)
#
# [tables.users]
# expose_as = "profiles"
# hide_columns = ["password_hash"]
#
# # array relation derived from FK posts.user_id -> users.id
# [[tables.users.relations]]
# name = "posts"
# kind = "array"
# target = "posts"
# mapping = [["id", "user_id"]]
```

Every emitted line outside the file header is `#`-prefixed. By default the file is a no-op overlay; the user un-comments specific stanzas to activate them. This preserves the overlay-is-an-overlay semantics — uncommenting only what you change avoids the "overlay drifts in lockstep with DB" problem.

The header (always emitted, all lines `#`-prefixed) embeds traceability metadata:

```toml
# Generated by vision-gql 0.2.0 on 2026-05-09T14:23:11Z
# Source: postgres://user@db.example.com:5432/myapp_prod
# Uncomment any stanza below to override defaults from introspection.
```

- Tool version comes from `env!("CARGO_PKG_VERSION")` of the CLI crate.
- Timestamp is UTC ISO-8601, formatted via the `time` crate at run time.
- Source URL has its password component stripped before printing. If parsing the URL fails, the line falls back to `# Source: <unparseable>`.

### `analyze::find_drift(&ConfigOverlay, &IntrospectedDb, &TableFilter) -> DiffReport`

Pure function. Walks every `tables.<key>` for which `filter.keep(&key)` is true and emits findings:

- **`missing_tables`** — overlay references table `<key>` but `(public, key)` is not in the introspected map.
- **`missing_columns`** — `hide_columns` entry, or relation mapping endpoint, references a column that doesn't exist on the relevant table.
- **`missing_relation_targets`** — manual relation `target` doesn't resolve to either an introspected physical table or another overlay table's `expose_as`.
- **`expose_as_collisions`** — two distinct overlay tables map to the same exposed name, or an overlay's `expose_as` collides with another physical table name.

```rust
pub struct DiffReport {
    pub missing_tables: Vec<String>,                       // overlay key
    pub missing_columns: Vec<MissingColumn>,
    pub missing_relation_targets: Vec<MissingRelTarget>,
    pub expose_as_collisions: Vec<Collision>,
}

pub struct MissingColumn {
    pub table: String,         // overlay key
    pub column: String,        // physical column name expected
    pub origin: ColumnOrigin,  // HideColumns | RelationLocal | RelationRemote
}

pub struct MissingRelTarget {
    pub table: String,         // overlay key
    pub relation: String,      // relation name from overlay
    pub target: String,        // unresolved target
}

pub struct Collision {
    pub exposed_name: String,
    pub sources: Vec<String>,  // physical names contributing
}
```

`DiffReport::is_clean()` returns true iff all four vectors are empty.

### `report::write(&DiffReport, Format, impl Write)`

- `Format::Text` — human-readable, grouped by category. Empty groups omitted. Suffix line: `<N> issues found`.
- `Format::Json` — `serde_json::to_writer_pretty(&report)`.

### `filter::TableFilter`

Pure helper. Built from `(include: Option<&[String]>, ignore: Option<&[String]>)`. Compiles each pattern via `globset::Glob::new` into a single `GlobSet` per side. `TableFilter::keep(&str) -> bool` returns true iff the include side matches (or is empty) AND the ignore side does not match. Used by `cmd_generate` (to filter `IntrospectedDb` table iteration) and `cmd_diff` (to filter overlay-key iteration).

Invalid glob input (e.g., unbalanced `[`) returns an error during construction; the CLI surfaces this with a clear message and exits 2.

### `cmd_generate` / `cmd_diff` / `cmd_validate`

Each is a thin orchestrator:

- `cmd_generate(url, output, force, filter)`:
  1. If `output != "-"` and the path exists and `!force`, return an error → exit 2.
  2. Build deadpool pool from `url` (NoTls).
  3. `introspect(&pool).await?`.
  4. Build `HeaderMeta { tool_version, timestamp, redacted_url }`.
  5. `let s = render::toml_template(&db, &filter, &header_meta);` (render does the filtering).
  6. Write to stdout if `output == "-"`, else to the path.

- `cmd_diff(url, config, format, filter)`:
  1. `let text = std::fs::read_to_string(config)?;`
  2. `let cfg = schema::config::parse(&text)?;`
  3. Build pool, `introspect`.
  4. `let report = analyze::find_drift(&cfg, &db, &filter);` — filter applied to which overlay keys get checked.
  5. `report::write(&report, format, &mut stdout())`.
  6. Return `Ok(())` if clean, else `Err(DriftDetected)` mapped to exit `1`.

- `cmd_validate(path)`:
  1. `let text = std::fs::read_to_string(path)?;`
  2. `let cfg = schema::config::parse(&text)?;` — surfaces TOML syntax + `deny_unknown_fields` errors.
  3. Run offline checks on `cfg`:
     - Each `relation.mapping` is non-empty.
     - Within the overlay alone, no two table keys share an `expose_as`.
     - Each manual relation `target` resolves either to another `tables.<key>` or, if not, is left as a forward reference (deferred to `diff`).
  4. Print `OK` to stdout (or a structural-error report) and exit 0/2.

`main.rs` uses `clap` derive macros, dispatches subcommands, configures `tracing-subscriber` from `-v`/`-q` first thing, prints errors via `anyhow`, and maps the dedicated `DriftDetected` error to exit `1` while everything else exits `2`.

## Data flow

```
generate:
  $DATABASE_URL ─→ Pool ─→ introspect ─→ IntrospectedDb ─┐
                                                         ├─→ render ─→ TOML text ─→ stdout / file
                                          TableFilter ───┘

diff:
  schema.toml ─→ parse ─→ ConfigOverlay ─┐
  $DATABASE_URL ─→ Pool ─→ introspect ───┼─→ analyze ─→ DiffReport ─→ report::write ─→ stdout
                          TableFilter ───┘                    │
                                                              └─→ exit code

validate:
  schema.toml ─→ parse ─→ ConfigOverlay ─→ offline checks ─→ stdout / exit code
```

## Testing

Following the project's existing conventions (`insta` snapshots, `testcontainers` for live PG):

**Unit:**
- `render::toml_template` — synthetic `IntrospectedDb` fixtures (empty DB, single table no FKs, two tables with one-to-many FK, multi-column PK + composite FK) → `insta::assert_snapshot!`. Header metadata stubbed to a fixed value so snapshots are deterministic.
- `analyze::find_drift` — synthetic `(ConfigOverlay, IntrospectedDb, TableFilter)` triples covering each finding category, a clean case, and a filtered case → assert on `DiffReport` shape.
- `filter::TableFilter` — include-only, ignore-only, both, neither, invalid glob → behavior + error.
- `report` — fixed `DiffReport` → assert text and JSON output shape via `insta`.
- Validate: parse + within-overlay structural checks against fixture toml inputs.

**E2E (in `crates/vision-graphql-cli/tests/`):**
- testcontainers PG, run a fixed DDL, then invoke the binary via `Command::new(env!("CARGO_BIN_EXE_vision-gql"))`.
  - `generate` produces non-empty stdout; piping it back through `schema::config::parse` parses successfully (since it's all comments → empty overlay).
  - `generate -o file.toml` works once; running it again without `--force` exits 2; with `--force` succeeds.
  - `generate --ignore-tables 'audit_*'` excludes audit tables from output; `--include-tables 'users'` produces only that one stanza.
  - `diff` against a known-good toml exits 0.
  - `diff` against a toml with a stale `hide_columns` entry exits 1 and prints the missing column.
  - `diff --ignore-tables` filters out the offending entry → exits 0.
  - `validate` against a syntactically valid toml exits 0; against a toml with duplicate `expose_as` exits 2.
  - `-q` suppresses tracing output to stderr; `-v` enables debug-level lines.

## Error handling

The CLI uses `anyhow` at the top level for ergonomic error chaining. A dedicated `DriftDetected` marker error (or a typed sentinel returned from `cmd_diff`) is matched in `main` to set exit code `1`; all other errors map to `2`. The library's `vision_graphql::Error` wraps fine into `anyhow::Error` via `?`.

Connection error messages should include the URL with password component stripped so the user can tell which database failed without leaking secrets. Parse errors should include the file path and line/column when possible (the `toml` crate already does this).

## Documentation

- New "CLI" section in the root `README.md` after "Building the schema":
  - Install: `cargo install vision-graphql-cli`.
  - One-liner for `generate`, `diff`, and `validate`, with sample output.
  - Document `--include-tables` / `--ignore-tables` glob syntax with one example.
  - Note: NoTls only, `public` schema only.
- Add a one-line forward reference in the existing "Building the schema → 2. TOML overlay" paragraph: "or run `vision-gql generate` to bootstrap a starter file from a live DB."

## Risks and mitigations

- **Library API surface drift.** The CLI imports `schema::introspect::introspect` and `schema::config::parse`. Today these are reachable; the spec's plan must verify and lock them in via a smoke test in the library crate that asserts the items are `pub`.
- **Workspace conversion side effects.** Adding `[workspace]` to the root might shift `target/` resolution and IDE indexing. Mitigation: add `members = [".", "crates/vision-graphql-cli"]` explicitly so the root remains a member; do not introduce `[workspace.dependencies]` in v1.
- **`cargo publish` regression.** The published tarball must remain `vision-graphql` only. The crate already has `exclude = ["docs/**", "tests/**", ...]`; we should also add `"crates/**"` to that list to prevent any chance of the CLI source leaking into the published lib tarball. Verify with `cargo package --list` before/after.
- **Comment-only template confusion.** A user might run `generate` and assume it does something. Mitigation: the file header explains that the default file is a no-op and instructs un-commenting.

## Out of scope (deferred)

- TLS support (would add a feature flag for `tokio-postgres` `with-rustls` or similar).
- Multi-schema introspection — needs a change in `introspect.rs` itself, tracked separately.
- `generate --merge` to preserve a user's existing edits.
- `diff` for full schema (DB-level) drift, not just overlay validity.
- Interactive REPL (`generate --interactive`).
