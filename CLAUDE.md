# Working on vision-graphql

What the code does is in the README; what the API is, is in the rustdoc. This
file is for what neither shows: the invariants that are easy to break without
any test failing, and how work moves from a branch to `main`.

## Invariants that break silently

Each of these has actually been broken here. None of them made a test fail.

**Cross-cutting passes belong on the IR, not on the parser.** The typed builder
(`Engine::run`) never goes near `parser.rs` — it constructs an `Operation`
directly. So a check that lives in lowering guards one of the two entry points
and leaves the other open. Scope rewriting and `ExecutionLimits` both run on the
IR for this reason. When adding another such pass, put it beside them, and route
it through `engine::prepare` so a new entry point cannot skip it by calling
`render_now` directly.

**New SQL that reads a table must be taught to `apply_scope`.** `scope.rs`
rewrites the IR before rendering. A renderer path that reaches a table without a
corresponding arm there produces SQL with no predicate — an unscoped read for a
scoped caller, with no error. Adding a table access point to `sql.rs` means
adding it to `scope.rs` in the same change, and to `ExecutionLimits`' walk if it
renders a subquery or a `LIMIT`.

**A value position in the IR must be `Val`, never `serde_json::Value`.** `Val`
is what carries a variable that has not been substituted yet. A new field typed
as `Value` compiles, works under `Engine::query` (which substitutes eagerly),
and silently breaks `Engine::compile` — which is the path where the value is not
known yet. The same applies to row counts and `Count`.

**Everything the engine publishes, it must implement.** The type system in
`type_system.rs` is derived from the schema and read by both SDL export and
`__schema`. Adding an operator to the comparison inputs there without adding it
to `lower_where` tells clients — and their code generators — that something
works which will then be rejected. The tests assert this direction explicitly.

**Recursion over documents needs a bound that survives the input.**
`ParseLimits` scans raw text before parsing because a deeply nested input value
overflows the stack *inside* the parser, and a stack overflow in Rust aborts the
process rather than unwinding: no `catch_unwind` at a request boundary contains
it. Fragment graphs are checked separately (`validate_fragments`) because a
cycle or a long chain nests one bracket and walks past a text-depth guard.
Anything new that recurses over a client-supplied structure needs the same
question asked of it.

## Working style

**Silent wrongness is the bug worth hunting.** Most of what has been fixed here
was not a crash but an answer that looked fine: an argument read by nobody, a
response key that overwrote another, a limit that skipped one root type. When
choosing between erroring and doing something reasonable-looking, error — unless
the silent behaviour is the deliberate trade, in which case say so in a comment
and make it opt-in.

**Verify against Postgres, not against the rendered string.** Unusual SQL — row
constructors, `DISTINCT` over a tuple, a subquery that must return one row — has
a way of being plausible and wrong. Assertions on SQL text catch typos;
integration tests catch semantics.

**Comments explain the decision, not the mechanism.** The code says what it
does. A comment is for why this and not the obvious alternative, and it earns
its place when a reader would otherwise "fix" the thing it protects.

## Workflow

1. **Branch.** `main` is merged into, not committed to. Names follow the
   history: `feat/…`, `fix/…`.
2. **Build the change with its tests.** A regression test for a bug should be
   written so that *reaching the assertion* is meaningful — several here would
   abort the process without the fix, and that is the test.
3. **Gate before review**, all four, all clean:
   ```
   cargo fmt --all
   cargo clippy --workspace --all-targets -- -D warnings
   cargo nextest run --workspace --test-threads 4
   cargo test --workspace --doc
   ```
4. **Review the branch, not each commit.** `/code-review <branch> high` at the
   end. The reviews that found the most here found it by seeing the whole
   surface at once: the worst bug was code from the first commit that only
   became reachable through the third.
5. **Fix the findings on the same branch**, with a test for each, then merge
   with `--no-ff`.
6. **CHANGELOG.** Every user-visible change goes under `## Unreleased`, in the
   voice of what went wrong and what it now does. Version bumps are their own
   `chore: release` commit — do not bump as a side effect.

## Testing environment

Integration tests need Docker: each boots its own Postgres through
testcontainers.

Use **`cargo nextest run --test-threads 4`**, not `cargo test`. Cargo runs test
*binaries* in parallel with no shared budget, and ~25 of them each starting
containers makes Docker return `PortNotExposed` roughly one run in three — a
failure that looks like a flaky test and is not one. nextest schedules
everything through one pool, so the cap is on containers. `cargo test` remains
the way to run doctests, which nextest does not.

## MSRV

`rust-version` tracks what the dependency graph actually requires — sqlx sets
the floor — and CI checks it against the published targets only. Test-only
dependencies carry their own, higher floors; failing the MSRV job on those would
be failing it for something no consumer is affected by.
