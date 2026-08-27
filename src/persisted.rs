//! A set of queries compiled at startup and run by key.
//!
//! This is the shape an endpoint should prefer when it can: the documents ship
//! with the client, the server compiles them once, and a request names one by
//! key instead of sending text. Everything this crate spends effort defending
//! against — a pathological document, an unbounded read, a statement per page
//! size — stops being reachable, because no new document is ever parsed. What is
//! left to check is that the key exists and the variables fit.
//!
//! It also moves failure to the right time. A typo in a column name, a table
//! outside the scope policy, a literal of the wrong type: all of it surfaces
//! when the registry is built, which is startup, rather than on the one request
//! that happens to hit that query.
//!
//! ```no_run
//! # use vision_graphql::{Engine, persisted::QueryRegistry};
//! # async fn f(engine: Engine) -> vision_graphql::error::Result<()> {
//! // once, at startup — a failure here names the key that failed
//! let registry = QueryRegistry::compile_all(
//!     &engine,
//!     [
//!         ("user-list", "query($n: Int!) { users(limit: $n) { id name } }"),
//!         ("user-by-id", "query($id: Int!) { users_by_pk(id: $id) { id name } }"),
//!     ],
//! )?;
//!
//! // per request
//! let data = engine
//!     .execute(registry.require("user-list")?, Some(serde_json::json!({"n": 20})))
//!     .await?;
//! # let _ = data; Ok(()) }
//! ```
//!
//! The key is whatever the application wants it to be — a name, a file path, a
//! hash of the document. This crate does not pick one: a registry keyed on the
//! SHA-256 of the text implements the persisted-query protocol clients speak,
//! and a registry keyed on names is easier to read in a log, and neither belongs
//! to the engine.

use crate::compiled::CompiledQuery;
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::policy::ScopePolicy;
use std::collections::HashMap;

/// Compiled queries, addressed by key.
#[derive(Debug, Default, Clone)]
pub struct QueryRegistry {
    queries: HashMap<String, CompiledQuery>,
}

impl QueryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Compile every entry, or fail naming the key that would not compile.
    ///
    /// The naming is the point: a compile error reports a path within the
    /// document (`users.where.id`), which says nothing about *which* document
    /// when a hundred are being compiled at once.
    pub fn compile_all<K, S, I>(engine: &Engine, entries: I) -> Result<Self>
    where
        K: Into<String>,
        S: AsRef<str>,
        I: IntoIterator<Item = (K, S)>,
    {
        let mut out = Self::new();
        for (key, source) in entries {
            out.insert(engine, key, source.as_ref())?;
        }
        Ok(out)
    }

    /// Same, under a scope policy: every query is compiled with the policy
    /// applied, and must then be run with a principal.
    pub fn compile_all_scoped<K, S, I>(
        engine: &Engine,
        entries: I,
        policy: &ScopePolicy,
    ) -> Result<Self>
    where
        K: Into<String>,
        S: AsRef<str>,
        I: IntoIterator<Item = (K, S)>,
    {
        let mut out = Self::new();
        for (key, source) in entries {
            out.insert_scoped(engine, key, source.as_ref(), policy)?;
        }
        Ok(out)
    }

    /// Compile `source` and store it under `key`.
    ///
    /// A key already taken is an error. A registry assembled from a directory of
    /// files, or from two lists merged, would otherwise keep whichever document
    /// came last and serve *that* under the key — the wrong query, silently, in
    /// the one structure whose entire purpose is to be certain which queries can
    /// run. Use [`QueryRegistry::replace`] where overwriting is meant.
    pub fn insert(&mut self, engine: &Engine, key: impl Into<String>, source: &str) -> Result<()> {
        let key = key.into();
        self.vacant(&key)?;
        let compiled = engine.compile(source).map_err(|e| label(&key, e))?;
        self.queries.insert(key, compiled);
        Ok(())
    }

    fn vacant(&self, key: &str) -> Result<()> {
        if self.queries.contains_key(key) {
            return Err(Error::Schema(format!(
                "persisted query '{key}': a query is already registered under this key"
            )));
        }
        Ok(())
    }

    /// [`QueryRegistry::insert`] under a scope policy.
    pub fn insert_scoped(
        &mut self,
        engine: &Engine,
        key: impl Into<String>,
        source: &str,
        policy: &ScopePolicy,
    ) -> Result<()> {
        let key = key.into();
        self.vacant(&key)?;
        let compiled = engine
            .compile_scoped(source, policy)
            .map_err(|e| label(&key, e))?;
        self.queries.insert(key, compiled);
        Ok(())
    }

    /// Store an already-compiled statement. Refuses a key already taken, for the
    /// reason [`QueryRegistry::insert`] gives.
    pub fn add(&mut self, key: impl Into<String>, compiled: CompiledQuery) -> Result<()> {
        let key = key.into();
        self.vacant(&key)?;
        self.queries.insert(key, compiled);
        Ok(())
    }

    /// Store under `key`, replacing whatever was there. For a caller that means
    /// to overwrite — a hot reload, a test — rather than one that collided by
    /// accident.
    pub fn replace(&mut self, key: impl Into<String>, compiled: CompiledQuery) {
        self.queries.insert(key.into(), compiled);
    }

    pub fn get(&self, key: &str) -> Option<&CompiledQuery> {
        self.queries.get(key)
    }

    /// The query under `key`, or an error saying it is not registered.
    ///
    /// An unknown key is an ordinary client mistake, not an internal failure,
    /// and the message deliberately does not list what *is* registered: the
    /// registry is an allowlist, and enumerating it to whoever asks defeats
    /// half of what it is for.
    pub fn require(&self, key: &str) -> Result<&CompiledQuery> {
        self.queries.get(key).ok_or_else(|| Error::Validate {
            path: key.to_string(),
            message: "no query is registered under this key".into(),
        })
    }

    pub fn contains(&self, key: &str) -> bool {
        self.queries.contains_key(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.queries.keys()
    }

    pub fn len(&self) -> usize {
        self.queries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }
}

/// Prefix a compile failure with the key it came from.
fn label(key: &str, e: Error) -> Error {
    match e {
        Error::Validate { path, message } => Error::Validate {
            path: format!("{key}: {path}"),
            message,
        },
        Error::NotCompilable { path, message } => Error::NotCompilable {
            path: format!("{key}: {path}"),
            message,
        },
        other => Error::Schema(format!("persisted query '{key}': {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::{col, principal};
    use crate::schema::{PgType, Schema, Table};

    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"]),
            )
            .build()
    }

    /// Compiling touches the schema and the parser, never the pool — so a lazy
    /// pool that never connects is enough. It still wants a runtime to live in,
    /// which is why these are async tests that never await anything.
    fn engine() -> Engine {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/does-not-exist")
            .unwrap();
        Engine::new(pool, schema())
    }

    #[tokio::test]
    async fn compiles_a_batch_and_serves_it_by_key() {
        let engine = engine();
        let reg = QueryRegistry::compile_all(
            &engine,
            [
                ("list", "query($n: Int!) { users(limit: $n) { id name } }"),
                ("by-id", "query($id: Int!) { users_by_pk(id: $id) { id } }"),
            ],
        )
        .unwrap();
        assert_eq!(reg.len(), 2);
        assert!(reg.contains("list"));
        assert_eq!(reg.require("list").unwrap().variables(), vec!["n"]);
        assert!(reg.get("nope").is_none());
    }

    #[tokio::test]
    async fn an_unknown_key_says_so_without_listing_the_others() {
        let engine = engine();
        let reg = QueryRegistry::compile_all(&engine, [("list", "{ users { id } }")]).unwrap();
        let err = reg.require("secret-report").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("secret-report"), "{msg}");
        assert!(
            !msg.contains("list"),
            "an allowlist should not enumerate itself: {msg}"
        );
    }

    /// A compile error reports a path inside the document, which does not say
    /// which document when a hundred are compiled at once.
    #[tokio::test]
    async fn a_failure_names_the_key_that_failed() {
        let engine = engine();
        let err = QueryRegistry::compile_all(
            &engine,
            [
                ("good", "{ users { id } }"),
                ("bad", "{ users { nonexistent } }"),
            ],
        )
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("bad:"), "{msg}");
        assert!(msg.contains("nonexistent"), "{msg}");
    }

    #[tokio::test]
    async fn a_query_that_cannot_be_compiled_is_refused_at_startup() {
        let engine = engine();
        let err = QueryRegistry::compile_all(
            &engine,
            [(
                "shape",
                "query($w: users_bool_exp) { users(where: $w) { id } }",
            )],
        )
        .unwrap_err();
        assert!(format!("{err}").contains("shape:"), "{err}");
        assert!(matches!(err, Error::NotCompilable { .. }));
    }

    #[tokio::test]
    async fn a_duplicate_key_is_refused_rather_than_overwritten() {
        let engine = engine();
        let err = QueryRegistry::compile_all(
            &engine,
            [("list", "{ users { id } }"), ("list", "{ users { name } }")],
        )
        .unwrap_err();
        assert!(format!("{err}").contains("already registered"), "{err}");

        // …unless the caller says so.
        let mut reg = QueryRegistry::new();
        reg.insert(&engine, "list", "{ users { id } }").unwrap();
        let other = engine.compile("{ users { name } }").unwrap();
        reg.replace("list", other);
        assert_eq!(reg.len(), 1);
    }

    #[tokio::test]
    async fn a_scoped_registry_compiles_the_policy_in() {
        let engine = engine();
        let schema = schema();
        let policy = ScopePolicy::builder()
            .allow("users", col("id").eq(principal()))
            .validate(&schema)
            .unwrap();
        let reg =
            QueryRegistry::compile_all_scoped(&engine, [("mine", "{ users { id } }")], &policy)
                .unwrap();
        assert!(reg.require("mine").unwrap().is_scoped());
    }
}
