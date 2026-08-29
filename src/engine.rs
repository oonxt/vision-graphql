//! Public engine API.

use crate::ast::Operation;
use crate::compiled::CompiledQuery;
use crate::error::{Error, Result};
use crate::limits::ExecutionLimits;
use crate::parse_cache::ParseCache;
use crate::policy::ScopePolicy;
use crate::predicate::Principal;
use crate::schema::Schema;
use crate::scope::{apply_scope, ScopeSet};
use crate::sql::render;
use crate::types::Inputs;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::postgres::Postgres;
use sqlx::PgPool;
use std::sync::Arc;

/// Typed shape of an `insert` / `update` / `delete` mutation result:
/// `{ "affected_rows": N, "returning": [...] }`. `returning` deserializes to
/// an empty `Vec` when the mutation did not request it.
#[derive(Debug, serde::Deserialize)]
pub struct MutationResult<T> {
    pub affected_rows: u64,
    #[serde(default = "Vec::new")]
    pub returning: Vec<T>,
}

/// When an operation has exactly one root field, return its response alias so
/// typed APIs can unwrap the Hasura data envelope (`{"users": [...]}` → `[...]`).
fn single_root_alias(op: &Operation) -> Option<&str> {
    match op {
        Operation::Query(roots) if roots.len() == 1 => Some(&roots[0].alias),
        Operation::Mutation(fields) if fields.len() == 1 => Some(fields[0].alias()),
        _ => None,
    }
}

fn unwrap_and_deserialize<T: DeserializeOwned>(mut data: Value, alias: Option<&str>) -> Result<T> {
    let payload = match alias {
        Some(a) => data
            .get_mut(a)
            .map(Value::take)
            .ok_or_else(|| Error::Decode(format!("root field '{a}' missing in result")))?,
        None => data,
    };
    serde_json::from_value(payload).map_err(|e| Error::Decode(e.to_string()))
}

/// Apply the limits, then render, leaving parameters symbolic. The one
/// pipeline every entry point shares, in this order: a new one that called
/// `render` directly would run unbounded, with nothing to notice it. The
/// compile path keeps the symbolic specs; the eager paths resolve them at once
/// via [`prepare`]. A pass added here reaches compiled and persisted
/// statements and one-shot requests alike — `compile_inner` used to re-spell
/// this inline, which is exactly how it would have missed the next pass.
fn prepare_symbolic(
    op: &mut Operation,
    schema: &Schema,
    limits: &ExecutionLimits,
) -> Result<(String, Vec<crate::types::BindSpec>)> {
    limits.apply(op, schema)?;
    render(op, schema)
}

/// [`prepare_symbolic`] for the fully-literal paths: every parameter resolves
/// now, with no variables and no principal.
fn prepare(
    op: &mut Operation,
    schema: &Schema,
    limits: &ExecutionLimits,
) -> Result<(String, Vec<crate::types::Bind>)> {
    let (sql, specs) = prepare_symbolic(op, schema, limits)?;
    let binds = crate::types::resolve_binds(&specs, &Inputs::none())?;
    Ok((sql, binds))
}

pub struct Engine {
    pool: PgPool,
    schema: Arc<Schema>,
    parse_cache: Arc<ParseCache>,
    limits: ExecutionLimits,
}

impl Engine {
    pub fn new(pool: PgPool, schema: Schema) -> Self {
        Self {
            pool,
            schema: Arc::new(schema),
            parse_cache: Arc::new(ParseCache::default()),
            limits: ExecutionLimits::default(),
        }
    }

    /// Same as [`Engine::new`], with an explicit parse-cache capacity.
    /// `capacity == 0` parses every request from scratch.
    pub fn with_parse_cache_capacity(pool: PgPool, schema: Schema, capacity: usize) -> Self {
        Self {
            pool,
            schema: Arc::new(schema),
            parse_cache: Arc::new(ParseCache::new(capacity)),
            limits: ExecutionLimits::default(),
        }
    }

    /// Same as [`Engine::new`] on a caller-owned [`ParseCache`].
    ///
    /// Two reasons to reach for this: to set [`ParseLimits`](crate::ParseLimits)
    /// other than the defaults, and to share one cache across several engines.
    /// Parsing is schema-independent, so an application running a separate
    /// engine per role — the way per-role column visibility is expressed — would
    /// otherwise parse the same document once per role.
    pub fn with_parse_cache(pool: PgPool, schema: Schema, parse_cache: Arc<ParseCache>) -> Self {
        Self {
            pool,
            schema: Arc::new(schema),
            parse_cache,
            limits: ExecutionLimits::default(),
        }
    }

    /// Bound what one request may cost. Unbounded by default — see
    /// [`ExecutionLimits`].
    ///
    /// Applies to every path: GraphQL strings, the typed builder, compiled
    /// statements, scoped handles and transactions alike, since all of them go
    /// through the IR these are checked on.
    pub fn with_limits(mut self, limits: ExecutionLimits) -> Self {
        self.limits = limits;
        self
    }

    /// The limits this engine applies.
    pub fn limits(&self) -> &ExecutionLimits {
        &self.limits
    }

    /// The schema this engine answers with — post-overlay, so exposed names,
    /// hidden columns and manual relations are all as the engine will actually
    /// serve them.
    ///
    /// Exists for hosts that validate *configuration* against the engine: a
    /// deployment that generates queries from config needs to check its tables,
    /// columns and keys against what this engine publishes, and re-running
    /// introspection to do so would validate against a schema that can drift
    /// from this one (a different overlay, a table created since).
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// The shared document cache. Exposed for `clear()` and for size
    /// inspection; every handle spawned from this engine uses the same one.
    pub fn parse_cache(&self) -> &Arc<ParseCache> {
        &self.parse_cache
    }

    /// Parse (via the cache) and lower `source` against this engine's schema.
    fn lower(&self, source: &str, vars: &Value, operation_name: Option<&str>) -> Result<Operation> {
        let doc = self.parse_cache.get(source)?;
        crate::parser::lower(&doc, vars, operation_name, &self.schema)
    }

    /// Parse a GraphQL query string, execute against PostgreSQL, return the
    /// Hasura-shaped `data` object as `serde_json::Value`.
    ///
    /// A document holding more than one operation needs
    /// [`Engine::query_with`] to say which.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query(&self, source: &str, variables: Option<Value>) -> Result<Value> {
        self.query_with(source, variables, None).await
    }

    /// [`Engine::query`] naming the operation to run.
    ///
    /// This is the third field of a GraphQL request body, beside `query` and
    /// `variables`: a client that ships one document holding every operation it
    /// might send picks one per request by name. Without it such a document can
    /// only be run through [`Engine::compile_with`], which has always taken one.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query_with(
        &self,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let mut op = self.lower(source, &vars, operation_name)?;
        let (sql, binds) = prepare(&mut op, &self.schema, &self.limits)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing");
        crate::executor::execute(&self.pool, &sql, &binds).await
    }

    /// Execute any [`crate::builder::IntoOperation`] (builders, raw `RootField`, or `Operation`).
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        let mut operation = op.into_operation();
        let (sql, binds) = prepare(&mut operation, &self.schema, &self.limits)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing");
        crate::executor::execute(&self.pool, &sql, &binds).await
    }

    /// Same as [`Engine::query`], but deserializes the whole Hasura `data`
    /// object into `T`. `T` must mirror the response envelope, e.g.
    /// `struct Data { users: Vec<User> }`.
    pub async fn query_as<T: DeserializeOwned>(
        &self,
        source: &str,
        variables: Option<Value>,
    ) -> Result<T> {
        self.query_as_with(source, variables, None).await
    }

    /// [`Engine::query_as`] naming the operation to run.
    pub async fn query_as_with<T: DeserializeOwned>(
        &self,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<T> {
        let data = self.query_with(source, variables, operation_name).await?;
        unwrap_and_deserialize(data, None)
    }

    /// Same as [`Engine::run`], but unwraps the single root field and
    /// deserializes its payload into `T`:
    ///
    /// - `Query::from(..)` → `Vec<Row>`
    /// - `Query::by_pk(..)` → `Option<Row>`
    /// - `Mutation::insert(..)` / `update` / `delete` → [`MutationResult<Row>`]
    /// - `*_by_pk` mutations → `Option<Row>`
    pub async fn run_as<T: DeserializeOwned>(
        &self,
        op: impl crate::builder::IntoOperation,
    ) -> Result<T> {
        let mut operation = op.into_operation();
        let alias = single_root_alias(&operation).map(String::from);
        let (sql, binds) = prepare(&mut operation, &self.schema, &self.limits)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing");
        let data = crate::executor::execute(&self.pool, &sql, &binds).await?;
        unwrap_and_deserialize(data, alias.as_deref())
    }

    /// Lower `source` once, with variables left symbolic, and render it to SQL.
    ///
    /// The result runs with any variables via [`Engine::execute`]. See
    /// [`crate::compiled`] for which queries can be compiled — a variable in a
    /// position that decides the shape of the SQL cannot be, and yields
    /// [`Error::NotCompilable`].
    pub fn compile(&self, source: &str) -> Result<CompiledQuery> {
        self.compile_inner(source, None, None)
    }

    /// Same as [`Engine::compile`], with `policy`'s predicates applied to every
    /// table access point.
    ///
    /// The policy is applied *symbolically*: the compiled SQL carries the
    /// predicates, but which rows they admit is decided per request by the
    /// principal passed to [`Engine::execute_scoped`]. One statement therefore
    /// serves every principal, and — because tables absent from the policy are
    /// denied at compile time — a table the policy does not mention fails here
    /// rather than at request time.
    pub fn compile_scoped(&self, source: &str, policy: &ScopePolicy) -> Result<CompiledQuery> {
        self.compile_inner(source, None, Some(policy))
    }

    /// [`Engine::compile`] / [`Engine::compile_scoped`] with an explicit
    /// operation name, for documents holding more than one operation.
    pub fn compile_with(
        &self,
        source: &str,
        operation_name: Option<&str>,
        policy: Option<&ScopePolicy>,
    ) -> Result<CompiledQuery> {
        self.compile_inner(source, operation_name, policy)
    }

    fn compile_inner(
        &self,
        source: &str,
        operation_name: Option<&str>,
        policy: Option<&ScopePolicy>,
    ) -> Result<CompiledQuery> {
        let doc = self.parse_cache.get(source)?;
        let mut op = crate::parser::lower_with(
            &doc,
            crate::parser::Bindings::Symbolic,
            operation_name,
            &self.schema,
        )?;
        if let Some(policy) = policy {
            apply_scope(&mut op, &policy.symbolic(), &self.schema)?;
        }
        let root_alias = single_root_alias(&op).map(String::from);
        let (sql, specs) = prepare_symbolic(&mut op, &self.schema, &self.limits)?;
        Ok(CompiledQuery {
            sql,
            specs,
            root_alias,
            defaults: crate::parser::variable_defaults(&doc, operation_name)?,
            scoped: policy.is_some(),
        })
    }

    /// Run a statement compiled by [`Engine::compile`] with this request's
    /// variables.
    ///
    /// Refuses a statement compiled against a policy: that one needs a
    /// principal, and running it without one would mean running a scoped query
    /// unscoped.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn execute(
        &self,
        compiled: &CompiledQuery,
        variables: Option<Value>,
    ) -> Result<Value> {
        if compiled.scoped {
            return Err(Error::Scope(
                "this query was compiled against a policy; run it with execute_scoped".into(),
            ));
        }
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let inputs = Inputs::variables(&vars).with_defaults(&compiled.defaults);
        let binds = crate::types::resolve_binds(&compiled.specs, &inputs)?;
        tracing::debug!(target: "vision_graphql::engine", sql = %compiled.sql, binds = binds.len(), "executing compiled");
        crate::executor::execute(&self.pool, &compiled.sql, &binds).await
    }

    /// Run a statement compiled by [`Engine::compile_scoped`], binding
    /// `principal` into the policy's predicates.
    ///
    /// Refuses a statement that was compiled without a policy, since its SQL
    /// carries no predicates and the principal would silently have no effect.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn execute_scoped(
        &self,
        compiled: &CompiledQuery,
        variables: Option<Value>,
        principal: &Principal,
    ) -> Result<Value> {
        if !compiled.scoped {
            return Err(Error::Scope(
                "this query was compiled without a policy, so a principal would not restrict it; \
                 compile it with compile_scoped"
                    .into(),
            ));
        }
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let inputs = Inputs::variables(&vars)
            .with_defaults(&compiled.defaults)
            .with_principal(principal);
        let binds = crate::types::resolve_binds(&compiled.specs, &inputs)?;
        tracing::debug!(target: "vision_graphql::engine", sql = %compiled.sql, binds = binds.len(), "executing compiled scoped");
        crate::executor::execute(&self.pool, &compiled.sql, &binds).await
    }

    /// Same as [`Engine::execute`], unwrapping the single root field and
    /// deserializing into `T`.
    pub async fn execute_as<T: DeserializeOwned>(
        &self,
        compiled: &CompiledQuery,
        variables: Option<Value>,
    ) -> Result<T> {
        let data = self.execute(compiled, variables).await?;
        unwrap_and_deserialize(data, compiled.root_alias.as_deref())
    }

    /// Same as [`Engine::execute_scoped`], unwrapping the single root field and
    /// deserializing into `T`.
    pub async fn execute_scoped_as<T: DeserializeOwned>(
        &self,
        compiled: &CompiledQuery,
        variables: Option<Value>,
        principal: &Principal,
    ) -> Result<T> {
        let data = self.execute_scoped(compiled, variables, principal).await?;
        unwrap_and_deserialize(data, compiled.root_alias.as_deref())
    }

    /// Scoped execution handle: every query it runs is rewritten so each
    /// table access point carries the [`ScopeSet`]'s predicate for that
    /// table, and tables without an entry are denied. See [`crate::scope`].
    pub fn scoped(&self, scope: ScopeSet) -> ScopedEngine<'_> {
        ScopedEngine {
            engine: self,
            scope,
        }
    }

    /// Run a closure inside a single PostgreSQL transaction. Every call to
    /// [`TxClient::query`] / [`TxClient::run`] inside the closure uses the
    /// same connection and the same tx. `Ok` commits; `Err` rolls back and
    /// the error is returned verbatim. Panics unwind; sqlx's `Drop` impl on
    /// the tx will roll back.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: AsyncFnOnce(&mut TxClient) -> Result<T>,
    {
        let tx = self.pool.begin().await?;
        let mut tc = TxClient {
            tx,
            schema: self.schema.clone(),
            parse_cache: self.parse_cache.clone(),
            limits: self.limits,
        };
        match f(&mut tc).await {
            Ok(v) => {
                tc.tx.commit().await?;
                Ok(v)
            }
            Err(e) => {
                let _ = tc.tx.rollback().await;
                Err(e)
            }
        }
    }
}

/// A handle to an open PostgreSQL transaction that exposes the same query
/// surface as [`Engine`]. Obtained via [`Engine::transaction`]; cannot be
/// constructed directly. Methods take `&mut self` because the underlying
/// connection is exclusively borrowed per statement.
pub struct TxClient {
    tx: sqlx::Transaction<'static, Postgres>,
    schema: Arc<Schema>,
    parse_cache: Arc<ParseCache>,
    limits: ExecutionLimits,
}

impl TxClient {
    /// Same as [`Engine::query`], but runs on the transaction's connection.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query(&mut self, source: &str, variables: Option<Value>) -> Result<Value> {
        self.query_with(source, variables, None).await
    }

    /// [`TxClient::query`] naming the operation to run.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query_with(
        &mut self,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let doc = self.parse_cache.get(source)?;
        let mut op = crate::parser::lower(&doc, &vars, operation_name, &self.schema)?;
        let (sql, binds) = prepare(&mut op, &self.schema, &self.limits)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing in tx");
        crate::executor::execute_on(&mut *self.tx, &sql, &binds).await
    }

    /// Same as [`Engine::run`], but runs on the transaction's connection.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&mut self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        let mut operation = op.into_operation();
        let (sql, binds) = prepare(&mut operation, &self.schema, &self.limits)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing in tx");
        crate::executor::execute_on(&mut *self.tx, &sql, &binds).await
    }

    /// Same as [`Engine::query_as`], but runs on the transaction's connection.
    pub async fn query_as<T: DeserializeOwned>(
        &mut self,
        source: &str,
        variables: Option<Value>,
    ) -> Result<T> {
        self.query_as_with(source, variables, None).await
    }

    /// The same, naming the operation to run.
    pub async fn query_as_with<T: DeserializeOwned>(
        &mut self,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<T> {
        let data = self.query_with(source, variables, operation_name).await?;
        unwrap_and_deserialize(data, None)
    }

    /// Same as [`Engine::run_as`], but runs on the transaction's connection.
    pub async fn run_as<T: DeserializeOwned>(
        &mut self,
        op: impl crate::builder::IntoOperation,
    ) -> Result<T> {
        let mut operation = op.into_operation();
        let alias = single_root_alias(&operation).map(String::from);
        let (sql, binds) = prepare(&mut operation, &self.schema, &self.limits)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing in tx");
        let data = crate::executor::execute_on(&mut *self.tx, &sql, &binds).await?;
        unwrap_and_deserialize(data, alias.as_deref())
    }
}

/// Scoped counterpart of [`Engine`], obtained via [`Engine::scoped`]. Mirrors
/// the same query surface; every operation passes through the scope rewrite
/// before rendering. Scoped `update`/`delete` (and their `_by_pk` forms) inject
/// the predicate as a filter; `insert` injects it as a post-insert check,
/// enforced at every nested level.
pub struct ScopedEngine<'e> {
    engine: &'e Engine,
    scope: ScopeSet,
}

impl ScopedEngine<'_> {
    fn prepare(&self, mut op: Operation) -> Result<(String, Vec<crate::types::Bind>)> {
        apply_scope(&mut op, &self.scope, &self.engine.schema)?;
        prepare(&mut op, &self.engine.schema, &self.engine.limits)
    }

    /// Same as [`Engine::query`], with the scope rewrite applied.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query(&self, source: &str, variables: Option<Value>) -> Result<Value> {
        self.query_with(source, variables, None).await
    }

    /// [`ScopedEngine::query`] naming the operation to run.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query_with(
        &self,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let op = self.engine.lower(source, &vars, operation_name)?;
        let (sql, binds) = self.prepare(op)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing scoped");
        crate::executor::execute(&self.engine.pool, &sql, &binds).await
    }

    /// Same as [`Engine::run`], with the scope rewrite applied.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        let (sql, binds) = self.prepare(op.into_operation())?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing scoped");
        crate::executor::execute(&self.engine.pool, &sql, &binds).await
    }

    /// Same as [`Engine::query_as`], with the scope rewrite applied.
    pub async fn query_as<T: DeserializeOwned>(
        &self,
        source: &str,
        variables: Option<Value>,
    ) -> Result<T> {
        self.query_as_with(source, variables, None).await
    }

    /// [`Engine::query_as`] naming the operation to run.
    pub async fn query_as_with<T: DeserializeOwned>(
        &self,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<T> {
        let data = self.query_with(source, variables, operation_name).await?;
        unwrap_and_deserialize(data, None)
    }

    /// Same as [`Engine::run_as`], with the scope rewrite applied.
    pub async fn run_as<T: DeserializeOwned>(
        &self,
        op: impl crate::builder::IntoOperation,
    ) -> Result<T> {
        let operation = op.into_operation();
        let alias = single_root_alias(&operation).map(String::from);
        let (sql, binds) = self.prepare(operation)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing scoped");
        let data = crate::executor::execute(&self.engine.pool, &sql, &binds).await?;
        unwrap_and_deserialize(data, alias.as_deref())
    }

    /// Same as [`Engine::transaction`], but the closure receives a
    /// [`ScopedTxClient`]: there is no way to escape the scope inside the
    /// transaction.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: AsyncFnOnce(&mut ScopedTxClient) -> Result<T>,
    {
        let tx = self.engine.pool.begin().await?;
        let mut tc = ScopedTxClient {
            tx,
            schema: self.engine.schema.clone(),
            parse_cache: self.engine.parse_cache.clone(),
            scope: self.scope.clone(),
            limits: self.engine.limits,
        };
        match f(&mut tc).await {
            Ok(v) => {
                tc.tx.commit().await?;
                Ok(v)
            }
            Err(e) => {
                let _ = tc.tx.rollback().await;
                Err(e)
            }
        }
    }
}

/// Scoped counterpart of [`TxClient`], obtained via
/// [`ScopedEngine::transaction`]. Cannot be constructed directly.
pub struct ScopedTxClient {
    tx: sqlx::Transaction<'static, Postgres>,
    schema: Arc<Schema>,
    parse_cache: Arc<ParseCache>,
    scope: ScopeSet,
    limits: ExecutionLimits,
}

impl ScopedTxClient {
    fn prepare(&self, mut op: Operation) -> Result<(String, Vec<crate::types::Bind>)> {
        apply_scope(&mut op, &self.scope, &self.schema)?;
        prepare(&mut op, &self.schema, &self.limits)
    }

    /// Same as [`TxClient::query`], with the scope rewrite applied.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query(&mut self, source: &str, variables: Option<Value>) -> Result<Value> {
        self.query_with(source, variables, None).await
    }

    /// [`ScopedTxClient::query`] naming the operation to run.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query_with(
        &mut self,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let doc = self.parse_cache.get(source)?;
        let op = crate::parser::lower(&doc, &vars, operation_name, &self.schema)?;
        let (sql, binds) = self.prepare(op)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing scoped in tx");
        crate::executor::execute_on(&mut *self.tx, &sql, &binds).await
    }

    /// Same as [`TxClient::run`], with the scope rewrite applied.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&mut self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        let (sql, binds) = self.prepare(op.into_operation())?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing scoped in tx");
        crate::executor::execute_on(&mut *self.tx, &sql, &binds).await
    }

    /// Same as [`TxClient::query_as`], with the scope rewrite applied.
    pub async fn query_as<T: DeserializeOwned>(
        &mut self,
        source: &str,
        variables: Option<Value>,
    ) -> Result<T> {
        self.query_as_with(source, variables, None).await
    }

    /// The same, naming the operation to run.
    pub async fn query_as_with<T: DeserializeOwned>(
        &mut self,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<T> {
        let data = self.query_with(source, variables, operation_name).await?;
        unwrap_and_deserialize(data, None)
    }

    /// Same as [`TxClient::run_as`], with the scope rewrite applied.
    pub async fn run_as<T: DeserializeOwned>(
        &mut self,
        op: impl crate::builder::IntoOperation,
    ) -> Result<T> {
        let operation = op.into_operation();
        let alias = single_root_alias(&operation).map(String::from);
        let (sql, binds) = self.prepare(operation)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing scoped in tx");
        let data = crate::executor::execute_on(&mut *self.tx, &sql, &binds).await?;
        unwrap_and_deserialize(data, alias.as_deref())
    }
}
