//! Public engine API.

use crate::ast::Operation;
use crate::backend::Backend;
use crate::compiled::CompiledQuery;
use crate::error::{Error, Result};
use crate::limits::ExecutionLimits;
use crate::parse_cache::ParseCache;
use crate::policy::ScopePolicy;
use crate::predicate::Principal;
use crate::schema::Schema;
use crate::scope::{apply_scope, ScopeSet};
use crate::sql::render;
use crate::types::{Bind, Inputs};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::postgres::Postgres;
use sqlx::Pool;
use std::future::Future;
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
pub(crate) fn single_root_alias(op: &Operation) -> Option<&str> {
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
pub(crate) fn prepare_symbolic(
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
) -> Result<(String, Vec<Bind>)> {
    let (sql, specs) = prepare_symbolic(op, schema, limits)?;
    let binds = crate::types::resolve_binds(&specs, &Inputs::none())?;
    Ok((sql, binds))
}

/// Where one statement goes: the engine's pool, a caller's executor, or a
/// transaction's connection.
///
/// Private, and the reason every executing method has one body: the three
/// kinds of target cannot share sqlx's `Executor` in code generic over the
/// backend. sqlx implements `Executor` for `&Pool<DB>` only under a
/// higher-ranked bound on `&mut DB::Connection` that would have to be
/// repeated on every public impl block here, and for the connection types
/// one driver at a time. This trait has one impl per kind and no such bound;
/// [`Backend`] supplies the driver-specific step.
trait Run<DB: Backend>: Send {
    fn run(self, sql: &str, binds: &[Bind]) -> impl Future<Output = Result<Value>> + Send;

    /// What to log the target as. The type is what tells a pool run from one
    /// on a borrowed connection — the distinction an operator needs when a
    /// statement did not see a row the transaction beside it just wrote.
    fn name(&self) -> &'static str;
}

impl<DB: Backend> Run<DB> for &Pool<DB> {
    async fn run(self, sql: &str, binds: &[Bind]) -> Result<Value> {
        let mut conn = self.acquire().await?;
        DB::execute_conn(&mut conn, sql, binds).await
    }

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

impl<DB: Backend> Run<DB> for &mut DB::Connection {
    fn run(self, sql: &str, binds: &[Bind]) -> impl Future<Output = Result<Value>> + Send {
        DB::execute_conn(self, sql, binds)
    }

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// A caller-supplied sqlx executor, as the `_on` methods take. The lifetime
/// is the executor's own (`Executor<'c>`), carried here so the impl can name
/// it.
struct External<'c, E>(E, std::marker::PhantomData<&'c ()>);

impl<'c, DB: Backend, E: sqlx::Executor<'c, Database = DB>> Run<DB> for External<'c, E> {
    fn run(self, sql: &str, binds: &[Bind]) -> impl Future<Output = Result<Value>> + Send {
        DB::execute(self.0, sql, binds)
    }

    fn name(&self) -> &'static str {
        std::any::type_name::<E>()
    }
}

pub struct Engine<DB: Backend = Postgres> {
    pool: Pool<DB>,
    schema: Arc<Schema>,
    parse_cache: Arc<ParseCache>,
    limits: ExecutionLimits,
}

/// Log what [`Schema::warnings`] found, once, at engine construction.
///
/// Here rather than at `SchemaBuilder::build`: every serving deployment passes
/// through an `Engine` constructor exactly once per engine, while schemas are
/// also built by tooling (SDL export, `vision-gql diff`) that reports the same
/// warnings on its own channel and does not want a second, unfilterable copy
/// on stderr.
fn log_schema_warnings(schema: &Schema) {
    for w in schema.warnings() {
        tracing::warn!(target: "vision_graphql::schema", "{w}");
    }
}

impl<DB: Backend> Engine<DB> {
    pub fn new(pool: Pool<DB>, schema: Schema) -> Self {
        log_schema_warnings(&schema);
        Self {
            pool,
            schema: Arc::new(schema),
            parse_cache: Arc::new(ParseCache::default()),
            limits: ExecutionLimits::default(),
        }
    }

    /// Same as [`Engine::new`], with an explicit parse-cache capacity.
    /// `capacity == 0` parses every request from scratch.
    pub fn with_parse_cache_capacity(pool: Pool<DB>, schema: Schema, capacity: usize) -> Self {
        log_schema_warnings(&schema);
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
    pub fn with_parse_cache(pool: Pool<DB>, schema: Schema, parse_cache: Arc<ParseCache>) -> Self {
        log_schema_warnings(&schema);
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
        lower_source(
            &self.parse_cache,
            &self.schema,
            source,
            vars,
            operation_name,
        )
    }

    /// Lower and run a text operation on `target`. Every text method on this
    /// engine — pool or `_on` — comes through here.
    async fn text_to<R: Run<DB>>(
        &self,
        target: R,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let op = self.lower(source, &vars, operation_name)?;
        run_operation(target, op, &self.schema, &self.limits, false).await
    }

    /// Parse a GraphQL query string, execute against the database, return the
    /// Hasura-shaped `data` object as `serde_json::Value`.
    ///
    /// A document holding more than one operation needs
    /// [`Engine::query_with`] to say which.
    ///
    /// Every executing method on this engine has an `_on` twin that runs on
    /// a caller-supplied connection instead of the pool — see
    /// [`Engine::query_on`] — and the pool method is that twin bound to the
    /// engine's own pool.
    pub async fn query(&self, source: &str, variables: Option<Value>) -> Result<Value> {
        self.query_with(source, variables, None).await
    }

    /// [`Engine::query`] on a caller-supplied executor: the engine's pool
    /// (`&pool`), a connection borrowed from a transaction the caller holds
    /// (`&mut *tx`), or any other [`sqlx::Executor`] of this engine's backend.
    ///
    /// The engine begins, commits and rolls back nothing here. This is the
    /// entry point for a host that owns the transaction — because its
    /// lifetime, its timeout and the other statements in it are the host's
    /// business — and wants this engine's statements to run in it. A
    /// statement sees what the connection sees, uncommitted rows included.
    pub async fn query_on<'c, E: sqlx::Executor<'c, Database = DB>>(
        &self,
        executor: E,
        source: &str,
        variables: Option<Value>,
    ) -> Result<Value> {
        self.query_with_on(executor, source, variables, None).await
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
        self.text_to(&self.pool, source, variables, operation_name)
            .await
    }

    /// [`Engine::query_with`] on a caller-supplied executor; see
    /// [`Engine::query_on`].
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query_with_on<'c, E: sqlx::Executor<'c, Database = DB>>(
        &self,
        executor: E,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<Value> {
        self.text_to(
            External(executor, std::marker::PhantomData),
            source,
            variables,
            operation_name,
        )
        .await
    }

    /// Execute any [`crate::builder::IntoOperation`] (builders, raw `RootField`, or `Operation`).
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        run_operation(
            &self.pool,
            op.into_operation(),
            &self.schema,
            &self.limits,
            false,
        )
        .await
    }

    /// [`Engine::run`] on a caller-supplied executor; see [`Engine::query_on`].
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run_on<'c, E: sqlx::Executor<'c, Database = DB>>(
        &self,
        executor: E,
        op: impl crate::builder::IntoOperation,
    ) -> Result<Value> {
        run_operation(
            External(executor, std::marker::PhantomData),
            op.into_operation(),
            &self.schema,
            &self.limits,
            false,
        )
        .await
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

    /// [`Engine::query_as`] on a caller-supplied executor; see
    /// [`Engine::query_on`].
    pub async fn query_as_on<'c, E: sqlx::Executor<'c, Database = DB>, T: DeserializeOwned>(
        &self,
        executor: E,
        source: &str,
        variables: Option<Value>,
    ) -> Result<T> {
        self.query_as_with_on(executor, source, variables, None)
            .await
    }

    /// [`Engine::query_as_with`] on a caller-supplied executor; see
    /// [`Engine::query_on`].
    pub async fn query_as_with_on<'c, E: sqlx::Executor<'c, Database = DB>, T: DeserializeOwned>(
        &self,
        executor: E,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<T> {
        let data = self
            .query_with_on(executor, source, variables, operation_name)
            .await?;
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
        let operation = op.into_operation();
        let alias = single_root_alias(&operation).map(String::from);
        let data = run_operation(&self.pool, operation, &self.schema, &self.limits, false).await?;
        unwrap_and_deserialize(data, alias.as_deref())
    }

    /// [`Engine::run_as`] on a caller-supplied executor; see
    /// [`Engine::query_on`].
    pub async fn run_as_on<'c, E: sqlx::Executor<'c, Database = DB>, T: DeserializeOwned>(
        &self,
        executor: E,
        op: impl crate::builder::IntoOperation,
    ) -> Result<T> {
        let operation = op.into_operation();
        let alias = single_root_alias(&operation).map(String::from);
        let data = run_operation(
            External(executor, std::marker::PhantomData),
            operation,
            &self.schema,
            &self.limits,
            false,
        )
        .await?;
        unwrap_and_deserialize(data, alias.as_deref())
    }

    /// Lower `source` once, with variables left symbolic, and render it to SQL.
    ///
    /// The result runs with any variables via [`Engine::execute`]. See
    /// [`crate::compiled`] for which queries can be compiled — a variable in a
    /// position that decides the shape of the SQL cannot be, and yields
    /// [`Error::NotCompilable`].
    pub fn compile(&self, source: &str) -> Result<CompiledQuery<DB>> {
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
    pub fn compile_scoped(&self, source: &str, policy: &ScopePolicy) -> Result<CompiledQuery<DB>> {
        self.compile_inner(source, None, Some(policy))
    }

    /// [`Engine::compile`] / [`Engine::compile_scoped`] with an explicit
    /// operation name, for documents holding more than one operation.
    pub fn compile_with(
        &self,
        source: &str,
        operation_name: Option<&str>,
        policy: Option<&ScopePolicy>,
    ) -> Result<CompiledQuery<DB>> {
        self.compile_inner(source, operation_name, policy)
    }

    fn compile_inner(
        &self,
        source: &str,
        operation_name: Option<&str>,
        policy: Option<&ScopePolicy>,
    ) -> Result<CompiledQuery<DB>> {
        let doc = self.parse_cache.get(source)?;
        crate::compiled::compile(&doc, operation_name, policy, &self.schema, &self.limits)
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
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
    ) -> Result<Value> {
        execute_compiled(&self.pool, compiled, variables, None).await
    }

    /// [`Engine::execute`] on a caller-supplied executor; see
    /// [`Engine::query_on`]. The same guard applies: a statement compiled
    /// against a policy is refused without a principal.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn execute_on<'c, E: sqlx::Executor<'c, Database = DB>>(
        &self,
        executor: E,
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
    ) -> Result<Value> {
        execute_compiled(
            External(executor, std::marker::PhantomData),
            compiled,
            variables,
            None,
        )
        .await
    }

    /// Run a statement compiled by [`Engine::compile_scoped`], binding
    /// `principal` into the policy's predicates.
    ///
    /// Refuses a statement that was compiled without a policy, since its SQL
    /// carries no predicates and the principal would silently have no effect.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn execute_scoped(
        &self,
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
        principal: &Principal,
    ) -> Result<Value> {
        execute_compiled(&self.pool, compiled, variables, Some(principal)).await
    }

    /// [`Engine::execute_scoped`] on a caller-supplied executor; see
    /// [`Engine::query_on`].
    ///
    /// This is how a host that holds its own transaction runs a
    /// policy-compiled statement inside it, beside whatever else the
    /// transaction carries, with the policy's predicates, the post-insert
    /// check and the principal binding all still the engine's: the host
    /// lends the connection, the engine executes. The principal is the
    /// caller's to supply per call, as on the pool.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn execute_scoped_on<'c, E: sqlx::Executor<'c, Database = DB>>(
        &self,
        executor: E,
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
        principal: &Principal,
    ) -> Result<Value> {
        execute_compiled(
            External(executor, std::marker::PhantomData),
            compiled,
            variables,
            Some(principal),
        )
        .await
    }

    /// Same as [`Engine::execute`], unwrapping the single root field and
    /// deserializing into `T`.
    pub async fn execute_as<T: DeserializeOwned>(
        &self,
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
    ) -> Result<T> {
        let data = self.execute(compiled, variables).await?;
        unwrap_and_deserialize(data, compiled.root_alias.as_deref())
    }

    /// Same as [`Engine::execute_scoped`], unwrapping the single root field and
    /// deserializing into `T`.
    pub async fn execute_scoped_as<T: DeserializeOwned>(
        &self,
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
        principal: &Principal,
    ) -> Result<T> {
        let data = self.execute_scoped(compiled, variables, principal).await?;
        unwrap_and_deserialize(data, compiled.root_alias.as_deref())
    }

    /// [`Engine::execute_as`] on a caller-supplied executor; see
    /// [`Engine::query_on`].
    pub async fn execute_as_on<'c, E: sqlx::Executor<'c, Database = DB>, T: DeserializeOwned>(
        &self,
        executor: E,
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
    ) -> Result<T> {
        let data = self.execute_on(executor, compiled, variables).await?;
        unwrap_and_deserialize(data, compiled.root_alias.as_deref())
    }

    /// [`Engine::execute_scoped_as`] on a caller-supplied executor; see
    /// [`Engine::execute_scoped_on`].
    pub async fn execute_scoped_as_on<
        'c,
        E: sqlx::Executor<'c, Database = DB>,
        T: DeserializeOwned,
    >(
        &self,
        executor: E,
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
        principal: &Principal,
    ) -> Result<T> {
        let data = self
            .execute_scoped_on(executor, compiled, variables, principal)
            .await?;
        unwrap_and_deserialize(data, compiled.root_alias.as_deref())
    }

    /// Scoped execution handle: every query it runs is rewritten so each
    /// table access point carries the [`ScopeSet`]'s predicate for that
    /// table, and tables without an entry are denied. See [`crate::scope`].
    pub fn scoped(&self, scope: ScopeSet) -> ScopedEngine<'_, DB> {
        ScopedEngine {
            engine: self,
            scope,
        }
    }

    /// Run a closure inside a single database transaction. Every call on
    /// the [`TxClient`] inside the closure — [`query`](TxClient::query),
    /// [`run`](TxClient::run), [`execute`](TxClient::execute) and
    /// [`execute_scoped`](TxClient::execute_scoped) — uses the same
    /// connection and the same tx. `Ok` commits; `Err` rolls back and
    /// the error is returned verbatim. Panics unwind; sqlx's `Drop` impl on
    /// the tx will roll back.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: AsyncFnOnce(&mut TxClient<DB>) -> Result<T>,
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

/// Run a compiled statement on `target`: the shape its `@choices` pick,
/// with variables, defaults and — for a statement compiled against a policy —
/// the principal resolved into binds. The one execution path for compiled
/// statements: the pool, the engine's own transaction and a caller's
/// connection all come through here, and so does the guard that pairs a
/// statement with the right entry point — a scoped statement without a
/// principal would run unscoped, and an unscoped one with a principal would
/// look restricted while restricting nothing.
async fn execute_compiled<DB: Backend, R: Run<DB>>(
    target: R,
    compiled: &CompiledQuery<DB>,
    variables: Option<Value>,
    principal: Option<&Principal>,
) -> Result<Value> {
    match (compiled.scoped, principal) {
        (true, None) => {
            return Err(Error::Scope(
                "this query was compiled against a policy; run it with execute_scoped".into(),
            ))
        }
        (false, Some(_)) => {
            return Err(Error::Scope(
                "this query was compiled without a policy, so a principal would not restrict it; \
                 compile it with compile_scoped"
                    .into(),
            ))
        }
        _ => {}
    }
    let vars = variables.unwrap_or(Value::Object(Default::default()));
    let shape = compiled.shape_for(&vars)?;
    let mut inputs = Inputs::variables(&vars).with_defaults(&compiled.defaults);
    if let Some(principal) = principal {
        inputs = inputs.with_principal(principal);
    }
    let binds = crate::types::resolve_binds(&shape.specs, &inputs)?;
    tracing::debug!(
        target: "vision_graphql::engine",
        sql = %shape.sql,
        binds = binds.len(),
        scoped = compiled.scoped,
        executor = target.name(),
        "executing compiled"
    );
    target.run(&shape.sql, &binds).await
}

/// Prepare an already-lowered (and, when `scoped`, already-rewritten)
/// operation and run it on `target`. The one execution path for text and
/// builder operations, as [`execute_compiled`] is for compiled ones.
async fn run_operation<DB: Backend, R: Run<DB>>(
    target: R,
    mut op: Operation,
    schema: &Schema,
    limits: &ExecutionLimits,
    scoped: bool,
) -> Result<Value> {
    let (sql, binds) = prepare(&mut op, schema, limits)?;
    tracing::debug!(
        target: "vision_graphql::engine",
        %sql,
        binds = binds.len(),
        scoped,
        executor = target.name(),
        "executing"
    );
    target.run(&sql, &binds).await
}

/// Parse (via the cache) and lower `source`. The one lowering step for every
/// text entry point — engine, transaction, scoped or not — so a pre-lowering
/// hook or a cache change reaches all of them.
fn lower_source(
    parse_cache: &ParseCache,
    schema: &Schema,
    source: &str,
    vars: &Value,
    operation_name: Option<&str>,
) -> Result<Operation> {
    let doc = parse_cache.get(source)?;
    crate::parser::lower(&doc, vars, operation_name, schema)
}

/// A handle to an open database transaction that exposes the same query
/// surface as [`Engine`] — text, builder and compiled statements alike.
/// Obtained via [`Engine::transaction`]; cannot be constructed directly.
/// Methods take `&mut self` because the underlying connection is exclusively
/// borrowed per statement.
///
/// A scoped statement runs here with its principal, as it does on the pool
/// ([`TxClient::execute_scoped`]). For a transaction that must stay inside
/// one [`ScopeSet`] whatever the closure does, see
/// [`ScopedEngine::transaction`].
pub struct TxClient<DB: Backend = Postgres> {
    tx: sqlx::Transaction<'static, DB>,
    schema: Arc<Schema>,
    parse_cache: Arc<ParseCache>,
    limits: ExecutionLimits,
}

impl<DB: Backend> TxClient<DB> {
    /// Same as [`Engine::query`], but runs on the transaction's connection.
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
        let op = lower_source(
            &self.parse_cache,
            &self.schema,
            source,
            &vars,
            operation_name,
        )?;
        run_operation::<DB, _>(&mut *self.tx, op, &self.schema, &self.limits, false).await
    }

    /// Same as [`Engine::run`], but runs on the transaction's connection.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&mut self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        run_operation::<DB, _>(
            &mut *self.tx,
            op.into_operation(),
            &self.schema,
            &self.limits,
            false,
        )
        .await
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
        let operation = op.into_operation();
        let alias = single_root_alias(&operation).map(String::from);
        let data =
            run_operation::<DB, _>(&mut *self.tx, operation, &self.schema, &self.limits, false)
                .await?;
        unwrap_and_deserialize(data, alias.as_deref())
    }

    /// Same as [`Engine::execute`], but runs on the transaction's connection.
    /// A statement compiled against a policy is refused here as there.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn execute(
        &mut self,
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
    ) -> Result<Value> {
        execute_compiled::<DB, _>(&mut *self.tx, compiled, variables, None).await
    }

    /// Same as [`Engine::execute_scoped`], but runs on the transaction's
    /// connection: a statement compiled against a policy, with `principal`
    /// bound into its predicates. This is what lets a host whose documents
    /// are compiled statements run several of them atomically without
    /// giving up the policy for the duration.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn execute_scoped(
        &mut self,
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
        principal: &Principal,
    ) -> Result<Value> {
        execute_compiled::<DB, _>(&mut *self.tx, compiled, variables, Some(principal)).await
    }

    /// Same as [`TxClient::execute`], unwrapping the single root field and
    /// deserializing into `T`.
    pub async fn execute_as<T: DeserializeOwned>(
        &mut self,
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
    ) -> Result<T> {
        let data = self.execute(compiled, variables).await?;
        unwrap_and_deserialize(data, compiled.root_alias.as_deref())
    }

    /// Same as [`TxClient::execute_scoped`], unwrapping the single root field
    /// and deserializing into `T`.
    pub async fn execute_scoped_as<T: DeserializeOwned>(
        &mut self,
        compiled: &CompiledQuery<DB>,
        variables: Option<Value>,
        principal: &Principal,
    ) -> Result<T> {
        let data = self.execute_scoped(compiled, variables, principal).await?;
        unwrap_and_deserialize(data, compiled.root_alias.as_deref())
    }
}

/// Scoped counterpart of [`Engine`], obtained via [`Engine::scoped`]. Mirrors
/// the same query surface; every operation passes through the scope rewrite
/// before rendering. Scoped `update`/`delete` (and their `_by_pk` forms) inject
/// the predicate as a filter; `insert` injects it as a post-insert check,
/// enforced at every nested level.
pub struct ScopedEngine<'e, DB: Backend = Postgres> {
    engine: &'e Engine<DB>,
    scope: ScopeSet,
}

impl<DB: Backend> ScopedEngine<'_, DB> {
    /// The scope rewrite, then execution on `target`. Every executing
    /// method on this handle comes through here, so none can reach the
    /// renderer unscoped. ([`ScopedEngine::transaction`] hands out a
    /// [`ScopedTxClient`], which has its own copy of the same two steps.)
    async fn run_scoped_to<R: Run<DB>>(&self, target: R, mut op: Operation) -> Result<Value> {
        apply_scope(&mut op, &self.scope, &self.engine.schema)?;
        run_operation(target, op, &self.engine.schema, &self.engine.limits, true).await
    }

    /// Lower, then [`run_scoped_to`](Self::run_scoped_to).
    async fn text_to<R: Run<DB>>(
        &self,
        target: R,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let op = self.engine.lower(source, &vars, operation_name)?;
        self.run_scoped_to(target, op).await
    }

    /// Same as [`Engine::query`], with the scope rewrite applied.
    ///
    /// As on [`Engine`], every executing method here has an `_on` twin that
    /// runs on a caller-supplied executor ([`ScopedEngine::query_on`]); the
    /// rewrite is the same either way, so the set holds on the caller's
    /// connection exactly as on the pool.
    pub async fn query(&self, source: &str, variables: Option<Value>) -> Result<Value> {
        self.query_with(source, variables, None).await
    }

    /// [`ScopedEngine::query`] on a caller-supplied executor; see
    /// [`Engine::query_on`] for what that means. The [`ScopeSet`] is this
    /// handle's, fixed when it was made; the connection is the caller's.
    pub async fn query_on<'c, E: sqlx::Executor<'c, Database = DB>>(
        &self,
        executor: E,
        source: &str,
        variables: Option<Value>,
    ) -> Result<Value> {
        self.query_with_on(executor, source, variables, None).await
    }

    /// [`ScopedEngine::query`] naming the operation to run.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query_with(
        &self,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<Value> {
        self.text_to(&self.engine.pool, source, variables, operation_name)
            .await
    }

    /// [`ScopedEngine::query_with`] on a caller-supplied executor; see
    /// [`ScopedEngine::query_on`].
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query_with_on<'c, E: sqlx::Executor<'c, Database = DB>>(
        &self,
        executor: E,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<Value> {
        self.text_to(
            External(executor, std::marker::PhantomData),
            source,
            variables,
            operation_name,
        )
        .await
    }

    /// Same as [`Engine::run`], with the scope rewrite applied.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        self.run_scoped_to(&self.engine.pool, op.into_operation())
            .await
    }

    /// [`ScopedEngine::run`] on a caller-supplied executor; see
    /// [`ScopedEngine::query_on`].
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run_on<'c, E: sqlx::Executor<'c, Database = DB>>(
        &self,
        executor: E,
        op: impl crate::builder::IntoOperation,
    ) -> Result<Value> {
        self.run_scoped_to(
            External(executor, std::marker::PhantomData),
            op.into_operation(),
        )
        .await
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

    /// [`ScopedEngine::query_as`] on a caller-supplied executor; see
    /// [`ScopedEngine::query_on`].
    pub async fn query_as_on<'c, E: sqlx::Executor<'c, Database = DB>, T: DeserializeOwned>(
        &self,
        executor: E,
        source: &str,
        variables: Option<Value>,
    ) -> Result<T> {
        self.query_as_with_on(executor, source, variables, None)
            .await
    }

    /// [`ScopedEngine::query_as_with`] on a caller-supplied executor; see
    /// [`ScopedEngine::query_on`].
    pub async fn query_as_with_on<'c, E: sqlx::Executor<'c, Database = DB>, T: DeserializeOwned>(
        &self,
        executor: E,
        source: &str,
        variables: Option<Value>,
        operation_name: Option<&str>,
    ) -> Result<T> {
        let data = self
            .query_with_on(executor, source, variables, operation_name)
            .await?;
        unwrap_and_deserialize(data, None)
    }

    /// Same as [`Engine::run_as`], with the scope rewrite applied.
    pub async fn run_as<T: DeserializeOwned>(
        &self,
        op: impl crate::builder::IntoOperation,
    ) -> Result<T> {
        let operation = op.into_operation();
        let alias = single_root_alias(&operation).map(String::from);
        let data = self.run_scoped_to(&self.engine.pool, operation).await?;
        unwrap_and_deserialize(data, alias.as_deref())
    }

    /// [`ScopedEngine::run_as`] on a caller-supplied executor; see
    /// [`ScopedEngine::query_on`].
    pub async fn run_as_on<'c, E: sqlx::Executor<'c, Database = DB>, T: DeserializeOwned>(
        &self,
        executor: E,
        op: impl crate::builder::IntoOperation,
    ) -> Result<T> {
        let operation = op.into_operation();
        let alias = single_root_alias(&operation).map(String::from);
        let data = self
            .run_scoped_to(External(executor, std::marker::PhantomData), operation)
            .await?;
        unwrap_and_deserialize(data, alias.as_deref())
    }

    /// Same as [`Engine::transaction`], but the closure receives a
    /// [`ScopedTxClient`]: there is no way to escape the scope inside the
    /// transaction.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: AsyncFnOnce(&mut ScopedTxClient<DB>) -> Result<T>,
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
///
/// This handle runs text and builder operations only — no compiled
/// statements. Its guarantee is that nothing the closure runs can leave the
/// [`ScopeSet`] it was opened with; a statement compiled against a policy
/// binds a principal per run, and letting the closure choose that principal
/// would be a way out. A transaction over compiled statements is
/// [`Engine::transaction`] with [`TxClient::execute_scoped`], where the
/// caller — not the closure's author — supplies the principal each time.
pub struct ScopedTxClient<DB: Backend = Postgres> {
    tx: sqlx::Transaction<'static, DB>,
    schema: Arc<Schema>,
    parse_cache: Arc<ParseCache>,
    scope: ScopeSet,
    limits: ExecutionLimits,
}

impl<DB: Backend> ScopedTxClient<DB> {
    async fn run_scoped(&mut self, mut op: Operation) -> Result<Value> {
        apply_scope(&mut op, &self.scope, &self.schema)?;
        run_operation::<DB, _>(&mut *self.tx, op, &self.schema, &self.limits, true).await
    }

    /// Same as [`TxClient::query`], with the scope rewrite applied.
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
        let op = lower_source(
            &self.parse_cache,
            &self.schema,
            source,
            &vars,
            operation_name,
        )?;
        self.run_scoped(op).await
    }

    /// Same as [`TxClient::run`], with the scope rewrite applied.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&mut self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        self.run_scoped(op.into_operation()).await
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
        let data = self.run_scoped(operation).await?;
        unwrap_and_deserialize(data, alias.as_deref())
    }
}
