//! Public engine API.

use crate::ast::Operation;
use crate::error::{Error, Result};
use crate::parser::parse_and_lower;
use crate::schema::Schema;
use crate::sql::render;
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

pub struct Engine {
    pool: PgPool,
    schema: Arc<Schema>,
}

impl Engine {
    pub fn new(pool: PgPool, schema: Schema) -> Self {
        Self {
            pool,
            schema: Arc::new(schema),
        }
    }

    /// Parse a GraphQL query string, execute against PostgreSQL, return the
    /// Hasura-shaped `data` object as `serde_json::Value`.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query(&self, source: &str, variables: Option<Value>) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let op = parse_and_lower(source, &vars, None, &self.schema)?;
        let (sql, binds) = render(&op, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing");
        crate::executor::execute(&self.pool, &sql, &binds).await
    }

    /// Execute any [`crate::builder::IntoOperation`] (builders, raw `RootField`, or `Operation`).
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        let operation = op.into_operation();
        let (sql, binds) = render(&operation, &self.schema)?;
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
        let data = self.query(source, variables).await?;
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
        let (sql, binds) = render(&operation, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing");
        let data = crate::executor::execute(&self.pool, &sql, &binds).await?;
        unwrap_and_deserialize(data, alias.as_deref())
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
}

impl TxClient {
    /// Same as [`Engine::query`], but runs on the transaction's connection.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query(&mut self, source: &str, variables: Option<Value>) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let op = parse_and_lower(source, &vars, None, &self.schema)?;
        let (sql, binds) = render(&op, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing in tx");
        crate::executor::execute_on(&mut *self.tx, &sql, &binds).await
    }

    /// Same as [`Engine::run`], but runs on the transaction's connection.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&mut self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        let operation = op.into_operation();
        let (sql, binds) = render(&operation, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing in tx");
        crate::executor::execute_on(&mut *self.tx, &sql, &binds).await
    }

    /// Same as [`Engine::query_as`], but runs on the transaction's connection.
    pub async fn query_as<T: DeserializeOwned>(
        &mut self,
        source: &str,
        variables: Option<Value>,
    ) -> Result<T> {
        let data = self.query(source, variables).await?;
        unwrap_and_deserialize(data, None)
    }

    /// Same as [`Engine::run_as`], but runs on the transaction's connection.
    pub async fn run_as<T: DeserializeOwned>(
        &mut self,
        op: impl crate::builder::IntoOperation,
    ) -> Result<T> {
        let operation = op.into_operation();
        let alias = single_root_alias(&operation).map(String::from);
        let (sql, binds) = render(&operation, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing in tx");
        let data = crate::executor::execute_on(&mut *self.tx, &sql, &binds).await?;
        unwrap_and_deserialize(data, alias.as_deref())
    }
}
