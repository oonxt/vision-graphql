//! Public engine API.

use crate::error::Result;
use crate::parser::parse_and_lower;
use crate::schema::Schema;
use crate::sql::render;
use deadpool_postgres::{Pool, Transaction as DeadpoolTx};
use serde_json::Value;
use std::sync::Arc;

pub struct Engine {
    pool: Pool,
    schema: Arc<Schema>,
}

impl Engine {
    pub fn new(pool: Pool, schema: Schema) -> Self {
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

    /// Run a closure inside a single PostgreSQL transaction. Every call to
    /// [`TxClient::query`] / [`TxClient::run`] inside the closure uses the
    /// same connection and the same tx. `Ok` commits; `Err` rolls back and
    /// the error is returned verbatim. Panics unwind; tokio-postgres's
    /// `Drop` impl on the tx will roll back.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'tx> AsyncFnOnce(TxClient<'tx>) -> Result<T>,
    {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let result = {
            let tc = TxClient {
                tx: &tx,
                schema: self.schema.clone(),
            };
            f(tc).await
        };
        match result {
            Ok(v) => {
                tx.commit().await?;
                Ok(v)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }
}

/// A handle to an open PostgreSQL transaction that exposes the same query
/// surface as [`Engine`]. Obtained via [`Engine::transaction`]; cannot be
/// constructed directly.
pub struct TxClient<'tx> {
    tx: &'tx DeadpoolTx<'tx>,
    schema: Arc<Schema>,
}

impl<'tx> TxClient<'tx> {
    /// Same as [`Engine::query`], but runs on the transaction's connection.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query(&self, source: &str, variables: Option<Value>) -> Result<Value> {
        let vars = variables.unwrap_or(Value::Object(Default::default()));
        let op = parse_and_lower(source, &vars, None, &self.schema)?;
        let (sql, binds) = render(&op, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing in tx");
        crate::executor::execute_on(self.tx, &sql, &binds).await
    }

    /// Same as [`Engine::run`], but runs on the transaction's connection.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&self, op: impl crate::builder::IntoOperation) -> Result<Value> {
        let operation = op.into_operation();
        let (sql, binds) = render(&operation, &self.schema)?;
        tracing::debug!(target: "vision_graphql::engine", %sql, binds = binds.len(), "executing in tx");
        crate::executor::execute_on(self.tx, &sql, &binds).await
    }
}
