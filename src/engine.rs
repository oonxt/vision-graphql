//! Public engine API.

use crate::error::Result;
use crate::parser::parse_and_lower;
use crate::schema::Schema;
use crate::sql::render;
use deadpool_postgres::Pool;
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
}
