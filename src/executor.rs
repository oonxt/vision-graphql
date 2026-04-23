//! Execute generated SQL against PostgreSQL.

use crate::error::{Error, Result};
use crate::types::Bind;
use deadpool_postgres::{GenericClient, Pool};
use serde_json::Value;
use tokio_postgres::types::ToSql;

/// Execute a single-statement SQL with bound parameters on any deadpool
/// `GenericClient` (pool client or transaction). The SQL is expected to
/// return exactly one row with one column containing a JSON value (generated
/// by [`crate::sql::render`]).
pub async fn execute_on<C: GenericClient>(
    client: &C,
    sql: &str,
    binds: &[Bind],
) -> Result<Value> {
    let stmt = client.prepare_cached(sql).await?;
    let params: Vec<&(dyn ToSql + Sync)> = binds.iter().map(|b| b as &(dyn ToSql + Sync)).collect();
    let row = client.query_one(&stmt, &params).await?;
    let json: Value = row
        .try_get::<_, Value>(0)
        .map_err(|e| Error::Decode(e.to_string()))?;
    Ok(json)
}

/// Execute against a fresh connection from the pool. Preserves the
/// pre-transaction-API call signature.
pub async fn execute(pool: &Pool, sql: &str, binds: &[Bind]) -> Result<Value> {
    let client = pool.get().await?;
    execute_on(&client, sql, binds).await
}
