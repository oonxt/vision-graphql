//! Execute a rendered statement against MySQL.

use super::plan_runner::{json_list, Done, PlanBackend, Runner};
use crate::error::{Error, Result};
use crate::plan::{MutationPlan, KEY_KEY};
use crate::types::{Bind, Inputs};
use serde_json::Value;
use sqlx::mysql::MySql;

/// Execute a single-statement SQL with bound parameters on any sqlx executor.
/// The SQL is expected to return exactly one row with one JSON column, as
/// [`crate::sql::render`] produces for [`Dialect::MySql`](crate::Dialect::MySql).
pub async fn execute_on<'c, E>(executor: E, sql: &str, binds: &[Bind]) -> Result<Value>
where
    E: sqlx::Executor<'c, Database = MySql>,
{
    Runner::<MySql>::execute_on(executor, sql, binds).await
}

/// Run a [`MutationPlan`] on a connection and assemble its response; see
/// [`Runner::execute_plan`].
pub async fn execute_plan(
    conn: &mut sqlx::MySqlConnection,
    plan: &MutationPlan,
    inputs: &Inputs<'_>,
) -> Result<Value> {
    Runner::<MySql>::execute_plan(conn, plan, inputs).await
}

impl PlanBackend for MySql {
    const DATABASE: &'static str = "MySQL";
    /// No `RETURNING`: a write is counted, or read back by key.
    const WRITES_RETURN_ROWS: bool = false;

    fn map_error(e: sqlx::Error) -> Error {
        Error::Database(e)
    }

    fn list<T: serde::Serialize>(items: &[Option<T>]) -> String {
        json_list(items)
    }

    /// Rows are identified by primary key — one value or a tuple — bound as
    /// a JSON array `JSON_TABLE` reads back.
    fn keys<'a>(rows: impl Iterator<Item = &'a Value>) -> Bind {
        let keys: Vec<Value> = rows
            .map(|row| row.get(KEY_KEY).cloned().unwrap_or(Value::Null))
            .collect();
        Bind::Text(Value::Array(keys).to_string())
    }

    fn done(result: sqlx::mysql::MySqlQueryResult) -> Done {
        Done {
            rows_affected: result.rows_affected(),
            last_insert_id: Some(result.last_insert_id()),
        }
    }
}
