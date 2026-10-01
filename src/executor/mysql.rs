//! Execute a rendered statement against MySQL.

use super::plan_runner::{Done, PlanBackend, Runner};
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
    fn map_error(e: sqlx::Error) -> Error {
        Error::Database(e)
    }

    /// Rows are identified by primary key — one value or a tuple — bound as
    /// a JSON array `JSON_TABLE` reads back.
    fn keys<'a>(rows: impl Iterator<Item = &'a Value>) -> Bind {
        let keys: Vec<Value> = rows
            .map(|row| row.get(KEY_KEY).cloned().unwrap_or(Value::Null))
            .collect();
        Bind::Text(Value::Array(keys).to_string())
    }

    fn last_insert_id(id: Option<u64>) -> Result<Bind> {
        match id {
            Some(id) => i64::try_from(id).map(Bind::Int8).map_err(|_| {
                Error::Decode(format!(
                    "LAST_INSERT_ID() {id} does not fit a signed integer"
                ))
            }),
            None => Err(Error::Schema(
                "internal: a LAST_INSERT_ID bind outside a read-back".into(),
            )),
        }
    }

    fn done(result: sqlx::mysql::MySqlQueryResult) -> Done {
        Done {
            rows_affected: result.rows_affected(),
            last_insert_id: Some(result.last_insert_id()),
        }
    }
}
