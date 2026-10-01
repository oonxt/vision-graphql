//! Execute a rendered statement against SQLite.

use super::plan_runner::{Done, PlanBackend, Runner};
use crate::error::{Error, Result};
use crate::plan::{MutationPlan, ROWID_KEY};
use crate::types::{Bind, Inputs};
use serde_json::Value;
use sqlx::sqlite::Sqlite;

/// Execute a single-statement SQL with bound parameters on any sqlx executor.
/// The SQL is expected to return exactly one row with one column holding
/// JSON text, as [`crate::sql::render`] produces for [`Dialect::Sqlite`](crate::Dialect::Sqlite).
pub async fn execute_on<'c, E>(executor: E, sql: &str, binds: &[Bind]) -> Result<Value>
where
    E: sqlx::Executor<'c, Database = Sqlite>,
{
    Runner::<Sqlite>::execute_on(executor, sql, binds).await
}

/// Run a [`MutationPlan`] on a connection and assemble its response; see
/// [`Runner::execute_plan`].
pub async fn execute_plan(
    conn: &mut sqlx::SqliteConnection,
    plan: &MutationPlan,
    inputs: &Inputs<'_>,
) -> Result<Value> {
    Runner::<Sqlite>::execute_plan(conn, plan, inputs).await
}

impl PlanBackend for Sqlite {
    /// `json('…')` on a non-JSON literal is the one way the rendered SQL can
    /// raise, and it raises exactly when a column holds a value its declared
    /// type does not admit (see `Dialect::value_as_json`). SQLite's message is
    /// the same for every such case; this one says what it means here.
    fn map_error(e: sqlx::Error) -> Error {
        match &e {
            sqlx::Error::Database(db) if db.message().contains("malformed JSON") => Error::Decode(
                "SQLite reports malformed JSON: a column holds a value its declared type does \
                 not admit — text in a BOOLEAN column, or a JSON column that is not JSON; the \
                 table is not STRICT (see SchemaWarning::LooselyTypedTable)"
                    .into(),
            ),
            _ => Error::Database(e),
        }
    }

    /// Rows are identified by `rowid`, bound as one list.
    fn keys<'a>(rows: impl Iterator<Item = &'a Value>) -> Bind {
        Bind::Int8Array(
            rows.map(|row| row.get(ROWID_KEY).and_then(Value::as_i64))
                .collect(),
        )
    }

    /// Every SQLite write returns its rows; a plan built for SQLite never
    /// reads a key back.
    fn last_insert_id(_id: Option<u64>) -> Result<Bind> {
        Err(Error::Schema(
            "internal: a LAST_INSERT_ID bind reached the SQLite backend".into(),
        ))
    }

    fn done(result: sqlx::sqlite::SqliteQueryResult) -> Done {
        Done {
            rows_affected: result.rows_affected(),
            last_insert_id: None,
        }
    }
}
