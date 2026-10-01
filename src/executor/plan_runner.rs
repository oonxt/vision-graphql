//! Run a [`MutationPlan`] — the statement sequence a mutation is on a
//! backend without CTE-able DML — on whichever driver the plan was built
//! for.
//!
//! One runner for SQLite and MySQL: the steps, the orphan rule, the scope
//! checks, the captures and the assembly of the response are the plan's
//! semantics and do not depend on the driver. What does — how a value is
//! bound, how a driver's error reads, what a captured row's key is and how
//! a set of them binds, what a statement without rows reports — is the
//! [`PlanBackend`] trait, answered per driver in a few lines. Before this
//! the two executors were copies that differed in those lines and had to
//! be fixed twice.

use crate::error::{Error, Result};
use crate::plan::{MutationPlan, PlanBind, ResponseShape, RowsFrom, Step};
use crate::types::{json_to_bind, Bind, Inputs, NullOf};
use serde_json::Value;
use sqlx::{Acquire, ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, Row, Type};

/// What a statement that returns no rows did.
pub(crate) struct Done {
    pub rows_affected: u64,
    /// The key the driver handed the last inserted row, where the driver
    /// has one; what [`PlanBind::LastInsertId`] binds.
    pub last_insert_id: Option<u64>,
}

/// The part of running a plan that is one driver's.
pub(crate) trait PlanBackend: Database {
    /// The driver's error, as the engine reports it.
    fn map_error(e: sqlx::Error) -> Error;

    /// A bound list, as the driver receives it. Neither SQLite nor MySQL
    /// has an array type: JSON text, read back by `json_each` /
    /// `JSON_TABLE` in the statement, is what keeps `_in` one placeholder
    /// whatever the list's length.
    fn list<T: serde::Serialize>(items: &[Option<T>]) -> String {
        serde_json::to_string(items).expect("a list of scalars serialises")
    }

    /// The keys of the rows earlier writes captured, bound for a statement
    /// that reads them back ([`PlanBind::Keys`]).
    fn keys<'a>(rows: impl Iterator<Item = &'a Value>) -> Bind;

    /// [`PlanBind::LastInsertId`]: the key the write before handed out, or
    /// a refusal where the driver (or the plan built for it) has none.
    fn last_insert_id(id: Option<u64>) -> Result<Bind>;

    /// What the driver's result of a statement without rows says.
    fn done(result: Self::QueryResult) -> Done;
}

/// Bind every parameter of `binds` onto `q`, in order.
fn bind_all<'q, DB>(
    mut q: sqlx::query::Query<'q, DB, DB::Arguments>,
    binds: &'q [Bind],
) -> sqlx::query::Query<'q, DB, DB::Arguments>
where
    DB: PlanBackend,
    bool: Encode<'q, DB> + Type<DB>,
    i32: Encode<'q, DB> + Type<DB>,
    i64: Encode<'q, DB> + Type<DB>,
    f64: Encode<'q, DB> + Type<DB>,
    String: Encode<'q, DB> + Type<DB>,
    Option<bool>: Encode<'q, DB> + Type<DB>,
    Option<i32>: Encode<'q, DB> + Type<DB>,
    Option<i64>: Encode<'q, DB> + Type<DB>,
    Option<f64>: Encode<'q, DB> + Type<DB>,
    Option<String>: Encode<'q, DB> + Type<DB>,
{
    for b in binds {
        q = match b {
            Bind::Null(of) => match of {
                NullOf::Bool => q.bind(None::<bool>),
                NullOf::Int4 => q.bind(None::<i32>),
                NullOf::Int8 => q.bind(None::<i64>),
                NullOf::Float8 => q.bind(None::<f64>),
                NullOf::Text
                | NullOf::BoolArray
                | NullOf::Int4Array
                | NullOf::Int8Array
                | NullOf::Float8Array
                | NullOf::TextArray => q.bind(None::<String>),
            },
            Bind::Bool(v) => q.bind(*v),
            Bind::Int4(v) => q.bind(*v),
            Bind::Int8(v) => q.bind(*v),
            Bind::Float8(v) => q.bind(*v),
            Bind::Text(v) => q.bind(v.clone()),
            Bind::BoolArray(v) => q.bind(DB::list(v)),
            Bind::Int4Array(v) => q.bind(DB::list(v)),
            Bind::Int8Array(v) => q.bind(DB::list(v)),
            Bind::Float8Array(v) => q.bind(DB::list(v)),
            Bind::TextArray(v) => q.bind(DB::list(v)),
        };
    }
    q
}

/// The runner over one driver. A struct rather than free functions so the
/// bounds — which sqlx states per driver, with nothing generic over
/// `DB::Connection` — are written once.
pub(crate) struct Runner<DB>(std::marker::PhantomData<DB>);

impl<DB> Runner<DB>
where
    DB: PlanBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB> + Acquire<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> bool: Encode<'q, DB> + Type<DB>,
    for<'q> i32: Encode<'q, DB> + Type<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + Decode<'q, DB>,
    for<'q> f64: Encode<'q, DB> + Type<DB>,
    for<'q> String: Encode<'q, DB> + Type<DB>,
    for<'q> Option<bool>: Encode<'q, DB> + Type<DB>,
    for<'q> Option<i32>: Encode<'q, DB> + Type<DB>,
    for<'q> Option<i64>: Encode<'q, DB> + Type<DB>,
    for<'q> Option<f64>: Encode<'q, DB> + Type<DB>,
    for<'q> Option<String>: Encode<'q, DB> + Type<DB>,
    for<'r> sqlx::types::Json<Value>: Decode<'r, DB> + Type<DB>,
    usize: ColumnIndex<DB::Row>,
{
    /// Execute a single-statement SQL with bound parameters on any sqlx
    /// executor. The SQL is expected to return exactly one row with one
    /// JSON column, as [`crate::sql::render`] produces for the dialect.
    pub async fn execute_on<'c, E>(executor: E, sql: &str, binds: &[Bind]) -> Result<Value>
    where
        E: Executor<'c, Database = DB>,
    {
        // The SQL is generated by our renderer (identifiers quoted, values
        // always bound), so asserting it safe here is sound.
        let row = bind_all::<DB>(sqlx::query(sqlx::AssertSqlSafe(sql)), binds)
            .fetch_one(executor)
            .await
            .map_err(DB::map_error)?;
        let json: sqlx::types::Json<Value> = row.try_get(0)?;
        Ok(json.0)
    }

    /// Run a [`MutationPlan`] on a connection and assemble its response.
    ///
    /// Opens a transaction on the connection — a savepoint, when the
    /// connection is already in one — and commits it at the end; an error
    /// anywhere drops it unfinished, which sqlx rolls back, so a scope
    /// violation on the third statement undoes the first two and nothing
    /// outside the plan.
    pub async fn execute_plan(
        conn: &mut DB::Connection,
        plan: &MutationPlan,
        inputs: &Inputs<'_>,
    ) -> Result<Value> {
        let mut tx = conn.begin().await?;
        let out = Self::run_plan(&mut tx, plan, inputs).await?;
        tx.commit().await?;
        Ok(out)
    }

    async fn run_plan(
        conn: &mut DB::Connection,
        plan: &MutationPlan,
        inputs: &Inputs<'_>,
    ) -> Result<Value> {
        let mut captured: Vec<Vec<Value>> = vec![Vec::new(); plan.captures];
        let mut reads: Vec<Value> = vec![Value::Null; plan.reads];
        for step in &plan.steps {
            match step {
                Step::Write {
                    sql,
                    binds,
                    capture,
                    rows,
                } => {
                    // A child of a parent that inserted nothing (DO NOTHING
                    // on a conflict, a conditional insert that found its
                    // row) is not inserted, as the join to an empty parent
                    // CTE inserts nothing on PostgreSQL.
                    let orphan = binds.iter().any(|b| {
                        matches!(b, PlanBind::Captured { capture, .. } if captured[*capture].is_empty())
                    });
                    if orphan {
                        continue;
                    }
                    let binds = Self::resolve_plan_binds(binds, inputs, &captured, None)?;
                    captured[*capture] = match rows {
                        RowsFrom::Statement => Self::fetch_rows(&mut *conn, sql, &binds).await?,
                        RowsFrom::Count => {
                            let done = Self::execute(&mut *conn, sql, &binds).await?;
                            vec![Value::Null; done.rows_affected as usize]
                        }
                        RowsFrom::Readback {
                            sql: read_sql,
                            binds: read_binds,
                        } => {
                            let done = Self::execute(&mut *conn, sql, &binds).await?;
                            if done.rows_affected == 0 {
                                Vec::new()
                            } else {
                                let binds = Self::resolve_plan_binds(
                                    read_binds,
                                    inputs,
                                    &captured,
                                    done.last_insert_id,
                                )?;
                                let rows = Self::fetch_rows(&mut *conn, read_sql, &binds).await?;
                                // One write, one row. Two means the object
                                // conflicted with two rows by two keys and
                                // the database updated one of them — a
                                // state PostgreSQL would have refused to
                                // enter.
                                if rows.len() > 1 {
                                    return Err(Error::Unsupported {
                                        message: format!(
                                            "an upsert matched {} rows by different unique keys; \
                                             MySQL updated one where PostgreSQL would have refused \
                                             the insert — resolve the conflict first",
                                            rows.len()
                                        ),
                                    });
                                }
                                rows
                            }
                        }
                    };
                }
                Step::Check {
                    sql,
                    binds,
                    table,
                    action,
                } => {
                    let binds = Self::resolve_plan_binds(binds, inputs, &captured, None)?;
                    let outside = Self::fetch_count(&mut *conn, sql, &binds).await?;
                    if outside > 0 {
                        return Err(Error::ScopeViolation {
                            table: table.clone(),
                            rows: outside,
                            action: (*action).to_string(),
                        });
                    }
                }
                Step::Read { sql, binds, into } => {
                    let binds = Self::resolve_plan_binds(binds, inputs, &captured, None)?;
                    reads[*into] = Self::fetch_json(&mut *conn, sql, &binds)
                        .await?
                        .unwrap_or(Value::Null);
                }
            }
        }

        let mut out = serde_json::Map::new();
        for field in &plan.fields {
            let value = match &field.shape {
                ResponseShape::Batch {
                    captures,
                    returning,
                    typenames,
                } => {
                    let affected: usize = captures.iter().map(|c| captured[*c].len()).sum();
                    let mut obj = serde_json::Map::new();
                    obj.insert("affected_rows".into(), Value::from(affected));
                    let rows = match returning {
                        Some(select) => {
                            let binds =
                                Self::resolve_plan_binds(&select.binds, inputs, &captured, None)?;
                            Self::fetch_json(&mut *conn, &select.sql, &binds)
                                .await?
                                .unwrap_or_else(|| Value::Array(Vec::new()))
                        }
                        None => Value::Array(Vec::new()),
                    };
                    obj.insert("returning".into(), rows);
                    for (key, name) in typenames {
                        obj.insert(key.clone(), Value::String(name.clone()));
                    }
                    Value::Object(obj)
                }
                ResponseShape::One { capture, returning } => {
                    if captured[*capture].is_empty() {
                        Value::Null
                    } else {
                        match returning {
                            Some(select) => {
                                let binds = Self::resolve_plan_binds(
                                    &select.binds,
                                    inputs,
                                    &captured,
                                    None,
                                )?;
                                Self::fetch_json(&mut *conn, &select.sql, &binds)
                                    .await?
                                    .unwrap_or(Value::Null)
                            }
                            None => Value::Object(serde_json::Map::new()),
                        }
                    }
                }
                ResponseShape::Deleted {
                    capture,
                    read,
                    typenames,
                    one,
                } => {
                    let affected = captured[*capture].len();
                    let rows = read.map(|r| reads[r].clone());
                    if *one {
                        if affected == 0 {
                            Value::Null
                        } else {
                            rows.unwrap_or_else(|| Value::Object(serde_json::Map::new()))
                        }
                    } else {
                        let mut obj = serde_json::Map::new();
                        obj.insert("affected_rows".into(), Value::from(affected));
                        obj.insert(
                            "returning".into(),
                            rows.unwrap_or_else(|| Value::Array(Vec::new())),
                        );
                        for (key, name) in typenames {
                            obj.insert(key.clone(), Value::String(name.clone()));
                        }
                        Value::Object(obj)
                    }
                }
            };
            out.insert(field.alias.clone(), value);
        }
        Ok(Value::Object(out))
    }

    /// The parameters of one plan statement, from the request and from
    /// what earlier statements captured. `last_insert_id` is the write's,
    /// for the read-back that follows it.
    fn resolve_plan_binds(
        binds: &[PlanBind],
        inputs: &Inputs<'_>,
        captured: &[Vec<Value>],
        last_insert_id: Option<u64>,
    ) -> Result<Vec<Bind>> {
        binds
            .iter()
            .map(|b| match b {
                PlanBind::Spec(spec) => spec.resolve(inputs),
                PlanBind::Captured {
                    capture,
                    column,
                    ty,
                } => {
                    let rows = &captured[*capture];
                    // Zero rows never reach here: the statement is skipped.
                    // More than one cannot happen for a single-row write.
                    let [row] = rows.as_slice() else {
                        return Err(Error::Schema(format!(
                            "internal: a nested insert expected one parent row and found {}",
                            rows.len()
                        )));
                    };
                    json_to_bind(row.get(column).unwrap_or(&Value::Null), ty)
                }
                PlanBind::Keys(captures) => {
                    Ok(DB::keys(captures.iter().flat_map(|c| captured[*c].iter())))
                }
                PlanBind::LastInsertId => DB::last_insert_id(last_insert_id),
            })
            .collect()
    }

    /// A statement that returns nothing: what it did.
    async fn execute(conn: &mut DB::Connection, sql: &str, binds: &[Bind]) -> Result<Done> {
        bind_all::<DB>(sqlx::query(sqlx::AssertSqlSafe(sql)), binds)
            .execute(conn)
            .await
            .map(DB::done)
            .map_err(DB::map_error)
    }

    /// Every row of a statement returning one JSON object per row.
    async fn fetch_rows(
        conn: &mut DB::Connection,
        sql: &str,
        binds: &[Bind],
    ) -> Result<Vec<Value>> {
        let rows = bind_all::<DB>(sqlx::query(sqlx::AssertSqlSafe(sql)), binds)
            .fetch_all(conn)
            .await
            .map_err(DB::map_error)?;
        rows.iter()
            .map(|r| {
                let json: sqlx::types::Json<Value> = r.try_get(0)?;
                Ok(json.0)
            })
            .collect()
    }

    async fn fetch_count(conn: &mut DB::Connection, sql: &str, binds: &[Bind]) -> Result<i64> {
        let row = bind_all::<DB>(sqlx::query(sqlx::AssertSqlSafe(sql)), binds)
            .fetch_one(conn)
            .await
            .map_err(DB::map_error)?;
        Ok(row.try_get::<i64, _>(0)?)
    }

    /// One JSON value, or none when the statement yields no row (`LIMIT 1`
    /// over nothing).
    async fn fetch_json(
        conn: &mut DB::Connection,
        sql: &str,
        binds: &[Bind],
    ) -> Result<Option<Value>> {
        let row = bind_all::<DB>(sqlx::query(sqlx::AssertSqlSafe(sql)), binds)
            .fetch_optional(conn)
            .await
            .map_err(DB::map_error)?;
        match row {
            Some(r) => {
                let json: Option<sqlx::types::Json<Value>> = r.try_get(0)?;
                Ok(json.map(|j| j.0))
            }
            None => Ok(None),
        }
    }
}
