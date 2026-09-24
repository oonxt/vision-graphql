//! What the engine asks of a database driver.
//!
//! [`Engine`](crate::Engine) is generic over a [`Backend`], which is one of
//! sqlx's database marker types: `sqlx::Postgres`, or `sqlx::Sqlite` behind
//! the `sqlite` feature. The trait is sealed:
//! a backend is a renderer dialect plus an execution model, both of which live
//! in this crate, so it is not something a caller implements — it is what a
//! caller *picks*, by the pool they hand the engine.

use crate::dialect::Dialect;
use crate::error::Result;
use crate::types::Bind;
use serde_json::Value;
use std::future::Future;

mod sealed {
    pub trait Sealed {}
}

/// A database the engine can render for and execute against. See the module
/// docs.
pub trait Backend: sqlx::Database + sealed::Sealed {
    /// The SQL this backend is rendered in.
    const DIALECT: Dialect;

    /// Check that a pool's connections behave the way the rendered SQL
    /// assumes. Run by the engine once, before the first statement on its
    /// own pool; PostgreSQL has nothing to check, SQLite has per-connection
    /// pragmas (see [`crate::sqlite::verify`]).
    fn verify_pool(pool: &sqlx::Pool<Self>) -> impl Future<Output = Result<()>> + Send;

    /// Run one rendered statement on a connection and return the one JSON
    /// value it yields.
    ///
    /// A connection rather than an `Executor`: sqlx implements `Executor`
    /// for `&mut PgConnection`, `&mut SqliteConnection` and so on one driver
    /// at a time, with no impl generic over `DB::Connection`, and for
    /// `&Pool<DB>` only under a bound every caller would have to repeat. The
    /// engine acquires a connection from whatever it was given and comes
    /// through here.
    fn execute_conn(
        conn: &mut Self::Connection,
        sql: &str,
        binds: &[Bind],
    ) -> impl Future<Output = Result<Value>> + Send;

    /// Run a [`MutationPlan`](crate::plan::MutationPlan) on a connection and
    /// assemble its response. Atomic: the backend opens a transaction on the
    /// connection — a savepoint, when it is already in one — and closes it,
    /// so a failure part-way undoes the plan's statements and nothing else.
    /// Only a dialect whose mutations are plans ever produces one; the
    /// others never see this called.
    fn execute_plan(
        conn: &mut Self::Connection,
        plan: &crate::plan::MutationPlan,
        inputs: &crate::types::Inputs<'_>,
    ) -> impl Future<Output = Result<Value>> + Send;
}

impl sealed::Sealed for sqlx::Postgres {}

impl Backend for sqlx::Postgres {
    const DIALECT: Dialect = Dialect::Postgres;

    fn verify_pool(_pool: &sqlx::Pool<Self>) -> impl Future<Output = Result<()>> + Send {
        std::future::ready(Ok(()))
    }

    fn execute_conn(
        conn: &mut sqlx::PgConnection,
        sql: &str,
        binds: &[Bind],
    ) -> impl Future<Output = Result<Value>> + Send {
        crate::executor::execute_on(conn, sql, binds)
    }

    fn execute_plan(
        _conn: &mut sqlx::PgConnection,
        _plan: &crate::plan::MutationPlan,
        _inputs: &crate::types::Inputs<'_>,
    ) -> impl Future<Output = Result<Value>> + Send {
        // PostgreSQL mutations render as one statement; the renderer never
        // produces a plan for this dialect.
        std::future::ready(Err(crate::error::Error::Schema(
            "internal: a mutation plan reached the PostgreSQL backend".into(),
        )))
    }
}

#[cfg(feature = "sqlite")]
impl sealed::Sealed for sqlx::Sqlite {}

#[cfg(feature = "sqlite")]
impl Backend for sqlx::Sqlite {
    const DIALECT: Dialect = Dialect::Sqlite;

    fn verify_pool(pool: &sqlx::Pool<Self>) -> impl Future<Output = Result<()>> + Send {
        crate::sqlite::verify(pool)
    }

    fn execute_conn(
        conn: &mut sqlx::SqliteConnection,
        sql: &str,
        binds: &[Bind],
    ) -> impl Future<Output = Result<Value>> + Send {
        crate::executor::sqlite::execute_on(conn, sql, binds)
    }

    fn execute_plan(
        conn: &mut sqlx::SqliteConnection,
        plan: &crate::plan::MutationPlan,
        inputs: &crate::types::Inputs<'_>,
    ) -> impl Future<Output = Result<Value>> + Send {
        crate::executor::sqlite::execute_plan(conn, plan, inputs)
    }
}
