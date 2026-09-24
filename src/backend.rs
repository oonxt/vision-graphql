//! What the engine asks of a database driver.
//!
//! [`Engine`](crate::Engine) is generic over a [`Backend`], which is one of
//! sqlx's database marker types (`sqlx::Postgres` today). The trait is sealed:
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

    /// Run one rendered statement on any executor of this database — the pool,
    /// a connection, a transaction's connection — and return the one JSON
    /// value it yields.
    fn execute<'c, E>(
        executor: E,
        sql: &str,
        binds: &[Bind],
    ) -> impl Future<Output = Result<Value>> + Send
    where
        E: sqlx::Executor<'c, Database = Self>;

    /// [`Backend::execute`] on a bare connection.
    ///
    /// Not a convenience: sqlx implements `Executor` for `&mut PgConnection`,
    /// `&mut SqliteConnection` and so on one driver at a time, with no impl
    /// generic over `DB::Connection`, so code generic over the backend cannot
    /// hand a transaction's connection to [`Backend::execute`]. This is the
    /// door it goes through instead.
    fn execute_conn(
        conn: &mut Self::Connection,
        sql: &str,
        binds: &[Bind],
    ) -> impl Future<Output = Result<Value>> + Send;
}

impl sealed::Sealed for sqlx::Postgres {}

impl Backend for sqlx::Postgres {
    const DIALECT: Dialect = Dialect::Postgres;

    fn execute<'c, E>(
        executor: E,
        sql: &str,
        binds: &[Bind],
    ) -> impl Future<Output = Result<Value>> + Send
    where
        E: sqlx::Executor<'c, Database = Self>,
    {
        crate::executor::execute_on(executor, sql, binds)
    }

    fn execute_conn(
        conn: &mut sqlx::PgConnection,
        sql: &str,
        binds: &[Bind],
    ) -> impl Future<Output = Result<Value>> + Send {
        crate::executor::execute_on(conn, sql, binds)
    }
}
