//! The `_on` twins are called from inside a host's own futures — an axum
//! handler, a `tokio::spawn` — and those must be `Send`. 0.24.0's bound on the
//! target (`sqlx::Acquire<'c>`) made every future that held such a call on a
//! borrowed connection fail that check, far from the call site, and no test
//! noticed: a `#[tokio::test]` body is never required to be `Send`.
//!
//! Nothing here runs. Each function states `+ Send` in its return type, so
//! reaching a green build is the assertion.

#![allow(dead_code)]
// `async fn` would not state `+ Send`, and stating it is the test.
#![allow(clippy::manual_async_fn)]

use serde_json::Value;
use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use std::future::Future;
use std::sync::Arc;
use vision_graphql::{CompiledQuery, Engine, Principal, Query, ScopeSet};

/// Every twin, each on a fresh `$t` (an expression re-borrowing the target).
/// A twin added to the engine belongs here too.
macro_rules! every_twin {
    ($e:expr, $t:expr, $q:expr, $p:expr, $scope:expr) => {{
        let e = $e;
        let _ = e.query_on($t, "{ x }", None).await;
        let _ = e.query_with_on($t, "{ x }", None, None).await;
        let _ = e.query_as_on::<_, Value>($t, "{ x }", None).await;
        let _ = e
            .query_as_with_on::<_, Value>($t, "{ x }", None, None)
            .await;
        let _ = e.run_on($t, Query::from("x")).await;
        let _ = e.run_as_on::<_, Value>($t, Query::from("x")).await;
        let _ = e.execute_on($t, $q, None).await;
        let _ = e.execute_as_on::<_, Value>($t, $q, None).await;
        let _ = e.execute_scoped_on($t, $q, None, $p).await;
        let _ = e.execute_scoped_as_on::<_, Value>($t, $q, None, $p).await;
        let s = e.scoped($scope);
        let _ = s.query_on($t, "{ x }", None).await;
        let _ = s.query_with_on($t, "{ x }", None, None).await;
        let _ = s.query_as_on::<_, Value>($t, "{ x }", None).await;
        let _ = s
            .query_as_with_on::<_, Value>($t, "{ x }", None, None)
            .await;
        let _ = s.run_on($t, Query::from("x")).await;
        let _ = s.run_as_on::<_, Value>($t, Query::from("x")).await;
    }};
}

fn pg_connection<'a>(
    e: &'a Engine,
    c: &'a mut PgConnection,
    q: &'a CompiledQuery,
    p: &'a Principal,
    scope: ScopeSet,
) -> impl Future<Output = ()> + Send + 'a {
    async move { every_twin!(e, &mut *c, q, p, scope) }
}

fn pg_transaction<'a>(
    e: &'a Engine,
    tx: &'a mut Transaction<'static, Postgres>,
    q: &'a CompiledQuery,
    p: &'a Principal,
    scope: ScopeSet,
) -> impl Future<Output = ()> + Send + 'a {
    async move {
        every_twin!(e, &mut **tx, q, p, scope.clone());
        // The transaction itself, as 0.24.0 accepted it.
        every_twin!(e, &mut *tx, q, p, scope)
    }
}

fn pg_pool<'a>(
    e: &'a Engine,
    pool: &'a PgPool,
    q: &'a CompiledQuery,
    p: &'a Principal,
    scope: ScopeSet,
) -> impl Future<Output = ()> + Send + 'a {
    async move {
        every_twin!(e, pool, q, p, scope.clone());
        let mut c: PoolConnection<Postgres> = pool.acquire().await.unwrap();
        every_twin!(e, &mut *c, q, p, scope.clone());
        every_twin!(e, &mut c, q, p, scope)
    }
}

/// The shape the report came from: a transaction begun inside a spawned task.
fn pg_spawned(e: Arc<Engine>, pool: PgPool) {
    tokio::spawn(async move {
        let mut tx = pool.begin().await.unwrap();
        let _ = e.query_on(&mut *tx, "{ x }", None).await;
        let _ = e.run_on(&mut tx, Query::from("x")).await;
        tx.commit().await.unwrap();
    });
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use sqlx::{Sqlite, SqliteConnection, SqlitePool};

    fn connection<'a>(
        e: &'a Engine<Sqlite>,
        c: &'a mut SqliteConnection,
        q: &'a CompiledQuery<Sqlite>,
        p: &'a Principal,
        scope: ScopeSet,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move { every_twin!(e, &mut *c, q, p, scope) }
    }

    fn transaction<'a>(
        e: &'a Engine<Sqlite>,
        tx: &'a mut Transaction<'static, Sqlite>,
        q: &'a CompiledQuery<Sqlite>,
        p: &'a Principal,
        scope: ScopeSet,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            every_twin!(e, &mut **tx, q, p, scope.clone());
            every_twin!(e, &mut *tx, q, p, scope)
        }
    }

    fn pool<'a>(
        e: &'a Engine<Sqlite>,
        pool: &'a SqlitePool,
        q: &'a CompiledQuery<Sqlite>,
        p: &'a Principal,
        scope: ScopeSet,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            every_twin!(e, pool, q, p, scope.clone());
            let mut c: PoolConnection<Sqlite> = pool.acquire().await.unwrap();
            every_twin!(e, &mut *c, q, p, scope.clone());
            every_twin!(e, &mut c, q, p, scope)
        }
    }

    fn spawned(e: Arc<Engine<Sqlite>>, pool: SqlitePool) {
        tokio::spawn(async move {
            let mut tx = pool.begin().await.unwrap();
            let _ = e.query_on(&mut *tx, "{ x }", None).await;
            let _ = e.run_on(&mut tx, Query::from("x")).await;
            tx.commit().await.unwrap();
        });
    }
}
