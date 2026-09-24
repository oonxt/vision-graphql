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

use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use std::future::Future;
use std::sync::Arc;
use vision_graphql::{CompiledQuery, Engine, Principal, ScopeSet};

fn pg_connection<'a>(
    e: &'a Engine,
    c: &'a mut PgConnection,
    q: &'a CompiledQuery,
    p: &'a Principal,
    scope: ScopeSet,
) -> impl Future<Output = ()> + Send + 'a {
    async move {
        let _ = e.query_on(&mut *c, "{ x }", None).await;
        let _ = e.query_with_on(&mut *c, "{ x }", None, None).await;
        let _ = e
            .query_as_on::<_, serde_json::Value>(&mut *c, "{ x }", None)
            .await;
        let _ = e.execute_on(&mut *c, q, None).await;
        let _ = e.execute_scoped_on(&mut *c, q, None, p).await;
        let _ = e
            .execute_scoped_as_on::<_, serde_json::Value>(&mut *c, q, None, p)
            .await;
        let s = e.scoped(scope);
        let _ = s.query_on(&mut *c, "{ x }", None).await;
        let _ = s.query_with_on(&mut *c, "{ x }", None, None).await;
    }
}

fn pg_transaction<'a>(
    e: &'a Engine,
    tx: &'a mut Transaction<'static, Postgres>,
    q: &'a CompiledQuery,
) -> impl Future<Output = ()> + Send + 'a {
    async move {
        let _ = e.query_on(&mut **tx, "{ x }", None).await;
        let _ = e.execute_on(&mut **tx, q, None).await;
        // The transaction itself, as 0.24.0 accepted it.
        let _ = e.query_on(&mut *tx, "{ x }", None).await;
        let _ = e
            .execute_scoped_on(&mut *tx, q, None, &Principal::new())
            .await;
    }
}

fn pg_pool<'a>(e: &'a Engine, pool: &'a PgPool) -> impl Future<Output = ()> + Send + 'a {
    async move {
        let _ = e.query_on(pool, "{ x }", None).await;
        let mut c: PoolConnection<Postgres> = pool.acquire().await.unwrap();
        let _ = e.query_on(&mut *c, "{ x }", None).await;
        let _ = e.query_on(&mut c, "{ x }", None).await;
    }
}

/// The shape the report came from: a transaction begun inside a spawned task.
fn pg_spawned(e: Arc<Engine>, pool: PgPool) {
    tokio::spawn(async move {
        let mut tx = pool.begin().await.unwrap();
        let _ = e.query_on(&mut *tx, "{ x }", None).await;
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
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let _ = e.query_on(&mut *c, "{ x }", None).await;
            let _ = e.execute_on(&mut *c, q, None).await;
            let _ = e.execute_scoped_on(&mut *c, q, None, p).await;
        }
    }

    fn spawned(e: Arc<Engine<Sqlite>>, pool: SqlitePool) {
        tokio::spawn(async move {
            let _ = e.query_on(&pool, "{ x }", None).await;
            let mut tx = pool.begin().await.unwrap();
            let _ = e.query_on(&mut *tx, "{ x }", None).await;
            let _ = e.query_on(&mut tx, "{ x }", None).await;
            tx.commit().await.unwrap();
        });
    }
}
