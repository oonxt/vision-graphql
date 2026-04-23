//! # vision-graphql
//!
//! A Hasura-style ORM for PostgreSQL. Accepts GraphQL query strings or
//! typed Rust builders and returns `serde_json::Value` in Hasura's data
//! shape.
//!
//! ## Quick start
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use deadpool_postgres::{Config, Runtime};
//! use tokio_postgres::NoTls;
//! use vision_graphql::{Engine, Query, Schema};
//!
//! let mut cfg = Config::new();
//! cfg.host = Some("localhost".into());
//! cfg.dbname = Some("mydb".into());
//! let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
//!
//! // Introspect the database to build the schema.
//! let schema = Schema::introspect(&pool).await?.build();
//! let engine = Engine::new(pool, schema);
//!
//! // GraphQL string path
//! let _ = engine
//!     .query("query { users { id name } }", None)
//!     .await?;
//!
//! // Builder path
//! let _ = engine
//!     .run(Query::from("users").select(&["id", "name"]).limit(10))
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Features
//!
//! - List, `_by_pk`, and `_aggregate` queries
//! - Nested `Object` and `Array` relations
//! - `EXISTS`-based relation filters in `where`
//! - Full mutation surface (`insert` / `insert_one` / `update` / `update_by_pk`
//!   / `delete` / `delete_by_pk`) with `on_conflict` and `returning`
//! - Comparison operators: `_eq`, `_neq`, `_gt`, `_gte`, `_lt`, `_lte`,
//!   `_like`, `_ilike`, `_nlike`, `_nilike`, `_in`, `_nin`, `_is_null`
//! - `order_by`, `limit`, `offset`, `distinct_on`
//! - GraphQL variables and fragments (named + inline)
//! - Schema introspection plus TOML config overlays
//! - Typed builder API equivalent to the GraphQL path

pub mod ast;
pub mod builder;
pub mod engine;
pub mod error;
pub mod executor;
pub mod parser;
pub mod schema;
pub mod sql;
pub mod types;

pub use builder::{
    AggregateBuilder, ByPkBuilder, DeleteBuilder, DeleteByPkBuilder, InsertBuilder, IntoOperation,
    Mutation, Query, QueryBuilder, UpdateBuilder, UpdateByPkBuilder,
};
pub use engine::{Engine, TxClient};
pub use error::Error;
pub use schema::Schema;
