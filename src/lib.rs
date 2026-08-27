//! # vision-graphql
//!
//! A Hasura-style GraphQL-to-SQL query engine for PostgreSQL. Accepts GraphQL query strings or
//! typed Rust builders and returns `serde_json::Value` in Hasura's data
//! shape.
//!
//! ## Quick start
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use sqlx::postgres::PgPoolOptions;
//! use vision_graphql::{Engine, Query, Schema};
//!
//! let pool = PgPoolOptions::new()
//!     .connect("postgres://localhost/mydb")
//!     .await?;
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
//! - List, `_by_pk`, and `_aggregate` queries, the last with `count(columns:,
//!   distinct:)` and field aliases
//! - Nested `Object` and `Array` relations
//! - `EXISTS`-based relation filters in `where`
//! - Full mutation surface (`insert` / `insert_one` / `update` / `update_by_pk`
//!   / `delete` / `delete_by_pk`) with `on_conflict` and `returning`
//! - Comparison operators: `_eq`, `_neq`, `_gt`, `_gte`, `_lt`, `_lte`,
//!   `_like`, `_ilike`, `_nlike`, `_nilike`, `_in`, `_nin`, `_is_null`
//! - `order_by`, `limit`, `offset`, `distinct_on`
//! - GraphQL variables, including defaults declared by the operation
//!   (`query($n: Int = 10)`), and fragments (named + inline)
//! - Schema introspection plus TOML config overlays
//! - Typed builder API equivalent to the GraphQL path
//! - Pre-parse limits on document size and nesting ([`limits::ParseLimits`]),
//!   applied on every path into the parser
//! - Compile once, execute many: [`Engine::compile`] renders the SQL ahead of
//!   time and [`Engine::execute`] runs it with any variables — see
//!   [`compiled`]. Parsing is cached across requests either way
//!   ([`parse_cache`]).

pub mod ast;
pub mod builder;
pub mod compiled;
pub mod engine;
pub mod error;
pub mod executor;
pub mod limits;
pub mod parse_cache;
pub mod parser;
pub mod policy;
pub mod predicate;
pub mod schema;
pub mod scope;
pub mod scope_config;
pub mod sql;
pub mod type_names;
pub mod types;

pub use builder::{
    AggregateBuilder, ByPkBuilder, DeleteBuilder, DeleteByPkBuilder, InsertBuilder, IntoOperation,
    Mutation, Query, QueryBuilder, UpdateBuilder, UpdateByPkBuilder,
};
pub use compiled::CompiledQuery;
pub use engine::{Engine, MutationResult, ScopedEngine, ScopedTxClient, TxClient};
pub use error::Error;
pub use limits::ParseLimits;
pub use parse_cache::ParseCache;
pub use policy::{ScopePolicy, ScopePolicyBuilder, ScopeRule};
pub use predicate::{and, col, not, or, param, principal, rel, Col, Operand, Principal, ScopeExpr};
pub use schema::Schema;
pub use scope::{ScopeSet, TableScope};
