//! # vision-graphql
//!
//! A Hasura-style ORM for PostgreSQL. Accepts GraphQL query strings and
//! returns `serde_json::Value` in Hasura's data shape.

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
pub use engine::Engine;
pub use error::Error;
pub use schema::Schema;
