//! # vision-graphql
//!
//! A Hasura-style ORM for PostgreSQL. Accepts GraphQL query strings and
//! returns `serde_json::Value` in Hasura's data shape.

pub mod ast;
pub mod engine;
pub mod error;
pub mod executor;
pub mod parser;
pub mod schema;
pub mod sql;
pub mod types;

pub use engine::Engine;
pub use error::Error;
pub use schema::Schema;
