//! Execute a rendered statement: one row, one JSON column, per backend.

#[cfg(feature = "mysql")]
pub mod mysql;
mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[allow(deprecated)]
pub use postgres::{execute, execute_on};
