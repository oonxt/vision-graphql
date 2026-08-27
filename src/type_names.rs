//! The GraphQL type name behind each position in the API.
//!
//! Nothing in the engine needed these while it only answered data queries — a
//! column is read by name and the type it belongs to never comes up. `__typename`
//! is the first thing that asks, and schema introspection and SDL export will
//! ask for the same names, so they live here rather than being spelled inline at
//! the places that render them.
//!
//! The scheme is Hasura's. That is not a matter of taste: a client generated
//! against a Hasura endpoint, or a `graphql-codegen` setup pointed at one,
//! expects `users`, `users_aggregate_fields`, `users_mutation_response`, and
//! matching those names is what lets that tooling work unchanged.

use crate::schema::Table;

/// The operation root types.
pub const QUERY_ROOT: &str = "query_root";
/// See [`QUERY_ROOT`].
pub const MUTATION_ROOT: &str = "mutation_root";

/// Type of a row of `table` — the exposed table name itself.
pub fn row(table: &Table) -> &str {
    &table.exposed_name
}

/// Type of the `<table>_aggregate` root field.
pub fn aggregate(table: &Table) -> String {
    format!("{}_aggregate", table.exposed_name)
}

/// Type of the `aggregate` field inside `<table>_aggregate`.
pub fn aggregate_fields(table: &Table) -> String {
    format!("{}_aggregate_fields", table.exposed_name)
}

/// Type of a `sum` / `avg` / `max` / `min` group inside `aggregate`.
pub fn agg_op_fields(table: &Table, op: &str) -> String {
    format!("{}_{op}_fields", table.exposed_name)
}

/// Type of the `{ affected_rows, returning }` object a non-`_by_pk` mutation
/// answers with.
pub fn mutation_response(table: &Table) -> String {
    format!("{}_mutation_response", table.exposed_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{PgType, Table};

    fn users() -> Table {
        Table::new("users", "public", "users").column("id", "id", PgType::Int4, false)
    }

    #[test]
    fn names_follow_the_hasura_scheme() {
        let t = users();
        assert_eq!(row(&t), "users");
        assert_eq!(aggregate(&t), "users_aggregate");
        assert_eq!(aggregate_fields(&t), "users_aggregate_fields");
        assert_eq!(agg_op_fields(&t, "sum"), "users_sum_fields");
        assert_eq!(mutation_response(&t), "users_mutation_response");
    }

    #[test]
    fn names_follow_the_exposed_name_not_the_physical_one() {
        let t = Table::new("profiles", "app", "users").column("id", "id", PgType::Int4, false);
        assert_eq!(row(&t), "profiles");
        assert_eq!(mutation_response(&t), "profiles_mutation_response");
    }
}
