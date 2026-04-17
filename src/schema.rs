//! Schema data structures.
//!
//! In Phase 1 the schema is constructed manually via [`Schema::builder`].
//! Introspection arrives in Phase 5.

use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgType {
    Int4,
    Int8,
    Text,
    Varchar,
    Bool,
    Float4,
    Float8,
    Numeric,
    Uuid,
    Timestamp,
    TimestampTz,
    Jsonb,
}

#[derive(Debug)]
pub struct Column {
    pub exposed_name: String,
    pub physical_name: String,
    pub pg_type: PgType,
    pub nullable: bool,
}

#[derive(Debug)]
pub struct Table {
    pub exposed_name: String,
    pub physical_schema: String,
    pub physical_name: String,
    columns_by_exposed: HashMap<String, Column>,
    pub primary_key: Vec<String>,
}

impl Table {
    pub fn new(exposed: &str, schema: &str, physical: &str) -> Self {
        Self {
            exposed_name: exposed.into(),
            physical_schema: schema.into(),
            physical_name: physical.into(),
            columns_by_exposed: HashMap::new(),
            primary_key: Vec::new(),
        }
    }

    pub fn column(mut self, exposed: &str, physical: &str, pg_type: PgType, nullable: bool) -> Self {
        self.columns_by_exposed.insert(
            exposed.into(),
            Column {
                exposed_name: exposed.into(),
                physical_name: physical.into(),
                pg_type,
                nullable,
            },
        );
        self
    }

    pub fn primary_key(mut self, cols: &[&str]) -> Self {
        self.primary_key = cols.iter().map(|s| (*s).into()).collect();
        self
    }

    pub fn find_column(&self, exposed: &str) -> Option<&Column> {
        self.columns_by_exposed.get(exposed)
    }
}

#[derive(Debug)]
pub struct Schema {
    tables_by_exposed: HashMap<String, Arc<Table>>,
}

impl Schema {
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder { tables: HashMap::new() }
    }

    pub fn table(&self, exposed: &str) -> Option<&Arc<Table>> {
        self.tables_by_exposed.get(exposed)
    }
}

pub struct SchemaBuilder {
    tables: HashMap<String, Arc<Table>>,
}

impl SchemaBuilder {
    pub fn table(mut self, t: Table) -> Self {
        self.tables.insert(t.exposed_name.clone(), Arc::new(t));
        self
    }

    pub fn build(self) -> Schema {
        Schema { tables_by_exposed: self.tables }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_users_schema() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"]),
            )
            .build();
        let users = schema.table("users").expect("users table");
        assert_eq!(users.physical_schema, "public");
        assert_eq!(users.physical_name, "users");
        assert!(users.find_column("id").is_some());
        assert!(users.find_column("missing").is_none());
    }
}
