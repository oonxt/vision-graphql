//! Schema data structures.
//!
//! The schema can be built manually via [`Schema::builder`], introspected from
//! a live database via [`Schema::introspect`], or loaded from a TOML config
//! via [`SchemaBuilder::load_config`].

pub mod config;
pub mod introspect;
pub mod merge;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelKind {
    Object,
    Array,
}

#[derive(Debug, Clone)]
pub struct Relation {
    pub kind: RelKind,
    pub target_table: String,
    /// `(local_exposed_column, remote_exposed_column)` pairs. Join condition is
    /// AND of equalities across all pairs.
    pub mapping: Vec<(String, String)>,
}

impl Relation {
    pub fn object(target: &str) -> RelationBuilder {
        RelationBuilder {
            kind: RelKind::Object,
            target: target.into(),
            mapping: Vec::new(),
        }
    }

    pub fn array(target: &str) -> RelationBuilder {
        RelationBuilder {
            kind: RelKind::Array,
            target: target.into(),
            mapping: Vec::new(),
        }
    }
}

pub struct RelationBuilder {
    kind: RelKind,
    target: String,
    mapping: Vec<(String, String)>,
}

impl RelationBuilder {
    pub fn on<I, A, B>(mut self, pairs: I) -> Relation
    where
        I: IntoIterator<Item = (A, B)>,
        A: Into<String>,
        B: Into<String>,
    {
        self.mapping = pairs
            .into_iter()
            .map(|(a, b)| (a.into(), b.into()))
            .collect();
        Relation {
            kind: self.kind,
            target_table: self.target,
            mapping: self.mapping,
        }
    }
}

#[derive(Debug)]
pub struct Table {
    pub exposed_name: String,
    pub physical_schema: String,
    pub physical_name: String,
    columns_by_exposed: HashMap<String, Column>,
    pub primary_key: Vec<String>,
    relations_by_name: HashMap<String, Relation>,
}

impl Table {
    pub fn new(exposed: &str, schema: &str, physical: &str) -> Self {
        Self {
            exposed_name: exposed.into(),
            physical_schema: schema.into(),
            physical_name: physical.into(),
            columns_by_exposed: HashMap::new(),
            primary_key: Vec::new(),
            relations_by_name: HashMap::new(),
        }
    }

    pub fn column(
        mut self,
        exposed: &str,
        physical: &str,
        pg_type: PgType,
        nullable: bool,
    ) -> Self {
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

    pub fn relation(mut self, name: &str, rel: Relation) -> Self {
        self.relations_by_name.insert(name.into(), rel);
        self
    }

    pub fn find_column(&self, exposed: &str) -> Option<&Column> {
        self.columns_by_exposed.get(exposed)
    }

    pub fn find_relation(&self, name: &str) -> Option<&Relation> {
        self.relations_by_name.get(name)
    }

    pub(crate) fn columns_iter(&self) -> impl Iterator<Item = &Column> {
        self.columns_by_exposed.values()
    }

    pub(crate) fn relations_iter(&self) -> impl Iterator<Item = (&String, &Relation)> {
        self.relations_by_name.iter()
    }
}

#[derive(Debug)]
pub struct Schema {
    tables_by_exposed: HashMap<String, Arc<Table>>,
}

impl Schema {
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder {
            tables: HashMap::new(),
        }
    }

    pub fn table(&self, exposed: &str) -> Option<&Arc<Table>> {
        self.tables_by_exposed.get(exposed)
    }

    /// Introspect the database and return a ready-to-customize builder.
    pub async fn introspect(pool: &deadpool_postgres::Pool) -> crate::error::Result<SchemaBuilder> {
        crate::schema::merge::introspect_into_builder(pool).await
    }
}

pub struct SchemaBuilder {
    pub(crate) tables: HashMap<String, Arc<Table>>,
}

impl SchemaBuilder {
    pub fn table(mut self, t: Table) -> Self {
        self.tables.insert(t.exposed_name.clone(), Arc::new(t));
        self
    }

    pub fn build(self) -> Schema {
        Schema {
            tables_by_exposed: self.tables,
        }
    }

    pub(crate) fn insert_raw(&mut self, exposed: String, t: Arc<Table>) {
        self.tables.insert(exposed, t);
    }

    pub(crate) fn remove_raw(&mut self, exposed: &str) -> Option<Arc<Table>> {
        self.tables.remove(exposed)
    }

    /// Load a TOML config file and apply it as an overlay.
    pub fn load_config<P: AsRef<std::path::Path>>(self, path: P) -> crate::error::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| crate::error::Error::Schema(format!("cannot read config: {e}")))?;
        let cfg = crate::schema::config::parse(&text)?;
        Ok(crate::schema::merge::apply_config(self, &cfg))
    }

    /// Apply a pre-parsed config overlay.
    pub fn apply_config(self, cfg: &crate::schema::config::ConfigOverlay) -> Self {
        crate::schema::merge::apply_config(self, cfg)
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

    #[test]
    fn build_users_posts_relations() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .build();

        let users = schema.table("users").unwrap();
        let rel = users.find_relation("posts").unwrap();
        assert_eq!(rel.kind, RelKind::Array);
        assert_eq!(rel.target_table, "posts");
        assert_eq!(rel.mapping, vec![("id".to_string(), "user_id".to_string())]);

        let posts = schema.table("posts").unwrap();
        let rel = posts.find_relation("user").unwrap();
        assert_eq!(rel.kind, RelKind::Object);
    }
}
