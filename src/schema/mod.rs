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
    Int2,
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
    Json,
    Jsonb,
    Date,
    Time,
    /// User-defined enum type; carries the schema-qualified type name used in
    /// cast expressions (`$1::"schema"."name"`).
    Enum {
        schema: String,
        name: String,
    },
}

impl PgType {
    /// Whether arithmetic applies: `sum`, `avg`, `stddev` and the rest are
    /// only defined over numbers, and PostgreSQL has no `sum(text)` to call.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            PgType::Int2
                | PgType::Int4
                | PgType::Int8
                | PgType::Float4
                | PgType::Float8
                | PgType::Numeric
        )
    }

    /// Whether values of this type have an order — which is what `_gt`-family
    /// comparisons need. Everything but `json`/`jsonb`, which PostgreSQL orders
    /// in a way nobody should depend on.
    pub fn is_orderable(&self) -> bool {
        !matches!(self, PgType::Json | PgType::Jsonb)
    }

    /// Whether PostgreSQL defines `max`/`min` for this type.
    ///
    /// Not the same question as [`is_orderable`](Self::is_orderable): `boolean`,
    /// `uuid` and user enums all order — `_gt` works on every one — but
    /// `max(boolean)`, `max(uuid)` and `max` of an enum do not exist (verified
    /// against 17.4), so publishing them would publish a query that cannot run.
    pub fn has_max_min(&self) -> bool {
        self.is_orderable() && !matches!(self, PgType::Bool | PgType::Uuid | PgType::Enum { .. })
    }
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
    /// Exposed column names in the order they were added, which for an
    /// introspected table is Postgres' `ordinal_position`.
    ///
    /// The map alone gives no order at all, and an unordered column list makes
    /// every artifact built from the schema — SDL above all — differ run to run
    /// for reasons that have nothing to do with the schema.
    column_order: Vec<String>,
    pub primary_key: Vec<String>,
    /// Unique (and primary key) constraints, as `constraint name -> columns`.
    ///
    /// Every constraint introspection found, including any covering a column
    /// `hide_columns` removed: Postgres resolves an `ON CONFLICT` target by
    /// name, so such a constraint is still a usable target. Deciding which of
    /// them to *publish* is a separate question, answered in
    /// [`crate::type_system`] — a constraint name tends to contain its column
    /// names, which is exactly what hiding a column meant to withhold.
    pub unique_constraints: std::collections::BTreeMap<String, Vec<String>>,
    relations_by_name: HashMap<String, Relation>,
    /// Relation names in the order they were added. Same reason as
    /// [`Table::column_order`].
    relation_order: Vec<String>,
    /// No `insert_` / `update_` / `delete_` root is derived for this table, and
    /// it cannot be the target of a nested insert.
    ///
    /// Mutation roots are derived from the exposed name by prefix, so anything
    /// reachable in the schema is writable unless it says otherwise. Views are
    /// the motivating case: introspection picks them up (they have columns in
    /// `information_schema`), and Postgres auto-updates a *simple* view straight
    /// through to its base table — so an unguarded `insert_my_view` really does
    /// write rows into the table behind it. Introspection marks views read-only;
    /// config can override in either direction (a view with INSTEAD OF triggers
    /// is genuinely writable; a base table may be deliberately frozen).
    pub read_only: bool,
}

impl Table {
    pub fn new(exposed: &str, schema: &str, physical: &str) -> Self {
        Self {
            exposed_name: exposed.into(),
            physical_schema: schema.into(),
            physical_name: physical.into(),
            columns_by_exposed: HashMap::new(),
            column_order: Vec::new(),
            primary_key: Vec::new(),
            unique_constraints: Default::default(),
            relations_by_name: HashMap::new(),
            relation_order: Vec::new(),
            read_only: false,
        }
    }

    /// Mark this table read-only: no mutation roots, no nested-insert target.
    pub fn read_only(mut self, yes: bool) -> Self {
        self.read_only = yes;
        self
    }

    pub fn column(
        mut self,
        exposed: &str,
        physical: &str,
        pg_type: PgType,
        nullable: bool,
    ) -> Self {
        if !self.columns_by_exposed.contains_key(exposed) {
            self.column_order.push(exposed.to_string());
        }
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

    /// Declare a unique constraint by name.
    pub fn unique_constraint(mut self, name: &str, cols: &[&str]) -> Self {
        self.unique_constraints.insert(
            name.to_string(),
            cols.iter().map(|c| (*c).to_string()).collect(),
        );
        self
    }

    pub fn primary_key(mut self, cols: &[&str]) -> Self {
        self.primary_key = cols.iter().map(|s| (*s).into()).collect();
        self
    }

    pub fn relation(mut self, name: &str, rel: Relation) -> Self {
        if !self.relations_by_name.contains_key(name) {
            self.relation_order.push(name.to_string());
        }
        self.relations_by_name.insert(name.into(), rel);
        self
    }

    pub fn find_column(&self, exposed: &str) -> Option<&Column> {
        self.columns_by_exposed.get(exposed)
    }

    pub fn find_relation(&self, name: &str) -> Option<&Relation> {
        self.relations_by_name.get(name)
    }

    /// Every exposed column, in no particular order.
    ///
    /// Public so an application can see what its schema actually exposes —
    /// which is the only way to check that an overlay's `hide_columns` did what
    /// was intended, and what any SDL export or admin tooling has to walk.
    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        self.column_order
            .iter()
            .filter_map(|n| self.columns_by_exposed.get(n))
    }

    /// Every relation, as `(exposed name, relation)`.
    pub fn relations(&self) -> impl Iterator<Item = (&String, &Relation)> {
        self.relation_order
            .iter()
            .filter_map(|n| self.relations_by_name.get_key_value(n))
    }

    pub(crate) fn columns_iter(&self) -> impl Iterator<Item = &Column> {
        self.columns()
    }

    pub(crate) fn relations_iter(&self) -> impl Iterator<Item = (&String, &Relation)> {
        self.relations()
    }
}

#[derive(Debug)]
pub struct Schema {
    tables_by_exposed: HashMap<String, Arc<Table>>,
    /// Whether `__schema` / `__type` may be answered at runtime.
    ///
    /// Off by default: introspection publishes the whole data model to anyone
    /// who can reach the endpoint, and upgrading a dependency should not widen
    /// what a deployment exposes.
    introspection: bool,
    /// Derived on first use and kept, since deriving it walks every table.
    type_system: std::sync::OnceLock<crate::type_system::TypeSystem>,
}

impl Schema {
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder {
            tables: HashMap::new(),
            introspection: false,
        }
    }

    /// Every exposed table, as `(exposed name, table)`, ordered by name.
    ///
    /// Sorted rather than in map order so anything derived from the whole schema
    /// — SDL, an introspection answer — is the same on every run. Lookups stay
    /// on the hash map; this is walked rarely.
    pub fn tables(&self) -> impl Iterator<Item = (&String, &Arc<Table>)> {
        let mut all: Vec<(&String, &Arc<Table>)> = self.tables_by_exposed.iter().collect();
        all.sort_by(|a, b| a.0.cmp(b.0));
        all.into_iter()
    }

    /// How many tables are exposed.
    pub fn len(&self) -> usize {
        self.tables_by_exposed.len()
    }

    /// Whether the schema exposes nothing at all.
    pub fn is_empty(&self) -> bool {
        self.tables_by_exposed.is_empty()
    }

    /// The GraphQL type system this schema exposes, derived on first call.
    ///
    /// Always available — SDL export is a build-time artifact and has nothing to
    /// do with whether runtime introspection is enabled.
    pub fn type_system(&self) -> &crate::type_system::TypeSystem {
        self.type_system
            .get_or_init(|| crate::type_system::TypeSystem::build(self))
    }

    /// Whether `__schema` / `__type` are answerable. See
    /// [`SchemaBuilder::enable_introspection`].
    pub fn introspection_enabled(&self) -> bool {
        self.introspection
    }

    pub fn table(&self, exposed: &str) -> Option<&Arc<Table>> {
        self.tables_by_exposed.get(exposed)
    }

    /// Introspect the `public` schema and return a ready-to-customize builder.
    /// Shorthand for [`Schema::introspect_schemas`] with `["public"]`.
    pub async fn introspect(pool: &sqlx::PgPool) -> crate::error::Result<SchemaBuilder> {
        crate::schema::merge::introspect_into_builder(pool).await
    }

    /// Introspect several schemas at once. Foreign keys that cross between them
    /// become relations like any other.
    ///
    /// **The first schema listed owns the bare table names; every other schema
    /// is exposed prefixed** — `introspect_schemas(pool, &["app", "audit"])`
    /// gives `orders` for `app.orders` and `audit_orders` for `audit.orders`.
    /// The order is fixed by this call rather than inferred, so creating a table
    /// in a later schema can never rename one that queries already depend on.
    /// Rename anything you don't like with the overlay's `expose_as`.
    ///
    /// ```no_run
    /// # async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    /// use vision_graphql::Schema;
    /// let schema = Schema::introspect_schemas(&pool, &["app", "audit"])
    ///     .await?
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub async fn introspect_schemas(
        pool: &sqlx::PgPool,
        schemas: &[&str],
    ) -> crate::error::Result<SchemaBuilder> {
        crate::schema::merge::introspect_schemas_into_builder(pool, schemas).await
    }
}

pub struct SchemaBuilder {
    pub(crate) tables: HashMap<String, Arc<Table>>,
    pub(crate) introspection: bool,
}

impl SchemaBuilder {
    pub fn table(mut self, t: Table) -> Self {
        self.tables.insert(t.exposed_name.clone(), Arc::new(t));
        self
    }

    /// Let `__schema` and `__type` be answered at runtime.
    ///
    /// Off by default. Introspection hands the caller the whole data model —
    /// every table, column, type and relation the schema exposes — which is a
    /// wider disclosure than answering data queries, and not one that should
    /// appear in an existing deployment because it upgraded. Turn it on where
    /// the endpoint is internal, or where the data model is public anyway, and
    /// leave it off on a public API whose clients ship pre-generated documents.
    ///
    /// SDL export ([`Schema::type_system`], [`crate::sdl`]) is unaffected: it is
    /// a build-time artifact, not something a request can ask for.
    pub fn enable_introspection(mut self) -> Self {
        self.introspection = true;
        self
    }

    pub fn build(self) -> Schema {
        Schema {
            tables_by_exposed: self.tables,
            introspection: self.introspection,
            type_system: std::sync::OnceLock::new(),
        }
    }

    /// Keep only the tables whose exposed name satisfies `keep`.
    ///
    /// For a caller that builds from introspection and then narrows — the CLI's
    /// `--include-tables` / `--ignore-tables`, or an application exposing one
    /// slice of a large database.
    pub fn retain_tables(mut self, keep: impl Fn(&str) -> bool) -> Self {
        self.tables.retain(|name, _| keep(name));
        self
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
