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
    /// Plain unique indexes (`CREATE UNIQUE INDEX`, no constraint), as
    /// `index name -> key columns`.
    ///
    /// Kept apart from [`unique_constraints`](Self::unique_constraints) because
    /// the two answer different questions: `ON CONFLICT ON CONSTRAINT` needs a
    /// real constraint name and would break on an index name, while uniqueness
    /// reasoning — [`Schema::warnings`] — accepts either. Postgres itself
    /// accepts a plain unique index as a foreign-key target, so treating only
    /// constraints as proof of uniqueness warned forever about schemas that
    /// are provably fine.
    pub unique_indexes: std::collections::BTreeMap<String, Vec<String>>,
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
            unique_indexes: Default::default(),
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

    /// Declare a plain unique index by name — uniqueness without a constraint.
    /// See [`Table::unique_indexes`] for why it is not a `unique_constraint`.
    pub fn unique_index(mut self, name: &str, cols: &[&str]) -> Self {
        self.unique_indexes.insert(
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

/// A schema shape that no query will fail on, but that can quietly answer one
/// wrongly. Produced by [`Schema::warnings`]; also logged (`tracing::warn`)
/// when an [`Engine`](crate::Engine) is constructed, so a serving deployment
/// that never calls `warnings()` still hears about it once at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaWarning {
    /// An object relation whose mapped remote columns are not covered by any
    /// unique constraint, plain unique index, or primary key of the target
    /// table.
    ///
    /// The subquery for an object relation renders `LIMIT 1`. When the mapped
    /// columns match more than one target row and the query gives no
    /// `order_by`, there is no `ORDER BY` either — which row Postgres returns
    /// is undefined, looks fine, and can differ between executions. The two
    /// ways out are per-query `order_by`, or extending the mapping until a
    /// unique constraint covers it. Relations derived from foreign keys never
    /// trigger this: Postgres requires the referenced columns to be unique.
    NonDeterministicObjectRelation {
        /// Exposed name of the table carrying the relation.
        table: String,
        /// The relation's field name.
        relation: String,
        /// Exposed name of the target table.
        target: String,
        /// The remote columns the mapping filters the target by.
        remote_columns: Vec<String>,
    },
}

impl std::fmt::Display for SchemaWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaWarning::NonDeterministicObjectRelation {
                table,
                relation,
                target,
                remote_columns,
            } => write!(
                f,
                "object relation {table}.{relation} can return an arbitrary row: \
                 no unique constraint, unique index, or primary key on \
                 \"{target}\" covers ({}) and its subquery is LIMIT 1 with no ORDER BY; \
                 add order_by in queries, or extend the mapping to columns a \
                 unique constraint covers",
                remote_columns.join(", ")
            ),
        }
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

    /// Shapes in this schema that will not fail any query but can quietly
    /// answer one wrongly. Ordered by table name, so the list is the same on
    /// every run.
    ///
    /// This exists because the failure it describes is invisible at runtime:
    /// an object relation over a non-unique match returns *a* row, the
    /// response validates, and the wrong answer is only noticed when the data
    /// has already gone somewhere. Field reports confirmed exactly that —
    /// "nothing broke on launch day, the data grew crooked". Callers building
    /// admin or validation tooling should surface these to whoever owns the
    /// schema; a serving deployment also hears them logged once when its
    /// [`Engine`](crate::Engine) is constructed.
    pub fn warnings(&self) -> Vec<SchemaWarning> {
        let mut out = Vec::new();
        for (tname, table) in self.tables() {
            for (rname, rel) in table.relations() {
                if rel.kind != RelKind::Object {
                    continue;
                }
                // A dangling target is a different problem with a loud failure
                // of its own at query time; nothing to say about row choice.
                let Some(target) = self.table(&rel.target_table) else {
                    continue;
                };
                // A mapping naming a column the target does not expose is a
                // broken relation with a loud error of its own at query time
                // ("unknown remote column"); row choice is not the thing to
                // say about it, and saying it anyway would put two findings
                // with conflicting advice on one typo.
                if rel
                    .mapping
                    .iter()
                    .any(|(l, r)| table.find_column(l).is_none() || target.find_column(r).is_none())
                {
                    continue;
                }
                // Compare everything in physical-column space. Mappings and
                // primary keys name exposed columns; constraints and indexes
                // introspected from Postgres carry physical names. Resolving
                // each name to exactly one canonical form (its physical name
                // when the exposed column exists, itself otherwise — a
                // constraint can cover a hidden column) keeps the check
                // injective: pooling both spellings into one set once let a
                // single mapping entry satisfy two different constraint
                // columns.
                let canonical = |name: &'_ str| -> String {
                    target
                        .find_column(name)
                        .map(|c| c.physical_name.clone())
                        .unwrap_or_else(|| name.to_string())
                };
                let remote: std::collections::BTreeSet<String> =
                    rel.mapping.iter().map(|(_, r)| canonical(r)).collect();
                let covered = |cols: &[String]| {
                    !cols.is_empty() && cols.iter().all(|c| remote.contains(&canonical(c)))
                };
                // The overlay's logical primary_key counts even though nothing
                // enforces it: it is the caller's assertion of what identifies
                // a row, and doubting it here would warn on every keyed view.
                let pinned = covered(&target.primary_key)
                    || target.unique_constraints.values().any(|c| covered(c))
                    || target.unique_indexes.values().any(|c| covered(c));
                if !pinned {
                    out.push(SchemaWarning::NonDeterministicObjectRelation {
                        table: tname.clone(),
                        relation: rname.clone(),
                        target: rel.target_table.clone(),
                        remote_columns: rel.mapping.iter().map(|(_, r)| r.clone()).collect(),
                    });
                }
            }
        }
        out
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

    // Deliberately silent: logging warnings here put every one on stderr twice
    // in `vision-gql diff` (once tracing-formatted, once in the report) and
    // sidestepped the CLI's table filters. Serving deployments hear them at
    // `Engine` construction instead; tooling calls [`Schema::warnings`].
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

    /// The dictionary table from the field report: keyed by (serial, type),
    /// but the relation maps only `serial`. Which dictionary row the object
    /// relation returns is then up to Postgres.
    fn dict_table() -> Table {
        Table::new("dict", "public", "dict")
            .column("id", "id", PgType::Int4, false)
            .column("serial", "serial", PgType::Text, false)
            .column("type", "type", PgType::Text, false)
            .primary_key(&["id"])
            .unique_constraint("dict_serial_type_key", &["serial", "type"])
    }

    fn results_table(mapping: &[(&str, &str)]) -> Table {
        Table::new("results", "public", "results")
            .column("id", "id", PgType::Int4, false)
            .column("serial", "serial", PgType::Text, false)
            .column("type", "type", PgType::Text, false)
            .primary_key(&["id"])
            .relation(
                "pathogen",
                Relation::object("dict").on(mapping.iter().copied()),
            )
    }

    #[test]
    fn object_relation_without_unique_coverage_warns() {
        let schema = Schema::builder()
            .table(dict_table())
            .table(results_table(&[("serial", "serial")]))
            .build();
        let warnings = schema.warnings();
        assert_eq!(
            warnings,
            vec![SchemaWarning::NonDeterministicObjectRelation {
                table: "results".into(),
                relation: "pathogen".into(),
                target: "dict".into(),
                remote_columns: vec!["serial".into()],
            }]
        );
        let text = warnings[0].to_string();
        assert!(text.contains("results.pathogen"), "got: {text}");
        assert!(text.contains("order_by"), "got: {text}");
    }

    /// The day-of fix from the report: mapping both columns reaches the
    /// composite unique constraint, and the warning goes away.
    #[test]
    fn object_relation_covered_by_composite_unique_is_silent() {
        let schema = Schema::builder()
            .table(dict_table())
            .table(results_table(&[("serial", "serial"), ("type", "type")]))
            .build();
        assert_eq!(schema.warnings(), vec![]);
    }

    #[test]
    fn object_relation_covered_by_primary_key_is_silent() {
        // build_users_posts_relations' shape: posts.user maps user_id -> users.id.
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .build();
        assert_eq!(schema.warnings(), vec![]);
    }

    /// A mapping *wider* than the constraint still pins one row: equality on a
    /// superset of a unique key matches at most what the key matches.
    #[test]
    fn mapping_superset_of_unique_key_is_silent() {
        let schema = Schema::builder()
            .table(dict_table())
            .table({
                let t = results_table(&[("serial", "serial"), ("type", "type")]);
                t.relation(
                    "pathogen",
                    Relation::object("dict").on([
                        ("serial", "serial"),
                        ("type", "type"),
                        ("id", "id"),
                    ]),
                )
            })
            .build();
        assert_eq!(schema.warnings(), vec![]);
    }

    /// Array relations are rendered as aggregates, not LIMIT 1 — many rows is
    /// their meaning, not a hazard.
    #[test]
    fn array_relation_never_warns() {
        let schema = Schema::builder()
            .table(dict_table())
            .table(
                Table::new("results", "public", "results")
                    .column("serial", "serial", PgType::Text, false)
                    .relation(
                        "entries",
                        Relation::array("dict").on([("serial", "serial")]),
                    ),
            )
            .build();
        assert_eq!(schema.warnings(), vec![]);
    }

    /// A target with no key knowledge at all — a view, typically — cannot be
    /// pinned, so an object relation onto it warns.
    #[test]
    fn object_relation_onto_keyless_target_warns() {
        let schema = Schema::builder()
            .table(Table::new("dict_view", "public", "dict_view").column(
                "serial",
                "serial",
                PgType::Text,
                false,
            ))
            .table(
                Table::new("results", "public", "results")
                    .column("serial", "serial", PgType::Text, false)
                    .relation(
                        "pathogen",
                        Relation::object("dict_view").on([("serial", "serial")]),
                    ),
            )
            .build();
        assert_eq!(schema.warnings().len(), 1);
    }

    /// A relation whose target is not in the schema fails loudly at query
    /// time; row choice is not the thing to say about it.
    #[test]
    fn dangling_target_is_not_this_warning() {
        let schema = Schema::builder()
            .table(results_table(&[("serial", "serial")]))
            .build();
        assert_eq!(schema.warnings(), vec![]);
    }

    /// Same reasoning as a dangling target: a mapping naming a column the
    /// target does not expose errors loudly at query time ("unknown remote
    /// column"), and warning about row choice on top of that drift would give
    /// one typo two findings with conflicting advice.
    #[test]
    fn broken_mapping_is_not_this_warning() {
        let schema = Schema::builder()
            .table(dict_table())
            .table(results_table(&[("serial", "serial_typo")]))
            .build();
        assert_eq!(schema.warnings(), vec![]);
    }

    /// Uniqueness via a plain `CREATE UNIQUE INDEX` counts: Postgres accepts
    /// one as an FK target, so a schema relying on it is provably fine and a
    /// permanent warning on it would teach people to ignore the real ones.
    #[test]
    fn plain_unique_index_coverage_is_silent() {
        let schema = Schema::builder()
            .table(
                Table::new("dict", "public", "dict")
                    .column("serial", "serial", PgType::Text, false)
                    .unique_index("dict_serial_idx", &["serial"]),
            )
            .table(results_table(&[("serial", "serial")]))
            .build();
        assert_eq!(schema.warnings(), vec![]);
    }

    /// Coverage is checked per canonical (physical) column, so one mapping
    /// entry cannot satisfy a constraint on a *different* column that merely
    /// shares a spelling across the exposed/physical namespaces.
    #[test]
    fn constraint_on_a_different_column_with_a_shared_spelling_still_warns() {
        let schema = Schema::builder()
            .table(
                Table::new("dict", "public", "dict")
                    // exposed "code" is physical "serial"; exposed "serial" is
                    // physical "legacy_serial", and carries the unique key.
                    .column("code", "serial", PgType::Text, false)
                    .column("serial", "legacy_serial", PgType::Text, false)
                    .unique_constraint("dict_serial_key", &["serial"]),
            )
            .table(
                Table::new("results", "public", "results")
                    .column("code", "code", PgType::Text, false)
                    .relation("pathogen", Relation::object("dict").on([("code", "code")])),
            )
            .build();
        assert_eq!(schema.warnings().len(), 1, "got {:?}", schema.warnings());
    }
}
