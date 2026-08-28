//! The GraphQL type system this engine exposes, derived from a [`Schema`].
//!
//! Two things need it and they need the same thing: SDL export, and answering
//! `__schema` / `__type`. So it is built once, as data, and both read it.
//!
//! # What it promises
//!
//! Every type here corresponds to something the engine actually implements. The
//! temptation with a Hasura-shaped API is to publish Hasura's full surface and
//! fail at runtime on the parts that were never built — an operator advertised
//! in `Int_comparison_exp` that the lowering rejects is worse than one that is
//! simply absent, because a client generates code against it. So the comparison
//! inputs carry exactly the operators [`crate::parser`] lowers, the mutation
//! root skips read-only tables, `<table>_constraint` is built from the unique
//! constraints introspection actually found, and `path` appears only on
//! `json`/`jsonb` columns.
//!
//! # Naming
//!
//! Hasura's, via [`crate::type_names`] — see the note there on why matching it
//! is worth doing.
//!
//! # What it does not model
//!
//! A Postgres enum column is published as a custom scalar named after the type
//! rather than as a GraphQL enum, because introspection reads the type's name
//! but not its variants. A client sees a named scalar and passes strings, which
//! is what the engine binds anyway.

use crate::ast::AggFunc;
use crate::schema::{PgType, RelKind, Schema, Table};
use crate::type_names;
use std::collections::{BTreeMap, BTreeSet};

/// A reference to a type from a field or argument position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeRef {
    Named(String),
    NonNull(Box<TypeRef>),
    List(Box<TypeRef>),
}

impl TypeRef {
    pub fn named(name: impl Into<String>) -> Self {
        TypeRef::Named(name.into())
    }

    /// `T!`
    pub fn non_null(self) -> Self {
        TypeRef::NonNull(Box::new(self))
    }

    /// `[T]`
    pub fn list(self) -> Self {
        TypeRef::List(Box::new(self))
    }

    /// `[T!]!` — the shape every row list in this API has.
    pub fn list_of_non_null_non_null(self) -> Self {
        self.non_null().list().non_null()
    }

    /// The name at the bottom of the wrappers.
    pub fn base_name(&self) -> &str {
        match self {
            TypeRef::Named(n) => n,
            TypeRef::NonNull(inner) | TypeRef::List(inner) => inner.base_name(),
        }
    }
}

/// An argument, or a field of an input object.
#[derive(Debug, Clone)]
pub struct InputValue {
    pub name: String,
    pub ty: TypeRef,
    pub description: Option<String>,
    /// Rendered exactly as it appears in SDL, e.g. `"10"` or `"[]"`.
    pub default_value: Option<String>,
}

impl InputValue {
    pub fn new(name: impl Into<String>, ty: TypeRef) -> Self {
        InputValue {
            name: name.into(),
            ty,
            description: None,
            default_value: None,
        }
    }

    pub fn described(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// A field of an object type.
#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub args: Vec<InputValue>,
    pub ty: TypeRef,
    pub description: Option<String>,
}

impl Field {
    pub fn new(name: impl Into<String>, ty: TypeRef) -> Self {
        Field {
            name: name.into(),
            args: Vec::new(),
            ty,
            description: None,
        }
    }

    pub fn with_args(mut self, args: Vec<InputValue>) -> Self {
        self.args = args;
        self
    }

    pub fn described(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// One named type.
#[derive(Debug, Clone)]
pub enum TypeDef {
    Scalar {
        name: String,
        description: Option<String>,
    },
    Object {
        name: String,
        description: Option<String>,
        fields: Vec<Field>,
    },
    InputObject {
        name: String,
        description: Option<String>,
        fields: Vec<InputValue>,
    },
    Enum {
        name: String,
        description: Option<String>,
        values: Vec<String>,
    },
}

impl TypeDef {
    pub fn name(&self) -> &str {
        match self {
            TypeDef::Scalar { name, .. }
            | TypeDef::Object { name, .. }
            | TypeDef::InputObject { name, .. }
            | TypeDef::Enum { name, .. } => name,
        }
    }

    /// The `__TypeKind` this answers with.
    pub fn kind(&self) -> &'static str {
        match self {
            TypeDef::Scalar { .. } => "SCALAR",
            TypeDef::Object { .. } => "OBJECT",
            TypeDef::InputObject { .. } => "INPUT_OBJECT",
            TypeDef::Enum { .. } => "ENUM",
        }
    }

    pub fn description(&self) -> Option<&str> {
        match self {
            TypeDef::Scalar { description, .. }
            | TypeDef::Object { description, .. }
            | TypeDef::InputObject { description, .. }
            | TypeDef::Enum { description, .. } => description.as_deref(),
        }
    }
}

/// Every type the API exposes, plus its root type names.
#[derive(Debug, Clone)]
pub struct TypeSystem {
    types: BTreeMap<String, TypeDef>,
    query_root: String,
    mutation_root: Option<String>,
}

impl TypeSystem {
    /// Derive the type system a [`Schema`] exposes.
    pub fn build(schema: &Schema) -> Self {
        Builder::new(schema).build()
    }

    /// Types in a stable order (by name), which is what makes an SDL export
    /// diffable in review.
    pub fn types(&self) -> impl Iterator<Item = &TypeDef> {
        self.types.values()
    }

    pub fn get(&self, name: &str) -> Option<&TypeDef> {
        self.types.get(name)
    }

    pub fn query_root(&self) -> &str {
        &self.query_root
    }

    /// `None` when the schema exposes no writable table, in which case there is
    /// no mutation root to publish.
    pub fn mutation_root(&self) -> Option<&str> {
        self.mutation_root.as_deref()
    }
}

/// GraphQL scalar name for a column type.
///
/// The names past the five built-ins are Hasura's, which are also the names a
/// client would already have a serializer for.
pub fn scalar_name(pg: &PgType) -> String {
    match pg {
        // Both are GraphQL `Int`: the spec's Int is 32-bit signed, which holds
        // every int2 and int4 value.
        PgType::Int2 | PgType::Int4 => "Int".into(),
        PgType::Int8 => "bigint".into(),
        PgType::Float4 | PgType::Float8 => "Float".into(),
        PgType::Bool => "Boolean".into(),
        PgType::Text | PgType::Varchar => "String".into(),
        PgType::Numeric => "numeric".into(),
        PgType::Uuid => "uuid".into(),
        PgType::Timestamp => "timestamp".into(),
        PgType::TimestampTz => "timestamptz".into(),
        PgType::Date => "date".into(),
        PgType::Time => "time".into(),
        PgType::Json => "json".into(),
        PgType::Jsonb => "jsonb".into(),
        // The variants are not introspected, so this cannot be a GraphQL enum;
        // see the module docs.
        PgType::Enum { name, .. } => name.clone(),
    }
}

/// Whether `func` means anything for a column of this type.
pub fn applies(func: AggFunc, pg: &PgType) -> bool {
    if func.numeric_only() {
        pg.is_numeric()
    } else {
        pg.has_max_min()
    }
}

/// Why [`applies`] said no, for the error the caller raises.
///
/// One sentence per way of failing, shared by both entry points — the parser
/// and the renderer — so the document path and the builder get the same reason.
pub fn why_inapplicable(func: AggFunc, pg: &PgType) -> &'static str {
    if func.numeric_only() {
        "it is not a number"
    } else if !pg.is_orderable() {
        "it has no ordering"
    } else {
        "PostgreSQL defines no max/min for its type"
    }
}

/// What PostgreSQL answers with.
///
/// Not the column's own type, which is what this used to publish: `avg` of an
/// `integer` is a `numeric`, and a client generating code against `Int` would
/// have been generating the wrong thing. The mapping is PostgreSQL's, from its
/// aggregate function table.
///
/// The *value* still travels as a JSON number, like a `numeric` column does
/// (see `numeric_accepts_the_json_numbers_it_returns`): digits beyond double
/// precision are rounded by the JSON round-trip on the way out. That is the
/// deliberate trade — a string would make one scalar serialize two ways — and
/// an opt-in exact (stringified) transport is the recorded follow-up.
fn result_type(func: AggFunc, pg: &PgType) -> PgType {
    use AggFunc::*;
    match func {
        // Ordering answers with what it was given.
        Max | Min => pg.clone(),
        // Summing widens, so that a total of many `int4` cannot overflow one.
        Sum => match pg {
            PgType::Int2 | PgType::Int4 => PgType::Int8,
            PgType::Int8 => PgType::Numeric,
            other => other.clone(),
        },
        // Everything arithmetic answers exactly for exact input and in floating
        // point for floating input.
        Avg | Stddev | StddevPop | StddevSamp | Variance | VarPop | VarSamp => match pg {
            PgType::Float4 | PgType::Float8 => PgType::Float8,
            _ => PgType::Numeric,
        },
    }
}

/// Whether `LIKE`-family operators apply, which is what decides whether the
/// comparison input for this scalar carries them.
fn is_stringish(pg: &PgType) -> bool {
    matches!(pg, PgType::Text | PgType::Varchar)
}

/// Whether a comparison operator is published for a column type — and therefore
/// whether the renderer accepts it, since the two must answer alike in both
/// directions. [`comparison_exp`](Builder::comparison_exp) builds the published
/// inputs from this, and `sql.rs` checks every rendered comparison against it;
/// a second hand-written copy of either side is how `_gt` over `jsonb` came to
/// be accepted while never being published.
pub(crate) fn cmp_applies(op: crate::ast::CmpOp, pg: &PgType) -> bool {
    use crate::ast::CmpOp::*;
    match op {
        Eq | Neq => true,
        Gt | Gte | Lt | Lte => pg.is_orderable(),
        Like | ILike | NLike | NILike => is_stringish(pg),
    }
}

/// Why [`cmp_applies`] said no, in the words of the schema.
pub(crate) fn why_cmp_inapplicable(op: crate::ast::CmpOp, pg: &PgType) -> &'static str {
    use crate::ast::CmpOp::*;
    match op {
        Eq | Neq => "always applies",
        Gt | Gte | Lt | Lte => {
            debug_assert!(!pg.is_orderable());
            "json/jsonb values have no published ordering"
        }
        Like | ILike | NLike | NILike => "pattern matching applies to text columns only",
    }
}

const BUILT_IN_SCALARS: [&str; 5] = ["Int", "Float", "String", "Boolean", "ID"];

/// Values of the `order_by` enum — exactly the placements
/// [`crate::ast::NullsOrder`] can express.
const ORDER_BY_VALUES: [&str; 6] = [
    "asc",
    "asc_nulls_first",
    "asc_nulls_last",
    "desc",
    "desc_nulls_first",
    "desc_nulls_last",
];

struct Builder<'a> {
    schema: &'a Schema,
    /// Tables that get types. A relation pointing at one that does not — a
    /// table whose columns are all hidden — must be left out too, or it names a
    /// type nothing defines.
    published: BTreeSet<String>,
    types: BTreeMap<String, TypeDef>,
    /// Scalars reached from a column, so only the comparison inputs that can be
    /// used get published.
    scalars: BTreeMap<String, PgType>,
    /// Scalars that appear only as an aggregate's *result* — `bigint` for the
    /// sum of an `integer`, say. They need a type of their own, but no
    /// comparison input: nothing can be filtered on them, since no column has
    /// one.
    result_scalars: BTreeSet<String>,
}

impl<'a> Builder<'a> {
    fn new(schema: &'a Schema) -> Self {
        Builder {
            schema,
            published: BTreeSet::new(),
            result_scalars: BTreeSet::new(),
            types: BTreeMap::new(),
            scalars: BTreeMap::new(),
        }
    }

    /// The table a relation points at, if it is one this type system publishes.
    fn target(&self, rel: &crate::schema::Relation) -> Option<&'a std::sync::Arc<Table>> {
        self.published
            .contains(&rel.target_table)
            .then(|| self.schema.table(&rel.target_table))
            .flatten()
    }

    fn add(&mut self, def: TypeDef) {
        self.types.insert(def.name().to_string(), def);
    }

    fn note_scalar(&mut self, pg: &PgType) -> String {
        let name = scalar_name(pg);
        if !BUILT_IN_SCALARS.contains(&name.as_str()) {
            self.scalars
                .entry(name.clone())
                .or_insert_with(|| pg.clone());
        } else {
            self.scalars
                .entry(name.clone())
                .or_insert_with(|| pg.clone());
        }
        name
    }

    fn build(mut self) -> TypeSystem {
        // A table whose every column was hidden would produce `type X { }`, an
        // empty select-column enum and an empty insert input — none of which is
        // a legal GraphQL type, so an SDL file carrying them cannot be loaded by
        // the tooling it exists for. Nothing can be selected from such a table
        // anyway. Filtered once, here, rather than at each place that walks the
        // list: a reference to a type that was never defined is exactly the bug
        // this is trying not to introduce.
        let tables: Vec<&std::sync::Arc<Table>> = self
            .schema
            .tables()
            .map(|(_, t)| t)
            .filter(|t| {
                if t.columns().next().is_none() {
                    tracing::warn!(
                        target: "vision_graphql::type_system",
                        table = %t.exposed_name,
                        "skipping table with no exposed columns"
                    );
                    return false;
                }
                true
            })
            .collect();

        self.published = tables.iter().map(|t| t.exposed_name.clone()).collect();

        for t in &tables {
            self.table_types(t);
        }

        self.add(TypeDef::Enum {
            name: "order_by".into(),
            description: Some("Column ordering, and where NULLs are placed.".into()),
            values: ORDER_BY_VALUES.iter().map(|v| (*v).to_string()).collect(),
        });

        // Comparison inputs, one per scalar any column uses.
        let scalars: Vec<(String, PgType)> = self
            .scalars
            .iter()
            .map(|(n, p)| (n.clone(), p.clone()))
            .collect();
        for (name, pg) in &scalars {
            if !BUILT_IN_SCALARS.contains(&name.as_str()) {
                self.add(TypeDef::Scalar {
                    name: name.clone(),
                    description: None,
                });
            }
            self.comparison_exp(name, pg);
        }

        let query_root = self.query_root(&tables);
        let mutation_root = self.mutation_root(&tables);

        let mut ts = TypeSystem {
            types: self.types,
            query_root,
            mutation_root,
        };

        // Built-in scalars are types like any other as far as a client is
        // concerned: it resolves `Int` when it walks an argument's type. Adding
        // exactly the ones something refers to — `Int` and `Boolean` are reached
        // through `limit` and `_is_null` even when no column has those types —
        // keeps the list complete without publishing scalars nothing uses.
        // Result-only scalars first: they are referenced but reach no column, so
        // nothing else would define them.
        let result_scalars = std::mem::take(&mut self.result_scalars);
        for name in result_scalars {
            ts.types.entry(name.clone()).or_insert(TypeDef::Scalar {
                name,
                description: None,
            });
        }
        for name in dangling_references(&ts) {
            debug_assert!(
                BUILT_IN_SCALARS.contains(&name.as_str()),
                "type system references an undefined type: {name}"
            );
            ts.types.insert(
                name.clone(),
                TypeDef::Scalar {
                    name,
                    description: None,
                },
            );
        }
        ts
    }

    /// Whether a real table claims the name of `t`'s aggregate type.
    ///
    /// Real thing wins, so the row type keeps the name and the aggregate
    /// machinery over `t` is not published at all — not its types (`add` would
    /// silently replace the row type in the map, and SDL/`__schema` would
    /// describe the wrong shape) and not the fields that answer with them. The
    /// lowering refuses the same aggregates, so publish and implement stay
    /// aligned.
    fn aggregate_shadowed(&self, t: &Table) -> bool {
        self.schema.table(&type_names::aggregate(t)).is_some()
    }

    /// Every type derived from one table.
    fn table_types(&mut self, t: &Table) {
        self.row_object(t);
        if !self.aggregate_shadowed(t) {
            self.aggregate_objects(t);
        }
        self.bool_exp(t);
        self.order_by_input(t);
        self.select_column_enum(t);
        if !t.read_only {
            self.mutation_inputs(t);
            self.add(TypeDef::Object {
                name: type_names::mutation_response(t),
                description: Some(format!(
                    "Rows affected by a mutation on `{}`.",
                    t.exposed_name
                )),
                fields: vec![
                    Field::new("affected_rows", TypeRef::named("Int").non_null()),
                    Field::new(
                        "returning",
                        TypeRef::named(type_names::row(t)).list_of_non_null_non_null(),
                    ),
                ],
            });
        }
    }

    fn column_fields(&mut self, t: &Table) -> Vec<Field> {
        let mut fields = Vec::new();
        for col in t.columns() {
            let scalar = self.note_scalar(&col.pg_type);
            let mut ty = TypeRef::named(&scalar);
            if !col.nullable {
                ty = ty.non_null();
            }
            let mut f = Field::new(&col.exposed_name, ty);
            if matches!(col.pg_type, PgType::Json | PgType::Jsonb) {
                f = f.with_args(vec![InputValue::new("path", TypeRef::named("String"))
                    .described("Dot-separated path extracted with `#>`, e.g. \"a.b.0\".")]);
            }
            fields.push(f);
        }
        fields
    }

    fn row_object(&mut self, t: &Table) {
        let mut fields = self.column_fields(t);
        for (name, rel) in t.relations() {
            let Some(target) = self.target(rel) else {
                continue;
            };
            let target_row = type_names::row(target).to_string();
            match rel.kind {
                RelKind::Object => {
                    fields.push(Field::new(name, TypeRef::named(target_row)));
                }
                RelKind::Array => {
                    fields.push(
                        Field::new(
                            name,
                            TypeRef::named(&target_row).list_of_non_null_non_null(),
                        )
                        .with_args(list_args(target)),
                    );
                    // The same field the root offers, over this row's children.
                    // Only for array relations: an object relation is one row
                    // and the lowering refuses to aggregate it, so publishing
                    // one would advertise what would then be rejected. And only
                    // when no real column or relation answers to the name — the
                    // lowering lets the real thing win over a synthesized
                    // field, so publishing both would put two fields under one
                    // key on this object. A shadowed target aggregate type
                    // (see aggregate_shadowed) takes the field with it.
                    let agg_name = format!("{name}_aggregate");
                    if t.find_column(&agg_name).is_none()
                        && t.find_relation(&agg_name).is_none()
                        && !self.aggregate_shadowed(target)
                    {
                        fields.push(
                            Field::new(
                                agg_name,
                                TypeRef::named(type_names::aggregate(target)).non_null(),
                            )
                            .with_args(aggregate_args(target)),
                        );
                    }
                }
            }
        }
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        self.add(TypeDef::Object {
            name: type_names::row(t).to_string(),
            description: Some(format!(
                "Rows of `{}`.`{}`.",
                t.physical_schema, t.physical_name
            )),
            fields,
        });
    }

    fn aggregate_objects(&mut self, t: &Table) {
        let row = type_names::row(t).to_string();
        self.add(TypeDef::Object {
            name: type_names::aggregate(t),
            description: None,
            fields: vec![
                Field::new("aggregate", TypeRef::named(type_names::aggregate_fields(t))),
                Field::new("nodes", TypeRef::named(&row).list_of_non_null_non_null()),
            ],
        });

        let mut agg_fields = vec![
            Field::new("count", TypeRef::named("Int").non_null()).with_args(vec![
                InputValue::new(
                    "columns",
                    TypeRef::named(select_column_enum_name(t)).non_null().list(),
                ),
                InputValue::new("distinct", TypeRef::named("Boolean")),
            ]),
        ];

        // sum/avg over numeric columns; max/min where PostgreSQL defines them.
        // A group with no columns to put in it is not published at all, rather
        // than published empty.
        for func in AggFunc::ALL {
            let cols: Vec<Field> = t
                .columns()
                .filter(|c| applies(func, &c.pg_type))
                .map(|c| {
                    let name = scalar_name(&result_type(func, &c.pg_type));
                    self.result_scalars.insert(name.clone());
                    Field::new(&c.exposed_name, TypeRef::named(name))
                })
                .collect();
            if cols.is_empty() {
                continue;
            }
            let op = func.name();
            let name = type_names::agg_op_fields(t, op);
            let mut cols = cols;
            cols.sort_by(|a, b| a.name.cmp(&b.name));
            self.add(TypeDef::Object {
                name: name.clone(),
                description: None,
                fields: cols,
            });
            agg_fields.push(Field::new(op, TypeRef::named(name)));
        }

        self.add(TypeDef::Object {
            name: type_names::aggregate_fields(t),
            description: None,
            fields: agg_fields,
        });
    }

    fn bool_exp(&mut self, t: &Table) {
        let name = bool_exp_name(t);
        let mut fields = vec![
            InputValue::new("_and", TypeRef::named(&name).non_null().list()),
            InputValue::new("_or", TypeRef::named(&name).non_null().list()),
            InputValue::new("_not", TypeRef::named(&name)),
        ];
        for col in t.columns() {
            let scalar = self.note_scalar(&col.pg_type);
            fields.push(InputValue::new(
                &col.exposed_name,
                TypeRef::named(comparison_exp_name(&scalar)),
            ));
        }
        // A relation in a `where` becomes EXISTS, for array and object alike.
        for (rel_name, rel) in t.relations() {
            let Some(target) = self.target(rel) else {
                continue;
            };
            fields.push(InputValue::new(
                rel_name,
                TypeRef::named(bool_exp_name(target)),
            ));
        }
        self.add(TypeDef::InputObject {
            name,
            description: Some(format!("Filter over `{}`.", t.exposed_name)),
            fields,
        });
    }

    fn order_by_input(&mut self, t: &Table) {
        let mut fields: Vec<InputValue> = t
            .columns()
            .map(|c| InputValue::new(&c.exposed_name, TypeRef::named("order_by")))
            .collect();
        // Only object relations: ordering by a column reached through an array
        // relation has no single value to sort on, and the lowering rejects it.
        for (rel_name, rel) in t.relations() {
            if rel.kind != RelKind::Object {
                continue;
            }
            let Some(target) = self.target(rel) else {
                continue;
            };
            fields.push(InputValue::new(
                rel_name,
                TypeRef::named(order_by_name(target)),
            ));
        }
        self.add(TypeDef::InputObject {
            name: order_by_name(t),
            description: None,
            fields,
        });
    }

    fn select_column_enum(&mut self, t: &Table) {
        self.add(TypeDef::Enum {
            name: select_column_enum_name(t),
            description: Some(format!("Columns of `{}`.", t.exposed_name)),
            values: t.columns().map(|c| c.exposed_name.clone()).collect(),
        });
    }

    fn mutation_inputs(&mut self, t: &Table) {
        // Constraints whose every column is exposed — see the note where the
        // enum is added. Computed up front because the nested insert wrappers
        // below are built before the `on_conflict` type exists to look up.
        let publishable: Vec<String> = t
            .unique_constraints
            .iter()
            .filter(|(_, cols)| cols.iter().all(|c| t.find_column(c).is_some()))
            .map(|(name, _)| name.clone())
            .collect();

        let row_cols: Vec<InputValue> = t
            .columns()
            .map(|c| {
                let scalar = scalar_name(&c.pg_type);
                InputValue::new(&c.exposed_name, TypeRef::named(scalar))
            })
            .collect();

        let mut insert_fields = row_cols.clone();
        for (rel_name, rel) in t.relations() {
            let Some(target) = self.target(rel) else {
                continue;
            };
            if target.read_only {
                continue;
            }
            let input = match rel.kind {
                RelKind::Array => arr_rel_insert_name(target),
                RelKind::Object => obj_rel_insert_name(target),
            };
            insert_fields.push(InputValue::new(rel_name, TypeRef::named(input)));
        }
        self.add(TypeDef::InputObject {
            name: insert_input_name(t),
            description: None,
            fields: insert_fields,
        });

        self.add(TypeDef::InputObject {
            name: set_input_name(t),
            description: None,
            fields: row_cols,
        });

        // The nested wrappers are published for every writable table, since any
        // table can be the target of a nested insert from somewhere. Their
        // `on_conflict` exists only where the table has a constraint to name —
        // publishing it unconditionally would point at a type that is never
        // defined.
        let nested_on_conflict = (!publishable.is_empty())
            .then(|| InputValue::new("on_conflict", TypeRef::named(on_conflict_name(t))));

        let mut arr_fields = vec![InputValue::new(
            "data",
            TypeRef::named(insert_input_name(t)).list_of_non_null_non_null(),
        )];
        arr_fields.extend(nested_on_conflict.clone());
        self.add(TypeDef::InputObject {
            name: arr_rel_insert_name(t),
            description: None,
            fields: arr_fields,
        });

        let mut obj_fields = vec![InputValue::new(
            "data",
            TypeRef::named(insert_input_name(t)).non_null(),
        )];
        obj_fields.extend(nested_on_conflict);
        self.add(TypeDef::InputObject {
            name: obj_rel_insert_name(t),
            description: None,
            fields: obj_fields,
        });

        // `on_conflict` is only meaningful where there is a constraint to name,
        // and only constraints whose columns are all exposed are published: a
        // constraint name usually contains its column names, so publishing one
        // over a hidden column hands back what hiding it withheld. The engine
        // still accepts it — see `Table::unique_constraints`.
        if !publishable.is_empty() {
            self.add(TypeDef::Enum {
                name: constraint_enum_name(t),
                description: Some(format!(
                    "Unique constraints on `{}`, nameable in `on_conflict`.",
                    t.exposed_name
                )),
                values: publishable,
            });
            self.add(TypeDef::Enum {
                name: update_column_enum_name(t),
                description: None,
                values: t.columns().map(|c| c.exposed_name.clone()).collect(),
            });
            self.add(TypeDef::InputObject {
                name: on_conflict_name(t),
                description: None,
                fields: vec![
                    InputValue::new(
                        "constraint",
                        TypeRef::named(constraint_enum_name(t)).non_null(),
                    ),
                    InputValue::new(
                        "update_columns",
                        TypeRef::named(update_column_enum_name(t)).list_of_non_null_non_null(),
                    ),
                    InputValue::new("where", TypeRef::named(bool_exp_name(t))),
                ],
            });
        }

        // Only when every key column is still exposed. `hide_columns` can take
        // one out from under the key — merge keeps the `primary_key` and warns —
        // and an input object with no fields is not a legal GraphQL type.
        if let Some(fields) = pk_args(t) {
            self.add(TypeDef::InputObject {
                name: pk_columns_input_name(t),
                description: None,
                fields,
            });
        }
    }

    fn comparison_exp(&mut self, scalar: &str, pg: &PgType) {
        let named = || TypeRef::named(scalar);
        let mut fields = vec![
            InputValue::new("_eq", named()),
            InputValue::new("_neq", named()),
            InputValue::new("_in", named().non_null().list()),
            InputValue::new("_nin", named().non_null().list()),
            InputValue::new("_is_null", TypeRef::named("Boolean")),
        ];
        if cmp_applies(crate::ast::CmpOp::Gt, pg) {
            for op in ["_gt", "_gte", "_lt", "_lte"] {
                fields.push(InputValue::new(op, named()));
            }
        }
        if cmp_applies(crate::ast::CmpOp::Like, pg) {
            for op in ["_like", "_nlike", "_ilike", "_nilike"] {
                fields.push(InputValue::new(op, named()));
            }
        }
        self.add(TypeDef::InputObject {
            name: comparison_exp_name(scalar),
            description: Some(format!("Comparisons available on `{scalar}`.")),
            fields,
        });
    }

    fn query_root(&mut self, tables: &[&std::sync::Arc<Table>]) -> String {
        // A real table named `users_aggregate` keeps its root field, and the
        // synthesized aggregate over `users` yields — the same real-thing-wins
        // rule the lowering applies. Emitting both meant two fields under one
        // name on this object, which is not a legal GraphQL type.
        let taken: std::collections::HashSet<&str> =
            tables.iter().map(|t| t.exposed_name.as_str()).collect();
        let mut fields = Vec::new();
        for t in tables {
            let row = type_names::row(t).to_string();
            fields.push(
                Field::new(
                    &t.exposed_name,
                    TypeRef::named(&row).list_of_non_null_non_null(),
                )
                .with_args(list_args(t))
                .described(format!("Rows of `{}`.", t.exposed_name)),
            );
            if !taken.contains(type_names::aggregate(t).as_str()) {
                fields.push(
                    Field::new(
                        type_names::aggregate(t),
                        TypeRef::named(type_names::aggregate(t)).non_null(),
                    )
                    .with_args(aggregate_args(t)),
                );
            }
            let by_pk = format!("{}_by_pk", t.exposed_name);
            if !taken.contains(by_pk.as_str()) {
                if let Some(args) = pk_args(t) {
                    fields.push(Field::new(by_pk, TypeRef::named(&row)).with_args(args));
                }
            }
        }
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        let name = type_names::QUERY_ROOT.to_string();
        self.add(TypeDef::Object {
            name: name.clone(),
            description: None,
            fields,
        });
        name
    }

    fn mutation_root(&mut self, tables: &[&std::sync::Arc<Table>]) -> Option<String> {
        // The query root's real-thing-wins rule, for mutation names: with
        // tables `users` and `users_one`, the name `insert_users_one` reads
        // literally as the bulk insert into `users_one`, so the synthesized
        // one-row insert for `users` yields — the lowering resolves it the
        // same way. Likewise `update_/delete_<t>_by_pk` against a table
        // actually named `<t>_by_pk`.
        let taken: std::collections::HashSet<&str> =
            tables.iter().map(|t| t.exposed_name.as_str()).collect();
        let mut fields = Vec::new();
        for t in tables {
            if t.read_only {
                continue;
            }
            let row = type_names::row(t).to_string();
            let response = TypeRef::named(type_names::mutation_response(t));
            let on_conflict = self
                .types
                .contains_key(&on_conflict_name(t))
                .then(|| InputValue::new("on_conflict", TypeRef::named(on_conflict_name(t))));

            let mut insert_args = vec![InputValue::new(
                "objects",
                TypeRef::named(insert_input_name(t)).list_of_non_null_non_null(),
            )];
            insert_args.extend(on_conflict.clone());
            fields.push(
                Field::new(format!("insert_{}", t.exposed_name), response.clone())
                    .with_args(insert_args),
            );

            if !taken.contains(format!("{}_one", t.exposed_name).as_str()) {
                let mut insert_one_args = vec![InputValue::new(
                    "object",
                    TypeRef::named(insert_input_name(t)).non_null(),
                )];
                insert_one_args.extend(on_conflict);
                fields.push(
                    Field::new(
                        format!("insert_{}_one", t.exposed_name),
                        TypeRef::named(&row),
                    )
                    .with_args(insert_one_args),
                );
            }

            fields.push(
                Field::new(format!("update_{}", t.exposed_name), response.clone()).with_args(vec![
                    InputValue::new("where", TypeRef::named(bool_exp_name(t)).non_null()),
                    InputValue::new("_set", TypeRef::named(set_input_name(t))),
                ]),
            );
            fields.push(
                Field::new(format!("delete_{}", t.exposed_name), response).with_args(vec![
                    InputValue::new("where", TypeRef::named(bool_exp_name(t)).non_null()),
                ]),
            );

            // Both `_by_pk` mutations hang on the same condition the query root
            // uses: a key every column of which is exposed. Publishing
            // `update_by_pk` on a weaker test left it pointing at an input type
            // that was never defined.
            if pk_args(t).is_some() && !taken.contains(format!("{}_by_pk", t.exposed_name).as_str())
            {
                fields.push(
                    Field::new(
                        format!("update_{}_by_pk", t.exposed_name),
                        TypeRef::named(&row),
                    )
                    .with_args(vec![
                        InputValue::new(
                            "pk_columns",
                            TypeRef::named(pk_columns_input_name(t)).non_null(),
                        ),
                        InputValue::new("_set", TypeRef::named(set_input_name(t))),
                    ]),
                );
                if let Some(args) = pk_args(t) {
                    fields.push(
                        Field::new(
                            format!("delete_{}_by_pk", t.exposed_name),
                            TypeRef::named(&row),
                        )
                        .with_args(args),
                    );
                }
            }
        }
        if fields.is_empty() {
            return None;
        }
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        let name = type_names::MUTATION_ROOT.to_string();
        self.add(TypeDef::Object {
            name: name.clone(),
            description: None,
            fields,
        });
        Some(name)
    }
}

/// The arguments every row list takes — root fields and array relations alike,
/// which is exactly the set [`crate::parser::lower_args`] accepts.
fn list_args(t: &Table) -> Vec<InputValue> {
    vec![
        InputValue::new("where", TypeRef::named(bool_exp_name(t))),
        InputValue::new(
            "order_by",
            TypeRef::named(order_by_name(t)).non_null().list(),
        ),
        InputValue::new("limit", TypeRef::named("Int")),
        InputValue::new("offset", TypeRef::named("Int")),
        InputValue::new(
            "distinct_on",
            TypeRef::named(select_column_enum_name(t)).non_null().list(),
        ),
    ]
}

/// Arguments an `_aggregate` field takes.
///
/// [`list_args`] minus `distinct_on`: the aggregate source does not render it,
/// and publishing an argument the lowering then refuses is the failure this
/// type system exists not to have.
fn aggregate_args(t: &Table) -> Vec<InputValue> {
    list_args(t)
        .into_iter()
        .filter(|a| a.name != "distinct_on")
        .collect()
}

/// `_by_pk` arguments, or `None` for a table with no primary key — where the
/// engine publishes no `_by_pk` field at all.
fn pk_args(t: &Table) -> Option<Vec<InputValue>> {
    if t.primary_key.is_empty() {
        return None;
    }
    let args: Vec<InputValue> = t
        .primary_key
        .iter()
        .filter_map(|pk| t.find_column(pk))
        .map(|c| {
            InputValue::new(
                &c.exposed_name,
                TypeRef::named(scalar_name(&c.pg_type)).non_null(),
            )
        })
        .collect();
    (args.len() == t.primary_key.len()).then_some(args)
}

pub fn bool_exp_name(t: &Table) -> String {
    format!("{}_bool_exp", t.exposed_name)
}

pub fn order_by_name(t: &Table) -> String {
    format!("{}_order_by", t.exposed_name)
}

pub fn select_column_enum_name(t: &Table) -> String {
    format!("{}_select_column", t.exposed_name)
}

pub fn update_column_enum_name(t: &Table) -> String {
    format!("{}_update_column", t.exposed_name)
}

pub fn constraint_enum_name(t: &Table) -> String {
    format!("{}_constraint", t.exposed_name)
}

pub fn insert_input_name(t: &Table) -> String {
    format!("{}_insert_input", t.exposed_name)
}

pub fn set_input_name(t: &Table) -> String {
    format!("{}_set_input", t.exposed_name)
}

pub fn on_conflict_name(t: &Table) -> String {
    format!("{}_on_conflict", t.exposed_name)
}

pub fn arr_rel_insert_name(t: &Table) -> String {
    format!("{}_arr_rel_insert_input", t.exposed_name)
}

pub fn obj_rel_insert_name(t: &Table) -> String {
    format!("{}_obj_rel_insert_input", t.exposed_name)
}

pub fn pk_columns_input_name(t: &Table) -> String {
    format!("{}_pk_columns_input", t.exposed_name)
}

pub fn comparison_exp_name(scalar: &str) -> String {
    format!("{scalar}_comparison_exp")
}

/// Names referenced by some type but never defined — a bug in the builder, and
/// the one thing a hand-built type system gets wrong silently. Used by the
/// tests, and cheap enough to be worth having.
pub fn dangling_references(ts: &TypeSystem) -> BTreeSet<String> {
    let defined: BTreeSet<&str> = ts.types().map(TypeDef::name).collect();
    let mut missing = BTreeSet::new();
    let mut check = |r: &TypeRef| {
        let n = r.base_name();
        if !defined.contains(n) {
            missing.insert(n.to_string());
        }
    };
    for def in ts.types() {
        match def {
            TypeDef::Object { fields, .. } => {
                for f in fields {
                    check(&f.ty);
                    for a in &f.args {
                        check(&a.ty);
                    }
                }
            }
            TypeDef::InputObject { fields, .. } => {
                for f in fields {
                    check(&f.ty);
                }
            }
            TypeDef::Scalar { .. } | TypeDef::Enum { .. } => {}
        }
    }
    missing
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{PgType, Relation, Schema, Table};

    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .column("data", "data", PgType::Jsonb, true)
                    .primary_key(&["id"])
                    .unique_constraint("users_pkey", &["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .column("score", "score", PgType::Numeric, true)
                    .primary_key(&["id"])
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .table(
                Table::new("summary", "public", "summary")
                    .column("total", "total", PgType::Int8, true)
                    .read_only(true),
            )
            .build()
    }

    fn ts() -> TypeSystem {
        TypeSystem::build(&schema())
    }

    #[test]
    fn every_referenced_type_is_defined() {
        assert!(
            dangling_references(&ts()).is_empty(),
            "{:?}",
            dangling_references(&ts())
        );
    }

    #[test]
    fn row_object_carries_columns_and_relations() {
        let ts = ts();
        let TypeDef::Object { fields, .. } = ts.get("users").unwrap() else {
            panic!("users should be an object");
        };
        let posts = fields.iter().find(|f| f.name == "posts").unwrap();
        assert_eq!(
            posts.ty,
            TypeRef::named("posts").non_null().list().non_null()
        );
        // An array relation takes the same arguments a root list does.
        assert!(posts.args.iter().any(|a| a.name == "where"));
        assert!(posts.args.iter().any(|a| a.name == "distinct_on"));
        // `path` only where it works.
        let data = fields.iter().find(|f| f.name == "data").unwrap();
        assert!(data.args.iter().any(|a| a.name == "path"));
        let name = fields.iter().find(|f| f.name == "name").unwrap();
        assert!(name.args.is_empty());
    }

    #[test]
    fn comparison_inputs_carry_only_operators_the_engine_lowers() {
        let ts = ts();
        let TypeDef::InputObject { fields, .. } = ts.get("String_comparison_exp").unwrap() else {
            panic!("expected input object");
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"_ilike"));
        assert!(names.contains(&"_is_null"));
        // Not implemented, so not advertised.
        assert!(!names.contains(&"_regex"));
        assert!(!names.contains(&"_similar"));

        let TypeDef::InputObject { fields, .. } = ts.get("jsonb_comparison_exp").unwrap() else {
            panic!("expected input object");
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        // jsonb has no ordering worth publishing, and no LIKE.
        assert!(!names.contains(&"_gt"));
        assert!(!names.contains(&"_like"));
        assert!(names.contains(&"_eq"));
    }

    #[test]
    fn read_only_tables_are_readable_but_get_no_mutation_fields() {
        let ts = ts();
        assert!(ts.get("summary").is_some());
        assert!(ts.get("summary_mutation_response").is_none());
        let TypeDef::Object { fields, .. } = ts.get(ts.mutation_root().unwrap()).unwrap() else {
            panic!("expected object");
        };
        assert!(!fields.iter().any(|f| f.name.contains("summary")));
        assert!(fields.iter().any(|f| f.name == "insert_users"));
        assert!(fields.iter().any(|f| f.name == "update_users_by_pk"));
    }

    #[test]
    fn on_conflict_is_published_only_where_a_constraint_exists() {
        let ts = ts();
        assert!(ts.get("users_on_conflict").is_some());
        assert!(ts.get("users_constraint").is_some());
        // `posts` has no unique constraint declared.
        assert!(ts.get("posts_on_conflict").is_none());
        let TypeDef::Object { fields, .. } = ts.get(ts.mutation_root().unwrap()).unwrap() else {
            panic!("expected object");
        };
        let insert_posts = fields.iter().find(|f| f.name == "insert_posts").unwrap();
        assert!(!insert_posts.args.iter().any(|a| a.name == "on_conflict"));
        let insert_users = fields.iter().find(|f| f.name == "insert_users").unwrap();
        assert!(insert_users.args.iter().any(|a| a.name == "on_conflict"));

        // The nested wrappers hang off the same condition, and are built before
        // the `on_conflict` type exists — so they cannot be decided by looking
        // it up.
        let TypeDef::InputObject { fields, .. } = ts.get("users_arr_rel_insert_input").unwrap()
        else {
            panic!("expected input object");
        };
        assert!(fields.iter().any(|f| f.name == "on_conflict"), "{fields:?}");
        let TypeDef::InputObject { fields, .. } = ts.get("posts_arr_rel_insert_input").unwrap()
        else {
            panic!("expected input object");
        };
        assert!(
            !fields.iter().any(|f| f.name == "on_conflict"),
            "{fields:?}"
        );
    }

    #[test]
    fn aggregate_groups_skip_columns_they_cannot_apply_to() {
        let ts = ts();
        // sum/avg reach the numeric columns only.
        let TypeDef::Object { fields, .. } = ts.get("users_sum_fields").unwrap() else {
            panic!("expected object");
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["id"], "only the numeric column");
        // max applies to text and dates too.
        let TypeDef::Object { fields, .. } = ts.get("users_max_fields").unwrap() else {
            panic!("expected object");
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"name"));
        assert!(!names.contains(&"data"), "jsonb has no max worth offering");
    }

    /// `boolean`, `uuid` and enums order — `_gt` works on every one — but
    /// PostgreSQL defines no `max`/`min` over them, so publishing those fields
    /// published a query whose only possible answer was a database error.
    #[test]
    fn max_min_skip_types_postgres_cannot_max() {
        let schema = Schema::builder()
            .table(
                Table::new("things", "public", "things")
                    .column("id", "id", PgType::Int4, false)
                    .column("label", "label", PgType::Text, true)
                    .column("day", "day", PgType::Date, true)
                    .column("flag", "flag", PgType::Bool, true)
                    .column("token", "token", PgType::Uuid, true)
                    .column(
                        "mood",
                        "mood",
                        PgType::Enum {
                            schema: "public".into(),
                            name: "mood".into(),
                        },
                        true,
                    )
                    .primary_key(&["id"]),
            )
            .build();
        let ts = TypeSystem::build(&schema);
        let TypeDef::Object { fields, .. } = ts.get("things_max_fields").unwrap() else {
            panic!("expected object");
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"label"), "{names:?}");
        assert!(names.contains(&"day"), "{names:?}");
        for absent in ["flag", "token", "mood"] {
            assert!(
                !names.contains(&absent),
                "PostgreSQL has no max over '{absent}': {names:?}"
            );
        }
        // The ordering comparisons stay: the types order fine.
        let TypeDef::InputObject { fields, .. } = ts.get("uuid_comparison_exp").unwrap() else {
            panic!("expected input object");
        };
        assert!(fields.iter().any(|f| f.name == "_gt"), "{fields:?}");
    }

    #[test]
    fn order_by_input_follows_object_relations_only() {
        let ts = ts();
        let TypeDef::InputObject { fields, .. } = ts.get("posts_order_by").unwrap() else {
            panic!("expected input object");
        };
        assert!(fields.iter().any(|f| f.name == "user"));
        let TypeDef::InputObject { fields, .. } = ts.get("users_order_by").unwrap() else {
            panic!("expected input object");
        };
        assert!(
            !fields.iter().any(|f| f.name == "posts"),
            "an array relation has no single value to sort on"
        );
    }

    #[test]
    fn a_table_without_a_primary_key_gets_no_by_pk() {
        let ts = ts();
        let TypeDef::Object { fields, .. } = ts.get(ts.query_root()).unwrap() else {
            panic!("expected object");
        };
        assert!(fields.iter().any(|f| f.name == "users_by_pk"));
        assert!(!fields.iter().any(|f| f.name == "summary_by_pk"));
    }

    /// `hide_columns` can take a column out from under the primary key: merge
    /// keeps the key and warns. Publishing `<t>_pk_columns_input` on the weaker
    /// test produced `input X { }`, which no GraphQL parser accepts — so a
    /// committed SDL file would not load, and `sdl --check` would keep
    /// certifying it.
    #[test]
    fn a_key_whose_column_is_hidden_publishes_no_by_pk_surface() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    // `id` is the declared key but is not exposed.
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"]),
            )
            .build();
        let ts = TypeSystem::build(&schema);
        assert!(ts.get("users_pk_columns_input").is_none());
        assert!(
            dangling_references(&ts).is_empty(),
            "{:?}",
            dangling_references(&ts)
        );

        let TypeDef::Object { fields, .. } = ts.get(ts.query_root()).unwrap() else {
            panic!("expected object");
        };
        assert!(!fields.iter().any(|f| f.name == "users_by_pk"));
        let TypeDef::Object { fields, .. } = ts.get(ts.mutation_root().unwrap()).unwrap() else {
            panic!("expected object");
        };
        assert!(
            !fields.iter().any(|f| f.name.ends_with("_by_pk")),
            "{fields:?}"
        );
    }

    #[test]
    fn a_table_with_nothing_exposed_is_skipped_rather_than_emitted_empty() {
        let schema = Schema::builder()
            .table(Table::new("hidden", "public", "hidden"))
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build();
        let ts = TypeSystem::build(&schema);
        assert!(ts.get("hidden").is_none());
        assert!(ts.get("hidden_select_column").is_none());
        let TypeDef::Object { fields, .. } = ts.get(ts.query_root()).unwrap() else {
            panic!("expected object");
        };
        assert!(
            !fields.iter().any(|f| f.name.starts_with("hidden")),
            "{fields:?}"
        );
        assert!(dangling_references(&ts).is_empty());
    }

    #[test]
    fn a_relation_pointing_at_a_skipped_table_is_left_out() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"])
                    .relation("ghosts", Relation::array("ghosts").on([("id", "user_id")])),
            )
            .table(Table::new("ghosts", "public", "ghosts"))
            .build();
        let ts = TypeSystem::build(&schema);
        assert!(
            dangling_references(&ts).is_empty(),
            "{:?}",
            dangling_references(&ts)
        );
        let TypeDef::Object { fields, .. } = ts.get("users").unwrap() else {
            panic!("expected object");
        };
        assert!(!fields.iter().any(|f| f.name == "ghosts"), "{fields:?}");
    }

    #[test]
    fn a_schema_with_nothing_writable_has_no_mutation_root() {
        let schema = Schema::builder()
            .table(
                Table::new("summary", "public", "summary")
                    .column("total", "total", PgType::Int8, true)
                    .read_only(true),
            )
            .build();
        assert!(TypeSystem::build(&schema).mutation_root().is_none());
    }

    /// A real table named like a synthesized field keeps its own root field,
    /// and the synthesized one yields — two fields under one name is not a
    /// legal GraphQL object, and the lowering resolves the name to the real
    /// table anyway.
    #[test]
    fn a_real_table_shadowing_a_synthesized_field_is_published_once() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .table(
                Table::new("users_aggregate", "public", "users_aggregate").column(
                    "id",
                    "id",
                    PgType::Int4,
                    false,
                ),
            )
            .table(Table::new("users_by_pk", "public", "users_by_pk").column(
                "id",
                "id",
                PgType::Int4,
                false,
            ))
            .build();
        let ts = TypeSystem::build(&schema);
        let TypeDef::Object { fields, .. } = ts.get(ts.query_root()).unwrap() else {
            panic!("expected object");
        };
        for name in ["users_aggregate", "users_by_pk"] {
            assert_eq!(
                fields.iter().filter(|f| f.name == name).count(),
                1,
                "{name}: {fields:?}"
            );
        }
        // The surviving field is the real table's list field, not the
        // aggregate object.
        let f = fields.iter().find(|f| f.name == "users_aggregate").unwrap();
        assert!(
            matches!(&f.ty, TypeRef::NonNull(inner) if matches!(**inner, TypeRef::List(_))),
            "{f:?}"
        );
    }

    /// The mutation root follows the same rule: `insert_users_one` belongs to
    /// the table literally named `users_one`, so the synthesized one-row
    /// insert for `users` yields — publishing both put two fields under one
    /// name on the mutation root.
    #[test]
    fn a_real_table_shadowing_a_mutation_field_is_published_once() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .table(
                Table::new("users_one", "public", "users_one")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .table(
                Table::new("users_by_pk", "public", "users_by_pk")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build();
        let ts = TypeSystem::build(&schema);
        let TypeDef::Object { fields, .. } = ts.get(ts.mutation_root().unwrap()).unwrap() else {
            panic!("expected object");
        };
        for name in [
            "insert_users_one",
            "update_users_by_pk",
            "delete_users_by_pk",
        ] {
            assert_eq!(
                fields.iter().filter(|f| f.name == name).count(),
                1,
                "{name}: {fields:?}"
            );
        }
        // The surviving insert_users_one is the bulk insert into `users_one`:
        // it takes `objects`, not `object`.
        let f = fields
            .iter()
            .find(|f| f.name == "insert_users_one")
            .unwrap();
        assert!(f.args.iter().any(|a| a.name == "objects"), "{f:?}");
    }

    /// When a real table claims the aggregate type's name, the aggregate
    /// machinery over the shadowed table is not published at all — its types
    /// would silently replace the row type in the map, and the fields would
    /// point at the wrong shape.
    #[test]
    fn a_shadowed_aggregate_type_is_withdrawn_with_its_fields() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .relation("author", Relation::array("users").on([("user_id", "id")])),
            )
            .table(
                Table::new("users_aggregate", "public", "users_aggregate").column(
                    "id",
                    "id",
                    PgType::Int4,
                    false,
                ),
            )
            .build();
        let ts = TypeSystem::build(&schema);
        // `users_aggregate` is the real table's row type, not the aggregate.
        let TypeDef::Object { fields, .. } = ts.get("users_aggregate").unwrap() else {
            panic!("expected object");
        };
        assert!(fields.iter().any(|f| f.name == "id"), "{fields:?}");
        assert!(!fields.iter().any(|f| f.name == "aggregate"), "{fields:?}");
        // The relation aggregate field that would answer with the withdrawn
        // type is withdrawn with it…
        let TypeDef::Object { fields, .. } = ts.get("posts").unwrap() else {
            panic!("expected object");
        };
        assert!(
            !fields.iter().any(|f| f.name == "author_aggregate"),
            "{fields:?}"
        );
        // …and nothing dangles.
        assert!(
            dangling_references(&ts).is_empty(),
            "{:?}",
            dangling_references(&ts)
        );
    }

    /// The same rule one level down: a column named `<rel>_aggregate` wins
    /// over the field synthesized from the relation.
    #[test]
    fn a_column_shadowing_a_relation_aggregate_is_published_once() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("posts_aggregate", "posts_aggregate", PgType::Text, true)
                    .primary_key(&["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false),
            )
            .build();
        let ts = TypeSystem::build(&schema);
        let TypeDef::Object { fields, .. } = ts.get("users").unwrap() else {
            panic!("expected object");
        };
        assert_eq!(
            fields
                .iter()
                .filter(|f| f.name == "posts_aggregate")
                .count(),
            1,
            "{fields:?}"
        );
    }
}
