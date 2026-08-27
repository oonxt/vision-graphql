//! Intermediate representation for queries.

use serde_json::Value;
use std::borrow::Cow;

use crate::error::{Error, Result};
use crate::types::Inputs;

/// A value position in the IR: known when the query was lowered, or a name
/// resolved when it executes.
///
/// Eager lowering ([`crate::Engine::query`]) substitutes variables while it
/// lowers, so it only ever produces [`Val::Lit`]. Symbolic lowering
/// ([`crate::Engine::compile`]) leaves them as [`Val::Var`] / [`Val::ScopeParam`],
/// which is what lets one lowered query — and one rendered SQL string — serve
/// every set of variable values and every principal.
#[derive(Debug, Clone, PartialEq)]
pub enum Val {
    /// A constant of the query text.
    Lit(Value),
    /// GraphQL variable `$name`, taken from the request's variables.
    Var(String),
    /// Scope parameter, taken from the request's [`crate::predicate::Principal`].
    ScopeParam(String),
    /// A list with value positions of its own, e.g. `_in: [1, $x]`.
    Array(Vec<Val>),
    /// An object with value positions of its own, e.g. `_eq: {a: $x}` on a
    /// json/jsonb column.
    Object(Vec<(String, Val)>),
}

impl Val {
    /// The literal this value carries, if it is one.
    pub fn as_lit(&self) -> Option<&Value> {
        match self {
            Val::Lit(v) => Some(v),
            _ => None,
        }
    }

    /// Whether this value is fully determined by the query text — i.e. it needs
    /// no variables and no principal to become a bind.
    pub fn is_lit(&self) -> bool {
        match self {
            Val::Lit(_) => true,
            Val::Array(items) => items.iter().all(Val::is_lit),
            Val::Object(items) => items.iter().all(|(_, v)| v.is_lit()),
            Val::Var(_) | Val::ScopeParam(_) => false,
        }
    }

    /// Fold a composite that turned out to contain no variables back into a
    /// plain [`Val::Lit`], so everything downstream only has to special-case
    /// composites that actually defer something.
    pub fn collapse(self) -> Val {
        if !self.is_lit() {
            return self;
        }
        match self {
            Val::Array(items) => Val::Lit(Value::Array(
                items
                    .into_iter()
                    .map(|i| match i.collapse() {
                        Val::Lit(v) => v,
                        _ => unreachable!("checked is_lit"),
                    })
                    .collect(),
            )),
            Val::Object(items) => Val::Lit(Value::Object(
                items
                    .into_iter()
                    .map(|(k, i)| match i.collapse() {
                        Val::Lit(v) => (k, v),
                        _ => unreachable!("checked is_lit"),
                    })
                    .collect(),
            )),
            other => other,
        }
    }

    /// Substitute variables and scope parameters, yielding a concrete JSON
    /// value. Borrows when nothing needs building.
    pub fn resolve<'a>(&'a self, inputs: &'a Inputs<'a>) -> Result<Cow<'a, Value>> {
        match self {
            Val::Lit(v) => Ok(Cow::Borrowed(v)),
            Val::Var(name) => inputs.variable(name).map(Cow::Borrowed),
            Val::ScopeParam(name) => inputs.scope_param(name).map(Cow::Borrowed),
            Val::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(item.resolve(inputs)?.into_owned());
                }
                Ok(Cow::Owned(Value::Array(out)))
            }
            Val::Object(items) => {
                let mut out = serde_json::Map::with_capacity(items.len());
                for (k, item) in items {
                    out.insert(k.clone(), item.resolve(inputs)?.into_owned());
                }
                Ok(Cow::Owned(Value::Object(out)))
            }
        }
    }

    /// Names of the GraphQL variables this value reads.
    pub fn collect_vars(&self, out: &mut Vec<String>) {
        match self {
            Val::Var(name) => out.push(name.clone()),
            Val::Array(items) => items.iter().for_each(|i| i.collect_vars(out)),
            Val::Object(items) => items.iter().for_each(|(_, i)| i.collect_vars(out)),
            Val::Lit(_) | Val::ScopeParam(_) => {}
        }
    }
}

impl<T: Into<Value>> From<T> for Val {
    fn from(v: T) -> Self {
        Val::Lit(v.into())
    }
}

/// Compare against a plain JSON value. Only a literal can match one: a value
/// still waiting on a variable is not equal to anything yet.
impl PartialEq<Value> for Val {
    fn eq(&self, other: &Value) -> bool {
        matches!(self, Val::Lit(v) if v == other)
    }
}

/// A row count (`limit` / `offset`): a constant of the query text, or a
/// variable.
///
/// A literal renders inline (`LIMIT 10`) exactly as it always has — it comes
/// from the query text, so it is as stable as the rest of the SQL. Only a
/// variable becomes a bind, which is what keeps `limit: $n` from forcing a
/// different SQL string per page size.
#[derive(Debug, Clone, PartialEq)]
pub enum Count {
    Lit(u64),
    /// A count known from the query text that renders as a bound parameter
    /// rather than inline.
    ///
    /// Only [`crate::limits::ExecutionLimits::bind_row_counts`] produces this.
    /// It exists because the two things one might want from a literal `limit`
    /// pull in opposite directions: rendered inline it keeps the statement
    /// readable and `EXPLAIN`-able, and rendered as a bind it keeps every page
    /// size on *one* statement — which is what a driver's prepared-statement
    /// cache, and Postgres' plan cache behind it, care about when the value
    /// comes from a client.
    Bound(u64),
    Var {
        name: String,
        /// Ceiling from [`crate::limits::ExecutionLimits`], checked when the
        /// variable resolves.
        ///
        /// A literal `limit` is capped where the limits are applied, but
        /// `limit: $n` has no value until the request arrives — and capping is
        /// pointless if `$n` can name any number. So the ceiling travels with
        /// the count, which is also what keeps a compiled statement carrying
        /// the cap it was compiled under.
        max: Option<u64>,
    },
}

impl Count {
    /// A variable count with no ceiling.
    pub fn var(name: impl Into<String>) -> Self {
        Count::Var {
            name: name.into(),
            max: None,
        }
    }

    /// Resolve a variable count to a non-negative integer.
    pub fn resolve(&self, inputs: &Inputs<'_>, path: &str) -> Result<u64> {
        match self {
            Count::Lit(n) | Count::Bound(n) => Ok(*n),
            Count::Var { name, max } => {
                let v = inputs.variable(name)?;
                let n = v.as_u64().ok_or_else(|| Error::Validate {
                    path: path.to_string(),
                    message: format!("expected a non-negative integer, got {v}"),
                })?;
                match max {
                    Some(max) if n > *max => Err(Error::Validate {
                        path: path.to_string(),
                        message: format!("{n} is over the limit of {max}"),
                    }),
                    _ => Ok(n),
                }
            }
        }
    }
}

impl From<u64> for Count {
    fn from(n: u64) -> Self {
        Count::Lit(n)
    }
}

#[derive(Debug, Clone)]
pub enum Operation {
    Query(Vec<RootField>),
    Mutation(Vec<MutationField>),
}

#[derive(Debug, Clone)]
pub struct RootField {
    pub table: String,
    pub alias: String,
    pub args: QueryArgs,
    pub body: RootBody,
}

#[derive(Debug, Clone)]
pub enum RootBody {
    List {
        selection: Vec<Field>,
    },
    Aggregate {
        ops: Vec<AggSelect>,
        nodes: Option<Vec<Field>>,
        /// Response keys asking for the `<table>_aggregate` type name — the
        /// level that holds `aggregate` and `nodes`, one above both of them.
        typenames: Vec<String>,
    },
    ByPk {
        /// `(exposed_column, value)` pairs. All PK columns must be present.
        pk: Vec<(String, Val)>,
        selection: Vec<Field>,
    },
    /// `__schema` / `__type`, answered from the schema rather than from
    /// Postgres and carried into the statement as a bound `json` parameter.
    ///
    /// Resolving at lowering time rather than around the SQL is what lets a
    /// document that mixes introspection with data stay one request, and what
    /// lets an introspection query be compiled like any other.
    Introspection(Value),
}

/// One entry under `aggregate { … }`: the response key it answers to, and the
/// function it computes.
///
/// The key is carried rather than derived from the function name because
/// `total: count` is a field alias like any other — deriving it would answer
/// under `count` and hand the caller a response shaped differently from the
/// document they sent.
#[derive(Debug, Clone)]
pub struct AggSelect {
    pub alias: String,
    pub op: AggOp,
}

/// A column inside `sum { … }` / `avg` / `max` / `min`, with the response key
/// it answers to. Same reason as [`AggSelect::alias`].
#[derive(Debug, Clone)]
pub struct AggCol {
    pub alias: String,
    pub column: String,
}

/// An entry inside a `sum` / `avg` / `max` / `min` group.
///
/// A client that injects `__typename` into every selection set — which is what
/// Apollo does — injects it here too, so the group cannot be a list of columns
/// alone.
#[derive(Debug, Clone)]
pub enum AggField {
    Column(AggCol),
    Typename { alias: String },
}

impl AggField {
    /// A column answering under its own name.
    pub fn column(column: impl Into<String>) -> Self {
        AggField::Column(AggCol::new(column))
    }
}

impl AggCol {
    /// A column answering under its own name.
    pub fn new(column: impl Into<String>) -> Self {
        let column = column.into();
        AggCol {
            alias: column.clone(),
            column,
        }
    }
}

#[derive(Debug, Clone)]
pub enum AggOp {
    /// `count`, `count(columns: [a, b])`, `count(distinct: true, columns: [a])`.
    ///
    /// Empty `columns` is `count(*)`. One column counts rows where it is not
    /// null. Several columns render as a row constructor, which is SQL NULL only
    /// when every field is — so without `distinct` that is `count(*)` again, and
    /// `distinct` is what makes naming several columns useful: distinct
    /// combinations.
    Count {
        columns: Vec<String>,
        distinct: bool,
    },
    Sum {
        fields: Vec<AggField>,
    },
    Avg {
        fields: Vec<AggField>,
    },
    Max {
        fields: Vec<AggField>,
    },
    Min {
        fields: Vec<AggField>,
    },
    /// `__typename` beside the aggregate functions; names the
    /// `<table>_aggregate_fields` type.
    Typename,
}

impl AggOp {
    /// `count(*)`.
    pub fn count() -> Self {
        AggOp::Count {
            columns: Vec::new(),
            distinct: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryArgs {
    pub where_: Option<BoolExpr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<Count>,
    pub offset: Option<Count>,
    pub distinct_on: Vec<String>,
}

impl QueryArgs {
    /// Whether the field was written with no arguments at all.
    ///
    /// Used to decide whether two relation fields under one response key can be
    /// merged: with no arguments on either side they ask the same question, and
    /// only their selections differ.
    pub fn is_empty(&self) -> bool {
        self.where_.is_none()
            && self.order_by.is_empty()
            && self.limit.is_none()
            && self.offset.is_none()
            && self.distinct_on.is_empty()
    }
}

/// One object-relation hop on the way to an `order_by` column.
#[derive(Debug, Clone)]
pub struct OrderByHop {
    /// Exposed name of the object relation to walk.
    pub relation: String,
    /// Scope predicate for this hop's target table, injected by `apply_scope`.
    /// `None` when the caller is unscoped, or the target is unrestricted.
    ///
    /// Ordering through a relation reads the target table just as selecting it
    /// does, so it must be filtered the same way — otherwise a scoped caller
    /// could sort by a column on rows it is not allowed to read, and recover
    /// those values from the resulting row order. The predicate lives per hop
    /// rather than per `OrderBy` so a multi-hop path cannot silently leave a
    /// middle table unfiltered.
    pub filter: Option<BoolExpr>,
}

impl OrderByHop {
    /// An unfiltered hop. `apply_scope` fills `filter` in for scoped callers.
    pub fn new(relation: impl Into<String>) -> Self {
        OrderByHop {
            relation: relation.into(),
            filter: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    /// Object-relation path to walk before reaching `column`. Empty for a column
    /// on the table being ordered.
    ///
    /// `order_by: {sample: {collected_at: asc}}` on `experiments` lowers to
    /// `path = [hop("sample")], column = "collected_at"`.
    pub path: Vec<OrderByHop>,
    pub column: String,
    pub direction: OrderDir,
    /// `None` = PostgreSQL's default placement for `direction`.
    pub nulls: Option<NullsOrder>,
}

impl OrderBy {
    /// Order by a column on the table itself.
    pub fn column(column: impl Into<String>, direction: OrderDir) -> Self {
        OrderBy {
            path: Vec::new(),
            column: column.into(),
            direction,
            nulls: None,
        }
    }

    /// Pin where NULLs land instead of taking PostgreSQL's default.
    pub fn nulls(mut self, nulls: NullsOrder) -> Self {
        self.nulls = Some(nulls);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDir {
    Asc,
    Desc,
}

/// Where NULLs sort. `None` on an [`OrderBy`] leaves PostgreSQL's default, which
/// is not symmetric: `ASC` puts NULLs last, `DESC` puts them first. So
/// `desc_nulls_last` is a real thing you have to ask for — plain `desc` will not
/// give it to you.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Field {
    Column {
        physical: String,
        alias: String,
    },
    /// Scalar read of a JSON/JSONB column through a `#>` path extraction, e.g.
    /// `abundance: data(path: "a.b")` → `data #> '{a,b}' AS "abundance"`.
    /// The result keeps its JSON/JSONB type (structure preserved), and numeric
    /// path components index into JSON arrays per PostgreSQL `#>` semantics.
    JsonPath {
        physical: String,
        alias: String,
        /// Non-empty list of key/index components. Rendered as a `text[]` bind.
        path: Vec<String>,
    },
    /// `__typename`. Renders as a string literal: the type name comes from the
    /// table the selection set belongs to, which only the renderer knows, so the
    /// IR carries the request and not the answer.
    Typename {
        alias: String,
    },
    Relation {
        /// Name of the relation on the parent table (resolved via schema at render).
        name: String,
        alias: String,
        args: QueryArgs,
        selection: Vec<Field>,
    },
}

#[derive(Debug, Clone)]
pub enum BoolExpr {
    And(Vec<BoolExpr>),
    Or(Vec<BoolExpr>),
    Not(Box<BoolExpr>),
    Compare {
        column: String,
        op: CmpOp,
        value: Val,
    },
    IsNull {
        column: String,
        negated: bool,
    },
    /// `column = ANY($n)` over a single bound array (`<> ALL($n)` when
    /// negated). NULL elements keep SQL `IN` semantics: they never match.
    ///
    /// `values` must resolve to a JSON array; it is a single [`Val`] rather
    /// than a `Vec` so the whole list can be one variable (`_in: $ids`).
    InList {
        column: String,
        values: Val,
        negated: bool,
    },
    /// Match rows where the named relation has at least one matching row.
    Relation {
        name: String,
        inner: Box<BoolExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    ILike,
    NLike,
    NILike,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum MutationField {
    Insert {
        alias: String,
        table: String,
        /// Each element is one parent row with its optional nested children.
        objects: Vec<InsertObject>,
        on_conflict: Option<OnConflict>,
        returning: Vec<Field>,
        /// Response keys asking for the mutation-response type name. Not part
        /// of `returning`, which selects from the row type.
        response_typenames: Vec<String>,
        /// true for `insert_users_one` (single object result); false for `insert_users`
        /// (array result wrapped in `{affected_rows, returning}`).
        one: bool,
        /// Post-insert scope check under deny-by-default scoped execution:
        /// every inserted row must satisfy this predicate or the whole
        /// statement aborts. `None` for unscoped runs and unrestricted tables.
        scope_check: Option<BoolExpr>,
    },
    Update {
        alias: String,
        table: String,
        where_: BoolExpr,
        /// `{ exposed_column -> new_value }`
        set: std::collections::BTreeMap<String, Val>,
        returning: Vec<Field>,
        /// See [`MutationField::Insert::response_typenames`].
        response_typenames: Vec<String>,
        /// Post-update scope check under deny-by-default scoped execution: every
        /// row left by the UPDATE must still satisfy this predicate or the whole
        /// statement aborts. The same predicate is also AND-ed into `where_` as a
        /// pre-image filter, so together a scoped caller may only modify rows
        /// already in scope and may not move a row out of scope. `None` for
        /// unscoped runs and unrestricted tables.
        scope_check: Option<BoolExpr>,
    },
    UpdateByPk {
        alias: String,
        table: String,
        pk: Vec<(String, Val)>,
        set: std::collections::BTreeMap<String, Val>,
        selection: Vec<Field>,
        /// Scope predicate under deny-by-default scoped execution, used twice:
        /// AND-ed onto the PK match as a pre-image filter (a row failing it does
        /// not match, so the mutation returns null), and re-checked as a
        /// post-update guard so an in-scope row cannot be moved out of scope by
        /// the update (a violation aborts the statement). `None` for unscoped
        /// runs and unrestricted tables.
        scope: Option<BoolExpr>,
    },
    Delete {
        alias: String,
        table: String,
        where_: BoolExpr,
        returning: Vec<Field>,
        /// See [`MutationField::Insert::response_typenames`].
        response_typenames: Vec<String>,
    },
    DeleteByPk {
        alias: String,
        table: String,
        pk: Vec<(String, Val)>,
        selection: Vec<Field>,
        /// Scope predicate AND-ed onto the PK match under deny-by-default
        /// scoped execution. `None` for unscoped runs and unrestricted tables.
        /// A row failing it simply does not match, so the mutation returns null.
        scope: Option<BoolExpr>,
    },
}

impl MutationField {
    /// The response key this mutation's result is nested under.
    pub fn alias(&self) -> &str {
        match self {
            MutationField::Insert { alias, .. }
            | MutationField::Update { alias, .. }
            | MutationField::UpdateByPk { alias, .. }
            | MutationField::Delete { alias, .. }
            | MutationField::DeleteByPk { alias, .. } => alias,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OnConflict {
    pub constraint: String,
    pub update_columns: Vec<String>,
    pub where_: Option<BoolExpr>,
}

/// One row being inserted. Carries regular column values, any nested
/// array-relation inserts (children), and any nested object-relation inserts
/// (a single related entity per parent row).
#[derive(Debug, Clone, Default)]
pub struct InsertObject {
    /// `{ exposed_column -> value }` for this parent row.
    pub columns: std::collections::BTreeMap<String, Val>,
    /// Array-relation (one-to-many) nested inserts, keyed by the parent-side
    /// relation name. Each value carries the rows to insert as children of
    /// *this* parent row.
    pub nested_arrays: std::collections::BTreeMap<String, NestedArrayInsert>,
    /// Object-relation (many-to-one) nested inserts, keyed by the parent-side
    /// relation name. Each value carries the single row whose PK becomes the
    /// parent row's FK. The engine inserts this row FIRST, before the parent.
    pub nested_objects: std::collections::BTreeMap<String, NestedObjectInsert>,
}

/// A nested `posts: { data: [...] }` block attached to one parent row.
#[derive(Debug, Clone)]
pub struct NestedArrayInsert {
    /// Target table name (resolved from the parent relation's `target_table`).
    pub table: String,
    /// Rows to insert as children. Each element is itself an `InsertObject`,
    /// so this recurses arbitrarily deep.
    pub rows: Vec<InsertObject>,
    /// Optional Hasura-style on_conflict applied when emitting this
    /// nested INSERT. When present with `update_columns: []`, the renderer
    /// transparently rewrites `DO NOTHING` → `DO UPDATE SET pk = table.pk`
    /// (a true no-op referencing the existing row's value, NOT EXCLUDED.pk
    /// which would change it to the proposed sequence id) to keep
    /// RETURNING correlated 1:1 with input ords.
    pub on_conflict: Option<OnConflict>,
    /// Post-insert scope check for this nested target table under deny-by-
    /// default scoped execution: every child row inserted here must satisfy
    /// it or the whole statement aborts. `None` for unscoped runs and
    /// unrestricted tables.
    pub scope_check: Option<BoolExpr>,
}

/// A nested `user: { data: {...} }` block attached to one parent row.
/// Exactly one row — object relations reference exactly one entity.
#[derive(Debug, Clone)]
pub struct NestedObjectInsert {
    /// Target table name (resolved from the parent relation's `target_table`).
    pub table: String,
    /// The row to insert. The engine inserts this BEFORE the parent row
    /// and uses its PK as the parent's FK.
    pub row: InsertObject,
    /// Optional Hasura-style on_conflict. Same rewrite semantics as
    /// NestedArrayInsert.
    pub on_conflict: Option<OnConflict>,
    /// Post-insert scope check for this nested target table. See
    /// [`NestedArrayInsert::scope_check`].
    pub scope_check: Option<BoolExpr>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_simple_root_field() {
        let root = RootField {
            table: "users".into(),
            alias: "users".into(),
            args: QueryArgs::default(),
            body: RootBody::List {
                selection: vec![
                    Field::Column {
                        physical: "id".into(),
                        alias: "id".into(),
                    },
                    Field::Column {
                        physical: "name".into(),
                        alias: "name".into(),
                    },
                ],
            },
        };
        assert_eq!(root.table, "users");
        match root.body {
            RootBody::List { selection } => assert_eq!(selection.len(), 2),
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn build_where_eq_expression() {
        let expr = BoolExpr::Compare {
            column: "id".into(),
            op: CmpOp::Eq,
            value: json!(42).into(),
        };
        match expr {
            BoolExpr::Compare { op: CmpOp::Eq, .. } => {}
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn build_field_relation() {
        let f = Field::Relation {
            name: "posts".into(),
            alias: "posts".into(),
            args: QueryArgs::default(),
            selection: vec![Field::Column {
                physical: "title".into(),
                alias: "title".into(),
            }],
        };
        match f {
            Field::Relation {
                name, selection, ..
            } => {
                assert_eq!(name, "posts");
                assert_eq!(selection.len(), 1);
            }
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn build_aggregate_root() {
        let body = RootBody::Aggregate {
            typenames: Vec::new(),
            ops: vec![
                AggSelect {
                    alias: "count".into(),
                    op: AggOp::count(),
                },
                AggSelect {
                    alias: "sum".into(),
                    op: AggOp::Sum {
                        fields: vec![AggField::column("age")],
                    },
                },
            ],
            nodes: Some(vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }]),
        };
        match body {
            RootBody::Aggregate { ops, nodes, .. } => {
                assert_eq!(ops.len(), 2);
                assert!(nodes.is_some());
            }
            _ => panic!("expected Aggregate"),
        }
    }

    #[test]
    fn build_insert_mutation() {
        use std::collections::BTreeMap;
        let mut columns = BTreeMap::new();
        columns.insert("name".to_string(), serde_json::json!("alice").into());
        let m = MutationField::Insert {
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested_arrays: BTreeMap::new(),
                nested_objects: BTreeMap::new(),
            }],
            on_conflict: None,
            returning: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
            response_typenames: Vec::new(),
            one: false,
            scope_check: None,
        };
        match m {
            MutationField::Insert { objects, .. } => assert_eq!(objects.len(), 1),
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn build_bool_expr_relation() {
        let e = BoolExpr::Relation {
            name: "posts".into(),
            inner: Box::new(BoolExpr::Compare {
                column: "published".into(),
                op: CmpOp::Eq,
                value: json!(true).into(),
            }),
        };
        match e {
            BoolExpr::Relation { name, .. } => assert_eq!(name, "posts"),
            _ => panic!("expected Relation"),
        }
    }
}
