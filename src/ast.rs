//! Intermediate representation for queries.

use serde_json::Value;

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
        ops: Vec<AggOp>,
        nodes: Option<Vec<Field>>,
    },
    ByPk {
        /// `(exposed_column, value)` pairs. All PK columns must be present.
        pk: Vec<(String, serde_json::Value)>,
        selection: Vec<Field>,
    },
}

#[derive(Debug, Clone)]
pub enum AggOp {
    Count,
    Sum { columns: Vec<String> },
    Avg { columns: Vec<String> },
    Max { columns: Vec<String> },
    Min { columns: Vec<String> },
}

#[derive(Debug, Clone, Default)]
pub struct QueryArgs {
    pub where_: Option<BoolExpr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub distinct_on: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub direction: OrderDir,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDir {
    Asc,
    Desc,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Field {
    Column {
        physical: String,
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
        value: Value,
    },
    IsNull {
        column: String,
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
        /// true for `insert_users_one` (single object result); false for `insert_users`
        /// (array result wrapped in `{affected_rows, returning}`).
        one: bool,
    },
    Update {
        alias: String,
        table: String,
        where_: BoolExpr,
        /// `{ exposed_column -> new_value }`
        set: std::collections::BTreeMap<String, serde_json::Value>,
        returning: Vec<Field>,
    },
    UpdateByPk {
        alias: String,
        table: String,
        pk: Vec<(String, serde_json::Value)>,
        set: std::collections::BTreeMap<String, serde_json::Value>,
        selection: Vec<Field>,
    },
    Delete {
        alias: String,
        table: String,
        where_: BoolExpr,
        returning: Vec<Field>,
    },
    DeleteByPk {
        alias: String,
        table: String,
        pk: Vec<(String, serde_json::Value)>,
        selection: Vec<Field>,
    },
}

#[derive(Debug, Clone)]
pub struct OnConflict {
    pub constraint: String,
    pub update_columns: Vec<String>,
    pub where_: Option<BoolExpr>,
}

/// One row being inserted. Carries regular column values plus any
/// nested array-relation inserts that should happen as children.
#[derive(Debug, Clone, Default)]
pub struct InsertObject {
    /// `{ exposed_column -> value }` for this parent row.
    pub columns: std::collections::BTreeMap<String, serde_json::Value>,
    /// Nested array-relation inserts, keyed by the parent-side relation name.
    /// Each value carries the rows to insert as children of *this* parent row.
    pub nested: std::collections::BTreeMap<String, NestedArrayInsert>,
}

/// A nested `posts: { data: [...] }` block attached to one parent row.
#[derive(Debug, Clone)]
pub struct NestedArrayInsert {
    /// Target table name (resolved from the parent relation's `target_table`).
    pub table: String,
    /// Rows to insert as children. Each element is itself an `InsertObject`,
    /// so this recurses arbitrarily deep.
    pub rows: Vec<InsertObject>,
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
            value: json!(42),
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
            ops: vec![
                AggOp::Count,
                AggOp::Sum {
                    columns: vec!["age".into()],
                },
            ],
            nodes: Some(vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }]),
        };
        match body {
            RootBody::Aggregate { ops, nodes } => {
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
        columns.insert("name".to_string(), serde_json::json!("alice"));
        let m = MutationField::Insert {
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![InsertObject {
                columns,
                nested: BTreeMap::new(),
            }],
            on_conflict: None,
            returning: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
            one: false,
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
                value: json!(true),
            }),
        };
        match e {
            BoolExpr::Relation { name, .. } => assert_eq!(name, "posts"),
            _ => panic!("expected Relation"),
        }
    }
}
