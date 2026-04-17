//! Ergonomic builder API for constructing queries and mutations.
//!
//! Both the GraphQL parser and these builders produce the same IR,
//! so the engine runs them through the same pipeline.

use crate::ast::{
    AggOp, BoolExpr, CmpOp, Field, MutationField, OnConflict, Operation, OrderBy, OrderDir,
    QueryArgs, RootBody, RootField,
};
use serde_json::Value;
use std::collections::BTreeMap;

/// Anything that can be turned into an [`Operation`] for `Engine::run`.
pub trait IntoOperation {
    fn into_operation(self) -> Operation;
}

impl IntoOperation for Operation {
    fn into_operation(self) -> Operation {
        self
    }
}

impl IntoOperation for RootField {
    fn into_operation(self) -> Operation {
        Operation::Query(vec![self])
    }
}

// ===== Query =====

pub struct Query;

impl Query {
    pub fn from(table: impl Into<String>) -> QueryBuilder {
        let t: String = table.into();
        QueryBuilder {
            table: t.clone(),
            alias: t,
            args: QueryArgs::default(),
            selection: Vec::new(),
        }
    }

    pub fn aggregate(table: impl Into<String>) -> AggregateBuilder {
        let t: String = table.into();
        AggregateBuilder {
            table: t.clone(),
            alias: format!("{t}_aggregate"),
            args: QueryArgs::default(),
            ops: Vec::new(),
            nodes: None,
        }
    }

    pub fn by_pk(
        table: impl Into<String>,
        pk: &[(impl Into<String> + Clone, Value)],
    ) -> ByPkBuilder {
        let t: String = table.into();
        ByPkBuilder {
            table: t.clone(),
            alias: format!("{t}_by_pk"),
            pk: pk
                .iter()
                .map(|(k, v)| (k.clone().into(), v.clone()))
                .collect(),
            selection: Vec::new(),
        }
    }
}

pub struct QueryBuilder {
    table: String,
    alias: String,
    args: QueryArgs,
    selection: Vec<Field>,
}

impl QueryBuilder {
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = alias.into();
        self
    }

    pub fn select(mut self, cols: &[&str]) -> Self {
        self.selection = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn column(mut self, col: impl Into<String>) -> Self {
        let c: String = col.into();
        self.selection.push(Field::Column {
            physical: c.clone(),
            alias: c,
        });
        self
    }

    pub fn where_expr(mut self, expr: BoolExpr) -> Self {
        self.args.where_ = Some(expr);
        self
    }

    pub fn where_eq(self, col: impl Into<String>, value: impl Into<Value>) -> Self {
        self.where_cmp(col, CmpOp::Eq, value.into())
    }

    pub fn where_cmp(mut self, col: impl Into<String>, op: CmpOp, value: Value) -> Self {
        let cmp = BoolExpr::Compare {
            column: col.into(),
            op,
            value,
        };
        self.args.where_ = Some(merge_and(self.args.where_.take(), cmp));
        self
    }

    pub fn order_by(mut self, col: impl Into<String>, direction: OrderDir) -> Self {
        self.args.order_by.push(OrderBy {
            column: col.into(),
            direction,
        });
        self
    }

    pub fn limit(mut self, n: u64) -> Self {
        self.args.limit = Some(n);
        self
    }

    pub fn offset(mut self, n: u64) -> Self {
        self.args.offset = Some(n);
        self
    }

    pub fn distinct_on(mut self, cols: &[&str]) -> Self {
        self.args.distinct_on = cols.iter().map(|s| (*s).to_string()).collect();
        self
    }

    pub fn with_relation(mut self, name: impl Into<String>, nested: QueryBuilder) -> Self {
        let n: String = name.into();
        self.selection.push(Field::Relation {
            name: n.clone(),
            alias: n,
            args: nested.args,
            selection: nested.selection,
        });
        self
    }

    pub fn build(self) -> RootField {
        RootField {
            table: self.table,
            alias: self.alias,
            args: self.args,
            body: RootBody::List {
                selection: self.selection,
            },
        }
    }
}

impl IntoOperation for QueryBuilder {
    fn into_operation(self) -> Operation {
        Operation::Query(vec![self.build()])
    }
}

// ===== Aggregate =====

pub struct AggregateBuilder {
    table: String,
    alias: String,
    args: QueryArgs,
    ops: Vec<AggOp>,
    nodes: Option<Vec<Field>>,
}

impl AggregateBuilder {
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = alias.into();
        self
    }

    pub fn where_expr(mut self, expr: BoolExpr) -> Self {
        self.args.where_ = Some(expr);
        self
    }

    pub fn where_eq(mut self, col: impl Into<String>, value: impl Into<Value>) -> Self {
        let cmp = BoolExpr::Compare {
            column: col.into(),
            op: CmpOp::Eq,
            value: value.into(),
        };
        self.args.where_ = Some(merge_and(self.args.where_.take(), cmp));
        self
    }

    pub fn count(mut self) -> Self {
        self.ops.push(AggOp::Count);
        self
    }

    pub fn sum(mut self, cols: &[&str]) -> Self {
        self.ops.push(AggOp::Sum {
            columns: cols.iter().map(|s| (*s).to_string()).collect(),
        });
        self
    }

    pub fn avg(mut self, cols: &[&str]) -> Self {
        self.ops.push(AggOp::Avg {
            columns: cols.iter().map(|s| (*s).to_string()).collect(),
        });
        self
    }

    pub fn max(mut self, cols: &[&str]) -> Self {
        self.ops.push(AggOp::Max {
            columns: cols.iter().map(|s| (*s).to_string()).collect(),
        });
        self
    }

    pub fn min(mut self, cols: &[&str]) -> Self {
        self.ops.push(AggOp::Min {
            columns: cols.iter().map(|s| (*s).to_string()).collect(),
        });
        self
    }

    pub fn nodes(mut self, cols: &[&str]) -> Self {
        self.nodes = Some(
            cols.iter()
                .map(|c| Field::Column {
                    physical: (*c).to_string(),
                    alias: (*c).to_string(),
                })
                .collect(),
        );
        self
    }

    pub fn build(self) -> RootField {
        RootField {
            table: self.table,
            alias: self.alias,
            args: self.args,
            body: RootBody::Aggregate {
                ops: self.ops,
                nodes: self.nodes,
            },
        }
    }
}

impl IntoOperation for AggregateBuilder {
    fn into_operation(self) -> Operation {
        Operation::Query(vec![self.build()])
    }
}

// ===== ByPk =====

pub struct ByPkBuilder {
    table: String,
    alias: String,
    pk: Vec<(String, Value)>,
    selection: Vec<Field>,
}

impl ByPkBuilder {
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = alias.into();
        self
    }

    pub fn select(mut self, cols: &[&str]) -> Self {
        self.selection = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn with_relation(mut self, name: impl Into<String>, nested: QueryBuilder) -> Self {
        let n: String = name.into();
        self.selection.push(Field::Relation {
            name: n.clone(),
            alias: n,
            args: nested.args,
            selection: nested.selection,
        });
        self
    }

    pub fn build(self) -> RootField {
        RootField {
            table: self.table,
            alias: self.alias,
            args: QueryArgs::default(),
            body: RootBody::ByPk {
                pk: self.pk,
                selection: self.selection,
            },
        }
    }
}

impl IntoOperation for ByPkBuilder {
    fn into_operation(self) -> Operation {
        Operation::Query(vec![self.build()])
    }
}

// ===== Mutations =====

pub struct Mutation;

impl Mutation {
    pub fn insert(
        table: impl Into<String>,
        objects: Vec<BTreeMap<String, Value>>,
    ) -> InsertBuilder {
        let t: String = table.into();
        InsertBuilder {
            alias: format!("insert_{t}"),
            table: t,
            objects,
            on_conflict: None,
            returning: Vec::new(),
            one: false,
        }
    }

    pub fn insert_one<I, K>(table: impl Into<String>, obj: I) -> InsertBuilder
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let t: String = table.into();
        let map: BTreeMap<String, Value> = obj.into_iter().map(|(k, v)| (k.into(), v)).collect();
        InsertBuilder {
            alias: format!("insert_{t}_one"),
            table: t,
            objects: vec![map],
            on_conflict: None,
            returning: Vec::new(),
            one: true,
        }
    }

    pub fn update(table: impl Into<String>) -> UpdateBuilder {
        let t: String = table.into();
        UpdateBuilder {
            alias: format!("update_{t}"),
            table: t,
            where_: None,
            set: BTreeMap::new(),
            returning: Vec::new(),
        }
    }

    pub fn update_by_pk(
        table: impl Into<String>,
        pk: &[(impl Into<String> + Clone, Value)],
    ) -> UpdateByPkBuilder {
        let t: String = table.into();
        UpdateByPkBuilder {
            alias: format!("update_{t}_by_pk"),
            table: t,
            pk: pk
                .iter()
                .map(|(k, v)| (k.clone().into(), v.clone()))
                .collect(),
            set: BTreeMap::new(),
            selection: Vec::new(),
        }
    }

    pub fn delete(table: impl Into<String>) -> DeleteBuilder {
        let t: String = table.into();
        DeleteBuilder {
            alias: format!("delete_{t}"),
            table: t,
            where_: None,
            returning: Vec::new(),
        }
    }

    pub fn delete_by_pk(
        table: impl Into<String>,
        pk: &[(impl Into<String> + Clone, Value)],
    ) -> DeleteByPkBuilder {
        let t: String = table.into();
        DeleteByPkBuilder {
            alias: format!("delete_{t}_by_pk"),
            table: t,
            pk: pk
                .iter()
                .map(|(k, v)| (k.clone().into(), v.clone()))
                .collect(),
            selection: Vec::new(),
        }
    }
}

pub struct InsertBuilder {
    alias: String,
    table: String,
    objects: Vec<BTreeMap<String, Value>>,
    on_conflict: Option<OnConflict>,
    returning: Vec<Field>,
    one: bool,
}

impl InsertBuilder {
    pub fn alias(mut self, a: impl Into<String>) -> Self {
        self.alias = a.into();
        self
    }

    pub fn on_conflict(mut self, oc: OnConflict) -> Self {
        self.on_conflict = Some(oc);
        self
    }

    pub fn returning(mut self, cols: &[&str]) -> Self {
        self.returning = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn build(self) -> MutationField {
        MutationField::Insert {
            alias: self.alias,
            table: self.table,
            objects: self.objects,
            on_conflict: self.on_conflict,
            returning: self.returning,
            one: self.one,
        }
    }
}

impl IntoOperation for InsertBuilder {
    fn into_operation(self) -> Operation {
        Operation::Mutation(vec![self.build()])
    }
}

pub struct UpdateBuilder {
    alias: String,
    table: String,
    where_: Option<BoolExpr>,
    set: BTreeMap<String, Value>,
    returning: Vec<Field>,
}

impl UpdateBuilder {
    pub fn alias(mut self, a: impl Into<String>) -> Self {
        self.alias = a.into();
        self
    }

    pub fn where_expr(mut self, expr: BoolExpr) -> Self {
        self.where_ = Some(expr);
        self
    }

    pub fn where_eq(mut self, col: impl Into<String>, value: impl Into<Value>) -> Self {
        let cmp = BoolExpr::Compare {
            column: col.into(),
            op: CmpOp::Eq,
            value: value.into(),
        };
        self.where_ = Some(merge_and(self.where_.take(), cmp));
        self
    }

    pub fn set(mut self, col: impl Into<String>, value: Value) -> Self {
        self.set.insert(col.into(), value);
        self
    }

    pub fn returning(mut self, cols: &[&str]) -> Self {
        self.returning = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn build(self) -> MutationField {
        MutationField::Update {
            alias: self.alias,
            table: self.table,
            where_: self.where_.expect("update builder: where clause required"),
            set: self.set,
            returning: self.returning,
        }
    }
}

impl IntoOperation for UpdateBuilder {
    fn into_operation(self) -> Operation {
        Operation::Mutation(vec![self.build()])
    }
}

pub struct UpdateByPkBuilder {
    alias: String,
    table: String,
    pk: Vec<(String, Value)>,
    set: BTreeMap<String, Value>,
    selection: Vec<Field>,
}

impl UpdateByPkBuilder {
    pub fn set(mut self, col: impl Into<String>, value: Value) -> Self {
        self.set.insert(col.into(), value);
        self
    }

    pub fn select(mut self, cols: &[&str]) -> Self {
        self.selection = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn build(self) -> MutationField {
        MutationField::UpdateByPk {
            alias: self.alias,
            table: self.table,
            pk: self.pk,
            set: self.set,
            selection: self.selection,
        }
    }
}

impl IntoOperation for UpdateByPkBuilder {
    fn into_operation(self) -> Operation {
        Operation::Mutation(vec![self.build()])
    }
}

pub struct DeleteBuilder {
    alias: String,
    table: String,
    where_: Option<BoolExpr>,
    returning: Vec<Field>,
}

impl DeleteBuilder {
    pub fn where_expr(mut self, expr: BoolExpr) -> Self {
        self.where_ = Some(expr);
        self
    }

    pub fn where_eq(mut self, col: impl Into<String>, value: impl Into<Value>) -> Self {
        let cmp = BoolExpr::Compare {
            column: col.into(),
            op: CmpOp::Eq,
            value: value.into(),
        };
        self.where_ = Some(merge_and(self.where_.take(), cmp));
        self
    }

    pub fn returning(mut self, cols: &[&str]) -> Self {
        self.returning = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn build(self) -> MutationField {
        MutationField::Delete {
            alias: self.alias,
            table: self.table,
            where_: self.where_.expect("delete builder: where clause required"),
            returning: self.returning,
        }
    }
}

impl IntoOperation for DeleteBuilder {
    fn into_operation(self) -> Operation {
        Operation::Mutation(vec![self.build()])
    }
}

pub struct DeleteByPkBuilder {
    alias: String,
    table: String,
    pk: Vec<(String, Value)>,
    selection: Vec<Field>,
}

impl DeleteByPkBuilder {
    pub fn select(mut self, cols: &[&str]) -> Self {
        self.selection = cols
            .iter()
            .map(|c| Field::Column {
                physical: (*c).to_string(),
                alias: (*c).to_string(),
            })
            .collect();
        self
    }

    pub fn build(self) -> MutationField {
        MutationField::DeleteByPk {
            alias: self.alias,
            table: self.table,
            pk: self.pk,
            selection: self.selection,
        }
    }
}

impl IntoOperation for DeleteByPkBuilder {
    fn into_operation(self) -> Operation {
        Operation::Mutation(vec![self.build()])
    }
}

// ===== Helpers =====

fn merge_and(existing: Option<BoolExpr>, new_term: BoolExpr) -> BoolExpr {
    match existing {
        None => new_term,
        Some(BoolExpr::And(mut parts)) => {
            parts.push(new_term);
            BoolExpr::And(parts)
        }
        Some(other) => BoolExpr::And(vec![other, new_term]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn query_builder_basic_shape() {
        let rf = Query::from("users")
            .select(&["id", "name"])
            .where_eq("active", true)
            .limit(10)
            .build();
        assert_eq!(rf.table, "users");
        assert_eq!(rf.alias, "users");
        assert_eq!(rf.args.limit, Some(10));
        let RootBody::List { selection } = &rf.body else {
            panic!("expected List");
        };
        assert_eq!(selection.len(), 2);
    }

    #[test]
    fn query_builder_nested_relation() {
        let rf = Query::from("users")
            .select(&["id"])
            .with_relation("posts", Query::from("posts").select(&["title"]).limit(5))
            .build();
        let RootBody::List { selection } = &rf.body else {
            panic!("expected List");
        };
        assert_eq!(selection.len(), 2);
        match &selection[1] {
            Field::Relation {
                name,
                args,
                selection,
                ..
            } => {
                assert_eq!(name, "posts");
                assert_eq!(args.limit, Some(5));
                assert_eq!(selection.len(), 1);
            }
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn aggregate_builder_count_sum() {
        let rf = Query::aggregate("users").count().sum(&["age"]).build();
        assert_eq!(rf.alias, "users_aggregate");
        let RootBody::Aggregate { ops, nodes } = &rf.body else {
            panic!("expected Aggregate");
        };
        assert_eq!(ops.len(), 2);
        assert!(nodes.is_none());
    }

    #[test]
    fn by_pk_builder() {
        let rf = Query::by_pk("users", &[("id", json!(7))])
            .select(&["name"])
            .build();
        let RootBody::ByPk { pk, selection } = &rf.body else {
            panic!("expected ByPk");
        };
        assert_eq!(pk[0].1, json!(7));
        assert_eq!(selection.len(), 1);
    }

    #[test]
    fn mutation_insert_one() {
        let op = Mutation::insert_one("users", [("name", json!("alice"))])
            .returning(&["id"])
            .into_operation();
        match op {
            Operation::Mutation(fields) => match &fields[0] {
                MutationField::Insert { objects, one, .. } => {
                    assert_eq!(objects.len(), 1);
                    assert!(one);
                }
                _ => panic!("expected Insert"),
            },
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn mutation_update_by_where() {
        let op = Mutation::update("users")
            .where_eq("id", 1)
            .set("name", json!("z"))
            .returning(&["name"])
            .into_operation();
        match op {
            Operation::Mutation(fields) => match &fields[0] {
                MutationField::Update { set, returning, .. } => {
                    assert!(set.contains_key("name"));
                    assert_eq!(returning.len(), 1);
                }
                _ => panic!("expected Update"),
            },
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn mutation_delete_by_pk() {
        let op = Mutation::delete_by_pk("users", &[("id", json!(5))])
            .select(&["id"])
            .into_operation();
        match op {
            Operation::Mutation(fields) => match &fields[0] {
                MutationField::DeleteByPk { pk, selection, .. } => {
                    assert_eq!(pk.len(), 1);
                    assert_eq!(selection.len(), 1);
                }
                _ => panic!("expected DeleteByPk"),
            },
            _ => panic!("expected Mutation"),
        }
    }
}
