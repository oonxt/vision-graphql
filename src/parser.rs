//! GraphQL string → IR.

use crate::ast::{
    BoolExpr, CmpOp, Field, Operation, OrderBy, OrderDir, QueryArgs, RootField,
};
use crate::error::{Error, Result};
use crate::schema::{Schema, Table};
use async_graphql_parser::parse_query;
use async_graphql_parser::types::{
    DocumentOperations, ExecutableDocument, OperationType, Selection, SelectionSet,
};
use async_graphql_parser::Positioned;
use async_graphql_value::{Name, Value as GqlValue};
use serde_json::Value;

pub fn parse_and_lower(
    source: &str,
    variables: &Value,
    operation_name: Option<&str>,
    schema: &Schema,
) -> Result<Operation> {
    let doc = parse_query(source).map_err(|e| Error::Parse(e.to_string()))?;
    let op = pick_operation(&doc, operation_name)?;
    match op.ty {
        OperationType::Query => lower_query(op.selection_set, schema, variables),
        OperationType::Mutation => Err(Error::Parse(
            "mutations are not supported in Phase 1".into(),
        )),
        OperationType::Subscription => Err(Error::Parse("subscriptions are not supported".into())),
    }
}

struct OpInfo<'a> {
    ty: OperationType,
    selection_set: &'a SelectionSet,
}

fn pick_operation<'a>(doc: &'a ExecutableDocument, name: Option<&str>) -> Result<OpInfo<'a>> {
    match (&doc.operations, name) {
        (DocumentOperations::Single(op), _) => Ok(OpInfo {
            ty: op.node.ty,
            selection_set: &op.node.selection_set.node,
        }),
        (DocumentOperations::Multiple(ops), Some(n)) => {
            let key = Name::new(n);
            let op = ops
                .get(&key)
                .ok_or_else(|| Error::Parse(format!("operation '{n}' not found")))?;
            Ok(OpInfo {
                ty: op.node.ty,
                selection_set: &op.node.selection_set.node,
            })
        }
        (DocumentOperations::Multiple(ops), None) => {
            if ops.len() == 1 {
                let (_, op) = ops.iter().next().unwrap();
                Ok(OpInfo {
                    ty: op.node.ty,
                    selection_set: &op.node.selection_set.node,
                })
            } else {
                Err(Error::Parse(
                    "document has multiple operations; operation_name required".into(),
                ))
            }
        }
    }
}

fn lower_query(set: &SelectionSet, schema: &Schema, vars: &Value) -> Result<Operation> {
    let mut roots = Vec::new();
    for sel in &set.items {
        match &sel.node {
            Selection::Field(f) => {
                let field = &f.node;
                let name = field.name.node.as_str();
                let alias = field
                    .alias
                    .as_ref()
                    .map(|a| a.node.as_str().to_string())
                    .unwrap_or_else(|| name.to_string());
                let table = schema.table(name).ok_or_else(|| Error::Validate {
                    path: alias.clone(),
                    message: format!("unknown root field '{name}'"),
                })?;
                let args = lower_args(&field.arguments, table, schema, vars, &alias)?;
                let selection =
                    lower_selection_set(&field.selection_set.node, table, schema, vars, &alias)?;

                roots.push(RootField {
                    table: name.to_string(),
                    alias,
                    args,
                    body: crate::ast::RootBody::List { selection },
                });
            }
            _ => {
                return Err(Error::Parse(
                    "fragments are not supported in Phase 1".into(),
                ))
            }
        }
    }
    Ok(Operation::Query(roots))
}

fn lower_selection_set(
    set: &SelectionSet,
    table: &Table,
    schema: &Schema,
    vars: &Value,
    parent_path: &str,
) -> Result<Vec<Field>> {
    let mut out = Vec::new();
    for sel in &set.items {
        match &sel.node {
            Selection::Field(f) => {
                let field = &f.node;
                let name = field.name.node.as_str();
                let alias = field
                    .alias
                    .as_ref()
                    .map(|a| a.node.as_str().to_string())
                    .unwrap_or_else(|| name.to_string());

                if let Some(rel) = table.find_relation(name) {
                    let target =
                        schema
                            .table(&rel.target_table)
                            .ok_or_else(|| Error::Validate {
                                path: format!("{parent_path}.{alias}"),
                                message: format!(
                                    "relation target table '{}' missing",
                                    rel.target_table
                                ),
                            })?;
                    let args = lower_args(
                        &field.arguments,
                        target,
                        schema,
                        vars,
                        &format!("{parent_path}.{alias}"),
                    )?;
                    let selection = lower_selection_set(
                        &field.selection_set.node,
                        target,
                        schema,
                        vars,
                        &format!("{parent_path}.{alias}"),
                    )?;
                    out.push(Field::Relation {
                        name: name.to_string(),
                        alias,
                        args,
                        selection,
                    });
                    continue;
                }

                let col = table.find_column(name).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.{alias}"),
                    message: format!("unknown column '{name}' on '{}'", table.exposed_name),
                })?;
                out.push(Field::Column {
                    physical: col.physical_name.clone(),
                    alias,
                });
            }
            _ => {
                return Err(Error::Parse(
                    "fragments are not supported in Phase 2".into(),
                ))
            }
        }
    }
    Ok(out)
}

fn lower_args(
    args: &[(Positioned<Name>, Positioned<GqlValue>)],
    table: &Table,
    schema: &Schema,
    vars: &Value,
    parent_path: &str,
) -> Result<QueryArgs> {
    let mut out = QueryArgs::default();
    for (name_p, value_p) in args {
        let name = name_p.node.as_str();
        let v = &value_p.node;
        match name {
            "where" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.where"))?;
                out.where_ = Some(lower_where(
                    &json,
                    table,
                    schema,
                    &format!("{parent_path}.where"),
                )?);
            }
            "order_by" => {
                out.order_by = lower_order_by(v, vars, &format!("{parent_path}.order_by"))?;
            }
            "limit" => {
                out.limit = Some(gql_u64(v, vars, &format!("{parent_path}.limit"))?);
            }
            "offset" => {
                out.offset = Some(gql_u64(v, vars, &format!("{parent_path}.offset"))?);
            }
            _ => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.{name}"),
                    message: format!("unknown argument '{name}'"),
                })
            }
        }
    }
    Ok(out)
}

fn lower_where(json: &Value, table: &Table, schema: &Schema, path: &str) -> Result<BoolExpr> {
    let obj = json.as_object().ok_or_else(|| Error::Validate {
        path: path.into(),
        message: "expected object".into(),
    })?;
    let mut parts: Vec<BoolExpr> = Vec::new();
    for (k, v) in obj {
        match k.as_str() {
            "_and" => {
                let arr = v.as_array().ok_or_else(|| Error::Validate {
                    path: format!("{path}._and"),
                    message: "expected array".into(),
                })?;
                let inner: Result<Vec<BoolExpr>> = arr
                    .iter()
                    .enumerate()
                    .map(|(i, x)| lower_where(x, table, schema, &format!("{path}._and[{i}]")))
                    .collect();
                parts.push(BoolExpr::And(inner?));
            }
            "_or" => {
                let arr = v.as_array().ok_or_else(|| Error::Validate {
                    path: format!("{path}._or"),
                    message: "expected array".into(),
                })?;
                let inner: Result<Vec<BoolExpr>> = arr
                    .iter()
                    .enumerate()
                    .map(|(i, x)| lower_where(x, table, schema, &format!("{path}._or[{i}]")))
                    .collect();
                parts.push(BoolExpr::Or(inner?));
            }
            "_not" => {
                parts.push(BoolExpr::Not(Box::new(lower_where(
                    v,
                    table,
                    schema,
                    &format!("{path}._not"),
                )?)));
            }
            col_name => {
                if let Some(rel) = table.find_relation(col_name) {
                    let target =
                        schema
                            .table(&rel.target_table)
                            .ok_or_else(|| Error::Validate {
                                path: format!("{path}.{col_name}"),
                                message: format!(
                                    "relation target table '{}' missing",
                                    rel.target_table
                                ),
                            })?;
                    let inner = lower_where(v, target, schema, &format!("{path}.{col_name}"))?;
                    parts.push(BoolExpr::Relation {
                        name: col_name.to_string(),
                        inner: Box::new(inner),
                    });
                    continue;
                }

                let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
                    path: format!("{path}.{col_name}"),
                    message: format!("unknown column '{col_name}' on '{}'", table.exposed_name),
                })?;
                let op_obj = v.as_object().ok_or_else(|| Error::Validate {
                    path: format!("{path}.{col_name}"),
                    message: "expected operator object".into(),
                })?;
                for (op_name, op_val) in op_obj {
                    let op = match op_name.as_str() {
                        "_eq" => CmpOp::Eq,
                        "_neq" => CmpOp::Neq,
                        "_gt" => CmpOp::Gt,
                        "_gte" => CmpOp::Gte,
                        "_lt" => CmpOp::Lt,
                        "_lte" => CmpOp::Lte,
                        other => {
                            return Err(Error::Validate {
                                path: format!("{path}.{col_name}"),
                                message: format!("unsupported operator '{other}'"),
                            })
                        }
                    };
                    parts.push(BoolExpr::Compare {
                        column: col.exposed_name.clone(),
                        op,
                        value: op_val.clone(),
                    });
                }
            }
        }
    }
    Ok(if parts.len() == 1 {
        parts.into_iter().next().unwrap()
    } else {
        BoolExpr::And(parts)
    })
}

fn lower_order_by(v: &GqlValue, vars: &Value, path: &str) -> Result<Vec<OrderBy>> {
    let json = gql_to_json(v, vars, path)?;
    let arr: Vec<&Value> = match &json {
        Value::Array(xs) => xs.iter().collect(),
        Value::Object(_) => vec![&json],
        _ => {
            return Err(Error::Validate {
                path: path.into(),
                message: "expected object or array".into(),
            })
        }
    };
    let mut out = Vec::new();
    for (i, item) in arr.iter().enumerate() {
        let obj = item.as_object().ok_or_else(|| Error::Validate {
            path: format!("{path}[{i}]"),
            message: "expected object".into(),
        })?;
        for (col, dir_val) in obj {
            let dir_s = dir_val.as_str().ok_or_else(|| Error::Validate {
                path: format!("{path}[{i}].{col}"),
                message: "expected 'asc' or 'desc'".into(),
            })?;
            let direction = match dir_s {
                "asc" => OrderDir::Asc,
                "desc" => OrderDir::Desc,
                other => {
                    return Err(Error::Validate {
                        path: format!("{path}[{i}].{col}"),
                        message: format!("unknown direction '{other}'"),
                    })
                }
            };
            out.push(OrderBy {
                column: col.clone(),
                direction,
            });
        }
    }
    Ok(out)
}

fn gql_u64(v: &GqlValue, vars: &Value, path: &str) -> Result<u64> {
    let json = gql_to_json(v, vars, path)?;
    json.as_u64().ok_or_else(|| Error::Validate {
        path: path.into(),
        message: "expected non-negative integer".into(),
    })
}

/// Convert a GraphQL value to JSON, resolving variable references from `vars`.
fn gql_to_json(v: &GqlValue, vars: &Value, path: &str) -> Result<Value> {
    match v {
        GqlValue::Null => Ok(Value::Null),
        GqlValue::Number(n) => serde_json::to_value(n).map_err(|e| Error::Parse(e.to_string())),
        GqlValue::String(s) => Ok(Value::String(s.clone())),
        GqlValue::Boolean(b) => Ok(Value::Bool(*b)),
        GqlValue::Enum(e) => Ok(Value::String(e.to_string())),
        GqlValue::List(xs) => {
            let mut out = Vec::with_capacity(xs.len());
            for (i, x) in xs.iter().enumerate() {
                out.push(gql_to_json(x, vars, &format!("{path}[{i}]"))?);
            }
            Ok(Value::Array(out))
        }
        GqlValue::Object(kv) => {
            let mut out = serde_json::Map::new();
            for (k, val) in kv {
                out.insert(
                    k.to_string(),
                    gql_to_json(val, vars, &format!("{path}.{k}"))?,
                );
            }
            Ok(Value::Object(out))
        }
        GqlValue::Variable(name) => {
            let nm = name.as_str();
            vars.get(nm).cloned().ok_or_else(|| Error::Variable {
                name: nm.to_string(),
                message: "not bound".into(),
            })
        }
        GqlValue::Binary(_) => Err(Error::Parse("binary literals not supported".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Field, Operation};
    use crate::schema::{PgType, Schema, Table};
    use serde_json::json;

    fn schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true),
            )
            .build()
    }

    #[test]
    fn parse_plain_list() {
        let op =
            parse_and_lower("query { users { id name } }", &json!({}), None, &schema()).unwrap();
        match op {
            Operation::Query(roots) => {
                assert_eq!(roots.len(), 1);
                assert_eq!(roots[0].table, "users");
                assert_eq!(roots[0].alias, "users");
                let crate::ast::RootBody::List { selection } = &roots[0].body else {
                    panic!("expected List");
                };
                assert_eq!(selection.len(), 2);
                match &selection[0] {
                    Field::Column { physical, alias } => {
                        assert_eq!(physical, "id");
                        assert_eq!(alias, "id");
                    }
                    _ => panic!("expected Column"),
                }
            }
        }
    }

    #[test]
    fn parse_respects_field_alias() {
        let op =
            parse_and_lower("query { users { uid: id } }", &json!({}), None, &schema()).unwrap();
        let Operation::Query(roots) = op;
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!("expected List");
        };
        match &selection[0] {
            Field::Column { physical, alias } => {
                assert_eq!(physical, "id");
                assert_eq!(alias, "uid");
            }
            _ => panic!("expected Column"),
        }
    }

    #[test]
    fn parse_rejects_unknown_table() {
        let err =
            parse_and_lower("query { widgets { id } }", &json!({}), None, &schema()).unwrap_err();
        assert!(format!("{err}").contains("unknown root field 'widgets'"));
    }

    #[test]
    fn parse_rejects_unknown_column() {
        let err =
            parse_and_lower("query { users { bogus } }", &json!({}), None, &schema()).unwrap_err();
        assert!(format!("{err}").contains("unknown column 'bogus'"));
    }

    #[test]
    fn parse_where_eq_with_variable() {
        let op = parse_and_lower(
            "query Q($uid: Int!) { users(where: {id: {_eq: $uid}}, limit: 10) { id name } }",
            &json!({"uid": 42}),
            Some("Q"),
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        let args = &roots[0].args;
        assert_eq!(args.limit, Some(10));
        match args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::Compare { column, op, value } => {
                assert_eq!(column, "id");
                assert!(matches!(op, crate::ast::CmpOp::Eq));
                assert_eq!(value, &json!(42));
            }
            _ => panic!("expected Compare"),
        }
    }

    #[test]
    fn parse_where_and_of_ops() {
        let op = parse_and_lower(
            "query { users(where: {_and: [{id: {_gt: 1}}, {name: {_neq: \"bob\"}}]}) { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::And(parts) => assert_eq!(parts.len(), 2),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn parse_order_by_list() {
        let op = parse_and_lower(
            "query { users(order_by: [{name: asc}, {id: desc}]) { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        assert_eq!(roots[0].args.order_by.len(), 2);
        assert_eq!(roots[0].args.order_by[0].column, "name");
        assert!(matches!(
            roots[0].args.order_by[0].direction,
            crate::ast::OrderDir::Asc
        ));
    }

    fn schema_with_relations() -> Schema {
        use crate::schema::Relation;
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("title", "title", PgType::Text, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .relation("user", Relation::object("users").on([("user_id", "id")])),
            )
            .build()
    }

    #[test]
    fn parse_nested_array_relation() {
        let op = parse_and_lower(
            "query { users { id posts(limit: 3) { title } } }",
            &json!({}),
            None,
            &schema_with_relations(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
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
                assert_eq!(args.limit, Some(3));
                assert_eq!(selection.len(), 1);
            }
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn parse_where_relation_exists() {
        let op = parse_and_lower(
            r#"query { users(where: {posts: {title: {_eq: "hello"}}}) { id } }"#,
            &json!({}),
            None,
            &schema_with_relations(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::Relation { name, inner } => {
                assert_eq!(name, "posts");
                match inner.as_ref() {
                    crate::ast::BoolExpr::Compare { column, .. } => {
                        assert_eq!(column, "title");
                    }
                    _ => panic!("expected Compare"),
                }
            }
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn parse_nested_object_relation() {
        let op = parse_and_lower(
            "query { posts { title user { name } } }",
            &json!({}),
            None,
            &schema_with_relations(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!("expected List");
        };
        match &selection[1] {
            Field::Relation { name, .. } => assert_eq!(name, "user"),
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn parse_missing_variable_errors() {
        let err = parse_and_lower(
            "query Q($uid: Int!) { users(where: {id: {_eq: $uid}}) { id } }",
            &json!({}),
            Some("Q"),
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("uid"));
    }
}
