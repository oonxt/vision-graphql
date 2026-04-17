//! GraphQL string → IR.

use crate::ast::{Field, Operation, QueryArgs, RootField, RootKind};
use crate::error::{Error, Result};
use crate::schema::{Schema, Table};
use async_graphql_parser::parse_query;
use async_graphql_parser::types::{
    DocumentOperations, ExecutableDocument, OperationType, Selection, SelectionSet,
};
use async_graphql_value::Name;
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
        (DocumentOperations::Multiple(_), None) => Err(Error::Parse(
            "document has multiple operations; operation_name required".into(),
        )),
    }
}

fn lower_query(set: &SelectionSet, schema: &Schema, _vars: &Value) -> Result<Operation> {
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

                let selection = lower_selection_set(&field.selection_set.node, table, &alias)?;

                roots.push(RootField {
                    table: name.to_string(),
                    alias,
                    kind: RootKind::List,
                    args: QueryArgs::default(),
                    selection,
                });
            }
            Selection::FragmentSpread(_) | Selection::InlineFragment(_) => {
                return Err(Error::Parse(
                    "fragments are not supported in Phase 1".into(),
                ));
            }
        }
    }
    Ok(Operation::Query(roots))
}

fn lower_selection_set(
    set: &SelectionSet,
    table: &Table,
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

                let col = table.find_column(name).ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.{alias}"),
                    message: format!("unknown column '{name}' on '{}'", table.exposed_name),
                })?;
                out.push(Field::Column {
                    physical: col.physical_name.clone(),
                    alias,
                });
            }
            Selection::FragmentSpread(_) | Selection::InlineFragment(_) => {
                return Err(Error::Parse(
                    "fragments are not supported in Phase 1".into(),
                ));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Field, Operation, RootKind};
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
        let op = parse_and_lower(
            "query { users { id name } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        match op {
            Operation::Query(roots) => {
                assert_eq!(roots.len(), 1);
                assert_eq!(roots[0].table, "users");
                assert_eq!(roots[0].alias, "users");
                assert!(matches!(roots[0].kind, RootKind::List));
                assert_eq!(roots[0].selection.len(), 2);
                match &roots[0].selection[0] {
                    Field::Column { physical, alias } => {
                        assert_eq!(physical, "id");
                        assert_eq!(alias, "id");
                    }
                }
            }
        }
    }

    #[test]
    fn parse_respects_field_alias() {
        let op = parse_and_lower(
            "query { users { uid: id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op;
        match &roots[0].selection[0] {
            Field::Column { physical, alias } => {
                assert_eq!(physical, "id");
                assert_eq!(alias, "uid");
            }
        }
    }

    #[test]
    fn parse_rejects_unknown_table() {
        let err = parse_and_lower(
            "query { widgets { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("unknown root field 'widgets'"));
    }

    #[test]
    fn parse_rejects_unknown_column() {
        let err = parse_and_lower(
            "query { users { bogus } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("unknown column 'bogus'"));
    }
}
