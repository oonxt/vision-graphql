//! GraphQL string → IR.

use crate::ast::{BoolExpr, CmpOp, Field, Operation, OrderBy, OrderDir, QueryArgs, RootField};
use crate::error::{Error, Result};
use crate::schema::{Schema, Table};
use async_graphql_parser::parse_query;
use async_graphql_parser::types::{
    DocumentOperations, ExecutableDocument, Field as GqlField, FragmentDefinition, OperationType,
    Selection, SelectionSet,
};
use async_graphql_parser::Positioned;
use async_graphql_value::{Name, Value as GqlValue};
use serde_json::Value;
use std::collections::HashMap;

type Fragments<'a> = HashMap<String, &'a FragmentDefinition>;

pub fn parse_and_lower(
    source: &str,
    variables: &Value,
    operation_name: Option<&str>,
    schema: &Schema,
) -> Result<Operation> {
    let doc = parse_query(source).map_err(|e| Error::Parse(e.to_string()))?;
    let mut fragments: Fragments<'_> = HashMap::new();
    for (name, def) in &doc.fragments {
        fragments.insert(name.as_str().to_string(), &def.node);
    }
    let op = pick_operation(&doc, operation_name)?;
    match op.ty {
        OperationType::Query => lower_query(op.selection_set, schema, variables, &fragments),
        OperationType::Mutation => lower_mutation(op.selection_set, schema, variables, &fragments),
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

fn lower_query(
    set: &SelectionSet,
    schema: &Schema,
    vars: &Value,
    fragments: &Fragments<'_>,
) -> Result<Operation> {
    let mut roots = Vec::new();
    for sel in &set.items {
        match &sel.node {
            Selection::FragmentSpread(fs) => {
                let name = fs.node.fragment_name.node.as_str();
                let frag = fragments.get(name).ok_or_else(|| Error::Validate {
                    path: "query".into(),
                    message: format!("unknown fragment '{name}'"),
                })?;
                if let Operation::Query(mut inner_roots) =
                    lower_query(&frag.selection_set.node, schema, vars, fragments)?
                {
                    roots.append(&mut inner_roots);
                }
                continue;
            }
            Selection::InlineFragment(ifr) => {
                if let Operation::Query(mut inner_roots) =
                    lower_query(&ifr.node.selection_set.node, schema, vars, fragments)?
                {
                    roots.append(&mut inner_roots);
                }
                continue;
            }
            Selection::Field(f) => {
                let field = &f.node;
                let name = field.name.node.as_str();
                let alias = field
                    .alias
                    .as_ref()
                    .map(|a| a.node.as_str().to_string())
                    .unwrap_or_else(|| name.to_string());

                // Aggregate root: "<table>_aggregate"
                if let Some(base_name) = name.strip_suffix("_aggregate") {
                    if let Some(table) = schema.table(base_name) {
                        let args = lower_args(&field.arguments, table, schema, vars, &alias)?;
                        let (ops, nodes) =
                            lower_aggregate_selection(&field.selection_set.node, table, &alias)?;
                        roots.push(RootField {
                            table: base_name.to_string(),
                            alias,
                            args,
                            body: crate::ast::RootBody::Aggregate { ops, nodes },
                        });
                        continue;
                    }
                }

                // By-PK root: "<table>_by_pk"
                if let Some(base_name) = name.strip_suffix("_by_pk") {
                    if let Some(table) = schema.table(base_name) {
                        if table.primary_key.is_empty() {
                            return Err(Error::Validate {
                                path: alias.clone(),
                                message: format!(
                                    "table '{}' has no primary key; _by_pk not available",
                                    table.exposed_name
                                ),
                            });
                        }
                        let mut pk: Vec<(String, serde_json::Value)> = Vec::new();
                        for pk_col in &table.primary_key {
                            let found = field
                                .arguments
                                .iter()
                                .find(|(n, _)| n.node.as_str() == pk_col);
                            let (_, value_p) = found.ok_or_else(|| Error::Validate {
                                path: alias.clone(),
                                message: format!(
                                    "required primary key argument '{pk_col}' missing"
                                ),
                            })?;
                            let json =
                                gql_to_json(&value_p.node, vars, &format!("{alias}.{pk_col}"))?;
                            pk.push((pk_col.clone(), json));
                        }
                        let selection = lower_selection_set(
                            &field.selection_set.node,
                            table,
                            schema,
                            vars,
                            fragments,
                            &alias,
                        )?;
                        roots.push(RootField {
                            table: base_name.to_string(),
                            alias,
                            args: QueryArgs::default(),
                            body: crate::ast::RootBody::ByPk { pk, selection },
                        });
                        continue;
                    }
                }

                let table = schema.table(name).ok_or_else(|| Error::Validate {
                    path: alias.clone(),
                    message: format!("unknown root field '{name}'"),
                })?;
                let args = lower_args(&field.arguments, table, schema, vars, &alias)?;
                let selection = lower_selection_set(
                    &field.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
                    &alias,
                )?;

                roots.push(RootField {
                    table: name.to_string(),
                    alias,
                    args,
                    body: crate::ast::RootBody::List { selection },
                });
            }
        }
    }
    Ok(Operation::Query(roots))
}

fn lower_mutation(
    set: &SelectionSet,
    schema: &Schema,
    vars: &Value,
    fragments: &Fragments<'_>,
) -> Result<Operation> {
    let mut fields: Vec<crate::ast::MutationField> = Vec::new();
    for sel in &set.items {
        let f = match &sel.node {
            Selection::Field(f) => f,
            Selection::FragmentSpread(fs) => {
                let name = fs.node.fragment_name.node.as_str();
                let frag = fragments.get(name).ok_or_else(|| Error::Validate {
                    path: "mutation".into(),
                    message: format!("unknown fragment '{name}'"),
                })?;
                if let Operation::Mutation(mut inner) =
                    lower_mutation(&frag.selection_set.node, schema, vars, fragments)?
                {
                    fields.append(&mut inner);
                }
                continue;
            }
            Selection::InlineFragment(ifr) => {
                if let Operation::Mutation(mut inner) =
                    lower_mutation(&ifr.node.selection_set.node, schema, vars, fragments)?
                {
                    fields.append(&mut inner);
                }
                continue;
            }
        };
        let field = &f.node;
        let name = field.name.node.as_str();
        let alias = field
            .alias
            .as_ref()
            .map(|a| a.node.as_str().to_string())
            .unwrap_or_else(|| name.to_string());
        let mf = lower_mutation_field(name, &alias, field, schema, vars)?;
        fields.push(mf);
    }
    Ok(Operation::Mutation(fields))
}

fn lower_mutation_field(
    name: &str,
    alias: &str,
    field: &GqlField,
    schema: &Schema,
    vars: &Value,
) -> Result<crate::ast::MutationField> {
    use crate::ast::MutationField;

    // insert_<table>_one
    if let Some(base) = name.strip_suffix("_one") {
        if let Some(base_name) = base.strip_prefix("insert_") {
            if let Some(table) = schema.table(base_name) {
                let (objects, on_conflict) =
                    parse_insert_args(&field.arguments, table, schema, vars, alias, true)?;
                let returning =
                    lower_selection_columns_only(&field.selection_set.node, table, alias)?;
                return Ok(MutationField::Insert {
                    alias: alias.to_string(),
                    table: base_name.to_string(),
                    objects,
                    on_conflict,
                    returning,
                    one: true,
                });
            }
        }
    }

    // insert_<table>
    if let Some(base_name) = name.strip_prefix("insert_") {
        if let Some(table) = schema.table(base_name) {
            let (objects, on_conflict) =
                parse_insert_args(&field.arguments, table, schema, vars, alias, false)?;
            let returning = parse_returning(&field.selection_set.node, table, alias)?;
            return Ok(MutationField::Insert {
                alias: alias.to_string(),
                table: base_name.to_string(),
                objects,
                on_conflict,
                returning,
                one: false,
            });
        }
    }

    // update_<table>_by_pk
    if let Some(base) = name.strip_suffix("_by_pk") {
        if let Some(base_name) = base.strip_prefix("update_") {
            if let Some(table) = schema.table(base_name) {
                if table.primary_key.is_empty() {
                    return Err(Error::Validate {
                        path: alias.into(),
                        message: format!(
                            "table '{}' has no primary key; _by_pk not available",
                            table.exposed_name
                        ),
                    });
                }
                let (pk, set) = parse_update_by_pk_args(&field.arguments, table, vars, alias)?;
                let selection =
                    lower_selection_columns_only(&field.selection_set.node, table, alias)?;
                return Ok(MutationField::UpdateByPk {
                    alias: alias.to_string(),
                    table: base_name.to_string(),
                    pk,
                    set,
                    selection,
                });
            }
        }
    }

    // update_<table>
    if let Some(base_name) = name.strip_prefix("update_") {
        if let Some(table) = schema.table(base_name) {
            let (where_, set) = parse_update_args(&field.arguments, table, schema, vars, alias)?;
            let returning = parse_returning(&field.selection_set.node, table, alias)?;
            return Ok(MutationField::Update {
                alias: alias.to_string(),
                table: base_name.to_string(),
                where_,
                set,
                returning,
            });
        }
    }

    // delete_<table>_by_pk
    if let Some(base) = name.strip_suffix("_by_pk") {
        if let Some(base_name) = base.strip_prefix("delete_") {
            if let Some(table) = schema.table(base_name) {
                if table.primary_key.is_empty() {
                    return Err(Error::Validate {
                        path: alias.into(),
                        message: format!(
                            "table '{}' has no primary key; _by_pk not available",
                            table.exposed_name
                        ),
                    });
                }
                let mut pk: Vec<(String, serde_json::Value)> = Vec::new();
                for pk_col in &table.primary_key {
                    let found = field
                        .arguments
                        .iter()
                        .find(|(n, _)| n.node.as_str() == pk_col);
                    let (_, value_p) = found.ok_or_else(|| Error::Validate {
                        path: alias.into(),
                        message: format!("required primary key argument '{pk_col}' missing"),
                    })?;
                    let json = gql_to_json(&value_p.node, vars, &format!("{alias}.{pk_col}"))?;
                    pk.push((pk_col.clone(), json));
                }
                let selection =
                    lower_selection_columns_only(&field.selection_set.node, table, alias)?;
                return Ok(MutationField::DeleteByPk {
                    alias: alias.to_string(),
                    table: base_name.to_string(),
                    pk,
                    selection,
                });
            }
        }
    }

    // delete_<table>
    if let Some(base_name) = name.strip_prefix("delete_") {
        if let Some(table) = schema.table(base_name) {
            let mut where_: Option<crate::ast::BoolExpr> = None;
            for (name_p, value_p) in &field.arguments {
                let aname = name_p.node.as_str();
                let v = &value_p.node;
                if aname == "where" {
                    let json = gql_to_json(v, vars, &format!("{alias}.where"))?;
                    where_ = Some(lower_where(
                        &json,
                        table,
                        schema,
                        &format!("{alias}.where"),
                    )?);
                } else {
                    return Err(Error::Validate {
                        path: format!("{alias}.{aname}"),
                        message: format!("unknown argument '{aname}'"),
                    });
                }
            }
            let where_ = where_.ok_or_else(|| Error::Validate {
                path: alias.into(),
                message: "delete requires 'where'".into(),
            })?;
            let returning = parse_returning(&field.selection_set.node, table, alias)?;
            return Ok(MutationField::Delete {
                alias: alias.to_string(),
                table: base_name.to_string(),
                where_,
                returning,
            });
        }
    }

    Err(Error::Validate {
        path: alias.into(),
        message: format!("mutation field '{name}' not yet supported"),
    })
}

#[allow(clippy::type_complexity)]
fn parse_insert_args(
    args: &[(Positioned<Name>, Positioned<GqlValue>)],
    table: &Table,
    schema: &Schema,
    vars: &Value,
    parent_path: &str,
    single: bool,
) -> Result<(
    Vec<std::collections::BTreeMap<String, serde_json::Value>>,
    Option<crate::ast::OnConflict>,
)> {
    use std::collections::BTreeMap;
    let mut objects: Vec<BTreeMap<String, serde_json::Value>> = Vec::new();
    let mut on_conflict: Option<crate::ast::OnConflict> = None;
    let _ = schema;

    for (name_p, value_p) in args {
        let aname = name_p.node.as_str();
        let v = &value_p.node;
        match aname {
            "object" if single => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.object"))?;
                let obj = json_object_to_map(&json, table, &format!("{parent_path}.object"))?;
                objects.push(obj);
            }
            "objects" if !single => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.objects"))?;
                let arr = json.as_array().ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.objects"),
                    message: "expected array".into(),
                })?;
                for (i, item) in arr.iter().enumerate() {
                    let obj =
                        json_object_to_map(item, table, &format!("{parent_path}.objects[{i}]"))?;
                    objects.push(obj);
                }
            }
            "on_conflict" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.on_conflict"))?;
                on_conflict = Some(parse_on_conflict(
                    &json,
                    table,
                    &format!("{parent_path}.on_conflict"),
                )?);
            }
            other => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.{other}"),
                    message: format!("unknown argument '{other}'"),
                });
            }
        }
    }
    if objects.is_empty() {
        return Err(Error::Validate {
            path: parent_path.into(),
            message: if single {
                "missing required argument 'object'".into()
            } else {
                "missing required argument 'objects'".into()
            },
        });
    }
    Ok((objects, on_conflict))
}

fn json_object_to_map(
    json: &Value,
    table: &Table,
    path: &str,
) -> Result<std::collections::BTreeMap<String, serde_json::Value>> {
    use std::collections::BTreeMap;
    let obj = json.as_object().ok_or_else(|| Error::Validate {
        path: path.into(),
        message: "expected object".into(),
    })?;
    let mut out: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (k, v) in obj {
        if table.find_column(k).is_none() {
            return Err(Error::Validate {
                path: format!("{path}.{k}"),
                message: format!("unknown column '{k}' on '{}'", table.exposed_name),
            });
        }
        out.insert(k.clone(), v.clone());
    }
    if out.is_empty() {
        return Err(Error::Validate {
            path: path.into(),
            message: "insert row must set at least one column".into(),
        });
    }
    Ok(out)
}

fn parse_on_conflict(json: &Value, table: &Table, path: &str) -> Result<crate::ast::OnConflict> {
    let obj = json.as_object().ok_or_else(|| Error::Validate {
        path: path.into(),
        message: "expected object".into(),
    })?;
    let constraint = obj
        .get("constraint")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Validate {
            path: format!("{path}.constraint"),
            message: "missing or non-string 'constraint'".into(),
        })?
        .to_string();
    let mut update_columns: Vec<String> = Vec::new();
    if let Some(cols) = obj.get("update_columns") {
        let arr = cols.as_array().ok_or_else(|| Error::Validate {
            path: format!("{path}.update_columns"),
            message: "expected array".into(),
        })?;
        for (i, c) in arr.iter().enumerate() {
            let cn = c.as_str().ok_or_else(|| Error::Validate {
                path: format!("{path}.update_columns[{i}]"),
                message: "expected string".into(),
            })?;
            if table.find_column(cn).is_none() {
                return Err(Error::Validate {
                    path: format!("{path}.update_columns[{i}]"),
                    message: format!("unknown column '{cn}' on '{}'", table.exposed_name),
                });
            }
            update_columns.push(cn.to_string());
        }
    }
    let where_ = obj
        .get("where")
        .map(|w| {
            lower_where(
                w,
                table,
                &Schema::builder().build(),
                &format!("{path}.where"),
            )
        })
        .transpose()?;
    Ok(crate::ast::OnConflict {
        constraint,
        update_columns,
        where_,
    })
}

fn parse_update_args(
    args: &[(Positioned<Name>, Positioned<GqlValue>)],
    table: &Table,
    schema: &Schema,
    vars: &Value,
    parent_path: &str,
) -> Result<(
    crate::ast::BoolExpr,
    std::collections::BTreeMap<String, serde_json::Value>,
)> {
    use std::collections::BTreeMap;
    let mut where_: Option<crate::ast::BoolExpr> = None;
    let mut set: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (name_p, value_p) in args {
        let aname = name_p.node.as_str();
        let v = &value_p.node;
        match aname {
            "where" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.where"))?;
                where_ = Some(lower_where(
                    &json,
                    table,
                    schema,
                    &format!("{parent_path}.where"),
                )?);
            }
            "_set" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}._set"))?;
                set = json_object_to_map(&json, table, &format!("{parent_path}._set"))?;
            }
            other => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.{other}"),
                    message: format!("unknown argument '{other}'"),
                });
            }
        }
    }
    let w = where_.ok_or_else(|| Error::Validate {
        path: parent_path.into(),
        message: "update requires 'where'".into(),
    })?;
    if set.is_empty() {
        return Err(Error::Validate {
            path: parent_path.into(),
            message: "update requires non-empty '_set'".into(),
        });
    }
    Ok((w, set))
}

#[allow(clippy::type_complexity)]
fn parse_update_by_pk_args(
    args: &[(Positioned<Name>, Positioned<GqlValue>)],
    table: &Table,
    vars: &Value,
    parent_path: &str,
) -> Result<(
    Vec<(String, serde_json::Value)>,
    std::collections::BTreeMap<String, serde_json::Value>,
)> {
    use std::collections::BTreeMap;
    let mut pk_obj: Option<serde_json::Map<String, serde_json::Value>> = None;
    let mut set: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (name_p, value_p) in args {
        let aname = name_p.node.as_str();
        let v = &value_p.node;
        match aname {
            "pk_columns" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.pk_columns"))?;
                let obj = json.as_object().ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.pk_columns"),
                    message: "expected object".into(),
                })?;
                pk_obj = Some(obj.clone());
            }
            "_set" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}._set"))?;
                set = json_object_to_map(&json, table, &format!("{parent_path}._set"))?;
            }
            other => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.{other}"),
                    message: format!("unknown argument '{other}'"),
                });
            }
        }
    }
    let pk_obj = pk_obj.ok_or_else(|| Error::Validate {
        path: parent_path.into(),
        message: "missing required 'pk_columns'".into(),
    })?;
    if set.is_empty() {
        return Err(Error::Validate {
            path: parent_path.into(),
            message: "update_by_pk requires non-empty '_set'".into(),
        });
    }
    let mut pk: Vec<(String, serde_json::Value)> = Vec::new();
    for pk_col in &table.primary_key {
        let v = pk_obj.get(pk_col).ok_or_else(|| Error::Validate {
            path: format!("{parent_path}.pk_columns.{pk_col}"),
            message: format!("missing primary key value '{pk_col}'"),
        })?;
        pk.push((pk_col.clone(), v.clone()));
    }
    Ok((pk, set))
}

fn parse_returning(set: &SelectionSet, table: &Table, parent_path: &str) -> Result<Vec<Field>> {
    let mut returning: Vec<Field> = Vec::new();
    for sel in &set.items {
        let Selection::Field(f) = &sel.node else {
            return Err(Error::Parse(
                "fragments not supported in mutation return".into(),
            ));
        };
        let field = &f.node;
        let fname = field.name.node.as_str();
        match fname {
            "affected_rows" => {}
            "returning" => {
                returning = lower_selection_columns_only(
                    &field.selection_set.node,
                    table,
                    &format!("{parent_path}.returning"),
                )?;
            }
            other => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.{other}"),
                    message: format!("unknown mutation return field '{other}'"),
                });
            }
        }
    }
    Ok(returning)
}

fn lower_selection_set(
    set: &SelectionSet,
    table: &Table,
    schema: &Schema,
    vars: &Value,
    fragments: &Fragments<'_>,
    parent_path: &str,
) -> Result<Vec<Field>> {
    let mut out = Vec::new();
    for sel in &set.items {
        match &sel.node {
            Selection::FragmentSpread(fs) => {
                let name = fs.node.fragment_name.node.as_str();
                let frag = fragments.get(name).ok_or_else(|| Error::Validate {
                    path: parent_path.into(),
                    message: format!("unknown fragment '{name}'"),
                })?;
                let mut inner = lower_selection_set(
                    &frag.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
                    parent_path,
                )?;
                out.append(&mut inner);
                continue;
            }
            Selection::InlineFragment(ifr) => {
                let mut inner = lower_selection_set(
                    &ifr.node.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
                    parent_path,
                )?;
                out.append(&mut inner);
                continue;
            }
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
                        fragments,
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
            "distinct_on" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.distinct_on"))?;
                let items = match &json {
                    Value::Array(xs) => xs.clone(),
                    single => vec![single.clone()],
                };
                let mut cols = Vec::new();
                for (i, item) in items.iter().enumerate() {
                    let s = item.as_str().ok_or_else(|| Error::Validate {
                        path: format!("{parent_path}.distinct_on[{i}]"),
                        message: "expected column name (enum or string)".into(),
                    })?;
                    if table.find_column(s).is_none() {
                        return Err(Error::Validate {
                            path: format!("{parent_path}.distinct_on[{i}]"),
                            message: format!("unknown column '{s}' on '{}'", table.exposed_name),
                        });
                    }
                    cols.push(s.to_string());
                }
                out.distinct_on = cols;
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
                    match op_name.as_str() {
                        "_eq" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Eq,
                            value: op_val.clone(),
                        }),
                        "_neq" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Neq,
                            value: op_val.clone(),
                        }),
                        "_gt" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Gt,
                            value: op_val.clone(),
                        }),
                        "_gte" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Gte,
                            value: op_val.clone(),
                        }),
                        "_lt" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Lt,
                            value: op_val.clone(),
                        }),
                        "_lte" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Lte,
                            value: op_val.clone(),
                        }),
                        "_like" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::Like,
                            value: op_val.clone(),
                        }),
                        "_ilike" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::ILike,
                            value: op_val.clone(),
                        }),
                        "_nlike" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::NLike,
                            value: op_val.clone(),
                        }),
                        "_nilike" => parts.push(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op: CmpOp::NILike,
                            value: op_val.clone(),
                        }),
                        "_is_null" => {
                            let b = op_val.as_bool().ok_or_else(|| Error::Validate {
                                path: format!("{path}.{col_name}._is_null"),
                                message: "expected boolean".into(),
                            })?;
                            parts.push(BoolExpr::IsNull {
                                column: col.exposed_name.clone(),
                                negated: !b,
                            });
                        }
                        "_in" => {
                            let arr = op_val.as_array().ok_or_else(|| Error::Validate {
                                path: format!("{path}.{col_name}._in"),
                                message: "expected array".into(),
                            })?;
                            let inner: Vec<BoolExpr> = arr
                                .iter()
                                .map(|v| BoolExpr::Compare {
                                    column: col.exposed_name.clone(),
                                    op: CmpOp::Eq,
                                    value: v.clone(),
                                })
                                .collect();
                            parts.push(BoolExpr::Or(inner));
                        }
                        "_nin" => {
                            let arr = op_val.as_array().ok_or_else(|| Error::Validate {
                                path: format!("{path}.{col_name}._nin"),
                                message: "expected array".into(),
                            })?;
                            let inner: Vec<BoolExpr> = arr
                                .iter()
                                .map(|v| BoolExpr::Compare {
                                    column: col.exposed_name.clone(),
                                    op: CmpOp::Neq,
                                    value: v.clone(),
                                })
                                .collect();
                            parts.push(BoolExpr::And(inner));
                        }
                        other => {
                            return Err(Error::Validate {
                                path: format!("{path}.{col_name}"),
                                message: format!("unsupported operator '{other}'"),
                            });
                        }
                    }
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

fn lower_aggregate_selection(
    set: &SelectionSet,
    table: &Table,
    parent_path: &str,
) -> Result<(Vec<crate::ast::AggOp>, Option<Vec<Field>>)> {
    let mut ops: Vec<crate::ast::AggOp> = Vec::new();
    let mut nodes: Option<Vec<Field>> = None;

    for sel in &set.items {
        let Selection::Field(f) = &sel.node else {
            return Err(Error::Parse(
                "fragments are not supported in Phase 3".into(),
            ));
        };
        let field = &f.node;
        let key = field.name.node.as_str();
        match key {
            "aggregate" => {
                for s in &field.selection_set.node.items {
                    let Selection::Field(sf) = &s.node else {
                        return Err(Error::Parse(
                            "fragments not supported inside aggregate".into(),
                        ));
                    };
                    let sf = &sf.node;
                    let op_name = sf.name.node.as_str();
                    match op_name {
                        "count" => {
                            ops.push(crate::ast::AggOp::Count);
                        }
                        "sum" | "avg" | "max" | "min" => {
                            let mut columns = Vec::new();
                            for cs in &sf.selection_set.node.items {
                                let Selection::Field(cf) = &cs.node else {
                                    return Err(Error::Parse(
                                        "fragments not supported inside aggregate".into(),
                                    ));
                                };
                                let cname = cf.node.name.node.as_str();
                                let col =
                                    table.find_column(cname).ok_or_else(|| Error::Validate {
                                        path: format!("{parent_path}.aggregate.{op_name}.{cname}"),
                                        message: format!(
                                            "unknown column '{cname}' on '{}'",
                                            table.exposed_name
                                        ),
                                    })?;
                                columns.push(col.exposed_name.clone());
                            }
                            let op = match op_name {
                                "sum" => crate::ast::AggOp::Sum { columns },
                                "avg" => crate::ast::AggOp::Avg { columns },
                                "max" => crate::ast::AggOp::Max { columns },
                                "min" => crate::ast::AggOp::Min { columns },
                                _ => unreachable!(),
                            };
                            ops.push(op);
                        }
                        other => {
                            return Err(Error::Validate {
                                path: format!("{parent_path}.aggregate.{other}"),
                                message: format!("unsupported aggregate '{other}'"),
                            });
                        }
                    }
                }
            }
            "nodes" => {
                let fields = lower_selection_columns_only(
                    &field.selection_set.node,
                    table,
                    &format!("{parent_path}.nodes"),
                )?;
                nodes = Some(fields);
            }
            other => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.{other}"),
                    message: format!("unknown aggregate subfield '{other}'"),
                });
            }
        }
    }
    Ok((ops, nodes))
}

fn lower_selection_columns_only(
    set: &SelectionSet,
    table: &Table,
    parent_path: &str,
) -> Result<Vec<Field>> {
    let mut out = Vec::new();
    for sel in &set.items {
        let Selection::Field(f) = &sel.node else {
            return Err(Error::Parse(
                "fragments not supported inside aggregate nodes".into(),
            ));
        };
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
    Ok(out)
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
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"]),
            )
            .build()
    }

    #[test]
    fn parse_plain_list() {
        let op =
            parse_and_lower("query { users { id name } }", &json!({}), None, &schema()).unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        {
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

    #[test]
    fn parse_respects_field_alias() {
        let op =
            parse_and_lower("query { users { uid: id } }", &json!({}), None, &schema()).unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
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
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
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
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
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
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
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
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
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
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
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
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!("expected List");
        };
        match &selection[1] {
            Field::Relation { name, .. } => assert_eq!(name, "user"),
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn parse_aggregate_basic() {
        let op = parse_and_lower(
            "query { users_aggregate { aggregate { count, sum { id } } nodes { id } } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        assert_eq!(roots[0].table, "users");
        match &roots[0].body {
            crate::ast::RootBody::Aggregate { ops, nodes } => {
                assert_eq!(ops.len(), 2);
                assert!(matches!(ops[0], crate::ast::AggOp::Count));
                match &ops[1] {
                    crate::ast::AggOp::Sum { columns } => {
                        assert_eq!(columns, &vec!["id".to_string()])
                    }
                    _ => panic!("expected Sum"),
                }
                assert_eq!(nodes.as_ref().map(|n| n.len()).unwrap_or(0), 1);
            }
            _ => panic!("expected Aggregate"),
        }
    }

    #[test]
    fn parse_insert_array() {
        let op = parse_and_lower(
            r#"mutation { insert_users(objects: [{name: "a"}, {name: "b"}]) { affected_rows returning { id } } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        match op {
            Operation::Mutation(fields) => {
                assert_eq!(fields.len(), 1);
                match &fields[0] {
                    crate::ast::MutationField::Insert {
                        objects,
                        returning,
                        one,
                        ..
                    } => {
                        assert_eq!(objects.len(), 2);
                        assert_eq!(returning.len(), 1);
                        assert!(!one);
                    }
                    _ => panic!("expected Insert"),
                }
            }
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn parse_insert_one() {
        let op = parse_and_lower(
            r#"mutation { insert_users_one(object: {name: "a"}) { id name } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        match op {
            Operation::Mutation(fields) => match &fields[0] {
                crate::ast::MutationField::Insert {
                    objects,
                    returning,
                    one,
                    ..
                } => {
                    assert_eq!(objects.len(), 1);
                    assert_eq!(returning.len(), 2);
                    assert!(one);
                }
                _ => panic!("expected Insert"),
            },
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn parse_insert_rejects_unknown_column() {
        let err = parse_and_lower(
            r#"mutation { insert_users(objects: [{bogus: 1}]) { affected_rows } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("unknown column 'bogus'"));
    }

    #[test]
    fn parse_where_like() {
        let op = parse_and_lower(
            r#"query { users(where: {name: {_like: "a%"}}) { id } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::Compare { op, .. } => {
                assert!(matches!(op, crate::ast::CmpOp::Like));
            }
            _ => panic!("expected Compare"),
        }
    }

    #[test]
    fn parse_where_is_null() {
        let op = parse_and_lower(
            r#"query { users(where: {name: {_is_null: true}}) { id } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::IsNull { column, negated } => {
                assert_eq!(column, "name");
                assert!(!negated);
            }
            _ => panic!("expected IsNull"),
        }
    }

    #[test]
    fn parse_where_in_expands_to_or() {
        let op = parse_and_lower(
            r#"query { users(where: {id: {_in: [1, 2, 3]}}) { id } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        match roots[0].args.where_.as_ref().unwrap() {
            crate::ast::BoolExpr::Or(parts) => assert_eq!(parts.len(), 3),
            _ => panic!("expected Or chain"),
        }
    }

    #[test]
    fn parse_named_fragment() {
        let op = parse_and_lower(
            r#"
            fragment UserFields on users { id name }
            query { users { ...UserFields } }
            "#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!("expected List");
        };
        assert_eq!(selection.len(), 2);
    }

    #[test]
    fn parse_inline_fragment() {
        let op = parse_and_lower(
            r#"query { users { ... on users { id name } } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query")
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!("expected List");
        };
        assert_eq!(selection.len(), 2);
    }

    #[test]
    fn parse_unknown_fragment_errors() {
        let err = parse_and_lower(
            r#"query { users { ...MissingFragment } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("MissingFragment"));
    }

    #[test]
    fn parse_distinct_on_list() {
        let op = parse_and_lower(
            "query { users(distinct_on: [name]) { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        assert_eq!(roots[0].args.distinct_on, vec!["name".to_string()]);
    }

    #[test]
    fn parse_by_pk_single_col() {
        let op = parse_and_lower(
            "query { users_by_pk(id: 7) { id name } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        assert_eq!(roots[0].table, "users");
        match &roots[0].body {
            crate::ast::RootBody::ByPk { pk, selection } => {
                assert_eq!(pk.len(), 1);
                assert_eq!(pk[0].0, "id");
                assert_eq!(pk[0].1, json!(7));
                assert_eq!(selection.len(), 2);
            }
            _ => panic!("expected ByPk"),
        }
    }

    #[test]
    fn parse_by_pk_with_variable() {
        let op = parse_and_lower(
            "query Q($uid: Int!) { users_by_pk(id: $uid) { name } }",
            &json!({"uid": 42}),
            Some("Q"),
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        match &roots[0].body {
            crate::ast::RootBody::ByPk { pk, .. } => {
                assert_eq!(pk[0].1, json!(42));
            }
            _ => panic!("expected ByPk"),
        }
    }

    #[test]
    fn parse_by_pk_missing_required_pk_errors() {
        let err = parse_and_lower(
            "query { users_by_pk { name } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("required primary key"));
    }

    #[test]
    fn parse_aggregate_count_only() {
        let op = parse_and_lower(
            "query { users_aggregate(where: {id: {_gt: 0}}) { aggregate { count } } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        match &roots[0].body {
            crate::ast::RootBody::Aggregate { ops, nodes } => {
                assert_eq!(ops.len(), 1);
                assert!(nodes.is_none());
            }
            _ => panic!("expected Aggregate"),
        }
        assert!(roots[0].args.where_.is_some());
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
