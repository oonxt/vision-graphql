//! Answering `__schema` and `__type` from a [`TypeSystem`].
//!
//! These are the only fields in this engine whose answer does not come from
//! Postgres. Everything else lowers to SQL; introspection is resolved here, in
//! memory, and the JSON is carried into the statement as a bound parameter so a
//! document that mixes introspection with data still runs as one request.
//!
//! The work is a small resolver over the meta-schema — `__Schema`, `__Type`,
//! `__Field`, `__InputValue`, `__EnumValue` — driven by the client's selection
//! set. It has to be selection-driven rather than a fixed payload because the
//! introspection query GraphiQL sends walks `ofType` seven levels deep, spreads
//! named fragments, and asks for a different subset than `graphql-codegen` does.
//!
//! # Off unless asked for
//!
//! Introspection publishes the whole data model — every table, column, type and
//! relation — to anyone who can reach the endpoint. That is a different
//! exposure than answering data queries, so it is not turned on by upgrading:
//! see [`SchemaBuilder::enable_introspection`](crate::schema::SchemaBuilder::enable_introspection).
//!
//! # Directives
//!
//! The directive list is empty, and it is honest: this engine implements no
//! directives, and the lowering rejects a document that carries one rather than
//! ignoring it. Publishing `@include`/`@skip` here would tell a client it may
//! send something that would then silently not happen.

use crate::error::{Error, Result};
use crate::parser::Fragments;
use crate::type_system::{Field as TsField, InputValue, TypeDef, TypeRef, TypeSystem};
use async_graphql_parser::types::{Selection, SelectionSet};
use serde_json::{Map, Value};

/// A `__Type` value: a named type, or one of the wrappers around it.
#[derive(Clone, Copy)]
enum MetaType<'a> {
    Named(&'a TypeDef),
    NonNull(&'a TypeRef),
    List(&'a TypeRef),
}

/// Resolve a `__schema` field's selection set.
pub fn resolve_schema(
    set: &SelectionSet,
    ts: &TypeSystem,
    fragments: &Fragments<'_>,
) -> Result<Value> {
    let mut out = Map::new();
    for (alias, field) in flatten(set, fragments)? {
        let name = field.name.node.as_str();
        let value = match name {
            "__typename" => Value::String("__Schema".into()),
            "description" => Value::Null,
            "types" => Value::Array(
                ts.types()
                    .map(|t| {
                        resolve_type(MetaType::Named(t), &field.selection_set.node, ts, fragments)
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            "queryType" => named_type(ts.query_root(), &field.selection_set.node, ts, fragments)?,
            "mutationType" => match ts.mutation_root() {
                Some(m) => named_type(m, &field.selection_set.node, ts, fragments)?,
                None => Value::Null,
            },
            // No subscriptions, and no directives — see the module docs.
            "subscriptionType" => Value::Null,
            "directives" => Value::Array(Vec::new()),
            other => {
                return Err(Error::Validate {
                    path: format!("__schema.{other}"),
                    message: format!("unknown field '{other}' on __Schema"),
                })
            }
        };
        insert_field(&mut out, alias, value)?;
    }
    Ok(Value::Object(out))
}

/// Resolve a `__type(name: "…")` field's selection set. `None` for a name the
/// schema does not define, which is a null answer rather than an error — that is
/// what the spec asks for, and how a client tests whether a type exists.
pub fn resolve_type_by_name(
    type_name: &str,
    set: &SelectionSet,
    ts: &TypeSystem,
    fragments: &Fragments<'_>,
) -> Result<Value> {
    match ts.get(type_name) {
        Some(def) => resolve_type(MetaType::Named(def), set, ts, fragments),
        None => Ok(Value::Null),
    }
}

fn named_type(
    name: &str,
    set: &SelectionSet,
    ts: &TypeSystem,
    fragments: &Fragments<'_>,
) -> Result<Value> {
    match ts.get(name) {
        Some(def) => resolve_type(MetaType::Named(def), set, ts, fragments),
        None => Ok(Value::Null),
    }
}

fn resolve_type(
    ty: MetaType<'_>,
    set: &SelectionSet,
    ts: &TypeSystem,
    fragments: &Fragments<'_>,
) -> Result<Value> {
    let mut out = Map::new();
    for (alias, field) in flatten(set, fragments)? {
        let name = field.name.node.as_str();
        let inner = &field.selection_set.node;
        let value = match (name, ty) {
            ("__typename", _) => Value::String("__Type".into()),

            ("kind", MetaType::Named(def)) => Value::String(def.kind().into()),
            ("kind", MetaType::NonNull(_)) => Value::String("NON_NULL".into()),
            ("kind", MetaType::List(_)) => Value::String("LIST".into()),

            // A wrapper has no name and no description of its own; that is how a
            // client knows to follow `ofType`.
            ("name", MetaType::Named(def)) => Value::String(def.name().into()),
            ("name", _) => Value::Null,
            ("description", MetaType::Named(def)) => match def.description() {
                Some(d) => Value::String(d.into()),
                None => Value::Null,
            },
            ("description", _) => Value::Null,

            ("fields", MetaType::Named(TypeDef::Object { fields, .. })) => Value::Array(
                fields
                    .iter()
                    .map(|f| resolve_field(f, inner, ts, fragments))
                    .collect::<Result<Vec<_>>>()?,
            ),
            ("fields", _) => Value::Null,

            ("inputFields", MetaType::Named(TypeDef::InputObject { fields, .. })) => Value::Array(
                fields
                    .iter()
                    .map(|f| resolve_input_value(f, inner, ts, fragments))
                    .collect::<Result<Vec<_>>>()?,
            ),
            ("inputFields", _) => Value::Null,

            ("enumValues", MetaType::Named(TypeDef::Enum { values, .. })) => Value::Array(
                values
                    .iter()
                    .map(|v| resolve_enum_value(v, inner, fragments))
                    .collect::<Result<Vec<_>>>()?,
            ),
            ("enumValues", _) => Value::Null,

            ("ofType", MetaType::NonNull(inner_ref) | MetaType::List(inner_ref)) => {
                resolve_type_ref(inner_ref, inner, ts, fragments)?
            }
            ("ofType", MetaType::Named(_)) => Value::Null,

            // No interfaces and no unions in this API: every object is concrete.
            ("interfaces", MetaType::Named(TypeDef::Object { .. })) => Value::Array(Vec::new()),
            ("interfaces", _) => Value::Null,
            ("possibleTypes", _) => Value::Null,
            ("specifiedByURL" | "specifiedByUrl", _) => Value::Null,
            ("isOneOf", _) => Value::Null,

            (other, _) => {
                return Err(Error::Validate {
                    path: format!("__type.{other}"),
                    message: format!("unknown field '{other}' on __Type"),
                })
            }
        };
        insert_field(&mut out, alias, value)?;
    }
    Ok(Value::Object(out))
}

fn resolve_type_ref(
    r: &TypeRef,
    set: &SelectionSet,
    ts: &TypeSystem,
    fragments: &Fragments<'_>,
) -> Result<Value> {
    match r {
        TypeRef::Named(name) => named_type(name, set, ts, fragments),
        TypeRef::NonNull(inner) => resolve_type(MetaType::NonNull(inner), set, ts, fragments),
        TypeRef::List(inner) => resolve_type(MetaType::List(inner), set, ts, fragments),
    }
}

fn resolve_field(
    f: &TsField,
    set: &SelectionSet,
    ts: &TypeSystem,
    fragments: &Fragments<'_>,
) -> Result<Value> {
    let mut out = Map::new();
    for (alias, field) in flatten(set, fragments)? {
        let name = field.name.node.as_str();
        let inner = &field.selection_set.node;
        let value = match name {
            "__typename" => Value::String("__Field".into()),
            "name" => Value::String(f.name.clone()),
            "description" => match &f.description {
                Some(d) => Value::String(d.clone()),
                None => Value::Null,
            },
            "args" => Value::Array(
                f.args
                    .iter()
                    .map(|a| resolve_input_value(a, inner, ts, fragments))
                    .collect::<Result<Vec<_>>>()?,
            ),
            "type" => resolve_type_ref(&f.ty, inner, ts, fragments)?,
            "isDeprecated" => Value::Bool(false),
            "deprecationReason" => Value::Null,
            other => {
                return Err(Error::Validate {
                    path: format!("__Field.{other}"),
                    message: format!("unknown field '{other}' on __Field"),
                })
            }
        };
        insert_field(&mut out, alias, value)?;
    }
    Ok(Value::Object(out))
}

fn resolve_input_value(
    v: &InputValue,
    set: &SelectionSet,
    ts: &TypeSystem,
    fragments: &Fragments<'_>,
) -> Result<Value> {
    let mut out = Map::new();
    for (alias, field) in flatten(set, fragments)? {
        let name = field.name.node.as_str();
        let value = match name {
            "__typename" => Value::String("__InputValue".into()),
            "name" => Value::String(v.name.clone()),
            "description" => match &v.description {
                Some(d) => Value::String(d.clone()),
                None => Value::Null,
            },
            "type" => resolve_type_ref(&v.ty, &field.selection_set.node, ts, fragments)?,
            "defaultValue" => match &v.default_value {
                Some(d) => Value::String(d.clone()),
                None => Value::Null,
            },
            "isDeprecated" => Value::Bool(false),
            "deprecationReason" => Value::Null,
            other => {
                return Err(Error::Validate {
                    path: format!("__InputValue.{other}"),
                    message: format!("unknown field '{other}' on __InputValue"),
                })
            }
        };
        insert_field(&mut out, alias, value)?;
    }
    Ok(Value::Object(out))
}

fn resolve_enum_value(v: &str, set: &SelectionSet, fragments: &Fragments<'_>) -> Result<Value> {
    let mut out = Map::new();
    for (alias, field) in flatten(set, fragments)? {
        let name = field.name.node.as_str();
        let value = match name {
            "__typename" => Value::String("__EnumValue".into()),
            "name" => Value::String(v.to_string()),
            "description" => Value::Null,
            "isDeprecated" => Value::Bool(false),
            "deprecationReason" => Value::Null,
            other => {
                return Err(Error::Validate {
                    path: format!("__EnumValue.{other}"),
                    message: format!("unknown field '{other}' on __EnumValue"),
                })
            }
        };
        insert_field(&mut out, alias, value)?;
    }
    Ok(Value::Object(out))
}

/// Selection set to `(response key, field)` pairs, with fragments spread in.
///
/// The standard introspection query is written with a named fragment
/// (`...FullType`) and inline fragments, so flattening them is not optional.
/// Type conditions are not checked: a fragment reaching a meta-object here was
/// written for it, and the alternative is duplicating the meta-schema's own type
/// graph to test against.
fn flatten<'a>(
    set: &'a SelectionSet,
    fragments: &Fragments<'a>,
) -> Result<Vec<(String, &'a async_graphql_parser::types::Field)>> {
    let mut out = Vec::new();
    for sel in &set.items {
        match &sel.node {
            Selection::Field(f) => {
                let field = &f.node;
                let alias = field
                    .alias
                    .as_ref()
                    .map(|a| a.node.as_str().to_string())
                    .unwrap_or_else(|| field.name.node.as_str().to_string());
                out.push((alias, field));
            }
            Selection::FragmentSpread(fs) => {
                let name = fs.node.fragment_name.node.as_str();
                let frag = fragments.get(name).ok_or_else(|| Error::Validate {
                    path: "__schema".into(),
                    message: format!("unknown fragment '{name}'"),
                })?;
                out.extend(flatten(&frag.selection_set.node, fragments)?);
            }
            Selection::InlineFragment(inline) => {
                out.extend(flatten(&inline.node.selection_set.node, fragments)?);
            }
        }
    }
    Ok(out)
}

/// Add one resolved field to the answer.
///
/// A key appearing twice is ordinary — a fragment that repeats `name` — and the
/// two answers are then identical, so one of them can go. Two *different*
/// answers under one key is a conflict, and dropping the loser silently is the
/// failure the data path already refuses (see
/// [`merge_fields`](crate::parser)); it is refused here for the same reason.
fn insert_field(out: &mut Map<String, Value>, alias: String, value: Value) -> Result<()> {
    match out.get(&alias) {
        Some(existing) if *existing != value => Err(Error::Validate {
            path: alias.clone(),
            message: format!(
                "two fields both answer to '{alias}' but ask for different things; \
                 give one of them an alias"
            ),
        }),
        _ => {
            out.insert(alias, value);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_document;
    use crate::schema::{PgType, Relation, Schema, Table};
    use async_graphql_parser::types::{DocumentOperations, Selection};
    use std::collections::HashMap;

    fn schema() -> Schema {
        Schema::builder()
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
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build()
    }

    /// Run an introspection document against the schema, returning the value of
    /// its single root field.
    fn run(source: &str) -> Value {
        let schema = schema();
        let ts = TypeSystem::build(&schema);
        let doc = parse_document(source).unwrap();
        let mut fragments: Fragments<'_> = HashMap::new();
        for (name, def) in &doc.fragments {
            fragments.insert(name.as_str().to_string(), &def.node);
        }
        let DocumentOperations::Single(op) = &doc.operations else {
            panic!("expected one operation");
        };
        let Selection::Field(root) = &op.node.selection_set.node.items[0].node else {
            panic!("expected a field");
        };
        match root.node.name.node.as_str() {
            "__schema" => resolve_schema(&root.node.selection_set.node, &ts, &fragments).unwrap(),
            "__type" => {
                resolve_type_by_name("users", &root.node.selection_set.node, &ts, &fragments)
                    .unwrap()
            }
            other => panic!("unexpected root {other}"),
        }
    }

    #[test]
    fn schema_answers_root_types_and_type_list() {
        let v =
            run("{ __schema { queryType { name } mutationType { name } types { name kind } } }");
        assert_eq!(v["queryType"]["name"], "query_root");
        assert_eq!(v["mutationType"]["name"], "mutation_root");
        let names: Vec<&str> = v["types"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"users_bool_exp"));
        assert!(names.contains(&"order_by"));
        // Built-in scalars are part of the schema a client resolves against.
        assert!(names.contains(&"Int"), "{names:?}");
        assert!(names.contains(&"Boolean"), "{names:?}");
    }

    #[test]
    fn wrappers_are_walked_through_of_type() {
        let v = run(
            "{ __type(name: \"users\") { fields { name type { kind name ofType { kind name \
             ofType { kind name ofType { kind name } } } } } } }",
        );
        let fields = v["fields"].as_array().unwrap();
        let id = fields.iter().find(|f| f["name"] == "id").unwrap();
        // Int! — NON_NULL wrapping a named scalar.
        assert_eq!(id["type"]["kind"], "NON_NULL");
        assert_eq!(id["type"]["name"], Value::Null);
        assert_eq!(id["type"]["ofType"]["name"], "Int");

        // [posts!]! — three wrappers deep.
        let posts = fields.iter().find(|f| f["name"] == "posts").unwrap();
        assert_eq!(posts["type"]["kind"], "NON_NULL");
        assert_eq!(posts["type"]["ofType"]["kind"], "LIST");
        assert_eq!(posts["type"]["ofType"]["ofType"]["kind"], "NON_NULL");
        assert_eq!(posts["type"]["ofType"]["ofType"]["ofType"]["name"], "posts");
    }

    #[test]
    fn fragments_are_spread_the_way_the_standard_query_writes_them() {
        let v = run("fragment F on __Type { name kind } \
             { __schema { types { ...F } queryType { ...F } } }");
        assert_eq!(v["queryType"]["name"], "query_root");
        assert_eq!(v["queryType"]["kind"], "OBJECT");
        assert!(v["types"][0]["kind"].is_string());
    }

    #[test]
    fn aliases_and_typename_are_honoured() {
        let v = run("{ __schema { __typename q: queryType { n: name __typename } } }");
        assert_eq!(v["__typename"], "__Schema");
        assert_eq!(v["q"]["n"], "query_root");
        assert_eq!(v["q"]["__typename"], "__Type");
    }

    #[test]
    fn input_fields_and_enum_values_answer_on_the_right_kinds() {
        let v = run("{ __type(name: \"users\") { inputFields { name } enumValues { name } } }");
        // `users` is an object: both are null, not empty lists.
        assert_eq!(v["inputFields"], Value::Null);
        assert_eq!(v["enumValues"], Value::Null);
    }

    #[test]
    fn args_carry_their_types() {
        let v = run(
            "{ __type(name: \"users\") { fields { name args { name type { kind ofType { name } \
             name } } } } }",
        );
        let fields = v["fields"].as_array().unwrap();
        let posts = fields.iter().find(|f| f["name"] == "posts").unwrap();
        let args = posts["args"].as_array().unwrap();
        let where_arg = args.iter().find(|a| a["name"] == "where").unwrap();
        assert_eq!(where_arg["type"]["name"], "posts_bool_exp");
        let limit = args.iter().find(|a| a["name"] == "limit").unwrap();
        assert_eq!(limit["type"]["name"], "Int");
    }

    #[test]
    fn an_unknown_type_is_null_and_an_unknown_meta_field_is_an_error() {
        let schema = schema();
        let ts = TypeSystem::build(&schema);
        let fragments: Fragments<'_> = HashMap::new();
        let doc = parse_document("{ __type(name: \"nope\") { name } }").unwrap();
        let DocumentOperations::Single(op) = &doc.operations else {
            panic!()
        };
        let Selection::Field(root) = &op.node.selection_set.node.items[0].node else {
            panic!()
        };
        assert_eq!(
            resolve_type_by_name("nope", &root.node.selection_set.node, &ts, &fragments).unwrap(),
            Value::Null
        );

        let doc = parse_document("{ __schema { nonsense } }").unwrap();
        let DocumentOperations::Single(op) = &doc.operations else {
            panic!()
        };
        let Selection::Field(root) = &op.node.selection_set.node.items[0].node else {
            panic!()
        };
        let err = resolve_schema(&root.node.selection_set.node, &ts, &fragments).unwrap_err();
        assert!(
            format!("{err}").contains("unknown field 'nonsense'"),
            "{err}"
        );
    }

    #[test]
    fn conflicting_repeats_under_one_key_are_rejected_not_dropped() {
        // Keeping the first and discarding the rest answered with only `name`,
        // silently. The data path calls this a conflict; so does this one.
        let schema = schema();
        let ts = TypeSystem::build(&schema);
        let fragments: Fragments<'_> = HashMap::new();
        let doc = parse_document("{ __type(name: \"users\") { fields { name } fields { name } } }")
            .unwrap();
        let DocumentOperations::Single(op) = &doc.operations else {
            panic!()
        };
        let Selection::Field(root) = &op.node.selection_set.node.items[0].node else {
            panic!()
        };
        // Identical repeats are fine — that is what a fragment spread produces.
        resolve_type_by_name("users", &root.node.selection_set.node, &ts, &fragments).unwrap();

        let doc = parse_document(
            "{ __type(name: \"users\") { fields { name } fields { type { name } } } }",
        )
        .unwrap();
        let DocumentOperations::Single(op) = &doc.operations else {
            panic!()
        };
        let Selection::Field(root) = &op.node.selection_set.node.items[0].node else {
            panic!()
        };
        let err = resolve_type_by_name("users", &root.node.selection_set.node, &ts, &fragments)
            .unwrap_err();
        assert!(
            format!("{err}").contains("both answer to 'fields'"),
            "{err}"
        );
    }

    #[test]
    fn directives_are_empty_because_none_are_implemented() {
        let v = run("{ __schema { directives { name } } }");
        assert_eq!(v["directives"], Value::Array(Vec::new()));
    }
}
