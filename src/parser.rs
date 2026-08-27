//! GraphQL string → IR.

use crate::ast::{
    BoolExpr, CmpOp, Count, Field, NullsOrder, Operation, OrderBy, OrderDir, QueryArgs, RootField,
    Val,
};
use crate::error::{Error, Result};
use crate::limits::ParseLimits;
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

/// Fragment definitions by name, as gathered from the document. Public so the
/// introspection resolver can spread the same fragments the data path does.
pub type Fragments<'a> = HashMap<String, &'a FragmentDefinition>;

#[tracing::instrument(level = "trace", skip_all)]
pub fn parse_and_lower(
    source: &str,
    variables: &Value,
    operation_name: Option<&str>,
    schema: &Schema,
) -> Result<Operation> {
    let doc = parse_document(source)?;
    lower(&doc, variables, operation_name, schema)
}

/// Parse GraphQL text into a document, under [`ParseLimits::default`].
/// Depends on nothing but the text, which is what makes it cacheable — see
/// [`crate::parse_cache`].
///
/// Every path into the parser goes through here, which is what makes this the
/// place the pre-parse guard lives: an over-deep document must be rejected
/// before `parse_query` sees it, because the overflow it causes aborts the
/// process rather than unwinding. See [`crate::limits`].
pub fn parse_document(source: &str) -> Result<ExecutableDocument> {
    parse_document_with(source, &ParseLimits::default())
}

/// [`parse_document`] under explicit limits.
pub fn parse_document_with(source: &str, limits: &ParseLimits) -> Result<ExecutableDocument> {
    limits.check(source)?;
    parse_query(source).map_err(|e| Error::Parse(e.to_string()))
}

/// Lower an already-parsed document to the IR.
///
/// Split out from [`parse_and_lower`] so a parsed document can be reused
/// across requests: parsing depends only on the source text, while lowering
/// depends on this request's `variables`. See [`crate::parse_cache`].
#[tracing::instrument(level = "trace", skip_all)]
pub fn lower(
    doc: &ExecutableDocument,
    variables: &Value,
    operation_name: Option<&str>,
    schema: &Schema,
) -> Result<Operation> {
    lower_with(doc, Bindings::Eager(variables), operation_name, schema)
}

/// Lower under an explicit [`Bindings`] mode. `Symbolic` keeps variables
/// unresolved so the result can be rendered once and reused.
#[tracing::instrument(level = "trace", skip_all)]
pub fn lower_with(
    doc: &ExecutableDocument,
    variables: Bindings<'_>,
    operation_name: Option<&str>,
    schema: &Schema,
) -> Result<Operation> {
    let mut fragments: Fragments<'_> = HashMap::new();
    for (name, def) in &doc.fragments {
        fragments.insert(name.as_str().to_string(), &def.node);
    }
    validate_fragments(&fragments)?;
    let op = pick_operation(doc, operation_name)?;
    reject_directives(op, &fragments)?;

    // `query($n: Int = 10)` means the request may leave `$n` out. Filling the
    // defaults in here, once, is what keeps every variable position below from
    // having to know about them — and an explicitly passed null still wins,
    // since only *missing* names are filled.
    let defaults = collect_defaults(op.variable_definitions)?;
    let merged;
    let variables = match (&variables, defaults.is_empty()) {
        (Bindings::Eager(vars), false) => {
            merged = with_defaults(vars, &defaults);
            Bindings::Eager(&merged)
        }
        _ => variables,
    };

    match op.ty {
        OperationType::Query => lower_query(op.selection_set, schema, variables, &fragments),
        OperationType::Mutation => lower_mutation(op.selection_set, schema, variables, &fragments),
        OperationType::Subscription => Err(Error::Parse("subscriptions are not supported".into())),
    }
}

/// Default values declared by an operation's variable definitions.
///
/// Public because the compiled path cannot apply them while lowering — it does
/// not have the request's variables yet — so [`crate::CompiledQuery`] carries
/// them and applies them at execute time. See [`crate::types::Inputs`].
pub fn variable_defaults(
    doc: &ExecutableDocument,
    operation_name: Option<&str>,
) -> Result<serde_json::Map<String, Value>> {
    collect_defaults(pick_operation(doc, operation_name)?.variable_definitions)
}

fn collect_defaults(
    defs: &[Positioned<async_graphql_parser::types::VariableDefinition>],
) -> Result<serde_json::Map<String, Value>> {
    let mut out = serde_json::Map::new();
    for def in defs {
        let Some(default) = def.node.default_value.as_ref() else {
            continue;
        };
        let name = def.node.name.node.as_str();
        let json = default
            .node
            .clone()
            .into_json()
            .map_err(|e| Error::Variable {
                name: name.to_string(),
                message: format!("default value is not representable as JSON: {e}"),
            })?;
        out.insert(name.to_string(), json);
    }
    Ok(out)
}

/// `vars` with any name it does not carry taken from `defaults`.
fn with_defaults(vars: &Value, defaults: &serde_json::Map<String, Value>) -> Value {
    let mut out = match vars {
        Value::Object(map) => map.clone(),
        _ => serde_json::Map::new(),
    };
    for (name, value) in defaults {
        out.entry(name.clone()).or_insert_with(|| value.clone());
    }
    Value::Object(out)
}

#[derive(Clone, Copy)]
struct OpInfo<'a> {
    ty: OperationType,
    selection_set: &'a SelectionSet,
    variable_definitions: &'a [Positioned<async_graphql_parser::types::VariableDefinition>],
    directives: &'a [Positioned<async_graphql_parser::types::Directive>],
}

fn pick_operation<'a>(doc: &'a ExecutableDocument, name: Option<&str>) -> Result<OpInfo<'a>> {
    match (&doc.operations, name) {
        // A name that matches nothing is an error even when the document holds
        // exactly one operation. Running it anyway would answer a question about
        // the operation that happens to be there when the caller asked about a
        // different one — the same silent substitution the multi-operation case
        // already refuses, and the spec says so too.
        (DocumentOperations::Single(op), Some(wanted))
            if !doc
                .operations
                .iter()
                .any(|(name, _)| name.map(|n| n.as_str()) == Some(wanted)) =>
        {
            Err(Error::Parse(format!("operation '{wanted}' not found")))
        }
        (DocumentOperations::Single(op), _) => Ok(OpInfo {
            ty: op.node.ty,
            selection_set: &op.node.selection_set.node,
            variable_definitions: &op.node.variable_definitions,
            directives: &op.node.directives,
        }),
        (DocumentOperations::Multiple(ops), Some(n)) => {
            let key = Name::new(n);
            let op = ops
                .get(&key)
                .ok_or_else(|| Error::Parse(format!("operation '{n}' not found")))?;
            Ok(OpInfo {
                ty: op.node.ty,
                selection_set: &op.node.selection_set.node,
                variable_definitions: &op.node.variable_definitions,
                directives: &op.node.directives,
            })
        }
        (DocumentOperations::Multiple(ops), None) => {
            if ops.len() == 1 {
                let (_, op) = ops.iter().next().unwrap();
                Ok(OpInfo {
                    ty: op.node.ty,
                    selection_set: &op.node.selection_set.node,
                    variable_definitions: &op.node.variable_definitions,
                    directives: &op.node.directives,
                })
            } else {
                Err(Error::Parse(
                    "document has multiple operations; operation_name required".into(),
                ))
            }
        }
    }
}

/// Every argument on a `_by_pk` field must name a primary key column.
///
/// The loop that reads the PK looks up the columns it wants and never sees the
/// rest, so without this an argument that is not part of the key — a typo, or a
/// `where` the caller believed was filtering — is accepted and silently
/// dropped. Every other argument position in this file rejects what it does not
/// know; this one has to as well.
fn reject_non_pk_arguments(
    args: &[(Positioned<Name>, Positioned<GqlValue>)],
    table: &Table,
    path: &str,
) -> Result<()> {
    for (name_p, _) in args {
        let name = name_p.node.as_str();
        if !table.primary_key.iter().any(|pk| pk == name) {
            return Err(Error::Validate {
                path: format!("{path}.{name}"),
                message: format!(
                    "unknown argument '{name}'; '{}' takes only its primary key ({})",
                    table.exposed_name,
                    table.primary_key.join(", ")
                ),
            });
        }
    }
    Ok(())
}

/// The introspection field every GraphQL client may add to any selection set.
/// Apollo and urql add it to *all* of them by default, which is why it has to be
/// accepted everywhere a selection set is lowered rather than only at the top.
pub(crate) const TYPENAME: &str = "__typename";

/// `__typename` takes no arguments. Rejecting them keeps it consistent with
/// every other field position rather than quietly ignoring what was written.
fn reject_typename_arguments(
    args: &[(Positioned<Name>, Positioned<GqlValue>)],
    alias: &str,
    parent_path: &str,
) -> Result<()> {
    if let Some((first, _)) = args.first() {
        return Err(Error::Validate {
            path: format!("{parent_path}.{alias}.{}", first.node.as_str()),
            message: "'__typename' takes no arguments".into(),
        });
    }
    Ok(())
}

/// Collapse fields that answer to the same response key.
///
/// Two fields with one key is not an error by itself — spreading a fragment
/// that repeats a column is ordinary, and the GraphQL spec says such fields
/// merge. What is an error is two fields that answer to one key and ask for
/// *different* things: only one of them can survive into `json_build_object`,
/// and until this existed the loser vanished with no word to the caller.
///
/// Identical scalar reads collapse to one. Relations collapse only when neither
/// carries arguments, since `posts(limit: 1)` and `posts` under one key have no
/// single answer — that is a conflict, and the fix is an alias.
fn merge_fields(fields: Vec<Field>, parent_path: &str) -> Result<Vec<Field>> {
    /// Whether two `nodes` selections ask for the same columns in the same
    /// order. Enough for the case this exists for — the same fragment spread
    /// twice — and anything less alike is a conflict, which is the safe way to
    /// be wrong.
    fn nodes_match(a: Option<&[Field]>, b: Option<&[Field]>) -> bool {
        match (a, b) {
            (None, None) => true,
            (Some(a), Some(b)) => {
                a.len() == b.len()
                    && a.iter().zip(b).all(|(x, y)| match (x, y) {
                        (
                            Field::Column {
                                column: xc,
                                alias: xa,
                            },
                            Field::Column {
                                column: yc,
                                alias: ya,
                            },
                        ) => xc == yc && xa == ya,
                        (Field::Typename { alias: xa }, Field::Typename { alias: ya }) => xa == ya,
                        _ => false,
                    })
            }
            _ => false,
        }
    }

    fn key_of(f: &Field) -> &str {
        match f {
            Field::Column { alias, .. }
            | Field::JsonPath { alias, .. }
            | Field::Typename { alias }
            | Field::Relation { alias, .. }
            | Field::RelationAggregate { alias, .. } => alias,
        }
    }

    fn conflict(alias: &str, parent_path: &str) -> Error {
        Error::Validate {
            path: format!("{parent_path}.{alias}"),
            message: format!(
                "two fields both answer to '{alias}' but ask for different things; \
                 give one of them an alias"
            ),
        }
    }

    let mut out: Vec<Field> = Vec::with_capacity(fields.len());
    for field in fields {
        let Some(pos) = out.iter().position(|e| key_of(e) == key_of(&field)) else {
            out.push(field);
            continue;
        };
        let alias = key_of(&field).to_string();
        match (&mut out[pos], field) {
            (Field::Typename { .. }, Field::Typename { .. }) => {}
            // A fragment spread twice is ordinary, and two aggregates asking
            // the same thing are the same request — but only then: differing
            // functions under one key have no single answer, as with relations.
            (
                Field::RelationAggregate {
                    name: a,
                    args: aargs,
                    ops: aops,
                    nodes: anodes,
                    typenames: atn,
                    ..
                },
                Field::RelationAggregate {
                    name: ref b,
                    args: bargs,
                    ops: ref bops,
                    nodes: ref bnodes,
                    typenames: ref btn,
                    ..
                },
            ) if a == b
                && aargs.is_empty()
                && bargs.is_empty()
                && aops == bops
                && atn == btn
                && nodes_match(anodes.as_deref(), bnodes.as_deref()) => {}
            (Field::Column { column: a, .. }, Field::Column { column: ref b, .. }) if a == b => {}
            (
                Field::JsonPath {
                    column: a,
                    path: pa,
                    ..
                },
                Field::JsonPath {
                    column: ref b,
                    path: ref pb,
                    ..
                },
            ) if a == b && pa == pb => {}
            (
                Field::Relation {
                    name: a,
                    args: aargs,
                    selection: asel,
                    ..
                },
                Field::Relation {
                    name: ref b,
                    args: bargs,
                    selection: bsel,
                    ..
                },
            ) if a == b && aargs.is_empty() && bargs.is_empty() => {
                asel.extend(bsel);
                let merged = std::mem::take(asel);
                *asel = merge_fields(merged, &format!("{parent_path}.{alias}"))?;
            }
            _ => return Err(conflict(&alias, parent_path)),
        }
    }
    Ok(out)
}

/// Every response key in one operation must be distinct.
///
/// Root fields are not merged the way selection-set fields are: two roots under
/// one key differ in what they select, what they filter and often in kind
/// (`users` vs `users_aggregate` aliased alike), and there is no defensible way
/// to fold that into one. Rendering both is what used to happen, and the second
/// silently overwrote the first in the result object.
fn ensure_unique_root_aliases<'a>(aliases: impl Iterator<Item = &'a str>) -> Result<()> {
    let mut seen: Vec<&str> = Vec::new();
    for alias in aliases {
        if seen.contains(&alias) {
            return Err(Error::Validate {
                path: alias.to_string(),
                message: format!(
                    "two root fields both answer to '{alias}'; give one of them an alias"
                ),
            });
        }
        seen.push(alias);
    }
    Ok(())
}

/// Answer `__schema` / `__type` now, from the schema's type system.
///
/// It happens here, in lowering, because this is where the document's fragments
/// and variables are — and because the answer then travels as an ordinary value
/// in the IR, which every path downstream already knows how to carry.
fn lower_introspection(
    name: &str,
    field: &async_graphql_parser::types::Field,
    schema: &Schema,
    vars: Bindings<'_>,
    fragments: &Fragments<'_>,
) -> Result<Value> {
    if !schema.introspection_enabled() {
        return Err(Error::Validate {
            path: name.to_string(),
            message: "introspection is disabled; enable it with \
                      Schema::builder().enable_introspection()"
                .into(),
        });
    }
    let ts = schema.type_system();
    if name == "__schema" {
        return crate::introspection::resolve_schema(&field.selection_set.node, ts, fragments);
    }

    let mut wanted: Option<String> = None;
    for (arg_name, value) in &field.arguments {
        match arg_name.node.as_str() {
            "name" => {
                let json = gql_to_json(&value.node, vars, "__type.name")?;
                wanted = Some(
                    json.as_str()
                        .ok_or_else(|| Error::Validate {
                            path: "__type.name".into(),
                            message: format!("expected a string, got {json}"),
                        })?
                        .to_string(),
                );
            }
            other => {
                return Err(Error::Validate {
                    path: format!("__type.{other}"),
                    message: format!("unknown argument '{other}' on '__type'"),
                })
            }
        }
    }
    let wanted = wanted.ok_or_else(|| Error::Validate {
        path: "__type".into(),
        message: "missing required argument 'name'".into(),
    })?;
    crate::introspection::resolve_type_by_name(&wanted, &field.selection_set.node, ts, fragments)
}

/// Refuse a document carrying directives.
///
/// This engine implements none — not `@include`, not `@skip` — and until this
/// check existed they were simply not looked at, so `field @include(if: false)`
/// came back included. A directive that silently does not happen is the worst of
/// the three options; erroring is the honest one, and it is why the introspection
/// answer publishes an empty directive list.
///
/// Every position the grammar allows one is checked: the operation, its variable
/// definitions, the fragment definitions, and every selection. Checking only
/// selections would leave `query Q @skip(if: true)` accepted and inert, which is
/// the same failure one level up.
fn reject_directives(op: OpInfo<'_>, fragments: &Fragments<'_>) -> Result<()> {
    fn refuse(
        directives: &[Positioned<async_graphql_parser::types::Directive>],
        where_: &str,
    ) -> Result<()> {
        if let Some(d) = directives.first() {
            let name = d.node.name.node.as_str();
            return Err(Error::Validate {
                path: format!("@{name}"),
                message: format!(
                    "directives are not supported; '@{name}' on {where_} would have no effect"
                ),
            });
        }
        Ok(())
    }

    fn walk(set: &SelectionSet) -> Result<()> {
        for sel in &set.items {
            match &sel.node {
                Selection::Field(f) => {
                    refuse(&f.node.directives, "a field")?;
                    walk(&f.node.selection_set.node)?;
                }
                Selection::InlineFragment(f) => {
                    refuse(&f.node.directives, "an inline fragment")?;
                    walk(&f.node.selection_set.node)?;
                }
                Selection::FragmentSpread(f) => refuse(&f.node.directives, "a fragment spread")?,
            }
        }
        Ok(())
    }

    refuse(op.directives, "an operation")?;
    for def in op.variable_definitions {
        refuse(&def.node.directives, "a variable definition")?;
    }
    // Fragment bodies are walked here rather than followed from the spreads, so
    // a fragment the operation does not use is still rejected — and the walk
    // needs no cycle guard, since it never follows a spread.
    for frag in fragments.values() {
        refuse(&frag.directives, "a fragment definition")?;
        walk(&frag.selection_set.node)?;
    }
    walk(op.selection_set)
}

/// Drop repeats anywhere in the list, not just adjacent ones.
///
/// `Vec::dedup` only collapses neighbours, so `__typename t: __typename
/// __typename` kept two entries under one key and wrote it twice into the same
/// `json_build_object`.
fn dedup_keys(keys: &mut Vec<String>) {
    let mut seen: Vec<String> = Vec::new();
    keys.retain(|k| {
        if seen.contains(k) {
            false
        } else {
            seen.push(k.clone());
            true
        }
    });
}

fn validate_fragments(fragments: &Fragments<'_>) -> Result<()> {
    /// Longest chain allowed. Tied to the text-nesting limit because it bounds
    /// the same thing — how deep the walkers below can recurse — just reached by
    /// a different route.
    const MAX_CHAIN: usize = crate::limits::DEFAULT_MAX_DEPTH;

    fn spreads_of<'a>(set: &'a SelectionSet, out: &mut Vec<&'a str>) {
        for sel in &set.items {
            match &sel.node {
                Selection::FragmentSpread(f) => out.push(f.node.fragment_name.node.as_str()),
                Selection::Field(f) => spreads_of(&f.node.selection_set.node, out),
                Selection::InlineFragment(f) => spreads_of(&f.node.selection_set.node, out),
            }
        }
    }

    /// Depth of the longest chain starting at `name`, or an error if `name` is
    /// reachable from itself. `path` is the chain currently being walked, which
    /// is what distinguishes a cycle from a fragment merely spread twice.
    fn depth<'a>(
        name: &'a str,
        fragments: &'a Fragments<'a>,
        path: &mut Vec<&'a str>,
        done: &mut std::collections::HashMap<&'a str, usize>,
    ) -> Result<usize> {
        if let Some(d) = done.get(name) {
            return Ok(*d);
        }
        if path.contains(&name) {
            path.push(name);
            return Err(Error::Validate {
                path: format!("fragment {name}"),
                message: format!("fragment cycle: {}", path.join(" -> ")),
            });
        }
        // An unknown spread is reported where it is used, with the field path.
        let Some((key, frag)) = fragments.get_key_value(name) else {
            return Ok(0);
        };
        path.push(key.as_str());
        let mut spreads = Vec::new();
        spreads_of(&frag.selection_set.node, &mut spreads);
        let mut deepest = 0;
        for next in spreads {
            deepest = deepest.max(1 + depth(next, fragments, path, done)?);
        }
        path.pop();
        if deepest > MAX_CHAIN {
            return Err(Error::Validate {
                path: format!("fragment {name}"),
                message: format!("fragment chain is deeper than the limit of {MAX_CHAIN}"),
            });
        }
        done.insert(key.as_str(), deepest);
        Ok(deepest)
    }

    let mut done = std::collections::HashMap::new();
    for name in fragments.keys() {
        depth(name.as_str(), fragments, &mut Vec::new(), &mut done)?;
    }
    Ok(())
}

/// `distinct_on` on an aggregate is refused, not ignored.
///
/// The aggregate's source is built by a different renderer than a row list's,
/// and that one does not emit `DISTINCT ON` — so the argument used to be parsed,
/// column-checked, and then vanish, leaving `count` to answer a question nobody
/// asked. Until the source renders it, saying so is the only honest option, and
/// the type system does not publish the argument either.
fn reject_distinct_on_aggregate(args: &QueryArgs, path: &str) -> Result<()> {
    if args.distinct_on.is_empty() {
        return Ok(());
    }
    Err(Error::Validate {
        path: format!("{path}.distinct_on"),
        message: "an aggregate cannot take 'distinct_on'; \
                  use `count(columns: [\u{2026}], distinct: true)` to count distinct values"
            .into(),
    })
}

fn lower_query(
    set: &SelectionSet,
    schema: &Schema,
    vars: Bindings<'_>,
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
                        reject_distinct_on_aggregate(&args, &alias)?;
                        let AggregateSelection {
                            ops,
                            nodes,
                            typenames,
                        } = lower_aggregate_selection(
                            &field.selection_set.node,
                            table,
                            vars,
                            &alias,
                        )?;
                        roots.push(RootField {
                            table: base_name.to_string(),
                            alias,
                            args,
                            body: crate::ast::RootBody::Aggregate {
                                ops,
                                nodes,
                                typenames,
                                nodes_limit: None,
                            },
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
                        reject_non_pk_arguments(&field.arguments, table, &alias)?;
                        let mut pk: Vec<(String, Val)> = Vec::new();
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
                            let val =
                                gql_to_val(&value_p.node, vars, &format!("{alias}.{pk_col}"))?;
                            pk.push((pk_col.clone(), val));
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

                if name == "__schema" || name == "__type" {
                    roots.push(RootField {
                        table: String::new(),
                        alias,
                        args: QueryArgs::default(),
                        body: crate::ast::RootBody::Introspection(lower_introspection(
                            name, field, schema, vars, fragments,
                        )?),
                    });
                    continue;
                }

                let table = schema.table(name).ok_or_else(|| {
                    if name == TYPENAME {
                        return Error::Validate {
                            path: alias.clone(),
                            message: "'__typename' is supported inside a field's selection set, \
                                      but not as a root field"
                                .into(),
                        };
                    }
                    Error::Validate {
                        path: alias.clone(),
                        message: format!("unknown root field '{name}'"),
                    }
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
    ensure_unique_root_aliases(roots.iter().map(|r| r.alias.as_str()))?;
    Ok(Operation::Query(roots))
}

fn lower_mutation(
    set: &SelectionSet,
    schema: &Schema,
    vars: Bindings<'_>,
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
        let mf = lower_mutation_field(name, &alias, field, schema, vars, fragments)?;
        fields.push(mf);
    }
    ensure_unique_root_aliases(fields.iter().map(crate::ast::MutationField::alias))?;
    Ok(Operation::Mutation(fields))
}

/// Reject a mutation aimed at a read-only table.
///
/// Mutation roots are derived from the exposed table name by prefix, so every
/// table in the schema is writable unless something says otherwise — and
/// introspection puts views in the schema, because they have columns in
/// `information_schema` like any other relation. Postgres auto-updates a *simple*
/// view straight through to its base table, so an unguarded `insert_my_view`
/// does not fail: it writes rows into the table behind the view. This is the
/// check that stops that, and it has to cover nested-insert targets too, which
/// reach a table without ever naming a root field.
fn ensure_mutable(table: &Table, path: &str) -> Result<()> {
    if table.read_only {
        return Err(Error::Validate {
            path: path.to_string(),
            message: format!(
                "table '{}' is read-only; mutations are not available",
                table.exposed_name
            ),
        });
    }
    Ok(())
}

fn lower_mutation_field(
    name: &str,
    alias: &str,
    field: &GqlField,
    schema: &Schema,
    vars: Bindings<'_>,
    fragments: &Fragments<'_>,
) -> Result<crate::ast::MutationField> {
    use crate::ast::MutationField;

    // insert_<table>_one
    if let Some(base) = name.strip_suffix("_one") {
        if let Some(base_name) = base.strip_prefix("insert_") {
            if let Some(table) = schema.table(base_name) {
                ensure_mutable(table, alias)?;
                let (objects, on_conflict) =
                    parse_insert_args(&field.arguments, table, schema, vars, alias, true)?;
                let returning = lower_selection_set(
                    &field.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
                    alias,
                )?;
                return Ok(MutationField::Insert {
                    alias: alias.to_string(),
                    table: base_name.to_string(),
                    objects,
                    on_conflict,
                    returning,
                    // `insert_one` answers with the row, not the
                    // `{affected_rows, returning}` wrapper, so a `__typename`
                    // in its selection set is a row typename and rides along in
                    // `returning`.
                    response_typenames: Vec::new(),
                    one: true,
                    scope_check: None,
                });
            }
        }
    }

    // insert_<table>
    if let Some(base_name) = name.strip_prefix("insert_") {
        if let Some(table) = schema.table(base_name) {
            ensure_mutable(table, alias)?;
            let (objects, on_conflict) =
                parse_insert_args(&field.arguments, table, schema, vars, alias, false)?;
            let returning = parse_returning(
                &field.selection_set.node,
                table,
                schema,
                vars,
                fragments,
                alias,
            )?;
            let (returning, response_typenames) = returning;
            return Ok(MutationField::Insert {
                alias: alias.to_string(),
                table: base_name.to_string(),
                objects,
                on_conflict,
                returning,
                response_typenames,
                one: false,
                scope_check: None,
            });
        }
    }

    // update_<table>_by_pk
    if let Some(base) = name.strip_suffix("_by_pk") {
        if let Some(base_name) = base.strip_prefix("update_") {
            if let Some(table) = schema.table(base_name) {
                ensure_mutable(table, alias)?;
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
                let selection = lower_selection_set(
                    &field.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
                    alias,
                )?;
                return Ok(MutationField::UpdateByPk {
                    alias: alias.to_string(),
                    table: base_name.to_string(),
                    pk,
                    set,
                    selection,
                    scope: None,
                });
            }
        }
    }

    // update_<table>
    if let Some(base_name) = name.strip_prefix("update_") {
        if let Some(table) = schema.table(base_name) {
            ensure_mutable(table, alias)?;
            let (where_, set) = parse_update_args(&field.arguments, table, schema, vars, alias)?;
            let returning = parse_returning(
                &field.selection_set.node,
                table,
                schema,
                vars,
                fragments,
                alias,
            )?;
            let (returning, response_typenames) = returning;
            return Ok(MutationField::Update {
                alias: alias.to_string(),
                table: base_name.to_string(),
                where_,
                set,
                returning,
                response_typenames,
                scope_check: None,
            });
        }
    }

    // delete_<table>_by_pk
    if let Some(base) = name.strip_suffix("_by_pk") {
        if let Some(base_name) = base.strip_prefix("delete_") {
            if let Some(table) = schema.table(base_name) {
                ensure_mutable(table, alias)?;
                if table.primary_key.is_empty() {
                    return Err(Error::Validate {
                        path: alias.into(),
                        message: format!(
                            "table '{}' has no primary key; _by_pk not available",
                            table.exposed_name
                        ),
                    });
                }
                reject_non_pk_arguments(&field.arguments, table, alias)?;
                let mut pk: Vec<(String, Val)> = Vec::new();
                for pk_col in &table.primary_key {
                    let found = field
                        .arguments
                        .iter()
                        .find(|(n, _)| n.node.as_str() == pk_col);
                    let (_, value_p) = found.ok_or_else(|| Error::Validate {
                        path: alias.into(),
                        message: format!("required primary key argument '{pk_col}' missing"),
                    })?;
                    let val = gql_to_val(&value_p.node, vars, &format!("{alias}.{pk_col}"))?;
                    pk.push((pk_col.clone(), val));
                }
                let selection = lower_selection_set(
                    &field.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
                    alias,
                )?;
                return Ok(MutationField::DeleteByPk {
                    alias: alias.to_string(),
                    table: base_name.to_string(),
                    pk,
                    selection,
                    scope: None,
                });
            }
        }
    }

    // delete_<table>
    if let Some(base_name) = name.strip_prefix("delete_") {
        if let Some(table) = schema.table(base_name) {
            ensure_mutable(table, alias)?;
            let mut where_: Option<crate::ast::BoolExpr> = None;
            for (name_p, value_p) in &field.arguments {
                let aname = name_p.node.as_str();
                let v = &value_p.node;
                if aname == "where" {
                    where_ = Some(lower_where(
                        v,
                        table,
                        schema,
                        vars,
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
            let returning = parse_returning(
                &field.selection_set.node,
                table,
                schema,
                vars,
                fragments,
                alias,
            )?;
            let (returning, response_typenames) = returning;
            return Ok(MutationField::Delete {
                alias: alias.to_string(),
                table: base_name.to_string(),
                where_,
                returning,
                response_typenames,
            });
        }
    }

    if name == TYPENAME {
        return Err(Error::Validate {
            path: alias.into(),
            message: "'__typename' is supported inside a field's selection set, but not as a \
                      root field"
                .into(),
        });
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
    vars: Bindings<'_>,
    parent_path: &str,
    single: bool,
) -> Result<(
    Vec<crate::ast::InsertObject>,
    Option<crate::ast::OnConflict>,
)> {
    let mut objects: Vec<crate::ast::InsertObject> = Vec::new();
    let mut on_conflict: Option<crate::ast::OnConflict> = None;

    for (name_p, value_p) in args {
        let aname = name_p.node.as_str();
        let v = &value_p.node;
        match aname {
            "object" if single => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.object"))?;
                let obj =
                    parse_insert_object(&json, table, schema, &format!("{parent_path}.object"))?;
                objects.push(obj);
            }
            "objects" if !single => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.objects"))?;
                let arr = json.as_array().ok_or_else(|| Error::Validate {
                    path: format!("{parent_path}.objects"),
                    message: "expected array".into(),
                })?;
                for (i, item) in arr.iter().enumerate() {
                    let obj = parse_insert_object(
                        item,
                        table,
                        schema,
                        &format!("{parent_path}.objects[{i}]"),
                    )?;
                    objects.push(obj);
                }
            }
            "on_conflict" => {
                let json = gql_to_json(v, vars, &format!("{parent_path}.on_conflict"))?;
                on_conflict = Some(parse_on_conflict(
                    &json,
                    table,
                    schema,
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

    // Batch-uniform rule for nested_objects: every row must have the same
    // set of object-relation keys (either all rows nest a given relation,
    // or none do). Mixed is rejected to keep the renderer's JOIN clean.
    if objects.len() > 1 {
        let first_keys: std::collections::BTreeSet<&str> = objects[0]
            .nested_objects
            .keys()
            .map(|s| s.as_str())
            .collect();
        for (i, obj) in objects.iter().enumerate().skip(1) {
            let these: std::collections::BTreeSet<&str> =
                obj.nested_objects.keys().map(|s| s.as_str()).collect();
            if these != first_keys {
                // Find a specific offender for the error message.
                let missing: Vec<&&str> = first_keys.difference(&these).collect();
                let extra: Vec<&&str> = these.difference(&first_keys).collect();
                let detail = if !missing.is_empty() {
                    format!("row 0 nests '{}' but row {i} does not", missing[0])
                } else {
                    format!("row {i} nests '{}' but row 0 does not", extra[0])
                };
                return Err(Error::Validate {
                    path: format!("{parent_path}.objects"),
                    message: format!(
                        "nested object-relation usage must be uniform across all rows in the batch: {detail}"
                    ),
                });
            }
        }

        // Batch-uniform rule for on_conflict on object relations: all rows must
        // carry the same on_conflict (or lack thereof) for a given relation,
        // because the renderer emits a single child INSERT CTE per relation and
        // cannot honor divergent clauses.
        for rel_name in objects[0].nested_objects.keys() {
            let first_oc = &objects[0].nested_objects[rel_name].on_conflict;
            for (i, obj) in objects.iter().enumerate().skip(1) {
                let these_oc = &obj.nested_objects[rel_name].on_conflict;
                if format!("{:?}", these_oc) != format!("{:?}", first_oc) {
                    return Err(Error::Validate {
                        path: format!("{parent_path}.objects[{i}].{rel_name}.on_conflict"),
                        message: format!(
                            "nested on_conflict for object-relation '{rel_name}' must be identical across all rows in the batch"
                        ),
                    });
                }
            }
        }
        // Same check for array relations: when the same array-relation key is
        // present in two or more rows, their on_conflicts must match.
        use std::collections::BTreeMap;
        let mut first_array_oc: BTreeMap<&str, &Option<crate::ast::OnConflict>> = BTreeMap::new();
        for obj in objects.iter() {
            for (rel_name, nai) in &obj.nested_arrays {
                if let Some(existing) = first_array_oc.get(rel_name.as_str()) {
                    if format!("{:?}", *existing) != format!("{:?}", nai.on_conflict) {
                        return Err(Error::Validate {
                            path: format!("{parent_path}.objects.{rel_name}.on_conflict"),
                            message: format!(
                                "nested on_conflict for array-relation '{rel_name}' must be identical across all rows in the batch that include it"
                            ),
                        });
                    }
                } else {
                    first_array_oc.insert(rel_name.as_str(), &nai.on_conflict);
                }
            }
        }
    }

    Ok((objects, on_conflict))
}

fn parse_insert_object(
    json: &Value,
    table: &Table,
    schema: &Schema,
    path: &str,
) -> Result<crate::ast::InsertObject> {
    use std::collections::BTreeMap;
    let obj = json.as_object().ok_or_else(|| Error::Validate {
        path: path.into(),
        message: "expected object".into(),
    })?;

    let mut columns: BTreeMap<String, Val> = BTreeMap::new();
    let mut nested_arrays: BTreeMap<String, crate::ast::NestedArrayInsert> = BTreeMap::new();
    let mut nested_objects: BTreeMap<String, crate::ast::NestedObjectInsert> = BTreeMap::new();

    for (k, v) in obj {
        // Try column first.
        if table.find_column(k).is_some() {
            columns.insert(k.clone(), Val::Lit(v.clone()));
            continue;
        }

        // Try relation.
        if let Some(rel) = table.find_relation(k) {
            match rel.kind {
                crate::schema::RelKind::Array => {
                    let target =
                        schema
                            .table(&rel.target_table)
                            .ok_or_else(|| Error::Validate {
                                path: format!("{path}.{k}"),
                                message: format!(
                                    "relation target table '{}' missing",
                                    rel.target_table
                                ),
                            })?;
                    ensure_mutable(target, &format!("{path}.{k}"))?;

                    // Validate shape: `{ data: [...] }`
                    let wrapper = v.as_object().ok_or_else(|| Error::Validate {
                        path: format!("{path}.{k}"),
                        message: "nested array insert expects object with 'data' key".into(),
                    })?;
                    let data = wrapper.get("data").ok_or_else(|| Error::Validate {
                        path: format!("{path}.{k}"),
                        message: "missing required key 'data' in nested array insert".into(),
                    })?;
                    let data_arr = data.as_array().ok_or_else(|| Error::Validate {
                        path: format!("{path}.{k}.data"),
                        message: "expected array".into(),
                    })?;

                    // Reject any extra keys in the wrapper.
                    for other_k in wrapper.keys() {
                        if other_k != "data" && other_k != "on_conflict" {
                            return Err(Error::Validate {
                                path: format!("{path}.{k}.{other_k}"),
                                message: format!(
                                    "unknown key '{other_k}' in nested array insert; only 'data' and 'on_conflict' are supported"
                                ),
                            });
                        }
                    }

                    // Parse optional on_conflict against the CHILD table.
                    let on_conflict = if let Some(oc_json) = wrapper.get("on_conflict") {
                        Some(parse_on_conflict(
                            oc_json,
                            target,
                            schema,
                            &format!("{path}.{k}.on_conflict"),
                        )?)
                    } else {
                        None
                    };

                    // Recurse into each child row.
                    let mut rows = Vec::with_capacity(data_arr.len());
                    for (i, item) in data_arr.iter().enumerate() {
                        let child = parse_insert_object(
                            item,
                            target,
                            schema,
                            &format!("{path}.{k}.data[{i}]"),
                        )?;

                        // Reject child input that sets the FK column(s) that the engine
                        // will supply from the parent.
                        for (_parent_col, child_fk_col) in &rel.mapping {
                            if child.columns.contains_key(child_fk_col) {
                                return Err(Error::Validate {
                                    path: format!("{path}.{k}.data[{i}].{child_fk_col}"),
                                    message: format!(
                                        "column '{child_fk_col}' is populated from the parent; must not appear in nested child input"
                                    ),
                                });
                            }
                        }

                        rows.push(child);
                    }

                    nested_arrays.insert(
                        k.clone(),
                        crate::ast::NestedArrayInsert {
                            table: rel.target_table.clone(),
                            rows,
                            on_conflict,
                            scope_check: None,
                        },
                    );
                    continue;
                }
                crate::schema::RelKind::Object => {
                    let target =
                        schema
                            .table(&rel.target_table)
                            .ok_or_else(|| Error::Validate {
                                path: format!("{path}.{k}"),
                                message: format!(
                                    "relation target table '{}' missing",
                                    rel.target_table
                                ),
                            })?;
                    ensure_mutable(target, &format!("{path}.{k}"))?;

                    // Validate shape: `{ data: <object> }`
                    let wrapper = v.as_object().ok_or_else(|| Error::Validate {
                        path: format!("{path}.{k}"),
                        message: "nested object insert expects object with 'data' key".into(),
                    })?;
                    let data = wrapper.get("data").ok_or_else(|| Error::Validate {
                        path: format!("{path}.{k}"),
                        message: "missing required key 'data' in nested object insert".into(),
                    })?;

                    // `data` must be a single object, not an array.
                    if data.is_array() {
                        return Err(Error::Validate {
                            path: format!("{path}.{k}.data"),
                            message: "object-relation 'data' must be a single object, not an array"
                                .into(),
                        });
                    }
                    if !data.is_object() {
                        return Err(Error::Validate {
                            path: format!("{path}.{k}.data"),
                            message: "object-relation 'data' must be an object".into(),
                        });
                    }

                    // Reject extra keys in the wrapper.
                    for other_k in wrapper.keys() {
                        if other_k != "data" && other_k != "on_conflict" {
                            return Err(Error::Validate {
                                path: format!("{path}.{k}.{other_k}"),
                                message: format!(
                                    "unknown key '{other_k}' in nested object insert; only 'data' and 'on_conflict' are supported"
                                ),
                            });
                        }
                    }

                    // Parse optional on_conflict against the CHILD table.
                    let on_conflict = if let Some(oc_json) = wrapper.get("on_conflict") {
                        Some(parse_on_conflict(
                            oc_json,
                            target,
                            schema,
                            &format!("{path}.{k}.on_conflict"),
                        )?)
                    } else {
                        None
                    };

                    // Reject FK-column-also-set conflict: the parent row must not
                    // specify the mapped FK column when it's also providing nested
                    // object data.
                    for (parent_fk_col, _) in &rel.mapping {
                        if columns.contains_key(parent_fk_col) {
                            return Err(Error::Validate {
                                path: format!("{path}.{k}"),
                                message: format!(
                                    "column '{parent_fk_col}' is populated from the nested object; must not also appear in the parent row"
                                ),
                            });
                        }
                    }

                    // Recurse into the inner object.
                    let child =
                        parse_insert_object(data, target, schema, &format!("{path}.{k}.data"))?;

                    nested_objects.insert(
                        k.clone(),
                        crate::ast::NestedObjectInsert {
                            table: rel.target_table.clone(),
                            row: child,
                            on_conflict,
                            scope_check: None,
                        },
                    );
                    continue;
                }
            }
        }

        return Err(Error::Validate {
            path: format!("{path}.{k}"),
            message: format!("unknown column '{k}' on '{}'", table.exposed_name),
        });
    }

    if columns.is_empty() && nested_arrays.is_empty() && nested_objects.is_empty() {
        return Err(Error::Validate {
            path: path.into(),
            message: "insert row must set at least one column or nested relation".into(),
        });
    }

    Ok(crate::ast::InsertObject {
        columns,
        nested_arrays,
        nested_objects,
    })
}

/// Validate that every key of an `_set`-shaped object is a known column on
/// `table` and return the column map. Used by the update helpers, which do not
/// support nested relations.
///
/// The *keys* are structure — they become the SET list of the UPDATE — so an
/// object supplied wholesale as `$set` has to be known when lowering. The
/// values are ordinary value positions and may stay symbolic.
fn gql_object_to_val_map(
    value: &GqlValue,
    table: &Table,
    vars: Bindings<'_>,
    path: &str,
) -> Result<std::collections::BTreeMap<String, Val>> {
    use std::collections::BTreeMap;
    let value = structural(value, vars, path)?;
    let GqlValue::Object(obj) = value.as_ref() else {
        return Err(Error::Validate {
            path: path.into(),
            message: "expected object".into(),
        });
    };
    let mut out: BTreeMap<String, Val> = BTreeMap::new();
    for (k, v) in obj {
        if table.find_column(k).is_none() {
            return Err(Error::Validate {
                path: format!("{path}.{k}"),
                message: format!("unknown column '{k}' on '{}'", table.exposed_name),
            });
        }
        out.insert(k.to_string(), gql_to_val(v, vars, &format!("{path}.{k}"))?);
    }
    if out.is_empty() {
        return Err(Error::Validate {
            path: path.into(),
            message: "insert row must set at least one column".into(),
        });
    }
    Ok(out)
}

fn parse_on_conflict(
    json: &Value,
    table: &Table,
    schema: &Schema,
    path: &str,
) -> Result<crate::ast::OnConflict> {
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
    // Checked here rather than left to Postgres, which answers 42704 at request
    // time — and so slips past `Engine::compile`, whose whole point is that a
    // query which cannot work fails at startup.
    //
    // Only when the table knows its constraints. A hand-built `Schema` declares
    // none, and rejecting every `on_conflict` against one would be enforcing a
    // list that was never claimed to be complete.
    if !table.unique_constraints.is_empty() && !table.unique_constraints.contains_key(&constraint) {
        // Only the ones a client could have learned about. Listing every
        // constraint would hand back the names withheld from
        // `<table>_constraint` — and a constraint name usually contains its
        // column names, which is what hiding a column meant to withhold. One
        // typo'd mutation would have been enough to read them out.
        let known: Vec<&str> = table
            .unique_constraints
            .iter()
            .filter(|(_, cols)| cols.iter().all(|c| table.find_column(c).is_some()))
            .map(|(name, _)| name.as_str())
            .collect();
        return Err(Error::Validate {
            path: format!("{path}.constraint"),
            message: format!(
                "'{constraint}' is not a unique constraint on '{}' ({})",
                table.exposed_name,
                known.join(", ")
            ),
        });
    }
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
                &json_to_gql(w),
                table,
                // The real schema: an empty one made a relation predicate here
                // fail with "relation target table missing", which named the
                // wrong cause and made `on_conflict: { where: { rel: … } }`
                // impossible to write.
                schema,
                // `w` came from `gql_to_json`, so every variable in it is
                // already substituted and the mode cannot matter.
                Bindings::Eager(&Value::Null),
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
    vars: Bindings<'_>,
    parent_path: &str,
) -> Result<(
    crate::ast::BoolExpr,
    std::collections::BTreeMap<String, Val>,
)> {
    use std::collections::BTreeMap;
    let mut where_: Option<crate::ast::BoolExpr> = None;
    let mut set: BTreeMap<String, Val> = BTreeMap::new();
    for (name_p, value_p) in args {
        let aname = name_p.node.as_str();
        let v = &value_p.node;
        match aname {
            "where" => {
                where_ = Some(lower_where(
                    v,
                    table,
                    schema,
                    vars,
                    &format!("{parent_path}.where"),
                )?);
            }
            "_set" => {
                set = gql_object_to_val_map(v, table, vars, &format!("{parent_path}._set"))?;
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
    vars: Bindings<'_>,
    parent_path: &str,
) -> Result<(Vec<(String, Val)>, std::collections::BTreeMap<String, Val>)> {
    use std::collections::BTreeMap;
    let mut pk_obj: Option<std::collections::BTreeMap<String, Val>> = None;
    let mut set: BTreeMap<String, Val> = BTreeMap::new();
    for (name_p, value_p) in args {
        let aname = name_p.node.as_str();
        let v = &value_p.node;
        match aname {
            "pk_columns" => {
                let path = format!("{parent_path}.pk_columns");
                let obj = structural(v, vars, &path)?;
                let GqlValue::Object(kv) = obj.as_ref() else {
                    return Err(Error::Validate {
                        path,
                        message: "expected object".into(),
                    });
                };
                let mut map = std::collections::BTreeMap::new();
                for (k, val) in kv {
                    // Same reason as `reject_non_pk_arguments`: only the primary
                    // key columns are read back out below, so anything else here
                    // would vanish without a word.
                    if !table.primary_key.iter().any(|pk| pk == k.as_str()) {
                        return Err(Error::Validate {
                            path: format!("{path}.{k}"),
                            message: format!(
                                "'{k}' is not a primary key column of '{}' ({})",
                                table.exposed_name,
                                table.primary_key.join(", ")
                            ),
                        });
                    }
                    map.insert(
                        k.to_string(),
                        gql_to_val(val, vars, &format!("{path}.{k}"))?,
                    );
                }
                pk_obj = Some(map);
            }
            "_set" => {
                set = gql_object_to_val_map(v, table, vars, &format!("{parent_path}._set"))?;
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
    let mut pk: Vec<(String, Val)> = Vec::new();
    for pk_col in &table.primary_key {
        let v = pk_obj.get(pk_col).ok_or_else(|| Error::Validate {
            path: format!("{parent_path}.pk_columns.{pk_col}"),
            message: format!("missing primary key value '{pk_col}'"),
        })?;
        pk.push((pk_col.clone(), v.clone()));
    }
    Ok((pk, set))
}

fn parse_returning(
    set: &SelectionSet,
    table: &Table,
    schema: &Schema,
    vars: Bindings<'_>,
    fragments: &Fragments<'_>,
    parent_path: &str,
) -> Result<(Vec<Field>, Vec<String>)> {
    let mut returning: Vec<Field> = Vec::new();
    // `__typename` here names the mutation-response type, not the row type, so
    // it cannot ride along inside `returning`.
    let mut typenames: Vec<String> = Vec::new();
    for sel in &set.items {
        let Selection::Field(f) = &sel.node else {
            return Err(Error::Parse(
                "fragments not supported in mutation return".into(),
            ));
        };
        let field = &f.node;
        let fname = field.name.node.as_str();
        match fname {
            TYPENAME => {
                let alias = field
                    .alias
                    .as_ref()
                    .map(|a| a.node.as_str().to_string())
                    .unwrap_or_else(|| fname.to_string());
                reject_typename_arguments(&field.arguments, &alias, parent_path)?;
                typenames.push(alias);
            }
            "affected_rows" => {}
            "returning" => {
                returning = lower_selection_set(
                    &field.selection_set.node,
                    table,
                    schema,
                    vars,
                    fragments,
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
    dedup_keys(&mut typenames);
    Ok((returning, typenames))
}

fn lower_selection_set(
    set: &SelectionSet,
    table: &Table,
    schema: &Schema,
    vars: Bindings<'_>,
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

                if name == TYPENAME {
                    reject_typename_arguments(&field.arguments, &alias, parent_path)?;
                    out.push(Field::Typename { alias });
                    continue;
                }

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

                // `<rel>_aggregate`, but only once a real column of that name
                // has had its chance: a column is the thing that exists, and a
                // synthesized field must not shadow one — the same rule that
                // keeps an auto-derived relation from shadowing a column.
                if let Some(base) = name.strip_suffix("_aggregate") {
                    if table.find_column(name).is_none() {
                        if let Some(rel) = table.find_relation(base) {
                            if rel.kind != crate::schema::RelKind::Array {
                                return Err(Error::Validate {
                                    path: format!("{parent_path}.{alias}"),
                                    message: format!(
                                        "'{base}' is an object relation, which is one row; \
                                         there is nothing to aggregate"
                                    ),
                                });
                            }
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
                            let path = format!("{parent_path}.{alias}");
                            let args = lower_args(&field.arguments, target, schema, vars, &path)?;
                            reject_distinct_on_aggregate(&args, &path)?;
                            let AggregateSelection {
                                ops,
                                nodes,
                                typenames,
                            } = lower_aggregate_selection(
                                &field.selection_set.node,
                                target,
                                vars,
                                &path,
                            )?;
                            out.push(Field::RelationAggregate {
                                name: base.to_string(),
                                alias,
                                args,
                                ops,
                                nodes,
                                typenames,
                                nodes_limit: None,
                            });
                            continue;
                        }
                    }
                }

                let col = table.find_column(name).ok_or_else(|| {
                    // Someone writing `posts_aggregate` meant the relation, and
                    // being told there is no such *column* sends them looking in
                    // the wrong place.
                    let message = match name.strip_suffix("_aggregate") {
                        Some(base) => format!(
                            "unknown field '{name}' on '{}': there is no column by that name, \
                             and no array relation '{base}' to aggregate",
                            table.exposed_name
                        ),
                        None => {
                            format!("unknown column '{name}' on '{}'", table.exposed_name)
                        }
                    };
                    Error::Validate {
                        path: format!("{parent_path}.{alias}"),
                        message,
                    }
                })?;
                out.push(lower_scalar_field(
                    &field.arguments,
                    col,
                    alias,
                    vars,
                    parent_path,
                )?);
            }
        }
    }
    merge_fields(out, parent_path)
}

/// Lower a scalar column selection into either a plain [`Field::Column`] or a
/// [`Field::JsonPath`] when a `path` argument is present.
///
/// `path` is a dot-separated string of key/index components; it is only valid on
/// `json`/`jsonb` columns. Any other argument on a scalar field is rejected
/// rather than silently ignored.
fn lower_scalar_field(
    arguments: &[(Positioned<Name>, Positioned<GqlValue>)],
    col: &crate::schema::Column,
    alias: String,
    vars: Bindings<'_>,
    parent_path: &str,
) -> Result<Field> {
    let mut path_value: Option<&GqlValue> = None;
    for (name_p, value_p) in arguments {
        match name_p.node.as_str() {
            "path" => path_value = Some(&value_p.node),
            other => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.{alias}"),
                    message: format!(
                        "unknown argument '{other}' on column '{}'",
                        col.exposed_name
                    ),
                })
            }
        }
    }

    let Some(v) = path_value else {
        return Ok(Field::Column {
            column: col.exposed_name.clone(),
            alias,
        });
    };

    use crate::schema::PgType;
    if !matches!(col.pg_type, PgType::Json | PgType::Jsonb) {
        return Err(Error::Validate {
            path: format!("{parent_path}.{alias}"),
            message: format!(
                "argument 'path' requires a json/jsonb column, but '{}' is not",
                col.exposed_name
            ),
        });
    }

    let json = gql_to_json(v, vars, &format!("{parent_path}.{alias}.path"))?;
    let raw = json.as_str().ok_or_else(|| Error::Validate {
        path: format!("{parent_path}.{alias}.path"),
        message: "argument 'path' must be a string (e.g. \"a.b.c\")".into(),
    })?;
    let path: Vec<String> = raw
        .split('.')
        .filter(|c| !c.is_empty())
        .map(|c| c.to_string())
        .collect();
    if path.is_empty() {
        return Err(Error::Validate {
            path: format!("{parent_path}.{alias}.path"),
            message: "argument 'path' must name at least one key".into(),
        });
    }

    Ok(Field::JsonPath {
        column: col.exposed_name.clone(),
        alias,
        path,
    })
}

fn lower_args(
    args: &[(Positioned<Name>, Positioned<GqlValue>)],
    table: &Table,
    schema: &Schema,
    vars: Bindings<'_>,
    parent_path: &str,
) -> Result<QueryArgs> {
    let mut out = QueryArgs::default();
    for (name_p, value_p) in args {
        let name = name_p.node.as_str();
        let v = &value_p.node;
        match name {
            "where" => {
                out.where_ = Some(lower_where(
                    v,
                    table,
                    schema,
                    vars,
                    &format!("{parent_path}.where"),
                )?);
            }
            "order_by" => {
                out.order_by =
                    lower_order_by(v, vars, &format!("{parent_path}.order_by"), table, schema)?;
            }
            "limit" => {
                out.limit = Some(gql_count(v, vars, &format!("{parent_path}.limit"))?);
            }
            "offset" => {
                out.offset = Some(gql_count(v, vars, &format!("{parent_path}.offset"))?);
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

/// Lower a Hasura-style `where` argument.
///
/// Walks the GraphQL value rather than a pre-substituted JSON one, because that
/// is where variables still exist: a variable in a *value* position becomes a
/// [`Val::Var`], while a variable standing in for structure (`where: $w`,
/// `_is_null: $b`) is resolved through [`structural`] and so is only allowed
/// when its value is already known.
pub(crate) fn lower_where(
    value: &GqlValue,
    table: &Table,
    schema: &Schema,
    vars: Bindings<'_>,
    path: &str,
) -> Result<BoolExpr> {
    let value = structural(value, vars, path)?;
    let GqlValue::Object(obj) = value.as_ref() else {
        return Err(Error::Validate {
            path: path.into(),
            message: "expected object".into(),
        });
    };
    let mut parts: Vec<BoolExpr> = Vec::new();
    for (k, v) in obj {
        match k.as_str() {
            "_and" | "_or" => {
                let key = k.as_str();
                let list = structural(v, vars, &format!("{path}.{key}"))?;
                let GqlValue::List(items) = list.as_ref() else {
                    return Err(Error::Validate {
                        path: format!("{path}.{key}"),
                        message: "expected array".into(),
                    });
                };
                let inner: Result<Vec<BoolExpr>> = items
                    .iter()
                    .enumerate()
                    .map(|(i, x)| {
                        lower_where(x, table, schema, vars, &format!("{path}.{key}[{i}]"))
                    })
                    .collect();
                parts.push(if key == "_and" {
                    BoolExpr::And(inner?)
                } else {
                    BoolExpr::Or(inner?)
                });
            }
            "_not" => {
                parts.push(BoolExpr::Not(Box::new(lower_where(
                    v,
                    table,
                    schema,
                    vars,
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
                    let inner =
                        lower_where(v, target, schema, vars, &format!("{path}.{col_name}"))?;
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
                let ops = structural(v, vars, &format!("{path}.{col_name}"))?;
                let GqlValue::Object(op_obj) = ops.as_ref() else {
                    return Err(Error::Validate {
                        path: format!("{path}.{col_name}"),
                        message: "expected operator object".into(),
                    });
                };
                for (op_name, op_val) in op_obj {
                    let op_path = || format!("{path}.{col_name}.{op_name}");
                    let cmp = |op: CmpOp, v: &GqlValue| -> Result<BoolExpr> {
                        Ok(BoolExpr::Compare {
                            column: col.exposed_name.clone(),
                            op,
                            value: gql_to_val(v, vars, &op_path())?,
                        })
                    };
                    let in_list = |negated: bool, v: &GqlValue| -> Result<BoolExpr> {
                        let values = gql_to_val(v, vars, &op_path())?;
                        // A written-out list must be a list; `_in: $ids` is
                        // checked when the request supplies it.
                        if let Val::Lit(lit) = &values {
                            if !lit.is_array() {
                                return Err(Error::Validate {
                                    path: op_path(),
                                    message: "expected array".into(),
                                });
                            }
                        }
                        Ok(BoolExpr::InList {
                            column: col.exposed_name.clone(),
                            values,
                            negated,
                        })
                    };
                    let part = match op_name.as_str() {
                        "_eq" => cmp(CmpOp::Eq, op_val)?,
                        "_neq" => cmp(CmpOp::Neq, op_val)?,
                        "_gt" => cmp(CmpOp::Gt, op_val)?,
                        "_gte" => cmp(CmpOp::Gte, op_val)?,
                        "_lt" => cmp(CmpOp::Lt, op_val)?,
                        "_lte" => cmp(CmpOp::Lte, op_val)?,
                        "_like" => cmp(CmpOp::Like, op_val)?,
                        "_ilike" => cmp(CmpOp::ILike, op_val)?,
                        "_nlike" => cmp(CmpOp::NLike, op_val)?,
                        "_nilike" => cmp(CmpOp::NILike, op_val)?,
                        // `_is_null` picks between `IS NULL` and `IS NOT NULL`,
                        // so its value is structure, not a bound parameter.
                        "_is_null" => {
                            let b = structural(op_val, vars, &op_path())?;
                            let GqlValue::Boolean(b) = b.as_ref() else {
                                return Err(Error::Validate {
                                    path: op_path(),
                                    message: "expected boolean".into(),
                                });
                            };
                            BoolExpr::IsNull {
                                column: col.exposed_name.clone(),
                                negated: !b,
                            }
                        }
                        "_in" => in_list(false, op_val)?,
                        "_nin" => in_list(true, op_val)?,
                        other => {
                            return Err(Error::Validate {
                                path: format!("{path}.{col_name}"),
                                message: format!("unsupported operator '{other}'"),
                            });
                        }
                    };
                    parts.push(part);
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

fn lower_order_by(
    v: &GqlValue,
    vars: Bindings<'_>,
    path: &str,
    table: &Table,
    schema: &Schema,
) -> Result<Vec<OrderBy>> {
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
        for (key, val) in obj {
            lower_order_by_entry(
                key,
                val,
                &format!("{path}[{i}]"),
                table,
                schema,
                &mut Vec::new(),
                &mut out,
            )?;
        }
    }
    Ok(out)
}

/// One `order_by` entry. A string value (`asc` / `desc`) terminates on a column;
/// an object value walks an object relation and recurses.
fn lower_order_by_entry(
    key: &str,
    val: &Value,
    path: &str,
    table: &Table,
    schema: &Schema,
    rel_path: &mut Vec<String>,
    out: &mut Vec<OrderBy>,
) -> Result<()> {
    // Nested: order by a column reached through an object relation.
    if let Value::Object(inner) = val {
        let rel = table.find_relation(key).ok_or_else(|| Error::Validate {
            path: format!("{path}.{key}"),
            message: format!(
                "unknown column or relation '{key}' on '{}'",
                table.exposed_name
            ),
        })?;
        // Ordering through an array relation would need an aggregate
        // (Hasura's `posts_aggregate: {count: desc}`), which is not implemented.
        if rel.kind != crate::schema::RelKind::Object {
            return Err(Error::Validate {
                path: format!("{path}.{key}"),
                message: format!(
                    "cannot order by array relation '{key}'; only object relations are supported"
                ),
            });
        }
        let target = schema
            .table(&rel.target_table)
            .ok_or_else(|| Error::Validate {
                path: format!("{path}.{key}"),
                message: format!("relation target table '{}' missing", rel.target_table),
            })?;

        rel_path.push(key.to_string());
        for (k, v) in inner {
            lower_order_by_entry(
                k,
                v,
                &format!("{path}.{key}"),
                target,
                schema,
                rel_path,
                out,
            )?;
        }
        rel_path.pop();
        return Ok(());
    }

    // Leaf: a direction on a column of the current table.
    let dir_s = val.as_str().ok_or_else(|| Error::Validate {
        path: format!("{path}.{key}"),
        message: "expected 'asc' or 'desc'".into(),
    })?;
    // Hasura's six: NULL placement is part of the direction token, because
    // PostgreSQL's default is asymmetric (ASC -> nulls last, DESC -> nulls first).
    let (direction, nulls) = match dir_s {
        "asc" => (OrderDir::Asc, None),
        "desc" => (OrderDir::Desc, None),
        "asc_nulls_first" => (OrderDir::Asc, Some(NullsOrder::First)),
        "asc_nulls_last" => (OrderDir::Asc, Some(NullsOrder::Last)),
        "desc_nulls_first" => (OrderDir::Desc, Some(NullsOrder::First)),
        "desc_nulls_last" => (OrderDir::Desc, Some(NullsOrder::Last)),
        other => {
            return Err(Error::Validate {
                path: format!("{path}.{key}"),
                message: format!(
                    "unknown direction '{other}'; expected asc / desc / \
                     asc_nulls_first / asc_nulls_last / desc_nulls_first / desc_nulls_last"
                ),
            })
        }
    };
    if table.find_column(key).is_none() {
        return Err(Error::Validate {
            path: format!("{path}.{key}"),
            message: format!("unknown column '{key}' on '{}'", table.exposed_name),
        });
    }
    out.push(OrderBy {
        path: rel_path
            .iter()
            .map(|r| crate::ast::OrderByHop::new(r.as_str()))
            .collect(),
        column: key.to_string(),
        direction,
        nulls,
    });
    Ok(())
}

/// Lower a `limit` / `offset`. A variable stays a variable: the count is a
/// bound parameter, not part of the statement's shape.
fn gql_count(v: &GqlValue, vars: Bindings<'_>, path: &str) -> Result<Count> {
    if let (GqlValue::Variable(name), Bindings::Symbolic) = (v, vars) {
        return Ok(Count::var(name.as_str()));
    }
    let json = gql_to_json(v, vars, path)?;
    json.as_u64()
        .map(Count::Lit)
        .ok_or_else(|| Error::Validate {
            path: path.into(),
            message: "expected non-negative integer".into(),
        })
}

/// Convert a GraphQL value to JSON, resolving variable references from `vars`.
fn gql_to_json(v: &GqlValue, vars: Bindings<'_>, path: &str) -> Result<Value> {
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
        GqlValue::Variable(name) => vars.require_value(name.as_str(), path).cloned(),
        GqlValue::Binary(_) => Err(Error::Parse("binary literals not supported".into())),
    }
}

/// How lowering treats `$variables`.
///
/// The two modes produce the same IR shape; they differ only in what happens at
/// a variable. Eager lowering has the values in hand and substitutes them, so
/// the IR it produces is specific to one request. Symbolic lowering keeps them
/// as names, so the IR — and the SQL rendered from it — is reusable across
/// requests; the price is that a variable in a position that decides the
/// *shape* of the SQL cannot be lowered at all.
#[derive(Clone, Copy)]
pub enum Bindings<'a> {
    /// Substitute from this request's variables while lowering.
    Eager(&'a Value),
    /// Leave variables as [`Val::Var`] for the request to fill in.
    Symbolic,
}

impl<'a> Bindings<'a> {
    /// Value of `$name`, for a position that needs it *now*.
    ///
    /// Under [`Bindings::Symbolic`] this is exactly the case that cannot be
    /// compiled: the caller is about to branch on the value, so leaving it
    /// symbolic would mean the rendered SQL depended on it.
    fn require_value(&self, name: &str, path: &str) -> Result<&'a Value> {
        match self {
            Bindings::Eager(vars) => vars.get(name).ok_or_else(|| Error::Variable {
                name: name.to_string(),
                message: "not bound".into(),
            }),
            Bindings::Symbolic => Err(Error::NotCompilable {
                path: path.to_string(),
                message: format!(
                    "'${name}' decides the shape of the SQL here, so it cannot be left \
                     to execution time; move it to a value position or run this query \
                     with Engine::query instead"
                ),
            }),
        }
    }
}

/// Prepare a value that lowering is about to *walk into* rather than store.
///
/// A variable standing in for a whole `where` object, list of rows, or argument
/// object is fine when its value is already known — the walk simply continues
/// through the substituted value. It is not fine symbolically, because the
/// structure being walked is what the SQL is generated from.
fn structural<'v>(
    v: &'v GqlValue,
    vars: Bindings<'_>,
    path: &str,
) -> Result<std::borrow::Cow<'v, GqlValue>> {
    match v {
        GqlValue::Variable(name) => {
            let json = vars.require_value(name.as_str(), path)?;
            Ok(std::borrow::Cow::Owned(json_to_gql(json)))
        }
        other => Ok(std::borrow::Cow::Borrowed(other)),
    }
}

/// Lower a value position: a variable here becomes a [`Val::Var`] under
/// symbolic lowering, and is substituted under eager lowering.
fn gql_to_val(v: &GqlValue, vars: Bindings<'_>, path: &str) -> Result<Val> {
    let val = match v {
        GqlValue::Variable(name) => match vars {
            Bindings::Symbolic => Val::Var(name.as_str().to_string()),
            Bindings::Eager(_) => Val::Lit(vars.require_value(name.as_str(), path)?.clone()),
        },
        GqlValue::List(xs) => {
            let mut items = Vec::with_capacity(xs.len());
            for (i, x) in xs.iter().enumerate() {
                items.push(gql_to_val(x, vars, &format!("{path}[{i}]"))?);
            }
            Val::Array(items)
        }
        GqlValue::Object(kv) => {
            let mut items = Vec::with_capacity(kv.len());
            for (k, val) in kv {
                items.push((
                    k.to_string(),
                    gql_to_val(val, vars, &format!("{path}.{k}"))?,
                ));
            }
            Val::Object(items)
        }
        scalar => Val::Lit(gql_to_json(scalar, vars, path)?),
    };
    // Collapse a composite with no variables in it back to a plain literal, so
    // downstream code sees the same `Val::Lit` it would have seen before.
    Ok(val.collapse())
}

/// Convert JSON back into a (variable-free) GraphQL value, so a substituted
/// variable can be walked by the same code that walks a written-out literal.
pub(crate) fn json_to_gql(v: &Value) -> GqlValue {
    match v {
        Value::Null => GqlValue::Null,
        Value::Bool(b) => GqlValue::Boolean(*b),
        // `async_graphql_value::Number` is a re-export of serde_json's, so this
        // is a move, not a reparse.
        Value::Number(n) => GqlValue::Number(n.clone()),
        Value::String(s) => GqlValue::String(s.clone()),
        Value::Array(xs) => GqlValue::List(xs.iter().map(json_to_gql).collect()),
        Value::Object(kv) => GqlValue::Object(
            kv.iter()
                .map(|(k, v)| (Name::new(k), json_to_gql(v)))
                .collect(),
        ),
    }
}

/// What an `_aggregate` field selected: the functions, the optional `nodes`
/// row selection, and any `__typename` at the `<table>_aggregate` level.
struct AggregateSelection {
    ops: Vec<crate::ast::AggSelect>,
    nodes: Option<Vec<Field>>,
    typenames: Vec<String>,
}

fn lower_aggregate_selection(
    set: &SelectionSet,
    table: &Table,
    vars: Bindings<'_>,
    parent_path: &str,
) -> Result<AggregateSelection> {
    use crate::ast::{AggCol, AggField, AggOp, AggSelect};

    let mut ops: Vec<AggSelect> = Vec::new();
    let mut nodes: Option<Vec<Field>> = None;
    let mut typenames: Vec<String> = Vec::new();

    for sel in &set.items {
        let Selection::Field(f) = &sel.node else {
            return Err(Error::Parse(
                "fragments are not supported inside an _aggregate field".into(),
            ));
        };
        let field = &f.node;
        let key = field.name.node.as_str();
        match key {
            "aggregate" => {
                for s in &field.selection_set.node.items {
                    let Selection::Field(sf) = &s.node else {
                        return Err(Error::Parse(
                            "fragments are not supported inside aggregate".into(),
                        ));
                    };
                    let sf = &sf.node;
                    let op_name = sf.name.node.as_str();
                    let alias = sf
                        .alias
                        .as_ref()
                        .map(|a| a.node.as_str().to_string())
                        .unwrap_or_else(|| op_name.to_string());
                    let path = format!("{parent_path}.aggregate.{alias}");
                    let op = match op_name {
                        TYPENAME => {
                            reject_typename_arguments(
                                &sf.arguments,
                                &alias,
                                &format!("{parent_path}.aggregate"),
                            )?;
                            AggOp::Typename
                        }
                        "count" => parse_count_args(&sf.arguments, table, vars, &path)?,
                        _ if crate::ast::AggFunc::from_name(op_name).is_some() => {
                            let func = crate::ast::AggFunc::from_name(op_name).unwrap();
                            if let Some((first, _)) = sf.arguments.first() {
                                return Err(Error::Validate {
                                    path: format!("{path}.{}", first.node.as_str()),
                                    message: format!(
                                        "'{op_name}' takes no arguments; name the columns in its \
                                         selection set instead"
                                    ),
                                });
                            }
                            let mut fields = Vec::new();
                            let mut column_count = 0usize;
                            for cs in &sf.selection_set.node.items {
                                let Selection::Field(cf) = &cs.node else {
                                    return Err(Error::Parse(
                                        "fragments are not supported inside aggregate".into(),
                                    ));
                                };
                                let cf = &cf.node;
                                let cname = cf.name.node.as_str();
                                let calias = cf
                                    .alias
                                    .as_ref()
                                    .map(|a| a.node.as_str().to_string())
                                    .unwrap_or_else(|| cname.to_string());
                                if let Some((first, _)) = cf.arguments.first() {
                                    return Err(Error::Validate {
                                        path: format!("{path}.{calias}.{}", first.node.as_str()),
                                        message: "a column inside an aggregate takes no arguments"
                                            .into(),
                                    });
                                }
                                if cname == TYPENAME {
                                    fields.push(AggField::Typename { alias: calias });
                                    continue;
                                }
                                let col =
                                    table.find_column(cname).ok_or_else(|| Error::Validate {
                                        path: format!("{path}.{calias}"),
                                        message: format!(
                                            "unknown column '{cname}' on '{}'",
                                            table.exposed_name
                                        ),
                                    })?;
                                // The type system publishes `sum` only over
                                // numbers; accepting it over a `text` column
                                // here would accept a query the schema says
                                // cannot exist, and PostgreSQL would answer
                                // "function sum(text) does not exist" at request
                                // time.
                                if !crate::type_system::applies(func, &col.pg_type) {
                                    return Err(Error::Validate {
                                        path: format!("{path}.{calias}"),
                                        message: format!(
                                            "'{op_name}' does not apply to '{}': {}",
                                            col.exposed_name,
                                            crate::type_system::why_inapplicable(
                                                func,
                                                &col.pg_type
                                            )
                                        ),
                                    });
                                }
                                column_count += 1;
                                fields.push(AggField::Column(AggCol {
                                    alias: calias,
                                    column: col.exposed_name.clone(),
                                }));
                            }
                            if column_count == 0 {
                                return Err(Error::Validate {
                                    path: path.clone(),
                                    message: format!("'{op_name}' needs at least one column"),
                                });
                            }
                            AggOp::Func { func, fields }
                        }
                        other => {
                            return Err(Error::Validate {
                                path: format!("{parent_path}.aggregate.{other}"),
                                message: format!("unsupported aggregate '{other}'"),
                            });
                        }
                    };
                    ops.push(AggSelect { alias, op });
                }
            }
            "nodes" => {
                let fields = lower_selection_columns_only(
                    &field.selection_set.node,
                    table,
                    vars,
                    &format!("{parent_path}.nodes"),
                )?;
                nodes = Some(fields);
            }
            TYPENAME => {
                let alias = field
                    .alias
                    .as_ref()
                    .map(|a| a.node.as_str().to_string())
                    .unwrap_or_else(|| key.to_string());
                reject_typename_arguments(&field.arguments, &alias, parent_path)?;
                typenames.push(alias);
            }
            other => {
                return Err(Error::Validate {
                    path: format!("{parent_path}.{other}"),
                    message: format!("unknown aggregate subfield '{other}'"),
                });
            }
        }
    }
    dedup_keys(&mut typenames);
    Ok(AggregateSelection {
        ops,
        nodes,
        typenames,
    })
}

/// `count`, `count(columns: [a, b])`, `count(distinct: true, columns: [a])`.
///
/// Arguments here used to be read by nobody: `count(distinct: true, columns:
/// [name])` rendered `count(*)` and returned a number that answered a different
/// question than the one asked, without an error. Whatever is not understood is
/// now rejected.
fn parse_count_args(
    args: &[(Positioned<Name>, Positioned<GqlValue>)],
    table: &Table,
    vars: Bindings<'_>,
    path: &str,
) -> Result<crate::ast::AggOp> {
    let mut columns: Vec<String> = Vec::new();
    let mut distinct = false;
    for (name_p, value_p) in args {
        let name = name_p.node.as_str();
        match name {
            "columns" => {
                let json = gql_to_json(&value_p.node, vars, &format!("{path}.columns"))?;
                let items = match &json {
                    Value::Array(xs) => xs.clone(),
                    single => vec![single.clone()],
                };
                for (i, item) in items.iter().enumerate() {
                    let cname = item.as_str().ok_or_else(|| Error::Validate {
                        path: format!("{path}.columns[{i}]"),
                        message: "expected column name (enum or string)".into(),
                    })?;
                    let col = table.find_column(cname).ok_or_else(|| Error::Validate {
                        path: format!("{path}.columns[{i}]"),
                        message: format!("unknown column '{cname}' on '{}'", table.exposed_name),
                    })?;
                    columns.push(col.exposed_name.clone());
                }
            }
            "distinct" => {
                let json = gql_to_json(&value_p.node, vars, &format!("{path}.distinct"))?;
                distinct = json.as_bool().ok_or_else(|| Error::Validate {
                    path: format!("{path}.distinct"),
                    message: format!("expected a boolean, got {json}"),
                })?;
            }
            other => {
                return Err(Error::Validate {
                    path: format!("{path}.{other}"),
                    message: format!(
                        "unknown argument '{other}' on 'count'; it takes 'columns' and 'distinct'"
                    ),
                })
            }
        }
    }
    if distinct && columns.is_empty() {
        return Err(Error::Validate {
            path: format!("{path}.distinct"),
            message: "'distinct' needs 'columns' to be distinct on".into(),
        });
    }
    Ok(crate::ast::AggOp::Count { columns, distinct })
}

fn lower_selection_columns_only(
    set: &SelectionSet,
    table: &Table,
    vars: Bindings<'_>,
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
        if name == TYPENAME {
            reject_typename_arguments(&field.arguments, &alias, parent_path)?;
            out.push(Field::Typename { alias });
            continue;
        }
        let col = table.find_column(name).ok_or_else(|| Error::Validate {
            path: format!("{parent_path}.{alias}"),
            message: format!("unknown column '{name}' on '{}'", table.exposed_name),
        })?;
        out.push(lower_scalar_field(
            &field.arguments,
            col,
            alias,
            vars,
            parent_path,
        )?);
    }
    merge_fields(out, parent_path)
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
                    .column("data", "data", PgType::Jsonb, true)
                    .primary_key(&["id"]),
            )
            .build()
    }

    #[test]
    fn parse_json_path_with_alias() {
        let op = parse_and_lower(
            r#"query { users { abundance: data(path: "a.b") } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!("expected List");
        };
        match &selection[0] {
            Field::JsonPath {
                column,
                alias,
                path,
            } => {
                assert_eq!(column, "data");
                assert_eq!(alias, "abundance");
                assert_eq!(path, &vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected JsonPath, got {other:?}"),
        }
    }

    #[test]
    fn parse_json_path_single_key() {
        let op = parse_and_lower(
            r#"query { users { data(path: "abundance") } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!("expected List");
        };
        match &selection[0] {
            Field::JsonPath { alias, path, .. } => {
                // No GraphQL alias → response key falls back to the field name.
                assert_eq!(alias, "data");
                assert_eq!(path, &vec!["abundance".to_string()]);
            }
            other => panic!("expected JsonPath, got {other:?}"),
        }
    }

    #[test]
    fn parse_json_path_rejects_non_json_column() {
        let err = parse_and_lower(
            r#"query { users { name(path: "a") } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("json/jsonb"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_rejects_unknown_column_argument() {
        let err = parse_and_lower(
            r#"query { users { data(bogus: "x") } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("unknown argument 'bogus'"),
            "unexpected error: {err}"
        );
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
                Field::Column { column, alias } => {
                    assert_eq!(column, "id");
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
            Field::Column { column, alias } => {
                assert_eq!(column, "id");
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
        assert_eq!(args.limit, Some(Count::Lit(10)));
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
                assert_eq!(args.limit, Some(Count::Lit(3)));
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
            crate::ast::RootBody::Aggregate { ops, nodes, .. } => {
                assert_eq!(ops.len(), 2);
                assert_eq!(ops[0].alias, "count");
                assert!(matches!(
                    ops[0].op,
                    crate::ast::AggOp::Count {
                        distinct: false,
                        ..
                    }
                ));
                assert_eq!(ops[1].alias, "sum");
                match &ops[1].op {
                    crate::ast::AggOp::Func { fields, .. } => {
                        assert_eq!(fields.len(), 1);
                        match &fields[0] {
                            crate::ast::AggField::Column(c) => {
                                assert_eq!(c.column, "id");
                                assert_eq!(c.alias, "id");
                            }
                            other => panic!("expected a column, got {other:?}"),
                        }
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
    fn parse_where_in_lowers_to_in_list() {
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
            crate::ast::BoolExpr::InList {
                column,
                values,
                negated,
            } => {
                assert_eq!(column, "id");
                assert_eq!(
                    values.as_lit().and_then(|v| v.as_array()).map(Vec::len),
                    Some(3)
                );
                assert!(!negated);
            }
            _ => panic!("expected InList"),
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
            crate::ast::RootBody::Aggregate { ops, nodes, .. } => {
                assert_eq!(ops.len(), 1);
                assert!(nodes.is_none());
            }
            _ => panic!("expected Aggregate"),
        }
        assert!(roots[0].args.where_.is_some());
    }

    fn agg(q: &str) -> Result<Vec<crate::ast::AggSelect>> {
        let op = parse_and_lower(q, &json!({}), None, &schema())?;
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        match &roots[0].body {
            crate::ast::RootBody::Aggregate { ops, .. } => Ok(ops.clone()),
            _ => panic!("expected Aggregate"),
        }
    }

    #[test]
    fn count_takes_columns_and_distinct() {
        let ops =
            agg("{ users_aggregate { aggregate { count(columns: [name], distinct: true) } } }")
                .unwrap();
        match &ops[0].op {
            crate::ast::AggOp::Count { columns, distinct } => {
                assert_eq!(columns, &vec!["name".to_string()]);
                assert!(distinct);
            }
            other => panic!("expected Count, got {other:?}"),
        }
    }

    #[test]
    fn count_rejects_what_it_does_not_understand() {
        // Silently ignoring these is what made `count` answer a different
        // question than the one asked.
        let err = agg("{ users_aggregate { aggregate { count(sneaky: 1) } } }").unwrap_err();
        assert!(
            format!("{err}").contains("unknown argument 'sneaky'"),
            "{err}"
        );

        let err = agg("{ users_aggregate { aggregate { count(columns: [nope]) } } }").unwrap_err();
        assert!(format!("{err}").contains("unknown column 'nope'"), "{err}");

        let err = agg("{ users_aggregate { aggregate { count(distinct: true) } } }").unwrap_err();
        assert!(format!("{err}").contains("needs 'columns'"), "{err}");

        let err =
            agg("{ users_aggregate { aggregate { sum(columns: [id]) { id } } } }").unwrap_err();
        assert!(format!("{err}").contains("takes no arguments"), "{err}");
    }

    #[test]
    fn aggregate_fields_keep_their_aliases() {
        let ops =
            agg("{ users_aggregate { aggregate { total: count highest: max { newest: id } } } }")
                .unwrap();
        assert_eq!(ops[0].alias, "total");
        assert_eq!(ops[1].alias, "highest");
        match &ops[1].op {
            crate::ast::AggOp::Func { fields, .. } => match &fields[0] {
                crate::ast::AggField::Column(c) => {
                    assert_eq!(c.alias, "newest");
                    assert_eq!(c.column, "id");
                }
                other => panic!("expected a column, got {other:?}"),
            },
            other => panic!("expected Max, got {other:?}"),
        }
    }

    #[test]
    fn repeated_identical_fields_collapse() {
        // The shape a fragment spread produces: `id` asked for twice.
        let op = parse_and_lower(
            "fragment F on users { id } query { users { id ...F } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        match &roots[0].body {
            crate::ast::RootBody::List { selection } => assert_eq!(selection.len(), 1),
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn conflicting_fields_under_one_key_are_rejected() {
        // Rendering both put two "name" keys in one json_build_object and the
        // second silently won.
        let err = parse_and_lower(
            r#"{ users { name: id name } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("both answer to 'name'"), "{err}");
    }

    #[test]
    fn duplicate_root_alias_is_rejected() {
        let err = parse_and_lower(
            "{ users { id } users { name } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("two root fields both answer to 'users'"),
            "{err}"
        );
        // An alias on one of them is the fix, and it works.
        parse_and_lower(
            "{ users { id } others: users { name } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
    }

    #[test]
    fn identical_argument_free_relations_merge_but_differing_ones_do_not() {
        let s = schema_with_relations();
        let op = parse_and_lower(
            "{ users { posts { id } posts { title } } }",
            &json!({}),
            None,
            &s,
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        match &roots[0].body {
            crate::ast::RootBody::List { selection } => {
                assert_eq!(selection.len(), 1);
                match &selection[0] {
                    Field::Relation { selection, .. } => assert_eq!(selection.len(), 2),
                    _ => panic!("expected Relation"),
                }
            }
            _ => panic!("expected List"),
        }

        let err = parse_and_lower(
            "{ users { posts { id } posts(limit: 1) { id } } }",
            &json!({}),
            None,
            &s,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("both answer to 'posts'"), "{err}");
    }

    #[test]
    fn declared_variable_defaults_are_applied() {
        let op = parse_and_lower(
            "query($n: Int = 7) { users(limit: $n) { id } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        assert!(matches!(
            roots[0].args.limit,
            Some(crate::ast::Count::Lit(7))
        ));

        // A supplied value beats the default.
        let op = parse_and_lower(
            "query($n: Int = 7) { users(limit: $n) { id } }",
            &json!({"n": 3}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        assert!(matches!(
            roots[0].args.limit,
            Some(crate::ast::Count::Lit(3))
        ));
    }

    #[test]
    fn typename_lowers_in_a_row_selection() {
        let op =
            parse_and_lower("{ users { __typename id } }", &json!({}), None, &schema()).unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        match &roots[0].body {
            crate::ast::RootBody::List { selection } => {
                assert!(
                    matches!(&selection[0], Field::Typename { alias } if alias == "__typename")
                );
            }
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn repeated_typename_collapses() {
        // Apollo injects `__typename` into every selection set, so a fragment
        // spread routinely asks for it twice.
        let op = parse_and_lower(
            "fragment F on users { __typename id } { users { __typename ...F } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        match &roots[0].body {
            crate::ast::RootBody::List { selection } => assert_eq!(selection.len(), 2),
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn typename_takes_no_arguments_and_is_not_a_root_field() {
        let err = parse_and_lower(
            "{ users { __typename(x: 1) } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("takes no arguments"), "{err}");

        let err = parse_and_lower("{ __typename }", &json!({}), None, &schema()).unwrap_err();
        assert!(format!("{err}").contains("not as a root field"), "{err}");
    }

    #[test]
    fn typename_is_accepted_at_every_aggregate_level() {
        let op = parse_and_lower(
            "{ users_aggregate { __typename aggregate { __typename max { __typename id } } \
              nodes { __typename } } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        match &roots[0].body {
            crate::ast::RootBody::Aggregate {
                ops,
                nodes,
                typenames,
                ..
            } => {
                assert_eq!(typenames, &vec!["__typename".to_string()]);
                assert!(matches!(ops[0].op, crate::ast::AggOp::Typename));
                match &ops[1].op {
                    crate::ast::AggOp::Func { fields, .. } => {
                        assert!(matches!(fields[0], crate::ast::AggField::Typename { .. }));
                    }
                    other => panic!("expected Max, got {other:?}"),
                }
                assert!(matches!(nodes.as_ref().unwrap()[0], Field::Typename { .. }));
            }
            _ => panic!("expected Aggregate"),
        }
    }

    /// A fragment that spreads itself. Before this was rejected, lowering
    /// recursed until the stack ran out and the process aborted — reaching the
    /// assertion at all is most of the test.
    #[test]
    fn a_fragment_cycle_is_rejected() {
        let err = parse_and_lower(
            "fragment F on users { id ...F } { users { ...F } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("fragment cycle"), "{err}");

        // Indirect, and through a field rather than at the top level.
        let err = parse_and_lower(
            "fragment A on users { id ...B } fragment B on users { ...A } { users { ...A } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("fragment cycle"), "{err}");
    }

    /// The same hazard by another route: no cycle, just a long chain. Each
    /// fragment nests one bracket deep, so the pre-parse text guard sees
    /// nothing.
    #[test]
    fn an_over_deep_fragment_chain_is_rejected() {
        let mut q = String::from("{ users { ...F0 } }");
        for i in 0..500 {
            q.push_str(&format!(" fragment F{i} on users {{ ...F{} }}", i + 1));
        }
        q.push_str(" fragment F500 on users { id }");
        let err = parse_and_lower(&q, &json!({}), None, &schema()).unwrap_err();
        assert!(format!("{err}").contains("deeper than the limit"), "{err}");
    }

    #[test]
    fn a_fragment_spread_twice_is_not_a_cycle() {
        parse_and_lower(
            "fragment F on users { id } { users { ...F } others: users { ...F } }",
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
    }

    #[test]
    fn directives_are_rejected_in_every_position() {
        for q in [
            "query Q @skip(if: true) { users { id } }",
            "query Q($n: Int @foo) { users(limit: $n) { id } }",
            "fragment F on users @foo { id } { users { ...F } }",
            "{ users { id @include(if: true) } }",
            "{ users { ... on users @foo { id } } }",
            "fragment F on users { id } { users { ...F @foo } }",
        ] {
            let err = parse_and_lower(q, &json!({}), None, &schema()).unwrap_err();
            assert!(
                format!("{err}").contains("directives are not supported"),
                "{q} -> {err}"
            );
        }
    }

    #[test]
    fn a_repeated_typename_answers_under_one_key() {
        let op = parse_and_lower(
            r#"mutation { insert_users(objects: [{name: "a"}]) {
                 __typename affected_rows t: __typename __typename } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        let Operation::Mutation(fields) = op else {
            panic!("expected Mutation");
        };
        match &fields[0] {
            crate::ast::MutationField::Insert {
                response_typenames, ..
            } => {
                // Non-adjacent repeats too: `Vec::dedup` would have kept both.
                assert_eq!(
                    response_typenames,
                    &vec!["__typename".to_string(), "t".to_string()]
                );
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    /// `on_conflict.where` used to be lowered against an empty schema, so a
    /// relation predicate failed with "relation target table missing" — naming
    /// the wrong cause, and making the shape unwritable.
    #[test]
    fn an_on_conflict_where_may_walk_a_relation() {
        let op = parse_and_lower(
            r#"mutation { insert_users(objects: [{id: 1}], on_conflict: {
                 constraint: "users_pkey", update_columns: ["id"],
                 where: {posts: {id: {_gt: 1}}}
               }) { affected_rows } }"#,
            &json!({}),
            None,
            &schema_with_relations(),
        )
        .unwrap();
        let Operation::Mutation(fields) = op else {
            panic!("expected Mutation");
        };
        match &fields[0] {
            crate::ast::MutationField::Insert { on_conflict, .. } => {
                let w = on_conflict.as_ref().unwrap().where_.as_ref().unwrap();
                assert!(matches!(w, BoolExpr::Relation { name, .. } if name == "posts"));
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_on_conflict_constraint_is_caught_before_postgres_sees_it() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("email", "email", PgType::Text, true)
                    .primary_key(&["id"])
                    .unique_constraint("users_pkey", &["id"])
                    .unique_constraint("users_email_key", &["email"]),
            )
            .build();
        let q = |c: &str| {
            format!(
                r#"mutation {{ insert_users(objects: [{{id: 1}}], on_conflict: {{
                     constraint: "{c}", update_columns: ["id"] }}) {{ affected_rows }} }}"#
            )
        };
        let err = parse_and_lower(&q("users_emial_key"), &json!({}), None, &schema).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("is not a unique constraint"), "{msg}");
        assert!(
            msg.contains("users_email_key"),
            "the known ones are listed: {msg}"
        );
        parse_and_lower(&q("users_email_key"), &json!({}), None, &schema).unwrap();
    }

    /// A hand-built schema declares no constraints. Enforcing an empty list
    /// would be enforcing one that was never claimed to be complete.
    #[test]
    fn a_schema_that_declares_no_constraints_accepts_any_name() {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build();
        parse_and_lower(
            r#"mutation { insert_users(objects: [{id: 1}], on_conflict: {
                 constraint: "whatever_the_database_calls_it", update_columns: ["id"]
               }) { affected_rows } }"#,
            &json!({}),
            None,
            &schema,
        )
        .unwrap();
    }

    /// `col = NULL` is no rows, not "the rows whose column is null" — so the
    /// answer to `_eq: null` looked like a right answer to a question nobody
    /// asked. Refused, with the operator that does mean it.
    #[test]
    fn comparing_against_null_is_refused_and_names_is_null() {
        let render = |q: &str, vars: serde_json::Value| {
            let s = schema();
            parse_and_lower(q, &vars, None, &s).and_then(|op| {
                crate::sql::render_now(&op, &s, &crate::types::Inputs::none()).map(|_| ())
            })
        };

        for q in [
            "{ users(where: {id: {_eq: null}}) { id } }",
            "{ users(where: {id: {_neq: null}}) { id } }",
            "{ users(where: {id: {_gt: null}}) { id } }",
            "{ users(where: {_or: [{id: {_eq: null}}]}) { id } }",
            "{ users_by_pk(id: null) { id } }",
        ] {
            let err = render(q, json!({})).unwrap_err();
            assert!(format!("{err}").contains("_is_null"), "{q} -> {err}");
        }

        // Through a variable, which is the shape a client produces by leaving
        // an optional filter unset.
        let err = render(
            "query($x: Int) { users(where: {id: {_eq: $x}}) { id } }",
            json!({ "x": null }),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("_is_null"), "{err}");

        // `_is_null` itself, and a null where it is a value, are untouched.
        render(
            "{ users(where: {name: {_is_null: true}}) { id } }",
            json!({}),
        )
        .unwrap();
        render(
            r#"mutation { insert_users(objects: [{name: null}]) { affected_rows } }"#,
            json!({}),
        )
        .unwrap();
        render(
            r#"mutation { update_users(where: {id: {_eq: 1}}, _set: {name: null}) {
                 affected_rows } }"#,
            json!({}),
        )
        .unwrap();
    }

    /// A name that matches nothing is an error however many operations the
    /// document holds — running the only one there would answer about that one
    /// when the caller asked about another.
    #[test]
    fn an_operation_name_that_matches_nothing_is_refused() {
        let s = schema();
        let doc = parse_document("query Solo { users { id } }").unwrap();
        assert!(lower(&doc, &json!({}), Some("Solo"), &s).is_ok());
        let err = lower(&doc, &json!({}), Some("Other"), &s).unwrap_err();
        assert!(format!("{err}").contains("'Other' not found"), "{err}");

        // An anonymous operation has no name to match.
        let doc = parse_document("{ users { id } }").unwrap();
        assert!(lower(&doc, &json!({}), None, &s).is_ok());
        let err = lower(&doc, &json!({}), Some("Anything"), &s).unwrap_err();
        assert!(format!("{err}").contains("'Anything' not found"), "{err}");
    }

    #[test]
    fn a_relation_aggregate_lowers_against_the_target_table() {
        let s = schema_with_relations();
        let op = parse_and_lower(
            "{ users { posts_aggregate(where: {id: {_gt: 1}}) { aggregate { count } } } }",
            &json!({}),
            None,
            &s,
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!("expected Query");
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!("expected List");
        };
        match &selection[0] {
            Field::RelationAggregate {
                name, args, ops, ..
            } => {
                assert_eq!(name, "posts");
                // The `where` belongs to the target, not the parent.
                assert!(args.where_.is_some());
                assert_eq!(ops.len(), 1);
            }
            other => panic!("expected RelationAggregate, got {other:?}"),
        }
    }

    /// A column is the thing that exists; a synthesized field must not shadow
    /// one — the same rule that keeps an auto-derived relation from doing it.
    #[test]
    fn a_real_column_wins_over_the_synthesized_aggregate_field() {
        let s = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("posts_aggregate", "posts_aggregate", PgType::Text, true)
                    .primary_key(&["id"])
                    .relation(
                        "posts",
                        crate::schema::Relation::array("posts").on([("id", "user_id")]),
                    ),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build();
        let op = parse_and_lower("{ users { posts_aggregate } }", &json!({}), None, &s).unwrap();
        let Operation::Query(roots) = op else {
            panic!()
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!()
        };
        assert!(
            matches!(&selection[0], Field::Column { column, .. } if column == "posts_aggregate"),
            "{:?}",
            selection[0]
        );
    }

    #[test]
    fn an_unknown_aggregate_field_says_what_it_looked_for() {
        let err = parse_and_lower("{ users { nope_aggregate } }", &json!({}), None, &schema())
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("no array relation 'nope'"), "{msg}");
    }

    #[test]
    fn a_relation_aggregate_spread_twice_collapses() {
        let s = schema_with_relations();
        let op = parse_and_lower(
            "fragment F on users { posts_aggregate { aggregate { count } } } \
             { users { posts_aggregate { aggregate { count } } ...F } }",
            &json!({}),
            None,
            &s,
        )
        .unwrap();
        let Operation::Query(roots) = op else {
            panic!()
        };
        let crate::ast::RootBody::List { selection } = &roots[0].body else {
            panic!()
        };
        assert_eq!(selection.len(), 1, "{selection:?}");

        // Differing ones under one key still have no single answer.
        let err = parse_and_lower(
            "{ users { posts_aggregate { aggregate { count } } \
               posts_aggregate { aggregate { max { id } } } } }",
            &json!({}),
            None,
            &s,
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("both answer to 'posts_aggregate'"),
            "{err}"
        );
    }

    #[test]
    fn an_aggregate_refuses_distinct_on_rather_than_dropping_it() {
        for q in [
            "{ users_aggregate(distinct_on: [id]) { aggregate { count } } }",
            "{ users { posts_aggregate(distinct_on: [id]) { aggregate { count } } } }",
        ] {
            let err = parse_and_lower(q, &json!({}), None, &schema_with_relations()).unwrap_err();
            assert!(
                format!("{err}").contains("cannot take 'distinct_on'"),
                "{q} -> {err}"
            );
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
