# Vision-GraphQL Phase 4 — Mutations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support Hasura-style mutations — `insert_users` / `insert_users_one` / `update_users` / `update_users_by_pk` / `delete_users` / `delete_users_by_pk` with `on_conflict` and `returning` — producing a single atomic SQL per operation.

**Architecture:** Extend `Operation` with `Mutation(Vec<MutationField>)`. Each `MutationField` becomes one CTE in the generated SQL; the outer `SELECT json_build_object(...)` assembles the result. List-form mutations return `{affected_rows, returning: [...]}`; `_by_pk` and `_one` forms return a single object or null via `LIMIT 1 + json_build_object`. Parser detects `insert_` / `update_` / `delete_` prefixes and routes accordingly. `on_conflict` becomes a PG `ON CONFLICT ON CONSTRAINT <name> DO UPDATE SET ...` clause (constraint name passed through unvalidated — introspection in Phase 5 will add validation).

**Tech Stack:** No new deps.

**Out of scope:**
- `returning` with nested relations (MVP: columns only)
- `_inc`, `_append`, `_prepend` column ops (only `_set` for updates)
- Cross-mutation data references
- Transactions beyond the single-statement-per-operation atomicity PG already provides

---

## File Structure

```
src/ast.rs      # Operation::Mutation, MutationField enum, OnConflict
src/sql.rs      # render Mutation branch, per-variant renderers
src/parser.rs   # detect mutation ops, dispatch by field prefix
src/engine.rs   # accept mutations through same Engine::query entry point
tests/integration_mutation.rs   # new
```

---

### Task 1: IR — MutationField and OnConflict

**Files:**
- Modify: `src/ast.rs`

- [ ] **Step 1: Extend Operation enum**

In `src/ast.rs`, replace `pub enum Operation` with:

```rust
#[derive(Debug, Clone)]
pub enum Operation {
    Query(Vec<RootField>),
    Mutation(Vec<MutationField>),
}
```

- [ ] **Step 2: Add MutationField and OnConflict**

Append to `src/ast.rs` (after `RootBody` / `AggOp`):

```rust
#[derive(Debug, Clone)]
pub enum MutationField {
    Insert {
        alias: String,
        table: String,
        /// Each inner map is `{ exposed_column -> value }` for one row to insert.
        objects: Vec<std::collections::BTreeMap<String, serde_json::Value>>,
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
```

- [ ] **Step 3: Smoke test**

Append to the `tests` mod in `src/ast.rs`:

```rust
    #[test]
    fn build_insert_mutation() {
        use std::collections::BTreeMap;
        let mut obj = BTreeMap::new();
        obj.insert("name".to_string(), serde_json::json!("alice"));
        let m = MutationField::Insert {
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![obj],
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
```

- [ ] **Step 4: Compile and run**

Run: `cargo test --lib ast::tests`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/ast.rs
git commit -m "feat(ast): Operation::Mutation with MutationField variants"
```

---

### Task 2: Parser — detect mutation operation

**Files:**
- Modify: `src/parser.rs`

- [ ] **Step 1: Route mutation operations to a new lowering function**

In `src/parser.rs`, replace the `match op.ty` block inside `parse_and_lower` with:

```rust
    match op.ty {
        OperationType::Query => lower_query(op.selection_set, schema, variables),
        OperationType::Mutation => lower_mutation(op.selection_set, schema, variables),
        OperationType::Subscription => Err(Error::Parse("subscriptions are not supported".into())),
    }
```

Append a stub `lower_mutation` function right after `lower_query`:

```rust
fn lower_mutation(
    set: &SelectionSet,
    schema: &Schema,
    vars: &Value,
) -> Result<Operation> {
    let mut fields: Vec<crate::ast::MutationField> = Vec::new();
    for sel in &set.items {
        let Selection::Field(f) = &sel.node else {
            return Err(Error::Parse(
                "fragments are not supported in mutations".into(),
            ));
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
    _field: &async_graphql_parser::types::Field,
    _schema: &Schema,
    _vars: &Value,
) -> Result<crate::ast::MutationField> {
    Err(Error::Validate {
        path: alias.into(),
        message: format!("mutation field '{name}' not yet supported"),
    })
}
```

- [ ] **Step 2: Import Field from parser types**

At the top of `src/parser.rs`, the existing import is `use async_graphql_parser::types::{DocumentOperations, ExecutableDocument, OperationType, Selection, SelectionSet};`. We also need the `Field` type for the stub's `_field` parameter. Update the import to:

```rust
use async_graphql_parser::types::{
    DocumentOperations, ExecutableDocument, Field as GqlField, OperationType, Selection,
    SelectionSet,
};
```

And change the stub's signature:

```rust
fn lower_mutation_field(
    name: &str,
    alias: &str,
    _field: &GqlField,
    _schema: &Schema,
    _vars: &Value,
) -> Result<crate::ast::MutationField> {
```

- [ ] **Step 3: Compile**

Run: `cargo build`
Expected: clean compile (warnings about unused parameters acceptable).

- [ ] **Step 4: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): route mutation operations via lower_mutation stub"
```

---

### Task 3: Parser — insert and insert_one

**Files:**
- Modify: `src/parser.rs`

- [ ] **Step 1: Implement lower_mutation_field for insert / insert_one**

Replace the stub `lower_mutation_field` with:

```rust
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

    Err(Error::Validate {
        path: alias.into(),
        message: format!("mutation field '{name}' not yet supported"),
    })
}

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
    let _ = schema; // reserved for future relation-aware inserts

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
                let json =
                    gql_to_json(v, vars, &format!("{parent_path}.on_conflict"))?;
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

fn parse_on_conflict(
    json: &Value,
    table: &Table,
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
        .map(|w| lower_where(w, table, &Schema::builder().build(), &format!("{path}.where")))
        .transpose()?;
    Ok(crate::ast::OnConflict {
        constraint,
        update_columns,
        where_,
    })
}

fn parse_returning(
    set: &SelectionSet,
    table: &Table,
    parent_path: &str,
) -> Result<Vec<Field>> {
    let mut returning: Vec<Field> = Vec::new();
    let mut saw_returning = false;
    let mut saw_affected_rows = false;
    for sel in &set.items {
        let Selection::Field(f) = &sel.node else {
            return Err(Error::Parse(
                "fragments not supported in mutation return".into(),
            ));
        };
        let field = &f.node;
        let fname = field.name.node.as_str();
        match fname {
            "affected_rows" => saw_affected_rows = true,
            "returning" => {
                saw_returning = true;
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
    let _ = (saw_returning, saw_affected_rows); // both are optional
    Ok(returning)
}
```

- [ ] **Step 2: Write tests**

Append to the `tests` mod in `src/parser.rs`:

```rust
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
                        objects, returning, one, ..
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
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib parser::tests::parse_insert_array`
Expected: PASS. Then run all parser tests:

Run: `cargo test --lib parser::tests`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): lower insert_<table> and insert_<table>_one mutations"
```

---

### Task 4: SQL — render Mutation entry point

**Files:**
- Modify: `src/sql.rs`

- [ ] **Step 1: Extend render() to branch on Operation**

In `src/sql.rs`, update `render`:

```rust
pub fn render(op: &Operation, schema: &Schema) -> Result<(String, Vec<Bind>)> {
    let mut ctx = RenderCtx::default();
    match op {
        Operation::Query(roots) => render_query(roots, schema, &mut ctx),
        Operation::Mutation(fields) => render_mutation(fields, schema, &mut ctx),
    }?;
    Ok((ctx.sql, ctx.binds))
}
```

Add the stub `render_mutation` after `render_query`:

```rust
fn render_mutation(
    fields: &[crate::ast::MutationField],
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::MutationField;
    // Each mutation becomes a CTE named m0, m1, ...
    ctx.sql.push_str("WITH ");
    for (i, mf) in fields.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let cte = format!("m{i}");
        match mf {
            MutationField::Insert {
                table,
                objects,
                on_conflict,
                ..
            } => {
                render_insert_cte(&cte, table, objects, on_conflict.as_ref(), schema, ctx)?;
            }
            _ => {
                return Err(Error::Validate {
                    path: "mutation".into(),
                    message: "mutation variant not yet implemented".into(),
                });
            }
        }
    }
    ctx.sql.push(' ');
    ctx.sql.push_str("SELECT json_build_object(");
    for (i, mf) in fields.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let cte = format!("m{i}");
        render_mutation_output_for(mf, &cte, schema, ctx)?;
    }
    ctx.sql.push_str(") AS result");
    Ok(())
}

fn render_insert_cte(
    cte: &str,
    table_name: &str,
    objects: &[std::collections::BTreeMap<String, serde_json::Value>],
    on_conflict: Option<&crate::ast::OnConflict>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;

    // Column list: union of all objects' keys, in sorted order (BTreeMap iteration).
    use std::collections::BTreeSet;
    let mut col_set: BTreeSet<String> = BTreeSet::new();
    for obj in objects {
        for k in obj.keys() {
            col_set.insert(k.clone());
        }
    }
    let cols: Vec<String> = col_set.into_iter().collect();

    write!(
        ctx.sql,
        "{cte} AS (INSERT INTO {}.{} (",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    for (i, exposed) in cols.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
            path: format!("{cte}.{exposed}"),
            message: format!("unknown column '{exposed}'"),
        })?;
        ctx.sql.push_str(&quote_ident(&col.physical_name));
    }
    ctx.sql.push_str(") VALUES ");

    for (r, obj) in objects.iter().enumerate() {
        if r > 0 {
            ctx.sql.push_str(", ");
        }
        ctx.sql.push('(');
        for (i, exposed) in cols.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(", ");
            }
            let value = obj.get(exposed);
            let col = table.find_column(exposed).unwrap();
            match value {
                None => {
                    // Column not supplied for this row -> DEFAULT.
                    ctx.sql.push_str("DEFAULT");
                }
                Some(v) => {
                    let bind = crate::types::json_to_bind(v, &col.pg_type).map_err(|e| {
                        Error::Validate {
                            path: format!("{cte}.objects[{r}].{exposed}"),
                            message: format!("{e}"),
                        }
                    })?;
                    ctx.binds.push(bind);
                    write!(ctx.sql, "${}", ctx.binds.len()).unwrap();
                }
            }
        }
        ctx.sql.push(')');
    }

    if let Some(oc) = on_conflict {
        render_on_conflict(oc, table, schema, ctx)?;
    }

    ctx.sql.push_str(" RETURNING *)");
    Ok(())
}

fn render_on_conflict(
    oc: &crate::ast::OnConflict,
    table: &Table,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    write!(
        ctx.sql,
        " ON CONFLICT ON CONSTRAINT {} ",
        quote_ident(&oc.constraint)
    )
    .unwrap();
    if oc.update_columns.is_empty() {
        ctx.sql.push_str("DO NOTHING");
    } else {
        ctx.sql.push_str("DO UPDATE SET ");
        for (i, exposed) in oc.update_columns.iter().enumerate() {
            if i > 0 {
                ctx.sql.push_str(", ");
            }
            let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
                path: format!("on_conflict.update_columns.{exposed}"),
                message: format!("unknown column '{exposed}' on '{}'", table.exposed_name),
            })?;
            write!(
                ctx.sql,
                "{} = EXCLUDED.{}",
                quote_ident(&col.physical_name),
                quote_ident(&col.physical_name),
            )
            .unwrap();
        }
        if let Some(expr) = oc.where_.as_ref() {
            ctx.sql.push_str(" WHERE ");
            render_bool_expr_no_alias(expr, table, schema, ctx)?;
        }
    }
    Ok(())
}

fn render_mutation_output_for(
    mf: &crate::ast::MutationField,
    cte: &str,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    use crate::ast::MutationField;
    match mf {
        MutationField::Insert {
            alias,
            table,
            returning,
            one,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(ctx.sql, "'{}', ", escape_string_literal(alias)).unwrap();
            if *one {
                // single object via LIMIT 1
                ctx.sql.push_str("(SELECT ");
                if returning.is_empty() {
                    ctx.sql.push_str("'{}'::json");
                } else {
                    render_json_build_object_for_nodes(returning, cte, tbl, alias, ctx)?;
                }
                write!(ctx.sql, " FROM {cte} LIMIT 1)").unwrap();
            } else {
                ctx.sql.push_str("json_build_object(");
                write!(
                    ctx.sql,
                    "'affected_rows', (SELECT count(*) FROM {cte})"
                )
                .unwrap();
                if !returning.is_empty() {
                    ctx.sql.push_str(", 'returning', (SELECT coalesce(json_agg(");
                    render_json_build_object_for_nodes(returning, cte, tbl, alias, ctx)?;
                    write!(ctx.sql, "), '[]'::json) FROM {cte})").unwrap();
                } else {
                    ctx.sql
                        .push_str(", 'returning', '[]'::json");
                }
                ctx.sql.push(')');
            }
        }
        _ => {
            return Err(Error::Validate {
                path: "mutation".into(),
                message: "mutation variant not yet implemented".into(),
            });
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Write SQL snapshot test**

Append to the `tests` mod in `src/sql.rs`:

```rust
    #[test]
    fn render_insert_array_with_returning() {
        use crate::ast::MutationField;
        use std::collections::BTreeMap;

        let mut obj = BTreeMap::new();
        obj.insert("name".to_string(), serde_json::json!("alice"));
        let op = Operation::Mutation(vec![MutationField::Insert {
            alias: "insert_users".into(),
            table: "users".into(),
            objects: vec![obj],
            on_conflict: None,
            returning: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
            one: false,
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert_eq!(binds.len(), 1);
    }

    #[test]
    fn render_insert_one() {
        use crate::ast::MutationField;
        use std::collections::BTreeMap;

        let mut obj = BTreeMap::new();
        obj.insert("name".to_string(), serde_json::json!("alice"));
        let op = Operation::Mutation(vec![MutationField::Insert {
            alias: "insert_users_one".into(),
            table: "users".into(),
            objects: vec![obj],
            on_conflict: None,
            returning: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
            one: true,
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }
```

- [ ] **Step 3: Accept snapshots**

Run: `INSTA_UPDATE=always cargo test --lib sql::tests`
Expected: both new snapshots accepted; all tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "feat(sql): render Insert mutations with RETURNING and on_conflict"
```

---

### Task 5: Integration — inserts

**Files:**
- Create: `tests/integration_mutation.rs`

- [ ] **Step 1: Write integration test**

Create `tests/integration_mutation.rs`:

```rust
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::Engine;

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .column("age", "age", PgType::Int4, true)
                .primary_key(&["id"]),
        )
        .build()
}

async fn setup() -> (
    Engine,
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .expect("start pg");
    let host_port = container.get_host_port_ipv4(5432).await.expect("port");
    let mut cfg = Config::new();
    cfg.host = Some("127.0.0.1".into());
    cfg.port = Some(host_port);
    cfg.user = Some("postgres".into());
    cfg.password = Some("postgres".into());
    cfg.dbname = Some("postgres".into());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).expect("pool");
    {
        let client = pool.get().await.expect("client");
        client
            .batch_execute(
                r#"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    age INT
                );
                "#,
            )
            .await
            .expect("seed");
    }
    let engine = Engine::new(pool, schema());
    (engine, container)
}

#[tokio::test]
async fn insert_array_returns_affected_rows_and_returning() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation { insert_users(objects: [{name: "alice", age: 30}, {name: "bob"}]) { affected_rows returning { id name } } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(2));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|r| r["name"] == json!("alice")));
}

#[tokio::test]
async fn insert_one_returns_single_object() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation { insert_users_one(object: {name: "cara"}) { id name } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let one = &v["insert_users_one"];
    assert_eq!(one["name"], json!("cara"));
    assert!(one["id"].is_number());
}

#[tokio::test]
async fn insert_with_on_conflict_do_update() {
    let (engine, _c) = setup().await;
    // Seed
    let _ = engine
        .query(
            r#"mutation { insert_users_one(object: {name: "dup", age: 1}) { id } }"#,
            None,
        )
        .await
        .expect("seed ok");
    // Conflict on name unique constraint; update age
    let v: Value = engine
        .query(
            r#"mutation { insert_users(
                objects: [{name: "dup", age: 99}],
                on_conflict: {constraint: "users_name_key", update_columns: ["age"]}
            ) { affected_rows returning { name age } } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let ret = &v["insert_users"]["returning"];
    assert_eq!(ret[0]["name"], json!("dup"));
    assert_eq!(ret[0]["age"], json!(99));
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test integration_mutation -- --test-threads=1`
Expected: 3 PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_mutation.rs
git commit -m "test: e2e insert / insert_one / on_conflict"
```

---

### Task 6: Parser — update and update_by_pk

**Files:**
- Modify: `src/parser.rs`

- [ ] **Step 1: Extend lower_mutation_field**

Inside `lower_mutation_field`, add these blocks before the final `Err(...)`:

```rust
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
                let (pk, set) =
                    parse_update_by_pk_args(&field.arguments, table, vars, alias)?;
                let selection =
                    lower_selection_columns_only(&field.selection_set.node, table, alias)?;
                return Ok(crate::ast::MutationField::UpdateByPk {
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
            return Ok(crate::ast::MutationField::Update {
                alias: alias.to_string(),
                table: base_name.to_string(),
                where_,
                set,
                returning,
            });
        }
    }
```

Append the helpers at the end of `src/parser.rs` (before `#[cfg(test)]`):

```rust
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
```

- [ ] **Step 2: Write tests**

Append:

```rust
    #[test]
    fn parse_update_by_where() {
        let op = parse_and_lower(
            r#"mutation { update_users(where: {id: {_eq: 1}}, _set: {name: "z"}) { affected_rows returning { id } } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        match op {
            Operation::Mutation(fields) => match &fields[0] {
                crate::ast::MutationField::Update { set, returning, .. } => {
                    assert!(set.contains_key("name"));
                    assert_eq!(returning.len(), 1);
                }
                _ => panic!("expected Update"),
            },
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn parse_update_by_pk() {
        let op = parse_and_lower(
            r#"mutation { update_users_by_pk(pk_columns: {id: 1}, _set: {name: "z"}) { id name } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        match op {
            Operation::Mutation(fields) => match &fields[0] {
                crate::ast::MutationField::UpdateByPk {
                    pk,
                    set,
                    selection,
                    ..
                } => {
                    assert_eq!(pk.len(), 1);
                    assert_eq!(pk[0].0, "id");
                    assert!(set.contains_key("name"));
                    assert_eq!(selection.len(), 2);
                }
                _ => panic!("expected UpdateByPk"),
            },
            _ => panic!("expected Mutation"),
        }
    }
```

- [ ] **Step 3: Run**

Run: `cargo test --lib parser::tests::parse_update_by_pk`
Expected: PASS.

Run: `cargo test --lib parser::tests`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): lower update_<table> and update_<table>_by_pk"
```

---

### Task 7: SQL — render Update and UpdateByPk

**Files:**
- Modify: `src/sql.rs`

- [ ] **Step 1: Implement CTE and output rendering**

In `src/sql.rs`, inside `render_mutation` replace the match arm:

```rust
        match mf {
            MutationField::Insert {
                table,
                objects,
                on_conflict,
                ..
            } => {
                render_insert_cte(&cte, table, objects, on_conflict.as_ref(), schema, ctx)?;
            }
            MutationField::Update {
                table, where_, set, ..
            } => {
                render_update_cte(&cte, table, where_, set, schema, ctx)?;
            }
            MutationField::UpdateByPk {
                table, pk, set, ..
            } => {
                render_update_by_pk_cte(&cte, table, pk, set, schema, ctx)?;
            }
            _ => {
                return Err(Error::Validate {
                    path: "mutation".into(),
                    message: "mutation variant not yet implemented".into(),
                });
            }
        }
```

Add helpers:

```rust
fn render_update_cte(
    cte: &str,
    table_name: &str,
    where_: &crate::ast::BoolExpr,
    set: &std::collections::BTreeMap<String, serde_json::Value>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;
    write!(
        ctx.sql,
        "{cte} AS (UPDATE {}.{} SET ",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    for (i, (exposed, value)) in set.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
            path: format!("{cte}._set.{exposed}"),
            message: format!("unknown column '{exposed}'"),
        })?;
        let bind = crate::types::json_to_bind(value, &col.pg_type).map_err(|e| {
            Error::Validate {
                path: format!("{cte}._set.{exposed}"),
                message: format!("{e}"),
            }
        })?;
        ctx.binds.push(bind);
        write!(
            ctx.sql,
            "{} = ${}",
            quote_ident(&col.physical_name),
            ctx.binds.len()
        )
        .unwrap();
    }
    ctx.sql.push_str(" WHERE ");
    render_bool_expr_no_alias(where_, table, schema, ctx)?;
    ctx.sql.push_str(" RETURNING *)");
    Ok(())
}

fn render_update_by_pk_cte(
    cte: &str,
    table_name: &str,
    pk: &[(String, serde_json::Value)],
    set: &std::collections::BTreeMap<String, serde_json::Value>,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;
    write!(
        ctx.sql,
        "{cte} AS (UPDATE {}.{} SET ",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    for (i, (exposed, value)) in set.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        let col = table.find_column(exposed).ok_or_else(|| Error::Validate {
            path: format!("{cte}._set.{exposed}"),
            message: format!("unknown column '{exposed}'"),
        })?;
        let bind = crate::types::json_to_bind(value, &col.pg_type).map_err(|e| {
            Error::Validate {
                path: format!("{cte}._set.{exposed}"),
                message: format!("{e}"),
            }
        })?;
        ctx.binds.push(bind);
        write!(
            ctx.sql,
            "{} = ${}",
            quote_ident(&col.physical_name),
            ctx.binds.len()
        )
        .unwrap();
    }
    ctx.sql.push_str(" WHERE ");
    for (i, (col_name, value)) in pk.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(" AND ");
        }
        let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
            path: format!("{cte}.pk.{col_name}"),
            message: format!("unknown column '{col_name}'"),
        })?;
        let bind =
            crate::types::json_to_bind(value, &col.pg_type).map_err(|e| Error::Validate {
                path: format!("{cte}.pk.{col_name}"),
                message: format!("{e}"),
            })?;
        ctx.binds.push(bind);
        write!(
            ctx.sql,
            "{} = ${}",
            quote_ident(&col.physical_name),
            ctx.binds.len()
        )
        .unwrap();
    }
    ctx.sql.push_str(" RETURNING *)");
    Ok(())
}
```

- [ ] **Step 2: Extend render_mutation_output_for**

Replace the match body in `render_mutation_output_for` with:

```rust
    match mf {
        MutationField::Insert {
            alias,
            table,
            returning,
            one,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(ctx.sql, "'{}', ", escape_string_literal(alias)).unwrap();
            if *one {
                ctx.sql.push_str("(SELECT ");
                if returning.is_empty() {
                    ctx.sql.push_str("'{}'::json");
                } else {
                    render_json_build_object_for_nodes(returning, cte, tbl, alias, ctx)?;
                }
                write!(ctx.sql, " FROM {cte} LIMIT 1)").unwrap();
            } else {
                ctx.sql.push_str("json_build_object(");
                write!(ctx.sql, "'affected_rows', (SELECT count(*) FROM {cte})").unwrap();
                if !returning.is_empty() {
                    ctx.sql.push_str(", 'returning', (SELECT coalesce(json_agg(");
                    render_json_build_object_for_nodes(returning, cte, tbl, alias, ctx)?;
                    write!(ctx.sql, "), '[]'::json) FROM {cte})").unwrap();
                } else {
                    ctx.sql.push_str(", 'returning', '[]'::json");
                }
                ctx.sql.push(')');
            }
        }
        MutationField::Update {
            alias,
            table,
            returning,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(ctx.sql, "'{}', json_build_object(", escape_string_literal(alias)).unwrap();
            write!(ctx.sql, "'affected_rows', (SELECT count(*) FROM {cte})").unwrap();
            if !returning.is_empty() {
                ctx.sql.push_str(", 'returning', (SELECT coalesce(json_agg(");
                render_json_build_object_for_nodes(returning, cte, tbl, alias, ctx)?;
                write!(ctx.sql, "), '[]'::json) FROM {cte})").unwrap();
            } else {
                ctx.sql.push_str(", 'returning', '[]'::json");
            }
            ctx.sql.push(')');
        }
        MutationField::UpdateByPk {
            alias,
            table,
            selection,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(ctx.sql, "'{}', (SELECT ", escape_string_literal(alias)).unwrap();
            if selection.is_empty() {
                ctx.sql.push_str("'{}'::json");
            } else {
                render_json_build_object_for_nodes(selection, cte, tbl, alias, ctx)?;
            }
            write!(ctx.sql, " FROM {cte} LIMIT 1)").unwrap();
        }
        _ => {
            return Err(Error::Validate {
                path: "mutation".into(),
                message: "mutation variant not yet implemented".into(),
            });
        }
    }
```

- [ ] **Step 3: Write snapshot tests**

Append to `src/sql.rs` tests:

```rust
    #[test]
    fn render_update_by_where() {
        use crate::ast::{BoolExpr, CmpOp, MutationField};
        use serde_json::json;
        use std::collections::BTreeMap;

        let mut set = BTreeMap::new();
        set.insert("name".to_string(), json!("z"));
        let op = Operation::Mutation(vec![MutationField::Update {
            alias: "update_users".into(),
            table: "users".into(),
            where_: BoolExpr::Compare {
                column: "id".into(),
                op: CmpOp::Eq,
                value: json!(1),
            },
            set,
            returning: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_update_by_pk() {
        use crate::ast::MutationField;
        use serde_json::json;
        use std::collections::BTreeMap;

        let mut set = BTreeMap::new();
        set.insert("name".to_string(), json!("z"));
        let op = Operation::Mutation(vec![MutationField::UpdateByPk {
            alias: "update_users_by_pk".into(),
            table: "users".into(),
            pk: vec![("id".into(), json!(5))],
            set,
            selection: vec![Field::Column {
                physical: "name".into(),
                alias: "name".into(),
            }],
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }
```

- [ ] **Step 4: Accept snapshots**

Run: `INSTA_UPDATE=always cargo test --lib sql::tests`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "feat(sql): render Update and UpdateByPk mutations"
```

---

### Task 8: Parser — delete and delete_by_pk

**Files:**
- Modify: `src/parser.rs`

- [ ] **Step 1: Extend lower_mutation_field**

Add these blocks inside `lower_mutation_field` before the final `Err`:

```rust
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
                return Ok(crate::ast::MutationField::DeleteByPk {
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
            return Ok(crate::ast::MutationField::Delete {
                alias: alias.to_string(),
                table: base_name.to_string(),
                where_,
                returning,
            });
        }
    }
```

- [ ] **Step 2: Tests**

Append:

```rust
    #[test]
    fn parse_delete_by_where() {
        let op = parse_and_lower(
            r#"mutation { delete_users(where: {id: {_eq: 1}}) { affected_rows returning { id } } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        match op {
            Operation::Mutation(fields) => match &fields[0] {
                crate::ast::MutationField::Delete { returning, .. } => {
                    assert_eq!(returning.len(), 1);
                }
                _ => panic!("expected Delete"),
            },
            _ => panic!("expected Mutation"),
        }
    }

    #[test]
    fn parse_delete_by_pk() {
        let op = parse_and_lower(
            r#"mutation { delete_users_by_pk(id: 1) { id name } }"#,
            &json!({}),
            None,
            &schema(),
        )
        .unwrap();
        match op {
            Operation::Mutation(fields) => match &fields[0] {
                crate::ast::MutationField::DeleteByPk {
                    pk, selection, ..
                } => {
                    assert_eq!(pk.len(), 1);
                    assert_eq!(selection.len(), 2);
                }
                _ => panic!("expected DeleteByPk"),
            },
            _ => panic!("expected Mutation"),
        }
    }
```

- [ ] **Step 3: Run**

Run: `cargo test --lib parser::tests`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add src/parser.rs
git commit -m "feat(parser): lower delete_<table> and delete_<table>_by_pk"
```

---

### Task 9: SQL — render Delete and DeleteByPk

**Files:**
- Modify: `src/sql.rs`

- [ ] **Step 1: Add CTE helpers**

In `src/sql.rs`, extend `render_mutation`'s match arm to handle delete variants (replace the `_ => ...` fallback with real arms):

```rust
            MutationField::Delete { table, where_, .. } => {
                render_delete_cte(&cte, table, where_, schema, ctx)?;
            }
            MutationField::DeleteByPk { table, pk, .. } => {
                render_delete_by_pk_cte(&cte, table, pk, schema, ctx)?;
            }
```

Add helpers:

```rust
fn render_delete_cte(
    cte: &str,
    table_name: &str,
    where_: &crate::ast::BoolExpr,
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;
    write!(
        ctx.sql,
        "{cte} AS (DELETE FROM {}.{} WHERE ",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    render_bool_expr_no_alias(where_, table, schema, ctx)?;
    ctx.sql.push_str(" RETURNING *)");
    Ok(())
}

fn render_delete_by_pk_cte(
    cte: &str,
    table_name: &str,
    pk: &[(String, serde_json::Value)],
    schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let table = schema.table(table_name).ok_or_else(|| Error::Validate {
        path: cte.into(),
        message: format!("unknown table '{table_name}'"),
    })?;
    write!(
        ctx.sql,
        "{cte} AS (DELETE FROM {}.{} WHERE ",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    for (i, (col_name, value)) in pk.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(" AND ");
        }
        let col = table.find_column(col_name).ok_or_else(|| Error::Validate {
            path: format!("{cte}.pk.{col_name}"),
            message: format!("unknown column '{col_name}'"),
        })?;
        let bind =
            crate::types::json_to_bind(value, &col.pg_type).map_err(|e| Error::Validate {
                path: format!("{cte}.pk.{col_name}"),
                message: format!("{e}"),
            })?;
        ctx.binds.push(bind);
        write!(
            ctx.sql,
            "{} = ${}",
            quote_ident(&col.physical_name),
            ctx.binds.len()
        )
        .unwrap();
    }
    ctx.sql.push_str(" RETURNING *)");
    Ok(())
}
```

- [ ] **Step 2: Extend render_mutation_output_for**

Replace the trailing `_ => ...` fallback in `render_mutation_output_for` with:

```rust
        MutationField::Delete {
            alias,
            table,
            returning,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(ctx.sql, "'{}', json_build_object(", escape_string_literal(alias)).unwrap();
            write!(ctx.sql, "'affected_rows', (SELECT count(*) FROM {cte})").unwrap();
            if !returning.is_empty() {
                ctx.sql.push_str(", 'returning', (SELECT coalesce(json_agg(");
                render_json_build_object_for_nodes(returning, cte, tbl, alias, ctx)?;
                write!(ctx.sql, "), '[]'::json) FROM {cte})").unwrap();
            } else {
                ctx.sql.push_str(", 'returning', '[]'::json");
            }
            ctx.sql.push(')');
        }
        MutationField::DeleteByPk {
            alias,
            table,
            selection,
            ..
        } => {
            let tbl = schema.table(table).ok_or_else(|| Error::Validate {
                path: alias.clone(),
                message: format!("unknown table '{table}'"),
            })?;
            write!(ctx.sql, "'{}', (SELECT ", escape_string_literal(alias)).unwrap();
            if selection.is_empty() {
                ctx.sql.push_str("'{}'::json");
            } else {
                render_json_build_object_for_nodes(selection, cte, tbl, alias, ctx)?;
            }
            write!(ctx.sql, " FROM {cte} LIMIT 1)").unwrap();
        }
```

- [ ] **Step 3: Tests**

Append:

```rust
    #[test]
    fn render_delete_by_where() {
        use crate::ast::{BoolExpr, CmpOp, MutationField};
        use serde_json::json;

        let op = Operation::Mutation(vec![MutationField::Delete {
            alias: "delete_users".into(),
            table: "users".into(),
            where_: BoolExpr::Compare {
                column: "id".into(),
                op: CmpOp::Eq,
                value: json!(1),
            },
            returning: vec![Field::Column {
                physical: "id".into(),
                alias: "id".into(),
            }],
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn render_delete_by_pk() {
        use crate::ast::MutationField;
        use serde_json::json;

        let op = Operation::Mutation(vec![MutationField::DeleteByPk {
            alias: "delete_users_by_pk".into(),
            table: "users".into(),
            pk: vec![("id".into(), json!(5))],
            selection: vec![Field::Column {
                physical: "name".into(),
                alias: "name".into(),
            }],
        }]);
        let (sql, _) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
    }
```

- [ ] **Step 4: Accept snapshots**

Run: `INSTA_UPDATE=always cargo test --lib sql::tests`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql.rs src/snapshots/
git commit -m "feat(sql): render Delete and DeleteByPk mutations"
```

---

### Task 10: Integration — updates + deletes

**Files:**
- Modify: `tests/integration_mutation.rs`

- [ ] **Step 1: Append update + delete tests**

Append to `tests/integration_mutation.rs`:

```rust
#[tokio::test]
async fn update_by_where_affected_rows_and_returning() {
    let (engine, _c) = setup().await;
    let _ = engine
        .query(
            r#"mutation { insert_users(objects: [{name: "u1"}, {name: "u2"}]) { affected_rows } }"#,
            None,
        )
        .await
        .expect("seed ok");
    let v: Value = engine
        .query(
            r#"mutation { update_users(where: {name: {_eq: "u1"}}, _set: {age: 99}) { affected_rows returning { name age } } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["update_users"]["affected_rows"], json!(1));
    assert_eq!(v["update_users"]["returning"][0]["age"], json!(99));
}

#[tokio::test]
async fn update_by_pk_returns_object() {
    let (engine, _c) = setup().await;
    let seed: Value = engine
        .query(
            r#"mutation { insert_users_one(object: {name: "pk_user"}) { id } }"#,
            None,
        )
        .await
        .expect("seed ok");
    let id = seed["insert_users_one"]["id"].as_i64().unwrap();
    let v: Value = engine
        .query(
            &format!(
                r#"mutation {{ update_users_by_pk(pk_columns: {{id: {id}}}, _set: {{age: 42}}) {{ id name age }} }}"#
            ),
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["update_users_by_pk"]["age"], json!(42));
}

#[tokio::test]
async fn update_by_pk_missing_row_returns_null() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation { update_users_by_pk(pk_columns: {id: 99999}, _set: {age: 1}) { id } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert!(v["update_users_by_pk"].is_null());
}

#[tokio::test]
async fn delete_by_where() {
    let (engine, _c) = setup().await;
    let _ = engine
        .query(
            r#"mutation { insert_users(objects: [{name: "d1"}, {name: "d2"}]) { affected_rows } }"#,
            None,
        )
        .await
        .expect("seed ok");
    let v: Value = engine
        .query(
            r#"mutation { delete_users(where: {name: {_eq: "d1"}}) { affected_rows returning { name } } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["delete_users"]["affected_rows"], json!(1));
    assert_eq!(v["delete_users"]["returning"][0]["name"], json!("d1"));
}

#[tokio::test]
async fn delete_by_pk_missing_returns_null() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation { delete_users_by_pk(id: 99999) { id } }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert!(v["delete_users_by_pk"].is_null());
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test integration_mutation -- --test-threads=1`
Expected: all 8 tests pass (3 from Task 5 + 5 new).

- [ ] **Step 3: Commit**

```bash
git add tests/integration_mutation.rs
git commit -m "test: e2e update + delete mutations"
```

---

### Task 11: Verify + tag Phase 4

- [ ] **Step 1: Full test suite**

Run: `cargo test`
Expected: all pass (40+ unit + 24+ integration).

- [ ] **Step 2: Lint**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Format**

Run: `cargo fmt --check`
Expected: clean. Else `cargo fmt` + commit.

- [ ] **Step 4: Tag**

```bash
git tag -a phase-4-mutations -m "Phase 4: insert/update/delete mutations with on_conflict"
```

- [ ] **Step 5: Done**

Phase 4 complete. Engine supports the full CRUD surface.
