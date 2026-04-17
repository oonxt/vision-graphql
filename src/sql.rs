//! SQL generation from IR.

use crate::ast::{Field, Operation, QueryArgs, RootField, RootKind};
use crate::error::{Error, Result};
use crate::schema::{Schema, Table};
use crate::types::Bind;
use std::fmt::Write as _;

/// Render an [`Operation`] into a single SQL statement plus bound parameters.
pub fn render(op: &Operation, schema: &Schema) -> Result<(String, Vec<Bind>)> {
    let mut ctx = RenderCtx::default();
    match op {
        Operation::Query(roots) => render_query(roots, schema, &mut ctx),
    }?;
    Ok((ctx.sql, ctx.binds))
}

#[derive(Default)]
struct RenderCtx {
    sql: String,
    binds: Vec<Bind>,
    alias_counter: usize,
}

impl RenderCtx {
    fn next_alias(&mut self, prefix: &str) -> String {
        let a = format!("{prefix}{}", self.alias_counter);
        self.alias_counter += 1;
        a
    }
}

fn render_query(roots: &[RootField], schema: &Schema, ctx: &mut RenderCtx) -> Result<()> {
    ctx.sql.push_str("SELECT json_build_object(");
    for (i, root) in roots.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        write!(ctx.sql, "'{}', ", escape_string_literal(&root.alias)).unwrap();
        render_root(root, schema, ctx)?;
    }
    ctx.sql.push_str(") AS result");
    Ok(())
}

fn render_root(root: &RootField, schema: &Schema, ctx: &mut RenderCtx) -> Result<()> {
    let table = schema
        .table(&root.table)
        .ok_or_else(|| Error::Validate {
            path: root.alias.clone(),
            message: format!("unknown table '{}'", root.table),
        })?;
    match root.kind {
        RootKind::List => render_list(root, table, schema, ctx),
    }
}

fn render_list(
    root: &RootField,
    table: &Table,
    _schema: &Schema,
    ctx: &mut RenderCtx,
) -> Result<()> {
    let inner_alias = ctx.next_alias("t");
    let row_alias = ctx.next_alias("r");
    ctx.sql.push_str("(SELECT coalesce(json_agg(row_to_json(");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push_str(")), '[]'::json) FROM (");
    render_inner_select(root, table, &inner_alias, ctx)?;
    ctx.sql.push_str(") ");
    ctx.sql.push_str(&row_alias);
    ctx.sql.push(')');
    Ok(())
}

fn render_inner_select(
    root: &RootField,
    table: &Table,
    table_alias: &str,
    ctx: &mut RenderCtx,
) -> Result<()> {
    ctx.sql.push_str("SELECT ");
    for (i, field) in root.selection.iter().enumerate() {
        if i > 0 {
            ctx.sql.push_str(", ");
        }
        match field {
            Field::Column { physical, alias } => {
                let col = table.find_column(physical).ok_or_else(|| Error::Validate {
                    path: format!("{}.{}", root.alias, alias),
                    message: format!("unknown column '{physical}' on '{}'", root.table),
                })?;
                write!(
                    ctx.sql,
                    r#"{table_alias}.{} AS "{}""#,
                    quote_ident(&col.physical_name),
                    alias
                )
                .unwrap();
            }
        }
    }
    write!(
        ctx.sql,
        " FROM {}.{} {table_alias}",
        quote_ident(&table.physical_schema),
        quote_ident(&table.physical_name),
    )
    .unwrap();
    render_where(&root.args, table, table_alias, ctx)?;
    render_order_by(&root.args, table, table_alias, ctx)?;
    render_limit_offset(&root.args, ctx);
    Ok(())
}

fn render_where(
    _args: &QueryArgs,
    _table: &Table,
    _table_alias: &str,
    _ctx: &mut RenderCtx,
) -> Result<()> {
    Ok(())
}

fn render_order_by(
    _args: &QueryArgs,
    _table: &Table,
    _table_alias: &str,
    _ctx: &mut RenderCtx,
) -> Result<()> {
    Ok(())
}

fn render_limit_offset(_args: &QueryArgs, _ctx: &mut RenderCtx) {}

fn quote_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

fn escape_string_literal(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Field, Operation, QueryArgs, RootField, RootKind};
    use crate::schema::{PgType, Schema, Table};

    fn users_schema() -> Schema {
        Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true),
            )
            .build()
    }

    #[test]
    fn render_plain_list() {
        let op = Operation::Query(vec![RootField {
            table: "users".into(),
            alias: "users".into(),
            kind: RootKind::List,
            args: QueryArgs::default(),
            selection: vec![
                Field::Column { physical: "id".into(), alias: "id".into() },
                Field::Column { physical: "name".into(), alias: "name".into() },
            ],
        }]);
        let (sql, binds) = render(&op, &users_schema()).unwrap();
        insta::assert_snapshot!(sql);
        assert!(binds.is_empty());
    }
}
