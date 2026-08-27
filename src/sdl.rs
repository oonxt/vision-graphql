//! Render a [`TypeSystem`] as SDL.
//!
//! The reason to want this is not the same as the reason to want `__schema`.
//! Introspection serves tooling at runtime; SDL is a file — the readable form of
//! an exposure surface that is otherwise implicit, spread across what
//! introspection found, what the overlay renamed, and what `hide_columns`
//! removed. Written to disk and committed, it turns "did that migration expose a
//! new column" into a diff in review.
//!
//! Types come out sorted by name, so that diff is about the schema and not about
//! `HashMap` iteration order.

use crate::type_system::{Field, InputValue, TypeDef, TypeRef, TypeSystem};
use std::fmt::Write;

/// SDL for the whole type system, roots included.
pub fn render(ts: &TypeSystem) -> String {
    let mut out = String::new();

    out.push_str("schema {\n");
    writeln!(out, "  query: {}", ts.query_root()).unwrap();
    if let Some(m) = ts.mutation_root() {
        writeln!(out, "  mutation: {m}").unwrap();
    }
    out.push_str("}\n");

    for def in ts.types() {
        out.push('\n');
        render_type(def, &mut out);
    }
    out
}

fn render_type(def: &TypeDef, out: &mut String) {
    render_description(def.description(), 0, out);
    match def {
        TypeDef::Scalar { name, .. } => {
            writeln!(out, "scalar {name}").unwrap();
        }
        TypeDef::Enum { name, values, .. } => {
            writeln!(out, "enum {name} {{").unwrap();
            for v in values {
                writeln!(out, "  {v}").unwrap();
            }
            out.push_str("}\n");
        }
        TypeDef::Object { name, fields, .. } => {
            writeln!(out, "type {name} {{").unwrap();
            for f in fields {
                render_field(f, out);
            }
            out.push_str("}\n");
        }
        TypeDef::InputObject { name, fields, .. } => {
            writeln!(out, "input {name} {{").unwrap();
            for f in fields {
                render_description(f.description.as_deref(), 2, out);
                writeln!(out, "  {}: {}{}", f.name, render_ref(&f.ty), default_of(f)).unwrap();
            }
            out.push_str("}\n");
        }
    }
}

fn render_field(f: &Field, out: &mut String) {
    render_description(f.description.as_deref(), 2, out);
    if f.args.is_empty() {
        writeln!(out, "  {}: {}", f.name, render_ref(&f.ty)).unwrap();
        return;
    }
    let args: Vec<String> = f
        .args
        .iter()
        .map(|a| format!("{}: {}{}", a.name, render_ref(&a.ty), default_of(a)))
        .collect();
    writeln!(
        out,
        "  {}({}): {}",
        f.name,
        args.join(", "),
        render_ref(&f.ty)
    )
    .unwrap();
}

fn default_of(v: &InputValue) -> String {
    match &v.default_value {
        Some(d) => format!(" = {d}"),
        None => String::new(),
    }
}

fn render_ref(r: &TypeRef) -> String {
    match r {
        TypeRef::Named(n) => n.clone(),
        TypeRef::NonNull(inner) => format!("{}!", render_ref(inner)),
        TypeRef::List(inner) => format!("[{}]", render_ref(inner)),
    }
}

/// Descriptions go out as block strings: a table comment or a column name can
/// contain a quote, and a block string is the form that survives it.
fn render_description(description: Option<&str>, indent: usize, out: &mut String) {
    let Some(d) = description else {
        return;
    };
    let pad = " ".repeat(indent);
    // The one sequence a block string cannot contain.
    let safe = d.replace(r#"""""#, r#"\""""#);
    writeln!(out, "{pad}\"\"\"{safe}\"\"\"").unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{PgType, Relation, Schema, Table};

    fn ts() -> TypeSystem {
        let schema = Schema::builder()
            .table(
                Table::new("users", "public", "users")
                    .column("id", "id", PgType::Int4, false)
                    .column("name", "name", PgType::Text, true)
                    .primary_key(&["id"])
                    .unique_constraint("users_pkey", &["id"])
                    .relation("posts", Relation::array("posts").on([("id", "user_id")])),
            )
            .table(
                Table::new("posts", "public", "posts")
                    .column("id", "id", PgType::Int4, false)
                    .column("user_id", "user_id", PgType::Int4, false)
                    .primary_key(&["id"]),
            )
            .build();
        TypeSystem::build(&schema)
    }

    #[test]
    fn renders_the_roots_and_a_row_type() {
        let sdl = render(&ts());
        assert!(sdl.contains("schema {\n  query: query_root\n  mutation: mutation_root\n}"));
        assert!(sdl.contains("type users {"), "{sdl}");
        assert!(sdl.contains("  id: Int!"), "{sdl}");
        assert!(sdl.contains("  name: String\n"), "{sdl}");
    }

    #[test]
    fn renders_arguments_and_wrapped_types() {
        let sdl = render(&ts());
        assert!(
            sdl.contains("posts(where: posts_bool_exp, order_by: [posts_order_by!], limit: Int, offset: Int, distinct_on: [posts_select_column!]): [posts!]!"),
            "{sdl}"
        );
    }

    #[test]
    fn renders_inputs_and_enums() {
        let sdl = render(&ts());
        assert!(sdl.contains("input users_bool_exp {"), "{sdl}");
        assert!(sdl.contains("  _and: [users_bool_exp!]"), "{sdl}");
        assert!(sdl.contains("enum order_by {"), "{sdl}");
        assert!(sdl.contains("  desc_nulls_last"), "{sdl}");
        assert!(sdl.contains("enum users_constraint {"), "{sdl}");
        assert!(sdl.contains("  users_pkey"), "{sdl}");
    }

    #[test]
    fn output_is_stable_across_renders() {
        // Same schema, same bytes — otherwise it is useless as a committed
        // artifact to diff.
        assert_eq!(render(&ts()), render(&ts()));
    }

    #[test]
    fn a_read_only_schema_renders_no_mutation_root() {
        let schema = Schema::builder()
            .table(
                Table::new("v", "public", "v")
                    .column("id", "id", PgType::Int4, false)
                    .read_only(true),
            )
            .build();
        let sdl = render(&TypeSystem::build(&schema));
        assert!(sdl.contains("query: query_root"), "{sdl}");
        assert!(!sdl.contains("mutation:"), "{sdl}");
    }
}
