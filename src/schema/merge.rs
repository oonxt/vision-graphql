//! Merge introspection results and TOML overlays into SchemaBuilder.

use crate::schema::introspect::IntrospectedDb;
use crate::schema::{RelKind, Relation, SchemaBuilder, Table};
use std::collections::BTreeMap;

pub fn build_from_introspection(db: IntrospectedDb) -> SchemaBuilder {
    let rels = derive_relations_from_fks(&db);
    let mut sb = crate::schema::Schema::builder();
    for ((_, tname), it) in &db.tables {
        let mut t = Table::new(tname, &it.schema, tname);
        for col in &it.columns {
            t = t.column(&col.name, &col.name, col.pg_type.clone(), col.nullable);
        }
        if !it.primary_key.is_empty() {
            let refs: Vec<&str> = it.primary_key.iter().map(String::as_str).collect();
            t = t.primary_key(&refs);
        }
        for (src, name, rel) in &rels {
            if src == tname {
                t = t.relation(name, rel.clone());
            }
        }
        sb = sb.table(t);
    }
    sb
}

pub async fn introspect_into_builder(
    pool: &deadpool_postgres::Pool,
) -> crate::error::Result<SchemaBuilder> {
    let db = crate::schema::introspect::introspect(pool).await?;
    Ok(build_from_introspection(db))
}

/// For each `(source_table, target_table)` pair that has exactly one foreign key
/// connecting them, derive an Object relation on the source and an Array
/// relation on the target. Pairs with multiple FKs are skipped with a warning.
///
/// Returns `(source_exposed_name, relation_name, Relation)` triples.
pub fn derive_relations_from_fks(db: &IntrospectedDb) -> Vec<(String, String, Relation)> {
    let mut out = Vec::new();
    let mut pair_counts: BTreeMap<(String, String, String, String), usize> = BTreeMap::new();
    for t in db.tables.values() {
        for fk in &t.foreign_keys {
            let key = (
                t.schema.clone(),
                t.name.clone(),
                fk.to_schema.clone(),
                fk.to_table.clone(),
            );
            *pair_counts.entry(key).or_insert(0) += 1;
        }
    }

    for t in db.tables.values() {
        for fk in &t.foreign_keys {
            let key = (
                t.schema.clone(),
                t.name.clone(),
                fk.to_schema.clone(),
                fk.to_table.clone(),
            );
            let count = *pair_counts.get(&key).unwrap_or(&0);
            if count != 1 {
                tracing::warn!(
                    target: "vision_graphql::merge",
                    from = %t.name,
                    to = %fk.to_table,
                    fks = count,
                    "skipping FK auto-relation: multiple FKs between same table pair"
                );
                continue;
            }

            if t.schema != "public" || fk.to_schema != "public" {
                continue;
            }

            let mapping: Vec<(String, String)> = fk
                .from_columns
                .iter()
                .zip(fk.to_columns.iter())
                .map(|(a, b)| (a.clone(), b.clone()))
                .collect();
            let singular = fk.to_table.trim_end_matches('s').to_string();
            let src_rel_name = if singular.is_empty() || singular == fk.to_table {
                fk.to_table.clone()
            } else {
                singular
            };
            out.push((
                t.name.clone(),
                src_rel_name,
                Relation {
                    kind: RelKind::Object,
                    target_table: fk.to_table.clone(),
                    mapping: mapping.clone(),
                },
            ));
            let rev_mapping: Vec<(String, String)> = fk
                .to_columns
                .iter()
                .zip(fk.from_columns.iter())
                .map(|(a, b)| (a.clone(), b.clone()))
                .collect();
            out.push((
                fk.to_table.clone(),
                t.name.clone(),
                Relation {
                    kind: RelKind::Array,
                    target_table: t.name.clone(),
                    mapping: rev_mapping,
                },
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::introspect::{IntrospectedColumn, IntrospectedForeignKey, IntrospectedTable};
    use crate::schema::PgType;

    fn fixture_with_posts_to_users() -> IntrospectedDb {
        let mut db = IntrospectedDb::default();
        db.tables.insert(
            ("public".into(), "users".into()),
            IntrospectedTable {
                schema: "public".into(),
                name: "users".into(),
                columns: vec![IntrospectedColumn {
                    name: "id".into(),
                    pg_type: PgType::Int4,
                    nullable: false,
                }],
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                foreign_keys: vec![],
            },
        );
        db.tables.insert(
            ("public".into(), "posts".into()),
            IntrospectedTable {
                schema: "public".into(),
                name: "posts".into(),
                columns: vec![
                    IntrospectedColumn {
                        name: "id".into(),
                        pg_type: PgType::Int4,
                        nullable: false,
                    },
                    IntrospectedColumn {
                        name: "user_id".into(),
                        pg_type: PgType::Int4,
                        nullable: false,
                    },
                ],
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                foreign_keys: vec![IntrospectedForeignKey {
                    constraint_name: "posts_user_id_fkey".into(),
                    from_columns: vec!["user_id".into()],
                    to_schema: "public".into(),
                    to_table: "users".into(),
                    to_columns: vec!["id".into()],
                }],
            },
        );
        db
    }

    #[test]
    fn single_fk_generates_both_directions() {
        let db = fixture_with_posts_to_users();
        let rels = derive_relations_from_fks(&db);
        assert_eq!(rels.len(), 2);
        assert!(rels
            .iter()
            .any(|(src, name, r)| src == "posts" && name == "user" && r.kind == RelKind::Object));
        assert!(rels
            .iter()
            .any(|(src, name, r)| src == "users" && name == "posts" && r.kind == RelKind::Array));
    }

    #[test]
    fn multiple_fks_to_same_target_skipped() {
        let mut db = fixture_with_posts_to_users();
        let posts = db.tables.get_mut(&("public".into(), "posts".into())).unwrap();
        posts.columns.push(IntrospectedColumn {
            name: "editor_id".into(),
            pg_type: PgType::Int4,
            nullable: true,
        });
        posts.foreign_keys.push(IntrospectedForeignKey {
            constraint_name: "posts_editor_fkey".into(),
            from_columns: vec!["editor_id".into()],
            to_schema: "public".into(),
            to_table: "users".into(),
            to_columns: vec!["id".into()],
        });
        let rels = derive_relations_from_fks(&db);
        assert!(rels.is_empty());
    }
}
