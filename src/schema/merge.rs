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

pub fn apply_config(
    mut sb: SchemaBuilder,
    cfg: &crate::schema::config::ConfigOverlay,
) -> SchemaBuilder {
    use crate::schema::config::RelationKindOverlay;
    use std::sync::Arc;

    let rename_map: BTreeMap<String, String> = cfg
        .tables
        .iter()
        .filter_map(|(old, o)| o.expose_as.clone().map(|new| (old.clone(), new)))
        .collect();

    let keys: Vec<String> = sb.tables.keys().cloned().collect();
    for exposed in keys {
        let Some(old) = sb.remove_raw(&exposed) else {
            continue;
        };
        let old_physical_schema = old.physical_schema.clone();
        let old_physical_name = old.physical_name.clone();
        let old_pk = old.primary_key.clone();

        let overlay = cfg.tables.get(&exposed);
        let new_exposed = overlay
            .and_then(|o| o.expose_as.clone())
            .unwrap_or_else(|| exposed.clone());

        let mut t = Table::new(&new_exposed, &old_physical_schema, &old_physical_name);

        let hidden: std::collections::BTreeSet<&str> = overlay
            .map(|o| o.hide_columns.iter().map(String::as_str).collect())
            .unwrap_or_default();
        for col in old.columns_iter() {
            if hidden.contains(col.exposed_name.as_str()) {
                continue;
            }
            t = t.column(
                &col.exposed_name,
                &col.physical_name,
                col.pg_type.clone(),
                col.nullable,
            );
        }

        if !old_pk.is_empty() {
            let refs: Vec<&str> = old_pk.iter().map(String::as_str).collect();
            t = t.primary_key(&refs);
        }

        let overlay_rel_names: std::collections::BTreeSet<&str> = overlay
            .map(|o| o.relations.iter().map(|r| r.name.as_str()).collect())
            .unwrap_or_default();
        for (name, rel) in old.relations_iter() {
            if overlay_rel_names.contains(name.as_str()) {
                continue;
            }
            let mut r = rel.clone();
            if let Some(new_target) = rename_map.get(&r.target_table) {
                r.target_table = new_target.clone();
            }
            t = t.relation(name, r);
        }

        if let Some(o) = overlay {
            for r in &o.relations {
                let kind = match r.kind {
                    RelationKindOverlay::Object => RelKind::Object,
                    RelationKindOverlay::Array => RelKind::Array,
                };
                t = t.relation(
                    &r.name,
                    Relation {
                        kind,
                        target_table: r.target.clone(),
                        mapping: r.mapping.clone(),
                    },
                );
            }
        }

        sb.insert_raw(new_exposed, Arc::new(t));
    }
    sb
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
    fn chained_merge_preserves_all_layers() {
        use crate::schema::config::ConfigOverlay;
        use crate::schema::{Schema, Table};

        let db = fixture_with_posts_to_users();
        let sb = build_from_introspection(db);

        let cfg = ConfigOverlay::default();
        let sb = apply_config(sb, &cfg);

        let sb = sb.table(
            Table::new("widgets", "public", "widgets")
                .column("id", "id", crate::schema::PgType::Int4, false),
        );

        let schema: Schema = sb.build();
        assert!(schema.table("users").is_some());
        assert!(schema.table("posts").is_some());
        assert!(schema.table("widgets").is_some());
        assert!(schema
            .table("users")
            .unwrap()
            .find_relation("posts")
            .is_some());
    }

    #[test]
    fn apply_config_renames_and_hides_and_adds_relation() {
        use crate::schema::config::{
            ConfigOverlay, RelationKindOverlay, RelationOverlay, TableOverlay,
        };

        let db = fixture_with_posts_to_users();
        let sb = build_from_introspection(db);

        let mut cfg = ConfigOverlay::default();
        let mut users_overlay = TableOverlay::default();
        users_overlay.expose_as = Some("profiles".into());
        users_overlay.relations.push(RelationOverlay {
            name: "followers".into(),
            kind: RelationKindOverlay::Array,
            target: "profiles".into(),
            mapping: vec![("id".into(), "followed_id".into())],
        });
        cfg.tables.insert("users".into(), users_overlay);

        let sb = apply_config(sb, &cfg);
        let schema = sb.build();
        assert!(schema.table("users").is_none());
        let profiles = schema.table("profiles").expect("profiles table");
        assert!(profiles.find_relation("posts").is_some());
        assert!(profiles.find_relation("followers").is_some());
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
