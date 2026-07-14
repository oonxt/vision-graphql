//! Merge introspection results and TOML overlays into SchemaBuilder.

use crate::schema::introspect::IntrospectedDb;
use crate::schema::{RelKind, Relation, SchemaBuilder, Table};
use std::collections::BTreeMap;

pub fn build_from_introspection(db: IntrospectedDb) -> SchemaBuilder {
    let rels = derive_relations_from_fks(&db);
    let mut sb = crate::schema::Schema::builder();
    for ((_, tname), it) in &db.tables {
        let mut t = Table::new(tname, &it.schema, tname);
        let column_names: std::collections::BTreeSet<&str> =
            it.columns.iter().map(|c| c.name.as_str()).collect();
        for col in &it.columns {
            t = t.column(&col.name, &col.name, col.pg_type.clone(), col.nullable);
        }
        if !it.primary_key.is_empty() {
            let refs: Vec<&str> = it.primary_key.iter().map(String::as_str).collect();
            t = t.primary_key(&refs);
        }
        for (src, name, rel) in &rels {
            if src != tname {
                continue;
            }
            // 自动推导出的关联绝不能遮蔽同名的真实列。
            // 选择字段时关联优先于列（parser 先查 find_relation），一旦同名，
            // 查询里写这个名字拿到的是关联对象而不是列值 —— 且没有任何报错，
            // 只是静默返回 {}。典型触发场景：文本列 + 指向同名查找表的外键
            // （value_type / container_type / role / experiment_type ...）。
            // 这种便利关联价值有限，列才是本体，冲突时让列赢。
            if column_names.contains(name.as_str()) {
                tracing::warn!(
                    target: "vision_graphql::merge",
                    table = %tname,
                    name = %name,
                    "skipping FK auto-relation: name collides with a column of the same name"
                );
                continue;
            }
            t = t.relation(name, rel.clone());
        }
        sb = sb.table(t);
    }
    sb
}

pub async fn introspect_into_builder(pool: &sqlx::PgPool) -> crate::error::Result<SchemaBuilder> {
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
    /// 文本列 + 指向同名查找表的外键（value_type 列 -> value_type 表）。
    /// 自动关联若沿用目标表名，就会和列同名；而选择字段时关联优先于列，
    /// 查询里写 value_type 会静默拿到一个空对象 {} 而不是列值。
    /// 列必须赢。
    fn fixture_column_shadowed_by_lookup_fk() -> IntrospectedDb {
        let mut db = IntrospectedDb::default();
        db.tables.insert(
            ("public".into(), "value_type".into()),
            IntrospectedTable {
                schema: "public".into(),
                name: "value_type".into(),
                columns: vec![IntrospectedColumn {
                    name: "title".into(),
                    pg_type: PgType::Text,
                    nullable: false,
                }],
                primary_key: vec!["title".into()],
                unique_constraints: Default::default(),
                foreign_keys: vec![],
            },
        );
        db.tables.insert(
            ("public".into(), "benchmarks".into()),
            IntrospectedTable {
                schema: "public".into(),
                name: "benchmarks".into(),
                columns: vec![
                    IntrospectedColumn {
                        name: "id".into(),
                        pg_type: PgType::Int4,
                        nullable: false,
                    },
                    IntrospectedColumn {
                        name: "value_type".into(),
                        pg_type: PgType::Text,
                        nullable: false,
                    },
                ],
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                foreign_keys: vec![IntrospectedForeignKey {
                    constraint_name: "benchmarks_value_type_fkey".into(),
                    to_schema: "public".into(),
                    to_table: "value_type".into(),
                    from_columns: vec!["value_type".into()],
                    to_columns: vec!["title".into()],
                }],
            },
        );
        db
    }

    #[test]
    fn auto_relation_never_shadows_a_column() {
        let schema = build_from_introspection(fixture_column_shadowed_by_lookup_fk()).build();
        let t = schema.table("benchmarks").expect("benchmarks table");

        // 列还在，且能被选中
        assert!(
            t.find_column("value_type").is_some(),
            "value_type 列必须存在"
        );
        // 同名的自动关联被跳过了，否则它会遮蔽这一列
        assert!(
            t.find_relation("value_type").is_none(),
            "与列同名的自动关联必须被跳过，否则查询会静默返回 {{}} 而不是列值"
        );
    }

    use crate::schema::introspect::{
        IntrospectedColumn, IntrospectedForeignKey, IntrospectedTable,
    };
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

        let sb = sb.table(Table::new("widgets", "public", "widgets").column(
            "id",
            "id",
            crate::schema::PgType::Int4,
            false,
        ));

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
        let users_overlay = TableOverlay {
            expose_as: Some("profiles".into()),
            hide_columns: Vec::new(),
            relations: vec![RelationOverlay {
                name: "followers".into(),
                kind: RelationKindOverlay::Array,
                target: "profiles".into(),
                mapping: vec![("id".into(), "followed_id".into())],
            }],
        };
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
        let posts = db
            .tables
            .get_mut(&("public".into(), "posts".into()))
            .unwrap();
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
