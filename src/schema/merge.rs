//! Merge introspection results and TOML overlays into SchemaBuilder.

use crate::schema::introspect::IntrospectedDb;
use crate::schema::{RelKind, Relation, SchemaBuilder, Table};
use std::collections::BTreeMap;

/// The order in which schemas compete for bare exposed names.
///
/// Normally this is exactly what the caller passed to
/// [`crate::schema::introspect::introspect_schemas`]. A hand-built
/// [`IntrospectedDb`] carries no such list, so fall back to a rule that keeps
/// the single-schema case behaving as it always has: `public` first, then
/// whatever else is present, alphabetically.
fn schema_priority(db: &IntrospectedDb) -> Vec<String> {
    if !db.schemas.is_empty() {
        // Keep only the first mention of each schema. A repeat would otherwise
        // get a second turn at a lower rank and rename its own tables from
        // `orders` to `app_orders` on the way past.
        let mut seen = std::collections::BTreeSet::new();
        return db
            .schemas
            .iter()
            .filter(|s| seen.insert((*s).clone()))
            .cloned()
            .collect();
    }
    let mut present: Vec<String> = db
        .tables
        .keys()
        .map(|(s, _)| s.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if let Some(i) = present.iter().position(|s| s == "public") {
        let p = present.remove(i);
        present.insert(0, p);
    }
    present
}

/// Map every introspected `(schema, table)` to the name it is exposed under.
///
/// **The first schema owns the bare table names; every later schema is prefixed
/// `{schema}_{table}`.** So introspecting `["app", "audit"]` gives `orders` and
/// `audit_orders`, and introspecting only `["public"]` gives exactly what it
/// always did — this rule is a no-op for the single-schema case.
///
/// The alternative — bare names until something collides, then qualify — was
/// rejected: it means creating a table in a second schema silently *renames* an
/// existing one, breaking queries that never mentioned the new table. Priority
/// order is fixed by the caller, so nothing an outside schema does can move a
/// name that is already spoken for.
///
/// A table whose derived name is still taken (a real `public.audit_orders`
/// standing where `audit.orders` wants to land) is dropped with a warning rather
/// than silently shadowing the incumbent. Rename either side with the overlay's
/// `expose_as` to recover it.
pub fn exposed_name_map(db: &IntrospectedDb) -> BTreeMap<(String, String), String> {
    let order = schema_priority(db);
    let mut out: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut taken: BTreeMap<String, (String, String)> = BTreeMap::new();

    for (rank, schema) in order.iter().enumerate() {
        for (skey, tname) in db.tables.keys() {
            if skey != schema {
                continue;
            }
            let candidate = if rank == 0 {
                tname.clone()
            } else {
                format!("{schema}_{tname}")
            };
            if let Some((owner_schema, owner_table)) = taken.get(&candidate) {
                tracing::warn!(
                    target: "vision_graphql::merge",
                    schema = %schema,
                    table = %tname,
                    exposed = %candidate,
                    owned_by = %format!("{owner_schema}.{owner_table}"),
                    "dropping table: its exposed name is already taken; rename one side with expose_as"
                );
                continue;
            }
            taken.insert(candidate.clone(), (schema.clone(), tname.clone()));
            out.insert((schema.clone(), tname.clone()), candidate);
        }
    }
    out
}

pub fn build_from_introspection(db: IntrospectedDb) -> SchemaBuilder {
    let names = exposed_name_map(&db);
    let rels = derive_relations_from_fks(&db);
    let mut sb = crate::schema::Schema::builder();
    for ((sname, tname), it) in &db.tables {
        // Absent from the map means the name was already taken and
        // `exposed_name_map` dropped it with a warning.
        let Some(exposed) = names.get(&(sname.clone(), tname.clone())) else {
            continue;
        };
        let mut t = Table::new(exposed, &it.schema, tname).read_only(it.read_only);
        let column_names: std::collections::BTreeSet<&str> =
            it.columns.iter().map(|c| c.name.as_str()).collect();
        for col in &it.columns {
            t = t.column(&col.name, &col.name, col.pg_type.clone(), col.nullable);
        }
        if !it.primary_key.is_empty() {
            let refs: Vec<&str> = it.primary_key.iter().map(String::as_str).collect();
            t = t.primary_key(&refs);
        }
        for (name, cols) in &it.unique_constraints {
            let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
            t = t.unique_constraint(name, &refs);
        }
        for (name, cols) in &it.unique_indexes {
            let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
            t = t.unique_index(name, &refs);
        }
        for (src, name, rel) in &rels {
            if src != exposed {
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
                    table = %exposed,
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
    introspect_schemas_into_builder(pool, &["public"]).await
}

pub async fn introspect_schemas_into_builder(
    pool: &sqlx::PgPool,
    schemas: &[&str],
) -> crate::error::Result<SchemaBuilder> {
    let db = crate::schema::introspect::introspect_schemas(pool, schemas).await?;
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
        let old_read_only = old.read_only;

        let overlay = cfg.tables.get(&exposed);
        let new_exposed = overlay
            .and_then(|o| o.expose_as.clone())
            .unwrap_or_else(|| exposed.clone());

        // The overlay overrides introspection in both directions: a view fronted
        // by INSTEAD OF triggers is genuinely writable, and a base table may be
        // deliberately frozen.
        let read_only = overlay.and_then(|o| o.read_only).unwrap_or(old_read_only);

        // Repoint the table at another physical schema without changing anything
        // else about it. Columns keep coming from what introspection read.
        let physical_schema = overlay
            .and_then(|o| o.schema.clone())
            .unwrap_or(old_physical_schema);

        let mut t =
            Table::new(&new_exposed, &physical_schema, &old_physical_name).read_only(read_only);

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

        // A view has no constraints, so introspection finds no PK for it. The
        // overlay is how a view declares the columns that logically identify a
        // row, which is what makes `_by_pk` available on it.
        let pk = overlay
            .and_then(|o| o.primary_key.clone())
            .unwrap_or(old_pk);
        for col in &pk {
            if t.find_column(col).is_none() {
                tracing::warn!(
                    target: "vision_graphql::merge",
                    table = %new_exposed,
                    column = %col,
                    "primary_key names a column the table does not expose; _by_pk on it will fail"
                );
            }
        }
        if !pk.is_empty() {
            let refs: Vec<&str> = pk.iter().map(String::as_str).collect();
            t = t.primary_key(&refs);
        }

        // Constraints survive `hide_columns` intact. Postgres resolves an
        // `ON CONFLICT` target by constraint name, so one covering a hidden
        // column is still a usable target — it is only its *name* that should
        // not be published, and that is decided where publishing happens
        // (`type_system`), not here where the engine's own knowledge lives.
        for (name, cols) in &old.unique_constraints {
            let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
            t = t.unique_constraint(name, &refs);
        }
        for (name, cols) in &old.unique_indexes {
            let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
            t = t.unique_index(name, &refs);
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
/// Returns `(source_exposed_name, relation_name, Relation)` triples. Both ends
/// are named by their exposed names, so a foreign key that crosses schemas
/// produces a relation just like a same-schema one — an FK from `app.orders` to
/// `audit.actors` becomes `orders.audit_actor` and `audit_actors.orders`.
///
/// A foreign key pointing at a schema that was not introspected has no exposed
/// name to target, so it is skipped: the relation would name a table the schema
/// does not contain, and every query touching it would fail at render time.
pub fn derive_relations_from_fks(db: &IntrospectedDb) -> Vec<(String, String, Relation)> {
    let names = exposed_name_map(db);
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

            // Either end may be missing an exposed name: its schema was not
            // introspected, or it lost a name collision.
            let (Some(src_exposed), Some(dst_exposed)) = (
                names.get(&(t.schema.clone(), t.name.clone())),
                names.get(&(fk.to_schema.clone(), fk.to_table.clone())),
            ) else {
                continue;
            };

            let mapping: Vec<(String, String)> = fk
                .from_columns
                .iter()
                .zip(fk.to_columns.iter())
                .map(|(a, b)| (a.clone(), b.clone()))
                .collect();
            let singular = dst_exposed.trim_end_matches('s').to_string();
            let src_rel_name = if singular.is_empty() || &singular == dst_exposed {
                dst_exposed.clone()
            } else {
                singular
            };
            out.push((
                src_exposed.clone(),
                src_rel_name,
                Relation {
                    kind: RelKind::Object,
                    target_table: dst_exposed.clone(),
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
                dst_exposed.clone(),
                src_exposed.clone(),
                Relation {
                    kind: RelKind::Array,
                    target_table: src_exposed.clone(),
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
                unique_indexes: Default::default(),
                foreign_keys: vec![],
                read_only: false,
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
                unique_indexes: Default::default(),
                foreign_keys: vec![IntrospectedForeignKey {
                    constraint_name: "benchmarks_value_type_fkey".into(),
                    to_schema: "public".into(),
                    to_table: "value_type".into(),
                    from_columns: vec!["value_type".into()],
                    to_columns: vec!["title".into()],
                }],
                read_only: false,
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
                unique_indexes: Default::default(),
                foreign_keys: vec![],
                read_only: false,
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
                unique_indexes: Default::default(),
                foreign_keys: vec![IntrospectedForeignKey {
                    constraint_name: "posts_user_id_fkey".into(),
                    from_columns: vec!["user_id".into()],
                    to_schema: "public".into(),
                    to_table: "users".into(),
                    to_columns: vec!["id".into()],
                }],
                read_only: false,
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
            schema: None,
            hide_columns: Vec::new(),
            relations: vec![RelationOverlay {
                name: "followers".into(),
                kind: RelationKindOverlay::Array,
                target: "profiles".into(),
                mapping: vec![("id".into(), "followed_id".into())],
            }],
            read_only: None,
            primary_key: None,
        };
        cfg.tables.insert("users".into(), users_overlay);

        let sb = apply_config(sb, &cfg);
        let schema = sb.build();
        assert!(schema.table("users").is_none());
        let profiles = schema.table("profiles").expect("profiles table");
        assert!(profiles.find_relation("posts").is_some());
        assert!(profiles.find_relation("followers").is_some());
    }

    /// Build a two-schema database: `app.orders` with an FK to `audit.actors`,
    /// plus an `audit.orders` that competes for the name `orders`.
    fn fixture_two_schemas(schemas: &[&str]) -> IntrospectedDb {
        fn table(
            schema: &str,
            name: &str,
            columns: Vec<IntrospectedColumn>,
            foreign_keys: Vec<IntrospectedForeignKey>,
        ) -> IntrospectedTable {
            IntrospectedTable {
                schema: schema.into(),
                name: name.into(),
                columns,
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                unique_indexes: Default::default(),
                foreign_keys,
                read_only: false,
            }
        }
        fn int_col(name: &str) -> IntrospectedColumn {
            IntrospectedColumn {
                name: name.into(),
                pg_type: PgType::Int4,
                nullable: false,
            }
        }

        let mut db = IntrospectedDb {
            skipped_columns: Vec::new(),
            tables: Default::default(),
            schemas: schemas.iter().map(|s| (*s).to_string()).collect(),
        };
        db.tables.insert(
            ("audit".into(), "actors".into()),
            table("audit", "actors", vec![int_col("id")], vec![]),
        );
        db.tables.insert(
            ("audit".into(), "orders".into()),
            table("audit", "orders", vec![int_col("id")], vec![]),
        );
        db.tables.insert(
            ("app".into(), "orders".into()),
            table(
                "app",
                "orders",
                vec![int_col("id"), int_col("actor_id")],
                vec![IntrospectedForeignKey {
                    constraint_name: "orders_actor_fkey".into(),
                    from_columns: vec!["actor_id".into()],
                    to_schema: "audit".into(),
                    to_table: "actors".into(),
                    to_columns: vec!["id".into()],
                }],
            ),
        );
        db
    }

    /// The first schema listed owns the bare names; the rest are prefixed. The
    /// point is stability: `app.orders` must keep the name `orders` no matter
    /// what `audit` contains, so a table appearing in a later schema can never
    /// silently rename one that queries already depend on.
    #[test]
    fn first_schema_owns_bare_names_later_ones_are_prefixed() {
        let db = fixture_two_schemas(&["app", "audit"]);
        let schema = build_from_introspection(db).build();

        let orders = schema
            .table("orders")
            .expect("app.orders keeps the bare name");
        assert_eq!(orders.physical_schema, "app");

        let audit_orders = schema
            .table("audit_orders")
            .expect("audit.orders is exposed prefixed");
        assert_eq!(audit_orders.physical_schema, "audit");
        assert_eq!(
            audit_orders.physical_name, "orders",
            "only the exposed name is prefixed; the physical name is untouched"
        );
        assert!(schema.table("audit_actors").is_some());
    }

    /// Same two schemas, opposite order: whoever is listed first wins the bare
    /// name. Nothing about the database changed — only the call did.
    #[test]
    fn schema_order_decides_who_owns_the_bare_name() {
        let db = fixture_two_schemas(&["audit", "app"]);
        let schema = build_from_introspection(db).build();

        assert_eq!(
            schema
                .table("orders")
                .expect("audit.orders")
                .physical_schema,
            "audit"
        );
        assert_eq!(
            schema
                .table("app_orders")
                .expect("app.orders is now the prefixed one")
                .physical_schema,
            "app"
        );
    }

    /// A schema named twice must not get a second turn at a lower rank — that
    /// would rename its own tables out from under the bare names it just won.
    #[test]
    fn a_repeated_schema_does_not_demote_itself() {
        let db = fixture_two_schemas(&["app", "audit", "app"]);
        let schema = build_from_introspection(db).build();

        assert_eq!(
            schema
                .table("orders")
                .expect("app.orders keeps the bare name")
                .physical_schema,
            "app"
        );
        assert!(
            schema.table("app_orders").is_none(),
            "app must not be processed a second time as a prefixed schema"
        );
    }

    /// A foreign key that crosses schemas is an ordinary relation — the whole
    /// point of introspecting more than one schema.
    #[test]
    fn cross_schema_fk_becomes_a_relation_on_both_sides() {
        let db = fixture_two_schemas(&["app", "audit"]);
        let schema = build_from_introspection(db).build();

        let rel = schema
            .table("orders")
            .unwrap()
            .find_relation("audit_actor")
            .expect("object relation named after the target's exposed name");
        assert_eq!(rel.kind, RelKind::Object);
        assert_eq!(rel.target_table, "audit_actors");
        assert_eq!(
            rel.mapping,
            vec![("actor_id".to_string(), "id".to_string())]
        );

        let back = schema
            .table("audit_actors")
            .unwrap()
            .find_relation("orders")
            .expect("array relation back to the source");
        assert_eq!(back.kind, RelKind::Array);
        assert_eq!(back.target_table, "orders");
    }

    /// A relation may only name a table the schema actually contains. An FK
    /// pointing into a schema that was not introspected has no such name, so it
    /// must be dropped — keeping it would render SQL against a table the schema
    /// cannot resolve.
    #[test]
    fn fk_into_an_uintrospected_schema_is_skipped() {
        let db = fixture_two_schemas(&["app"]);
        let schema = build_from_introspection(db).build();

        assert!(schema.table("orders").is_some());
        assert!(
            schema.table("audit_actors").is_none(),
            "audit was not introspected"
        );
        assert!(
            schema
                .table("orders")
                .unwrap()
                .relations_iter()
                .next()
                .is_none(),
            "the FK target is not in the schema, so no relation may point at it"
        );
    }

    /// A hand-built `IntrospectedDb` carries no schema list. The fallback must
    /// keep `public` in front so single-schema behaviour is unchanged.
    #[test]
    fn absent_schema_list_falls_back_to_public_first() {
        let db = fixture_with_posts_to_users(); // built with `schemas` empty
        assert!(db.schemas.is_empty());
        let schema = build_from_introspection(db).build();
        assert!(schema.table("users").is_some());
        assert!(schema.table("posts").is_some());
        assert!(schema.table("public_users").is_none());
    }

    /// Two tables wanting the same exposed name: the incumbent wins and the
    /// loser is dropped, rather than silently shadowing a table that queries
    /// may already be using.
    #[test]
    fn exposed_name_collision_drops_the_latecomer() {
        let mut db = fixture_two_schemas(&["app", "audit"]);
        // A real `app.audit_orders` standing exactly where `audit.orders` wants
        // to land.
        db.tables.insert(
            ("app".into(), "audit_orders".into()),
            IntrospectedTable {
                schema: "app".into(),
                name: "audit_orders".into(),
                columns: vec![IntrospectedColumn {
                    name: "id".into(),
                    pg_type: PgType::Int4,
                    nullable: false,
                }],
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                unique_indexes: Default::default(),
                foreign_keys: vec![],
                read_only: false,
            },
        );

        let schema = build_from_introspection(db).build();
        let winner = schema.table("audit_orders").expect("one of them survives");
        assert_eq!(
            winner.physical_schema, "app",
            "the first schema's table keeps the name it already had"
        );
    }

    /// The overlay can move a table to a different physical schema without
    /// touching anything else about it.
    #[test]
    fn overlay_schema_repoints_the_physical_schema() {
        use crate::schema::config::{ConfigOverlay, TableOverlay};

        let sb = build_from_introspection(fixture_with_posts_to_users());
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "posts".into(),
            TableOverlay {
                schema: Some("archive".into()),
                ..Default::default()
            },
        );

        let schema = apply_config(sb, &cfg).build();
        let posts = schema.table("posts").expect("still exposed as posts");
        assert_eq!(posts.physical_schema, "archive");
        assert_eq!(posts.physical_name, "posts");
        assert!(
            posts.find_column("user_id").is_some(),
            "columns still come from what introspection read"
        );
        assert_eq!(
            schema.table("users").unwrap().physical_schema,
            "public",
            "other tables are untouched"
        );
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
