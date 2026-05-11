//! Validate a parsed ConfigOverlay against an introspected database.

use crate::filter::TableFilter;
use serde::Serialize;
use std::collections::BTreeMap;
use vision_graphql::schema::config::ConfigOverlay;
use vision_graphql::schema::introspect::{IntrospectedDb, IntrospectedTable};

#[derive(Debug, Serialize, Default)]
pub struct DiffReport {
    pub missing_tables: Vec<String>,
    pub missing_columns: Vec<MissingColumn>,
    pub missing_relation_targets: Vec<MissingRelTarget>,
    pub expose_as_collisions: Vec<Collision>,
}

#[derive(Debug, Serialize)]
pub struct MissingColumn {
    pub table: String,
    pub column: String,
    pub origin: ColumnOrigin,
}

#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ColumnOrigin {
    HideColumns,
    RelationLocal,
    RelationRemote,
}

#[derive(Debug, Serialize)]
pub struct MissingRelTarget {
    pub table: String,
    pub relation: String,
    pub target: String,
}

#[derive(Debug, Serialize)]
pub struct Collision {
    pub exposed_name: String,
    pub sources: Vec<String>,
}

impl DiffReport {
    pub fn is_clean(&self) -> bool {
        self.missing_tables.is_empty()
            && self.missing_columns.is_empty()
            && self.missing_relation_targets.is_empty()
            && self.expose_as_collisions.is_empty()
    }

    pub fn issue_count(&self) -> usize {
        self.missing_tables.len()
            + self.missing_columns.len()
            + self.missing_relation_targets.len()
            + self.expose_as_collisions.len()
    }
}

pub fn find_drift(cfg: &ConfigOverlay, db: &IntrospectedDb, filter: &TableFilter) -> DiffReport {
    let mut report = DiffReport::default();

    // Index physical tables for quick lookup; only `public` is in scope.
    let by_name: BTreeMap<&str, &IntrospectedTable> = db
        .tables
        .iter()
        .filter(|((schema, _), _)| schema == "public")
        .map(|((_, name), t)| (name.as_str(), t))
        .collect();

    // expose_as collisions: track all exposed names.
    let mut exposed_owners: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for name in by_name.keys() {
        exposed_owners
            .entry((*name).to_string())
            .or_default()
            .push((*name).to_string());
    }
    for (key, overlay) in &cfg.tables {
        if !filter.keep(key) {
            continue;
        }
        if let Some(new) = &overlay.expose_as {
            // Replace the original physical name's claim with the renamed one.
            exposed_owners.entry(key.clone()).and_modify(|v| {
                v.retain(|s| s != key);
            });
            exposed_owners
                .entry(new.clone())
                .or_default()
                .push(key.clone());
        }
    }
    for (exposed, sources) in &exposed_owners {
        if sources.len() > 1 {
            report.expose_as_collisions.push(Collision {
                exposed_name: exposed.clone(),
                sources: sources.clone(),
            });
        }
    }

    // Per-overlay-table checks.
    for (key, overlay) in &cfg.tables {
        if !filter.keep(key) {
            continue;
        }
        let Some(table) = by_name.get(key.as_str()) else {
            report.missing_tables.push(key.clone());
            continue;
        };
        let col_set: std::collections::BTreeSet<&str> =
            table.columns.iter().map(|c| c.name.as_str()).collect();
        for hidden in &overlay.hide_columns {
            if !col_set.contains(hidden.as_str()) {
                report.missing_columns.push(MissingColumn {
                    table: key.clone(),
                    column: hidden.clone(),
                    origin: ColumnOrigin::HideColumns,
                });
            }
        }
        for rel in &overlay.relations {
            // Resolve target: physical table name OR another overlay's expose_as.
            let target_physical = if by_name.contains_key(rel.target.as_str()) {
                Some(rel.target.clone())
            } else {
                cfg.tables
                    .iter()
                    .find(|(_, o)| o.expose_as.as_deref() == Some(rel.target.as_str()))
                    .map(|(k, _)| k.clone())
            };
            let Some(target_phys) = target_physical else {
                report.missing_relation_targets.push(MissingRelTarget {
                    table: key.clone(),
                    relation: rel.name.clone(),
                    target: rel.target.clone(),
                });
                continue;
            };
            for (local, remote) in &rel.mapping {
                if !col_set.contains(local.as_str()) {
                    report.missing_columns.push(MissingColumn {
                        table: key.clone(),
                        column: local.clone(),
                        origin: ColumnOrigin::RelationLocal,
                    });
                }
                if let Some(target_table) = by_name.get(target_phys.as_str()) {
                    let remote_set: std::collections::BTreeSet<&str> =
                        target_table.columns.iter().map(|c| c.name.as_str()).collect();
                    if !remote_set.contains(remote.as_str()) {
                        report.missing_columns.push(MissingColumn {
                            table: key.clone(),
                            column: remote.clone(),
                            origin: ColumnOrigin::RelationRemote,
                        });
                    }
                }
            }
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use vision_graphql::schema::config::{
        RelationKindOverlay, RelationOverlay, TableOverlay,
    };
    use vision_graphql::schema::introspect::{IntrospectedColumn, IntrospectedTable};
    use vision_graphql::schema::PgType;

    fn db_users_only() -> IntrospectedDb {
        let mut db = IntrospectedDb::default();
        db.tables.insert(
            ("public".into(), "users".into()),
            IntrospectedTable {
                schema: "public".into(),
                name: "users".into(),
                columns: vec![
                    IntrospectedColumn { name: "id".into(),    pg_type: PgType::Int4, nullable: false },
                    IntrospectedColumn { name: "email".into(), pg_type: PgType::Text, nullable: true  },
                ],
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                foreign_keys: vec![],
            },
        );
        db
    }

    fn no_filter() -> TableFilter {
        TableFilter::new(None, None).unwrap()
    }

    #[test]
    fn clean_overlay_against_clean_db() {
        let db = db_users_only();
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "users".into(),
            TableOverlay {
                expose_as: Some("profiles".into()),
                hide_columns: vec!["email".into()],
                relations: vec![],
            },
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert!(r.is_clean(), "expected clean, got {:?}", r);
    }

    #[test]
    fn missing_table_reported() {
        let db = db_users_only();
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "ghosts".into(),
            TableOverlay::default(),
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert_eq!(r.missing_tables, vec!["ghosts".to_string()]);
    }

    #[test]
    fn stale_hide_column_reported() {
        let db = db_users_only();
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "users".into(),
            TableOverlay {
                expose_as: None,
                hide_columns: vec!["password_hash".into()],
                relations: vec![],
            },
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert_eq!(r.missing_columns.len(), 1);
        assert_eq!(r.missing_columns[0].column, "password_hash");
        assert!(matches!(r.missing_columns[0].origin, ColumnOrigin::HideColumns));
    }

    #[test]
    fn missing_relation_target_reported() {
        let db = db_users_only();
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "users".into(),
            TableOverlay {
                expose_as: None,
                hide_columns: vec![],
                relations: vec![RelationOverlay {
                    name: "ghosts".into(),
                    kind: RelationKindOverlay::Array,
                    target: "ghost_table".into(),
                    mapping: vec![("id".into(), "user_id".into())],
                }],
            },
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert_eq!(r.missing_relation_targets.len(), 1);
        assert_eq!(r.missing_relation_targets[0].target, "ghost_table");
    }

    #[test]
    fn expose_as_collision_reported() {
        let mut db = db_users_only();
        db.tables.insert(
            ("public".into(), "profiles".into()),
            IntrospectedTable {
                schema: "public".into(),
                name: "profiles".into(),
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
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "users".into(),
            TableOverlay {
                expose_as: Some("profiles".into()),
                ..Default::default()
            },
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert_eq!(r.expose_as_collisions.len(), 1);
        assert_eq!(r.expose_as_collisions[0].exposed_name, "profiles");
    }

    #[test]
    fn filter_skips_overlay_entries() {
        let db = db_users_only();
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert("ghosts".into(), TableOverlay::default());
        let ignore = vec!["ghosts".to_string()];
        let f = TableFilter::new(None, Some(&ignore)).unwrap();
        let r = find_drift(&cfg, &db, &f);
        assert!(r.is_clean(), "ignored entry should not surface");
    }
}
