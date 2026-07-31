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
    /// `schema = "..."` repoints that are definitely broken.
    pub bad_repoints: Vec<BadRepoint>,
    /// `schema = "..."` repoints aimed at a schema that was not introspected,
    /// so nothing here could be checked either way. Reported so the hole is
    /// visible, but not counted as drift — "I could not look" is not a finding.
    pub unverified_repoints: Vec<UnverifiedRepoint>,
}

/// An overlay `schema = "..."` that points somewhere it cannot work.
///
/// A repoint moves only the schema qualifier; columns keep coming from the table
/// introspection actually read. So the target must exist and must carry those
/// columns, or the mismatch surfaces as a Postgres error at query time — which
/// is exactly the class of thing `diff` exists to catch first.
#[derive(Debug, Serialize)]
pub struct BadRepoint {
    pub table: String,
    pub schema: String,
    pub problem: RepointProblem,
    /// Columns the target lacks. Empty unless `problem` is `ColumnsMissing`.
    pub missing_columns: Vec<String>,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RepointProblem {
    /// The schema was introspected, but has no table of that name.
    TableMissing,
    /// The target exists but is missing columns the exposed table declares.
    ColumnsMissing,
}

#[derive(Debug, Serialize)]
pub struct UnverifiedRepoint {
    pub table: String,
    pub schema: String,
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
        self.issue_count() == 0
    }

    /// `unverified_repoints` is deliberately excluded: it records what could not
    /// be checked, not drift that was found, and must not fail `diff`.
    pub fn issue_count(&self) -> usize {
        self.missing_tables.len()
            + self.missing_columns.len()
            + self.missing_relation_targets.len()
            + self.expose_as_collisions.len()
            + self.bad_repoints.len()
    }
}

pub fn find_drift(cfg: &ConfigOverlay, db: &IntrospectedDb, filter: &TableFilter) -> DiffReport {
    let mut report = DiffReport::default();

    // Index tables under the names the schema actually exposes them as — which
    // is what an overlay key refers to. With one schema that is just the table
    // name; with several, later schemas are prefixed.
    let exposed = vision_graphql::schema::merge::exposed_name_map(db);
    let by_name: BTreeMap<String, &IntrospectedTable> = db
        .tables
        .iter()
        .filter_map(|(key, t)| exposed.get(key).map(|name| (name.clone(), t)))
        .collect();

    // expose_as collisions: track all exposed names.
    let mut exposed_owners: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for name in by_name.keys() {
        exposed_owners
            .entry(name.clone())
            .or_default()
            .push(name.clone());
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

        // `schema = "..."` renders this table out of a different physical schema
        // while keeping the columns introspection read here, so the target has to
        // exist and carry them.
        if let Some(target_schema) = &overlay.schema {
            let introspected = db.schemas.iter().any(|s| s == target_schema)
                // A hand-built IntrospectedDb carries no schema list; fall back to
                // whether any table from that schema is present.
                || (db.schemas.is_empty()
                    && db.tables.keys().any(|(s, _)| s == target_schema));

            if !introspected {
                report.unverified_repoints.push(UnverifiedRepoint {
                    table: key.clone(),
                    schema: target_schema.clone(),
                });
            } else {
                match db.tables.get(&(target_schema.clone(), table.name.clone())) {
                    None => report.bad_repoints.push(BadRepoint {
                        table: key.clone(),
                        schema: target_schema.clone(),
                        problem: RepointProblem::TableMissing,
                        missing_columns: Vec::new(),
                    }),
                    Some(target) => {
                        let target_cols: std::collections::BTreeSet<&str> =
                            target.columns.iter().map(|c| c.name.as_str()).collect();
                        let missing: Vec<String> = col_set
                            .iter()
                            .filter(|c| !target_cols.contains(*c))
                            .map(|c| (*c).to_string())
                            .collect();
                        if !missing.is_empty() {
                            report.bad_repoints.push(BadRepoint {
                                table: key.clone(),
                                schema: target_schema.clone(),
                                problem: RepointProblem::ColumnsMissing,
                                missing_columns: missing,
                            });
                        }
                    }
                }
            }
        }

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
                    let remote_set: std::collections::BTreeSet<&str> = target_table
                        .columns
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect();
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
    use vision_graphql::schema::config::{RelationKindOverlay, RelationOverlay, TableOverlay};
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
                    IntrospectedColumn {
                        name: "id".into(),
                        pg_type: PgType::Int4,
                        nullable: false,
                    },
                    IntrospectedColumn {
                        name: "email".into(),
                        pg_type: PgType::Text,
                        nullable: true,
                    },
                ],
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                foreign_keys: vec![],
                read_only: false,
            },
        );
        db
    }

    fn no_filter() -> TableFilter {
        TableFilter::new(None, None).unwrap()
    }

    fn col(name: &str) -> IntrospectedColumn {
        IntrospectedColumn {
            name: name.into(),
            pg_type: PgType::Text,
            nullable: true,
        }
    }

    /// `db_users_only` plus an `archive` schema, introspected, holding a `users`
    /// table with the given columns.
    fn db_with_archive(archive_cols: Vec<IntrospectedColumn>) -> IntrospectedDb {
        let mut db = db_users_only();
        db.schemas = vec!["public".into(), "archive".into()];
        db.tables.insert(
            ("archive".into(), "users".into()),
            IntrospectedTable {
                schema: "archive".into(),
                name: "users".into(),
                columns: archive_cols,
                primary_key: vec!["id".into()],
                unique_constraints: Default::default(),
                foreign_keys: vec![],
                read_only: false,
            },
        );
        db
    }

    fn repoint_cfg(to: &str) -> ConfigOverlay {
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert(
            "users".into(),
            TableOverlay {
                schema: Some(to.into()),
                ..Default::default()
            },
        );
        cfg
    }

    /// A repoint keeps the columns introspection read here, so a target carrying
    /// all of them is fine.
    #[test]
    fn repoint_to_matching_table_is_clean() {
        let db = db_with_archive(vec![col("id"), col("email")]);
        let r = find_drift(&repoint_cfg("archive"), &db, &no_filter());
        assert!(r.is_clean(), "expected clean, got {r:?}");
    }

    /// The target exists but lacks a column the exposed table declares — that is
    /// a Postgres error at query time, which is what `diff` is for.
    #[test]
    fn repoint_to_table_missing_columns_is_reported() {
        let db = db_with_archive(vec![col("id")]);
        let r = find_drift(&repoint_cfg("archive"), &db, &no_filter());
        assert_eq!(r.bad_repoints.len(), 1, "got {r:?}");
        assert_eq!(r.bad_repoints[0].problem, RepointProblem::ColumnsMissing);
        assert_eq!(r.bad_repoints[0].missing_columns, vec!["email".to_string()]);
        assert!(!r.is_clean());
    }

    /// The schema was introspected and simply has no such table.
    #[test]
    fn repoint_to_absent_table_is_reported() {
        let mut db = db_users_only();
        db.schemas = vec!["public".into(), "archive".into()];
        let r = find_drift(&repoint_cfg("archive"), &db, &no_filter());
        assert_eq!(r.bad_repoints.len(), 1, "got {r:?}");
        assert_eq!(r.bad_repoints[0].problem, RepointProblem::TableMissing);
        assert!(!r.is_clean());
    }

    /// Aimed at a schema nobody introspected: report the hole, but do not call
    /// it drift — `diff` did not look, which is different from finding nothing.
    #[test]
    fn repoint_to_uintrospected_schema_is_unverified_not_drift() {
        let mut db = db_users_only();
        db.schemas = vec!["public".into()];
        let r = find_drift(&repoint_cfg("cold_storage"), &db, &no_filter());

        assert!(r.bad_repoints.is_empty(), "got {r:?}");
        assert_eq!(r.unverified_repoints.len(), 1);
        assert_eq!(r.unverified_repoints[0].schema, "cold_storage");
        assert!(r.is_clean(), "unverifiable must not fail diff");
    }

    /// No `schema` key at all: none of this machinery fires.
    #[test]
    fn overlay_without_repoint_reports_nothing_new() {
        let db = db_with_archive(vec![col("id")]);
        let r = find_drift(&ConfigOverlay::default(), &db, &no_filter());
        assert!(r.bad_repoints.is_empty());
        assert!(r.unverified_repoints.is_empty());
        assert!(r.is_clean());
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
                ..Default::default()
            },
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert!(r.is_clean(), "expected clean, got {:?}", r);
    }

    #[test]
    fn missing_table_reported() {
        let db = db_users_only();
        let mut cfg = ConfigOverlay::default();
        cfg.tables.insert("ghosts".into(), TableOverlay::default());
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
                ..Default::default()
            },
        );
        let r = find_drift(&cfg, &db, &no_filter());
        assert_eq!(r.missing_columns.len(), 1);
        assert_eq!(r.missing_columns[0].column, "password_hash");
        assert!(matches!(
            r.missing_columns[0].origin,
            ColumnOrigin::HideColumns
        ));
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
                ..Default::default()
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
                read_only: false,
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
