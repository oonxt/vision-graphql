//! Format a DiffReport for human or machine consumption.

use crate::analyze::{ColumnOrigin, DiffReport};
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub enum Format {
    Text,
    Json,
}

pub fn write<W: Write>(report: &DiffReport, format: Format, out: &mut W) -> std::io::Result<()> {
    match format {
        Format::Text => write_text(report, out),
        Format::Json => write_json(report, out),
    }
}

fn write_text<W: Write>(report: &DiffReport, out: &mut W) -> std::io::Result<()> {
    if report.is_clean() {
        writeln!(out, "OK: no overlay drift detected")?;
        return Ok(());
    }
    if !report.missing_tables.is_empty() {
        writeln!(
            out,
            "missing tables (overlay references nonexistent table):"
        )?;
        for t in &report.missing_tables {
            writeln!(out, "  - {t}")?;
        }
    }
    if !report.missing_columns.is_empty() {
        writeln!(out, "missing columns:")?;
        for c in &report.missing_columns {
            let origin = match c.origin {
                ColumnOrigin::HideColumns => "hide_columns",
                ColumnOrigin::RelationLocal => "relation.mapping local",
                ColumnOrigin::RelationRemote => "relation.mapping remote",
            };
            writeln!(out, "  - {}.{} (from {})", c.table, c.column, origin)?;
        }
    }
    if !report.missing_relation_targets.is_empty() {
        writeln!(out, "missing relation targets:")?;
        for r in &report.missing_relation_targets {
            writeln!(out, "  - {}.{}: target = {}", r.table, r.relation, r.target)?;
        }
    }
    if !report.expose_as_collisions.is_empty() {
        writeln!(out, "expose_as collisions:")?;
        for c in &report.expose_as_collisions {
            writeln!(out, "  - {} <- {}", c.exposed_name, c.sources.join(", "))?;
        }
    }
    writeln!(out, "{} issues found", report.issue_count())?;
    Ok(())
}

fn write_json<W: Write>(report: &DiffReport, out: &mut W) -> std::io::Result<()> {
    serde_json::to_writer_pretty(&mut *out, report)?;
    out.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyze::{Collision, ColumnOrigin, DiffReport, MissingColumn, MissingRelTarget};

    fn dirty_report() -> DiffReport {
        DiffReport {
            missing_tables: vec!["ghosts".into()],
            missing_columns: vec![MissingColumn {
                table: "users".into(),
                column: "password_hash".into(),
                origin: ColumnOrigin::HideColumns,
            }],
            missing_relation_targets: vec![MissingRelTarget {
                table: "users".into(),
                relation: "owner".into(),
                target: "people".into(),
            }],
            expose_as_collisions: vec![Collision {
                exposed_name: "profiles".into(),
                sources: vec!["users".into(), "profiles".into()],
            }],
        }
    }

    #[test]
    fn clean_text_reports_ok() {
        let mut buf = Vec::new();
        write(&DiffReport::default(), Format::Text, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("OK"));
    }

    #[test]
    fn dirty_text_lists_each_issue() {
        let mut buf = Vec::new();
        write(&dirty_report(), Format::Text, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("missing tables"));
        assert!(s.contains("ghosts"));
        assert!(s.contains("password_hash"));
        assert!(s.contains("owner"));
        assert!(s.contains("profiles"));
        assert!(s.contains("4 issues found"));
    }

    #[test]
    fn json_round_trips() {
        let mut buf = Vec::new();
        write(&dirty_report(), Format::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["missing_tables"][0], "ghosts");
        assert_eq!(v["missing_columns"][0]["origin"], "hide_columns");
    }
}
