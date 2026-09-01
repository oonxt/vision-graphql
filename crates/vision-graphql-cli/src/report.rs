//! Format a DiffReport for human or machine consumption.

use crate::analyze::{ColumnOrigin, DiffReport, RepointProblem};
use std::collections::BTreeSet;
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
    } else {
        write_drift(report, out)?;
    }
    // Sections that print whether or not drift was found: what could not be
    // checked, what the engine cannot carry, and what will answer wrongly
    // without erroring. One tail for both paths — when this was written twice,
    // every new section was one forgotten call away from silently never
    // printing on one of them.
    write_unverified(report, out)?;
    write_skipped(report, out)?;
    write_warnings(report, out)?;
    Ok(())
}

fn write_drift<W: Write>(report: &DiffReport, out: &mut W) -> std::io::Result<()> {
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
    if !report.bad_repoints.is_empty() {
        writeln!(out, "broken schema repoints:")?;
        for r in &report.bad_repoints {
            match r.problem {
                RepointProblem::TableMissing => writeln!(
                    out,
                    "  - {}: schema = \"{}\" has no table \"{}\"",
                    r.table, r.schema, r.table
                )?,
                RepointProblem::ColumnsMissing => writeln!(
                    out,
                    "  - {}: \"{}\".\"{}\" lacks column(s) {}",
                    r.table,
                    r.schema,
                    r.table,
                    r.missing_columns.join(", ")
                )?,
            }
        }
    }
    writeln!(out, "{} issues found", report.issue_count())?;
    Ok(())
}

/// Object relations that can silently answer with the wrong row. Not drift —
/// the overlay matches the database — but printed on both paths because this
/// is the only report that can see it before the data goes crooked.
fn write_warnings<W: Write>(report: &DiffReport, out: &mut W) -> std::io::Result<()> {
    if report.relation_warnings.is_empty() {
        return Ok(());
    }
    writeln!(out, "warnings (not drift, not counted):")?;
    for w in &report.relation_warnings {
        writeln!(out, "  - {}", w.message)?;
    }
    Ok(())
}

/// Columns the engine cannot carry. Printed on both the clean and dirty paths:
/// not drift, but the only place it becomes visible — from the outside the
/// column simply is not there.
fn write_skipped<W: Write>(report: &DiffReport, out: &mut W) -> std::io::Result<()> {
    if report.skipped_columns.is_empty() {
        return Ok(());
    }
    writeln!(
        out,
        "not exposed (no type mapping — these columns are absent from the schema):"
    )?;
    for c in &report.skipped_columns {
        writeln!(out, "  - {}.{} ({})", c.table, c.column, c.data_type)?;
    }
    let types: BTreeSet<&str> = report
        .skipped_columns
        .iter()
        .map(|c| c.data_type.as_str())
        .collect();
    writeln!(
        out,
        "    {} column(s) across {} type(s): {}",
        report.skipped_columns.len(),
        types.len(),
        types.into_iter().collect::<Vec<_>>().join(", ")
    )?;
    Ok(())
}

/// Repoints aimed at a schema that was not introspected. Printed on both paths
/// for the same reason: leaving it silent would let `diff` read as "everything
/// checked out" when part of the overlay was never looked at.
fn write_unverified<W: Write>(report: &DiffReport, out: &mut W) -> std::io::Result<()> {
    if report.unverified_repoints.is_empty() {
        return Ok(());
    }
    writeln!(out, "not checked (schema not introspected):")?;
    for r in &report.unverified_repoints {
        writeln!(
            out,
            "  - {}: schema = \"{}\" — add --schema {} to verify it",
            r.table, r.schema, r.schema
        )?;
    }
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
    use crate::analyze::{
        BadRepoint, Collision, ColumnOrigin, DiffReport, MissingColumn, MissingRelTarget,
        UnverifiedRepoint,
    };

    fn dirty_report() -> DiffReport {
        DiffReport {
            skipped_columns: Vec::new(),
            relation_warnings: Vec::new(),
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
            bad_repoints: vec![BadRepoint {
                table: "orders".into(),
                schema: "archive".into(),
                problem: RepointProblem::ColumnsMissing,
                missing_columns: vec!["total".into()],
            }],
            unverified_repoints: vec![UnverifiedRepoint {
                table: "invoices".into(),
                schema: "cold_storage".into(),
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
        assert!(s.contains("broken schema repoints"));
        assert!(s.contains("archive"));
        assert!(s.contains("5 issues found"), "got: {s}");
    }

    /// An unverifiable repoint is not drift, so it must not be counted — but it
    /// must still be printed, or `diff` reads as "all checked" when it wasn't.
    #[test]
    fn unverified_repoints_print_without_counting_as_issues() {
        let report = DiffReport {
            unverified_repoints: vec![UnverifiedRepoint {
                table: "invoices".into(),
                schema: "cold_storage".into(),
            }],
            ..Default::default()
        };
        assert!(report.is_clean(), "unverified must not fail diff");
        assert_eq!(report.issue_count(), 0);

        let mut buf = Vec::new();
        write(&report, Format::Text, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("OK"), "got: {s}");
        assert!(s.contains("not checked"), "got: {s}");
        assert!(s.contains("--schema cold_storage"), "got: {s}");
    }

    /// A relation warning is advice, not drift: it prints on the clean path
    /// and must not flip the exit code.
    #[test]
    fn relation_warnings_print_without_counting_as_issues() {
        let report = DiffReport {
            relation_warnings: vec![crate::analyze::RelationWarning {
                table: "results".into(),
                relation: "pathogen".into(),
                target: "dict".into(),
                remote_columns: vec!["serial".into()],
                message: "object relation results.pathogen can return an arbitrary row".into(),
            }],
            ..Default::default()
        };
        assert!(report.is_clean(), "warnings must not fail diff");
        assert_eq!(report.issue_count(), 0);

        let mut buf = Vec::new();
        write(&report, Format::Text, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("OK"), "got: {s}");
        assert!(s.contains("warnings (not drift"), "got: {s}");
        assert!(s.contains("results.pathogen"), "got: {s}");

        let mut buf = Vec::new();
        write(&report, Format::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["relation_warnings"][0]["table"], "results");
        assert_eq!(v["relation_warnings"][0]["remote_columns"][0], "serial");
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
