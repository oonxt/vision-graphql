//! Write the GraphQL schema a live database (plus an optional overlay) exposes.
//!
//! The exposed surface is otherwise implicit — it is what introspection found,
//! minus what `hide_columns` removed, renamed by `expose_as`, plus whatever the
//! overlay declared. Committing the SDL turns "did that migration expose a new
//! column" into a line in a diff. `--check` is the CI half of that: it fails
//! when the file on disk no longer matches the database.

use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use vision_graphql::schema::introspect::introspect_schemas;
use vision_graphql::schema::merge::build_from_introspection;

use crate::cmd_generate::build_pool_pub;
use crate::filter::TableFilter;
use crate::render::redact_url;
use crate::DriftDetected;

pub struct Args {
    pub url: String,
    pub output: String,
    pub force: bool,
    pub check: bool,
    pub config: Option<PathBuf>,
    pub schemas: Vec<String>,
    pub include: Option<Vec<String>>,
    pub ignore: Option<Vec<String>>,
}

pub async fn run(args: Args) -> Result<()> {
    let pool = build_pool_pub(&args.url)?;
    let schemas: Vec<&str> = args.schemas.iter().map(String::as_str).collect();
    let db = introspect_schemas(&pool, &schemas)
        .await
        .with_context(|| format!("introspect failed against {}", redact_url(&args.url)))?;

    let filter = TableFilter::new(args.include.as_deref(), args.ignore.as_deref())?;
    // Filtering happens on the exposed names, the same ones the overlay and the
    // generated TOML use.
    let mut builder = build_from_introspection(db).retain_tables(|name| filter.keep(name));

    if let Some(path) = &args.config {
        builder = builder
            .load_config(path)
            .with_context(|| format!("loading overlay {}", path.display()))?;
    }

    let schema = builder.build();
    let sdl = vision_graphql::sdl::render(schema.type_system());

    if args.check {
        let path = match &args.output {
            o if o == "-" => bail!("--check needs a file to compare against, not stdout"),
            o => PathBuf::from(o),
        };
        let on_disk = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {} to compare against", path.display()))?;
        if on_disk == sdl {
            return Ok(());
        }
        eprintln!(
            "{} is out of date with {}",
            path.display(),
            redact_url(&args.url)
        );
        for line in diff_summary(&on_disk, &sdl) {
            eprintln!("{line}");
        }
        return Err(DriftDetected.into());
    }

    match args.output.as_str() {
        "-" => print!("{sdl}"),
        path => {
            let path = PathBuf::from(path);
            if path.exists() && !args.force {
                bail!("refusing to overwrite {} without --force", path.display());
            }
            std::fs::write(&path, sdl.as_bytes())
                .with_context(|| format!("writing {}", path.display()))?;
        }
    }
    Ok(())
}

/// What changed, summarised for a terminal.
///
/// Type declarations first — `type users {`, `input users_bool_exp {` — because
/// a schema drift is nearly always "a type appeared or went away", and a flat
/// line diff buries that under the fields of whatever was added. Capped, since
/// the file itself is the full answer.
fn diff_summary(old: &str, new: &str) -> Vec<String> {
    const MAX: usize = 20;

    fn declares(line: &str) -> bool {
        let l = line.trim_start();
        ["type ", "input ", "enum ", "scalar ", "schema {"]
            .iter()
            .any(|k| l.starts_with(k))
    }

    let old_lines: Vec<&str> = old.lines().collect();
    let new_lines: Vec<&str> = new.lines().collect();

    let added: Vec<&&str> = new_lines
        .iter()
        .filter(|l| !old_lines.contains(l))
        .collect();
    let removed: Vec<&&str> = old_lines
        .iter()
        .filter(|l| !new_lines.contains(l))
        .collect();

    let mut out = Vec::new();
    let mut total = 0;
    for (sign, lines) in [("+", &added), ("-", &removed)] {
        total += lines.len();
        // Declarations first, then anything else, so the cap spends itself on
        // what identifies the change.
        for line in lines
            .iter()
            .filter(|l| declares(l))
            .chain(lines.iter().filter(|l| !declares(l)))
        {
            if out.len() >= MAX {
                break;
            }
            out.push(format!("  {sign} {}", line.trim_end()));
        }
    }
    if total > out.len() {
        out.push(format!(
            "  … {} more changed lines; regenerate to see the whole file",
            total - out.len()
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_summary_reports_both_directions() {
        let old = "type users {\n  id: Int!\n}\n";
        let new = "type users {\n  id: Int!\n  secret: String\n}\n";
        let d = diff_summary(old, new);
        assert!(d.iter().any(|l| l.contains("+   secret: String")), "{d:?}");
    }

    #[test]
    fn diff_summary_truncates_and_says_how_much_it_left_out() {
        let new: String = (0..100).map(|i| format!("line {i}\n")).collect();
        let d = diff_summary("", &new);
        assert!(d.last().unwrap().contains("more changed lines"), "{d:?}");
    }

    #[test]
    fn diff_summary_leads_with_type_declarations() {
        // The fields of a new type must not crowd out the fact that a type
        // appeared, which is the thing a reviewer is looking for.
        let new =
            "type users {\n  a: Int\n  b: Int\n  c: Int\n}\ninput users_bool_exp {\n  a: Int\n}\n";
        let d = diff_summary("", new);
        assert!(d[0].contains("type users {"), "{d:?}");
        assert!(d[1].contains("input users_bool_exp {"), "{d:?}");
    }
}
