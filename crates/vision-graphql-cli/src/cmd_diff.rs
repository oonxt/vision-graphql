//! Compare a schema.toml against a live database for stale references.

use anyhow::{Context, Result};
use vision_graphql::schema::config::parse;

use crate::analyze::{find_drift, schema_warnings};
use crate::filter::TableFilter;
use crate::report::{self, Format};
use crate::DriftDetected;

pub struct Args {
    pub url: String,
    pub config: std::path::PathBuf,
    pub format: Format,
    pub schemas: Option<Vec<String>>,
    pub include: Option<Vec<String>>,
    pub ignore: Option<Vec<String>>,
}

pub async fn run(args: Args) -> Result<()> {
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("reading {}", args.config.display()))?;
    let cfg = parse(&text).with_context(|| format!("parsing {}", args.config.display()))?;

    let source = crate::db::connect(&args.url)?;
    let found = source.introspect(args.schemas.as_deref()).await?;

    let filter = TableFilter::new(args.include.as_deref(), args.ignore.as_deref())?;
    let mut report = find_drift(&cfg, &found.db, found.dialect, &filter);
    let warnings = schema_warnings(found.into_builder(), &cfg, &filter);
    report.relation_warnings = warnings.relations;
    report.table_warnings = warnings.tables;

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    report::write(&report, args.format, &mut out)?;

    if !report.is_clean() {
        return Err(DriftDetected.into());
    }
    Ok(())
}
