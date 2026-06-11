//! Compare a schema.toml against a live database for stale references.

use anyhow::{Context, Result};
use vision_graphql::schema::config::parse;
use vision_graphql::schema::introspect::introspect;

use crate::analyze::find_drift;
use crate::cmd_generate;
use crate::filter::TableFilter;
use crate::render::redact_url;
use crate::report::{self, Format};
use crate::DriftDetected;

pub struct Args {
    pub url: String,
    pub config: std::path::PathBuf,
    pub format: Format,
    pub include: Option<Vec<String>>,
    pub ignore: Option<Vec<String>>,
}

pub async fn run(args: Args) -> Result<()> {
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("reading {}", args.config.display()))?;
    let cfg = parse(&text).with_context(|| format!("parsing {}", args.config.display()))?;

    let pool = cmd_generate::build_pool_pub(&args.url)?;
    let db = introspect(&pool)
        .await
        .with_context(|| format!("introspect failed against {}", redact_url(&args.url)))?;

    let filter = TableFilter::new(args.include.as_deref(), args.ignore.as_deref())?;
    let report = find_drift(&cfg, &db, &filter);

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    report::write(&report, args.format, &mut out)?;

    if !report.is_clean() {
        return Err(DriftDetected.into());
    }
    Ok(())
}
