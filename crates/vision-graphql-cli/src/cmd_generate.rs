//! Generate a starter schema.toml from a live database.

use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::filter::TableFilter;
use crate::render::{redact_url, toml_template, HeaderMeta};

pub struct Args {
    pub url: String,
    pub output: String,
    pub force: bool,
    pub schemas: Option<Vec<String>>,
    pub include: Option<Vec<String>>,
    pub ignore: Option<Vec<String>>,
}

pub async fn run(args: Args) -> Result<()> {
    let output_target = if args.output == "-" {
        OutputTarget::Stdout
    } else {
        OutputTarget::File(PathBuf::from(&args.output))
    };

    if let OutputTarget::File(p) = &output_target {
        if p.exists() && !args.force {
            bail!("refusing to overwrite {} without --force", p.display());
        }
    }

    let source = crate::db::connect(&args.url)?;
    let found = source.introspect(args.schemas.as_deref()).await?;

    let filter = TableFilter::new(args.include.as_deref(), args.ignore.as_deref())?;
    let meta = HeaderMeta {
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp_iso8601: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown".into()),
        redacted_source_url: redact_url(&args.url),
    };

    let body = toml_template(&found.db, &filter, &meta, found.dialect);

    match output_target {
        OutputTarget::Stdout => {
            print!("{body}");
        }
        OutputTarget::File(p) => {
            std::fs::write(&p, body.as_bytes())
                .with_context(|| format!("writing {}", p.display()))?;
        }
    }
    Ok(())
}

enum OutputTarget {
    Stdout,
    File(PathBuf),
}
