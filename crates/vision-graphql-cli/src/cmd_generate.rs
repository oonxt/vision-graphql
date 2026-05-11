//! Generate a starter schema.toml from a live database.

use anyhow::{bail, Context, Result};
use deadpool_postgres::{Config, Runtime};
use std::path::PathBuf;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio_postgres::NoTls;
use vision_graphql::schema::introspect::introspect;

use crate::filter::TableFilter;
use crate::render::{redact_url, toml_template, HeaderMeta};

pub struct Args {
    pub url: String,
    pub output: String,
    pub force: bool,
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
            bail!(
                "refusing to overwrite {} without --force",
                p.display()
            );
        }
    }

    let pool = build_pool(&args.url)?;
    let db = introspect(&pool)
        .await
        .with_context(|| format!("introspect failed against {}", redact_url(&args.url)))?;

    let filter = TableFilter::new(args.include.as_deref(), args.ignore.as_deref())?;
    let meta = HeaderMeta {
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp_iso8601: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown".into()),
        redacted_source_url: redact_url(&args.url),
    };

    let body = toml_template(&db, &filter, &meta);

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

fn build_pool(url: &str) -> Result<deadpool_postgres::Pool> {
    let cfg: tokio_postgres::Config = url
        .parse()
        .with_context(|| format!("parsing connection URL {}", redact_url(url)))?;
    let mut dp = Config::new();
    dp.host = cfg
        .get_hosts()
        .iter()
        .find_map(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => Some(s.clone()),
            _ => None,
        });
    dp.port = cfg.get_ports().first().copied();
    dp.user = cfg.get_user().map(str::to_string);
    dp.password = cfg
        .get_password()
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(str::to_string);
    dp.dbname = cfg.get_dbname().map(str::to_string);
    dp.create_pool(Some(Runtime::Tokio1), NoTls)
        .context("creating connection pool")
}
