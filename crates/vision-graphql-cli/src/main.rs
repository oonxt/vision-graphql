mod analyze;
mod cmd_diff;
mod cmd_generate;
mod cmd_validate;
mod filter;
mod log_init;
mod render;
mod report;

use anyhow::{Context, Result};
use clap::{ArgAction, Args as ClapArgs, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "vision-gql",
    version,
    about = "vision-graphql schema overlay tool"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,

    #[arg(short = 'v', long = "verbose", action = ArgAction::Count, global = true)]
    verbose: u8,

    #[arg(short = 'q', long = "quiet", global = true, conflicts_with = "verbose")]
    quiet: bool,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Generate a starter schema.toml from a live database.
    Generate(GenerateArgs),
    /// Validate a schema.toml against a live database.
    Diff(DiffArgs),
    /// Validate a schema.toml's structure without connecting to a database.
    Validate(ValidateArgs),
}

#[derive(ClapArgs, Debug)]
struct CommonDb {
    /// Postgres connection URL. Falls back to $DATABASE_URL if not set.
    #[arg(long)]
    url: Option<String>,

    /// Comma-separated globs; restrict to matching tables.
    #[arg(long, value_delimiter = ',')]
    include_tables: Option<Vec<String>>,

    /// Comma-separated globs; exclude matching tables.
    #[arg(long, value_delimiter = ',')]
    ignore_tables: Option<Vec<String>>,
}

#[derive(ClapArgs, Debug)]
struct GenerateArgs {
    #[command(flatten)]
    db: CommonDb,

    /// Output path; "-" for stdout (default).
    #[arg(short = 'o', long = "output", default_value = "-")]
    output: String,

    /// Overwrite an existing output file.
    #[arg(short = 'f', long = "force")]
    force: bool,
}

#[derive(ClapArgs, Debug)]
struct DiffArgs {
    #[command(flatten)]
    db: CommonDb,

    /// Path to the overlay TOML file.
    #[arg(long, default_value = "./schema.toml")]
    config: PathBuf,

    /// Output format.
    #[arg(long, default_value = "text", value_parser = ["text", "json"])]
    format: String,
}

#[derive(ClapArgs, Debug)]
struct ValidateArgs {
    /// Path to the overlay TOML file.
    path: PathBuf,
}

fn resolve_url(opt: Option<String>) -> Result<String> {
    opt.or_else(|| std::env::var("DATABASE_URL").ok())
        .context("no --url given and DATABASE_URL is not set")
}

fn main() {
    let cli = Cli::parse();
    log_init::install(cli.verbose, cli.quiet);
    let exit = match dispatch(cli) {
        Ok(()) => 0,
        Err(e) => {
            if e.downcast_ref::<DriftDetected>().is_some() {
                1
            } else {
                eprintln!("error: {e:#}");
                2
            }
        }
    };
    std::process::exit(exit);
}

fn dispatch(cli: Cli) -> Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting tokio runtime")?;
    rt.block_on(async {
        match cli.cmd {
            Cmd::Generate(a) => {
                let url = resolve_url(a.db.url)?;
                cmd_generate::run(cmd_generate::Args {
                    url,
                    output: a.output,
                    force: a.force,
                    include: a.db.include_tables,
                    ignore: a.db.ignore_tables,
                })
                .await
            }
            Cmd::Diff(a) => {
                let url = resolve_url(a.db.url)?;
                let format = match a.format.as_str() {
                    "json" => report::Format::Json,
                    _ => report::Format::Text,
                };
                cmd_diff::run(cmd_diff::Args {
                    url,
                    config: a.config,
                    format,
                    include: a.db.include_tables,
                    ignore: a.db.ignore_tables,
                })
                .await
            }
            Cmd::Validate(a) => cmd_validate::run(&a.path),
        }
    })
}

#[derive(Debug)]
pub struct DriftDetected;
impl std::fmt::Display for DriftDetected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "drift detected")
    }
}
impl std::error::Error for DriftDetected {}
