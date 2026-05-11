mod filter;
mod render;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "vision-gql", version, about = "vision-graphql schema overlay tool")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Generate a starter schema.toml from a live database.
    Generate,
    /// Validate a schema.toml against a live database.
    Diff,
    /// Validate a schema.toml's structure without connecting to a database.
    Validate,
}

fn main() {
    let _ = Cli::parse();
    eprintln!("not yet implemented");
    std::process::exit(2);
}
