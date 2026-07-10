//! `cargo xtask` dev tooling for SQE.
//!
//! Subcommands:
//!   matrix-report -- Recompute the Iceberg compatibility matrix state
//!                    from running integration tests and print/write JSON.

use anyhow::Result;
use clap::{Parser, Subcommand};

mod matrix_report;

#[derive(Parser)]
#[command(name = "xtask", about = "SQE repo-local dev tooling")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Recompute the Iceberg compatibility matrix state and write docs/iceberg-matrix-state.json.
    MatrixReport(matrix_report::Args),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::MatrixReport(args) => matrix_report::run(args),
    }
}
