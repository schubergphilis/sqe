//! `cargo xtask` dev tooling for SQE.
//!
//! Subcommands:
//!   matrix-report -- Recompute the Iceberg compatibility matrix state
//!                    from running integration tests and print/write JSON.
//!   test-groups   -- The `#[ignore]` vocabulary: list ignored tests by the
//!                    stack they need, check that every reason is in the
//!                    vocabulary (CI), print one group's test names.
//!   budgets       -- Size and structure budgets (.config/budgets.json):
//!                    lines per file and crate, first-party dependency
//!                    edges, Cargo.lock size. `--check` runs in CI.

use anyhow::Result;
use clap::{Parser, Subcommand};

mod budgets;
mod matrix_report;
mod test_groups;

#[derive(Parser)]
#[command(name = "xtask", about = "SQE repo-local dev tooling")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Recompute the Iceberg compatibility matrix state and write docs/evidence/iceberg-matrix-state.json.
    MatrixReport(matrix_report::Args),
    /// List `#[ignore]`d tests by the stack they need; `--check` enforces the vocabulary.
    TestGroups(test_groups::Args),
    /// Size and structure budgets from .config/budgets.json; `--check` enforces them.
    Budgets(budgets::Args),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::MatrixReport(args) => matrix_report::run(args),
        Cmd::TestGroups(args) => test_groups::run(args),
        Cmd::Budgets(args) => budgets::run(args),
    }
}
