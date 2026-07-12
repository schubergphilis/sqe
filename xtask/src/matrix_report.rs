//! Regenerate the Iceberg compatibility matrix state.
//!
//! Today (Phase 1): reads the hand-maintained seed at `docs/iceberg-matrix-state.json`,
//! validates it, prints the aggregate score, and optionally checks that the score
//! has not regressed against a reference value.
//!
//! Later phases (tracked in openspec/changes/iceberg-matrix-parity/tasks.md):
//!  - Phase A through H: wire real integration tests to programmatically update
//!    support levels per feature. Each feature rating is justified by a named
//!    integration test. Regressions fail the report.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Parser)]
pub struct Args {
    /// Path to the matrix state file. Defaults to docs/iceberg-matrix-state.json.
    #[arg(long, default_value = "docs/iceberg-matrix-state.json")]
    path: PathBuf,

    /// Fail if the computed percentage is below this value.
    #[arg(long)]
    min_percent: Option<f64>,

    /// Print the aggregate score and exit 0 regardless of min_percent.
    #[arg(long)]
    print_only: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct State {
    #[serde(rename = "$schema", default, skip_serializing_if = "Option::is_none")]
    schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_rubric: Option<String>,
    platform: Platform,
    #[serde(default)]
    generated_at: Option<String>,
    #[serde(default)]
    generated_by: Option<String>,
    score: Score,
    #[serde(default)]
    note: Option<String>,
    support: BTreeMap<String, Entry>,
    #[serde(flatten)]
    rest: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Platform {
    id: String,
    name: String,
    vendor: String,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    group: Option<String>,
    #[serde(default, rename = "docUrl")]
    doc_url: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Score {
    raw: u32,
    max: u32,
    percent: f64,
}

#[derive(Debug, Deserialize, Serialize)]
struct Entry {
    level: String,
    #[serde(default)]
    notes: Option<String>,
    #[serde(default)]
    evidence: Option<String>,
    #[serde(default)]
    caveats: Vec<String>,
}

fn level_weight(level: &str) -> Result<u32> {
    Ok(match level {
        "full" => 3,
        "partial" => 2,
        "unknown" => 1,
        "none" => 0,
        other => bail!("unknown support level: {other}"),
    })
}

pub fn run(args: Args) -> Result<()> {
    let raw = std::fs::read_to_string(&args.path)
        .with_context(|| format!("reading {}", args.path.display()))?;
    let state: State =
        serde_json::from_str(&raw).with_context(|| format!("parsing {}", args.path.display()))?;

    let entries = state.support.len() as u32;
    let max = entries * 3;
    let raw_score: u32 = state
        .support
        .values()
        .map(|e| level_weight(&e.level))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum();
    let percent = if max == 0 {
        0.0
    } else {
        100.0 * raw_score as f64 / max as f64
    };

    println!(
        "platform={} entries={} score={}/{} ({:.1}%)",
        state.platform.id, entries, raw_score, max, percent
    );

    if state.score.raw != raw_score || state.score.max != max {
        bail!(
            "declared score {}/{} disagrees with computed {}/{} -- update the file",
            state.score.raw,
            state.score.max,
            raw_score,
            max,
        );
    }

    if !args.print_only {
        if let Some(min) = args.min_percent {
            if percent < min {
                return Err(anyhow!(
                    "matrix score {:.1}% is below minimum {:.1}%",
                    percent,
                    min
                ));
            }
        }
    }

    Ok(())
}
