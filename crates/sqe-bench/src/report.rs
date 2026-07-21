use crate::status::{legacy_bucket, LegacyBucket, QueryOutcome};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

/// A single query's run result. `outcome` carries the richer `QueryOutcome`
/// taxonomy; the report layer (this module) collapses it onto the stable
/// legacy buckets via `legacy_bucket` so `BENCH_SUMMARY` and the JSON schema
/// stay byte-compatible.
pub struct QueryResult {
    pub id: String,
    pub outcome: QueryOutcome,
    pub duration: std::time::Duration,
    pub rows: usize,
}

#[derive(Serialize)]
pub struct BenchmarkReport {
    pub benchmark: String,
    pub scale_factor: f64,
    pub protocol: String,
    pub timestamp: String,
    pub summary: Summary,
    pub queries: Vec<QueryReportEntry>,
}

#[derive(Serialize)]
pub struct Summary {
    pub total: usize,
    pub pass: usize,
    pub fail: usize,
    pub diff: usize,
    pub skip: usize,
    pub error: usize,
    pub total_duration_ms: u64,
}

#[derive(Serialize)]
pub struct QueryReportEntry {
    pub id: String,
    pub status: String,
    pub duration_ms: u64,
    pub rows: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// ---------------------------------------------------------------------------
// Terminal output
// ---------------------------------------------------------------------------

/// Print a formatted summary table to stdout.
pub fn print_summary(benchmark: &str, scale: f64, protocol: &str, results: &[QueryResult]) {
    println!(
        "\n{} SF{} — {} protocol",
        benchmark.to_uppercase(),
        scale,
        protocol
    );
    println!("{}", "\u{2500}".repeat(60));

    for r in results {
        let (icon, msg) = match &r.outcome {
            QueryOutcome::Pass | QueryOutcome::Vacuous => ("v", String::new()),
            QueryOutcome::WrongRows(m) => ("X", format!("  ({m})")),
            QueryOutcome::Diff(m) => ("~", format!("  ({m})")),
            QueryOutcome::Skip(m) => ("-", format!("  ({m})")),
            QueryOutcome::Error(m) => ("!", format!("  ({m})")),
            QueryOutcome::Timeout(n) => ("!", format!("  (Timed out after {n}s)")),
        };
        println!(
            "{icon} {:<8} {:>8.2}s {:>10} rows{msg}",
            r.id,
            r.duration.as_secs_f64(),
            r.rows,
        );
    }

    let (pass, fail, diff, skip, error, total_ms) = count_results(results);
    let total = results.len();

    println!();
    println!(
        "Results: {pass} pass, {fail} fail, {diff} diff, {skip} skip, {error} error  (total {:.1}s)",
        total_ms as f64 / 1_000.0
    );

    // Extra visibility for the new outcomes folded into the legacy buckets
    // above: timeouts collapse into `error`, vacuous into `pass`. Counted
    // directly from `results` (not via `legacy_bucket`) so they're additive
    // and don't perturb the BENCH_SUMMARY line below.
    let timeout = results
        .iter()
        .filter(|r| matches!(r.outcome, QueryOutcome::Timeout(_)))
        .count();
    let vacuous = results
        .iter()
        .filter(|r| matches!(r.outcome, QueryOutcome::Vacuous))
        .count();
    println!("Extra: {timeout} timeout, {vacuous} vacuous (non-failing)");

    // Machine-readable summary line for shell script parsing
    println!("BENCH_SUMMARY:{benchmark}:{pass}:{fail}:{diff}:{skip}:{error}:{total}:{total_ms}");
}

// ---------------------------------------------------------------------------
// JSON report
// ---------------------------------------------------------------------------

/// Serialise the results to a JSON file under `benchmarks/results/` and
/// return the path of the written file.
pub fn write_json_report(
    benchmark: &str,
    scale: f64,
    protocol: &str,
    results: &[QueryResult],
) -> anyhow::Result<String> {
    let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
    let (pass, fail, diff, skip, error, total_ms) = count_results(results);

    let queries: Vec<QueryReportEntry> = results
        .iter()
        .map(|r| {
            let status_str = match legacy_bucket(&r.outcome) {
                LegacyBucket::Pass => "pass".to_string(),
                LegacyBucket::Fail => "fail".to_string(),
                LegacyBucket::Diff => "diff".to_string(),
                LegacyBucket::Skip => "skip".to_string(),
                LegacyBucket::Error => "error".to_string(),
            };
            let message = match &r.outcome {
                QueryOutcome::WrongRows(m)
                | QueryOutcome::Diff(m)
                | QueryOutcome::Skip(m)
                | QueryOutcome::Error(m) => Some(m.clone()),
                QueryOutcome::Timeout(n) => Some(format!("Timed out after {n}s")),
                QueryOutcome::Pass | QueryOutcome::Vacuous => None,
            };
            QueryReportEntry {
                id: r.id.clone(),
                status: status_str,
                duration_ms: r.duration.as_millis() as u64,
                rows: r.rows,
                message,
            }
        })
        .collect();

    let report = BenchmarkReport {
        benchmark: benchmark.to_string(),
        scale_factor: scale,
        protocol: protocol.to_string(),
        timestamp: timestamp.clone(),
        summary: Summary {
            total: results.len(),
            pass,
            fail,
            diff,
            skip,
            error,
            total_duration_ms: total_ms,
        },
        queries,
    };

    let path = format!("benchmarks/results/{benchmark}-sf{scale}-{protocol}-{timestamp}.json");
    std::fs::create_dir_all("benchmarks/results/")?;
    std::fs::write(&path, serde_json::to_string_pretty(&report)?)?;
    Ok(path)
}

/// Serialise a `ComparisonReport` to a JSON file under `output_dir` and return
/// the written path. The filename format `compare-{bench}-sf{scale}-{ts}.json`
/// is glob-matched by committed results + chart tooling, so it must stay stable.
/// Only the filename timestamp is generated here; `report.timestamp` (the
/// rfc3339 field set at report-build time) is left untouched.
pub(crate) fn write_comparison_report(
    report: &ComparisonReport,
    benchmark: &str,
    scale: f64,
    output_dir: &str,
) -> anyhow::Result<String> {
    let output_path = std::path::Path::new(output_dir);
    std::fs::create_dir_all(output_path)?;
    let filename = format!(
        "compare-{}-sf{}-{}.json",
        benchmark,
        crate::format_scale(scale),
        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S")
    );
    let report_path = output_path.join(&filename);
    std::fs::write(&report_path, serde_json::to_string_pretty(report)?)?;
    Ok(report_path.display().to_string())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns (pass, fail, diff, skip, error, total_duration_ms).
fn count_results(results: &[QueryResult]) -> (usize, usize, usize, usize, usize, u64) {
    let pass = results
        .iter()
        .filter(|r| legacy_bucket(&r.outcome) == LegacyBucket::Pass)
        .count();
    let fail = results
        .iter()
        .filter(|r| legacy_bucket(&r.outcome) == LegacyBucket::Fail)
        .count();
    let diff = results
        .iter()
        .filter(|r| legacy_bucket(&r.outcome) == LegacyBucket::Diff)
        .count();
    let skip = results
        .iter()
        .filter(|r| legacy_bucket(&r.outcome) == LegacyBucket::Skip)
        .count();
    let error = results
        .iter()
        .filter(|r| legacy_bucket(&r.outcome) == LegacyBucket::Error)
        .count();
    let total_ms: u64 = results.iter().map(|r| r.duration.as_millis() as u64).sum();
    (pass, fail, diff, skip, error, total_ms)
}

// ---------------------------------------------------------------------------
// Comparison report types (SQE vs Trino)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryComparison {
    pub query_name: String,
    pub sqe_time_ms: u64,
    pub trino_time_ms: u64,
    pub speedup: f64,
    pub sqe_rows: usize,
    pub trino_rows: usize,
    pub rows_match: bool,
    pub sqe_error: Option<String>,
    pub trino_error: Option<String>,
    pub status: CompareStatusReport,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CompareStatusReport {
    Match,
    /// Both engines returned zero rows. They agree, but the query validated
    /// nothing: with a shared (possibly broken) dataset, empty-vs-empty says
    /// nothing about engine correctness. Tracked separately from Match so
    /// vacuous coverage is visible in every report.
    Vacuous,
    /// Both engines returned zero rows AND the canonical answer for this query
    /// is genuinely zero rows at this scale. The empty result is correct, so
    /// this counts as a pass (distinct from a plain Match, which had rows).
    ExpectedEmpty,
    /// Both engines returned zero rows BUT the canonical answer for this query
    /// is non-zero at this scale. Agreement-on-nothing is hiding a generator or
    /// engine bug: this counts as a failure.
    VacuousBug,
    /// Both engines succeeded with DIFFERENT row counts, but SQE matches the
    /// canonical answer (DuckDB/ClickHouse) and Trino diverges. This is a SQL
    /// dialect difference where Trino is the outlier, not a SQE bug -- e.g.
    /// clickbench q28's `regexp_replace(..., '\1')`: `\1` is a capture-group
    /// backreference in DataFusion/Postgres/ClickHouse/DuckDB but a literal in
    /// Trino. SQE is correct, so this counts as a pass.
    DialectDiff,
    RowDiff,
    SqeFailed,
    TrinoFailed,
    BothFailed,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComparisonReport {
    pub benchmark: String,
    pub scale: f64,
    pub timestamp: String,
    pub sqe_endpoint: String,
    pub trino_endpoint: String,
    pub queries: Vec<QueryComparison>,
    pub summary: ComparisonSummary,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComparisonSummary {
    pub total: usize,
    pub matched: usize,
    #[serde(default)]
    pub vacuous: usize,
    /// Vacuous queries confirmed correct against the canonical-row manifest
    /// (canonical count == 0). Counted as passes.
    #[serde(default)]
    pub expected_empty: usize,
    /// Vacuous queries that should NOT be empty per the manifest (canonical
    /// count > 0). Counted as failures.
    #[serde(default)]
    pub vacuous_bug: usize,
    /// Row counts differed but SQE matched the canonical answer while Trino
    /// diverged on a SQL dialect difference (Trino is the outlier). Passes.
    #[serde(default)]
    pub dialect_diff: usize,
    pub row_diff: usize,
    pub sqe_failed: usize,
    pub trino_failed: usize,
    pub both_failed: usize,
    pub avg_speedup: f64,
    pub median_speedup: f64,
    pub sqe_total_ms: u64,
    pub trino_total_ms: u64,
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_results() -> Vec<QueryResult> {
        vec![
            QueryResult {
                id: "q01".to_string(),
                outcome: QueryOutcome::Pass,
                duration: std::time::Duration::from_millis(120),
                rows: 4,
            },
            QueryResult {
                id: "q02".to_string(),
                outcome: QueryOutcome::WrongRows("row count mismatch".to_string()),
                duration: std::time::Duration::from_millis(88),
                rows: 0,
            },
            QueryResult {
                id: "q03".to_string(),
                outcome: QueryOutcome::Skip("requires: lateral_join".to_string()),
                duration: std::time::Duration::ZERO,
                rows: 0,
            },
        ]
    }

    #[test]
    fn count_results_correct() {
        let results = make_results();
        let (pass, fail, diff, skip, error, total_ms) = count_results(&results);
        assert_eq!(pass, 1);
        assert_eq!(fail, 1);
        assert_eq!(diff, 0);
        assert_eq!(skip, 1);
        assert_eq!(error, 0);
        assert_eq!(total_ms, 208);
    }

    #[test]
    fn write_json_report_creates_file() {
        let results = make_results();
        let path = write_json_report("tpch", 0.001, "flight", &results).unwrap();
        assert!(std::path::Path::new(&path).exists());

        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(v["benchmark"], "tpch");
        assert_eq!(v["summary"]["pass"], 1);
        assert_eq!(v["summary"]["fail"], 1);
        assert_eq!(v["summary"]["skip"], 1);
        assert_eq!(v["queries"][1]["message"], "row count mismatch");

        // Clean up
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn timeout_serializes_as_error_with_message() {
        let results = vec![QueryResult {
            id: "q04".to_string(),
            outcome: QueryOutcome::Timeout(60),
            duration: std::time::Duration::from_secs(60),
            rows: 0,
        }];
        let path = write_json_report("tpch-timeout-test", 0.001, "flight", &results).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(v["queries"][0]["status"], "error");
        assert_eq!(v["queries"][0]["message"], "Timed out after 60s");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn vacuous_serializes_as_pass_with_no_message() {
        let results = vec![QueryResult {
            id: "q05".to_string(),
            outcome: QueryOutcome::Vacuous,
            duration: std::time::Duration::from_millis(10),
            rows: 0,
        }];
        let path = write_json_report("tpch-vacuous-test", 0.001, "flight", &results).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(v["queries"][0]["status"], "pass");
        assert!(v["queries"][0]["message"].is_null());

        let _ = std::fs::remove_file(&path);
    }
}
