use crate::test::{QueryResult, TestStatus};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

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
        let (icon, msg) = match &r.status {
            TestStatus::Pass => ("v", String::new()),
            TestStatus::Fail(m) => ("X", format!("  ({m})")),
            TestStatus::Diff(m) => ("~", format!("  ({m})")),
            TestStatus::Skip(m) => ("-", format!("  ({m})")),
            TestStatus::Error(m) => ("!", format!("  ({m})")),
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

    // Machine-readable summary line for shell script parsing
    println!(
        "BENCH_SUMMARY:{benchmark}:{pass}:{fail}:{diff}:{skip}:{error}:{total}:{total_ms}"
    );
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
            let (status_str, message) = match &r.status {
                TestStatus::Pass => ("pass".to_string(), None),
                TestStatus::Fail(m) => ("fail".to_string(), Some(m.clone())),
                TestStatus::Diff(m) => ("diff".to_string(), Some(m.clone())),
                TestStatus::Skip(m) => ("skip".to_string(), Some(m.clone())),
                TestStatus::Error(m) => ("error".to_string(), Some(m.clone())),
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

    let path = format!(
        "benchmarks/results/{benchmark}-sf{scale}-{protocol}-{timestamp}.json"
    );
    std::fs::create_dir_all("benchmarks/results/")?;
    std::fs::write(&path, serde_json::to_string_pretty(&report)?)?;
    Ok(path)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns (pass, fail, diff, skip, error, total_duration_ms).
fn count_results(results: &[QueryResult]) -> (usize, usize, usize, usize, usize, u64) {
    let pass = results
        .iter()
        .filter(|r| matches!(r.status, TestStatus::Pass))
        .count();
    let fail = results
        .iter()
        .filter(|r| matches!(r.status, TestStatus::Fail(_)))
        .count();
    let diff = results
        .iter()
        .filter(|r| matches!(r.status, TestStatus::Diff(_)))
        .count();
    let skip = results
        .iter()
        .filter(|r| matches!(r.status, TestStatus::Skip(_)))
        .count();
    let error = results
        .iter()
        .filter(|r| matches!(r.status, TestStatus::Error(_)))
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
    use crate::test::{QueryResult, TestStatus};

    fn make_results() -> Vec<QueryResult> {
        vec![
            QueryResult {
                id: "q01".to_string(),
                status: TestStatus::Pass,
                duration: std::time::Duration::from_millis(120),
                rows: 4,
            },
            QueryResult {
                id: "q02".to_string(),
                status: TestStatus::Fail("row count mismatch".to_string()),
                duration: std::time::Duration::from_millis(88),
                rows: 0,
            },
            QueryResult {
                id: "q03".to_string(),
                status: TestStatus::Skip("requires: lateral_join".to_string()),
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
}
