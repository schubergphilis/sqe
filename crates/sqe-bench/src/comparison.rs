//! Side-by-side benchmark comparison: run identical queries against SQE and Trino.

use crate::client::BenchClient;
use crate::report::{
    CompareStatusReport, ComparisonReport, ComparisonSummary, QueryComparison,
};
use std::collections::HashMap;
use std::time::Instant;
use tracing::{info, warn};

/// Default location (relative to repo root) of the canonical-row manifest.
const DEFAULT_EXPECTED_ROWS_PATH: &str = "benchmarks/expected/canonical_rows_duckdb.json";

/// Canonical-row manifest: { benchmark -> { query_name -> { "sf{N}_official_rows": count } } }.
type ExpectedRows = HashMap<String, HashMap<String, HashMap<String, i64>>>;

/// Load the canonical-row manifest. Path comes from `BENCH_EXPECTED_ROWS`, or
/// defaults to `benchmarks/expected/canonical_rows_duckdb.json`. Returns `None`
/// if the file is absent (existing runs keep working) or unparseable (with a
/// warning), so the assertion gracefully no-ops.
fn load_expected_rows() -> Option<ExpectedRows> {
    let path = std::env::var("BENCH_EXPECTED_ROWS")
        .unwrap_or_else(|_| DEFAULT_EXPECTED_ROWS_PATH.to_string());
    if !std::path::Path::new(&path).exists() {
        return None;
    }
    match std::fs::read_to_string(&path) {
        Ok(s) => match serde_json::from_str::<ExpectedRows>(&s) {
            Ok(m) => Some(m),
            Err(e) => {
                warn!("Could not parse expected-rows manifest {}: {}", path, e);
                None
            }
        },
        Err(e) => {
            warn!("Could not read expected-rows manifest {}: {}", path, e);
            None
        }
    }
}

/// Look up the canonical row count for `(benchmark, query_name)` at `scale`.
/// Returns `None` when there is no manifest, no entry, or no count for this
/// scale (e.g. SF10, which is not in the manifest yet). The scale key is built
/// from `format_scale` so it is forward-compatible: scale 1.0 -> `sf1_official_rows`.
fn canonical_rows(
    manifest: Option<&ExpectedRows>,
    benchmark: &str,
    query_name: &str,
    scale: f64,
) -> Option<i64> {
    let key = format!("sf{}_official_rows", crate::format_scale(scale));
    manifest?
        .get(benchmark)?
        .get(query_name)?
        .get(&key)
        .copied()
}

/// Classify a single query comparison. Pure: no I/O, so it is unit-testable.
/// `canonical` is the manifest-declared row count for this query/scale, or
/// `None` when unknown. The vacuous (0-rows-on-both) arm splits on `canonical`:
/// unknown -> `Vacuous`, `Some(0)` -> `ExpectedEmpty` (pass), `Some(n>0)` ->
/// `VacuousBug` (fail). All other arms are unchanged.
fn classify_status(
    sqe_error: &Option<String>,
    trino_error: &Option<String>,
    sqe_rows: usize,
    trino_rows: usize,
    canonical: Option<i64>,
) -> CompareStatusReport {
    let rows_match = sqe_error.is_none() && trino_error.is_none() && sqe_rows == trino_rows;
    match (sqe_error, trino_error) {
        (None, None) if rows_match && sqe_rows == 0 => match canonical {
            Some(0) => CompareStatusReport::ExpectedEmpty,
            Some(_) => CompareStatusReport::VacuousBug,
            None => CompareStatusReport::Vacuous,
        },
        (None, None) if rows_match => CompareStatusReport::Match,
        (None, None) => CompareStatusReport::RowDiff,
        (Some(_), None) => CompareStatusReport::SqeFailed,
        (None, Some(_)) => CompareStatusReport::TrinoFailed,
        (Some(_), Some(_)) => CompareStatusReport::BothFailed,
    }
}

/// Connection/transport-level failure, as opposed to a query-level error.
/// These are safe to retry once: the query never reached execution, or the
/// stream died for reasons unrelated to the SQL.
fn is_transport_error(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    m.contains("h2 protocol error")
        || m.contains("transport error")
        || m.contains("connection reset")
        || m.contains("connection refused")
        || m.contains("broken pipe")
        || m.contains("goaway")
}

/// Run comparison benchmark.
#[allow(clippy::too_many_arguments)]
pub async fn run_comparison(
    benchmark: &str,
    scale: f64,
    sqe_client: &dyn BenchClient,
    trino_client: &dyn BenchClient,
    sqe_endpoint: &str,
    trino_endpoint: &str,
    query_filter: Option<&str>,
    output_dir: &str,
) -> anyhow::Result<ComparisonReport> {
    // Load query files
    let query_dir = format!("benchmarks/queries/{}", benchmark);
    let mut query_files: Vec<_> = std::fs::read_dir(&query_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "sql"))
        .collect();
    query_files.sort_by_key(|e| e.file_name());

    // Filter to single query if specified
    if let Some(q) = query_filter {
        let q_normalized = q.trim_start_matches('q');
        query_files.retain(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.contains(q) || name.contains(&format!("q{}", q_normalized))
        });
    }

    info!("Comparing {} queries from {}", query_files.len(), benchmark);

    // Canonical-row manifest for the expected-row-count assertion. Absent file
    // => None => vacuous queries keep today's behavior.
    let expected_rows = load_expected_rows();

    let mut comparisons = Vec::new();

    for entry in &query_files {
        let query_name = entry
            .file_name()
            .to_string_lossy()
            .trim_end_matches(".sql")
            .to_string();
        let raw_sql = std::fs::read_to_string(entry.path())?;

        // Qualify bare table names with the benchmark namespace.
        // TPC-BB uses TPC-DS namespace (same as sqe-bench test).
        let namespace = if benchmark == "tpcbb" {
            crate::bench_namespace("tpcds", scale)
        } else {
            crate::bench_namespace(benchmark, scale)
        };
        let sql = crate::test::prefix_tables(&raw_sql, &namespace, benchmark);
        // Strip trailing semicolons -- Trino HTTP protocol rejects them.
        // Trim whitespace first (files end with \n after ;)
        let sql = sql.trim().trim_end_matches(';').trim().to_string();

        // Skip DML queries (UPDATE, DELETE, INSERT, MERGE) in comparison mode.
        // Both engines would modify the same table, causing data corruption.
        // DML correctness is verified by the regular sqe-bench test, not compare.
        let sql_upper = sql.trim().to_uppercase();
        let is_dml = sql_upper.starts_with("UPDATE ")
            || sql_upper.starts_with("DELETE ")
            || sql_upper.starts_with("INSERT ")
            || sql_upper.starts_with("MERGE ");
        // Also check after stripping comments (-- name: ...)
        let first_stmt = sql.lines()
            .find(|l| !l.trim().starts_with("--") && !l.trim().is_empty())
            .unwrap_or("")
            .trim()
            .to_uppercase();
        let is_dml = is_dml
            || first_stmt.starts_with("UPDATE ")
            || first_stmt.starts_with("DELETE ")
            || first_stmt.starts_with("INSERT ")
            || first_stmt.starts_with("MERGE ");

        if is_dml {
            info!("  {} ... SKIPPED (DML)", query_name);
            continue;
        }

        info!("  {} ...", query_name);

        // Run against SQE. A long compare sweep reuses one gRPC channel
        // for ~100 queries; a single h2-level connection failure (e.g.
        // GoAway FRAME_SIZE_ERROR, seen once per multi-hour SF10 sweep)
        // otherwise reports as a 1ms "0 rows" SqeFailed and poisons the
        // comparison. tonic channels reconnect lazily, so one retry on a
        // transport-shaped error runs on a fresh connection. Query-level
        // errors (plan, execution) are NOT retried -- those are real.
        let sqe_start = Instant::now();
        let mut sqe_result = sqe_client.execute(&sql).await;
        let mut sqe_elapsed = sqe_start.elapsed();
        if sqe_result
            .as_ref()
            .err()
            .is_some_and(|e| is_transport_error(&e.to_string()))
        {
            info!(
                "  {} SQE transport error ({}), retrying once on a fresh connection",
                query_name,
                sqe_result.as_ref().err().map(|e| e.to_string()).unwrap_or_default()
            );
            let retry_start = Instant::now();
            sqe_result = sqe_client.execute(&sql).await;
            sqe_elapsed = retry_start.elapsed();
        }

        // Run against Trino
        let trino_start = Instant::now();
        let trino_result = trino_client.execute(&sql).await;
        let trino_elapsed = trino_start.elapsed();

        let sqe_rows = sqe_result
            .as_ref()
            .map(|batches| batches.iter().map(|b| b.num_rows()).sum::<usize>())
            .unwrap_or(0);
        let sqe_error = sqe_result.as_ref().err().map(|e| e.to_string());

        let trino_rows = trino_result
            .as_ref()
            .map(|batches| batches.iter().map(|b| b.num_rows()).sum::<usize>())
            .unwrap_or(0);
        let trino_error = trino_result.as_ref().err().map(|e| e.to_string());

        let sqe_time_ms = sqe_elapsed.as_millis() as u64;
        let trino_time_ms = trino_elapsed.as_millis() as u64;

        let rows_match =
            sqe_error.is_none() && trino_error.is_none() && sqe_rows == trino_rows;

        let canonical =
            canonical_rows(expected_rows.as_ref(), benchmark, &query_name, scale);
        let status =
            classify_status(&sqe_error, &trino_error, sqe_rows, trino_rows, canonical);

        let speedup = if sqe_time_ms > 0 {
            trino_time_ms as f64 / sqe_time_ms as f64
        } else {
            0.0
        };

        info!(
            "    SQE: {}ms ({} rows) | Trino: {}ms ({} rows) | {:.1}x | {:?}",
            sqe_time_ms, sqe_rows, trino_time_ms, trino_rows, speedup, status
        );

        comparisons.push(QueryComparison {
            query_name,
            sqe_time_ms,
            trino_time_ms,
            speedup,
            sqe_rows,
            trino_rows,
            rows_match,
            sqe_error,
            trino_error,
            status,
        });
    }

    // Compute summary
    let total = comparisons.len();
    let matched = comparisons
        .iter()
        .filter(|c| matches!(c.status, CompareStatusReport::Match))
        .count();
    let vacuous = comparisons
        .iter()
        .filter(|c| matches!(c.status, CompareStatusReport::Vacuous))
        .count();
    let expected_empty = comparisons
        .iter()
        .filter(|c| matches!(c.status, CompareStatusReport::ExpectedEmpty))
        .count();
    let vacuous_bug = comparisons
        .iter()
        .filter(|c| matches!(c.status, CompareStatusReport::VacuousBug))
        .count();
    let row_diff = comparisons
        .iter()
        .filter(|c| matches!(c.status, CompareStatusReport::RowDiff))
        .count();
    let sqe_failed = comparisons
        .iter()
        .filter(|c| matches!(c.status, CompareStatusReport::SqeFailed))
        .count();
    let trino_failed = comparisons
        .iter()
        .filter(|c| matches!(c.status, CompareStatusReport::TrinoFailed))
        .count();
    let both_failed = comparisons
        .iter()
        .filter(|c| matches!(c.status, CompareStatusReport::BothFailed))
        .count();

    let sqe_total_ms: u64 = comparisons.iter().map(|c| c.sqe_time_ms).sum();
    let trino_total_ms: u64 = comparisons.iter().map(|c| c.trino_time_ms).sum();

    let successful: Vec<f64> = comparisons
        .iter()
        .filter(|c| {
            matches!(
                c.status,
                CompareStatusReport::Match
                    | CompareStatusReport::Vacuous
                    | CompareStatusReport::ExpectedEmpty
                    | CompareStatusReport::VacuousBug
                    | CompareStatusReport::RowDiff
            )
        })
        .map(|c| c.speedup)
        .collect();
    let avg_speedup = if successful.is_empty() {
        0.0
    } else {
        successful.iter().sum::<f64>() / successful.len() as f64
    };
    let median_speedup = if successful.is_empty() {
        0.0
    } else {
        let mut sorted = successful.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        sorted[sorted.len() / 2]
    };

    let report = ComparisonReport {
        benchmark: benchmark.to_string(),
        scale,
        timestamp: chrono::Utc::now().to_rfc3339(),
        sqe_endpoint: sqe_endpoint.to_string(),
        trino_endpoint: trino_endpoint.to_string(),
        queries: comparisons,
        summary: ComparisonSummary {
            total,
            matched,
            vacuous,
            expected_empty,
            vacuous_bug,
            row_diff,
            sqe_failed,
            trino_failed,
            both_failed,
            avg_speedup,
            median_speedup,
            sqe_total_ms,
            trino_total_ms,
        },
    };

    // Save JSON report
    let output_path = std::path::Path::new(output_dir);
    std::fs::create_dir_all(output_path)?;
    let filename = format!(
        "compare-{}-sf{}-{}.json",
        benchmark,
        crate::format_scale(scale),
        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S")
    );
    let report_path = output_path.join(&filename);
    std::fs::write(&report_path, serde_json::to_string_pretty(&report)?)?;
    info!("Report saved to {}", report_path.display());

    // Print markdown summary
    println!(
        "\n## {} SF{} — SQE vs Trino\n",
        benchmark.to_uppercase(),
        crate::format_scale(scale)
    );
    println!("| Query | SQE (ms) | Trino (ms) | Speedup | Rows | Status |");
    println!("|---|---|---|---|---|---|");
    for q in &report.queries {
        let status_icon = match q.status {
            CompareStatusReport::Match => "OK",
            CompareStatusReport::Vacuous => "VACUOUS",
            CompareStatusReport::ExpectedEmpty => "EMPTY OK",
            CompareStatusReport::VacuousBug => "VACUOUS BUG",
            CompareStatusReport::RowDiff => "DIFF",
            CompareStatusReport::SqeFailed => "FAIL SQE",
            CompareStatusReport::TrinoFailed => "FAIL Trino",
            CompareStatusReport::BothFailed => "FAIL Both",
        };
        println!(
            "| {} | {} | {} | {:.1}x | {}/{} | {} |",
            q.query_name,
            q.sqe_time_ms,
            q.trino_time_ms,
            q.speedup,
            q.sqe_rows,
            q.trino_rows,
            status_icon
        );
    }
    println!(
        "\n**Total:** SQE {}ms, Trino {}ms, Avg speedup {:.1}x, Matched {}/{} ({} vacuous: 0 rows on both engines, {} expected-empty: canonically 0, {} vacuous-bug: canonically non-zero but empty)\n",
        report.summary.sqe_total_ms,
        report.summary.trino_total_ms,
        report.summary.avg_speedup,
        report.summary.matched,
        report.summary.total,
        report.summary.vacuous,
        report.summary.expected_empty,
        report.summary.vacuous_bug
    );

    Ok(report)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn none() -> Option<String> {
        None
    }
    fn err() -> Option<String> {
        Some("boom".to_string())
    }

    #[test]
    fn vacuous_with_canonical_zero_is_expected_empty() {
        let s = classify_status(&none(), &none(), 0, 0, Some(0));
        assert!(matches!(s, CompareStatusReport::ExpectedEmpty));
    }

    #[test]
    fn vacuous_with_canonical_nonzero_is_vacuous_bug() {
        let s = classify_status(&none(), &none(), 0, 0, Some(5));
        assert!(matches!(s, CompareStatusReport::VacuousBug));
    }

    #[test]
    fn vacuous_without_manifest_entry_stays_vacuous() {
        let s = classify_status(&none(), &none(), 0, 0, None);
        assert!(matches!(s, CompareStatusReport::Vacuous));
    }

    #[test]
    fn non_vacuous_unaffected_by_canonical() {
        // Rows on both, matching => Match regardless of canonical.
        assert!(matches!(
            classify_status(&none(), &none(), 5, 5, Some(0)),
            CompareStatusReport::Match
        ));
        // Row count differs => RowDiff.
        assert!(matches!(
            classify_status(&none(), &none(), 5, 7, Some(0)),
            CompareStatusReport::RowDiff
        ));
        // Engine failures unchanged.
        assert!(matches!(
            classify_status(&err(), &none(), 0, 0, Some(0)),
            CompareStatusReport::SqeFailed
        ));
        assert!(matches!(
            classify_status(&none(), &err(), 0, 0, Some(7)),
            CompareStatusReport::TrinoFailed
        ));
        assert!(matches!(
            classify_status(&err(), &err(), 0, 0, None),
            CompareStatusReport::BothFailed
        ));
    }

    #[test]
    fn canonical_rows_builds_scale_key_and_gates() {
        let mut manifest: ExpectedRows = HashMap::new();
        let mut tpcds = HashMap::new();
        let mut q17 = HashMap::new();
        q17.insert("sf1_official_rows".to_string(), 0i64);
        tpcds.insert("q17".to_string(), q17);
        manifest.insert("tpcds".to_string(), tpcds);

        // sf1 entry exists -> Some(0).
        assert_eq!(
            canonical_rows(Some(&manifest), "tpcds", "q17", 1.0),
            Some(0)
        );
        // sf10 has no key in the manifest -> None (treated as today).
        assert_eq!(canonical_rows(Some(&manifest), "tpcds", "q17", 10.0), None);
        // Unknown query -> None.
        assert_eq!(canonical_rows(Some(&manifest), "tpcds", "q99", 1.0), None);
        // No manifest at all -> None.
        assert_eq!(canonical_rows(None, "tpcds", "q17", 1.0), None);
    }
}
