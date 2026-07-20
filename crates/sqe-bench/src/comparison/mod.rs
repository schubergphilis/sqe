//! Side-by-side benchmark comparison: run identical queries against SQE and Trino.

pub mod classify;

use crate::client::BenchClient;
use crate::report::{CompareStatusReport, ComparisonReport, ComparisonSummary, QueryComparison};
use classify::{canonical_rows, classify_status, is_transport_error, load_expected_rows};
use std::time::Instant;
use tracing::info;

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
        let sql = crate::query::prefix_tables(&raw_sql, &namespace, benchmark);
        // Strip trailing semicolons -- Trino HTTP protocol rejects them.
        // Trim whitespace first (files end with \n after ;)
        let sql = sql.trim().trim_end_matches(';').trim().to_string();

        // Skip DML queries (UPDATE, DELETE, INSERT, MERGE) in comparison mode.
        // Both engines would modify the same table, causing data corruption.
        // DML correctness is verified by the regular sqe-bench test, not compare.
        if classify::is_dml(&sql) {
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
                sqe_result
                    .as_ref()
                    .err()
                    .map(|e| e.to_string())
                    .unwrap_or_default()
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

        let rows_match = sqe_error.is_none() && trino_error.is_none() && sqe_rows == trino_rows;

        let canonical = canonical_rows(expected_rows.as_ref(), benchmark, &query_name, scale);
        let status = classify_status(&sqe_error, &trino_error, sqe_rows, trino_rows, canonical);

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
    let dialect_diff = comparisons
        .iter()
        .filter(|c| matches!(c.status, CompareStatusReport::DialectDiff))
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
                    | CompareStatusReport::DialectDiff
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
            dialect_diff,
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
            CompareStatusReport::DialectDiff => "DIALECT OK",
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
        "\n**Total:** SQE {}ms, Trino {}ms, Avg speedup {:.1}x, Matched {}/{} ({} vacuous: 0 rows on both engines, {} expected-empty: canonically 0, {} vacuous-bug: canonically non-zero but empty, {} dialect-diff: SQE matches canonical, Trino diverges)\n",
        report.summary.sqe_total_ms,
        report.summary.trino_total_ms,
        report.summary.avg_speedup,
        report.summary.matched,
        report.summary.total,
        report.summary.vacuous,
        report.summary.expected_empty,
        report.summary.vacuous_bug,
        report.summary.dialect_diff
    );

    Ok(report)
}
