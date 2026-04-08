//! Side-by-side benchmark comparison: run identical queries against SQE and Trino.

use crate::client::BenchClient;
use crate::report::{
    CompareStatusReport, ComparisonReport, ComparisonSummary, QueryComparison,
};
use std::time::Instant;
use tracing::info;

/// Run comparison benchmark.
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
    let query_dir = format!("crates/sqe-bench/queries/{}", benchmark);
    let mut query_files: Vec<_> = std::fs::read_dir(&query_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
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

    let mut comparisons = Vec::new();

    for entry in &query_files {
        let query_name = entry
            .file_name()
            .to_string_lossy()
            .trim_end_matches(".sql")
            .to_string();
        let sql = std::fs::read_to_string(entry.path())?;

        info!("  {} ...", query_name);

        // Run against SQE
        let sqe_start = Instant::now();
        let sqe_result = sqe_client.execute(&sql).await;
        let sqe_elapsed = sqe_start.elapsed();

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

        let status = match (&sqe_error, &trino_error) {
            (None, None) if rows_match => CompareStatusReport::Match,
            (None, None) => CompareStatusReport::RowDiff,
            (Some(_), None) => CompareStatusReport::SqeFailed,
            (None, Some(_)) => CompareStatusReport::TrinoFailed,
            (Some(_), Some(_)) => CompareStatusReport::BothFailed,
        };

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
                CompareStatusReport::Match | CompareStatusReport::RowDiff
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
        "\n**Total:** SQE {}ms, Trino {}ms, Avg speedup {:.1}x, Matched {}/{}\n",
        report.summary.sqe_total_ms,
        report.summary.trino_total_ms,
        report.summary.avg_speedup,
        report.summary.matched,
        report.summary.total
    );

    Ok(report)
}
