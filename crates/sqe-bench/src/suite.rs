//! Single-pass suite driver.
//!
//! `run_suite` executes each benchmark query EXACTLY ONCE against SQE and, when
//! a Trino client is supplied, ONCE against Trino, deriving both the standard
//! per-query report results and the optional SQE-vs-Trino comparison from one
//! sweep. It replaces the previous two-pass shape where the test path
//! (`test::run_benchmark_test`) and the compare path (`comparison::run_comparison`)
//! each ran SQE independently. This module produces the in-memory
//! `SuiteOutcome`; writing the report/compare JSON is the caller's job.

use crate::client::BenchClient;
use crate::comparison::classify;
use crate::report::{
    CompareStatusReport, ComparisonReport, ComparisonSummary, QueryComparison, QueryResult,
};
use crate::status::{classify_vs_expected, QueryOutcome};

/// The result of a single-pass suite run: the per-query report results always,
/// and the SQE-vs-Trino comparison when a Trino client was supplied.
pub struct SuiteOutcome {
    pub results: Vec<QueryResult>,
    pub comparison: Option<ComparisonReport>,
}

/// Run a benchmark suite in a single pass. Loads the query files, then executes
/// each query once against SQE (and once against Trino when `trino` is `Some`).
#[allow(clippy::too_many_arguments)]
pub async fn run_suite(
    sqe: &dyn BenchClient,
    trino: Option<&dyn BenchClient>,
    benchmark: &str,
    scale: f64,
    query_filter: Option<&str>,
    catalog: Option<&str>,
    namespace_override: Option<&str>,
    sqe_endpoint: &str,
    trino_endpoint: Option<&str>,
) -> anyhow::Result<SuiteOutcome> {
    let queries = crate::query::load_query_files(benchmark)?;
    run_suite_with_queries(
        sqe,
        trino,
        benchmark,
        scale,
        query_filter,
        queries,
        catalog,
        namespace_override,
        sqe_endpoint,
        trino_endpoint,
    )
    .await
}

/// Inner driver: the file IO is done by `run_suite`; this owns the per-query
/// loop. Split out so the in-binary single-pass test can inject a hand-built
/// `Vec<QueryFile>` and counting `BenchClient`s without touching the filesystem.
#[allow(clippy::too_many_arguments)]
async fn run_suite_with_queries(
    sqe: &dyn BenchClient,
    trino: Option<&dyn BenchClient>,
    benchmark: &str,
    scale: f64,
    query_filter: Option<&str>,
    queries: Vec<crate::query::QueryFile>,
    catalog: Option<&str>,
    namespace_override: Option<&str>,
    sqe_endpoint: &str,
    trino_endpoint: Option<&str>,
) -> anyhow::Result<SuiteOutcome> {
    // Namespace: mirror `test::run_benchmark_test` exactly (override wins;
    // tpcbb reads the tpcds namespace; then apply the catalog prefix).
    let ns_base = match namespace_override {
        Some(ns) => ns.to_string(),
        None if benchmark == "tpcbb" => crate::bench_namespace("tpcds", scale),
        None => crate::bench_namespace(benchmark, scale),
    };
    let namespace = match catalog {
        Some(cat) => format!("{cat}.{ns_base}"),
        None => ns_base,
    };

    // Canonical-row manifest for the comparison arm's expected-row assertion.
    // Absent file => None => vacuous queries keep today's behavior. Loaded once.
    let expected_rows = classify::load_expected_rows();

    let mut results: Vec<QueryResult> = Vec::new();
    let mut comparisons: Vec<QueryComparison> = Vec::new();

    for query in &queries {
        // Filter: honor `--query`, accepting both "q01" and "1" style ids.
        if let Some(filter) = query_filter {
            if crate::query::normalize_query_id(&query.id)
                != crate::query::normalize_query_id(filter)
            {
                continue;
            }
        }

        // Queries requiring unsupported features are skipped without execution.
        if !query.requires.is_empty() {
            results.push(QueryResult {
                id: query.id.clone(),
                outcome: QueryOutcome::Skip(format!("requires: {}", query.requires.join(", "))),
                duration: std::time::Duration::ZERO,
                rows: 0,
            });
            continue;
        }

        // Qualify bare table names with the benchmark namespace. This is the
        // SQL sent to SQE, byte-identical to the test path.
        let sql = crate::query::prefix_tables(&query.sql, &namespace, benchmark);
        let timeout = crate::execute::resolve_timeout(query.timeout_secs);

        // The ONE SQE execution. Its `QueryRun` feeds BOTH the report outcome
        // and (in compare mode) the comparison's sqe_rows/sqe_error/time.
        let run = crate::execute::run_query(sqe, &query.id, &sql, timeout).await;

        let outcome = if let Some(n) = run.timed_out_after {
            QueryOutcome::Timeout(n)
        } else {
            match &run.result {
                Err(e) => QueryOutcome::Error(e.clone()),
                Ok(batches) => {
                    let c = classify_vs_expected(benchmark, scale, &query.id, batches);
                    // A zero-row result that classifies Pass only because there
                    // is no expected file is vacuous, not a real pass.
                    if run.rows == 0
                        && c == QueryOutcome::Pass
                        && crate::status::load_expected(benchmark, scale, &query.id)?.is_none()
                    {
                        QueryOutcome::Vacuous
                    } else {
                        c
                    }
                }
            }
        };

        results.push(QueryResult {
            id: query.id.clone(),
            outcome,
            duration: run.duration,
            rows: run.rows,
        });

        // Comparison arm: only when a Trino client is supplied.
        if let Some(trino_client) = trino {
            // Trino's HTTP protocol rejects trailing semicolons, so strip them
            // (and surrounding whitespace) for the Trino call and the DML check.
            // SQE keeps the un-stripped SQL above; the strip is inert for SQE,
            // so its rows/time are unchanged vs the old compare path.
            let trino_sql = sql.trim().trim_end_matches(';').trim().to_string();

            // Skip DML in comparison mode: running it against both engines would
            // mutate the one shared table. DML correctness is checked by SQE
            // above (it still ran); it just gets no comparison entry.
            if classify::is_dml(&trino_sql) {
                continue;
            }

            // Derive SQE's comparison inputs from the single run above.
            let sqe_rows = run.rows;
            let sqe_error = run.result.as_ref().err().cloned();
            let sqe_time_ms = run.duration.as_millis() as u64;

            // The ONE Trino execution.
            let trino_run =
                crate::execute::run_query(trino_client, &query.id, &trino_sql, timeout).await;
            let trino_rows = trino_run.rows;
            let trino_error = trino_run.result.as_ref().err().cloned();
            let trino_time_ms = trino_run.duration.as_millis() as u64;

            let rows_match = sqe_error.is_none() && trino_error.is_none() && sqe_rows == trino_rows;
            let canonical =
                classify::canonical_rows(expected_rows.as_ref(), benchmark, &query.id, scale);
            let status = classify::classify_status(
                &sqe_error,
                &trino_error,
                sqe_rows,
                trino_rows,
                canonical,
            );
            let speedup = if sqe_time_ms > 0 {
                trino_time_ms as f64 / sqe_time_ms as f64
            } else {
                0.0
            };

            comparisons.push(QueryComparison {
                query_name: query.id.clone(),
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
    }

    // Assemble the comparison report only when Trino was queried. Mirrors the
    // summary computation in `comparison::run_comparison`.
    let comparison = if trino.is_some() {
        Some(build_comparison_report(
            benchmark,
            scale,
            sqe_endpoint,
            trino_endpoint.unwrap_or(""),
            comparisons,
        ))
    } else {
        None
    };

    Ok(SuiteOutcome {
        results,
        comparison,
    })
}

/// Roll the per-query comparisons into a `ComparisonReport`, computing the same
/// summary counts and speedup statistics as `comparison::run_comparison`.
fn build_comparison_report(
    benchmark: &str,
    scale: f64,
    sqe_endpoint: &str,
    trino_endpoint: &str,
    comparisons: Vec<QueryComparison>,
) -> ComparisonReport {
    let total = comparisons.len();
    let count =
        |pred: &dyn Fn(&QueryComparison) -> bool| comparisons.iter().filter(|c| pred(c)).count();

    let matched = count(&|c| matches!(c.status, CompareStatusReport::Match));
    let vacuous = count(&|c| matches!(c.status, CompareStatusReport::Vacuous));
    let expected_empty = count(&|c| matches!(c.status, CompareStatusReport::ExpectedEmpty));
    let vacuous_bug = count(&|c| matches!(c.status, CompareStatusReport::VacuousBug));
    let dialect_diff = count(&|c| matches!(c.status, CompareStatusReport::DialectDiff));
    let row_diff = count(&|c| matches!(c.status, CompareStatusReport::RowDiff));
    let sqe_failed = count(&|c| matches!(c.status, CompareStatusReport::SqeFailed));
    let trino_failed = count(&|c| matches!(c.status, CompareStatusReport::TrinoFailed));
    let both_failed = count(&|c| matches!(c.status, CompareStatusReport::BothFailed));

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

    ComparisonReport {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::BenchClient;
    use arrow_array::RecordBatch;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A `BenchClient` that counts how many times `execute` is called and
    /// always returns an empty result set.
    struct CountingStub {
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl BenchClient for CountingStub {
        async fn execute(&self, _sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![])
        }
        async fn execute_update(&self, _sql: &str) -> anyhow::Result<()> {
            Ok(())
        }
        fn protocol_name(&self) -> &str {
            "counting-stub"
        }
    }

    fn qf(id: &str, sql: &str) -> crate::query::QueryFile {
        crate::query::QueryFile {
            id: id.to_string(),
            name: String::new(),
            sql: sql.to_string(),
            requires: Vec::new(),
            timeout_secs: 60,
        }
    }

    /// The core single-pass guarantee: a filtered-in query executes exactly
    /// once against SQE and exactly once against Trino, and the comparison is
    /// present when a Trino client is supplied. Independent of classification
    /// (a filtered query executes regardless of whether expected files exist),
    /// so it is deterministic with CWD at the crate root.
    #[tokio::test]
    async fn single_pass_runs_each_engine_once_for_filtered_query() {
        let sqe = CountingStub {
            calls: AtomicUsize::new(0),
        };
        let trino = CountingStub {
            calls: AtomicUsize::new(0),
        };
        let queries = vec![
            qf("q01", "SELECT 1"),
            qf("q02", "SELECT 2"),
            qf("q03", "SELECT 3"),
        ];

        let outcome = run_suite_with_queries(
            &sqe,
            Some(&trino),
            "tpch",
            1.0,
            Some("q02"),
            queries,
            None,
            None,
            "sqe://test",
            Some("trino://test"),
        )
        .await
        .unwrap();

        assert_eq!(
            sqe.calls.load(Ordering::SeqCst),
            1,
            "SQE must execute exactly once for the filtered query"
        );
        assert_eq!(
            trino.calls.load(Ordering::SeqCst),
            1,
            "Trino must execute exactly once for the filtered query"
        );
        assert!(
            outcome.comparison.is_some(),
            "comparison must be present when a Trino client is supplied"
        );
        assert_eq!(
            outcome.results.len(),
            1,
            "only the filtered-in query belongs in the results"
        );
    }
}
