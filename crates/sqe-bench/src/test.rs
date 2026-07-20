use crate::client::BenchClient;
use crate::report::QueryResult;

// ---------------------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------------------

/// Run all (or a filtered subset of) benchmark queries and collect results.
///
/// Thin wrapper over the single-pass `suite::run_suite` driver with no Trino
/// client, so each query runs exactly once against SQE. The `Test` verb calls
/// this directly; `run_and_report` layers the artifact emission on top.
pub async fn run_benchmark_test(
    client: &dyn BenchClient,
    benchmark: &str,
    scale: f64,
    query_filter: Option<&str>,
    catalog: Option<&str>,
    namespace_override: Option<&str>,
) -> anyhow::Result<Vec<QueryResult>> {
    let out = crate::suite::run_suite(
        client,
        None,
        benchmark,
        scale,
        query_filter,
        catalog,
        namespace_override,
        "",
        None,
    )
    .await?;
    Ok(out.results)
}

/// Run a suite against the golden catalog and emit the standard artifacts
/// (per-query summary + JSON report). Returns the results for the caller's
/// pass/fail tally.
///
/// Retained as the SQE-only artifact helper. The `run` verb inlines this same
/// print + write sequence so it can share its single `run_suite` call with the
/// compare arm (a separate helper here would double-execute SQE), which leaves
/// this uncalled for now.
#[allow(dead_code)]
pub async fn run_and_report(
    client: &dyn BenchClient,
    benchmark: &str,
    scale: f64,
    query_filter: Option<&str>,
) -> anyhow::Result<Vec<QueryResult>> {
    let results =
        run_benchmark_test(client, benchmark, scale, query_filter, Some("golden"), None).await?;
    crate::report::print_summary(benchmark, scale, "flight", &results);
    let path = crate::report::write_json_report(benchmark, scale, "flight", &results)?;
    println!("Report written to: {path}");
    Ok(results)
}
