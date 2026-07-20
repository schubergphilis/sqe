use crate::client::BenchClient;
use crate::report::QueryResult;

// ---------------------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------------------

/// Run all (or a filtered subset of) benchmark queries and collect results.
///
/// Thin wrapper over the single-pass `suite::run_suite` driver with no Trino
/// client, so each query runs exactly once against SQE. The `Test` verb calls
/// this directly (and prints the summary + writes the report itself); the `run`
/// verb inlines the same single-pass path so it can share one `run_suite` call
/// with its compare arm.
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
