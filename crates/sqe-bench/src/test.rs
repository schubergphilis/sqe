use crate::client::BenchClient;
use crate::query::{load_query_files, normalize_query_id, prefix_tables};
use crate::report::QueryResult;
use crate::status::{classify_vs_expected, QueryOutcome};

// ---------------------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------------------

/// Run all (or a filtered subset of) benchmark queries and collect results.
pub async fn run_benchmark_test(
    client: &dyn BenchClient,
    benchmark: &str,
    scale: f64,
    query_filter: Option<&str>,
    catalog: Option<&str>,
    namespace_override: Option<&str>,
) -> anyhow::Result<Vec<QueryResult>> {
    let ns_base = match namespace_override {
        Some(ns) => ns.to_string(),
        None if benchmark == "tpcbb" => crate::bench_namespace("tpcds", scale),
        None => crate::bench_namespace(benchmark, scale),
    };
    let namespace = match catalog {
        Some(cat) => format!("{cat}.{ns_base}"),
        None => ns_base,
    };
    let queries = load_query_files(benchmark)?;
    let mut results = Vec::new();

    for query in &queries {
        // Skip if filter provided and this query doesn't match
        if let Some(filter) = query_filter {
            // Accept both "q01" and "1" style filters
            let normalized_filter = normalize_query_id(filter);
            let normalized_id = normalize_query_id(&query.id);
            if normalized_id != normalized_filter {
                continue;
            }
        }

        // Skip if requires unsupported features
        if !query.requires.is_empty() {
            results.push(QueryResult {
                id: query.id.clone(),
                outcome: QueryOutcome::Skip(format!("requires: {}", query.requires.join(", "))),
                duration: std::time::Duration::ZERO,
                rows: 0,
            });
            continue;
        }

        // Qualify unqualified table names with the benchmark namespace
        let sql = prefix_tables(&query.sql, &namespace, benchmark);

        eprintln!("[bench] Running {} ({} chars)...", query.id, sql.len());
        if std::env::var("BENCH_DEBUG").is_ok() {
            eprintln!("[bench] SQL:\n{sql}\n---");
        }

        // Per-query timeout resolution (highest priority first):
        //   1. `BENCH_QUERY_TIMEOUT_SECS` env var: overrides EVERYTHING,
        //      including the `-- timeout: Ns` header in the .sql file.
        //      Use this for SF100+ runs where the committed headers
        //      (typically 60-120s, sized for SF1) are too tight.
        //   2. `-- timeout: Ns` header in the .sql file, when positive.
        //      A `-- timeout: 0` (or a missing header) falls through to
        //      the default rather than disabling the timeout entirely,
        //      so a typo in one file does not let a runaway query stall
        //      the suite.
        //   3. Default 300s (from `parse_query_file`).
        //
        // Previously a 120s floor was applied on top of the header value,
        // which silently bumped all the SF1 sweeps' `-- timeout: 60s` and
        // `-- timeout: 30s` headers up to 120s. tpcds q72 ran for 100s
        // during the perf regression in #131 without ever tripping its
        // own declared 60s ceiling because of that floor. Honouring the
        // header value as-written restores the intent.
        let default_timeout = if query.timeout_secs > 0 {
            query.timeout_secs
        } else {
            300
        };
        let timeout_secs = std::env::var("BENCH_QUERY_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(default_timeout);
        let start = std::time::Instant::now();

        // Use tokio::select! so the timeout fires even if the gRPC stream
        // is stuck in a non-cancellation-safe recv. The losing branch gets
        // dropped, which closes the connection.
        let execute_result = tokio::select! {
            result = client.execute(&sql) => {
                Some(result)
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => {
                eprintln!("[bench] {} TIMEOUT after {}s — skipping", query.id, timeout_secs);
                None
            }
        };

        match execute_result {
            None => {
                results.push(QueryResult {
                    id: query.id.clone(),
                    outcome: QueryOutcome::Timeout(timeout_secs),
                    duration: start.elapsed(),
                    rows: 0,
                });
                continue;
            }
            Some(Err(e)) => {
                results.push(QueryResult {
                    id: query.id.clone(),
                    outcome: QueryOutcome::Error(e.to_string()),
                    duration: start.elapsed(),
                    rows: 0,
                });
                continue;
            }
            Some(Ok(batches)) => {
                let duration = start.elapsed();
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

                // BENCH_DEBUG=1 prints the batch contents for ad-hoc
                // tracing (e.g. running EXPLAIN ANALYZE through the
                // bench harness to capture the new phase rows).
                if std::env::var("BENCH_DEBUG").is_ok() && rows < 200 {
                    eprintln!("[bench] {} result ({} rows):", query.id, rows);
                    for batch in &batches {
                        match arrow::util::pretty::pretty_format_batches(std::slice::from_ref(
                            batch,
                        )) {
                            Ok(s) => eprintln!("{}", s),
                            Err(e) => eprintln!("[bench] pretty-format error: {e}"),
                        }
                    }
                }

                let outcome = classify_vs_expected(benchmark, scale, &query.id, &batches);

                results.push(QueryResult {
                    id: query.id.clone(),
                    outcome,
                    duration,
                    rows,
                });
            }
        }
    }

    Ok(results)
}

/// Run a suite against the golden catalog and emit the standard artifacts
/// (per-query summary + JSON report). Returns the results for the caller's
/// pass/fail tally.
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
