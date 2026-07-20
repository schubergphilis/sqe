//! Single-query execution primitive: timeout + transport-error retry.
//!
//! Extracted from `test.rs`'s `run_benchmark_test` (timeout `tokio::select!`)
//! and `comparison.rs`'s `run_comparison` (transport retry) so a later task
//! can wire one execution path into the run loop instead of two divergent
//! copies. Not yet wired in -- purely additive.

use crate::client::BenchClient;

/// Result of running a single query once (including any retry).
#[allow(dead_code)]
pub struct QueryRun {
    /// Total rows returned on success; 0 on error or timeout.
    pub rows: usize,
    /// Wall-clock time for the (possibly retried) execution.
    pub duration: std::time::Duration,
    /// `Ok(batches)` on success, `Err(message)` on query error or timeout.
    pub result: Result<Vec<arrow_array::RecordBatch>, String>,
    /// Set when the query was cut off by the timeout, to the timeout used.
    pub timed_out_after: Option<u64>,
}

/// Resolve the effective per-query timeout (seconds), highest priority first:
///   1. `BENCH_QUERY_TIMEOUT_SECS` env var: overrides everything, including
///      `query_timeout_secs` (e.g. the `-- timeout: Ns` header value).
///   2. `query_timeout_secs`, when positive.
///   3. Default 300s.
#[allow(dead_code)]
pub fn resolve_timeout(query_timeout_secs: u64) -> u64 {
    let default_timeout = if query_timeout_secs > 0 {
        query_timeout_secs
    } else {
        300
    };
    std::env::var("BENCH_QUERY_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default_timeout)
}

/// Run `sql` once against `client`, under `timeout_secs`, retrying once on a
/// transport-shaped error (a fresh connection, since tonic channels reconnect
/// lazily). Query-level errors (plan, execution) are not retried. Never
/// panics -- all failure modes are reported via `QueryRun.result`.
#[allow(dead_code)]
pub async fn run_query(
    client: &dyn BenchClient,
    id: &str,
    sql: &str,
    timeout_secs: u64,
) -> QueryRun {
    let start = std::time::Instant::now();

    // Use tokio::select! so the timeout fires even if the gRPC stream is
    // stuck in a non-cancellation-safe recv. The losing branch gets dropped,
    // which closes the connection.
    let outcome = tokio::select! {
        result = async {
            let mut result = client.execute(sql).await;
            if result
                .as_ref()
                .err()
                .is_some_and(|e| crate::comparison::classify::is_transport_error(&e.to_string()))
            {
                eprintln!(
                    "[bench] {id} transport error ({}), retrying once on a fresh connection",
                    result.as_ref().err().map(|e| e.to_string()).unwrap_or_default()
                );
                result = client.execute(sql).await;
            }
            result
        } => Some(result),
        _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => {
            eprintln!("[bench] {id} TIMEOUT after {timeout_secs}s -- skipping");
            None
        }
    };

    match outcome {
        None => QueryRun {
            rows: 0,
            duration: start.elapsed(),
            result: Err(format!("Timed out after {timeout_secs}s")),
            timed_out_after: Some(timeout_secs),
        },
        Some(Err(e)) => QueryRun {
            rows: 0,
            duration: start.elapsed(),
            result: Err(e.to_string()),
            timed_out_after: None,
        },
        Some(Ok(batches)) => {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            QueryRun {
                rows,
                duration: start.elapsed(),
                result: Ok(batches),
                timed_out_after: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::BenchClient;
    use arrow_array::RecordBatch;

    struct Stub {
        delay_ms: u64,
        err: Option<String>,
    }
    #[async_trait::async_trait]
    impl BenchClient for Stub {
        async fn execute(&self, _sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
            tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
            match &self.err {
                Some(e) => anyhow::bail!("{e}"),
                None => Ok(vec![]),
            }
        }
        async fn execute_update(&self, _sql: &str) -> anyhow::Result<()> {
            Ok(())
        }
        fn protocol_name(&self) -> &str {
            "stub"
        }
    }

    #[tokio::test]
    async fn timeout_yields_timed_out_after() {
        let run = run_query(
            &Stub {
                delay_ms: 10_000,
                err: None,
            },
            "q",
            "SELECT 1",
            1,
        )
        .await;
        assert_eq!(run.timed_out_after, Some(1));
        assert!(run.result.is_err());
    }

    #[test]
    fn resolve_timeout_precedence() {
        // header value used when env unset and header > 0
        assert_eq!(resolve_timeout(60), 60);
        // zero header -> default
        assert_eq!(resolve_timeout(0), 300);
    }
}
