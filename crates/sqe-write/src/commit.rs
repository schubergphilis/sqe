//! Optimistic-concurrency commit helpers shared by every Iceberg writer in
//! this crate: the write handler, the Puffin statistics carry, the
//! maintenance ledger and the maintenance lease.
//!
//! Moved out of `write_handler.rs` so the maintenance cluster stops reaching
//! into the write handler for two free functions
//! (`docs/internal/plans/2026-09-07-coordinator-crate-split.md`, MR A).

use iceberg::table::Table as IcebergTable;
use iceberg::{Catalog, TableIdent};
use tracing::{info, warn};
use uuid::Uuid;

/// Backoff (exponential + jitter, capped ~1s) before retrying commit
/// `attempt` (1-based) after an optimistic-concurrency conflict.
pub(crate) fn conflict_backoff_ms(attempt: u32) -> u64 {
    let base = 50_u64.saturating_mul(1_u64 << attempt.saturating_sub(1));
    let jitter = (Uuid::new_v4().as_u128() as u64) % 50;
    base.saturating_add(jitter).min(1000)
}

/// Bounded retry around an Iceberg commit that may lose an optimistic-concurrency
/// race against another writer.
///
/// iceberg-rust's vendored catalogs (SQL, in-memory, our REST bridge) tag conflict
/// errors with `retryable = true`. Polaris and HMS surface the same condition as
/// `CatalogCommitConflicts`. The fix for #47 wraps every commit site that builds
/// a Transaction so a conflict reloads the table and rebuilds the action against
/// the new base snapshot. Without the reload the next attempt commits over the
/// same stale metadata-location and the conflict repeats.
///
/// `build_and_commit` runs once per attempt; it receives a freshly-loaded
/// IcebergTable and is expected to build a `Transaction`, apply its action, and
/// call `tx.commit(catalog).await`. The closure clones data files / delete files
/// it captures so each attempt operates on its own action graph.
///
/// Backoff: exponential with jitter, capped at ~1s base. After `max_attempts`
/// the last error propagates unchanged so the caller's error-mapping still runs.
/// `pub` so the coordinator's `maintenance_log::append_row` can reuse it too: the
/// ledger table is also the multi-coordinator lease table
/// (`MaintenanceSchedulerConfig::lease = Catalog`), so append commits need
/// the same retry-on-conflict behavior as any other writer, not a
/// single-shot commit.
#[tracing::instrument(skip(catalog, build_and_commit), fields(table = %table_ident, op = %op), name = "sqe.write_commit")]
pub async fn commit_with_retry<F, Fut>(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    op: &str,
    mut build_and_commit: F,
) -> std::result::Result<IcebergTable, iceberg::Error>
where
    F: FnMut(IcebergTable) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<IcebergTable, iceberg::Error>>,
{
    const MAX_ATTEMPTS: u32 = 4;
    let mut last_err: Option<iceberg::Error> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        let table = retry_transient_load(table_ident, op, || catalog.load_table(table_ident)).await?;
        match build_and_commit(table).await {
            Ok(t) => {
                if attempt > 1 {
                    info!(
                        table = %table_ident,
                        op,
                        attempts = attempt,
                        "commit succeeded after retry"
                    );
                }
                return Ok(t);
            }
            Err(e) => {
                let retryable = e.retryable() || is_conflict_message(&e.to_string());
                if !retryable || attempt == MAX_ATTEMPTS {
                    last_err = Some(e);
                    break;
                }
                let sleep_ms = conflict_backoff_ms(attempt);
                warn!(
                    table = %table_ident,
                    op,
                    attempt,
                    backoff_ms = sleep_ms,
                    error = %e,
                    "commit conflict; reloading table and retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "commit_with_retry exhausted attempts without capturing an error",
        )
    }))
}

/// Load attempts for the table reload that opens each commit attempt.
const LOAD_ATTEMPTS: u32 = 3;

/// Reload the table for a commit attempt, retrying a transient failure
/// ([`is_transient_load_error`]) a few times before giving up.
///
/// The reload used to sit outside every retry, so one failed request
/// discarded the whole write: a 176 s, 133M-row CTAS failed at commit on a
/// single catalog 401 that the bench loader's identical retry then got past
/// (sqe#803). Retrying uses the same credentials; a persistent failure still
/// propagates, after well under a second.
async fn retry_transient_load<T, F, Fut>(
    table_ident: &TableIdent,
    op: &str,
    mut load: F,
) -> std::result::Result<T, iceberg::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, iceberg::Error>>,
{
    let mut attempt = 1;
    loop {
        match load().await {
            Err(e) if attempt < LOAD_ATTEMPTS && is_transient_load_error(&e) => {
                let sleep_ms = conflict_backoff_ms(attempt + 1);
                warn!(
                    table = %table_ident,
                    op,
                    attempt,
                    backoff_ms = sleep_ms,
                    error = %e,
                    "table reload for commit failed; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                attempt += 1;
            }
            result => return result,
        }
    }
}

/// A catalog failure worth one more try: tagged retryable, a 401 (seen once
/// mid-write from a token that loaded the same table minutes earlier,
/// sqe#803), or a 5xx.
pub(crate) fn is_transient_load_error(e: &iceberg::Error) -> bool {
    if e.retryable() {
        return true;
    }
    let msg = e.to_string().to_lowercase();
    msg.contains("status: 401") || ["500", "502", "503", "504"].iter().any(|code| msg.contains(&format!("status: {code}")))
}

/// `pub` so the coordinator's `maintenance_lease.rs` can classify a losing claim commit
/// with the exact same transient-conflict heuristic `commit_with_retry` uses
/// (a real conflict, e.g. the SQL catalog's optimistic-concurrency CAS loss,
/// warrants a bounded retry; the `ErrorKind::DataInvalid` from a lost
/// `check_file_existence` lease race does not match this heuristic and must
/// be classified separately -- see `lease_cas_spike_test.rs` / Task 1).
pub fn is_conflict_message(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("commitconflict")
        || lower.contains("commit conflict")
        || lower.contains("stale snapshot")
        || lower.contains("rowdelta conflict")
        || lower.contains("retryable")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident() -> TableIdent {
        TableIdent::from_strs(["ns", "t"]).unwrap()
    }

    fn http_error(status: &str) -> iceberg::Error {
        iceberg::Error::new(iceberg::ErrorKind::Unexpected, "Failed to load table")
            .with_context("status", status)
    }

    #[test]
    fn transient_load_errors_are_401_5xx_and_retryable() {
        assert!(is_transient_load_error(&http_error("401 Unauthorized")));
        assert!(is_transient_load_error(&http_error("503 Service Unavailable")));
        assert!(is_transient_load_error(
            &iceberg::Error::new(iceberg::ErrorKind::Unexpected, "x").with_retryable(true)
        ));
        assert!(!is_transient_load_error(&http_error("404 Not Found")));
        assert!(!is_transient_load_error(&http_error("403 Forbidden")));
    }

    #[tokio::test]
    async fn reload_recovers_from_a_transient_401() {
        let mut calls = 0;
        let got = retry_transient_load(&ident(), "test", || {
            calls += 1;
            let n = calls;
            async move { if n < 3 { Err(http_error("401 Unauthorized")) } else { Ok(n) } }
        })
        .await;
        assert_eq!(got.unwrap(), 3);
    }

    #[tokio::test]
    async fn reload_gives_up_after_the_attempt_budget() {
        let mut calls = 0;
        let got: std::result::Result<(), _> = retry_transient_load(&ident(), "test", || {
            calls += 1;
            async { Err(http_error("401 Unauthorized")) }
        })
        .await;
        assert!(got.is_err());
        assert_eq!(calls, LOAD_ATTEMPTS);
    }

    #[tokio::test]
    async fn reload_does_not_retry_a_missing_table() {
        let mut calls = 0;
        let got: std::result::Result<(), _> = retry_transient_load(&ident(), "test", || {
            calls += 1;
            async { Err(http_error("404 Not Found")) }
        })
        .await;
        assert!(got.is_err());
        assert_eq!(calls, 1);
    }
}
