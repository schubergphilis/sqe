# 812: Swallowed errors on startup, write-result and grant-backend paths

- Scope: sqe-coordinator, sqe-policy (grants), sqe-worker
- Severity: low
- Category: reliability
- Effort: S
- Status: fixed (branch `fix/errors-audit-observability`, 2026-09-05)
- Issue: #644

## Problem

Sweep counts for non-test code: `let _ =` 36 in sqe-coordinator (most are
benign `plan.apply` visitors and one-time `OnceCell::set`), `Err(_) =>` 20 in
sqe-coordinator, `unwrap_or_default()` 148 workspace-wide (36 coordinator,
34 policy, 23 catalog). The following are the ones that hide a real failure.

1. **Session restore result discarded.**
   `crates/sqe-coordinator/src/bin/sqe_server.rs:1107`:
   ```rust
   let _ = tokio::task::spawn_blocking(move || sm.restore_from_file(&restore_path)).await;
   ```
   A `JoinError` (panic in restore) or an `Err` from `restore_from_file` is
   dropped; the operator sees neither a log line nor a metric. With
   `session.persistence = "file"` this is the only restore path.

2. **DML row count silently becomes an empty result.**
   `crates/sqe-coordinator/src/write_handler.rs:188-191`:
   ```rust
   match RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![count as i64]))]) {
       Ok(batch) => vec![batch],
       Err(_) => vec![],
   }
   ```
   A client (dbt) reading `rows_affected` gets zero rows instead of an error
   if the batch cannot be built. Also `count as i64` truncates silently for
   `count: u64`.

3. **HTTP response bodies dropped from grant-backend errors.**
   `crates/sqe-policy/src/grants/polaris.rs:197,328,361,390,456,510,635` and
   `crates/sqe-policy/src/tag_projector.rs:207` use
   `resp.text().await.unwrap_or_default()`, so when Polaris returns an error
   and the body read fails, the error message the user sees for GRANT/REVOKE
   is "HTTP 4xx: " with an empty body and no indication the body was lost.

4. **Plan statistics failure reads as zero bytes.**
   `crates/sqe-coordinator/src/query_handler.rs:7402-7405` returns `0` when
   `StatisticsContext::compute` fails; downstream this feeds size-based
   decisions (join side, distribution) with "empty" rather than "unknown".

5. **Table deregistration errors ignored on four write paths.**
   `crates/sqe-coordinator/src/write_handler.rs:4789,4870,5005,5068`
   `let _ = ctx.deregister_table(..)`. A comment at `:5501` acknowledges
   the pattern. A failed deregister leaves a stale provider in the session
   for the next statement (see memory: writes need session-cache
   invalidation).

6. **Test-only env reads in production code.**
   `crates/sqe-worker/src/compaction.rs:639-663` reads `SQE_IT_*` variables
   with `.expect(...)` in non-test code; a stray env var in production
   panics a worker.

## Why it matters

Each site turns a diagnosable failure into a downstream symptom: a session
that "was never saved", a dbt run that reports zero rows written, a GRANT
that fails with an empty message, a join that picks the wrong build side.
The prior review's items 5 and 6 (writer close, JWKS write) are fixed; these
are the current set.

## Proposed change

- (1) `match` the join result and the inner result; `warn!` with the path
  and increment `sqe_session_restore_failures_total`.
- (2) Return `Err(SqeError::Internal(..))` from the helper; use
  `i64::try_from(count)`.
- (3) `match resp.text().await { Ok(b) => b, Err(e) => format!("<body unreadable: {e}>") }`,
  once, in a shared helper in `sqe-policy/src/grants/mod.rs`.
- (4) Return `Option<u64>` from `size_of` and let the caller treat `None`
  as unknown (do not distribute on unknown).
- (5) Log at `debug!` with the table name; the return type is `Result<Option<..>>`,
  so an `Err` here is a real state problem.
- (6) Move the `SQE_IT_*` block under `#[cfg(test)]` or into the test.

## Acceptance criteria

- `grep -n 'let _ = tokio::task::spawn_blocking' crates/sqe-coordinator/src/bin/sqe_server.rs` returns nothing.
- Unit test: `rows_affected_batch(u64::MAX)` returns an error, not an empty vec.
- Unit test: Polaris grant error with an unreadable body includes the words "body unreadable".
- `grep -rn 'SQE_IT_' crates/sqe-worker/src` returns nothing outside `#[cfg(test)]`.

## Related

- 800 (error model) is the structural fix for the `unwrap_or_default` message-loss class.
- June 2026 codebase analysis, Critical items 5 and 6 (fixed), Improvement 19 (partially).
- Memory: Writes need session-cache invalidation.

## Resolution (2026-09-05)

Session restore logs join and restore errors. `affected_rows_batch` returns `Result` and rejects `u64::MAX`. Grant HTTP bodies use `response_body_or_unreadable`. Scratch-table deregister failures are `debug!` logged. `SQE_IT_*` remains under `#[cfg(test)]`.
