# 402: Distribution failures fall back to local execution silently, and mid-stream worker loss is not retried

- Scope: sqe-coordinator, read side (query_handler.rs try_distribute, distributed_scan.rs)
- Severity: high
- Category: reliability
- Effort: M (days)
- Status: open
- Issue: #582

## Problem

**1. `try_distribute` cannot fail.** Its signature returns the plan, not a `Result`
(`crates/sqe-coordinator/src/query_handler.rs:3720-3725`). Eleven early exits return the
undistributed plan (`:3739, :3756, :3766, :3775, :3782, :3801, :3824, :3848, :4058, :4093,
:4257`). Only some of them are policy decisions (thresholds). The rest are failures that
now run a scan meant for N workers on the coordinator:

- `:3768-3776` `data_file_info()` errored: `warn!` then `return plan`.
- `:4088-4094` the scheduler returned `NoHealthyWorkers` (a race with the health checker):
  `warn!` then `return plan`.
- `:3745-3752` `find_iceberg_scan` returned a node that does not downcast: `warn!` then
  `return plan`.
- `crates/sqe-coordinator/src/query_handler.rs:7453-7459` `replace_scan_in_plan` swallows a
  `replace_children` error with `unwrap_or_else(|_| Arc::clone(plan))` and no log. The
  freshly built `DistributedScanExec` is dropped and the local scan executes.

Only two of these exits increment `scheduler_decisions{local}`; the failure exits do not,
so the metric under-reports.

**2. Retry covers dispatch only.** In `DistributedScanExec::execute`
(`crates/sqe-coordinator/src/distributed_scan.rs:715-830`) the `for attempt in 0..=max_retries`
loop retries `dispatch_to_worker` (connect + `do_get` headers). Once a `FlightRecordBatchStream`
is returned, the fragment is wrapped in `TerminateOnErrorStream`
(`distributed_scan.rs:1220-1244`), which logs the error and ends the stream. Nothing marks the
worker unhealthy, nothing re-dispatches the morsel, and the whole query fails with
`Mid-stream worker failure`. A worker pod evicted at minute 4 of a 5 minute scan kills every
query that had a fragment on it. `mark_unhealthy` is only called on dispatch failure
(`distributed_scan.rs:796-798`).

**3. Local fallback is unwired but its path is live.** `with_local_executor` has no production
caller (`grep -rn with_local_executor crates/sqe-coordinator/src/{main.rs,bin,query_handler.rs}`
is empty), so in production the "all workers failed" branch (`distributed_scan.rs:832-880`)
returns the error. That is the right behaviour; the dead code and the tests that exercise it
(`test_local_fallback_*`) suggest a contract that does not exist.

## Why it matters

- A cluster where Polaris is slow or a worker flaps degrades to "coordinator does all the
  work" with only a `warn!` line. The memory note "Native two-worker rig" records exactly this
  symptom: queries silently ran local and the cluster looked idle. Memory note
  "JoinStrategyRule SMJ regression" records the same pattern (an `Err` arm hiding a regression
  for weeks).
- Worker loss mid-stream is the normal Kubernetes failure (spot reclaim, OOM kill, rolling
  upgrade). Read fragments are idempotent, so the retry is safe and cheap; not doing it means
  every rolling upgrade fails in-flight distributed queries.

## Proposed change

1. Split `try_distribute` into a decision (`DistributionDecision::{Distribute, Local(reason)}`)
   and an action that returns `Result<Arc<dyn ExecutionPlan>>`. Policy skips stay `Local`;
   infrastructure errors become `Err`, and the caller decides via a config flag
   `query.distribution_fallback = "error" | "local"` (default `error` when
   `coordinator.worker_urls` or `worker_secret` is set). Record every `Local(reason)` in the
   `scheduler_decisions` counter with the reason as a label and surface it in `EXPLAIN` the way
   `HiveLocalScanExec` already does for Hive (`query_handler.rs:4211-4217`).
2. Make `replace_scan_in_plan` return `Result` and propagate.
3. Move the retry loop from "resolve a stream" to "drive a stream": on a mid-stream error
   before any batch was yielded downstream, re-dispatch to another healthy worker (the morsel
   is idempotent); after batches were yielded, fail the query but still call
   `registry.mark_unhealthy` and fire the fragment callback with `success=false`. Because
   `TerminateOnErrorStream` already guarantees a clean prefix, the "no batch yet" case can be
   detected with the existing `output_rows` counter in `CallbackStream`.
4. Delete `LocalExecutor` and `with_local_executor` or wire them deliberately.

## Acceptance criteria

- Unit test: `replace_scan_in_plan` with a parent whose `with_new_children` errors returns
  `Err`, not the original plan.
- Unit test with the mock worker used in `test_retry_marks_worker_unhealthy`: a stream that
  yields `Err` after zero batches is retried on a second worker and succeeds; a stream that
  yields `Err` after one batch fails the query and marks the worker unhealthy.
- `make test-distributed` gains a case that kills one worker during a multi-fragment scan and
  asserts the query fails fast with a worker-named error (or, with the retry, succeeds).
- `scheduler_decisions` has a `reason` label and the sum of `local` + `distributed` equals the
  number of scans that reached `try_distribute`.

## Related

- 2026-07-25 review P0-3 (multi-node behaviour not measured) still holds.
- 408 (scan-offload architecture), 411 (test gaps).
