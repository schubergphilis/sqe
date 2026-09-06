# 602: Worker errors reach the coordinator as strings, so a user-caused failure evicts healthy workers and retries a task that cannot succeed

- Scope: sqe-worker (flight_service.rs, executor.rs), sqe-coordinator (distributed_scan.rs, worker_registry.rs)
- Severity: high
- Category: reliability
- Effort: M (days)
- Status: open
- Issue: #609

## Problem

The worker collapses every failure into one of two shapes:

- Setup failures become `Status::internal("Scan execution failed: {e}")`
  regardless of cause: `crates/sqe-worker/src/flight_service.rs:1043-1046`.
  That includes S3 `AccessDenied`/`ExpiredToken` from the user's own
  credentials, a missing file, and a worker-side OOM.
- Mid-stream failures become `FlightError::from_external_error(io::Error::other(e.to_string()))`:
  `flight_service.rs:1054-1060`. The `anyhow::Error` chain is flattened to a
  string before it crosses the wire.
- End-of-stream cleanup is signalled by injecting a fake error carrying the
  literal `__SQE_CLEANUP_SENTINEL__` and filtering it back out by substring
  match: `flight_service.rs:1061-1075`. A control-flow signal is encoded in
  the error channel.

The coordinator has no classifier to apply. On any `dispatch_to_worker` error
it does three things unconditionally
(`crates/sqe-coordinator/src/distributed_scan.rs:764-820`):

1. `registry.mark_unhealthy(&current_worker_url)` (`:797`), which sets
   `consecutive_failures = max` and removes the worker from the healthy set
   (`worker_registry.rs:332-343`).
2. Picks the next healthy worker and retries the identical task (`:804-812`).
3. Repeats until `max_retries`, then falls back to local execution (`:832-850`).

The only code-based distinction anywhere is the channel-pool invalidation on
`Unavailable | DeadlineExceeded` (`distributed_scan.rs:1071-1079`), and even
there the returned error is `DataFusionError::Execution(format!(...))`.

Walk one scenario: a user's STS token expired mid-query. Worker 1 gets S3
403, returns `INTERNAL`. Coordinator marks worker 1 unhealthy, retries on
worker 2, which gets the same 403 and is marked unhealthy too. With two
workers the registry is now empty until the next heartbeat interval; every
other user's queries in that window run single-node. The user still gets an
error.

## Why it matters

- A single bad ticket, expired credential, or deleted file removes healthy
  workers from the pool. This is a self-inflicted availability incident that
  scales with `max_retries`.
- Operators cannot tell from the error whether the worker, the object store,
  the catalog or the user's token failed. The `Sourced { code }` taxonomy in
  `sqe-core` exists for the coordinator path but stops at the worker boundary.
- The sentinel hack means any real error whose message contains the sentinel
  text is silently swallowed, and the pattern invites copy-paste.

## Proposed change

1. Define a `WorkerFault` enum in `sqe-planner` (shared by both crates)
   with at least: `TaskInvalid` (bad ticket, version), `PrincipalDenied`
   (S3 403/401, expired token), `SourceMissing` (404), `SourceThrottled`
   (429/503 from S3), `WorkerResourceExhausted` (pool/spill), `WorkerInternal`.
   Serialise it as a `grpc-status-details-bin` payload or a well-known
   trailer (`x-sqe-fault`) on the `Status`; map to gRPC codes as a secondary
   channel (`InvalidArgument`, `PermissionDenied`, `NotFound`, `Unavailable`,
   `ResourceExhausted`, `Internal`).
2. Worker: classify in `executor.rs` where the `object_store::Error` and
   parquet errors are still typed, not after `to_string()`. Replace the
   sentinel with a `StreamEnd` wrapper item or a `futures::stream::once` that
   drops the guard without emitting an error.
3. Coordinator: in `distributed_scan.rs` retry loop, only `mark_unhealthy` and
   retry on `WorkerResourceExhausted`, `WorkerInternal`, transport errors.
   `TaskInvalid`, `PrincipalDenied`, `SourceMissing` fail the query
   immediately without touching the registry. `SourceThrottled` retries on
   the same worker with backoff.
4. Surface the fault kind in the `sqe_worker_fragments_failed_total` counter
   label and in the audit `Outcome::Failure.error_code`.

## Acceptance criteria

- Unit test in `sqe-coordinator`: a `dispatch_to_worker` stub returning
  `PermissionDenied` leaves the registry unchanged and returns an error with
  `SqeErrorCode::AccessDenied`.
- Unit test: `ResourceExhausted` from worker 1 retries on worker 2 and marks
  worker 1 unhealthy.
- `grep -rn "__SQE_CLEANUP_SENTINEL__" crates` returns nothing.
- Integration (`make test-distributed`): revoke the test user's S3 key
  mid-suite and assert `healthy_workers()` count is unchanged afterwards.

## Related

- Memory note "Native two-worker rig: workers must ACCEPT before coordinator
  or queries silently run local" is a symptom of the same silent fallback.
- Issue 609 (registry protocol), 612 (metrics labels for fault kinds).
