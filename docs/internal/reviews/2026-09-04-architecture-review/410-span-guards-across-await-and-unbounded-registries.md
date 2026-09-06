# 410: Query spans are entered across `.await`, and three coordinator registries only grow

- Scope: sqe-coordinator, read side (query_handler.rs, worker_registry.rs, memory.rs, query_handler.rs per-user maps)
- Severity: low
- Category: operability
- Effort: S (hours)
- Status: open
- Issue: #590

## Problem

**Span guard held across await points.** Both statement entry points create the `sqe.query`
span and hold its `Entered` guard for the rest of an `async fn` that awaits many times:

- `crates/sqe-coordinator/src/query_handler.rs:1042` `let _query_guard = query_span.enter();`
  inside `pub async fn execute` (`:870`), followed by dozens of `.await`s up to `:2807`.
- `crates/sqe-coordinator/src/query_handler.rs:2954` the same in `execute_stream` (`:2809`).

`tracing` documents that `Span::enter` guards must not be held across `.await`: when the
executor parks the future, the span stays "entered" on that OS thread, so unrelated tasks
polled next on the same worker thread record their events under this query's span, and when
this future resumes on another thread its own events are not under the span. The result is
mis-parented traces and, with OTel export, child spans (worker `dispatch_to_worker`, Polaris
calls) attached to the wrong query. `open_stream` (`:3080`) uses `#[tracing::instrument]`
correctly, so the pattern is known in the file.

**Worker registry never forgets.** `register_heartbeat` inserts a `WorkerState` per advertised
URL (`crates/sqe-coordinator/src/worker_registry.rs:308-315`); `mark_failed` and
`mark_unhealthy` only flip `healthy = false` (`:332-372`). Nothing removes an entry. Each
Kubernetes pod restart yields a new pod IP, so a cluster of 4 workers accumulates one dead URL
per restart until `max_workers` (1024, `:113`) is hit, after which real workers are refused
(`:296-306`). Meanwhile `check_all_workers` (`:386-414`) health-checks every dead URL every
5 s, each costing a 3 s timeout (`:452`) and a `channel_pool.invalidate`.

**Per-user maps grow per distinct username.** `per_user_semaphores`
(`query_handler.rs:150`) and `PerUserMemoryRegistry::used`
(`crates/sqe-coordinator/src/memory.rs:190-200`) are `DashMap<String, _>` with insert-only
paths (`query_handler.rs:912-919`, `:2848-2855`). Under OIDC with per-employee identities this
is bounded by the workforce; under service accounts with per-job usernames it is not.

## Why it matters

- Trace-based debugging (the Victoria observability work in memory) is undermined when
  query spans contain other queries' events; it is also the kind of bug that only shows at
  concurrency, so it survives single-user testing.
- After enough restarts a cluster silently refuses new workers with `resource_exhausted`
  and spends its health-check loop on ghosts.

## Proposed change

1. Replace the two `enter()` guards with `.instrument(query_span.clone())` on the execution
   future (the batch path already has an `execution_future` value at `:1240`) and pass the
   span into `StreamFinalizer` as today. Add `#![deny(clippy::await_holding_lock)]`-style
   protection by enabling the `tracing` lint `clippy::await_holding_refcell_ref` companion:
   there is no built-in lint for span guards, so add a `grep`-based check to the CI lint
   script for `= .*span.enter();` inside `async fn` bodies.
2. Add `last_seen: Instant` to `WorkerState`; drop entries unhealthy for longer than
   `coordinator.worker_forget_after_secs` (default 10 minutes) in `check_all_workers`. Keep
   seed URLs from config permanently.
3. Remove per-user semaphore and memory entries when their count returns to zero
   (`DashMap::remove_if`).

## Acceptance criteria

- `grep -n 'span.enter()' crates/sqe-coordinator/src/query_handler.rs` returns nothing
  inside async functions.
- Unit test: a registry with a URL marked failed for longer than the forget window no longer
  appears in `total_workers()`; a config seed URL does.
- Unit test: after a user's last query completes, `per_user_semaphores.len()` is 0.

## Related

- Memory note "Victoria observability branch" (W3C trace correlation).
- 403 (cancel and tracker) touches the same span plumbing.
