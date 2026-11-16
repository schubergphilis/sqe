# Design: admission queue and drain

## Context

`admission.rs` takes, in order: the memory-pressure check, the per-user semaphore, the global `query_semaphore`, then the per-user memory reservation. Every step uses `try_*` and returns `ExecutionFailed` on failure. `QueryTracker::active_count()` already counts running statements and feeds the web UI and the metrics sampler.

## Decisions

### D1. The queue waits for the global permit only, holding nothing

A waiter holds no permit and no memory reservation while it waits; otherwise a queued statement would block the user's other work and the memory it does not use yet. Order on admission:

1. memory-pressure check (refuse: `memory_pressure`),
2. per-user semaphore, `try_acquire` (refuse: `user_limit`). A user at their own limit is not queued: their own statements are what would free the slot, and queueing them hides a client that floods.
3. global permit: `try_acquire`; on failure enter the queue (refuse `queue_full` when `queue_max` waiters exist), wait up to `queue_timeout_secs` (refuse `queue_timeout`). On timeout the per-user permit from step 2 is released by drop.
4. per-user memory reservation (refuse: `memory_budget`).

Rejected: queueing in front of step 2 as well. It doubles the waiting surface for no gain the issue asks for.

### D2. Fairness: round robin over per-user lanes

A `std::sync::Mutex<Queue>` holds a `VecDeque<UserLane>`; each lane holds that user's waiters in FIFO order as `oneshot::Sender<OwnedSemaphorePermit>`. When a statement finishes, its permit is not returned to the semaphore while waiters exist: the dropper hands it to the head of the next lane and rotates the lane to the back. A cancelled waiter (client gone, timeout) is detected when the send fails, and the permit goes to the next one. One user with 50 queued statements and another with one: the second runs after at most one of the first's.

The handoff wraps the permit in a guard whose `Drop` performs it, so every exit path (success, error, panic, stream dropped mid-flight) feeds the queue.

### D3. A reason on the error, not a new variant per reason

`SqeError` gains an optional `refusal: Option<RefusalReason>` on the typed path (`with_code`), set by `SqeError::refused(reason, message)`, which also fixes the code to `ResourceExhausted`. `RefusalReason` is a closed enum with `as_str()` giving the wire names (`queue_full`, `queue_timeout`, `user_limit`, `memory_budget`, `memory_pressure`, `rate_limited`, `draining`). DataFusion memory errors stay `ResourceExhausted` without a reason: they are a runtime failure, not a refusal, and a client must not retry them blindly.

Wire mapping:

| Protocol | Code | Reason |
|---|---|---|
| Flight SQL | `Status::resource_exhausted` | metadata `x-sqe-refusal-reason` |
| Trino HTTP | `errorType = INSUFFICIENT_RESOURCES` | `errorName = SQE_<REASON>` (Trino's own names such as `QUERY_QUEUE_FULL` are not reused: they do not cover every reason) |
| HTTP (rate limiter, statement API) | 429 | JSON `{"reason": "..."}`, `Retry-After` for `queue_full`, `rate_limited`, `draining` |

### D4. Drain waits on the tracker

`shutdown_with_drain` becomes: flip readiness; set `draining` in the handler so admission refuses with reason `draining`; poll `active_count()` every 250 ms until 0 or the deadline; then the existing `before_close` path. A queued waiter is refused with `draining` at once, not left to time out. `effective_drain_secs` keeps forcing 0 on a terminal.

Worker: on SIGTERM it flips its own readiness, refuses new `DoGet` fragments with `Unavailable`, and waits for its in-flight fragment count to reach 0 or the deadline. The coordinator already retries a fragment on another worker on `Unavailable`.

### D5. Idle gauge from a timestamp, not the active count

The handler keeps `last_finished: AtomicI64` (unix ms), stored when the tracker removes a statement. The sampler sets `sqe_idle_seconds = if active_count() > 0 { 0 } else { now - last_finished }`. Before the first statement, `last_finished` is process start. The first task of the change verifies `sqe_active_queries` and `active_count()` agree for streamed statements (a stream that ends by drop must still be removed).

## Risks

- A lost handoff (a permit dropped without feeding the queue) stalls waiters until they time out. Mitigation: the guard type in D2 is the only way to hold a global permit; unit tests for each exit path.
- Clients that treat any `ResourceExhausted` as fatal now see one for busy refusals that used to be `ExecutionFailed`. Release note; the reason field lets them tell.
