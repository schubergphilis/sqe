# 811: `std::sync::Mutex::lock().unwrap()` in async request handlers, held across awaits

- Scope: sqe-trino-compat (`server.rs`), sqe-metrics (audit logger), sqe-mcp, sqe-compaction
- Severity: low
- Category: reliability
- Effort: S
- Status: open

## Problem

Non-test `.lock().unwrap()` / `.read().unwrap()` / `.write().unwrap()` call
counts by crate: sqe-trino-compat 29, sqe-metrics 25, sqe-mcp 9,
sqe-catalog 9, sqe-compaction 8, sqe-coordinator 7.

In the Trino HTTP server the per-query `handle.status` and `handle.abort`
are `std::sync::Mutex` fields read and written from async handlers
(`crates/sqe-trino-compat/src/server.rs:219,221,858,1321,1333,1353,1364,1370,1376,1393,1625,1745,1748`).
At least one guard is alive across an `.await`:

`crates/sqe-trino-compat/src/server.rs:1370-1374`:

```rust
*handle.abort.lock().unwrap() = Some(task.abort_handle());
...
.await
```

(The assignment statement drops the temporary guard at the semicolon, so
this one is safe, but the same pattern at `:2369-2373` matches
`*handle.status.lock().unwrap()` inside a `matches!` argument list that
continues to an `.await`; a reviewer has to reason about temporary lifetimes
for each site.)

Every `unwrap()` on a poisoned lock is a panic in a request handler. A
panic in any task that holds one of these locks (for example a panic inside
`QueryStatus::Failed(Box::new(trino_error))` construction at `:1364`)
poisons the mutex, and the next poll of `/v1/statement/{id}` panics at
`:1393` or `:1625`. tonic/axum catch the panic and return 500, so the effect
is that one bad query makes its own status endpoint permanently 500 until
the handle is evicted.

The June review's item 12 (`catalog_ops.rs` counter lock) is fixed; the
pattern moved rather than disappeared.

## Why it matters

- Silent turn of a single failure into a poisoned handle that returns 500s
  to a Trino client that is polling for a result; the client sees a
  protocol error instead of the real failure.
- `std::sync::Mutex` held across `.await` blocks a runtime worker and, if
  the same lock is then taken on another task on the same worker, deadlocks.
  The code has to stay carefully written for this not to happen, and there
  is no lint enabled for it.

## Proposed change

1. In `sqe-trino-compat/src/server.rs`, make `QueryHandle.status` and
   `.abort` `tokio::sync::Mutex` (or `parking_lot::Mutex`, which does not
   poison and returns no `Result`) and remove the `unwrap()`s.
2. Enable `clippy::await_holding_lock` at the workspace level
   (`[workspace.lints.clippy] await_holding_lock = "deny"`) so the compiler
   finds the remaining cross-await holds.
3. For the audit logger's `spool_sink_worker.lock().unwrap()` chain
   (`crates/sqe-metrics/src/audit/logger.rs:364-510`, 14 sites on one worker
   thread), replace with `parking_lot::Mutex` or `lock().unwrap_or_else(|p| p.into_inner())`
   so a panicked sink does not stop audit shipping.

## Acceptance criteria

- `cargo clippy --all-targets -- -D clippy::await_holding_lock` passes.
- `grep -c '.lock().unwrap()' crates/sqe-trino-compat/src/server.rs` is 0.
- Test: a Trino query whose executor panics returns a `QUERY_FAILED`-style
  error on the next status poll, not HTTP 500.

## Related

- June 2026 codebase analysis, High item 12 (fixed at that site; pattern recurs).
- 700-range Trino compat issues.
