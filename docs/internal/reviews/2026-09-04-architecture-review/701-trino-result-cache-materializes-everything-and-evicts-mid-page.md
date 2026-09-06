# 701: Trino result path materializes the full result as JSON and serves pages from a shared, evictable cache

- Scope: sqe-trino-compat (server.rs, protocol.rs)
- Severity: high
- Category: reliability
- Effort: L (week+)
- Status: open
- Issue: #621

## Problem

The executor contract collects every batch before the first byte is sent, `crates/sqe-trino-compat/src/server.rs:270-281`:

```rust
async fn execute(&self, session: &Session, sql: &str)
    -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError>;
```

The batches are then converted row by row into `serde_json::Value` and split into pages, `server.rs:997-1040` (`build_paginated_result`) and `crates/sqe-trino-compat/src/protocol.rs:314-353` (`batches_to_trino`). Both the Arrow batches and the JSON tree are alive at the same time, and neither is charged to DataFusion's memory pool. The conversion, `estimate_paginated_bytes` (`server.rs:133-146`, walks every JSON value) and `paginate_rows` run on the tokio worker inside the spawned task with no `spawn_blocking`.

Pages are then served from a cache that is shared by every user and bounded two ways, `server.rs:31-37` and `:56-62`:

```rust
const RESULT_TTL_SECS: u64 = 300;
const RESULT_CACHE_MAX_BYTES: u64 = 512 * 1024 * 1024;
MokaCache::builder()
    .max_capacity(RESULT_CACHE_MAX_BYTES)
    .weigher(|_key, value| value.estimated_bytes.max(1))
    .time_to_live(std::time::Duration::from_secs(RESULT_TTL_SECS))
```

A page fetch that misses the cache returns HTTP 404 "Query not found" with `error_code: 1`, `server.rs:1508-1523`, indistinguishable from a bogus id. The last page is invalidated eagerly, `server.rs:1495-1503`, so a client retrying the final `nextUri` after a transport blip also gets "Query not found". `time_to_live` is measured from insert, so any client that takes longer than five minutes to page a large result loses it.

## Why it matters

- A single result larger than the pool of RAM the operator budgeted for DataFusion is still allowed; it lives in JSON outside the pool. Two users each fetching 300 MB results evict each other's pages while paging. The client sees a 404 and reports a failed query with no server-side error log.
- The Trino JDBC driver and trino-python-client retry the same `nextUri` on IOException. Eager last-page invalidation turns a retry into a hard failure.
- CPU-bound JSON conversion of a multi-GB result on a runtime worker stalls other requests on that worker.
- The coordinator's Flight path streams; the Trino path does not. BI tools on the Trino path (Metabase, Superset, dbt-trino) are exactly the clients that pull large result sets.

## Proposed change

1. Change `TrinoQueryExecutor::execute` to return a `SendableRecordBatchStream` (or a `Pin<Box<dyn Stream<Item = Result<RecordBatch>>>>`). The coordinator adapter in `crates/sqe-coordinator/src/bin/sqe_server.rs:724-740` already has a streaming `QueryHandler` underneath.
2. Replace `PaginatedResult` with a per-query `ResultCursor { stream, next_token, buffered_page }` held in the existing `queries` registry (`QueryHandle`). Each `GET /v1/statement/{id}/{token}` pulls the next batch, converts one page, and keeps the previous page for exactly one retry (Trino semantics: a token may be re-requested once).
3. Charge the buffered page to the DataFusion memory pool via a `MemoryReservation` on the session's runtime, so Trino-path memory is visible to the same limits as everything else.
4. Convert Arrow to JSON per page with `arrow_json::ArrayWriter` (already supports `Utf8View`, decimals, timestamps) instead of `arrow_value_to_json` per cell (`crates/sqe-trino-compat/src/types.rs:118-330`), or move the per-page conversion under `tokio::task::spawn_blocking`.
5. Drop the 512 MB byte cap; keep idle-eviction on the registry (already `time_to_idle`, `server.rs:215-228`).
6. Distinguish "unknown query id" from "result expired" in the error body so clients can report the right thing.

## Acceptance criteria

- `make test-integration FILTER=trino` includes a test that streams a result larger than `RESULT_CACHE_MAX_BYTES` over the Trino HTTP path and pages it to completion.
- A test re-requests the last `nextUri` twice and gets the same page both times.
- A test pages a result with a 6-minute gap between page 0 and page 1 (use a mocked clock) and succeeds.
- Peak RSS of the coordinator during a 2 GB Trino-path result stays within `memory_limit` plus one page.
- `TrinoQueryExecutor::execute` no longer returns `Vec<RecordBatch>`.

## Related

- 400-range findings on Flight SQL streaming (coordinator read side) for the shared executor.
- Memory: Flight frame budget (#407, !867) did the analogous accounting for Flight; Trino path has no equivalent.
- `docs/internal/reviews/2026-07-25-production-readiness-review.md` item 15 (no Trino integration tests) still holds.
