# 806: Synchronous DataFusion catalog hooks do network IO through a block_on bridge and turn failures into empty results

- Scope: sqe-catalog (`runtime_bridge.rs`, `schema_provider.rs`, `catalog_provider.rs`, `hive/provider.rs`, file TVFs)
- Severity: medium
- Category: reliability
- Effort: M
- Status: open
- Issue: #638

## Problem

Thirteen production call sites run async catalog or object-store work from
inside synchronous DataFusion hooks by way of
`crate::runtime_bridge::block_on_compat`:

- `crates/sqe-catalog/src/schema_provider.rs:205,223` (`table_names`)
- `crates/sqe-catalog/src/catalog_provider.rs:530`
- `crates/sqe-catalog/src/hive/provider.rs:1123,1189`
- `crates/sqe-catalog/src/writable_iceberg_catalog.rs:135,176`
- `crates/sqe-catalog/src/read_parquet.rs:196`, `read_csv.rs:267`, `read_json.rs:283,297`, `read_delta.rs:113`

On the deployed multi-thread runtime the bridge is
`tokio::task::block_in_place(|| handle.block_on(fut))`
(`crates/sqe-catalog/src/runtime_bridge.rs:143`). Each call evicts a runtime
worker thread for the duration of a Polaris round trip or an S3 footer read.
On a current-thread runtime the bridge spawns a fresh OS thread and a fresh
tokio runtime per call (`runtime_bridge.rs:157-165`) and detaches the thread
on timeout (`runtime_bridge.rs:170-180`, "That leaks a thread and a runtime").

The module doc is candid about the root cause: "The real fix is not to call
async catalog work from a sync hook on a single-threaded runtime"
(`runtime_bridge.rs:52-54`). Issue #195 and the catalog-traversal wedge were
both this seam.

The same hooks convert failures into empty answers.
`schema_provider.rs:209-222`:

```rust
Ok(Err(e)) => { error!(namespace = %ns, error = %e, "Failed to list tables"); Vec::new() }
Err(bridge) => { error!(...); return Vec::new(); }
```

`SchemaProvider::table_names` has no error channel, so a Polaris 503 becomes
"this schema has no tables" for `SHOW TABLES`, `information_schema` and
every BI tool's catalog browser. The July review recorded this as P1-7; it is
unchanged.

## Why it matters

- Under concurrent catalog browsing (dbt `information_schema` queries, a
  BI tool enumerating 2,000 tables) each `block_in_place` removes a worker
  from the runtime; tokio replaces it, but the replacements are bounded by
  `max_blocking_threads` and the churn shows up as tail latency on
  unrelated queries.
- Empty-on-error is indistinguishable from empty, so outages look like data
  loss to clients and to the audit log.
- Every new file TVF or catalog backend copies the pattern (four readers
  already do).

## Proposed change

1. Stop calling the network from sync hooks. DataFusion's
   `SchemaProvider::table()` is already async; use it for table
   resolution and make `table_names()` return the last snapshot from a
   cache that is refreshed asynchronously (a `watch`-style
   `Arc<ArcSwap<Vec<String>>>` refreshed by `refresh_namespace()` called from
   the coordinator's async path before planning). `catalog_provider.rs:766`
   already has a test named `a_bridge_failure_keeps_previous_snapshot`;
   make that the only behaviour.
2. For `TableFunctionImpl::call` (file TVFs), resolve schema inference in
   the coordinator's async pre-plan step (`query_handler`) and pass the
   inferred `SchemaRef` into the TVF via the session state, so `call()` is
   pure.
3. Distinguish "denied" from "unavailable": when the snapshot is stale
   because of a non-auth error, `information_schema` queries return an
   `SqeError` with code `CatalogUnavailable` (already defined,
   `sqe-core/src/error.rs:334`) instead of rows.
4. Delete the current-thread branch of `block_on_compat` once no
   production caller remains; keep the bridge only under `#[cfg(test)]`.

## Acceptance criteria

- `grep -rn 'runtime_bridge::block_on_compat' crates/sqe-catalog/src | grep -v test` returns 0 hits.
- Integration test: stop Polaris, run `SHOW TABLES` on a warm session; the
  result is the last known list plus a warning, and `SELECT * FROM
  information_schema.tables` returns `CatalogUnavailable`, not zero rows.
- Load test: 200 concurrent `information_schema.columns` queries do not
  increase `tokio_runtime_blocking_threads` (or the equivalent metric) above baseline.

## Related

- July 2026 production readiness review, P1-7 (still holds).
- Memory: #195 current-thread block_on wedge.
- 300-range catalog issues.
- Overlaps 306 (empty-result half) and 310 (catalog boundary); this file is the runtime-bridge half.
