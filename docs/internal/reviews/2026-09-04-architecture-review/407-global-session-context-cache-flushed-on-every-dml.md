# 407: The process-global SessionContext cache is flushed for every user on every successful DML

- Scope: sqe-coordinator, read side (session_context.rs, query_handler.rs execute)
- Severity: medium
- Category: performance
- Effort: M (days)
- Status: open

## Problem

`SESSION_CONTEXT_CACHE` is a `static LazyLock<moka::Cache<String, (SessionContext,
Arc<SessionCatalog>)>>` (`crates/sqe-coordinator/src/session_context.rs:64-72`), capacity 2000,
keyed by `username:token_fp:catalog:schema` (`:209-216`). Each entry is expensive: it lists
Polaris namespaces, builds a `SqeCatalogProvider` per configured catalog, and registers
snapshot-bound `TableProvider`s.

Because those providers are bound to a snapshot, any Iceberg write must evict them. The code
does so for everyone:

- `crates/sqe-coordinator/src/query_handler.rs:2210-2222`
  ```rust
  if result.is_ok() && matches!(kind, Insert | Ctas | Merge | Delete | Update | Truncate) {
      crate::session_context::invalidate_all_session_caches().await;
  }
  ```
- `crates/sqe-coordinator/src/session_context.rs:1116-1119` `invalidate_all()` then
  `run_pending_tasks()`.

DDL paths do a per-user prefix scan of the whole cache
(`session_context.rs:1082-1090`, `SESSION_CONTEXT_CACHE.iter().filter(starts_with)`), and
the memory note "Writes need session-cache invalidation" records that every new write path
has to remember to call one of these.

The cache TTL is also a process-wide `OnceLock` set at startup
(`session_context.rs:55-62`), and `SessionContext` construction reads config, policy store,
metrics, table cache, runtime catalogs, Hive cache and secrets through a ten-argument free
function (`session_context.rs:190-201`).

## Why it matters

- One dbt run doing 200 small `INSERT`s per minute drops every other tenant's context 200
  times per minute. Each of those tenants' next queries rebuilds against Polaris
  (`/v1/config`, namespace list, per-table `load_table`), which is the thundering-herd shape
  the 2026-07-25 review flagged as P1-6 for auth failures, now triggered by ordinary writes.
- Because the invalidation is by side effect in `execute()`, the streaming path, the
  maintenance scheduler, and any future write entry point can miss it (the memory note is the
  evidence that this already happened).
- A `static` cache means tests share state across the process and the coordinator cannot
  host two isolated engines (multi-tenant deployments, embedded test harnesses).

## Proposed change

1. Stop binding providers to a snapshot inside the cached context. Make `SqeTableProvider`
   resolve the current snapshot at `scan()` time from `TableMetadataCache` (which already
   has event-driven invalidation per the "Catalog cache refresh" memory note). Then a write
   only needs `table_cache.invalidate(table)`, which the catalog bridge already does, and
   `invalidate_all_session_caches` becomes unnecessary for DML.
2. Move `SESSION_CONTEXT_CACHE` and its TTL off the `static` into a `SessionContextFactory`
   struct owned by `QueryHandler` (constructed once in `main.rs`/`sqe_server.rs`), with the
   ten arguments as fields. Invalidate through the factory.
3. Until step 1 lands, make DML invalidation table-scoped: keep a secondary index
   `table_fqn -> Vec<cache_key>` filled when a provider is registered and evict only those keys.

## Acceptance criteria

- Two users, one running `INSERT` in a loop, the other running `SELECT`s: the second user's
  `SessionContext cache miss` log line appears once, not once per insert.
- `grep -rn 'invalidate_all_session_caches' crates/` returns only DDL that changes the
  namespace list (CREATE/DROP SCHEMA, ATTACH/DETACH, RENAME).
- No `static` in `session_context.rs`; `cargo test -p sqe-coordinator` runs with
  `--test-threads=8` without cross-test cache interference.

## Related

- 2026-07-25 review P1-6 still holds; same remedy (invalidate by identity, not globally).
- Memory notes "Writes need session-cache invalidation", "Catalog cache refresh".
- 409 (QueryHandler shape) for where the factory should live.
