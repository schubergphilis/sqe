# 901: Inline S3 stores overwrite a process-wide registry across sessions

- Area: External data access and credential isolation
- Priority: P1
- Order within area: 2
- Scope: security and large-data execution
- Severity: high
- Category: security
- Effort: M
- Status: fixed (branch `fix/security-scale-review-2026-09-05`, 2026-09-05)
- Issue: #646
- Confidence: high in source-level defect; deployment impact depends on stated prerequisites

## Problem

Sessions clone the same runtime at `crates/sqe-coordinator/src/session_context.rs:387`. Their TVFs receive that runtime at `session_context.rs:833`. During inference, `read_parquet` registers the inline endpoint/credentials under only `s3://<bucket>` on that runtime (`crates/sqe-catalog/src/read_parquet.rs:235`, `:264`). CSV/JSON share the equivalent helper (`crates/sqe-catalog/src/file_tvf_common.rs:696`, `:727`). `LazyHttpObjectStoreRegistry::register_store` forwards replacement directly (`crates/sqe-catalog/src/lazy_object_store.rs:132`).

The comment claiming session-scoped registration in `file_tvf_common.rs:693` does not match the shared-runtime wiring.

## Why it matters

Two callers using the same bucket name can change which endpoint and credential set another query resolves during physical planning/execution. A caller using valid inline credentials can install a store for a bucket before or during another session's scan. This can cause failures or substitute data from a different endpoint; credential-context reuse is also possible for paths resolving through the shared registry.

The per-call path policy still applies. This finding does not claim that every caller can read any bucket or extract another caller's secret key. Exact effects depend on lookup timing and which scan providers capture a store versus resolving it later.

## Proposed change

Keep the memory pool and disk manager shared, but isolate object-store registries and relevant caches per credential context. Prefer binding an immutable store directly to each provider/physical scan so different credentials for the same bucket within one query cannot overwrite each other either. Remove planning-time mutation of a process-wide credential registry.

## Validation and acceptance criteria

Use two local S3-compatible endpoints with the same bucket/key and distinct contents. Interleave two sessions between inference and execution; each must use its own endpoint and credentials. Repeat with two TVFs in one SQL statement and with cancellation after registration. This review traced the production call chain; it did not execute the concurrent integration scenario.

## Related

Distinct from 401/601: this is coordinator-side credential isolation, with no distributed worker required.

## Resolution (2026-09-05)

File TVFs no longer register inline stores on the shared session runtime. `crates/sqe-catalog/src/scoped_table.rs` builds a per-call inference runtime that shares the parent memory pool and disk manager but owns its object-store registry and file caches, and `ScopedTable` binds the resulting `ListingTable` to that runtime through planning and execution (`ScopedExec` swaps the runtime in the `TaskContext`). `read_parquet`, `read_csv`, `read_json` and the JSON buffer path all go through it; the `runtime_env.register_object_store` call in `read_parquet.rs` is gone. Unit test `identical_bucket_paths_keep_their_own_stores_through_execution` runs two TVFs over the same `s3://` bucket name with different stores in one query. The two-endpoint concurrent integration scenario was not executed.
