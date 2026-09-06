# 803: Crate layering: catalog depends on policy, both depend on the telemetry crate, and wire types leak across protocol crates

- Scope: sqe-catalog, sqe-policy, sqe-metrics, sqe-mcp, sqe-bench, sqe-coordinator
- Severity: medium
- Category: architecture
- Effort: M
- Status: open
- Issue: #635

## Problem

`cargo metadata` first-party edges (2026-09-04):

```
sqe-catalog     -> sqe-core, sqe-metrics, sqe-policy, sqe-sql
sqe-policy      -> sqe-core, sqe-metrics, sqe-sql
sqe-compaction  -> sqe-catalog, sqe-core, sqe-metrics
sqe-mcp         -> sqe-auth, sqe-core, sqe-sql, sqe-trino-compat
sqe-bench       -> sqe-trino-compat
sqe-coordinator -> sqe-worker (+ 13 others)
```

Three edges point the wrong way relative to the architecture in CLAUDE.md
(Coordinator -> policy rewriting -> catalog -> Iceberg):

1. **sqe-catalog -> sqe-policy.** The catalog's `information_schema` and
   catalog provider take a policy store: `crates/sqe-catalog/src/info_schema.rs:15,27`
   and `crates/sqe-catalog/src/catalog_provider.rs:11,188`
   (`policy_store: Option<Arc<dyn PolicyStore>>`). Because policy cannot in
   turn depend on catalog, sqe-policy grew its own registry of catalog
   names to avoid the cycle: `crates/sqe-policy/src/plan_rewriter.rs:776`
   `pub struct HiveCatalogs(RwLock<HashSet<String>>)`, and the catalog's
   `mount.rs:160` has to explain that "that check belongs where ATTACH is
   dispatched, not here". The cycle is avoided by moving state, not by
   fixing the direction.

2. **sqe-policy and sqe-catalog -> sqe-metrics.** They need one type,
   `MetricsRegistry` (`crates/sqe-policy/src/ranger_store.rs:18`,
   `crates/sqe-catalog/src/rest_catalog.rs:258`, `table_provider.rs:36`,
   `schema_provider.rs:51`, `catalog_provider.rs:192`) plus one helper
   `propagation::trace_context_http_headers` (`rest_catalog.rs:15`). To get
   it they link the whole telemetry crate, whose `Cargo.toml` pulls
   `axum`, `tonic`, `opentelemetry-otlp`, `opentelemetry_sdk`,
   `tracing-subscriber` and `prometheus`. A pure library like sqe-policy
   now transitively owns an HTTP server framework.

3. **sqe-mcp and sqe-bench -> sqe-trino-compat.** sqe-mcp uses one
   function, `sqe_trino_compat::types::arrow_value_to_json`
   (`crates/sqe-mcp/src/lib.rs:730`); sqe-bench uses two response structs
   (`crates/sqe-bench/src/client/trino.rs:7`). The Arrow-to-JSON encoder is
   protocol-neutral but lives in the Trino wire crate.

4. **sqe-coordinator -> sqe-worker** exists only so the `sqe-server` binary
   can embed a worker (`crates/sqe-coordinator/src/bin/sqe_server.rs:1801,1809`);
   the library uses it for nothing (all other hits are doc comments,
   `compaction_dispatch.rs:62,240,282,772`, `credential_refresh.rs:48`).
   The coordinator library therefore compiles the full worker for every
   build even though it never calls it.

`sqe-core` itself is clean (serde, tokio, tracing, uuid, url only).

## Why it matters

- The catalog cannot be tested or reused without a policy engine, and the
  policy crate cannot be tested without linking an OTLP exporter; unit test
  binaries for these crates are correspondingly heavy.
- The `HiveCatalogs` name registry inside the policy rewriter is a
  correctness seam: two crates each hold half of "which catalogs exist",
  kept in sync only by the coordinator remembering to call both.
- The coordinator->worker edge means a worker-only change rebuilds the
  coordinator library and its 56 integration test files (see the build-time
  plan; this is the dependency-structure half of that problem).

## Proposed change

1. Define the two traits the catalog actually needs in sqe-core:
   `pub trait ColumnVisibility { fn visible_columns(&self, user, table, cols) -> ... }`
   and have sqe-policy implement it. sqe-catalog depends on sqe-core only.
   Move `HiveCatalogs` to sqe-catalog (it is catalog state) and let the
   rewriter receive `Arc<dyn CatalogNames>`.
2. Split sqe-metrics into `sqe-metrics-core` (registry types, propagation
   helpers, no axum/tonic/otel) and `sqe-metrics` (exporters, audit sinks).
   sqe-policy, sqe-catalog and sqe-compaction depend on the core crate.
3. Move `arrow_value_to_json` and `TrinoColumn`/`TrinoResponse` into a small
   `sqe-wire-json` module in sqe-core or a new leaf crate; sqe-mcp and
   sqe-bench drop the trino-compat edge.
4. Move the embedded-worker wiring out of `sqe-server`: either the binary
   depends on both libraries (bins may) via a separate `sqe-server` crate,
   or `sqe-coordinator/Cargo.toml` marks `sqe-worker` as a
   `[[bin]]`-only dependency through a feature `embedded-worker` default-off
   for library consumers.

## Acceptance criteria

- `cargo tree -p sqe-policy -e normal | grep -E 'axum|tonic|opentelemetry'` is empty.
- `cargo tree -p sqe-catalog -e normal | grep sqe-policy` is empty.
- `cargo tree -p sqe-mcp -e normal | grep sqe-trino-compat` is empty.
- `cargo build -p sqe-coordinator --lib` does not compile sqe-worker.
- Existing information_schema column-restriction tests in sqe-catalog pass
  with an in-memory `ColumnVisibility` implementation.

## Related

- 804 (coordinator God crate).
- `docs/internal/plans/2026-09-04-build-test-time-program.md` (build-time
  consequences of the same edges).
- Overlaps 310 (catalog boundary grab-bag) and 706 (bench recompiling library modules).
