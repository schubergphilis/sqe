# 309: Scan tuning knobs are copied through four structs with defaults repeated in each

- Scope: sqe-catalog (catalog_provider.rs, schema_provider.rs, table_provider.rs, iceberg_scan.rs), sqe-coordinator session_context.rs
- Severity: medium
- Category: maintainability
- Effort: S (hours)
- Status: open

## Problem

Nine scan-tuning values (`small_file_threshold_bytes`, `manifest_concurrency`, `prefetch_concurrency`, `runtime_filter_clustering_skip`, `runtime_filter_uniform_threshold`, `runtime_filter_wait_ms`, `runtime_filter_bloom_probe`, `runtime_filter_bloom_max_values`, `scan_fetch_ahead`) are stored as separate fields, with separate `with_*` setters, on four types, and forwarded field by field at each hop:

- `crates/sqe-catalog/src/catalog_provider.rs:193-214` fields, `340-348` and `436-444` defaults (two constructors), `359-416` setters, `559-571` forwarding.
- `crates/sqe-catalog/src/schema_provider.rs:52-72` fields, `97-105` defaults, `117-172` setters, `284-297` forwarding.
- `crates/sqe-catalog/src/table_provider.rs:41-64` fields, `91-99` defaults, `111-171` setters, `299-314` forwarding.
- `crates/sqe-catalog/src/iceberg_scan.rs:353-363` defaults again.
- `crates/sqe-coordinator/src/session_context.rs:120-138` performs the outermost copy from `CatalogConfig`.

The literal defaults `0.8`, `65536`, `true` appear in five places with no shared constant (for example `runtime_filter_uniform_threshold: 0.8` at `catalog_provider.rs:344`, `catalog_provider.rs:440`, `schema_provider.rs:101`, `table_provider.rs:95`, `iceberg_scan.rs:360`).

## Why it matters

Every new knob (issue #132 clustering, #369 bloom probing, the fetch pipeline) has touched four files plus the coordinator, and a forgotten hop silently keeps a default: `with_namespaces` at `catalog_provider.rs:420-446` already diverges from `try_new_with_options` by not accepting any of them, so a provider built that way ignores config. Reviewers cannot tell from a diff whether a value reached `IcebergScanExec`. This is the "real cost on every change" bar for medium.

## Proposed change

1. Introduce `pub struct ScanTuning { .. }` in `iceberg_scan.rs` with `impl Default` holding the single source of defaults, and `From<&CatalogConfig>` in the coordinator (or a constructor taking the three config sub-structs).
2. Store one `ScanTuning` on `SqeCatalogProvider`, `SqeSchemaProvider`, `SqeTableProvider` and `IcebergScanExec`; replace the nine setters with `with_tuning(ScanTuning)`.
3. Delete the per-field forwarding blocks; `session_context.rs:120-138` becomes one line.

## Acceptance criteria

- `grep -rn '65536\|0\.8' crates/sqe-catalog/src/*.rs` finds the value once, in `ScanTuning::default`.
- A unit test constructs a `ScanTuning` with non-default values, threads it through `SqeCatalogProvider::with_tuning` and asserts the `IcebergScanExec` produced by `scan()` carries each value (add getters on the exec for the test).

## Related

- 310 (crate boundary), 800-series cross-cutting configuration issue if filed.
