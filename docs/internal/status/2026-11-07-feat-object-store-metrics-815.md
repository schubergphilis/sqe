**STATUS 2026-11-07: object-store request metrics, part A of #815 (`feat/object-store-metrics-815`, #815).**

What changed:

- New `sqe_catalog::store_metrics`: `install_storage_metrics(&Registry)` turns on
  OpenDAL's Prometheus layer (vendored `iceberg-storage-opendal` feature `prometheus`)
  and registers the `sqe_object_store_*` families. `metered()` / `metered_for_url()`
  wrap an `Arc<dyn ObjectStore>` in `MeteredStore` (requests, errors by class,
  duration, time to first byte on `get`). Both are no-ops until installed, and never
  wrap twice.
- Wired in: `LazyHttpObjectStoreRegistry` (Hive, file TVFs; every store it registers or
  builds), the worker's `s3_object_store_factory`, and `sqe-server` main for both
  coordinator and worker registries, right after the registry is built and before any
  catalog or operator exists (OpenDAL operators built before the install carry no layer).
- Docs: `features/observability.md`, section "Object-store requests".

Tests:

- Unit: `store_metrics::tests::installed_metrics_count_object_store_and_opendal_requests`
  (object_store and OpenDAL on `file://`), passed.
- Gate: `make test-integration FILTER=hive_scan_counts_object_store_requests_815`,
  a Hive parquet scan on RustFS that asserts `sqe_object_store_requests_total{scheme="s3",bucket="warehouse"} > 0`:
  1 passed (`hive_external_e2e::hive_scan_counts_object_store_requests_815`).
- `cargo clippy -p sqe-catalog -p sqe-worker --all-targets -- -D warnings`: clean.

Limits and decisions:

- Labels name buckets (`bucket`, OpenDAL `namespace`/`root`), not catalogs.
- object_store retries inside one request, so `sqe_object_store_*` counts the request
  once; retries are visible only as `opendal_http_*` minus `opendal_operation_*`.
- Spill (`sqe-spill/src/store_s3.rs`) is not wrapped: spill has its own byte metrics and
  is not catalog I/O. Revisit if spill latency needs diagnosing.
- Deps: adds `opendal-layer-prometheus` 0.58.2 (and procfs/rustix 0.38 via
  prometheus `process`); `cargo deny check` clean.

**NEXT:** #815 part B: `sqe_table_data_files` / `sqe_table_manifests` gauges in
`analyze_one_table`, fix `manifest_count` in `maintenance.rs` (counts summary
properties, not manifests), alert rules for delete-file amplification and manifest
count in `deploy/observability/alerts.yml` with a promtool test.
