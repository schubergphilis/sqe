**STATUS 2026-10-23: file TVF scans count in the bytes stats; coordinator-local scans no longer double count (`fix/stats-bytes-datasource-exec`).**

Follow-up to the Trino stats bytes work (portal request: processedBytes for every scan).

Cause:
- `extract_plan_metrics` in `crates/sqe-coordinator/src/query_handler.rs` matched scan nodes by name (`Scan`, `Parquet`, `Csv`). DataFusion 55 names its file scan `DataSourceExec`, so `read_parquet` / `read_csv` reported 0 bytes.
- `CoordinatorLocalScanExec` (`distributed_scan.rs`) returned `self.input.metrics()`. The walk visits both the wrapper and its child, so every byte counted twice.

Fix:
- A `DataSourceExec` whose data source is a `FileScanConfig` counts as a scan; `MemorySourceConfig` (VALUES, in-memory tables) does not.
- `CoordinatorLocalScanExec` drops its `metrics()` override; the child reports its own.

Gate:
- `cargo test -p sqe-coordinator --lib plan_metrics_tests`: 3 passed (DataSourceExec bytes, no double count through the wrapper, VALUES ignored).
- `make test-integration FILTER=trino_stats`: 2 passed (`trino_stats_report_bytes_for_read_parquet` new, `trino_stats_report_the_bytes_a_select_read`).
- `cargo clippy -p sqe-coordinator --all-targets -- -D warnings` clean.

Left open: worker-side scans in distributed mode are still not counted (next branch, `fix/stats-bytes-worker-scans`).

**NEXT:** unchanged
