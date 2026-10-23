**STATUS 2026-10-23: Trino `/v1/statement` stats carry `physicalInputBytes` and `processedBytes` (`feat/trino-stats-bytes`).**

Requested by the portal (`docs/sqe-requests.md` §5): per-statement bytes for cost attribution.

Change:
- `sqe-trino-compat`: `TrinoResultStream::with_bytes(probe)`; the cursor calls the probe once, on the last page, and `build_page_response` fills both stats. Earlier pages report 0.
- `sqe-coordinator`: `PlanMetrics.physical_input_bytes` sums `bytes_scanned` over every plan node; `QueryRecord.physical_input_bytes` is set after `complete` on the buffered and streaming paths; `QueryHandler::statement_bytes_probe(id)` reads the tracker.
- `sqe-server`: the Trino adapter attaches the probe whenever it has the query id.
- `sqe-catalog`: `IcebergRecordBatchStream` now records output through `RecordOutput`, so `output_bytes` / `output_batches` are set. Before, every Iceberg scan reported 0 output bytes, so `processedBytes`, audit `bytes_scanned` and `system.runtime.queries.bytes_scanned` were 0.

Gate:
- `make test-integration FILTER=trino_stats_bytes`: 1 passed (`trino_stats_report_the_bytes_a_select_read`: 5000-row table, `physicalInputBytes` in `(0, SUM(file_size_in_bytes)]`, `processedBytes > 0`, `SELECT 1` reports 0 for both). Before the scan fix it failed with `processedBytes` 0, `physicalInputBytes` 18683.
- `FILTER=audit` 1, `FILTER=trino` 6, `FILTER=explain_analyze` 1 passed.
- `cargo test -p sqe-trino-compat --lib` 186 passed (incl. `test_build_page_response_reports_bytes_on_the_last_page`), `-p sqe-catalog --lib` 668, `-p sqe-coordinator --lib` 872.
- clippy `-p sqe-catalog -p sqe-trino-compat -p sqe-coordinator -p sqe-server --all-targets -D warnings`: clean.

Open:
- Distributed mode: scans that run on workers are not counted (their metrics stay on the worker).
- `processedBytes` counts nodes whose name matches `Scan`/`Parquet`/`Csv`; DataFusion's `DataSourceExec` (file TVFs) is not matched, so a file TVF read reports `physicalInputBytes` but `processedBytes` 0.

**NEXT:** unchanged
