### Fixed

- File TVF reads reported 0 bytes, and coordinator-local scans counted their bytes twice

    `read_parquet`, `read_csv` and the other file table functions scan through DataFusion's `DataSourceExec`, which the bytes walk behind the audit record, `system.runtime.queries` and Trino's `processedBytes` did not treat as a scan. File-backed `DataSourceExec` nodes now count; in-memory sources (`VALUES`) still do not. `CoordinatorLocalScanExec` also passed its child's metrics through as its own, so a scan the coordinator ran itself was counted twice in the same walk and in `EXPLAIN ANALYZE`.
