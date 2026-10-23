### Added

- Trino `/v1/statement` reports `physicalInputBytes` and `processedBytes`

    The last page of a statement now carries the bytes it read: `physicalInputBytes` is what the scans fetched from storage (Parquet footers, pages and delete files), and `processedBytes` is the decoded size of the rows the scans produced. Earlier pages and statements without a table scan report 0. Scans that run on workers in distributed mode are not counted yet.

### Fixed

- Iceberg scans reported 0 output bytes

    `IcebergScanExec` recorded only its output rows, so the audit record's `bytes_scanned`, `system.runtime.queries` and `EXPLAIN ANALYZE` showed 0 bytes for every Iceberg read. The scan now records output batches and bytes as DataFusion's own scans do.
