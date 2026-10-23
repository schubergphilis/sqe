### Added

- Trino `/v1/statement` reports `physicalInputBytes` and `processedBytes`

    The last page of a statement now carries the bytes it read: `physicalInputBytes` is what the scans fetched from storage (Parquet footers, pages and delete files), and `processedBytes` is the decoded size of the rows the scans produced. Earlier pages and statements without a table scan report 0. A scan that runs on workers reports the file bytes its fragments were assigned as `physicalInputBytes`, not the bytes the workers fetched, so a projection or row-group pruning on a worker is not reflected.

### Fixed

- Iceberg scans reported 0 output bytes

    `IcebergScanExec` recorded only its output rows, so the audit record's `bytes_scanned`, `system.runtime.queries` and `EXPLAIN ANALYZE` showed 0 bytes for every Iceberg read. The scan now records output batches and bytes as DataFusion's own scans do.
