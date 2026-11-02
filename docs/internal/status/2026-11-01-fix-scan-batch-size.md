**STATUS 2026-11-01: Iceberg and worker scans decode DataFusion-sized batches (`fix/scan-batch-size`).**

The streaming Iceberg reader (`IcebergScanExec`, `iceberg_scan.rs`) and the
worker's parquet reader (`open_parquet_stream`, `sqe-worker/src/executor.rs`)
never set a batch size, so the parquet crate's 1024-row default applied:
8x the batches DataFusion's operators are tuned for. The direct-read path,
`incremental_provider.rs` and the Hive reader already used 8192.

The Iceberg scan now uses the session's `datafusion.execution.batch_size`;
the worker uses 8192 (`SCAN_READER_BATCH_SIZE`).

Measured on the engine-bench 150M-row table (local warehouse, one release
binary before/after, host under load):

| | before | after |
|---|---|---|
| scan batches, `GROUP BY store_id, status` | 146.5 K | 18.3 K |
| user CPU, same query | 7.1 s | 5.3 s |
| q1 wall (3 reps) | 1.6-2.5 s | 1.18-1.44 s |
| q2 wall | 3.2-5.0 s | 2.75-3.52 s |
| q4 wall | 6.3-6.6 s | 5.66-6.45 s |
| q3 sort spills / RSS at 3 GB | 99 / 2.23-2.25 GiB | 99 / 2.27-2.29 GiB |

The plain parquet path (`read_parquet`, 8192-row `Utf8View` batches) runs
the same aggregate in 2.8 s of user CPU. The remaining gap is the string
type: the Iceberg reader decodes strings to `Utf8` (dictionary values copied
into offset buffers, then hashed and compared byte-wise in the group-by),
where `read_parquet` decodes to `Utf8View`.

**NEXT:** Iceberg scans decoding strings as `Utf8View` (upstream first; then
the provider schema, worker reader schema and write/policy paths).
