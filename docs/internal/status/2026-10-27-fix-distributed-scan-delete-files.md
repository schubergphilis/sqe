**STATUS 2026-10-27: distributed scans no longer return deleted rows (`fix/distributed-scan-delete-files`).**

The Databricks Unity Catalog interop run (harness under `scripts/interop/databricks-uc/`) showed it: Databricks wrote a deletion vector into an SQE-written partitioned V3 table, and the distributed read still returned the deleted row. The coordinator-local and embedded reads were correct.

Cause: `ScanTask` carries only data file paths and the worker reads them with a plain Parquet reader (`sqe-worker/src/executor.rs`), so no delete file (position, DV or equality) was ever applied on a worker. `decide_iceberg_distribution` now returns `Local { reason: "has_delete_files" }` when the snapshot has delete manifests (a manifest-list read error counts as "has deletes"). The sub-file morsel gate in `distribute_iceberg_scan` was unreachable for deletes after that and is gone. Regression test: `tests/it/distributed_delete_files.rs` (V2 position deletes and V3 DVs, in-process worker, plus a delete-free control that does reach the worker).

Limit: a merge-on-read table with live deletes is now scanned on the coordinator only, until it is compacted.

**NEXT:** carry delete files in the scan ticket (paths + referenced data file, DV offsets) and apply them on the worker, so MoR tables distribute again.
