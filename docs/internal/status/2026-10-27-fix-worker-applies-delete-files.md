**STATUS 2026-10-27: workers apply position deletes and deletion vectors (`fix/worker-applies-delete-files`).**

Follow-up to !1570, which kept every snapshot with delete manifests on the coordinator. `IcebergScan::data_file_plan` returns the pruned data files plus the `FileScanTask` of each one that has deletes. `decide_iceberg_distribution` now stays local only for equality deletes (`has_equality_deletes`). `distribute_iceberg_scan` plans delete-bearing files as whole-file morsels (`plan_scan_morsels_by_file`) and attaches the JSON-encoded tasks to the ticket (`ScanTask::delete_tasks`, `sqe_catalog::delete_tasks`), which makes it a version 6 ticket; a version 5 worker refuses it.

Worker: `sqe-worker/src/delete_files.rs` decodes the tasks once per scan, builds a `FileIO` from the ticket's credentials (rebuilt in `apply_refreshed_credentials` when they refresh), loads each file's deletes with `iceberg::arrow::load_position_deletes`, and `row_selection` turns the file-absolute positions into a `RowSelection` over the selected row groups. `open_parquet_stream` applies it after the morsel, statistics and variant pruning. Regression test `tests/it/distributed_delete_files.rs` flipped: after the DELETE the scan must reach the worker (V2 position deletes and V3 DVs).

Limits: delete files are read outside the #407 fetch budget; equality deletes stay coordinator-only.

**NEXT:** DESCRIBE / SHOW CREATE TABLE fidelity for Databricks tables (geometry types, catalog-owned properties, format-version, sort order) and ST_SRID from field metadata.
