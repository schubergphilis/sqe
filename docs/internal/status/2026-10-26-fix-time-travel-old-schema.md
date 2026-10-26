**STATUS 2026-10-26: distributed scans read evolved schemas by field id (`fix/time-travel-old-schema`).**

The playground3 Databricks run found that distributed time travel to a snapshot from before a schema change returned NULLs, while the coordinator-local scan was correct. The integration test for it then found a second bug: the worker's pushed predicate dropped rows.

- `sqe-coordinator/src/query_handler.rs` `distribute_iceberg_scan`: the projected names and field ids come from `snapshot_schema(table, snapshot_id)`, not `current_schema()`. Before, a renamed column fell back to name matching (NULL on Databricks `col-N` files), and a dropped-and-re-added column resolved to the new field id.
- `sqe-catalog/src/iceberg_scan.rs` `projection_evolved`: same, against the scanned snapshot's schema.
- `sqe-worker/src/executor.rs` `predicate_in_file_names`: the pushed predicate is rewritten to the file's column names by field id before `build_physical_predicate`. A predicate column absent from the file means no worker predicate; the coordinator's FilterExec decides. Files or senders without field ids keep name binding.
  - The bug it fixes: after `RENAME COLUMN name TO label` + `ADD COLUMN name`, `WHERE name IS NULL` bound the old file's `name` (field 2) and the RowFilter dropped rows 1 and 2. Silent wrong result.
  - Side effect: Databricks files (`col-N`) now get worker row-group pruning and late materialization. Before, the predicate failed to bind ("No field named d") and was silently skipped.
- `sqe-catalog/src/late_materialize.rs` `build_row_filter`: the predicate column is looked up among the parquet root fields. It took the position among the leaf columns and passed it to `ProjectionMask::roots`, so a struct column before the predicate column made the RowFilter evaluate another column. Unit test `open_parquet_stream_predicate_after_a_struct_column`: 10 rows instead of 5 before the fix.

Gates:
- `make test-integration FILTER=distributed_schema_evolution_e2e` -> 3 passed, including the new `time_travel_reads_the_snapshot_columns_on_a_worker`.
  - Without the coordinator fix it fails with `["1|NULL|NULL","2|NULL|NULL"]`.
  - Without the worker fix it fails with `name IS NULL` -> `[]`.
- `make test-integration FILTER=distributed` with native workers on :50052/:50053 (`allow_unauthenticated`, `advertise_url`) -> 17 passed, 2 failed, both unrelated:
  - `distributed_rewrite_wall_clock_ratio` wants a third worker on :50054;
  - `partitioned_with_deletes_parity` fails to parse `PARTITIONED BY (region) TBLPROPERTIES (...)`.
- `cargo test -p sqe-worker --lib -- predicate` -> 11 passed, including `predicate_columns_bind_by_field_id` and `open_parquet_stream_predicate_after_a_struct_column`.
- `cargo test -p sqe-catalog --lib -- late_materialize` -> 12 passed.
- `cargo clippy -p sqe-worker -p sqe-catalog -p sqe-coordinator --all-targets -- -D warnings` -> clean.
- Live, Databricks playground3 (UC Iceberg REST, coordinator + 1 worker, this build, 2026-10-26): `FOR VERSION AS OF` on three snapshots of `sqe_probe.dbx_evo` (a DATE column `d` dropped, then re-added as a STRING `d`). The worker ran 3 scan tasks; the output now equals the coordinator-local run: old snapshots show the dates, the newest shows the string `d` only for the new row. Before the fix the distributed run returned `d` empty on the old snapshots.

Open:
- A predicate column outside the projection no longer binds by name when field ids are present. The coordinator's pushed filters only reference scan output columns, so nothing is lost today.

**NEXT:** the `PARTITIONED BY ... TBLPROPERTIES` parse failure; then the embedded CLI findings (`FOR VERSION AS OF`, `SHOW TABLES IN`, DDL through `sqe-sql`).
