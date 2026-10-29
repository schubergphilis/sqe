# fix/partitioned-by-bare-columns (2026-10-29)

**NEXT:** embedded Iceberg `SHOW CREATE TABLE`, then time travel and `CREATE VIEW` in embedded mode.

## Done

- `SqlFrontDoor::normalize` ran `normalize_partitioned_by` only when the
  first parse failed. Bare `PARTITIONED BY (id)` parses natively as
  `hive_distribution: PARTITIONED`, which `build_partition_spec` never
  reads, so embedded mode and the Trino HTTP path created the table
  unpartitioned without an error. Transforms (`bucket(4, id)`) fail the
  native parse and were rewritten, which hid the bug.
- The parse-success branch now applies `normalize_partitioned_by` and
  `reorder_create_table_clauses` to every `CREATE TABLE`, matching the
  Flight path's `pre_parse_pipeline`. `CREATE EXTERNAL TABLE` is skipped
  inside `normalize_partitioned_by`, so Hive external tables keep their
  typed partition column list.
- A typed Hive partition list on a managed table (`PARTITIONED BY (dt
  STRING)`) is still refused in embedded mode, with DataFusion's "Hive
  distribution not supported".

## Tests

- `front_door::bare_column_partitioned_by_becomes_partition_by` (every
  wire dialect; external table unchanged).
- `cli_smoke::embedded_warehouse_partitioned_by_bare_columns` (spec fields
  and `region=eu/id=1` directory).
