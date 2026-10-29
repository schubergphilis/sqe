# feat/embedded-metadata-tables (2026-10-29)

**NEXT:** fix bare-column `PARTITIONED BY (id)` being dropped in embedded mode and on the Trino HTTP path (front door only normalizes on a parse failure), then embedded Iceberg `SHOW CREATE TABLE`, time travel, `CREATE VIEW`.

## Done

- The six Iceberg metadata TVFs (`table_snapshots`, `table_manifests`,
  `table_history`, `table_files`, `table_partitions`, `table_refs`) are
  registered in embedded mode, and registered again after `ATTACH` / `DETACH`.
- The TVFs take an optional third argument, the catalog. The
  `"t$snapshots"` rewrite in `sqe-sql` passes it for `cat.ns."t$snapshots"`.
  The coordinator still binds the session's effective catalog and ignores
  the argument (`CatalogSource::Fixed`), so cluster behaviour and the #750
  authorization gates are unchanged. Embedded uses a resolver over its
  writable catalog map (`CatalogSource::Resolver`).
- A bare `ns."t$snapshots"` reads the only Iceberg catalog, and is refused
  with a "qualify the table" error when several are mounted. Binding to
  one catalog would silently read a same-named table from the wrong one.
- Embedded mode attaches no `MetadataPolicy`: it has no policy engine.

## Found, not planned

- `SET WRITE_BRANCH` and `CALL system.*` procedures in embedded mode.
- Bare-column `PARTITIONED BY (id)` / `(a, b)` parses natively as a Hive
  column list, so `front_door` never runs `normalize_partitioned_by` and the
  table is created unpartitioned with no error. Transforms such as
  `bucket(4, id)` fail the native parse and are normalized, so they work.
  Next MR.

## Tests

- `cli_smoke::embedded_warehouse_metadata_tables` (snapshots, history,
  files, refs counts; unknown catalog error).
- `iceberg_metadata_tvf` unit tests for the catalog argument and resolver.
- `trino_compat` dollar-table tests now expect the catalog argument.
