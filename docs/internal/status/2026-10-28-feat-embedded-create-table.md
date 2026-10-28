**STATUS 2026-10-28: embedded CREATE TABLE through the sqe-write DDL handler (`feat/embedded-create-table`, Databricks UC interop gap 3b, part 1).**

`sqe-cli --embedded` routes `CREATE TABLE` with a column list on a writable
Iceberg catalog to `WriteHandler::handle_create_table_on`, then reloads the
namespace's DataFusion schema provider (new vendor patch
`IcebergCatalogProvider::schema_provider`). The front door's fallback chain
now runs `normalize_partitioned_by` and `reorder_create_table_clauses`, so
`PARTITIONED BY (day(ts))` and Spark clause order parse on every path. The
`format-version` property is sent only to REST catalogs (`iceberg_rest`
ATTACH); the SQLite catalog rejects reserved properties.

Writing to the partitioned tables this makes possible found two bugs in the
vendored `datafusion-iceberg` writer, both patched in `project.rs`:
an exact schema comparison refused non-nullable input, and `PartitionExpr`
declared no children, so a merged projection panicked on an empty batch.
A manual probe ran DELETE, UPDATE and MERGE on a `day(ts)`-partitioned table
in copy-on-write and merge-on-read mode: correct rows, files in the right
partition directories.

Found and not fixed here:

- Embedded `INSERT` of NULL into a `NOT NULL` column of an unpartitioned
  table stores `0`. The partitioned path refuses nullable input at plan time
  instead. Both need a runtime NOT NULL check.
- `INSERT ... VALUES` with an `ARRAY` literal fails on field-id metadata;
  `INSERT ... SELECT` works.
- `SHOW CREATE TABLE` renders Hive tables only in embedded mode; Iceberg
  tables return an empty definition.

**NEXT:** the embedded NOT NULL fix, then embedded `ALTER TABLE` (gap 3b part 2), then the live Databricks rerun.
