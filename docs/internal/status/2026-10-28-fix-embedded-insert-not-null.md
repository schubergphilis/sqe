**STATUS 2026-10-28: embedded INSERT refuses NULL in a NOT NULL column (`fix/embedded-insert-not-null`).**

Found while building embedded `CREATE TABLE` (!1581). Embedded `INSERT`
writes through the vendored `datafusion-iceberg` `IcebergWriteExec`, which had
no equivalent of the coordinator's `check_required_columns` (#834). A NULL from
a query or an omitted column reached the parquet writer, which stored the
zeroed value slot: the row read back as 0. `INSERT ... VALUES (NULL)` was
already refused by DataFusion's VALUES planning, with a raw Arrow message.

Two vendor patches:

- `write.rs`: `IcebergWriteExec` refuses a batch with a NULL in a required
  top-level column, with the coordinator's message.
- `project.rs`: the partitioned input-schema check ignores top-level
  nullability, so a query from a nullable column into a NOT NULL column of a
  partitioned table plans; the runtime check refuses real NULLs.

Nested required fields are not checked, the same as on the coordinator.

**NEXT:** embedded `ALTER TABLE` (gap 3b part 2), then the `INSERT ... VALUES`
ARRAY field-id bug and embedded Iceberg `SHOW CREATE TABLE`, then the live
Databricks rerun.
