**STATUS 2026-10-29: embedded `SHOW CREATE TABLE` for Iceberg tables (`feat/embedded-show-create`).**

`sqe-cli --embedded` printed DDL for Hive tables only; an Iceberg table fell
through to DataFusion. The coordinator's renderer (columns, `PARTITIONED BY`,
sort-order comment, `TBLPROPERTIES` with the format version) moved to
`sqe_catalog::show_create::render_create_table`. The coordinator's output is
unchanged; embedded mode takes its columns from the DataFusion provider and
its metadata from the Iceberg catalog it resolves in. The cli_smoke test
asserts the exact text and re-runs it under a new name to prove the
round-trip. A missing table errors with `table not found: <name>`.

Limits: an unqualified or two-part name resolves in the session default
catalog, which is DataFusion's memory catalog in embedded mode; qualify with
`iceberg.` as for every other embedded statement.

**NEXT:** embedded time travel (`FOR VERSION AS OF` / `FOR SYSTEM_TIME AS OF`), then `CREATE VIEW` in Iceberg catalogs.
