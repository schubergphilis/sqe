**STATUS 2026-10-29: embedded backend catalogs really read-only (`fix/embedded-backend-read-only`).**

`build_embedded_context_with_backend` mounted the Glue / S3 Tables catalog
through `datafusion_iceberg::IcebergCatalogProvider` and documented it as
read-only. It is not: its schema's `register_table` creates an Iceberg table
(so `CREATE TABLE` and `CREATE VIEW` created Glue tables), `deregister_table`
drops the real table, and `IcebergTableProvider::insert_into` commits.
Found while closing the embedded view gaps after !1593.

- `crates/sqe-cli/src/read_only_catalog.rs`: `ReadOnlyCatalog` /
  `ReadOnlySchema` / `ReadOnlyTable` delegate every read and refuse
  `register_schema`, `deregister_schema`, `register_table`,
  `deregister_table`, `insert_into`, `delete_from`, `update`, `truncate`
  (`merge_into` keeps DataFusion's refusing default).
- DataFusion turns a `deregister_table` error into "Table '...' doesn't
  exist", and `DynamicFileCatalog` answers `deregister_schema` with "not
  implemented", so `EmbeddedClient::refuse_drop_on_read_only` refuses
  `DROP TABLE` / `DROP VIEW` / `DROP SCHEMA` on the backend catalog by name
  before `ctx.sql`. The name is held on the client
  (`read_only_catalog`) because `DynamicFileCatalog` wraps every catalog
  and hides the wrapper from a downcast.

Unit test `backend_catalog_refuses_every_write` mounts a memory catalog
through the wrapper and checks each write statement errors with the
read-only message, then that `SELECT` and `SHOW TABLES` still work. Not run
against a live Glue account.

**NEXT:** the embedded Iceberg view gaps on their own branch: list views in
`SHOW TABLES` / information_schema, `SHOW CREATE VIEW`, rewrite the attach
name to the warehouse name in stored bodies, and a self-reference guard on
both the embedded and coordinator view paths.
