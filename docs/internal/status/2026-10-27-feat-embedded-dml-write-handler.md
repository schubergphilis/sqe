**STATUS 2026-10-27: embedded UPDATE / DELETE / MERGE through the sqe-write handlers (`feat/embedded-dml-write-handler`).**

Item 3a of the Databricks UC gap list. The embedded CLI planned DML through DataFusion, which refused it for Iceberg tables.

- `WriteHandler::dispatch_{delete,update,merge}_on` take a resolved `SessionCatalog`; the coordinator dispatchers resolve and call them.
- `SessionCatalog::local` wraps a non-REST `iceberg::Catalog` (SQLite warehouse, an ATTACHed REST catalog); `WritableIcebergCatalog::client` exposes it.
- `SqeConfig::embedded_defaults` gives the handler a config without a TOML file.
- `merge_source_sql` moved from the coordinator to `sqe_write::merge_sql`.
- The embedded context gets an empty `datafusion.public` catalog on the first DML: the handlers register their scratch tables there.

Gates:
- `cargo test -p sqe-cli --test cli_smoke embedded_warehouse` -> 1 passed (DELETE, UPDATE, MERGE, read back in a second process).
- `cargo test -p sqe-core embedded_defaults_parse`, `-p sqe-coordinator merge_source_sql` -> passed.
- clippy on sqe-cli, sqe-write, sqe-catalog, sqe-core, sqe-coordinator with `--tests -D warnings` -> clean.
- `cargo hakari generate --diff` -> no drift.
- Live: embedded DELETE / UPDATE / MERGE on an attached Unity catalog; Databricks SQL reads back 2|z, 3|m, 4|n.

No benchmark: the change only routes embedded statements; the coordinator path is unchanged.

**NEXT:** item 3b: embedded CREATE TABLE with PARTITIONED BY / STRUCT / ARRAY / TBLPROPERTIES and ALTER TABLE through CatalogOps.
