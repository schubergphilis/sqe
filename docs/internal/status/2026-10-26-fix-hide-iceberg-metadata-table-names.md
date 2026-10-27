**STATUS 2026-10-26: metadata tables no longer listed for ATTACHed / embedded Iceberg catalogs (`fix/hide-iceberg-metadata-table-names`).**

Found on the playground3 Databricks run: embedded `SHOW TABLES IN dbx.sqe_probe` returned 64 rows for 16 tables. The vendored `datafusion-iceberg` `IcebergSchemaProvider::table_names` lists `t$snapshots`, `t$manifests` and `t$history` next to every table (upstream behaviour, tested upstream). Trino and Spark list base tables only.

- `writable_iceberg_catalog::BaseTablesOnly` wraps every schema provider `WritableIcebergCatalog` hands out (construction and `create_namespace`). It filters `<name>$<MetadataTableType>` from `table_names` and delegates everything else, so `table_exist` / `table` still resolve the metadata names. No second vendor patch. `WritableIcebergCatalog` backs both the embedded CLI and the coordinator's ATTACHed catalogs (`runtime_catalog.rs`).

Gates:
- `cargo test -p sqe-catalog --lib writable_iceberg_catalog` -> 1 passed (`listing_drops_metadata_tables_but_they_still_resolve`; a real `cost$eur` stays listed).
- `cargo test -p sqe-cli` -> 101 + 10 passed; `persistent_warehouse_survives_client_restart` now asserts no `$` names in `information_schema.tables`.
- `cargo clippy -p sqe-catalog -p sqe-cli --all-targets -- -D warnings` -> clean.
- Live, embedded against Unity Catalog `pg3-bronze`: `information_schema.tables` for `sqe_part%` -> 2 rows; `table_name LIKE '%$%'` across the catalog -> 0.

Left open: querying `"t$snapshots"` in embedded mode fails ("table function 'table_snapshots' not found"); the Trino-compat rewrite runs before resolution. Already in the embedded-parity issue draft.

**NEXT:** push with the rest of the queue in `handover.md` once GitLab is reachable.
