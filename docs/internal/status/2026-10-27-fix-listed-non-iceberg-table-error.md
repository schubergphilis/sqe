**STATUS 2026-10-27: a `SELECT` from a listed Delta table on Unity Catalog explains itself (`fix/listed-non-iceberg-table-error`).**

Follow-up to `fix/info-schema-unloadable-tables` (#860). That branch made a `CatalogHttp` 400 on `loadTable` resolve to no table, so `information_schema.columns` survives UC's Delta tables. The side effect: `SELECT * FROM tpch.nation` said "table 'dbx.tpch.nation' not found" while `SHOW TABLES` listed it.

- `SqeSchemaProvider::table()` notes `(namespace, table)` in the statement scope (`file_tvf_prepare::note_not_iceberg`) when it answers `Ok(None)` for a 400.
- `plan_sql`, `plan_parsed`, `plan_sql_without_policy` and `create_logical_plan` map DataFusion's "table '<ref>' not found" to "table '<ref>' is not an Iceberg table: the catalog lists it but does not serve it as Iceberg ..." when the ref's schema and table match a noted name (`explain_not_iceberg`, matched on `error.find_root()`). Any other not-found is unchanged.
- The note lives only in the statement's task-local scope, so `information_schema.columns` (which also hits the 400) keeps skipping the table silently.

Gates:
- `make test-integration FILTER="not_found information_schema describe show_create missing nonexistent"` -> 13 passed, 0 failed.
- `cargo test -p sqe-coordinator --test it -- not_found information_schema describe show_create missing nonexistent` (plain tests) -> 17 passed.
- `cargo test -p sqe-catalog --lib` -> 691 passed; `builtin_columns_skip_a_listed_name_that_is_not_iceberg` asserts the new message for the 400 name and the plain not-found for a 404 name.
- `cargo clippy -p sqe-catalog --all-targets -- -D warnings` -> clean.
- Live, playground3 UC via the dist coordinator: `SELECT * FROM tpch.nation LIMIT 1` -> "table 'dbx.tpch.nation' is not an Iceberg table: ...". Full suite full1/full2: the only error is that statement.

Left open (carried from the previous branch):
- A Polaris persona that may list a table but not load it (403) still fails `information_schema.columns` for the whole catalog; probe with `make test-access-control`.
- `SHOW CREATE TABLE` on UC echoes UC-injected properties (`databricks.internal.*`, `io.unitycatalog.tableId`, `write.data.path`, `write.metadata.path`) as `TBLPROPERTIES`.

**NEXT:** the Polaris list-only 403 probe, then the UC `TBLPROPERTIES` filter.
