**STATUS 2026-10-27: column metadata works on Databricks Unity Catalog (`fix/info-schema-unloadable-tables`).**

Finding C of the playground3 UC run (#860). UC lists its Delta tables in the Iceberg REST namespace and answers 400 ("not an Iceberg compatible table") to `loadTable`. DataFusion's built-in `information_schema.columns` loads every listed table of every catalog and stops at the first error, so `information_schema.columns`, `DESCRIBE`, `SHOW COLUMNS` and `SHOW CREATE TABLE` all failed on the catalog. `information_schema.tables` worked (it answers from the listing snapshot).

- `SqeSchemaProvider::table()`: a `CatalogHttp` 400 on loading a name resolves to `Ok(None)` (`table_error_is_not_iceberg`). 401/403/404/5xx unchanged. Any 400 matches, regardless of body; no Polaris path answers 400 for a listed name.
- `DESCRIBE` / `SHOW COLUMNS` / `SHOW CREATE TABLE` read the named table alone (`single_table_columns`), rendering `data_type` / `is_nullable` as the built-in does, instead of a filtered `information_schema.columns` read that loaded every table.
  - A bare name now resolves in the session default catalog and schema, as a `SELECT` does. Before, it matched the name in any schema, so `DESCRIBE foo` with `foo` in two schemas concatenated both tables' columns. `deny_hidden_table` still checks a bare name under `default` and the session schema (a superset).
  - The listing refresh before the lookup only covers the namespaces the name references, not every catalog (the Glue walk the Hive branch avoided).

Gates:
- `make test-integration FILTER="show_create describe show_columns information_schema trino_metadata access_control_e2e hive_ddl v3_e2e ctas_write_modes variant_e2e show_stats_policy"` -> 114 passed, 0 failed (incl. `show_create_table_right_after_ctas_lists_columns_850`, `describe_with_quote_in_name_is_escaped_not_injected`, `describe_table_aliases_to_show_columns`, `show_create_table_round_trips_its_column_types`).
- `cargo test -p sqe-coordinator --test it -- show_create describe show_columns information_schema hive_ddl` (plain tests, not in the `--ignored` gate) -> 33 passed.
- `cargo test -p sqe-catalog --lib -- builtin_columns_skip only_a_bad_request` -> 2 passed; the e2e test fails with the 400 arm disabled.
- `cargo test -p sqe-coordinator --lib -- single_table_columns` -> 3 passed.
- `cargo clippy -p sqe-catalog -p sqe-coordinator --all-targets -- -D warnings` -> clean.
- Live, playground3 UC via the dist coordinator: `information_schema.columns` 174 rows (was an error), `SHOW COLUMNS` / `DESCRIBE` / `SHOW CREATE TABLE` on SQE- and Databricks-written tables answer. Full suite (full1/full2): 51 + 49 statements, the only error is the expected Delta `tpch.nation` read (was 3 + 12 errors).

Left open (follow-ups):
- A `SELECT` from a listed Delta table now says "table not found" instead of UC's "not an Iceberg compatible table", while `SHOW TABLES` still lists it. Reword the not-found for a listed name, or drop 400-answered names from the listing.
- A Polaris persona that may list a table but not load it (403) still fails `information_schema.columns` for the whole catalog; probe with `make test-access-control`.
- `SHOW CREATE TABLE` on UC echoes UC-injected properties (`databricks.internal.*`, `io.unitycatalog.tableId`, `write.data.path`, `write.metadata.path`) as `TBLPROPERTIES`; they are not user-set and should not replay on another catalog.

**NEXT:** branch for the Delta not-found message, then the Polaris list-only 403 probe, then the UC `TBLPROPERTIES` filter.
