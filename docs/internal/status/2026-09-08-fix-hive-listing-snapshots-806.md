**STATUS 2026-09-08: finding 806 (#638), Hive half, on `fix/hive-listing-snapshots-806`: the Hive catalog's synchronous listing hooks serve snapshots; no `block_on` bridge left in the Hive read path.**

`HiveCatalogProvider::schema_names()` and `HiveSchemaProvider::table_names()`
used to list the metastore through `runtime_bridge::block_on_compat` inside
DataFusion's sync hooks and return an empty list on any failure (P1-7). They
now serve `HiveListings` snapshots (`crates/sqe-catalog/src/hive/provider.rs`),
per provider, reusing the Iceberg `NamespaceListing` shape: last known
names plus `stale_error`/`denied`, so an outage is served as the previous
list with a warning and `table_exist` answers unknown as present rather
than absent. `refresh_referenced(sql, default_catalog, default_schema)`
fills them on the async path: the database list when older than the TTL,
the tables of every database the statement references in that root
(`cat.db.t`; `db.t` and the session schema when the root is the default
catalog), and every database when the statement reads
`information_schema`. The coordinator calls it from one new helper,
`QueryHandler::refresh_listing_snapshots`, which also carries the Iceberg
refresh and now runs on every path that plans through `ctx.sql`: execute,
the Flight stream planner (`plan_sql_for_stream`, which had no Iceberg
refresh before), `get_schema` and `columns_for_table` (DESCRIBE / SHOW
COLUMNS). The embedded CLI refreshes its attached roots before `ctx.sql`.
DDL invalidation drops the table and the database snapshot. Six unit tests
pin the rules with a scripted metadata source. Gates: hive_ddl_test 20,
hive_mount_test 8, sqe-cli 109, `it` 150, clippy clean.

Left of 806, next MR: the four file table functions (`read_parquet`,
`read_csv`, `read_json`, `read_delta`) still infer schemas inside the sync
`TableFunctionImpl::call` through the bridge (5 sites), plus
`SqeCatalogProvider::schema()`'s re-list on miss and the writable Iceberg
catalog's create/drop namespace (3 sites); those already report the
bridge error as an error, so the acceptance criterion (zero non-test
bridge calls) is what remains, not the P1-7 empty-result bug.
**NEXT:** 806 second half (file TVF schema inference off the sync path),
then 205/212 (#653), 610 (#657), 627, #659 partials; the docker-capable CI
runner for the `polaris` group (807 item 3).
