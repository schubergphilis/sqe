**STATUS 2026-11-14: Trino `format_version` and table comment honoured (`feat/trino-table-properties-897`, #897, openspec trino-table-properties 1.1-1.6).**

Stock dbt-trino sends a model's `properties` as a Trino `WITH (...)` list. Before this change SQE stored every item as a plain Iceberg property, so `format_version = 3` created a table at the server default and nothing reported it.

- CREATE TABLE and CTAS take the version from `format_version` (number or string), `'format-version'` or `TBLPROPERTIES`. `format_version` is no longer stored as a property.
- `ALTER TABLE ... SET PROPERTIES format_version = N` (and `SET TBLPROPERTIES ('format-version' = ...)`) upgrades in one commit; the same version is a no-op, a lower one is refused.
- sqlparser keeps `COMMENT`, `WITH` and `TBLPROPERTIES` in one options slot. `create table t comment '...' WITH (...)` (dbt-trino `persist_docs`) did not parse; the front door now folds the comment into the list as `comment`. A plain `CREATE TABLE ... COMMENT '...'` was dropped silently and is now stored as `comment`.
- Finding, fixed here: `WITH (...) TBLPROPERTIES (...)` parsed but dropped the `WITH` items. The front door joins the two lists.

Gate: `make test-integration FILTER=trino_format_version_property_897` -> 1 passed. Unit: `cargo test -p sqe-sql --lib` (645 passed), `RUST_MIN_STACK=33554432 cargo test -p sqe-write --features test-sqlite --lib` (`committing::format_version`, `table_props`). Regression: `FILTER=partition` -> 19 passed; the one failure is `rewrite_data_files_distributed_parity`, which needs `make test-distributed` workers. Stock dbt-trino (dbt-core 1.12) against a real `sqe-server` with a V2 default, two `dbt build` runs (fresh CTAS, then the `rename` rebuild): both PASS=4; the two marts with `+properties: {format_version: "3"}` came out V3, a plain model V2, the `persist_docs` description stored as `comment`, and no `__dbt_tmp` / `__dbt_backup` left. The Trino door needs Basic auth while dbt-trino forces https for every auth method, so the run went through a local http proxy that adds the header.

Left open (MR 2, `openspec/changes/trino-table-properties`): `partitioning`, `sorted_by`, `location`, `extra_properties` and the rest of the vocabulary are still stored as plain properties; an unknown key is not yet an error. `on_table_exists = replace` keeps a format upgrade only with the atomic replace (#892, !1787); the default `rename` needs only this change. `views_enabled = false` is untested.

**NEXT:** #897 MR 2, the Trino property vocabulary per `openspec/changes/trino-table-properties/tasks.md` sections 2-4; then #886, #884.
