## Context

The Trino door (`sqe-trino-compat`) passes the statement text to the query handler. The front door (`sqe-sql::front_door::normalize`) parses it, and every rewrite there must survive printing and re-parsing. Create-table options reach `sqe-write` as `CreateTableOptions` (`With`, `TableProperties`, `Plain`), flattened by `create_table_options_as_slice`. `merge_user_table_properties` turned every `KeyValue` into an Iceberg property.

sqlparser keeps `COMMENT`, `WITH (...)` and `TBLPROPERTIES` in one options slot. The first MR handles that in `reorder_create_table_clauses`.

## Goals / Non-Goals

**Goals**
- Honour or refuse every property of the Trino Iceberg connector. No property is stored and ignored.
- One translation, used by CREATE TABLE, CTAS, CREATE OR REPLACE and ALTER TABLE SET PROPERTIES.

**Non-Goals**
- ORC and Avro data files.
- Trino's Hive and Delta connector properties.

## Decisions

### D1. Unquoted `WITH` keys are the Trino vocabulary, on every wire
The query handler does not know which wire a statement came from, and threading that through would touch every entry point. Instead, a key is classified by its spelling:
- Iceberg property names hold dots and must be quoted (`'write.delete.mode'`).
- An unquoted single-word key in `WITH (...)` is the Trino vocabulary. An unknown one is an error that lists the supported names.
- Quoted keys and every `TBLPROPERTIES` key pass through as Iceberg properties, as today.
- `WITH (...) TBLPROPERTIES (...)` reaches the handler as one `WITH` list: the front door appends the `TBLPROPERTIES` items with their keys quoted, so an unquoted `TBLPROPERTIES` key stays an Iceberg property (`sqe-sql/src/partition.rs`, `quote_option_keys`).

*Alternative:* refuse unknown keys on the Trino door only. Rejected: the same `CREATE TABLE ... WITH (partitioning = ...)` would mean different things on Flight SQL and Trino.

### D2. The vocabulary

| Trino property | SQE | Notes |
|---|---|---|
| `format_version` | version of the new table; ALTER upgrades | done (first MR) |
| `comment` (folded from `COMMENT '...'`) | property `comment` | done (first MR); not a Trino `WITH` key, accepted because the front door writes it |
| `partitioning = ARRAY['col', 'bucket(col, 16)', 'truncate(col, 4)', 'year(ts)', 'month(ts)', 'day(ts)', 'hour(ts)', 'void(col)']` | partition spec, via `build_partition_spec` | Trino puts the column first: `bucket(col, n)`, unlike Spark's `bucket(n, col)`. It is an error together with `PARTITIONED BY` |
| `sorted_by = ARRAY['col', 'col DESC', 'col ASC NULLS LAST']` | table sort order | metadata only (D3) |
| `location = '...'` | `TableCreation.location` | a refusal by the catalog is the error; no retry without it (D4) |
| `extra_properties = MAP(ARRAY[k...], ARRAY[v...])` | Iceberg properties | reserved keys (`format-version`, `current-snapshot-id`, ...) refused |
| `format = 'PARQUET'` | accepted | `ORC` and `AVRO` refused |
| `format_version`-like Iceberg keys inside `extra_properties` | refused | use `format_version` |
| `parquet_bloom_filter_columns = ARRAY['c']` | `write.parquet.bloom-filter-enabled.column.c = true` | SQE writes bloom filters from these properties |
| `max_commit_retry = n` | refused | SQE's commit retry is a fixed number of attempts (`commit::commit_with_retry`); it does not read `commit.retry.num-retries` |
| `data_location = '...'` | refused | the writer writes under the table location; `write.data.path` is read only by the purge safety check |
| `gc_enabled` | `gc.enabled` | read by maintenance (expire, purge) |
| `parquet_writer_row_group_size = '128MB'` | `write.parquet.row-group-size-bytes` | a Trino data size, binary units; read by `parquet_writer_config` |
| `compression_codec` | refused | the codec comes from the session (`iceberg.compression_codec`) or the configuration (`resolve_write_compression`) |
| `target_max_file_size` | refused | the writer does not read a per-table target file size |
| `delete_after_commit_enabled`, `max_previous_versions` | refused | SQE does not manage metadata files; the error names the quoted Iceberg keys for the catalog |
| `object_store_layout_enabled` | refused | SQE's writer has no object-store layout |
| `orc_bloom_filter_columns`, `orc_bloom_filter_fpp` | refused | no ORC |

### D3. `sorted_by` is metadata only
The table gets the sort order as its default, so other engines that sort on write use it. SQE's writer does not sort CTAS output, so its data files keep `sort_order_id` unset. A reader that trusts the sort order (`trust_sort_order`) must never see a file that claims an order it does not have.

### D4. `location` never falls back
If Polaris refuses the location (outside the allowed locations), the statement fails with the catalog's error. Retrying without the location would create the table somewhere the user did not ask for.

### D5. ALTER TABLE SET PROPERTIES
The same vocabulary, in one commit where possible:
- `format_version` upgrades the table (done);
- `partitioning` adds a spec and makes it the default (partition evolution);
- `sorted_by` replaces the default sort order;
- `extra_properties` sets the keys it lists.

Trino resets a property with `SET PROPERTIES p = DEFAULT`. That form is refused until it is needed.

The quoting rule is the one CREATE uses. The front door's `SET PROPERTIES` rewrite keeps an identifier key unquoted and quotes a dotted one, and every `SET TBLPROPERTIES` key is quoted, so `SET TBLPROPERTIES` keeps its old meaning. An unquoted key is Trino vocabulary when its name is a supported or refused Trino property; any other unquoted key stays an Iceberg property, as `SET PROPERTIES` always stored it, so an unknown key is not an error in ALTER (existing `SET PROPERTIES owner = '...'` statements keep working). `location` is refused: moving a table is not a property change.

A partition field the table had in any earlier spec, matched on source column and transform, keeps its field id and name; a new field takes the next id after the last assigned. A new field whose name an earlier field used (`bucket(id, 8)` after `bucket(id, 4)`) gets the parameter as a suffix (`id_bucket_8`), as Iceberg's own spec evolution names it. The commit asserts the default spec and sort order ids it read, beside the table uuid, schema id and last partition id. The catalog reuses a spec or sort order equal to one the table already has, and then has no "last added" one, so going back to an earlier layout sets the default by its id instead of adding it again.

## Risks / Trade-offs

- **The unknown-key error is a behaviour change.** Existing SQL with an unquoted custom key fails. The error message tells the user to quote the key.
- **Trino and Spark disagree on transform argument order.** `partitioning` uses Trino's order and `PARTITIONED BY` keeps Spark's. Each form gets a test.

## Open Questions

- ~~Does `sqe-write`'s commit retry read `commit.retry.num-retries`, and does the writer read `write.data.path`?~~ No to both, so `max_commit_retry` and `data_location` are refused (D2).
