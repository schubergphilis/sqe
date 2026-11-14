**STATUS 2026-11-14: Trino table property vocabulary on CREATE TABLE / CTAS / CREATE OR REPLACE (`feat/trino-table-vocabulary-897`, #897, openspec trino-table-properties 2.1-2.9, 4.1, 4.3).**

MR 1 (!1789) honoured `format_version` and the table comment. Every other key in a Trino `WITH (...)` list was still stored as a plain Iceberg property, so `partitioning`, `sorted_by` and `location` did nothing and a typo was silently kept.

- An unquoted key in a `WITH (...)` list is Trino vocabulary (`crates/sqe-write/src/write_handler/trino_props.rs`). Quoted keys and `TBLPROPERTIES` pass through as Iceberg properties, as before.
- Honoured: `partitioning` (Trino argument order, `bucket(col, n)`; an error together with `PARTITIONED BY`), `sorted_by` (the table's default sort order, ASC/DESC, NULLS FIRST/LAST), `location`, `extra_properties` (a MAP; reserved keys refused), `format` (PARQUET only), `gc_enabled`, `parquet_bloom_filter_columns`, `parquet_writer_row_group_size` (binary units).
- Refused with a reason: `compression_codec`, `target_max_file_size`, `max_commit_retry` (the retry count is fixed in `commit_with_retry`), `delete_after_commit_enabled`, `max_previous_versions`, `data_location` (`write.data.path` is read only by the purge check), `object_store_layout_enabled`, the ORC keys. Any other unquoted key is an error. A refused or unknown key leaves no table behind.
- CREATE OR REPLACE replaces in place only when schema, partitioning and sort order all match; a `location` must equal the live one. Since the merge with #892 (single-commit replace) every replace keeps the live table, so the location check applies on both paths, and a changed `sorted_by` goes through `ReplaceTableShapeAction::with_sort_order` (vendor, `SQE PATCH (sqe#897)`): the order becomes default in the same commit as the data; an empty `sorted_by` unsorts, an absent one keeps the live order.
- Changed from MR 1: `WITH (...) TBLPROPERTIES (...)` joins the `TBLPROPERTIES` items into the `WITH` list with their keys quoted, so they stay plain properties.
- `sorted_by` is metadata only: no write path sets a data file's `sort_order_id`, so scans trust the order only with `[catalog] trust_sort_order = true`.

Gate: `make test-integration FILTER=_897` -> 2 passed (`trino_format_version_property_897`, `trino_table_vocabulary_897`). Unit: `RUST_MIN_STACK=33554432 cargo test -p sqe-write --features test-sqlite --lib` (356 passed, 6 new in `trino_props`), `cargo test -p sqe-sql --lib`. `cargo clippy -p sqe-write -p sqe-sql --all-targets -- -D warnings` clean. Budget: sqe-write ceiling 28500 -> 29500 (the new module, and headroom for the ALTER follow-up).

Left open: tasks 3.1-3.2 (`ALTER TABLE ... SET PROPERTIES` for `partitioning` as spec evolution, `sorted_by`, `extra_properties`, `= DEFAULT` refused); 4.2 (stock dbt-trino with `on_table_exists = replace` and `views_enabled = false`).

**NEXT:** #897 MR 3, ALTER SET PROPERTIES (tasks 3.x, 4.2); then #886, #884.
