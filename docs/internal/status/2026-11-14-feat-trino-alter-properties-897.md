**STATUS 2026-11-14: Trino table properties on ALTER TABLE SET PROPERTIES (`feat/trino-alter-properties-897`, #897, openspec trino-table-properties 3.1-3.2).**

Stacked on `feat/trino-table-vocabulary-897`. `ALTER TABLE ... SET PROPERTIES` stored every key, `partitioning` and `sorted_by` included, as a plain Iceberg property, so dbt-trino could not change a table's layout.

- The front door's `SET PROPERTIES` rewrite (`sqe-sql/src/classifier.rs`, `rewrite_set_properties`) now keeps an identifier key unquoted and quotes only dotted or user-quoted keys, so ALTER uses CREATE's rule: unquoted = Trino, quoted = Iceberg. `SET TBLPROPERTIES` is unchanged.
- `TrinoTableOptions::parse_alter` (`crates/sqe-write/src/write_handler/trino_props.rs`): an unquoted key SQE knows as a Trino property is mapped or refused; any other key stays an Iceberg property, so an unknown key is not an error in ALTER. `location` and `p = DEFAULT` are refused.
- `partitioning` evolves the spec: a field an earlier spec had (same source column and transform) keeps its field id and name; a new field takes the next id, and a reused name gets the parameter (`id_bucket_8`). `AddSpec` + `SetDefaultSpec(-1)` with `LastAssignedPartitionIdMatch` and `DefaultSpecIdMatch`; `sorted_by` asserts `DefaultSortOrderIdMatch`.
- `sorted_by` replaces the default sort order; `ARRAY[]` is unsorted.
- Finding, fixed here: Polaris reuses a spec or sort order equal to one the table has and then refuses `SetDefaultSpec(-1)` ("no spec has been added"). Going back to an earlier layout now sets the default by the existing id.
- A layout the table already has commits nothing (the IT checks the metadata log does not grow). The IT also checks that the INSERT after each ALTER writes a manifest under the new default spec, and that rows written under all three specs read back.
- Every layout update carries `UuidMatch` and `CurrentSchemaIdMatch`; it is one commit with the property and format-version updates (`catalog_ops::set_table_properties`).

Gate: `make test-integration FILTER=_897` -> 3 passed (`trino_alter_properties_897`, `trino_format_version_property_897`, `trino_table_vocabulary_897`); full `make test-integration` -> 392 passed. `cargo clippy -p sqe-sql -p sqe-write -p sqe-coordinator --all-targets -- -D warnings` clean; `cargo xtask budgets --check` ok.

Left open: 4.2 (stock dbt-trino with `on_table_exists = replace` and `views_enabled = false`).

**NEXT:** #897 task 4.2; then #886 (OpenSpec first: Hive locations gated by the user TVF allow-list), #884.
