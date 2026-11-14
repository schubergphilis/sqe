## 1. Format version and comment (first MR)

- [x] 1.1 CREATE TABLE and CTAS take the version from `format_version` / `'format-version'` / `TBLPROPERTIES`; `format_version` is not stored as a property (`new_table_format_version`)
- [x] 1.2 `ALTER TABLE ... SET PROPERTIES format_version = N` upgrades in one commit, a same version is a no-op, a lower one is refused (`catalog_ops::set_table_properties`)
- [x] 1.3 `comment '...'` beside `WITH (...)` / `TBLPROPERTIES` folds into the list as `comment`; a plain `COMMENT` is stored as `comment`
- [x] 1.4 `WITH (...)` beside `TBLPROPERTIES (...)` joins it instead of being dropped
- [x] 1.5 Wire test through `/v1/statement` (`trino_table_properties_e2e`), SQLite unit tests (`committing::format_version`), sqe-sql rewrite tests with a print/re-parse round trip
- [x] 1.6 Stock dbt-trino run against the real server, two builds (fresh create, then the `rename` rebuild)

## 2. Vocabulary

- [x] 2.1 Answer the open questions: `commit.retry.num-retries` and `write.data.path` honoured or not
- [x] 2.2 A `trino_props` module in `sqe-write`: classify each option (D1), map or refuse (D2); unknown unquoted key -> error listing the supported names
- [x] 2.3 `partitioning` in Trino argument order through `build_partition_spec`; error with `PARTITIONED BY`
- [x] 2.4 `sorted_by` as the default sort order; data files keep `sort_order_id` unset (D3)
- [x] 2.5 `location` passed to `TableCreation`; no fallback (D4)
- [x] 2.6 `extra_properties` MAP; reserved keys refused
- [x] 2.7 `format`, `parquet_bloom_filter_columns`, `max_commit_retry`, `data_location`, `object_store_layout_enabled`, ORC keys per D2
- [x] 2.8 A unit test per property, each with a print/re-parse round trip of the statement
- [x] 2.9 CREATE OR REPLACE uses the same mapping, in place and schema-changing

## 3. ALTER TABLE SET PROPERTIES

- [x] 3.1 `partitioning` as partition evolution, `sorted_by` replaces the sort order, `extra_properties`
- [x] 3.2 `SET PROPERTIES p = DEFAULT` refused with a clear error

## 4. Wire and dbt

- [x] 4.1 Extend `trino_table_properties_e2e` to assert spec, sort order and properties per property
- [ ] 4.2 dbt-trino run with `on_table_exists` = `rename`, `drop`, `replace` and `skip`, and with `views_enabled = false`
- [x] 4.3 Docs: the Trino compatibility section of the DDL reference lists every property and its mapping; fix the CTAS row that claims `WITH (partitioning = ...)` works
