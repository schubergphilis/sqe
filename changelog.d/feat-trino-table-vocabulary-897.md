### Added

- CREATE TABLE, CTAS and CREATE OR REPLACE honour the Trino table properties `partitioning`, `sorted_by`, `location`, `extra_properties`, `format`, `gc_enabled`, `parquet_bloom_filter_columns` and `parquet_writer_row_group_size` (#897)

    dbt-trino sends a model's `properties` as a Trino `WITH (...)` list, and SQE
    stored every item as a plain table property, so `partitioning` and
    `sorted_by` had no effect. An unquoted key in `WITH (...)` is now Trino
    vocabulary: supported keys map to the Iceberg partition spec, sort order,
    location or properties; Trino keys SQE cannot honour (`compression_codec`,
    `max_commit_retry`, `data_location` and others) are refused with the reason,
    and an unknown key is an error. Quoted keys and `TBLPROPERTIES` stay plain
    Iceberg properties. CREATE OR REPLACE with a different `location` is refused.

### Changed

- `WITH (...) TBLPROPERTIES (...)` joins the `TBLPROPERTIES` items into the `WITH` list with quoted keys, so they stay plain Iceberg properties (#897)
