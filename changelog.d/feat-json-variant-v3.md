### Added

- `JSON` columns are Iceberg variants on format-version 3 tables (#849)

    On a V3 table a `JSON` column, also inside `STRUCT` / `ROW`, is stored
    as `VARIANT`; on a V1 / V2 table it stays a string. `CREATE TABLE` is
    V3 when it has a V3-only column, when it asks for it with
    `WITH ('format-version' = '3')`, Trino's `WITH (format_version = 3)` or
    `TBLPROPERTIES`, or when `default_table_format_version = 3`.
    `ALTER TABLE ... ADD COLUMN c JSON` on a V3 table adds a variant. The
    `json*` functions (`json_extract`,
    `json_extract_scalar`, `json_get_*`, ...) take a variant as its JSON
    text, so queries read a V3 variant column like a V2 string column.

### Changed

- New tables are Iceberg format-version 3 by default (#849)

    `catalog.default_table_format_version` now defaults to 3, so a plain
    `CREATE TABLE t (doc JSON)` stores a variant. Trino < 481, Spark < 4.0
    and Iceberg < 1.10 cannot read a V3 table: set
    `default_table_format_version = 2`, or create the table
    `WITH ('format-version' = '2')`, where those readers share the tables.
    Existing tables keep their version.

- `CREATE TABLE` refuses a `format-version` other than 1, 2 or 3, and
  refuses format-version 1 or 2 on a table with a V3-only column (variant,
  nanosecond timestamp, DEFAULT) instead of creating a table whose
  properties contradict its metadata.
