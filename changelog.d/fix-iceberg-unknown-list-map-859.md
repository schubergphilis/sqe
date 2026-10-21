### Fixed

- `ALTER COLUMN ... SET DATA TYPE` refuses type changes Iceberg does not allow (#859)

    Before, any type change was committed: `VARCHAR` to `INT`, `BIGINT` to
    `INT` or a narrower `DECIMAL` went through, and every later scan of the
    table failed because the data files keep the old type. Only the spec's
    promotions are accepted now: `INT` to `BIGINT`, `REAL` to `DOUBLE`, a
    `DECIMAL` to a wider precision with the same scale, and `UNKNOWN` to any
    primitive type. A struct, list or map column, or a nested target type, is
    refused with a message instead of "Field id not found".

- A table read fails after `ALTER COLUMN ... SET DATA TYPE DOUBLE` or a wider `DECIMAL` (#859)

    Data files written before the change keep the old type, and the scan
    widened only `INT` to `BIGINT`. A `REAL` column promoted to `DOUBLE`, or
    a `DECIMAL` promoted to a wider precision, failed every later query with
    "failed to conform scan batch". Older files now read as the new type.

### Changed

- Unknown lists, maps and all-unknown structs measured against Iceberg Java (#859)

    `ARRAY<UNKNOWN>`, a map with an unknown value and a struct whose members
    are all unknown can be declared and read, and SQE refuses to write a row
    to them. Iceberg Java (Spark 4.1 + Iceberg 1.12) refuses the same writes,
    because Parquet has no column for them; a test now asserts both. The
    Iceberg matrix `unknown-type` cell moves to full.
