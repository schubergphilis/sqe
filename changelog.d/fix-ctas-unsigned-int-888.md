### Fixed

- CTAS and Flight SQL ingest create accept unsigned 64-bit columns (#888)

    `row_number()`, `rank()`, `dense_rank()` and `ntile()` return `UInt64`, which
    Iceberg has no type for, so `CREATE TABLE ... AS SELECT row_number() OVER (...)`
    failed with "UInt64 is not supported". The column is now created as `bigint`.
    The write casts with a checked cast, so a value above `9223372036854775807`
    fails the statement instead of wrapping or turning NULL, and a failed
    `CREATE OR REPLACE` leaves the previous table in place. `UInt8`, `UInt16` and
    `UInt32` were already mapped to `int`/`bigint`. Unsigned fields nested in a
    struct, list or map are not mapped yet.
