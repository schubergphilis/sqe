### Added

- Embedded `CREATE TABLE` keeps partitioning, table properties and nested columns

    `sqe-cli --embedded` handed `CREATE TABLE` to DataFusion, which could not
    parse `PARTITIONED BY (day(ts))` or `STRUCT<...>` columns and refused
    `TBLPROPERTIES` and `WITH (...)`. On the `--warehouse` catalog and on
    attached Iceberg catalogs such as Unity it now runs through the
    coordinator's `sqe-write` DDL handler. New embedded tables are format
    version 3, as on a coordinator, where they used to be version 2;
    `WITH ('format-version' = '2')` keeps version 2. `CREATE TABLE ... LIKE`
    still runs on a coordinator only, and a CTAS with `PARTITIONED BY` or
    options is refused instead of dropping them.

### Fixed

- Embedded `INSERT` into a partitioned Iceberg table

    The vendored DataFusion Iceberg writer refused a non-nullable input
    column (`INSERT ... SELECT 1, ...`) for a nullable table column, and its
    partition-value expression declared no inputs, so DataFusion's projection
    merge evaluated it on an empty batch and the CLI panicked. Both are SQE
    patches in `vendor/datafusion-iceberg`.
