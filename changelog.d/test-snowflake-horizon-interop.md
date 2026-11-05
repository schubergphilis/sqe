### Added

- Snowflake Horizon interop harness (`scripts/interop/snowflake-horizon/`)

    Snowflake and SQE each write Iceberg tables into one Horizon schema, run
    DELETE, UPDATE and MERGE on the other's tables, and compare 40 reads
    value by value. They include a merge-on-read table whose SQE deletion
    vector Snowflake must apply, and two 1M-row merge-on-read Snowflake
    tables whose position deletes and deletion vectors SQE must apply. The
    run is manual and needs a Snowflake user with a key pair. On 2026-11-05
    all 40 reads matched in embedded, coordinator and distributed mode, and
    the Databricks harness still matched 27 of 27 on the same build. The
    Iceberg matrix now rates Snowflake Horizon full on V2 and V3 (158/162).
