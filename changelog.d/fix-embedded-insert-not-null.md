### Fixed

- Embedded `INSERT` stored NULL as 0 in a `NOT NULL` column

    On the `--warehouse` catalog and on attached Iceberg catalogs,
    `INSERT ... SELECT` with a NULL, or an `INSERT` that left a `NOT NULL`
    column out of its column list, committed the row with 0 in that column.
    The vendored DataFusion Iceberg writer now refuses the batch with the
    coordinator's message, `NULL value not allowed for NOT NULL column`. A
    query into a `NOT NULL` column of a partitioned table is accepted when
    it has no NULLs, where it used to be refused at plan time.
