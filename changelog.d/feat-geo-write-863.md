### Added

- SQE writes GEOMETRY and GEOGRAPHY columns (#863)

    INSERT, UPDATE and `rewrite_data_files` now write into an Iceberg V3
    table with a geospatial column, where they used to fail with
    `NOT_SUPPORTED`. A value must be valid WKB; anything else fails the write
    with the column and row. Parquet tags the column as GEOMETRY / GEOGRAPHY,
    and each data file carries a geometry column's bounding box as its
    manifest bounds, as the Iceberg spec defines them. A geography column
    gets no bounds. Spark 4.1 with Iceberg 1.12 reads the rows SQE writes and
    the files compaction writes. `CREATE TABLE` with a geospatial column is
    still not supported, and there are no `ST_*` functions.
