### Fixed

- A worker scan of a table another engine wrote no longer fails on its BINARY or geospatial columns (#863)

    A Parquet file without an Arrow schema (Spark, Trino) decodes BINARY and
    GEOMETRY / GEOGRAPHY WKB as `Binary`, where SQE plans the column as
    `LargeBinary`. The coordinator's own scan widened the column; a batch
    streamed back from a worker kept the narrow type, and the first function
    over the column failed with `Function 'encode' returned value of type
    'Utf8' while ... 'LargeUtf8'` was promised. The coordinator now rebuilds
    every worker batch in the planned schema: a width change or an Iceberg
    type promotion is cast, and the GeoArrow tag is restored. A column of any
    other type is still an error.
