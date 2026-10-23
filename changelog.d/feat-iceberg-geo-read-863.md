### Added

- Read Iceberg V3 GEOMETRY / GEOGRAPHY columns (#863)

    The vendored iceberg-rust gains the upstream geospatial types (apache
    `697abcee`) and the read support from apache/iceberg-rust #3341. A
    geospatial column reads as WKB bytes (Arrow `LargeBinary` tagged
    `geoarrow.wkb`); `information_schema` reports it as `varbinary` with
    `udt_name` `geometry` / `geography`. Verified against a table written by
    Spark 4.1 + Iceberg 1.12. A predicate on a geospatial column is never
    pushed into the scan (no bounding-box pruning). Writes into a table with a
    geospatial column fail with `NOT_SUPPORTED`, and `CREATE TABLE` with a
    `GEOMETRY` column is still refused. Matrix 150/162 -> 152/162 (93.8%).

### Fixed

- The direct Parquet read widens a `Binary` column to the table's
  `LargeBinary` (and back), for files written without an Arrow schema such as
  Spark's (#863)
