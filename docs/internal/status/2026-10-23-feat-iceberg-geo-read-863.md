**STATUS 2026-10-23: SQE reads Iceberg V3 GEOMETRY / GEOGRAPHY columns (`feat/iceberg-geo-read-863`).**

- Vendor backport: apache `697abcee` (geospatial types, 1/3) and PR #3341 at
  `a7af50d5` (read path, 2/3) into `vendor/iceberg-rust`; noted in its README,
  not as `SQE PATCH` sites. Adds `parquet-geospatial` and parquet's
  `geospatial` feature (hakari updated).
- Columns read as `LargeBinary` + `geoarrow.wkb`. `iceberg_scan.rs` widens a
  file's `Binary` to the table's `LargeBinary` (Spark files carry no Arrow
  schema). `expr_to_predicate.rs` keeps geoarrow columns out of pushdown:
  iceberg cannot bind a binary datum to a geometry type, and the bounds are a
  bounding box anyway.
- `reject_geospatial_write` in `sqe-compaction/src/writer.rs` refuses every
  streaming write into a table with a geospatial column (NOT_SUPPORTED).
- IT `geo_spark_interop::sqe_reads_a_spark_geospatial_table`: Spark writes,
  SQE reads, filters and self-joins; INSERT and `CREATE TABLE ... GEOMETRY`
  are refused. Passes locally (`make test-integration`).
- Matrix `geometry-type:v3` none -> partial: 152/162 (93.8%).

**NEXT:** worker-path geo read test (distributed rig); then the iceberg-rust
geospatial writer (upstream part 3/3) to lift the write refusal, and
GEOMETRY / GEOGRAPHY DDL in `sqe-sql`.
