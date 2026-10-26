**STATUS 2026-10-25: SQE writes geospatial columns (`feat/geo-write-863`, part of #863).**

- **Vendor (`parquet_writer.rs`, `SQE PATCH (sqe#863, from apache/iceberg-rust #3342)`)**: `MinMaxColAggregator` skips GEOMETRY / GEOGRAPHY (a byte-wise min/max of WKB is meaningless and would prune wrongly). A new `GeoBoundsAggregator` reads each row group's parquet geospatial bounding box and unions them per column; the bounds are the spec's corners (x, y[, z][, m] as little-endian doubles, z NaN when only m is present). A row group with values but no box drops the column's bounds; an all-null row group is skipped. Only GEOMETRY gets bounds: parquet computes no box for GEOGRAPHY.
- **`sqe-compaction/src/writer.rs`**: `reject_geospatial_write` is gone. `check_geospatial_wkb` parses every non-null geospatial value (top level and nested in struct/list/map) with `parquet_geospatial`'s bounder and fails the write with `InvalidCast` naming the column and row. Parquet itself would write any bytes and leave the file without a box.
- Parquet 59 with the `geospatial` feature already maps the `geoarrow.wkb` field to the GEOMETRY / GEOGRAPHY logical type, so no schema work was needed.
- SQE's readers stay safe with the new bounds: `pruning_stats.rs` ignores binary bounds and `expr_to_predicate.rs` keeps geospatial columns out of pushdown.

Gates:
- `make test-integration FILTER=geo_spark_interop` -> 1 passed (`sqe_reads_a_spark_geospatial_table`), not skipped (Spark container up, 77 s). New: SQE INSERT of `POINT (5 6)` -> its file's bounds for field 3 are the point, field 4 (geography) has none; INSERT of `X'00'` fails "is not a valid WKB"; UPDATE succeeds; Spark reads the three rows back via `ST_AsBinary`; `rewrite_data_files` merges to one file whose box is (1 2)..(5 6), and Spark reads it.
- Vendor `cargo test -p iceberg --lib` -> 2145 passed (new `test_parquet_writer_geospatial_bounds`: two row groups, xy and xyzm, geography and all-null columns; `test_geo_box_corners`).
- `cargo test -p sqe-compaction --lib` -> 120 passed (`geospatial_writes_require_wkb` replaces the refusal test). Clippy clean on sqe-compaction.

Open:
- GEOMETRY / GEOGRAPHY DDL in sqe-sql (with CRS), `ST_*` functions, bounding-box pruning.
- Worker scan of a geospatial table untested.
- Finding, not this concern: `table_files` builds its bounds JSON from `Datum`'s Display, which prints binary, date, timestamp and UUID bounds unquoted, so `lower_bounds` is not valid JSON for those columns. Next branch.
- #863 is closed; this MR says "Part of #863".

**NEXT:** fix the `table_files` bounds JSON for non-numeric types, then GEOMETRY / GEOGRAPHY DDL (#863).
