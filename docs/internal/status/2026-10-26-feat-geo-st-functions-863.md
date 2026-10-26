**STATUS 2026-10-26: planar `ST_*` functions over GEOMETRY (`feat/geo-st-functions-863`, part of #863).**

- **New crate `sqe-geo`**: 30 scalar UDFs (plus 4 aliases) over WKB. Decode with `wkb`, compute with `geo` 0.33 (default features off), parse/print WKT with `wkt`. Registered in `session_context.rs`, `sqe-cli/src/embedded.rs` and the worker's `runtime.rs`. Strict: NULL in, NULL out; bad WKB / WKT is an error. `ST_X` / `ST_Y` on a non-point and `ST_Length` on a non-line are errors, as in Trino.
- **Geometry results are untagged `LargeBinary`.** A `geoarrow.wkb` tag on the result broke `INSERT ... VALUES (ST_Point(1, 2)), (NULL)`: DataFusion refuses a VALUES list whose rows differ in field metadata. Results carry no CRS either; written into a GEOMETRY column they take the column's.
- **GEOGRAPHY**: a field whose GeoArrow metadata has non-planar `edges` (iceberg-rust writes `"edges":"spherical"`) is refused by the measures and predicates at planning time. Formatting and accessors work. Opt-in: `ST_GeomFromBinary(ST_AsBinary(g))`.
- **Pushdown decode fix** (`sqe-worker/src/executor.rs`, `sqe-coordinator/src/scan_pushdown.rs`): the worker decoded the pushed predicate with `Expr::from_bytes`, whose registry is empty, so any function call failed the decode and the whole predicate was dropped. It decodes against its session now, and the coordinator drops only the conjuncts the worker cannot decode (`worker_task_ctx`: DataFusion built-ins plus `sqe-geo`; not the variant functions, whose prune rides its own ticket field). In practice this mattered for a variant prune filter sitting next to plain conjuncts.
- **Leakproof**: `sqe-policy/src/leakproof.rs` is an allowlist, so every `ST_*` call counts as fallible. A plan holding one reads row-filtered scans through the `CASE WHEN <row filter> THEN col END` guard, so a geometry error cannot reveal a hidden row, on the coordinator or on a worker.

Gates:
- `cargo test -p sqe-geo` -> 5 passed. Worker `decode_pushed_predicate*` -> 4 passed; coordinator `scan_pushdown` lib tests pass, including `predicate_drops_only_the_conjunct_a_worker_cannot_decode`.
- `make test-integration FILTER=geo_` -> ok, 5 passed (223 s, rebased on main with !1551): `geo_functions::st_functions_over_a_worker_scan`, `geo_functions::st_functions_over_an_sqe_geometry_table`, `geo_spark_interop::spark_geospatial_table_scans_on_a_worker`, `geo_spark_interop::spark_reads_an_sqe_geospatial_table`, `geo_spark_interop::sqe_reads_a_spark_geospatial_table`.
- `FILTER=distributed_failure_e2e` -> ok, 6 passed. `FILTER=variant_distributed`: the two tests that need a real worker on `localhost:50052` fail under plain `make test-integration` (no worker; environmental), the chaos-worker ones pass.
- Not re-run: `make test-distributed`. It does not rebuild the worker / coordinator images (8 days old) and the rebuild was interrupted.
- `cargo xtask matrix-report --print-only` -> 154/162 (95.1%), level unchanged.

Findings:
- `wkt` 0.14 turns `POINT EMPTY` into an empty MULTIPOINT, so `ST_GeomFromText('POINT EMPTY')` silently returned a different type. It is refused now, as an empty WKB point already was (`from_wkt`).
- An `ST_*` filter is never pushed to the scan: `SqeTableProvider::supports_filters_pushdown` marks only Iceberg-convertible filters and variant prunes `Inexact`. So the worker ships every row and the coordinator filters. The IT asserts that (3 rows shipped) until spatial pruning pushes the filter.

Open:
- Spatial file pruning on the geometry bounding box (next branch), which also pushes the `ST_*` filter to the worker.
- Spherical GEOGRAPHY functions. Geospatial types nested in STRUCT / ARRAY / MAP.

**NEXT:** spatial file pruning: mark `ST_Intersects` / `ST_Contains` / `ST_Within` / `ST_DWithin` against a constant geometry `Inexact` and prune data files on their bounding-box bounds, coordinator and worker.
