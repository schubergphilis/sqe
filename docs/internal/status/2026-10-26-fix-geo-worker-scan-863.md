**STATUS 2026-10-26: worker scans of a Spark-written BINARY / geospatial column keep the planned `LargeBinary` (`fix/geo-worker-scan-863`, #863).**

The new worker-scan test for a Spark-written geospatial table
(`geo_spark_interop::spark_geospatial_table_scans_on_a_worker`, in-process
`PassThrough` worker) failed on its first run: `encode(g, 'hex')` returned
`Utf8` where the plan promised `LargeUtf8`. A file without an Arrow schema
decodes WKB as `Binary`. Without a projection fill (sent only when the
projection evolved) the worker streams the file's types, and
`reassemble_worker_batch` passed a batch through whenever the column names
matched, without comparing types. The coordinator's own scan widens the
column in `conform_batch_to_projection`, so only the distributed path broke.
Any Spark-written plain BINARY column was hit the same way.

`reassemble_worker_batch` now rebuilds every batch in the planned schema
(the GeoArrow tag included). A column of another type is conformed with
`sqe_core::unknown::fill_unknown`, the worker's own conform step, but only
for a width or view change (binary, string), an Iceberg promotion (int to
long, float to double, wider decimal), typed NULLs, or a nested rebuild.
Any other mismatch is still an error. Before the fix, the positional rename
branch refused it in `try_new`, and the names-match branch passed it through.

Gates:
- `cargo test -p sqe-coordinator --lib distributed_scan::tests::reassemble`: 8 passed (2 new).
- `make test-integration FILTER=geo_spark_interop` (after the rebase on !1550): 3 passed (`spark_geospatial_table_scans_on_a_worker`, `sqe_reads_a_spark_geospatial_table`, `spark_reads_an_sqe_geospatial_table`).
- `FILTER=distributed_failure_e2e`: 6 passed.
- `FILTER=prunes_shredded_row_groups_on_a_worker`: 1 passed.
- `FILTER=nested_shredded_variant_scans_on_a_worker`: 1 passed.
- `cargo clippy -p sqe-coordinator --all-targets -- -D warnings`: clean.

Rebased on !1550 (the DDL). The geometry-type:v3 evidence now lists the new
test. The level stays partial: there are no `ST_*` functions and no spatial
file pruning. Score unchanged, 154/162 (95.1%).

**NEXT:** `ST_*` functions (a GeoArrow/geo crate over the WKB column) and spatial file pruning on geometry bounds; both are needed before geometry-type:v3 is full.
