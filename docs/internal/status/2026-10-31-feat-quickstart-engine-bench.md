**STATUS 2026-10-31: engine-bench quickstart: SQE embedded (Parquet, Iceberg) and on Polaris vs DuckDB and Polars (`feat/quickstart-engine-bench`).**

`quickstart/engine-bench/` runs the public engine-bench workload (pinned at
`920658085cd9`) against `sqe-parquet`, `sqe-iceberg`, `sqe-polaris`, DuckDB
and Polars, validates every answer against DuckDB's, and writes `OUTPUT.md`.
It found one bug, fixed here: CTAS from Parquet with `timestamp[ms]`/`[s]`
columns failed ("Unsupported Arrow data type"); those are now widened to
microseconds on the coordinator and embedded write paths.

The run also showed three gaps, not fixed in this MR:

- Embedded `--warehouse` reads through the vendored `IcebergCatalogProvider`,
  whose `IcebergTableScan` advertises one partition and reloads the table on
  every scan, so `sqe-iceberg` decodes on one thread and is about 2x slower
  than `sqe-parquet` on the same data.
- The coordinator rejects `SET datafusion.execution.target_partitions` over
  Flight, so `sqe-polaris` runs at one partition per core, not 8.
- The coordinator rejects `COPY ... TO`. Left as is: it would write the
  server's filesystem as the server process.

**2026-11-02: `make bench`, `STORAGE=s3`.** Locally `sqe-polaris` was the
only engine reading through the Docker Desktop port forward (~220 MB/s), so
its gap to the others was mostly transport. With `STORAGE=s3` every engine
reads the same 3.36 GB of Parquet from RustFS. 150M rows, medians: SQE on
Parquet 9.92 s geomean (0.9x DuckDB), SQE on Polaris 11.91 s (1.1x), DuckDB
10.69 s, Spark 27.70 s. Polars is killed on q3 at 4.5 GiB (it finishes in
35.5 s at 8 GiB). On q1 every engine reads 1.57-1.74 GB from the store, so
SQE does not over-read; `sqe-polaris` pays a fixed 1.5-2 s on q1/q2 on top
(being traced).

q3 (latest row per customer, 15M partitions) is a full sort of 150M rows with
4.3 GB of spill. DataFusion 55.2's `WindowTopN` does not match `rn = 1` and
its `PartitionedTopK` keeps one heap per key: past 5 minutes and 6 GB at 15M
keys, and it errors instead of spilling. The upstream rework (main, after
55.2) is 6.5k changed lines on arrow 60, so it arrives with DataFusion 56,
not as a backport. `first_value(... ORDER BY ...) GROUP BY` is no way out
either: 73 s and 3.7 GiB, since its groups accumulator keeps a
`Vec<ScalarValue>` per group.

The q3 rewrite is its own branch, `feat/top-row-number`: `rn = 1` as a hash
aggregate, ~8.9 s -> ~5.6 s at 3 GiB, same rows as DuckDB (a plain `max()`
per customer, the floor for that shape, is 3.6-3.9 s).

**NEXT:** Polaris fixed-overhead trace (q1/q2 phases); re-run q3 at 150M
once `feat/top-row-number` merges; then embedded Iceberg scan through
`SqeTableProvider`, `target_partitions` over Flight.
