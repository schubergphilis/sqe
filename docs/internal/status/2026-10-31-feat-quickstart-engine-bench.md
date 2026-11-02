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

**NEXT:** embedded Iceberg scan through `SqeTableProvider` + the parallel scan
rules (stacked on this branch); then `target_partitions` as an allow-listed,
capped session property over Flight; COPY TO only behind an explicit,
canonicalised output root if wanted.
