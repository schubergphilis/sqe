### Added

- `quickstart/engine-bench`: the engine-bench workload against SQE embedded and on Polaris, next to DuckDB and Polars

    Runs the public engine-bench workload (a seeded 10M-row star schema and four
    queries: filtered rollup, fact-to-dimension join, latest row per customer,
    one row per customer) unchanged against three ways of running SQE:
    `sqe-cli --embedded` over `read_parquet()`, `sqe-cli --embedded` on a local
    Iceberg warehouse, and a native `sqe-server` coordinator on Polaris 1.7.0 +
    RustFS with an ADBC Flight SQL client. DuckDB and Polars (and Spark with
    `WITH_SPARK=1`) run through upstream's own runners. Every run is a fresh
    process with the upstream 4.5 GiB process-tree watchdog, and every answer
    is checked against DuckDB's. `./run.sh --check` asserts that every run
    finished and every answer matches.

- `make bench` / `make bench-smoke`, and `STORAGE=s3` for engine-bench: every engine reads the same Parquet from S3

    `make bench` runs engine-bench at 150M rows with Spark, the setup of the
    upstream write-up; `make bench-smoke` runs 10M rows with `--check`.
    `STORAGE=s3` (`BENCH_STORAGE=s3`) copies the data into RustFS and points
    `sqe-parquet`, DuckDB, Polars and Spark at it, so they read over the same
    path as `sqe-polaris`, and records the bytes each run read from the store.
    Results go to `OUTPUT-s3.md`. `bench.py run --budget-for polars=8` raises
    the kill threshold for one engine, and failed runs now keep their first
    exception lines rather than only the stack frames.

### Fixed

- CTAS from Parquet with `timestamp[ms]` or `timestamp[s]` columns failed with "Unsupported Arrow data type: Timestamp(ms)"

    Iceberg stores timestamps in microseconds (nanoseconds only in format
    version 3), so the table schema now widens second and millisecond
    timestamps to microseconds, on the coordinator write path and in embedded
    `CREATE TABLE ... AS SELECT`. Nanosecond timestamps are narrowed to
    microseconds below format version 3, as before.
