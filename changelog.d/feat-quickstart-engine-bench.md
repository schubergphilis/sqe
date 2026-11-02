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

### Fixed

- CTAS from Parquet with `timestamp[ms]` or `timestamp[s]` columns failed with "Unsupported Arrow data type: Timestamp(ms)"

    Iceberg stores timestamps in microseconds (nanoseconds only in format
    version 3), so the table schema now widens second and millisecond
    timestamps to microseconds, on the coordinator write path and in embedded
    `CREATE TABLE ... AS SELECT`. Nanosecond timestamps are narrowed to
    microseconds below format version 3, as before.
