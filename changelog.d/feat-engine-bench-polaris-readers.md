### Added

- engine-bench: DuckDB, Polars and embedded SQE read the Polaris tables over the Iceberg REST catalog API

    `POLARIS_READERS=1 ./run.sh` adds `sqe-embedded-polaris` (`sqe-cli
    --embedded` with `ATTACH ... (TYPE iceberg_rest)`), `duckdb-polaris`
    (DuckDB's `iceberg` extension) and `polars-polaris` (pyiceberg plus
    `pl.scan_iceberg`, planned by `polars-iceberg`). All three read
    `polaris.bench_<size>`, the snapshot `sqe-polaris` reads, so every engine
    reads one catalog and one store. The new `run_engine_polaris.py` holds the
    DuckDB and Polars runners. `run.sh` also takes `BUDGET_FOR` (per-engine kill
    thresholds) and `OUTPUT`. A Polaris run writes `OUTPUT-polaris.md`, so the
    committed `OUTPUT.md` / `OUTPUT-s3.md` stay as they are. `stderr_tail`
    now masks the Polaris root secret as well.
