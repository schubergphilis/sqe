### Fixed

- `make test-distributed` starts the native workers its distributed tests dial

    Six tests (`test_distributed_select`, the three
    `rewrite_data_files_distributed_parity` cases,
    `distributed_rewrite_wall_clock_ratio`,
    `json_variant_column_scans_on_a_worker`) failed with "worker unreachable at
    http://localhost:50052" because nothing started a worker there.
    `scripts/integration-test.sh` now starts two `sqe-server --mode worker`
    processes on `:50052` and `:50053` when `DISTRIBUTED=1`, waits until they
    accept, and stops them on exit. `make test` sets `RUST_MIN_STACK` to
    32 MiB, as CI does.
