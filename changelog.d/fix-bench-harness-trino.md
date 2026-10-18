### Fixed

- `make benchmark_sf<N>_trino` works under GNU make 3.81 (macOS)

    make 3.81 takes the first matching pattern rule, so the plain
    `benchmark_sf%` rule ran with scale `1_trino` and every suite failed to
    load. The `_trino` rule now comes first. `scripts/benchmark-test.sh`
    installs its cleanup as soon as the coordinator starts, so a failed
    Trino start no longer leaves a coordinator on :60051, and it stops with
    a `TRINO_PORT` hint when the Trino port is already taken.
