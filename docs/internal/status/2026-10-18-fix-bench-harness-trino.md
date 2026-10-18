**STATUS 2026-10-18: benchmark Trino compare runs on macOS make (`fix/bench-harness-trino`).**

`benchmark_sf%_trino` now precedes `benchmark_sf%` in the Makefile. GNU make 3.81 picks the first matching pattern rule, so `make benchmark_sf1_trino` had run scale `1_trino`. `scripts/benchmark-test.sh` installs its cleanup trap right after the coordinator starts, waits up to 15 s for it to stop, then sends SIGKILL. It checks the Trino port before `docker run`. Checked with `TRINO_PORT=38080 make benchmark_sf0.1_trino BENCH_SUITES=tpch` while another container held 38080: the run stopped with the port hint, the cleanup ran, and :60051 was free afterwards.

**NEXT:** unchanged.
