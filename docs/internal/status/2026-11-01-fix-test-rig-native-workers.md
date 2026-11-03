**STATUS 2026-11-01: `make test-distributed` starts its own native workers (`fix/test-rig-native-workers`).**

`make test-distributed` failed six tests on a healthy machine, all with
"worker unreachable at http://localhost:50052":

- `integration_test::test_distributed_select`
- `rewrite_data_files_distributed_parity::{position_delete_parity, equality_delete_parity, partitioned_with_deletes_parity}`
- `compaction_distributed_benchmark::distributed_rewrite_wall_clock_ratio`
- `variant_distributed::json_variant_column_scans_on_a_worker`

Cause: these tests build their own coordinator in the test process and dial
workers on `localhost:50052`/`:50053`. The overlay's worker containers are on
`60061`/`60062` (since July), and could not serve these tests anyway: the test
coordinator hands each worker an S3 connection from `tests/sqe-test.toml`
(`http://localhost:19000`), which a container resolves to itself. Nothing
started a worker on the host, and the headers told you to run
`cargo run -p sqe-worker`, which is a library.

Fix:
- `scripts/integration-test.sh`, with `DISTRIBUTED=1`: builds `sqe-server`
  the `cargo dev-server` way and starts two native workers on `:50052` and
  `:50053` (own metrics port, spill dir, `advertise_url` equal to the URL the
  tests dial so ticket audiences match, `allow_unauthenticated`). It waits
  until both accept on Flight, exports `SQE_IT_WORKER_URLS`, and kills them on
  exit. Setting `SQE_IT_WORKER_URLS` yourself skips the start.
- `scripts/integration-preflight.sh --distributed` checks the six host ports
  those workers bind.
- The test headers and the "worker unreachable" message point at
  `make test-distributed` instead of the dead `cargo run -p sqe-worker`.
- `make test` sets `RUST_MIN_STACK=33554432`, as CI does
  (`AIKIDO_RUST_MIN_STACK`).

Verified 2026-11-01: `make test-distributed` passes 349 of 349 (before: 343
passed, the six above failed). `compaction_distributed_benchmark` now runs over
two registered workers and reports local 1.78 s, distributed 22.5 s (ratio
0.08x) on its synthetic wiring-smoke table.

**NEXT:** find where the distributed rewrite spends its 20 s on a table that
rewrites locally in under 2 s (the ratio is informational; the absolute gap is
a fixed cost, not data volume).
