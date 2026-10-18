**STATUS 2026-10-18: iceberg-rust read correctness backports (`fix/iceberg-read-correctness-backports`).**

18 apache/iceberg-rust main fixes and RisingWave `c5498953` / `30a72274` cherry-picked onto the vendored tree; the list with commit ids is in `vendor/iceberg-rust/README.md`. The shredded-variant unshred now lives in #3255's `PromotePlan` as `PromotePlan::Unshred`. No new crates: `Cargo.lock` is unchanged.

Tests: vendored `cargo test -p iceberg --lib` 2080 passed; `make test-integration` 273 passed. With `SQE_CATALOG__SMALL_FILE_THRESHOLD_MB=0` (vendored reader path) the variant tests (9) and unknown-type tests (3) pass. `make test-distributed FILTER=distributed` against two native workers on :50052/:50053: 12 passed, including both `variant_distributed` tests. Two fail for reasons outside this change: `distributed_rewrite_wall_clock_ratio` wants a third worker on :50054, and `partitioned_with_deletes_parity` stops on a `TBLPROPERTIES` parse error. No integration test reads an Iceberg `uuid` column (SQE DDL maps `UUID` to string). A manual check used a Trino-created `uuid` table. Over ADBC Flight SQL the column now arrives as `FixedSizeBinary(16)` with the `arrow.uuid` extension, so pyarrow returns `uuid.UUID` values where it used to return 16 raw bytes. SQE `INSERT ... SELECT` and CTAS keep the Iceberg `uuid` type, and Trino reads the rows back. The Trino-protocol endpoint still returns the column as base64 `varbinary`.

`make benchmark_sf1_trino`: 230/230 queries pass. Every suite matched Trino as on the 10-10 run (tpcds q64 returns 0 rows on both engines, as on 10-10; clickbench 41/43). Total SQE time against the 10-10 run: tpch -25%, ssb -12%, tpcds -28%, tpcc -23%, tpce -20%, tpcbb -33%, bank -54%, clickbench +13% (1.7 s to 1.9 s).

**NEXT:**
- MR2: RisingWave `d294ab23` (variant predicates).
- apache #3145 (`FileScanTaskDeleteFile` private fields, fallible builder) needs its own MR that migrates `table_health.rs`, `sqe-compaction` and the worker, and reconciles the DV patches.
- Deferred: apache #3354 (Avro reader locking), #2904, #3107.
- Full re-vendor onto apache main.
- Nested unknown fields: fill missing struct children on read and prune them on write.
- `make test-distributed` without a FILTER aborts in debug builds: `scripts/integration-test.sh` drops the `prod_stack_*` skips when `DISTRIBUTED=1`, so the deep-OR overflow guard SIGABRTs the test binary. The skip should apply in both modes.
- `rewrite_data_files_distributed_parity::partitioned_with_deletes_parity` uses `TBLPROPERTIES`, which the parser rejects.
- UUID columns: comparing to a string literal (`u = '...'`) fails type coercion and `CAST(u AS VARCHAR)` is unimplemented (both predate this change). The Trino-protocol endpoint should report `uuid`, not `varbinary`.
- bank q04 went from ~0.7 s (09-30) to ~2.4 s (10-10, unchanged here).
- `benchmark_sf<N>_trino` ran scale `1_trino` under GNU make 3.81 (macOS); the rule-order fix gets its own MR. `scripts/benchmark-test.sh` leaves its coordinator on :60051 when Trino fails to start.
