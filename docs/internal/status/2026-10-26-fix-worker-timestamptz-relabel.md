**STATUS 2026-10-26: a distributed scan reads a foreign `timestamptz` (`fix/worker-timestamptz-relabel`).**

The playground3 run with a worker and UC vended credentials found that every distributed scan of a Databricks `timestamptz` column failed. The coordinator reported: `worker column 'ts' (Timestamp(µs, "UTC")) cannot be read as Timestamp(µs, "+00:00"): not a width change or type promotion`. A file without an Arrow schema decodes the column as `"UTC"`, while the plan says `"+00:00"`. !1553 fixed the local scan (`is_timezone_relabel` in `sqe-catalog/src/iceberg_scan.rs`). The worker path conforms batches in `reassemble_worker_batch`, and its `is_conformable` had no timestamp case.

- `sqe-coordinator/src/distributed_scan.rs` `is_conformable`: a timezone-labelled timestamp conforms to another timezone label of the same unit. `fill_unknown` casts it (a relabel; both labels hold UTC instants). A different unit stays refused.
- Portal request §6 is now covered on both scan paths.
  - It differs from the request's acceptance: any zone label relabels, not only the UTC spellings. Arrow stores every zoned timestamp as a UTC instant, so no value changes.

Gates:
- `cargo test -p sqe-coordinator --lib -- reassemble` -> 9 passed, including the new `utc_timestamp_relabels_and_another_unit_is_refused`.
- `cargo clippy -p sqe-coordinator --all-targets -- -D warnings` -> clean.
- playground3, distributed (coordinator, one worker, `distributed_credentials = "vended"`, thresholds 0):
  - Before the fix, the `dbx_basic` timestamp queries failed.
  - After it, the whole probe output is identical to the local run, with `sqe_scheduler_decisions_total{decision="distributed"} 5`.
- Not run: `make test-distributed`. No distributed integration test has a Spark-written `timestamptz` table.

**NEXT:** a distributed Spark-interop test with a `timestamptz` column; then the embedded CLI findings from the playground3 run (`SHOW TABLES IN` on an attached catalog, DDL through `sqe-sql`).
