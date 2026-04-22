## Why

TPC-E SF100 `trade_result_update_holding` times out at the 120 s benchmark cap. The SF10 baseline ran in 10.94 s, so 10x data took at least 11x wall clock. Result: `tpce-sf100-flight-2026-04-21T19:09:34.json` (17/18 pass, one error).

The timeout is the headline. The broader signal is that every CoW UPDATE in the suite went super-linear SF10 -> SF100:

| query | SF10 | SF100 | factor for 10x data |
|---|---|---|---|
| trade_result_update_holding | 10,936 ms | >120,000 ms | >= 11x |
| trade_result_update_settlement | 475 ms | 11,692 ms | 24x |
| trade_update_executor | 902 ms | 14,263 ms | 16x |
| data_maintenance_update | 275 ms | 3,902 ms | 14x |
| trade_result_update_status | 722 ms | 9,410 ms | 13x |

`trade_result_update_holding` is first to trip the cap because its SF10 baseline was already 10 s. At SF1000 `settlement` and `executor` follow.

Three root causes in `crates/sqe-coordinator/src/write_handler.rs`:

1. **Serial per-file loop.** `handle_update` (line 1010) and `handle_delete` (line 729) iterate `for data_file in &old_data_files`. Every file is read, rewritten, and written before the next one starts. On an N-file UPDATE, wall clock is O(files) before any other cost.

2. **Double WHERE evaluation per batch.** `apply_update` (line 1803) runs `SELECT CASE WHEN (where_sql) THEN new ELSE old END`. `count_matching_rows` (line 1897) runs a second SQL round trip `SELECT COUNT(*) ... WHERE where_sql` on the same batch purely for the affected-row count. Per batch: two MemTable registrations, two SQL parses, two plans, two executions. The second pass adds no information the first pass didn't already compute.

3. **`apply_update` collects the whole rewrite before writing.** Line 1879-1882 calls `.collect().await?`. Memory scales with the file's batch output. Line 1887-1890 then takes only `result_batches.into_iter().next()`, silently dropping any second or later batch. Latent correctness bug on files whose rewrite produces more than one output batch.

## What Changes

Three compositional fixes, one MR:

- **A: Parallelise the per-file rewrite loop.** Replace the serial `for` with `futures::stream::iter(files).map(...).buffer_unordered(N)`. N defaults to `min(logical_cpus, 8)` and is tunable via config. Writers are `Send + 'static`; the commit stays single-threaded (already atomic via `Transaction::rewrite_files().add_data_files(...).delete_files(...)`).

- **B: Count matched rows from the rewrite itself, not from a second query.** Add `CAST((where_sql) AS INT) AS __sqe_matched` to the SELECT in `apply_update`. Sum the column across result batches. Strip it before writing. Delete `count_matching_rows` and its call sites. Cuts per-batch SQL work in half and removes the second MemTable registration round trip.

- **D: Stream rewrites into the writer.** Swap `.collect().await?` for `DataFrame::execute_stream().await?` piped into the existing `write_data_files_streaming_with_metrics` helper (`crates/sqe-coordinator/src/writer.rs:265`, already used by CTAS and INSERT). Peak memory drops from whole-file to one batch. Fixes the `result_batches.into_iter().next()` correctness bug as a side effect.

A config knob `cow_dml.writer_parallelism` (default `min(cpus, 8)`, range 1-64) lets operators tune or disable parallelism if they hit a memory ceiling.

## Capabilities

### Modified Capabilities

- `write-path`: CoW DML throughput scales with writer concurrency. Per-batch work is halved. Peak memory per in-flight file rewrite drops to a single streaming batch. No semantic change: the same rows are matched, the same new files are written, the same single atomic commit lands.

## Impact

- `crates/sqe-coordinator/src/write_handler.rs`: per-file loop rewritten as a bounded-concurrency stream. `apply_update` gains the `__sqe_matched` projection and returns a stream. `count_matching_rows` deleted.
- `crates/sqe-coordinator/src/writer.rs`: no change. `write_data_files_streaming_with_metrics` already exists; we wire it up.
- `crates/sqe-core/src/config.rs`: one new field `cow_dml.writer_parallelism: usize`.
- `crates/sqe-coordinator/src/session_context.rs`: thread the config into `WriteHandler` (already wired for related settings).
- No public API change. No spec change for `sql-extensions` or `security-policy`. No catalog state change.
- Performance target: TPC-E SF100 `trade_result_update_holding` completes under 60 s on Apple Silicon (5.5x at most from the 10.94 s SF10 baseline, given 10x data and 8-way parallelism). All other TPC-E UPDATEs drop proportionally.

## Success Criteria

1. TPC-E SF100 full run: 18/18 pass. `trade_result_update_holding` completes under the 120 s harness cap. Result JSON committed to `benchmarks/results/`.
2. TPC-E SF10 full run: no regression (tolerance +/- 10% vs `tpce-sf10-flight-2026-04-21T12:20:36.json` baseline). Every DML query still returns the same row count.
3. TPC-H SF1 single-node: 22/22, no regression vs `tpch-sf1-flight-2026-04-02T14:16:27.json`.
4. `cargo test -p sqe-coordinator --lib` passes (unit tests, 248/248 on current tip).
5. `cargo test -p sqe-coordinator --test in_subquery_view_rewrite` passes. Existing 10/10 + 1 release-only stress test gate against DML regressions.
6. New `cow_dml_parallelism.rs` integration test: 4-file UPDATE with `writer_parallelism = 1` and `= 4` both produce identical result sets (determinism gate).
7. `cargo clippy --all-targets --all-features -- -D warnings` clean.
8. Peak RSS during the SF100 run stays under 8 GiB on a 36 GiB machine.

## Rollback

The new behaviour is gated by `cow_dml.writer_parallelism`. Setting it to `1` at config time restores serial behaviour. Reverting the commit is a one-file revert (`write_handler.rs`) plus dropping the config field. No data migration, no catalog state change.

If a production failure surfaces that the regression tests missed, the operator can set `writer_parallelism = 1` to flip back to one-file-at-a-time execution without a deploy, preserving the other two fixes (B + D) which don't change semantics.
