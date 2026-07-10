## 1. Config plumbing

- [ ] 1.1 Add `CowDmlConfig { writer_parallelism: usize }` to `crates/sqe-core/src/config.rs` with Serde derives.
- [ ] 1.2 Default to `num_cpus::get().min(8)`. Clamp on load to `1..=64`; warn on clamp.
- [ ] 1.3 Expose it on `WriteHandler` via the existing config handle in `session_context.rs`.
- [ ] 1.4 Add a config test asserting default, min, max, and clamp behaviour.

## 2. Streaming + match-count projection in `apply_update`

- [ ] 2.1 Rename `apply_update` -> `apply_update_streaming`; change return to `SendableRecordBatchStream`.
- [ ] 2.2 Add `CAST((where_sql) AS INT) AS __sqe_matched` as the final projected column in the SELECT text.
- [ ] 2.3 Replace `.collect().await?` at line 1879-1882 with `.execute_stream().await?`.
- [ ] 2.4 Keep scratch MemTable registration outside the stream so it outlives the consumer; deregister in the handler's cleanup path after the outer stream closes.
- [ ] 2.5 Delete `count_matching_rows` (lines 1897-1945). Remove its call sites in `handle_update`.
- [ ] 2.6 Update the existing `apply_update` unit tests (if any) to assert the `__sqe_matched` projection is correct for matching, non-matching, and NULL-on-WHERE cases.

## 3. Streaming writer wiring

- [ ] 3.1 Replace the per-file `rewritten_batches.push(...)` + `write_data_files_with_metrics(vec, ...)` sequence in `handle_update` with a streaming pipeline that:
  - reads batches from the parquet file,
  - pipes each through `apply_update_streaming`,
  - sums `__sqe_matched`,
  - strips the matched column via `RecordBatch::project`,
  - hands the stripped stream to `write_data_files_streaming_with_metrics`.
- [ ] 3.2 Extract the pipeline into `rewrite_file_for_update` returning `(Vec<DataFile>, usize)`.
- [ ] 3.3 Fix the latent correctness bug: verify that multi-batch rewrite outputs are all written, not just the first batch. Add a unit test on a synthetically-large input that forces multi-batch output.
- [ ] 3.4 Do the same refactor for `handle_delete` (CoW path), introducing `rewrite_file_for_delete_cow`.
- [ ] 3.5 Do the same refactor for `handle_delete_mor` (MoR path with position deletes), introducing `rewrite_file_for_delete_mor`. `filter_batch_match` / `filter_batch_negate` gain a streaming variant that returns `SendableRecordBatchStream`.

## 4. Parallelise the outer file loop

- [ ] 4.1 Wrap `handle_update`'s outer file loop in `futures::stream::iter(files).map(...).buffer_unordered(N).try_collect()` where `N = config.cow_dml.writer_parallelism`.
- [ ] 4.2 Aggregate `(new_data_files, total_updated)` via `unzip` + `flatten` + `sum`.
- [ ] 4.3 Same for `handle_delete` and `handle_delete_mor`.
- [ ] 4.4 Confirm writers and `DataFrame` futures are `Send + 'static`. Adjust lifetimes (likely `table_ident.clone()`, `assignments` captured by value) so closures compile.
- [ ] 4.5 Ensure the `_in_subq_guard` binding outlives the stream. Bind it above the `stream::iter` call; dropped only after `try_collect` resolves.

## 5. Regression tests

- [ ] 5.1 New file `crates/sqe-coordinator/tests/cow_dml_parallelism.rs`.
- [ ] 5.2 Test `update_produces_same_rowset_with_parallelism_1_vs_4`: create a table with 8 data files, run the same UPDATE twice (once with `writer_parallelism=1`, once with `=4`), assert identical post-state.
- [ ] 5.3 Test `delete_produces_same_rowset_with_parallelism_1_vs_4`.
- [ ] 5.4 Test `matched_row_count_matches_old_count_matching_rows`: UPDATE with a known-selective WHERE on a 3-file table, assert the returned affected count matches the deterministic ground truth.
- [ ] 5.5 Test `multi_batch_output_preserved`: force a file rewrite whose output exceeds the DF batch size (set `datafusion.execution.batch_size = 4096`, insert > 4096 rows), assert all rows appear in the committed new file. This is the latent-bug gate.
- [ ] 5.6 Test `error_on_one_file_aborts_transaction`: inject a parser error in one file's rewrite, assert no new data files were committed and the table state is unchanged.
- [ ] 5.7 Test `in_subquery_path_survives_parallelism`: UPDATE with `WHERE col IN (SELECT ...)` across 4 files with `writer_parallelism=4`, assert correctness. Guards that `_in_subq_guard` lifetime is right under parallelism.

## 6. Benchmark validation

- [ ] 6.1 TPC-E SF10 full run: no regression vs `tpce-sf10-flight-2026-04-21T12:20:36.json` baseline (tolerance +/- 10%). All 18 queries still pass. Commit JSON.
- [ ] 6.2 TPC-E SF100 full run: 18/18 pass. `trade_result_update_holding` completes under 120 s. Commit JSON.
- [ ] 6.3 TPC-E SF100 ablation: run the same SF100 with `writer_parallelism=1` and `=8`, commit both JSONs. Document per-query speedup in a table in the change's archive entry.
- [ ] 6.4 TPC-H SF1: 22/22, no regression vs `tpch-sf1-flight-2026-04-02T14:16:27.json`.
- [ ] 6.5 Peak RSS check: run SF100 with `time -v` or `memusg`; assert peak stays under 8 GiB on the benchmark host.

## 7. Documentation + cleanup

- [ ] 7.1 `cargo clippy -p sqe-coordinator --all-targets --all-features -- -D warnings` clean.
- [ ] 7.2 `cargo test -p sqe-coordinator --lib` clean.
- [ ] 7.3 Update `docs/roadmap.md` with a Completed entry for "CoW DML parallel + streaming at SF100".
- [ ] 7.4 Update `nextsteps.md`: tick SF100-blocker item, shift NEXT pointer to follow-up #3 (LeftSemi IN rewrite).
- [ ] 7.5 Follow-up draft: "Move `decorrelate_scalar_subqueries` outside the per-batch loop" in `followups.md`. It currently runs once per batch; it only needs to run once per DML statement. Small win, no scope creep into this change.
- [ ] 7.6 Follow-up draft: "Rewrite `lift_in_subqueries` to emit `JoinType::LeftSemi` via LogicalPlan" in `followups.md`. Addresses the IN-subquery scaling separately; composes on top of this change. References `openspec/changes/dml-subquery-streaming/followups.md` #3 which already captures this.
- [ ] 7.7 After archive: unblock `openspec/changes/dml-subquery-streaming/followups.md` #1 (revert 8 MiB stack) if SF100 passes. The acceptance criterion there requires SF100 green.
