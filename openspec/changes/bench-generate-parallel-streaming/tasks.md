## 1. Infrastructure: config + streaming writer

- [x] 1.1 Add `GenerateConfig { threads, compression, row_group_size }` to `crates/sqe-bench/src/generate/config.rs` (split out of mod.rs for isolation).
- [x] 1.2 Add `CompressionKind` enum (`Zstd1`, `Zstd3`, `Zstd9`, `Snappy`, `None`) with `parse_cli` for CLI/env parsing (named `parse_cli` rather than `from_str` to avoid shadowing the stdlib trait convention per clippy).
- [x] 1.3 Add `BENCH_GEN_THREADS`, `BENCH_GEN_COMPRESSION`, `BENCH_GEN_ROW_GROUP_SIZE` env var readers with validation and clamp-to-range on threads.
- [x] 1.4 Add `--threads`, `--compression`, `--row-group-size` flags to `sqe-bench generate` in `crates/sqe-bench/src/cli.rs`.
- [x] 1.5 Enforce precedence CLI > env > default in `main.rs::Command::Generate` dispatch via `GenerateConfig::resolve`.
- [x] 1.6 Add `write_parquet_stream<I: IntoIterator<Item = RecordBatch>>(...)` to `parquet_writer.rs`. Reuses rotate-at-`MAX_FILE_BYTES`. Accepts `file_prefix: &str` so partitions can name their files independently.
- [x] 1.7 Keep `write_parquet_files(&[RecordBatch], ...)` as a thin wrapper over `write_parquet_stream` for backward compat (tiny tables + legacy callers).

## 2. Parallel dispatcher

- [x] 2.1 No `rayon` dep needed: `std::thread::scope` + `std::thread::available_parallelism()` are both stable and sufficient for this chunky-pre-partitioned workload.
- [x] 2.2 Add `partition(total_rows: usize, parts: usize) -> Vec<Range<usize>>` helper in `config.rs`.
- [x] 2.3 Add `seed_for_table_partition(base_seed: u64, part_idx: usize) -> u64` with golden-ratio XOR mixing.
- [x] 2.4 Add `parallel_generate_table<G, I>(table_name, schema, total_rows, base_seed, output_dir, config, gen_range)` dispatcher in `mod.rs`.
- [x] 2.5 Fan out per-partition writes with `std::thread::scope`. Aggregate `(rows, bytes, files)` across workers through a `Mutex<Vec<_>>`.
- [x] 2.6 On worker error, the first `Err` returned is propagated; in-flight workers complete their own batch and exit. Single-commit Transaction is not applicable here (we're writing to parquet files, not Iceberg).

## 3. Generator refactor: TPC-H

- [x] 3.1 Schema helpers already existed as standalone functions (`lineitem_schema()`, `orders_schema()`, etc.). Nothing to extract.
- [x] 3.2 Rewrite `generate_lineitem(scale)` as `generate_lineitem_range(range, scale, seed) -> impl Iterator<Item = RecordBatch> + Send` using `std::iter::from_fn`. Batch built lazily in `next()`.
- [x] 3.3 Same for `orders`, `customer`, `part`, `supplier`, `partsupp`. `partsupp_range` reconstructs its `(part_idx, supp_offset_idx)` state from the row-range starting offset.
- [x] 3.4 Update `TpchGenerator::generate_table` to dispatch scaling tables through `parallel_generate_table`. Tiny fixed tables (`region`, `nation`) bypass the dispatcher and write one batch serially.
- [ ] 3.5 Verify `BENCH_GEN_THREADS=1` produces byte-identical output. Deferred to followup `followups.md` #2 (not blocking; would require a golden-hash regression gate).

## 4. Generator refactor: SSB, TPC-DS, TPC-C, TPC-E, TPC-BB, ClickBench

> Deferred to follow-up MR(s). Each generator accepts the new `&GenerateConfig` param (via `_config` prefix) so the workspace compiles, but currently routes through its pre-change serial body. The refactor pattern is mechanical (already exercised for TPC-H), but each benchmark has its own FK invariants that need review. Tracked in `followups.md` #1.

- [ ] 4.1 SSB: split generators, route through `parallel_generate_table`.
- [ ] 4.2 TPC-DS: 24 tables. Watch for FK consistency (store_sales.ss_customer_sk -> customer.c_customer_sk).
- [ ] 4.3 TPC-C: warehouse-scoped generators. Partition by warehouse_id, not row index.
- [ ] 4.4 TPC-E: security-scoped generators. Partition by security_id range. Audit inter-table contracts.
- [ ] 4.5 TPC-BB: same as TPC-DS (reuses TPC-DS tables).
- [ ] 4.6 ClickBench: single `hits` table. Trivial.

## 5. Regression tests

- [x] 5.1 Create `crates/sqe-bench/tests/generate_parallel.rs`.
- [x] 5.2 `parallel_and_serial_produce_same_row_counts_for_every_tpch_table`: generate SF0.01 with `threads=1` and `threads=4`, assert same row count for every table, cross-checked against parquet-on-disk row counts.
- [ ] 5.3 Byte-identical `threads=1` golden-hash test. Deferred to `followups.md` #2.
- [ ] 5.4 `generate_memory_budget_test` with `/usr/bin/time -v`. Deferred; manual memory checks on the 32-CPU box validate the 2.2 GiB RSS number.
- [x] 5.5 Compression codec runtime tests in `config.rs::tests::compression_parses_all_accepted_forms` and `compression_rejects_unknown`.
- [x] 5.6 `config_env_and_precedence_behaviour`: one consolidated test covering CLI > env > default, clamp, and invalid-value fail-fast.
- [x] 5.7 `parallel_4_produces_disjoint_file_namespaces_per_partition`: verifies the `{partition:04}{file_index:05}.parquet` layout and checks for name collisions.
- [x] 5.8 `serial_generation_at_threads_one_matches_expected_total_rows`: `threads=1` matches `TableDef::row_count` for every table.

## 6. Benchmark validation

- [ ] 6.1 SF1 TPC-H generate + load + test: deferred, requires the benchmark stack (docker compose). Expected no regression.
- [ ] 6.2 SF100 TPC-H generate: measured 36s total (vs ~12 min extrapolated serial; 20x speedup). Result `tpch-generate-sf100-zstd3-2026-04-22T13:04:08.json`.
- [x] 6.3 SF1000 TPC-H lineitem generate: **measured 4:43** on a 32-CPU / 512 GiB box, total 6:23 across all tables. 29x speedup vs extrapolated single-thread (8340s), 91% scaling efficiency, 2.2 GiB peak RSS for 6B rows in flight. Result `tpch-generate-sf1000-zstd3-2026-04-22T13:04:08.json`.
- [ ] 6.4 TPC-E SF10 generate: deferred, this change only parallelises TPC-H. TPC-E generator keeps serial behaviour.
- [ ] 6.5 TPC-DS SF1 generate: deferred, same reason.
- [x] 6.6 Compression sweep at SF100 (none, snappy, zstd3, zstd9). Confirms zstd3 as the right default. Result `tpch-generate-sf100-compression-sweep-2026-04-22T13:04:08.json`.

## 7. Documentation + cleanup

- [x] 7.1 `cargo clippy -p sqe-bench --all-targets --all-features -- -D warnings` clean.
- [x] 7.2 `cargo test -p sqe-bench` clean (131/131 pass: 128 unit + 3 integration).
- [x] 7.3 Update `docs/roadmap.md`: added Completed entry with measured numbers, moved "other 6 generators" to In Progress.
- [x] 7.4 Update `nextsteps.md` status line with the SF1000 measurement.
- [ ] 7.5 Update `scripts/benchmark-test.sh` header to document `BENCH_GEN_THREADS` passthrough. Deferred to the follow-up MR that also adds --threads to the script's default cargo build step.
- [ ] 7.6 Remove `write_parquet_files` thin wrapper once all generators are migrated. Deferred until Phase 4 is done (the wrapper is the bridge keeping SSB/TPC-DS/etc. compiling).
- [x] 7.7 Follow-up drafts captured in `followups.md`: (1) parallelise the other 6 generators, (2) byte-identical `threads=1` golden-hash gate, (3) cross-table parallelism, (4) `ChaCha8Rng` with `set_word_pos`.
