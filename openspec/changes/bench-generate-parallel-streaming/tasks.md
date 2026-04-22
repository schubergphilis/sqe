## 1. Infrastructure: config + streaming writer

- [ ] 1.1 Add `GenerateConfig { threads, compression, row_group_size }` to `crates/sqe-bench/src/generate/mod.rs`.
- [ ] 1.2 Add `CompressionKind` enum (`Zstd(i32)`, `Snappy`, `None`) with `From<&str>` for env parsing.
- [ ] 1.3 Add `BENCH_GEN_THREADS`, `BENCH_GEN_COMPRESSION`, `BENCH_GEN_ROW_GROUP_SIZE` env var readers with validation.
- [ ] 1.4 Add `--threads`, `--compression`, `--row-group-size` flags to `sqe-bench generate` in `crates/sqe-bench/src/cli.rs`.
- [ ] 1.5 Enforce precedence CLI > env > default in `main.rs::Command::Generate` dispatch.
- [ ] 1.6 Add `write_parquet_stream<I: Iterator<Item = RecordBatch>>(...)` to `parquet_writer.rs`. Reuse the rotate-at-`MAX_FILE_BYTES` logic. Accept `file_prefix: &str` so partitions can name their files independently.
- [ ] 1.7 Keep `write_parquet_files(&[RecordBatch], ...)` as a thin wrapper over `write_parquet_stream` for backward compat (short-lived).

## 2. Parallel dispatcher

- [ ] 2.1 Add `rayon` to `crates/sqe-bench/Cargo.toml` dependencies (workspace dep if available).
- [ ] 2.2 Add `partition(total_rows: usize, parts: usize) -> Vec<Range<usize>>` helper.
- [ ] 2.3 Add `seed_for_table_partition(table: &str, part_idx: usize) -> u64` (hash-based, deterministic).
- [ ] 2.4 Add `parallel_generate_table<G, T>(table_name, schema, total_rows, gen_range, output_dir, config)` dispatcher in `mod.rs`.
- [ ] 2.5 Fan out per-partition writes with `rayon::scope`. Aggregate `(rows, bytes, files, duration)` from each worker.
- [ ] 2.6 On worker error, set a cancellation flag so remaining workers exit early. Return the first error.

## 3. Generator refactor: TPC-H

- [ ] 3.1 Extract `lineitem_schema()`, `orders_schema()`, etc. as standalone functions returning `SchemaRef`.
- [ ] 3.2 Rewrite `generate_lineitem(scale)` as `generate_lineitem_range(range: Range<usize>, scale: f64, seed: u64) -> impl Iterator<Item = RecordBatch>`. Build batches lazily in the iterator's `next()`.
- [ ] 3.3 Same for `orders`, `customer`, `part`, `supplier`, `partsupp`, `nation`, `region`.
- [ ] 3.4 Update `TpchGenerator::generate_table` to compute `total_rows` and call `parallel_generate_table`.
- [ ] 3.5 Verify `BENCH_GEN_THREADS=1` produces byte-identical output to the pre-change implementation via a golden-file test.

## 4. Generator refactor: SSB, TPC-DS, TPC-C, TPC-E, TPC-BB, ClickBench

- [ ] 4.1 SSB: split generators, route through `parallel_generate_table`.
- [ ] 4.2 TPC-DS: 24 tables. Same pattern. Watch for foreign-key consistency (store_sales.ss_customer_sk references customer.c_customer_sk); verify parallel generation preserves the relationship.
- [ ] 4.3 TPC-C: warehouse-scoped generators. Partition by warehouse_id, not by row index; each warehouse's rows stay together.
- [ ] 4.4 TPC-E: security-scoped generators. Partition by security_id range. Audit the spec's inter-table contracts (trade -> holding_summary).
- [ ] 4.5 TPC-BB: same as TPC-DS (reuses TPC-DS tables).
- [ ] 4.6 ClickBench: single `hits` table. Partition by row index; trivial.

## 5. Regression tests

- [ ] 5.1 Create `crates/sqe-bench/tests/generate_parallel.rs`.
- [ ] 5.2 `parallel_matches_serial_rowset_tpch`: generate SF1 TPC-H with `threads=1` and `threads=4`, assert both produce the same set of rows for every table (order-insensitive).
- [ ] 5.3 `parallel_output_bytes_identical_at_threads_one`: assert `threads=1` SHA-256 matches a golden hash committed in the test.
- [ ] 5.4 `generate_memory_budget_test`: use `/usr/bin/time -v` or a memory probe to assert SF10 lineitem generate stays under 1 GiB RSS with `threads=8`. Release-only `#[ignore]`.
- [ ] 5.5 `compression_switch_produces_readable_output`: generate with `compression=snappy`, `compression=zstd9`, `compression=none`; read each back with `arrow::parquet::arrow::ParquetRecordBatchReader`; assert row counts match.
- [ ] 5.6 `invalid_compression_fails_fast`: `BENCH_GEN_COMPRESSION=gzip` returns a clear error, does not silently fall back.

## 6. Benchmark validation

- [ ] 6.1 SF1 TPC-H generate + load + test: no regression on query results or load time. Commit resulting JSON.
- [ ] 6.2 SF100 TPC-H generate: compare wall clock pre-change vs post-change on the same box; target >= 8x speedup.
- [ ] 6.3 SF1000 TPC-H lineitem generate: compare pre/post on the 32-CPU box; target >= 10x speedup, RSS under 4 GiB. Document the number in the change's archive notes.
- [ ] 6.4 TPC-E SF10 generate + load + test: no regression on query correctness. Commit JSON.
- [ ] 6.5 TPC-DS SF1 generate + load + test: no regression. Spot-check a table with FK dependencies (store_sales -> customer).

## 7. Documentation + cleanup

- [ ] 7.1 `cargo clippy -p sqe-bench --all-targets --all-features -- -D warnings` clean.
- [ ] 7.2 `cargo test -p sqe-bench` clean.
- [ ] 7.3 Update `docs/roadmap.md` with Completed entry "Parallel+streaming bench generation (SF1000 lineitem: 208.5s -> Xs on 32-CPU)".
- [ ] 7.4 Update `nextsteps.md` status line.
- [ ] 7.5 Update `scripts/benchmark-test.sh` header to document `BENCH_GEN_THREADS` passthrough, similar to the existing `BENCH_SCALE` / `BENCH_PROTOCOL` patterns.
- [ ] 7.6 Remove `write_parquet_files` thin wrapper once all generators are migrated to `write_parquet_stream`.
- [ ] 7.7 Follow-up draft: "Cross-table parallelism for benchmarks with many small tables (TPC-DS 24 tables)" in `followups.md`. Orthogonal to this change; worth considering after the big-table win lands.
- [ ] 7.8 Follow-up draft: "Optional ChaCha8Rng with `set_word_pos` for byte-identical parallel output" in `followups.md`. Only needed if a future test requires it.
