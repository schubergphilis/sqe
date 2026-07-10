# Follow-up issues

Captured here as ready-to-file drafts. Open in GitLab once this change merges. Reference this OpenSpec change (`bench-generate-parallel-streaming`) and MR !78.

## 1. Parallelise the other 6 generators (SSB, TPC-DS, TPC-C, TPC-E, TPC-BB, ClickBench)

**Title:** Extend parallel+streaming generation to SSB, TPC-DS, TPC-C, TPC-E, TPC-BB, ClickBench

**Body:**

MR !78 shipped parallel generation for TPC-H only. The infrastructure (`GenerateConfig`, `parallel_generate_table`, `write_parquet_stream`) is in place and generic; each remaining generator just needs its per-table `_range(range, scale, seed)` iterator-returning refactor.

Why not in MR !78: each benchmark has its own FK invariants. TPC-DS has 24 inter-related tables; TPC-C partitions by warehouse_id not row index; TPC-E has security-scoped data with trade -> holding_summary contracts. Mechanical work per table but each needs review.

**Files:**

- `crates/sqe-bench/src/generate/ssb.rs`
- `crates/sqe-bench/src/generate/tpcds.rs`
- `crates/sqe-bench/src/generate/tpcc.rs`
- `crates/sqe-bench/src/generate/tpce.rs`
- `crates/sqe-bench/src/generate/tpcbb.rs`
- `crates/sqe-bench/src/generate/clickbench.rs`

**Acceptance:**

- Each generator's `generate_table` dispatches through `parallel_generate_table` at `threads > 1`.
- Row counts at `threads=1` vs `threads=N` match for every table in every benchmark (same row-set, order-insensitive).
- Integration test `tests/generate_parallel.rs` extended to cover each benchmark.
- SF100 TPC-DS generate time drops proportionally to TPC-H's observed 20x speedup.
- Removes the temporary `write_parquet_files` wrapper once all callers migrate.

## 2. Byte-identical output at `BENCH_GEN_THREADS=1`

**Title:** Golden-hash regression gate for `BENCH_GEN_THREADS=1` determinism

**Body:**

The design promises that `threads=1` produces byte-identical output to the pre-parallel implementation. We preserved that property in code (single-threaded fast path in `parallel_generate_table` uses the whole row range with the base seed), but there's no regression test proving it.

Add a test that generates SF0.01 TPC-H at `threads=1`, computes SHA-256 of each resulting parquet file (sorted by path), and asserts the hashes match a committed golden value. Re-run the test after any change to the generators, RNG usage, or parquet writer properties.

**Files:**

- `crates/sqe-bench/tests/generate_deterministic.rs` (new)
- `crates/sqe-bench/tests/golden/tpch-sf0_01-threads1-zstd3.sha256` (new, committed golden)

**Acceptance:**

- Test passes with the golden file committed against the current `HEAD`.
- Test fails if someone reorders RNG calls, changes batch boundaries, or modifies the parquet writer properties without regenerating the golden.

## 3. Cross-table parallelism for benchmarks with many small tables

**Title:** Generate multiple small tables in parallel (TPC-DS, TPC-E)

**Body:**

Current flow: `main.rs` iterates `for table_def in gen.tables() { gen.generate_table(...) }` serially across tables. TPC-H has only 8 tables, one of which (lineitem) dominates, so cross-table parallelism adds almost nothing. TPC-DS has 24 tables, most small; running them in parallel could halve total generate time even if each table is already internally parallel.

Care required: per-CPU saturation. If each of 24 tables uses 32 threads for internal parallelism, we'd oversubscribe 768-way. Need to either:
- Share a global thread budget via an `Arc<Semaphore>` and have each table claim partitions.
- Or run small tables in parallel at `threads=1` each and still reserve the full budget for the big tables.

**Files:**

- `crates/sqe-bench/src/main.rs` (Command::Generate dispatch)
- `crates/sqe-bench/src/generate/mod.rs` (possibly a new cross-table scheduler)

**Acceptance:**

- Total TPC-DS SF100 generate time drops by 2x or more vs. serial-table-iteration.
- Thread count stays bounded at `BENCH_GEN_THREADS` total across all concurrent tables.

## 4. ChaCha8Rng with `set_word_pos` for byte-identical parallel output

**Title:** Preserve byte-identical parallel generator output via jumpable RNG

**Body:**

At `BENCH_GEN_THREADS > 1`, row values differ from what the serial code would have produced at the same row offset. Each partition uses a different seed, so the RNG state at row i under 4 threads differs from the state at row i under 1 thread. The row set is the same but the values are not.

For most users this doesn't matter (queries are order-insensitive). For anyone building a regression test that pins specific row values, or comparing against reference TPC-H `dbgen` output, byte-identical parallel output is required.

Switch from `StdRng` (ChaCha12 under the hood) to `ChaCha8Rng`, which supports `set_word_pos` for O(log n) fast-forward. Each partition seeds the same base RNG, then fast-forwards to its starting offset times the per-row call count. Requires auditing each generator to count "RNG calls per row" exactly, which is fragile.

Alternative: use `rand_chacha::ChaCha8Rng::seed_from_u64(base)` directly and expose `set_stream(partition_idx)` instead of `set_word_pos`. Neumann & Kemper-style "counter-mode" generator where partition_idx is the stream id; byte-identical only requires stream=0 to match the pre-change path.

**Files:**

- `crates/sqe-bench/src/generate/tpch.rs` (seed derivation)
- `crates/sqe-bench/src/generate/config.rs` (`seed_for_table_partition`)
- `Cargo.toml` (add `rand_chacha`)

**Acceptance:**

- `threads=N` for any `N` produces byte-identical parquet output to `threads=1` for every TPC-H table.
- Test 2 above passes regardless of thread count.
