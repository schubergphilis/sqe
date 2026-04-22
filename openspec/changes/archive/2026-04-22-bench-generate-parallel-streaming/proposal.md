## Why

`sqe-bench generate tpch --scale 1000` takes 208.5 s and holds 28.7 GiB RSS to produce 150M lineitem rows into 154 parquet files on a 32-CPU / 512 GiB RAM box. Observed via `top`:

```
PID  USER     %CPU  %MEM  RES    COMMAND
86839 jacob   90.9   5.7  28.7g  sqe-bench
```

Two problems, same shape:

1. **Single-threaded generator.** `crates/sqe-bench/src/generate/tpch.rs` functions like `generate_lineitem` (line 667-763) are a single `while offset < total` loop feeding `batches.push(...)`. On a 32-CPU box, 31 cores idle. Wall clock scales linearly with data size even when spare compute exists.

2. **Whole-table materialisation before writing.** Each generator returns `(SchemaRef, Vec<RecordBatch>)`. The caller holds every batch in memory, then `parquet_writer::write_parquet_files` (line 18-66) iterates over the vec. At SF1000 lineitem: 150M rows × Arrow overhead = ~28 GiB RSS. At SF10000 this OOMs a 512 GiB box.

Both problems are in the generator layer, not the benchmark harness, not the writer. The writer itself already rotates files at 128 MiB and uses one `ArrowWriter` per file; it just can't do that in parallel because it receives a pre-materialised `Vec` from a single thread.

## What Changes

Three compositional fixes, one MR:

- **A: Stream batches from generators.** Change the generator return type from `Vec<RecordBatch>` to a batch-producing iterator (or channel). The writer consumes as batches arrive. RSS drops from whole-table to a few batches in flight.

- **B: Partition row ranges across CPUs.** Add `generate_<table>_range(start, end, seed)` functions that produce rows for a deterministic slice. Spawn N worker threads via `rayon`; each owns its own row range and its own output file(s). Wall clock drops close to `total / N` because ZSTD compression (the dominant cost) is embarrassingly parallel.

- **C: Surface knobs.** Config precedence: `BENCH_GEN_THREADS` env var (default `num_cpus::get()`), `BENCH_GEN_COMPRESSION` (`zstd3` default, `zstd1`, `snappy`, `none`), `BENCH_GEN_ROW_GROUP_SIZE` (default current). A `sqe-bench generate --threads N --compression X` CLI alternative. Let operators tune for throughput vs. size.

## Capabilities

### Modified Capabilities

- `bench-generate`: TPC-H, TPC-DS, SSB, TPC-C, TPC-E, TPC-BB, and ClickBench generators produce data in parallel across all available CPUs. Memory for in-flight batches is bounded regardless of scale factor.

## Impact

- `crates/sqe-bench/src/generate/mod.rs`: `BenchmarkGenerator` trait signature changes to accept a thread count and return streaming stats. New `RowRange` helper struct.
- `crates/sqe-bench/src/generate/tpch.rs` and the other six generator modules: refactor `generate_<table>` functions into `generate_<table>_range(start, end, seed) -> impl Iterator<Item = RecordBatch>`.
- `crates/sqe-bench/src/generate/parquet_writer.rs`: new `write_parquet_stream` that consumes an iterator and rotates files as today. Old `write_parquet_files` kept as a thin wrapper for any non-parallel callers.
- `crates/sqe-bench/Cargo.toml`: add `rayon = "1.10"` (workspace dep likely already available via DataFusion).
- `crates/sqe-bench/src/cli.rs` + `src/main.rs`: add `--threads` and `--compression` flags to the `generate` subcommand.
- No breaking change for end users: defaults match current behaviour for correctness tests (single-threaded deterministic when `BENCH_GEN_THREADS=1`), parallel when unset.

Performance target (SF1000 lineitem, 32-CPU box):

| metric | current | target |
|---|---|---|
| wall clock | 208.5 s | under 15 s |
| peak RSS | 28.7 GiB | under 2 GiB |
| output files | 154 | 32-200 (parallelism-dependent) |
| output bytes | N (zstd3) | N (unchanged) |

## Success Criteria

1. SF1 TPC-H generate produces the same *set* of rows as the pre-change implementation. Query results on the generated data match the existing expected-results files (order-insensitive).
2. SF1000 TPC-H lineitem generate completes under 20 s on a 32-CPU box (>10x wall-clock improvement over 208.5 s).
3. Peak RSS during SF1000 lineitem generate stays under 4 GiB on a 32-CPU box, verified via `/usr/bin/time -v` or `memusg`.
4. `BENCH_GEN_THREADS=1` produces byte-identical output to the pre-change implementation (determinism regression gate).
5. `cargo test -p sqe-bench` passes.
6. `cargo clippy -p sqe-bench --all-targets --all-features -- -D warnings` clean.
7. New integration test `generate_parallel_matches_serial` asserts `BENCH_GEN_THREADS=N` and `BENCH_GEN_THREADS=1` produce the same row set (modulo order).

## Rollback

Gated by `BENCH_GEN_THREADS`. Setting `BENCH_GEN_THREADS=1` restores exactly the old serial behaviour at runtime. Reverting the commit is a one-PR revert with no data migration, no API changes to other crates, no on-disk format changes.

If a production regression surfaces on one generator (e.g., TPC-E where inter-table foreign keys matter more than in TPC-H), the operator can set `BENCH_GEN_THREADS=1` without a redeploy. Existing benchmark JSONs in `benchmarks/results/` are unaffected because they compare query outputs, not raw generator outputs.
