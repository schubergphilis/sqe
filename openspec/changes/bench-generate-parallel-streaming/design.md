## Context

`sqe-bench generate <benchmark> --scale N` produces parquet files that the `sqe-bench load` step ingests into Iceberg. At low scale factors (SF1, SF10) the generator is fast enough that its serial-ness is hidden by everything else in the pipeline (Iceberg commit, network round trips, compaction). At SF1000 the generator dominates: 208.5 s for lineitem alone, with another 31 CPUs sitting idle.

Code path today (citations from the repo at this change's base):

- `crates/sqe-bench/src/main.rs:31-50`: `Command::Generate` matches; calls `get_generator(&benchmark)` then `gen.generate_table(...)` in a serial loop over tables.
- `crates/sqe-bench/src/generate/mod.rs:37-47`: `BenchmarkGenerator::generate_table(&self, table: &str, scale: f64, output_dir: &str) -> Result<GenerateStats>`.
- `crates/sqe-bench/src/generate/tpch.rs:667-763`: `generate_lineitem(scale)` returns `(SchemaRef, Vec<RecordBatch>)` built by a single `while offset < total` loop.
- `crates/sqe-bench/src/generate/parquet_writer.rs:18-66`: `write_parquet_files(batches: &[RecordBatch], ...)` iterates the slice through a single `ArrowWriter`, rotating files at `MAX_FILE_BYTES = 128 MiB`.

Observed at SF1000 lineitem on a 32-CPU / 512 GiB box: 90.9% CPU (one core), 28.7 GiB RSS, 208.5 s wall clock, 154 output files.

## Goals / Non-Goals

**Goals:**

- Wall clock scales with `total_rows / num_cpus` for CPU-bound stages (RNG, Arrow array build, ZSTD compression).
- Peak memory scales with `batch_size × num_threads` instead of `total_rows`.
- Deterministic row-set (but not row-order) regardless of thread count. At `BENCH_GEN_THREADS=1`, byte-identical output to the pre-change implementation.
- Apply to all seven generators: TPC-H, TPC-DS, SSB, TPC-C, TPC-E, TPC-BB, ClickBench. Shared infrastructure (streaming writer, thread pool, config) lives in `generate/mod.rs` and `generate/parquet_writer.rs`.

**Non-Goals:**

- Switch to an external data generator (dbgen, DuckDB `CALL dbgen`, `tpchgen-rs`). Staying in-tree keeps the generator deterministic against our test expectations and avoids a build-tool dep.
- Distribute generation across machines. Single-machine multicore is sufficient for SF10000 on a 512 GiB box.
- Cross-table parallelism (generate lineitem and orders concurrently). Tables have loose ordering dependencies in the TPC-H spec (orders totals derive from lineitem in some generators). We parallelise *within* a table, which is always safe.
- Change the parquet file layout, compression default, or schema. Output is byte-identical at `BENCH_GEN_THREADS=1`.

## Architecture

Before:

```
Generate command
  for table in tables:
    (schema, batches: Vec<RecordBatch>) = generate_<table>(scale)   <- all in memory
    write_parquet_files(&batches, schema, dir, name)                <- single writer
```

After:

```
Generate command
  threads = env("BENCH_GEN_THREADS") or num_cpus()
  for table in tables:
    total_rows = row_count(scale)
    ranges = partition(total_rows, threads)     # [0..N/T, N/T..2N/T, ...]

    rayon::scope(|s| {
        for (part_idx, range) in ranges:
            s.spawn(|_| {
                seed = seed_for_table_partition(table, part_idx)
                iter = generate_<table>_range(range, seed)    <- returns impl Iterator<Item=RecordBatch>
                write_parquet_stream(iter, schema, dir, format!("{part_idx:04}"))
                                                              <- one writer per partition, rotates at 128 MiB
            });
    })

    aggregate stats from each partition
```

Key shape changes:

- **Row generation returns an iterator.** `generate_lineitem_range(range, seed) -> impl Iterator<Item = RecordBatch>` instead of `-> Vec<RecordBatch>`. Implemented with a `struct LineitemBatchIter { rng, offset, end, batch_size, ... }` that builds one batch at a time in `next()`.
- **One parquet writer per partition.** Each rayon worker owns its own writer, writes to its own file prefix (`00000.parquet`, `00001.parquet`, etc. namespaced by partition). File count becomes `threads × ceil(partition_rows × row_bytes / MAX_FILE_BYTES)`.
- **`rayon::scope` for threading.** Deterministic completion (scope blocks until all spawned tasks finish), no tokio runtime needed (generation is pure CPU), no unsafe cross-thread shared state.

## Key Design Decisions

### Why iterators, not channels

An `impl Iterator<Item = RecordBatch>` with `next()` building one batch per call has the exact memory profile we want: one batch in flight per partition. A channel + producer thread would add a thread-per-partition just for generation and a separate thread-per-partition for writing, doubling the thread count with no throughput win when the producer is strictly faster than the consumer (ZSTD compression dominates). Iterators keep generation and writing on the same rayon worker thread.

### Partitioning scheme and determinism

Each partition `i` gets a seed `seed_for_table("lineitem") ^ (i as u64).wrapping_mul(GOLDEN_RATIO)`. With a different seed per partition, the generated row *values* differ from what the serial code would have produced at the same row offset. The row *set*, however, is deterministic: given the same `(table, scale, threads)` tuple, every run produces the same set of rows in the same file layout.

This is a behavioural change the proposal accepts. The existing expected-results files in `benchmarks/queries/` test *query results over the generated data*, not individual generator row values, and TPC-H queries are order-insensitive on the base tables. The determinism regression gate at `BENCH_GEN_THREADS=1` keeps the old behaviour available exactly.

### File layout

Current: one writer rotates at 128 MiB, producing `00000.parquet`, `00001.parquet`, etc. in `{output_dir}/{table_name}/`.

New: each partition gets its own file prefix. Two schemes considered:

- **Scheme A (chosen): flat numbering across partitions.** Partition 0 writes `00000.parquet`, `00001.parquet`, ...; partition 1 writes `01000.parquet`, `01001.parquet`, .... A fixed stride per partition lets each worker rotate independently without coordination. Drawback: gaps in the numbering (partition 0 might only produce 2 files, so `00002.parquet` through `00999.parquet` don't exist).
- **Scheme B (rejected): atomic counter shared across partitions.** Workers pull file indices from a `AtomicU64` at rotation time. Clean contiguous numbering but adds synchronisation cost and makes rotation non-deterministic.

Scheme A's gaps are harmless: `sqe-bench load` uses glob `*.parquet` and doesn't depend on contiguous numbering.

### Compression config

Default stays ZSTD(3). The new `BENCH_GEN_COMPRESSION` env var accepts:

- `zstd1`: 3× faster compression, ~10% larger files. Good for "regenerate quickly during iteration".
- `zstd3` (default): current behaviour.
- `zstd9`: 2× slower, ~5% smaller files. For "I'm going to reuse this dataset many times".
- `snappy`: 2× faster than zstd1, ~30% larger files. Drop-in for TPC-H reference compare runs.
- `none`: no compression. Raw parquet. For profiling when we want to isolate the compression cost.

### Config knob precedence

1. CLI: `sqe-bench generate --threads N --compression X` (new flags).
2. Env: `BENCH_GEN_THREADS=N`, `BENCH_GEN_COMPRESSION=X`.
3. Default: `num_cpus::get()` for threads, `zstd3` for compression.

Matches the precedence convention in the rest of the codebase (CLI > env > default).

### Parquet writer still rotates at 128 MiB

Per-partition writers still track `current_bytes` and rotate at `MAX_FILE_BYTES`. That cap is a balance between Iceberg scan planning overhead (too many tiny files = slow manifest reads) and memory per file (too big = slow list_stats(), slow row-group reads). No change.

### Why rayon not tokio

Generation is CPU-bound: RNG math, Vec builds, Arrow array construction, ZSTD compression. No I/O between generator and writer (the writer flushes bytes synchronously inside its `write(batch)` call, which goes to the OS page cache, not a network). Rayon's work-stealing scheduler is the canonical fit. Tokio would introduce overhead (reactor wake-ups, task state machines) with no async-awaitable work to justify it.

Rayon is already a transitive dep via DataFusion, so no new crate dependency surface.

### What we do NOT change

- Table schemas: untouched.
- Row count formulas: `scaled(scale, base)` functions unchanged.
- RNG algorithm: still `StdRng` (ChaCha12 under the hood). Could switch to a jump-able RNG (`rand_chacha::ChaCha8Rng` with `set_word_pos`) to preserve byte-identical output per row index regardless of threading, but that's a separate change. The current design accepts non-identical values at `BENCH_GEN_THREADS > 1` in exchange for simplicity.
- `sqe-bench load`: already accepts a directory of parquet files. Works unchanged against the new file layout.

## Rust Shapes

Trait change:

```rust
pub trait BenchmarkGenerator: Send + Sync {
    fn name(&self) -> &str;
    fn tables(&self) -> Vec<TableDef>;

    /// Generate one table, optionally across multiple threads. Returns stats
    /// aggregated across all partitions. Callers pass `threads` from CLI/env;
    /// `threads = 1` produces byte-identical output to the pre-change impl.
    fn generate_table(
        &self,
        table: &str,
        scale: f64,
        output_dir: &str,
        config: GenerateConfig,
    ) -> anyhow::Result<GenerateStats>;
}

pub struct GenerateConfig {
    pub threads: usize,               // default num_cpus::get()
    pub compression: CompressionKind, // default Zstd(3)
    pub row_group_size: Option<usize>,
}
```

Per-table function split:

```rust
// Old shape:
fn generate_lineitem(scale: f64) -> (SchemaRef, Vec<RecordBatch>);

// New shape:
fn lineitem_schema() -> SchemaRef;

fn generate_lineitem_range(
    range: std::ops::Range<usize>,
    scale_factor: f64,
    seed: u64,
) -> impl Iterator<Item = RecordBatch>;
```

Streaming writer:

```rust
pub fn write_parquet_stream<I>(
    batches: I,
    schema: SchemaRef,
    output_dir: &str,
    file_prefix: &str,     // e.g. "00000" for partition 0
    config: &GenerateConfig,
) -> anyhow::Result<(usize, u64)>
where
    I: Iterator<Item = RecordBatch>,
{
    // Same rotate-at-128MiB logic as today, but reads from an iterator
    // and uses `file_prefix` plus a per-partition file index to name files.
}
```

Dispatch helper in `mod.rs`:

```rust
pub fn parallel_generate_table<G, T>(
    table_name: &str,
    schema: SchemaRef,
    total_rows: usize,
    gen_range: G,
    output_dir: &str,
    config: &GenerateConfig,
) -> anyhow::Result<GenerateStats>
where
    G: Fn(Range<usize>, u64) -> T + Sync + Send,
    T: Iterator<Item = RecordBatch> + Send,
{
    let ranges = partition(total_rows, config.threads);
    rayon::scope(|s| {
        for (part_idx, range) in ranges.into_iter().enumerate() {
            s.spawn(|_| {
                let seed = seed_for_table_partition(table_name, part_idx);
                let iter = gen_range(range, seed);
                write_parquet_stream(iter, schema.clone(), output_dir,
                                     &format!("{part_idx:04}"), config)
            });
        }
    });
    // aggregate stats from scope results
    Ok(stats)
}
```

Each table's `impl BenchmarkGenerator::generate_table` becomes a two-liner: compute `total_rows`, call `parallel_generate_table` with that table's `gen_range` function.

## Data Flow Example

TPC-H lineitem at SF1000 with `BENCH_GEN_THREADS=32`:

```
total_rows = 150_000_000
ranges = [0..4687500, 4687500..9375000, ..., 145312500..150000000]

t0: rayon spawns 32 workers, one per range
t0: each worker builds its LineitemBatchIter with its own RNG seed
t0+r: workers yield batches from their iterators;
      each batch goes through its worker's ArrowWriter;
      ArrowWriter rotates at 128 MiB into files named
      "{part_idx:04}{file_idx:05}.parquet"
...
t0+T: rayon::scope blocks until all 32 workers finish
t0+T: aggregate stats (sum of rows, bytes, files) returned to caller
```

Expected on 32-CPU box: `T ≈ 208.5 / 32 ≈ 6.5 s`. Realistic target accounting for rayon overhead and non-uniform partition size: 10-15 s. Peak RSS: ~32 × 128 MiB (one writer's rotation buffer per partition) + 32 × batch_size (in-flight generation) ≈ 5 GiB. Comfortable on 512 GiB.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Non-identical row values at `BENCH_GEN_THREADS > 1` break a test that pins specific values | All benchmark tests compare query results over generated data (order-insensitive). No tests today pin raw row values. `BENCH_GEN_THREADS=1` gate preserves byte-identical output for anyone who needs it. |
| Inter-table FK consistency (TPC-C, TPC-E where orders reference lineitems) | TPC-C/TPC-E generators produce their own inter-table keys deterministically from per-row state and a shared seed space. The parallelisation is per-table, so FK relationships within one table are preserved. Cross-table relationships are already computed by the existing deterministic formulas. |
| ZSTD compression becomes memory-bound under 32-way parallelism | Each writer's ZSTD working set is ~2-8 MiB. 32 × 8 MiB = 256 MiB. Well under any realistic RAM. |
| rayon::scope panics leave partial files | Partial parquet files are uncloseable; `ArrowWriter::close` failure is reported per partition. Caller logic should delete the output dir on error. Existing `generate` command already has a "clean on failure" pattern; extend to cover parallel partial output. |
| 32 concurrent fs::File::create calls stress the local fs | macOS APFS and Linux ext4/xfs handle 32 concurrent creates fine. S3-backed file systems (RustFS, MinIO in tests) may throttle; if observed, we can serialise `fs::File::create` via a small `Mutex<()>` without losing meaningful parallelism. |
| File count inflation (32 partitions × several files each = 500+ files) | TPC-H SF1000 today already produces 154 files. Roughly 3-4x more is acceptable for Iceberg scan planning at this scale; if problematic, we can merge small partition tails post-hoc. Out of scope for this change. |
| `BENCH_GEN_COMPRESSION=none` produces huge datasets that fill the disk | Documented. Defaults unchanged. Operators opting in to `none` know what they're doing. |

## Open Questions

**Q1: Should we switch `StdRng` to `ChaCha8Rng` with `set_word_pos` to preserve byte-identical output across thread counts?**

Deferred. The jump-ahead math is one more thing that can go subtly wrong, and it's not required by any test today. Revisit if a future contributor wants byte-identical parallel output.

**Q2: Should `sqe-bench generate` also parallelise across tables?**

Partial. Tables within a benchmark are small compared to lineitem. Parallelising tables would help the "many small tables" case (TPC-DS has 24 tables) but is orthogonal to the big-table wall clock problem. Proposal as follow-up in `tasks.md` cleanup phase.

**Q3: Row-group size tuning?**

`BENCH_GEN_ROW_GROUP_SIZE` is exposed as a knob but the default is unchanged. Optimal row-group size is workload-dependent (read patterns, predicate selectivity). Tuning that is a separate investigation; the knob is there so operators can experiment without a rebuild.
