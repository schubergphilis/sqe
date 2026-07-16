# Generic direct-to-Iceberg sink for benchmark generators

## Summary

Generalize the bank benchmark's direct-to-Iceberg data sink so every
generator-based benchmark (tpch, tpcds, ssb, clickbench, tpcbb, tpcc, tpce) can
write generated data straight into Iceberg tables through the catalog REST API,
skipping the Parquet-staging + engine-load round trip. Introduce a `RowSink`
abstraction at the existing `parallel_generate_table` seam so the generators
stay unchanged in their row-production logic and gain the direct sink by
injection.

## Motivation

Two data paths exist today and they do not share code:

- **Every generator** implements `BenchmarkGenerator::generate_table`, which
  drives `parallel_generate_table(gen_range, ...)`. `gen_range: Fn(Range, seed)
  -> Iterator<RecordBatch>` is the universal row source. That function currently
  hardcodes `parquet_writer::write_parquet_stream` (Parquet staging). A separate
  `sqe-bench load` step then ingests those Parquet files into Iceberg *through
  the engine*, writing every byte twice.
- **Bank** bypasses all of that with `sink::iceberg::run_bank`: it writes
  partition-aligned Parquet straight to the table's storage location and commits
  with `fast_append`, one snapshot per trading day, resumable via snapshot /
  table-property markers. Every byte is written once and the engine stays out of
  the loop.

The bank path is strictly better for load throughput, but it is welded to the
bank schema and its per-day partition model. The other benchmarks cannot use it.

## What changes

> **Architecture correction (2026-07-16).** An earlier draft of this spec
> proposed a `RowSink` trait injected at the `parallel_generate_table` seam.
> Investigation killed that approach for two reasons: (1) only 2 of 8 generators
> (bank, tpch) route through `parallel_generate_table` at all; the other six
> (ssb, tpcds, tpcc, tpce, tpcbb, clickbench) build a `Vec<RecordBatch>` and call
> `write_parquet_files` directly. (2) The generation framework is
> synchronous/thread-scoped, but the Iceberg writer is async/tokio; embedding an
> async write inside a sync sink would run in two incompatible contexts (tokio
> workers for the Vec generators, plain OS threads for tpch/bank's
> `thread::scope`), and no single `block_on` bridge is legal in both. The
> corrected design below mirrors `run_bank`: an async driver fed by a batch
> producer, with the Parquet staging path left completely untouched.

1. **A `batch_source` on each generator.** Add
   `fn generate_batches(&self, table: &str, scale: f64, config: &GenerateConfig)
   -> anyhow::Result<BatchSource>` to `BenchmarkGenerator`, where `BatchSource`
   yields `RecordBatch`es (plus the table's `SchemaRef`). For the six Vec-based
   generators this extracts the batch-building already inside `generate_table`;
   for tpch/bank it wraps the existing `gen_range` iterators. The existing
   `generate_table`, `write_parquet_files`, and `parallel_generate_table` are
   **left unchanged** — the Iceberg sink is a strictly additive parallel path,
   so the working staging flow carries zero regression risk.
2. **A generic `IcebergDirectSink` async driver**, extracted from `run_bank`'s
   reusable pieces (`write_unit`, `commit_files`, the semaphore/task pattern,
   table-create-from-arrow-schema). It pulls batches from a `BatchSource`
   (producer on `spawn_blocking`, bounded channel to the async writer), writes
   `DataFile`s, and commits one `fast_append` per table. Every sync/async
   boundary lives inside the driver's control, exactly as `run_bank` has today.
3. **`--sink iceberg` covers all generator benchmarks.** Table creation derives
   the Arrow schema from `TableDef.schema` via `arrow_schema_to_schema`,
   unpartitioned (the existing `ensure_tables` else-branch; `write_unit` already
   handles `partition_key = None`).
4. **Per-table resume.** `commit_files`'s day-marker is generalized to an
   arbitrary property key; the driver sets `sqe-bench.table.<name> = done` in the
   same commit transaction. A re-run skips completed tables.
5. **Bank becomes a caller** of the shared driver machinery, keeping its own
   day-partition orchestration on top. `run_bank` is refactored to call the
   extracted helpers rather than owning private copies — not rewritten, not
   duplicated. Its externally observable behavior (day snapshots, per-day resume)
   is unchanged.
6. **Parallelism scope.** Table-level parallelism for all benchmarks;
   shard-parallelism (intra-table range splitting) only where a generator already
   exposes `gen_range` (tpch, bank). The six serial generators stay serial
   per table — tpcds is already serial today, so this is no regression. No
   range-splitting is retrofitted onto the six.

## Non-goals

- Not changing any generator's row-generation logic, seeds, or determinism.
- Not changing `generate_table`, `write_parquet_files`, or
  `parallel_generate_table`. The staging path is untouched.
- Not partitioning the analytic bench tables on write (they load unpartitioned
  today; partition-on-write is a separate open item).
- Not retrofitting intra-table range-splitting onto the six serial generators.
- Not touching the query/load/compare flows beyond adding the sink route.

## Architecture

### The batch source (new generator method)

The one thing all 8 generators share is the ability to produce a table's rows as
Arrow batches. Expose that directly instead of routing through the sync writer:

```rust
/// A table's rows as Arrow batches, plus the table's schema. `shards` is
/// the disjoint (range, seed) work list for generators that support
/// range-splitting (tpch, bank); serial generators return a single shard.
pub struct BatchSource {
    pub schema: SchemaRef,
    pub total_rows: usize,
    pub shards: Vec<BatchShard>,
}

pub struct BatchShard {
    /// A boxed factory producing this shard's batches. Called on a blocking
    /// thread by the driver.
    pub make: Box<dyn FnOnce() -> Box<dyn Iterator<Item = RecordBatch> + Send> + Send>,
}

// Added to BenchmarkGenerator:
fn generate_batches(
    &self,
    table: &str,
    scale: f64,
    config: &GenerateConfig,
) -> anyhow::Result<BatchSource>;
```

For the six Vec-based generators, `generate_batches` reuses the same
batch-building code path `generate_table` uses today (factored so both call it),
returning one shard. For tpch/bank it wraps the existing `gen_range` closures
into N shards using `config::partition` + `config::seed_for_table_partition`,
exactly as `parallel_generate_table` does — so determinism and seeds are
identical to the staging path.

### The driver (extracted from run_bank)

`run_bank`'s guts become reusable, benchmark-agnostic helpers in
`sink/iceberg.rs`:

- `ensure_table(catalog, ns, name, arrow_schema, partition_col: Option<&str>,
  clean) -> TableCtx` — the current `ensure_tables` loop body for one table.
- `write_shard(ctx, table_ctx, file_prefix, batches, partition_key: Option<...>)
  -> Vec<DataFile>` — the current `write_unit`, with the partition key passed in
  rather than derived from a bank day.
- `commit_files(ctx, table, files, extra_props: HashMap<String,String>)` — the
  current `commit_files`, with the day-marker generalized to arbitrary
  properties.

`IcebergDirectSink::run(target, generator, tables, scale, config)`:

1. Build the catalog handle (`RestCatalogBuilder`, as `run_bank` does).
2. For each `TableDef` in `generator.tables()`:
   - Skip if `sqe-bench.table.<name> = done` (resume) unless `clean`.
   - `ensure_table(...)` unpartitioned from `TableDef.schema`.
   - `let src = generator.generate_batches(name, scale, config)?;`
   - Spawn one async task per shard, bounded by a `Semaphore(config.threads)`.
     Each task: `spawn_blocking` the shard's `make()` producer feeding a bounded
     `tokio::sync::mpsc` channel; the async side pulls batches and calls
     `write_shard`. This keeps the sync producer and async writer on their proper
     runtimes — the sync/async boundary the earlier `RowSink` design could not
     satisfy.
   - Join shards, collect `DataFile`s, `commit_files(..., {table-done marker})`.

Peak memory stays bounded by the semaphore and the channel bound, matching
`run_bank`.

### Bank as a caller

`run_bank` keeps its day-grouping and per-day `commit_files` calls, but its
`ensure_tables` / `write_unit` / `commit_files` bodies are replaced by calls to
the shared `ensure_table` / `write_shard` / `commit_files` helpers. Day markers
become the `extra_props` argument. No behavior change.

### Resume

Before generating a table, read its table properties. If
`sqe-bench.table.<name> = done`, skip it. This makes a re-run after a mid-way
failure cheap and idempotent at table granularity. Unlike bank's per-day
markers, there is no intra-table resume: a partially-written table is
re-created (drop + recreate, or overwrite) so a crashed table restarts clean.

## CLI

`sqe-bench generate --benchmark <name> --sink iceberg [iceberg target flags]`
routes through `IcebergDirectSink`. Today `main.rs` hard-errors unless
`benchmark == "bank"` for `--sink iceberg`; that guard is removed and the
non-bank benchmarks dispatch into the generic driver. The default sink stays
`parquet` so existing generate + load flows are unchanged. The Iceberg target
flags already exist on the `Generate` command (catalog URI, warehouse,
namespace, credential/bearer, S3 endpoint/keys/region/path-style, target file
size, clean/resume) and are reused verbatim.

## Error handling

- Table-create failure -> abort that table with catalog context.
- Partition write failure -> propagate; the table is left uncommitted (no
  `fast_append`), so no partial snapshot is visible. Resume re-runs the table.
- Commit (`fast_append`) failure -> typed error; the resume marker is set only
  inside the committed transaction, so a failed commit leaves the table
  un-marked and re-runnable.

## Testing

- Unit: `generate_batches` for each generator yields the same total row count
  and schema as `generate_table` for a small scale, and (tpch/bank) the same
  rows across shards as the single-shard case — determinism preserved.
- Unit: the extracted `ensure_table` / `write_shard` / `commit_files` helpers
  keep `run_bank`'s existing bank tests green (refactor-safety: no behavior
  change).
- Integration (stack-gated, Polaris + S3): generate tpch SF-small with
  `--sink iceberg`, then query the resulting Iceberg tables and assert row
  counts match the generator's expected counts; re-run and assert completed
  tables are skipped via the resume marker.

The stack-gated integration test needs the Polaris + S3 quickstart and will
contend with perf runs; it is documented as the pre-merge validation, not part
of the unit loop.

## Files

- `crates/sqe-bench/src/generate/mod.rs` — add `BatchSource` / `BatchShard`
  types and the `generate_batches` method on `BenchmarkGenerator`. No change to
  `parallel_generate_table` / `write_parquet_files`.
- `crates/sqe-bench/src/generate/{ssb,tpcds,tpcc,tpce,tpcbb,clickbench}.rs` —
  factor each `generate_table`'s batch-building into a shared helper that both
  `generate_table` and `generate_batches` call; `generate_batches` returns one
  shard. `generate_table` itself keeps working unchanged.
- `crates/sqe-bench/src/generate/tpch.rs` and `bank.rs` — implement
  `generate_batches` by wrapping the existing `gen_range` closures into shards.
- `crates/sqe-bench/src/sink/iceberg.rs` — extract `ensure_table`,
  `write_shard`, generalized `commit_files` from `run_bank`; add
  `IcebergDirectSink::run`. Refactor `run_bank` to call the extracted helpers.
- `crates/sqe-bench/src/main.rs` — remove the `benchmark == "bank"` guard on
  `--sink iceberg`; dispatch non-bank benchmarks into `IcebergDirectSink::run`.
- `crates/sqe-bench/src/cli.rs` — update the `--sink` / iceberg-flag doc strings
  (no longer "bank only").

## Rollback

Additive and default-off. The default sink remains Parquet staging, so existing
generate + load + compare flows are byte-identical, and the staging code paths
are literally unchanged. Reverting removes `generate_batches` + the driver. No
data migration.

## Sequencing note

This is the second of two designs brainstormed together (the other is
`read_json` compression/formats). They are independent and land as separate
MRs. Implementation order is interchangeable.
