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

1. **`RowSink` trait** at the `parallel_generate_table` seam, with two impls:
   - `ParquetStagingSink` — the current behavior, extracted from
     `parallel_generate_table` unchanged.
   - `IcebergDirectSink` — for each partition shard, write a `DataFile` via the
     `DataFileWriterBuilder` machinery already in `sink/iceberg.rs`, collect the
     `DataFile`s, and commit one `fast_append` per table.
2. **`parallel_generate_table` takes a sink** (a `RowSink`; dynamic vs. generic
   dispatch decided in planning, see the trait note below) instead of an
   `output_dir` string. The 8 generators forward the sink through
   `generate_table` unchanged; their row-production logic is untouched.
3. **`--sink iceberg` covers all generator benchmarks.** Table creation derives
   the Arrow schema from `TableDef.schema`; unpartitioned bench tables get
   `PartitionSpec::unpartitioned` (the bench tables already load unpartitioned).
4. **Per-table resume.** Each committed table is marked with a
   `sqe-bench.table.<name> = done` table property. A re-run skips completed
   tables. This mirrors the bank's durability idea at table granularity, which
   fits the unpartitioned tables.
5. **Bank keeps `run_bank`** for now. Its per-day partitioning + per-day resume
   are genuinely bank-specific. Folding bank onto the generic sink (as a
   day-partition strategy) is a later, optional step and is out of scope here.

## Non-goals

- Not changing any generator's row-generation logic, seeds, or determinism.
- Not partitioning the analytic bench tables on write (they load unpartitioned
  today; partition-on-write is a separate open item).
- Not refactoring `run_bank` onto the generic sink in this change.
- Not touching the query/load/compare flows beyond adding the sink route.

## Architecture

### The seam

`parallel_generate_table` already abstracts "produce RecordBatches for a
disjoint row range, per partition, deterministically." Today it hardcodes the
Parquet writer. Replace the `output_dir: &str` parameter with a sink:

```rust
pub trait RowSink: Sync {
    /// Consume one partition's batches. `part_idx` gives the shard a unique
    /// identity (file prefix for staging; data-file naming for Iceberg).
    fn write_partition(
        &self,
        table: &str,
        schema: SchemaRef,
        part_idx: usize,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> anyhow::Result<PartitionStats>;

    /// Called once after all partitions for a table complete. Staging sink:
    /// no-op. Iceberg sink: commit the collected DataFiles as one fast_append
    /// and set the resume marker.
    fn finish_table(&self, table: &str, schema: SchemaRef) -> anyhow::Result<()>;
}
```

(The exact trait shape is refined during planning; object-safety vs. generic
dispatch and how partition `DataFile`s are collected across threads are
implementation details. The key contract is: per-partition write + per-table
finish/commit.)

### ParquetStagingSink

Wraps the current `parquet_writer::write_parquet_stream` call and the
`{part_idx:04}` prefixing. `finish_table` is a no-op. Byte-for-byte identical
output to today, preserving the single-thread determinism fast path.

### IcebergDirectSink

Reuses the writer machinery from `sink/iceberg.rs`:

- Holds the `IcebergTarget` (catalog URI, warehouse, namespace, OAuth2 / bearer,
  S3 settings) and a shared catalog handle.
- On first `write_partition` for a table, ensure the table exists (create from
  `TableDef.schema`, unpartitioned) unless the resume marker says it is done.
- `write_partition`: build a `DataFileWriterBuilder` +
  `RollingFileWriterBuilder` + `ParquetWriterBuilder` (as `write_unit` does
  today), write the batches, close, return the `DataFile`s.
- Collect `DataFile`s per table across partitions (a `Mutex<Vec<DataFile>>` or a
  per-table channel).
- `finish_table`: one `Transaction::fast_append().add_data_files(files)`, set the
  `sqe-bench.table.<name> = done` property, commit.

Concurrency is bounded exactly as bank's sink is: a semaphore sized to the
configured thread count so peak memory stays `permits x (batch + row-group
buffer + upload buffer)`.

### Resume

Before generating a table, read its table properties. If
`sqe-bench.table.<name> = done`, skip it. This makes a re-run after a mid-way
failure cheap and idempotent at table granularity. Unlike bank's per-day
markers, there is no intra-table resume: a partially-written table is
re-created (drop + recreate, or overwrite) so a crashed table restarts clean.

## CLI

`sqe-bench generate --benchmark <name> --sink iceberg [iceberg target flags]`
routes through `IcebergDirectSink`. The default sink stays `parquet`
(`ParquetStagingSink`) so existing generate + load flows are unchanged. The
Iceberg target flags reuse the ones `run_bank` already defines (catalog URI,
warehouse, namespace, credential/bearer, S3 endpoint/keys/region/path-style).

## Error handling

- Table-create failure -> abort that table with catalog context.
- Partition write failure -> propagate; the table is left uncommitted (no
  `fast_append`), so no partial snapshot is visible. Resume re-runs the table.
- Commit (`fast_append`) failure -> typed error; the resume marker is set only
  inside the committed transaction, so a failed commit leaves the table
  un-marked and re-runnable.

## Testing

- Unit: `RowSink` contract with a fake sink; assert `parallel_generate_table`
  calls `write_partition` once per partition and `finish_table` once per table,
  with disjoint ranges and deterministic seeds preserved.
- Unit: `ParquetStagingSink` produces byte-identical output to the pre-refactor
  path for a small table at `threads = 1` and `threads > 1`.
- Integration (stack-gated, Polaris + S3): generate tpch SF-small with
  `--sink iceberg`, then query the resulting Iceberg tables and assert row
  counts match the generator's expected counts; re-run and assert completed
  tables are skipped via the resume marker.

The stack-gated integration test needs the Polaris + S3 quickstart and will
contend with perf runs; it is documented as the pre-merge validation, not part
of the unit loop.

## Files

- `crates/sqe-bench/src/generate/mod.rs` — define `RowSink`; change
  `parallel_generate_table` to take a sink; thread it through
  `BenchmarkGenerator::generate_table`.
- `crates/sqe-bench/src/generate/parquet_writer.rs` — `ParquetStagingSink` impl
  wrapping `write_parquet_stream`.
- `crates/sqe-bench/src/sink/iceberg.rs` — extract the reusable writer/commit
  machinery from `run_bank` into helpers; add `IcebergDirectSink`.
- `crates/sqe-bench/src/generate/{tpch,tpcds,ssb,clickbench,tpcbb,tpcc,tpce}.rs`
  — forward the sink through `generate_table` (mechanical).
- `crates/sqe-bench/src/cli.rs` + `main.rs` — `--sink iceberg` for the generate
  subcommand across all benchmarks.

## Rollback

Additive and default-off. The default sink remains Parquet staging, so existing
generate + load + compare flows are byte-identical. Reverting restores the
prior seam. No data migration.

## Sequencing note

This is the second of two designs brainstormed together (the other is
`read_json` compression/formats). They are independent and land as separate
MRs. Implementation order is interchangeable.
