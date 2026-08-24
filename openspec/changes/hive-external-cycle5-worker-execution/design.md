# Cycle 5: Hive scan execution on workers

## The three gaps, measured

Read `crates/sqe-planner/src/scan_task.rs`, `crates/sqe-worker/src/executor.rs`, and `crates/sqe-catalog/src/hive/provider.rs` before changing any of them. What is written below is what those files do today, not what the roadmap says.

### 1. Format: the worker only reads Parquet

`execute_scan` and `open_parquet_stream` construct readers exclusively through `ParquetRecordBatchStreamBuilder`. There is no CSV or JSON path. `ScanTask` carries no format field because it never needed one.

Hive tables are CSV, line-delimited JSON, or Parquet, resolved by `serde_map::resolve_format` into a `ResolvedFormat` carrying delimiter, quote, escape, and header flags. Those parameters live in the manifest and would have to reach the worker.

### 2. Projection: field IDs vs names

`projected_field_ids` is resolved against `PARQUET:field_id`. A Hive table has none. `projected_columns` is already the documented fallback ("used when the parquet file lacks PARQUET:field_id metadata"), so a Hive task simply leaves the field-ID vector empty. This gap is nearly free and is listed only so it is not mistaken for work.

### 3. Partition values: the actual obstacle

`attach_partition_values` (provider.rs) builds a `ProjectionExec` per partition. For each column of the output schema it emits either a column reference (index `< n_file`, the column is in the data file) or a **literal** (index `>= n_file`, the column is directory-encoded), typed via `partition_scalar` and defaulting to `__HIVE_DEFAULT_PARTITION__` when the key is absent.

Three consequences for the wire format:

- The values are **per partition**, so a task must not span partitions unless it carries a value map per file.
- The output schema is `file_schema` ++ `partition_columns`, and the split index `n_file` is load-bearing. The worker needs both, not just a column list.
- `partition_scalar` types the literal from the *field's* data type, with a string fallback. That typing must be identical on both paths or a partition column silently changes type between coordinator-local and distributed execution.

## Wire format

`ScanTask` v3 adds one optional field:

```rust
/// Present only for a Hive external table scan. Absent (None) on every
/// Iceberg task, so an Iceberg ticket is byte-identical to v2.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub hive: Option<HiveScanDescriptor>,
```

```rust
pub struct HiveScanDescriptor {
    /// csv | json | parquet.
    pub format: HiveFileFormat,
    /// SerDe parameters as resolved from the manifest. Ignored for parquet.
    pub delimiter: u8,
    pub quote: Option<u8>,
    pub escape: Option<u8>,
    pub has_header: bool,
    /// Directory-encoded partition values for this task's files.
    pub partition_values: BTreeMap<String, String>,
    /// Declared partition key order. `partition_values` is a map and a map has
    /// no order; the projection indexes by position, so the order must be
    /// carried explicitly rather than inferred from iteration.
    pub partition_columns: Vec<String>,
    /// Arrow schema of the DATA FILE, serialized. The worker needs the split
    /// point between file and partition columns, and needs the field types to
    /// reproduce `partition_scalar` exactly.
    pub file_schema_json: String,
}
```

`skip_serializing_if` is what keeps the Iceberg path untouched, and a serialization test asserts that rather than trusting it.

## Why not reuse `DistributedScanExec` unchanged

`DistributedScanExec` already ships tasks and collects results, and it should keep doing so. The change is confined to what a task *contains* and how a worker *opens* it. The alternative considered and rejected was a second exec node for Hive: it would duplicate the retry, credential-refresh, and worker-registry logic that `DistributedScanExec` already carries, and those are the parts most expensive to get wrong twice.

## Fallback contract

A worker cannot serve every Hive table on day one. The fallback must be **named and observable**, never silent:

- The coordinator decides at planning time whether a table is worker-servable.
- A table that is not runs coordinator-local, exactly as today.
- The decision is recorded so `EXPLAIN` and metrics can show it.

A silent fallback is the failure mode to design against, because it turns a performance feature into an invisible one: the query still works, so nobody notices it never distributed.

## Correctness risk, and the test that closes it

The dangerous outcome is not a crash. It is a worker producing *different rows* than coordinator-local execution: a mistyped partition literal, a CSV header counted as data, a column order swap between file and partition columns.

So the acceptance test is differential rather than golden: run the same query both ways and compare, per format, including a `__HIVE_DEFAULT_PARTITION__` partition and a non-string partition key. A golden file would encode whichever path was run first, including its bugs.

## Sequencing

Parquet first: it is the format the worker already reads, so it isolates the partition-materialization problem from the reader problem. CSV and JSON follow, and they are where the SerDe parameters start to matter.
