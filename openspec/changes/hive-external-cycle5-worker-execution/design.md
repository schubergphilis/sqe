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
- The output schema is `file_schema` ++ `partition_columns`, and the split index `n_file` is load-bearing. The worker needs the FULL schema: the first draft of this design said "file schema", which is wrong and was caught while implementing. A partition column is not in the data file, so a file-only schema carries no type for it and the worker would default it to Utf8 -- silently changing an integer partition key's type between execution paths.
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
    /// The FULL schema, serialized: file columns then partition columns.
    /// NOT the file schema -- a partition column is absent from the data file,
    /// so a file-only schema carries no type for it. The split point is
    /// derived as `len() - partition_columns.len()`.
    pub full_schema_json: String,
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

## Where partition materialization must happen on the worker, and why not later

The worker's scan returns `(SchemaRef, ScanBatchStream)`, and
`ScanBatchStream` yields `AccountedBatch = Accounted<RecordBatch>`: a batch
paired with a `BytePermit` sized to that batch.

The obvious integration is to leave the Parquet reader alone and map each batch
through `append_partition_columns` on the way out. It is wrong. Appending
partition columns allocates a full constant array per column per batch, so the
batch grows AFTER its permit was acquired, and the worker then holds more memory
than it reserved. `Accounted::get_mut` makes that easy to do by accident: the
permit stays at the old size and nothing complains.

The size is not marginal either. A Utf8 partition column materializes one string
per row, matching what coordinator-local execution does (`ProjectionExec` over a
literal also produces a full array), so the growth is real on both paths -- but
only the worker path has a permit that would be wrong about it.

So materialization must happen **before the permit is acquired**, inside batch
production, not as a stream transform afterwards. That is a deeper integration
point than task 2 assumed, and it is the reason task 2's function takes a
`RecordBatch` and returns one rather than operating on the stream: the pure
function is correct and reusable, and only its CALL SITE is constrained.

Alternatives considered:

- Grow the permit after the fact. Possible, but it moves an acquire into the
  hot per-batch path and can fail mid-stream, which turns a memory question into
  an error-handling question.
- Materialize partition columns as dictionary or run-end arrays so the growth is
  negligible. Attractive, and it is what several engines do, but the output would
  then differ in ARRAY TYPE from coordinator-local execution, which is exactly
  the equivalence the differential tests exist to protect. It would need the
  coordinator path changed in the same way, in the same change.

## The real obstacle is the plan shape, not the ticket

This design opened by saying `ScanTask` cannot carry a Hive scan, following the
reference page's framing that cycle 5 is blocked "because `ScanTask` today is
shaped for Parquet and Iceberg field IDs". That is true, and it is now fixed,
and it was not the hard part.

The coordinator decides what to distribute by finding ONE node:

```rust
let scan_node = find_iceberg_scan(&plan);
let iceberg_scan = scan_node.downcast_ref::<IcebergScanExec>();
```

`IcebergScanExec` is a single, SQE-owned plan node that knows its table, its
projection and its files. Everything downstream -- `dim_build_swap`,
`explain.rs`, `parallel_probe_scan` -- finds it the same way.

A Hive scan has no such node. `scan_from_entries` emits, per surviving
partition, a `ListingTable` scan wrapped in `attach_partition_values`'
`ProjectionExec` and then `nullify_default_partition_columns`' second
`ProjectionExec`, and unions them:

```
UnionExec
├── ProjectionExec(nullify) -> ProjectionExec(partition values) -> ListingTable scan
└── ProjectionExec(nullify) -> ProjectionExec(partition values) -> ListingTable scan
```

There is nothing to downcast to and nothing carrying the table's identity. So
the remaining work is not "call `build_hive_scan_tasks` from the provider"; it
is giving Hive a scan node.

### Two ways, and only one of them is safe

**Recognize the shape.** Walk the plan for a `UnionExec` whose children all
match projection-over-projection-over-`ListingTable`. Cheap to write and a trap:
it is pattern-matching on the output of two other functions plus whatever
DataFusion's optimizer did to them. Any rule that collapses a single-partition
union, reorders a projection, or fuses the two projections silently stops the
match, and the symptom is not an error -- it is the scan quietly failing to
distribute, which is the exact invisibility `HiveLocalScanExec` was built to
prevent.

**Give Hive its own node.** `HiveScanExec`, emitted by `HiveTableProvider`,
carrying the planned partition entries, the resolved format, the partition
columns and the full schema -- the Hive analogue of `IcebergScanExec`. The
coordinator finds it by downcast, exactly as it finds the Iceberg one, and
replaces it with a `DistributedScanExec` built from `build_hive_scan_tasks`.

The second is the real work of cycle 5 and it is a provider refactor, not a
wiring task: `scan_from_entries` currently BUILDS the execution plan, and it
would instead have to describe the scan and let one node own execution. The
partition materialization and sentinel-nullification logic moves inside that
node, which has the side benefit of putting it next to the worker-side copy that
must agree with it.

Nothing already built is wasted: the ticket, the worker-side materialization,
the task builder and the servability decision are all needed either way. But
the sequencing changes -- `HiveScanExec` comes before the differential harness,
because until it exists there is nothing to dispatch.
