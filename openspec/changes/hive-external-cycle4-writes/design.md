# Cycle 4: writes into Hive partition directories

Read `crates/sqe-catalog/src/hive/provider.rs`, `crates/sqe-coordinator/src/query_handler.rs` around `reject_unsupported`, and DataFusion 55's `datafusion-catalog-listing/src/table.rs` `insert_into` before changing any of them. What is below is what those do today.

## What already exists, measured

`HiveTableProvider::insert_into` delegates:

```rust
async fn insert_into(&self, state, input, insert_op) -> DFResult<Arc<dyn ExecutionPlan>> {
    self.inner.insert_into(state, input, insert_op).await
}
```

DataFusion 55's `ListingTable::insert_into` builds a `FileSinkConfig` carrying `table_partition_cols: self.options.table_partition_cols.clone()`, and `HiveSchemaProvider::table` already constructs the inner `ListingTable` WITH those partition columns (that is how the read path gets Hive's "partition keys last" column order for free). So the `k=v/` fan-out writer is upstream, present, and already configured.

The write mechanics are therefore not this cycle's problem. Three other things are.

## Decision 1: there is no atomic commit, and the contract says so

An Iceberg write ends with a metadata swap. `commit_written_files` builds the new file list and lands it in one `rewrite_files` operation, so a reader sees either the old snapshot or the new one. That is why INSERT OVERWRITE was possible for Iceberg in cycle `!669`.

A Hive external table has no equivalent. Its content is *whatever is in the directory*. That is not an implementation gap, it is the format's contract, and it is the reason Hive external tables interoperate: Athena, Spark, and Trino all read the same directory and none of them consults SQE's sidecar to decide which files count.

Three options were considered.

**(a) Write directly into the final partition directories.** A reader mid-write sees some of the new files. This is what Hive and Athena `INSERT INTO` do.

**(b) Stage under a temporary prefix, then move.** S3 has no multi-object atomic rename, so a "move" is copy-then-delete per object. The inconsistency window shrinks; it does not close. The cost is doubling the bytes written, and a failure now leaves orphans in two places instead of one.

**(c) Make the sidecar manifest the authority for which FILES belong to the table,** so a single manifest `put` is the commit. This is the Iceberg design, and it works -- for SQE only. Every other engine reading the directory would see files SQE considers uncommitted. It trades the one property that makes these tables worth having.

**Chosen: (a).** Files land in their final locations. The visibility contract is:

- A single object's appearance IS atomic on an object store: S3 and compatible stores make a `PUT` visible whole or not at all, so no reader ever sees a torn file.
- On a local filesystem that is not true, so the local path writes to a temporary name and renames within the same directory. `rename(2)` on one filesystem is atomic.
- A multi-file write is NOT atomic. A concurrent reader may see a subset of a write's files. There is no fix for this inside the format.
- A failed write leaves the files it had already landed. They are real rows, not corruption, and they are visible to any reader that lists the directory.

Saying this in the reference page is part of the deliverable. A user who needs snapshot isolation on a write needs an Iceberg table, and should be told so rather than discovering it.

## Decision 2: data first, then the index, with MSCK REPAIR as recovery

Cycle 2's `sqe.partition_index` is what makes a read plan without listing the table prefix. A write must maintain it or the write is invisible.

The order is load-bearing and the two directions fail differently:

- **Index first, then data.** The index promises a partition whose files do not exist yet. A read plans that partition, lists it, and gets nothing -- or fails, depending on how the listing handles a missing prefix. The table is broken by a write that had not written anything yet.
- **Data first, then index.** The files exist and the index does not know. A read omits them. The data is intact and invisible, and `MSCK REPAIR TABLE` -- which already exists, from cycle 2 -- recovers it exactly.

So: write the data files, then update the index, then invalidate the manifest cache. An index update that fails after the data landed must report that the rows were written but are not yet visible, and name `MSCK REPAIR TABLE` as the fix. A generic error there sends the user looking for lost data that is sitting in the bucket.

This is the same invariant cycle 2 already stated for its own path -- "a stale index omits partitions rather than inventing them" -- and the write path has to preserve it rather than establish a new one.

### The manifest cache

`HiveMetadataCache` has a 60-second TTL. Without an explicit invalidation, a write followed immediately by a read can serve the pre-write manifest and its pre-write index, so the new partition stays invisible for up to a minute even though the index was updated correctly. The DDL handler already invalidates on `ALTER TABLE ADD PARTITION`; writes need the same call, for the same reason.

## Decision 3: round-trip, not write-and-check

The write is only correct if the read agrees, and the two disagree in ways that a write test cannot see.

**`keep_partition_by_columns` must be false.** DataFusion's default is false, which is right, but relying on a default here is relying on the wrong thing. The read path derives its file schema from the manifest's declared column list -- file columns only, partition columns excluded, because they are directory-encoded. A data file that ALSO contains the partition columns has more fields than the reader expects, and the failure surfaces on the next SELECT rather than at write time. Pin it.

**CSV headers round-trip or they do not.** `skip.header.line.count` is a read-side declaration in the manifest. DataFusion's CSV writer emits a header by default. The two combinations that go wrong are silent:

| Declared | Writer emits | Read-back |
|---|---|---|
| `= 1` | header | correct |
| `= 1` | no header | first real row eaten as the header |
| `= 0` | header | phantom row, all columns as strings-of-column-names |
| `= 0` | no header | correct |

Neither wrong case errors. So the writer's header setting must be derived from the manifest's declaration, and the test must write and then READ, comparing row counts. A test that only checks the file exists, or that the writer was configured, passes on both wrong rows.

There is reason to think this one is already right, and it is worth checking rather than assuming either way. `serde_map`'s `listing_options` builds `CsvFormat::default().with_has_header(r.has_header)` from the resolved format, and in DataFusion the same `CsvFormat` instance serves both directions -- `has_header` governs whether the reader skips a line AND whether the writer emits one. Since a Hive table's read and write both go through that one `ListingOptions`, the two cannot disagree by construction. **Verify it with the round-trip test before writing anything.** If it holds, task 2.3 becomes a test rather than a change, and that is a better outcome than a redundant setting nobody can remove later because nobody knows whether it was load-bearing.

**JSON and Parquet have the analogous risk in type fidelity.** A CSV or JSON write serializes to text, so a `DOUBLE` round-trips through a decimal rendering. The round-trip test compares values, not just counts.

## Decision 4: INSERT OVERWRITE is refused, loudly

`InsertOp` has an `Overwrite` variant, and DataFusion's listing sink accepts it. What it does for a partitioned table is not what a Hive user means by `INSERT OVERWRITE PARTITION`: Hive replaces the named partition's contents, atomically from the reader's point of view.

Without an atomic commit, an overwrite is delete-then-write. An interruption between the two loses the partition. That is real data loss from a statement that reads as routine, and it is not something to ship alongside the first working append.

So `INSERT OVERWRITE` fails with a message that names what it would have had to do (delete the partition's objects, then write, with no way to make that atomic) and points at the alternative. **The failure mode to avoid is not the refusal -- it is `Overwrite` reaching the sink unnoticed and being treated as an append, which doubles the data and returns success.** The refusal is therefore a match on `insert_op` in the provider, not a check in the parser, so it cannot be bypassed by a path that constructs the plan differently.

## Decision 5: file naming

Two concurrent inserts into the same partition must not collide. This repo has already been bitten by static file names in the Iceberg path: identical names caused INSERT failures at scale, found through a dbt load. DataFusion's sink generates names from a per-write UUID plus a partition index, which is sufficient -- but it is worth a test rather than an assumption, because the failure only appears under concurrency and the symptom is a lost file, not an error.

## What is deliberately not in this cycle

- `INSERT OVERWRITE` (decision 4).
- `DELETE`, `UPDATE`, `MERGE`. A Hive external table has no delete mechanism short of rewriting a partition, so these need the same missing primitive overwrite needs.
- Compaction of small files written by many inserts. Real, and it belongs with the compaction roadmap rather than here.
- Writing through workers. Cycle 5 moved SCANS to workers; the write sink stays coordinator-local, which is where the Iceberg write path also runs.
