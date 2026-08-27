## Why

A Hive external table is read-only. `INSERT INTO` and CTAS are rejected by `reject_unsupported(statement, 4)`, which names this cycle. That makes SQE a query engine over someone else's Hive data rather than a participant in a Hive pipeline: a user can read an Athena table but cannot land a derived table beside it, so every write goes back to Athena, Spark, or a hand-rolled job.

The gap is narrower than it looks, and the reason matters for how this cycle is scoped. `HiveTableProvider::insert_into` already delegates to its inner `ListingTable`, and DataFusion 55's `ListingTable::insert_into` passes `table_partition_cols` into `FileSinkConfig`. The `k=v/` fan-out writer exists upstream and is already wired. What is missing is not a writer.

What is missing is everything around it:

- **Routing.** The statement never reaches the provider; the coordinator rejects it first.
- **Bookkeeping.** Cycle 2 made reads plan from `sqe.partition_index` instead of listing the table prefix. A write that lands files in a new partition directory is therefore INVISIBLE to every subsequent query until the index knows about it. The strongest read optimization in the Hive path is the thing that makes the first write look like a lost write.
- **A visibility contract.** A Hive external table has no metadata pointer to swap. There is no `rewrite_files` commit and nothing to make a multi-file write atomic. Pretending otherwise is worse than saying so.
- **Round-trip fidelity per format.** A CSV table's `skip.header.line.count` is a read-side declaration. A writer that disagrees with it does not fail: the table gains or loses one row per file.

## What Changes

- Route `INSERT INTO` and `CREATE TABLE AS SELECT` for a Hive external table to the provider instead of rejecting them.
- Pin `keep_partition_by_columns = false` for Hive writes. Partition columns are directory-encoded; a file that also contains them no longer matches the read path's file schema, and the table breaks on READ, not on write.
- Maintain `sqe.partition_index` as part of the write: add new partition entries, update `file_count` and `total_bytes` on touched ones, and invalidate the manifest cache for the table.
- Refuse `INSERT OVERWRITE` explicitly. Deferring the feature is fine; silently translating it to an append is not, because it doubles the data and returns success.
- A round-trip test per format: write, then read back through the normal read path, and compare row counts and values. Not a write test, which would pass on a file nobody can read correctly.
- State the visibility contract in the reference page: per-object atomicity, no multi-file atomicity, and what a reader sees mid-write.

## Capabilities

### New Capabilities
- `hive-writes`: `INSERT INTO`, CTAS, partition-directory fan-out, index maintenance, and the visibility contract.

### Modified Capabilities
- `hive-external-tables`: the statement surface gains two write statements. Reads are unchanged.
- `hive-partition-index` (cycle 2): the index is now written by more than `MSCK REPAIR` and `ALTER TABLE`, so its consistency rules apply to a concurrent-writer case they did not have to before.

## Success Criteria

- `INSERT INTO` a partitioned Hive table lands files under the correct `k=v/` directories, and a subsequent query returns exactly the inserted rows plus what was there before.
- A write into a NEW partition is visible to the next query without a manual `MSCK REPAIR`.
- Round-trip per format: Parquet, CSV (both `skip.header.line.count` settings), and line-delimited JSON each return the rows they were given, same count, same values.
- Partition columns are NOT present in any written data file.
- `INSERT OVERWRITE` fails with a message naming what it would have done, rather than appending.
- Two concurrent inserts into the same partition do not overwrite each other's files.
- An interrupted write leaves the table readable, with the documented recovery path.

## Rollback

Writes are opt-in at the statement level: the routing change is what enables them, and reverting it restores `reject_unsupported`. No on-disk format changes, and the index format is cycle 2's, unversioned by this change. A table written by this cycle stays readable by a build without it, because the files and directories are ordinary Hive layout.
