# Write modes: Merge-on-Read vs Copy-on-Write

Iceberg tables can choose how DELETE, UPDATE, and MERGE statements persist
changes. SQE honours the standard Iceberg table properties. Each DML kind
has its own property so you can run DELETE as MoR but keep UPDATE as CoW
on the same table.

## Properties

| Property             | Scope   | Default         | Accepted values                     |
|----------------------|---------|-----------------|-------------------------------------|
| `write.delete.mode`  | DELETE  | `copy-on-write` | `copy-on-write`, `merge-on-read`    |
| `write.update.mode`  | UPDATE  | `copy-on-write` | `copy-on-write`, `merge-on-read`    |
| `write.merge.mode`   | MERGE   | `copy-on-write` | `copy-on-write`, `merge-on-read`    |

The dispatcher is strict. Typos like `"mor"`, `"MoR"`, or `"COPY-ON-WRITE"`
raise an error at DML time so silent mode mismatches do not happen.

Set the property on a new table:

```sql
CREATE TABLE ns.orders (
    id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(18,2)
)
WITH (
    identifier_field_ids = 'id',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode'  = 'merge-on-read'
);
```

Or toggle it on an existing table:

```sql
ALTER TABLE ns.orders
SET TBLPROPERTIES ('write.update.mode' = 'merge-on-read');
```

## When each mode wins

### Copy-on-Write (default)

CoW rewrites every data file that contains a matched row. One snapshot
produces a clean set of parquet files with no delete-file layer for
readers to merge.

Pick CoW when:

- The UPDATE or DELETE touches most rows in each file (rewriting costs
  the same as reading and the read path stays simple).
- You care about read latency on small tables and want to avoid the
  equality-delete merge overhead.
- You write to the table through engines that have weak or no
  equality-delete read support.

### Merge-on-Read

MoR keeps the old data files untouched and adds a small equality-delete
file plus, for UPDATE and MERGE, a new data file with the replacement
rows. Commit is atomic via `RowDeltaAction`.

Pick MoR when:

- The statement touches a small fraction of a large table. The SF100
  TPC-E `trade_result_update_holding` query updates thousands of rows
  per call in a partition with hundreds of millions of rows. CoW times
  out at 120 seconds because it rewrites every file; MoR only writes
  the matched rows.
- You need snapshot-stable keys: rows added later that match the same
  equality keys are also excluded at scan time without a new delete
  file.
- Writes arrive faster than compaction can run.

### Mixed configuration

Setting only `write.delete.mode = 'merge-on-read'` gives you DELETE as
MoR while UPDATE and MERGE still rewrite files. This is useful during
rollout: enable the fastest lane first, check correctness across reader
engines, then extend to UPDATE and MERGE.

## Compatibility

MoR requires reader engines that understand position deletes and
equality deletes.

- **Spark 4.1** with `iceberg-spark-runtime` 1.x: full support.
- **Trino 465** with `trino-iceberg-connector`: full support.
- **Spark 3.3 or older**: V2 position-delete reads work; V3 position
  deletes need Spark 4.x. Equality deletes require Spark 3.1+.
- **DuckDB iceberg extension**: position-delete reads land in the 2025
  release; equality deletes are not yet supported.

If you cannot guarantee all readers support the delete-file format, run
compaction (`CALL system.rewrite_data_files(...)`) regularly to collapse
delete files back into data files.

## Primary keys

MoR UPDATE and MERGE need a primary key because the equality-delete file
has to reference the old row by value. SQE reads the key from the table
schema's `identifier-field-ids`.

Without a PK the dispatcher falls back to CoW with a log entry rather
than fail. MoR DELETE without a PK falls back to position deletes.

## Performance expectations

The `scripts/benchmark-mor-vs-cow.sh` harness exercises a 1k-row UPDATE
on a 1M-row table under each mode. The expected shape:

| Mode            | Duration   | New data files | Removed data files | Delete files |
|-----------------|------------|----------------|--------------------|--------------|
| copy-on-write   | seconds    | one per file rewritten | all matched files | 0          |
| merge-on-read   | milliseconds | 1 new data file with 1k rows | 0 | 1 equality delete |

Latest numbers land in `benchmarks/results/mor-vs-cow-<timestamp>.json`.

## Known limitations

- **Native MERGE plan**: DataFusion's upcoming MERGE INTO plan
  (apache/datafusion#20746) is not yet available. SQE rewrites MERGE as
  a composition of DELETE + UPDATE + INSERT internally. The MoR version
  builds the row delta from that composition in one commit.
- **Snapshot conflict on concurrent writes**: the `RowDeltaAction` calls
  `validate_from_snapshot(S)` and fails with a retryable conflict if
  another writer advances the snapshot first. Clients should re-read
  state and re-apply the delta.
