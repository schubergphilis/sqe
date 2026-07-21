# CALL procedures

Iceberg maintenance operations exposed as SQL `CALL` statements. SQE wraps the vendored iceberg-rust action APIs (`RewriteFilesAction`, `RemoveSnapshotAction`, `RewriteManifestsAction`) and adds an SQE-specific bloom-filter suggestion procedure that walks recent query history.

All procedures use Iceberg's named-argument syntax: `CALL system.<proc>(name => value, ...)`. Unknown argument names raise a parse error so typos fail fast.

Source: `crates/sqe-sql/src/procedures.rs`. Handlers in `crates/sqe-coordinator/src/maintenance.rs`.

## Reference

| Procedure | Origin | Required args | Optional args | Notes |
|---|---|---|---|---|
| `system.rewrite_data_files` | `sqe-sql` + `sqe-coordinator` | `table => 'ns.t'` | `target_file_size_bytes => N`, `min_input_files => N`, `max_concurrent_file_group_rewrites => N`, `strategy => 'binpack'\|'sort'`, `sort_order => 'col ASC, ...'\|'zorder(a, b)'`, `delete_file_threshold => N`, `distributed => 'auto'\|'local'\|'require'` | Compacts small data files (delete-aware). Default target 512 MiB, min 5 files per group, max 4 concurrent groups. `strategy => 'sort'` sorts a whole partition by `sort_order` (a column list or `zorder(...)`) via a spillable DataFusion sort and rolls output at the target size, producing files with disjoint key ranges. `delete_file_threshold => N` also rewrites any data file with at least N delete files applying to it, even when it is already large. `distributed => ...` overrides `[maintenance.distribution] mode` for this one call (see [Configuration](../deployment/configuration.md) and [Distributed compaction](../design-notes/distributed-compaction.md)); omit it to use the configured mode. A manual `CALL` commits with no extra snapshot properties; the auto-compaction scheduler (see [Maintenance (auto-compaction)](../deployment/configuration.md#maintenance-auto-compaction)) calls this same handler internally and stamps `sqe.maintenance.job-id`/`principal`/`trigger` onto the snapshot it commits, so an autonomous compaction is attributable in the table's history while a manual one is not. |
| `system.expire_snapshots` | `sqe-sql` + `sqe-coordinator` | `table => 'ns.t'` | `older_than => TIMESTAMP`, `retain_last => N` | Drops old snapshots. `older_than` and `retain_last` combine: a snapshot must be older than `older_than` and beyond the `retain_last` window before it is removed. |
| `system.remove_orphan_files` | `sqe-sql` + `sqe-coordinator` | `table => 'ns.t'` | `older_than => TIMESTAMP` | Deletes files under the table prefix not referenced by any live snapshot. Default `older_than` is 3 days ago, to avoid racing with in-flight writes. |
| `system.rewrite_manifests` | `sqe-sql` + `sqe-coordinator` | `table => 'ns.t'` | - | Consolidates many small manifest files into fewer larger ones. Speeds up planning on large tables. |
| `system.suggest_bloom_filter_columns` | `sqe-sql` + `sqe-coordinator` | `table => 'ns.t'` | `history_limit => N` | SQE-specific. Walks the last N finished queries (default 1000), counts equality predicates per column, returns ranked suggestions for `write.parquet.bloom-filter-columns`. |
| `system.table_health` | `sqe-sql` + `sqe-coordinator` | `table => 'ns.t'` | - | SQE-specific (auto-compaction maintenance subsystem, see [Maintenance (auto-compaction)](../deployment/configuration.md#maintenance-auto-compaction)). Read-only compaction-debt report: live/small file counts, avg/p50 file size, delete-file and delete-heavy counts, eligible bin-pack groups, estimated rewrite bytes, last compaction snapshot, and whether the table has opted into the maintenance scheduler. Never rewrites anything, and available regardless of `maintenance.mode`. |

## Comparison to other engines

| Procedure | SQE | Trino + Iceberg | Spark + Iceberg | DuckDB |
|---|---|---|---|---|
| Compact small files | `CALL system.rewrite_data_files(...)` | `ALTER TABLE t EXECUTE optimize` | `CALL t.system.rewrite_data_files(...)` | - |
| Expire old snapshots | `CALL system.expire_snapshots(...)` | `ALTER TABLE t EXECUTE expire_snapshots(...)` | `CALL t.system.expire_snapshots(...)` | - |
| Remove orphans | `CALL system.remove_orphan_files(...)` | `ALTER TABLE t EXECUTE remove_orphan_files(...)` | `CALL t.system.remove_orphan_files(...)` | - |
| Rewrite manifests | `CALL system.rewrite_manifests(...)` | `ALTER TABLE t EXECUTE optimize_manifests` | `CALL t.system.rewrite_manifests(...)` | - |
| Suggest bloom filters | `CALL system.suggest_bloom_filter_columns(...)` | - | - | - |

The Spark and SQE shapes are aligned: Spark uses `t.system.<proc>` (table-qualified), SQE uses `system.<proc>(table => 'ns.t')` (named arg). Both are explicit. Trino prefers `EXECUTE`-as-DDL syntax which is harder to script.

## Examples

### Compact a partitioned fact table

```sql
CALL system.rewrite_data_files(
    table => 'analytics.events',
    target_file_size_bytes => 268435456,    -- 256 MiB
    min_input_files => 8
);
```

Returns one summary row:

```text
+----------------------+----------------------+----------------------+
| files_rewritten      | bytes_rewritten      | snapshot_id          |
+----------------------+----------------------+----------------------+
| 142                  | 39283744832          | 8472810294831234567  |
+----------------------+----------------------+----------------------+
```

### Sort-compact for read pruning

Load fast (unsorted), then compact into sorted files once. The sort strategy
gathers a whole partition into one stream, orders it by `sort_order` through a
spillable DataFusion sort, and rolls the output at `target_file_size_bytes`.
Sorting the partition as a single stream is what makes the result prunable: the
output files come out with disjoint key ranges instead of each file spanning the
full domain. The sort spills to disk, so the rewrite stays memory-bounded even
when a partition is larger than RAM. Unlike bin-pack, the sort strategy also
rewrites files already at or above the target size, because they still have to
be re-laid-out to join the sorted layout.

```sql
-- Lexicographic sort on one or more columns.
CALL system.rewrite_data_files(
    table => 'analytics.events',
    strategy => 'sort',
    sort_order => 'event_date ASC, user_id ASC'
);

-- Z-order clustering for multi-dimensional locality.
CALL system.rewrite_data_files(
    table => 'analytics.events',
    strategy => 'sort',
    sort_order => 'zorder(user_id, device_id)'
);
```

Sorted files give the reader tight min/max stats per file, so predicate
pruning skips more files. Z-order clusters several columns at once, which helps
when queries filter on different subsets of those columns. Iceberg's sort-order
metadata cannot express z-order, so none is stamped for the z-order case
(matches Spark).

Verify the layout with `table_files`: after a sort compaction the `lower_bounds`
/ `upper_bounds` of the output files should not overlap on the sort column.

```sql
SELECT file_path, lower_bounds, upper_bounds
FROM table_files('analytics', 'events')
ORDER BY lower_bounds;
```

### Clean up delete-heavy Merge-on-Read files

On a Merge-on-Read table, repeated `DELETE`/`UPDATE`/`MERGE` accumulate delete
files. A data file with many deletes is slow to read (every delete file has to
be applied on scan). `delete_file_threshold` rewrites any data file with at
least that many delete files applying to it, even when the file is already at or
above the target size, so bin-pack would otherwise leave it alone.

```sql
CALL system.rewrite_data_files(
    table => 'analytics.events',
    delete_file_threshold => 10
);
```

The count includes every delete file the scan attaches to the data file, both
position and equality deletes. A low threshold on an equality-heavy table
therefore rewrites broadly, since one equality delete can apply to many files.
The option is off by default and is a no-op under `strategy => 'sort'`, which
already rewrites the whole partition.

### Override the distribution mode for one call

`distributed => 'auto'|'local'|'require'` overrides
`[maintenance.distribution] mode` (see
[Configuration](../deployment/configuration.md)) for this one `CALL`,
without touching the coordinator's config file. `'require'` fails the
call immediately if fewer than `min_workers` workers are currently
healthy, rather than silently falling back to a coordinator-local
rewrite:

```sql
CALL system.rewrite_data_files(
    table => 'analytics.events',
    distributed => 'require'
);
```

`'local'` forces a coordinator-local rewrite even with a healthy fleet
present, useful for a one-off run you want to keep off the workers (a
small table, or a maintenance window where the fleet is busy with query
traffic). Omitting `distributed` entirely uses the configured
`[maintenance.distribution] mode`. See [Distributed
compaction](../design-notes/distributed-compaction.md) for how a
distributed call plans, dispatches, and commits.

### Check compaction debt before deciding whether to run a rewrite

`table_health` is read-only: it reuses the same file-collection and bin-pack
logic `rewrite_data_files` uses to plan a rewrite, but never writes a file or
commits a snapshot. It bypasses the write-privilege gate entirely, so a
`SELECT`-only session can run it.

```sql
CALL system.table_health(table => 'analytics.events');
```

Returns one summary row:

```text
+-----------------+-------------+----------------+----------------+--------------+---------------------+------------------+--------------------+-----------------------------+----------------------+
| live_data_files | small_files | avg_file_bytes | p50_file_bytes | delete_files | delete_heavy_files  | eligible_groups  | est_rewrite_bytes  | last_compaction_snapshot_ms| maintenance_enabled  |
+-----------------+-------------+----------------+----------------+--------------+---------------------+------------------+--------------------+-----------------------------+----------------------+
| 1842            | 611         | 41943040       | 33554432       | 96           | 12                  | 7                | 2248146944         | NULL                       | true                 |
+-----------------+-------------+----------------+----------------+--------------+---------------------+------------------+--------------------+-----------------------------+----------------------+
```

Column notes:

- `small_files` counts live data files below `[maintenance.compaction].target_file_size_bytes` (default 512 MiB).
- `eligible_groups` / `est_rewrite_bytes` report pure bin-pack debt: groups that meet `min_input_files` on file count alone. `delete_heavy_files` is a separate signal, files with at least `delete_file_threshold` delete files applying to them. A later `rewrite_data_files(delete_file_threshold => N)` call rewrites the union of both sets, so treat the two counts as additive, not `eligible_groups` already including delete-heavy files.
- `last_compaction_snapshot_ms` is always `NULL`. Active-mode compactions do stamp `sqe.maintenance.job-id`/`principal`/`trigger` onto the snapshot they commit (see the `system.rewrite_data_files` note above), but `table_health` does not yet read that snapshot property back; check the table's snapshot history directly for compaction attribution until a later phase wires this column up.
- `maintenance_enabled` reflects the `sqe.maintenance.enabled` table property, i.e. whether the advisory/active scheduler would even consider this table. It does not mean a rewrite ran: advisory mode never mutates, and active mode may still find no eligible compaction debt on a given tick.

### Drop snapshots older than 30 days, keeping the last 10

```sql
CALL system.expire_snapshots(
    table => 'analytics.events',
    older_than => TIMESTAMP '2026-04-08 00:00:00',
    retain_last => 10
);
```

The `retain_last` floor is enforced even when `older_than` would clear more. Useful for keeping a rollback budget while clamping storage growth.

### Bloom filter suggestion before a tuning pass

```sql
CALL system.suggest_bloom_filter_columns(
    table => 'analytics.events',
    history_limit => 5000
);
```

Returns one row per column with a positive equality-predicate count, ranked descending:

```text
+----------+-------------------+------------------+
| column   | equality_pred_hits | recommendation  |
+----------+-------------------+------------------+
| user_id  | 4823              | strongly suggested |
| event_id | 1241              | suggested         |
| device   | 312               | weak             |
+----------+-------------------+------------------+
```

Apply with:

```sql
ALTER TABLE analytics.events SET TBLPROPERTIES (
    'write.parquet.bloom-filter-columns' = 'user_id,event_id'
);
```

The next write picks up the new property; existing files are unaffected until rewritten.

### Combined maintenance run

```sql
-- Once a week, in this order:
CALL system.expire_snapshots(table => 'analytics.events',
    older_than => TIMESTAMP '2026-04-08 00:00:00', retain_last => 30);
CALL system.remove_orphan_files(table => 'analytics.events',
    older_than => TIMESTAMP '2026-04-08 00:00:00');
CALL system.rewrite_manifests(table => 'analytics.events');
CALL system.rewrite_data_files(table => 'analytics.events');
```

Order matters: expire snapshots before removing orphan files (otherwise files referenced by snapshots about to expire look orphaned), and rewrite manifests before rewriting data files (so the rewrite plan reads compact manifests).

## Permissions

Procedures inherit the calling user's grants on the target table:

- `system.rewrite_data_files`, `system.rewrite_manifests` need `MODIFY` (writes new files, commits a snapshot).
- `system.expire_snapshots`, `system.remove_orphan_files` need `MODIFY` and `DROP` (alters retention, deletes files).
- `system.suggest_bloom_filter_columns` is read-only against query history; `SELECT` on the table is enough.
- `system.table_health` is read-only against the table's live metadata; `SELECT` on the table is enough. It bypasses the write-privilege gate entirely, unlike every other procedure in this table.

A user without the right grant gets a clear "policy denied" error instead of a generic execution failure.

When no OPA / Cedar policy store is wired, an engine-level heuristic acts as the last line of defence. A session is treated as read-only when any of its roles matches `read*`, `select*`, or contains `readonly`, and no role contains `write`, `admin`, or `owner`. Read-only sessions are denied every maintenance procedure and the attempt is recorded in the audit log with `status = "denied"`. A policy store overrides this heuristic once configured.

## Safety notes

- **`remove_orphan_files` with no `older_than`** uses the 3-day default, which is conservative against compaction or COPY jobs in flight. Override with `older_than` only after confirming no concurrent writers.
- **`expire_snapshots` is destructive** for time-travel queries. Once a snapshot is expired, `FOR VERSION AS OF <id>` for that snapshot fails. Document a retention window your team agrees on, and stick to it.
- **`rewrite_data_files` rewrites entire data files**, not row groups. Two consecutive calls can churn the same files; rely on the `min_input_files` floor (default 5) to keep churn bounded.
- **`rewrite_data_files` is delete-aware on Merge-on-Read tables.** It reads each file group through the Iceberg scan, so position and equality deletes are applied during the rewrite and deleted rows never reappear. The compacted output is pinned to the sequence number of the snapshot it read, so an equality delete another writer commits mid-compaction still applies to the compacted files. Fully-covered position delete files are dropped in the same commit; equality deletes are left to age out via `expire_snapshots`. It groups files per partition, so partitioned tables consolidate within each partition.
- **`rewrite_data_files` retries on conflict.** A concurrent writer that commits between the read and the commit produces a retryable conflict; the procedure re-reads and retries with backoff a bounded number of times before surfacing the conflict.
- **Run procedures in a quiet window.** A concurrent writer that commits mid-run can cause `rewrite_data_files` to return a retryable error. The other procedures tolerate concurrency and reconcile against the live snapshot.

## Commit failures

Every procedure commits through the same REST catalog path that CTAS and INSERT use, so commit failures surface as `SqeError::Execution` and fall into two buckets:

- **Retryable.** The message contains `conflict` or `retry`. `rewrite_data_files` already re-reads and retries a bounded number of times internally; a retryable error surfaced to the caller means those attempts were exhausted, so schedule another run after a back-off. The other procedures surface the conflict directly.
- **Permanent.** Everything else. Check the message for the upstream cause.

## What is not exposed

The vendored iceberg-rust crate has more transaction actions than SQE wires up. Notable omissions:

- `expire_snapshots_by_id` (drop a specific snapshot rather than by age). easy to add if needed.
- `rewrite_position_deletes` (compact MoR delete files). not yet wrapped; on the V13 backlog.
- `cherrypick_snapshot` (apply a non-current snapshot's changes to the head). out of scope for now; rare use case.

File an issue if you hit one of these in production.
