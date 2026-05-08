# CALL procedures

Iceberg maintenance operations exposed as SQL `CALL` statements. SQE wraps the vendored iceberg-rust action APIs (`RewriteFilesAction`, `RemoveSnapshotAction`, `RewriteManifestsAction`) and adds an SQE-specific bloom-filter suggestion procedure that walks recent query history.

All procedures use Iceberg's named-argument syntax: `CALL system.<proc>(name => value, ...)`. Unknown argument names raise a parse error so typos fail fast.

Source: `crates/sqe-sql/src/procedures.rs`. Handlers in `crates/sqe-coordinator/src/maintenance.rs`.

## Reference

| Procedure | Origin | Required args | Optional args | Notes |
|---|---|---|---|---|
| `system.rewrite_data_files` | `sqe-sql` + `sqe-coordinator` | `table => 'ns.t'` | `target_file_size_bytes => N`, `min_input_files => N`, `max_concurrent_file_group_rewrites => N` | Bin-packs small data files into larger ones. Default target 512 MiB, min 5 files per group, max 4 concurrent groups. |
| `system.expire_snapshots` | `sqe-sql` + `sqe-coordinator` | `table => 'ns.t'` | `older_than => TIMESTAMP`, `retain_last => N` | Drops old snapshots. `older_than` and `retain_last` combine: a snapshot must be older than `older_than` and beyond the `retain_last` window before it is removed. |
| `system.remove_orphan_files` | `sqe-sql` + `sqe-coordinator` | `table => 'ns.t'` | `older_than => TIMESTAMP` | Deletes files under the table prefix not referenced by any live snapshot. Default `older_than` is 3 days ago, to avoid racing with in-flight writes. |
| `system.rewrite_manifests` | `sqe-sql` + `sqe-coordinator` | `table => 'ns.t'` | - | Consolidates many small manifest files into fewer larger ones. Speeds up planning on large tables. |
| `system.suggest_bloom_filter_columns` | `sqe-sql` + `sqe-coordinator` | `table => 'ns.t'` | `history_limit => N` | SQE-specific. Walks the last N finished queries (default 1000), counts equality predicates per column, returns ranked suggestions for `write.parquet.bloom-filter-columns`. |

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

A user without the right grant gets a clear "policy denied" error instead of a generic execution failure.

## Safety notes

- **`remove_orphan_files` with no `older_than`** uses the 3-day default, which is conservative against compaction or COPY jobs in flight. Override with `older_than` only after confirming no concurrent writers.
- **`expire_snapshots` is destructive** for time-travel queries. Once a snapshot is expired, `FOR VERSION AS OF <id>` for that snapshot fails. Document a retention window your team agrees on, and stick to it.
- **`rewrite_data_files` rewrites entire data files**, not row groups. Two consecutive calls can churn the same files; rely on the `min_input_files` floor (default 5) to keep churn bounded.

## What is not exposed

The vendored iceberg-rust crate has more transaction actions than SQE wires up. Notable omissions:

- `expire_snapshots_by_id` (drop a specific snapshot rather than by age). easy to add if needed.
- `rewrite_position_deletes` (compact MoR delete files). not yet wrapped; on the V13 backlog.
- `cherrypick_snapshot` (apply a non-current snapshot's changes to the head). out of scope for now; rare use case.

File an issue if you hit one of these in production.
