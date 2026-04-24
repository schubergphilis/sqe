# Operations

Day-two operations for SQE run through `CALL system.*` SQL procedures. Each
procedure wraps a vendored iceberg-rust transaction action and commits
through the same REST catalog path that CTAS and INSERT use. Every
procedure requires write privilege on the target table. Read-only sessions
are denied and the attempt is recorded in the audit log.

## Supported procedures

| Procedure | Purpose | Vendored action |
|---|---|---|
| `system.rewrite_data_files` | Compact small data files | `transaction/rewrite_files.rs::RewriteFilesAction` |
| `system.expire_snapshots` | Drop old snapshots | `transaction/remove_snapshots.rs::RemoveSnapshotAction` |
| `system.remove_orphan_files` | Delete unreferenced files under the table prefix | `actions/remove_orphan_files.rs::RemoveOrphanFilesAction` |
| `system.rewrite_manifests` | Consolidate small manifests | `transaction/rewrite_manifests.rs::RewriteManifestsAction` |

All procedures return a single-row `RecordBatch` with columns:

- `procedure` - name of the called procedure
- `table` - fully qualified table reference in `namespace.name` form
- `input_count` - pre-commit file / snapshot count
- `output_count` - post-commit file / snapshot count
- `input_bytes` - pre-commit total bytes (0 when the action does not expose it)
- `output_bytes` - post-commit total bytes (0 when not exposed)
- `status` - `committed`, `skipped: reason`, or a detailed reason string

## Compaction

```sql
CALL system.rewrite_data_files(table => 'analytics.orders');
```

Optional arguments, all named:

- `target_file_size_bytes` (u64, default `512 * 1024 * 1024`) - target output
  file size; groups below `min_input_files` are skipped.
- `min_input_files` (usize, default `5`) - groups with fewer than this many
  candidate files are left alone.
- `max_concurrent_file_group_rewrites` (usize, default `4`) - caps the
  parallelism of group rewrites.

Small tables that sit below `min_input_files` return `status='skipped:
below min_input_files'`. Concurrent writer conflicts surface as a retryable
error so the client can back off and try again. Row counts stay constant
across a successful rewrite.

## Snapshot expiry

```sql
-- time-based
CALL system.expire_snapshots(
  table => 'analytics.orders',
  older_than => '2026-04-01T00:00:00Z'
);

-- count-based
CALL system.expire_snapshots(
  table => 'analytics.orders',
  retain_last => 5
);
```

Defaults come from the vendored action: 5-day maximum age, minimum of one
snapshot retained. Branch-referenced and tag-referenced snapshots stay
regardless of age; that contract will hold once Phase C branching lands.
The current snapshot is always retained.

## Orphan file removal

```sql
CALL system.remove_orphan_files(table => 'analytics.orders');
```

Default `older_than` is 3 days before now, matching the spec. Files newer
than that threshold are preserved to avoid races with in-flight writes.
Override the threshold explicitly when running an offline cleanup:

```sql
CALL system.remove_orphan_files(
  table => 'analytics.orders',
  older_than => '2026-03-15T00:00:00Z'
);
```

The procedure returns `status='deleted=N'` where N is the count of deleted
paths. The list of paths goes to the tracing log at `info` level, not the
result batch, to keep the response cardinality bounded.

## Manifest consolidation

```sql
CALL system.rewrite_manifests(table => 'analytics.orders');
```

This calls `RewriteManifestsAction` with default clustering, taking
advantage of the RisingWave fork's parallel manifest loader. Data file
references are unchanged: a `SELECT` against the table before and after
the rewrite must return the same rows.

## Privileges and audit

Maintenance procedures mutate table state, so every call goes through a
write-privilege check before any catalog traffic. The engine-level check
applies these rules in order:

1. If any role in the session matches `read*`, `select*`, or contains
   `readonly`, AND no role contains `write`, `admin`, or `owner`, the
   session is treated as read-only.
2. Otherwise the session is write-capable.

Denials are recorded in the audit log (`status = "denied"`). A Polaris or
OPA/Cedar policy store overrides this check once wired; the engine-level
rule exists as the last line of defence.

## Error classification

Commit failures fall into two buckets. Both surface as `SqeError::Execution`
so existing error handling works unchanged:

- **Retryable**: messages containing `conflict` or `retry`. The iceberg-rust
  retry loop has already given up, so the caller is responsible for
  scheduling another run.
- **Permanent**: everything else. Check the message for the upstream cause.

## Example maintenance window

```sql
-- 1. Compact recent writes
CALL system.rewrite_data_files(table => 'analytics.orders');

-- 2. Consolidate manifests
CALL system.rewrite_manifests(table => 'analytics.orders');

-- 3. Expire old snapshots (keep two weeks)
CALL system.expire_snapshots(
  table => 'analytics.orders',
  older_than => '2026-04-10T00:00:00Z'
);

-- 4. Remove orphan files (default 3-day threshold)
CALL system.remove_orphan_files(table => 'analytics.orders');
```

Run each procedure in a quiet window. A concurrent writer that commits
between step 1 and step 4 can cause step 1 to return a retryable error;
the other steps tolerate concurrency and reconcile against the live
snapshot.
