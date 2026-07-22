# Autonomous Compaction

Iceberg tables accumulate small files under normal write traffic: frequent
appends, streaming ingest, and Merge-on-Read `DELETE`/`UPDATE`/`MERGE`
statements that leave position and equality delete files behind. Left
alone, small-file counts and delete debt both grow, and every scan pays
for it: more files to open, more delete files to apply, worse pruning.

SQE compacts tables two ways. Run it on demand with `CALL
system.rewrite_data_files(...)`, the same procedure Trino and Spark expose
under their own `optimize`/`rewrite_data_files` syntax. Or opt a table into
the autonomous path: a background scheduler that watches opted-in tables,
reports their compaction debt, and, once you trust it, compacts them on a
cron schedule with no human in the loop.

This page is the orienting map. The full sizing knobs live in
[Configuration](../deployment/configuration.md#maintenance-auto-compaction),
the procedure arguments live in [CALL
procedures](../sql-reference/procedures.md), and the distributed dispatch
mechanics live in [Distributed
compaction](../design-notes/distributed-compaction.md).

## What it does

A compaction rewrite reads a group of small data files, applies any
Merge-on-Read deletes that cover them so deleted rows never reappear, and
writes new, larger Parquet files in their place. The whole operation is
one Iceberg commit: old files out, new files in, atomically. A `strategy`
argument picks the layout, and one more argument targets delete-heavy
files regardless of strategy:

- **`strategy => 'binpack'`** (default). Groups small files by partition
  and rolls them up to `target_file_size_bytes` (512 MiB by default).
  Cheapest option, no reordering.
- **`strategy => 'sort'` with `sort_order`**. Gathers a whole partition
  into one stream, sorts it by the given column list through a spillable
  DataFusion sort, and rolls the output at the target size. Sorting the
  partition as a single stream is what makes the result prunable: output
  files land with disjoint key ranges instead of each file spanning the
  full domain, so predicate pushdown skips more files at scan time.
- **`sort_order => 'zorder(col_a, col_b)'`**. Z-order clustering for
  queries that filter on different subsets of several columns at once.
  Iceberg's sort-order metadata cannot express z-order, so SQE stamps none
  for this case, matching Spark's behavior.
- **`delete_file_threshold => N`**. Rewrites any data file with at least N
  delete files applying to it, even if the file is already at or above the
  target size. The threshold is the lever for delete-heavy Merge-on-Read
  tables, where the file itself is fine but every scan pays to apply a
  stack of deletes against it.
- **`rewrite_all => true`**. Forces a rewrite of every data file, including
  files already at or above the target size and partitions below
  `min_input_files`. It applies all deletes and re-encodes at the target
  size. Use it to force a clean pass after a schema or partition-spec
  change, or to apply accumulated deletes across a whole table in one
  commit. Off by default; subsumed by `strategy => 'sort'`, which already
  rewrites the whole partition.

`CALL system.table_health('ns', 't')` is the read-only companion: it reuses
the same file-collection and bin-pack logic as a rewrite, but never writes
anything, so a `SELECT`-only session can check compaction debt before
deciding whether to run one. It reports live and small file counts, delete
and delete-heavy file counts, eligible bin-pack groups, and estimated
rewrite bytes. See [CALL procedures](../sql-reference/procedures.md) for
the full argument list and column reference for both procedures.

## Autonomy: advisory before active

The `[maintenance]` config block gates the background path with a mode
ladder, and every step up that ladder is a decision an operator makes
explicitly, never a default:

- **`mode = "off"`** (the default). No maintenance principal is built, no
  scheduler task runs. Total absence, not a loop that declines to fire.
- **`mode = "advisory"`**. The scheduler discovers opted-in tables and
  publishes the same health report `CALL system.table_health` returns.
  Nothing is rewritten.
- **`mode = "active"`**. The scheduler commits real rewrites against
  tables that are both due and opted in, through the same handler `CALL
  system.rewrite_data_files` uses interactively.

Run advisory mode first. Let it report real compaction debt against your
actual write pattern before opting a table into active mode. Active mode
mutates data files and commits snapshots; treat the switch from advisory
to active as a reviewed, per-table decision, not a rollout.

A table only ever gets touched by the active scheduler when three things
line up: the global mode is `advisory` or `active`, the table owner has
set `sqe.maintenance.enabled = true` via `ALTER TABLE ... SET
TBLPROPERTIES`, and the maintenance principal holds a
`TABLE_WRITE_DATA` grant on that table's namespace in Polaris. Miss any
one of the three and nothing happens to that table. Full detail on the
gates, the cron schedule, and per-table overrides for the sizing knobs is
in [Configuration](../deployment/configuration.md#maintenance-auto-compaction).

## A dedicated, isolated principal

The background scheduler never runs as you, and it never runs as the
service account behind interactive queries either. `[maintenance.principal]`
configures a separate OAuth2 client-credentials identity, used solely by
the maintenance path. It is not added to the interactive auth chain, so
there is no code path by which a query session could authenticate as it,
even by accident.

Give this principal the least privilege it needs: `TABLE_READ_DATA` for
advisory analysis, `TABLE_WRITE_DATA` added only for tables opted into
active mode, never `CREATE` or `DROP` or admin grants. Polaris enforces
that boundary server-side, on top of SQE's own opt-in and mode gates.

## Distribution: coordinator-local or the worker fleet

`[maintenance.distribution] mode` decides whether an active-mode rewrite
runs on the coordinator alone or fans its file groups out across the
worker fleet:

- **`auto`** (default). Coordinator-local below `min_workers` healthy
  workers, fans out once the fleet reaches that floor.
- **`local`**. Always coordinator-local, even with a healthy fleet.
- **`require`**. Always fans out. Below `min_workers` it does not fall
  back quietly: a scheduled tick skips the job loudly, and a manual `CALL
  ... distributed => 'require'` errors outright.

In the distributed path, workers read data files, apply deletes, sort if
requested, and write new Parquet directly against S3, using their own S3
credentials and no catalog token at all. Commit authority never leaves the
coordinator: it validates every worker's output, re-checks the row-count
invariant across the whole job, and commits one atomic snapshot that swaps
every old file for every new file at once. By default a job either commits
everything or nothing. Opting into `[maintenance.distribution]
partial_progress` trades that guarantee for incremental batch commits on
very large tables, at the cost of a larger commit-conflict surface. See
[Configuration](../deployment/configuration.md) for the knob, and
[Distributed compaction](../design-notes/distributed-compaction.md) for
the full planning, dispatch, and commit flow, including the
partial-progress commit model.

## High availability

Running the scheduler on more than one coordinator needs a way to stop two
of them compacting the same table in the same window. `[maintenance.scheduler]
lease` picks the guard:

- **`none`**. No lease. Only safe for a single-coordinator deployment,
  and validation requires an explicit `single_scheduler_acknowledged =
  true` before it will accept this setting with the scheduler enabled.
- **`catalog`** (default). Before dispatching a rewrite, the scheduler
  claims a lease row in the state table. A coordinator that finds the
  lease already held skips its tick for that table; a crashed holder's
  claim expires after `lease_ttl_secs`.

The lease is an efficiency mechanism, not the source of correctness.
Correctness comes from Iceberg's optimistic-concurrency commit: if a lease
operation fails, or two coordinators somehow race to compact the same
table, exactly one of them wins the commit and the other re-plans against
the new snapshot and finds nothing left to do. The lease only saves the
loser the cost of a redundant scan and rewrite. The alternative HA shape,
running with the in-process scheduler disabled everywhere and driving
timing from an external Kubernetes `CronJob` with `concurrencyPolicy:
Forbid`, needs no lease at all, because there is only ever one caller in
flight.

## Audited and reversible

Every advisory-mode analysis and every active-mode commit emits an
`AuditKind::Maintenance` audit event. Job history, per-table last-run
state, and lease rows live in `sqe_system.maintenance_log`, a normal SQL
table an operator creates once and queries like any other: filter it by
status, table, or time range to see what the scheduler has actually done.

A compaction commit is an ordinary Iceberg snapshot. The files it
superseded stay in place until `expire_snapshots` removes them, which
means an autonomous compaction is reversible within the snapshot-retention
window. Read the table's snapshot history, find the snapshot before the
compaction landed, and run:

```sql
CALL system.rollback_to_snapshot(table => 'analytics.events', snapshot_id => 8472810294831234567);
```

That single call points the table back at the prior snapshot and undoes
the compaction, as long as that snapshot has not aged out.

## Getting started: advisory to active

1. **Check debt, no config changes required.** `CALL
   system.table_health(table => 'ns.t')` works regardless of
   `maintenance.mode`, on any session with `SELECT` on the table. Use it
   to see whether a table is worth compacting at all.
2. **Turn on advisory mode.** Set `[maintenance] mode = "advisory"`, add a
   `[maintenance.principal]` block, and set `scheduler.enabled = true`.
   `mode = "active"` comes later, in step 4. Let advisory mode run against
   your real tables long enough to see debt accumulate and validate the
   cron schedule.
3. **Opt one table in.** `ALTER TABLE ns.t SET TBLPROPERTIES
   ('sqe.maintenance.enabled' = 'true')`, and grant the maintenance
   principal `TABLE_WRITE_DATA` on that table's namespace in Polaris.
4. **Flip the global mode to `active`.** Only opted-in tables with the
   write grant are ever touched; every other table keeps behaving exactly
   as it did under `advisory`.
5. **Watch `sqe_system.maintenance_log`.** Confirm jobs land as
   `success`, not `skipped` or `failed`, before opting in more tables.

Run manual `CALL system.rewrite_data_files(...)` calls at any point in
this progression: the manual path and the autonomous path are the same
handler, and neither depends on the other being configured.
