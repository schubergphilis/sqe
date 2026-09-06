# 511: Scheduler tick loads every table in the catalog, runs compactions inline on the tick task, bursts missed ticks, and grows the ledger without bound

- Scope: sqe-coordinator maintenance (maintenance_scheduler.rs, maintenance_log.rs, maintenance_lease.rs)
- Severity: medium
- Category: operability
- Effort: M (days)
- Status: fixed (branch `fix/data-consistency-p1`, 2026-09-05)
- Issue: #603

## Problem

Four compounding behaviours in the scheduler loop.

1. Discovery is O(all tables) per tick.
   `crates/sqe-coordinator/src/maintenance_scheduler.rs:242-283`: every tick
   lists every namespace, lists every table, and `load_table`s each one to read
   its properties, only to filter by `sqe.maintenance.enabled` afterwards
   (`select_enabled`, `:284`). With `tick_secs = 60` and 5,000 tables that is
   5,000 metadata.json GETs a minute from each coordinator, before any table is
   due.

2. Compaction runs inline on the tick.
   `maintenance_scheduler.rs:318-330`: `active_one_table(...).await` is
   awaited sequentially per due table inside `advisory_tick`, which itself
   runs inside the `interval` loop (`:1086-1104`). A 40-minute rewrite of one
   table delays health reporting and compaction of every other table on this
   coordinator by 40 minutes.

3. Missed ticks burst. `tokio::time::interval` at `:1089` is used with the
   default `MissedTickBehavior::Burst`; no `set_missed_tick_behavior` anywhere
   in the file (`grep` returns nothing). After the 40-minute job above, 40
   ticks fire back to back, each doing the full discovery in (1).

4. The ledger only grows. Every due table that has no debt writes a
   `skipped` row (`:505-515`, `record_skipped_job`), every job writes running
   and terminal rows, and every lease acquire/renew/release writes a row
   (`maintenance_lease.rs` module docs). Each row is one Parquet file appended
   to `sqe_system.maintenance_log`. Lease operations then decode every live
   file in that table on each call (`maintenance_lease.rs:75-90`, "O(total rows
   ever written to the ledger)"), several times per compaction. Nothing
   compacts or expires the ledger; `grep -n 'expire\|rewrite_data_files' maintenance_log.rs`
   finds only doc comments.

## Why it matters

In a multi-coordinator deployment each coordinator repeats (1) independently,
so catalog load scales with coordinators x tables per minute. A single slow
table starves the fleet's compaction schedule, and the post-job burst makes
the coordinator's catalog client the noisiest tenant on Polaris exactly when
it is recovering. The ledger growth turns lease acquisition, which runs on the
critical path of every scheduled compaction, into a linearly slowing scan,
while the lease's own documented purpose is to save work.

## Proposed change

1. Discovery: cache `(ident, properties)` per tick with a TTL of several ticks
   and only `load_table` tables that are due or newly seen; use
   `list_tables` results as the change signal. Where the catalog is Polaris,
   the event-driven invalidation added in !683 can drive the refresh.
2. Execution: hand each due table to a bounded `JoinSet` (concurrency from
   `MaintenanceCompactionConfig`, default 1 to preserve today's serialization)
   and let the tick return immediately; a table already in flight is skipped
   on the next tick.
3. `ticker.set_missed_tick_behavior(MissedTickBehavior::Skip)` in `spawn` and
   in `run_with_lease_renewal`.
4. Ledger: do not write a `skipped` row when the previous row for the table
   was also `skipped` with the same reason inside the same schedule window;
   add a retention procedure (`maintenance_log` rows older than N days
   rewritten into one file per day) and run it from the scheduler itself; move
   lease rows into their own partition or a separate table so lease reads stop
   scanning job history.

## Acceptance criteria

- Test with a `catalog_factory` fake recording `load_table` calls: two
  consecutive ticks with no property change call `load_table` at most once per
  table.
- Test: a tick whose one table's rewrite takes longer than `tick_secs`
  does not delay the next tick's health gauges for other tables.
- Test: 100 ticks with no debt write at most one `skipped` row per table per
  schedule window.
- `grep -n set_missed_tick_behavior crates/sqe-coordinator/src/maintenance_scheduler.rs`
  returns two hits.

## Related

- Memory note "Catalog cache refresh: event-driven invalidation !683".
- 510 (authorization for the retention procedure).

## Resolution (2026-09-05)

All four arms fixed on `fix/data-consistency-p1`. The shape of the change:
a tick now DECIDES and hands off; the job set does the work.

### 1. Discovery is cached across ticks

`advisory_tick`'s inline discovery loop moved to
`MaintenanceScheduler::discover_tables`, backed by a new `DiscoveryCache`
(`(ident, properties)` keyed on `ident.to_string()`, held in the scheduler
so it survives ticks). Every tick still lists every namespace and table --
that listing IS the change signal: a newly listed table is loaded on the
spot, a table that stops being listed is evicted. What no longer runs every
tick is `load_table`: a cached entry younger than
`[maintenance.scheduler] discovery_cache_ticks` (new key, default `5`, `0`
treated as `1`) is reused as-is.

Staleness is bounded and harmless by construction: the cached properties
only ever decide WHETHER and WHEN to schedule, and the job itself re-loads
the table and re-resolves its `sqe.maintenance.*` overrides from that fresh
copy, so no rewrite can run with stale parameters. A `load_table` failure
keeps the existing entry rather than dropping the table for that tick.

### 2. Jobs run on a bounded `JoinSet`, not on the tick

Each due table is spawned into `MaintenanceScheduler::jobs` (a
`JoinSet<()>`), bounded by a semaphore of
`[maintenance.scheduler] max_concurrent_jobs` permits. That key already
existed with default `1` and was wired to nothing; it is now the concurrency
knob, so the default preserves today's one-job-at-a-time serialization while
the tick itself returns immediately. A table whose job is still in flight is
SKIPPED by the next tick (RAII `InflightGuard`, released on return, panic,
or abort) rather than queued a second time -- queueing would let a table
slower than its schedule period build an unbounded backlog of jobs
compacting debt the previous one already handled.

Two consequences worth knowing:

- `advisory_tick` returning no longer means the work finished. Tests and
  graceful shutdown use the new `MaintenanceScheduler::drain_inflight_jobs`;
  the supervised loop calls `abort_inflight_jobs` on cancellation, which is
  what the pre-511 code did implicitly by dropping the in-flight tick.
- Finished jobs are reaped at the top of each tick (`try_join_next`), so the
  `JoinSet` does not accumulate completed entries in a process that never
  drains.

### 3. Missed ticks skip

`ticker.set_missed_tick_behavior(MissedTickBehavior::Skip)` in both interval
loops -- `spawn` and `run_with_lease_renewal`. `grep -n
set_missed_tick_behavior crates/sqe-coordinator/src/maintenance_scheduler.rs`
returns two hits.

### 4. Ledger

- **Skip dedup.** `record_skipped_job` writes a `skipped` row only when the
  skip is new information: a different schedule window (the cron fire
  instant, from the new `schedule_window_start_ms`, which `table_due` now
  shares its implementation with) or a different reason than the last row
  this process wrote for that table. State is one entry per table
  (`SkipLedger`), bounded by table count, not tick count. The metric and the
  audit event are deliberately NOT deduped: those are aggregate-shaped
  signals an operator reads as rates, while the ledger row is the
  one-file-per-write one. So `sqe_maintenance_job_total{status="skipped"}`
  counts skip DECISIONS and the ledger records skip WINDOWS; that difference
  is the fix, not a disagreement.
- **Retention.** New `maintenance_log::compact_history(catalog, state_table,
  retention_days, now_ms)`: rows older than `retention_days` are rewritten
  into one data file per UTC day in a single `rewrite_files` CAS commit
  (`set_check_file_existence(true)`, so two coordinators running it
  concurrently cannot both win -- the loser cleans up its files and reports
  a no-op; no lease is taken). It is a compaction, not an expiry: every row
  in every deleted file is re-written, so no history disappears. It never
  touches a file holding a recent row, and never one holding a lease row
  (`trigger = "lease"`) -- moving a live claim into a day bundle would break
  the exact-path delete the next `renew`/`release` CAS depends on. The
  scheduler calls it once per UTC day (first tick after start, then daily),
  spawned into the job set WITHOUT taking a compaction permit so a
  housekeeping pass cannot block compaction at `max_concurrent_jobs = 1`.
  `HISTORY_RETENTION_DAYS = 7` is the scheduler's cadence default; the
  procedure itself takes `retention_days` as a parameter for a future
  operator-driven `CALL` (see 510 for that surface's authorization).
  Planning runs off manifest bounds (`started_at_ms` for the day buckets,
  `trigger` for lease-freeness), so a pass reads only the files it is about
  to rewrite -- the steady state, an already-one-file-per-day ledger, costs
  zero file reads. That matters because this runs daily forever against a
  table that only grows; a decode-everything pass would have replaced
  unbounded file growth with an unbounded daily read. One pass rewrites at
  most `MAX_FILES_PER_PASS = 5000` files (any subset is safe -- whole files
  in, all their rows out), so a large backlog drains over several days
  instead of in one enormous commit.
- **Lease reads stop scanning job history.** Done without a schema
  migration, via the manifest column bounds Iceberg already records:
  `read_lease_rows` prunes any data file whose `trigger` lower/upper bounds
  prove it holds no lease row (`file_excludes_lease_rows`), so a lease
  operation no longer pays an object-store GET plus a Parquet decode per row
  the ledger ever accumulated. Absent bounds mean "unknown" and the file is
  read, and truncated string bounds only widen the range, so a file that
  really holds a lease row can never be pruned. `maintenance_log`'s
  retention pass asks the same predicate for the opposite reason, so the two
  cannot disagree.

#### Left as a described migration (not done)

The proposal's "move lease rows into their own partition or a separate
table" is NOT implemented: `sqe_system.maintenance_log` is created
out-of-band by the operator (the whole module is built around never needing
a `CREATE TABLE` grant), so either variant is a migration of an
operator-owned table rather than a code change. If it is ever wanted, the
shape is:

1. `ALTER TABLE sqe_system.maintenance_log ADD PARTITION FIELD identity(trigger)`
   -- a partition-spec evolution, not a schema change, so existing rows stay
   readable under spec 0 while new writes land under spec 1.
2. Lease reads then filter partitions instead of files, and can drop the
   bounds pruning added here; existing (spec 0) files still need it until
   they age out through `compact_history`.
3. The operator's documented `CREATE TABLE` DDL in `maintenance_log.rs`'s
   module docs gains `PARTITIONED BY (trigger)`, and the mismatch between
   an old unpartitioned table and a new partitioned one has to be tolerated
   (the appender writes through `write_data_files`, which reads the spec
   from the loaded table, so both work -- but this needs a test).

Given the bounds pruning above already removes the per-file reads, and
retention removes the file growth that made the scan expensive, the
migration buys only the manifest scan itself. It was not worth an
operator-visible DDL change here.

### Config keys

- `[maintenance.scheduler] max_concurrent_jobs` (existing, default `1`) --
  now actually wired: the job-set concurrency bound.
- `[maintenance.scheduler] discovery_cache_ticks` (new, default `5`) --
  how many ticks cached table properties are reused before a re-`load_table`.

### Tests

- `crates/sqe-coordinator/src/maintenance_scheduler.rs`:
  `discovery_cache_e2e::two_consecutive_ticks_load_each_table_once` (the
  acceptance criterion, with a `catalog_factory` fake recording `load_table`
  calls -- it lives in the lib, not `tests/`, because `async-trait` is a
  normal dependency and an integration test cannot implement the `Catalog`
  trait), `discovery_cache_ticks_of_one_loads_every_tick`,
  `a_newly_listed_table_is_loaded_on_the_next_tick`, plus the pure
  `discovery_cache_*`, `skip_ledger_*`, `inflight_guard_*` and
  `schedule_window_start_ms_*` unit tests.
- `crates/sqe-coordinator/tests/maintenance_scheduler_test.rs`:
  `a_parked_job_does_not_delay_the_next_ticks_gauges_for_other_tables`,
  `a_hundred_idle_ticks_write_at_most_one_skipped_row_per_window`,
  `history_retention_rewrites_aged_rows_into_one_file_per_day`,
  `history_retention_leaves_lease_rows_alone`.
- `crates/sqe-coordinator/src/maintenance_lease.rs`:
  `bounds_pruning_e2e::job_history_files_are_pruned_and_lease_files_are_not`
  -- guards against the pruning silently becoming a no-op if the writer ever
  stops recording column bounds (it is a pure optimisation, so nothing else
  would notice).
- `crates/sqe-coordinator/src/maintenance_log.rs`: `plan_history_rewrite_*`
  (including the "a rewritten file's lonely day comes along too" case that
  would otherwise drop rows, and idempotency), `rows_to_record_batch`
  round-trip, `day_of_*`.
