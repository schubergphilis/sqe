# 511: Scheduler tick loads every table in the catalog, runs compactions inline on the tick task, bursts missed ticks, and grows the ledger without bound

- Scope: sqe-coordinator maintenance (maintenance_scheduler.rs, maintenance_log.rs, maintenance_lease.rs)
- Severity: medium
- Category: operability
- Effort: M (days)
- Status: open

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
