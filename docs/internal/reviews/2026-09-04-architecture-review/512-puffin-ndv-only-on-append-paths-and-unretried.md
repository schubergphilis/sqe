# 512: Puffin NDV sidecars are emitted only by append-shaped writes; every rewrite drops statistics, and the statistics commit is not retried

- Scope: sqe-coordinator write path and maintenance (write_handler Puffin hooks, maintenance.rs rewrite commits)
- Severity: medium
- Category: performance
- Effort: M (days)
- Status: open
- Issue: #604

## Problem

The read side binds statistics to the snapshot:
`crates/sqe-catalog/src/table_provider.rs:377` calls
`for_snapshot(snapshot_id, metadata.statistics_for_snapshot(snapshot_id))`, and
`puffin_stats.rs:146` only accepts blobs whose `snapshot_id` matches. So a
snapshot without its own statistics file has no NDV estimates at all.

Sidecars are written from exactly four places, all append-shaped:
`crates/sqe-coordinator/src/write_handler.rs:1168` (buffered CTAS, dead code),
`:1389` (streaming CTAS), `:1550` (streaming INSERT, including INSERT
OVERWRITE where the sketch covers only the new rows, not the surviving
partitions), `:1870` (buffered INSERT, dead code). Nothing on:

- CoW DELETE/UPDATE (`:2284-2322`, `:2943-2982`), CoW MERGE (`:3654-3668`),
- MoR DELETE/UPDATE/MERGE row-delta commits (`:2480-2496`, `:3266-3294`,
  `:4257-4291`),
- `commit_written_files` overwrite branch (`:4385-4453`),
- compaction `rewrite_data_files_once` / `commit_aggregated_batch`
  (`crates/sqe-coordinator/src/maintenance.rs:1240-1290`, `:2270-2300`),
- ingest (`write_handler.rs:2059-2080`).

Every one of those produces a new current snapshot with no statistics, so a
table that had NDV after its load loses it on the first DELETE or the first
scheduled compaction, until the next INSERT happens to run.

Separately, the statistics commit at `write_handler.rs:1003-1013` is a plain
`tx.commit`, not `commit_with_retry`. A concurrent writer between the data
commit and the statistics commit turns it into a lost-conflict path that
deletes the staged sidecar (`cleanup_staged`) and logs at warn.

## Why it matters

NDV estimates drive join ordering and the memory note "Puffin NDV consumer"
records real plan differences with and without them. Statistics that
disappear after routine maintenance make plan quality time-dependent: the
same query is fast after a load and slow after the nightly compaction. Under
concurrent load the append paths lose statistics too. Operators have no signal
either way (the warn is the only trace).

## Proposed change

1. Carry forward: in every rewrite/row-delta commit, if the previous snapshot
   had a statistics file, re-register it for the new snapshot when the write
   did not change row identity for the sketched columns (compaction,
   rewrite_manifests), or merge sketches: theta sketches union cheaply, so
   INSERT can union the previous blob with the new sketch; DELETE/UPDATE can
   keep the old NDV as an upper bound with a flag.
2. Move sidecar emission into the shared commit helper from 506 so any new
   commit site inherits it.
3. Route the `UpdateStatistics` commit through `commit_with_retry`.
4. Emit `sqe_puffin_sidecar_total{op,outcome}` so lost statistics are visible.

## Acceptance criteria

- Test: CTAS with `write.puffin.stats = true`, then `DELETE ... WHERE`, then
  `SHOW STATS` still reports NDV for untouched columns.
- Test: `CALL system.rewrite_data_files` on a table with statistics leaves the
  new snapshot with a statistics file.
- Test with a forced conflict on the statistics commit: the sidecar is
  registered after retry, no orphan `.stats` object remains.

## Related

- Memory notes "Puffin sidecar stale under compaction" and "Puffin NDV
  consumer: streaming CTAS/INSERT never wrote sidecars" (the latter fixed; this
  is the remaining gap).
- 506 (shared commit helper).
