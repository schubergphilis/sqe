**STATUS 2026-11-21: incremental scan skips compaction snapshots (`fix/incremental-scan-skip-replace`).**

`plan_incremental` (`crates/sqe-catalog/src/incremental_scan.rs`) collected
the added files of every snapshot in `(start, end]`, including `replace`
snapshots. Compaction re-adds rows that an earlier append already produced, so
a range over an append and its compaction returned those rows twice (10-row
append, compaction, 5-row append: 25 instead of 15). Found while designing
incremental refresh for materialized views, which depends on this range
being exact.

The new `change_set_snapshots` drops `replace` snapshots before files are
collected. `resolve_range` and `snapshots_in_range` are unchanged; the parent
walk still crosses the compaction.

Tests: unit tests on `change_set_snapshots`, and
`crates/sqe-coordinator/tests/it/incremental_replace_test.rs`, which commits
append / `rewrite_files` / append to a SQLite-backed catalog and checks the
planned row counts (0..C = 15, A..C = 5, A..B = 0). No compose stack needed
(`--features test-sqlite`).

Not covered: a position delete committed after a compaction targets the
rewritten file, which is not in the change set, so `reconcile_in_range_deletes`
drops it and the original row still comes back. Equality deletes are applied
by sequence number and are unaffected.

**NEXT:** decide whether in-range position deletes on compacted files should be
mapped back to the original rows (needs a path-to-path map from the replace
snapshot's manifests).
