**STATUS 2026-08-27: Hive external tables, cycle 5 (worker execution) complete except measurement, on `feat/hive-cycle5-finish`.**

Hive scans run on workers behind `query.distributed_hive_scans` (default false). One `ScanTask` per surviving partition, built from the cycle-2 partition index; the coordinator lists the survivors so placement has sizes. All three formats now have worker-side readers: parquet as before, CSV and line-delimited JSON new in `sqe-worker/src/hive_reader.rs`, both bound to the DECLARED file schema rather than an inferred one. A scan that cannot distribute runs coordinator-local and plain `EXPLAIN` prints why, because a silent fallback returns correct rows and leaves the cluster idle.

The differential suite (`crates/sqe-coordinator/tests/hive_differential_test.rs`) found a real bug that no row-count check would have: `try_distribute_hive` replaced a `HiveScanExec` carrying the query's projection with a `DistributedScanExec` carrying the FULL schema, and every parent indexes its child by position, so `SELECT dt FROM t` returned `id`'s values under the name `dt`. Fixed by `align_to_scan_schema`, with the test running the real substitution against the real parent plan.

**Open in cycle 5:** the measurement (6.1, 6.2). Deliberately unchecked -- an uncounterbalanced run on a shared box has already produced a 22% within-arm swing on identical code here, and a weak number behind a checked box reads as "measured" forever. Also open, and recorded rather than implied: no predicate or projection pushdown INSIDE a distributed Hive task, and CSV/JSON read each file whole.

**NEXT: cycle 4, writes into partition directories.** No atomic commit exists for a Hive table (there is no metadata pointer to swap), and a write invalidates the cycle-2 partition index. Both belong in the design, not a footnote.

---
