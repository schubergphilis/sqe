# Cycle 5 tasks

## 1. Wire format

- [x] 1.1 `HiveScanDescriptor` in `scan_task.rs`: format, SerDe params, partition values, partition column order, file schema.
- [x] 1.2 Optional `hive` field on `ScanTask`, `skip_serializing_if = "Option::is_none"`, version bumped to 3.
- [x] 1.3 Test: an Iceberg `ScanTask` serializes byte-identically to its v2 encoding. This is the regression guard for the whole change.
- [x] 1.4 Test: a v3 worker accepts a v2 ticket, and a v2 worker rejects a v3 Hive ticket by version rather than misreading it.

## 2. Worker: partition materialization (Parquet only)

- [x] 2.1 Rebuild `attach_partition_values`' projection worker-side from the descriptor.
- [x] 2.2 Reuse `partition_scalar`'s typing exactly. Extract it to a shared crate rather than reimplementing: two copies of this typing rule WILL drift, and the symptom is a partition column changing type between execution paths.
- [x] 2.3 Test: `__HIVE_DEFAULT_PARTITION__` and a typed (non-string) partition key both materialize identically to the coordinator-local path.

## 3. Coordinator: task splitting

- [x] 3.1 Split a Hive scan into per-partition tasks from the cycle-2 partition index. (pure builder + tests; the listing that feeds it is still to wire)
- [x] 3.2 Decide worker-servability per table; record the decision. (decision function + reasons; recording into EXPLAIN is 3.3)
- [x] 3.3 Fall back to coordinator-local when not servable, and surface it in `EXPLAIN`. (marker node + reason; provider wiring pending the dispatch path)
- [x] 3.4 Test: an unservable table runs coordinator-local AND is observably marked as such. A test that only checks the rows would pass on a silent fallback.

## 3b. Worker-side execution of a Hive task

- [x] 3b.1 Read `task.hive` in the worker scan path and report the FULL schema.
- [x] 3b.2 Call `append_partition_columns` BEFORE the batch's `BytePermit` is
      acquired. See design.md: doing it as a stream transform afterwards
      under-accounts memory, because the permit was sized to the pre-partition
      batch and `Accounted::get_mut` will not complain.
- [x] 3b.3 Test: the permit's logical size covers the materialized batch, not
      the file-columns-only batch.
- [x] 3b.4 Refuse a v3 Hive task whose format the worker cannot read, with the
      reason, rather than failing inside a parquet footer read.

## 3c. Give Hive a scan node (prerequisite for everything below)

Discovered while wiring 3.1: the coordinator finds what to distribute by
`downcast_ref::<IcebergScanExec>()`, and a Hive scan is a UnionExec of
projections over ListingTable scans with no SQE-owned node in it. See design.md.

- [ ] 3c.1 `HiveScanExec`: one node owning the planned entries, resolved format,
      partition columns and full schema.
- [ ] 3c.2 `HiveTableProvider::scan` emits it instead of building the union.
      Partition materialization and sentinel nullification move inside it,
      next to the worker-side copy they must agree with.
- [ ] 3c.3 Coordinator finds it by downcast and replaces it with a
      `DistributedScanExec` built from `build_hive_scan_tasks`.
- [ ] 3c.4 Do NOT pattern-match the union shape as a shortcut. It matches on the
      output of two other functions plus the optimizer, and when a rule reshapes
      the plan the scan silently stops distributing -- no error, just an idle
      cluster.
- [ ] 3c.5 Regression: the rows an unpartitioned and a multi-partition Hive
      table return are unchanged by the refactor, BEFORE any dispatch is added.

## 4. Differential correctness

- [ ] 4.1 Harness: run a query coordinator-local and worker-distributed, compare rows.
- [ ] 4.2 Parquet, partitioned, multi-partition.
- [ ] 4.3 Predicate pushdown active (partition pruning must agree on both paths).
- [ ] 4.4 Projection subset, including a partition column only, and a file column only.

## 5. CSV and JSON readers

- [ ] 5.1 CSV reader on the worker honoring delimiter, quote, escape, header.
- [ ] 5.2 Line-delimited JSON reader.
- [ ] 5.3 Differential tests per format.
- [ ] 5.4 Decide and document byte-range splitting for CSV/JSON, or state that they stay one task per file. Cycle 1 already notes compressed CSV/JSON is one file per thread; do not silently inherit that as if it were designed here.

## 6. Measurement

- [ ] 6.1 Multi-worker rig, partitioned Hive table, against the cycle-2 coordinator-local baseline.
- [ ] 6.2 Record in `docs/evidence/perf/`. Counterbalance the runs: a plain new-first/old-second comparison on a shared box has already proven worthless once (a 22% within-arm swing on identical code).

## 7. Docs

- [ ] 7.1 Reference page: worker execution, the fallback contract, and what remains coordinator-local.
- [ ] 7.2 Remove the cycle-5 limitation note once it is no longer true.
- [ ] 7.3 `nextsteps.md` and README roadmap.
