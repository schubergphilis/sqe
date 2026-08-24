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

- [ ] 3.1 Split a Hive scan into per-partition tasks from the cycle-2 partition index.
- [ ] 3.2 Decide worker-servability per table; record the decision.
- [ ] 3.3 Fall back to coordinator-local when not servable, and surface it in `EXPLAIN`.
- [ ] 3.4 Test: an unservable table runs coordinator-local AND is observably marked as such. A test that only checks the rows would pass on a silent fallback.

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
