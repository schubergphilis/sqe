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

- [x] 3c.1 `HiveScanExec`: one node owning the planned entries, resolved format,
      partition columns and full schema.
- [x] 3c.2 `HiveTableProvider::scan` emits it (wrapping the union for now; moving materialization inside is the follow-up).
      Partition materialization and sentinel nullification move inside it,
      next to the worker-side copy they must agree with.
- [x] 3c.3 Coordinator finds it by downcast and replaces it with a
      `DistributedScanExec` built from `build_hive_scan_tasks`. Opt-in behind
      `query.distributed_hive_scans`, default false.
- [x] 3c.4 Do NOT pattern-match the union shape as a shortcut. It matches on the
      output of two other functions plus the optimizer, and when a rule reshapes
      the plan the scan silently stops distributing -- no error, just an idle
      cluster.
- [x] 3c.5 Regression: the rows an unpartitioned and a multi-partition Hive
      table return are unchanged by the refactor, BEFORE any dispatch is added.

## 4. Differential correctness

- [x] 4.1 Harness: run a query coordinator-local and worker-distributed, compare rows.
      `crates/sqe-coordinator/tests/hive_diff/mod.rs`. In-process against a
      `LocalFileSystem` store, calling the same task builder and the same worker
      scan entry point production calls -- not a Flight cluster, which would add
      transport rather than correctness coverage.
- [x] 4.2 Parquet, partitioned, multi-partition. Plus a typed (Int64) partition
      key and `__HIVE_DEFAULT_PARTITION__`, both of which return rows either way
      when they are wrong.
- [x] 4.3 Predicate pushdown active (partition pruning must agree on both paths).
      Pruning happens once, on the coordinator, before either path exists, so
      both inherit the same survivors -- asserted, along with the surviving
      partition being the REQUESTED one. File and row-group pushdown INSIDE a
      distributed Hive task is absent, not broken: `predicate_proto` is `None`
      and the `FilterExec` above the scan still applies. Recorded in the
      reference page rather than left for a green check to imply.
- [x] 4.4 Projection subset, including a partition column only, and a file column
      only. **This found a real bug.** `try_distribute_hive` replaced a
      `HiveScanExec` carrying the QUERY's projection with a `DistributedScanExec`
      carrying the FULL schema. Every parent indexes its child by position, so
      `SELECT dt FROM t` returned `id`'s values under the name `dt` -- no error,
      wrong column. Fixed by `align_to_scan_schema`, and the test runs the real
      `replace_scan_in_plan` against the real parent plan so a re-introduction
      cannot pass.

## 5. CSV and JSON readers

- [x] 5.1 CSV reader on the worker honoring delimiter, quote, escape, header.
      `crates/sqe-worker/src/hive_reader.rs`. Schema is DECLARED, never inferred:
      inference is how two files in one table disagree on a type and how a header
      row becomes the type sample and then a data row.
- [x] 5.2 Line-delimited JSON reader.
- [x] 5.3 Differential tests per format. Same data in all three formats, so the
      expected row count is known rather than being whatever that format's
      reader produced. Plus a quoted-delimiter CSV case, which separates "read
      the delimiter" from "read the quoteChar".
- [x] 5.4 Decided: one task per file, read whole, no byte-range splitting.
      Reasoning in design.md under "Why CSV and JSON are one task per file",
      written as a decision rather than inherited from cycle 1's compressed-file
      note. The memory cost it accepts is stated there and in the reference page.
- [x] 5.5 Lift BOTH refusals together. `servability`'s parquet-only rule and the
      executor's matching `bail!` each independently made the new readers
      unreachable; leaving either would have kept every CSV table falling back,
      which looks exactly like success. Both are now exhaustive matches, so a
      format added later is a compile error rather than a corrupt-footer error.

## 6. Measurement

**Both open, deliberately.** The correctness work above is complete and lands
without them. A number from an uncounterbalanced run on a shared box is worse
than no number: this repo has already produced a 22% within-arm swing on
identical code, and a checked box with a weak number behind it reads as
"measured" forever.

- [ ] 6.1 Multi-worker rig, partitioned Hive table, against the cycle-2 coordinator-local baseline.
      Needs the native two-worker rig (workers must ACCEPT before the
      coordinator starts, or every query silently runs local) and a Hive dataset
      big enough for the partition count to matter.
- [ ] 6.2 Record in `docs/evidence/perf/`. Counterbalance the runs: a plain new-first/old-second comparison on a shared box has already proven worthless once (a 22% within-arm swing on identical code).

## 7. Docs

- [x] 7.1 Reference page: worker execution, the fallback contract, and what
      remains coordinator-local. New "Worker execution" section in
      `docs/site/book/src/reference/hive-external-tables.md`, with the fallback
      table and the marker's EXPLAIN output.
- [x] 7.2 Remove the cycle-5 limitation note once it is no longer true. The
      "Server mode runs Hive scans on the coordinator" limitation is replaced by
      the flag's default-off state, which is the accurate version. The
      compressed-CSV limitation is widened to all text files, because the reason
      turned out to be the absence of row groups, not compression.
- [x] 7.3 `nextsteps.md` and README roadmap.
