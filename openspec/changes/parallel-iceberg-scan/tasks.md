## 1. Config flag + scan partition count

- [x] 1.1 Add `execution.parallel_scan` (bool, default `false`) in `sqe-core` (lands as `query.parallel_scan`; sqe-core has no `[execution]` section, the sibling `parallel_probe_scan` lives under `[query]`)
- [x] 1.2 When enabled, derive N from `execution.target_partitions` (or core count) and a file-count / byte threshold (reuse distribution thresholds) -- N from `config.execution.target_partitions`; byte threshold reuses `query.distribution_threshold`. File-count threshold not applied: the rule runs post-planning without the file list (only cached manifest byte size is available synchronously)
- [x] 1.3 Split scan files into N file-group partitions via the existing bin-packing helper -- the split already lives inside `IcebergScanExec::execute` (size-descending round-robin slice per partition, `iceberg_scan.rs:886-912`); the rule reuses it via `with_target_partitions` rather than the distributed `splitter` bin-packer
- [x] 1.4 Below the threshold, keep `target_partitions = 1` (no parallelism)

## 2. Partitioning-aware planner pass

Approach note (deviation from the design doc, verified empirically): the design
proposed placing `RepartitionExec` explicitly and NOT relying on
`EnforceDistribution`. That assumed the scan reaches its join through a
pre-existing exchange. It does not: an `IcebergScanExec` is always one partition
when `EnforceDistribution` first runs, so a `Partitioned` join ends up directly
over its scan inputs with no repartition between (printed and asserted in
`enforce_distribution_reconciles_partitioned_join`). Placing a `RepartitionExec(Hash)`
per scan by hand then cannot keep both join inputs at the same partition count
(bump only the fact side and the join sees N vs 1, which is invalid). The
faithful variant achieving the same success criteria is: bump every qualifying
non-build scan to `RoundRobinBatch(N)` and re-run `EnforceDistribution` once. It
inserts `RepartitionExec(Hash(key), N)` above BOTH sides of a mismatched
`Partitioned` join, keeps the join `Partitioned`, and never puts a
`CoalescePartitionsExec` above a scan. This is the mechanism the sibling #235
rule (`ParallelProbeScanRule`) already ships and that q72 was validated against.

- [x] 2.1 Walk the physical plan; for each `IcebergScanExec`, inspect the parent operator -- implemented as a taint walk over `required_input_distribution` (`crates/sqe-coordinator/src/parallel_scan.rs`)
- [x] 2.2 Parent is hash join / hash aggregate: set scan to `RoundRobinBatch(N)`, insert explicit `RepartitionExec(Hash(key), N)` between scan and parent -- realized via bump + `EnforceDistribution` re-run (see approach note): the re-run inserts the `Hash` repartition on both `Partitioned`-join sides and the `Hash`/coalesce before a final aggregate. Partial-aggregate and `Partitioned`-join scans are covered
- [x] 2.3 Parent has no distribution requirement (filter/project): set scan to `RoundRobinBatch(N)` -- filter/projection scans are collected as bumpable; the re-run adds only a root coalesce (via `execute_stream`), no exchange between scan and filter
- [x] 2.4 Parent requires single-partition ordering (global sort): leave `target_partitions = 1` -- a `SinglePartition` requirement or a required input ordering taints the subtree, so those scans are excluded
- [x] 2.5 Recover the hash key from the join / aggregate node; fall back to round-robin when no key is recoverable -- not needed: `EnforceDistribution` derives the hash key from the join node during the re-run
- [x] 2.6 Wire `IcebergScanExec::with_target_partitions` and emit the chosen `Partitioning` (`crates/sqe-catalog/src/iceberg_scan.rs`) -- setter now advertises `RoundRobinBatch(N)` instead of `UnknownPartitioning(N)`; `table_provider.rs` auto-wiring stays disabled (the pass runs post-planning)

## 3. Plan-shape assertions (regression guard)

- [x] 3.1 Unit test: q72-shaped plan under the flag contains no `CoalescePartitionsExec` directly above the scan -- `enforce_distribution_reconciles_partitioned_join` asserts no `CoalescePartitionsExec` after the re-run
- [x] 3.2 Unit test: the q72 hash join is `Partitioned`, not `CollectLeft`, under the flag -- same test asserts `mode=Partitioned` with a `Hash` repartition above both inputs
- [x] 3.3 Unit test: a filter-only plan parallelizes with `RoundRobinBatch(N)` and no inserted exchange -- `filter_passes_scan_through` (the filter scan is collected as bumpable)
- [x] 3.4 Unit test: unrecognized parent leaves the scan serial (conservative default) -- `collect_left_excludes_build_includes_probe`, `nested_collect_left_collects_only_the_fact_probe`, `global_sort_excludes_scan`, `ordering_requiring_parent_excludes_scan`

Note: the guard-decision tests assert the pure taint walk (`collect_non_build_leaves`) on stand-in `LazyMemoryExec` leaves, mirroring the sibling `parallel_probe_scan` tests, because constructing a live Iceberg `Table` for a real `IcebergScanExec` in a unit test is impractical (iceberg-rust is a git dependency with no in-memory table builder here). The end-to-end reconciliation test uses real DataFusion `MemTable`s to validate the `EnforceDistribution` behaviour the rule depends on.

Deferred (out of scope for sections 1-3, belongs to the benchmark-gate phase): a
bare `SELECT count(*) ... WHERE` whose only consumer is a single-partition
aggregate collapses to an all-single-partition plan before the pass runs, so its
scan is tainted and left serial. Parallelizing it would require the pass to see
past the collapse.

## 4. Benchmarks (gates)

- [ ] 4.1 TPC-DS q72 SF1 with `parallel_scan = true` <= 1.1x the baseline q72 = 756ms (`compare-tpcds-sf1-2026-05-28T14:19:18.json`)
- [ ] 4.2 Scan-bound query (TPC-H Q1 / Q6 / large `count(*)` with filter) speeds up >= 2x on a 4+ core coordinator vs the single-partition baseline
- [ ] 4.3 TPC-H SF1 suite does not regress against `tpch-sf1-flight-2026-04-02T14:16:27.json` (22/22, 37.5s)
- [ ] 4.4 Commit benchmark JSON to `benchmarks/results/`; flip `parallel_scan` default to `true` only after gates 4.1-4.3 pass

## 5. Phase 2 (optional, deferred)

- [ ] 5.1 Doris-style in-process local shuffle as a lower-overhead alternative to `RepartitionExec(Hash)`, behind the same flag
- [ ] 5.2 Adaptive partition-count sizing based on file sizes and core count
