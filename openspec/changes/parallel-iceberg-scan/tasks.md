## 1. Config flag + scan partition count

- [x] 1.1 Add `execution.parallel_scan` (bool, default `false`) in `sqe-core` (lands as `query.parallel_scan`; sqe-core has no `[execution]` section, the sibling `parallel_probe_scan` lives under `[query]`)
- [x] 1.2 When enabled, derive N from `execution.target_partitions` (or core count) and a file-count / byte threshold (reuse distribution thresholds) -- N from `config.execution.target_partitions`; byte threshold reuses `query.distribution_threshold`. File-count threshold not applied: the rule runs post-planning without the file list (only cached manifest byte size is available synchronously)
- [x] 1.3 Split scan files into N file-group partitions via the existing bin-packing helper -- the split already lives inside `IcebergScanExec::execute` (size-descending round-robin slice per partition, `iceberg_scan.rs:886-912`); the rule reuses it via `with_target_partitions` rather than the distributed `splitter` bin-packer
- [x] 1.4 Below the threshold, keep `target_partitions = 1` (no parallelism)

## 2. Partitioning-aware planner pass

- [x] 2.1 Walk the physical plan; for each `IcebergScanExec`, inspect the parent operator (`crates/sqe-coordinator/src/parallel_scan.rs`)
- [x] 2.2 Parent is hash join / hash aggregate: set scan to `RoundRobinBatch(N)`, insert explicit `RepartitionExec(Hash(key), N)` between scan and parent -- driven by the parent's `required_input_distribution`: `HashPartitioned(keys)` (direct Partitioned-join child) inserts the repartition; the production Partitioned-join shape has the `RepartitionExec(Hash)` already placed by `EnforceDistribution`, so the scan sits under it and is bumped with no insert. Partial aggregates are left serial (deferred; bumping without a guaranteed final-merge boundary returns partial results)
- [x] 2.3 Parent has no distribution requirement (filter/project): set scan to `RoundRobinBatch(N)`, insert nothing (only when the parallelism is absorbed above; see the `Ctx` model)
- [x] 2.4 Parent requires single-partition ordering (global sort): leave `target_partitions = 1` (falls out of `SinglePartition` / `required_input_ordering`)
- [x] 2.5 Recover the hash key from the join / aggregate node; fall back to round-robin when no key is recoverable -- keys come directly from `Distribution::HashPartitioned(keys)`; no join node introspection needed
- [x] 2.6 Wire `IcebergScanExec::with_target_partitions` and emit the chosen `Partitioning` (`crates/sqe-catalog/src/iceberg_scan.rs`) -- setter now advertises `RoundRobinBatch(N)` instead of `UnknownPartitioning(N)`. `table_provider.rs` auto-wiring stays disabled (the pass runs post-planning)

## 3. Plan-shape assertions (regression guard)

- [x] 3.1 Unit test: q72-shaped plan under the flag contains no `CoalescePartitionsExec` directly above the scan -- asserted via the decision function (`BumpHash` inserts a `RepartitionExec(Hash)`, never a coalesce)
- [x] 3.2 Unit test: the q72 hash join is `Partitioned`, not `CollectLeft`, under the flag -- the pass never rebuilds the join node, so a `Partitioned` join stays `Partitioned`
- [x] 3.3 Unit test: a filter-only plan parallelizes with `RoundRobinBatch(N)` and no inserted exchange
- [x] 3.4 Unit test: unrecognized parent leaves the scan serial (conservative default) -- also covered: `CollectLeft` build side (q72 guard), ordering-requiring parent, and blocked pipeline

Note: plan-shape tests assert the pure decision function on stand-in leaves (mirroring the sibling `parallel_probe_scan` tests), because constructing a live Iceberg `Table` for a real `IcebergScanExec` in a unit test is impractical (iceberg-rust is a git dependency with no in-memory table builder available here).

## 4. Benchmarks (gates)

- [ ] 4.1 TPC-DS q72 SF1 with `parallel_scan = true` <= 1.1x the baseline q72 = 756ms (`compare-tpcds-sf1-2026-05-28T14:19:18.json`)
- [ ] 4.2 Scan-bound query (TPC-H Q1 / Q6 / large `count(*)` with filter) speeds up >= 2x on a 4+ core coordinator vs the single-partition baseline
- [ ] 4.3 TPC-H SF1 suite does not regress against `tpch-sf1-flight-2026-04-02T14:16:27.json` (22/22, 37.5s)
- [ ] 4.4 Commit benchmark JSON to `benchmarks/results/`; flip `parallel_scan` default to `true` only after gates 4.1-4.3 pass

## 5. Phase 2 (optional, deferred)

- [ ] 5.1 Doris-style in-process local shuffle as a lower-overhead alternative to `RepartitionExec(Hash)`, behind the same flag
- [ ] 5.2 Adaptive partition-count sizing based on file sizes and core count
