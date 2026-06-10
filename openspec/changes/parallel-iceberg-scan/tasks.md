## 1. Config flag + scan partition count

- [ ] 1.1 Add `execution.parallel_scan` (bool, default `false`) in `sqe-core`
- [ ] 1.2 When enabled, derive N from `execution.target_partitions` (or core count) and a file-count / byte threshold (reuse distribution thresholds)
- [ ] 1.3 Split scan files into N file-group partitions via the existing bin-packing helper
- [ ] 1.4 Below the threshold, keep `target_partitions = 1` (no parallelism)

## 2. Partitioning-aware planner pass

- [ ] 2.1 Walk the physical plan; for each `IcebergScanExec`, inspect the parent operator
- [ ] 2.2 Parent is hash join / hash aggregate: set scan to `RoundRobinBatch(N)`, insert explicit `RepartitionExec(Hash(key), N)` between scan and parent
- [ ] 2.3 Parent has no distribution requirement (filter/project): set scan to `RoundRobinBatch(N)`, insert nothing
- [ ] 2.4 Parent requires single-partition ordering (global sort): leave `target_partitions = 1`
- [ ] 2.5 Recover the hash key from the join / aggregate node; fall back to round-robin when no key is recoverable
- [ ] 2.6 Wire `IcebergScanExec::with_target_partitions` and emit the chosen `Partitioning` (`crates/sqe-catalog/src/table_provider.rs:242-245`, `crates/sqe-catalog/src/iceberg_scan.rs`)

## 3. Plan-shape assertions (regression guard)

- [ ] 3.1 Unit test: q72-shaped plan under the flag contains no `CoalescePartitionsExec` directly above the scan
- [ ] 3.2 Unit test: the q72 hash join is `Partitioned`, not `CollectLeft`, under the flag
- [ ] 3.3 Unit test: a filter-only plan parallelizes with `RoundRobinBatch(N)` and no inserted exchange
- [ ] 3.4 Unit test: unrecognized parent leaves the scan serial (conservative default)

## 4. Benchmarks (gates)

- [ ] 4.1 TPC-DS q72 SF1 with `parallel_scan = true` <= 1.1x the baseline q72 = 756ms (`compare-tpcds-sf1-2026-05-28T14:19:18.json`)
- [ ] 4.2 Scan-bound query (TPC-H Q1 / Q6 / large `count(*)` with filter) speeds up >= 2x on a 4+ core coordinator vs the single-partition baseline
- [ ] 4.3 TPC-H SF1 suite does not regress against `tpch-sf1-flight-2026-04-02T14:16:27.json` (22/22, 37.5s)
- [ ] 4.4 Commit benchmark JSON to `benchmarks/results/`; flip `parallel_scan` default to `true` only after gates 4.1-4.3 pass

## 5. Phase 2 (optional, deferred)

- [ ] 5.1 Doris-style in-process local shuffle as a lower-overhead alternative to `RepartitionExec(Hash)`, behind the same flag
- [ ] 5.2 Adaptive partition-count sizing based on file sizes and core count
