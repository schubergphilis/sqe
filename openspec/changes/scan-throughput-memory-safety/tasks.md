# Tasks: Scan Throughput + Memory Safety

## 0. Baselines and instrumentation (prerequisite for every gate)

- [ ] 0.1 Freeze the 2026-07-06 rig baselines as the comparison set
      (`benchmarks/results/compare-{tpch,ssb,tpcds}-sf10-2026-07-06*.json`,
      SSB per-query walls + the measured 4.2-5.0 core utilization)
- [ ] 0.2 Per-query memory log line: pool reserved at end, process RSS,
      delta vs start (INFO, coordinator)
- [ ] 0.3 Pool-reservation dump by consumer name (debug endpoint or command)
- [ ] 0.4 `sqe-metrics` gauges for RSS + pool reserved (scrapeable during
      rig sweeps)

## 1. Phase A: parallel scan output

- [ ] 1.1 Execute `openspec/changes/parallel-iceberg-scan/tasks.md` sections
      1-3 (flag + partition count, partitioning-aware planner pass,
      plan-shape assertions) unchanged
- [ ] 1.2 Memory clamp: bound N by
      `pool_free x clamp_fraction / est_partition_footprint`; config knob for
      the fraction; unit tests for clamp-to-1 and no-clamp-when-plentiful
- [ ] 1.3 Register per-subtask decode buffers against the pool via a
      `MemoryReservation` sized to channel capacity x batch size (vendor-glue
      only; add to the vendor re-apply list)
- [ ] 1.4 Execute `parallel-iceberg-scan` tasks section 4 (q72 gate, 2x
      scan-bound speedup, TPC-H SF1 no-regress), plus:
- [ ] 1.5 Rig gate: SSB SF10 q2.x/q3.x core utilization >= 6.5/8 and suite
      total >= 0.95x Trino on the `BENCH_WAREHOUSE=external` recipe
- [ ] 1.6 Retire the standalone `parallel-iceberg-scan` change folder
      (absorbed here); note it in its proposal header

## 2. Phase B: tracked write sink

- [ ] 2.1 Wrap the ingest/CTAS sink buffer in `TrackedBatchBuffer`
      (`write_handler.rs`), with the degrade ladder: early file flush ->
      sort-on-write failover -> typed `ResourceExhausted`
- [ ] 2.2 Same wrapper for UPDATE/DELETE rewrite buffers
- [ ] 2.3 Fanout writer: reservation across per-partition open files;
      exhaustion closes the largest partition file early
- [ ] 2.4 Kill-switch config `write.tracked_buffers` (default on)
- [ ] 2.5 Unit tests: exhaustion returns typed error, degrade ladder order,
      kill-switch restores untracked behaviour
- [ ] 2.6 Rig gate: TPC-DS SF10 load at a 14GB cap on the 31GB box completes
      or fails typed; `dmesg` shows no oom-kill (the 2026-07-06 repro)

## 3. Phase C: cross-query retention

- [ ] 3.1 Run a 200-query SF10 sweep (tpcds suite + compare) with 0.2-0.4
      instrumentation; attribute the growth (facts before fixes)
- [ ] 3.2 Fix the top holders identified (session caches / global cache
      budgets / profiling retention / allocator purge hook, as applicable)
- [ ] 3.3 Repeat 3.1; acceptance: RSS envelope bounded (within 20% of the
      post-first-query level) and zero unattributed pool residue per query
- [ ] 3.4 Rig gate: full TPC-DS SF10 suite + comparison in ONE coordinator
      at an 8GB cap, no kernel kill (the 2026-07-06 repro)

## 4. Phase D: cpu-efficiency checkpoint

- [ ] 4.1 `perf record` (dev-release symbols) of SSB q3.1 x5 on the rig with
      phase A enabled; attribute cpu-seconds (zstd / arrow decode /
      membership filter / join probe / aggregation)
- [ ] 4.2 Pull Trino `cpuTime` for the same query from `/v1/query` stats for
      a same-box efficiency comparison
- [ ] 4.3 Write the evidence note under `docs/evidence/perf/` and record the
      follow-on decision (reader decode tuning vs predicate transfer) in
      `nextsteps.md`

## 5. Wrap-up

- [ ] 5.1 Full rig sweep SF1 + SF10 (`--compare-trino`), commit report JSONs
- [ ] 5.2 No-regress check against the frozen 0.1 baselines for TPC-H/TPC-DS
- [ ] 5.3 Update README roadmap + `nextsteps.md`; flip
      `execution.parallel_scan` default only if all gates in 1.4-1.5 passed
