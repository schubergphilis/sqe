# Design: Scan Throughput + Memory Safety

## Context

Two measured problems from the 2026-07-06 fair-rig sweep (see proposal.md for
the evidence tables):

- Scan-bound SSB queries run at 4.2-5.0 of 8 cores because the scan merges to
  one output partition; everything above decode is single-streamed.
- The coordinator was kernel-OOM-killed twice at SF10: once by untracked
  write-sink buffers (load), once by cross-query retention (suite+compare).

The phases share one invariant: **all significant buffering is visible to the
memory pool, and every parallelism decision consults the budget.**

## Phase A: parallel scan output (adopts `parallel-iceberg-scan`)

The full design lives in `openspec/changes/parallel-iceberg-scan/design.md`
and is adopted unchanged: split scan files into N file-group partitions with
the existing bin-packing, announce `RoundRobinBatch(N)` (or place an explicit
`RepartitionExec(Hash(key), N)` when feeding a hash join/aggregate), never
advertise `UnknownPartitioning(N)` (the q72 lesson: `EnforceDistribution`
inserts a gather + round-robin and serializes the plan), flag
`execution.parallel_scan` default-off until gates pass.

### A-extension: memory-clamped partition count

New on top of the adopted design. The effective partition count is:

```text
N = min(
    execution.target_partitions (or core count),
    file/byte-threshold-derived N (adopted design),
    floor(pool.free_at_plan_time x clamp_fraction / est_partition_footprint),
)
```

`est_partition_footprint` is conservative: the configured scan batch size x
row-width estimate x the reader's per-partition buffer depth (prefetch depth +
decode-in-flight, both already config knobs in `[storage]`). The clamp keeps
today's behaviour when memory is plentiful and degrades N smoothly (never
below 1) when it is not. Rationale: at SF100 a 16-partition scan of a wide
table can hold hundreds of MB of decoded batches in flight per scan; the
planner must not promise parallelism the pool cannot back.

### A-extension: pool-registered decode buffers

The vendored reader's per-subtask decode buffers (parallel decode from #131,
`split_file_scan_task` subtasks + mpsc channel) allocate outside the
DataFusion pool today. The scan's stream wrapper grows/shrinks a
`MemoryReservation` sized to the channel capacity x batch size as subtasks
start/finish. This is bookkeeping, not new limits: it makes scan pressure
visible so spillable operators above yield earlier, and it feeds the clamp's
estimate with real numbers. VENDOR-adjacent glue only; the vendored reader
itself keeps its channel semantics (re-apply note in the vendor patch list,
same as the #195 block_on patch).

## Phase B: tracked write sink (ingest/CTAS, UPDATE/DELETE, fanout)

Pattern already in-tree: `crates/sqe-coordinator/src/write_memory.rs`
(`TrackedBatchBuffer`) wraps the MERGE target buffer and returns typed
`ResourceExhausted` when the reservation cannot grow. Extend the same wrapper
to the remaining write paths in `write_handler.rs`:

1. **Ingest/CTAS sink** (the path that died: TPC-DS SF10 `inventory` CTAS,
   20.5GB RSS under a 14GB cap). Buffered batches between the input stream and
   the Iceberg file writer get a reservation; exhaustion triggers the existing
   degrade ladder: flush current file early -> drop the sort-on-write
   clustering (failover already exists for pool exhaustion, see
   `is_resource_exhausted` in sqe-bench's loader for the observable behaviour)
   -> typed error only if a single batch cannot fit.
2. **UPDATE/DELETE rewrite buffers**: same wrapper around the rewritten-file
   accumulation.
3. **Fanout writer** (partitioned writes): per-partition open-file buffers are
   the multiplier; reservation covers the sum, and exhaustion closes the
   largest open partition file early (more, smaller files: correct, slower,
   alive; compaction exists).

Explicitly out of scope: rewriting the vendored Iceberg writer. The wrapper
sits in SQE's sink glue, where the MERGE version already sits.

### Why the kernel killed us below the cap

The pool cap bounds *tracked* reservations. The kill at 20.5GB RSS under a
14GB cap is the untracked delta: writer buffers (phase B), decode buffers
(phase A-extension), and whatever phase C finds. The invariant after this
change: tracked reservations account for every allocation class that scales
with data volume; constant overhead (code, caches with fixed budgets) is the
only untracked remainder.

## Phase C: cross-query retention

Evidence: suite+compare (198 SF10 queries) in one process dies at any cap;
each query alone is fine; a fresh process running half the work passes.

Step 1 is instrumentation, not guessing:
- A per-query log line (INFO) with: pool reserved bytes at query end, process
  RSS, delta vs query start. Gauge equivalents in `sqe-metrics`.
- A debug endpoint (or `EXPLAIN`-adjacent command) dumping pool reservations
  by consumer name, so a leaked reservation is attributable.

Candidate holders, to be confirmed by the instrumentation (not fixed blind):
session-context caches keyed per session that outlive queries; the metadata /
manifest / footer caches growing without global budget coordination; plan or
statistics objects retained by profiling (`query_profile = "all"` in the test
config); arrow allocator retention (jemalloc/mimalloc arenas not returning to
the OS, which is RSS-visible but pool-invisible; if this dominates, the fix is
an allocator-level `purge` hook between queries, not a leak hunt).

Acceptance is behavioural, not structural: RSS after query k stays within a
fixed envelope of RSS after query 1 for a 200-query SF10 sweep, and the
tracked-pool residue after each query is zero (or attributed and budgeted).

## Phase D: cpu-efficiency profiling checkpoint

After A lands (utilization fixed), profile SSB q3.1 on the rig with the
`dev-release` profile (symbols retained) and `perf record` (Linux box, no
macOS sample/strip issues): attribute cpu-seconds to zstd decompress, arrow
decode, `MembershipSet` probe, hash-join probe, aggregation. Compare against
Trino's `cpuTime` from its `/v1/query` stats for the same query. Output: a
short evidence doc + a decision (vendored reader decode tuning vs predicate
transfer as the next change). No production code in this phase.

## Key decisions

1. **One change, four phases** rather than two changes: A without B/C ships a
   parallelism feature that converts a known kernel-kill into a more frequent
   one; B/C without A fixes crashes while leaving the only losing benchmark
   unfixed. The coupling (clamp + registered buffers) is the novel part.
2. **Adopt, do not rewrite, `parallel-iceberg-scan`**: its analysis of the
   q72/EnforceDistribution failure is correct and its gates are kept. This
   change adds the memory dimension its design predates.
3. **Degrade, never abort, on scan-side pressure**: aborting a read because
   parallel buffers do not fit re-introduces the sort-on-write OOM lesson
   ([`sort-merge-oom-not-spill`]) on the read side. N clamps to 1 and the
   query runs serial, exactly like today.
4. **Instrument before fixing retention**: the June test-config caveat shows
   this has been guessed at before. The per-query reservation dump makes the
   holder a fact, not a hypothesis.
5. **Rig as the measurement substrate**: every gate runs on the
   `BENCH_WAREHOUSE=external` recipe where both engines share the network
   path. Dev-Mac numbers are explicitly non-authoritative (Docker NAT
   confound, documented 2026-07-06).

## Risks

- Hash-partitioned scan output changing join modes has plan-wide effects;
  mitigated by the adopted flag + q72 gate + plan-shape unit tests.
- The footprint estimate for the clamp can be wrong in both directions;
  mitigated by conservative defaults and the pool registration making the
  real number observable.
- Allocator retention (C) may dominate and be non-trivial to purge; the
  phase's deliverable is then the attribution + an allocator decision, which
  is still progress over today's blind kills.
- Rig is shared with an unrelated demo stack (~6GB); gates use pool caps that
  leave headroom, and the kernel-OOM criterion (never invoked) is robust to
  background noise.
