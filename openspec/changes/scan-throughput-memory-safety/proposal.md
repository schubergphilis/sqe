# Scan Throughput + Memory Safety (SF10 pipeline efficiency)

## Why

The first fair SQE-vs-Trino sweep (2026-07-06, rig idp-gpu-01, 8 cores / 31GB,
both engines reading the same StorageGRID endpoint, warehouse
`s3://sqe-testlake2`, source data `s3://sqe-benchmark`) leaves exactly one
suite where SQE trails: SSB SF10 at 0.79x (27.0s vs 21.4s). Everything else is
ahead at SF10: TPC-H 1.5x, TPC-DS 1.46x, TPC-C 3.3x, TPC-BB 3.4x, ClickBench
2.7x. The same sweep produced two kernel OOM kills of the coordinator. These
are the two remaining efficiency problems, and they are coupled: the fix for
the first multiplies concurrent buffering, which the second must bound.

**Throughput evidence.** The SSB loss is uniform 0.63-0.71x on every
fact-scan-bound query regardless of selectivity (q2.3 returns 7 rows, q3.2
returns 600; same ratio), so it is a pipeline-throughput constant, not a plan
shape. Measured on the idle rig via `/proc/<pid>/stat` deltas:

| query | SQE wall/run | SQE avg cores (of 8) | Trino wall (compare) |
|---|---|---|---|
| q2.2 | 2.8s | 4.2 | 1.7s |
| q3.1 | 2.9s | 5.0 | 2.1s |
| q4.3 | 2.3s | 4.5 | 1.5s |

The scan decodes in parallel (issue #131 intra-file split) but still merges to
a single output partition, so filter evaluation, join probe, and partial
aggregation above it are paced by one stream and ~40% of the machine idles.
The core-utilization ratio alone (4.4/7.5) reproduces the observed 0.63-0.71x.
A second, smaller factor remains: SQE spends ~14.5 cpu-seconds on q3.1 where
Trino finishes faster at similar core counts, so per-cpu-second cost (decode +
filter evaluation) needs profiling after utilization is fixed. Where direct
predicates prune row groups (q1.x), SQE wins 1.5-2.5x already.

**Memory evidence.** Same sweep, same box (31GB, ~6GB used by an unrelated
stack, Trino capped at 12-16GB):

1. TPC-DS SF10 load: coordinator kernel-OOM-killed at 20.5GB anon RSS under a
   14GB pool cap. The Iceberg write sink's buffers are not pool-tracked on
   the ingest/CTAS path (the MERGE target path already is, via
   `TrackedBatchBuffer` in `crates/sqe-coordinator/src/write_memory.rs`).
2. TPC-DS SF10 suite (99 queries) + comparison (99 more) in one coordinator
   process: kernel-OOM-killed mid-comparison at BOTH an 8GB and a 6GB pool
   cap. A fresh coordinator running only the comparison passed. Memory is not
   returned between queries; the caveat documented in `tests/sqe-test.toml`
   ("per-query memory isn't fully released back to the pool") now has a
   kernel kill to its name.

Parallelizing the scan output multiplies concurrent decode buffers and
per-partition operator state. Shipping it without tracked, degradable memory
turns the SSB fix into new OOM kills at SF100. Hence one change.

## What Changes

Four phases, ordered by dependency:

1. **Parallel scan output** (phase A): execute the existing
   `parallel-iceberg-scan` change (its proposal, design, and 20 tasks are
   adopted as-is: emit optimizer-consumable partitioning, place
   `RepartitionExec` explicitly, gate behind `execution.parallel_scan`,
   q72 regression gate), extended with a memory coupling: the partition
   count N is additionally clamped by the memory budget, and per-partition
   decode buffers are registered against the pool.
2. **Write-path ingest tracking** (phase B): extend the in-tree
   `TrackedBatchBuffer` pattern from the MERGE target path to the
   ingest/CTAS sink (and the UPDATE/DELETE and fanout writers), so a too-big
   write degrades to a typed `ResourceExhausted` + the existing
   unsorted-write failover instead of a kernel kill.
3. **Cross-query memory retention** (phase C): instrument RSS and pool usage
   per query, find what holds memory after query completion (session
   contexts, plan/metadata caches, arrow buffer reuse), and fix the largest
   holders. Acceptance is a bounded RSS envelope across a 200-query SF10
   sweep in one process.
4. **CPU-efficiency profiling checkpoint** (phase D): with utilization fixed,
   a symbolized profile of SSB q3.1 on the rig splits the remaining
   cpu-seconds (zstd decompress, arrow decode, membership-filter evaluation,
   hash probe) and decides the follow-on (vendored reader decode tuning vs
   predicate transfer from the lakehouse roadmap). Decision output, not code.

## Capabilities

### New Capabilities
- `scan-parallel-roundrobin`, `scan-parallel-hash`: adopted unchanged from
  `parallel-iceberg-scan`.
- `memory-gated-parallelism`: scan partition count degrades under memory
  pressure instead of aborting or overshooting the pool.
- `tracked-write-sink`: ingest/CTAS/UPDATE/DELETE/fanout writer buffers are
  pool-tracked with typed exhaustion errors.
- `bounded-query-retention`: coordinator RSS returns to a bounded envelope
  after each query.

### Modified Capabilities
- `iceberg-scan`: advertises meaningful partitioning under the flag (from
  `parallel-iceberg-scan`).

## Impact

- `sqe-catalog`: scan partitioning + partition-count clamp; decode-buffer
  pool registration in the vendored reader glue.
- `sqe-planner` / `sqe-coordinator`: partitioning-aware planner pass (from
  `parallel-iceberg-scan`); `write_memory.rs` extension to the remaining
  write paths; per-query memory release fixes.
- `sqe-core`: config for `execution.parallel_scan` (from
  `parallel-iceberg-scan`) and the memory clamp knob.
- No SQL surface, catalog, or wire-protocol change.
- Supersedes: `parallel-iceberg-scan` is absorbed as phase A (folder is
  retired when this change lands). Continues: `feat/write-path-memory-safety`
  branch scope becomes phase B.

## Rollback

Every phase is independently revertible:
- Phase A: `execution.parallel_scan = false` (default until gates pass) is
  today's behaviour, as specified in `parallel-iceberg-scan`.
- Phase B: tracking is observability + typed errors around existing buffers;
  a config kill-switch (`write.tracked_buffers = false`) restores untracked
  behaviour.
- Phase C: individual fixes are small and independently revertible.
- Phase D: produces a document, nothing to roll back.

## Success Criteria

All measured on the rig recipe (`BENCH_WAREHOUSE=external`, both engines on
the same S3 endpoint), which removes the network confound:

1. SSB SF10 total >= 0.95x Trino (from 0.79x), with q2.x/q3.x scan-pipeline
   core utilization >= 6.5 of 8 (from 4.2-5.0).
2. The `parallel-iceberg-scan` gates hold unchanged: TPC-DS q72 SF1 <= 1.1x
   its 756ms baseline; TPC-H SF1 suite no regression; no
   `CoalescePartitionsExec` directly above a parallel scan.
3. TPC-DS SF10 load under a 14GB cap on a 31GB box completes or fails with a
   typed `ResourceExhausted`; the kernel OOM killer is never invoked.
4. The full TPC-DS SF10 suite + comparison (198 queries) runs in ONE
   coordinator process at an 8GB cap without being killed.
5. TPC-H/TPC-DS/SSB SF10 rig totals do not regress from the 2026-07-06
   baselines (`benchmarks/results/compare-*-sf10-2026-07-06*.json`).
6. Phase D delivers a written cpu-second breakdown for q3.1 and a decision
   on the follow-on lever.
