**STATUS 2026-11-01: distributed compaction plans once per job per worker (`fix/compaction-plan-per-job`).**

Each `compact_file_group` call loaded the table, planned a delete-aware read
of the whole table and listed the live delete files before rewriting its
two-file group. Phase timing on the `distributed_rewrite_wall_clock_ratio`
rig (debug workers, 200 files, 100 groups): load 5 ms, plan 393 ms, rewrite
21 ms per group. `CompactionPlanCache` in `crates/sqe-worker/src/compaction.rs`
keys a `PinnedJob` (table, read plan, live deletes, Arrow schema) on job id,
metadata location, snapshot id and a credential fingerprint; groups share one
`OnceCell` build, failures are not cached, capacity 4 with a 5-minute idle
TTL and no eviction of a plan in use. The snapshot pin check still runs
before any manifest I/O.

Measured with `make test-distributed FILTER=distributed_rewrite_wall_clock_ratio`:
distributed wall-clock 22.8 s -> 2.9 s, 98/100 groups reused the plan
(local rewrite on the same rig: 1.8-2.0 s).

**NEXT:** the remaining 2.9 s is ~50 sequential groups per worker at
`max_inflight_groups_per_worker = 1`; measure a higher default on release
workers, and consider having the coordinator ship each group's scan tasks so
workers skip planning entirely.
