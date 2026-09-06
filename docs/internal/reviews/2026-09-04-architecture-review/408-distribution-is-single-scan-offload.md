# 408: "Distributed execution" is single-scan offload; joins, aggregates and shuffles run on the coordinator, and the scheduler mixes units

- Scope: sqe-coordinator, read side (query_handler.rs try_distribute, scheduler.rs, distributed_scan.rs)
- Severity: medium
- Category: architecture
- Effort: L (week+)
- Status: open
- Issue: #588

## Problem

The README and CLAUDE.md describe "Workers (DataFusion)" executing "plan fragments" and a
"bespoke scheduler + stateless DataFusion workers over Arrow Flight". The code distributes
exactly one leaf scan per query:

- `crates/sqe-coordinator/src/query_handler.rs:7389-7419` `find_iceberg_scan` collects all
  `IcebergScanExec` nodes and returns the single largest by statistics
  (`scans.into_iter().max_by_key(size_of)`). Every other scan in a join stays local.
- `query_handler.rs:4224-4232` `replace_scan_in_plan(&plan, &scan_node, dist_scan)` swaps that
  leaf; all parents (filters above the pushed predicate, joins, aggregates, sorts, window
  functions) remain coordinator operators.
- `crates/sqe-coordinator/src/distributed_scan.rs:379-435` `DistributedScanExec` has
  `children() == []` and is a source; there is no exchange, no stage, no shuffle node in the
  coordinator crate. The 2026-07-25 review's P0-1 ("shuffle has no spill") refers to the
  worker-side shuffle used by the compaction/rewrite path, not the read path, which has no
  shuffle at all.

The scheduler that assigns morsels compares incompatible units:

- `crates/sqe-coordinator/src/scheduler.rs:154-158` initial load is
  `u64::from(w.active_fragments)` (a fragment count from `WorkerLoadTracker`), and
  `scheduler.rs:204` adds `cost`, which is megabytes (`estimate_cost`, `:74-84`). A worker with
  3 in-flight fragments looks 3 MB "busier" than an idle one, so cross-query balancing is
  effectively off once tasks exceed a few MB.
- Affinity hashes on file paths (`:106-119`) but the health checker's per-worker state does not
  feed back throughput, memory or queue depth (P1-2 of the 2026-07-25 review still holds).

The `worker_load` reservation is released when the `FragmentCallback` fires
(`query_handler.rs:4118-4121`), which `CallbackStream::drop` deliberately does not do
(`distributed_scan.rs:1286-1310`), so a cancelled or LIMIT-terminated query keeps its
reservations until the whole plan `Arc` is dropped.

## Why it matters

- A join of two large fact tables ships one of them over Flight to the coordinator and reads
  the other locally there; hash build, probe and aggregate all run on one node. Coordinator
  memory and CPU are the ceiling for every SF100 query regardless of worker count, which is
  the crossover measured in the "Clean-rig SF10 verdict" and "Distributed MPP architecture"
  memory notes.
- The wire protocol between coordinator and worker is a `ScanTask` JSON plus an optional
  `datafusion_proto` logical `Expr` (`scan_pushdown.rs:42-56`). The `Expr` bytes are version
  coupled to the exact DataFusion release on both sides, but `ScanTask::version` only guards
  the JSON layout; a coordinator/worker DataFusion skew fails at decode time inside the worker
  with a generic error.
- Documentation, roadmap ("Phase 3 -- Distributed execution: bespoke scheduler + stateless
  DataFusion workers") and Helm sizing all imply a capability that is scan offload.

## Proposed change

Decide and document one of two directions; do not leave the current shape described as MPP.

1. **Own it as scan offload.** Rename `DistributedScanExec` documentation and the README
   phase to "distributed scan", size the coordinator accordingly in Helm, and finish the
   scheduler: convert `active_fragments` to bytes-in-flight (track reserved bytes in
   `WorkerLoadTracker`), release reservations from `CallbackStream::drop` with a distinct
   `Cancelled` state, and publish a worker load vector on the heartbeat.
2. **Build stages.** Introduce a stage planner in `sqe-planner` that cuts the physical plan at
   `RepartitionExec`/`CoalescePartitionsExec` boundaries, serializes whole fragments with
   `datafusion_proto::physical_plan_to_bytes` plus `SqePhysicalCodec` (the codec exists at
   `codec.rs` and is unused), runs them on workers, and adds a `ShuffleReadExec` on the
   coordinator. Add a protocol version handshake (`sqe.proto.version`, DataFusion version
   string) to the heartbeat so skew fails at registration, not mid-query.

Either way, fix the unit mismatch in `scheduler.rs` now (S effort): initialise load in the
same unit as `estimate_cost`.

## Acceptance criteria

- `README.md`/`CLAUDE.md` architecture section matches what `try_distribute` does.
- Unit test in `scheduler.rs`: two workers, one with `active_fragments = 1`, two 500 MB tasks
  land one per worker (today both land on the idle worker only when the busy one's count
  exceeds the MB cost).
- Worker heartbeat carries a protocol version; a mismatched worker is refused with a clear
  `Status`.
- If direction 2: TPC-H q9 at SF10 on the two-worker rig shows coordinator peak RSS lower than
  single-node.

## Related

- 2026-07-25 review P0-1, P0-3, P1-2 all still hold.
- Memory notes "Distributed MPP architecture", "Ballista cutover", "Clean-rig SF10 verdict".
- 401 (credentials in tickets), 402 (fallbacks).
