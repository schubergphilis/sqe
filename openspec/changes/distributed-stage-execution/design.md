## Context

The distributed path runs one transformation: find the first scan, distribute it, leave the rest on the coordinator (`crates/sqe-coordinator/src/query_handler.rs:2080-2253`). For a scan-bound aggregation that is enough. For a join over two large tables it is not: one side distributes, the other is read locally, and every row flows back to the coordinator for the join and aggregate.

The pieces for real multi-stage execution are already in the tree, half-built:
- `stage_planner::decompose_plan` walks a physical plan and splits it at shuffle boundaries (`HashJoinExec`, `SortMergeJoinExec`, `SortExec`) into a topologically ordered `Vec<QueryStage>` (`crates/sqe-planner/src/stage_planner.rs:1-95`).
- `ShuffleReaderExec` reads shuffled batches from an mpsc channel and presents them as a normal stream. It works (`crates/sqe-planner/src/shuffle_exec.rs:271-446`).
- The worker `do_exchange` handler decodes the `ExchangeDescriptor`, looks up the stage receiver, and forwards batches into the partition channel (`crates/sqe-worker/src/flight_service.rs:420-568`).
- `ShuffleWriterExec::execute()` is the stub: it drops its input stream and returns `EmptyRecordBatchStream` (`crates/sqe-planner/src/shuffle_exec.rs:254-268`).

So the decision is straightforward. Finish the machinery. The read side and the transport already work; the missing code is the write side, the scheduler that submits the stage DAG, and a completion protocol that makes failures loud.

## Goals / Non-Goals

**Goals:**
- Push filters, projections, and partial aggregates onto workers.
- Distribute both sides of a hash join via hash-partitioned shuffle on the join key.
- Execute the stage DAG wave by wave with the coordinator wiring endpoints between waves.
- Make a shuffle decode error or a lost partition fail the query, never truncate it.

**Non-Goals:**
- The predicate + `LIMIT` pushdown into `ScanTask`. A separate branch lands that safe partial; this change assumes it and builds the shuffle/join layer above it.
- Adaptive / cost-based re-planning mid-query. Stage assignment is decided up front.
- Spill-aware shuffle to disk on the receiver. Shuffle stays memory-bounded with backpressure; disk spill is a follow-up.
- Broadcast-join optimization tuning. `ShuffleType::Broadcast` exists; sizing heuristics are deferred.

## Architecture

### Stage boundaries

A shuffle boundary is inserted where data must be redistributed:

| Operator | Boundary | Partitioning |
|---|---|---|
| Hash join | both inputs | Hash on the join key columns |
| Aggregate (high cardinality group-by) | between partial and final | Hash on the group-by keys |
| Aggregate (low cardinality / scalar) | partial on worker, final on coordinator | gather (no shuffle) |
| Order by / global sort | before the sort | Range on the sort key |
| Scan | leaf | none (file-group parallelism via `DistributedScanExec`) |

### Plan with shuffle: a two-table join

```
                         coordinator final stage
                       ┌───────────────────────────┐
                       │  Final Aggregate           │
                       │  ShuffleReaderExec         │<-- gather
                       └─────────────┬──────────────┘
                                     │ do_exchange
              ┌──────────────────────┴──────────────────────┐
              │            join stage (per worker)            │
              │  Partial Aggregate                            │
              │  HashJoinExec(key = o_orderkey)               │
              │   ┌─────────────────┐   ┌─────────────────┐   │
              │   │ ShuffleReader   │   │ ShuffleReader   │   │
              │   │ (orders side)   │   │ (lineitem side) │   │
              │   └────────^────────┘   └────────^────────┘   │
              └────────────┼─────────────────────┼───────────┘
                           │ do_exchange         │ do_exchange
              ┌────────────┴──────┐   ┌──────────┴────────────┐
              │ scan stage:orders │   │ scan stage:lineitem   │
              │ Filter+Project    │   │ Filter+Project        │
              │ ShuffleWriter     │   │ ShuffleWriter         │
              │  Hash(o_orderkey) │   │  Hash(l_orderkey)     │
              │ DistributedScan   │   │ DistributedScan       │
              └───────────────────┘   └───────────────────────┘
                  wave 1 (leaves)          wave 1 (leaves)
```

Wave 1: both scan stages run on all workers, each filtering, projecting, and hash-partitioning its output on the join key. Wave 2: the join stage runs on all workers; each worker reads the matching hash partition from both sides via `ShuffleReaderExec`, joins, and computes a partial aggregate. Wave 3: the coordinator gathers partials and finalizes.

### Shuffle write/read protocol over do_exchange

`ShuffleWriterExec::execute()` (the stub at `crates/sqe-planner/src/shuffle_exec.rs:254-268`) becomes:
1. Run the input plan for this partition.
2. For each batch, partition it with `HashPartitioner` / `RangePartitioner` (`sqe-worker::shuffle`) per the `ShufflePartitioning` descriptor (`crates/sqe-planner/src/shuffle_exec.rs:45-59`).
3. Open one `do_exchange` stream per target endpoint; the first FlightData message carries the `ExchangeDescriptor` (query_id, stage_id, partition_id) the receiver already keys on (`crates/sqe-worker/src/flight_service.rs:447-455`).
4. Stream each partition's batches to its target.
5. On input exhaustion, send an explicit end-of-stream marker (below) per target, then close.

The read side is unchanged: `do_exchange` forwards into the stage receiver channel, `ShuffleReaderExec` drains it.

### Completion protocol: no silent truncation

This is the correctness requirement, and the current receiver gets it wrong. In `do_exchange`, a decode error `break`s the intake loop and a zero-row batch is skipped (`crates/sqe-worker/src/flight_service.rs:506-530`). A mid-stream failure therefore ends the channel exactly as a clean completion would. The downstream join sees a short input and produces a wrong answer with no error.

Fix:
- The writer sends a typed end-of-stream marker per partition (an `ExchangeDescriptor` flag, or a sentinel final message carrying the total batch/row count it sent).
- The receiver tracks, per `(query_id, stage_id, partition_id)`, whether it saw the marker. Channel close without a marker is a hard error: the stage fails, the failure propagates to the wave scheduler, the query fails.
- A decode error stops being a silent `break`; it sets a poison flag on the receiver so the reader surfaces a `DataFusionError`, not end-of-stream.

```
  writer:  [desc][batch]...[batch][EOS marker, sent_count=N]
  reader:  count received batches; require marker; received != N or no marker -> fail query
```

### Wave scheduler

The coordinator:
1. Calls `decompose_plan` to get the stage DAG (`crates/sqe-planner/src/stage_planner.rs`).
2. Computes waves by topological level (leaves = wave 1).
3. For each wave: assign stages to workers, register stage receivers, submit fragments, and wire each `ShuffleWriterExec`'s `target_endpoints` to the next wave's `ShuffleReaderExec` placement.
4. Tracks per-stage state in the existing `QueryTracker` fragment list (`crates/sqe-coordinator/src/query_tracker.rs:50-86`) so the web UI shows stages.

### Failure and retry semantics

| Failure | Behaviour |
|---|---|
| Shuffle decode error | Receiver poisons the partition; stage fails; query fails with a clear error. Never truncate. |
| Channel closed without EOS marker | Treated identically: hard failure. |
| Worker dies mid-stage | Stage fails. Phase 2: retry the failed stage on a surviving worker if its inputs are still materializable; Phase 1: fail the query, client retries. |
| Backpressure | The shuffle channel is a bounded mpsc; a slow receiver blocks the sender, propagating backpressure up the writer's input. No unbounded buffering. |

### Composition with DistributedScanExec

`DistributedScanExec` stays the leaf. The stage planner treats a distributed scan as the source of a leaf stage; the `ShuffleWriterExec` sits directly above it inside the same stage. Scan-only distribution (`distribution_mode = scan_only`) remains a valid configuration: it is the degenerate single-stage case.

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Finish vs redesign | Finish the machinery | Read side, transport, descriptor routing, and `decompose_plan` already work and are tested; only the write side + scheduler + completion are missing |
| Shuffle transport | Flight `do_exchange` | Already implemented on the worker; descriptor routing is in place |
| Join distribution | Hash-partition both sides on the join key | Standard shuffle join; avoids pulling either side to the coordinator |
| Aggregate | Partial on worker, final gathered | Cuts the data crossing the network by the group cardinality |
| Completion | Explicit per-partition EOS marker + count | Channel-close-as-completion is the silent-truncation bug; a marker makes failures loud |
| Backpressure | Bounded mpsc | Memory-safe; no unbounded shuffle buffering |
| Mode gate | `distribution_mode` default `scan_only` | Ship behind a flag until benchmarks pass |

## Risks

| Risk | Mitigation |
|---|---|
| Silent truncation on shuffle failure | Explicit completion marker + count; decode error poisons the partition (the core correctness fix) |
| Multi-stage slower than scan-only on small joins | Keep the scan-only path; only decompose when stage count and data size warrant it; gate behind `distribution_mode` |
| Shuffle memory blowup | Bounded mpsc backpressure; disk spill on receiver deferred but tracked |
| Worker failure mid-query | Phase 1 fails the query (client retry); stage-level retry is Phase 2 |
| Skewed hash partitions | Monitor per-partition row counts; salting / range fallback is a follow-up |
| Regressing the scan-only baseline | Benchmark against `tpch-sf1-flight-2026-04-06T20:57:10.json` and `tpch-sf1-flight-2026-04-02T14:16:27.json` before flipping the default |
