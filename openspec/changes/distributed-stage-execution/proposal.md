## Why

Distributed execution today is scan-only. `try_distribute` finds the first `IcebergScanExec` in the physical plan, splits its data files into bin-packed `ScanTask`s, and swaps that one leaf for a `DistributedScanExec` (`crates/sqe-coordinator/src/query_handler.rs:2080-2253`). Everything above the scan -- filters, joins, aggregations, sorts -- still runs single-node on the coordinator. The coordinator pulls every scanned row back over Flight and does all the real work itself. It is the bandwidth and CPU bottleneck.

It is also single-scan: `find_iceberg_scan` returns the *first* scan in a depth-first walk (`crates/sqe-coordinator/src/query_handler.rs:4048-4059`). A join over two large tables distributes one side and reads the other locally.

The machinery for real multi-stage execution exists but is unwired. `stage_planner::decompose_plan` can split a plan at shuffle boundaries into a stage DAG (`crates/sqe-planner/src/stage_planner.rs:1-95`). `ShuffleReaderExec` works and the worker's `do_exchange` handler forwards shuffled batches into a stage receiver (`crates/sqe-worker/src/flight_service.rs:420-568`). But `ShuffleWriterExec::execute()` is a stub: it drops its input and returns an `EmptyRecordBatchStream` (`crates/sqe-planner/src/shuffle_exec.rs:254-268`), and `try_distribute` never calls the stage planner. Issue #181 tracks the unwired stub; this change closes it.

A separate branch is already landing the safe partial of predicate and `LIMIT` pushdown into the `ScanTask`. This change is the full picture beyond that partial: real multi-stage execution with shuffle, partial aggregates on workers, both sides of a join distributed, and a wave scheduler.

## What Changes

Recommendation: **finish the stage machinery, do not redesign it.** `ShuffleReaderExec`, the `do_exchange` receiver path, `ExchangeDescriptor` routing, the `ShufflePartitioning` descriptor, and `decompose_plan` are all in place and tested. The missing piece is last-mile wiring: the `ShuffleWriterExec` send loop, invoking `decompose_plan` from `try_distribute`, and a wave scheduler that submits stages and wires their endpoints. A rewrite would throw away working, tested code.

1. **Wire `ShuffleWriterExec::execute()`** to partition each input batch (hash or range via `sqe-worker::shuffle`) and send each partition to its target executor over Flight `do_exchange`.
2. **Invoke `decompose_plan`** from a new distributed path in `query_handler`, replacing the single-scan swap with a full stage DAG when the plan warrants it.
3. **Push compute to workers:** filters and projections sink into scan stages; aggregations split into worker-side partial aggregates plus a coordinator-side (or shuffled) final aggregate; both sides of a hash join become shuffle-partitioned stages on the join key.
4. **Wave scheduler:** submit leaf stages first, then dependent stages as inputs complete, wiring each stage's shuffle endpoints to the next.
5. **Completion protocol:** add an explicit end-of-stream signal per partition so a shuffle decode error fails the query instead of silently truncating (see Risks).
6. **Compose with `DistributedScanExec`:** leaf scan stages keep using the existing scan-distribution path; the stage DAG sits above them.

## Capabilities

### New Capabilities
- `distributed-shuffle-write`: `ShuffleWriterExec` partitions and ships batches over `do_exchange`.
- `distributed-multi-stage`: plans decompose into a stage DAG executed wave by wave.
- `distributed-partial-aggregate`: aggregations run partial-on-worker, final-on-coordinator-or-shuffle.
- `distributed-join`: hash joins shuffle both inputs on the join key.
- `distributed-shuffle-completion`: explicit per-partition completion so decode errors fail loudly.

### Modified Capabilities
- `distributed-execution`: scan-only distribution becomes full multi-stage distribution; `DistributedScanExec` becomes the leaf-stage building block.

## Impact

- `sqe-planner`: implement `ShuffleWriterExec::execute()`; extend `stage_planner` to emit partial/final aggregate splits and join shuffles; add the completion signal to `ExchangeDescriptor`.
- `sqe-coordinator`: new distributed path in `query_handler` that calls `decompose_plan` and a wave scheduler; the single-scan `try_distribute` becomes the leaf-stage path.
- `sqe-worker`: harden `do_exchange` so decode errors and partition completion propagate as query failure, not stream end (`crates/sqe-worker/src/flight_service.rs:506-530`).
- No SQL-surface change. No catalog change.

## Rollback

Gated by `query.distribution_mode` (`scan_only` | `multi_stage`), default `scan_only`. `multi_stage` is opt-in until the success criteria pass, then becomes default. Setting it back to `scan_only` restores today's behaviour with no plan-format migration.

## Success Criteria

- TPC-H SF1 distributed (multi-stage) completes 22/22 and beats the scan-only distributed baseline `tpch-sf1-flight-2026-04-06T20:57:10.json` (22/22, 12.0s) on join-heavy queries (Q5, Q7, Q8, Q9), and beats the single-node baseline `tpch-sf1-flight-2026-04-02T14:16:27.json` (22/22, 37.5s) overall.
- TPC-DS q72 (the join-heavy regression canary) completes under multi-stage without error.
- A forced shuffle decode error fails the query with a clear error, never returns a short result.
