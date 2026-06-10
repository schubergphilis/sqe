## 1. Shuffle completion protocol (correctness first)

- [ ] 1.1 Add an end-of-stream marker + sent-batch/row count to `ExchangeDescriptor` (`crates/sqe-planner/src/shuffle_exec.rs`)
- [ ] 1.2 In `do_exchange`, stop treating a decode error as stream end: poison the partition and surface a `DataFusionError` (`crates/sqe-worker/src/flight_service.rs:506-530`)
- [ ] 1.3 In `do_exchange`, require the EOS marker; channel close without it is a hard stage failure
- [ ] 1.4 `ShuffleReaderExec` surfaces a poisoned/incomplete partition as a query error, not an empty stream
- [ ] 1.5 Unit test: injected decode error fails the read with an error, never an empty/short stream
- [ ] 1.6 Unit test: missing EOS marker fails the stage; received-count mismatch fails the stage

## 2. ShuffleWriterExec send path

- [ ] 2.1 Replace the `EmptyRecordBatchStream` stub in `ShuffleWriterExec::execute()` with the real send loop (`crates/sqe-planner/src/shuffle_exec.rs:254-268`)
- [ ] 2.2 Partition each batch via `HashPartitioner` / `RangePartitioner` per the `ShufflePartitioning` descriptor
- [ ] 2.3 Open one `do_exchange` stream per target endpoint; first message carries the `ExchangeDescriptor`
- [ ] 2.4 Send the EOS marker per partition on input exhaustion (Section 1)
- [ ] 2.5 Honour bounded-channel backpressure end to end (slow receiver throttles the writer's input)
- [ ] 2.6 Unit test: writer hash-partitions a known batch set to the expected target distribution
- [ ] 2.7 Integration test: writer -> do_exchange -> reader round-trips all rows with no loss

## 3. Stage planning for compute pushdown

- [ ] 3.1 Sink filters and projections into leaf scan stages
- [ ] 3.2 Split aggregates into worker-side partial + final (hash-shuffle for high-cardinality group-by; gather for scalar/low-cardinality)
- [ ] 3.3 Decompose hash joins into two hash-partitioned input stages on the join key
- [ ] 3.4 Handle multi-scan plans (remove the first-scan-only limitation at `crates/sqe-coordinator/src/query_handler.rs:4048-4059`)
- [ ] 3.5 Unit test: `decompose_plan` on a two-table join emits 3 stages with correct hash partitioning
- [ ] 3.6 Unit test: aggregate plan emits partial + final stages

## 4. Wave scheduler

- [ ] 4.1 New distributed path in `query_handler` that calls `decompose_plan` when `distribution_mode = multi_stage`
- [ ] 4.2 Compute waves by topological level; assign stages to workers
- [ ] 4.3 Register stage receivers and wire `ShuffleWriterExec.target_endpoints` to the next wave's reader placement
- [ ] 4.4 Submit fragments wave by wave; advance when input stages complete
- [ ] 4.5 Record per-stage progress in `QueryTracker` fragments (`crates/sqe-coordinator/src/query_tracker.rs:50-86`)
- [ ] 4.6 Worker failure mid-stage fails the query (Phase 1); stage-level retry deferred to Phase 2

## 5. Config + composition

- [ ] 5.1 Add `query.distribution_mode` (`scan_only` | `multi_stage`), default `scan_only`
- [ ] 5.2 Keep scan-only as the degenerate single-stage path; `DistributedScanExec` is the leaf-stage source
- [ ] 5.3 Decompose only when stage count and data size justify it; else fall back to scan-only

## 6. Benchmarks (gates)

- [ ] 6.1 TPC-H SF1 multi-stage: 22/22, beats `tpch-sf1-flight-2026-04-06T20:57:10.json` (12.0s) on Q5/Q7/Q8/Q9
- [ ] 6.2 TPC-H SF1 multi-stage overall beats single-node `tpch-sf1-flight-2026-04-02T14:16:27.json` (37.5s)
- [ ] 6.3 TPC-DS q72 completes under multi-stage with no error
- [ ] 6.4 Forced shuffle decode error fails the query, no short result
- [ ] 6.5 Commit benchmark JSON to `benchmarks/results/`; flip `distribution_mode` default only after gates pass
