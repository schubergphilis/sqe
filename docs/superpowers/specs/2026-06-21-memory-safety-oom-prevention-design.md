# Memory safety and OOM prevention under high concurrency

Date: 2026-06-21
Status: Draft for review
Scope: Subsystem A of the enterprise concurrency-hardening program (A: memory safety, B: admission and fair-share scheduling, C: multi-tenant isolation, D: fault tolerance). This spec covers A only. B, C, and D are deferred to their own specs.

## Context and motivation

SQE is already a genuine stage-based MPP engine: distributed hash joins, distributed aggregation, and range-partitioned distributed sort, decomposed into stages by the stage planner and exchanged over Arrow Flight `DoExchange` (`sqe-planner/src/distributed_join.rs`, `distributed_aggregate.rs`, `distributed_sort.rs`, `shuffle_exec.rs`; `sqe-worker/src/shuffle.rs`). The distributed layer scales. The gap is reliability under load.

Two memory paths cannot spill today:
1. The shuffle receiver buffers are "per-stage partition buffers backed by bounded mpsc channels" (`sqe-worker/src/shuffle.rs`). In-memory only, no disk tier.
2. Sort-on-write has `can_spill=false` (`ExternalSorterMerge`), so partitioned or sorted CTAS hard-OOMs instead of degrading.

There is also no memory budget shared across concurrent queries on a single node. Each query reasoning about memory independently means N concurrent queries can collectively overcommit RAM and OOM-kill the process.

The target is enterprise and financial usage inside the Chameleon data platform, where a process OOM-kill mid-query is disqualifying. For that audience, reliability and correctness outrank raw speed: a slightly slower engine that never crashes a worker beats a faster one that falls over under concurrency.

## Goal and non-goals

### Goal

Guarantee that no query can OOM a worker or the coordinator, holding at roughly 100 to 300 concurrent queries per cluster. Every memory consumer either shares a bounded budget or spills to local disk. A minimal admission cap makes the guarantee real. The blast radius of any single failure is exactly one query.

### In scope

- A single shared memory budget per node, on both workers and the coordinator, not per-query pools.
- Spill on every pipeline breaker: hash-join build, hash aggregate, sort (including the sort-on-write `can_spill=false` fix), and the shuffle receiver buffers.
- A per-query minimum memory floor plus a minimal admission cap (floor availability and max-concurrent), nothing more.
- A fail-query-not-cluster error path with clean resource reclaim and an audit event.
- Observability: node memory utilization, bytes spilled, queries killed by memory, admissions rejected.

### Out of scope (deferred)

- Fair-share scheduling and query priorities (subsystem B).
- Per-tenant isolation and guaranteed resource shares (subsystem C).
- Mid-query worker-failure recovery beyond the existing fragment retry (subsystem D).
- Cost-based admission estimation. Admission here is a floor-availability and concurrency guard, not a predictive cost model.

## Design decision: hybrid shared pool with per-query floor

Three models were considered.

1. Single shared per-worker pool. All queries draw from one `FairSpillPool`. Simple and OOM-safe, but no per-query floor; at 300 queries the pool fragments into tiny shares, a greedy query starves the rest, and unspillable demand can still wedge it.
2. Per-query pools plus a worker admission gate. Strong isolation, but fixed reservations waste memory and it pulls real admission logic forward from subsystem B.
3. Hybrid: a shared per-node pool for elasticity, plus a guaranteed per-query minimum floor, plus an irreducible admission rule that refuses to seat a query whose floor cannot be met.

We choose option 3. To guarantee no OOM at 300-way concurrency, some cap is required, because unspillable working memory (shuffle in-flight buffers, the coordinator final stage) has a non-zero floor per query, so unlimited admission eventually cannot fit even fully spilled. The floor guarantees progress, the shared pool avoids the waste of fixed reservations, and the cap makes the guarantee real. The per-query floor is also the natural hook for per-tenant guarantees in subsystem C, so the layers compose.

## Architecture

Every node, worker and coordinator alike, runs a `NodeMemoryGovernor` that owns one bounded memory pool sized at `RAM - headroom`. Operators reserve from the pool. Under pressure the pool drives the largest spillable consumer to local NVMe. An `AdmissionGate` in front of execution refuses work that cannot be seated with its floor. When an unspillable reservation cannot be met even after all spillable consumers have spilled, the offending query is failed and fully reclaimed; the node and all other queries continue.

The coordinator is treated as just another governed node, because its final stage can OOM the same way a worker stage can.

## Components

### 1. NodeMemoryGovernor (new, shared by worker and coordinator)

Owns the single `FairSpillPool` sized at `RAM - headroom`. Tracks active queries, their floors, and current reservation. Exposes `try_admit(query) -> Admitted | Rejected { reason }` and reservation handles that release on drop. Builds on the existing `sqe-coordinator/src/memory.rs` and the `FairSpillPool` already in use (the q39 fix). This is the heart of the design: a budget accountant with a hard ceiling that cannot be exceeded.

### 2. Spillable-operator audit

Verify that hash-join build, hash aggregate, and sort all register with the governor pool and route to the DataFusion `DiskManager`. Fix sort-on-write `can_spill=false` so partitioned and sorted CTAS spills instead of OOMing. This is mostly wiring and verification on top of DataFusion's existing spill support, plus one real fix.

### 3. SpillableShuffleReceiver (the custom piece, Ballista-informed)

The bounded-mpsc receiver buffers gain a disk-spill tier. When an in-memory partition buffer exceeds its budget, batches spill to local NVMe as Arrow IPC partition files and stream back on read. The Arrow IPC on-disk format follows Ballista's shuffle approach (executors write shuffle partitions to local disk and serve them over Flight), which both bounds shuffle memory and sets up stage-level retry for subsystem D later. SQE stays memory-first and spills only under pressure, keeping the latency advantage of in-memory streaming on the common path, rather than Ballista's always-materialize default.

### 4. AdmissionGate (minimal)

At query dispatch: a floor-availability check plus a max-concurrent cap, expressed as both slots and memory floors (the slot concept borrowed from Ballista executor task slots). On rejection, a brief bounded queue, then a typed `Rejected` error. This is explicitly not fair-share or priority logic; that is subsystem B. The existing `WeightedScheduler` (least-loaded bin-packing) is retained for placement, as the Ballista eval found it ahead of Ballista 53's slot-binding.

### 5. QueryAbortPath (failure-taxonomy-driven)

A typed failure taxonomy modeled on Ballista's reason enum, where each variant tags `retryable` and `counts_to_failure`. Initial variants: `MemoryExhausted` (mid-flight, not retryable as-is, reduce scope) and `Rejected` (admission, retryable later). Cancellation signals all of the query's tasks across workers; each releases pool reservations and deletes spill files, with cleanup guaranteed on panic via drop guards. The client receives a clear typed message and an OCSF audit event records the kill (query id, user, bytes spilled, reason).

Critical correctness rule, drawn directly from the Ballista failure that decided its wind-down: a query-level memory error must never be misclassified as a node failure. In Ballista, a task `InvalidArgument` evicted the whole executor and degraded the cluster. Here, `MemoryExhausted` fails one query and leaves the node serving.

### 6. Observability

Prometheus gauges and counters via `sqe-metrics`: node memory utilization, bytes spilled, queries killed by memory, admissions rejected. EXPLAIN ANALYZE already surfaces `spill_count`, `spilled_bytes`, and `spilled_rows` (`sqe-coordinator/src/explain.rs`); this lifts them to cluster telemetry. The per-query and per-stage telemetry shapes (akin to Ballista's `QueryStageSummary` and `TaskSummary`) extend the existing web UI spec (`2026-06-01-sqe-web-ui-design.md`) rather than introducing a new UI here.

## Query lifecycle under memory pressure

1. Admission. The coordinator receives a query. `AdmissionGate.try_admit` checks the coordinator node floor and the global max-concurrent cap. Rejected leads to a brief bounded queue, then a typed `Rejected` error.
2. Stage dispatch. The coordinator plans and dispatches stages. Each worker `NodeMemoryGovernor.try_admit` checks it can seat the stage floor. A worker that cannot is rejected; the coordinator uses existing failover to try another healthy worker, else fails the query with `MemoryExhausted`.
3. Steady execution. Operators reserve from the shared pool. With no pressure the query runs at full speed.
4. Spill cascade. The pool reaches its ceiling and drives the largest spillable consumer (hash-join build, aggregate, sort, or shuffle receiver) to local NVMe. The query slows but completes. This is the common pressure case and is invisible to the client beyond latency.
5. Backstop. If an unspillable reservation still cannot be met after all spillable consumers have spilled, the governor denies it, the operator returns `MemoryExhausted`, and `QueryAbortPath` fails that one query. Every other query and the node itself are untouched.

## Error handling

- Typed errors with distinct remediation. `MemoryExhausted` (mid-flight, reduce query scope) is separate from `Rejected` (admission, retry later). Both carry node, operator, and requested-versus-available bytes. Each variant carries `retryable` and `counts_to_failure` flags.
- Deterministic cleanup. Cancellation signals the query's tasks across all workers; each releases pool reservations and deletes spill files. Drop guards run cleanup even on panic, so a killed query never leaks memory or scratch disk.
- Clear client message plus an OCSF audit event with query id, user, bytes spilled, and kill reason.
- Coordinator parity. The coordinator final stage is a governed node; final-stage exhaustion fails the one query identically and never the process.

## Ballista learnings applied

From `docs/ballista-evaluation-learnings.md`:

- Disk-materialized shuffle. Ballista executors write shuffle partitions to local disk and serve them over Flight. Adopted as the on-disk format and spill model for `SpillableShuffleReceiver`, kept hybrid rather than always-on.
- Failure taxonomy enum with `retryable` and `counts_to_failure`. Adopted as the shape of the `QueryAbortPath` error type from day one, so subsystem D slots in cleanly.
- The cautionary anti-pattern. Ballista evicting a whole executor on a query-level error is the exact failure the blast-radius rule forbids.
- Task slots. Borrowed as the concurrency-cap unit in `AdmissionGate`, while keeping SQE's superior `WeightedScheduler` for placement.
- Observability response shapes. Noted as a future hook for the existing web UI spec, not built here.

We borrow patterns, not the framework. The prior attempt to adopt Ballista pulled SQE away from its own backend, codec, and per-task STS credential model. Lifting these patterns costs none of that.

## Testing strategy

- Unit. Governor accounting (floors, admit and reject, reclaim to zero). `SpillableShuffleReceiver` spill and restore round-trip correctness.
- Operator spill correctness. Force a tiny memory limit and assert hash-join, aggregate, sort, and shuffle spill and produce results identical to the non-spilled run.
- Sort-on-write regression. The partitioned or sorted CTAS that currently OOMs must now spill and complete.
- Concurrency soak (the headline gate). 100 to 300 concurrent queries on a memory-constrained cluster, asserting zero process OOM-kills (no SIGKILL); every query either completes or fails with a typed error.
- Blast-radius. One deliberately oversized query among many small ones, asserting only the big one fails and all small ones complete.
- Leak check. After kills, spill files are deleted and pool reservations are back to zero.

## Success criteria

At high data scale with 100 to 300 concurrent queries on a deliberately memory-starved cluster: no process is ever OOM-killed, all failures are clean typed errors, and queries that spill return correct results.

## References

- SQE distributed layer: `sqe-planner/src/{distributed_join,distributed_aggregate,distributed_sort,shuffle_exec}.rs`, `sqe-worker/src/shuffle.rs`
- Current memory pool: `sqe-coordinator/src/memory.rs`, EXPLAIN spill metrics in `sqe-coordinator/src/explain.rs`
- Ballista evaluation: `docs/ballista-evaluation-learnings.md`
- Lakehouse engine comparison and roadmap: `docs/internal/lakehouse-engine-comparison.md`
- Related specs: `2026-06-01-sqe-web-ui-design.md`, `2026-03-30-error-handling-design.md`, `2026-04-01-scheduling-evolution-design.md`
