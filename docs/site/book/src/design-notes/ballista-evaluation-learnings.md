# Ballista evaluation: what we learned, and why we wound it down

Date: 2026-05-31
Status: final. Ballista integration removed from the codebase on branch
`chore/wind-down-ballista`. The detailed design and phase reports are archived
under `docs/archive/ballista-evaluation/`.

## The one-paragraph version

We evaluated Apache Ballista 53 as a drop-in distributed execution engine,
opt-in behind a `[query] engine = "ballista"` flag, with the bespoke layer
staying the default. It reached correctness parity on the common path (TPC-H
22/22 at SF0.1 and SF1) but lost on the parts that matter: it is roughly 2.2x
slower where it completes, it cannot finish the TPC-DS analytical core, and its
scheduler is less capable than the one we already have. Adopting it would have
been a step down in execution, traded for a maintained scheduler we do not
actually need yet. So we removed the integration and kept the bespoke layer.
This document records what the experiment taught us, so the next person does
not repeat it, and so the genuinely useful findings survive.

## What we set out to do

The goal was to reduce the bespoke distributed-execution plumbing (roughly
11.5K lines: `distributed_scan`, `shuffle`, `stage_planner`,
`distributed_join|sort|aggregate`, `worker_registry`, `heartbeat`,
`channel_pool`, `credential_refresh`) by leaning on Ballista's scheduler. The
contract was deliberately cautious: bespoke stays the default, Ballista is
opt-in, and the bespoke layer retires only when Ballista reaches functional
parity AND speed parity. Correctness first, speed last.

We built the whole opt-in path: a `sqe-ballista` crate with logical and
physical extension codecs, a coordinator-embedded scheduler, `sqe-worker`
running as a Ballista executor, per-user bearer threaded through the plan, and
SQE's UDFs registered on the scheduler and executor. It worked end to end for
simple queries.

## What we found

The honest verdict, measured on the same debug build and the same machine
(co-located scheduler plus two executors):

| Workload | Bespoke | Ballista |
|---|---|---|
| TPC-H (common path) | 22/22, ~10.8s | 22/22, ~24s (~2.2x slower) |
| TPC-DS (analytical core) | 99/99, ~33s | does not complete |
| SSB | 13/13 | 11/13, ~18x slower |
| Cross-stage dynamic filters | yes | disabled |
| Cache-affinity placement | yes | round-robin only |
| Per-task STS credential refresh | yes | no hook |

The product surface (auth, protocols, Trino-compat, GRANT/REVOKE, masks, row
filters, catalog backends) never moves to Ballista. It lives coordinator-side,
above the execution seam, and we keep all of it regardless of engine. So the
only real comparison is the distributed-execution layer, and there Ballista is
behind what we already built, not ahead.

The catch that makes this decisive: Ballista does not sell the scheduler alone.
The scheduler is welded to its executor and shuffle protocol. To get the
maintained scheduling brain you have to swallow the executor, and that executor
is the thing that loses on speed and cannot run TPC-DS. There is no seam to
borrow the cheap half.

## Why TPC-DS does not complete (the two real blockers)

These two are upstream Ballista or datafusion-proto problems, not anything SQE
diverged on. Both are worth filing upstream.

1. **Aggregate output naming does not survive the proto round-trip.**
   Multi-stage queries with `count(*)` over joins fail on the executor with a
   DataFusion internal assertion: `Input field name <col> does not match with
   the projection expression count(*)`. The mismatch is in an
   `AggregateExec`/`ProjectionExec` pair that crosses the stage boundary via
   Ballista's default physical codec (datafusion-proto), not SQE's scan codec.
   We saw 48 occurrences in a single TPC-DS sweep. This blocks the analytical
   core.

2. **A task error evicts the whole executor.** When a task fails with the
   assertion above (a query-level error), Ballista's scheduler treats it as an
   executor transport failure and removes the executor from its registry. The
   mechanism is precise: in the push path, `launch_tasks` RPCs `LaunchMultiTask`;
   the executor's handler decodes the physical plan from the task proto and
   returns `Status::invalid_argument` if decoding fails; that surfaces at the
   scheduler as an internal error, and `state/mod.rs` evicts the executor with
   the comment "It's OK to remove executor aggressively." So one bad query
   degrades the whole cluster. A task `InvalidArgument` (fail the query) should
   be distinguished from a transport failure (evict the executor).

There was also a simpler hang on the first pass (executors blocking the tokio
runtime in a sync codec decode that did an async catalog round-trip per task).
We fixed that by serializing enough table state into the plan bytes to rebuild
the scan without a catalog call. That fix was necessary and worked, but it only
peeled the top layer and exposed the two blockers above.

## Ballista's architecture, briefly

Three roles. The **scheduler** (a gRPC server) splits the physical plan into
stages at shuffle boundaries, holds cluster state in memory, and push-schedules
tasks to executors. Each **executor** (gRPC plus an Arrow Flight server)
registers, heartbeats, runs a stage fragment, writes shuffle partitions to
local disk, and serves them to peer executors over Flight. The **client**
submits a plan to the scheduler and streams the final stage back. Plans cross
the wire as protobuf, with extension codecs for custom nodes, and those codecs
must match at all three sites.

What Ballista deliberately does not have: any authentication or per-user
identity, any frontend protocol beyond its own, any SQL dialect or policy
layer, any catalog abstraction, and any per-task credential hook. It assumes a
trusted, single-tenant, internal cluster. That is the right design for what it
is, and exactly why the entire SQE product surface has to sit in front of it.

## Where SQE is actually ahead

This surprised us. SQE's `WeightedScheduler` (least-loaded bin-packing with
consistent-hash worker affinity) and `WorkerRegistry` (health plus in-flight
load) are ahead of Ballista 53's scheduler, which only offers Bias and
RoundRobin slot-binding. Ballista 53 even removed its consistent-hash policy
(a source comment notes it does not work in pull mode), has no
locality/data-affinity for source scans, and has no speculative execution or
straggler handling. So "learn from Ballista's scheduling" resolved to: do not
borrow its scheduling core.

## Borrowable ideas (the useful residue)

These are worth lifting into the bespoke layer or the planned web UI. File and
line references point into the Ballista 53 source for whoever implements them.

1. **Failure taxonomy with `retryable` and `count_to_failures` flags.** A small
   enum of failure reasons, each tagging whether to retry and whether the
   attempt counts toward a cap, cleanly separates transient I/O from
   query-level errors from shuffle-data-loss. This is precisely the distinction
   the eviction bug blurred, and the lesson applies to our own failure
   handling. (`ballista-core/src/error.rs`)

2. **The REST observability API and its JSON shapes.** Ballista 53 ships no
   bundled web UI, only a feature-gated axum REST API under `/api`: `state`,
   `executors`, `executor/{id}`, `jobs`, `job/{id}`, `job/{id}/stages`,
   `job/{id}/dot`. The response shapes (`JobResponse`, `QueryStageSummary`,
   `TaskSummary`, `ExecutorResponse`, including per-stage task duration and
   input-row percentiles) map almost one-to-one onto SQE's existing
   `QueryRecord`, `FragmentInfo`, and `WorkerState`. This is the contract to
   mirror for an SQE web UI, the same way Trino's UI organizes queries, stages,
   tasks, and workers. (`ballista-scheduler/src/api/`)

3. **DOT/SVG query-graph generation from the execution graph.** Ballista turns
   the stage DAG into Graphviz and serves it at `/api/job/{id}/dot[_svg]`. A
   low-effort way to give a query-plan visualization.
   (`ballista-scheduler/src/state/execution_graph_dot.rs`)

4. **A five-state stage machine with explicit Resolved/UnResolved.** Modeling
   "inputs not yet ready" as a first-class state, gated by a `resolvable()`
   check, makes shuffle-dependency scheduling explicit and testable.
   (`ballista-scheduler/src/state/execution_stage.rs`)

5. **Per-partition attempt tracking inside the stage.** A `Vec<usize>` of
   per-partition failure counts alongside the task list gives clean two-tier
   (task then stage) retry caps. Our `FragmentInfo` could adopt the same shape.

6. **An encoded-stage-plan cache.** Memoizing serialized physical plans so
   re-binding a stage's tasks does not re-encode the plan each time. Relevant to
   our per-fragment dispatch hot path.

What to skip: Ballista's slot-binding policies (ours are better), its lack of
scan locality, and its absence of speculative/straggler handling (a real gap in
both systems, worth solving on our own terms).

## What we kept from the experiment

- **The ADBC unpadded-base64 handshake fix** in the Flight SQL server. We found
  it while testing through an ADBC client: the coordinator's Basic-auth decoder
  was padding-strict and rejected the unpadded base64 that the Go ADBC driver
  sends, so no ADBC client (including the dbt-sqe adapter) could connect at all.
  That fix stands on its own and stays.
- **The `/api/v1/status` health endpoint** (Ballista/DataFusion-style cluster
  status JSON) predates this work and is unaffected.

## Where the detail lives

The full design, the phase reports, the PoC, and the divergence ledger (D1
through D13) are archived under `docs/archive/ballista-evaluation/`. The git
history on the abandoned `feat/vendored-ballista-improvements` branch carries
the implementation if it is ever needed again.
