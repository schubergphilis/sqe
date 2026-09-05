# 600: DoExchange shuffle subsystem is unreachable in production but exposed as a live worker endpoint

- Scope: sqe-worker (flight_service.rs, shuffle.rs, spill_buffer.rs), sqe-planner (shuffle_exec.rs, distributed_join.rs), sqe-spill (exchange.rs)
- Severity: high
- Category: architecture
- Effort: M (days)
- Status: open

## Problem

The worker advertises and serves `DoExchange` as a shuffle intake, and roughly
2,200 lines (shuffle.rs 1,293 + spill_buffer.rs 936 + the `do_exchange_spill`
half of flight_service.rs) implement spillable partition buffers, attempt
gating, governor grants and durable exchange manifests behind it. Nothing in
the engine ever calls it.

- The only producer, `ShuffleWriterExec::execute`, drops its input and returns an
  empty stream: `crates/sqe-planner/src/shuffle_exec.rs:288-295`
  ("For now, we return an empty stream. The actual Flight DoExchange send logic
  will be wired in ..."). `distributed_join.rs:459,471` constructs these
  writers, but `distributed_join` is only re-exported from
  `crates/sqe-planner/src/lib.rs:24`; no coordinator module references it.
- No caller of `.do_exchange(` exists anywhere under `crates/` outside the trait
  impl itself (grep confirmed).
- The legacy mpsc path requires a pre-registered receiver, but
  `ShuffleManager::register` is only ever called from a unit test
  (`crates/sqe-worker/src/shuffle.rs:889`). In production the branch at
  `flight_service.rs:1374-1383` always returns `NOT_FOUND`.
- The worker only ever executes `ScanTask`s (file list + projection + predicate
  + limit, `crates/sqe-planner/src/scan_task.rs:84`). All joins, aggregates and
  sorts run on the coordinator. The system is a distributed scan, not a
  stage-based MPP, and the docs and memory notes describing "genuine stage-based
  MPP" and "shuffle has no spill" describe code that is not on any query path.

Yet the endpoint is live. Any caller holding the worker secret (see issue 601)
can push arbitrary RecordBatches into it. With a spill manager configured
(`flight_service.rs:1358-1369`) those bytes are written to the worker's spill
store keyed by caller-chosen `query_id`/`stage_id`. Two registries grow without
bound because nothing ever removes entries:

- `AttemptGate::winners` (`shuffle.rs:117-148`) has `admit` and `winner` but no
  removal.
- `ExchangeAttemptStore` (`crates/sqe-spill/src/exchange.rs`, `impl` block)
  exposes `publish`, `commit_winner`, `admit`, `reusable`, `winner`, `get` and
  no prune or remove.

## Why it matters

- Every reviewer, including the July production-readiness review (P0-1
  "shuffle is memory-only, not byte bounded"), has spent effort hardening a
  path no query uses. The "spillable DoExchange" work that followed fixed the
  same unused path.
- The endpoint is attack surface with no benefit: a secret holder can fill the
  spill volume and leak memory in two unbounded maps on every worker.
- Architecture docs (CLAUDE.md "Workers: Stateless DataFusion executors
  receiving secured plan fragments") do not match the code. New contributors
  design against a shuffle that does not exist.

## Proposed change

Decide, then make the code match the decision:

1. If stage-based execution is not on the roadmap for the next two quarters:
   delete `do_exchange`/`do_exchange_spill`, `shuffle.rs` receiver/manager
   types, `spill_buffer.rs`, `ExchangeAttemptStore`, and the
   `ShuffleWriterExec`/`ShuffleReaderExec`/`distributed_join` planner nodes.
   Keep `HashPartitioner`/`RangePartitioner` only if something uses them.
   Return `Status::unimplemented` from `do_exchange`.
2. If it is on the roadmap: put the whole subsystem behind a cargo feature
   (`worker-shuffle`) that is off by default, add a `ShuffleManager::remove` and
   `ExchangeAttemptStore::forget_query` called from a coordinator
   "query finished" action, and write the producer side before shipping the
   consumer side again.

Either way, rewrite the CLAUDE.md worker paragraph and
`docs/site/book/src/design-notes/datafusion-architecture.md` to say
"distributed scan with coordinator-side joins" until a real exchange exists.

## Acceptance criteria

- `grep -rn "do_exchange\|ShuffleWriterExec" crates --include='*.rs'` returns
  either nothing outside a feature-gated module, or a call site on a real query
  path with an integration test that shuffles a join across two workers.
- No `HashMap` keyed by a network-supplied `query_id` survives without a
  removal path; a unit test registers 10k attempts and asserts the map is
  empty after the query-finished hook.
- Architecture docs describe the executed topology.

## Related

- July 2026 production-readiness review P0-1 and P0-2 (targeted this path).
- Memory: "Distributed MPP architecture" and "Enterprise concurrency
  hardening" notes assume a stage-based shuffle exists.
- Issue 601 (who can reach the endpoint), 609 (heartbeat has no capability
  negotiation to gate it).
