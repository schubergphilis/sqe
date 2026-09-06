# 600: DoExchange shuffle subsystem is unreachable in production but exposed as a live worker endpoint

- Scope: sqe-worker (flight_service.rs, shuffle.rs, spill_buffer.rs), sqe-planner (shuffle_exec.rs, distributed_join.rs), sqe-spill (exchange.rs)
- Severity: high
- Category: architecture
- Effort: M (days)
- Status: fixed (branch `fix/identity-transport-trust-p1`, 2026-09-05)
- Issue: #607

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

## Resolution (2026-09-05)

Option 2. Stage-based execution stays on the roadmap, so the subsystem is kept
but compiled out by default, and the live endpoint is inert without it.

### Feature names (all off by default)

| Crate | Feature | Gates |
| --- | --- | --- |
| `sqe-worker` | `worker-shuffle` | `shuffle.rs`, `spill_buffer.rs`, `do_exchange_spill`, the shuffle-only service state, and the serving `do_exchange`. Implies `sqe-spill/exchange`. |
| `sqe-planner` | `shuffle-planning` | `shuffle_exec.rs`, `distributed_join.rs` and their `lib.rs` re-exports. |
| `sqe-spill` | `exchange` | `exchange.rs` (`ExchangeAttemptStore`, `AttemptManifest`, `TaskKey`) and its `lib.rs` re-exports. |

### What changed

- `crates/sqe-worker/Cargo.toml`, `crates/sqe-planner/Cargo.toml`,
  `crates/sqe-spill/Cargo.toml`: new `[features]` sections, `default = []`.
- `crates/sqe-worker/src/lib.rs`: `shuffle` and `spill_buffer` are
  `#[cfg(feature = "worker-shuffle")]`.
- `crates/sqe-worker/src/flight_service.rs`: two `do_exchange` bodies selected
  by `cfg`. Without the feature it keeps `verify_worker_secret` (so an
  anonymous caller cannot probe the build) and then returns
  `Status::unimplemented("DoExchange shuffle intake is not enabled on this
  worker")` without reading a frame. Gated alongside it: the
  `shuffle_manager` / `shuffle_compression` / `shuffle_memory_budget` /
  `exchange_store` fields and both constructors' initialisers, the
  `with_shuffle_compression` / `with_shuffle_memory_budget` /
  `with_shuffle_memory_budget_atom` builders, the `shuffle_manager` /
  `exchange_store` / `shuffle_memory_budget` accessors, `do_exchange_spill`,
  `ShufflePartitionConsumer`, `GrantHeldStream`, `is_exchange_cancelled`,
  `DEFAULT_SHUFFLE_MEMORY_BUDGET`, and the `FlightRecordBatchStream`,
  `crate::shuffle`, `crate::spill_buffer` and shuffle-only `sqe_spill`
  imports. With the feature off no map keyed by a network-supplied
  `query_id`/`stage_id` exists in the process.
- `crates/sqe-worker/src/bootstrap.rs`: `with_shuffle_compression` and
  `with_shuffle_memory_budget_atom` move into a `cfg` block. `WorkerHotConfig`
  still carries the resolved shuffle budget either way, so config validation
  and hot-reload arithmetic are unchanged.
- `crates/sqe-planner/src/lib.rs`, `crates/sqe-spill/src/lib.rs`: module
  declarations and re-exports gated.
- `crates/sqe-worker/tests/zero_pruning_memory.rs`: the two shuffle gates
  (`phase0_reproducer_shuffle_exceeds_byte_budget`,
  `shuffle_ten_x_budget_completes_with_spill`) and their imports are gated.

### Removal paths (the second acceptance criterion)

Every one of these maps is keyed by ids the caller supplies on the wire, and
none of them had a removal path:

- `AttemptGate::forget_query(query_id) -> usize` and
  `AttemptGate::tracked_attempts()` (`crates/sqe-worker/src/shuffle.rs`).
- `ShuffleManager::forget_query(query_id) -> usize` and
  `ShuffleManager::tracked_stages()` (same file). The pre-existing `remove`
  takes a `(query_id, stage_id)` pair, so it cannot clean up a query that ends
  without a per-stage teardown; `forget_query` sweeps the receiver registry and
  delegates to the attempt gate.
- `ExchangeAttemptStore::forget_query(query_id) -> usize`, plus
  `tracked_manifests()` / `tracked_winners()`
  (`crates/sqe-spill/src/exchange.rs`).
- `WorkerFlightService::forget_query(query_id)` ties the two together as the
  single "query finished" entry point for a coordinator hook.

### Tests

- `sqe_worker::shuffle::tests::forget_query_empties_every_registry_after_ten_thousand_attempts`
  (feature-gated with the module): registers 100 stages x 100 partitions =
  10,000 attempts across one query in all three registries plus one attempt for
  a second query, asserts `forget_query` drops exactly 30,100 entries, that the
  second query's entries survive, and that all three maps are empty after the
  second sweep.
- `flight_service::tests::do_exchange_is_unimplemented_without_worker_shuffle`
  (`#[cfg(not(feature = "worker-shuffle"))]`): the default build answers
  `Unimplemented` with the exact message.
- `flight_service::tests::do_exchange_still_rejects_a_bad_secret_first`
  (same cfg): the secret gate is still in front, and the request decoder is
  never touched.

### Verification

Run on branch `fix/identity-transport-trust-p1` (a shared worktree with several
engineers' changes in flight, so the workspace baseline moved under these runs):

| Command | Result |
| --- | --- |
| `cargo check -p sqe-worker --all-targets` (default) | pass |
| `cargo check -p sqe-worker --all-targets --features worker-shuffle` | pass |
| `cargo check -p sqe-planner --all-targets` / `--features shuffle-planning` | pass |
| `cargo check -p sqe-spill --all-targets` / `--all-features` | pass |
| `cargo test -p sqe-worker` (default) | 117 passed, 0 failed, 2 ignored |
| `cargo test -p sqe-worker --features worker-shuffle` | 162 passed, 0 failed, 2 ignored |
| `cargo clippy -p sqe-worker -p sqe-planner -p sqe-spill --all-targets` and `--all-features` | pass, zero warnings in these three crates |

The first acceptance grep,

```
grep -rn "do_exchange\|ShuffleWriterExec" crates --include='*.rs'
```

now hits only feature-gated modules (`shuffle_exec.rs`, `distributed_join.rs`,
`spill_buffer.rs`, the gated halves of `flight_service.rs` and the gated
re-exports in `sqe-planner/src/lib.rs`), the inert `do_exchange` stub, tests,
and prose doc comments in `distributed_sort.rs` that name the nodes without
calling them.

`cargo check --workspace --all-targets` still fails on
`crates/sqe-coordinator/tests/hive_scan_tasks_test.rs:175` with "no field
`s3_access_key` on type `&ScanTask`", from the concurrent `ScanCredentials`
refactor on this branch. Unrelated to this change: every other workspace member
compiles.

CI keeps the gated code covered. `aikido-test-rust` runs `cargo test
--workspace --all-features` and `make clippy` runs `cargo clippy --all-targets
--all-features`, so `worker-shuffle`, `shuffle-planning` and `exchange` are all
compiled and tested on every pipeline; only shipped binaries lose them.

### Left in place

- `HashPartitioner` / `RangePartitioner` stay inside `shuffle.rs` (gated with
  it); nothing outside the shuffle path uses them.
- `distributed_sort.rs` and `stage_planner.rs` keep prose references to
  `ShuffleWriterExec` / `ShuffleReaderExec` in doc comments. They are comments,
  not call sites, and both modules stay in the default build.
- `spill_manager`, `memory_governor` and `live_consumers` on
  `WorkerFlightService` are NOT gated: they are the general bounded-memory
  substrate (operator spill, admission), not shuffle-only state.
