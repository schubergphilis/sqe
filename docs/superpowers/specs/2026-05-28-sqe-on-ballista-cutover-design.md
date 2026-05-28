# SQE-on-ballista cutover design

Date: 2026-05-28
Status: approved-in-principle (user: "let's do the cutover", "keep on building")
PoC: `docs/superpowers/specs/2026-05-28-sqe-on-ballista-poc-report.md` (GREEN)
Branch: `feat/sqe-ballista-poc-spike` -> cutover work continues here / new `feat/sqe-ballista-cutover`

## Goal

Replace SQE's bespoke distributed execution layer (~11,560 LOC across 17
files) with Apache Ballista 53.0.0 as the maintained distributed runtime.

**Guiding rule (user directive):** use ballista's code wherever it covers
the need. Diverge only where ballista is missing functionality, or where
SQE's existing integration is demonstrably better. Every divergence gets
written down: what we changed, why, and how ballista could be improved
upstream. That ledger lives in this doc (section "Divergence ledger") and
grows as we build.

## What gets removed (the bespoke layer)

| Crate | Files | LOC |
|---|---|---|
| sqe-coordinator | distributed_scan, worker_registry, scheduler, channel_pool, credential_refresh | ~3,540 |
| sqe-planner | stage_planner, shuffle_exec, distributed_join, distributed_sort, distributed_aggregate | ~4,751 |
| sqe-worker | flight_service (distributed parts), shuffle, executor (scan dispatch), heartbeat, credential_channel | ~3,020 |

Ballista replaces each concern:

| SQE bespoke concern | Ballista replacement |
|---|---|
| `WeightedScheduler` + `stage_planner` (stage decomposition, task placement) | Ballista scheduler + DataFusion physical optimizer stage splitting |
| `shuffle_exec` / worker `shuffle.rs` (hash/range/broadcast shuffle over Flight DoExchange) | Ballista `ShuffleWriterExec` / `ShuffleReaderExec` |
| `distributed_join` / `distributed_sort` / `distributed_aggregate` | DataFusion's own distributed-aware physical plan under ballista |
| `worker_registry` + `heartbeat` + `channel_pool` (discovery, health, conns) | Ballista executor registration + heartbeat + scheduler gRPC |
| `distributed_scan.rs` (Flight do_get scan dispatch, retry, failover, local fallback) | Ballista task scheduling + retry + the iceberg `TableProvider` returning a plain scan, bridged by our codecs |

## What we keep (SQE's value, not ballista's job)

- **Flight SQL frontend** (`flight_sql.rs`) — SQE's client-facing protocol. Ballista is the backend; clients never speak to it directly.
- **Session management, OIDC auth, policy enforcement** — planning stays in the coordinator. The policy-rewritten `LogicalPlan` is what we submit to ballista.
- **The iceberg catalog integration** (`sqe-catalog`) and the per-query bearer model.
- **Credential refresh** for long-lived STS tokens — ballista has no equivalent (see Divergence ledger D3).

## Architecture (target topology)

```
Client (Flight SQL / Trino HTTP)
   |
   v
SQE Coordinator process
  - Flight SQL server (unchanged frontend)
  - OIDC auth + session manager
  - SQL -> LogicalPlan -> policy rewrite -> optimize
  - EMBEDS ballista SchedulerServer (in-process)
      - session builder installs: iceberg catalog provider,
        IcebergLogicalCodec, IcebergPhysicalCodec, per-query bearer
  - submits the rewritten LogicalPlan to the scheduler
   |
   v  (ballista scheduler <-> executor gRPC + shuffle)
SQE Worker process(es) = ballista Executor
  - override_config_producer: install per-query bearer + iceberg catalog
  - override_runtime_producer: object store with per-query S3 creds
  - codecs rehydrate IcebergTableScan from the catalog (executor-side creds)
```

### New crate: `sqe-ballista`

Promote the PoC crate `sqe-ballista-poc` into a real integration crate
`sqe-ballista` (library, not a bin). It owns:

- `IcebergLogicalCodec` + `IcebergPhysicalCodec` (moved out of the PoC, hardened).
- The session-builder / config-producer / runtime-producer wiring that
  installs iceberg + auth into both scheduler and executor session state.
- A thin `BallistaCoordinator` facade the Flight SQL handler calls:
  `submit(logical_plan, session) -> RecordBatchStream`.
- The executor bootstrap (`run_executor(config)`), replacing sqe-worker's
  bespoke flight service.

`sqe-coordinator` depends on `sqe-ballista` and calls the facade instead of
`try_distribute_scan` / `DistributedScanExec`. `sqe-worker` becomes a thin
binary that calls `sqe_ballista::run_executor`.

## The one real design problem: predicate / runtime-filter serialization

The PoC physical codec bails when the scan carries pushed-down predicates.
Two predicate kinds must cross the wire:

1. **Iceberg `Predicate`** pushed into the scan at plan time (static
   filters). **DONE (Phase 1).** Iceberg's `Predicate` already derives
   `Serialize`/`Deserialize`, so it rides the wire as a field of
   `EncodedScan` directly; the executor re-binds it against the reloaded
   table schema via `scan_builder.with_filter` when the scan runs. No
   custom IR needed. Covered by `encoded_scan_round_trips_predicate`.

2. **SQE `DynamicPredicate` runtime filters** (build-side join bloom/min-max
   pushed into the probe-side scan). These are produced *during* execution,
   so they cannot be baked into the submitted plan. Options:
   - **2a (chosen for v1):** disable cross-stage dynamic-filter pushdown on
     the ballista path initially; rely on ballista's own join execution.
     Static predicates still push down. Document the perf delta vs the
     bespoke path; measure it.
   - **2b (follow-up):** implement dynamic filter transport as a ballista
     physical node + codec. Defer until v1 parity is proven.

This is the only work-package that is design, not mechanical. Everything
else is "wire ballista in, delete the old path".

## Phasing (each phase ends GREEN + committed)

- **Phase 0 — `sqe-ballista` crate.** Promote PoC -> library crate. Move
  both codecs in, add unit tests. Keep the PoC bin as an example/smoke test.
- **Phase 1 — static predicate serialization.** Extend `EncodedScan` +
  `from_codec_parts` to carry the iceberg `Predicate`. Test: a scan with a
  `WHERE` filter round-trips through the codec and prunes correctly.
- **Phase 2 — coordinator embeds the scheduler.** `BallistaCoordinator`
  facade; Flight SQL handler submits the rewritten LogicalPlan. Behind a
  config switch `[coordinator] engine = "ballista" | "legacy"` so we can
  A/B and keep the old path alive during migration.
- **Phase 3 — worker as ballista executor.** `sqe_ballista::run_executor`
  with config/runtime producers installing per-query bearer + iceberg
  catalog + object store. `sqe-worker` bin shrinks to a wrapper.
- **Phase 4 — credential passthrough + refresh on the ballista path.**
  Per-query bearer install (config producer) + STS refresh hook (D3).
- **Phase 5 — parity + perf.** Run TPC-H / TPC-DS / SSB at SF0.1 then SF1
  in ballista mode; compare against committed baselines
  (`tpch-sf1-flight-2026-04-06T20:57:10.json`, distributed 22/22 12.0s).
  Gate: correctness parity 100%, perf within agreed band.
- **Phase 6 — delete the bespoke layer.** Remove the 17 files once ballista
  mode is default and parity holds. Drop the `engine = "legacy"` switch.

## Testing strategy

- **Unit:** codec round-trip (logical + physical), predicate encode/decode.
- **Integration (single-node ballista standalone):** the PoC query +
  filtered scans + a join + an aggregate, against the Polaris+RustFS stack.
- **Integration (multi-executor):** `docker-compose.distributed.yml`
  repurposed to run 1 coordinator (embedded scheduler) + 2 ballista
  executors. Reuse `scripts/distributed-test.sh` assertions.
- **Parity:** full TPC-H/DS/SSB compare-vs-trino at SF0.1, then SF1.
- **Regression gate:** benchmark JSON committed; compare to baselines.

## Divergence ledger

Each entry: what we diverge on, why (ballista missing / SQE better), and the
upstream improvement. Appended as we build.

- **D1 — iceberg codecs.** Ballista (and `iceberg-datafusion`) ship no
  `Logical`/`PhysicalExtensionCodec` for iceberg tables. *Why diverge:*
  missing functionality; serialization is mandatory. *Upstream:*
  `iceberg-datafusion` should own both codecs, parameterized over the
  catalog. Highest-value PR.
- **D2 — `IcebergTableScan::from_codec_parts`.** Stock `new()` is
  `pub(crate)` and takes raw DataFusion `Expr`. *Why diverge:* no public
  constructor usable from an out-of-crate codec. *Upstream:* add a public
  constructor (pairs with D1). Currently a vendor patch.
- **D3 — STS credential refresh mid-scan.** Ballista has no per-task
  credential hook; object store creds are static for the executor lifetime.
  SQE refreshes vended S3 creds before the 5-min expiry. *Why keep SQE's:*
  long scans outlive STS tokens. *Upstream:* ballista executor needs a
  per-task credential/runtime hook (the `override_runtime_producer` runs
  once at startup, not per task). For v1, reload-from-catalog at task start
  mints fresh creds per task; document if long single tasks still exceed
  expiry.
- **D4 — sync codec on async catalog.** `try_decode` is sync but the
  catalog lookup is async, on a tokio worker. We use `block_in_place` +
  `Handle::block_on`. *Upstream:* the codec trait could expose an async
  variant, or ballista could decode off the reactor.
- **D5 — (candidate) cache-affinity scheduling.** SQE's `WeightedScheduler`
  places scan tasks on workers that already cache the relevant manifests
  (20% tolerance). Ballista's scheduler is load/round-robin. *Decide in
  Phase 5:* measure the hit-rate loss; if material, upstream a pluggable
  task-placement hook to ballista. Otherwise drop the heuristic.
- **D6 — dynamic / runtime-filter pushdown across stages.** SQE's
  `IcebergTableScan` absorbs build-side join filters (`DynamicFilterPhysicalExpr`)
  via `handle_child_pushdown_result` and feeds them into iceberg row-group
  pruning mid-stream. These are produced *during* execution, so they can't
  be serialized into the submitted plan. *v1 decision:* the physical codec
  carries only static `Predicate`s; runtime filters stay local to whatever
  ballista stage runs the join+scan together, and cross-stage dynamic
  pushdown is disabled on the ballista path. *Why diverge:* ballista has no
  dynamic-filter transport. *Upstream:* a ballista physical node + codec for
  dynamic-filter propagation between stages. Measure the perf delta in
  Phase 5; implement transport as a follow-up only if material.

## Rollback

The `engine = "legacy"` config switch keeps the bespoke path runnable
through Phase 5. If ballista mode fails parity or perf gates, flip back to
legacy with zero code change. Phase 6 (deletion) only happens after the
gate passes and ballista mode has been default for a soak period.

## Success criteria

1. TPC-H/DS/SSB correctness parity 100% in ballista mode (SF0.1 + SF1).
2. Perf within agreed band of the distributed baseline (12.0s SF1 TPC-H).
3. The 17 bespoke files deleted; net LOC down ~10K.
4. Divergence ledger complete; upstream PRs filed for D1/D2 at minimum.
