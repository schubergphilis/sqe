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

### Codec target correction (found in Phase 2)

The PoC codecs target **iceberg-datafusion's** `IcebergTableProvider` /
`IcebergTableScan`. The real coordinator plan does **not** contain those.
SQE registers its own `SqeCatalogProvider` (`sqe-catalog`), whose tables are
`SqeTableProvider`, and whose `scan()` returns SQE's own `IcebergScanExec`
(`crates/sqe-catalog/src/iceberg_scan.rs`). That node carries features the
upstream node lacks: pushed-down dynamic filters, late materialization,
small-file handling, manifest/direct-read concurrency, cached statistics,
policy integration. Converging SQE's scan onto the upstream node would
forfeit those, so the production codecs **target SQE's own nodes**:

- Logical codec rehydrates `SqeTableProvider` via the registered
  `SqeCatalogProvider` (`schema(ns).table(name)`), same reference-encode /
  catalog-reload pattern the PoC proved.
- Physical codec encodes `(namespace, table, snapshot_id, projection,
  predicate, output schema, config knobs)` and rebuilds `IcebergScanExec`
  on the executor by reloading the `Table` from the `SessionCatalog`.
  Needs a public reconstruct constructor on `IcebergScanExec`
  (`from_codec_parts`-style), the sqe-catalog analogue of the
  iceberg-datafusion D2 patch.

`IcebergScanExec.table: iceberg::Table` is not serializable (holds
`FileIO`, S3 creds), confirming the reload-from-catalog approach. Dynamic
`pushed_down_filters` are runtime-only and are NOT serialized (ledger D6).
The PoC's iceberg-datafusion codecs stay in the crate as the upstream-PR
reference (D1) but are not on SQE's hot path.

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
- **Phase 2 — coordinator embeds the scheduler. DONE / GREEN.** Config
  switch `[query] engine = "ballista" | "legacy"` (default legacy);
  `submit_standalone` facade; `open_stream` branch submits the
  policy-rewritten LogicalPlan to an in-process ballista standalone cluster
  per query. **Validated live:** full TPC-H SF0.1 (22/22) through the real
  coordinator wiring in ballista mode, row counts identical to the legacy
  path query-for-query — correctness parity confirmed. Perf: ballista 31.4s
  vs legacy 12.8s at SF0.1, the expected cost of standalone-per-query
  (a fresh scheduler+executor per statement); Phase 3's shared cluster +
  remote executors closes that gap. Benchmark JSON committed.
- **Phase 3 — worker as ballista executor.** `sqe_ballista::run_executor`
  boots a real ballista executor process (`start_executor_process`) with the
  SQE codecs + config/runtime producers. Coordinator embeds a shared ballista
  **scheduler** (`start_server`) at startup and submits via
  `SessionContext::remote_with_state(scheduler_url, state)` — replacing the
  standalone-per-query facade, which closes the Phase 2 perf gap.
  `sqe-worker` bin shrinks to call `run_executor`.

  **Auth scope decision:** the *legacy* distributed path already uses static
  storage creds from `[storage]` (try_distribute passes no per-session
  bearer — confirmed in the code). So Phase 3 targets legacy parity: the
  executor + scheduler build their `SessionCatalog` / `SqeCatalogProvider`
  from their **own config** (catalog url + warehouse + static S3 creds),
  single-tenant. The codecs on the cluster side therefore hold a
  config-built catalog, not a per-session one. Per-user OIDC bearer
  passthrough to executors is **Phase 4** (the multi-process auth question);
  it requires propagating the bearer through the submitted SessionConfig and
  having the executor codec build/caches a per-token catalog. Codecs must be
  installed in all three places (client SessionConfig, SchedulerConfig,
  ExecutorProcessConfig) and match.

  **DONE / GREEN.** `sqe-ballista/src/cluster.rs` implements
  `build_cluster_catalog`, `run_executor`, `start_scheduler`,
  `submit_remote`, and a process-global `get_or_init_runtime`. The
  coordinator starts the embedded scheduler eagerly at startup and submits
  via `submit_remote`; `sqe-worker` runs as a ballista executor when
  `engine=ballista`. **Validated live:** coordinator (embedded scheduler,
  no local executor) + **two separate executor processes** ran full TPC-H
  SF0.1 22/22, row counts identical to legacy. Since the coordinator hosts
  no executor, the two worker processes provably executed every task.
  Perf at SF0.1: ~27s multi-process vs ~31s standalone vs ~13s legacy — the
  cluster overhead still dominates at tiny scale; the shared-cluster win is
  expected to show at SF1+ (Phase 5). Endpoints via env
  `SQE_BALLISTA_SCHEDULER_HOST/PORT`, `SQE_BALLISTA_EXECUTOR_HOST/GRPC_PORT`.
- **Phase 4 — credential passthrough + refresh on the ballista path.**
  **PARTIAL — infra in place, per-user passthrough blocked by ballista D8.**
  Built: `auth_ext::SqeAuthOptions` config extension, executor-side
  `SqePhysicalCodec::resolve_catalog` (mints + caches a per-user
  `SessionCatalog` from the bearer, falls back to the single-tenant config
  catalog when absent), config producers that register the extension on
  scheduler + executor, and `submit_remote` stamping the user bearer.
  **Found during validation:** the bearer does NOT reach the executor.
  Ballista propagates session settings via `ConfigOptions::entries()` ->
  `set()`, but DataFusion emits `ConfigExtension` entries *unprefixed*
  ("bearer", not "sqe_auth.bearer") and the receiving `set()` can't route an
  unprefixed key back to the extension, so it is silently dropped (ledger
  D8). The executor falls back to single-tenant, which **equals legacy
  parity** (the legacy distributed path also used static `[storage]` creds).
  Per-user passthrough is an enhancement *beyond* legacy and is deferred to a
  designed follow-up: thread the bearer through the plan node
  (`SqeLogicalCodec` encodes it on the client -> scheduler stamps it on the
  rehydrated `SqeTableProvider` -> `IcebergScanExec` -> `EncodedSqeScan`
  physical bytes -> executor `resolve_catalog`), bypassing ballista session
  propagation entirely. The single-principal test stack ("all users share a
  single service token") cannot validate true multi-tenancy regardless. STS
  refresh: largely obviated by reload-per-task (each task mints fresh vended
  creds at load_table); true mid-task refresh for very long tasks remains a
  deferred edge case (D3).
- **Phase 5 — parity + perf. CORRECTNESS GREEN; perf gate deferred to
  real hardware.** TPC-H SF1 in ballista multi-process mode: **22/22, row
  counts identical to legacy** (correctness parity confirmed at SF1, not
  just SF0.1). Perf on a single dev machine (debug build, 2 co-located
  executors): ballista 147s vs legacy 102s — ballista *slower*, as expected:
  co-located executors share one machine's cores so there's no parallelism
  to win, only serialization + shuffle-over-gRPC + per-task table-reload
  overhead. **The perf question cannot be answered here.** A real evaluation
  needs release builds on separate worker machines (the committed 12.0s
  distributed baseline was exactly that). Until that runs, ballista mode is
  correct but its production perf is unproven. TPC-DS/SSB parity in ballista
  mode also still to run.
- **Phase 6 — delete the bespoke layer. BLOCKED on the Phase 5 perf gate.**
  Removing ~11.5K LOC is irreversible-ish and must not happen until ballista
  mode is proven on real multi-node release hardware AND made default after a
  soak. Correctness parity alone is not sufficient. Do NOT delete yet.

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
- **D7 — bearer in session config (trace-log risk).** The per-query bearer
  was to ride in a session-config value; ballista logs config keys at
  `trace`. Mitigation: keep cluster traffic internal, no `trace` in prod.
  Superseded in practice by D8 (the value doesn't propagate anyway).
- **D8 — ballista does not round-trip DataFusion `ConfigExtension` values.**
  `ConfigOptions::entries()` emits extension entries *unprefixed* (DataFusion:
  "The prefix is not used for extensions"), so ballista ships key `bearer`,
  and the peer's `ConfigOptions::set("bearer", ..)` can't route it back to the
  `sqe_auth` extension -> silently dropped. *Why it matters:* blocks the
  simplest per-query-secret passthrough. *Upstream:* ballista should prefix
  extension keys in `to_key_value_pairs` (or DataFusion's extension
  `entries()` should emit prefixed keys). *SQE workaround (designed, not yet
  built):* thread the secret through the plan-node bytes instead of session
  config. Verified empirically: client `bearer_len=630`, executor
  `bearer_len=0`.

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
