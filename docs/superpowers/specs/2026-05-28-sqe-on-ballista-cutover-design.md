# SQE-on-ballista cutover design

Date: 2026-05-28 (migration contract added 2026-05-30)
Status: approved-in-principle (user: "let's do the cutover", "keep on building");
migration contract = Option 3, parity-gated retirement (see "Migration contract
& parity gate", 2026-05-30) — bespoke default, ballista opt-in, retire at
functional+speed parity, functional blockers first
PoC: `docs/superpowers/specs/2026-05-28-sqe-on-ballista-poc-report.md` (GREEN)
Branch: `feat/sqe-ballista-poc-spike` -> cutover work continues here / new `feat/sqe-ballista-cutover`

## Goal

Replace SQE's bespoke distributed execution layer (~11,560 LOC across 17
files) with Apache Ballista 53.0.0 as the maintained distributed runtime.

> **OUTCOME UPDATE (2026-05-29).** Benchmark testing reshaped the goal. Ballista
> gives **correctness parity** for the common path (TPC-H 22/22 SF0.1 + SF1) and
> its scheduling/orchestration is usable, **but SQE's bespoke execution is
> materially faster and more robust on complex queries** (SSB 2 errors + ~18x
> slower; TPC-DS hangs; see ledger D9). Decision (with the user): this is **not a
> wholesale replacement**. Ballista becomes an **optional engine behind the
> `[query] engine` flag** (the superset model); SQE's bespoke layer stays as the
> performant default and is **NOT deleted**. Phase 6 (deletion) is cancelled
> unless a future, optimized ballista path actually beats the bespoke layer on a
> fair multi-node release benchmark.

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
  **DONE (Phase 4b landed 2026-05-30): per-user bearer now threads through the
  plan, not ballista session config.** The narrative below is the original
  Phase 4 finding that motivated the plan-node design; the resolution is in
  ledger D8 and the parity-gate table (criterion #1, code-complete).
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
- **D9 — ballista distributed execution is materially slower than SQE's
  bespoke layer, and errors/hangs on complex queries.** Measured (SF0.1,
  same debug build + same single machine, ballista cluster = embedded
  scheduler + 2 executors):
  - TPC-H: 22/22 correctness parity, but ~2x slower (22.9s vs 10.8s).
  - SSB: **11/13 pass, 2 error** (q4.2, q4.3), ~18x slower (135.9s vs 7.7s).
  - TPC-DS: many queries hang past the 60s client timeout (legacy: all 99
    in 33s). Effectively does not complete.

  *Verdict (confirmed with the user):* **adopt ballista's scheduling /
  orchestration where it helps, but SQE's bespoke execution is "way better"
  — keep it.** This is the strongest "we did it better" divergence: do NOT
  delete the bespoke distributed layer. The cutover becomes a **superset**
  (ballista as an optional engine behind the flag; SQE execution remains the
  performant default), not a wholesale replacement.

  *Root cause of the "TPC-DS hang" — found and partly fixed (2026-05-29,
  commit `fix(ballista): bound executor memory + ...`):* it was not one bug
  but a crash stacked on a perf wall.
  - **The crash was OOM-SIGKILL, not a logic wedge.** Ballista executors ran
    with `memory_pool_size: None` (effectively unbounded), unlike the legacy
    worker. Co-located on one 36 GB box under sustained shuffle load they
    over-allocated and got OOM-killed — taking the coordinator down with
    them (the "transport error" / "crash at ~8 queries" we first read as a
    ballista perf wall). Fix: `ExecutorOptions.memory_pool_bytes` (default
    4 GiB). After the fix the coordinator + both executors **survive the
    full TPC-DS run** — no crash.
  - **Two contributing perf bugs, now fixed:** (a) the executor
    `SessionCatalog` and every per-user catalog were built with
    `table_cache = None`, so `load_table` re-hit Polaris on every scan task
    (a metadata-fetch storm on multi-stage plans) — now share a
    `TableMetadataCache`; (b) the scheduler session used
    `with_default_features` only, so SQE's Trino-compat UDFs were unknown at
    plan time — now registered in the scheduler `session_builder`.

  *The perf gap persists after all three fixes — but only the TPC-H number
  is a complete, measured result.* On the fully-fixed cluster (bounded mem +
  table cache + scheduler UDFs, SF0.1, embedded scheduler + 2 executors,
  debug build):
  - TPC-H: 22/22 parity, **24.3s vs legacy 10.8s (~2.2x slower)** —
    essentially unchanged from the pre-fix 22.9s, so the gap is *not* a
    memory or cache artifact. This is a clean full-run datapoint.
  - TPC-DS: **does not complete in reasonable time on the ballista path.**
    Run was killed at ~22.5 min after ~47 query *starts* (vs legacy's
    all-99-in-33s). **Cause not yet isolated:** the run was stopped before a
    pass/fail breakdown, so we cannot say whether queries are executing
    slowly or hanging/erroring — "starts" is not "completions." Executor
    logs showed **no task activity** (startup lines only), which fits a
    dispatch-but-never-complete wedge as well as it fits raw shuffle
    overhead. Live suspects, both unfixed — **(b) is the leading one given
    the signature:** (a) the executor has no SQE physical-optimizer rules
    and no UDF *execution* registry (`override_function_registry`) — an
    integration gap, but it would surface as *errors* ("function not found",
    cf. the 2 pre-fix SSB errors), not the silent hang we saw, so it does
    not fit TPC-DS; (b) the sync-codec-over-async-catalog blocking pattern
    (ledger **D4**): physical `try_decode` calls
    `block_in_place(|| Handle::block_on(resolve_catalog().load_table()))`
    **per scan task**. On a multi-stage plan that dispatches many scan tasks
    at once, if `concurrent_tasks` ≈ executor worker-thread count, every
    worker thread enters `block_in_place` + `block_on` simultaneously and no
    thread is left to drive the inner async Polaris round-trips → runtime
    starvation → tasks hang, no output, no error, client times out at 60s.
    This matches the observed signature exactly; TPC-H survives because its
    shallower plans decode fewer concurrent scan tasks per stage.
    *Cheap confirmation (one knob, not a full re-run):* set executor
    `concurrent_tasks = 1`; if the hung TPC-DS queries then complete
    (slowly), runtime starvation is confirmed. *Real fix:* make decode
    non-blocking — serialize enough scan state into the encoded plan bytes
    to rebuild the scan without a catalog round-trip (dovetails with the 4b
    plan-node threading), or an async decode hook upstream (D4).

  *Verdict (confirmed with the user):* the OOM crash was our integration
  bug and is fixed. Where ballista completes (TPC-H) it is ~2.2x slower than
  bespoke; complex suites (TPC-DS) need integration work we have not
  finished. Both justify keeping SQE's bespoke execution as the default —
  without claiming the TPC-DS gap is "inherent," which the data does not
  establish. *Wedge status (ledger D9 earlier text):* the original wedge was
  about degradation across *repeated* runs on one cluster; that was **not
  retested** after these fixes. A single fresh TPC-DS pass is consistent
  with the wedge still present (SSB grinding on the already-used cluster
  weakly supports that). Treat the wedge as untested post-fix, not resolved.
- **D4 OUTCOME (2026-05-29, built + tested, commit `feat(ballista): D4 ...`).**
  Built the non-blocking decode: `EncodedSqeScan` now carries the iceberg
  `TableMetadata` (serde JSON) + `metadata_location`, and the executor rebuilds
  the `Table` synchronously (`FileIOBuilder::build` + `Table::builder().build`
  are both sync) from static `[storage]` config — no catalog round-trip, no
  `block_on` in decode. **Result: the decode-starvation hang is gone.** TPC-DS
  q01-q13 run ~2s each (were ALL timing out at 60s pre-D4); TPC-H still 22/22,
  22.7s (no regression). So D4 was *necessary* and is a confirmed win, but it
  is **not sufficient** to complete TPC-DS — it peeled the top layer and
  exposed D10/D11 below. The decode fast path is single-tenant (static creds);
  per-user vended creds through the same plan bytes is Phase 4b (G3).
- **D10 — `count(*)` aggregate physical-plan serialization assertion.** Once D4
  let complex queries actually execute, multi-stage TPC-DS queries with
  `count(*)` over joins fail on the executor with a DataFusion internal
  assertion: `Input field name r_reason_sk does not match with the projection
  expression count(*)`. The mismatch is in an `AggregateExec`/`ProjectionExec`
  pair that crosses the stage boundary via **ballista's default physical
  codec** (datafusion-proto), *not* SQE's scan codec — the aggregate
  expression naming doesn't survive the proto round-trip. 48 occurrences in one
  TPC-DS sweep. *Why it matters:* blocks the analytical core of TPC-DS on the
  ballista path. *Upstream:* a datafusion-proto / ballista physical-plan
  serialization bug for aggregate output naming; reproduce minimally and file.
  Not caused by anything SQE diverged on.
- **D11 — ballista evicts the executor on a task `InvalidArgument` error.** When
  a task fails with the D10 assertion (a *query* error), ballista's scheduler
  reports it as "Failed to connect to executor" and **removes the executor**
  from its registry (`executor_manager`), rather than failing just that query.
  48 D10 errors caused **24 executor removals**; executors re-register on the
  next heartbeat, but the churn means complex queries cascade into timeouts and
  the cluster appears to "wedge." *This is the precise mechanism behind the D9
  "wedge"* — not `block_on` starvation (that was the *simple*-query hang, fixed
  by D4), but executor eviction on task errors. *Why it matters:* one bad query
  degrades the whole cluster. *Upstream:* ballista should distinguish a
  task-level `InvalidArgument` (fail the query) from an executor transport
  failure (evict the executor). Robustness blocker for shared clusters.
- **D8 — ballista does not round-trip DataFusion `ConfigExtension` values.**
  `ConfigOptions::entries()` emits extension entries *unprefixed* (DataFusion:
  "The prefix is not used for extensions"), so ballista ships key `bearer`,
  and the peer's `ConfigOptions::set("bearer", ..)` can't route it back to the
  `sqe_auth` extension -> silently dropped. *Why it matters:* blocks the
  simplest per-query-secret passthrough. *Upstream:* ballista should prefix
  extension keys in `to_key_value_pairs` (or DataFusion's extension
  `entries()` should emit prefixed keys). Verified empirically: client
  `bearer_len=630`, executor `bearer_len=0`.
  *SQE resolution (BUILT 2026-05-30, plan `2026-05-30-ballista-bearer-passthrough.md`):*
  thread the bearer through the plan instead of session config. The client
  `SqeLogicalCodec` stamps the bearer onto the encoded table-provider bytes
  (`"<bearer>\n<tableref>"`); the scheduler's `try_decode_table_provider`
  attaches it to the rehydrated `SqeTableProvider`; `scan()` bakes it onto
  `IcebergScanExec`; the physical codec writes it into `EncodedSqeScan.bearer`;
  the executor mints a per-(user,table) `FileIO` from it, cached with a
  per-key `OnceCell` single-flight so the D4 no-per-task-round-trip invariant
  holds (one bearer->vended-creds exchange per (user,table) per executor, not
  per task). Trust model preserved: only the bearer crosses the wire, never S3
  secrets. The `SqeAuthOptions` session-config insert is retained as a no-op in
  case a future ballista round-trips extension keys. Per-user isolation is NOT
  end-to-end verifiable on the single-principal dev stack (all users share one
  service token); unit-tested units are the wire round-trip, the cache keying
  (keyed on the full bearer, no hash-collision crossover), and the no-bearer
  static fallback.
- **D12 — cluster catalog namespace snapshot is stale for namespaces created
  after coordinator start.** Found during the 2026-05-30 bearer smoke. The
  embedded scheduler/executor build their single-tenant cluster catalog
  (`build_cluster_catalog`, service token) at coordinator startup. The codec's
  `try_decode_table_provider` resolves the table against that catalog via
  `self.catalog.schema(ns)`. A namespace created *after* startup (e.g.
  `benchmark-test.sh` starts the coordinator, then runs CTAS load) is not
  visible, so every ballista query fails fast with `sqe codec: namespace '<ns>'
  not found on executor catalog` (scheduler gRPC parse). *Evidence:* fresh
  coordinator started before load -> TPC-H 0/22 (all "namespace not found");
  same build, coordinator restarted with the data already present -> **22/22**.
  *Not a bearer-threading regression* (the bearer is stripped correctly; the
  namespace string in the error is exact). *Why it matters:* ballista mode
  needs the namespace to exist at cluster-catalog build time, or the catalog
  must resolve schemas dynamically/refresh. *Fix options:* (a) resolve
  `schema()` live against the catalog instead of a startup snapshot, or
  (b) rebuild/invalidate the cluster catalog on DDL. Pre-existing; orthogonal to
  parity #1. The single-node (legacy) path is unaffected (per-session catalog).
  *FIXED (2026-05-30, option a, surgical):* `SqeCatalogProvider` gains an opt-in
  `with_live_schema_resolution()` that makes `schema(name)` resolve the requested
  namespace directly instead of gating on the construction-time
  `cached_namespaces` snapshot; table existence is then decided live by the
  schema provider's `table()`. Set ONLY on the long-lived cluster catalog
  (`build_cluster_catalog`); the per-statement coordinator catalog keeps the
  snapshot guard (always fresh, preserves "schema not found" semantics) so its
  hot path is byte-for-byte unchanged. `schema_names()` enumeration still uses
  the snapshot (no async in the sync trait method). *Verified:* the exact
  failing scenario (coordinator started, THEN TPC-H SF0.1 loaded, ballista mode)
  now passes **22/22** (was 0/22); plus a unit regression test in
  `catalog_provider.rs`.
- **D13 — cluster catalog bootstrap hardcodes OAuth `ClientCredentials`, so
  non-OAuth backends (Glue/S3Tables/Hadoop) can't use the ballista path.**
  Found during the 2026-05-31 parity #3 assessment. `build_cluster_catalog`
  (`sqe-ballista/cluster.rs`, run at scheduler + executor bootstrap)
  unconditionally builds `CatalogAuthConfig::ClientCredentials` from `[auth]`
  and calls `resolve_bearer`, minting an OAuth token regardless of the catalog
  backend. The coordinator's per-session path instead resolves auth per-backend
  (`Aws` -> empty bearer for Glue/S3Tables; the AWS SDK chain supplies creds).
  For an AWS-IAM deployment (Glue/S3Tables, `[auth].token_endpoint` empty), the
  forced OAuth mint hits `OAuthClient::new("", ..).get_token()` and fails, so
  the cluster catalog never builds and the whole ballista path is unavailable.
  *Not verified at runtime* (no Glue/S3Tables creds on this stack); confirmed by
  code reading. *Why it matters:* blocks non-OAuth backends on ballista even in
  the `full-backends` image. *FIXED (2026-05-31):* `build_cluster_catalog` now
  resolves the service identity per-backend: an explicit `[catalogs.*.auth]`
  override wins; otherwise REST keeps OAuth `ClientCredentials` from `[auth]`
  (unchanged, tested path), Glue/S3Tables use `Aws` (SDK provider chain, no
  bearer), HMS/JDBC/Hadoop use `Anonymous`. The forced OAuth mint is gone, so
  non-OAuth backends no longer fail cluster-catalog bootstrap. Verified the REST
  branch is unchanged (Polaris `sha256` still returns through ballista); the
  non-REST branches are code-only (no Glue/S3Tables creds here). Orthogonal to
  the executor's D4 rebuild, which is backend-agnostic (metadata + S3 FileIO).

## End goal + plumbing-reduction gates

**North Star (user, 2026-05-29):** reduce SQE's bespoke plumbing by leaning
on ballista where possible — **without losing functionality or speed.** Both
halves are hard constraints, and they shape what the cutover is allowed to do.

### Migration contract & parity gate (user, 2026-05-30)

The user reframed the relationship precisely: **SQE is the lakehouse SQL
server** (like the Polaris / Iceberg-REST / Glue / S3Tables stack it fronts),
and its identity is four pillars it owns — **protocols** (Flight SQL, Trino
HTTP), **targets** (Polaris, Iceberg REST, Glue/S3Tables, Unity, Nessie,
Hadoop), **speed** (our scan / IO / spill tuning), and **policy SQL**
(GRANT/REVOKE, column masks, row filters). **Ballista's job is narrowed to one
thing: the distributed scheduler / task-management brain** — chosen because its
scheduling is more mature than the bespoke `WeightedScheduler` + `stage_planner`.

**The contract (Option 3, parity-gated retirement):**

- **Now:** bespoke is the **default**; ballista is **opt-in** behind
  `[query] engine = "ballista"`. Nothing in the bespoke layer is touched.
- **Retire trigger:** when ballista reaches **functional parity** *and* **speed
  parity** (the gate below), the bespoke distributed layer retires and ballista
  becomes the single path.
- **Ordering (user directive):** **correctness before performance.** Close the
  functional blockers first; speed parity is the *last* gate before retirement.

**The parity gate (retire trigger) — ordered, honest status:**

| # | Criterion | Status (verified vs. assumed) | Maps to |
|---|---|---|---|
| 1 | **Bearer passthrough** — per-user OIDC bearer reaches executors; the query authenticates *as the user* to Polaris/S3 (no service account) | **CODE-COMPLETE + E2E-SMOKED (2026-05-30).** Bearer threaded through the plan (logical codec -> provider -> `IcebergScanExec` -> `EncodedSqeScan` -> executor per-(user,table) `FileIO`, D4-safe cache); see D8. Unit-tested: wire round-trip, full-bearer cache keying, no-bearer fallback. **Ballista-mode smoke: TPC-H SF0.1 22/22** (single-principal stack; exercises the full bearer path with a real token). Per-user *isolation* still NOT E2E-verifiable on the single-principal stack (needs a multi-principal env). NB: the smoke first hit 0/22 from a pre-existing cluster-catalog freshness bug (D12, now FIXED), not from bearer threading; ballista-mode TPC-H is 22/22 in the start-then-load order too. | G3, task 4b |
| 2 | **Policy plans survive the codec** — injected column-mask / row-filter nodes round-trip through `SqeLogicalCodec` and enforce on executors | **GAP FOUND + FIXED + VERIFIED E2E (2026-05-31).** Assessed: row filters (standard `Filter` exprs) and column restriction (projection) survive via ballista's default codec; **column masks did NOT** — the mask UDF (`sha256`) was registered on neither the scheduler (planning) nor the executor (run). Same gap hit JSON funcs + Trino-function *execution*. Fix (commit 2a06257): shared `register_sqe_session_udfs` (sha256 + Trino + extended-Trino + JSON) wired into coordinator + scheduler `session_builder` + executor `override_function_registry`. **E2E PROOF:** distributed stack (coordinator scheduler + rebuilt `sqe-worker` executor), ADBC FlightSQL client, ballista mode: `SELECT l_orderkey, sha256(l_comment) FROM tpch_sf0_1.lineitem LIMIT 3` returned real SHA-256 hashes, and `count(*)` returned 600000 (no regression). Plus a unit test (helper registers `sha256`, flows into `BallistaFunctionRegistry`). NB: reaching the E2E surfaced + fixed a **separate** coordinator bug (commit 3c280a5): the Flight Basic-auth handshake used a padding-strict base64 decoder and rejected the Go ADBC driver's unpadded base64 ("Invalid base64 in auth"), so **no ADBC client (incl. the dbt-sqe adapter) could connect at all**; now `DecodePaddingMode::Indifferent`. Row-filter *enforcement* E2E (with a live policy backend) still not stood up. | G3 |
| 3 | **All catalog backends decode** — Glue/S3Tables/Unity/Nessie/Hadoop rebuild executor-side, not just REST/Polaris | **ASSESSED (2026-05-31): REST-family verified, non-REST code-only with a confirmed gap. NOT a blanket green.** The literal "rebuild executor-side" is **backend-agnostic by D4 design**: the executor rebuilds the `Table` from shipped `metadata_json` + an S3 `build_file_io`, never contacting the catalog — so any S3-backed Iceberg table decodes identically regardless of catalog. **REST-family (Polaris/Nessie/Unity)** share the REST path and are covered by the Polaris E2E (criterion #2 ran real queries through it). **Non-REST (Glue/S3Tables/HMS/JDBC):** code-only, NOT runtime-verified (no creds/stacks here), and **D13 (the cluster-bootstrap OAuth-hardcode blocker) is now FIXED** — `build_cluster_catalog` resolves the service identity per-backend (REST→OAuth unchanged, Glue/S3Tables→`Aws`, HMS/JDBC/Hadoop→`Anonymous`), so AWS-IAM backends can initialize the cluster catalog. Still requires the `full-backends` build (Dockerfile.full compiles both `sqe-server` + `sqe-worker` with it), and the non-REST paths remain runtime-unverified (no creds/stacks here — only the REST branch is exercised). **Out of scope:** Hadoop (catalog stubbed, `mount.rs` returns "not yet") and non-S3 storage (`build_file_io` is S3-only) — engine-wide limits, not ballista-specific. | G3 |
| 4 | **Protocols route** — both Flight SQL and Trino HTTP drive the ballista path | **GAP FOUND + FIXED + VERIFIED E2E (2026-05-31).** Assessed: the `use_ballista` switch lived ONLY in `open_stream` (the Flight SQL streaming path). **Trino HTTP** (and any `execute`-based protocol, e.g. Quack) reaches the engine via `execute -> execute_query`, which had **no** ballista branch — so Trino queries ran on the legacy bespoke path even in ballista mode. Fix (commit 2cec096): early-return in `execute_query` to a new isolated `execute_query_ballista` helper (`submit_remote` + the same row/memory caps); the legacy path is untouched. **E2E PROOF:** Trino HTTP `SELECT sha256(l_comment) ...` returned the same hashes as the Flight/ADBC path; discriminating test = killing the executor makes the Trino sha256 *scan* stall ("no alive executors") while `count(*)` (metadata-answerable) still finishes, proving the scan routes through ballista. Flight SQL was already routed (criterion #2 E2E). | G3 |
| 5 | **Speed parity** — measured **multi-node at SF1+** (task 5b), ballista within an agreed band of bespoke (e.g. ≤1.2x). *Not* measurable on the 36GB co-located dev box: co-located executors share one machine's cores, so ballista can only win at multi-node SF1+. | DEFERRED to real hardware; band = G2's ≤1.2x. | G2 |

Criteria 1-4 are the functional expansion of gate **G3** (plus **G1** robustness:
a query that hangs fails both functionality and speed). Criterion 5 is gate
**G2**. Retirement is post-**G4** soak. The G1-G4 table below is the engineering
backlog; this checklist is the *contract* the gates serve.

**What never moves (engine-agnostic, above the execution seam).** The product
surface is coordinator-side and independent of the execution engine. It is
*never* a candidate for "reduce plumbing via ballista":
- Multi-protocol frontends: Flight SQL, Trino HTTP (`sqe-trino-compat`),
  Quack/DuckDB (`sqe-quack-server`, HTTP `POST /quack`).
- Security: OIDC auth (`sqe-auth`) + policy rewrite (OPA/Cedar, column masks,
  row filters) applied to the LogicalPlan *before* the engine branch.
- Pluggable catalog backends (`CatalogKind`: IcebergRest, Glue, S3Tables, HMS,
  SQLite; JDBC/Hadoop stubbed) via `sqe-catalog/mount.rs`.

The engine (bespoke or ballista) only ever receives an already-secured,
already-planned fragment and reloads tables by identifier. It sees no protocol
and makes no auth/policy decision.

**The plumbing that *could* be reduced** is only the distributed-execution
layer (~11.5K LOC): `distributed_scan`, `shuffle`, `stage_planner`,
`distributed_join|sort|aggregate`, `worker_registry`, `heartbeat`,
`channel_pool`, `credential_refresh`.

**Why it can't be reduced today.** Ballista's scheduler and executor are one
runtime — the scheduler drives executors over ballista's task-gRPC + shuffle
protocol, bridged by the codec. There is no seam to borrow ballista's
orchestration while keeping bespoke execution; adopting the cheap half
requires the expensive half. And measured today (D9), the expensive half
loses **both** constraints at once: speed (TPC-H ~2.2x slower, TPC-DS hangs)
and functionality (per-user security does not reach distributed executors,
D8). So "use ballista where possible" on the distributed hot path = nowhere,
today, without breaking the North Star.

**The reduction is therefore a gated destination, not a switch.** Each gate is
an upstream contribution; the divergence ledger doubles as the backlog. The
~11.5K bespoke LOC are deleted only after all gates pass on a fair benchmark:

| Gate | Unlocks | Blocking work |
|---|---|---|
| **G1 robustness** | ballista completes complex queries (no hang) | **D4** non-blocking decode — DONE (simple-query hang fixed). Now blocked on **D10** (`count(*)` aggregate serialization assertion) + **D11** (executor eviction on task error). |
| **G2 speed** | ballista within an agreed band of bespoke (e.g. ≤1.2x) on a RELEASE build across SEPARATE worker machines | shuffle/serialization + per-task overhead; fair bench (Phase 5b) |
| **G3 functionality** | per-user identity (bearer + STS) reaches distributed executors, honoring the no-service-account model | **4b** plan-node bearer threading + **D3** per-task credential hook |
| **G4 soak** | ballista default for a soak period, no regressions | flip default; observe |

Only after G1-G4 do the bespoke distributed files get deleted — that is when
the plumbing reduction is banked *without* losing speed or functionality.
Until then: bespoke is the default engine, ballista is optional behind the
`[query] engine` flag (cheap insurance, already built), nothing is deleted.
Doing the contribution work (D4, D8, D1/D2, D3) is precisely how each gate is
earned; it is the path to the end goal, not a detour from it.

## Rollback

The `engine = "legacy"` config switch keeps the bespoke path runnable
through Phase 5. If ballista mode fails parity or perf gates, flip back to
legacy with zero code change. Phase 6 (deletion) only happens after the
gates above (G1-G4) pass and ballista mode has been default for a soak period.

## Success criteria

1. TPC-H/DS/SSB correctness parity 100% in ballista mode (SF0.1 + SF1).
   **Status:** TPC-H 22/22 (SF0.1+SF1); TPC-DS/SSB blocked on G1 (D4 hang).
2. Perf within agreed band of bespoke on a fair release/multi-node bench
   (gate G2). **Status:** not met — TPC-H ~2.2x slower on the shared-box
   debug cluster; fair bench is Phase 5b.
3. The bespoke distributed files deleted; net LOC down ~10K. **Gated on
   G1-G4** (see "End goal + plumbing-reduction gates"); not done, not before
   the gates pass. This is the *payoff* of the end goal, deferred until
   earned.
4. Divergence ledger complete; upstream PRs filed (D8 + D1/D2 are the
   smallest; D4 is the keystone). On request.
