# SQE-on-ballista PoC spike plan

Date: 2026-05-28
Status: approved, ready to execute
Branch: `feat/sqe-ballista-poc-spike`

## Background

SQE today has a hand-written distributed execution layer:
`crates/sqe-coordinator/src/{distributed_scan,codec,worker_registry,
channel_pool,credential_refresh}.rs` plus `crates/sqe-worker/src/
{executor,shuffle,flight_service,heartbeat,credential_channel}.rs` — about
3.3K lines of code. It uses arrow-flight for transport, vends per-query OIDC
bearer tokens to worker tasks, and authenticates the coordinator → worker
hop with a shared `x-sqe-worker-secret` header.

CLAUDE.md calls this layer "Ballista (forked)" but it isn't — it's bespoke.
We're not getting upstream features (AQE, sort-shuffle improvements,
broadcast joins, REST API, TUI) that landed in apache/datafusion-ballista
53.0.0 on 2026-05-25.

The agreed long-term direction is: **SQE becomes a superset on top of
ballista**, with multiple frontend protocols (Trino HTTP, Flight SQL,
Quack/DuckDB wire) and multiple execution backends. Ballista is one such
backend, replacing the bespoke distributed layer.

The risk: we don't know if every SQE-specific feature can live on top of
ballista without an intrusive fork. Specifically: per-query OIDC bearer
passthrough, runtime filter pushdown across stages, and the coordinator-
authored policy-enforced LogicalPlan all assume things ballista's stock
model may not natively support.

Before committing to a multi-week migration, we spike.

## Goal

Submit `SELECT COUNT(*) FROM tpch_sf0_1.lineitem` (≈600,000 rows, SF0.1
TPC-H) end-to-end through a real `ballista-scheduler:53.0.0` and
`ballista-executor:53.0.0` pair, with SQE's iceberg, OIDC, and codec layers
wired in. Either the stack returns the correct count or we have a concrete
list of where it breaks.

## Non-goals

- Multiple executors / horizontal scaling.
- DynamicPredicate runtime filter pushdown across stages.
- Policy enforcement (row filters, column masks).  Test query has no policy
  bound to it.
- Trino-HTTP or Quack protocols on the new path.  Test invokes ballista
  programmatically via `BallistaContext`.
- `DistributedScanExec` retry logic.
- Performance comparison vs current SQE distributed layer.
- TUI / REST API integration.

## Build artifacts

- New workspace member `crates/sqe-ballista-poc/` containing:
  - `Cargo.toml` pulling `ballista = "53.0.0"`.
  - `src/main.rs`: a binary that connects to the existing test stack
    (Polaris + RustFS), submits the test query through ballista, and
    asserts the row count.
  - `src/codec.rs`: trivial `PhysicalExtensionCodec` impl reusing
    `sqe_coordinator::codec::SqePhysicalCodec` once we know what to
    serialize across the ballista boundary.  Initial PoC may not need a
    codec at all if the query plan doesn't include SQE-only nodes.
- New `docker-compose.ballista.yml` overlay adding
  `ballista-scheduler:53.0.0` and one `ballista-executor:53.0.0` service
  to the existing test+compare stack.  Existing `docker-compose.test.yml`
  Polaris and RustFS services are reused.
- A standalone integration test that runs the binary against the running
  docker stack.

## Verification points

Each is a pass/fail flag.  Stop and reconsider if any go red.

| # | Check | Pass criterion |
|---|---|---|
| 1 | Iceberg `TableProvider` reachable from ballista executor | The executor is configured (via `ExecutorProcessConfig::override_config_producer`) with an `IcebergCatalog` registered as a DataFusion catalog.  A simple `SHOW TABLES` or `SELECT * FROM tpch_sf0_1.region LIMIT 1` through `BallistaContext` returns the table schema and one row.  This is the prerequisite for any iceberg work on ballista. |
| 2 | Per-query OIDC bearer reaches the executor | The PoC binary obtains a Polaris token via OAuth client_credentials and passes it through to ballista somehow (mechanism TBD — that's what the spike resolves).  The executor then uses that bearer to read from RustFS.  Pass if the query in point 3 succeeds.  If ballista's `TaskDefinition` (or equivalent) can't natively carry per-task auth metadata, document the workaround used (sidecar mount, credential broker, custom `override_config_producer` closure, ballista PR shape) and note the trade-offs. |
| 3 | Result equals 600,000 | `SELECT COUNT(*) FROM tpch_sf0_1.lineitem` submitted via the PoC returns 600,000 — matches what a plain SQE-coordinator run returns against the same SF0.1 data. |

### Codec roundtrip — deferred check

`SqePhysicalCodec` exists today to serialize `DistributedScanExec`, which
in the target architecture goes away (replaced by ballista's task
distribution).  We may not need SQE-side custom physical plan nodes at
all in the new model — the iceberg `TableProvider` returns a plain
DataFusion `ParquetExec`, and ballista handles distribution.

For this PoC: do NOT block on codec roundtrip.  Note in the spike report
which SQE custom plan nodes (if any) still need wire-serialization in
the target architecture, and confirm that ballista's
`BallistaCodec<T, U>` generic accepts our codec.  Concrete codec
exercise can be a follow-up spike or fall out naturally during the full
migration.

## Exit gates

- **Green** — all three pass: move directly to writing the full SQE-on-
  ballista architecture design.  Findings folded in.
- **Yellow** — auth works only via a workaround we identify: write the
  design with the workaround captured.  Open an apache/datafusion-ballista
  issue or PR shape for the proper fix.  Continue to design.
- **Red** — codec or auth fundamentally broken (e.g. ballista's
  `TaskDefinition` is closed to custom metadata and the workaround
  requires patching ballista core in invasive ways): **stop**.  Reconsider
  the migration shape — backend-abstraction-first (option 1) or
  parallel-deploy (option 2) become more attractive than direct cutover.

## Outputs

- Working PoC binary committed under `crates/sqe-ballista-poc/`.
- `docker-compose.ballista.yml`.
- Spike report at
  `docs/superpowers/specs/2026-05-28-sqe-on-ballista-poc-report.md`
  with what worked, what didn't, what needs upstream PRs, and the
  gating recommendation for the full migration.

## Time budget

Three working days for code + report.  If we're not at a verification
verdict by day 3 EOD, the spike is producing more questions than answers
and we step back.

## Reference

- Ballista 53.0.0 release: <https://github.com/apache/datafusion-ballista/releases/tag/53.0.0>
- Ballista `SchedulerServer::new(..., codec: BallistaCodec<T, U>, ...)` —
  custom codec injection point confirmed via generics.
- Ballista `ExecutorProcessConfig::{override_config_producer,
  override_runtime_producer}` — session config and runtime env hooks.
- SQE current distributed surface: `distributed_scan.rs:24-200`,
  `codec.rs`, `worker_registry.rs`, `credential_refresh.rs`.
