# OpenLineage Emitter Design

**Status:** draft, awaiting review
**Date:** 2026-05-08
**Owner:** Jacob Verhoeks
**Tracks deferred item:** `docs/superpowers/plans/2026-04-24-iceberg-matrix-parity.md` row 16 ("OpenLineage emitter, +2, deferred; low user demand")

## 1. Overview

Add a coordinator-side OpenLineage (OL) emitter to SQE. Emits OL `RunEvent` per query: START at submit time, COMPLETE on success, FAIL on error. Carries dataset-level lineage (inputs and outputs) plus column-level lineage on output datasets. Supports two transports running side by side: a JSONL file sink and an HTTP POST sink for an OL collector (Marquez, DataHub, or any OL-2-0-2 receiver). HTTP transport falls back to a bounded disk spool when the collector is unavailable, replays on recovery.

Off by default. Zero overhead in the query path when disabled.

## 2. Goals and non-goals

**Goals**

- OL spec compliance: events conform to `https://openlineage.io/spec/2-0-2/OpenLineage.json`. Per-file facet versions pinned at implementation time.
- Multi-catalog awareness: dataset namespace derived from each TableScan's catalog REST URL. A query joining `polaris.a.x` and `nessie.b.y` produces two input datasets with the right namespaces.
- Full column-level lineage on writes: every output column maps to its input columns with transformation kind (DIRECT/IDENTITY, DIRECT/TRANSFORMATION, DIRECT/AGGREGATION, INDIRECT/FILTER, INDIRECT/JOIN, etc.).
- Operational resilience: collector outage does not block queries, does not lose events up to a configured cap, and recovers automatically.
- Pluggable transports: file and HTTP coexist. Adding a Kafka sink later is a one-file change behind the existing `Sink` trait.
- Zero net-new external dependencies.

**Non-goals (v1)**

- Worker-side emit. Coordinator-only. Workers do not see SQL or session identity in this design.
- Custom OL extension namespaces beyond what SQE needs to express column masks (`MASKED` subtype). No new producer-defined facet types.
- Heartbeats / partial-progress events on long-running queries. Two events per query (START plus COMPLETE/FAIL) only.
- mTLS to the OL collector. Bearer auth only.
- Lineage on maintenance procedures (OPTIMIZE, VACUUM, REWRITE_MANIFESTS). Always skipped.
- Bench/load validation of <2ms p99 overhead. Tracked separately under future work.
- Correlated subqueries deeper than one level get reduced fidelity (INDIRECT/CONDITIONAL without column granularity). Documented as a v1 limitation.

## 3. Architecture

### 3.1 New crate `sqe-lineage`

```
crates/sqe-lineage/
├── Cargo.toml
└── src/
    ├── lib.rs                # re-exports + LineageObserver trait
    ├── event.rs              # OL RunEvent, Job, Run, Dataset, facets
    ├── extract/
    │   ├── mod.rs            # entry: plan -> (inputs, output, col_lineage)
    │   ├── datasets.rs       # walk for TableScan + write target
    │   └── columns.rs        # walk Projection chain -> columnLineage facet
    ├── observer.rs           # ChannelObserver: bounded mpsc producer
    ├── emitter.rs            # background task, drains channel, fans out
    ├── sink.rs               # Sink trait + MultiSink combinator
    └── sinks/
        ├── file.rs           # JSONL appender
        ├── http.rs           # POST to OL collector
        └── spool.rs          # disk-buffered fallback wrapping Http
```

### 3.2 Observer interface

```rust
#[async_trait]
pub trait LineageObserver: Send + Sync {
    fn on_query_start(&self, ctx: QueryStartCtx);
    fn on_query_complete(&self, ctx: QueryCompleteCtx);
    fn on_query_fail(&self, ctx: QueryFailCtx);
}
```

`QueryStartCtx`, `QueryCompleteCtx`, `QueryFailCtx` carry: `run_id`, `job_namespace`, `sql`, `user`, `session_id`, `started_at`, `statement_kind`, `plan: Option<PlanOrHint>`, `duration` (complete/fail), `rows_returned` (complete), `error_message` (fail). `PlanOrHint` is an enum: either a real `LogicalPlan` for queries that produced one, or a `LineageHint::DdlSchema { catalog, schema, table, columns }` for DDL paths that bypass the planner.

Hooks are synchronous and non-blocking. They push to a bounded `tokio::sync::mpsc::Sender<LineageMsg>`. A full channel drops the newest message and increments `sqe_lineage_dropped_events_total{reason="channel_full"}`.

### 3.3 Production wiring vs test injection

Production: `Some(Arc::new(ChannelObserver::new(emitter_handle)))` constructed in `bin/sqe_server.rs` after config validation. `emitter` task spawns at the same time, owning the receiver and the `MultiSink`.

Tests: a `MockObserver` in `sqe-lineage::testing` records calls into a `Vec<RecordedEvent>` for assertion. Coordinator integration tests use the real observer pointed at a `wiremock` HTTP collector.

### 3.4 Data flow

```
query_handler::execute_statement
  -> Observer::on_query_start (push to mpsc, non-blocking)
  -> [statement runs]
  -> Observer::on_query_complete | on_query_fail (push to mpsc)

emitter task (separate tokio task):
  recv() from mpsc
  -> extract::datasets + extract::columns (only for *_complete and *_fail with a real plan)
  -> build OL RunEvent
  -> MultiSink::send (parallel join_all over all configured sinks)
```

## 4. OL event schema

### 4.1 Top-level

```rust
struct RunEvent {
    eventType:   EventType,        // START | COMPLETE | FAIL
    eventTime:   String,           // RFC3339, UTC
    producer:    String,           // "https://github.com/sbp/sqe/v<crate_version>"
    schemaURL:   String,           // "https://openlineage.io/spec/2-0-2/OpenLineage.json"
    run:         Run,
    job:         Job,
    inputs:      Vec<InputDataset>,
    outputs:     Vec<OutputDataset>,
}
```

START events carry empty `inputs` / `outputs` (the plan is not yet captured). COMPLETE and FAIL carry the full lineage.

### 4.2 Run, Job, Dataset

```rust
struct Run {
    runId: Uuid,
    facets: RunFacets,
}

struct RunFacets {
    nominalTime:   NominalTimeFacet,                   // start time
    parent:        Option<ParentRunFacet>,             // parent.run.runId = session.id
    errorMessage:  Option<ErrorMessageFacet>,          // FAIL only
}

struct Job {
    namespace: String,                                  // cfg.job_namespace, default "sqe"
    name:      String,                                  // "<statement_kind>:<query_hash>"
    facets:    JobFacets,
}

struct JobFacets {
    sql:           Option<SqlFacet>,                   // dialect = "sqe", query = redacted SQL
}

struct InputDataset {
    namespace: String,  // catalog REST URL, fallback "sqe://<catalog_name>"
    name:      String,  // "<schema>.<table>"
    facets:    DatasetFacets,
}

struct DatasetFacets {
    schema:     Option<SchemaFacet>,                   // columns + types
    dataSource: Option<DataSourceFacet>,               // catalog name + URI
}

struct OutputDataset {
    namespace: String,
    name:      String,
    facets:    DatasetFacets,
    outputFacets: OutputDatasetFacets,
}

struct OutputDatasetFacets {
    columnLineage: Option<ColumnLineageFacet>,         // produced by extract::columns
}
```

Per-file facet versions pinned at implementation time by reading the current value from the OL spec repo and recording it in `event.rs` as a `pub const SCHEMA_URL_*` constant.

### 4.3 SQL facet redaction

`JobFacets.sql.query` is the SQL text with PII redaction applied via the existing `sqe_metrics::audit::redact_pii` helper. Email, SSN, phone, card patterns get masked. The SHA-256 `query_hash` already used by the audit log is the second component of `job.name`, so even if the redacted SQL still leaked PII, the job identity is hash-stable and PII-free.

### 4.4 Dataset naming

A `TableScan` arrives qualified as `<catalog>.<schema>.<table>`:

```
namespace = flattened_catalogs()[catalog].catalog_url
            (fallback: "sqe://<catalog_name>" when URL is empty / embedded mode)
name      = "<schema>.<table>"
```

The catalog REST URL is the primary identifier so renames and S3 path changes don't break lineage continuity in the OL UI.

### 4.5 Run / job naming

| Field | Value |
|---|---|
| `run.runId` | the existing `query_id: uuid::Uuid` from `query_handler` |
| `run.facets.parent.run.runId` | `session.id` UUID, links queries within an OIDC session |
| `run.facets.nominalTime.nominalStartTime` | `started_at` RFC3339 |
| `job.namespace` | `cfg.job_namespace` (default `"sqe"`, configurable per env) |
| `job.name` | `<statement_kind>:<query_hash>` |
| `producer` | `https://github.com/sbp/sqe/v<CARGO_PKG_VERSION>` |

`statement_kind` reuses `StatementKind::name()`. `query_hash` reuses `sqe_metrics::audit::query_hash()`.

### 4.6 Emit-decision matrix

| Statement | Emits? | Inputs | Outputs |
|---|---|---|---|
| SELECT | only if `cfg.emit_selects = true` | yes | none |
| INSERT, CTAS, MERGE | always | yes (sources) | yes (target) |
| UPDATE, DELETE | always | yes (target) | yes (target) |
| CREATE TABLE (no AS) | always | none | yes (with `schema` facet) |
| ALTER TABLE, DROP | always | none | yes (with current `schema` facet) |
| OPTIMIZE, VACUUM, REWRITE_MANIFESTS, others | never | n/a | n/a |

Skip checks short-circuit before the channel send. Skipped statements never consume queue capacity.

## 5. Column-lineage extractor

Lives in `sqe-lineage/src/extract/columns.rs`.

### 5.1 Algorithm

Bottom-up walk of the `LogicalPlan`. At each node compute a `ColumnTrace`: `Vec<Vec<ColumnDep>>` indexed by the node's output column ordinal. `ColumnDep` is a leaf reference `{ catalog, schema, table, field, transformation: TransformationType }`. The trace at the root of a write plan is exactly `columnLineage.fields[*].inputFields`.

### 5.2 Per-node rules

| LogicalPlan node | Rule |
|---|---|
| `TableScan` | each scan column emits a single `ColumnDep { ..., transformation: Direct(Identity) }`; terminal |
| `Projection` | for each output expr, extract `Expr::column_refs()`, map each ref through child trace, classify by expr shape: column ref alone -> `Direct(Identity)`; otherwise `Direct(Transformation)` |
| `Filter` | passthrough on output columns. predicate's `column_refs()` add `Indirect(Filter)` deps to every downstream output column |
| `Aggregate` | group-by exprs map with `Direct(Identity)` or `Direct(Transformation)`. agg-fn args map with `Direct(Aggregation)`. group-by columns also add `Indirect(GroupBy)` deps to all aggregated output columns |
| `Join` | each side passes through its own column lineage. join `on` predicate columns add `Indirect(Join)` deps to all output columns |
| `Union` | merge child traces by positional column: output col i depends on col i of each branch |
| `Sort` | passthrough. sort keys add `Indirect(Sort)` deps |
| `Window` | passthrough on inputs. window-fn args -> `Direct(Window)`. partition-by / order-by -> `Indirect(Window)` |
| `SubqueryAlias`, `Limit`, `Distinct` | passthrough |
| correlated `Subquery` | walk inner plan; correlated outer-column refs become `Indirect(Conditional)` deps on the enclosing scope's outputs. v1 traces one level only; deeper correlation falls back to `Indirect(Conditional)` without column granularity, with a warn log |
| `Extension` (SQE policy enforcer, incremental TVF) | passthrough on outputs. expressions become `Indirect(Conditional)`. Policy column-mask rewrites annotate output deps with the `Masked` subtype on the `transformations` array |

### 5.3 Output mapping for write statements

For INSERT, CTAS, MERGE the *output* dataset is the write target. Column-name alignment uses target schema ordinals: DataFusion's planner already resolves the source `Projection` columns to match target schema by position. We map `target_schema.field(i) -> trace[source_root][i]`.

MERGE: `WHEN MATCHED UPDATE SET col = expr` and `WHEN NOT MATCHED INSERT` are separate sub-plans. v1 emits one OL output dataset whose column lineage covers both branches; transformations carry custom subtypes `MergeInsert` and `MergeUpdate` so consumers can distinguish.

### 5.4 v1 limitations (documented, not bugs)

- Correlated subqueries deeper than one level: `Indirect(Conditional)` only, no column granularity. Logged as warn.
- Window functions with `LAG` / `LEAD`: column dependency tracked. Offset constants don't appear (correct per OL spec).
- `INSERT INTO t SELECT *`: DataFusion expands the star before plan construction; safe.
- `Distinct` treated as passthrough. Strictly speaking distinct-elimination changes row identity, but no OL transformation type expresses "row deduplication", so passthrough is the closest fit.

### 5.5 Implementation primitives

DataFusion exposes `Expr::column_refs() -> HashSet<&Column>` and `LogicalPlan::expressions() -> Vec<Expr>`. Combined with `TreeNode::transform_up` for the recursion, the extractor is approximately 500 LOC plus tests.

## 6. Sinks

### 6.1 Trait

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    async fn send(&self, event: &RunEvent) -> Result<(), SinkError>;
    fn name(&self) -> &'static str;
}
```

### 6.2 MultiSink fan-out

The emitter task owns `Vec<Arc<dyn Sink>>`. For each event it calls each sink in parallel via `futures::future::join_all`. Sink failures are isolated. Each failure increments `sqe_lineage_sink_errors_total{sink="..."}` (Prometheus).

### 6.3 File sink

JSONL appender mirroring `sqe_metrics::audit::AuditLogger`. Single `Mutex<BufWriter<File>>`, flush after each event. Path from `cfg.file_path`. Empty path means sink not constructed. Same poison-recovery pattern as `AuditLogger`.

### 6.4 HTTP sink

`reqwest::Client` with rustls-tls, gzip, http/2. POST `application/json` to `cfg.http_endpoint`. Default timeout 5s, 1 retry on 5xx / timeout (250ms then 500ms backoff). Auth header per `auth_mode`:

| `auth_mode` | Header |
|---|---|
| `none` | (none) |
| `bearer` | `Authorization: Bearer <cfg.api_key>` |
| `user_token` | `Authorization: Bearer <session.user.bearer>` |

When `auth_mode = "user_token"` but the session has no bearer (e.g. API-key auth), fall through to `none` and emit a warn metric (`sqe_lineage_user_token_missing_total`).

### 6.5 Spool sink

Wraps an HTTP sink. `Spool::send(event)` calls `inner.send` first. On error, appends event JSON to `<spool_path>/spool.jsonl`. A background `replay_loop` task wakes every `replay_interval_secs` (default 30) and tries to drain rotated spool segments. The drain reads `spool.jsonl.<rotation_id>` files in order, never the live `spool.jsonl` (avoids a writer/reader race; current writer always appends to the live file).

Cap policy. Hard cap from `cfg.spool_max_bytes` (default 100 MiB). When the live file plus rotated segments exceed the cap, drop newest: log a warn, increment `sqe_lineage_spool_drops_total`, do not write the new event. Older queued events are preserved because OL run reconstruction is more forgiving when COMPLETE arrives without START than the inverse.

### 6.6 Construction

```rust
let mut sinks: Vec<Arc<dyn Sink>> = vec![];
if !cfg.file_path.is_empty()     { sinks.push(Arc::new(FileSink::new(...)?)); }
if !cfg.http_endpoint.is_empty() {
    let http = HttpSink::new(...)?;
    sinks.push(if cfg.spool_path.is_empty() {
        Arc::new(http)
    } else {
        Arc::new(SpoolSink::wrap(http, ...))
    });
}
```

Validation in `bin/sqe_server.rs` ensures at least one sink is configured when `enabled = true`.

## 7. Lifecycle hooks in `query_handler`

### 7.1 Hook placement

```rust
pub async fn execute_statement(...) {
    let query_id = uuid::Uuid::new_v4();
    let started_at = Utc::now();

    if let Some(obs) = &self.lineage {
        if should_emit(&kind, &cfg) {
            obs.on_query_start(QueryStartCtx { run_id: query_id, ..., });
        }
    }

    let mut captured_plan: Option<PlanOrHint> = None;
    let result = /* dispatch: execute_query / handle_grant / handle_drop / ... */;

    if let Some(obs) = &self.lineage {
        if should_emit(&kind, &cfg) {
            match &result {
                Ok(_)  => obs.on_query_complete(QueryCompleteCtx { ..., plan: captured_plan }),
                Err(e) => obs.on_query_fail(QueryFailCtx { ..., plan: captured_plan, error: e.to_string() }),
            }
        }
    }
}
```

### 7.2 Plan capture

`execute_query` already builds the logical plan around line 1168 (`let plan = df.logical_plan().clone();`). It captures *after* policy enforcement runs so the lineage reflects what executed (column masks visible, row filters annotated). The capture happens via a `&mut Option<PlanOrHint>` out-parameter on the function signature.

DDL paths populate a synthetic hint instead of a real plan: `LineageHint::DdlSchema { catalog, schema, table, columns }`. This avoids forging a `LogicalPlan` for statements that don't have one.

### 7.3 Skip cases

`should_emit(kind, cfg)`:

1. `kind == StatementKind::Query` and `cfg.emit_selects == false` -> skip.
2. `kind` in `MAINTENANCE_KINDS` (Optimize, Vacuum, RewriteManifests, ...) -> skip.
3. otherwise emit.

Skips short-circuit before any channel send.

## 8. Configuration

### 8.1 TOML

```toml
[metrics.openlineage]
enabled        = false                # master switch
job_namespace  = "sqe"                # per-env: "sqe-prod", "sqe-dev"
producer       = ""                   # default: "https://github.com/sbp/sqe/v<crate_version>"

emit_selects   = false                # opt-in: SELECT emits an OL run

# sinks (at least one required when enabled = true)
file_path      = ""                   # JSONL sink, empty = disabled
http_endpoint  = ""                   # OL collector URL, empty = disabled

# HTTP transport
auth_mode               = "none"      # "none" | "bearer" | "user_token"
api_key                 = ""          # used when auth_mode = "bearer"
http_timeout_ms         = 5000
http_retry_attempts     = 1

# Spool (only when http_endpoint is set)
spool_path           = ""             # empty = no spool, fail-loud on HTTP error
spool_max_bytes      = 104857600      # 100 MiB
replay_interval_secs = 30

# back-pressure
channel_capacity = 10000
```

### 8.2 Env overrides

Every scalar field gets an override in `apply_env_overrides`:

```
SQE_METRICS__OPENLINEAGE__ENABLED
SQE_METRICS__OPENLINEAGE__JOB_NAMESPACE
SQE_METRICS__OPENLINEAGE__EMIT_SELECTS
SQE_METRICS__OPENLINEAGE__FILE_PATH
SQE_METRICS__OPENLINEAGE__HTTP_ENDPOINT
SQE_METRICS__OPENLINEAGE__AUTH_MODE
SQE_METRICS__OPENLINEAGE__API_KEY
SQE_METRICS__OPENLINEAGE__HTTP_TIMEOUT_MS
SQE_METRICS__OPENLINEAGE__HTTP_RETRY_ATTEMPTS
SQE_METRICS__OPENLINEAGE__SPOOL_PATH
SQE_METRICS__OPENLINEAGE__SPOOL_MAX_BYTES
SQE_METRICS__OPENLINEAGE__REPLAY_INTERVAL_SECS
SQE_METRICS__OPENLINEAGE__CHANNEL_CAPACITY
```

### 8.3 Validation

`OpenLineageConfig::validate()` runs in `bin/sqe_server.rs` between config load and `QueryHandler::new`. Failures are fatal.

1. `enabled = true` and both `file_path` empty and `http_endpoint` empty: error.
2. `auth_mode = "bearer"` and `api_key` empty: error.
3. `auth_mode = "user_token"` while only API-key auth is configured: warn.
4. `spool_path` non-empty and `http_endpoint` empty: error.
5. `spool_max_bytes < 1 MiB`: error.

### 8.4 Default posture

`enabled = false` -> `QueryHandler::lineage` is `None` -> the `if let Some(obs)` guards short-circuit. Zero lineage code runs in the hot path. Operators opt in deliberately.

### 8.5 Embedded mode

`crates/sqe-cli/src/embedded.rs::EmbeddedClient` accepts a `Option<Arc<dyn LineageObserver>>` parameter constructed from the same TOML. CLI users can emit OL events from one-shot queries without running the coordinator.

## 9. Testing strategy

### 9.1 Unit tests (in `sqe-lineage`)

| File | Coverage |
|---|---|
| `extract_columns_test.rs` | one test per node rule from §5.2; ~25 tests with hand-built `LogicalPlan`s |
| `event_serialise_test.rs` | round-trip every `RunEvent` flavour (START/COMPLETE/FAIL × {SELECT, INSERT, CTAS, MERGE, DDL}) against committed JSON fixtures |
| `sinks_file_test.rs` | append, restart, append again, ordering preserved, poison recovery |
| `sinks_http_test.rs` | wiremock collector stub: auth header per `auth_mode`, timeout, retry on 503, success on 200 |
| `sinks_spool_test.rs` | force HTTP failure -> spool grows; HTTP recovers + replay loop drains spool; cap drops newest with metric increment |
| `observer_test.rs` | full channel triggers drop-newest with metric increment; closed channel logs warn without panic |

### 9.2 Integration tests (in `crates/sqe-coordinator/tests`)

| File | Coverage |
|---|---|
| `lineage_emit_test.rs` | wiremock collector + Polaris quickstart stack. Run SELECT, CTAS, INSERT, MERGE, DDL. Assert each statement's RunEvent shape against §4.6 |
| `lineage_disabled_test.rs` | `enabled = false`, run identical workload, verify no events emitted |
| `lineage_failure_test.rs` | collector returns 500 -> spool populated. Collector recovers -> replay drains spool |
| `lineage_session_parent_test.rs` | three queries in one session share the same `parent.run.runId`; new session gets a new parent |

### 9.3 Snapshot tests

`insta` crate. Six representative queries -> committed `.snap` files of produced `RunEvent` JSON. Catches schema-shape regressions when facet versions bump.

### 9.4 Deferred

Bench/load validation of <2ms p99 overhead under TPC-H SF1 with `enabled = true`. Tracked as a future-work item.

## 10. File layout and doc deltas

### 10.1 New files

```
crates/sqe-lineage/Cargo.toml
crates/sqe-lineage/src/lib.rs
crates/sqe-lineage/src/event.rs
crates/sqe-lineage/src/extract/mod.rs
crates/sqe-lineage/src/extract/datasets.rs
crates/sqe-lineage/src/extract/columns.rs
crates/sqe-lineage/src/observer.rs
crates/sqe-lineage/src/emitter.rs
crates/sqe-lineage/src/sink.rs
crates/sqe-lineage/src/sinks/file.rs
crates/sqe-lineage/src/sinks/http.rs
crates/sqe-lineage/src/sinks/spool.rs
crates/sqe-lineage/tests/extract_columns_test.rs
crates/sqe-lineage/tests/sinks_file_test.rs
crates/sqe-lineage/tests/sinks_http_test.rs
crates/sqe-lineage/tests/sinks_spool_test.rs
crates/sqe-lineage/tests/observer_test.rs
crates/sqe-lineage/tests/event_serialise_test.rs
crates/sqe-lineage/tests/snapshots/             (insta .snap files committed)

crates/sqe-coordinator/tests/lineage_emit_test.rs
crates/sqe-coordinator/tests/lineage_disabled_test.rs
crates/sqe-coordinator/tests/lineage_failure_test.rs
crates/sqe-coordinator/tests/lineage_session_parent_test.rs
```

### 10.2 Modified files

```
Cargo.toml                                      # workspace member
crates/sqe-coordinator/Cargo.toml               # depend on sqe-lineage
crates/sqe-coordinator/src/query_handler.rs     # plumb Option<Arc<dyn LineageObserver>>
crates/sqe-coordinator/src/main.rs              # construct observer
crates/sqe-coordinator/src/bin/sqe_server.rs    # construct observer + validation
crates/sqe-cli/src/embedded.rs                  # plumb observer through embedded client
crates/sqe-core/src/config.rs                   # OpenLineageConfig + env overrides + validation
```

### 10.3 Documentation deltas

| File | Change |
|---|---|
| `docs/book/src/SUMMARY.md` | Add `Lineage (OpenLineage)` entry under Operations / Observability |
| `docs/book/src/operations/openlineage.md` *(new)* | Operator chapter: what gets emitted, config reference, sink-choice guidance, Marquez/DataHub quickstart, troubleshooting (spool growing, dropped events metric), v1 limitations |
| `docs/ebook/chapters/16e-the-lineage-trail.md` *(new, written after implementation lands)* | Narrative chapter following the 16b/16c/16d pattern. Voice rules from `docs/ebook/voice.md` and `CLAUDE.md` apply: no emdash, no endash, no Unicode arrows, no AI-tells |
| `docs/book/src/development/roadmap.md` | Move OpenLineage emitter from "deferred" to "shipped" after merge |
| `docs/roadmap.md` | Strike OL line from deferred list; add to shipped |
| `docs/superpowers/plans/2026-04-24-iceberg-matrix-parity.md` | Update line 874 to point at the implementation MR |
| `nextsteps.md` | Mark step done, shift NEXT pointer (per CLAUDE.md "After Completing Work") |
| `README.md` | Update roadmap checklist if it lists OL |
| `docs/datafusion-architecture.md` | Add `sqe-lineage` row to the crate table; one-paragraph note in the diagram explanation |

The ebook chapter is written after implementation, not before. The chapter describes what we built and what we learned; pre-writing it would force us to predict implementation details we'll discover during the work. This matches how `16d-the-duckdb-drift.md` was written after the V8/V9/V12 series shipped.

### 10.4 Net-new dependencies

Zero. Every needed crate (`reqwest`, `tokio`, `serde`, `serde_json`, `chrono`, `uuid`, `async-trait`, `tracing`, `prometheus`, `wiremock` (dev), `insta` (dev)) is already in the workspace.

## 11. Open knobs to confirm during implementation

These defaults are reasonable but not load-tested. Implementation phase confirms each.

- `spool_max_bytes` default of 100 MiB. Sized for a multi-day Marquez outage at moderate query volume; revisit after a week of soak.
- `replay_interval_secs` default of 30. Aggressive enough to recover quickly, quiet enough to avoid hammering during prolonged outages.
- `channel_capacity` default of 10000. Sized so a 10s emitter stall during normal load (~1k QPS) doesn't drop. Revisit at scale.
- HTTP retry count of 1. Higher counts mask collector problems; lower counts increase spool churn. 1 is the OL Java client default.

## 12. Future work (explicitly deferred to v2)

- Worker-side emit for distributed-mode column-lineage details that are only knowable post-physical-planning.
- mTLS to the OL collector (`tls_cert_path`, `tls_key_path`).
- Heartbeat / "running" events for queries longer than a configurable threshold.
- Kafka transport sink.
- Custom OL extension facets for SQE-specific concepts (per-query memory budget, spill events).
- Bench/load validation of overhead.
- Correlated subqueries deeper than one level with full column granularity.
