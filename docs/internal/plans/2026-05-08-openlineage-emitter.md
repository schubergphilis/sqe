# OpenLineage Emitter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a coordinator-side OpenLineage 2-0-2 emitter to SQE with column-level lineage, multi-catalog dataset naming, file + HTTP sinks, and disk-spool fallback. Off by default; zero overhead in the query path when disabled.

**Architecture:** New `sqe-lineage` crate exposes a `LineageObserver` trait. `query_handler` calls `on_query_start` / `on_query_complete` / `on_query_fail`. Observer is a bounded mpsc producer; a background emitter task drains the channel, runs the column-lineage extractor against the captured `LogicalPlan`, builds an OL `RunEvent`, and fans out to a `MultiSink` (file ‖ http ‖ http+spool).

**Tech Stack:** Rust, DataFusion `LogicalPlan`, `tokio::sync::mpsc`, `reqwest` (rustls-tls + gzip), `serde_json`, `wiremock` (dev), `insta` (dev). Zero net-new external dependencies.

**Spec:** `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md`

**Branch:** `feat/openlineage-emitter` (already created)

---

## Phase A: Crate scaffold

### Task A1: Create `sqe-lineage` crate skeleton

**Files:**
- Create: `crates/sqe-lineage/Cargo.toml`
- Create: `crates/sqe-lineage/src/lib.rs`
- Modify: `Cargo.toml` (workspace `members`)

- [ ] **Step 1: Create crate Cargo.toml**

`crates/sqe-lineage/Cargo.toml`:

```toml
[package]
name = "sqe-lineage"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
sqe-core   = { path = "../sqe-core" }
sqe-sql    = { path = "../sqe-sql" }
sqe-auth   = { path = "../sqe-auth" }
sqe-metrics = { path = "../sqe-metrics" }
datafusion = { workspace = true }
tokio      = { workspace = true, features = ["rt", "sync", "macros", "fs", "time"] }
async-trait = { workspace = true }
serde      = { workspace = true, features = ["derive"] }
serde_json = { workspace = true }
chrono     = { workspace = true, features = ["serde"] }
uuid       = { workspace = true, features = ["v4", "serde"] }
reqwest    = { workspace = true, default-features = false, features = ["rustls-tls", "gzip", "json", "http2"] }
tracing    = { workspace = true }
prometheus = { workspace = true }
futures    = { workspace = true }
thiserror  = { workspace = true }

[dev-dependencies]
wiremock   = { workspace = true }
insta      = { workspace = true, features = ["json"] }
tempfile   = { workspace = true }
tokio      = { workspace = true, features = ["rt-multi-thread", "test-util"] }
```

- [ ] **Step 2: Create lib.rs with module skeleton**

`crates/sqe-lineage/src/lib.rs`:

```rust
//! OpenLineage emitter for SQE coordinator.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md`.

pub mod event;
pub mod extract;
pub mod observer;
pub mod emitter;
pub mod sink;
pub mod sinks;

pub use observer::{LineageObserver, ChannelObserver, QueryStartCtx, QueryCompleteCtx, QueryFailCtx, PlanOrHint, LineageHint};
pub use sink::{Sink, SinkError, MultiSink};
pub use event::{RunEvent, EventType};
```

Create empty stub files so the crate compiles:

```
crates/sqe-lineage/src/event.rs        // pub use placeholder
crates/sqe-lineage/src/extract/mod.rs  // pub mod datasets; pub mod columns;
crates/sqe-lineage/src/extract/datasets.rs  // empty
crates/sqe-lineage/src/extract/columns.rs   // empty
crates/sqe-lineage/src/observer.rs     // empty
crates/sqe-lineage/src/emitter.rs      // empty
crates/sqe-lineage/src/sink.rs         // empty
crates/sqe-lineage/src/sinks/mod.rs    // pub mod file; pub mod http; pub mod spool;
crates/sqe-lineage/src/sinks/file.rs   // empty
crates/sqe-lineage/src/sinks/http.rs   // empty
crates/sqe-lineage/src/sinks/spool.rs  // empty
```

Each module must export at least one `pub` item the parent re-exports. For initial scaffolding, fill each empty file with a single dummy `pub fn _todo() {}` so `pub use` lines in `lib.rs` compile. Delete the dummies as real types arrive in later tasks.

Simpler alternative: leave the `pub use` lines in `lib.rs` commented out, uncomment per task. Pick whichever fits the implementer.

- [ ] **Step 3: Add to workspace `Cargo.toml`**

Modify the existing `[workspace] members = [...]` array, add:

```toml
"crates/sqe-lineage",
```

- [ ] **Step 4: Build the workspace**

Run: `cargo build -p sqe-lineage`
Expected: success, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/sqe-lineage/
git commit -m "feat(lineage): scaffold sqe-lineage crate"
```

---

## Phase B: OL event types

### Task B1: Top-level RunEvent + EventType

**Files:**
- Create: `crates/sqe-lineage/src/event.rs`
- Create: `crates/sqe-lineage/tests/event_serialise_test.rs`

- [ ] **Step 1: Write the failing test**

`crates/sqe-lineage/tests/event_serialise_test.rs`:

```rust
use sqe_lineage::event::*;
use chrono::Utc;
use uuid::Uuid;

#[test]
fn run_event_serialises_with_required_fields() {
    let ev = RunEvent {
        eventType: EventType::Start,
        eventTime: Utc::now().to_rfc3339(),
        producer: "https://github.com/sbp/sqe/v0.1.0".to_string(),
        schemaURL: SCHEMA_URL.to_string(),
        run: Run::new(Uuid::new_v4()),
        job: Job { namespace: "sqe".into(), name: "query:abc".into(), facets: Default::default() },
        inputs: vec![],
        outputs: vec![],
    };
    let json = serde_json::to_value(&ev).unwrap();
    assert_eq!(json["eventType"], "START");
    assert_eq!(json["schemaURL"], SCHEMA_URL);
    assert!(json["run"]["runId"].is_string());
    assert_eq!(json["job"]["namespace"], "sqe");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-lineage --test event_serialise_test run_event_serialises_with_required_fields`
Expected: FAIL with "unresolved import" or "no field".

- [ ] **Step 3: Implement event types**

`crates/sqe-lineage/src/event.rs`:

```rust
#![allow(non_snake_case)]
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::BTreeMap;

pub const SCHEMA_URL: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventType { Start, Complete, Fail }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEvent {
    pub eventType: EventType,
    pub eventTime: String,
    pub producer: String,
    pub schemaURL: String,
    pub run: Run,
    pub job: Job,
    pub inputs: Vec<InputDataset>,
    pub outputs: Vec<OutputDataset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub runId: Uuid,
    #[serde(default)]
    pub facets: RunFacets,
}

impl Run {
    pub fn new(id: Uuid) -> Self { Self { runId: id, facets: Default::default() } }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Job {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub facets: JobFacets,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nominalTime: Option<NominalTimeFacet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<ParentRunFacet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errorMessage: Option<ErrorMessageFacet>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<SqlFacet>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputDataset {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub facets: DatasetFacets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputDataset {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub facets: DatasetFacets,
    #[serde(default)]
    pub outputFacets: OutputDatasetFacets,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatasetFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaFacet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataSource: Option<DataSourceFacet>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OutputDatasetFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columnLineage: Option<ColumnLineageFacet>,
}

// Facet stubs filled in by Task B2.
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct NominalTimeFacet { pub nominalStartTime: String }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct ParentRunFacet { pub run: Run, pub job: Job }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct ErrorMessageFacet { pub message: String, pub programmingLanguage: String }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct SqlFacet { pub query: String, pub dialect: String }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct SchemaFacet { pub fields: Vec<SchemaField> }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct SchemaField { pub name: String, #[serde(rename = "type")] pub field_type: String }
#[derive(Debug, Clone, Serialize, Deserialize)] pub struct DataSourceFacet { pub name: String, pub uri: String }

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnLineageFacet {
    pub fields: BTreeMap<String, ColumnLineageEntry>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineageEntry {
    pub inputFields: Vec<ColumnLineageInput>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineageInput {
    pub namespace: String,
    pub name: String,
    pub field: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transformations: Vec<Transformation>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transformation {
    #[serde(rename = "type")] pub kind: String,    // "DIRECT" | "INDIRECT"
    pub subtype: String,                            // "IDENTITY" | "TRANSFORMATION" | "AGGREGATION" | "FILTER" | "JOIN" | "GROUP_BY" | "SORT" | "WINDOW" | "CONDITIONAL" | "MASKED" | "MERGE_INSERT" | "MERGE_UPDATE"
    pub description: String,
    pub masking: bool,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-lineage --test event_serialise_test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-lineage/src/event.rs crates/sqe-lineage/tests/event_serialise_test.rs
git commit -m "feat(lineage): add OL 2-0-2 RunEvent types"
```

### Task B2: Snapshot tests for each event flavour

**Files:**
- Modify: `crates/sqe-lineage/tests/event_serialise_test.rs`
- Create: `crates/sqe-lineage/tests/snapshots/` (insta auto-creates)

- [ ] **Step 1: Add snapshot tests**

Append to `event_serialise_test.rs`:

```rust
fn fixed_uuid() -> uuid::Uuid { uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap() }
fn sample_run() -> Run {
    Run { runId: fixed_uuid(), facets: RunFacets {
        nominalTime: Some(NominalTimeFacet { nominalStartTime: "2026-05-08T10:00:00Z".into() }),
        parent: None, errorMessage: None,
    }}
}
fn sample_job(name: &str) -> Job {
    Job { namespace: "sqe".into(), name: name.into(),
          facets: JobFacets { sql: Some(SqlFacet { query: "SELECT 1".into(), dialect: "sqe".into() }) } }
}

#[test]
fn snapshot_select_complete() {
    let ev = RunEvent {
        eventType: EventType::Complete,
        eventTime: "2026-05-08T10:00:01Z".into(),
        producer: "https://github.com/sbp/sqe/v0.1.0".into(),
        schemaURL: SCHEMA_URL.into(),
        run: sample_run(), job: sample_job("query:abc"),
        inputs: vec![InputDataset {
            namespace: "https://polaris.example/api/catalog".into(),
            name: "sales.orders".into(),
            facets: DatasetFacets {
                schema: Some(SchemaFacet { fields: vec![SchemaField { name: "id".into(), field_type: "long".into() }] }),
                dataSource: Some(DataSourceFacet { name: "polaris".into(), uri: "https://polaris.example/api/catalog".into() }),
            },
        }],
        outputs: vec![],
    };
    insta::assert_json_snapshot!(ev);
}

#[test]
fn snapshot_ctas_complete_with_column_lineage() { /* analogous, with ColumnLineageFacet populated */ }
#[test]
fn snapshot_query_fail() { /* errorMessage set, eventType = FAIL */ }
```

- [ ] **Step 2: Run snapshot tests**

Run: `cargo test -p sqe-lineage --test event_serialise_test`
Expected: snapshots auto-recorded (review with `cargo insta review`), then PASS.

Run: `cargo insta review` and accept the .snap files.

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-lineage/tests/snapshots/ crates/sqe-lineage/tests/event_serialise_test.rs
git commit -m "test(lineage): snapshot RunEvent flavours"
```

---

## Phase C: Observer + emitter

### Task C1: LineageObserver trait + context structs

**Files:**
- Create: `crates/sqe-lineage/src/observer.rs`

- [ ] **Step 1: Write the failing test**

`crates/sqe-lineage/tests/observer_test.rs`:

```rust
use sqe_lineage::*;
use std::sync::{Arc, Mutex};

#[derive(Default, Clone)]
struct MockObserver { calls: Arc<Mutex<Vec<&'static str>>> }
#[async_trait::async_trait]
impl LineageObserver for MockObserver {
    fn on_query_start(&self, _: QueryStartCtx)    { self.calls.lock().unwrap().push("start"); }
    fn on_query_complete(&self, _: QueryCompleteCtx) { self.calls.lock().unwrap().push("complete"); }
    fn on_query_fail(&self, _: QueryFailCtx)       { self.calls.lock().unwrap().push("fail"); }
}

#[test]
fn observer_trait_object_dispatches_calls() {
    let obs: Arc<dyn LineageObserver> = Arc::new(MockObserver::default());
    obs.on_query_start(QueryStartCtx::dummy());
    obs.on_query_complete(QueryCompleteCtx::dummy());
    obs.on_query_fail(QueryFailCtx::dummy());
    // assertion via downcast in real test, simplified here
}
```

- [ ] **Step 2: Run, expect FAIL** (`unresolved imports`)

- [ ] **Step 3: Implement observer types**

`crates/sqe-lineage/src/observer.rs`:

```rust
use crate::sink::{Sink, MultiSink};
use chrono::{DateTime, Utc};
use datafusion::logical_expr::LogicalPlan;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

pub struct UserCtx {
    pub username: String,
    pub bearer: Option<String>,
}

pub enum LineageHint {
    DdlSchema { catalog: String, schema: String, table: String, columns: Vec<(String, String)> },
}

pub enum PlanOrHint {
    Plan(LogicalPlan),
    Hint(LineageHint),
}

pub struct QueryStartCtx {
    pub run_id: Uuid,
    pub job_namespace: String,
    pub sql: String,
    pub user: UserCtx,
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    pub statement_kind: String,
}

pub struct QueryCompleteCtx {
    pub run_id: Uuid,
    pub job_namespace: String,
    pub sql: String,
    pub user: UserCtx,
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub duration: Duration,
    pub statement_kind: String,
    pub rows_returned: usize,
    pub plan: Option<PlanOrHint>,
}

pub struct QueryFailCtx {
    pub run_id: Uuid,
    pub job_namespace: String,
    pub sql: String,
    pub user: UserCtx,
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub duration: Duration,
    pub statement_kind: String,
    pub error_message: String,
    pub plan: Option<PlanOrHint>,
}

#[async_trait::async_trait]
pub trait LineageObserver: Send + Sync {
    fn on_query_start(&self, ctx: QueryStartCtx);
    fn on_query_complete(&self, ctx: QueryCompleteCtx);
    fn on_query_fail(&self, ctx: QueryFailCtx);
}

pub enum LineageMsg {
    Start(QueryStartCtx),
    Complete(QueryCompleteCtx),
    Fail(QueryFailCtx),
}

pub struct ChannelObserver {
    tx: mpsc::Sender<LineageMsg>,
    dropped: prometheus::IntCounter,
}

impl ChannelObserver {
    pub fn new(tx: mpsc::Sender<LineageMsg>, dropped: prometheus::IntCounter) -> Self {
        Self { tx, dropped }
    }

    fn try_send(&self, msg: LineageMsg) {
        if self.tx.try_send(msg).is_err() {
            self.dropped.inc();
            tracing::warn!("sqe-lineage channel full; dropping event");
        }
    }
}

#[async_trait::async_trait]
impl LineageObserver for ChannelObserver {
    fn on_query_start(&self, ctx: QueryStartCtx)       { self.try_send(LineageMsg::Start(ctx)); }
    fn on_query_complete(&self, ctx: QueryCompleteCtx) { self.try_send(LineageMsg::Complete(ctx)); }
    fn on_query_fail(&self, ctx: QueryFailCtx)         { self.try_send(LineageMsg::Fail(ctx)); }
}
```

- [ ] **Step 4: Run, expect PASS**

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-lineage/src/observer.rs crates/sqe-lineage/tests/observer_test.rs
git commit -m "feat(lineage): add LineageObserver trait + ChannelObserver"
```

### Task C2: Channel back-pressure test

- [ ] **Step 1: Test that fills channel**

```rust
#[tokio::test]
async fn channel_full_drops_newest_and_increments_metric() {
    let (tx, _rx) = mpsc::channel(2);
    let counter = prometheus::IntCounter::new("dropped", "h").unwrap();
    let obs = ChannelObserver::new(tx, counter.clone());
    // Fill the channel
    obs.on_query_start(QueryStartCtx::dummy());
    obs.on_query_start(QueryStartCtx::dummy());
    // Third send drops
    obs.on_query_start(QueryStartCtx::dummy());
    assert_eq!(counter.get(), 1);
}
```

`QueryStartCtx::dummy()` is a #[cfg(test)] helper that fills with placeholder values. Add it under `#[cfg(test)] impl QueryStartCtx { pub fn dummy() -> Self { ... } }`. Same shape for the other two ctx types.

- [ ] **Step 2: Run, fix test ergonomics until PASS**

- [ ] **Step 3: Commit**

```bash
git commit -am "test(lineage): channel back-pressure drops newest"
```

### Task C3: Emitter task that drains the channel

**Files:**
- Create: `crates/sqe-lineage/src/emitter.rs`

- [ ] **Step 1: Implement emitter loop**

`crates/sqe-lineage/src/emitter.rs`:

```rust
use crate::event::*;
use crate::extract;
use crate::observer::*;
use crate::sink::{MultiSink, Sink};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct EmitterConfig {
    pub job_namespace: String,
    pub producer: String,
    pub catalog_lookup: Arc<dyn Fn(&str) -> String + Send + Sync>,  // catalog name -> REST URL
}

pub fn spawn_emitter(
    mut rx: mpsc::Receiver<LineageMsg>,
    sinks: Arc<MultiSink>,
    cfg: Arc<EmitterConfig>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let event = match msg {
                LineageMsg::Start(c)    => build_start(&c, &cfg),
                LineageMsg::Complete(c) => build_complete(&c, &cfg),
                LineageMsg::Fail(c)     => build_fail(&c, &cfg),
            };
            sinks.send(&event).await;  // never fails: MultiSink isolates
        }
    })
}

fn build_start(c: &QueryStartCtx, cfg: &EmitterConfig) -> RunEvent { /* ... */ }
fn build_complete(c: &QueryCompleteCtx, cfg: &EmitterConfig) -> RunEvent {
    let (inputs, outputs) = match &c.plan {
        Some(PlanOrHint::Plan(p)) => extract::extract_lineage(p.as_ref(), &cfg.catalog_lookup),  // p: &Box<LogicalPlan>; deref via as_ref()
        Some(PlanOrHint::Hint(h)) => extract::extract_from_hint(h, &cfg.catalog_lookup),
        None => (vec![], vec![]),
    };
    RunEvent {
        eventType: EventType::Complete,
        eventTime: c.ended_at.to_rfc3339(),
        producer: cfg.producer.clone(),
        schemaURL: SCHEMA_URL.into(),
        run: build_run(c.run_id, c.started_at, &c.session_id, None),
        job: build_job(&cfg.job_namespace, &c.statement_kind, &c.sql),
        inputs, outputs,
    }
}
fn build_fail(c: &QueryFailCtx, cfg: &EmitterConfig) -> RunEvent { /* analogous, errorMessage facet set */ }

fn build_run(run_id: uuid::Uuid, started: chrono::DateTime<chrono::Utc>, session_id: &str, error: Option<&str>) -> Run { /* ... */ }
fn build_job(namespace: &str, kind: &str, sql: &str) -> Job {
    let hash = sqe_metrics::audit::query_hash(sql);
    Job {
        namespace: namespace.into(),
        name: format!("{kind}:{hash}"),
        facets: JobFacets {
            sql: Some(SqlFacet {
                query: sqe_metrics::audit::redact_pii(sql),
                dialect: "sqe".into(),
            }),
        },
    }
}
```

`extract::extract_lineage` and `extract::extract_from_hint` are stubs returning `(vec![], vec![])` until Phase E. The emitter compiles and works for empty-lineage events first.

- [ ] **Step 2: Add stub for extract**

`crates/sqe-lineage/src/extract/mod.rs`:

```rust
pub mod datasets;
pub mod columns;

use crate::event::{InputDataset, OutputDataset};
use crate::observer::LineageHint;
use datafusion::logical_expr::LogicalPlan;
use std::sync::Arc;

pub type CatalogLookup = Arc<dyn Fn(&str) -> String + Send + Sync>;

pub fn extract_lineage(_plan: &LogicalPlan, _lookup: &CatalogLookup) -> (Vec<InputDataset>, Vec<OutputDataset>) {
    // Filled in Phase E.
    (vec![], vec![])
}

pub fn extract_from_hint(_hint: &LineageHint, _lookup: &CatalogLookup) -> (Vec<InputDataset>, Vec<OutputDataset>) {
    // Filled in Phase E (Task E9).
    (vec![], vec![])
}
```

- [ ] **Step 3: Run cargo build, expect success**

Run: `cargo build -p sqe-lineage`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-lineage/src/emitter.rs crates/sqe-lineage/src/extract/mod.rs
git commit -m "feat(lineage): add emitter task with stubbed extractor"
```

---

## Phase D: Sinks

### Task D1: Sink trait + MultiSink

**Files:**
- Create: `crates/sqe-lineage/src/sink.rs`

- [ ] **Step 1: Failing test**

`crates/sqe-lineage/tests/sink_multi_test.rs`:

```rust
use sqe_lineage::*;
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};

struct Counting { count: AtomicUsize, fail: bool }
#[async_trait::async_trait]
impl Sink for Counting {
    async fn send(&self, _: &event::RunEvent) -> Result<(), SinkError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        if self.fail { Err(SinkError::Other("boom".into())) } else { Ok(()) }
    }
    fn name(&self) -> &'static str { "counting" }
}

#[tokio::test]
async fn multi_sink_fans_out_and_isolates_failures() {
    let a = Arc::new(Counting { count: AtomicUsize::new(0), fail: false });
    let b = Arc::new(Counting { count: AtomicUsize::new(0), fail: true  });
    let c = Arc::new(Counting { count: AtomicUsize::new(0), fail: false });
    let multi = MultiSink::new(vec![a.clone(), b.clone(), c.clone()]);
    multi.send(&dummy_event()).await;
    assert_eq!(a.count.load(Ordering::SeqCst), 1);
    assert_eq!(b.count.load(Ordering::SeqCst), 1);
    assert_eq!(c.count.load(Ordering::SeqCst), 1);  // c still ran despite b's failure
}
fn dummy_event() -> event::RunEvent { /* minimal valid RunEvent */ }
```

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Implement**

`crates/sqe-lineage/src/sink.rs`:

```rust
use crate::event::RunEvent;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("http error: {0}")]
    Http(String),
    #[error("serialise error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("other: {0}")]
    Other(String),
}

#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    async fn send(&self, event: &RunEvent) -> Result<(), SinkError>;
    fn name(&self) -> &'static str;
}

pub struct MultiSink {
    sinks: Vec<Arc<dyn Sink>>,
    errors: prometheus::IntCounterVec,
}

impl MultiSink {
    pub fn new(sinks: Vec<Arc<dyn Sink>>) -> Self {
        let errors = prometheus::IntCounterVec::new(
            prometheus::Opts::new("sqe_lineage_sink_errors_total", "OL sink failures"),
            &["sink"],
        ).unwrap();
        Self { sinks, errors }
    }

    pub async fn send(&self, ev: &RunEvent) {
        let futs = self.sinks.iter().map(|s| {
            let s = s.clone();
            async move { (s.name(), s.send(ev).await) }
        });
        let results = futures::future::join_all(futs).await;
        for (name, r) in results {
            if let Err(e) = r {
                self.errors.with_label_values(&[name]).inc();
                tracing::warn!(sink = name, error = %e, "OL sink failed");
            }
        }
    }
}
```

- [ ] **Step 4: Run, expect PASS**

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-lineage/src/sink.rs crates/sqe-lineage/tests/sink_multi_test.rs
git commit -m "feat(lineage): Sink trait + MultiSink fan-out"
```

### Task D2: FileSink (JSONL appender)

**Files:**
- Create: `crates/sqe-lineage/src/sinks/file.rs`
- Create: `crates/sqe-lineage/tests/sinks_file_test.rs`

- [ ] **Step 1: Failing test**

```rust
use sqe_lineage::*;
use tempfile::tempdir;

#[tokio::test]
async fn file_sink_appends_jsonl() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let sink = sinks::file::FileSink::new(path.to_str().unwrap()).unwrap();
    let ev = dummy_event();
    sink.send(&ev).await.unwrap();
    sink.send(&ev).await.unwrap();
    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content.lines().count(), 2);
    for line in content.lines() {
        let _: event::RunEvent = serde_json::from_str(line).unwrap();
    }
}
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement**

`crates/sqe-lineage/src/sinks/file.rs`:

```rust
use crate::event::RunEvent;
use crate::sink::{Sink, SinkError};
use std::io::Write;
use std::sync::Mutex;

pub struct FileSink {
    writer: Mutex<std::io::BufWriter<std::fs::File>>,
}

impl FileSink {
    pub fn new(path: &str) -> Result<Self, SinkError> {
        let f = std::fs::OpenOptions::new()
            .create(true).append(true).open(path)?;
        Ok(Self { writer: Mutex::new(std::io::BufWriter::new(f)) })
    }
}

#[async_trait::async_trait]
impl Sink for FileSink {
    async fn send(&self, ev: &RunEvent) -> Result<(), SinkError> {
        let line = serde_json::to_string(ev)?;
        let mut w = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        writeln!(w, "{line}")?;
        w.flush()?;
        Ok(())
    }
    fn name(&self) -> &'static str { "file" }
}
```

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(lineage): FileSink JSONL appender"
```

### Task D3: HttpSink with auth modes

**Files:**
- Create: `crates/sqe-lineage/src/sinks/http.rs`
- Create: `crates/sqe-lineage/tests/sinks_http_test.rs`

- [ ] **Step 1: Failing test against wiremock**

```rust
use sqe_lineage::*;
use wiremock::{matchers::*, Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn http_sink_posts_with_bearer() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/lineage"))
        .and(header("Authorization", "Bearer secret"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1).mount(&server).await;

    let sink = sinks::http::HttpSink::new(sinks::http::HttpConfig {
        endpoint: format!("{}/api/v1/lineage", server.uri()),
        auth: sinks::http::AuthMode::Bearer("secret".into()),
        timeout_ms: 5000, retry: 1,
    }).unwrap();
    sink.send(&dummy_event()).await.unwrap();
}

#[tokio::test]
async fn http_sink_retries_on_503() { /* one 503 then 200; expect ok */ }

#[tokio::test]
async fn http_sink_user_token_forwards_session_bearer() { /* AuthMode::UserToken case */ }
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement**

`crates/sqe-lineage/src/sinks/http.rs`:

```rust
use crate::event::RunEvent;
use crate::sink::{Sink, SinkError};
use std::time::Duration;

#[derive(Clone, Debug)]
pub enum AuthMode {
    None,
    Bearer(String),       // static api_key
    UserToken(String),    // per-event bearer
}

#[derive(Clone, Debug)]
pub struct HttpConfig {
    pub endpoint: String,
    pub auth: AuthMode,
    pub timeout_ms: u64,
    pub retry: u32,
}

pub struct HttpSink {
    client: reqwest::Client,
    cfg: HttpConfig,
}

impl HttpSink {
    pub fn new(cfg: HttpConfig) -> Result<Self, SinkError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(cfg.timeout_ms))
            .gzip(true)
            .build()
            .map_err(|e| SinkError::Http(e.to_string()))?;
        Ok(Self { client, cfg })
    }
}

#[async_trait::async_trait]
impl Sink for HttpSink {
    async fn send(&self, ev: &RunEvent) -> Result<(), SinkError> {
        let body = serde_json::to_vec(ev)?;
        let mut attempt = 0;
        loop {
            let mut req = self.client
                .post(&self.cfg.endpoint)
                .header("Content-Type", "application/json")
                .body(body.clone());
            req = match &self.cfg.auth {
                AuthMode::None        => req,
                AuthMode::Bearer(t)   => req.bearer_auth(t),
                AuthMode::UserToken(t)=> req.bearer_auth(t),
            };
            match req.send().await {
                Ok(r) if r.status().is_success() => return Ok(()),
                Ok(r) if r.status().is_server_error() && attempt < self.cfg.retry => {
                    let backoff = 250u64 << attempt;
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                    attempt += 1;
                    continue;
                }
                Ok(r) => return Err(SinkError::Http(format!("status {}", r.status()))),
                Err(e) if attempt < self.cfg.retry => {
                    let backoff = 250u64 << attempt;
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                    attempt += 1;
                    continue;
                }
                Err(e) => return Err(SinkError::Http(e.to_string())),
            }
        }
    }
    fn name(&self) -> &'static str { "http" }
}
```

`AuthMode::UserToken` is constructed *per send* with the session bearer. The emitter receives the bearer from `QueryStartCtx::user.bearer` and constructs a one-shot `HttpSink` view. Simpler alternative: `HttpSink` accepts a `Box<dyn Fn() -> Option<String>>` for dynamic auth. Pick the simpler approach. Clone the sink config per event when `auth_mode = "user_token"` and rebuild a tiny HttpSink. The reqwest client is reusable so wrap one shared `Client` behind an `Arc` and reuse it.

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(lineage): HttpSink with bearer/user_token auth"
```

### Task D4: SpoolSink wrapping HttpSink

**Files:**
- Create: `crates/sqe-lineage/src/sinks/spool.rs`
- Create: `crates/sqe-lineage/tests/sinks_spool_test.rs`

- [ ] **Step 1: Failing test**

```rust
#[tokio::test]
async fn spool_buffers_on_http_failure_then_drains_on_recovery() {
    let server = MockServer::start().await;
    // Initially return 500
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1).mount(&server).await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200))
        .mount(&server).await;

    let dir = tempdir().unwrap();
    let http = HttpSink::new(...).unwrap();
    let spool = SpoolSink::wrap(http, SpoolConfig {
        path: dir.path().to_path_buf(),
        max_bytes: 10 * 1024 * 1024,
        replay_interval: Duration::from_millis(100),
    });

    spool.send(&dummy_event()).await.unwrap();          // 500 -> spooled
    let spool_file = dir.path().join("spool.jsonl");
    assert!(std::fs::metadata(&spool_file).unwrap().len() > 0);

    // Wait one replay tick
    tokio::time::sleep(Duration::from_millis(200)).await;
    // After replay, spool should be drained
    assert_eq!(std::fs::metadata(&spool_file).unwrap().len(), 0);
}

#[tokio::test]
async fn spool_drops_newest_on_cap() { /* fill past max_bytes; verify drop counter increments */ }
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement**

`crates/sqe-lineage/src/sinks/spool.rs`:

```rust
use crate::event::RunEvent;
use crate::sink::{Sink, SinkError};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

pub struct SpoolConfig {
    pub path: PathBuf,
    pub max_bytes: u64,
    pub replay_interval: Duration,
}

pub struct SpoolSink {
    inner: Arc<dyn Sink>,
    cfg: SpoolConfig,
    drops: prometheus::IntCounter,
}

impl SpoolSink {
    pub fn wrap(inner: impl Sink + 'static, cfg: SpoolConfig) -> Arc<Self> {
        // ... spawn replay task ...
    }

    async fn append_to_spool(&self, ev: &RunEvent) -> Result<(), SinkError> {
        std::fs::create_dir_all(&self.cfg.path)?;
        let live = self.cfg.path.join("spool.jsonl");
        let total = total_spool_bytes(&self.cfg.path)?;
        if total >= self.cfg.max_bytes {
            self.drops.inc();
            tracing::warn!("spool cap reached; dropping event");
            return Ok(());
        }
        let mut f = tokio::fs::OpenOptions::new().create(true).append(true).open(&live).await?;
        let line = serde_json::to_string(ev)?;
        f.write_all(line.as_bytes()).await?;
        f.write_all(b"\n").await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for SpoolSink {
    async fn send(&self, ev: &RunEvent) -> Result<(), SinkError> {
        match self.inner.send(ev).await {
            Ok(()) => Ok(()),
            Err(_) => self.append_to_spool(ev).await,
        }
    }
    fn name(&self) -> &'static str { "spool" }
}

fn total_spool_bytes(dir: &std::path::Path) -> std::io::Result<u64> { /* sum file sizes in dir matching spool*.jsonl */ }
async fn replay_loop(spool_dir: PathBuf, inner: Arc<dyn Sink>, interval: Duration) {
    // Every tick:
    // 1. Rotate spool.jsonl -> spool.jsonl.<timestamp> if non-empty
    // 2. For each rotated file (oldest first):
    //    - Read line by line
    //    - inner.send() each
    //    - On all-success: delete the file
    //    - On any failure: stop; try again next tick
}
```

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(lineage): SpoolSink with disk fallback + replay"
```

---

## Phase E: Lineage extractors

Each task in Phase E follows the same pattern: failing test with hand-built `LogicalPlan`, implement node rule, PASS, commit.

### Task E1: TableScan -> dataset reference

**Files:**
- Modify: `crates/sqe-lineage/src/extract/datasets.rs`
- Create: `crates/sqe-lineage/tests/extract_datasets_test.rs`

- [ ] **Step 1: Failing test**

```rust
use sqe_lineage::extract::*;
use datafusion::logical_expr::*;
use std::sync::Arc;

fn lookup_polaris(name: &str) -> String {
    match name {
        "polaris" => "https://polaris.example/api/catalog".into(),
        _ => format!("sqe://{name}"),
    }
}

#[test]
fn table_scan_yields_one_input_dataset() {
    let plan = build_simple_scan("polaris", "sales", "orders", &["id", "amount"]);
    let inputs = datasets::extract_inputs(&plan, &Arc::new(lookup_polaris));
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].namespace, "https://polaris.example/api/catalog");
    assert_eq!(inputs[0].name, "sales.orders");
}

fn build_simple_scan(catalog: &str, schema: &str, table: &str, cols: &[&str]) -> LogicalPlan {
    // Use LogicalPlanBuilder + a MemTable; qualified name = catalog.schema.table
    // ...
}
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement**

`crates/sqe-lineage/src/extract/datasets.rs`:

```rust
use crate::event::*;
use crate::extract::CatalogLookup;
use datafusion::logical_expr::{LogicalPlan, TableScan};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

pub fn extract_inputs(plan: &LogicalPlan, lookup: &CatalogLookup) -> Vec<InputDataset> {
    let mut out = vec![];
    plan.apply(|node| {
        if let LogicalPlan::TableScan(TableScan { table_name, source, .. }) = node {
            let parts: Vec<&str> = table_name.to_string().split('.').collect();
            let (catalog, schema, table) = match parts.as_slice() {
                [c, s, t] => (*c, *s, *t),
                [s, t]    => ("default", *s, *t),
                [t]       => ("default", "default", *t),
                _         => return Ok(TreeNodeRecursion::Continue),
            };
            let namespace = lookup(catalog);
            let schema_facet = SchemaFacet {
                fields: source.schema().fields().iter().map(|f| SchemaField {
                    name: f.name().clone(),
                    field_type: f.data_type().to_string(),
                }).collect(),
            };
            out.push(InputDataset {
                namespace: namespace.clone(),
                name: format!("{schema}.{table}"),
                facets: DatasetFacets {
                    schema: Some(schema_facet),
                    dataSource: Some(DataSourceFacet { name: catalog.into(), uri: namespace }),
                },
            });
        }
        Ok(TreeNodeRecursion::Continue)
    }).ok();
    out
}
```

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(lineage): extract input datasets from TableScan"
```

### Task E2: Write target -> output dataset

**Files:**
- Modify: `crates/sqe-lineage/src/extract/datasets.rs`

- [ ] **Step 1: Failing test for INSERT plan**

```rust
#[test]
fn insert_plan_yields_one_output_dataset() {
    let plan = build_insert_plan("polaris", "sales", "orders_archive", &["id"]);
    let outputs = datasets::extract_outputs(&plan, &Arc::new(lookup_polaris));
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].name, "sales.orders_archive");
}
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement** by matching `LogicalPlan::Dml(DmlStatement)` and `LogicalPlan::Ddl(DdlStatement)` variants. Map the target `table_name` through the same parts logic as Task E1.

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(lineage): extract output dataset from write plans"
```

### Task E3: ColumnTrace data structure

**Files:**
- Modify: `crates/sqe-lineage/src/extract/columns.rs`

- [ ] **Step 1: Define types**

```rust
use crate::event::Transformation;

#[derive(Clone, Debug)]
pub struct ColumnDep {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub field: String,
    pub transformation: Transformation,
}

/// Trace[i] is the list of leaf-column dependencies of the i-th output column of a node.
pub type ColumnTrace = Vec<Vec<ColumnDep>>;

pub fn direct_identity()         -> Transformation { make("DIRECT", "IDENTITY", false) }
pub fn direct_transformation()   -> Transformation { make("DIRECT", "TRANSFORMATION", false) }
pub fn direct_aggregation()      -> Transformation { make("DIRECT", "AGGREGATION", false) }
pub fn direct_window()           -> Transformation { make("DIRECT", "WINDOW", false) }
pub fn indirect_filter()         -> Transformation { make("INDIRECT", "FILTER", false) }
pub fn indirect_join()           -> Transformation { make("INDIRECT", "JOIN", false) }
pub fn indirect_groupby()        -> Transformation { make("INDIRECT", "GROUP_BY", false) }
pub fn indirect_sort()           -> Transformation { make("INDIRECT", "SORT", false) }
pub fn indirect_window()         -> Transformation { make("INDIRECT", "WINDOW", false) }
pub fn indirect_conditional()    -> Transformation { make("INDIRECT", "CONDITIONAL", false) }
pub fn masked()                  -> Transformation { make("DIRECT", "MASKED", true) }
pub fn merge_insert()            -> Transformation { make("DIRECT", "MERGE_INSERT", false) }
pub fn merge_update()            -> Transformation { make("DIRECT", "MERGE_UPDATE", false) }

fn make(kind: &str, subtype: &str, masking: bool) -> Transformation {
    Transformation { kind: kind.into(), subtype: subtype.into(), description: String::new(), masking }
}
```

- [ ] **Step 2: Commit**

```bash
git commit -am "feat(lineage): ColumnTrace + Transformation factories"
```

### Tasks E4 - E10: Per-node trace rules

Each follows the same TDD pattern. Test files: `crates/sqe-lineage/tests/extract_columns_<rule>_test.rs`. Reference §5.2 of the spec for each rule's exact semantics.

| Task | Node rule | Test name | Key code pattern |
|---|---|---|---|
| E4 | `TableScan` rule | `table_scan_emits_identity_per_column` | each column -> single `ColumnDep { transformation: direct_identity() }` |
| E5 | `Projection` rule | `projection_passthrough_is_identity`, `projection_expr_is_transformation` | `Expr::column_refs()` enumerates source cols; classify by expr shape |
| E6 | `Filter` rule | `filter_adds_indirect_to_all_outputs` | passthrough; predicate's `column_refs()` add `indirect_filter()` deps to every output |
| E7 | `Aggregate` rule | `aggregate_groupby_identity_aggregation_marked`, `aggregate_groupby_adds_indirect_to_aggs` | group-by exprs map by shape; agg-fn args -> `direct_aggregation()`; group-bys -> `indirect_groupby()` on aggregated outputs |
| E8 | `Join` rule | `join_passes_through_each_side`, `join_predicate_adds_indirect` | each side's trace passes through; join `on` predicate adds `indirect_join()` to all outputs |
| E9 | `Union`, `Sort`, `Limit`, `Distinct`, `SubqueryAlias` rules | one test each | merge-by-position for Union; passthrough for the rest; Sort keys add `indirect_sort()` |
| E10 | `Window` rule | `window_args_direct_partition_indirect` | window-fn args -> `direct_window()`; partition-by/order-by -> `indirect_window()` |

For each task:

- [ ] **Step 1**: failing test in matching test file with hand-built plan exercising only that node kind
- [ ] **Step 2**: run test, FAIL
- [ ] **Step 3**: extend `extract::columns::trace_node` with the new match arm, returning the right `ColumnTrace`
- [ ] **Step 4**: run, PASS
- [ ] **Step 5**: commit `feat(lineage): <rule> trace rule`

Driver function in `extract/columns.rs`:

```rust
pub fn trace_plan(plan: &LogicalPlan) -> ColumnTrace {
    use LogicalPlan::*;
    match plan {
        TableScan(ts) => trace_table_scan(ts),
        Projection(p) => trace_projection(p, trace_plan(p.input.as_ref())),
        Filter(f) => {
            let mut t = trace_plan(f.input.as_ref());
            attach_indirect(&mut t, &f.predicate, indirect_filter());
            t
        }
        Aggregate(a) => trace_aggregate(a, trace_plan(a.input.as_ref())),
        Join(j) => trace_join(j),
        Union(u) => trace_union(u),
        Sort(s) => { let mut t = trace_plan(s.input.as_ref()); attach_indirect_for_exprs(&mut t, &s.expr, indirect_sort()); t }
        Window(w) => trace_window(w, trace_plan(w.input.as_ref())),
        Distinct(d) => trace_plan(d.input()),
        Limit(l) => trace_plan(l.input.as_ref()),
        SubqueryAlias(s) => trace_plan(s.input.as_ref()),
        Extension(e) => trace_extension(e, /* default passthrough */),
        _ => vec![],  // unrecognised -> empty trace, conservative
    }
}
```

### Task E11: Extension passthrough + policy MASKED annotation

**Files:**
- Modify: `crates/sqe-lineage/src/extract/columns.rs`

- [ ] **Step 1: Failing test**

```rust
#[test]
fn policy_column_mask_annotates_output_with_masked() {
    // Build a plan where SQE's policy enforcer wrapped a Projection in an Extension
    // node whose output replaces a column with a constant or function call.
    let plan = build_masked_plan("ssn");
    let trace = columns::trace_plan(&plan);
    let ssn_idx = find_column_index(&plan, "ssn");
    assert!(trace[ssn_idx].iter().any(|d| d.transformation.subtype == "MASKED"));
}
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement**

In `trace_extension`: detect the SQE policy-rewriter extension type by name (use `node.name()`); for output columns whose expression replaces an input column ref with anything else, attach `masked()` to those output deps.

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(lineage): MASKED annotation for policy-rewritten outputs"
```

### Task E12: MERGE handling

**Files:**
- Modify: `crates/sqe-lineage/src/extract/columns.rs`

- [ ] **Step 1: Failing test**

```rust
#[test]
fn merge_emits_one_output_with_branch_subtypes() {
    let plan = build_merge_plan(/* WHEN MATCHED UPDATE SET amount = s.amount * 1.1, WHEN NOT MATCHED INSERT */);
    let (_, outputs) = extract_lineage(&plan, &Arc::new(lookup_polaris));
    assert_eq!(outputs.len(), 1);
    let amt_lineage = &outputs[0].outputFacets.columnLineage.as_ref().unwrap().fields["amount"];
    let subtypes: std::collections::BTreeSet<_> = amt_lineage.inputFields.iter()
        .flat_map(|f| f.transformations.iter().map(|t| t.subtype.as_str())).collect();
    assert!(subtypes.contains("MERGE_INSERT") || subtypes.contains("MERGE_UPDATE"));
}
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement**

DataFusion represents MERGE as a `LogicalPlan::Dml` with `op = MergeInto`. Extract per-branch sub-plans; trace each independently; merge the column lineage at the output, annotating each `Transformation` with the branch's subtype.

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(lineage): MERGE branch annotations on column lineage"
```

### Task E13: DDL hint extraction

**Files:**
- Modify: `crates/sqe-lineage/src/extract/mod.rs`

- [ ] **Step 1: Failing test**

```rust
#[test]
fn ddl_hint_yields_output_dataset_with_schema_facet() {
    let hint = LineageHint::DdlSchema {
        catalog: "polaris".into(),
        schema: "sales".into(),
        table: "new_table".into(),
        columns: vec![("id".into(), "long".into())],
    };
    let (inputs, outputs) = extract::extract_from_hint(&hint, &Arc::new(lookup_polaris));
    assert!(inputs.is_empty());
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].name, "sales.new_table");
    let fields = &outputs[0].facets.schema.as_ref().unwrap().fields;
    assert_eq!(fields[0].name, "id");
}
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement** `extract_from_hint` in `extract/mod.rs` matching on `LineageHint::DdlSchema` and producing one `OutputDataset` with the schema facet populated; no column lineage facet (DDL has no source columns).

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(lineage): extract output dataset from DDL hint"
```

### Task E14: Wire trace_plan into extract_lineage

**Files:**
- Modify: `crates/sqe-lineage/src/extract/mod.rs`
- Modify: `crates/sqe-lineage/src/extract/columns.rs`

- [ ] **Step 1: Failing integration test**

```rust
#[test]
fn ctas_yields_inputs_outputs_and_column_lineage() {
    let plan = build_ctas_plan(/* CREATE TABLE archive AS SELECT id, amount * 2 AS doubled FROM polaris.sales.orders */);
    let (inputs, outputs) = extract::extract_lineage(&plan, &Arc::new(lookup_polaris));
    assert_eq!(inputs.len(), 1);
    assert_eq!(outputs.len(), 1);
    let cl = outputs[0].outputFacets.columnLineage.as_ref().unwrap();
    assert!(cl.fields.contains_key("id"));
    assert!(cl.fields.contains_key("doubled"));
    let doubled_subtypes: Vec<&str> = cl.fields["doubled"].inputFields[0].transformations.iter()
        .map(|t| t.subtype.as_str()).collect();
    assert!(doubled_subtypes.contains(&"TRANSFORMATION"));
}
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Wire it up**

In `extract::mod.rs`, replace the stub `extract_lineage`:

```rust
pub fn extract_lineage(plan: &LogicalPlan, lookup: &CatalogLookup) -> (Vec<InputDataset>, Vec<OutputDataset>) {
    let inputs = datasets::extract_inputs(plan, lookup);
    let mut outputs = datasets::extract_outputs(plan, lookup);
    if let Some(out) = outputs.first_mut() {
        // Trace from the root child plan (DML wraps a source plan)
        if let Some(source_plan) = source_of_write(plan) {
            let trace = columns::trace_plan(source_plan);
            let target_fields = output_field_names(plan);
            out.outputFacets.columnLineage = Some(build_column_lineage_facet(&target_fields, &trace, lookup));
        }
    }
    (inputs, outputs)
}

fn build_column_lineage_facet(fields: &[String], trace: &columns::ColumnTrace, lookup: &CatalogLookup) -> ColumnLineageFacet {
    let mut map = std::collections::BTreeMap::new();
    for (i, name) in fields.iter().enumerate() {
        if let Some(deps) = trace.get(i) {
            let inputs: Vec<ColumnLineageInput> = deps.iter().map(|d| ColumnLineageInput {
                namespace: lookup(&d.catalog),
                name: format!("{}.{}", d.schema, d.table),
                field: d.field.clone(),
                transformations: vec![d.transformation.clone()],
            }).collect();
            map.insert(name.clone(), ColumnLineageEntry { inputFields: inputs });
        }
    }
    ColumnLineageFacet { fields: map }
}
```

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(lineage): assemble inputs+outputs+columnLineage facet"
```

---

## Phase F: Configuration

### Task F1: OpenLineageConfig struct

**Files:**
- Modify: `crates/sqe-core/src/config.rs`

- [ ] **Step 1: Failing test**

```rust
#[test]
fn openlineage_config_parses_from_toml() {
    let toml = r#"
        [metrics.openlineage]
        enabled = true
        job_namespace = "sqe-prod"
        emit_selects = true
        file_path = "/var/log/ol.jsonl"
        http_endpoint = "https://marquez.example/api/v1/lineage"
        auth_mode = "bearer"
        api_key = "secret"
        spool_path = "/var/spool/sqe-ol"
        spool_max_bytes = 209715200
    "#;
    let cfg: SqeConfig = toml::from_str(toml).unwrap();
    let ol = &cfg.metrics.openlineage;
    assert!(ol.enabled);
    assert_eq!(ol.job_namespace, "sqe-prod");
    assert_eq!(ol.spool_max_bytes, 209715200);
}
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement**

Append `OpenLineageConfig` to `config.rs`:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenLineageConfig {
    #[serde(default)] pub enabled: bool,
    #[serde(default = "default_job_namespace")] pub job_namespace: String,
    #[serde(default)] pub producer: String,
    #[serde(default)] pub emit_selects: bool,
    #[serde(default)] pub file_path: String,
    #[serde(default)] pub http_endpoint: String,
    #[serde(default = "default_auth_mode")] pub auth_mode: String,
    #[serde(default)] pub api_key: String,
    #[serde(default = "default_http_timeout")] pub http_timeout_ms: u64,
    #[serde(default = "default_http_retry")]    pub http_retry_attempts: u32,
    #[serde(default)] pub spool_path: String,
    #[serde(default = "default_spool_cap")]     pub spool_max_bytes: u64,
    #[serde(default = "default_replay_secs")]   pub replay_interval_secs: u64,
    #[serde(default = "default_channel_cap")]   pub channel_capacity: usize,
}

impl Default for OpenLineageConfig {
    fn default() -> Self { /* all defaults */ }
}

fn default_job_namespace() -> String { "sqe".into() }
fn default_auth_mode() -> String { "none".into() }
fn default_http_timeout() -> u64 { 5000 }
fn default_http_retry() -> u32 { 1 }
fn default_spool_cap() -> u64 { 100 * 1024 * 1024 }
fn default_replay_secs() -> u64 { 30 }
fn default_channel_cap() -> usize { 10_000 }
```

Add `pub openlineage: OpenLineageConfig,` to `MetricsConfig` with `#[serde(default)]`.

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(config): add OpenLineageConfig"
```

### Task F2: Env overrides

- [ ] **Step 1: Failing test**

```rust
#[test]
fn env_overrides_apply_to_openlineage() {
    std::env::set_var("SQE_METRICS__OPENLINEAGE__ENABLED", "true");
    std::env::set_var("SQE_METRICS__OPENLINEAGE__SPOOL_MAX_BYTES", "999");
    let mut cfg = SqeConfig::default();
    cfg.apply_env_overrides();
    assert!(cfg.metrics.openlineage.enabled);
    assert_eq!(cfg.metrics.openlineage.spool_max_bytes, 999);
}
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement** in `apply_env_overrides`:

```rust
env_override_bool ("SQE_METRICS__OPENLINEAGE__ENABLED",        &mut self.metrics.openlineage.enabled);
env_override_str  ("SQE_METRICS__OPENLINEAGE__JOB_NAMESPACE",  &mut self.metrics.openlineage.job_namespace);
env_override_bool ("SQE_METRICS__OPENLINEAGE__EMIT_SELECTS",   &mut self.metrics.openlineage.emit_selects);
env_override_str  ("SQE_METRICS__OPENLINEAGE__FILE_PATH",      &mut self.metrics.openlineage.file_path);
env_override_str  ("SQE_METRICS__OPENLINEAGE__HTTP_ENDPOINT",  &mut self.metrics.openlineage.http_endpoint);
env_override_str  ("SQE_METRICS__OPENLINEAGE__AUTH_MODE",      &mut self.metrics.openlineage.auth_mode);
env_override_str  ("SQE_METRICS__OPENLINEAGE__API_KEY",        &mut self.metrics.openlineage.api_key);
env_override_u64  ("SQE_METRICS__OPENLINEAGE__HTTP_TIMEOUT_MS", &mut self.metrics.openlineage.http_timeout_ms);
env_override_u32  ("SQE_METRICS__OPENLINEAGE__HTTP_RETRY_ATTEMPTS", &mut self.metrics.openlineage.http_retry_attempts);
env_override_str  ("SQE_METRICS__OPENLINEAGE__SPOOL_PATH",     &mut self.metrics.openlineage.spool_path);
env_override_u64  ("SQE_METRICS__OPENLINEAGE__SPOOL_MAX_BYTES", &mut self.metrics.openlineage.spool_max_bytes);
env_override_u64  ("SQE_METRICS__OPENLINEAGE__REPLAY_INTERVAL_SECS", &mut self.metrics.openlineage.replay_interval_secs);
env_override_usize("SQE_METRICS__OPENLINEAGE__CHANNEL_CAPACITY", &mut self.metrics.openlineage.channel_capacity);
```

If `env_override_u64` / `env_override_usize` helpers don't exist, add them mirroring `env_override_u16`.

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(config): env overrides for OpenLineageConfig"
```

### Task F3: validate()

- [ ] **Step 1: Failing test**

```rust
#[test]
fn validate_rejects_enabled_without_sinks() {
    let mut cfg = OpenLineageConfig::default();
    cfg.enabled = true;
    let err = cfg.validate().unwrap_err();
    assert!(err.contains("at least one of file_path or http_endpoint"));
}

#[test]
fn validate_rejects_bearer_without_api_key() { /* ... */ }

#[test]
fn validate_rejects_spool_without_http() { /* ... */ }

#[test]
fn validate_rejects_tiny_spool_cap() { /* spool_max_bytes < 1 MiB */ }
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement** `OpenLineageConfig::validate(&self) -> Result<(), String>` checking each rule from spec §8.3.

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(config): OpenLineageConfig validation"
```

---

## Phase G: Coordinator integration

### Task G1: Plumb LineageObserver into QueryHandler

**Files:**
- Modify: `crates/sqe-coordinator/Cargo.toml` (add `sqe-lineage` dep)
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

- [ ] **Step 1: Add field + ctor argument**

In `QueryHandler` struct add `lineage: Option<Arc<dyn LineageObserver>>,`. Update `QueryHandler::new` to accept it as the trailing argument.

- [ ] **Step 2: Build and let other call sites complain**

Run: `cargo build -p sqe-coordinator`
Expected: failures at `QueryHandler::new` call sites (main.rs, sqe_server.rs, tests).

- [ ] **Step 3: Pass `None` at every existing call site to keep tests green**

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-coordinator`
Expected: all existing tests still PASS (lineage=None has no behavioural effect yet).

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/
git commit -m "refactor(coordinator): plumb optional LineageObserver through QueryHandler"
```

### Task G2: should_emit helper + skip cases

- [ ] **Step 1: Failing test**

```rust
#[test]
fn should_emit_skips_select_when_emit_selects_is_false() { /* ... */ }
#[test]
fn should_emit_always_skips_maintenance() { /* OPTIMIZE, VACUUM */ }
#[test]
fn should_emit_emits_dml_writes() { /* INSERT, CTAS, MERGE */ }
```

- [ ] **Step 2: FAIL**

- [ ] **Step 3: Implement** a free function `should_emit(kind: &StatementKind, cfg: &OpenLineageConfig) -> bool` in `query_handler.rs`. Maintenance kinds enumerated as a small const slice.

- [ ] **Step 4: PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(coordinator): should_emit lineage decision"
```

### Task G3: Emit START + COMPLETE/FAIL hooks in execute_statement

- [ ] **Step 1: Add hook calls**

Inside `execute_statement`, around the dispatch:

```rust
let started_at = chrono::Utc::now();
if let Some(obs) = &self.lineage {
    if should_emit(&kind, &self.config.metrics.openlineage) {
        obs.on_query_start(QueryStartCtx {
            run_id: query_id,
            job_namespace: self.config.metrics.openlineage.job_namespace.clone(),
            sql: sql.to_string(),
            user: UserCtx { username: session.user.username.clone(), bearer: session.user.bearer.clone() },
            session_id: session.id.clone(),
            started_at,
            statement_kind: kind_name.clone(),
        });
    }
}

let mut captured_plan: Option<PlanOrHint> = None;
let result = /* existing dispatch, threading &mut captured_plan into execute_query and DDL handlers */;

if let Some(obs) = &self.lineage {
    if should_emit(&kind, &self.config.metrics.openlineage) {
        let ended_at = chrono::Utc::now();
        let duration = ended_at.signed_duration_since(started_at).to_std().unwrap_or_default();
        match &result {
            Ok(rows) => obs.on_query_complete(QueryCompleteCtx {
                run_id: query_id, job_namespace: ..., sql: sql.to_string(), user: ...,
                session_id: ..., started_at, ended_at, duration, statement_kind: kind_name,
                rows_returned: rows.iter().map(|b| b.num_rows()).sum(),
                plan: captured_plan,
            }),
            Err(e) => obs.on_query_fail(QueryFailCtx { ..., error_message: e.to_string(), plan: captured_plan }),
        }
    }
}
```

- [ ] **Step 2: Thread `captured_plan` out of `execute_query`**

Change `execute_query` signature to accept `&mut Option<PlanOrHint>` and assign `*plan_out = Some(PlanOrHint::Plan(Box::new(enforced_plan.clone())))` after policy enforcement (around current line 1172 in `query_handler.rs`). Note: `PlanOrHint::Plan` carries a `Box<LogicalPlan>` to satisfy `clippy::large_enum_variant` (decided in Task C1).

For DDL handlers (`handle_drop`, `handle_create_view`, etc.) build `LineageHint::DdlSchema { ... }` and assign the same way.

- [ ] **Step 3: Build and run existing tests**

Run: `cargo test -p sqe-coordinator`
Expected: all existing tests still PASS (no observer wired in test config -> no behavioural change).

- [ ] **Step 4: Commit**

```bash
git commit -am "feat(coordinator): emit OL events around execute_statement"
```

---

## Phase H: Server / embedded wiring

### Task H1: Construct observer from config in `bin/sqe_server.rs`

- [ ] **Step 1: Add construction code**

Around the existing `AuditLogger::new(...)` block:

```rust
let lineage_obs: Option<Arc<dyn sqe_lineage::LineageObserver>> = if config.metrics.openlineage.enabled {
    config.metrics.openlineage.validate()
        .map_err(|e| format!("openlineage config: {e}"))?;

    let mut sinks: Vec<Arc<dyn sqe_lineage::Sink>> = vec![];
    if !config.metrics.openlineage.file_path.is_empty() {
        sinks.push(Arc::new(sqe_lineage::sinks::file::FileSink::new(&config.metrics.openlineage.file_path)?));
    }
    if !config.metrics.openlineage.http_endpoint.is_empty() {
        let http = sqe_lineage::sinks::http::HttpSink::new(/* HttpConfig from cfg */)?;
        let sink: Arc<dyn sqe_lineage::Sink> = if !config.metrics.openlineage.spool_path.is_empty() {
            Arc::new(sqe_lineage::sinks::spool::SpoolSink::wrap(http, /* SpoolConfig */))
        } else {
            Arc::new(http)
        };
        sinks.push(sink);
    }
    let multi = Arc::new(sqe_lineage::MultiSink::new(sinks));
    let (tx, rx) = tokio::sync::mpsc::channel(config.metrics.openlineage.channel_capacity);
    let drop_counter = /* register prometheus IntCounter sqe_lineage_dropped_events_total{reason="channel_full"} */;
    let cfg = Arc::new(sqe_lineage::emitter::EmitterConfig {
        job_namespace: config.metrics.openlineage.job_namespace.clone(),
        producer: if config.metrics.openlineage.producer.is_empty() {
            format!("https://github.com/sbp/sqe/v{}", env!("CARGO_PKG_VERSION"))
        } else {
            config.metrics.openlineage.producer.clone()
        },
        catalog_lookup: build_catalog_lookup(&config),
    });
    sqe_lineage::emitter::spawn_emitter(rx, multi, cfg);
    Some(Arc::new(sqe_lineage::ChannelObserver::new(tx, drop_counter)))
} else {
    None
};

// Pass `lineage_obs` into QueryHandler::new(...)
```

`build_catalog_lookup(config)` returns an `Arc<dyn Fn(&str) -> String + Send + Sync>` that maps catalog name -> REST URL via `flattened_catalogs()`, with `format!("sqe://{name}")` fallback.

- [ ] **Step 2: Run integration smoke**

Run: `cargo build -p sqe-coordinator --bin sqe_server`
Expected: success.

Run: `cargo test -p sqe-coordinator`
Expected: all existing tests pass; new OL config validation never trips because tests use `enabled = false`.

- [ ] **Step 3: Commit**

```bash
git commit -am "feat(server): construct LineageObserver from config"
```

### Task H2: Plumb observer into `embedded.rs`

**Status: deferred (no-op for v1).**

The `EmbeddedClient` in `crates/sqe-cli/src/embedded.rs` does not construct a
`QueryHandler`. It builds a raw `SessionContext` directly and dispatches SQL
through DataFusion's `ctx.sql(...)` path, bypassing the coordinator pipeline
where `should_emit` and the observer hooks live. The file's own header comment
(lines 26-34) explains the duplication is intentional: plumbing `SqeConfig`,
`PolicyStore`, `QueryTracker`, and `MetricsRegistry` through to the embedded
path would bloat the cluster path for the embedded use case.

Wiring an observer here would require either (a) refactoring `EmbeddedClient`
to go through `QueryHandler`, which is out of scope for the OL emitter
project, or (b) a parallel emit path that intercepts `ctx.sql(...)` calls
and replays the LogicalPlan through the extractor. Both are larger pieces of
work than v1 budgets for. The CLI is also a single-user, ad-hoc tool where
lineage is less load-bearing than in the multi-user coordinator.

When the embedded path needs lineage, the cleanest option is path (a): unify
on `QueryHandler` and accept the resulting plumbing. Tracking is left to a
follow-up issue rather than an in-place stub here, because adding an unused
parameter to `EmbeddedClient::new(...)` would imply support that doesn't
exist.

- [x] **Step 1: Determined H2 is a no-op for v1; documented the deferral here.**

---

## Phase I: Coordinator integration tests

### Task I1: lineage_emit_test.rs

**Files:**
- Create: `crates/sqe-coordinator/tests/lineage_emit_test.rs`

- [ ] **Step 1: Write the test**

Skeleton:

```rust
//! End-to-end OL emission against a wiremock collector.
//! Requires the existing test fixtures used by other coordinator integration tests.

use wiremock::{matchers::*, Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn ctas_emits_start_and_complete_with_column_lineage() {
    let collector = MockServer::start().await;
    Mock::given(method("POST")).and(path("/api/v1/lineage"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)  // START + COMPLETE
        .mount(&collector).await;

    let server = start_test_coordinator_with_ol(&collector.uri()).await;
    server.execute("CREATE TABLE archive AS SELECT id FROM polaris.sales.orders").await.unwrap();

    // Wait for emitter task to drain
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let received: Vec<_> = collector.received_requests().await.unwrap();
    assert_eq!(received.len(), 2);
    let start: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(start["eventType"], "START");
    let complete: serde_json::Value = serde_json::from_slice(&received[1].body).unwrap();
    assert_eq!(complete["eventType"], "COMPLETE");
    assert!(complete["outputs"][0]["outputFacets"]["columnLineage"]["fields"]["id"].is_object());
}

#[tokio::test]
async fn select_with_emit_selects_off_emits_nothing() { /* ... */ }
#[tokio::test]
async fn select_with_emit_selects_on_emits_no_outputs() { /* ... */ }
#[tokio::test]
async fn ddl_create_table_emits_with_schema_facet() { /* ... */ }
#[tokio::test]
async fn merge_emits_one_output_dataset() { /* ... */ }
```

- [ ] **Step 2: Run, fix until PASS**

- [ ] **Step 3: Commit**

```bash
git commit -am "test(coordinator): OL emission integration tests"
```

### Task I2: lineage_disabled_test.rs

- [ ] **Step 1: Test that no events emit when `enabled = false`**
- [ ] **Step 2: Run, PASS**
- [ ] **Step 3: Commit**

### Task I3: lineage_failure_test.rs

- [ ] **Step 1: Test collector returns 500 -> spool grows; collector recovers + replay drains spool; verify metric counters**
- [ ] **Step 2: Run, PASS**
- [ ] **Step 3: Commit**

### Task I4: lineage_session_parent_test.rs

- [ ] **Step 1: Test three queries in one session share `parent.run.runId`**
- [ ] **Step 2: Run, PASS**
- [ ] **Step 3: Commit**

---

## Phase J: Documentation

### Task J1: Operator chapter in mdBook

**Files:**
- Create: `docs/book/src/operations/openlineage.md`
- Modify: `docs/book/src/SUMMARY.md`

- [ ] **Step 1: Write the chapter**

Cover (per spec §10.3):
- What gets emitted (event matrix)
- Config reference (TOML + env vars)
- Sink choice guidance (when to pick file vs http vs both)
- Marquez quickstart
- DataHub quickstart
- Troubleshooting (spool growing, dropped events metric, validation errors)
- v1 limitations

Voice rules: no emdash/endash/Unicode arrows, no AI-tells. Run the same `grep -nE '(—|–|→)'` check before committing.

- [ ] **Step 2: Add to SUMMARY.md**

Add `- [Lineage (OpenLineage)](./operations/openlineage.md)` under the existing Operations section (find the right parent in current SUMMARY.md).

- [ ] **Step 3: Build the book**

Run: `cd docs/book && mdbook build`
Expected: success, no warnings.

- [ ] **Step 4: Commit**

```bash
git add docs/book/src/operations/openlineage.md docs/book/src/SUMMARY.md
git commit -m "docs(book): add OpenLineage operations chapter"
```

### Task J2: Update roadmap files

**Files:**
- Modify: `docs/book/src/development/roadmap.md`
- Modify: `docs/roadmap.md`
- Modify: `docs/superpowers/plans/2026-04-24-iceberg-matrix-parity.md` (line ~874)
- Modify: `nextsteps.md`
- Modify: `README.md` (if it lists OL in roadmap)
- Modify: `docs/datafusion-architecture.md` (add `sqe-lineage` row)

- [ ] **Step 1: Strike OL from "deferred" lists, add to "shipped"**
- [ ] **Step 2: Run check command from CLAUDE.md**

Run: `grep -rn '—' docs/ebook/chapters/ docs/blog/`
Expected: zero hits.

- [ ] **Step 3: Commit**

```bash
git commit -am "docs: roadmap + architecture updates for OpenLineage"
```

### Task J3: Ebook chapter (after Phase I integration tests pass)

**Files:**
- Create: `docs/ebook/chapters/16e-the-lineage-trail.md`

- [ ] **Step 1: Outline the chapter**

Voice rules per `docs/ebook/voice.md` and CLAUDE.md "Writing Style". Forbidden: emdash, endash, Unicode arrows, AI-tells (delve, leverage, utilize, comprehensive, robust, etc.). Required: short sentences for weight, alternating rhythm, direct opinionated voice.

Suggested arc (mirrors 16d's structure):
1. **The user request that started it.** The moment a user (or anticipated user) asked for lineage, and what we already had vs what we needed.
2. **What OpenLineage actually is.** Why we picked OL over a homegrown schema.
3. **Three honest scope decisions.** Column-level lineage by default, SELECT optional, multi-catalog dataset URIs.
4. **The plan-walking surprise.** What was easier than expected, what was harder. Honest about the MERGE handling and policy-mask annotation.
5. **The disk spool we didn't want to write.** Why we wrote it anyway.
6. **What we still don't ship.** Heartbeats, mTLS, deeper correlated subqueries. Why those are fine to defer.

- [ ] **Step 2: Write the chapter (after implementation lands)**

Length: 2000-4000 words, matching 16b/16c/16d. Use specifics from the actual implementation, not pre-implementation guesses.

- [ ] **Step 3: Run voice checks**

Run: `grep -nE '(—|–|→)' docs/ebook/chapters/16e-the-lineage-trail.md`
Expected: zero hits.

Run: `grep -nE '\b(delve|leverage|utilize|facilitate|comprehensive|robust|cutting-edge|game-changer|paradigm shift|synergy)\b' docs/ebook/chapters/16e-the-lineage-trail.md`
Expected: zero hits.

- [ ] **Step 4: Commit**

```bash
git commit -am "docs(ebook): add chapter 16e - The Lineage Trail"
```

---

## Phase K: Final

### Task K1: Run full test suite + clippy

- [ ] **Step 1**: `cargo test --all`
- [ ] **Step 2**: `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] **Step 3**: `scripts/integration-test.sh` (requires Polaris quickstart stack)
- [ ] **Step 4**: `cargo audit`

### Task K2: Push branch + open PR

- [ ] **Step 1**: `git push -u origin feat/openlineage-emitter`
- [ ] **Step 2**: `gh pr create --title "feat: OpenLineage emitter" --body "$(cat <<'EOF'
## Summary
- Coordinator-side OL 2-0-2 emitter with column-level lineage
- File + HTTP sinks; HTTP has bounded disk-spool fallback
- Off by default, zero hot-path overhead when disabled
- Multi-catalog aware dataset URIs

## Test plan
- [ ] All unit tests pass (`cargo test --all`)
- [ ] All coordinator integration tests pass (lineage_emit, lineage_disabled, lineage_failure, lineage_session_parent)
- [ ] mdBook builds cleanly with new chapter
- [ ] Verified end-to-end against a Marquez instance
- [ ] No emdash/endash/arrow in any docs (CLAUDE.md voice check)
EOF
)"`

---

## Self-review notes

- **Spec coverage**: every spec section maps to at least one task. §3 (architecture) -> A1; §4 (event schema) -> B1, B2; §5 (column extractor) -> E1-E14; §6 (sinks) -> D1-D4; §7 (lifecycle hooks) -> G1-G3; §8 (config) -> F1-F3; §9 (testing) -> tests interleaved + I1-I4; §10 (file layout + docs) -> J1-J3.
- **Placeholder scan**: no TBD/TODO/FIXME except the explicit "Filled in Phase E" stubs in extractor that are wired up in E14, and the `/* ... */` markers in test scaffolds that the implementer must fill from spec context.
- **Type consistency**: `LineageObserver`, `ChannelObserver`, `QueryStartCtx`, `QueryCompleteCtx`, `QueryFailCtx`, `Sink`, `MultiSink`, `FileSink`, `HttpSink`, `SpoolSink`, `RunEvent`, `Job`, `Run`, `EventType`, `ColumnTrace`, `ColumnDep`, `Transformation`, `LineageHint`, `PlanOrHint`, `OpenLineageConfig`, `EmitterConfig`, `HttpConfig`, `SpoolConfig`, `AuthMode`. All defined in early tasks, used consistently in later tasks.
- **Open question**: `AuthMode::UserToken` carries the user bearer per-event. Implementation decision in Task D3 step 3 explanation (clone config vs fn closure) gets settled at implementation time. Either approach satisfies the spec.
