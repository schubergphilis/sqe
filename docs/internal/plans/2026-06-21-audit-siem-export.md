# Audit SIEM Export (OTLP logs + durable spool) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the complete OCSF audit stream to an OTLP logs endpoint (collector -> SIEM) with at-least-once delivery and a durable local spool, after first making every audited activity a canonical OCSF event.

**Architecture:** Phase B1 promotes the remaining emit sites (Flight streaming SELECT, DML/DDL, maintenance) from the legacy `log(&AuditEntry)` path to canonical `AuditEvent` via `log_event`, so the OCSF stream is complete. Phase B2 treats the OCSF JSONL file as a durable spool and adds a background `OtlpLogShipper` task that tails it from a persisted `seq` cursor, maps each record to an OTel `LogRecord`, exports through a dedicated OTLP `LogExporter`, and advances the cursor only on a successful export ack.

**Tech Stack:** Rust, `opentelemetry` / `opentelemetry_sdk` / `opentelemetry-otlp` 0.31 (grpc-tonic, logs), `tokio`, `serde_json`, the existing `sqe-metrics` audit module and `otel.rs` wiring.

## Global Constraints

- Spec: `docs/internal/specs/2026-06-21-audit-siem-export-design.md`.
- Sub-project B only. Do NOT build spool rotation/retention (only a bounded-growth WARN), a Kafka target (reserved config value), rich/full OTLP attributes, or sub-project C (op-logging polish, gating `/api/v1/queries`).
- Export is opt-in. `[metrics.audit_export] enabled = false` (default) MUST produce behavior identical to A.
- Delivery is at-least-once: the cursor advances only after a successful `LogExporter::export` ack (`OTelSdkResult` == `Ok`). Duplicates across a crash/replay boundary are acceptable; loss is not.
- The audit query path MUST NEVER block on export. Export runs on a background task; a down collector grows the spool, it does not slow queries.
- Audit export MUST NOT route through the `OpenTelemetryTracingBridge` / tracing `EnvFilter` (records could be sampled or filtered out). Use a dedicated `LogExporter`.
- Cursor is on `integrity.seq` (monotonic), not byte offset.
- The tailer reads only complete lines (never past the last newline).
- B1 MUST NOT change `query_cache` behavior: cache invalidation keeps its own `extract_table_names`; audit emission independently uses `resources_from_plan`.
- Secret-bearing statements (CREATE/DROP/SHOW SECRET) stay on the redacted legacy `log()` path.
- OCSF UIDs (from A): Query 6005/cat6, Auth 3002/cat3, Session 3003/cat3, Grant 3001/cat3, AdminDdl 3004/cat3.
- No emdash, endash, or unicode arrows in code or docs (use `->`). Match Jacob's writing style in docs.
- `cargo clippy --all-targets --all-features -- -D warnings` clean before each commit.
- Existing tests keep passing, especially `crates/sqe-coordinator/tests/it/audit_e2e_test.rs` and `crates/sqe-coordinator/src/streaming.rs` tests.

---

## File Structure

- `crates/sqe-metrics/src/audit/ocsf.rs` (modify): add `integrity.seq` to `to_ocsf` output.
- `crates/sqe-coordinator/src/streaming.rs` (modify): `StreamFinalizer` carries an `Actor` + `Vec<Resource>`; the 3 emit points build a canonical `AuditEvent` and call `log_event`.
- `crates/sqe-coordinator/src/maintenance.rs` (modify) and the DML/DDL emit path in `query_handler.rs` (modify): emit canonical `AuditEvent` (`AdminDdl`/`Query`) via `log_event`; secrets stay legacy.
- `crates/sqe-core/src/config.rs` (modify): add `AuditExportConfig` + `MetricsConfig.audit_export` + env overrides.
- `crates/sqe-metrics/src/audit/export/mod.rs` (new): export module root + `pub use`.
- `crates/sqe-metrics/src/audit/export/cursor.rs` (new): `SeqCursor` (load/store/fsync).
- `crates/sqe-metrics/src/audit/export/record.rs` (new): `ocsf_to_log_record` mapping + `LogShipExporter` trait (injection seam).
- `crates/sqe-metrics/src/audit/export/shipper.rs` (new): `OtlpLogShipper` (tail spool -> batch -> export -> advance cursor).
- `crates/sqe-metrics/src/audit/logger.rs` (modify): provision the OCSF spool file when export is enabled (`with_export_spool`).
- `crates/sqe-metrics/src/otel.rs` (modify) or `export/shipper.rs`: construct the dedicated OTLP `LogExporter` for audit.
- `crates/sqe-coordinator/src/main.rs` and `crates/sqe-coordinator/src/bin/sqe_server.rs` (modify): spawn the shipper when enabled.
- `crates/sqe-metrics/src/lib.rs` (modify): register export metrics.
- `docs/site/book/src/operations/audit-logging.md` (modify): export section. `docs/internal/roadmap-tracker.md` (modify): mark B done.

---

## Phase B1: Complete canonical emit coverage

### Task 1: Emit `integrity.seq` in the OCSF output

**Files:**
- Modify: `crates/sqe-metrics/src/audit/ocsf.rs`
- Test: inline in `ocsf.rs`

**Interfaces:**
- Consumes: `AuditEvent.integrity.seq: u64` (exists from A).
- Produces: `to_ocsf(event)["metadata"]["uid"]` stays the hash; adds `to_ocsf(event)["metadata"]["sequence"] = event.integrity.seq`. (OCSF `metadata.sequence` is a real field for ordering, so this is a clean mapping rather than `unmapped`.)

- [ ] **Step 1: Write the failing test.**

```rust
#[test]
fn ocsf_carries_integrity_seq_in_metadata_sequence() {
    use crate::audit::*;
    let mut ev = sample_query_event();
    ev.integrity.seq = 42;
    let v = to_ocsf(&ev);
    assert_eq!(v["metadata"]["sequence"], 42);
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p sqe-metrics ocsf_carries_integrity_seq`
Expected: FAIL (no `sequence` field).

- [ ] **Step 3: Implement.** In `to_ocsf`, in the `metadata` object construction, add the sequence next to `uid`:

```rust
"metadata": {
    "product": { "name": "SQE", "vendor_name": "SQE" },
    "version": "1.3.0",
    "uid": event.integrity.hash,
    "sequence": event.integrity.seq,
},
```

- [ ] **Step 4: Run to verify pass.**

Run: `cargo test -p sqe-metrics ocsf`
Expected: PASS.

- [ ] **Step 5: Clippy and commit.**

```bash
cargo clippy -p sqe-metrics --all-targets -- -D warnings
git add crates/sqe-metrics/src/audit/ocsf.rs
git commit -m "feat(audit): emit integrity.seq as OCSF metadata.sequence (export cursor key)"
```

### Task 2: Migrate streaming SELECT emit to canonical `AuditEvent`

**Files:**
- Modify: `crates/sqe-coordinator/src/streaming.rs` (the `StreamFinalizer` struct, its construction in `query_handler.rs`, and the 3 emit points at the success/error/drop branches)
- Test: inline in `streaming.rs`, plus `crates/sqe-coordinator/tests/it/audit_e2e_test.rs`

**Interfaces:**
- Consumes: `sqe_metrics::audit::{AuditEvent, AuditKind, Actor, Resource, Outcome, QueryInfo, Timing, QueryStats, PolicyAudit}`, `AuditLogger::log_event`, `Actor::from_parts`, `crate::audit_resources::resources_from_plan`.
- Produces: `StreamFinalizer` gains `pub actor: Actor` and `pub resources: Vec<Resource>` fields; the 3 emit sites call `audit.log_event(AuditEvent { kind: AuditKind::Query, .. })`.

Current code (streaming.rs): `StreamFinalizer` holds `audit: Option<Arc<AuditLogger>>`, `sql: String`, `tables_touched: Vec<String>`, a `policy_summary`, plus username/session/client_ip used to build the `AuditEntry` at lines ~215, ~278, ~322. The 3 branches build `AuditEntry { username, session_id, query_hash, query_text, statement_type, duration_ms, rows_returned, status, client_ip, tables_touched, + policy fields }` and call `audit.log(&entry)`.

- [ ] **Step 1: Write the failing e2e test** in `audit_e2e_test.rs` (follow the existing harness; a streaming SELECT path). Drive a streaming SELECT through the finalizer success path with a real `AuditLogger`, flush, read the JSONL, and assert the line is a canonical `AuditEvent`:

```rust
#[tokio::test]
async fn streaming_select_emits_canonical_query_event() {
    // Build a StreamFinalizer wired to a tempfile AuditLogger, an Actor with a
    // username, and resources for the scanned table; run the success finalizer.
    // (Follow the existing audit_e2e_test fixtures + the streaming.rs test
    //  `audit_entry_records_policy_summary_on_streaming_success` for setup.)
    // After flush, parse the single line as JSON and assert:
    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(v["kind"], "query");
    assert_eq!(v["actor"]["username"], "auditor");
    assert!(v["resources"].is_array());
    assert!(v.get("statement_type").is_none(), "must be AuditEvent, not flat AuditEntry");
    assert!(v["integrity"]["seq"].is_u64());
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p sqe-coordinator streaming_select_emits_canonical_query_event`
Expected: FAIL (currently emits flat `AuditEntry` with top-level `statement_type`).

- [ ] **Step 3: Extend `StreamFinalizer`.** Add `pub actor: sqe_metrics::audit::Actor` and `pub resources: Vec<sqe_metrics::audit::Resource>`. At the construction site in `query_handler.rs` (the streaming path), set `actor: Actor::from_parts(session.user.username.clone(), session.user.subject.clone(), session.user.email.clone(), session.user.roles.clone(), session.user.groups.clone())` and `resources: resources_from_plan(&plan, default_catalog)` (the plan + session are in scope there, same as A Task 11/12). Keep `tables_touched` for `query_cache` (unchanged).

- [ ] **Step 4: Replace the 3 emit points.** At each branch (success ~215, error ~278, client-drop ~322), replace the `AuditEntry`/`log` with:

```rust
if let Some(ref audit) = self.audit {
    let outcome = if is_error {
        sqe_metrics::audit::Outcome::Failure {
            error_type: self.error_type.clone(),
            error_code: self.error_code.clone(),
            message: self.error_message.clone(),
        }
    } else {
        sqe_metrics::audit::Outcome::Success
    };
    audit.log_event(sqe_metrics::audit::AuditEvent {
        time: chrono::Utc::now(),
        kind: sqe_metrics::audit::AuditKind::Query,
        actor: self.actor.clone(),
        outcome,
        resources: self.resources.clone(),
        policy: Some(self.policy_summary_to_audit()),
        timing: Some(sqe_metrics::audit::Timing { duration_ms: elapsed_ms, ..Default::default() }),
        stats: Some(sqe_metrics::audit::QueryStats { rows_returned, ..Default::default() }),
        query: Some(sqe_metrics::audit::QueryInfo {
            text: Some(self.sql.clone()),
            query_hash: sqe_metrics::audit::query_hash(&self.sql),
            statement_type: "query".to_string(),
        }),
        session_id: self.session_id.clone(),
        client_ip: self.client_ip.clone(),
        integrity: Default::default(),
    });
}
```

`policy_summary_to_audit()` maps the existing `policy_summary` to `PolicyAudit { row_filters_applied, columns_masked, columns_restricted, denied }` (the same fields the old `AuditEntry` set). Use whichever of `is_error`/`error_*` fields the branch already has; the success branch uses `Outcome::Success`. Do NOT pre-redact `self.sql` (the worker thread redacts; A's Task 11 fix made redaction unconditional on `log_event`).

- [ ] **Step 5: Run tests.**

Run: `cargo test -p sqe-coordinator streaming`
Expected: PASS, including the existing `audit_entry_records_policy_summary_on_streaming_success` (update that test if it asserted flat `AuditEntry` field names: change to canonical `policy.row_filters_applied` etc. and note the change).

- [ ] **Step 6: Clippy and commit.**

```bash
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src/streaming.rs crates/sqe-coordinator/src/query_handler.rs crates/sqe-coordinator/tests/it/audit_e2e_test.rs
git commit -m "feat(audit): emit canonical Query AuditEvent on the streaming path"
```

### Task 3: Migrate DML/DDL and maintenance emits to canonical `AuditEvent`

**Files:**
- Modify: `crates/sqe-coordinator/src/maintenance.rs` (~line 336 denial emit), and the DML/DDL emit branch in `crates/sqe-coordinator/src/query_handler.rs` (the non-Query, non-secret legacy `log()` calls)
- Test: `crates/sqe-coordinator/tests/it/audit_e2e_test.rs`

**Interfaces:**
- Consumes: `AuditLogger::log_event`, `AuditKind::{AdminDdl, Query}`, `Actor::from_parts`, `Outcome`.

- [ ] **Step 1: Write failing e2e assertions.** A CREATE/DROP (DDL) statement produces a canonical `AuditEvent` with `kind == "admin_ddl"` and an actor; a non-procedure DML produces `kind == "query"`; the maintenance denial path produces a canonical event with `Outcome::Failure`. A CREATE SECRET statement STILL produces a redacted legacy entry (token absent) - this guards that secrets stay on the legacy path.

```rust
#[tokio::test]
async fn ddl_emits_canonical_admin_ddl_event() { /* drive a CREATE/DROP, assert kind == "admin_ddl" + actor present */ }

#[tokio::test]
async fn create_secret_stays_redacted_legacy() { /* CREATE SECRET ... TOKEN '...'; assert token absent AND it is the legacy flat shape (statement_type present) */ }
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p sqe-coordinator ddl_emits_canonical_admin_ddl_event`
Expected: FAIL (DDL currently legacy).

- [ ] **Step 3: Implement.** In `query_handler.rs`, the audit dispatch already branches `StatementKind::Query(_)` (A Task 11) and `Grant/Revoke` (A Task 14) to `log_event`, with an `else` that calls `log(&AuditEntry)`. Split the `else`: secret-bearing kinds (CreateSecret/DropSecret/ShowSecrets and Attach/Detach if they carry secrets) stay on `log()`; all other admin/DDL/DML kinds build an `AuditEvent` (`AdminDdl` for catalog/DDL, `Query` for DML) via `log_event`, with `actor` from `session.user`, `resources` from `resources_from_plan` where a plan exists (else empty), `query` with the SQL + statement_type. In `maintenance.rs` line ~336, replace the denial `AuditEntry` with an `AuditEvent { kind: AdminDdl, outcome: Failure { error_type: Some("AdminGateDenied"), .. }, actor, .. }` via `log_event` (the maintenance handler has the session/identity to build the actor; if only a username is available, use `Actor::from_parts(username, None, None, vec![], vec![])`).

- [ ] **Step 4: Run tests.**

Run: `cargo test -p sqe-coordinator audit`
Expected: PASS, including the existing secret-redaction tests (unchanged behavior for secrets).

- [ ] **Step 5: Clippy and commit.**

```bash
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src/maintenance.rs crates/sqe-coordinator/src/query_handler.rs crates/sqe-coordinator/tests/it/audit_e2e_test.rs
git commit -m "feat(audit): emit canonical AdminDdl/Query events for DML/DDL and maintenance"
```

---

## Phase B2: OTLP export shipper

### Task 4: `AuditExportConfig`

**Files:**
- Modify: `crates/sqe-core/src/config.rs`
- Test: inline in `config.rs`

**Interfaces:**
- Produces: `pub struct AuditExportConfig { enabled: bool, target: String, otlp_endpoint: String, spool_path: String, batch_max: usize, flush_interval_ms: u64, max_spool_bytes: u64, start_at: String }` and `MetricsConfig.audit_export: AuditExportConfig`.

- [ ] **Step 1: Write the failing defaults test.**

```rust
#[test]
fn audit_export_config_defaults() {
    let c = AuditExportConfig::default();
    assert!(!c.enabled);
    assert_eq!(c.target, "otlp");
    assert_eq!(c.batch_max, 512);
    assert_eq!(c.flush_interval_ms, 2000);
    assert_eq!(c.start_at, "now");
    assert_eq!(c.max_spool_bytes, 1_073_741_824);
}
```

- [ ] **Step 2: Run, verify fail, then implement** the struct with serde defaults mirroring the spec, add `#[serde(default)] pub audit_export: AuditExportConfig` to `MetricsConfig` and its `Default`, and env overrides `SQE_METRICS__AUDIT_EXPORT__ENABLED`, `SQE_METRICS__AUDIT_EXPORT__OTLP_ENDPOINT`, `SQE_METRICS__AUDIT_EXPORT__SPOOL_PATH` next to the existing audit env overrides.

```rust
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuditExportConfig {
    #[serde(default)] pub enabled: bool,
    #[serde(default = "default_export_target")] pub target: String,
    #[serde(default)] pub otlp_endpoint: String,
    #[serde(default)] pub spool_path: String,
    #[serde(default = "default_export_batch_max")] pub batch_max: usize,
    #[serde(default = "default_export_flush_ms")] pub flush_interval_ms: u64,
    #[serde(default = "default_export_max_spool")] pub max_spool_bytes: u64,
    #[serde(default = "default_export_start_at")] pub start_at: String,
}
fn default_export_target() -> String { "otlp".into() }
fn default_export_batch_max() -> usize { 512 }
fn default_export_flush_ms() -> u64 { 2000 }
fn default_export_max_spool() -> u64 { 1_073_741_824 }
fn default_export_start_at() -> String { "now".into() }
impl Default for AuditExportConfig {
    fn default() -> Self { Self { enabled: false, target: default_export_target(), otlp_endpoint: String::new(), spool_path: String::new(), batch_max: default_export_batch_max(), flush_interval_ms: default_export_flush_ms(), max_spool_bytes: default_export_max_spool(), start_at: default_export_start_at() } }
}
```

- [ ] **Step 3: Run, clippy, commit.**

```bash
cargo test -p sqe-core audit_export_config_defaults
cargo clippy -p sqe-core --all-targets -- -D warnings
git add crates/sqe-core/src/config.rs
git commit -m "feat(audit-export): AuditExportConfig with back-compatible defaults"
```

### Task 5: `SeqCursor` (durable last-acked seq)

**Files:**
- Create: `crates/sqe-metrics/src/audit/export/mod.rs`, `crates/sqe-metrics/src/audit/export/cursor.rs`
- Modify: `crates/sqe-metrics/src/audit/mod.rs` (`mod export; pub use export::...`)
- Test: inline in `cursor.rs`

**Interfaces:**
- Produces: `pub struct SeqCursor { path: PathBuf, last: u64 }` with `pub fn load(path: PathBuf, start_at_beginning: bool) -> Self` (missing/corrupt -> 0 if beginning else u64::MAX sentinel meaning "from now"; see below), `pub fn last(&self) -> u64`, `pub fn advance_to(&mut self, seq: u64) -> std::io::Result<()>` (writes + fsyncs).

Semantics: the cursor stores "the highest seq already shipped". `last()` returns it. A record with `seq <= last()` is skipped. For `start_at = "now"` with no existing cursor, the shipper sets the cursor to the current spool tail's highest seq at startup (see Task 8) so history is not backfilled; `load` for a missing file returns `last = 0` and a `fresh: bool` flag the shipper uses to decide the from-now seek.

- [ ] **Step 1: Write failing tests.**

```rust
#[test]
fn cursor_roundtrips_and_fsyncs() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("audit.cursor");
    let mut c = SeqCursor::load(p.clone(), true);
    assert_eq!(c.last(), 0);
    c.advance_to(7).unwrap();
    let c2 = SeqCursor::load(p, true);
    assert_eq!(c2.last(), 7);
}

#[test]
fn cursor_corrupt_file_resets() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("audit.cursor");
    std::fs::write(&p, b"not-a-number").unwrap();
    let c = SeqCursor::load(p, true);
    assert_eq!(c.last(), 0); // corrupt -> reset to start
}
```

- [ ] **Step 2: Run fail, implement** `SeqCursor` (parse `u64` from the file; on parse error or missing, `last = 0` and mark fresh; `advance_to` writes the decimal then `file.sync_all()`), wire the `export` module into `mod.rs`. **Step 3:** run pass, clippy, commit `feat(audit-export): durable seq cursor`.

### Task 6: `LogShipExporter` trait + `ocsf_to_log_record` mapping

**Files:**
- Create: `crates/sqe-metrics/src/audit/export/record.rs`
- Modify: `crates/sqe-metrics/src/audit/export/mod.rs`
- Test: inline in `record.rs`

**Interfaces:**
- Produces:
  - `pub trait LogShipExporter: Send { fn export_batch(&self, records: &[ShipRecord]) -> Result<(), String>; }` (the injection seam; the real impl wraps OTLP, a stub is used in tests).
  - `pub struct ShipRecord { pub seq: u64, pub time_unix_ms: i64, pub severity: Severity, pub body: serde_json::Value, pub class_uid: i64, pub category_uid: i64, pub kind: String, pub status_id: i64, pub user_name: String }` and `pub enum Severity { Info, Warn, Error }`.
  - `pub fn ocsf_to_ship_record(ocsf_line: &str) -> Option<ShipRecord>`: parse one OCSF JSON line into a `ShipRecord`, deriving `seq` from `metadata.sequence`, severity from `status_id` (1 -> Info; 2 -> Warn; absent/other -> Info), and the indexed fields. Returns `None` for an unparseable line (logged + skipped, not fatal).

- [ ] **Step 1: Write failing tests** for `ocsf_to_ship_record`: a success OCSF line -> `Severity::Info`, correct `seq`/`class_uid`/`user_name`; a failure line (`status_id: 2`) -> `Severity::Warn`; a garbage line -> `None`.

- [ ] **Step 2: Run fail, implement** the trait, structs, and the parser (use `serde_json::from_str::<Value>` then extract fields). **Step 3:** run pass, clippy, commit `feat(audit-export): OCSF-line to ship-record mapping + exporter trait`.

### Task 7: `OtlpLogShipper` (tail -> batch -> export -> advance cursor)

**Files:**
- Create: `crates/sqe-metrics/src/audit/export/shipper.rs`
- Modify: `crates/sqe-metrics/src/audit/export/mod.rs`
- Test: inline in `shipper.rs` (uses a stub `LogShipExporter`)

**Interfaces:**
- Consumes: `SeqCursor`, `LogShipExporter`, `ocsf_to_ship_record`, `AuditExportConfig`.
- Produces: `pub struct OtlpLogShipper { spool_path, cursor, exporter: Arc<dyn LogShipExporter>, batch_max, max_spool_bytes }` with `pub async fn run(self, shutdown: tokio::sync::watch::Receiver<bool>)` and a testable `pub fn ship_once(&mut self) -> ShipOutcome` that does ONE tail+batch+export+advance pass and returns `{ shipped: usize, advanced_to: u64, failed: bool }`.

Core `ship_once` logic:
1. Open the spool file, seek to the byte offset corresponding to the first line with `seq > cursor.last()` (track a byte offset in-memory derived from the last read position; on cold start with `start_at = "now"`, scan to EOF and set `cursor` to the last seq without shipping).
2. Read complete lines only (stop at a line lacking a trailing newline; leave it for next pass).
3. Parse each via `ocsf_to_ship_record`; skip `None` (warn).
4. Collect up to `batch_max` records with `seq > cursor.last()`.
5. If none, return `{shipped:0,...}`.
6. Call `exporter.export_batch(&records)`. On `Ok`: `cursor.advance_to(max_seq)`; return `{shipped, advanced_to:max_seq, failed:false}`. On `Err`: return `{shipped:0, failed:true}` WITHOUT advancing the cursor.
7. If spool file size > `max_spool_bytes`: `tracing::warn!` once per threshold crossing + set the lag metric.

`run` loops: `ship_once` every `flush_interval_ms` (or immediately again if a full batch shipped), with exponential backoff (capped) after a `failed` pass, until `shutdown` flips.

- [ ] **Step 1: Write the failing PROOF test (outage -> replay).**

```rust
struct StubExporter { fail: std::sync::atomic::AtomicBool, shipped: std::sync::Mutex<Vec<u64>> }
impl LogShipExporter for StubExporter {
    fn export_batch(&self, records: &[ShipRecord]) -> Result<(), String> {
        if self.fail.load(Ordering::SeqCst) { return Err("collector down".into()); }
        self.shipped.lock().unwrap().extend(records.iter().map(|r| r.seq));
        Ok(())
    }
}

#[test]
fn outage_freezes_cursor_then_replays_without_loss() {
    // spool file with 5 OCSF lines, seq 1..=5 (use to_ocsf on sample events with set seq)
    // exporter starts failing:
    let stub = Arc::new(StubExporter { fail: AtomicBool::new(true), shipped: Default::default() });
    let mut shipper = OtlpLogShipper::new(spool, cursor_path, stub.clone(), /*batch_max*/ 10, /*start_at*/ Beginning, ...);
    let o1 = shipper.ship_once();
    assert!(o1.failed);
    assert_eq!(SeqCursor::load(cursor_path.clone(), true).last(), 0, "cursor frozen on outage");
    // recover:
    stub.fail.store(false, Ordering::SeqCst);
    let o2 = shipper.ship_once();
    assert!(!o2.failed);
    assert_eq!(stub.shipped.lock().unwrap().clone(), vec![1,2,3,4,5], "backlog shipped in order, no loss");
    assert_eq!(SeqCursor::load(cursor_path, true).last(), 5, "cursor advanced after ack");
}

#[test]
fn restart_resumes_from_cursor_no_gap() {
    // ship seq 1..=3 (Ok), drop the shipper, build a new one over the same spool+cursor,
    // append seq 4..=5, ship_once -> only 4,5 shipped (1..3 not re-sent).
}

#[test]
fn tail_ignores_half_written_last_line() {
    // spool ends with a line WITHOUT a trailing newline -> that record is not shipped this pass.
}
```

- [ ] **Step 2: Run fail, implement** `OtlpLogShipper` + `ship_once` + `run`. **Step 3:** run pass (`cargo test -p sqe-metrics shipper` and the proof test), clippy, commit `feat(audit-export): OtlpLogShipper with at-least-once cursor-on-ack`.

### Task 8: Real OTLP exporter impl + spool provisioning

**Files:**
- Modify: `crates/sqe-metrics/src/audit/export/record.rs` (or a new `otlp.rs`): `pub struct OtlpExporter` implementing `LogShipExporter` by building OTel `LogRecord`s and calling a dedicated `opentelemetry_sdk` `LogExporter::export(batch)`, mapping `OTelSdkResult` -> `Result<(), String>`.
- Modify: `crates/sqe-metrics/src/audit/logger.rs`: `AuditLogger::with_export_spool(self, spool_path: &str) -> Self` that adds an `OcsfJsonlSink` pointed at `spool_path` to the worker's sink set, so every canonical event lands in the spool regardless of `format`.
- Test: inline (the OTLP impl is integration-tested lightly; the contract is the trait, already proven with the stub in Task 7).

**Interfaces:**
- Consumes: `opentelemetry-otlp` `LogExporter` (0.31, grpc-tonic). Pin the exact 0.31 API: `LogExporter::builder().with_tonic().with_endpoint(ep).build()` (as in `otel.rs`), then build a `LogBatch` of `SdkLogRecord`s and call `exporter.export(batch).await` returning `OTelSdkResult`. VERIFY the exact `LogBatch`/`SdkLogRecord` constructor names against the vendored 0.31 crate before writing (the trait `export` returning `OTelSdkResult` is confirmed; the record-builder names may differ).
- Produces: a working `OtlpExporter` and the `with_export_spool` provisioning hook.

- [ ] **Step 1:** Implement `with_export_spool` (mirrors how `OcsfJsonlSink` is constructed in `with_config`; the spool sink is always written when present). Add a test: `with_config(path, Native).with_export_spool(spool)` -> a `log_event` writes a canonical line to BOTH the native file and the spool file.
- [ ] **Step 2:** Implement `OtlpExporter::export_batch`: map each `ShipRecord` to an OTel `LogRecord` (timestamp from `time_unix_ms`, severity_number from `Severity`, body = `ShipRecord.body` as the OTel body, attributes `ocsf.class_uid`/`ocsf.category_uid`/`audit.kind`/`audit.status_id`/`user.name`/`audit.seq`), call the dedicated exporter's `export`, and translate the result. Block on the async export with the task's runtime handle (the shipper `run` is async; `export_batch` may be made async - if so, change the trait to `async fn` via `async-trait` or return a boxed future; PREFER making `LogShipExporter::export_batch` async and the stub async too, so no nested runtime). If you change the trait to async, update Task 6/7 signatures consistently.
- [ ] **Step 3:** Run `cargo test -p sqe-metrics`, clippy, commit `feat(audit-export): OTLP exporter impl + OCSF spool provisioning`.

Note: if making the trait async is cleaner (it is, given OTel `export` is async), do it in this task and adjust the Task 6/7 `export_batch` signatures to `async fn export_batch(&self, ...) -> Result<(), String>` (with `async-trait`); the stub becomes async; `ship_once` becomes `async fn ship_once`. Keep the proof test as `#[tokio::test]`.

### Task 9: Spawn the shipper at startup + metrics

**Files:**
- Modify: `crates/sqe-coordinator/src/main.rs`, `crates/sqe-coordinator/src/bin/sqe_server.rs`, `crates/sqe-metrics/src/lib.rs`
- Test: a focused startup-wiring test if feasible; otherwise rely on the shipper unit tests + a manual smoke note

**Interfaces:**
- Consumes: `config.metrics.audit_export`, `AuditLogger::with_export_spool`, `OtlpLogShipper`, `OtlpExporter`.

- [ ] **Step 1:** In `lib.rs` register the 5 metrics from the spec (`sqe_audit_export_records_total{status}`, `sqe_audit_export_batch_failures_total`, `sqe_audit_export_spool_lag_bytes`, `sqe_audit_export_cursor_seq`, `sqe_audit_export_last_success_timestamp`) on the existing registry; expose setters the shipper calls.
- [ ] **Step 2:** In `main.rs`/`sqe_server.rs`, when `config.metrics.audit_export.enabled` and `target == "otlp"`: derive `spool_path` (config or `<audit_log_path>.ocsf.spool.jsonl`), call `.with_export_spool(&spool_path)` on the `AuditLogger`, build the `OtlpExporter` from `audit_export.otlp_endpoint` (fallback `metrics.otlp_endpoint`), construct `OtlpLogShipper`, and `tokio::spawn(shipper.run(shutdown_rx))`. Unknown `target` -> `tracing::warn!` + skip. Disabled -> nothing (default).
- [ ] **Step 3:** Run `cargo test -p sqe-coordinator -p sqe-metrics`, `cargo check --workspace`, clippy, commit `feat(audit-export): spawn OTLP shipper at startup with metrics`.

### Task 10: Docs + roadmap + full regression

**Files:**
- Modify: `docs/site/book/src/operations/audit-logging.md`, `docs/internal/roadmap-tracker.md`
- Test: full workspace

- [ ] **Step 1:** Add an "Exporting to a SIEM (OTLP)" section to the audit doc: the `[metrics.audit_export]` config block, the at-least-once + durable-spool behavior, the cursor/spool files, the OTLP record mapping (body = OCSF, indexed attributes), the metrics, and the deferred items (rotation, Kafka). Note that B1 made all audited activity canonical (streaming SELECT + DML/DDL now OCSF). Honor the no-emdash rule.
- [ ] **Step 2:** Mark sub-project B done in `docs/internal/roadmap-tracker.md` following its convention; note Kafka/rotation deferred.
- [ ] **Step 3:** Full regression: `cargo test --all` (note docker-gated tests are `#[ignore]`d; a pre-existing `oidc_m2m`/`channel_pool` network test may fail offline - report as pre-existing), `cargo clippy --all-targets --all-features -- -D warnings`. Confirm `audit_e2e_test.rs` passes and `enabled=false` output is unchanged from A.
- [ ] **Step 4:** No-emdash check: `grep -rn '—' docs/site/book/src/operations/audit-logging.md docs/internal/roadmap-tracker.md | grep -v '`' || echo clean`.
- [ ] **Step 5:** Commit `docs(audit-export): document SIEM export + mark roadmap done`.

---

## Self-Review

**Spec coverage:**
- B1 complete coverage: Tasks 2 (streaming), 3 (DML/DDL/maintenance); `integrity.seq` in OCSF: Task 1. B2 spool-as-OCSF + provisioning: Task 8 (`with_export_spool`). Cursor-on-`seq`: Task 5. OCSF->LogRecord mapping + severity + attributes: Task 6, 8. Shipper tail/batch/export/advance + at-least-once + read-to-newline: Task 7. Dedicated OTLP exporter off the bridge: Task 8. Config `[metrics.audit_export]`: Task 4. Metrics: Task 9. start_at/from-now: Tasks 5, 7. Bounded-growth WARN: Task 7. Docs: Task 10. The outage->replay proof test: Task 7. Defaults-unchanged regression: Task 10 Step 3.
- Deferred items (rotation, Kafka, rich attributes) are correctly not built and are documented in Task 10.

**Placeholder scan:** Task 2/3 reference the existing harness and the prior streaming test by name for setup rather than reproducing the whole fixture - acceptable (the local pattern exists and is cited). Task 8 explicitly flags the one API detail to verify against the vendored 0.31 crate (the `LogBatch`/`SdkLogRecord` constructor), with the confirmed contract (`export -> OTelSdkResult`) stated; this is a real verification step, not a vague placeholder.

**Type consistency:** `LogShipExporter::export_batch` is introduced in Task 6 and made async in Task 8 with a note to update Task 6/7 signatures together (flagged explicitly so the implementer keeps them consistent). `ShipRecord`, `Severity`, `SeqCursor` (`load`/`last`/`advance_to`), `ocsf_to_ship_record`, `OtlpLogShipper` (`ship_once`/`run`), `AuditLogger::with_export_spool`, `AuditExportConfig` are defined once and used consistently. `to_ocsf` `metadata.sequence` (Task 1) is the field `ocsf_to_ship_record` reads (Task 6).

**Open risk:** the async-trait decision (Task 8) ripples to Tasks 6/7; the plan calls this out so the implementer makes `export_batch`/`ship_once` async from the start if they prefer (recommended) rather than retrofitting. The exact OTel 0.31 log-record builder is pinned at implementation time against the vendored crate.
