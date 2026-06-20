# OCSF Audit Logging and Identity Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give SQE a single canonical audit event that flows through one logger, serializes to OCSF on demand, carries enriched identity (subject, email, groups), masks GDPR-tagged fields in logged SQL, never logs result rows, and is tamper-evident via a hash chain.

**Architecture:** Keep the existing `AuditLogger` (mpsc channel plus dedicated writer thread). Introduce a canonical `AuditEvent` that all event kinds flow through. The single JSONL writer becomes a list of sinks; each sink serializes the same redacted, hash-chained event independently. OCSF is a pure mapping function over the canonical event, selected by config. Identity enrichment happens upstream in `sqe-auth` and `sqe-core`, then flows into the event's `Actor`.

**Tech Stack:** Rust, `tracing`, `serde`/`serde_json`, `sha2`, `chrono`, `regex`, `moka` (existing `TableMetadataCache`), DataFusion `LogicalPlan` for resource resolution.

## Global Constraints

- This is Sub-project A only. Do NOT build the OTel/SIEM sink transport (Sub-project B) or the operational-logging polish and `/api/v1/queries` gating (Sub-project C). A's sinks are file-backed.
- Layer OCSF on top of the canonical event. Do NOT replace the canonical event with an OCSF struct.
- On-disk default format stays native JSONL. OCSF is opt-in via `[audit] format`.
- Default config must preserve current behavior: empty `audit_log_path` disables logging; `email_claim` and `groups_claim` default empty (off); `roles_claim` default stays `"realm_access.roles"`; `subject_claim` default `"sub"`.
- GDPR tags come from the existing `sqe-policy::tag_source::TagSource` (Iceberg property `sqe.column-tags`). Reuse it via dependency inversion; do NOT make `sqe-metrics` depend on `sqe-policy` or `sqe-catalog`.
- Never log result rows. The only escape hatch is `[audit] superdebug_log_results`, default `false`.
- GDPR masking: always strip literal values compared against GDPR columns; column identifier handled per `gdpr_identifier_mode` = `tokenize` (default) | `drop` | `keep`.
- OCSF UIDs (verified against schema.ocsf.io on 2026-06-20): Datastore Activity `class_uid=6005 category_uid=6`; Authentication `3002`/`3`; Authorize Session `3003`/`3`; Account Change `3001`/`3`; Entity Management `3004`/`3`.
- No emdash, endash, or unicode arrows anywhere in code comments or docs (project rule). Use `->` in code, plain hyphens in prose.
- Run `cargo clippy --all-targets --all-features -- -D warnings` clean before each commit.
- Existing tests must keep passing, especially `crates/sqe-coordinator/tests/it/audit_e2e_test.rs` with `format = "native"`.

---

## File Structure

New and modified files, by responsibility:

- `crates/sqe-metrics/src/audit/mod.rs` (new): re-exports; today's `audit.rs` becomes this module's root after a mechanical move.
- `crates/sqe-metrics/src/audit/event.rs` (new): canonical `AuditEvent`, `AuditKind`, `Actor`, `Resource`, `ObjectType`, `Outcome`, `QueryInfo`, `Timing`, `QueryStats`, `Integrity`.
- `crates/sqe-metrics/src/audit/redact.rs` (new): `redact_pii`, `strip_sql_literals` (moved verbatim from `audit.rs`), plus new `mask_gdpr_columns`.
- `crates/sqe-metrics/src/audit/ocsf.rs` (new): `to_ocsf(&AuditEvent) -> serde_json::Value`.
- `crates/sqe-metrics/src/audit/sink.rs` (new): `AuditSink` trait, `NativeJsonlSink`, `OcsfJsonlSink`.
- `crates/sqe-metrics/src/audit/chain.rs` (new): hash-chain computation and `verify_chain`.
- `crates/sqe-metrics/src/audit/logger.rs` (new): `AuditLogger` (moved from `audit.rs`, refactored to fan out to sinks and apply chain/redaction).
- `crates/sqe-metrics/src/audit/tag_lookup.rs` (new): `TagLookup` trait (dependency-inversion seam for GDPR tags) and `NoopTagLookup`.
- `crates/sqe-core/src/config.rs` (modify): add `AuditConfig`; add claim-path fields to the bearer/oidc provider config.
- `crates/sqe-auth/src/provider.rs` (modify): extend `Identity` with `subject`, `email`, `groups`.
- `crates/sqe-auth/src/bearer_token.rs`, `crates/sqe-auth/src/oidc_provider.rs` (modify): extract new claims.
- `crates/sqe-core/src/session.rs` (modify): extend `SessionUser`; add identity builder.
- `crates/sqe-coordinator/src/session_manager.rs` (modify): thread new identity fields from `Identity` into `SessionUser`.
- `crates/sqe-coordinator/src/query_handler.rs`, `streaming.rs`, `maintenance.rs`, `flight_sql.rs` (modify): build `AuditEvent`s, populate `Actor` and `Resource`s, emit new kinds.
- `crates/sqe-coordinator/src/audit_tag_adapter.rs` (new): coordinator-side `TagLookup` impl backed by the existing `TagSource`.

---

## Phase 1: Canonical event and OCSF mapping (pure, sqe-metrics)

### Task 1: Move audit into a module and add the canonical `AuditEvent`

**Files:**
- Create: `crates/sqe-metrics/src/audit/mod.rs`, `crates/sqe-metrics/src/audit/event.rs`, `crates/sqe-metrics/src/audit/redact.rs`, `crates/sqe-metrics/src/audit/logger.rs`
- Modify: `crates/sqe-metrics/src/lib.rs` (module path stays `pub mod audit;`)
- Delete: `crates/sqe-metrics/src/audit.rs` (contents distributed into the module)
- Test: tests live inline in `event.rs`

**Interfaces:**
- Produces: `AuditEvent`, `AuditKind`, `Actor`, `Resource`, `ObjectType`, `Outcome`, `QueryInfo`, `Timing`, `QueryStats`, `Integrity`. Existing public items keep their paths: `sqe_metrics::audit::{AuditEntry, AuditLogger, redact_pii, strip_sql_literals, query_hash}`.

- [ ] **Step 1: Mechanical move with no behavior change.** Create `crates/sqe-metrics/src/audit/mod.rs`. Move `redact_pii`, `strip_sql_literals`, `is_ident_byte`, `utf8_char_len` and their tests into `redact.rs`. Move `AuditEntry`, `query_hash`, `AuditMsg`, `AuditLogger` and their tests into `logger.rs`. In `mod.rs` add:

```rust
mod event;
mod logger;
mod redact;

pub use event::{
    Actor, AuditEvent, AuditKind, Integrity, ObjectType, Outcome, QueryInfo, QueryStats,
    Resource, Timing,
};
pub use logger::{query_hash, AuditEntry, AuditLogger};
pub use redact::{redact_pii, strip_sql_literals};
```

- [ ] **Step 2: Run the moved tests to prove the move is behavior-preserving.**

Run: `cargo test -p sqe-metrics audit`
Expected: PASS (same tests as before the move).

- [ ] **Step 3: Write the failing test for the canonical event in `event.rs`.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample_query_event() -> AuditEvent {
        AuditEvent {
            time: chrono::Utc.with_ymd_and_hms(2026, 6, 20, 12, 0, 0).unwrap(),
            kind: AuditKind::Query,
            actor: Actor {
                username: "alice".into(),
                subject: Some("user-1".into()),
                email: Some("alice@corp.example".into()),
                roles: vec!["analyst".into()],
                groups: vec!["hr".into()],
            },
            outcome: Outcome::Success,
            resources: vec![Resource {
                catalog: Some("polaris".into()),
                namespace: vec!["hr".into()],
                name: "employees".into(),
                object_type: ObjectType::Table,
            }],
            policy: None,
            timing: Some(Timing { duration_ms: 42, queued_ms: 0, planning_ms: 5, execution_ms: 37 }),
            stats: Some(QueryStats { rows_returned: 10, bytes_scanned: 1024, rows_scanned: 100, spill_bytes: 0, peak_memory_bytes: 0 }),
            query: Some(QueryInfo {
                text: Some("SELECT 1".into()),
                query_hash: "abc".into(),
                statement_type: "query".into(),
            }),
            session_id: Some("sess-1".into()),
            client_ip: Some("10.0.0.1".into()),
            integrity: Integrity::default(),
        }
    }

    #[test]
    fn canonical_event_serializes_with_kind_and_actor() {
        let ev = sample_query_event();
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"kind\":\"query\""));
        assert!(json.contains("\"username\":\"alice\""));
        assert!(json.contains("\"groups\":[\"hr\"]"));
        assert!(json.contains("\"object_type\":\"table\""));
    }

    #[test]
    fn actor_omits_empty_optional_identity() {
        let mut ev = sample_query_event();
        ev.actor.email = None;
        ev.actor.subject = None;
        ev.actor.groups = vec![];
        let json = serde_json::to_string(&ev.actor).unwrap();
        assert!(!json.contains("email"));
        assert!(!json.contains("subject"));
        assert!(!json.contains("groups"));
    }
}
```

- [ ] **Step 4: Run it to verify failure.**

Run: `cargo test -p sqe-metrics canonical_event`
Expected: FAIL with unresolved `AuditEvent`/`Actor` etc.

- [ ] **Step 5: Implement the canonical types in `event.rs`.**

```rust
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditKind {
    Query,
    Auth,
    Session,
    Grant,
    AdminDdl,
    PolicyDecision,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Actor {
    pub username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectType {
    Table,
    View,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespace: Vec<String>,
    pub name: String,
    pub object_type: ObjectType,
}

impl Resource {
    /// Fully-qualified `catalog.ns1.ns2.name`, used for OCSF resource fields
    /// and log display. Catalog omitted when unknown.
    pub fn fqn(&self) -> String {
        let mut parts: Vec<&str> = Vec::new();
        if let Some(c) = self.catalog.as_deref() {
            parts.push(c);
        }
        parts.extend(self.namespace.iter().map(String::as_str));
        parts.push(&self.name);
        parts.join(".")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Success,
    Failure {
        #[serde(skip_serializing_if = "Option::is_none")]
        error_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_code: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    pub query_hash: String,
    pub statement_type: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Timing {
    pub duration_ms: u64,
    #[serde(default)]
    pub queued_ms: u64,
    #[serde(default)]
    pub planning_ms: u64,
    #[serde(default)]
    pub execution_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryStats {
    pub rows_returned: usize,
    #[serde(default)]
    pub bytes_scanned: u64,
    #[serde(default)]
    pub rows_scanned: u64,
    #[serde(default)]
    pub spill_bytes: u64,
    #[serde(default)]
    pub peak_memory_bytes: u64,
}

/// Policy decision summary carried in the audit event. A plain copy of the
/// fields from `sqe_policy::PolicySummary` so `sqe-metrics` need not depend on
/// `sqe-policy`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolicyAudit {
    #[serde(default)]
    pub row_filters_applied: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns_masked: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns_restricted: Vec<String>,
    #[serde(default)]
    pub denied: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Integrity {
    pub seq: u64,
    pub prev_hash: String,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub time: DateTime<Utc>,
    pub kind: AuditKind,
    pub actor: Actor,
    #[serde(flatten)]
    pub outcome: Outcome,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<Resource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyAudit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timing: Option<Timing>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<QueryStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<QueryInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
    #[serde(default)]
    pub integrity: Integrity,
}
```

Add `PolicyAudit` to the `pub use` list in `mod.rs`.

- [ ] **Step 6: Run the tests to verify they pass.**

Run: `cargo test -p sqe-metrics`
Expected: PASS (new canonical tests plus all moved tests).

- [ ] **Step 7: Clippy and commit.**

```bash
cargo clippy -p sqe-metrics --all-targets -- -D warnings
git add crates/sqe-metrics/src/audit crates/sqe-metrics/src/lib.rs
git rm crates/sqe-metrics/src/audit.rs
git commit -m "feat(audit): canonical AuditEvent model in sqe-metrics audit module"
```

### Task 2: OCSF mapping for the Query kind (Datastore Activity 6005)

**Files:**
- Create: `crates/sqe-metrics/src/audit/ocsf.rs`
- Modify: `crates/sqe-metrics/src/audit/mod.rs` (add `mod ocsf; pub use ocsf::to_ocsf;`)
- Test: inline in `ocsf.rs`

**Interfaces:**
- Consumes: `AuditEvent` (Task 1).
- Produces: `pub fn to_ocsf(event: &AuditEvent) -> serde_json::Value`.

- [ ] **Step 1: Write the failing golden test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::*;
    use chrono::TimeZone;

    fn query_event() -> AuditEvent {
        AuditEvent {
            time: chrono::Utc.with_ymd_and_hms(2026, 6, 20, 12, 0, 0).unwrap(),
            kind: AuditKind::Query,
            actor: Actor { username: "alice".into(), subject: Some("u1".into()), email: Some("a@x.io".into()), roles: vec!["analyst".into()], groups: vec!["hr".into()] },
            outcome: Outcome::Success,
            resources: vec![Resource { catalog: Some("polaris".into()), namespace: vec!["hr".into()], name: "employees".into(), object_type: ObjectType::Table }],
            policy: None,
            timing: Some(Timing { duration_ms: 42, queued_ms: 0, planning_ms: 5, execution_ms: 37 }),
            stats: Some(QueryStats { rows_returned: 10, bytes_scanned: 2048, rows_scanned: 100, spill_bytes: 0, peak_memory_bytes: 0 }),
            query: Some(QueryInfo { text: Some("SELECT 1".into()), query_hash: "abc".into(), statement_type: "query".into() }),
            session_id: Some("sess-1".into()),
            client_ip: Some("10.0.0.1".into()),
            integrity: Integrity::default(),
        }
    }

    #[test]
    fn query_maps_to_datastore_activity() {
        let v = to_ocsf(&query_event());
        assert_eq!(v["class_uid"], 6005);
        assert_eq!(v["category_uid"], 6);
        assert_eq!(v["status_id"], 1); // Success
        assert_eq!(v["actor"]["user"]["name"], "alice");
        assert_eq!(v["actor"]["user"]["uid"], "u1");
        assert_eq!(v["actor"]["user"]["email_addr"], "a@x.io");
        assert_eq!(v["actor"]["user"]["groups"][0]["name"], "hr");
        assert_eq!(v["metadata"]["product"]["name"], "SQE");
        // SQE-specific stats live under enrichments/unmapped, not core fields.
        assert_eq!(v["unmapped"]["bytes_scanned"], 2048);
        assert_eq!(v["unmapped"]["query_hash"], "abc");
    }

    #[test]
    fn failure_outcome_sets_status_id_2() {
        let mut ev = query_event();
        ev.outcome = Outcome::Failure { error_type: Some("PlanError".into()), error_code: Some("E1".into()), message: Some("bad".into()) };
        let v = to_ocsf(&ev);
        assert_eq!(v["status_id"], 2);
    }
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p sqe-metrics query_maps_to_datastore_activity`
Expected: FAIL with unresolved `to_ocsf`.

- [ ] **Step 3: Implement `to_ocsf` with the Query mapping and shared envelope.**

```rust
use serde_json::{json, Value};
use super::event::{AuditEvent, AuditKind, Outcome};

/// Map a canonical `AuditEvent` to an OCSF event object. OCSF is a wire schema:
/// SQE-specific fields that have no OCSF home travel under `unmapped`.
pub fn to_ocsf(event: &AuditEvent) -> Value {
    let (class_uid, category_uid) = class_for(&event.kind);
    let status_id = match event.outcome {
        Outcome::Success => 1,
        Outcome::Failure { .. } => 2,
    };

    let mut groups = Vec::new();
    for g in &event.actor.groups {
        groups.push(json!({ "name": g }));
    }
    let mut user = json!({ "name": event.actor.username });
    if let Some(sub) = &event.actor.subject { user["uid"] = json!(sub); }
    if let Some(email) = &event.actor.email { user["email_addr"] = json!(email); }
    if !groups.is_empty() { user["groups"] = json!(groups); }
    if !event.actor.roles.is_empty() { user["roles"] = json!(event.actor.roles); }

    let mut unmapped = serde_json::Map::new();
    if let Some(q) = &event.query {
        unmapped.insert("query_hash".into(), json!(q.query_hash));
        unmapped.insert("statement_type".into(), json!(q.statement_type));
        if let Some(t) = &q.text { unmapped.insert("query_text".into(), json!(t)); }
    }
    if let Some(s) = &event.stats {
        unmapped.insert("bytes_scanned".into(), json!(s.bytes_scanned));
        unmapped.insert("rows_scanned".into(), json!(s.rows_scanned));
        unmapped.insert("spill_bytes".into(), json!(s.spill_bytes));
        unmapped.insert("peak_memory_bytes".into(), json!(s.peak_memory_bytes));
        unmapped.insert("rows_returned".into(), json!(s.rows_returned));
    }
    if let Some(p) = &event.policy {
        unmapped.insert("policy".into(), serde_json::to_value(p).unwrap_or(Value::Null));
    }

    let mut out = json!({
        "class_uid": class_uid,
        "category_uid": category_uid,
        "status_id": status_id,
        "time": event.time.timestamp_millis(),
        "severity_id": 1,
        "metadata": {
            "product": { "name": "SQE", "vendor_name": "SQE" },
            "version": "1.3.0",
            "uid": event.integrity.hash,
        },
        "actor": { "user": user },
        "unmapped": Value::Object(unmapped),
    });

    if let Outcome::Failure { message, .. } = &event.outcome {
        if let Some(m) = message { out["message"] = json!(m); }
    }
    if let Some(ip) = &event.client_ip { out["src_endpoint"] = json!({ "ip": ip }); }
    if !event.resources.is_empty() {
        let resources: Vec<Value> = event.resources.iter().map(|r| json!({
            "name": r.fqn(),
            "type": match r.object_type { super::event::ObjectType::Table => "Table", super::event::ObjectType::View => "View" },
        })).collect();
        out["resources"] = json!(resources);
    }
    out
}

fn class_for(kind: &AuditKind) -> (u32, u32) {
    match kind {
        AuditKind::Query | AuditKind::PolicyDecision => (6005, 6),
        AuditKind::Auth => (3002, 3),
        AuditKind::Session => (3003, 3),
        AuditKind::Grant => (3001, 3),
        AuditKind::AdminDdl => (3004, 3),
    }
}
```

- [ ] **Step 4: Run to verify pass.**

Run: `cargo test -p sqe-metrics ocsf`
Expected: PASS.

- [ ] **Step 5: Clippy and commit.**

```bash
cargo clippy -p sqe-metrics --all-targets -- -D warnings
git add crates/sqe-metrics/src/audit/ocsf.rs crates/sqe-metrics/src/audit/mod.rs
git commit -m "feat(audit): OCSF mapping for query events (Datastore Activity 6005)"
```

### Task 3: OCSF mapping golden tests for the other kinds

**Files:**
- Modify: `crates/sqe-metrics/src/audit/ocsf.rs` (tests only; `class_for` already covers all kinds)
- Test: inline in `ocsf.rs`

**Interfaces:**
- Consumes: `to_ocsf` (Task 2).

- [ ] **Step 1: Write failing golden tests for each non-query kind.**

```rust
#[test]
fn auth_session_grant_admin_class_uids() {
    use crate::audit::*;
    use chrono::TimeZone;
    let base = |kind: AuditKind| AuditEvent {
        time: chrono::Utc.with_ymd_and_hms(2026, 6, 20, 12, 0, 0).unwrap(),
        kind,
        actor: Actor { username: "bob".into(), subject: None, email: None, roles: vec![], groups: vec![] },
        outcome: Outcome::Success,
        resources: vec![],
        policy: None, timing: None, stats: None, query: None,
        session_id: None, client_ip: None, integrity: Integrity::default(),
    };
    assert_eq!(to_ocsf(&base(AuditKind::Auth))["class_uid"], 3002);
    assert_eq!(to_ocsf(&base(AuditKind::Session))["class_uid"], 3003);
    assert_eq!(to_ocsf(&base(AuditKind::Grant))["class_uid"], 3001);
    assert_eq!(to_ocsf(&base(AuditKind::AdminDdl))["class_uid"], 3004);
    // Policy deny rides on Datastore Activity with a Failure status.
    let mut deny = base(AuditKind::PolicyDecision);
    deny.outcome = Outcome::Failure { error_type: Some("PolicyDenied".into()), error_code: None, message: Some("deny-all".into()) };
    let v = to_ocsf(&deny);
    assert_eq!(v["class_uid"], 6005);
    assert_eq!(v["status_id"], 2);
}
```

- [ ] **Step 2: Run to verify failure, then pass.**

Run: `cargo test -p sqe-metrics auth_session_grant_admin_class_uids`
Expected: PASS (mapping already implemented in Task 2; this test pins the contract). If it fails, fix `class_for`.

- [ ] **Step 3: Commit.**

```bash
git add crates/sqe-metrics/src/audit/ocsf.rs
git commit -m "test(audit): pin OCSF class_uid mapping for all event kinds"
```

---

## Phase 2: Sinks, format config, hash chain

### Task 4: Sink trait and native/OCSF file sinks

**Files:**
- Create: `crates/sqe-metrics/src/audit/sink.rs`
- Modify: `crates/sqe-metrics/src/audit/mod.rs`
- Test: inline in `sink.rs`

**Interfaces:**
- Consumes: `AuditEvent`, `to_ocsf`.
- Produces: `pub trait AuditSink { fn write_line(&mut self, event: &AuditEvent) -> std::io::Result<()>; fn flush(&mut self) -> std::io::Result<()>; }`, `NativeJsonlSink`, `OcsfJsonlSink`, and `pub enum AuditFormat { Native, Ocsf, Both }`.

- [ ] **Step 1: Write the failing test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::*;
    use std::io::Write;

    struct VecWriter(std::rc::Rc<std::cell::RefCell<Vec<u8>>>);
    impl Write for VecWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> { self.0.borrow_mut().extend_from_slice(buf); Ok(buf.len()) }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }

    fn ev() -> AuditEvent { /* reuse a minimal Query event as in ocsf tests */ unimplemented!() }

    #[test]
    fn native_sink_writes_canonical_json_line() {
        let buf = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let mut sink = NativeJsonlSink::from_writer(Box::new(VecWriter(buf.clone())));
        sink.write_line(&ev()).unwrap();
        let s = String::from_utf8(buf.borrow().clone()).unwrap();
        assert!(s.contains("\"kind\":\"query\""));
        assert!(s.ends_with('\n'));
    }

    #[test]
    fn ocsf_sink_writes_class_uid_line() {
        let buf = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let mut sink = OcsfJsonlSink::from_writer(Box::new(VecWriter(buf.clone())));
        sink.write_line(&ev()).unwrap();
        let s = String::from_utf8(buf.borrow().clone()).unwrap();
        assert!(s.contains("\"class_uid\":6005"));
    }
}
```

Replace `ev()` with a shared `sample_query_event()` helper extracted into `event.rs` as `#[cfg(test)] pub(crate) fn sample_query_event()` so tests across modules reuse it (DRY).

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p sqe-metrics native_sink_writes_canonical_json_line`
Expected: FAIL (types undefined).

- [ ] **Step 3: Implement the sink trait and two sinks.**

```rust
use std::io::Write;
use serde::{Deserialize, Serialize};
use super::event::AuditEvent;
use super::ocsf::to_ocsf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AuditFormat {
    #[default]
    Native,
    Ocsf,
    Both,
}

pub trait AuditSink: Send {
    fn write_line(&mut self, event: &AuditEvent) -> std::io::Result<()>;
    fn flush(&mut self) -> std::io::Result<()>;
}

pub struct NativeJsonlSink { w: Box<dyn Write + Send> }
impl NativeJsonlSink {
    pub fn from_writer(w: Box<dyn Write + Send>) -> Self { Self { w } }
}
impl AuditSink for NativeJsonlSink {
    fn write_line(&mut self, event: &AuditEvent) -> std::io::Result<()> {
        let line = serde_json::to_string(event).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        writeln!(self.w, "{line}")
    }
    fn flush(&mut self) -> std::io::Result<()> { self.w.flush() }
}

pub struct OcsfJsonlSink { w: Box<dyn Write + Send> }
impl OcsfJsonlSink {
    pub fn from_writer(w: Box<dyn Write + Send>) -> Self { Self { w } }
}
impl AuditSink for OcsfJsonlSink {
    fn write_line(&mut self, event: &AuditEvent) -> std::io::Result<()> {
        let line = serde_json::to_string(&to_ocsf(event)).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        writeln!(self.w, "{line}")
    }
    fn flush(&mut self) -> std::io::Result<()> { self.w.flush() }
}
```

- [ ] **Step 4: Run to verify pass.**

Run: `cargo test -p sqe-metrics sink`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
cargo clippy -p sqe-metrics --all-targets -- -D warnings
git add crates/sqe-metrics/src/audit/sink.rs crates/sqe-metrics/src/audit/mod.rs crates/sqe-metrics/src/audit/event.rs
git commit -m "feat(audit): AuditSink trait with native and OCSF JSONL sinks"
```

### Task 5: Hash chain and verifier

**Files:**
- Create: `crates/sqe-metrics/src/audit/chain.rs`
- Modify: `crates/sqe-metrics/src/audit/mod.rs`
- Test: inline in `chain.rs`

**Interfaces:**
- Consumes: `AuditEvent`, `Integrity`.
- Produces: `pub struct HashChain { /* seq, prev_hash */ }` with `fn stamp(&mut self, event: &mut AuditEvent)`, and `pub fn verify_chain(events: &[AuditEvent]) -> Result<(), ChainError>`.

- [ ] **Step 1: Write the failing tests.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::sample_query_event;

    #[test]
    fn chain_is_well_ordered_and_verifies() {
        let mut chain = HashChain::new();
        let mut a = sample_query_event();
        let mut b = sample_query_event();
        chain.stamp(&mut a);
        chain.stamp(&mut b);
        assert_eq!(a.integrity.seq, 0);
        assert_eq!(b.integrity.seq, 1);
        assert_eq!(b.integrity.prev_hash, a.integrity.hash);
        verify_chain(&[a, b]).unwrap();
    }

    #[test]
    fn tampered_record_fails_verification() {
        let mut chain = HashChain::new();
        let mut a = sample_query_event();
        let mut b = sample_query_event();
        chain.stamp(&mut a);
        chain.stamp(&mut b);
        a.actor.username = "mallory".into(); // tamper after stamping
        assert!(verify_chain(&[a, b]).is_err());
    }

    #[test]
    fn truncated_tail_is_detectable_via_seq_gap() {
        let mut chain = HashChain::new();
        let mut a = sample_query_event();
        let mut b = sample_query_event();
        let mut c = sample_query_event();
        chain.stamp(&mut a); chain.stamp(&mut b); chain.stamp(&mut c);
        // Dropping b leaves a seq gap and a broken prev_hash link.
        assert!(verify_chain(&[a, c]).is_err());
    }
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p sqe-metrics chain_is_well_ordered`
Expected: FAIL (types undefined).

- [ ] **Step 3: Implement the chain.** The hash covers the event with its `integrity.hash` field zeroed, so verification recomputes deterministically.

```rust
use sha2::{Digest, Sha256};
use super::event::AuditEvent;

const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

pub struct HashChain { next_seq: u64, prev_hash: String }

impl HashChain {
    pub fn new() -> Self { Self { next_seq: 0, prev_hash: GENESIS.to_string() } }

    pub fn stamp(&mut self, event: &mut AuditEvent) {
        event.integrity.seq = self.next_seq;
        event.integrity.prev_hash = self.prev_hash.clone();
        event.integrity.hash = compute_hash(event);
        self.prev_hash = event.integrity.hash.clone();
        self.next_seq += 1;
    }
}

impl Default for HashChain { fn default() -> Self { Self::new() } }

/// Hash of the event with its own `hash` field blanked, chained on `prev_hash`.
fn compute_hash(event: &AuditEvent) -> String {
    let mut clone = event.clone();
    clone.integrity.hash = String::new();
    let body = serde_json::to_string(&clone).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(clone.integrity.prev_hash.as_bytes());
    hasher.update(body.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[derive(Debug)]
pub enum ChainError {
    SeqGap { expected: u64, found: u64 },
    BrokenLink { seq: u64 },
    BadHash { seq: u64 },
}

pub fn verify_chain(events: &[AuditEvent]) -> Result<(), ChainError> {
    let mut prev = GENESIS.to_string();
    for (i, ev) in events.iter().enumerate() {
        let expected_seq = i as u64;
        if ev.integrity.seq != expected_seq {
            return Err(ChainError::SeqGap { expected: expected_seq, found: ev.integrity.seq });
        }
        if ev.integrity.prev_hash != prev {
            return Err(ChainError::BrokenLink { seq: ev.integrity.seq });
        }
        if compute_hash(ev) != ev.integrity.hash {
            return Err(ChainError::BadHash { seq: ev.integrity.seq });
        }
        prev = ev.integrity.hash.clone();
    }
    Ok(())
}
```

Note: `verify_chain` assumes the slice starts at `seq=0`. For files that have rotated, the verifier seeds `prev` from the first record's `prev_hash` and checks relative continuity; add that variant only if log rotation lands in Sub-project B. For A, files are append-only from genesis.

- [ ] **Step 4: Run to verify pass.**

Run: `cargo test -p sqe-metrics chain`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
cargo clippy -p sqe-metrics --all-targets -- -D warnings
git add crates/sqe-metrics/src/audit/chain.rs crates/sqe-metrics/src/audit/mod.rs
git commit -m "feat(audit): tamper-evident hash chain with verifier"
```

### Task 6: `AuditConfig` and `AuditLogger` fan-out

**Files:**
- Modify: `crates/sqe-core/src/config.rs` (add `AuditConfig`, default impl, env overrides), `crates/sqe-metrics/src/audit/logger.rs`
- Test: inline in `logger.rs` and `config.rs`

**Interfaces:**
- Consumes: `AuditEvent`, `AuditSink`, `AuditFormat`, `HashChain`.
- Produces: `AuditLogger::with_config(path: &str, format: AuditFormat) -> Result<Self, String>` and `AuditLogger::log_event(&self, event: AuditEvent)`. Keep `AuditLogger::new(path)` as `with_config(path, AuditFormat::Native)` for back-compat, and keep `log(&AuditEntry)` delegating to `log_event` via `AuditEntry -> AuditEvent` conversion.
- Produces: `pub struct AuditConfig { format, gdpr_tags, gdpr_identifier_mode, superdebug_log_results }` in `sqe-core`.

- [ ] **Step 1: Write the failing config test in `config.rs`.**

```rust
#[test]
fn audit_config_defaults_are_back_compatible() {
    let c = AuditConfig::default();
    assert_eq!(c.format, "native");
    assert!(c.gdpr_tags.is_empty());
    assert_eq!(c.gdpr_identifier_mode, "tokenize");
    assert!(!c.superdebug_log_results);
}
```

Add `AuditConfig`:

```rust
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuditConfig {
    #[serde(default = "default_audit_format")]
    pub format: String, // "native" | "ocsf" | "both"
    #[serde(default)]
    pub gdpr_tags: Vec<String>,
    #[serde(default = "default_gdpr_identifier_mode")]
    pub gdpr_identifier_mode: String, // "tokenize" | "drop" | "keep"
    #[serde(default)]
    pub superdebug_log_results: bool,
}
fn default_audit_format() -> String { "native".to_string() }
fn default_gdpr_identifier_mode() -> String { "tokenize".to_string() }
impl Default for AuditConfig {
    fn default() -> Self {
        Self { format: default_audit_format(), gdpr_tags: Vec::new(), gdpr_identifier_mode: default_gdpr_identifier_mode(), superdebug_log_results: false }
    }
}
```

Add `#[serde(default)] pub audit: AuditConfig` to `MetricsConfig` (keep `audit_log_path` where it is; `audit` holds the new knobs) and to its `Default`. Add env overrides next to the existing `SQE_METRICS__AUDIT_LOG_PATH` line: `SQE_METRICS__AUDIT__FORMAT`, `SQE_METRICS__AUDIT__SUPERDEBUG_LOG_RESULTS`.

- [ ] **Step 2: Run config test.**

Run: `cargo test -p sqe-core audit_config_defaults`
Expected: PASS after the struct is added.

- [ ] **Step 3: Write the failing logger fan-out test in `logger.rs`.**

```rust
#[test]
fn both_format_writes_native_and_ocsf_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let logger = AuditLogger::with_config(path.to_str().unwrap(), crate::audit::AuditFormat::Both).unwrap();
    logger.log_event(crate::audit::sample_query_event());
    logger.flush();
    let native = std::fs::read_to_string(&path).unwrap();
    let ocsf = std::fs::read_to_string(dir.path().join("audit.ocsf.jsonl")).unwrap();
    assert!(native.contains("\"kind\":\"query\""));
    assert!(ocsf.contains("\"class_uid\":6005"));
    // Every native record carries an integrity hash.
    assert!(native.contains("\"hash\":"));
}
```

For `Both`, the OCSF file path is the native path with `.ocsf.jsonl` substituted for the final extension (or appended). Document this derivation in a doc-comment.

- [ ] **Step 4: Run to verify failure.**

Run: `cargo test -p sqe-metrics both_format_writes`
Expected: FAIL.

- [ ] **Step 5: Refactor `AuditLogger`.** Change the worker thread to own a `Vec<Box<dyn AuditSink>>` and a `HashChain`. On each event: stamp the chain, then `write_line` to every sink. Keep batching/flush semantics. Sink set is derived from `AuditFormat`:
  - `Native` -> `[NativeJsonlSink(path)]`
  - `Ocsf` -> `[OcsfJsonlSink(ocsf_path)]`
  - `Both` -> `[NativeJsonlSink(path), OcsfJsonlSink(ocsf_path)]`

Change `AuditMsg::Entry(Box<AuditEntry>)` to `AuditMsg::Event(Box<AuditEvent>)`. Provide `From<AuditEntry> for AuditEvent` (kind = Query, mapping the existing flat fields into `Actor`/`QueryInfo`/`Timing`/`QueryStats`/`PolicyAudit`) so the legacy `log(&AuditEntry)` path keeps compiling and behaving. Redaction (Task 7) and chain stamping run on the worker thread before sinks.

- [ ] **Step 6: Run to verify pass and that the legacy audit tests still pass.**

Run: `cargo test -p sqe-metrics`
Expected: PASS, including all Task 1 moved tests.

- [ ] **Step 7: Wire the format from config at startup.** In `crates/sqe-coordinator/src/main.rs` near line 212, replace `AuditLogger::new(&config.metrics.audit_log_path)` with `AuditLogger::with_config(&config.metrics.audit_log_path, parse_format(&config.metrics.audit.format))`, where `parse_format` maps the string to `AuditFormat` (unknown -> `Native` with a WARN). Repeat in `sqe_server.rs` near line 777.

- [ ] **Step 8: Clippy and commit.**

```bash
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-core/src/config.rs crates/sqe-metrics/src/audit crates/sqe-coordinator/src/main.rs crates/sqe-coordinator/src/sqe_server.rs
git commit -m "feat(audit): config-selectable format and multi-sink fan-out with hash chain"
```

---

## Phase 3: GDPR-tag masking

### Task 7: GDPR column masking function

**Files:**
- Modify: `crates/sqe-metrics/src/audit/redact.rs`
- Test: inline in `redact.rs`

**Interfaces:**
- Produces: `pub enum GdprIdentifierMode { Tokenize, Drop, Keep }` and `pub fn mask_gdpr_columns(sql: &str, masked_columns: &[String], mode: GdprIdentifierMode, salt: &str) -> String`. Always strips literals adjacent to a masked column; the identifier itself is handled per mode.

- [ ] **Step 1: Write the failing tests.**

```rust
#[test]
fn tokenize_hides_value_and_replaces_identifier_stably() {
    let sql = "SELECT id FROM users WHERE email = 'alice@x.io' AND email <> 'bob@x.io'";
    let out = mask_gdpr_columns(sql, &["email".into()], GdprIdentifierMode::Tokenize, "s1");
    assert!(!out.contains("alice@x.io"));
    assert!(!out.contains("bob@x.io"));
    assert!(!out.contains("email"));
    // Same column tokenizes to the same token within one salt (correlatable).
    let token_count = out.matches("col_").count();
    assert_eq!(token_count, 2);
}

#[test]
fn drop_mode_removes_identifier_entirely() {
    let sql = "SELECT email FROM users";
    let out = mask_gdpr_columns(sql, &["email".into()], GdprIdentifierMode::Drop, "s1");
    assert!(!out.contains("email"));
}

#[test]
fn keep_mode_keeps_identifier_but_strips_value() {
    let sql = "SELECT id FROM users WHERE email = 'alice@x.io'";
    let out = mask_gdpr_columns(sql, &["email".into()], GdprIdentifierMode::Keep, "s1");
    assert!(out.contains("email"));
    assert!(!out.contains("alice@x.io"));
}

#[test]
fn non_gdpr_columns_untouched() {
    let sql = "SELECT id FROM users WHERE country = 'NL'";
    let out = mask_gdpr_columns(sql, &["email".into()], GdprIdentifierMode::Tokenize, "s1");
    assert_eq!(out, sql);
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p sqe-metrics tokenize_hides_value`
Expected: FAIL.

- [ ] **Step 3: Implement `mask_gdpr_columns`.** Use a case-insensitive word-boundary regex per masked column. Replace the identifier per mode; in `tokenize`, the replacement is `col_<first 8 hex of sha256(salt+lowercased name)>`. After identifier handling, run `strip_sql_literals` over the whole string only when at least one masked column was present, to guarantee adjacent literal values cannot survive. (Literal stripping is global and conservative; that is acceptable since these queries already touched GDPR data.)

```rust
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GdprIdentifierMode { Tokenize, Drop, Keep }

pub fn mask_gdpr_columns(sql: &str, masked_columns: &[String], mode: GdprIdentifierMode, salt: &str) -> String {
    if masked_columns.is_empty() { return sql.to_string(); }
    let mut out = sql.to_string();
    let mut any = false;
    for col in masked_columns {
        let re = regex::Regex::new(&format!(r"(?i)\b{}\b", regex::escape(col)));
        let re = match re { Ok(r) => r, Err(_) => continue };
        if !re.is_match(&out) { continue; }
        any = true;
        let replacement = match mode {
            GdprIdentifierMode::Keep => col.clone(),
            GdprIdentifierMode::Drop => "[GDPR]".to_string(),
            GdprIdentifierMode::Tokenize => {
                let mut h = Sha256::new();
                h.update(salt.as_bytes());
                h.update(col.to_lowercase().as_bytes());
                let hex = format!("{:x}", h.finalize());
                format!("col_{}", &hex[..8])
            }
        };
        out = re.replace_all(&out, replacement.as_str()).to_string();
    }
    if any { out = strip_sql_literals(&out); }
    out
}
```

- [ ] **Step 4: Run to verify pass.**

Run: `cargo test -p sqe-metrics mask_gdpr`
Expected: PASS. Add `pub use redact::{mask_gdpr_columns, GdprIdentifierMode};` to `mod.rs`.

- [ ] **Step 5: Commit.**

```bash
cargo clippy -p sqe-metrics --all-targets -- -D warnings
git add crates/sqe-metrics/src/audit/redact.rs crates/sqe-metrics/src/audit/mod.rs
git commit -m "feat(audit): GDPR column masking for logged SQL (tokenize/drop/keep)"
```

### Task 8: `TagLookup` seam and writer-thread GDPR masking

**Files:**
- Create: `crates/sqe-metrics/src/audit/tag_lookup.rs`, `crates/sqe-coordinator/src/audit_tag_adapter.rs`
- Modify: `crates/sqe-metrics/src/audit/logger.rs`, `crates/sqe-metrics/src/audit/mod.rs`, `crates/sqe-coordinator/src/main.rs`
- Test: inline in `logger.rs` (with a stub `TagLookup`) and `audit_tag_adapter.rs`

**Interfaces:**
- Produces (sqe-metrics): `pub trait TagLookup: Send + Sync { fn column_tags(&self, catalog: Option<&str>, namespace: &[String], table: &str) -> Option<std::collections::HashMap<String, Vec<String>>>; }` and `NoopTagLookup`. This mirrors `sqe_policy::tag_source::TagSource` exactly, kept in `sqe-metrics` to avoid a `sqe-metrics -> sqe-policy` dependency.
- Produces: `AuditLogger::with_gdpr(self, tags: Vec<String>, mode: GdprIdentifierMode, salt: String, lookup: Arc<dyn TagLookup>) -> Self` (builder applied after `with_config`).
- Consumes (coordinator): the existing `Arc<dyn sqe_policy::tag_source::TagSource>` wrapped by `AuditTagAdapter` implementing `TagLookup`.

- [ ] **Step 1: Write the failing masking-integration test in `logger.rs`.**

```rust
#[test]
fn gdpr_tagged_column_is_masked_in_written_log() {
    use std::collections::HashMap;
    struct Stub;
    impl crate::audit::TagLookup for Stub {
        fn column_tags(&self, _c: Option<&str>, _n: &[String], _t: &str) -> Option<HashMap<String, Vec<String>>> {
            let mut m = HashMap::new();
            m.insert("email".to_string(), vec!["gdpr".to_string()]);
            Some(m)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let logger = AuditLogger::with_config(path.to_str().unwrap(), crate::audit::AuditFormat::Native)
        .unwrap()
        .with_gdpr(vec!["gdpr".into()], crate::audit::GdprIdentifierMode::Tokenize, "salt".into(), std::sync::Arc::new(Stub));
    let mut ev = crate::audit::sample_query_event();
    ev.resources = vec![crate::audit::Resource { catalog: Some("polaris".into()), namespace: vec!["hr".into()], name: "users".into(), object_type: crate::audit::ObjectType::Table }];
    ev.query = Some(crate::audit::QueryInfo { text: Some("SELECT id FROM users WHERE email = 'alice@x.io'".into()), query_hash: "h".into(), statement_type: "query".into() });
    logger.log_event(ev);
    logger.flush();
    let content = std::fs::read_to_string(&path).unwrap();
    assert!(!content.contains("alice@x.io"), "GDPR value leaked: {content}");
    assert!(!content.contains("email"), "GDPR identifier leaked: {content}");
}

#[test]
fn unknown_tag_state_falls_back_to_literal_stripping() {
    use std::collections::HashMap;
    struct Unknown;
    impl crate::audit::TagLookup for Unknown {
        fn column_tags(&self, _c: Option<&str>, _n: &[String], _t: &str) -> Option<HashMap<String, Vec<String>>> { None }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let logger = AuditLogger::with_config(path.to_str().unwrap(), crate::audit::AuditFormat::Native)
        .unwrap()
        .with_gdpr(vec!["gdpr".into()], crate::audit::GdprIdentifierMode::Tokenize, "salt".into(), std::sync::Arc::new(Unknown));
    let mut ev = crate::audit::sample_query_event();
    ev.resources = vec![crate::audit::Resource { catalog: None, namespace: vec![], name: "users".into(), object_type: crate::audit::ObjectType::Table }];
    ev.query = Some(crate::audit::QueryInfo { text: Some("SELECT id FROM users WHERE ssn = '123-45-6789'".into()), query_hash: "h".into(), statement_type: "query".into() });
    logger.log_event(ev);
    logger.flush();
    let content = std::fs::read_to_string(&path).unwrap();
    assert!(!content.contains("123-45-6789"), "literal survived unknown-tag fallback: {content}");
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p sqe-metrics gdpr_tagged_column_is_masked`
Expected: FAIL.

- [ ] **Step 3: Implement `TagLookup` and `NoopTagLookup` in `tag_lookup.rs`** (copy the `TagSource` trait shape and its doc-comment about `None` = unknown = fail closed). Add to `mod.rs` exports.

- [ ] **Step 4: Apply GDPR masking on the worker thread before sinks.** In the worker, for each event with a `query.text`:
  1. Collect masked columns: for each `Resource`, call `lookup.column_tags(...)`. If `Some(map)`, add columns whose tag set intersects `gdpr_tags`. If any resource returns `None`, set a `fallback` flag.
  2. If masked columns is non-empty, run `mask_gdpr_columns(text, &cols, mode, &salt)`.
  3. Always also run the existing `redact_pii`. If `fallback` is set and no columns matched, run `strip_sql_literals` over the text as the conservative path.
  4. Then stamp the chain and write to sinks.

The order matters: GDPR masking and PII redaction happen before chain stamping so the chain covers the redacted bytes. Default (no `with_gdpr`) uses `NoopTagLookup` and an empty tag list, leaving existing behavior unchanged (only `redact_pii`, as today).

- [ ] **Step 5: Run to verify pass.**

Run: `cargo test -p sqe-metrics gdpr`
Expected: PASS.

- [ ] **Step 6: Implement the coordinator adapter.** In `audit_tag_adapter.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use sqe_metrics::audit::TagLookup;
use sqe_policy::tag_source::TagSource;

pub struct AuditTagAdapter(pub Arc<dyn TagSource>);

impl TagLookup for AuditTagAdapter {
    fn column_tags(&self, catalog: Option<&str>, namespace: &[String], table: &str) -> Option<HashMap<String, Vec<String>>> {
        self.0.column_tags(catalog, namespace, table)
    }
}
```

In `main.rs`, after constructing the `AuditLogger` and the policy `TagSource`, apply `.with_gdpr(config.metrics.audit.gdpr_tags.clone(), parse_mode(&config.metrics.audit.gdpr_identifier_mode), audit_salt, Arc::new(AuditTagAdapter(tag_source.clone())))` only when `gdpr_tags` is non-empty. `audit_salt` is read from config or derived once at startup (document: stable within a deployment, correlation aid, not secret-grade).

- [ ] **Step 7: Clippy and commit.**

```bash
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-metrics/src/audit crates/sqe-coordinator/src/audit_tag_adapter.rs crates/sqe-coordinator/src/main.rs
git commit -m "feat(audit): GDPR-tag masking on the audit writer via TagLookup seam"
```

---

## Phase 4: Identity enrichment

### Task 9: Extend `Identity` and extract subject/email/groups claims

**Files:**
- Modify: `crates/sqe-auth/src/provider.rs` (Identity), `crates/sqe-auth/src/bearer_token.rs`, `crates/sqe-auth/src/oidc_provider.rs`, `crates/sqe-core/src/config.rs` (claim-path fields on the bearer/oidc provider config struct, near `user_claim`/`roles_claim`)
- Test: inline in `bearer_token.rs`

**Interfaces:**
- Produces: `Identity { ..., subject: Option<String>, email: Option<String>, groups: Vec<String> }` with the new fields defaulting to `None`/empty so existing constructors compile (use `..Default::default()` where Identity is built in tests, or add the fields to every literal; see Step 3).
- Consumes config: `subject_claim` (default `"sub"`), `email_claim` (default `""`), `groups_claim` (default `""`).

- [ ] **Step 1: Write failing extraction tests in `bearer_token.rs`.**

```rust
#[test]
fn extracts_email_and_groups_when_claims_configured() {
    let claims = serde_json::json!({
        "sub": "u-42",
        "email": "alice@corp.example",
        "groups": ["hr", "finance"],
        "realm_access": { "roles": ["analyst"] }
    });
    assert_eq!(BearerTokenProvider::extract_claim_by_path(&claims, "email"), Some("alice@corp.example".to_string()));
    let groups = BearerTokenProvider::extract_string_list(&claims, "groups");
    assert_eq!(groups, vec!["hr".to_string(), "finance".to_string()]);
}

#[test]
fn empty_groups_claim_yields_no_groups() {
    let claims = serde_json::json!({ "sub": "u-1" });
    assert!(BearerTokenProvider::extract_string_list(&claims, "").is_empty());
}
```

(`extract_string_list` is `extract_roles` generalized; rename or add an alias so groups and roles share one helper. DRY.)

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p sqe-auth extracts_email_and_groups`
Expected: FAIL.

- [ ] **Step 3: Implement.**
  - Add `subject: Option<String>, email: Option<String>, groups: Vec<String>` to `Identity` (provider.rs). Update every `Identity { .. }` literal in the auth crate to set them (subject from `sub` where available, others `None`/empty). Grep: `rg -n 'Identity \{' crates/sqe-auth/src`.
  - Add `subject_claim`, `email_claim`, `groups_claim` to the bearer/oidc provider config with the defaults above (alongside `user_claim`/`roles_claim` near config.rs:45 and the corresponding bearer_token defaults near :73).
  - Generalize `extract_roles` to `extract_string_list(claims, path)` returning `Vec<String>` (empty for empty path). Keep `extract_roles` calling it.
  - In `authenticate` (bearer_token.rs ~529): set `subject = extract_claim_by_path(claims, &cfg.subject_claim)`, `email = (if !cfg.email_claim.is_empty()) extract_claim_by_path(claims, &cfg.email_claim)`, `groups = extract_string_list(claims, &cfg.groups_claim)`.
  - Mirror in `oidc_provider.rs`.

- [ ] **Step 4: Run to verify pass.**

Run: `cargo test -p sqe-auth`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
cargo clippy -p sqe-auth -p sqe-core --all-targets -- -D warnings
git add crates/sqe-auth/src crates/sqe-core/src/config.rs
git commit -m "feat(auth): extract subject, email, and groups claims into Identity"
```

### Task 10: Extend `SessionUser` and thread identity through the session manager

**Files:**
- Modify: `crates/sqe-core/src/session.rs`, `crates/sqe-coordinator/src/session_manager.rs`
- Test: inline in `session.rs`

**Interfaces:**
- Produces: `SessionUser { username, roles, subject: Option<String>, email: Option<String>, groups: Vec<String> }`, plus `Session::new` unchanged in signature, and a new `Session::with_identity(self, subject: Option<String>, email: Option<String>, groups: Vec<String>) -> Self` builder so existing `Session::new(...)` call sites keep working and only the auth path enriches.

- [ ] **Step 1: Write the failing test.**

```rust
#[test]
fn session_carries_enriched_identity() {
    let s = Session::new("alice".into(), SecretString::new("t".into()), None, Utc::now(), vec!["analyst".into()])
        .with_identity(Some("u-1".into()), Some("alice@x.io".into()), vec!["hr".into()]);
    assert_eq!(s.user.subject.as_deref(), Some("u-1"));
    assert_eq!(s.user.email.as_deref(), Some("alice@x.io"));
    assert_eq!(s.user.groups, vec!["hr".to_string()]);
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p sqe-core session_carries_enriched_identity`
Expected: FAIL.

- [ ] **Step 3: Implement.** Add the three fields to `SessionUser` (default empty in `Session::new`). Add `with_identity`:

```rust
#[must_use = "with_identity consumes self; bind the returned Session"]
pub fn with_identity(mut self, subject: Option<String>, email: Option<String>, groups: Vec<String>) -> Self {
    self.user.subject = subject;
    self.user.email = email;
    self.user.groups = groups;
    self
}
```

- [ ] **Step 4: Thread from `Identity` in `session_manager.rs`.** Where the session is constructed from an `Identity`, chain `.with_identity(identity.subject.clone(), identity.email.clone(), identity.groups.clone())`. Grep for `Session::new` in the coordinator to find the construction site.

- [ ] **Step 5: Run to verify pass (workspace build).**

Run: `cargo test -p sqe-core -p sqe-coordinator session`
Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-core/src/session.rs crates/sqe-coordinator/src/session_manager.rs
git commit -m "feat(session): carry subject, email, and groups on SessionUser"
```

### Task 11: Build `Actor` from the session at emit sites

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs` (audit emit near line 1472), `crates/sqe-metrics/src/audit/event.rs` (add a constructor helper)
- Test: inline in `event.rs`

**Interfaces:**
- Produces: `Actor::from_parts(username, subject, email, roles, groups)` (or build inline). The Query emit path now produces a full `AuditEvent` with a populated `Actor` and `Resource`s instead of an `AuditEntry`.

- [ ] **Step 1: Write a failing helper test.**

```rust
#[test]
fn actor_from_parts_populates_all_fields() {
    let a = Actor::from_parts("alice".into(), Some("u1".into()), Some("a@x.io".into()), vec!["r".into()], vec!["g".into()]);
    assert_eq!(a.username, "alice");
    assert_eq!(a.groups, vec!["g".to_string()]);
}
```

- [ ] **Step 2: Implement `from_parts` on `Actor`, then migrate the emit site.** In `query_handler.rs`, replace the `AuditEntry { .. }` construction with an `AuditEvent` of `kind: AuditKind::Query`, building `actor` from `session.user` (username, subject, email, roles, groups), `resources` from Task 12, `policy` from the `PolicySummary`, `timing`/`stats` from the tracker numbers already in scope, and `query` from the SQL and `query_hash`. Call `logger.log_event(event)`. Keep `streaming.rs` and `maintenance.rs` on the `AuditEntry -> AuditEvent` conversion path for now (migrated implicitly by Task 6's `From` impl), or migrate them here if the numbers are readily in scope.

- [ ] **Step 3: Run to verify pass.**

Run: `cargo test -p sqe-coordinator -p sqe-metrics`
Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src/query_handler.rs crates/sqe-metrics/src/audit/event.rs
git commit -m "feat(audit): emit full AuditEvent with enriched Actor on the query path"
```

---

## Phase 5: Resources, new event kinds, result policy, e2e

### Task 12: Fully-qualified resource resolution (table vs view)

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs` (where `tables_touched` is currently built)
- Create helper: `crates/sqe-coordinator/src/audit_resources.rs`
- Test: inline in `audit_resources.rs`

**Interfaces:**
- Produces: `pub fn resources_from_plan(plan: &datafusion::logical_expr::LogicalPlan, default_catalog: Option<&str>) -> Vec<sqe_metrics::audit::Resource>`. Walks the plan for `TableScan` nodes, resolves each `TableReference` to `catalog.namespace.name`, and sets `object_type` by checking whether the resolved relation is a view (via the existing catalog/schema provider that already distinguishes views) or a table.

- [ ] **Step 1: Write the failing test** using a hand-built `LogicalPlan` over a known `TableReference` (`catalog.ns.table`), asserting the produced `Resource` has `catalog=Some("catalog")`, `namespace=["ns"]`, `name="table"`, `object_type=Table`. Use DataFusion test helpers already used elsewhere in the coordinator tests (grep for `LogicalPlanBuilder` usage in `crates/sqe-coordinator`).

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement `resources_from_plan`.** Use `plan.apply` / `TreeNode` traversal (the codebase already traverses plans in the policy rewriter; follow that pattern) to collect `TableScan.table_name: TableReference`. Map `TableReference::{Bare,Partial,Full}` into `catalog`, `namespace`, `name`, filling missing catalog from `default_catalog`. Determine view-vs-table from the resolved provider; if unknown, default to `Table` and note it. Dedup by FQN.

- [ ] **Step 4: Wire it** into the emit site (Task 11) replacing the flat `tables_touched` collection. Keep populating `tables_touched` on the legacy `AuditEntry` path via `resources.iter().map(|r| r.fqn())` so the native JSONL `tables_touched` field stays populated for back-compat consumers (the `AuditEvent` carries the richer `resources`).

- [ ] **Step 5: Run, clippy, commit.**

```bash
cargo test -p sqe-coordinator audit_resources
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src/audit_resources.rs crates/sqe-coordinator/src/query_handler.rs
git commit -m "feat(audit): resolve fully-qualified table/view resources from the plan"
```

### Task 13: Emit Authentication events

**Files:**
- Modify: `crates/sqe-coordinator/src/flight_sql.rs` (auth path, lines ~505-561), and wherever the `AuthChain` resolves success/failure
- Test: extend `crates/sqe-coordinator/tests/it/audit_e2e_test.rs`

**Interfaces:**
- Consumes: `AuditLogger::log_event`, `AuditKind::Auth`.

- [ ] **Step 1: Write a failing e2e test** that performs a failed auth (bad token) and asserts an `AuditKind::Auth` record with `status=failure` lands in the native log, carrying `client_ip` and the provider, but NOT the token.

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement.** At the success branch, emit an `AuditEvent { kind: Auth, outcome: Success, actor: from identity, client_ip, .. }`. At the failure branches (invalid/expired bearer, invalid session, failed JWT validation), emit `outcome: Failure { error_type: Some("AuthFailed"), .. }`. The logger needs to be reachable from the auth path; pass the `Arc<AuditLogger>` into the relevant handler (it already exists in the coordinator). Never include token material in the event.

- [ ] **Step 4: Run, clippy, commit.**

```bash
cargo test -p sqe-coordinator --test it audit
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src/flight_sql.rs crates/sqe-coordinator/tests/it/audit_e2e_test.rs
git commit -m "feat(audit): emit Authentication events on auth success and failure"
```

### Task 14: Emit Session, Grant, AdminDdl, and PolicyDecision events

**Files:**
- Modify: `crates/sqe-coordinator/src/session_manager.rs` (session create/expire), the GRANT/REVOKE handling path (grep `grant_backend` in `crates/sqe-coordinator`), the admin-DDL path (CREATE/DROP CATALOG, secrets, ATTACH/DETACH already partly handled), and the policy enforcement path that produces a deny / breaker-open
- Test: extend `audit_e2e_test.rs`

**Interfaces:**
- Consumes: `AuditLogger::log_event` with `AuditKind::{Session, Grant, AdminDdl, PolicyDecision}`.

- [ ] **Step 1: Write failing e2e assertions** for each: a session-create record (`Session`, success); a `GRANT SELECT ON ... TO role` record (`Grant`, with the grantee and object in `resources`); a `CREATE CATALOG` record (`AdminDdl`); and a policy-denied query record (`PolicyDecision` or Query with `policy.denied=true`). Note the existing e2e test already covers some admin-gate denials; extend rather than duplicate.

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement each emit point.** Reuse `Actor` from the session. For `Grant`, put the target object as a `Resource` and the grantee in the event message or `unmapped` (decide and pin in the test). For `PolicyDecision`, emit when `PolicySummary.denied` or on breaker-open transitions (the breaker logs to tracing today; add an audit emit alongside). Keep these emits cheap and non-blocking (they go through the same channel).

- [ ] **Step 4: Run, clippy, commit.**

```bash
cargo test -p sqe-coordinator --test it audit
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src crates/sqe-coordinator/tests/it/audit_e2e_test.rs
git commit -m "feat(audit): emit Session, Grant, AdminDdl, and PolicyDecision events"
```

### Task 15: `superdebug_log_results` guard

**Files:**
- Modify: `crates/sqe-coordinator/src/main.rs` (startup warning plus enable-audit event), and the result path if a sampling hook is added
- Test: inline config test plus a startup assertion test if feasible

**Interfaces:**
- Consumes: `config.metrics.audit.superdebug_log_results`.

- [ ] **Step 1: Write a failing test** asserting that when `superdebug_log_results = true`, an `AuditKind::AdminDdl` event with a message like `"result logging enabled"` is emitted at startup, and that the default (`false`) emits nothing and logs no result rows anywhere.

- [ ] **Step 2: Implement.** On startup, if the flag is on: emit a loud `tracing::warn!`, and emit an audit `AdminDdl` event recording that result logging is enabled and by which config. Do NOT implement actual result-row capture in A beyond the guard and the warning; A's contract is that no result rows are ever written. (Row capture, if ever wanted, is a separate guarded feature.)

- [ ] **Step 3: Run, clippy, commit.**

```bash
cargo test -p sqe-coordinator superdebug
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src/main.rs
git commit -m "feat(audit): guard and audit the superdebug_log_results escape hatch"
```

### Task 16: Documentation, roadmap, and full regression

**Files:**
- Modify: `README.md` (roadmap checklist), `nextsteps.md`, `docs/` audit/config reference page (grep for where `audit_log_path` is documented), and add a short `docs/` section on the `[audit]` keys and OCSF mapping table
- Test: full workspace test plus the leak-scan

- [ ] **Step 1: Update docs.** Document the `[audit]` config block (`format`, `gdpr_tags`, `gdpr_identifier_mode`, `superdebug_log_results`), the new claim paths (`subject_claim`, `email_claim`, `groups_claim`), the OCSF class mapping table from the spec, and the hash-chain verification note. Honor the no-emdash rule.

- [ ] **Step 2: Update `README.md` roadmap and `nextsteps.md`** per the project's "After Completing Work" convention: mark OCSF audit logging (Sub-project A) done, point NEXT at Sub-project B (OTel/SIEM export).

- [ ] **Step 3: Full regression.**

Run: `cargo test --all`
Run: `cargo clippy --all-targets --all-features -- -D warnings`
Expected: PASS, zero warnings. Confirm `audit_e2e_test.rs` passes with default (`native`) format.

- [ ] **Step 4: No-emdash check on touched docs.**

Run: `grep -rn '—' docs/ README.md nextsteps.md | grep -v '`' || echo clean`
Expected: clean.

- [ ] **Step 5: Commit.**

```bash
git add README.md nextsteps.md docs/
git commit -m "docs(audit): document OCSF audit config, claims, and mapping; update roadmap"
```

---

## Self-Review

**Spec coverage:**
- A1 canonical event: Task 1. A3 OCSF mapping: Tasks 2, 3. Sink list and format: Task 4, 6. Hash chain (A7): Task 5. GDPR masking (A5): Tasks 7, 8. Identity enrichment (A4): Tasks 9, 10, 11. Resources (catalog.namespace.object, table/view): Task 12. New event kinds (A2): Tasks 13, 14. Never-log-results + superdebug (A6): Task 15. Compliance mapping (A8) and config (A2 keys): documented in Task 16; config structs in Task 6 and Task 9. The unauthenticated `/api/v1/queries` gate is correctly deferred to Sub-project C and is noted, not implemented.

**Placeholder scan:** No "TBD"/"TODO" left as deliverables. Task 12 Step 1 references existing test helpers by grep rather than inventing them, which is acceptable (the implementer finds the local pattern). The `ev()` stub in Task 4 Step 1 is explicitly replaced by the shared `sample_query_event()` helper in the same step.

**Type consistency:** `AuditEvent`, `Actor`, `Resource`, `ObjectType`, `Outcome`, `QueryInfo`, `Timing`, `QueryStats`, `PolicyAudit`, `Integrity` are defined in Task 1 and used unchanged thereafter. `to_ocsf` (Task 2) is consumed by `OcsfJsonlSink` (Task 4). `HashChain::stamp` / `verify_chain` (Task 5) are used by the logger (Task 6). `mask_gdpr_columns` / `GdprIdentifierMode` (Task 7) are used by the worker (Task 8). `TagLookup` (Task 8) mirrors `sqe_policy::tag_source::TagSource` exactly. `with_config`, `with_gdpr`, `log_event` on `AuditLogger` are introduced in Task 6 and Task 8 and used consistently. `Session::with_identity` (Task 10) matches its use in Task 10 Step 4 and the enriched `SessionUser` fields read in Task 11.

**Open risk carried from the spec:** exact OCSF sub-field placement (for example which OCSF Datastore Activity field best holds the database identity) is pinned by the golden tests in Tasks 2 and 3; if the implementer finds a more correct OCSF field during implementation, update the golden test and the spec's A3 table together.
