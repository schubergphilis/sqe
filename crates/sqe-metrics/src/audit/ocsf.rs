use serde_json::{json, Value};
use super::event::{AuditEvent, AuditKind, ObjectType, Outcome};

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

    if let Outcome::Failure { message: Some(m), .. } = &event.outcome {
        out["message"] = json!(m);
    }
    if let Some(ip) = &event.client_ip { out["src_endpoint"] = json!({ "ip": ip }); }
    if !event.resources.is_empty() {
        let resources: Vec<Value> = event.resources.iter().map(|r| json!({
            "name": r.fqn(),
            "type": match r.object_type { ObjectType::Table => "Table", ObjectType::View => "View" },
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
