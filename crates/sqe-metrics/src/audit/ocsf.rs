//! Render an `AuditEvent` as an OCSF (Open Cybersecurity Schema Framework)
//! JSON object via [`to_ocsf`].

use super::event::{AuditEvent, AuditKind, ObjectType, Outcome};
use serde_json::{json, Value};

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
    if let Some(sub) = &event.actor.subject {
        user["uid"] = json!(sub);
    }
    if let Some(email) = &event.actor.email {
        user["email_addr"] = json!(email);
    }
    if !groups.is_empty() {
        user["groups"] = json!(groups);
    }
    if !event.actor.roles.is_empty() {
        user["roles"] = json!(event.actor.roles);
    }
    let mut actor = json!({ "user": user });
    if let Some(app_name) = &event.actor.app_name {
        actor["app_name"] = json!(app_name);
    }

    let mut unmapped = serde_json::Map::new();
    if let Some(q) = &event.query {
        unmapped.insert("query_hash".into(), json!(q.query_hash));
        unmapped.insert("statement_type".into(), json!(q.statement_type));
        if let Some(t) = &q.text {
            unmapped.insert("query_text".into(), json!(t));
        }
    }
    if let Some(s) = &event.stats {
        unmapped.insert("bytes_scanned".into(), json!(s.bytes_scanned));
        unmapped.insert("rows_scanned".into(), json!(s.rows_scanned));
        unmapped.insert("spill_bytes".into(), json!(s.spill_bytes));
        unmapped.insert("peak_memory_bytes".into(), json!(s.peak_memory_bytes));
        unmapped.insert("rows_returned".into(), json!(s.rows_returned));
        if let Some(rw) = s.rows_written {
            unmapped.insert("rows_written".into(), json!(rw));
        }
    }
    if let Some(p) = &event.policy {
        unmapped.insert(
            "policy".into(),
            serde_json::to_value(p).unwrap_or(Value::Null),
        );
    }
    if let Some(t) = &event.transaction {
        unmapped.insert(
            "transaction".into(),
            serde_json::to_value(t).unwrap_or(Value::Null),
        );
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
            "sequence": event.integrity.seq,
        },
        "actor": actor,
        "unmapped": Value::Object(unmapped),
    });

    if let Outcome::Failure {
        message: Some(m), ..
    } = &event.outcome
    {
        out["message"] = json!(m);
    }
    if let Some(ip) = &event.client_ip {
        out["src_endpoint"] = json!({ "ip": ip });
    }
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
        AuditKind::Query
        | AuditKind::PolicyDecision
        | AuditKind::Scan
        | AuditKind::Transaction => (6005, 6),
        AuditKind::Auth => (3002, 3),
        AuditKind::Session => (3003, 3),
        AuditKind::Grant => (3001, 3),
        // Maintenance shares AdminDdl's class: both are system/operational
        // change activity rather than a user query or interactive session.
        // AuditResume is a writer-integrity marker, not a user action.
        AuditKind::AdminDdl | AuditKind::Maintenance | AuditKind::AuditResume => (3004, 3),
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
            actor: Actor {
                username: "alice".into(),
                subject: Some("u1".into()),
                email: Some("a@x.io".into()),
                roles: vec!["analyst".into()],
                groups: vec!["hr".into()],
                app_name: Some("mcp:execute_sql_readonly:claude-code/2.1".into()),
            },
            outcome: Outcome::Success,
            resources: vec![Resource {
                catalog: Some("polaris".into()),
                namespace: vec!["hr".into()],
                name: "employees".into(),
                object_type: ObjectType::Table,
            }],
            policy: None,
            timing: Some(Timing {
                duration_ms: 42,
                queued_ms: 0,
                planning_ms: 5,
                execution_ms: 37,
            }),
            stats: Some(QueryStats {
                rows_returned: 10,
                rows_written: None,
                bytes_scanned: 2048,
                rows_scanned: 100,
                spill_bytes: 0,
                peak_memory_bytes: 0,
            }),
            query: Some(QueryInfo {
                text: Some("SELECT 1".into()),
                query_hash: "abc".into(),
                statement_type: "query".into(),
            }),
            session_id: Some("sess-1".into()),
            client_ip: Some("10.0.0.1".into()),
            trace_id: None,
            query_id: None,
            transaction: None,
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
        // The initiating client lands on OCSF's own field for it, not unmapped.
        assert_eq!(v["actor"]["app_name"], "mcp:execute_sql_readonly:claude-code/2.1");
        assert_eq!(v["metadata"]["product"]["name"], "SQE");
        // SQE-specific stats live under enrichments/unmapped, not core fields.
        assert_eq!(v["unmapped"]["bytes_scanned"], 2048);
        assert_eq!(v["unmapped"]["query_hash"], "abc");
    }

    #[test]
    fn failure_outcome_sets_status_id_2() {
        let mut ev = query_event();
        ev.outcome = Outcome::Failure {
            error_type: Some("PlanError".into()),
            error_code: Some("E1".into()),
            message: Some("bad".into()),
        };
        let v = to_ocsf(&ev);
        assert_eq!(v["status_id"], 2);
    }

    #[test]
    fn ocsf_carries_integrity_seq_in_metadata_sequence() {
        use crate::audit::*;
        let mut ev = sample_query_event();
        ev.integrity.seq = 42;
        let v = to_ocsf(&ev);
        assert_eq!(v["metadata"]["sequence"], 42);
    }

    #[test]
    fn auth_session_grant_admin_class_uids() {
        use crate::audit::*;
        use chrono::TimeZone;
        let base = |kind: AuditKind| AuditEvent {
            time: chrono::Utc.with_ymd_and_hms(2026, 6, 20, 12, 0, 0).unwrap(),
            kind,
            actor: Actor {
                username: "bob".into(),
                subject: None,
                email: None,
                roles: vec![],
                groups: vec![],
                app_name: None,
            },
            outcome: Outcome::Success,
            resources: vec![],
            policy: None,
            timing: None,
            stats: None,
            query: None,
            session_id: None,
            client_ip: None,
            trace_id: None,
            query_id: None,
            transaction: None,
            integrity: Integrity::default(),
        };
        // No client label: the key is absent, not null.
        assert!(to_ocsf(&base(AuditKind::Auth))["actor"].get("app_name").is_none());
        assert_eq!(to_ocsf(&base(AuditKind::Auth))["class_uid"], 3002);
        assert_eq!(to_ocsf(&base(AuditKind::Session))["class_uid"], 3003);
        assert_eq!(to_ocsf(&base(AuditKind::Grant))["class_uid"], 3001);
        assert_eq!(to_ocsf(&base(AuditKind::AdminDdl))["class_uid"], 3004);
        // Policy deny rides on Datastore Activity with a Failure status.
        let mut deny = base(AuditKind::PolicyDecision);
        deny.outcome = Outcome::Failure {
            error_type: Some("PolicyDenied".into()),
            error_code: None,
            message: Some("deny-all".into()),
        };
        let v = to_ocsf(&deny);
        assert_eq!(v["class_uid"], 6005);
        assert_eq!(v["status_id"], 2);
    }

    #[test]
    fn transaction_event_rides_on_datastore_activity_with_its_details() {
        let mut ev = query_event();
        ev.kind = AuditKind::Transaction;
        ev.query = None;
        ev.stats = None;
        ev.transaction = Some(TransactionAudit {
            id: "t1".into(),
            action: "commit".into(),
            reason: Some("committed".into()),
            duration_ms: Some(1200),
            published: vec![PublishedSnapshot {
                table: "polaris.hr.employees".into(),
                snapshot_id: 42,
            }],
        });
        let v = to_ocsf(&ev);
        assert_eq!(v["class_uid"], 6005);
        let t = &v["unmapped"]["transaction"];
        assert_eq!(t["id"], "t1");
        assert_eq!(t["action"], "commit");
        assert_eq!(t["reason"], "committed");
        assert_eq!(t["published"][0]["snapshot_id"], 42);
        // Other events carry no transaction key.
        assert!(to_ocsf(&query_event())["unmapped"].get("transaction").is_none());
    }

    proptest::proptest! {
        /// Caller-controlled text (user name, groups, client label, query text,
        /// error message) lands verbatim in its field, through a serialize /
        /// parse round trip, and an absent value is an absent key, never null.
        #[test]
        fn caller_text_is_carried_verbatim(
            username in "\\PC{0,16}",
            subject in proptest::option::of("\\PC{0,8}"),
            groups in proptest::collection::vec("\\PC{0,8}", 0..3),
            app_name in proptest::option::of("\\PC{0,16}"),
            text in proptest::option::of("\\PC{0,32}"),
            message in proptest::option::of(proptest::option::of("\\PC{0,16}")),
            kind in proptest::sample::select(vec![
                AuditKind::Query, AuditKind::Auth, AuditKind::Session, AuditKind::Grant,
                AuditKind::AdminDdl, AuditKind::PolicyDecision, AuditKind::Maintenance,
                AuditKind::Scan, AuditKind::Transaction, AuditKind::AuditResume,
            ]),
        ) {
            use proptest::prop_assert_eq;
            let mut ev = query_event();
            ev.kind = kind.clone();
            ev.actor.username = username.clone();
            ev.actor.subject = subject.clone();
            ev.actor.groups = groups.clone();
            ev.actor.app_name = app_name.clone();
            ev.query.as_mut().unwrap().text = text.clone();
            ev.outcome = match &message {
                None => Outcome::Success,
                Some(m) => Outcome::Failure {
                    error_type: None,
                    error_code: None,
                    message: m.clone(),
                },
            };

            let wire = serde_json::to_string(&to_ocsf(&ev)).unwrap();
            let v: Value = serde_json::from_str(&wire).unwrap();

            prop_assert_eq!(&v["class_uid"], &json!(class_for(&kind).0));
            prop_assert_eq!(&v["status_id"], &json!(if message.is_some() { 2 } else { 1 }));
            let user = &v["actor"]["user"];
            prop_assert_eq!(&user["name"], &json!(username));
            prop_assert_eq!(user.get("uid").cloned(), subject.map(Value::from));
            let got_groups: Vec<_> = user
                .get("groups")
                .map(|g| g.as_array().unwrap().iter().map(|g| g["name"].clone()).collect())
                .unwrap_or_default();
            prop_assert_eq!(got_groups, groups.into_iter().map(Value::from).collect::<Vec<_>>());
            prop_assert_eq!(v["actor"].get("app_name").cloned(), app_name.map(Value::from));
            prop_assert_eq!(v["unmapped"].get("query_text").cloned(), text.map(Value::from));
            prop_assert_eq!(v.get("message").cloned(), message.flatten().map(Value::from));
        }
    }
}
