//! Audit event model: [`AuditKind`], [`Actor`], outcome, and the structured
//! `AuditEvent` that every audited operation produces.

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

impl Actor {
    /// Construct an `Actor` from the session user's identity fields.
    ///
    /// All optional fields accept `None` when the underlying identity provider
    /// did not supply them (e.g. a token without an `email` claim).
    pub fn from_parts(
        username: String,
        subject: Option<String>,
        email: Option<String>,
        roles: Vec<String>,
        groups: Vec<String>,
    ) -> Self {
        Self { username, subject, email, roles, groups }
    }
}

/// Shared test fixture: a minimal valid Query event for use across test modules.
#[cfg(test)]
pub(crate) fn sample_query_event() -> AuditEvent {
    use chrono::TimeZone;
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
        stats: Some(QueryStats {
            rows_returned: 10,
            bytes_scanned: 1024,
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
        integrity: Integrity::default(),
    }
}

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
    fn actor_from_parts_populates_all_fields() {
        let a = Actor::from_parts(
            "alice".into(),
            Some("u1".into()),
            Some("a@x.io".into()),
            vec!["r".into()],
            vec!["g".into()],
        );
        assert_eq!(a.username, "alice");
        assert_eq!(a.subject, Some("u1".to_string()));
        assert_eq!(a.email, Some("a@x.io".to_string()));
        assert_eq!(a.roles, vec!["r".to_string()]);
        assert_eq!(a.groups, vec!["g".to_string()]);
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
