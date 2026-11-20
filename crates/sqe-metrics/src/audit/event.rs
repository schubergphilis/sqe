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
    /// Advisory/active maintenance subsystem activity (Phase 4a auto-
    /// compaction scheduler): a per-table compaction-debt analysis pass, or
    /// (later) a rewrite job. Never a user query or an interactive admin
    /// action; the actor is always the dedicated maintenance principal.
    Maintenance,
    /// Worker-side scan of a distributed fragment. The ticket does not carry
    /// a user principal, so `query_id` / `trace_id` identify the coordinator
    /// statement that produced the ticket.
    Scan,
    /// An explicit transaction started (its first statement registered it)
    /// or ended: COMMIT, ROLLBACK, a conflict, or a rollback by the
    /// coordinator (idle, lifetime, session end, shutdown). Details in
    /// [`AuditEvent::transaction`].
    Transaction,
    /// Writer resumed after an unparseable tail (torn write or crash mid-line).
    /// `integrity.prev_hash` is `BROKEN:<reason>` so verifiers can distinguish
    /// this from silent truncation back to genesis.
    AuditResume,
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
    /// The client application that initiated the activity, as the wire
    /// protocol reported it: `X-Trino-Source` on Trino HTTP, and
    /// `mcp:<tool>:<client>` on the MCP endpoint. Maps to OCSF
    /// `actor.app_name`. `None` when the protocol carries no such label
    /// (Flight SQL) or the event has no client (maintenance, system).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_name: Option<String>,
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
    /// Byte length of the statement text, set when `omit_query_text` left
    /// the text out of the log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_len: Option<usize>,
    /// Hash of the statement with every literal replaced by a placeholder
    /// ([`super::query_fingerprint`]): the same query shape with different
    /// values shares one fingerprint, where `query_hash` differs per value.
    /// Holds no literal, so it stays when `omit_query_text` drops the text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_fingerprint: Option<String>,
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
    /// Rows written/affected by a DML statement (INSERT/CTAS/UPDATE/DELETE/
    /// MERGE). Omitted from the serialized event for reads and for writes that
    /// reported no count, so SELECT audit records stay unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_written: Option<u64>,
    #[serde(default)]
    pub bytes_scanned: u64,
    #[serde(default)]
    pub rows_scanned: u64,
    #[serde(default)]
    pub spill_bytes: u64,
    #[serde(default)]
    pub peak_memory_bytes: u64,
    /// Keys of the snapshot properties a data write carried into its commit
    /// (`SET SNAPSHOT_PROPERTIES`), sorted. Never the values: they are free
    /// text an operator may treat as sensitive. Omitted when empty, so reads,
    /// writes without properties and every record written before this field
    /// serialise, and hash into the audit chain, byte for byte as before.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshot_property_keys: Vec<String>,
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

/// The transaction an [`AuditKind::Transaction`] event is about.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransactionAudit {
    pub id: String,
    /// `begin`, `commit` or `rollback`.
    pub action: String,
    /// Why it ended: `committed`, `rolled_back`, `conflict`,
    /// `idle_timeout`, `lifetime`, `session_ended` or `shutdown`. Absent on
    /// `begin`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// How long the transaction was open; absent on `begin`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// The snapshot a COMMIT moved each table's `main` to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub published: Vec<PublishedSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedSnapshot {
    pub table: String,
    pub snapshot_id: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Integrity {
    pub seq: u64,
    pub prev_hash: String,
    pub hash: String,
}

/// The catalog and namespace a statement's session resolved to: the session's
/// `USE` / `X-Trino-Catalog` / Flight default, else the configured default.
/// Names where unqualified identifiers pointed, whatever tables the statement
/// touched (a `SELECT 1` touches none, a join can span two catalogs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionScope {
    pub catalog: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
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
    /// W3C trace ID (32 hex chars) for correlation with OTel traces (O4).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    /// SQE internal query ID (uuid v7 string) for joining audit + history + spans.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
    /// Set on [`AuditKind::Transaction`] events only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction: Option<TransactionAudit>,
    /// The session's resolved catalog and namespace (#910). Omitted when
    /// unset (authentication, maintenance, scheduler and system events, and
    /// every record written before this field), so those records serialise,
    /// and hash into the audit chain, byte for byte as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionScope>,
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
        Self {
            username,
            subject,
            email,
            roles,
            groups,
            app_name: None,
        }
    }

    /// Attach the client application label (`Session.source`). Chained after
    /// [`Actor::from_parts`] wherever a session is at hand.
    #[must_use = "with_app_name consumes self; bind the returned Actor"]
    pub fn with_app_name(mut self, app_name: Option<String>) -> Self {
        self.app_name = app_name;
        self
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
            app_name: None,
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
            bytes_scanned: 1024,
            rows_scanned: 100,
            spill_bytes: 0,
            peak_memory_bytes: 0,
            snapshot_property_keys: Vec::new(),
        }),
        query: Some(QueryInfo {
            text: Some("SELECT 1".into()),
            query_hash: "abc".into(),
            statement_type: "query".into(),
            text_len: None,
            query_fingerprint: None,
        }),
        session_id: Some("sess-1".into()),
        client_ip: Some("10.0.0.1".into()),
        trace_id: Some("0af7651916cd43dd8448eb211c80319c".into()),
        query_id: Some("019f510a-a20f-74f1-81cd-d1d22ae2e18b".into()),
        transaction: None,
        session: None,
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
                app_name: None,
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
                bytes_scanned: 1024,
                rows_scanned: 100,
                spill_bytes: 0,
                peak_memory_bytes: 0,
                snapshot_property_keys: Vec::new(),
            }),
            query: Some(QueryInfo {
                text: Some("SELECT 1".into()),
                query_hash: "abc".into(),
                statement_type: "query".into(),
                text_len: None,
                query_fingerprint: None,
            }),
            session_id: Some("sess-1".into()),
            client_ip: Some("10.0.0.1".into()),
            trace_id: None,
            query_id: None,
            transaction: None,
            session: None,
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

    /// #910: a record without a session scope serialises without the key, so
    /// it hashes into the audit chain as it always did; with one it carries
    /// the object and round-trips.
    #[test]
    fn session_scope_is_omitted_when_unset_and_round_trips_when_set() {
        let ev = sample_query_event();
        let json = serde_json::to_string(&ev).unwrap();
        assert!(!json.contains("\"session\""), "{json}");
        // A line written before the field existed still reads.
        let back: AuditEvent = serde_json::from_str(&json).unwrap();
        assert!(back.session.is_none());

        let mut ev = sample_query_event();
        ev.session = Some(SessionScope {
            catalog: "finance".into(),
            namespace: Some("gold".into()),
        });
        let json = serde_json::to_string(&ev).unwrap();
        assert!(
            json.contains("\"session\":{\"catalog\":\"finance\",\"namespace\":\"gold\"}"),
            "{json}"
        );
        let back: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.session, ev.session);

        ev.session = Some(SessionScope {
            catalog: "finance".into(),
            namespace: None,
        });
        let json = serde_json::to_string(&ev).unwrap();
        assert!(
            json.contains("\"session\":{\"catalog\":\"finance\"}"),
            "{json}"
        );
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

    /// Empty keys leave the serialised stats exactly as they were before the
    /// field existed: the literal below is the pre-field output, and the chain
    /// hash is taken over these bytes, so existing logs keep verifying.
    #[test]
    fn stats_without_snapshot_property_keys_serialise_as_before() {
        let ev = sample_query_event();
        let stats = serde_json::to_string(ev.stats.as_ref().unwrap()).unwrap();
        assert_eq!(
            stats,
            r#"{"rows_returned":10,"bytes_scanned":1024,"rows_scanned":100,"spill_bytes":0,"peak_memory_bytes":0}"#
        );
        assert!(!serde_json::to_string(&ev)
            .unwrap()
            .contains("snapshot_property_keys"));
    }

    #[test]
    fn stats_with_snapshot_property_keys_round_trip() {
        let mut ev = sample_query_event();
        ev.stats.as_mut().unwrap().snapshot_property_keys =
            vec!["copy.run".into(), "etl.job".into()];
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains(r#""snapshot_property_keys":["copy.run","etl.job"]"#));
        let back: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(
            back.stats.unwrap().snapshot_property_keys,
            vec!["copy.run".to_string(), "etl.job".to_string()]
        );
        // A record written before the field existed still parses.
        let old = r#"{"rows_returned":1}"#;
        let stats: QueryStats = serde_json::from_str(old).unwrap();
        assert!(stats.snapshot_property_keys.is_empty());
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
