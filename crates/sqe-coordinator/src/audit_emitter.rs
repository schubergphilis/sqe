//! One place that turns a finished statement into its audit record.
//!
//! Review 409 counted four copies of the same `AuditEvent` construction in
//! `QueryHandler::execute_inner` (SELECT, GRANT/REVOKE/DENY, secret-bearing
//! statements on the redacting legacy path, everything else) and three more
//! in the streaming finalizer (success, error, cancel). Each copy rebuilt the
//! actor, the outcome, the timing and the query block by hand, so a field
//! added to one was missed in the others. The builders live here; the two
//! call sites hand over what only they know (which statement, how it ended,
//! what it touched) and get one event.
//!
//! Redaction contract, unchanged: SQL text is passed raw in `query.text`.
//! The audit worker thread applies `redact_pii` before chain stamping and
//! writing, and redacting here would double-apply. Secret-bearing kinds
//! (CREATE/DROP/SHOW SECRET, ATTACH, DETACH) stay on the legacy
//! `log(&AuditEntry)` path, which redacts before writing; they never go
//! through `log_event`.

use std::time::Duration;

use sqe_core::{Session, SqeError};
use sqe_metrics::audit::{
    Actor, AuditEntry, AuditEvent, AuditKind, AuditLogger, Integrity, Outcome, PolicyAudit,
    QueryInfo, QueryStats, Resource, Timing,
};
use sqe_policy::PolicySummary;
use sqe_sql::StatementKind;

use crate::query_handler::PlanMetrics;

/// The audit actor for a session: identity, roles, groups and the client
/// application name.
pub(crate) fn actor_for(session: &Session) -> Actor {
    Actor::from_parts(
        session.user.username.clone(),
        session.user.subject.clone(),
        session.user.email.clone(),
        session.user.roles.clone(),
        session.user.groups.clone(),
    )
    .with_app_name(session.source.clone())
}

/// `Success`, or the failure block built from the error's classified code and
/// its client-safe message (never the raw `Display`, which can carry paths
/// and literals the sanitiser suppresses from the client).
pub(crate) fn outcome_of(error: Option<&SqeError>) -> Outcome {
    match error {
        None => Outcome::Success,
        Some(e) => Outcome::Failure {
            error_type: Some(e.error_code().trino_error_type().to_string()),
            error_code: Some(e.error_code().name().to_string()),
            message: Some(e.client_message()),
        },
    }
}

/// The enforcer's summary as the audit policy block. Always emitted for
/// reads and writes, even when every field is zero: the policy decision is
/// recorded explicitly for SIEM completeness.
pub(crate) fn policy_audit(summary: &PolicySummary) -> PolicyAudit {
    PolicyAudit {
        row_filters_applied: summary.row_filters_applied,
        columns_masked: summary.columns_masked.clone(),
        columns_restricted: summary.columns_restricted.clone(),
        denied: summary.denied,
    }
}

/// The query block: raw text (see the module doc on redaction), its hash and
/// the statement kind label.
pub(crate) fn query_info(sql: &str, statement_type: String) -> QueryInfo {
    QueryInfo {
        text: Some(sql.to_string()),
        query_hash: sqe_metrics::audit::query_hash(sql),
        statement_type,
    }
}

/// Timing with the two fields this call site knows. `queued_ms` and
/// `planning_ms` are tracked by `QueryTracker` but not exposed per id here.
pub(crate) fn timing(duration: Duration, execution_ms: u64) -> Timing {
    Timing {
        duration_ms: duration.as_millis() as u64,
        queued_ms: 0,
        planning_ms: 0,
        execution_ms,
    }
}

fn is_dml(kind: &StatementKind) -> bool {
    matches!(
        kind,
        StatementKind::Insert(_)
            | StatementKind::Ctas(_)
            | StatementKind::Merge(_)
            | StatementKind::Delete(_)
            | StatementKind::Update(_)
            | StatementKind::Truncate(_)
    )
}

fn is_secret_bearing(kind: &StatementKind) -> bool {
    matches!(
        kind,
        StatementKind::CreateSecret(_)
            | StatementKind::DropSecret(_)
            | StatementKind::ShowSecrets
            | StatementKind::Attach(_)
            | StatementKind::Detach(_)
    )
}

/// Everything `execute_inner` knows about a finished statement that the
/// audit record needs.
pub(crate) struct StatementAudit<'a> {
    pub session: &'a Session,
    pub sql: &'a str,
    pub kind: &'a StatementKind,
    pub kind_name: String,
    /// `None` on success.
    pub error: Option<&'a SqeError>,
    /// `"success"` or `"error"`; the legacy entry's status column.
    pub status: &'a str,
    pub duration: Duration,
    pub execution_ms: u64,
    pub rows: usize,
    /// Rows a DML statement reported as affected; `None` for reads.
    pub rows_written: Option<u64>,
    pub plan_metrics: &'a PlanMetrics,
    pub policy_summary: Option<PolicySummary>,
    /// Structured resources from the plan; the legacy entry lists their FQNs.
    pub resources: Vec<Resource>,
    pub tables_touched: Vec<String>,
    /// Resources for GRANT/REVOKE/DENY, from the parsed grant statement.
    pub grant_resources: Vec<Resource>,
    pub client_ip: Option<String>,
    pub trace_id: Option<String>,
    pub query_id: uuid::Uuid,
}

/// Emit the one audit record for a buffered statement.
///
/// - `Query` (SELECT and friends): `AuditKind::Query` with policy, timing,
///   stats and the plan's resources.
/// - GRANT / REVOKE / DENY: `AuditKind::Grant` (OCSF Account Change 3001)
///   with the grant target as the resource.
/// - Secret-bearing kinds: the legacy redacting `log(&AuditEntry)` path.
/// - Everything else: DML as `AuditKind::Query` with stats and policy, DDL
///   and admin statements as `AuditKind::AdminDdl`.
pub(crate) fn emit_statement_audit(audit: &AuditLogger, a: StatementAudit<'_>) {
    let ps = a.policy_summary.clone().unwrap_or_default();

    if is_secret_bearing(a.kind) {
        // PII redaction runs inside log() before the entry is written. Never
        // route these through log_event: the legacy path is the established
        // redaction contract for bearer tokens and catalog credentials in SQL.
        audit.log(&AuditEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            username: a.session.user.username.clone(),
            session_id: Some(a.session.id.clone()),
            query_hash: sqe_metrics::audit::query_hash(a.sql),
            query_text: Some(a.sql.to_string()),
            statement_type: a.kind_name,
            duration_ms: a.duration.as_millis() as u64,
            rows_returned: a.rows,
            status: a.status.to_string(),
            client_ip: None,
            tables_touched: a.tables_touched,
            row_filters_applied: ps.row_filters_applied,
            columns_masked: ps.columns_masked,
            columns_restricted: ps.columns_restricted,
            policy_denied: ps.denied,
        });
        return;
    }

    let pm = a.plan_metrics;
    let stats = |rows_written: Option<u64>| QueryStats {
        rows_returned: a.rows,
        rows_written,
        bytes_scanned: pm.bytes_scanned,
        rows_scanned: pm.rows_scanned,
        spill_bytes: pm.spill_bytes,
        peak_memory_bytes: pm.peak_memory_bytes,
    };

    let (kind, resources, policy, timing, stats) = match a.kind {
        StatementKind::Query(_) => (
            AuditKind::Query,
            a.resources,
            Some(policy_audit(&ps)),
            timing(a.duration, a.execution_ms),
            Some(stats(None)),
        ),
        StatementKind::Grant(_) | StatementKind::Revoke(_) | StatementKind::Deny(_) => (
            AuditKind::Grant,
            a.grant_resources,
            None,
            timing(a.duration, 0),
            None,
        ),
        kind if is_dml(kind) => (
            AuditKind::Query,
            a.resources,
            Some(policy_audit(&ps)),
            timing(a.duration, a.execution_ms),
            Some(stats(a.rows_written)),
        ),
        _ => (
            AuditKind::AdminDdl,
            a.resources,
            Some(policy_audit(&ps)),
            timing(a.duration, 0),
            None,
        ),
    };

    audit.log_event(AuditEvent {
        time: chrono::Utc::now(),
        kind,
        actor: actor_for(a.session),
        outcome: outcome_of(a.error),
        resources,
        policy,
        timing: Some(timing),
        stats,
        query: Some(query_info(a.sql, a.kind_name)),
        session_id: Some(a.session.id.clone()),
        client_ip: a.client_ip,
        trace_id: a.trace_id,
        query_id: Some(a.query_id.to_string()),
        integrity: Integrity::default(),
    });
}
