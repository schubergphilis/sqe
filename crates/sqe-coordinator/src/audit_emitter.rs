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

use sqe_core::snapshot_properties::SnapshotProperties;
use sqe_core::{Session, SqeError};
use sqe_metrics::audit::{
    Actor, AuditEntry, AuditEvent, AuditKind, AuditLogger, Integrity, Outcome, PolicyAudit,
    QueryInfo, QueryStats, Resource, Timing,
};
use sqe_policy::PolicySummary;
use sqe_sql::StatementKind;

use crate::query_handler::PlanMetrics;
use crate::session_manager::session_log_id;

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

/// The query block: raw text (see the module doc on redaction), its hash,
/// its literal-free fingerprint and the statement kind label.
pub(crate) fn query_info(sql: &str, statement_type: String) -> QueryInfo {
    QueryInfo {
        text: Some(sql.to_string()),
        query_hash: sqe_metrics::audit::query_hash(sql),
        statement_type,
        text_len: None,
        query_fingerprint: Some(sqe_metrics::audit::query_fingerprint(sql)),
    }
}

/// Timing with the two fields the buffered path knows. It plans and runs in
/// one call, so `queued_ms` and `planning_ms` stay 0; the streaming path
/// fills them from its `QueryTracker` record.
pub(crate) fn timing(duration: Duration, execution_ms: u64) -> Timing {
    Timing {
        duration_ms: duration.as_millis() as u64,
        queued_ms: 0,
        planning_ms: 0,
        execution_ms,
    }
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

/// The snapshot property keys `kind`'s commit carries, sorted, for the
/// audit record: the keys only, never the values. Empty unless `kind` is a
/// data write (INSERT, CTAS, UPDATE, DELETE, MERGE, TRUNCATE) and `props`
/// holds something, so a read in a scope that has properties lists none.
pub(crate) fn property_keys(
    kind: &StatementKind,
    props: Option<&SnapshotProperties>,
) -> Vec<String> {
    match props {
        Some(props) if kind.is_data_write() => props.iter().map(|(k, _)| k.to_string()).collect(),
        _ => Vec::new(),
    }
}

/// [`property_keys`] for the properties of the current statement scope
/// (`sqe_catalog::txn::write_scope`). Call it inside that scope: the value
/// is a task-local and is gone outside the statement's task.
pub(crate) fn snapshot_property_keys_for(kind: &StatementKind) -> Vec<String> {
    property_keys(kind, sqe_catalog::txn::snapshot_properties().as_deref())
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
    /// Keys of the snapshot properties the write carried into its commit,
    /// from [`snapshot_property_keys_for`]; empty for everything else. Only
    /// a data write's stats block lists them.
    pub snapshot_property_keys: Vec<String>,
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

/// Emit the one audit record for a statement that failed before the
/// statement's normal audit point: parse, catalog preflight, planning,
/// access denied at planning, and Flight SQL `GetFlightInfo` /
/// `CreatePreparedStatement` planning, which never reach `execute`.
///
/// `kind` is `None` when the SQL did not classify (a parse error). A
/// classified statement goes through [`emit_statement_audit`] exactly as an
/// execution failure does, so routing (secret-bearing kinds stay on the
/// redacting legacy path), redaction and the OCSF mapping are unchanged. An
/// unparsed statement has no kind to route on; it is recorded as a failed
/// `AuditKind::Query` with `statement_type` `"unknown"`, and its text gets
/// the worker's `redact_pii` like every canonical event.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_failed_statement(
    audit: &AuditLogger,
    session: &Session,
    sql: &str,
    kind: Option<&StatementKind>,
    error: &SqeError,
    duration: Duration,
    client_ip: Option<String>,
    query_id: uuid::Uuid,
) {
    let trace_id = sqe_metrics::propagation::current_trace_id();
    let Some(kind) = kind else {
        audit.log_event(AuditEvent {
            time: chrono::Utc::now(),
            kind: AuditKind::Query,
            actor: actor_for(session),
            outcome: outcome_of(Some(error)),
            resources: Vec::new(),
            policy: None,
            timing: Some(timing(duration, 0)),
            stats: None,
            query: Some(query_info(sql, "unknown".to_string())),
            session_id: Some(session_log_id(&session.id)),
            client_ip,
            trace_id,
            query_id: Some(query_id.to_string()),
            transaction: None,
            integrity: Integrity::default(),
        });
        return;
    };
    emit_statement_audit(
        audit,
        StatementAudit {
            session,
            sql,
            kind,
            kind_name: kind.name().to_string(),
            error: Some(error),
            status: "error",
            duration,
            execution_ms: 0,
            rows: 0,
            rows_written: None,
            // Failed before the write started: nothing was carried.
            snapshot_property_keys: Vec::new(),
            plan_metrics: &PlanMetrics::default(),
            policy_summary: None,
            resources: Vec::new(),
            tables_touched: Vec::new(),
            grant_resources: Vec::new(),
            client_ip,
            trace_id,
            query_id,
        },
    );
}

/// Bytes of an oversized statement's text kept in its audit record.
pub(crate) const OVERSIZED_TEXT_BYTES: usize = 1024;

/// The first [`OVERSIZED_TEXT_BYTES`] of `sql`, cut on a char boundary and
/// never inside a string literal. The audit worker redacts secrets and PII
/// by matching whole quoted literals (`redact_pii`); a literal cut open
/// would no longer match and its value would be written in the clear. An
/// odd number of quotes means the cut is inside one (an escaped `''` adds
/// two), so the prefix stops before the last opening quote.
pub(crate) fn oversized_text_prefix(sql: &str) -> &str {
    let mut cut = sql.len().min(OVERSIZED_TEXT_BYTES);
    while !sql.is_char_boundary(cut) {
        cut -= 1;
    }
    let prefix = &sql[..cut];
    if prefix.bytes().filter(|b| *b == b'\'').count() % 2 == 1 {
        let open = prefix.rfind('\'').unwrap_or(0);
        return &prefix[..open];
    }
    prefix
}

/// Emit the one audit record for a statement refused for its size
/// (`query.max_sql_bytes`, #403 / #882).
///
/// The statement is never classified: parsing it is exactly what the size
/// cap prevents. It is recorded like an unparsed statement (`statement_type`
/// `"unknown"`), with its text cut to [`OVERSIZED_TEXT_BYTES`] on a char
/// boundary and its full byte length in `text_len`. `query_hash` covers the
/// full text, so repeated refusals of one statement still correlate; the
/// literal-free fingerprint is left out.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_oversized_statement(
    audit: &AuditLogger,
    session: &Session,
    sql: &str,
    error: &SqeError,
    duration: Duration,
    client_ip: Option<String>,
    query_id: uuid::Uuid,
) {
    let text = oversized_text_prefix(sql);
    audit.log_event(AuditEvent {
        time: chrono::Utc::now(),
        kind: AuditKind::Query,
        actor: actor_for(session),
        outcome: outcome_of(Some(error)),
        resources: Vec::new(),
        policy: None,
        timing: Some(timing(duration, 0)),
        stats: None,
        query: Some(QueryInfo {
            text: Some(text.to_string()),
            query_hash: sqe_metrics::audit::query_hash(sql),
            statement_type: "unknown".to_string(),
            text_len: Some(sql.len()),
            query_fingerprint: None,
        }),
        session_id: Some(session_log_id(&session.id)),
        client_ip,
        trace_id: sqe_metrics::propagation::current_trace_id(),
        query_id: Some(query_id.to_string()),
        transaction: None,
        integrity: Integrity::default(),
    });
}

/// What a Flight SQL bulk ingest (`CommandStatementIngest`) leaves for its
/// audit record. Filled as the request is read, so a refusal records what
/// was known when it failed.
pub(crate) struct IngestAudit {
    /// The target table as named in the request.
    pub table: Resource,
    /// `if_not_exist=..., if_exists=...`, once the options were read.
    pub mode: Option<String>,
    /// Keys of the snapshot properties the commit carries, sorted. Set once
    /// the options were resolved; recorded on success only, like an INSERT.
    pub snapshot_property_keys: Vec<String>,
}

impl IngestAudit {
    /// The statement text of the record: the INSERT the ingest stands for
    /// (the same text admission uses), the mode in a comment.
    fn text(&self) -> String {
        match &self.mode {
            Some(mode) => format!(
                "INSERT INTO {} /* flight sql ingest: {mode} */",
                self.table.fqn()
            ),
            None => format!("INSERT INTO {} /* flight sql ingest */", self.table.fqn()),
        }
    }
}

/// Emit the one audit record for a Flight SQL bulk ingest (#887): a data
/// write recorded like an INSERT (`AuditKind::Query`, the target table as
/// its resource, `rows_written`, the snapshot property keys), with
/// `statement_type` `"ingest"`. Every ingest request that passed
/// authentication leaves exactly one, whatever it ended with.
pub(crate) fn emit_ingest_audit(
    audit: &AuditLogger,
    session: &Session,
    ingest: &IngestAudit,
    result: Result<u64, &SqeError>,
    duration: Duration,
    client_ip: Option<String>,
    query_id: uuid::Uuid,
) {
    let (error, rows_written, keys) = match result {
        Ok(rows) => (None, Some(rows), ingest.snapshot_property_keys.clone()),
        Err(e) => (Some(e), None, Vec::new()),
    };
    audit.log_event(AuditEvent {
        time: chrono::Utc::now(),
        kind: AuditKind::Query,
        actor: actor_for(session),
        outcome: outcome_of(error),
        resources: vec![ingest.table.clone()],
        policy: None,
        timing: Some(timing(duration, duration.as_millis() as u64)),
        stats: Some(QueryStats {
            rows_written,
            snapshot_property_keys: keys,
            ..QueryStats::default()
        }),
        query: Some(query_info(&ingest.text(), "ingest".to_string())),
        session_id: Some(session_log_id(&session.id)),
        client_ip,
        trace_id: sqe_metrics::propagation::current_trace_id(),
        query_id: Some(query_id.to_string()),
        transaction: None,
        integrity: Integrity::default(),
    });
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
            session_id: Some(session_log_id(&a.session.id)),
            query_hash: sqe_metrics::audit::query_hash(a.sql),
            query_text: Some(a.sql.to_string()),
            query_text_len: None,
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
    let stats = |rows_written: Option<u64>, snapshot_property_keys: Vec<String>| QueryStats {
        rows_returned: a.rows,
        rows_written,
        bytes_scanned: pm.bytes_scanned,
        rows_scanned: pm.rows_scanned,
        spill_bytes: pm.spill_bytes,
        peak_memory_bytes: pm.peak_memory_bytes,
        snapshot_property_keys,
    };

    let (kind, resources, policy, timing, stats) = match a.kind {
        StatementKind::Query(_) => (
            AuditKind::Query,
            a.resources,
            Some(policy_audit(&ps)),
            timing(a.duration, a.execution_ms),
            Some(stats(None, Vec::new())),
        ),
        StatementKind::Grant(_)
        | StatementKind::GrantWithPolicy(_)
        | StatementKind::Revoke(_)
        | StatementKind::Deny(_) => (
            AuditKind::Grant,
            a.grant_resources,
            None,
            timing(a.duration, 0),
            None,
        ),
        kind if kind.is_data_write() => (
            AuditKind::Query,
            a.resources,
            Some(policy_audit(&ps)),
            timing(a.duration, a.execution_ms),
            Some(stats(a.rows_written, a.snapshot_property_keys)),
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
        session_id: Some(session_log_id(&a.session.id)),
        client_ip: a.client_ip,
        trace_id: a.trace_id,
        query_id: Some(a.query_id.to_string()),
        transaction: None,
        integrity: Integrity::default(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn props(pairs: &[(&str, &str)]) -> Arc<SnapshotProperties> {
        Arc::new(
            SnapshotProperties::try_new(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())))
                .unwrap(),
        )
    }

    #[test]
    fn oversized_text_prefix_is_bounded_and_never_cuts_a_literal_open() {
        let short = "SELECT 1";
        assert_eq!(oversized_text_prefix(short), short);

        let long = format!("SELECT {}", "x".repeat(4 * OVERSIZED_TEXT_BYTES));
        assert_eq!(oversized_text_prefix(&long).len(), OVERSIZED_TEXT_BYTES);

        // The cut lands inside the secret literal: the prefix stops before it.
        let head = "CREATE SECRET s (TYPE s3, SECRET '";
        let secret = format!("{head}{}')", "k".repeat(2 * OVERSIZED_TEXT_BYTES));
        let prefix = oversized_text_prefix(&secret);
        assert_eq!(prefix, &head[..head.len() - 1]);
        assert!(!prefix.contains('k'));

        // A closed literal with an escaped quote stays.
        let closed = format!("SELECT 'it''s' {}", "y".repeat(2 * OVERSIZED_TEXT_BYTES));
        assert!(oversized_text_prefix(&closed).starts_with("SELECT 'it''s' y"));

        // Multibyte text is cut on a char boundary.
        let wide = "é".repeat(OVERSIZED_TEXT_BYTES);
        let prefix = oversized_text_prefix(&wide);
        assert!(prefix.len() <= OVERSIZED_TEXT_BYTES && prefix.chars().all(|c| c == 'é'));
    }

    #[test]
    fn property_keys_are_sorted_keys_for_data_writes_only() {
        let set = props(&[("etl.job", "secret-value"), ("copy.run", "42")]);
        for sql in [
            "INSERT INTO t VALUES (1)",
            "CREATE TABLE t2 AS SELECT 1 AS a",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "TRUNCATE TABLE t",
        ] {
            let kind = sqe_sql::parse_and_classify(sql).expect(sql);
            assert!(kind.is_data_write(), "{sql}");
            assert_eq!(
                property_keys(&kind, Some(&set)),
                vec!["copy.run".to_string(), "etl.job".to_string()],
                "{sql}"
            );
            assert!(property_keys(&kind, None).is_empty(), "{sql}");
        }
        for sql in ["SELECT 1", "SHOW TABLES", "CREATE SCHEMA s"] {
            let kind = sqe_sql::parse_and_classify(sql).expect(sql);
            assert!(property_keys(&kind, Some(&set)).is_empty(), "{sql}");
        }
    }

    /// The audit record of a data write that ran in a scope with snapshot
    /// properties lists the keys and never a value; a read in the same scope
    /// lists none.
    #[tokio::test]
    async fn audit_lists_snapshot_properties_keys_not_values_for_a_write_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("audit.jsonl");
        let audit = AuditLogger::new(path.to_str().unwrap()).expect("audit logger");
        let session = Session::new(
            "alice".to_string(),
            sqe_core::SecretString::new("access_tok".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        );
        let set = props(&[("etl.job", "never-in-the-log"), ("copy.run", "42")]);

        sqe_catalog::txn::write_scope(None, None, Some(set), async {
            for sql in ["INSERT INTO t VALUES (1)", "SELECT 1"] {
                let kind = sqe_sql::parse_and_classify(sql).expect(sql);
                emit_statement_audit(
                    &audit,
                    StatementAudit {
                        session: &session,
                        sql,
                        kind: &kind,
                        kind_name: kind.name().to_string(),
                        error: None,
                        status: "success",
                        duration: Duration::from_millis(1),
                        execution_ms: 1,
                        rows: 1,
                        rows_written: Some(1),
                        snapshot_property_keys: snapshot_property_keys_for(&kind),
                        plan_metrics: &PlanMetrics::default(),
                        policy_summary: None,
                        resources: Vec::new(),
                        tables_touched: Vec::new(),
                        grant_resources: Vec::new(),
                        client_ip: None,
                        trace_id: None,
                        query_id: uuid::Uuid::now_v7(),
                    },
                );
            }
        })
        .await;
        audit.flush();

        let content = std::fs::read_to_string(&path).expect("audit file");
        assert!(
            !content.contains("never-in-the-log") && !content.contains("\"42\""),
            "a snapshot property value reached the audit log: {content}"
        );
        let events: Vec<serde_json::Value> = content
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .filter(|v: &serde_json::Value| v.get("stats").is_some())
            .collect();
        assert_eq!(events.len(), 2, "{content}");
        let insert = events
            .iter()
            .find(|v| v["query"]["statement_type"] == "insert")
            .expect("insert record");
        assert_eq!(
            insert["stats"]["snapshot_property_keys"],
            serde_json::json!(["copy.run", "etl.job"])
        );
        let select = events
            .iter()
            .find(|v| v["query"]["statement_type"] != "insert")
            .expect("select record");
        assert!(
            select["stats"].get("snapshot_property_keys").is_none(),
            "{select}"
        );
    }

    /// A Flight session id is a bearer credential: the audit record carries
    /// its hash, never the raw value.
    #[test]
    fn audit_event_carries_hashed_session_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("audit.jsonl");
        let audit = AuditLogger::new(path.to_str().unwrap()).expect("audit logger");
        let session = Session::new(
            "alice".to_string(),
            sqe_core::SecretString::new("access_tok".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        );
        let error = SqeError::with_code(sqe_core::SqeErrorCode::InternalError, "boom");

        emit_failed_statement(
            &audit,
            &session,
            "SELEC 1",
            None,
            &error,
            Duration::from_millis(1),
            None,
            uuid::Uuid::now_v7(),
        );
        audit.flush();

        let content = std::fs::read_to_string(&path).expect("audit file");
        assert!(
            !content.contains(&session.id),
            "raw session id leaked into audit: {content}"
        );
        assert!(
            content.contains(&session_log_id(&session.id)),
            "hashed session id missing from audit: {content}"
        );
    }
}
