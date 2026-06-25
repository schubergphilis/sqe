pub mod adaptive_sort;
pub mod auth_session;
pub mod audit_resources;
pub mod audit_tag_adapter;
pub mod metrics_history;
pub mod policy_wiring;
pub mod tag_source_impl;
pub mod catalog_ops;
pub mod channel_pool;
pub mod maintenance;
pub mod tls;
pub mod transport;
pub mod codec;
pub mod credential_refresh;
pub mod distributed_scan;
pub mod explain;
pub mod flight_sql;
pub mod flight_sql_helpers;
pub mod memory;
pub mod mode;
pub mod quack_executor;
pub mod query_handler;
pub mod runtime;
pub mod runtime_catalog;
pub mod session_context;
pub mod query_cache;
pub mod query_tracker;
pub mod rate_limiter;
pub mod scan_pushdown;
pub mod scheduler;
pub mod streaming;
pub mod suggest_bloom;
pub mod session_manager;
pub mod web_auth;
pub mod web_ui;
pub mod worker_registry;
pub mod write_handler;
pub mod writer;

pub use mode::Mode;
pub use quack_executor::CoordinatorExecutor;
pub use query_handler::QueryHandler;
pub use runtime_catalog::{AttachedCatalog, RuntimeCatalogRegistry};
pub use session_manager::SessionManager;

/// Parse the `audit.gdpr_identifier_mode` config string.
///
/// Accepts "tokenize", "drop", or "keep" (case-insensitive).
/// Any unknown value falls back to `Tokenize` (most privacy-preserving).
pub fn parse_gdpr_mode(s: &str) -> sqe_metrics::audit::GdprIdentifierMode {
    match s.to_lowercase().as_str() {
        "drop" => sqe_metrics::audit::GdprIdentifierMode::Drop,
        "keep" => sqe_metrics::audit::GdprIdentifierMode::Keep,
        "tokenize" => sqe_metrics::audit::GdprIdentifierMode::Tokenize,
        other => {
            tracing::warn!(
                mode = other,
                "Unknown audit.gdpr_identifier_mode value; falling back to \"tokenize\""
            );
            sqe_metrics::audit::GdprIdentifierMode::Tokenize
        }
    }
}

/// Parse the `audit.format` config string into an `AuditFormat` enum value.
///
/// Accepts "native", "ocsf", or "both" (case-insensitive). Any unknown value
/// falls back to `Native` with a warning, preserving existing behavior.
pub fn parse_audit_format(s: &str) -> sqe_metrics::audit::AuditFormat {
    match s.to_lowercase().as_str() {
        "ocsf" => sqe_metrics::audit::AuditFormat::Ocsf,
        "both" => sqe_metrics::audit::AuditFormat::Both,
        "native" => sqe_metrics::audit::AuditFormat::Native,
        other => {
            tracing::warn!(
                format = other,
                "Unknown audit.format value; falling back to \"native\""
            );
            sqe_metrics::audit::AuditFormat::Native
        }
    }
}

/// Guard for the `metrics.audit.superdebug_log_results` flag.
///
/// When the flag is enabled, this function:
/// 1. Emits a loud `tracing::warn!` so the operator cannot miss the non-compliance.
/// 2. Emits a canonical `AdminDdl` audit event so the enabling action is itself
///    recorded in the audit trail (config change = auditable admin action).
///
/// When the flag is false (the default) this function is a no-op.
///
/// Contract: result rows are NEVER written to any audit sink. `AuditEvent` has no
/// field for result data. This function only guards and audits the flag itself.
///
/// The self-audit event is a best-effort record. When `audit_log_path` is empty
/// the logger is a no-op, so the event drops silently while the `warn!` still fires.
pub fn maybe_warn_superdebug(audit: &sqe_metrics::audit::AuditLogger, config: &sqe_core::SqeConfig) {
    if !config.metrics.audit.superdebug_log_results {
        return;
    }

    tracing::warn!(
        "WARNING: metrics.audit.superdebug_log_results = true -- \
         result-row logging is enabled. This flag is NON-COMPLIANT for production \
         deployments and violates SOC2, ISO 27001, and GDPR data-minimization \
         requirements. Disable it before going to production."
    );

    let event = sqe_metrics::audit::AuditEvent {
        time: chrono::Utc::now(),
        kind: sqe_metrics::audit::AuditKind::AdminDdl,
        actor: sqe_metrics::audit::Actor::from_parts(
            "system".into(),
            None,
            None,
            vec![],
            vec![],
        ),
        outcome: sqe_metrics::audit::Outcome::Success,
        resources: vec![],
        policy: None,
        timing: None,
        stats: None,
        query: Some(sqe_metrics::audit::QueryInfo {
            text: Some(
                "superdebug_log_results enabled via metrics.audit.superdebug_log_results = true".into(),
            ),
            query_hash: sqe_metrics::audit::query_hash("superdebug_log_results_enabled"),
            statement_type: "superdebug_log_results_enabled".into(),
        }),
        session_id: None,
        client_ip: None,
        integrity: sqe_metrics::audit::Integrity::default(),
    };
    audit.log_event(event);
}

/// Derive the spool path for the audit export pipeline.
///
/// When `configured_spool_path` is non-empty it is used directly.
/// Otherwise the spool is placed adjacent to the main audit log:
/// `<audit_log_path>.ocsf.spool.jsonl`.
pub fn derive_spool_path(configured_spool_path: &str, audit_log_path: &str) -> String {
    if !configured_spool_path.is_empty() {
        configured_spool_path.to_string()
    } else {
        format!("{audit_log_path}.ocsf.spool.jsonl")
    }
}

/// Parse the `audit_export.start_at` config string into a `StartAt` enum.
///
/// Accepts "beginning" (case-insensitive). Anything else (including the
/// default "now") maps to `StartAt::Now`.
pub fn parse_start_at(s: &str) -> sqe_metrics::audit::export::StartAt {
    if s.eq_ignore_ascii_case("beginning") {
        sqe_metrics::audit::export::StartAt::Beginning
    } else {
        sqe_metrics::audit::export::StartAt::Now
    }
}

#[cfg(test)]
mod spool_path_tests {
    use super::{derive_spool_path, parse_start_at};
    use sqe_metrics::audit::export::StartAt;

    #[test]
    fn derive_spool_path_uses_configured_when_set() {
        let result = derive_spool_path("/custom/spool.jsonl", "/var/log/audit.jsonl");
        assert_eq!(result, "/custom/spool.jsonl");
    }

    #[test]
    fn derive_spool_path_defaults_to_audit_log_adjacent() {
        let result = derive_spool_path("", "/var/log/sqe-audit.jsonl");
        assert_eq!(result, "/var/log/sqe-audit.jsonl.ocsf.spool.jsonl");
    }

    #[test]
    fn derive_spool_path_both_empty_gives_relative_path() {
        let result = derive_spool_path("", "");
        assert_eq!(result, ".ocsf.spool.jsonl");
    }

    #[test]
    fn parse_start_at_now_default() {
        assert_eq!(parse_start_at("now"), StartAt::Now);
        assert_eq!(parse_start_at(""), StartAt::Now);
        assert_eq!(parse_start_at("unknown"), StartAt::Now);
    }

    #[test]
    fn parse_start_at_beginning_case_insensitive() {
        assert_eq!(parse_start_at("beginning"), StartAt::Beginning);
        assert_eq!(parse_start_at("BEGINNING"), StartAt::Beginning);
        assert_eq!(parse_start_at("Beginning"), StartAt::Beginning);
    }
}

/// Test-only re-exports used by integration tests under `tests/`.
///
/// Kept behind a sentinel name so accidental use in production code
/// stands out in review.
#[doc(hidden)]
pub mod __test_support {
    use iceberg::spec::Schema as IcebergSchema;
    use sqe_core::Result;

    pub fn sql_type_to_arrow_public(
        sql_type: &sqlparser::ast::DataType,
    ) -> Result<arrow_schema::DataType> {
        crate::write_handler::sql_type_to_arrow(sql_type)
    }

    /// Build an Iceberg schema from a parsed `CREATE TABLE`, applying DEFAULT
    /// literals and preserving nanosecond timestamp mappings.
    pub fn build_iceberg_schema_with_defaults(
        ct: &sqlparser::ast::CreateTable,
    ) -> Result<IcebergSchema> {
        use arrow_schema::{Field, Schema as ArrowSchema};

        let arrow_fields: Vec<Field> = ct
            .columns
            .iter()
            .map(|col| {
                let arrow_type = sql_type_to_arrow_public(&col.data_type)?;
                let nullable = !col
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
                Ok(Field::new(col.name.value.clone(), arrow_type, nullable))
            })
            .collect::<Result<Vec<_>>>()?;
        let arrow_schema = ArrowSchema::new(arrow_fields);
        crate::write_handler::arrow_schema_to_iceberg_with_defaults(&arrow_schema, &ct.columns)
    }

    /// Report whether a `CREATE TABLE` would require Iceberg format-version 3.
    pub fn needs_v3(ct: &sqlparser::ast::CreateTable) -> Result<bool> {
        let schema = build_iceberg_schema_with_defaults(ct)?;
        Ok(crate::write_handler::requires_v3_features(&ct.columns, &schema))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sqe_core::SqeConfig;
    use sqe_metrics::audit::AuditLogger;
    use tempfile::TempDir;

    fn config_with_superdebug(enabled: bool) -> SqeConfig {
        let toml = format!(
            r#"
[coordinator]
[auth]
[catalog]
catalog_url = "http://localhost:59999"
[metrics.audit]
superdebug_log_results = {enabled}
"#
        );
        toml::from_str(&toml).expect("config parses")
    }

    fn fresh_logger() -> (TempDir, Arc<AuditLogger>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("audit.jsonl");
        let logger = AuditLogger::with_config(
            path.to_str().unwrap(),
            sqe_metrics::audit::AuditFormat::Native,
        )
        .expect("logger");
        (dir, Arc::new(logger))
    }

    fn read_lines(dir: &TempDir, logger: &AuditLogger) -> Vec<serde_json::Value> {
        logger.flush();
        let path = dir.path().join("audit.jsonl");
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).expect("valid JSON line"))
            .collect()
    }

    /// When superdebug_log_results = true, maybe_warn_superdebug emits exactly
    /// one AdminDdl event with statement_type = "superdebug_log_results_enabled".
    #[test]
    fn superdebug_enabled_emits_admin_ddl_event() {
        let (dir, audit) = fresh_logger();
        let config = config_with_superdebug(true);

        super::maybe_warn_superdebug(&audit, &config);

        let lines = read_lines(&dir, &audit);
        assert_eq!(lines.len(), 1, "expected exactly one event, got: {lines:?}");

        let ev = &lines[0];
        assert_eq!(
            ev["kind"].as_str(),
            Some("admin_ddl"),
            "kind must be admin_ddl, got: {ev}"
        );
        assert_eq!(
            ev["query"]["statement_type"].as_str(),
            Some("superdebug_log_results_enabled"),
            "statement_type must identify the flag, got: {ev}"
        );
        assert_eq!(
            ev["actor"]["username"].as_str(),
            Some("system"),
            "actor must be system, got: {ev}"
        );
        assert_eq!(
            ev["status"].as_str(),
            Some("success"),
            "outcome must be success, got: {ev}"
        );
    }

    /// When superdebug_log_results = false (the default), maybe_warn_superdebug
    /// is a no-op: no event is written to the audit log.
    #[test]
    fn superdebug_disabled_emits_nothing() {
        let (dir, audit) = fresh_logger();
        let config = config_with_superdebug(false);

        super::maybe_warn_superdebug(&audit, &config);

        let lines = read_lines(&dir, &audit);
        assert!(
            lines.is_empty(),
            "no event must be emitted when flag is false, got: {lines:?}"
        );
    }
}
