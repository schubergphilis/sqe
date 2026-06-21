//! End-to-end content verification for the audit log.
//!
//! Issue #91: `AuditLogger::log` is called from `query_handler.rs`,
//! `streaming.rs`, and `maintenance.rs`, but until now no test ran a
//! full `QueryHandler::execute(...)` and read the resulting JSONL line
//! back from disk to assert what was written. The only audit test was a
//! no-op (`test_audit_logger_noop`) that proved the logger did not
//! panic. If someone refactored `query_handler.rs` and replaced
//! `self.audit.as_ref()` with `None` for one branch, no test fired.
//!
//! These tests construct a `QueryHandler` wired to a real `AuditLogger`
//! pointing at a `tempfile::tempdir()` path and exercise the in-memory
//! statement paths that don't need Docker (CREATE SECRET, DROP SECRET,
//! SHOW SECRETS, ATTACH, DETACH, plus an admin-gate denial). After each
//! call the file is read and the JSONL line is parsed to assert
//! `statement_type`, `username`, `status`, and that secret values do
//! not appear in the persisted form.

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_metrics::audit::AuditLogger;
use sqe_policy::PassthroughEnforcer;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const MINIMAL_TOML: &str = r#"
[coordinator]

[auth]

[catalog]
catalog_url = "http://localhost:59997"
"#;

fn minimal_config() -> SqeConfig {
    toml::from_str(MINIMAL_TOML).expect("minimal config")
}

fn admin_session() -> Session {
    session_with_roles(vec!["service_admin".to_string()])
}

fn session_with_roles(roles: Vec<String>) -> Session {
    Session::new(
        "auditor".to_string(),
        sqe_core::SecretString::new("tok".to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        roles,
    )
}

struct AuditFixture {
    handler: QueryHandler,
    audit: Arc<AuditLogger>,
    log_path: PathBuf,
    _dir: TempDir,
}

impl AuditFixture {
    fn read_lines(&self) -> Vec<Value> {
        self.audit.flush();
        read_audit_lines(&self.log_path)
    }
}

fn make_fixture() -> AuditFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let log_path = dir.path().join("audit.jsonl");
    let audit = Arc::new(
        AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"),
    );
    let config = minimal_config();
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        Some(audit.clone()),
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new");
    AuditFixture {
        handler,
        audit,
        log_path,
        _dir: dir,
    }
}

/// Build a fixture whose catalog points at a caller-supplied URL (e.g. a MockServer).
/// Used for tests that need `create_session_context` to succeed so SELECT planning
/// can proceed and `captured_plan` is populated (enabling `resources`).
fn make_fixture_with_url(catalog_url: &str) -> AuditFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let log_path = dir.path().join("audit.jsonl");
    let audit = Arc::new(
        AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"),
    );
    let toml = format!(
        "[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{catalog_url}\"\n"
    );
    let config: SqeConfig = toml::from_str(&toml).expect("catalog-url config");
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        Some(audit.clone()),
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new");
    AuditFixture {
        handler,
        audit,
        log_path,
        _dir: dir,
    }
}

fn read_audit_lines(path: &PathBuf) -> Vec<Value> {
    let content = std::fs::read_to_string(path).expect("audit file readable");
    content
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str::<Value>(l).expect("each line is JSON"))
        .collect()
}

#[tokio::test]
async fn audit_logs_create_secret_with_redacted_token() {
    let fx = make_fixture();
    let session = admin_session();

    fx.handler
        .execute(
            &session,
            "CREATE SECRET my_audit_tok (TYPE bearer, TOKEN 'super_secret_value_xyz')",
        )
        .await
        .expect("create secret");

    let lines = fx.read_lines();
    assert_eq!(lines.len(), 1, "exactly one audit line written");
    let entry = &lines[0];
    assert_eq!(entry["statement_type"], "create_secret");
    assert_eq!(entry["username"], "auditor");
    assert_eq!(entry["status"], "success");
    let query_text = entry["query_text"].as_str().expect("query_text string");
    assert!(
        !query_text.contains("super_secret_value_xyz"),
        "raw bearer must not appear in audit line: {query_text}"
    );
    assert!(
        entry["query_hash"].as_str().map(|h| !h.is_empty()).unwrap_or(false),
        "query_hash must be present"
    );
}

#[tokio::test]
async fn audit_logs_show_and_drop_secret_each_emit_one_line() {
    let fx = make_fixture();
    let session = admin_session();

    fx.handler
        .execute(&session, "CREATE SECRET tmp_tok (TYPE bearer, TOKEN 'x')")
        .await
        .expect("create");
    fx.handler
        .execute(&session, "SHOW SECRETS")
        .await
        .expect("show");
    fx.handler
        .execute(&session, "DROP SECRET tmp_tok")
        .await
        .expect("drop");

    let lines = fx.read_lines();
    assert_eq!(lines.len(), 3, "one audit line per execute call");
    assert_eq!(lines[0]["statement_type"], "create_secret");
    assert_eq!(lines[1]["statement_type"], "show_secrets");
    assert_eq!(lines[2]["statement_type"], "drop_secret");
    for entry in &lines {
        assert_eq!(entry["status"], "success");
        assert_eq!(entry["username"], "auditor");
    }
}

#[tokio::test]
async fn audit_logs_failed_create_with_error_status() {
    let fx = make_fixture();
    let session = admin_session();

    let _ = fx
        .handler
        .execute(&session, "CREATE SECRET bad (TYPE bearer)")
        .await
        .expect_err("missing TOKEN should fail");

    let lines = fx.read_lines();
    assert_eq!(lines.len(), 1, "failed calls still emit audit lines");
    assert_eq!(lines[0]["status"], "error");
}

#[tokio::test]
async fn audit_logs_attach_and_detach_against_mock_rest() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"overrides":{},"defaults":{}}"#))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[]}"#))
        .mount(&server)
        .await;

    let fx = make_fixture();
    let session = admin_session();
    let url = server.uri();

    let attach_sql = format!(
        "ATTACH '{url}' AS audit_cat (TYPE iceberg_rest, WAREHOUSE 'wh')"
    );
    fx.handler.execute(&session, &attach_sql).await.expect("attach");
    fx.handler
        .execute(&session, "DETACH audit_cat")
        .await
        .expect("detach");

    let lines = fx.read_lines();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["statement_type"], "attach");
    assert_eq!(lines[1]["statement_type"], "detach");
    for entry in &lines {
        assert_eq!(entry["status"], "success");
    }
}

/// Regression guard for the PII redaction regression introduced by Task 11.
///
/// Task 11 migrated `StatementKind::Query` (SELECT/DQL) from the legacy
/// `log(&AuditEntry)` path to the canonical `log_event(AuditEvent)` path.
/// The legacy path always called `redact_pii` on the caller thread. The
/// `log_event` path only called `redact_pii` inside `apply_gdpr_masking`,
/// which the worker skipped when no GDPR config was active. This test drives
/// a real SELECT through `QueryHandler::execute`, reads the JSONL line back
/// from disk, and asserts:
///
/// 1. The written line is a canonical `AuditEvent` (has `kind` = "query"),
///    not a legacy flat `AuditEntry` (which lacks a top-level `kind` field).
/// 2. `actor.username` equals the session username.
/// 3. `resources` is present and is an array (structured resource list from
///    the `information_schema.tables` scan).
/// 4. The PII literal `leak@example.com` does NOT appear anywhere in the
///    written content (directly guards the Finding 1 regression).
///
/// The fixture's catalog URL points at a MockServer that responds to the
/// Iceberg REST `/v1/config` and `/v1/namespaces` probes so that
/// `create_session_context` succeeds, the SELECT against the built-in
/// `information_schema.tables` virtual table can plan, and `captured_plan`
/// is populated (enabling a non-empty `resources` list).
#[tokio::test]
async fn audit_select_query_emits_canonical_event_and_redacts_pii() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"namespaces":[]}"#),
        )
        .mount(&server)
        .await;

    let fx = make_fixture_with_url(&server.uri());
    let session = admin_session();

    // SELECT from information_schema.tables with a PII literal in the predicate.
    // information_schema is built into DataFusion and resolves without any real
    // Iceberg tables. The WHERE clause never matches (no table is named like an
    // email), but the literal travels through the SQL text and must be redacted
    // before the line reaches disk.
    let result = fx
        .handler
        .execute(
            &session,
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'leak@example.com'",
        )
        .await;

    // The query may return zero rows or fail on edge cases. Either way the audit
    // line must be written. Tolerate both outcomes.
    let _ = result;

    let lines = fx.read_lines();
    assert_eq!(
        lines.len(),
        1,
        "exactly one audit line written for the SELECT; got:\n{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(lines.clone())).unwrap_or_default()
    );
    let entry = &lines[0];

    // Finding 1 guard: PII must not appear anywhere in the raw file content.
    let raw_content = std::fs::read_to_string(&fx.log_path).expect("audit file readable");
    assert!(
        !raw_content.contains("leak@example.com"),
        "PII literal leaked to audit log (Task 11 regression): {raw_content}"
    );

    // Assertion 1: canonical AuditEvent has top-level "kind" = "query".
    // The legacy flat AuditEntry has no "kind" field; it has "statement_type" at top level.
    assert_eq!(
        entry["kind"].as_str(),
        Some("query"),
        "written line must be a canonical AuditEvent (kind: query); got: {entry}"
    );

    // Assertion 2: actor username equals the session username.
    assert_eq!(
        entry["actor"]["username"].as_str(),
        Some("auditor"),
        "actor.username must match the session username; got: {entry}"
    );

    // Assertion 3: resources is present and is an array (the information_schema.tables
    // virtual table produces a TableScan node, so resources_from_plan finds it).
    assert!(
        entry["resources"].is_array(),
        "AuditEvent must carry a structured resources array; got: {entry}"
    );

    // Assertion 4: the line does NOT carry a flat AuditEntry's top-level "statement_type"
    // (in AuditEvent, statement_type lives under "query.statement_type" not at top level).
    assert!(
        entry["statement_type"].is_null(),
        "legacy flat field 'statement_type' must not appear at top level in AuditEvent; got: {entry}"
    );
}

#[tokio::test]
async fn audit_logs_denied_admin_call_as_error() {
    let fx = make_fixture();
    let session = session_with_roles(vec!["analyst".to_string()]);

    let _ = fx
        .handler
        .execute(&session, "CREATE SECRET nope (TYPE bearer, TOKEN 'x')")
        .await
        .expect_err("non-admin must be denied");

    let lines = fx.read_lines();
    assert_eq!(lines.len(), 1, "denied calls still produce an audit line");
    let entry = &lines[0];
    assert_eq!(entry["status"], "error");
    assert_eq!(entry["statement_type"], "create_secret");
    assert_eq!(entry["username"], "auditor");
}
