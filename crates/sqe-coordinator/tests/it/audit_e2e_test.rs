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

use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::CommandGetCatalogs;
use arrow_flight::FlightDescriptor;
use serde_json::Value;
use sqe_auth::{AnonymousProvider, AnonymousProviderConfig};
use sqe_coordinator::flight_sql::SqeFlightSqlService;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry, SessionManager};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_metrics::audit::AuditLogger;
use sqe_policy::PassthroughEnforcer;
use tempfile::TempDir;
use tonic::Request;
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

/// TDD guard for Task 13: Authentication audit events.
///
/// Drives a `SqeFlightSqlService` with an invalid session token (not in the
/// session map, no dots so it is not treated as a JWT). Asserts that:
///
/// 1. An `AuditKind::Auth` event with status "failure" lands in the log.
/// 2. The raw token string does NOT appear anywhere in the audit file
///    (no credential leakage, per the CRITICAL constraint in the task brief).
/// 3. The event carries `error_type == "AuthFailed"`.
/// 4. `actor.username` is "unknown" (no identity available for a bad token).
///
/// `client_ip` is soft-asserted: a synthetic `tonic::Request` has no peer
/// address, so the field is present but may be an empty string or absent.
#[tokio::test]
async fn audit_emits_auth_failure_event_for_invalid_session_token() {
    let fx = make_fixture();

    // Extract the parts we need before partially moving the fixture.
    let audit = fx.audit.clone();
    let log_path = fx.log_path.clone();

    // Build a SessionManager with an anonymous provider. Any JWT validation
    // attempt will succeed via anonymous, but a plain non-JWT token (no dots)
    // goes through the session-lookup path, finds nothing, and falls through
    // to the INVALID_SESSION failure branch.
    let provider = Arc::new(AnonymousProvider::new(AnonymousProviderConfig::default()));
    let session_manager = Arc::new(SessionManager::with_provider(provider));

    // Construct the service using the fixture's QueryHandler. The handler was
    // built with a tempfile-backed AuditLogger, so `query_handler.audit()`
    // returns Some(...). SqeFlightSqlService::new clones it via audit().
    let service = SqeFlightSqlService::new(
        session_manager,
        Arc::new(fx.handler),
        minimal_config(),
    );

    // Craft a request with an invalid Bearer token (no dots, so NOT treated
    // as a JWT; not in the session map; triggers the INVALID_SESSION branch).
    let bogus_token = "not-a-real-session-token-xyzzy";
    let mut req = Request::new(FlightDescriptor::new_cmd(vec![]));
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {bogus_token}").parse().unwrap(),
    );

    // The call fails with unauthenticated. We don't care about the error, only
    // that the audit event was written.
    let _ = service
        .get_flight_info_catalogs(CommandGetCatalogs {}, req)
        .await;

    // Flush and read the audit log.
    audit.flush();
    let lines = read_audit_lines(&log_path);

    // --- Assertion 1: exactly one auth event was written ---
    assert_eq!(
        lines.len(),
        1,
        "expected exactly one audit line for the failed auth; got:\n{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(lines.clone())).unwrap_or_default()
    );
    let entry = &lines[0];

    // --- Assertion 2: the event is an Auth kind with failure status ---
    assert_eq!(
        entry["kind"].as_str(),
        Some("auth"),
        "audit event must have kind 'auth'; got: {entry}"
    );
    assert_eq!(
        entry["status"].as_str(),
        Some("failure"),
        "outcome must be failure; got: {entry}"
    );

    // --- Assertion 3: error_type carries the reason ---
    assert_eq!(
        entry["error_type"].as_str(),
        Some("AuthFailed"),
        "error_type must be 'AuthFailed'; got: {entry}"
    );

    // --- Assertion 4: actor username is 'unknown', not the token ---
    assert_eq!(
        entry["actor"]["username"].as_str(),
        Some("unknown"),
        "actor.username must be 'unknown' for an unidentified caller; got: {entry}"
    );

    // --- Critical: the raw token must NOT appear anywhere in the file ---
    let raw_content = std::fs::read_to_string(&fx.log_path).expect("audit file readable");
    assert!(
        !raw_content.contains(bogus_token),
        "token material must not appear in the audit log (credential leak): {raw_content}"
    );
}

// ---------------------------------------------------------------------------
// Task 14: Session event
// ---------------------------------------------------------------------------

/// TDD guard for Task 14: Session lifecycle events.
///
/// Session events (OCSF Authorize Session 3003) are emitted only from
/// `do_handshake`, which is the single true session-establishment gate for
/// interactive clients. The JWT per-request path (`get_session_from_request`)
/// creates a new session on every RPC and must NOT emit a Session event, since
/// that would produce one event per query rather than one per login.
///
/// This test verifies the cardinality constraint: a JWT bearer request that
/// successfully authenticates emits exactly one `Auth` event and zero `Session`
/// events.
///
/// The `do_handshake` path is not exercised here because it requires a
/// `tonic::Streaming<HandshakeRequest>` which cannot be constructed without
/// the gRPC server machinery. The Session event from `do_handshake` is covered
/// by the `emit_session_event` unit test in the logger module. The
/// end-to-end path will be covered by an integration test suite once the
/// full gRPC test harness is available.
#[tokio::test]
async fn audit_jwt_path_emits_auth_not_session() {
    let fx = make_fixture();

    let audit = fx.audit.clone();
    let log_path = fx.log_path.clone();

    let provider = Arc::new(AnonymousProvider::new(AnonymousProviderConfig::default()));
    let session_manager = Arc::new(SessionManager::with_provider(provider));

    let service = SqeFlightSqlService::new(
        session_manager,
        Arc::new(fx.handler),
        minimal_config(),
    );

    // A dotted token (three segments) is treated as a JWT; AnonymousProvider
    // accepts it and returns an anonymous identity.
    let dotted_token = "header.payload.sig";
    let mut req = Request::new(FlightDescriptor::new_cmd(vec![]));
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {dotted_token}").parse().unwrap(),
    );

    let _ = service
        .get_flight_info_catalogs(CommandGetCatalogs {}, req)
        .await;

    audit.flush();
    let lines = read_audit_lines(&log_path);

    // The JWT path must emit an Auth event (authentication attempt record).
    let auth_events: Vec<&serde_json::Value> = lines
        .iter()
        .filter(|e| e["kind"].as_str() == Some("auth"))
        .collect();
    assert_eq!(
        auth_events.len(),
        1,
        "JWT path must emit one auth event; got:\n{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(lines.clone())).unwrap_or_default()
    );
    assert_eq!(auth_events[0]["status"].as_str(), Some("success"));

    // The JWT path must NOT emit a Session event. Session establishment is
    // exclusive to do_handshake (password-credential exchange).
    let session_events: Vec<&serde_json::Value> = lines
        .iter()
        .filter(|e| e["kind"].as_str() == Some("session"))
        .collect();
    assert_eq!(
        session_events.len(),
        0,
        "JWT per-request path must not emit session events; got:\n{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(lines.clone())).unwrap_or_default()
    );
}

/// Unit-level guard that the AuditLogger round-trips a Session event correctly.
/// Constructs an `AuditEvent` with `kind=Session` and logs it directly via
/// `audit.log_event`, then reads back the JSONL and asserts the serialized
/// fields. This does NOT exercise `do_handshake` or `emit_session_event`
/// (both require gRPC server machinery). The handshake-vs-JWT cardinality
/// contract is covered by `audit_jwt_path_emits_auth_not_session`.
///
/// Asserts kind="session", status="success", actor.username, and session_id.
#[tokio::test]
async fn audit_logger_round_trips_session_event() {
    // Construct an AuditLogger backed by a tempfile and log a Session event
    // directly to verify the serialization round-trip.
    let dir = tempfile::tempdir().expect("tempdir");
    let log_path = dir.path().join("audit.jsonl");
    let audit = Arc::new(
        sqe_metrics::audit::AuditLogger::new(log_path.to_str().unwrap())
            .expect("audit logger"),
    );

    // Emit a session event directly using the same actor and session_id
    // that do_handshake would produce.
    let actor = sqe_metrics::audit::Actor::from_parts(
        "testuser".to_string(),
        Some("sub-abc".to_string()),
        None,
        vec!["analyst".to_string()],
        vec![],
    );
    let event = sqe_metrics::audit::AuditEvent {
        time: chrono::Utc::now(),
        kind: sqe_metrics::audit::AuditKind::Session,
        actor: actor.clone(),
        outcome: sqe_metrics::audit::Outcome::Success,
        resources: vec![],
        policy: None,
        timing: None,
        stats: None,
        query: None,
        session_id: Some("test-session-id-001".to_string()),
        client_ip: Some("127.0.0.1".to_string()),
        integrity: sqe_metrics::audit::Integrity::default(),
    };
    audit.log_event(event);
    audit.flush();

    let lines = read_audit_lines(&log_path);
    assert_eq!(lines.len(), 1, "one session event written");
    let entry = &lines[0];

    assert_eq!(
        entry["kind"].as_str(),
        Some("session"),
        "kind must be session; got: {entry}"
    );
    assert_eq!(
        entry["status"].as_str(),
        Some("success"),
        "status must be success; got: {entry}"
    );
    assert_eq!(
        entry["actor"]["username"].as_str(),
        Some("testuser"),
        "actor.username must match; got: {entry}"
    );
    assert_eq!(
        entry["session_id"].as_str(),
        Some("test-session-id-001"),
        "session_id must match; got: {entry}"
    );
}

// ---------------------------------------------------------------------------
// Task 14: Grant event
// ---------------------------------------------------------------------------

use sqe_policy::grants::{
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantStatement,
    RevokeStatement,
};

/// Minimal recording stub for tests that need a grant backend.
#[derive(Default)]
struct RecordingGrantBackend;

#[async_trait::async_trait]
impl GrantBackend for RecordingGrantBackend {
    async fn grant(&self, _token: &str, _stmt: &GrantStatement) -> sqe_core::Result<()> {
        Ok(())
    }

    async fn revoke(&self, _token: &str, _stmt: &RevokeStatement) -> sqe_core::Result<()> {
        Ok(())
    }

    async fn show_grants(
        &self,
        _token: &str,
        _filter: &GrantFilter,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        Ok(vec![])
    }

    async fn show_effective(
        &self,
        _token: &str,
        _user: &str,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        Ok(vec![])
    }

    async fn check_access(
        &self,
        _token: &str,
        _check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        Ok(AccessCheckResult { allowed: true, reason: None })
    }

    fn backend_name(&self) -> &str {
        "recording"
    }
}

fn make_fixture_with_grant_backend() -> AuditFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let log_path = dir.path().join("audit.jsonl");
    let audit = Arc::new(
        AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"),
    );
    let config = minimal_config();
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let backend = Arc::new(RecordingGrantBackend);
    let handler = QueryHandler::new(
        Arc::new(sqe_policy::PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        Some(audit.clone()),
        tracker,
        None,
        Some(backend),
        None,
        RuntimeCatalogRegistry::new(),
        sqe_core::SecretStore::new(),
    )
    .expect("QueryHandler::new");
    AuditFixture {
        handler,
        audit,
        log_path,
        _dir: dir,
    }
}

/// TDD guard for Task 14: Grant audit events.
///
/// Executes a `GRANT SELECT ON sales.orders TO ROLE analyst` via an admin
/// session and asserts:
/// 1. Exactly one audit event is written with kind="grant".
/// 2. Status is "success".
/// 3. `actor.username` matches the session username.
/// 4. `resources` contains one entry whose `name` equals "orders".
/// 5. The query text carries the grantee info (raw SQL).
#[tokio::test]
async fn audit_emits_grant_event_with_resource_and_grantee() {
    let fx = make_fixture_with_grant_backend();
    let session = admin_session();

    fx.handler
        .execute(&session, "GRANT SELECT ON sales.orders TO ROLE analyst")
        .await
        .expect("grant must succeed for admin");

    let lines = fx.read_lines();

    let grant_events: Vec<&serde_json::Value> = lines
        .iter()
        .filter(|e| e["kind"].as_str() == Some("grant"))
        .collect();

    assert_eq!(
        grant_events.len(),
        1,
        "exactly one grant event must be written; got lines:\n{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(lines.clone())).unwrap_or_default()
    );
    let entry = grant_events[0];

    assert_eq!(
        entry["status"].as_str(),
        Some("success"),
        "grant event must have success status; got: {entry}"
    );
    assert_eq!(
        entry["actor"]["username"].as_str(),
        Some("auditor"),
        "actor.username must match session; got: {entry}"
    );

    // resources[0].name must be the table name.
    let resources = entry["resources"].as_array().expect("resources must be array");
    assert_eq!(resources.len(), 1, "one resource for the granted table; got: {entry}");
    assert_eq!(
        resources[0]["name"].as_str(),
        Some("orders"),
        "resource name must be the granted table; got: {entry}"
    );

    // The raw SQL (carried in query.text) must contain the grantee.
    // `unwrap_or("")` is intentionally avoided here: if query.text is absent
    // the assertion must fail, not silently pass.
    let query_text = entry["query"]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("query.text must be present in the grant event; got: {entry}"));
    assert!(
        query_text.contains("analyst"),
        "query.text must carry the grantee name 'analyst'; got query.text={query_text:?}, entry={entry}"
    );
}

/// TDD guard for Task 14: Revoke audit events.
///
/// Executes a `REVOKE SELECT ON sales.orders FROM ROLE analyst` via an admin
/// session and asserts kind="grant" (same class for revoke, grant_type
/// distinguishes direction), status="success", and resource.name="orders".
#[tokio::test]
async fn audit_emits_grant_event_for_revoke() {
    let fx = make_fixture_with_grant_backend();
    let session = admin_session();

    fx.handler
        .execute(&session, "REVOKE SELECT ON sales.orders FROM ROLE analyst")
        .await
        .expect("revoke must succeed for admin");

    let lines = fx.read_lines();

    let grant_events: Vec<&serde_json::Value> = lines
        .iter()
        .filter(|e| e["kind"].as_str() == Some("grant"))
        .collect();

    assert_eq!(
        grant_events.len(),
        1,
        "exactly one grant event for revoke; got:\n{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(lines.clone())).unwrap_or_default()
    );
    let entry = grant_events[0];
    assert_eq!(entry["status"].as_str(), Some("success"));
    let resources = entry["resources"].as_array().expect("resources array");
    assert_eq!(resources[0]["name"].as_str(), Some("orders"));
}
