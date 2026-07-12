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
use sqe_coordinator::{
    query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry, SessionManager,
};
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
    let audit = Arc::new(AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"));
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
    let audit = Arc::new(AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"));
    let toml = format!("[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{catalog_url}\"\n");
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
            None,
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
        entry["query_hash"]
            .as_str()
            .map(|h| !h.is_empty())
            .unwrap_or(false),
        "query_hash must be present"
    );
}

#[tokio::test]
async fn audit_logs_show_and_drop_secret_each_emit_one_line() {
    let fx = make_fixture();
    let session = admin_session();

    fx.handler
        .execute(
            &session,
            "CREATE SECRET tmp_tok (TYPE bearer, TOKEN 'x')",
            None,
        )
        .await
        .expect("create");
    fx.handler
        .execute(&session, "SHOW SECRETS", None)
        .await
        .expect("show");
    fx.handler
        .execute(&session, "DROP SECRET tmp_tok", None)
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
        .execute(&session, "CREATE SECRET bad (TYPE bearer)", None)
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
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
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

    let attach_sql = format!("ATTACH '{url}' AS audit_cat (TYPE iceberg_rest, WAREHOUSE 'wh')");
    fx.handler
        .execute(&session, &attach_sql, None)
        .await
        .expect("attach");
    fx.handler
        .execute(&session, "DETACH audit_cat", None)
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
            ResponseTemplate::new(200).set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[]}"#))
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
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'leak@example.com'", None)
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
        .execute(
            &session,
            "CREATE SECRET nope (TYPE bearer, TOKEN 'x')",
            None,
        )
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
    let service = SqeFlightSqlService::new(session_manager, Arc::new(fx.handler), minimal_config());

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

    let service = SqeFlightSqlService::new(session_manager, Arc::new(fx.handler), minimal_config());

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
        sqe_metrics::audit::AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger"),
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

    async fn show_effective(&self, _token: &str, _user: &str) -> sqe_core::Result<Vec<GrantEntry>> {
        Ok(vec![])
    }

    async fn check_access(
        &self,
        _token: &str,
        _check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        Ok(AccessCheckResult {
            allowed: true,
            reason: None,
        })
    }

    fn backend_name(&self) -> &str {
        "recording"
    }
}

fn make_fixture_with_grant_backend() -> AuditFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let log_path = dir.path().join("audit.jsonl");
    let audit = Arc::new(AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"));
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
        .execute(
            &session,
            "GRANT SELECT ON sales.orders TO ROLE analyst",
            None,
        )
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
    let resources = entry["resources"]
        .as_array()
        .expect("resources must be array");
    assert_eq!(
        resources.len(),
        1,
        "one resource for the granted table; got: {entry}"
    );
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
        .execute(
            &session,
            "REVOKE SELECT ON sales.orders FROM ROLE analyst",
            None,
        )
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

/// TDD guard for Task 2: streaming SELECT migrates to canonical `AuditEvent`.
///
/// Builds a `StreamFinalizer` wired to a real `AuditLogger`, sets an `Actor`
/// with a known username and a non-empty `resources` list, drives the success
/// finalization path, flushes, reads the JSONL line, and asserts it is a
/// canonical `AuditEvent` - not the legacy flat `AuditEntry`.
///
/// Assertions mirror the brief's required test contract:
/// 1. `kind` == "query" (canonical AuditEvent discriminant)
/// 2. `actor.username` == "auditor" (Actor field populated from session)
/// 3. `resources` is a non-empty array (Resource list threaded through)
/// 4. No top-level `statement_type` field (was top-level in AuditEntry)
/// 5. `integrity.seq` is a u64 (hash-chain populated by the logger)
#[tokio::test]
async fn streaming_select_emits_canonical_query_event() {
    use arrow_array::{Int64Array, RecordBatch};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use sqe_coordinator::query_tracker::QueryTracker;
    use sqe_coordinator::streaming::{StreamFinalizer, TrackedRecordBatchStream};
    use sqe_core::QueryHistoryConfig;
    use sqe_metrics::audit::{Actor, AuditLogger, ObjectType, Resource};
    use std::sync::Arc;

    // Build a trivial physical plan via SessionContext (mirrors the inline test).
    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT 1 AS x").await.unwrap();
    let schema: std::sync::Arc<arrow_schema::Schema> =
        std::sync::Arc::new(df.schema().as_arrow().clone());
    let plan = df.create_physical_plan().await.unwrap();
    let runtime = ctx.runtime_env();

    let tracker = Arc::new(QueryTracker::new(&QueryHistoryConfig {
        max_entries: 128,
        ttl_secs: 60,
    }));

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let audit = Arc::new(AuditLogger::new(path.to_str().unwrap()).unwrap());

    // Construct a StreamFinalizer with a known Actor and non-empty resources.
    let fin = StreamFinalizer {
        tracker: Arc::clone(&tracker),
        metrics: None,
        audit: Some(Arc::clone(&audit)),
        query_id: uuid::Uuid::now_v7(),
        username: "auditor".to_string(),
        session_id: "sess-streaming-1".to_string(),
        sql: "SELECT 1 AS x".to_string(),
        kind_name: "Query".to_string(),
        plan,
        runtime,
        start: std::time::Instant::now(),
        slow_query_threshold_secs: 0,
        sql_length: 14,
        tables_touched: vec!["polaris.public.orders".to_string()],
        policy_summary: sqe_policy::PolicySummary::default(),
        profile_mode: sqe_core::ProfileMode::Off,
        actor: Actor::from_parts(
            "auditor".to_string(),
            Some("sub-001".to_string()),
            Some("auditor@example.com".to_string()),
            vec!["analyst".to_string()],
            vec![],
        ),
        resources: vec![Resource {
            catalog: Some("polaris".to_string()),
            namespace: vec!["public".to_string()],
            name: "orders".to_string(),
            object_type: ObjectType::Table,
        }],
        client_ip: None,
    };

    let qid = fin.query_id;
    tracker.start(
        qid,
        "auditor",
        None,
        "SELECT 1 AS x",
        "sess-streaming-1",
        None,
        vec![],
        None,
    );

    // Drive the success path: create a single-batch stream and drain it.
    let arr = Int64Array::from_iter_values(0..5);
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)]).unwrap();
    let s = futures::stream::iter(vec![Ok(batch)]);
    let inner: datafusion::physical_plan::SendableRecordBatchStream =
        Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), s));
    let mut stream = TrackedRecordBatchStream::new(inner, fin, None);
    while let Some(b) = stream.next().await {
        let _ = b.unwrap();
    }
    drop(stream);
    audit.flush();

    let content = std::fs::read_to_string(&path).unwrap();
    assert!(
        !content.is_empty(),
        "audit file must have content after flush"
    );
    let line = content
        .lines()
        .next()
        .expect("at least one line in audit file");
    let v: serde_json::Value = serde_json::from_str(line).unwrap();

    // 1. canonical AuditEvent discriminant
    assert_eq!(v["kind"], "query", "expected kind=query, got: {v}");
    // 2. Actor field populated from session
    assert_eq!(
        v["actor"]["username"], "auditor",
        "expected actor.username=auditor, got: {v}"
    );
    // 3. non-empty resources array (we set one resource above)
    assert!(
        v["resources"].is_array(),
        "AuditEvent must carry a structured resources array; got: {v}"
    );
    assert!(
        !v["resources"].as_array().unwrap().is_empty(),
        "resources array must be non-empty; got: {v}"
    );
    // 4. no top-level statement_type (legacy AuditEntry artifact)
    assert!(
        v.get("statement_type").is_none() || v["statement_type"].is_null(),
        "top-level statement_type must not appear in canonical AuditEvent; got: {v}"
    );
    // 5. hash-chain integrity.seq populated
    assert!(
        v["integrity"]["seq"].is_u64(),
        "integrity.seq must be a u64; got: {v}"
    );
}

// ---------------------------------------------------------------------------
// Task 3: DDL and DML migrate to canonical AuditEvent
// ---------------------------------------------------------------------------

/// TDD guard for Task 3: DDL statements emit a canonical `AuditEvent` with
/// `kind == "admin_ddl"`, not the legacy flat `AuditEntry`.
///
/// A `DROP TABLE` is used because it requires only the parser (no catalog
/// connection needed to parse the statement), and a parser-level failure still
/// runs through the full audit dispatch block, emitting a Failure-outcome
/// canonical event. The test asserts:
///
/// 1. `kind` == "admin_ddl" (canonical AuditEvent, not legacy flat entry).
/// 2. `actor.username` == "auditor" (Actor populated from session).
/// 3. No top-level `statement_type` (legacy AuditEntry artifact absent).
/// 4. `integrity.seq` is a u64 (hash-chain populated by the logger).
#[tokio::test]
async fn ddl_emits_canonical_admin_ddl_event() {
    let fx = make_fixture();
    let session = admin_session();

    // DROP TABLE will fail at execution (no catalog), but the audit block
    // still runs. A Failure-outcome canonical AuditEvent is emitted.
    let _ = fx
        .handler
        .execute(&session, "DROP TABLE IF EXISTS myns.mytable", None)
        .await;

    let lines = fx.read_lines();
    assert_eq!(
        lines.len(),
        1,
        "exactly one audit line written for DDL; got:\n{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(lines.clone())).unwrap_or_default()
    );
    let entry = &lines[0];

    // 1. canonical AuditEvent kind discriminant must be "admin_ddl"
    assert_eq!(
        entry["kind"].as_str(),
        Some("admin_ddl"),
        "DDL must emit a canonical AuditEvent with kind=admin_ddl; got: {entry}"
    );

    // 2. actor.username must match the session username
    assert_eq!(
        entry["actor"]["username"].as_str(),
        Some("auditor"),
        "actor.username must be populated from the session; got: {entry}"
    );

    // 3. no top-level statement_type (that is the legacy AuditEntry shape)
    assert!(
        entry["statement_type"].is_null(),
        "top-level statement_type must NOT appear in a canonical AuditEvent; got: {entry}"
    );

    // 4. hash-chain integrity.seq must be a u64
    assert!(
        entry["integrity"]["seq"].is_u64(),
        "integrity.seq must be a u64; got: {entry}"
    );
}

/// TDD guard for Task 3: `CREATE SECRET` statements stay on the redacted legacy
/// `log(&AuditEntry)` path after the DDL/DML migration. The legacy path applies
/// PII redaction before the line is written, which is the established contract
/// for secret SQL.
///
/// Asserts:
///
/// 1. The raw bearer token does NOT appear anywhere in the written file.
/// 2. The written line has `statement_type` at TOP LEVEL (legacy flat shape).
/// 3. The written line does NOT have a top-level `kind` field (canonical shape
///    is NOT used for secrets).
#[tokio::test]
async fn create_secret_stays_redacted_legacy_after_ddl_migration() {
    let fx = make_fixture();
    let session = admin_session();

    fx.handler
        .execute(
            &session,
            "CREATE SECRET task3_guard (TYPE bearer, TOKEN 'task3_secret_material_xyz')",
            None,
        )
        .await
        .expect("create secret must succeed");

    let lines = fx.read_lines();
    assert_eq!(lines.len(), 1, "exactly one audit line written");
    let entry = &lines[0];

    // 1. The raw token must NOT appear in the file (redaction contract).
    let raw_content = std::fs::read_to_string(&fx.log_path).expect("audit file readable");
    assert!(
        !raw_content.contains("task3_secret_material_xyz"),
        "raw bearer token must not appear in audit log after DDL migration: {raw_content}"
    );

    // 2. The line carries top-level `statement_type` (legacy flat AuditEntry shape).
    assert_eq!(
        entry["statement_type"].as_str(),
        Some("create_secret"),
        "CREATE SECRET must remain on legacy path with top-level statement_type; got: {entry}"
    );

    // 3. The line does NOT carry a top-level `kind` field (not a canonical AuditEvent).
    assert!(
        entry["kind"].is_null(),
        "CREATE SECRET must NOT be emitted as a canonical AuditEvent (kind must be absent); got: {entry}"
    );
}

/// TDD guard for sub-project C Task 3: per-request `client_ip` threads into
/// the Query `AuditEvent`.
///
/// Drives `QueryHandler::execute` with `client_ip: Some("10.1.2.3".into())`
/// and asserts the written canonical `AuditEvent` line carries
/// `client_ip == "10.1.2.3"`. The query is a SELECT against the built-in
/// `information_schema.tables` virtual table (no Iceberg catalog required once
/// the fixture's catalog URL satisfies the `/v1/config` probe).
///
/// RED: before the param is added, the call will fail to compile (wrong arity).
/// GREEN: after the param is accepted and threaded to the audit event.
#[tokio::test]
async fn execute_threads_client_ip_into_query_audit_event() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[]}"#))
        .mount(&server)
        .await;

    let fx = make_fixture_with_url(&server.uri());
    let session = admin_session();

    let _ = fx
        .handler
        .execute(
            &session,
            "SELECT table_name FROM information_schema.tables",
            Some("10.1.2.3".to_string()),
        )
        .await;

    let lines = fx.read_lines();
    assert_eq!(
        lines.len(),
        1,
        "exactly one audit line written; got:\n{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(lines.clone())).unwrap_or_default()
    );
    let entry = &lines[0];

    // Must be a canonical AuditEvent.
    assert_eq!(
        entry["kind"].as_str(),
        Some("query"),
        "written line must be canonical AuditEvent (kind: query); got: {entry}"
    );

    // client_ip must be threaded through from the caller.
    assert_eq!(
        entry["client_ip"].as_str(),
        Some("10.1.2.3"),
        "client_ip must equal the value passed to execute(); got: {entry}"
    );
}

// ---------------------------------------------------------------------------
// Task 4 (folded-in): Grant and DDL audit events carry client_ip
// ---------------------------------------------------------------------------

/// TDD guard for Task 4 (Grant branch): `client_ip` passed to `execute()`
/// appears in the GRANT audit event.
///
/// RED before the `client_ip: None` -> `client_ip.clone()` fix in
/// `query_handler.rs` ~1658.
/// GREEN after.
#[tokio::test]
async fn grant_audit_event_carries_client_ip() {
    let fx = make_fixture_with_grant_backend();
    let session = admin_session();

    fx.handler
        .execute(
            &session,
            "GRANT SELECT ON sales.orders TO ROLE analyst",
            Some("192.168.1.100".to_string()),
        )
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
        "exactly one grant event; got lines:\n{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(lines.clone())).unwrap_or_default()
    );

    let entry = grant_events[0];
    assert_eq!(
        entry["client_ip"].as_str(),
        Some("192.168.1.100"),
        "grant audit event must carry client_ip; got: {entry}"
    );
}

/// TDD guard for Task 4 (DDL branch): `client_ip` passed to `execute()`
/// appears in the DDL (admin_ddl) audit event.
///
/// RED before the `client_ip: None` -> `client_ip.clone()` fix in
/// `query_handler.rs` ~1748.
/// GREEN after.
#[tokio::test]
async fn ddl_audit_event_carries_client_ip() {
    let fx = make_fixture();
    let session = admin_session();

    // DROP TABLE fails at execution (no catalog) but the audit block still
    // runs, emitting a Failure-outcome canonical AuditEvent.
    let _ = fx
        .handler
        .execute(
            &session,
            "DROP TABLE IF EXISTS myns.mytable",
            Some("10.20.30.40".to_string()),
        )
        .await;

    let lines = fx.read_lines();
    assert_eq!(
        lines.len(),
        1,
        "exactly one audit line for DDL; got:\n{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(lines.clone())).unwrap_or_default()
    );
    let entry = &lines[0];

    assert_eq!(
        entry["kind"].as_str(),
        Some("admin_ddl"),
        "DDL must emit kind=admin_ddl; got: {entry}"
    );
    assert_eq!(
        entry["client_ip"].as_str(),
        Some("10.20.30.40"),
        "DDL audit event must carry client_ip; got: {entry}"
    );
}
