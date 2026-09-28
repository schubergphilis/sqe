//! Statements that fail during parse or planning still leave an audit line.
//!
//! Found wiring SQE into a SIEM (SQE 0.48.1, `format = "both"`):
//! `SELECT * FROM nosuch.tbl` wrote nothing to `audit.jsonl` or
//! `audit.ocsf.jsonl`. The streaming path (Flight SQL DoGet, Trino HTTP,
//! Quack) recorded a planning failure in the tracker and metrics but not the
//! audit log; Flight `GetFlightInfo` plans through `get_schema` and never
//! reaches DoGet at all; and the buffered path returned before its audit
//! block on parse and catalog-preflight errors.
//!
//! Each test drives one failing statement through one entry point and
//! asserts exactly one failed statement record in both the native and the
//! OCSF file (`status_id` 2), and that the native chain still verifies.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::CommandStatementQuery;
use arrow_flight::FlightDescriptor;
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde_json::Value;
use sqe_auth::{AnonymousProvider, AnonymousProviderConfig};
use sqe_coordinator::flight_sql::SqeFlightSqlService;
use sqe_coordinator::{
    query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry, SessionManager,
};
use sqe_core::{SecretStore, Session, SessionUser, SqeConfig, SqeError, SqeErrorCode};
use sqe_metrics::audit::{verify_audit_bytes, AuditFormat, AuditLogger};
use sqe_policy::{PassthroughEnforcer, PolicyEnforcer, PolicySummary};
use tempfile::TempDir;
use tonic::Request;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Denies every plan, the way a policy engine denies at planning time.
struct DenyEnforcer;

#[async_trait]
impl PolicyEnforcer for DenyEnforcer {
    async fn evaluate(
        &self,
        _user: &SessionUser,
        _plan: LogicalPlan,
    ) -> sqe_core::Result<(LogicalPlan, PolicySummary)> {
        Err(SqeError::with_code(
            SqeErrorCode::AccessDenied,
            "access denied by policy",
        ))
    }
}

struct Fixture {
    handler: Arc<QueryHandler>,
    audit: Arc<AuditLogger>,
    native: PathBuf,
    ocsf: PathBuf,
    config: SqeConfig,
    _server: MockServer,
    _dir: TempDir,
}

impl Fixture {
    /// Flushes, then returns the native and OCSF records, after checking the
    /// native hash chain.
    fn records(&self) -> (Vec<Value>, Vec<Value>) {
        self.audit.flush();
        let bytes = std::fs::read(&self.native).expect("native audit file");
        let report = verify_audit_bytes(&bytes).expect("audit hash chain verifies");
        let native = parse_lines(&bytes);
        assert_eq!(report.records, native.len() as u64);
        let ocsf = parse_lines(&std::fs::read(&self.ocsf).unwrap_or_default());
        (native, ocsf)
    }
}

fn parse_lines(bytes: &[u8]) -> Vec<Value> {
    std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).expect("each line is JSON"))
        .collect()
}

/// A handler over a mock REST catalog with no namespaces, writing audit in
/// `format = "both"`.
async fn fixture(enforcer: Arc<dyn PolicyEnforcer>) -> Fixture {
    let server = crate::common::mock_server().await;
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

    let dir = tempfile::tempdir().expect("tempdir");
    let native = dir.path().join("audit.jsonl");
    let audit = Arc::new(
        AuditLogger::with_config(native.to_str().unwrap(), AuditFormat::Both)
            .expect("audit logger opens"),
    );
    let toml = format!(
        "[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{}\"\n",
        server.uri()
    );
    let config: SqeConfig = toml::from_str(&toml).expect("config");
    let handler = QueryHandler::new(
        enforcer,
        None,
        config.clone(),
        None,
        None,
        None,
        Some(audit.clone()),
        Arc::new(QueryTracker::new(&config.query_history)),
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new");
    Fixture {
        handler: Arc::new(handler),
        audit,
        ocsf: dir.path().join("audit.ocsf.jsonl"),
        native,
        config,
        _server: server,
        _dir: dir,
    }
}

fn session() -> Session {
    Session::new(
        "auditor".to_string(),
        sqe_core::SecretString::new(format!("tok-{}", uuid::Uuid::new_v4())),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        vec!["analyst".to_string()],
    )
}

/// Exactly one failed statement record per file, carrying `sql` and
/// `statement_type`. Returns the native record.
fn assert_one_failed_statement(
    native: &[Value],
    ocsf: &[Value],
    sql: &str,
    statement_type: &str,
) -> Value {
    let dump = || serde_json::to_string_pretty(&native).unwrap_or_default();
    let statements: Vec<&Value> = native
        .iter()
        .filter(|l| l["query"]["text"].as_str() == Some(sql))
        .collect();
    assert_eq!(
        statements.len(),
        1,
        "one native statement record; got:\n{}",
        dump()
    );
    let s = statements[0];
    assert_eq!(s["kind"], "query", "{s}");
    assert_eq!(s["status"], "failure", "{s}");
    assert_eq!(s["query"]["statement_type"], statement_type, "{s}");
    assert!(s["error_code"].is_string(), "error class recorded: {s}");
    assert!(s["integrity"]["seq"].is_u64(), "chained: {s}");

    let ocsf_statements: Vec<&Value> = ocsf
        .iter()
        .filter(|l| l["unmapped"]["query_text"].as_str() == Some(sql))
        .collect();
    assert_eq!(
        ocsf_statements.len(),
        1,
        "one OCSF statement record; got: {ocsf:?}"
    );
    let o = ocsf_statements[0];
    assert_eq!(o["status_id"], 2, "OCSF failure: {o}");
    assert_eq!(o["class_uid"], 6005, "{o}");
    assert_eq!(
        o["metadata"]["uid"], s["integrity"]["hash"],
        "same chained record"
    );
    s.clone()
}

#[tokio::test]
async fn parse_error_buffered_is_audited() {
    let fx = fixture(Arc::new(PassthroughEnforcer)).await;
    let sql = "SELEC nonsense FROM";
    fx.handler
        .execute(&session(), sql, None)
        .await
        .expect_err("parse error");
    let (native, ocsf) = fx.records();
    assert_eq!(native.len(), 1);
    assert_one_failed_statement(&native, &ocsf, sql, "unknown");
}

#[tokio::test]
async fn parse_error_streaming_is_audited() {
    let fx = fixture(Arc::new(PassthroughEnforcer)).await;
    let sql = "SELEC nonsense FROM";
    fx.handler
        .execute_stream(&session(), sql, None)
        .await
        .err()
        .expect("parse error");
    let (native, ocsf) = fx.records();
    assert_eq!(native.len(), 1);
    assert_one_failed_statement(&native, &ocsf, sql, "unknown");
}

#[tokio::test]
async fn missing_table_streaming_is_audited() {
    let fx = fixture(Arc::new(PassthroughEnforcer)).await;
    let sql = "SELECT * FROM nosuch.tbl";
    fx.handler
        .execute_stream(&session(), sql, None)
        .await
        .err()
        .expect("table not found");
    let (native, ocsf) = fx.records();
    assert_eq!(native.len(), 1);
    assert_one_failed_statement(&native, &ocsf, sql, "query");
}

#[tokio::test]
async fn missing_table_buffered_is_audited_once() {
    let fx = fixture(Arc::new(PassthroughEnforcer)).await;
    let sql = "SELECT * FROM nosuch.tbl";
    fx.handler
        .execute(&session(), sql, None)
        .await
        .expect_err("table not found");
    let (native, ocsf) = fx.records();
    assert_eq!(native.len(), 1);
    assert_one_failed_statement(&native, &ocsf, sql, "query");
}

#[tokio::test]
async fn policy_denial_at_planning_is_audited() {
    let fx = fixture(Arc::new(DenyEnforcer)).await;
    let sql = "SELECT 1 AS x";
    let err = fx
        .handler
        .execute_stream(&session(), sql, None)
        .await
        .err()
        .expect("denied");
    assert!(
        matches!(err.error_code(), SqeErrorCode::AccessDenied),
        "{err}"
    );
    fx.handler
        .execute(&session(), sql, None)
        .await
        .expect_err("denied");
    let (native, _) = fx.records();
    assert_eq!(
        native.len(),
        2,
        "one record per attempt, buffered and streaming"
    );
    for s in &native {
        assert_eq!(s["status"], "failure", "{s}");
        assert_eq!(s["error_code"], "ACCESS_DENIED", "{s}");
    }
}

/// The reported repro: Flight SQL plans at `GetFlightInfo`, and a statement
/// that fails there is never executed.
#[tokio::test]
async fn missing_table_flight_get_flight_info_is_audited() {
    let fx = fixture(Arc::new(PassthroughEnforcer)).await;
    let provider = Arc::new(AnonymousProvider::new(AnonymousProviderConfig::default()));
    let service = SqeFlightSqlService::new(
        Arc::new(SessionManager::with_provider(provider)),
        Arc::clone(&fx.handler),
        fx.config.clone(),
    );
    let sql = "SELECT * FROM nosuch.tbl";
    let mut req = Request::new(FlightDescriptor::new_cmd(vec![]));
    req.metadata_mut().insert(
        "authorization",
        "Bearer header.payload.sig".parse().unwrap(),
    );
    let cmd = CommandStatementQuery {
        query: sql.to_string(),
        transaction_id: None,
    };
    service
        .get_flight_info_statement(cmd, req)
        .await
        .expect_err("table not found");
    let (native, ocsf) = fx.records();
    // The JWT request also writes its `auth` record; both share one chain.
    assert_eq!(native.len(), 2, "{native:?}");
    assert_one_failed_statement(&native, &ocsf, sql, "query");
}
