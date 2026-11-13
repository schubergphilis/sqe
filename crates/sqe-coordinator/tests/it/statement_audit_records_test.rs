//! How many audit records one statement leaves, per outcome.
//!
//! A safety net for the restructure of `QueryHandler::execute_inner`
//! (`openspec/changes/statement-path-restructure`, tasks 1.2 and 1.3). The
//! spec asks for exactly one record for every statement that reaches
//! classification, whatever its outcome, and for the statements refused
//! before classification to keep writing whatever they write today. Each
//! test drives one outcome through the buffered path (`execute`) and asserts
//! the record count and the outcome fields in both the native and the OCSF
//! file.
//!
//! These tests pin the behaviour as it is. An assertion marked
//! `// pins current behaviour:` records something the spec does not ask for;
//! it is there so a refactor that changes it does so on purpose.
//!
//! The harness is the one `audit_planning_failure_test` uses: a handler over
//! a wiremock REST catalog, an `AuditLogger` on a temp file in `both` format,
//! and the native hash chain verified on every read. A statement is held in
//! execution by a slow catalog response (`FixtureOpts::namespaces_delay`),
//! so timeout and cancellation need no sleep in the engine and no race with
//! a fast query.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use sqe_core::{SqeError, SqeErrorCode};
use sqe_policy::PassthroughEnforcer;

use crate::audit_planning_failure_test::{fixture_opts, session_for, Fixture, FixtureOpts};

/// Longer than any test waits: a statement held by it is ended by the
/// timeout, the cancellation or the end of the test, never by the response.
const STUCK: Duration = Duration::from_secs(60);

/// Per-test user names: the session-context cache is keyed by user name and
/// every test in the binary shares it.
fn opts(query_toml: &str) -> FixtureOpts {
    FixtureOpts {
        query_toml: query_toml.to_string(),
        ..FixtureOpts::default()
    }
}

fn stuck(query_toml: &str) -> FixtureOpts {
    FixtureOpts {
        query_toml: query_toml.to_string(),
        namespaces_delay: STUCK,
        ..FixtureOpts::default()
    }
}

/// Every record the statement left, in both files, plus the tracker's view.
struct Written {
    native: Vec<Value>,
    ocsf: Vec<Value>,
}

impl Written {
    fn of(fx: &Fixture) -> Self {
        let (native, ocsf) = fx.records();
        Self { native, ocsf }
    }

    /// Exactly one native and one OCSF record in all, a statement record
    /// that failed with `error_code`; returns the native one. The count is
    /// over the whole file, so a second record under a different text (an
    /// `auth` or `transaction` record, say) is caught too.
    fn one_failure(&self, sql: &str, error_code: &str) -> Value {
        let dump = serde_json::to_string_pretty(&self.native).unwrap_or_default();
        assert_eq!(self.native.len(), 1, "one native record in all:\n{dump}");
        assert_eq!(self.ocsf.len(), 1, "one OCSF record in all");
        let s = &self.native[0];
        assert_eq!(s["kind"], "query", "{s}");
        assert_eq!(s["query"]["text"], sql, "{s}");
        assert_eq!(s["status"], "failure", "{s}");
        assert_eq!(s["error_code"], error_code, "{s}");
        assert!(s["integrity"]["seq"].is_u64(), "chained: {s}");
        assert_eq!(self.ocsf[0]["status_id"], 2, "OCSF failure");
        assert_eq!(
            self.ocsf[0]["metadata"]["uid"], s["integrity"]["hash"],
            "the OCSF line is the chained native record"
        );
        s.clone()
    }
}

async fn catalog_namespace_requests(fx: &Fixture) -> usize {
    fx.server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| r.method.as_str() == "GET" && r.url.path() == "/v1/namespaces")
        .count()
}

/// Waits until the held request reached the mock catalog: the statement is
/// then inside execution, past the query tracker's start.
async fn wait_until_held(fx: &Fixture) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while catalog_namespace_requests(fx).await == 0 {
        assert!(
            Instant::now() < deadline,
            "the statement never reached the catalog"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// -- Refused before classification (#882) ----------------------------------

/// The size check runs before admission and never classifies the statement
/// (#403): its record is unparsed (`statement_type` `"unknown"`) and carries
/// the text cut to its first KiB, with the full length in `text_len`.
#[tokio::test]
async fn an_oversized_statement_writes_one_audit_record() {
    let fx = fixture_opts(Arc::new(PassthroughEnforcer), opts("max_sql_bytes = 1100")).await;
    let sql = format!("SELECT '{}' AS a", "x".repeat(2048));
    let err = fx
        .handler
        .execute(&session_for("size-user"), &sql, None)
        .await
        .expect_err("oversized");
    assert_eq!(err.error_code(), SqeErrorCode::InvalidArguments, "{err}");
    assert!(err.to_string().contains("max_sql_bytes"), "{err}");

    let written = Written::of(&fx);
    // The cut lands inside the literal, so the kept text stops before it.
    let s = written.one_failure("SELECT ", "INVALID_ARGUMENTS");
    assert_eq!(s["query"]["statement_type"], "unknown", "{s}");
    assert_eq!(s["query"]["text_len"], sql.len(), "{s}");
    // No tracker entry: the statement never started.
    assert!(fx.handler.query_tracker().records().is_empty());
}

/// Admission refuses before classification: here the per-user memory budget
/// is smaller than one statement's quota.
#[tokio::test]
async fn an_admission_refusal_writes_one_audit_record() {
    let fx = fixture_opts(
        Arc::new(PassthroughEnforcer),
        opts("per_user_memory_budget = \"1MB\"\nmax_query_memory = \"64MB\""),
    )
    .await;
    let sql = "SELECT 1 AS a";
    let err = fx
        .handler
        .execute(&session_for("admission-user"), sql, None)
        .await
        .expect_err("over the per-user memory budget");
    assert_eq!(err.error_code(), SqeErrorCode::ExecutionFailed, "{err}");
    assert!(
        err.to_string().contains("Per-user memory budget exceeded"),
        "{err}"
    );

    Written::of(&fx).one_failure(sql, "EXECUTION_FAILED");
    assert!(fx.handler.query_tracker().records().is_empty());
}

// -- Refused after classification, before the tracker starts ----------------

/// `CREATE TABLE ... AS SELECT` with a write branch set is refused after
/// classification: the `failed` closure writes the record, and the tracker
/// never starts.
#[tokio::test]
async fn a_refusal_after_classification_writes_one_failure_record() {
    let fx = fixture_opts(Arc::new(PassthroughEnforcer), FixtureOpts::default()).await;
    let mut session = session_for("refused-user");
    session.set_write_branch(Some("feature".to_string()));
    let sql = "CREATE TABLE iceberg.s.t AS SELECT 1 AS a";
    let err = fx
        .handler
        .execute(&session, sql, None)
        .await
        .expect_err("a CTAS cannot write to a branch");
    assert_eq!(err.error_code(), SqeErrorCode::NotSupported, "{err}");

    let record = Written::of(&fx).one_failure(sql, "NOT_SUPPORTED");
    assert_eq!(record["query"]["statement_type"], "ctas", "{record}");
    assert!(
        fx.handler.query_tracker().records().is_empty(),
        "refused before the tracker started"
    );
}

// -- After the tracker starts -----------------------------------------------

/// A policy denial at planning happens inside execution, so completion
/// writes the one record and preflight writes none.
#[tokio::test]
async fn an_execution_failure_writes_one_failure_record() {
    struct Deny;
    #[async_trait::async_trait]
    impl sqe_policy::PolicyEnforcer for Deny {
        async fn evaluate(
            &self,
            _user: &sqe_core::SessionUser,
            _scope: &sqe_policy::TableScope,
            _plan: datafusion::logical_expr::LogicalPlan,
        ) -> sqe_core::Result<(
            datafusion::logical_expr::LogicalPlan,
            sqe_policy::PolicySummary,
        )> {
            Err(SqeError::with_code(
                SqeErrorCode::AccessDenied,
                "access denied by policy",
            ))
        }
    }
    let fx = fixture_opts(Arc::new(Deny), FixtureOpts::default()).await;
    let sql = "SELECT 1 AS x";
    let err = fx
        .handler
        .execute(&session_for("exec-fail-user"), sql, None)
        .await
        .expect_err("denied");
    assert_eq!(err.error_code(), SqeErrorCode::AccessDenied, "{err}");

    Written::of(&fx).one_failure(sql, "ACCESS_DENIED");
    let records = fx.handler.query_tracker().records();
    assert_eq!(records.len(), 1, "the tracker started and finished it");
}

/// A cache hit writes one success record of its own, on top of the record of
/// the run that filled the entry. (`audit_planning_failure_test` pins the
/// record's content; this pins the count over the whole file.)
#[tokio::test]
async fn a_result_cache_hit_writes_one_success_record() {
    let fx = fixture_opts(
        Arc::new(PassthroughEnforcer),
        FixtureOpts {
            cache: true,
            ..FixtureOpts::default()
        },
    )
    .await;
    let session = session_for("cache-hit-user");
    let sql = "SELECT 1 AS a";
    fx.handler
        .execute(&session, sql, None)
        .await
        .expect("fills");
    let after_fill = Written::of(&fx);
    assert_eq!(after_fill.native.len(), 1, "the filling run: one record");

    fx.handler.execute(&session, sql, None).await.expect("hit");
    let both = Written::of(&fx);
    assert_eq!(both.native.len(), 2, "the hit adds exactly one record");
    assert_eq!(both.ocsf.len(), 2);
    let hit = &both.native[1];
    assert_eq!(hit["status"], "success", "{hit}");
    assert_eq!(hit["query"]["text"], sql, "{hit}");
    assert_eq!(hit["stats"]["rows_returned"], 1, "{hit}");
    assert_ne!(
        both.native[0]["query_id"], hit["query_id"],
        "two statements, two ids"
    );
}

/// A statement that outlives `query.timeout_secs` leaves one failure record
/// that names the timeout.
#[tokio::test]
async fn a_timeout_writes_one_failure_record() {
    let fx = fixture_opts(Arc::new(PassthroughEnforcer), stuck("timeout_secs = 1")).await;
    let sql = "SELECT 1 AS a";
    let started = Instant::now();
    let err = fx
        .handler
        .execute(&session_for("timeout-user"), sql, None)
        .await
        .expect_err("timed out");
    let waited = started.elapsed();
    assert!(
        waited < Duration::from_secs(20),
        "ended by the 1s timeout, not by the catalog's response: {waited:?}"
    );
    assert_eq!(err.error_code(), SqeErrorCode::ExecutionFailed, "{err}");
    assert!(err.to_string().contains("timed out after 1s"), "{err}");

    let record = Written::of(&fx).one_failure(sql, "EXECUTION_FAILED");
    assert!(
        record.to_string().contains("timed out after 1s"),
        "the record names the timeout: {record}"
    );
    assert_eq!(fx.handler.query_tracker().records().len(), 1);
}

/// Requests of `method` whose path ends with `suffix`, for diagnostics and
/// for proving the write reached the catalog's create-table call.
async fn catalog_requests(fx: &Fixture, http_method: &str, suffix: &str) -> usize {
    fx.server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| r.method.as_str() == http_method && r.url.path().ends_with(suffix))
        .count()
}

/// A CTAS cancelled while the catalog holds its create-table call has
/// already captured its plan (the write handler records the plan before the
/// create-table call), so the record names the plan's tables.
#[tokio::test]
async fn a_cancelled_ctas_still_audits_the_tables_of_its_plan() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};

    let fx = fixture_opts(Arc::new(PassthroughEnforcer), FixtureOpts::default()).await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/s"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"namespace":["s"],"properties":{}}"#),
        )
        .mount(&fx.server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/namespaces/s/tables"))
        .respond_with(ResponseTemplate::new(500).set_delay(STUCK))
        .mount(&fx.server)
        .await;

    let sql = "CREATE TABLE iceberg.s.t AS SELECT table_name FROM information_schema.tables";
    let id = uuid::Uuid::now_v7();
    let handler = Arc::clone(&fx.handler);
    let session = session_for("cancel-ctas-user");
    let run =
        tokio::spawn(async move { handler.execute_with_id(&session, sql, None, Some(id)).await });

    let deadline = Instant::now() + Duration::from_secs(20);
    while catalog_requests(&fx, "POST", "/v1/namespaces/s/tables").await == 0 {
        if Instant::now() >= deadline {
            let seen: Vec<String> = fx
                .server
                .received_requests()
                .await
                .unwrap_or_default()
                .iter()
                .map(|r| format!("{} {}", r.method, r.url.path()))
                .collect();
            panic!("the CTAS never reached create-table; catalog saw {seen:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        fx.handler.query_tracker().cancel(&id),
        "the query is active"
    );
    let err = tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("cancellation ends the statement")
        .expect("task")
        .expect_err("cancelled");
    assert!(err.to_string().contains("Query cancelled"), "{err}");

    let record = Written::of(&fx).one_failure(sql, "EXECUTION_FAILED");
    assert_eq!(record["query"]["statement_type"], "ctas", "{record}");
    assert!(
        record["message"]
            .as_str()
            .is_some_and(|m| m.contains("Query cancelled")),
        "{record}"
    );
    // The plan was captured before the create-table call: the audited
    // resources are the target and the source table, in this order.
    let resources: Vec<(String, String)> = record["resources"]
        .as_array()
        .expect("resources")
        .iter()
        .map(|r| {
            (
                r["namespace"].to_string().replace('"', ""),
                r["name"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    assert_eq!(
        resources,
        vec![
            ("[s]".to_string(), "t".to_string()),
            ("[information_schema]".to_string(), "tables".to_string())
        ],
        "{record}"
    );
}

/// A statement cancelled through the query tracker (the admin
/// `CancelQuery` action and Flight `CancelFlightInfo` both end there) leaves
/// one failure record.
#[tokio::test]
async fn a_cancellation_writes_one_failure_record() {
    let fx = fixture_opts(Arc::new(PassthroughEnforcer), stuck("")).await;
    let sql = "SELECT 1 AS a";
    let id = uuid::Uuid::now_v7();
    let handler = Arc::clone(&fx.handler);
    let session = session_for("cancel-user");
    let run =
        tokio::spawn(async move { handler.execute_with_id(&session, sql, None, Some(id)).await });

    wait_until_held(&fx).await;
    assert!(
        fx.handler.query_tracker().cancel(&id),
        "the query is active"
    );
    let err = tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("cancellation ends the statement")
        .expect("task")
        .expect_err("cancelled");
    assert_eq!(err.error_code(), SqeErrorCode::ExecutionFailed, "{err}");
    assert!(err.to_string().contains("Query cancelled"), "{err}");

    let record = Written::of(&fx).one_failure(sql, "EXECUTION_FAILED");
    assert!(
        record.to_string().contains("Query cancelled"),
        "the record names the cancellation: {record}"
    );
    assert_eq!(
        record["query_id"],
        id.to_string(),
        "the record carries the caller's id"
    );
}

// -- The audit-closed refusal -------------------------------------------------

/// A `TagLookup` that parks the audit writer thread until released, so the
/// bounded queue fills deterministically (no sleeps, no load).
#[derive(Default)]
struct ParkedLookup {
    state: std::sync::Mutex<(bool, bool)>, // (entered, released)
    cv: std::sync::Condvar,
}

impl ParkedLookup {
    fn wait_entered(&self) {
        let mut s = self.state.lock().unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !s.0 {
            let left = deadline
                .checked_duration_since(Instant::now())
                .expect("the audit writer never reached the lookup");
            s = self.cv.wait_timeout(s, left).unwrap().0;
        }
    }

    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.cv.notify_all();
    }
}

impl sqe_metrics::audit::TagLookup for ParkedLookup {
    fn column_tags(
        &self,
        _catalog: Option<&str>,
        _namespace: &[String],
        _table: &str,
    ) -> Option<std::collections::HashMap<String, Vec<String>>> {
        let mut s = self.state.lock().unwrap();
        s.0 = true;
        self.cv.notify_all();
        while !s.1 {
            s = self.cv.wait(s).unwrap();
        }
        Some(std::collections::HashMap::new())
    }
}

/// Releases the parked writer when dropped, so a failed assertion cannot
/// leave the logger's `Drop` waiting on a writer that never wakes.
struct Release(Arc<ParkedLookup>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// Under `fail_mode = closed` a full audit queue refuses the next statement
/// before classification, with no record and no tracker entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_audit_log_refuses_the_statement_and_writes_no_record() {
    use sqe_metrics::audit::{AuditFailMode, AuditLogger, AuditLoggerOptions, GdprIdentifierMode};

    let lookup = Arc::new(ParkedLookup::default());
    let for_logger = Arc::clone(&lookup);
    let fx = fixture_opts(
        Arc::new(PassthroughEnforcer),
        FixtureOpts {
            audit: Some(Box::new(move |path| {
                AuditLogger::with_options(
                    path,
                    sqe_metrics::audit::AuditFormat::Both,
                    AuditLoggerOptions {
                        queue_capacity: 1,
                        fail_mode: AuditFailMode::Closed,
                        ..AuditLoggerOptions::default()
                    },
                )
                .expect("audit logger opens")
                .with_gdpr(
                    vec!["pii".to_string()],
                    GdprIdentifierMode::Drop,
                    "salt".to_string(),
                    for_logger,
                )
            })),
            ..FixtureOpts::default()
        },
    )
    .await;
    // Declared after `fx`, so it drops first.
    let _release = Release(Arc::clone(&lookup));
    let session = session_for("audit-closed-user");
    let with_resource = "SELECT table_name FROM information_schema.tables";

    // 1: the writer takes this record and parks in the tag lookup.
    fx.handler
        .execute(&session, with_resource, None)
        .await
        .expect("first");
    tokio::task::block_in_place(|| lookup.wait_entered());
    assert!(fx.audit.is_writable());
    // 2: fills the one-slot queue.
    fx.handler
        .execute(&session, with_resource, None)
        .await
        .expect("second fills the queue");
    assert!(fx.audit.is_writable());
    // 3: finds the queue full; the statement itself still runs, its record is
    // dropped and the logger turns unwritable.
    fx.handler
        .execute(&session, with_resource, None)
        .await
        .expect("third runs, its record overflows");
    assert!(!fx.audit.is_writable(), "closed mode after an overflow");

    // 4: refused at the top of execute, before size check order matters.
    let err = fx
        .handler
        .execute(&session, "SELECT 1 AS a", None)
        .await
        .expect_err("audit closed");
    assert_eq!(err.error_code(), SqeErrorCode::InternalError, "{err}");
    assert!(
        err.to_string()
            .contains("audit logger unavailable (fail_mode=closed)"),
        "{err}"
    );
    // pins current behaviour: the refusal itself is not audited and has no
    // tracker entry (only statements 1-3 are in the tracker)
    assert_eq!(fx.handler.query_tracker().records().len(), 3);

    lookup.release();
    let (native, ocsf) = fx.records();
    // pins current behaviour: the overflowed third record is lost for good
    assert_eq!(native.len(), 2, "records 1 and 2 only: {native:?}");
    assert_eq!(ocsf.len(), 2);
    assert!(
        native.iter().all(|r| r["query"]["text"] != "SELECT 1 AS a"),
        "the refused statement left no record"
    );
}
