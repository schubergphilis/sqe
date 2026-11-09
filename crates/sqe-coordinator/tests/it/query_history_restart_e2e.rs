//! `system.runtime.query_history` keeps a finished query across a
//! coordinator restart (#812).
//!
//! `system.runtime.queries` reads the in-memory tracker, so a restart empties
//! it. `query_history` also reads the native audit log, which survives. The
//! test runs a query on one handler, drops it, builds a second handler over
//! the same audit file with a fresh tracker, and looks the query up on both
//! tables.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_metrics::audit::{AuditFormat, AuditLogger};
use sqe_policy::PassthroughEnforcer;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A handler over a mock REST catalog with no namespaces, auditing to
/// `audit` in `format = "both"`, with a fresh tracker.
fn handler(server: &MockServer, audit: &Path) -> (QueryHandler, Arc<AuditLogger>) {
    let logger = Arc::new(
        AuditLogger::with_config(audit.to_str().unwrap(), AuditFormat::Both)
            .expect("audit logger opens"),
    );
    let toml = format!(
        "[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{}\"\n\n\
         [metrics]\naudit_log_path = \"{}\"\n",
        server.uri(),
        audit.display()
    );
    let config: SqeConfig = toml::from_str(&toml).expect("config");
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config.clone(),
        None,
        None,
        None,
        Some(logger.clone()),
        Arc::new(QueryTracker::new(&config.query_history)),
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new");
    (handler, logger)
}

fn session() -> Session {
    Session::new(
        "historian".to_string(),
        sqe_core::SecretString::new(format!("tok-{}", uuid::Uuid::new_v4())),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        vec!["analyst".to_string()],
    )
}

fn strings(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let column = arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Utf8)
                .expect("cast to Utf8");
            let column = column.as_any().downcast_ref::<StringArray>().unwrap();
            (0..column.len())
                .map(|i| column.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_history_restart_keeps_finished_queries() {
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
    let audit = dir.path().join("audit.jsonl");

    let marker = format!("qh-{}", uuid::Uuid::new_v4());
    let sql = format!("SELECT '{marker}' AS marker");
    {
        let (before, logger) = handler(&server, &audit);
        let rows = before.execute(&session(), &sql, None).await.expect("query");
        assert_eq!(strings(&rows), std::slice::from_ref(&marker));
        logger.flush();
    }

    // The restarted coordinator: same audit file, empty tracker.
    let (after, _logger) = handler(&server, &audit);
    let quoted = sql.replace('\'', "''");
    let history =
        format!("SELECT state FROM system.runtime.query_history WHERE query = '{quoted}'");
    let rows = after
        .execute(&session(), &history, None)
        .await
        .expect("history");
    assert_eq!(strings(&rows), ["FINISHED"], "the audited query survives");

    let live = format!("SELECT state FROM system.runtime.queries WHERE query = '{quoted}'");
    let rows = after
        .execute(&session(), &live, None)
        .await
        .expect("queries");
    assert!(strings(&rows).is_empty(), "the tracker starts empty");
}
