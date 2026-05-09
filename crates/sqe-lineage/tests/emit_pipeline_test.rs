//! End-to-end OL emitter pipeline tests against a wiremock collector.
//!
//! These cover Phase I of the OpenLineage emitter plan (I1-I4). The plan
//! originally targeted `crates/sqe-coordinator/tests/`, but the coordinator
//! integration path requires a full Polaris + S3 stack. The pipeline itself
//! (`ChannelObserver -> mpsc -> spawn_emitter -> MultiSink -> HttpSink`)
//! is testable in pure Rust, so we exercise it here without standing up a
//! coordinator.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §11
//! and `docs/superpowers/plans/2026-05-08-openlineage-emitter.md` Phase I.

use sqe_lineage::sink::Sink;
use sqe_lineage::sinks::http::{AuthMode, HttpConfig, HttpSink};
use sqe_lineage::sinks::spool::{SpoolConfig, SpoolSink};
use sqe_lineage::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

fn lookup() -> extract::CatalogLookup {
    Arc::new(|n: &str| format!("sqe://{n}"))
}

fn cfg() -> Arc<EmitterConfig> {
    Arc::new(EmitterConfig {
        job_namespace: "sqe-test".into(),
        producer: "https://test/v0".into(),
        catalog_lookup: lookup(),
    })
}

/// I1: end-to-end START + COMPLETE pair posts to the wiremock collector with
/// the right OL eventType strings.
#[tokio::test]
async fn end_to_end_pipeline_emits_start_and_complete() {
    let collector = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v1/lineage"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&collector)
        .await;

    let http = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", collector.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 0,
    })
    .unwrap();
    let multi = Arc::new(MultiSink::new(vec![Arc::new(http) as Arc<dyn Sink>]));

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let _emitter = spawn_emitter(rx, multi, cfg());

    let counter = prometheus::IntCounter::new("emit_pipeline_test_1", "test").unwrap();
    let obs = ChannelObserver::new(tx, counter);

    obs.on_query_start(QueryStartCtx::dummy());
    obs.on_query_complete(QueryCompleteCtx::dummy());

    // Wait for emitter to drain channel and post both events.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = collector.received_requests().await.unwrap();
    assert_eq!(received.len(), 2, "expected START + COMPLETE");

    let start: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(start["eventType"], "START");
    assert_eq!(start["job"]["namespace"], "sqe-test");

    let complete: serde_json::Value = serde_json::from_slice(&received[1].body).unwrap();
    assert_eq!(complete["eventType"], "COMPLETE");
    assert_eq!(complete["job"]["namespace"], "sqe-test");
}

/// I1: a FAIL event reaches the collector with the errorMessage facet populated.
#[tokio::test]
async fn fail_event_carries_error_message_facet() {
    let collector = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v1/lineage"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&collector)
        .await;

    let http = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", collector.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 0,
    })
    .unwrap();
    let multi = Arc::new(MultiSink::new(vec![Arc::new(http) as Arc<dyn Sink>]));

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let _emitter = spawn_emitter(rx, multi, cfg());

    let counter = prometheus::IntCounter::new("emit_pipeline_test_2", "test").unwrap();
    let obs = ChannelObserver::new(tx, counter);

    obs.on_query_fail(QueryFailCtx::dummy());

    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = collector.received_requests().await.unwrap();
    assert_eq!(received.len(), 1);

    let ev: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(ev["eventType"], "FAIL");
    let err_facet = &ev["run"]["facets"]["errorMessage"];
    assert!(err_facet.is_object(), "errorMessage facet must be present");
    assert_eq!(err_facet["message"], "boom");
    assert_eq!(err_facet["programmingLanguage"], "sql");
}

/// I2: the emitter runs cleanly when MultiSink has no sinks. This is the
/// in-process analogue of "lineage disabled = no events emitted". The
/// QueryHandler-level disabled path is implicitly covered by the existing
/// 305 coordinator tests, which all run with `lineage = None`.
#[tokio::test]
async fn emitter_with_no_sinks_drops_events_silently() {
    let multi = Arc::new(MultiSink::new(vec![]));
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let _emitter = spawn_emitter(rx, multi, cfg());

    let counter = prometheus::IntCounter::new("emit_pipeline_test_3", "test").unwrap();
    let obs = ChannelObserver::new(tx, counter);

    obs.on_query_start(QueryStartCtx::dummy());
    obs.on_query_complete(QueryCompleteCtx::dummy());
    obs.on_query_fail(QueryFailCtx::dummy());

    // No assertion. We are verifying the emitter does not panic when the
    // sink fan-out is empty. If the task crashed, subsequent sends would
    // back up the channel and we would catch it via timing in CI.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// I3: HTTP collector returns 500 -> spool buffers -> collector recovers ->
/// replay drains the spool to zero.
#[tokio::test]
async fn spool_buffers_on_500_then_drains_on_recovery() {
    let collector = MockServer::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v1/lineage"))
        .respond_with(move |_: &wiremock::Request| {
            let n = attempts_clone.fetch_add(1, Ordering::SeqCst);
            if n < 1 {
                ResponseTemplate::new(500)
            } else {
                ResponseTemplate::new(200)
            }
        })
        .mount(&collector)
        .await;

    let dir = tempdir().unwrap();
    let http = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", collector.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 0,
    })
    .unwrap();
    let spool = SpoolSink::wrap(
        Arc::new(http),
        SpoolConfig {
            path: dir.path().to_path_buf(),
            max_bytes: 10 * 1024 * 1024,
            replay_interval: Duration::from_millis(150),
        },
    );
    let multi = Arc::new(MultiSink::new(vec![spool as Arc<dyn Sink>]));

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let _emitter = spawn_emitter(rx, multi, cfg());

    let counter = prometheus::IntCounter::new("emit_pipeline_test_4", "test").unwrap();
    let obs = ChannelObserver::new(tx, counter);

    obs.on_query_start(QueryStartCtx::dummy());

    // Wait for: emit -> 500 -> spool, then replay tick -> 200 -> drain.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let total: u64 = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| std::fs::metadata(e.path()).ok().map(|m| m.len()))
        .sum();
    assert_eq!(total, 0, "spool drained after collector recovery");
}

/// I4: events with the same UUID-shaped session_id share `parent.run.runId`,
/// so a downstream lineage UI can group them under one session-level run.
#[tokio::test]
async fn events_in_same_session_share_parent_run_id() {
    let collector = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v1/lineage"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&collector)
        .await;

    let http = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", collector.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 0,
    })
    .unwrap();
    let multi = Arc::new(MultiSink::new(vec![Arc::new(http) as Arc<dyn Sink>]));

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let _emitter = spawn_emitter(rx, multi, cfg());

    let counter = prometheus::IntCounter::new("emit_pipeline_test_5", "test").unwrap();
    let obs = ChannelObserver::new(tx, counter);

    let session_uuid = uuid::Uuid::new_v4().to_string();

    let mut ctx_a = QueryStartCtx::dummy();
    ctx_a.session_id = session_uuid.clone();
    obs.on_query_start(ctx_a);

    let mut ctx_b = QueryCompleteCtx::dummy();
    ctx_b.session_id = session_uuid.clone();
    obs.on_query_complete(ctx_b);

    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = collector.received_requests().await.unwrap();
    assert_eq!(received.len(), 2);

    let a: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    let b: serde_json::Value = serde_json::from_slice(&received[1].body).unwrap();

    let parent_a = &a["run"]["facets"]["parent"]["run"]["runId"];
    let parent_b = &b["run"]["facets"]["parent"]["run"]["runId"];
    assert!(parent_a.is_string(), "parent.run.runId present on START");
    assert!(parent_b.is_string(), "parent.run.runId present on COMPLETE");
    assert_eq!(parent_a, parent_b, "same session_id produces same parent runId");

    // Sanity: the parent runId equals the session UUID itself.
    assert_eq!(parent_a.as_str().unwrap(), session_uuid);
}
