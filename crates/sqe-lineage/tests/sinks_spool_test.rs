use sqe_lineage::event::*;
use sqe_lineage::sink::Sink;
use sqe_lineage::sinks::http::{AuthMode, HttpConfig, HttpSink};
use sqe_lineage::sinks::spool::{SpoolConfig, SpoolSink};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

fn dummy_event() -> RunEvent {
    RunEvent {
        eventType: EventType::Start,
        eventTime: "2026-05-08T10:00:00Z".into(),
        producer: "test".into(),
        schemaURL: SCHEMA_URL.into(),
        run: Run::new(uuid::Uuid::nil()),
        job: Job { namespace: "sqe".into(), name: "query:test".into(), facets: Default::default() },
        inputs: vec![],
        outputs: vec![],
    }
}

#[tokio::test]
async fn spool_passes_through_on_success() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempdir().unwrap();
    let http = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", server.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 0,
    }).unwrap();

    let spool = SpoolSink::wrap(Arc::new(http), SpoolConfig {
        path: dir.path().to_path_buf(),
        max_bytes: 10 * 1024 * 1024,
        replay_interval: Duration::from_secs(60),
    });

    spool.send(&dummy_event()).await.unwrap();

    // Spool dir should be empty (or contain only an empty live file)
    let live = dir.path().join("spool.jsonl");
    let live_size = std::fs::metadata(&live).map(|m| m.len()).unwrap_or(0);
    assert_eq!(live_size, 0, "no spool write on success");
}

#[tokio::test]
async fn spool_buffers_on_http_failure() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let dir = tempdir().unwrap();
    let http = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", server.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 0,
    }).unwrap();

    let spool = SpoolSink::wrap(Arc::new(http), SpoolConfig {
        path: dir.path().to_path_buf(),
        max_bytes: 10 * 1024 * 1024,
        replay_interval: Duration::from_secs(60),  // long interval — no replay during test
    });

    // Send and expect Ok (spool returns Ok after appending to disk)
    spool.send(&dummy_event()).await.unwrap();

    let live = dir.path().join("spool.jsonl");
    let content = std::fs::read_to_string(&live).unwrap();
    assert_eq!(content.lines().count(), 1, "one event spooled");
    let _: RunEvent = serde_json::from_str(content.lines().next().unwrap()).unwrap();
}

#[tokio::test]
async fn spool_drops_newest_when_cap_reached() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let dir = tempdir().unwrap();
    let http = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", server.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 0,
    }).unwrap();

    // Tiny cap — first event fits, second is dropped
    let spool = SpoolSink::wrap(Arc::new(http), SpoolConfig {
        path: dir.path().to_path_buf(),
        max_bytes: 200,
        replay_interval: Duration::from_secs(60),
    });

    spool.send(&dummy_event()).await.unwrap();
    spool.send(&dummy_event()).await.unwrap();

    let live = dir.path().join("spool.jsonl");
    let line_count = std::fs::read_to_string(&live).unwrap().lines().count();

    // First event fit (~150 bytes serialised); second exceeded cap
    assert_eq!(line_count, 1, "second event dropped due to cap");
    assert_eq!(spool.dropped_count(), 1);
}

#[tokio::test]
async fn spool_drains_when_http_recovers() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let server = MockServer::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));

    // First N calls return 500, rest return 200
    let attempts_clone = attempts.clone();
    Mock::given(matchers::method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            let n = attempts_clone.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                ResponseTemplate::new(500)
            } else {
                ResponseTemplate::new(200)
            }
        })
        .mount(&server)
        .await;

    let dir = tempdir().unwrap();
    let http = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", server.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 0,
    }).unwrap();

    let spool = SpoolSink::wrap(Arc::new(http), SpoolConfig {
        path: dir.path().to_path_buf(),
        max_bytes: 10 * 1024 * 1024,
        replay_interval: Duration::from_millis(100),
    });

    spool.send(&dummy_event()).await.unwrap();  // 500 -> spooled

    let live = dir.path().join("spool.jsonl");
    assert!(std::fs::metadata(&live).unwrap().len() > 0);

    // Wait long enough for at least one replay tick + drain. The replay loop
    // wakes every 100ms; under parallel test load a single 300ms wait is too
    // tight, so we poll up to 2s for the spool dir to empty.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let total: u64 = std::fs::read_dir(dir.path()).unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| std::fs::metadata(e.path()).ok().map(|m| m.len()))
            .sum();
        if total == 0 || std::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // After replay, the rotated file should be drained.
    // Live file may exist but be empty (or rotated away).
    let total_spool: u64 = std::fs::read_dir(dir.path()).unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| std::fs::metadata(e.path()).ok().map(|m| m.len()))
        .sum();
    assert_eq!(total_spool, 0, "spool should be empty after recovery + replay");
}
