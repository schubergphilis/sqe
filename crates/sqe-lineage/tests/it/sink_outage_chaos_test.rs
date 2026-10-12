//! OpenLineage collector outages (#816): queries are never held up, a full
//! spool keeps the newest events and counts what it dropped, and the spool
//! replays in order once the collector is back.

use sqe_lineage::event::*;
use sqe_lineage::sink::Sink;
use sqe_lineage::sinks::http::{AuthMode, HttpConfig, HttpSink};
use sqe_lineage::sinks::spool::{SpoolConfig, SpoolSink};
use sqe_lineage::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::tempdir;
use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

fn event(i: usize) -> RunEvent {
    RunEvent {
        eventType: EventType::Start,
        eventTime: "2026-10-12T10:00:00Z".into(),
        producer: "test".into(),
        schemaURL: SCHEMA_URL.into(),
        run: Run::new(uuid::Uuid::nil()),
        job: Job {
            namespace: "sqe".into(),
            name: format!("job-{i:02}"),
            facets: Default::default(),
        },
        inputs: vec![],
        outputs: vec![],
    }
}

fn line_len() -> u64 {
    serde_json::to_string(&event(0)).unwrap().len() as u64 + 1
}

fn http_sink(server: &MockServer) -> Arc<dyn Sink> {
    Arc::new(
        HttpSink::new(HttpConfig {
            endpoint: format!("{}/api/v1/lineage", server.uri()),
            auth: AuthMode::None,
            timeout_ms: 2000,
            retry_attempts: 0,
        })
        .unwrap(),
    )
}

/// Job names in the spool, oldest first.
fn spooled_jobs(dir: &std::path::Path) -> Vec<String> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("spool.jsonl.")
        })
        .collect();
    files.sort();
    files.push(dir.join("spool.jsonl"));
    files
        .iter()
        .filter_map(|f| std::fs::read_to_string(f).ok())
        .flat_map(|c| {
            c.lines()
                .filter(|l| !l.is_empty())
                .map(|l| serde_json::from_str::<RunEvent>(l).unwrap().job.name)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn spool_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("spool.jsonl"))
        .map(|e| e.metadata().unwrap().len())
        .sum()
}

#[tokio::test]
async fn a_full_spool_drops_the_oldest_events_and_counts_them() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let dir = tempdir().unwrap();
    let cap = 5 * line_len();
    let counter = prometheus::IntCounter::new("chaos_spool_dropped", "test").unwrap();
    let spool = SpoolSink::wrap_with_counter(
        http_sink(&server),
        SpoolConfig {
            path: dir.path().to_path_buf(),
            max_bytes: cap,
            replay_interval: Duration::from_secs(3600),
        },
        Some(counter.clone()),
    );

    for i in 0..20 {
        spool
            .send(&event(i))
            .await
            .expect("a spooled send never fails");
    }

    let kept = spooled_jobs(dir.path());
    assert!(!kept.is_empty() && kept.len() <= 5, "kept: {kept:?}");
    let expected: Vec<String> = (20 - kept.len()..20)
        .map(|i| format!("job-{i:02}"))
        .collect();
    assert_eq!(kept, expected, "the newest events are kept, in order");
    assert_eq!(spool.dropped_count(), (20 - kept.len()) as u64);
    assert_eq!(counter.get(), spool.dropped_count());
    assert!(
        spool_bytes(dir.path()) <= cap,
        "the spool stays under its cap"
    );
}

#[tokio::test]
async fn the_spool_replays_in_order_when_the_collector_returns() {
    let server = MockServer::start().await;
    let up = Arc::new(AtomicBool::new(false));
    let delivered = Arc::new(Mutex::new(Vec::<String>::new()));
    let (up_c, delivered_c) = (up.clone(), delivered.clone());
    Mock::given(matchers::method("POST"))
        .respond_with(move |req: &wiremock::Request| {
            if !up_c.load(Ordering::SeqCst) {
                return ResponseTemplate::new(503);
            }
            let ev: RunEvent = serde_json::from_slice(&req.body).unwrap();
            delivered_c.lock().unwrap().push(ev.job.name);
            ResponseTemplate::new(200)
        })
        .mount(&server)
        .await;
    let dir = tempdir().unwrap();
    let spool = SpoolSink::wrap(
        http_sink(&server),
        SpoolConfig {
            path: dir.path().to_path_buf(),
            max_bytes: 10 * 1024 * 1024,
            replay_interval: Duration::from_millis(50),
        },
    );

    // Down long enough for several replay ticks to fail.
    for i in 0..10 {
        spool.send(&event(i)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(delivered.lock().unwrap().is_empty());
    up.store(true, Ordering::SeqCst);

    let deadline = Instant::now() + Duration::from_secs(10);
    while spool_bytes(dir.path()) > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(spool_bytes(dir.path()), 0, "spool drained after recovery");
    let expected: Vec<String> = (0..10).map(|i| format!("job-{i:02}")).collect();
    assert_eq!(
        *delivered.lock().unwrap(),
        expected,
        "replayed once each, in order"
    );
    assert_eq!(spool.dropped_count(), 0);
}

/// A collector that accepts connections but never answers must not slow the
/// query path: the observer only `try_send`s, so a full channel drops (and
/// counts) lineage instead of waiting.
#[tokio::test]
async fn a_hung_collector_never_blocks_queries() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
        .mount(&server)
        .await;
    let multi = Arc::new(MultiSink::new(vec![http_sink(&server)]));
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let cfg = Arc::new(EmitterConfig {
        job_namespace: "sqe".into(),
        producer: "test".into(),
        catalog_lookup: Arc::new(|n: &str| extract::CatalogInfo::new(n, None)),
    });
    let _emitter = spawn_emitter(rx, multi, cfg);
    let counter = prometheus::IntCounter::new("chaos_channel_dropped", "test").unwrap();
    let obs = ChannelObserver::new(tx, counter.clone());

    let started = Instant::now();
    for _ in 0..200 {
        obs.on_query_start(QueryStartCtx::dummy());
    }
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "{:?}",
        started.elapsed()
    );
    assert!(counter.get() >= 190, "dropped {} of 200", counter.get());
}
