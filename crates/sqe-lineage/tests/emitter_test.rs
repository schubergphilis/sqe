use sqe_lineage::*;
use sqe_lineage::sinks::file::FileSink;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

fn cfg() -> Arc<EmitterConfig> {
    Arc::new(EmitterConfig {
        job_namespace: "sqe-test".into(),
        producer: "https://test/v0".into(),
        catalog_lookup: Arc::new(|n| format!("sqe://{n}")),
    })
}

#[tokio::test]
async fn emitter_drains_channel_and_writes_events_to_file_sink() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let file = FileSink::new(path.to_str().unwrap()).unwrap();
    let multi = Arc::new(MultiSink::new(vec![Arc::new(file) as Arc<dyn Sink>]));

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let _emitter = spawn_emitter(rx, multi, cfg());

    // Push a START + COMPLETE pair.
    let counter = prometheus::IntCounter::new("test", "test").unwrap();
    let obs = ChannelObserver::new(tx, counter);
    obs.on_query_start(QueryStartCtx::dummy());
    obs.on_query_complete(QueryCompleteCtx::dummy());

    // Wait briefly for the emitter to drain.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content.lines().count(), 2, "two events written");

    // Both events parse as RunEvents.
    let events: Vec<event::RunEvent> = content.lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(events[0].eventType, event::EventType::Start);
    assert_eq!(events[1].eventType, event::EventType::Complete);
    assert_eq!(events[0].job.namespace, "sqe-test");
}
