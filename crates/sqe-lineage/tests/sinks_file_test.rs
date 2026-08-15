use sqe_lineage::event::*;
use sqe_lineage::sinks::file::FileSink;
use sqe_lineage::*;
use tempfile::tempdir;

fn dummy_event() -> RunEvent {
    RunEvent {
        eventType: EventType::Start,
        eventTime: "2026-05-08T10:00:00Z".into(),
        producer: "test".into(),
        schemaURL: SCHEMA_URL.into(),
        run: Run::new(uuid::Uuid::nil()),
        job: Job {
            namespace: "sqe".into(),
            name: "query:test".into(),
            facets: Default::default(),
        },
        inputs: vec![],
        outputs: vec![],
    }
}

#[tokio::test]
async fn file_sink_appends_jsonl_one_line_per_event() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    let sink = FileSink::new(path.to_str().unwrap()).unwrap();
    sink.send(&dummy_event()).await.unwrap();
    sink.send(&dummy_event()).await.unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content.lines().count(), 2, "two events => two lines");
    for line in content.lines() {
        let _: RunEvent = serde_json::from_str(line).expect("each line round-trips RunEvent");
    }
}

#[tokio::test]
async fn file_sink_persists_across_drop_and_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("ol.jsonl");
    {
        let sink = FileSink::new(path.to_str().unwrap()).unwrap();
        sink.send(&dummy_event()).await.unwrap();
    }
    // First sink dropped; reopen and append
    let sink = FileSink::new(path.to_str().unwrap()).unwrap();
    sink.send(&dummy_event()).await.unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content.lines().count(), 2);
}
