use sqe_lineage::event::*;
use chrono::Utc;
use uuid::Uuid;

#[test]
fn run_event_serialises_with_required_fields() {
    let ev = RunEvent {
        eventType: EventType::Start,
        eventTime: Utc::now().to_rfc3339(),
        producer: "https://github.com/sbp/sqe/v0.1.0".to_string(),
        schemaURL: SCHEMA_URL.to_string(),
        run: Run::new(Uuid::new_v4()),
        job: Job { namespace: "sqe".into(), name: "query:abc".into(), facets: Default::default() },
        inputs: vec![],
        outputs: vec![],
    };
    let json = serde_json::to_value(&ev).unwrap();
    assert_eq!(json["eventType"], "START");
    assert_eq!(json["schemaURL"], SCHEMA_URL);
    assert!(json["run"]["runId"].is_string());
    assert_eq!(json["job"]["namespace"], "sqe");
}
