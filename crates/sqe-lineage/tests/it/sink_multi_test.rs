use sqe_lineage::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Counting {
    count: AtomicUsize,
    fail: bool,
}

#[async_trait::async_trait]
impl Sink for Counting {
    async fn send(&self, _: &event::RunEvent) -> Result<(), SinkError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            Err(SinkError::Other("boom".into()))
        } else {
            Ok(())
        }
    }
    fn name(&self) -> &'static str {
        "counting"
    }
}

fn dummy_event() -> event::RunEvent {
    event::RunEvent {
        eventType: event::EventType::Start,
        eventTime: "2026-05-08T10:00:00Z".into(),
        producer: "test".into(),
        schemaURL: event::SCHEMA_URL.into(),
        run: event::Run::new(uuid::Uuid::nil()),
        job: event::Job {
            namespace: "sqe".into(),
            name: "query:test".into(),
            facets: Default::default(),
        },
        inputs: vec![],
        outputs: vec![],
    }
}

#[tokio::test]
async fn multi_sink_fans_out_and_isolates_failures() {
    let a = Arc::new(Counting {
        count: AtomicUsize::new(0),
        fail: false,
    });
    let b = Arc::new(Counting {
        count: AtomicUsize::new(0),
        fail: true,
    });
    let c = Arc::new(Counting {
        count: AtomicUsize::new(0),
        fail: false,
    });

    let multi = MultiSink::new(vec![
        a.clone() as Arc<dyn Sink>,
        b.clone() as Arc<dyn Sink>,
        c.clone() as Arc<dyn Sink>,
    ]);

    multi.send(&dummy_event()).await;

    assert_eq!(a.count.load(Ordering::SeqCst), 1);
    assert_eq!(
        b.count.load(Ordering::SeqCst),
        1,
        "b ran even though it returns an error"
    );
    assert_eq!(
        c.count.load(Ordering::SeqCst),
        1,
        "c ran even though b failed before it"
    );
}
