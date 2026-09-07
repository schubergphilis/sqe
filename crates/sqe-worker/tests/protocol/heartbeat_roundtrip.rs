//! Finding 611 item 1 / finding 609: the heartbeat task against a coordinator
//! that speaks the `sqe-planner` hello types.
//!
//! `sqe-coordinator` is not a dev-dependency of this crate (and pulling the
//! whole coordinator in for one handler would double this binary's build), so
//! the coordinator side is a minimal in-test Flight `do_action` stub that
//! decodes `WorkerHello`, keeps a last-seen registry and answers with a
//! `CoordinatorHello` in the generation each scenario selects. The real
//! handler is tested in `sqe-coordinator` against the same planner types.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use sqe_core::ticket_signing::TicketSigner;
use sqe_planner::{
    CoordinatorHello, WorkerFault, WorkerHello, HELLO_VERSION, SCAN_TASK_VERSION_CURRENT,
};
use sqe_worker::heartbeat::start_heartbeat_task;
use sqe_worker::status::{build_identifier, WorkerStatus};
use sqe_worker::ticket_pin::TicketKeyPin;
use tonic::{Request, Response, Status, Streaming};

use super::fixtures::{wait_for, ServedFlight};

const WORKER_URL: &str = "http://worker-1.test:50052";
const INTERVAL: Duration = Duration::from_millis(100);
/// A worker is healthy while its last heartbeat is younger than this.
const HEALTH_TTL: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug)]
enum ReplyMode {
    /// Current coordinator: `CoordinatorHello::accepted` with the ticket key.
    Accept,
    /// Current coordinator that answers Ok but does not admit the worker.
    Reject,
    /// Pre-hello coordinator: bare `ok`.
    LegacyOk,
    /// Coordinator that refuses the RPC itself (version skew).
    RefuseFailedPrecondition,
}

#[derive(Debug)]
struct Seen {
    last: Instant,
    count: u32,
    hello: WorkerHello,
}

#[derive(Clone)]
struct StubCoordinator {
    mode: ReplyMode,
    ticket_key_b64: String,
    registry: Arc<Mutex<HashMap<String, Seen>>>,
}

impl StubCoordinator {
    fn new(mode: ReplyMode, ticket_key_b64: &str) -> Self {
        Self {
            mode,
            ticket_key_b64: ticket_key_b64.to_string(),
            registry: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn snapshot(&self, url: &str) -> Option<(Instant, u32, WorkerHello)> {
        self.registry
            .lock()
            .unwrap()
            .get(url)
            .map(|s| (s.last, s.count, s.hello.clone()))
    }

    fn healthy(&self, url: &str) -> bool {
        self.snapshot(url)
            .is_some_and(|(last, _, _)| last.elapsed() < HEALTH_TTL)
    }
}

type Boxed<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for StubCoordinator {
    type HandshakeStream = Boxed<HandshakeResponse>;
    type ListFlightsStream = Boxed<FlightInfo>;
    type DoGetStream = Boxed<FlightData>;
    type DoPutStream = Boxed<PutResult>;
    type DoExchangeStream = Boxed<FlightData>;
    type DoActionStream = Boxed<arrow_flight::Result>;
    type ListActionsStream = Boxed<ActionType>;

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        if action.r#type != "heartbeat" {
            return Err(Status::unimplemented("stub coordinator: heartbeat only"));
        }
        let hello = WorkerHello::parse(&action.body).map_err(Status::invalid_argument)?;
        {
            let mut reg = self.registry.lock().unwrap();
            let entry = reg.entry(hello.url.clone()).or_insert(Seen {
                last: Instant::now(),
                count: 0,
                hello: hello.clone(),
            });
            entry.last = Instant::now();
            entry.count += 1;
            entry.hello = hello;
        }
        let body = match self.mode {
            ReplyMode::Accept => CoordinatorHello::accepted("test-cluster", "coord-build")
                .with_ticket_public_key(self.ticket_key_b64.clone())
                .encode(),
            ReplyMode::Reject => {
                CoordinatorHello::rejected("test-cluster", "coord-build", "single-node mode")
                    .encode()
            }
            ReplyMode::LegacyOk => b"ok".to_vec(),
            ReplyMode::RefuseFailedPrecondition => {
                return Err(Status::failed_precondition(
                    "ScanTask version 2 (worker) < 3 (coordinator)",
                ));
            }
        };
        let result = arrow_flight::Result {
            body: bytes::Bytes::from(body),
        };
        Ok(Response::new(Box::pin(futures::stream::once(async {
            Ok(result)
        }))))
    }

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn do_get(&self, _: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("stub"))
    }
}

struct Rig {
    coordinator: StubCoordinator,
    _served: ServedFlight,
    status: Arc<WorkerStatus>,
    pin: Arc<TicketKeyPin>,
    handle: tokio::task::JoinHandle<()>,
}

async fn rig(mode: ReplyMode) -> Rig {
    let signer = TicketSigner::generate().expect("signer");
    let coordinator = StubCoordinator::new(mode, signer.public_key_b64());
    let served = ServedFlight::start(coordinator.clone()).await;
    let status = WorkerStatus::new();
    let pin = Arc::new(TicketKeyPin::floating());
    let handle = start_heartbeat_task(
        served.url(),
        WORKER_URL.to_string(),
        INTERVAL,
        String::new(),
        None,
        Some(Arc::clone(&pin)),
        Arc::clone(&status),
    );
    Rig {
        coordinator,
        _served: served,
        status,
        pin,
        handle,
    }
}

/// The worker registers within two intervals with a full hello, reports
/// worker-attributable failures exactly once each, and ages out of the
/// registry once the task stops.
#[tokio::test]
async fn heartbeat_roundtrip() {
    let rig = rig(ReplyMode::Accept).await;

    // The first tick is consumed at task start, so the first hello lands
    // after one interval; two intervals is the acceptance bound.
    let (_, _, hello) = wait_for(2 * INTERVAL, || rig.coordinator.snapshot(WORKER_URL))
        .await
        .expect("worker registered within two intervals");
    assert!(rig.coordinator.healthy(WORKER_URL));
    assert_eq!(hello.v, HELLO_VERSION);
    assert!(!hello.is_legacy());
    assert_eq!(hello.url, WORKER_URL);
    assert_eq!(hello.instance_id, rig.status.instance_id());
    assert_eq!(hello.build, build_identifier());
    assert_eq!(hello.scan_task_version, SCAN_TASK_VERSION_CURRENT);
    assert_eq!(hello.datafusion_version, datafusion::DATAFUSION_VERSION);
    assert_eq!(hello.capacity.inflight_scans, 0);
    assert_eq!(hello.failures_since_last, 0);
    assert!(rig.status.coordinator_accepted());
    assert!(
        rig.pin.verifier().is_some(),
        "ticket public key from the reply must be pinned"
    );

    // A worker fault is reported on the next hello and acknowledged after
    // it; a user fault is never reported.
    rig.status.record_fault(WorkerFault::WorkerInternal);
    rig.status.record_fault(WorkerFault::PrincipalDenied);
    let (_, count_with_failure, _) = wait_for(3 * INTERVAL, || {
        rig.coordinator
            .snapshot(WORKER_URL)
            .filter(|(_, _, h)| h.failures_since_last == 1)
    })
    .await
    .expect("hello carrying the failure");
    wait_for(3 * INTERVAL, || {
        rig.coordinator
            .snapshot(WORKER_URL)
            .filter(|(_, count, h)| *count > count_with_failure && h.failures_since_last == 0)
    })
    .await
    .expect("failure acknowledged on the following hello");
    assert_eq!(rig.status.failures_since_last(), 0);

    // Stop heartbeating: the registry entry ages out.
    rig.handle.abort();
    let (_, count_at_stop, _) = rig.coordinator.snapshot(WORKER_URL).unwrap();
    tokio::time::sleep(HEALTH_TTL + INTERVAL).await;
    let (_, count_after, _) = rig.coordinator.snapshot(WORKER_URL).unwrap();
    assert!(
        count_after <= count_at_stop + 1,
        "at most one in-flight heartbeat may land after abort: {count_at_stop} -> {count_after}"
    );
    assert!(
        !rig.coordinator.healthy(WORKER_URL),
        "worker must age out once it stops heartbeating"
    );
}

/// `accepted: false` is surfaced (logged every tick, visible on the status)
/// but the worker keeps running and keeps heartbeating.
#[tokio::test]
async fn rejected_hello_is_reported_every_tick_not_fatal() {
    let rig = rig(ReplyMode::Reject).await;
    wait_for(2 * INTERVAL, || rig.coordinator.snapshot(WORKER_URL))
        .await
        .expect("heartbeat delivered");
    wait_for(INTERVAL, || {
        (!rig.status.coordinator_accepted()).then_some(())
    })
    .await
    .expect("rejection recorded on the status");
    let (_, first, _) = rig.coordinator.snapshot(WORKER_URL).unwrap();
    wait_for(3 * INTERVAL, || {
        rig.coordinator
            .snapshot(WORKER_URL)
            .filter(|(_, count, _)| *count > first)
    })
    .await
    .expect("worker keeps heartbeating after a rejection");
    assert!(!rig.status.coordinator_accepted());
    assert!(
        rig.pin.verifier().is_none(),
        "a rejecting reply carries no key"
    );
    rig.handle.abort();
}

/// A pre-hello coordinator answers `ok`: accepted, no key to pin, warned once.
#[tokio::test]
async fn legacy_ok_reply_is_tolerated() {
    let rig = rig(ReplyMode::LegacyOk).await;
    wait_for(2 * INTERVAL, || rig.coordinator.snapshot(WORKER_URL))
        .await
        .expect("heartbeat delivered");
    let (_, first, _) = rig.coordinator.snapshot(WORKER_URL).unwrap();
    wait_for(3 * INTERVAL, || {
        rig.coordinator
            .snapshot(WORKER_URL)
            .filter(|(_, count, _)| *count > first)
    })
    .await
    .expect("worker keeps heartbeating a legacy coordinator");
    assert!(rig.status.coordinator_accepted());
    assert!(rig.pin.verifier().is_none());
    rig.handle.abort();
}

/// The coordinator refusing the RPC (version skew: FailedPrecondition) is
/// recorded as not admitted; the worker never receives a ticket because it is
/// never in the registry, and it keeps retrying so an upgraded coordinator
/// picks it up.
#[tokio::test]
async fn failed_precondition_from_coordinator_marks_not_admitted() {
    let rig = rig(ReplyMode::RefuseFailedPrecondition).await;
    wait_for(2 * INTERVAL, || rig.coordinator.snapshot(WORKER_URL))
        .await
        .expect("heartbeat delivered");
    wait_for(INTERVAL, || {
        (!rig.status.coordinator_accepted()).then_some(())
    })
    .await
    .expect("refusal recorded on the status");
    let (_, first, _) = rig.coordinator.snapshot(WORKER_URL).unwrap();
    wait_for(3 * INTERVAL, || {
        rig.coordinator
            .snapshot(WORKER_URL)
            .filter(|(_, count, _)| *count > first)
    })
    .await
    .expect("worker keeps retrying after a refusal");
    rig.handle.abort();
}
