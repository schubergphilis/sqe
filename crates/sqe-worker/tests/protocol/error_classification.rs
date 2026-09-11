//! Finding 611 item 3 / finding 602: typed object-store and budget failures
//! leave the worker as a `WorkerFault` in the `x-sqe-fault` metadata with the
//! matching gRPC code, at setup and mid-stream, in-process and over tonic.
//!
//! The registry-side half (which faults mark a worker unhealthy and which
//! retry where) is the coordinator's and is tested there against the same
//! `WorkerFault` predicates; here we check the worker-side counter that the
//! hello reports: user faults never count, worker faults do.

use futures::StreamExt;
use sqe_planner::WorkerFault;
use sqe_worker::executor::ScanBudget;
use tonic::{Code, Request, Status};

use super::fixtures::{
    do_get, drain, fault_label, worker_service, FaultKind, ServedFlight, StubBucket,
};

fn assert_fault(status: &Status, fault: WorkerFault) {
    assert_eq!(status.code(), fault.grpc_code(), "{status}");
    assert_eq!(
        fault_label(status).as_deref(),
        Some(fault.metric_label()),
        "{status}"
    );
}

/// The first file fails: `do_get` itself returns the classified Status.
#[tokio::test]
async fn setup_faults_carry_fault_metadata_and_grpc_codes() {
    let cases = [
        (
            FaultKind::Denied,
            WorkerFault::PrincipalDenied,
            Code::PermissionDenied,
        ),
        (
            FaultKind::Missing,
            WorkerFault::SourceMissing,
            Code::NotFound,
        ),
        (
            FaultKind::Throttled,
            WorkerFault::SourceThrottled,
            Code::Unavailable,
        ),
    ];
    for (kind, fault, code) in cases {
        let mut bucket = StubBucket::new();
        bucket.fail("data/first.parquet", kind);
        let svc = worker_service(bucket.factory(), None, None);
        let task = bucket.scan_task(&format!("frag-{kind:?}"));
        let status = match do_get(&svc, &task).await {
            Ok(_) => panic!("{kind:?}: setup must fail"),
            Err(s) => s,
        };
        assert_eq!(status.code(), code, "{kind:?}: {status}");
        assert_fault(&status, fault);
        assert!(
            status.message().contains("Scan execution failed"),
            "{kind:?}: {status}"
        );
        // None of these are the worker's fault.
        assert_eq!(
            svc.status().failures_since_last(),
            0,
            "{kind:?} must not count against the worker"
        );
        assert_eq!(svc.status().inflight_scans(), 0);
    }
}

/// The second file fails: the stream carries the classified Status after the
/// first file's frames.
#[tokio::test]
async fn mid_stream_fault_is_classified_in_the_stream() {
    let mut bucket = StubBucket::new();
    bucket.put_parquet("data/ok.parquet", &[2]).await;
    bucket.fail("data/denied.parquet", FaultKind::Denied);
    let svc = worker_service(bucket.factory(), None, None);
    let task = bucket.scan_task("frag-mid");

    let stream = do_get(&svc, &task).await.expect("setup").into_inner();
    let (frames, err) = drain(stream).await;
    assert!(frames >= 2, "schema + first file's batch before the error");
    let status = err.expect("stream must end with the fault");
    assert_fault(&status, WorkerFault::PrincipalDenied);
    assert_eq!(svc.status().failures_since_last(), 0);
}

/// A batch the scan budget cannot ever admit is this worker's problem:
/// ResourceExhausted, and it counts towards `failures_since_last`.
#[tokio::test]
async fn budget_exhaustion_is_a_worker_fault_and_counts() {
    let mut bucket = StubBucket::new();
    // One 1024-row Int64 group: an 8 KiB batch against a 4 KiB budget (a
    // 3 KiB decode lane once the fetch lane takes its unit).
    bucket.put_parquet("data/big.parquet", &[1024]).await;
    let scan_budget = ScanBudget::with_granularity(4096, 1024, None);
    let svc = worker_service(bucket.factory(), Some(scan_budget.clone()), None);
    let task = bucket.scan_task("frag-oom");

    let stream = do_get(&svc, &task).await.expect("setup").into_inner();
    let (_, err) = drain(stream).await;
    let status = err.expect("budget refusal must surface");
    assert_fault(&status, WorkerFault::WorkerResourceExhausted);
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(
        svc.status().failures_since_last(),
        1,
        "a worker fault is reported on the next hello"
    );
    assert_eq!(
        scan_budget.used_bytes(),
        0,
        "failed scan releases its permits"
    );
}

/// Over a real tonic connection the fault metadata rides the trailers, so a
/// coordinator's `WorkerFault::from_status` sees the exact variant, not just
/// the code.
#[tokio::test]
async fn mid_stream_fault_metadata_survives_tonic_transport() {
    let mut bucket = StubBucket::new();
    bucket.put_parquet("data/ok.parquet", &[2]).await;
    bucket.fail("data/slow.parquet", FaultKind::Throttled);
    let svc = worker_service(bucket.factory(), None, None);
    let task = bucket.scan_task("frag-wire");
    let served = ServedFlight::start(svc).await;
    let mut client = served.client().await;

    let ticket = arrow_flight::Ticket {
        ticket: bytes::Bytes::from(task.to_bytes().unwrap()),
    };
    let mut stream = client
        .do_get(Request::new(ticket))
        .await
        .expect("setup succeeds; the fault is mid-stream")
        .into_inner();
    let mut frames = 0;
    let mut fault = None;
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => frames += 1,
            Err(status) => {
                fault = Some(status);
                break;
            }
        }
    }
    assert!(frames >= 2, "first file streamed before the fault");
    let status = fault.expect("stream ends with the fault");
    assert_fault(&status, WorkerFault::SourceThrottled);
    assert_eq!(
        WorkerFault::from_status(&status),
        Some(WorkerFault::SourceThrottled),
        "coordinator-side decoder sees the exact variant"
    );
    assert!(
        !WorkerFault::SourceThrottled.marks_worker_unhealthy(),
        "throttling must not evict the worker"
    );

    // And a setup failure over the wire is the RPC's own error.
    let mut bucket = StubBucket::new();
    bucket.fail("data/gone.parquet", FaultKind::Missing);
    let svc = worker_service(bucket.factory(), None, None);
    let task = bucket.scan_task("frag-wire-setup");
    let served = ServedFlight::start(svc).await;
    let mut client = served.client().await;
    let ticket = arrow_flight::Ticket {
        ticket: bytes::Bytes::from(task.to_bytes().unwrap()),
    };
    let status = match client.do_get(Request::new(ticket)).await {
        Ok(_) => panic!("setup must fail"),
        Err(s) => s,
    };
    assert_fault(&status, WorkerFault::SourceMissing);
}
