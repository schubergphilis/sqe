//! Finding 611 item 2: credentials pushed through `refresh_credentials` while
//! a scan is mid-stream are used for the NEXT file open, and every file after.

use std::time::Duration;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Action;
use futures::StreamExt;
use sqe_worker::credential_channel::RefreshableCredentials;
use sqe_worker::executor::ScanBudget;
use tonic::Request;

use super::fixtures::{do_get, worker_service, StubBucket, NEW_KEY, OLD_KEY};

/// Budget unit. A 1024-row Int64 batch decodes to 8 KiB (plus reader slack),
/// so it charges one or two units and never more than half the budget.
const UNIT: usize = 16 * 1024;
/// Rows per row group in every fixture file: one batch per group.
const ROWS: usize = 1024;

/// Three-file ticket; new credentials after file one; files two and three
/// read with the new key.
///
/// Timing is made deterministic by the scan budget rather than by sleeps.
/// The producer checks for refreshed credentials immediately before opening
/// each file after the first. File one has eight row groups (eight batches of
/// at least one unit each) against a five-unit budget (four of them the
/// decode lane), so the producer cannot finish file one -- let alone reach
/// that check -- until the consumer starts draining. A refresh published
/// before the first frame is consumed is therefore guaranteed to precede the
/// open of file two.
#[tokio::test]
async fn credential_refresh_mid_scan() {
    let mut bucket = StubBucket::new();
    bucket.put_parquet("data/f1.parquet", &[ROWS; 8]).await;
    bucket.put_parquet("data/f2.parquet", &[ROWS]).await;
    bucket.put_parquet("data/f3.parquet", &[ROWS]).await;

    let scan_budget = ScanBudget::with_granularity(5 * UNIT, UNIT, None);
    let svc = worker_service(bucket.factory(), Some(scan_budget.clone()), None);
    let task = bucket.scan_task("frag-refresh");

    let response = do_get(&svc, &task).await.expect("scan setup");
    let mut stream = response.into_inner();

    // File one is open with the ticket's key and the producer is parked at
    // the watermark. Push new credentials through the real action.
    let creds = RefreshableCredentials {
        fragment_id: "frag-refresh".to_string(),
        access_key_id: NEW_KEY.to_string(),
        secret_access_key: "new-secret".to_string(),
        session_token: "new-token".to_string(),
        expiry: chrono::Utc::now() + chrono::Duration::hours(1),
    };
    let action = Action {
        r#type: "refresh_credentials".to_string(),
        body: bytes::Bytes::from(serde_json::to_vec(&creds).unwrap()),
    };
    let mut reply = svc
        .do_action(Request::new(action))
        .await
        .expect("refresh accepted")
        .into_inner();
    let body = reply.next().await.unwrap().unwrap().body;
    assert_eq!(
        body.as_ref(),
        b"accepted",
        "fragment must have an active channel"
    );

    // Now consume the whole scan.
    let mut frames = 0usize;
    let drained = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(item) = stream.next().await {
            item.expect("no stream error");
            frames += 1;
        }
    })
    .await;
    drained.expect("scan completes");
    // Schema frame + one frame per row group.
    assert_eq!(frames, 1 + 10, "schema + ten row-group batches");

    let f1 = bucket.labels_for("data/f1.parquet");
    let f2 = bucket.labels_for("data/f2.parquet");
    let f3 = bucket.labels_for("data/f3.parquet");
    assert!(
        !f1.is_empty() && !f2.is_empty() && !f3.is_empty(),
        "every file was read"
    );
    assert!(
        f1.iter().all(|l| l == OLD_KEY),
        "file one was opened before the refresh: {f1:?}"
    );
    assert!(
        f2.iter().all(|l| l == NEW_KEY),
        "file two must use the refreshed key: {f2:?}"
    );
    assert!(
        f3.iter().all(|l| l == NEW_KEY),
        "file three must keep the refreshed key: {f3:?}"
    );
    assert_eq!(scan_budget.used_bytes(), 0, "all permits released");
}

/// Without a refresh every file uses the ticket's key, and a refresh for a
/// fragment that is not scanning is answered `no_active_scan`.
#[tokio::test]
async fn no_refresh_keeps_the_ticket_key_and_unknown_fragment_is_reported() {
    let mut bucket = StubBucket::new();
    bucket.put_parquet("data/a.parquet", &[ROWS]).await;
    bucket.put_parquet("data/b.parquet", &[ROWS]).await;
    let svc = worker_service(bucket.factory(), None, None);

    let creds = RefreshableCredentials {
        fragment_id: "frag-nobody".to_string(),
        access_key_id: NEW_KEY.to_string(),
        secret_access_key: "s".to_string(),
        session_token: String::new(),
        expiry: chrono::Utc::now() + chrono::Duration::hours(1),
    };
    let action = Action {
        r#type: "refresh_credentials".to_string(),
        body: bytes::Bytes::from(serde_json::to_vec(&creds).unwrap()),
    };
    let mut reply = svc
        .do_action(Request::new(action))
        .await
        .expect("action ok")
        .into_inner();
    let body = reply.next().await.unwrap().unwrap().body;
    assert_eq!(body.as_ref(), b"no_active_scan");

    let task = bucket.scan_task("frag-plain");
    let stream = do_get(&svc, &task).await.expect("setup").into_inner();
    let (frames, err) = super::fixtures::drain(stream).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(frames, 1 + 2);
    for key in ["data/a.parquet", "data/b.parquet"] {
        let labels = bucket.labels_for(key);
        assert!(!labels.is_empty());
        assert!(labels.iter().all(|l| l == OLD_KEY), "{key}: {labels:?}");
    }
}
