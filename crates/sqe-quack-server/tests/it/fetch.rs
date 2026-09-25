//! Finding 606, part 2: results are paged out under a per-response byte cap
//! and a total-result cap, instead of being materialised whole into one
//! `PrepareResponse`.

use crate::support;

use std::sync::Arc;

use sqe_quack_server::{QuackServerState, QueryExecutor};
use sqe_quack_wire::message::QuackMessage;

use support::{
    accept_provider, connect_as, int32_values, noop_executor, send_fetch_as, send_prepare_as,
    spawn_state, trusts_loopback_proxy, CountingExecutor, EmptyResultExecutor,
};

const KIB: usize = 1024;

fn prepare_response(body: QuackMessage) -> sqe_quack_wire::message::PrepareResponse {
    match body {
        QuackMessage::PrepareResponse(r) => r,
        other => panic!("expected PrepareResponse, got {other:?}"),
    }
}

fn error_message(body: QuackMessage) -> String {
    match body {
        QuackMessage::ErrorResponse(e) => e.message,
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

/// The headline case: 200 batches of 1 000 Int32 rows with a 64 KiB response
/// cap. The client must walk the result through several `FetchRequest`s, the
/// concatenation must equal the input exactly, and the loop must terminate on
/// an empty fetch (which is the only end-of-result signal the existing
/// `sqe-quack-client` understands).
#[tokio::test]
async fn large_result_pages_through_multiple_fetches() {
    let executor = CountingExecutor {
        batches: 200,
        rows_per_batch: 1_000,
    };
    let expected = executor.expected_values();
    let state = QuackServerState::new(
        accept_provider(),
        Arc::new(executor) as Arc<dyn QueryExecutor>,
    )
    .with_result_limits(64 * KIB, 256 * 1024 * KIB);
    let (base, _sessions, pending) = spawn_state(state).await;

    let connection_id = connect_as(&base, None).await;
    let prepare = prepare_response(send_prepare_as(&base, &connection_id, "SELECT x", None).await);

    assert!(
        prepare.needs_more_fetch,
        "a result far above the response cap must not be inlined"
    );
    assert_ne!(prepare.result_uuid, 0, "a paged result needs a uuid");
    assert_eq!(prepare.result_names, vec!["x".to_string()]);
    assert_eq!(prepare.result_types.len(), 1);

    let mut got: Vec<i32> = Vec::with_capacity(expected.len());
    for chunk in &prepare.results {
        got.extend(int32_values(chunk));
    }
    let first_page_rows = got.len();
    assert!(
        first_page_rows < expected.len(),
        "the first page must not contain the whole result"
    );

    // The only non-panicking exit from this loop is an empty `results`, which
    // is the end-of-result signal the client relies on.
    let mut fetches = 0usize;
    loop {
        let body = send_fetch_as(&base, &connection_id, prepare.result_uuid, None).await;
        let fetch = match body {
            QuackMessage::FetchResponse(r) => r,
            other => panic!("expected FetchResponse, got {other:?}"),
        };
        fetches += 1;
        assert_eq!(
            fetch.batch_index,
            Some(fetches as u64),
            "batch_index must count the pages served"
        );
        if fetch.results.is_empty() {
            break;
        }
        for chunk in &fetch.results {
            got.extend(int32_values(chunk));
        }
        assert!(fetches < 1_000, "fetch loop failed to terminate");
    }

    assert!(
        fetches > 1,
        "a 200-batch result under a 64 KiB cap must take more than one fetch, took {fetches}"
    );
    assert_eq!(got, expected, "paged result must equal the input exactly");

    // The exhausted result must not linger in the pending store.
    pending.run_pending_tasks();
    assert_eq!(pending.len(), 0, "a drained result must be dropped");
}

/// The total cap is the backstop the per-response cap cannot provide: paging
/// bounds peak memory, not total work. Exceeding it must fail the query and
/// drop the parked stream, not simply return a short result.
#[tokio::test]
async fn total_result_cap_aborts_and_drops_the_pending_entry() {
    let executor = CountingExecutor {
        batches: 200,
        rows_per_batch: 1_000,
    };
    let state = QuackServerState::new(
        accept_provider(),
        Arc::new(executor) as Arc<dyn QueryExecutor>,
    )
    .with_result_limits(64 * KIB, 128 * KIB);
    let (base, _sessions, pending) = spawn_state(state).await;

    let connection_id = connect_as(&base, None).await;
    let prepare = prepare_response(send_prepare_as(&base, &connection_id, "SELECT x", None).await);
    assert!(prepare.needs_more_fetch);

    let mut last = String::new();
    for _ in 0..10 {
        let body = send_fetch_as(&base, &connection_id, prepare.result_uuid, None).await;
        match body {
            QuackMessage::FetchResponse(_) => continue,
            QuackMessage::ErrorResponse(e) => {
                last = e.message;
                break;
            }
            other => panic!("expected FetchResponse or ErrorResponse, got {other:?}"),
        }
    }
    assert!(
        last.starts_with("SQE-EXEC: result exceeds "),
        "expected the total-result cap error, got {last:?}"
    );
    assert!(last.ends_with(" bytes; narrow the query"), "{last}");

    pending.run_pending_tasks();
    assert_eq!(
        pending.len(),
        0,
        "an over-cap result must leave no parked stream behind"
    );
}

/// A result that fits under the cap keeps the old shape exactly: everything
/// inlined, `needs_more_fetch` false, uuid 0, nothing parked.
#[tokio::test]
async fn small_result_is_still_inlined_with_no_pending_entry() {
    let state = QuackServerState::new(accept_provider(), noop_executor());
    let (base, _sessions, pending) = spawn_state(state).await;

    let connection_id = connect_as(&base, None).await;
    let prepare = prepare_response(send_prepare_as(&base, &connection_id, "SELECT x", None).await);

    assert!(!prepare.needs_more_fetch);
    assert_eq!(prepare.result_uuid, 0);
    assert_eq!(prepare.results.len(), 1);
    assert_eq!(prepare.results[0].row_count, 3);

    pending.run_pending_tasks();
    assert_eq!(pending.len(), 0);
}

/// Types now come from the stream schema, so a query that yields no batches at
/// all still names and types its columns. Reading them off the first batch
/// returned an empty `result_types`, which the client turns into a
/// zero-column schema.
#[tokio::test]
async fn zero_row_result_still_reports_names_and_types() {
    let state = QuackServerState::new(
        accept_provider(),
        Arc::new(EmptyResultExecutor) as Arc<dyn QueryExecutor>,
    );
    let (base, _sessions, _pending) = spawn_state(state).await;

    let connection_id = connect_as(&base, None).await;
    let prepare =
        prepare_response(send_prepare_as(&base, &connection_id, "SELECT x WHERE 1=0", None).await);

    assert!(!prepare.needs_more_fetch);
    assert!(prepare.results.is_empty());
    assert_eq!(prepare.result_names, vec!["x".to_string()]);
    assert_eq!(prepare.result_types.len(), 1);
    assert_eq!(
        prepare.result_types[0].id,
        sqe_quack_wire::data_chunk::LogicalTypeId::Integer
    );
}

/// A uuid nobody issued, and a uuid issued to somebody else, get the same
/// answer: a fetch must not be a probe for other sessions' result ids.
#[tokio::test]
async fn fetch_rejects_unknown_and_foreign_result_uuids() {
    let executor = CountingExecutor {
        batches: 200,
        rows_per_batch: 1_000,
    };
    let state = QuackServerState::new(
        accept_provider(),
        Arc::new(executor) as Arc<dyn QueryExecutor>,
    )
    .with_result_limits(64 * KIB, 256 * 1024 * KIB)
    .with_security(trusts_loopback_proxy());
    let (base, _sessions, _pending) = spawn_state(state).await;

    let owner = connect_as(&base, Some("203.0.113.5")).await;
    let prepare =
        prepare_response(send_prepare_as(&base, &owner, "SELECT x", Some("203.0.113.5")).await);
    assert!(prepare.needs_more_fetch);

    // Unknown uuid on a valid session.
    let unknown = send_fetch_as(&base, &owner, 123_456_789i128, Some("203.0.113.5")).await;
    assert_eq!(
        error_message(unknown),
        "SQE-EXEC: unknown or expired result_uuid"
    );

    // A second, legitimate session cannot fetch the first session's result.
    let other = connect_as(&base, Some("198.51.100.9")).await;
    let foreign = send_fetch_as(&base, &other, prepare.result_uuid, Some("198.51.100.9")).await;
    assert_eq!(
        error_message(foreign),
        "SQE-EXEC: unknown or expired result_uuid",
        "a foreign uuid must be indistinguishable from an unknown one"
    );
}

/// Disconnect releases the parked stream. Without this, a client that opens a
/// large result and disconnects leaves the DataFusion stream (and its memory
/// reservation) alive until the idle sweep.
#[tokio::test]
async fn disconnect_drops_parked_results() {
    let executor = CountingExecutor {
        batches: 200,
        rows_per_batch: 1_000,
    };
    let state = QuackServerState::new(
        accept_provider(),
        Arc::new(executor) as Arc<dyn QueryExecutor>,
    )
    .with_result_limits(64 * KIB, 256 * 1024 * KIB);
    let (base, _sessions, pending) = spawn_state(state).await;

    let connection_id = connect_as(&base, None).await;
    let prepare = prepare_response(send_prepare_as(&base, &connection_id, "SELECT x", None).await);
    assert!(prepare.needs_more_fetch);
    pending.run_pending_tasks();
    assert_eq!(pending.len(), 1);

    let header = sqe_quack_wire::message::MessageHeader {
        r#type: sqe_quack_wire::message::MessageType::DisconnectMessage,
        connection_id: connection_id.clone(),
        client_query_id: Some(9),
    };
    let (_, body) =
        support::post_message(&base, &header, &QuackMessage::DisconnectMessage, None).await;
    assert!(matches!(body, QuackMessage::SuccessResponse));

    pending.run_pending_tasks();
    assert_eq!(
        pending.len(),
        0,
        "disconnect must drop the session's parked results"
    );
}
