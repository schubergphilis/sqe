//! Finding 606, part 1: the `connection_id` is not a standalone, immortal
//! bearer credential. It expires with the token it was minted from, and it is
//! bound to the client that opened the connection.

mod support;

use std::sync::Arc;
use std::time::Duration;

use sqe_auth::AuthProvider;
use sqe_quack_server::QuackServerState;
use sqe_quack_wire::message::QuackMessage;

use support::{
    connect_as, noop_executor, send_prepare_as, spawn_state, trusts_loopback_proxy, AcceptProvider,
};

fn error_message(body: QuackMessage) -> String {
    match body {
        QuackMessage::ErrorResponse(e) => e.message,
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

/// A session minted from a token that expires in 1 s must be refused after 2 s
/// *even though* the client kept it warm with a request every 500 ms. Polling
/// used to be enough to keep a session alive forever: the store only had a
/// `time_to_idle`, and nothing compared `core_session.token_expiry` to now.
#[tokio::test]
async fn polling_does_not_keep_an_expired_session_alive() {
    let provider: Arc<dyn AuthProvider> = Arc::new(AcceptProvider::expiring_in(
        "test-user",
        chrono::Duration::seconds(1),
    ));
    // Idle window far longer than the token lifetime: the only thing that can
    // refuse this session is the token-expiry check.
    let state = QuackServerState::new(provider, noop_executor())
        .with_session_limits(Duration::from_secs(600), Duration::from_secs(8 * 60 * 60));
    let (base, _sessions, _pending) = spawn_state(state).await;

    let connection_id = connect_as(&base, None).await;

    // Poll every 500 ms for 2 s. The first polls succeed, and the moment the
    // token's `exp` passes, the session must be refused.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut last: Option<String> = None;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let body = send_prepare_as(&base, &connection_id, "SELECT x FROM t", None).await;
        match body {
            QuackMessage::PrepareResponse(_) => last = None,
            QuackMessage::ErrorResponse(e) => {
                last = Some(e.message);
                break;
            }
            other => panic!("expected PrepareResponse or ErrorResponse, got {other:?}"),
        }
    }

    assert_eq!(
        last.as_deref(),
        Some("SQE-AUTH: session expired"),
        "a polled session whose token expired must be refused"
    );

    // And it stays refused — as an unknown id, because the expired entry was
    // dropped rather than left to be re-tested on every request.
    let body = send_prepare_as(&base, &connection_id, "SELECT x FROM t", None).await;
    assert_eq!(error_message(body), "SQE-AUTH: unknown connection_id");
}

/// The `connection_id` travels in every request body. Binding it to the client
/// key means a captured id is useless from anywhere else. The wire message is
/// deliberately the same as for an unknown id, so the endpoint is not an oracle
/// for "this id exists, you're just on the wrong address".
#[tokio::test]
async fn prepare_from_a_different_client_key_is_refused() {
    let provider: Arc<dyn AuthProvider> = Arc::new(AcceptProvider::new("test-user"));
    // Trusting loopback as a proxy makes `x-forwarded-for` the client key, so
    // two "clients" can share one loopback socket.
    let state =
        QuackServerState::new(provider, noop_executor()).with_security(trusts_loopback_proxy());
    let (base, _sessions, _pending) = spawn_state(state).await;

    let connection_id = connect_as(&base, Some("203.0.113.5")).await;

    // The client that opened the connection still works.
    let ok = send_prepare_as(
        &base,
        &connection_id,
        "SELECT x FROM t",
        Some("203.0.113.5"),
    )
    .await;
    assert!(
        matches!(ok, QuackMessage::PrepareResponse(_)),
        "the original client must keep working"
    );

    // A different client presenting the same id is refused, with the generic
    // message.
    let stolen = send_prepare_as(
        &base,
        &connection_id,
        "SELECT x FROM t",
        Some("198.51.100.9"),
    )
    .await;
    assert_eq!(
        error_message(stolen),
        "SQE-AUTH: unknown connection_id",
        "a mismatched client must not learn that the id is real"
    );

    // A refused replay must not have invalidated the legitimate session.
    let still_ok = send_prepare_as(
        &base,
        &connection_id,
        "SELECT x FROM t",
        Some("203.0.113.5"),
    )
    .await;
    assert!(
        matches!(still_ok, QuackMessage::PrepareResponse(_)),
        "a rejected replay must not lock the real client out"
    );
}
