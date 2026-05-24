//! Integration tests for the DisconnectMessage handler.

use std::time::Duration;

use sqe_quack_server::{router, QuackServerState};
use sqe_quack_wire::message::{
    decode_message, encode_message, ConnectionRequest, MessageHeader, MessageType, QuackMessage,
};

async fn spawn_server() -> (String, sqe_quack_server::SessionStore) {
    let state = QuackServerState::new();
    let sessions = state.sessions.clone();
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (format!("http://{addr}"), sessions)
}

async fn connect(base: &str) -> String {
    let header = MessageHeader {
        r#type: MessageType::ConnectionRequest,
        connection_id: String::new(),
        client_query_id: None,
    };
    let body = QuackMessage::ConnectionRequest(ConnectionRequest {
        auth_string: "super_secret".to_string(),
        client_duckdb_version: "v1.5.2".to_string(),
        client_platform: "test".to_string(),
        min_supported_quack_version: 1,
        max_supported_quack_version: 1,
    });
    let request_bytes = encode_message(&header, &body);
    let resp = reqwest::Client::new()
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb")
        .body(request_bytes)
        .send()
        .await
        .unwrap();
    let (resp_header, _) = decode_message(&resp.bytes().await.unwrap()).unwrap();
    resp_header.connection_id
}

#[tokio::test]
async fn disconnect_removes_session_and_returns_success() {
    let (base, sessions) = spawn_server().await;
    let connection_id = connect(&base).await;
    sessions.run_pending_tasks();
    assert!(sessions.get(&connection_id).is_some());

    let header = MessageHeader {
        r#type: MessageType::DisconnectMessage,
        connection_id: connection_id.clone(),
        client_query_id: Some(1),
    };
    let body = QuackMessage::DisconnectMessage;
    let resp = reqwest::Client::new()
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb")
        .body(encode_message(&header, &body))
        .send()
        .await
        .unwrap();
    let (resp_header, resp_body) = decode_message(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(resp_header.r#type, MessageType::SuccessResponse);
    assert!(matches!(resp_body, QuackMessage::SuccessResponse));

    // moka's entry_count is a hint not a strict count; run a sync pass first.
    sessions.run_pending_tasks();
    assert!(sessions.get(&connection_id).is_none());
}

#[tokio::test]
async fn disconnect_with_unknown_connection_id_returns_auth_error() {
    let (base, _sessions) = spawn_server().await;

    let header = MessageHeader {
        r#type: MessageType::DisconnectMessage,
        connection_id: "not-a-real-id".to_string(),
        client_query_id: None,
    };
    let body = QuackMessage::DisconnectMessage;
    let resp = reqwest::Client::new()
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb")
        .body(encode_message(&header, &body))
        .send()
        .await
        .unwrap();
    let (_, resp_body) = decode_message(&resp.bytes().await.unwrap()).unwrap();
    match resp_body {
        QuackMessage::ErrorResponse(e) => assert!(e.message.starts_with("SQE-AUTH")),
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}
