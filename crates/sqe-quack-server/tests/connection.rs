//! Integration test against a real running server bound to a random port.

use std::time::Duration;

use sqe_quack_server::{router, QuackServerState};
use sqe_quack_wire::message::{
    decode_message, encode_message, ConnectionRequest, MessageHeader, MessageType, QuackMessage,
};

async fn spawn_server() -> String {
    let state = QuackServerState::new();
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    // Give the listener a moment to come up.
    tokio::time::sleep(Duration::from_millis(20)).await;
    format!("http://{addr}")
}

#[tokio::test]
async fn root_returns_identification_string() {
    let base = spawn_server().await;
    let body = reqwest::get(format!("{base}/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("Quack"));
}

#[tokio::test]
async fn connection_request_returns_response_with_fresh_connection_id() {
    let base = spawn_server().await;
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

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb")
        .body(request_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let resp_bytes = resp.bytes().await.unwrap();

    let (resp_header, resp_body) = decode_message(&resp_bytes).unwrap();
    assert_eq!(resp_header.r#type, MessageType::ConnectionResponse);
    assert!(
        !resp_header.connection_id.is_empty(),
        "server should assign a connection_id"
    );
    match resp_body {
        QuackMessage::ConnectionResponse(c) => {
            assert_eq!(c.quack_version, 1);
        }
        other => panic!("expected ConnectionResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn connection_request_with_empty_token_is_rejected() {
    let base = spawn_server().await;
    let header = MessageHeader {
        r#type: MessageType::ConnectionRequest,
        connection_id: String::new(),
        client_query_id: None,
    };
    let body = QuackMessage::ConnectionRequest(ConnectionRequest {
        auth_string: String::new(),
        client_duckdb_version: "v1.5.2".to_string(),
        client_platform: "test".to_string(),
        min_supported_quack_version: 1,
        max_supported_quack_version: 1,
    });
    let request_bytes = encode_message(&header, &body);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb")
        .body(request_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let (_, resp_body) = decode_message(&resp.bytes().await.unwrap()).unwrap();
    match resp_body {
        QuackMessage::ErrorResponse(e) => assert!(e.message.starts_with("SQE-AUTH")),
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}
