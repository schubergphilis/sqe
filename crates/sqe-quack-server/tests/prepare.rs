//! Integration test for the PrepareRequest stub.

mod support;

use sqe_quack_wire::message::{
    decode_message, encode_message, ConnectionRequest, MessageHeader, MessageType, PrepareRequest,
    QuackMessage,
};

use support::{accept_provider, spawn_server_with};

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
    let resp = reqwest::Client::new()
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb")
        .body(encode_message(&header, &body))
        .send()
        .await
        .unwrap();
    let (resp_header, _) = decode_message(&resp.bytes().await.unwrap()).unwrap();
    resp_header.connection_id
}

#[tokio::test]
async fn prepare_request_returns_sqe_exec_error_until_coordinator_wires_up() {
    let base = spawn_server_with(accept_provider()).await;
    let connection_id = connect(&base).await;

    let header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: connection_id.clone(),
        client_query_id: Some(1),
    };
    let body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: "SELECT 1".to_string(),
    });
    let resp = reqwest::Client::new()
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb")
        .body(encode_message(&header, &body))
        .send()
        .await
        .unwrap();
    let (resp_header, resp_body) = decode_message(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(resp_header.r#type, MessageType::ErrorResponse);
    assert_eq!(resp_header.connection_id, connection_id);
    assert_eq!(resp_header.client_query_id, Some(1));
    match resp_body {
        QuackMessage::ErrorResponse(e) => {
            assert!(e.message.starts_with("SQE-EXEC"));
            assert!(
                e.message.to_lowercase().contains("coordinator")
                    || e.message.to_lowercase().contains("result"),
                "error message should reference the missing coordinator integration: {}",
                e.message
            );
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn prepare_request_with_unknown_connection_id_is_rejected() {
    let base = spawn_server_with(accept_provider()).await;

    let header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: "not-a-real-id".to_string(),
        client_query_id: None,
    };
    let body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: "SELECT 1".to_string(),
    });
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
