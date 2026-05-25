//! Integration test against a real running server bound to a random port.

mod support;

use std::sync::Arc;

use sqe_quack_wire::message::{
    decode_message, encode_message, ConnectionRequest, MessageHeader, MessageType, QuackMessage,
};

use support::{accept_provider, spawn_server_with, RejectProvider, SkipProvider};

fn sample_connection_request(token: &str) -> Vec<u8> {
    let header = MessageHeader {
        r#type: MessageType::ConnectionRequest,
        connection_id: String::new(),
        client_query_id: None,
    };
    let body = QuackMessage::ConnectionRequest(ConnectionRequest {
        auth_string: token.to_string(),
        client_duckdb_version: "v1.5.2".to_string(),
        client_platform: "test".to_string(),
        min_supported_quack_version: 1,
        max_supported_quack_version: 1,
    });
    encode_message(&header, &body)
}

async fn post_quack(base: &str, bytes: Vec<u8>) -> Vec<u8> {
    let resp = reqwest::Client::new()
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb")
        .body(bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.bytes().await.unwrap().to_vec()
}

#[tokio::test]
async fn root_returns_identification_string() {
    let base = spawn_server_with(accept_provider()).await;
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
    let base = spawn_server_with(accept_provider()).await;
    let resp_bytes = post_quack(&base, sample_connection_request("super_secret")).await;

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
async fn connection_request_with_empty_token_is_rejected_before_provider_runs() {
    // Even an accept-all provider should never see an empty token — we reject
    // before calling the chain to avoid burning auth calls on garbage input.
    let base = spawn_server_with(accept_provider()).await;
    let resp_bytes = post_quack(&base, sample_connection_request("")).await;
    let (_, resp_body) = decode_message(&resp_bytes).unwrap();
    match resp_body {
        QuackMessage::ErrorResponse(e) => {
            assert!(e.message.starts_with("SQE-AUTH"));
            assert!(e.message.contains("required"));
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn connection_request_surfaces_auth_failed_message_from_provider() {
    let provider = Arc::new(RejectProvider {
        reason: "token expired".to_string(),
    });
    let base = spawn_server_with(provider).await;
    let resp_bytes = post_quack(&base, sample_connection_request("expired-jwt")).await;
    let (_, resp_body) = decode_message(&resp_bytes).unwrap();
    match resp_body {
        QuackMessage::ErrorResponse(e) => {
            assert!(e.message.starts_with("SQE-AUTH"));
            assert!(
                e.message.contains("token expired"),
                "expected provider reason in error: {}",
                e.message
            );
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn connection_request_reports_no_provider_accepted_credentials() {
    // SkipProvider always returns NotMyCredentials, simulating a chain that
    // has no matching provider for the credential type.
    let base = spawn_server_with(Arc::new(SkipProvider)).await;
    let resp_bytes = post_quack(&base, sample_connection_request("some-token")).await;
    let (_, resp_body) = decode_message(&resp_bytes).unwrap();
    match resp_body {
        QuackMessage::ErrorResponse(e) => {
            assert!(e.message.starts_with("SQE-AUTH"));
            assert!(
                e.message.contains("no provider"),
                "expected 'no provider accepted' wording: {}",
                e.message
            );
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}
