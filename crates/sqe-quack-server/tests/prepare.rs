//! Integration tests for the PrepareRequest handler going through the
//! `QueryExecutor` trait.

mod support;

use std::sync::Arc;

use sqe_quack_server::QueryError;
use sqe_quack_wire::data_chunk::{LogicalTypeId, VectorData};
use sqe_quack_wire::message::{
    decode_message, encode_message, ConnectionRequest, MessageHeader, MessageType, PrepareRequest,
    QuackMessage,
};

use support::{accept_provider, spawn_server_with, spawn_server_with_executor, ErroringExecutor};

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

async fn send_prepare(base: &str, connection_id: &str, sql: &str) -> Vec<u8> {
    let header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: connection_id.to_string(),
        client_query_id: Some(1),
    };
    let body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: sql.to_string(),
    });
    let resp = reqwest::Client::new()
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb")
        .body(encode_message(&header, &body))
        .send()
        .await
        .unwrap();
    resp.bytes().await.unwrap().to_vec()
}

#[tokio::test]
async fn prepare_request_returns_prepare_response_with_result_chunk() {
    let base = spawn_server_with(accept_provider()).await;
    let connection_id = connect(&base).await;
    let response_bytes = send_prepare(&base, &connection_id, "SELECT x FROM t").await;

    let (resp_header, resp_body) = decode_message(&response_bytes).unwrap();
    assert_eq!(resp_header.r#type, MessageType::PrepareResponse);
    assert_eq!(resp_header.connection_id, connection_id);
    assert_eq!(resp_header.client_query_id, Some(1));

    match resp_body {
        QuackMessage::PrepareResponse(r) => {
            assert_eq!(r.result_names, vec!["x".to_string()]);
            assert_eq!(r.result_types.len(), 1);
            assert_eq!(r.result_types[0].id, LogicalTypeId::Integer);
            assert!(!r.needs_more_fetch);
            assert_eq!(r.results.len(), 1);
            assert_eq!(r.results[0].row_count, 3);
            assert_eq!(r.results[0].columns.len(), 1);
            match &r.results[0].columns[0].data {
                VectorData::Fixed(bytes) => {
                    let expected: Vec<u8> =
                        [1i32, 2, 3].iter().flat_map(|v| v.to_le_bytes()).collect();
                    assert_eq!(bytes, &expected);
                }
                other => panic!("expected Fixed VectorData, got {other:?}"),
            }
        }
        other => panic!("expected PrepareResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn prepare_request_with_unknown_connection_id_is_rejected() {
    let base = spawn_server_with(accept_provider()).await;
    let response_bytes = send_prepare(&base, "not-a-real-id", "SELECT 1").await;
    let (_, resp_body) = decode_message(&response_bytes).unwrap();
    match resp_body {
        QuackMessage::ErrorResponse(e) => assert!(e.message.starts_with("SQE-AUTH")),
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn executor_parse_error_maps_to_sqe_parse() {
    let executor = Arc::new(ErroringExecutor {
        error: || QueryError::Parse("unexpected token at line 1".to_string()),
    });
    let base = spawn_server_with_executor(accept_provider(), executor).await;
    let connection_id = connect(&base).await;
    let response_bytes = send_prepare(&base, &connection_id, "SELEKT 1").await;
    let (_, resp_body) = decode_message(&response_bytes).unwrap();
    match resp_body {
        QuackMessage::ErrorResponse(e) => {
            assert!(e.message.starts_with("SQE-PARSE"), "{}", e.message);
            assert!(e.message.contains("unexpected token"));
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn executor_execution_error_maps_to_sqe_exec() {
    let executor = Arc::new(ErroringExecutor {
        error: || QueryError::Execution("table 'orders' not found".to_string()),
    });
    let base = spawn_server_with_executor(accept_provider(), executor).await;
    let connection_id = connect(&base).await;
    let response_bytes = send_prepare(&base, &connection_id, "SELECT * FROM orders").await;
    let (_, resp_body) = decode_message(&response_bytes).unwrap();
    match resp_body {
        QuackMessage::ErrorResponse(e) => {
            assert!(e.message.starts_with("SQE-EXEC"), "{}", e.message);
            assert!(e.message.contains("table 'orders' not found"));
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn executor_policy_error_does_not_leak_reason_to_client() {
    let executor = Arc::new(ErroringExecutor {
        error: || QueryError::Policy("user lacks read on hr.salaries".to_string()),
    });
    let base = spawn_server_with_executor(accept_provider(), executor).await;
    let connection_id = connect(&base).await;
    let response_bytes = send_prepare(&base, &connection_id, "SELECT * FROM hr.salaries").await;
    let (_, resp_body) = decode_message(&response_bytes).unwrap();
    match resp_body {
        QuackMessage::ErrorResponse(e) => {
            assert_eq!(e.message, "SQE-POLICY: access denied");
            // Don't leak the policy reason. That's the row-filter / mask payload.
            assert!(!e.message.contains("salaries"));
            assert!(!e.message.contains("user lacks"));
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn executor_internal_error_does_not_leak_details() {
    let executor = Arc::new(ErroringExecutor {
        error: || QueryError::Internal("panic in plan optimisation".to_string()),
    });
    let base = spawn_server_with_executor(accept_provider(), executor).await;
    let connection_id = connect(&base).await;
    let response_bytes = send_prepare(&base, &connection_id, "SELECT 1").await;
    let (_, resp_body) = decode_message(&response_bytes).unwrap();
    match resp_body {
        QuackMessage::ErrorResponse(e) => {
            assert_eq!(e.message, "SQE-EXEC: internal execution error");
            assert!(!e.message.contains("panic"));
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}
