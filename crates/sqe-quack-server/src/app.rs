//! Axum HTTP application: `GET /` identification + `POST /quack` RPC.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use bytes::Bytes;
use sqe_quack_wire::message::{
    decode_message, encode_message, ConnectionResponse, ErrorResponse, MessageHeader, MessageType,
    QuackMessage,
};
use uuid::Uuid;

use crate::session::{Session, SessionStore};

const QUACK_VERSION: u64 = 1;
const APPLICATION_VND_DUCKDB: &str = "application/vnd.duckdb";

#[derive(Clone)]
pub struct QuackServerState {
    pub sessions: SessionStore,
    pub server_duckdb_version: String,
    pub server_platform: String,
}

impl QuackServerState {
    pub fn new() -> Self {
        Self {
            sessions: SessionStore::new(Duration::from_secs(600)),
            server_duckdb_version: format!("sqe-{}", env!("CARGO_PKG_VERSION")),
            server_platform: std::env::consts::OS.to_string(),
        }
    }
}

impl Default for QuackServerState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn router(state: QuackServerState) -> Router {
    Router::new()
        .route("/", get(identify))
        .route("/quack", post(handle_quack))
        .with_state(Arc::new(state))
}

async fn identify() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain")],
        "This is a DuckDB Quack RPC endpoint, served by SQE.\n",
    )
}

async fn handle_quack(
    State(state): State<Arc<QuackServerState>>,
    body: Bytes,
) -> impl IntoResponse {
    let (request_header, request_body) = match decode_message(&body) {
        Ok(v) => v,
        Err(e) => return error_response("", None, format!("SQE-PARSE: {e}")),
    };

    match (request_header.r#type, request_body) {
        (MessageType::ConnectionRequest, QuackMessage::ConnectionRequest(req)) => {
            handle_connection_request(&state, &request_header, req)
        }
        (MessageType::DisconnectMessage, QuackMessage::DisconnectMessage) => {
            handle_disconnect_message(&state, &request_header)
        }
        (MessageType::PrepareRequest, QuackMessage::PrepareRequest(req)) => {
            handle_prepare_request(&state, &request_header, req)
        }
        (msg_type, _) => error_response(
            &request_header.connection_id,
            request_header.client_query_id,
            format!("SQE-DIALECT: message type {msg_type:?} not yet supported"),
        ),
    }
}

fn handle_prepare_request(
    state: &QuackServerState,
    request_header: &MessageHeader,
    _req: sqe_quack_wire::message::PrepareRequest,
) -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
    if state.sessions.get(&request_header.connection_id).is_none() {
        return error_response(
            &request_header.connection_id,
            request_header.client_query_id,
            "SQE-AUTH: unknown connection_id".to_string(),
        );
    }
    error_response(
        &request_header.connection_id,
        request_header.client_query_id,
        "SQE-EXEC: query execution is not yet wired to sqe-coordinator; result \
         encoding is in place but no plan is built"
            .to_string(),
    )
}

fn handle_disconnect_message(
    state: &QuackServerState,
    request_header: &MessageHeader,
) -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
    if state.sessions.get(&request_header.connection_id).is_none() {
        return error_response(
            &request_header.connection_id,
            request_header.client_query_id,
            "SQE-AUTH: unknown connection_id".to_string(),
        );
    }
    state.sessions.remove(&request_header.connection_id);

    let response_header = MessageHeader {
        r#type: MessageType::SuccessResponse,
        connection_id: request_header.connection_id.clone(),
        client_query_id: request_header.client_query_id,
    };
    let bytes = encode_message(&response_header, &QuackMessage::SuccessResponse);
    (
        StatusCode::OK,
        [("content-type", APPLICATION_VND_DUCKDB)],
        bytes,
    )
}

fn handle_connection_request(
    state: &QuackServerState,
    request_header: &MessageHeader,
    req: sqe_quack_wire::message::ConnectionRequest,
) -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
    if req.auth_string.is_empty() {
        return error_response(
            "",
            request_header.client_query_id,
            "SQE-AUTH: bearer token is required".to_string(),
        );
    }
    let connection_id = Uuid::new_v4().to_string();
    state.sessions.insert(Session {
        connection_id: connection_id.clone(),
        bearer_token: req.auth_string,
    });

    let response_header = MessageHeader {
        r#type: MessageType::ConnectionResponse,
        connection_id,
        client_query_id: request_header.client_query_id,
    };
    let response_body = QuackMessage::ConnectionResponse(ConnectionResponse {
        server_duckdb_version: state.server_duckdb_version.clone(),
        server_platform: state.server_platform.clone(),
        quack_version: QUACK_VERSION,
    });
    let bytes = encode_message(&response_header, &response_body);
    (
        StatusCode::OK,
        [("content-type", APPLICATION_VND_DUCKDB)],
        bytes,
    )
}

fn error_response(
    connection_id: &str,
    client_query_id: Option<u64>,
    message: String,
) -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
    let header = MessageHeader {
        r#type: MessageType::ErrorResponse,
        connection_id: connection_id.to_string(),
        client_query_id,
    };
    let body = QuackMessage::ErrorResponse(ErrorResponse { message });
    let bytes = encode_message(&header, &body);
    (
        StatusCode::OK,
        [("content-type", APPLICATION_VND_DUCKDB)],
        bytes,
    )
}
