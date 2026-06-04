//! Axum HTTP application: `GET /` identification + `POST /quack` RPC.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use bytes::Bytes;
use sqe_auth::{AuthError, AuthProvider, FlightCredentials};
use sqe_core::SecretString;
use sqe_quack_wire::arrow_bridge::record_batch_to_data_chunk;
use sqe_quack_wire::codec::BinaryDeserializer;
use sqe_quack_wire::data_chunk::DataChunk;
use sqe_quack_wire::message::{
    decode_message, encode_message, ConnectionResponse, ErrorResponse, MessageHeader, MessageType,
    PrepareResponse, QuackMessage,
};
use uuid::Uuid;

use crate::query_executor::{QueryError, QueryExecutor};
use crate::session::{identity_to_core_session, Session, SessionStore};

const QUACK_VERSION: u64 = 1;
const APPLICATION_VND_DUCKDB: &str = "application/vnd.duckdb";

/// Explicit request-body cap for the `/quack` endpoint. The DataChunk-carrying
/// decode paths are reachable pre-auth, so we keep the ceiling small and
/// independent of axum's implicit default. 4 MiB comfortably covers a
/// handshake, a DISCONNECT, and a reasonable PREPARE SQL string while leaving
/// no room for a multi-MB recursion / allocation-count payload.
const QUACK_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone)]
pub struct QuackServerState {
    pub sessions: SessionStore,
    pub server_duckdb_version: String,
    pub server_platform: String,
    pub auth_provider: Arc<dyn AuthProvider>,
    pub query_executor: Arc<dyn QueryExecutor>,
}

impl QuackServerState {
    /// Construct a server state with a pluggable auth provider and query
    /// executor. Use the production `AuthChain` + a `QueryHandler` adapter in
    /// production; tests typically supply a stub for both.
    pub fn new(
        auth_provider: Arc<dyn AuthProvider>,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Self {
        Self {
            sessions: SessionStore::new(Duration::from_secs(600)),
            server_duckdb_version: format!("sqe-{}", env!("CARGO_PKG_VERSION")),
            server_platform: std::env::consts::OS.to_string(),
            auth_provider,
            query_executor,
        }
    }
}

pub fn router(state: QuackServerState) -> Router {
    Router::new()
        .route("/", get(identify))
        .route("/quack", post(handle_quack))
        // Explicit body limit so the cap does not silently depend on axum's
        // implicit default; the pre-auth decode paths must stay bounded.
        .layer(DefaultBodyLimit::max(QUACK_MAX_BODY_BYTES))
        .with_state(Arc::new(state))
}

/// True for message types a *client* must never send: server-only responses
/// and the invalid sentinel. We reject these from the wire-supplied header
/// before decoding the (potentially DataChunk-carrying) body, so a malicious
/// caller cannot drive the expensive response-body decode paths.
fn is_server_only_message(ty: MessageType) -> bool {
    matches!(
        ty,
        MessageType::Invalid
            | MessageType::ConnectionResponse
            | MessageType::PrepareResponse
            | MessageType::FetchResponse
            | MessageType::SuccessResponse
            | MessageType::ErrorResponse
    )
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
    // Decode only the header first and reject server-only / response message
    // types up front. This avoids running the full body decoder (including the
    // DataChunk-carrying response paths) for messages a client must never send.
    let mut header_d = BinaryDeserializer::new(&body);
    let pre_header = match MessageHeader::decode(&mut header_d) {
        Ok(h) => h,
        Err(e) => return error_response("", None, format!("SQE-PARSE: {e}")),
    };
    if is_server_only_message(pre_header.r#type) {
        return error_response(
            &pre_header.connection_id,
            pre_header.client_query_id,
            format!(
                "SQE-DIALECT: message type {:?} is server-only and not accepted",
                pre_header.r#type
            ),
        );
    }

    let (request_header, request_body) = match decode_message(&body) {
        Ok(v) => v,
        Err(e) => return error_response("", None, format!("SQE-PARSE: {e}")),
    };

    match (request_header.r#type, request_body) {
        (MessageType::ConnectionRequest, QuackMessage::ConnectionRequest(req)) => {
            handle_connection_request(&state, &request_header, req).await
        }
        (MessageType::DisconnectMessage, QuackMessage::DisconnectMessage) => {
            handle_disconnect_message(&state, &request_header)
        }
        (MessageType::PrepareRequest, QuackMessage::PrepareRequest(req)) => {
            handle_prepare_request(&state, &request_header, req).await
        }
        (msg_type, _) => error_response(
            &request_header.connection_id,
            request_header.client_query_id,
            format!("SQE-DIALECT: message type {msg_type:?} not yet supported"),
        ),
    }
}

async fn handle_prepare_request(
    state: &QuackServerState,
    request_header: &MessageHeader,
    req: sqe_quack_wire::message::PrepareRequest,
) -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
    let session = match state.sessions.get(&request_header.connection_id) {
        Some(s) => s,
        None => {
            return error_response(
                &request_header.connection_id,
                request_header.client_query_id,
                "SQE-AUTH: unknown connection_id".to_string(),
            );
        }
    };

    let batches = match state
        .query_executor
        .execute(&session.core_session, &req.sql_query)
        .await
    {
        Ok(b) => b,
        Err(QueryError::Parse(msg)) => {
            return error_response(
                &request_header.connection_id,
                request_header.client_query_id,
                format!("SQE-PARSE: {msg}"),
            );
        }
        Err(QueryError::Policy(msg)) => {
            // Don't echo the policy decision detail — that's the row-filter
            // / column-mask payload, and leaking it defeats the policy.
            tracing::info!(reason = %msg, "policy denied query");
            return error_response(
                &request_header.connection_id,
                request_header.client_query_id,
                "SQE-POLICY: access denied".to_string(),
            );
        }
        Err(QueryError::Execution(msg)) => {
            return error_response(
                &request_header.connection_id,
                request_header.client_query_id,
                format!("SQE-EXEC: {msg}"),
            );
        }
        Err(QueryError::Internal(msg)) => {
            tracing::warn!(error = %msg, "query executor internal error");
            return error_response(
                &request_header.connection_id,
                request_header.client_query_id,
                "SQE-EXEC: internal execution error".to_string(),
            );
        }
    };

    // Drain the batches into our DataChunk representation. Type/name schema
    // is captured from the first batch; later batches must match (DataFusion
    // guarantees this within a single result stream).
    let (result_types, result_names) = match batches.first() {
        Some(first) => {
            let names: Vec<String> = first
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            // We materialise the first batch to read its column types into
            // LogicalType; conversion happens once we collect all chunks.
            let first_chunk = match record_batch_to_data_chunk(first) {
                Ok(c) => c,
                Err(e) => {
                    return error_response(
                        &request_header.connection_id,
                        request_header.client_query_id,
                        format!("SQE-EXEC: {e}"),
                    );
                }
            };
            let types: Vec<_> = first_chunk
                .columns
                .iter()
                .map(|v| v.logical_type.clone())
                .collect();
            (types, names)
        }
        None => (Vec::new(), Vec::new()),
    };

    let mut chunks: Vec<DataChunk> = Vec::with_capacity(batches.len());
    for batch in &batches {
        match record_batch_to_data_chunk(batch) {
            Ok(c) => chunks.push(c),
            Err(e) => {
                return error_response(
                    &request_header.connection_id,
                    request_header.client_query_id,
                    format!("SQE-EXEC: {e}"),
                );
            }
        }
    }

    let response_header = MessageHeader {
        r#type: MessageType::PrepareResponse,
        connection_id: request_header.connection_id.clone(),
        client_query_id: request_header.client_query_id,
    };
    let body = QuackMessage::PrepareResponse(PrepareResponse {
        result_types,
        result_names,
        // All batches inlined; FETCH loop lands once result streaming is needed.
        needs_more_fetch: false,
        results: chunks,
        result_uuid: 0,
    });
    let bytes = encode_message(&response_header, &body);
    (
        StatusCode::OK,
        [("content-type", APPLICATION_VND_DUCKDB)],
        bytes,
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

async fn handle_connection_request(
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

    let credentials = FlightCredentials {
        bearer_token: Some(SecretString::new(req.auth_string.clone())),
        ..Default::default()
    };

    let identity = match state.auth_provider.authenticate(&credentials).await {
        Ok(identity) => identity,
        Err(AuthError::NotMyCredentials) => {
            return error_response(
                "",
                request_header.client_query_id,
                "SQE-AUTH: no provider accepted the bearer token".to_string(),
            );
        }
        Err(AuthError::AuthFailed(msg)) => {
            return error_response(
                "",
                request_header.client_query_id,
                format!("SQE-AUTH: {msg}"),
            );
        }
        Err(AuthError::Internal(e)) => {
            tracing::warn!(error = %e, "auth provider internal error");
            // Don't leak internals to the wire (issue #38 style).
            return error_response(
                "",
                request_header.client_query_id,
                "SQE-AUTH: internal authentication error".to_string(),
            );
        }
    };

    let connection_id = Uuid::new_v4().to_string();
    let core_session = identity_to_core_session(&identity);
    state.sessions.insert(Session {
        connection_id: connection_id.clone(),
        bearer_token: req.auth_string,
        identity,
        core_session,
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
