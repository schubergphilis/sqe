//! Axum HTTP application: `GET /` identification + `POST /quack` RPC.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};

use arrow_array::RecordBatch;
use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use sqe_auth::{AuthError, AuthProvider, FlightCredentials};
use sqe_core::config::{strip_port, SecurityConfig};
use sqe_core::SecretString;
use sqe_quack_wire::arrow_bridge::{arrow_schema_to_logical, record_batch_to_data_chunk};
use sqe_quack_wire::codec::BinaryDeserializer;
use sqe_quack_wire::data_chunk::{DataChunk, LogicalType};
use sqe_quack_wire::message::{
    decode_message, encode_message, ConnectionResponse, ErrorResponse, FetchResponse,
    MessageHeader, MessageType, PrepareResponse, QuackMessage,
};
use uuid::Uuid;

use crate::pending::{PendingResult, PendingResults, PENDING_RESULT_IDLE};
use crate::query_executor::{QueryError, QueryExecutor, QueryStream};
use crate::session::{
    Session, SessionRejection, SessionStore, DEFAULT_SESSION_IDLE, DEFAULT_SESSION_MAX_LIFETIME,
};

const QUACK_VERSION: u64 = 1;
const APPLICATION_VND_DUCKDB: &str = "application/vnd.duckdb";

/// What every handler returns: status, the single content-type header, and the
/// encoded Quack message body.
type QuackResponse = (StatusCode, [(&'static str, &'static str); 1], Vec<u8>);

/// QUACK-08: ceiling on `ConnectionRequest` auth attempts per second, per
/// client IP. Each request triggers an IdP round-trip, so an un-throttled path
/// is a brute-force / IdP-amplification oracle. Connections are long-lived and
/// reused for queries, so legitimate clients connect rarely; this bound is
/// generous for real use and hard on a credential-stuffing loop. Burst equals
/// the per-second rate. The limiter is keyed by the resolved client IP (see
/// `client_key`), so one abusive source cannot exhaust the budget for every
/// other client the way a single global bucket would.
const QUACK_AUTH_RATE_PER_SEC: u32 = 10;

/// QUACK-08: opportunistic garbage-collection cadence for the keyed limiter.
/// governor's keyed limiter never frees per-key state on its own, so every
/// distinct source IP would otherwise leave a permanent map entry and an
/// attacker spraying spoofed `x-forwarded-for` values (behind a trusted proxy)
/// or rotating real source addresses could grow it without bound. Every Nth
/// check we call `retain_recent`, which drops keys whose buckets have fully
/// refilled (i.e. idle long enough to be indistinguishable from never having
/// existed). The cadence keeps the sweep off the hot path while bounding the
/// map to roughly the set of IPs active within one refill window.
const QUACK_GC_EVERY_N_CHECKS: u64 = 256;

/// Explicit request-body cap for the `/quack` endpoint. The DataChunk-carrying
/// decode paths are reachable pre-auth, so we keep the ceiling small and
/// independent of axum's implicit default. 4 MiB comfortably covers a
/// handshake, a DISCONNECT, and a reasonable PREPARE SQL string while leaving
/// no room for a multi-MB recursion / allocation-count payload.
const QUACK_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Per-response ceiling on encoded result bytes (finding 606). The server
/// stops filling a `PrepareResponse`/`FetchResponse` once the Arrow batches it
/// has pulled reach this size and parks the rest of the stream for the next
/// `FetchRequest`. 8 MiB keeps one response comfortably inside a normal HTTP
/// body while still amortising the round trip.
const QUACK_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Ceiling on the *total* Arrow bytes one query may return across its
/// `PrepareResponse` and every subsequent `FetchResponse`. Paging alone bounds
/// peak memory but not total work, so an unqualified `SELECT *` over a large
/// table would still stream forever; past this point the query is refused and
/// the parked stream dropped.
const QUACK_MAX_RESULT_BYTES: usize = 256 * 1024 * 1024;

/// The two result-size bounds, together so they travel as one unit.
#[derive(Debug, Clone, Copy)]
pub struct ResultLimits {
    pub max_response_bytes: usize,
    pub max_result_bytes: usize,
}

impl Default for ResultLimits {
    fn default() -> Self {
        Self {
            max_response_bytes: QUACK_MAX_RESPONSE_BYTES,
            max_result_bytes: QUACK_MAX_RESULT_BYTES,
        }
    }
}

#[derive(Clone)]
pub struct QuackServerState {
    pub sessions: SessionStore,
    /// Half-drained result streams awaiting a `FetchRequest`.
    pub pending: PendingResults,
    /// Per-response and per-result byte ceilings.
    pub limits: ResultLimits,
    pub server_duckdb_version: String,
    pub server_platform: String,
    pub auth_provider: Arc<dyn AuthProvider>,
    pub query_executor: Arc<dyn QueryExecutor>,
    /// QUACK-08: per-client-IP throttle on the ConnectionRequest auth path.
    /// Keyed so one source cannot deny auth to every other client.
    pub auth_limiter: Arc<DefaultKeyedRateLimiter<String>>,
    /// Trusted-proxy allowlist used to resolve the real client IP from the
    /// peer address and `x-forwarded-for` (Issue #74 semantics). Empty by
    /// default, so the limiter keys on the TCP peer and ignores the header.
    pub security: SecurityConfig,
    /// Counts limiter checks so we can run governor's `retain_recent` GC
    /// once every `QUACK_GC_EVERY_N_CHECKS` rather than on every request.
    gc_counter: Arc<AtomicU64>,
}

impl QuackServerState {
    /// Construct a server state with a pluggable auth provider and query
    /// executor. Use the production `AuthChain` + a `QueryHandler` adapter in
    /// production; tests typically supply a stub for both. The trusted-proxy
    /// allowlist defaults to empty; call [`with_security`](Self::with_security)
    /// to honour `x-forwarded-for` from known proxies.
    pub fn new(
        auth_provider: Arc<dyn AuthProvider>,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Self {
        Self {
            sessions: SessionStore::new(DEFAULT_SESSION_IDLE, DEFAULT_SESSION_MAX_LIFETIME),
            pending: PendingResults::new(PENDING_RESULT_IDLE),
            limits: ResultLimits::default(),
            server_duckdb_version: format!("sqe-{}", env!("CARGO_PKG_VERSION")),
            server_platform: std::env::consts::OS.to_string(),
            auth_provider,
            query_executor,
            auth_limiter: Arc::new(RateLimiter::keyed(Quota::per_second(
                NonZeroU32::new(QUACK_AUTH_RATE_PER_SEC).expect("auth rate must be > 0"),
            ))),
            security: SecurityConfig::default(),
            gc_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Set the trusted-proxy allowlist used when resolving the client IP for
    /// the per-IP auth limiter. Without it the limiter keys on the raw TCP
    /// peer, which is correct for a directly-exposed deployment but wrong
    /// behind a load balancer (every request would share the proxy's IP).
    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.security = security;
        self
    }

    /// Override the session idle window and absolute lifetime. Defaults are
    /// 600 s idle / 8 h maximum; the token's own `exp` is checked per request
    /// regardless and is usually the tighter bound.
    pub fn with_session_limits(mut self, idle: Duration, max_lifetime: Duration) -> Self {
        self.sessions = SessionStore::new(idle, max_lifetime);
        self
    }

    /// Override the per-response and per-result byte ceilings (defaults 8 MiB
    /// and 256 MiB).
    pub fn with_result_limits(
        mut self,
        max_response_bytes: usize,
        max_result_bytes: usize,
    ) -> Self {
        self.limits = ResultLimits {
            max_response_bytes,
            max_result_bytes,
        };
        self
    }

    /// Check the per-IP auth budget for `client_ip` (already port-stripped),
    /// running an opportunistic GC sweep on a fixed cadence so the keyed map
    /// stays bounded to recently-active sources.
    fn check_auth_rate(&self, client_ip: &str) -> Result<(), ()> {
        let result = self.auth_limiter.check_key(&client_ip.to_string());
        if self.gc_counter.fetch_add(1, Ordering::Relaxed) % QUACK_GC_EVERY_N_CHECKS == 0 {
            // Drops keys whose buckets have fully refilled; cheap relative to
            // the IdP round-trip the limiter is gating.
            self.auth_limiter.retain_recent();
        }
        result.map_err(|_| ())
    }
}

/// Resolve the rate-limit key for a request: the client IP with its ephemeral
/// source port stripped. `x-forwarded-for` is honoured only when the TCP peer
/// is a configured trusted proxy (Issue #74, via `SecurityConfig`). The port
/// strip is load-bearing: keeping it would give every fresh TCP connection a
/// distinct key and defeat per-IP limiting entirely.
fn client_key(peer: SocketAddr, forwarded_for: Option<&str>, security: &SecurityConfig) -> String {
    let resolved = security.resolve_client_ip(Some(&peer.to_string()), forwarded_for);
    strip_port(&resolved).to_string()
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

#[tracing::instrument(skip_all, name = "quack.handle")]
async fn handle_quack(
    State(state): State<Arc<QuackServerState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    // `Bytes` consumes the request body, so it must be the last extractor.
    body: Bytes,
) -> impl IntoResponse {
    // O6: Trace propagation for Quack.
    sqe_metrics::propagation::attach_trace_context_from_headers(&headers);

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

    // Resolve the client key once, for every message type. The auth limiter
    // needs it on the ConnectionRequest path; every *post*-connect path needs
    // it to check the session's client binding (finding 606), so it can no
    // longer live inside the ConnectionRequest arm.
    let forwarded_for = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok());
    let client_ip = client_key(peer, forwarded_for, &state.security);

    match (request_header.r#type, request_body) {
        (MessageType::ConnectionRequest, QuackMessage::ConnectionRequest(req)) => {
            handle_connection_request(&state, &request_header, &client_ip, req).await
        }
        (MessageType::DisconnectMessage, QuackMessage::DisconnectMessage) => {
            handle_disconnect_message(&state, &request_header, &client_ip)
        }
        (MessageType::PrepareRequest, QuackMessage::PrepareRequest(req)) => {
            handle_prepare_request(&state, &request_header, &client_ip, req).await
        }
        (MessageType::FetchRequest, QuackMessage::FetchRequest(req)) => {
            handle_fetch_request(&state, &request_header, &client_ip, req).await
        }
        (msg_type, _) => error_response(
            &request_header.connection_id,
            request_header.client_query_id,
            format!("SQE-DIALECT: message type {msg_type:?} not yet supported"),
        ),
    }
}

/// Wire message for a refused session. `Unknown` and `ClientMismatch` share one
/// string on purpose: a caller replaying a stolen `connection_id` from the
/// wrong source must not learn that the id itself was valid.
fn session_rejection_message(rejection: SessionRejection) -> &'static str {
    match rejection {
        SessionRejection::Expired => "SQE-AUTH: session expired",
        SessionRejection::Unknown | SessionRejection::ClientMismatch => {
            "SQE-AUTH: unknown connection_id"
        }
    }
}

/// Resolve the session for a post-connect request, dropping any parked results
/// belonging to a session that has just been invalidated.
fn authorize(
    state: &QuackServerState,
    request_header: &MessageHeader,
    client_ip: &str,
) -> Result<Session, QuackResponse> {
    match state
        .sessions
        .get_authorized(&request_header.connection_id, client_ip)
    {
        Ok(session) => Ok(session),
        Err(rejection) => {
            if rejection == SessionRejection::Expired {
                // The session is gone; its parked streams must go with it,
                // otherwise an expired connection keeps DataFusion streams
                // (and their memory reservations) alive until the idle sweep.
                state
                    .pending
                    .remove_for_connection(&request_header.connection_id);
            }
            Err(error_response(
                &request_header.connection_id,
                request_header.client_query_id,
                session_rejection_message(rejection).to_string(),
            ))
        }
    }
}

/// One page of results: batches pulled from `stream` until either the stream
/// ended or the accumulated Arrow size reached `max_response_bytes`.
struct Page {
    batches: Vec<RecordBatch>,
    bytes: usize,
    /// True when the stream reported EOF while filling this page.
    exhausted: bool,
}

async fn pull_page(
    stream: &mut BoxStream<'static, Result<RecordBatch, QueryError>>,
    max_response_bytes: usize,
) -> Result<Page, QueryError> {
    let mut batches = Vec::new();
    let mut bytes = 0usize;
    let mut exhausted = true;
    while let Some(item) = stream.next().await {
        let batch = item?;
        bytes = bytes.saturating_add(batch.get_array_memory_size());
        batches.push(batch);
        if bytes >= max_response_bytes {
            exhausted = false;
            break;
        }
    }
    Ok(Page {
        batches,
        bytes,
        exhausted,
    })
}

fn encode_page(batches: &[RecordBatch]) -> Result<Vec<DataChunk>, sqe_quack_wire::WireError> {
    batches.iter().map(record_batch_to_data_chunk).collect()
}

/// Map a `QueryError` onto the wire, applying the masking rules: policy
/// denials and internal errors never carry their detail to the client.
fn query_error_response(request_header: &MessageHeader, err: QueryError) -> QuackResponse {
    let message = match err {
        QueryError::Parse(msg) => format!("SQE-PARSE: {msg}"),
        QueryError::Policy(msg) => {
            // Don't echo the policy decision detail — that's the row-filter
            // / column-mask payload, and leaking it defeats the policy.
            tracing::info!(reason = %msg, "policy denied query");
            "SQE-POLICY: access denied".to_string()
        }
        QueryError::Execution(msg) => format!("SQE-EXEC: {msg}"),
        QueryError::Internal(msg) => {
            tracing::warn!(error = %msg, "query executor internal error");
            "SQE-EXEC: internal execution error".to_string()
        }
    };
    error_response(
        &request_header.connection_id,
        request_header.client_query_id,
        message,
    )
}

fn result_too_large_response(
    request_header: &MessageHeader,
    max_result_bytes: usize,
) -> QuackResponse {
    error_response(
        &request_header.connection_id,
        request_header.client_query_id,
        format!("SQE-EXEC: result exceeds {max_result_bytes} bytes; narrow the query"),
    )
}

async fn handle_prepare_request(
    state: &QuackServerState,
    request_header: &MessageHeader,
    client_ip: &str,
    req: sqe_quack_wire::message::PrepareRequest,
) -> QuackResponse {
    let session = match authorize(state, request_header, client_ip) {
        Ok(s) => s,
        Err(resp) => return resp,
    };

    let QueryStream {
        schema,
        mut batches,
    } = match state
        .query_executor
        .execute(&session.core_session, &req.sql_query)
        .await
    {
        Ok(s) => s,
        Err(e) => return query_error_response(request_header, e),
    };

    // Column names and types come from the *schema*, not from the first batch:
    // a query returning zero rows still has to name and type its columns.
    let result_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let page = match pull_page(&mut batches, state.limits.max_response_bytes).await {
        Ok(p) => p,
        Err(e) => return query_error_response(request_header, e),
    };
    if page.bytes > state.limits.max_result_bytes {
        return result_too_large_response(request_header, state.limits.max_result_bytes);
    }

    let chunks = match encode_page(&page.batches) {
        Ok(c) => c,
        Err(e) => {
            return error_response(
                &request_header.connection_id,
                request_header.client_query_id,
                format!("SQE-EXEC: {e}"),
            );
        }
    };
    let result_types = match result_types_for(&schema, &chunks) {
        Ok(t) => t,
        Err(e) => {
            return error_response(
                &request_header.connection_id,
                request_header.client_query_id,
                format!("SQE-EXEC: {e}"),
            );
        }
    };

    let (needs_more_fetch, result_uuid) = if page.exhausted {
        (false, 0i128)
    } else {
        // Park the rest of the stream. The uuid is random (not a counter) so a
        // client cannot walk another session's result ids; the connection_id
        // stored alongside is what actually authorises the fetch.
        let uuid = Uuid::new_v4().as_u128() as i128;
        state.pending.insert(
            uuid,
            Arc::new(PendingResult::new(
                request_header.connection_id.clone(),
                result_names.clone(),
                batches,
                page.bytes as u64,
            )),
        );
        (true, uuid)
    };

    let response_header = MessageHeader {
        r#type: MessageType::PrepareResponse,
        connection_id: request_header.connection_id.clone(),
        client_query_id: request_header.client_query_id,
    };
    let body = QuackMessage::PrepareResponse(PrepareResponse {
        result_types,
        result_names,
        needs_more_fetch,
        results: chunks,
        result_uuid,
    });
    let bytes = encode_message(&response_header, &body);
    (
        StatusCode::OK,
        [("content-type", APPLICATION_VND_DUCKDB)],
        bytes,
    )
}

/// Column types for the response. The schema is the source of truth (it exists
/// even for a zero-row result); the encoded chunks are the fallback for the one
/// case the type alone cannot express — DuckDB `ENUM`, whose dictionary values
/// live in the array rather than in the Arrow `DataType`.
fn result_types_for(
    schema: &arrow_schema::SchemaRef,
    chunks: &[DataChunk],
) -> Result<Vec<LogicalType>, sqe_quack_wire::WireError> {
    match arrow_schema_to_logical(schema.as_ref()) {
        Ok(types) => Ok(types),
        Err(e) => match chunks.first() {
            Some(first) => Ok(first
                .columns
                .iter()
                .map(|v| v.logical_type.clone())
                .collect()),
            None => Err(e),
        },
    }
}

async fn handle_fetch_request(
    state: &QuackServerState,
    request_header: &MessageHeader,
    client_ip: &str,
    req: sqe_quack_wire::message::FetchRequest,
) -> QuackResponse {
    if let Err(resp) = authorize(state, request_header, client_ip) {
        return resp;
    }

    // Unknown uuid and wrong-owner uuid share one message: a fetch is not a
    // probe for which result ids exist on other connections.
    let unknown = || {
        error_response(
            &request_header.connection_id,
            request_header.client_query_id,
            "SQE-EXEC: unknown or expired result_uuid".to_string(),
        )
    };
    let pending = match state.pending.get(req.uuid) {
        Some(p) if p.connection_id == request_header.connection_id => p,
        Some(_) => {
            tracing::warn!(
                connection_id = %request_header.connection_id,
                "Quack FetchRequest for a result_uuid owned by another connection"
            );
            return unknown();
        }
        None => return unknown(),
    };

    let mut guard = pending.stream.lock().await;
    let page = match guard.as_mut() {
        Some(stream) => match pull_page(stream, state.limits.max_response_bytes).await {
            Ok(p) => p,
            Err(e) => {
                drop(guard);
                state.pending.remove(req.uuid);
                return query_error_response(request_header, e);
            }
        },
        // Stream already reported EOF on an earlier fetch; this call is the
        // empty terminator the client's loop is waiting for.
        None => Page {
            batches: Vec::new(),
            bytes: 0,
            exhausted: true,
        },
    };
    if page.exhausted {
        // Release the DataFusion stream as soon as it is done, even though the
        // entry survives one more call to answer with an empty page.
        *guard = None;
    }

    let total = pending.charge(page.bytes as u64);
    if total > state.limits.max_result_bytes as u64 {
        drop(guard);
        state.pending.remove(req.uuid);
        return result_too_large_response(request_header, state.limits.max_result_bytes);
    }

    if page.batches.is_empty() {
        drop(guard);
        state.pending.remove(req.uuid);
        return fetch_response(request_header, Vec::new(), pending.next_page());
    }

    let chunks = match encode_page(&page.batches) {
        Ok(c) => c,
        Err(e) => {
            drop(guard);
            state.pending.remove(req.uuid);
            return error_response(
                &request_header.connection_id,
                request_header.client_query_id,
                format!("SQE-EXEC: {e}"),
            );
        }
    };
    let page_no = pending.next_page();
    drop(guard);
    fetch_response(request_header, chunks, page_no)
}

fn fetch_response(
    request_header: &MessageHeader,
    results: Vec<DataChunk>,
    page_no: u64,
) -> QuackResponse {
    let response_header = MessageHeader {
        r#type: MessageType::FetchResponse,
        connection_id: request_header.connection_id.clone(),
        client_query_id: request_header.client_query_id,
    };
    let body = QuackMessage::FetchResponse(FetchResponse {
        results,
        batch_index: Some(page_no),
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
    client_ip: &str,
) -> QuackResponse {
    if let Err(resp) = authorize(state, request_header, client_ip) {
        return resp;
    }
    state.sessions.remove(&request_header.connection_id);
    // Parked results belong to the session, not to the client's goodwill:
    // dropping them here releases their DataFusion streams immediately.
    state
        .pending
        .remove_for_connection(&request_header.connection_id);

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
    client_ip: &str,
    req: sqe_quack_wire::message::ConnectionRequest,
) -> QuackResponse {
    // QUACK-08: throttle before touching the IdP, keyed by client IP. Rejected
    // attempts never reach authenticate(), so a credential-stuffing loop cannot
    // amplify into the IdP, and one abusive source cannot exhaust the budget
    // for other clients.
    if state.check_auth_rate(client_ip).is_err() {
        tracing::warn!(client_ip = %client_ip, "Quack auth path rate-limited a ConnectionRequest");
        return error_response(
            "",
            request_header.client_query_id,
            "SQE-RATELIMIT: too many connection attempts; slow down".to_string(),
        );
    }

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
    // Shared mapping (finding 204): carries subject, email and groups so
    // group-scoped policies apply over Quack exactly as over Flight SQL.
    let core_session = identity.to_session(None);
    state.sessions.insert(Session {
        connection_id: connection_id.clone(),
        bearer_token: SecretString::new(req.auth_string),
        identity,
        core_session,
        // 606: bind the connection to the client that opened it. The
        // connection_id is otherwise a replayable bearer credential travelling
        // in every request body.
        client_key: client_ip.to_string(),
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
) -> QuackResponse {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn keyed_limiter() -> DefaultKeyedRateLimiter<String> {
        // Built exactly like QuackServerState's auth_limiter (QUACK-08).
        RateLimiter::keyed(Quota::per_second(
            NonZeroU32::new(QUACK_AUTH_RATE_PER_SEC).unwrap(),
        ))
    }

    #[test]
    fn auth_limiter_denies_after_burst_per_key() {
        let limiter = keyed_limiter();
        let key = "203.0.113.5".to_string();
        // Burst capacity equals the per-second rate: that many pass instantly.
        for _ in 0..QUACK_AUTH_RATE_PER_SEC {
            assert!(limiter.check_key(&key).is_ok());
        }
        // The next attempt in the same instant (no replenishment yet) is denied.
        assert!(limiter.check_key(&key).is_err());
    }

    /// The core fix: two distinct source IPs get independent buckets, so one
    /// IP exhausting its budget never blocks another. A keyless global limiter
    /// would fail this because both keys would share one counter.
    #[test]
    fn distinct_ips_get_independent_buckets() {
        let limiter = keyed_limiter();
        let attacker = "203.0.113.5".to_string();
        let victim = "198.51.100.9".to_string();

        // Attacker exhausts its own bucket entirely.
        for _ in 0..QUACK_AUTH_RATE_PER_SEC {
            assert!(limiter.check_key(&attacker).is_ok());
        }
        assert!(
            limiter.check_key(&attacker).is_err(),
            "attacker's bucket should be exhausted"
        );

        // The victim's first attempt still succeeds: separate bucket.
        assert!(
            limiter.check_key(&victim).is_ok(),
            "a different IP must not be blocked by the attacker"
        );
    }

    /// Guards the load-bearing port strip. The key must be derived the way
    /// production derives it (peer SocketAddr -> resolve -> strip_port), not
    /// from a bare IP literal. Two connections from one IP on different
    /// ephemeral ports must collapse to one key; otherwise the limiter would
    /// be a silent no-op (every new TCP connection a fresh bucket).
    #[test]
    fn client_key_collapses_ports_and_separates_ips() {
        let sec = SecurityConfig::default();
        let a1: SocketAddr = "203.0.113.5:40001".parse().unwrap();
        let a2: SocketAddr = "203.0.113.5:55002".parse().unwrap();
        let b: SocketAddr = "198.51.100.9:40001".parse().unwrap();

        assert_eq!(
            client_key(a1, None, &sec),
            client_key(a2, None, &sec),
            "same IP, different ports must share one bucket"
        );
        assert_ne!(
            client_key(a1, None, &sec),
            client_key(b, None, &sec),
            "different IPs must map to different buckets"
        );

        // And the derived keys behave as independent buckets through the limiter.
        let limiter = keyed_limiter();
        let ka = client_key(a1, None, &sec);
        let kb = client_key(b, None, &sec);
        for _ in 0..QUACK_AUTH_RATE_PER_SEC {
            assert!(limiter.check_key(&ka).is_ok());
        }
        // Same source IP on a new port is still rate-limited (one bucket).
        assert!(limiter.check_key(&client_key(a2, None, &sec)).is_err());
        // Different IP is unaffected.
        assert!(limiter.check_key(&kb).is_ok());
    }

    /// `x-forwarded-for` is ignored unless the peer is a trusted proxy; with
    /// the proxy trusted, the key follows the forwarded client IP, so distinct
    /// real clients behind one proxy get distinct buckets.
    #[test]
    fn client_key_honours_trusted_proxy_xff() {
        let proxy: SocketAddr = "10.0.0.1:50000".parse().unwrap();

        // Untrusted by default: key is the proxy's own IP, header ignored.
        let untrusted = SecurityConfig::default();
        assert_eq!(
            client_key(proxy, Some("203.0.113.5"), &untrusted),
            "10.0.0.1"
        );

        // Trusted proxy: key follows the forwarded client.
        let trusted = SecurityConfig {
            trusted_proxies: vec!["10.0.0.1".to_string()],
            ..SecurityConfig::default()
        };
        assert_eq!(
            client_key(proxy, Some("203.0.113.5"), &trusted),
            "203.0.113.5"
        );
        assert_ne!(
            client_key(proxy, Some("203.0.113.5"), &trusted),
            client_key(proxy, Some("198.51.100.9"), &trusted),
            "two real clients behind one trusted proxy must not share a bucket"
        );
    }
}
