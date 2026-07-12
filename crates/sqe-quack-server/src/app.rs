//! Axum HTTP application: `GET /` identification + `POST /quack` RPC.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};

use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use bytes::Bytes;
use sqe_auth::{AuthError, AuthProvider, FlightCredentials};
use sqe_core::config::{strip_port, SecurityConfig};
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

#[derive(Clone)]
pub struct QuackServerState {
    pub sessions: SessionStore,
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
            sessions: SessionStore::new(Duration::from_secs(600)),
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

async fn handle_quack(
    State(state): State<Arc<QuackServerState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    // `Bytes` consumes the request body, so it must be the last extractor.
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
            // Resolve the per-IP key only on the auth path; the limiter gates
            // ConnectionRequest specifically, since that is the IdP-amplifying
            // path. The header is read here (not in the dispatcher) so the
            // other message types pay no resolution cost.
            let forwarded_for = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok());
            let client_ip = client_key(peer, forwarded_for, &state.security);
            handle_connection_request(&state, &request_header, &client_ip, req).await
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
    client_ip: &str,
    req: sqe_quack_wire::message::ConnectionRequest,
) -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
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
    let core_session = identity_to_core_session(&identity);
    state.sessions.insert(Session {
        connection_id: connection_id.clone(),
        bearer_token: SecretString::new(req.auth_string),
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
