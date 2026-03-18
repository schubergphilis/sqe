use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use dashmap::DashMap;
use tracing::{info, warn};
use uuid::Uuid;

use sqe_core::Session;

use crate::protocol::{self, NodeVersion, ServerInfo, TrinoError, TrinoResponse, TrinoStats};

/// Shared context for Trino /v1/info endpoints.
pub struct NodeContext {
    pub version: String,
    pub ready: Arc<AtomicBool>,
    pub started_at: Instant,
}

pub struct TrinoState<A, Q> {
    pub authenticator: Arc<A>,
    pub query_handler: Arc<Q>,
    pub results: DashMap<String, CachedResult>,
    pub node: NodeContext,
}

pub struct CachedResult {
    pub response: TrinoResponse,
}

#[async_trait::async_trait]
pub trait TrinoAuthenticator: Send + Sync + 'static {
    async fn authenticate(&self, username: &str, password: &str) -> Result<Session, String>;
}

#[async_trait::async_trait]
pub trait TrinoQueryExecutor: Send + Sync + 'static {
    async fn execute(
        &self,
        session: &Session,
        sql: &str,
    ) -> Result<Vec<arrow_array::RecordBatch>, String>;
}

pub fn start_trino_server<A, Q>(
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    port: u16,
    node: NodeContext,
) -> tokio::task::JoinHandle<()>
where
    A: TrinoAuthenticator,
    Q: TrinoQueryExecutor,
{
    let state = Arc::new(TrinoState {
        authenticator,
        query_handler,
        results: DashMap::new(),
        node,
    });

    tokio::spawn(async move {
        let app = Router::new()
            .route("/v1/info", get(server_info::<A, Q>))
            .route("/v1/info/state", get(server_state::<A, Q>))
            .route("/v1/statement", post(submit_query::<A, Q>))
            .route("/v1/statement/{id}/{token}", get(get_results::<A, Q>))
            .route("/v1/statement/{id}", delete(cancel_query::<A, Q>))
            .with_state(state);

        let addr = format!("0.0.0.0:{port}");
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

        info!("Trino-compat HTTP server listening on {addr}");

        axum::serve(listener, app).await.unwrap();
    })
}

// ── Trino /v1/info ────────────────────────────────────────────

fn format_uptime(started_at: Instant) -> String {
    let secs = started_at.elapsed().as_secs();
    if secs < 60 {
        format!("{secs}.00s")
    } else if secs < 3600 {
        format!("{:.2}m", secs as f64 / 60.0)
    } else if secs < 86400 {
        format!("{:.2}h", secs as f64 / 3600.0)
    } else {
        format!("{:.2}d", secs as f64 / 86400.0)
    }
}

async fn server_info<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
) -> Json<ServerInfo> {
    let ready = state.node.ready.load(Ordering::Relaxed);
    Json(ServerInfo {
        node_version: NodeVersion {
            version: state.node.version.clone(),
        },
        environment: "production".to_string(),
        coordinator: true,
        starting: !ready,
        uptime: format_uptime(state.node.started_at),
    })
}

async fn server_state<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
) -> String {
    let ready = state.node.ready.load(Ordering::Relaxed);
    if ready {
        "ACTIVE".to_string()
    } else {
        "STARTING".to_string()
    }
}

fn error_response(status: StatusCode, msg: impl Into<String>) -> Response {
    let body = TrinoResponse {
        id: String::new(),
        info_uri: None,
        next_uri: None,
        columns: None,
        data: None,
        stats: TrinoStats::failed(),
        error: Some(TrinoError {
            message: msg.into(),
            error_code: 1,
            error_name: "USER_ERROR".to_string(),
            error_type: "USER_ERROR".to_string(),
        }),
    };
    (status, Json(body)).into_response()
}

#[tracing::instrument(skip_all, name = "trino.submit_query")]
async fn submit_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let sql = body.trim();
    if sql.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "Empty query");
    }

    let session = if let Some(token) = extract_bearer_token(&headers) {
        // Bearer token auth: create session directly from the JWT.
        // The backend already authenticated the user via Keycloak and passes
        // the access token + X-Trino-User header.
        let username = headers
            .get("x-trino-user")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_string();
        Session::new(
            username,
            token,
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        )
    } else if let Some((user, pass)) = extract_basic_auth(&headers) {
        // Basic auth: authenticate via Keycloak ROPC
        match state.authenticator.authenticate(&user, &pass).await {
            Ok(s) => s,
            Err(e) => {
                return error_response(
                    StatusCode::UNAUTHORIZED,
                    format!("Authentication failed: {e}"),
                );
            }
        }
    } else {
        return error_response(StatusCode::UNAUTHORIZED, "Missing Authorization header");
    };

    let query_id = Uuid::new_v4().to_string();

    match state.query_handler.execute(&session, sql).await {
        Ok(batches) => {
            let (columns, data) = protocol::batches_to_trino(&batches);
            let response = TrinoResponse {
                id: query_id.clone(),
                info_uri: None,
                next_uri: None,
                columns: Some(columns),
                data: Some(data),
                stats: TrinoStats::finished(),
                error: None,
            };
            state.results.insert(query_id, CachedResult { response: response.clone() });
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            warn!(error = %e, sql = sql, "Trino query execution failed");
            let response = TrinoResponse {
                id: query_id,
                info_uri: None,
                next_uri: None,
                columns: None,
                data: None,
                stats: TrinoStats::failed(),
                error: Some(TrinoError {
                    message: e.to_string(),
                    error_code: 1,
                    error_name: "INTERNAL_ERROR".to_string(),
                    error_type: "INTERNAL_ERROR".to_string(),
                }),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
    }
}

#[tracing::instrument(skip_all, name = "trino.get_results")]
async fn get_results<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    Path((id, _token)): Path<(String, String)>,
) -> Response {
    match state.results.get(&id) {
        Some(cached) => (StatusCode::OK, Json(cached.response.clone())).into_response(),
        None => {
            let response = TrinoResponse {
                id,
                info_uri: None,
                next_uri: None,
                columns: None,
                data: None,
                stats: TrinoStats::failed(),
                error: Some(TrinoError {
                    message: "Query not found".to_string(),
                    error_code: 1,
                    error_name: "USER_ERROR".to_string(),
                    error_type: "USER_ERROR".to_string(),
                }),
            };
            (StatusCode::NOT_FOUND, Json(response)).into_response()
        }
    }
}

async fn cancel_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    Path(id): Path<String>,
) -> StatusCode {
    state.results.remove(&id);
    StatusCode::NO_CONTENT
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let token = auth.strip_prefix("Bearer ")?;
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

fn extract_basic_auth(headers: &HeaderMap) -> Option<(String, String)> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let encoded = auth.strip_prefix("Basic ")?;
    let decoded = String::from_utf8(base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        encoded,
    ).ok()?).ok()?;
    let (user, pass) = decoded.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_uptime_seconds() {
        // Instant doesn't let us control elapsed, so test the boundaries via the function logic
        // We'll test the function indirectly through the string format patterns
        let started = Instant::now();
        let uptime = format_uptime(started);
        // Just started, should be "0.00s" or very small seconds
        assert!(uptime.ends_with('s'), "Expected seconds format, got: {uptime}");
    }

    #[tokio::test]
    async fn test_server_info_when_ready() {
        let ready = Arc::new(AtomicBool::new(true));
        let started_at = Instant::now();

        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: ready.clone(),
                started_at,
            },
        });

        let Json(info) = server_info(State(state)).await;
        assert_eq!(info.node_version.version, "0.1.0");
        assert!(info.coordinator);
        assert!(!info.starting);
        assert_eq!(info.environment, "production");
    }

    #[tokio::test]
    async fn test_server_info_when_starting() {
        let ready = Arc::new(AtomicBool::new(false));
        let started_at = Instant::now();

        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready,
                started_at,
            },
        });

        let Json(info) = server_info(State(state)).await;
        assert!(info.starting);
    }

    #[tokio::test]
    async fn test_server_state_active() {
        let ready = Arc::new(AtomicBool::new(true));
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready,
                started_at: Instant::now(),
            },
        });

        let result = server_state(State(state)).await;
        assert_eq!(result, "ACTIVE");
    }

    #[tokio::test]
    async fn test_server_state_starting() {
        let ready = Arc::new(AtomicBool::new(false));
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready,
                started_at: Instant::now(),
            },
        });

        let result = server_state(State(state)).await;
        assert_eq!(result, "STARTING");
    }

    // Minimal mock types for handler tests
    struct MockAuth;
    #[async_trait::async_trait]
    impl TrinoAuthenticator for MockAuth {
        async fn authenticate(&self, _: &str, _: &str) -> Result<Session, String> {
            Err("mock".to_string())
        }
    }
    struct MockQuery;
    #[async_trait::async_trait]
    impl TrinoQueryExecutor for MockQuery {
        async fn execute(&self, _: &Session, _: &str) -> Result<Vec<arrow_array::RecordBatch>, String> {
            Err("mock".to_string())
        }
    }

    #[test]
    fn test_extract_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer eyJhbGciOi.payload.sig".parse().unwrap());
        let token = extract_bearer_token(&headers).unwrap();
        assert_eq!(token, "eyJhbGciOi.payload.sig");
    }

    #[test]
    fn test_extract_bearer_token_missing() {
        let headers = HeaderMap::new();
        assert!(extract_bearer_token(&headers).is_none());
    }

    #[test]
    fn test_extract_bearer_token_empty() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer ".parse().unwrap());
        assert!(extract_bearer_token(&headers).is_none());
    }

    #[test]
    fn test_extract_bearer_token_basic_auth_ignored() {
        let mut headers = HeaderMap::new();
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"user:pass",
        );
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());
        assert!(extract_bearer_token(&headers).is_none());
    }

    #[test]
    fn test_extract_basic_auth() {
        let mut headers = HeaderMap::new();
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"root:root123",
        );
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());

        let (user, pass) = extract_basic_auth(&headers).unwrap();
        assert_eq!(user, "root");
        assert_eq!(pass, "root123");
    }

    #[test]
    fn test_extract_basic_auth_missing() {
        let headers = HeaderMap::new();
        assert!(extract_basic_auth(&headers).is_none());
    }

    #[test]
    fn test_extract_basic_auth_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token123".parse().unwrap());
        assert!(extract_basic_auth(&headers).is_none());
    }
}
