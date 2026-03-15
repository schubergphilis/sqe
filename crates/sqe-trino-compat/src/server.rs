use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use dashmap::DashMap;
use tracing::{info, warn};
use uuid::Uuid;

use sqe_core::Session;

use crate::protocol::{self, TrinoError, TrinoResponse, TrinoStats};

pub struct TrinoState<A, Q> {
    pub authenticator: Arc<A>,
    pub query_handler: Arc<Q>,
    pub results: DashMap<String, CachedResult>,
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
) -> tokio::task::JoinHandle<()>
where
    A: TrinoAuthenticator,
    Q: TrinoQueryExecutor,
{
    let state = Arc::new(TrinoState {
        authenticator,
        query_handler,
        results: DashMap::new(),
    });

    tokio::spawn(async move {
        let app = Router::new()
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

async fn submit_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let sql = body.trim();
    if sql.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "Empty query");
    }

    let session = match extract_basic_auth(&headers) {
        Some((user, pass)) => {
            match state.authenticator.authenticate(&user, &pass).await {
                Ok(s) => s,
                Err(e) => {
                    return error_response(
                        StatusCode::UNAUTHORIZED,
                        format!("Authentication failed: {e}"),
                    );
                }
            }
        }
        None => {
            return error_response(StatusCode::UNAUTHORIZED, "Missing Authorization header");
        }
    };

    let query_id = Uuid::new_v4().to_string();

    match state.query_handler.execute(&session, sql).await {
        Ok(batches) => {
            let (columns, data) = protocol::batches_to_trino(&batches);
            let response = TrinoResponse {
                id: query_id,
                info_uri: None,
                next_uri: None,
                columns: Some(columns),
                data: Some(data),
                stats: TrinoStats::finished(),
                error: None,
            };
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
