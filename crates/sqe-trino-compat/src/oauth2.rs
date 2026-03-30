//! Trino-compatible OAuth2 external authentication endpoints.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

use sqe_auth::auth_code::AuthCodeService;
use sqe_auth::pending_auth::{PendingAuth, PendingAuthStore};

/// Shared state for OAuth2 endpoints.
pub struct OAuth2State {
    pub auth_code_service: Arc<AuthCodeService>,
    pub pending_store: Arc<PendingAuthStore>,
    pub base_url: String,
}

/// Generate a 401 WWW-Authenticate challenge for Trino external auth.
/// Returns `(auth_id, www_authenticate_header_value)`.
pub async fn generate_challenge(state: &OAuth2State) -> Result<(String, String), StatusCode> {
    let challenge = state.auth_code_service.start_challenge().await.map_err(|e| {
        warn!(error = %e, "Failed to start auth code challenge");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let auth_id = challenge.auth_id.clone();
    let auth_id_hash = hex::encode(Sha256::digest(auth_id.as_bytes()));

    state.pending_store.insert_pending(&auth_id, challenge.code_verifier, challenge.state.clone());

    let initiate_url = format!("{}/oauth2/token/initiate/{}", state.base_url, auth_id_hash);
    let token_url = format!("{}/oauth2/token/{}", state.base_url, auth_id);

    let www_authenticate = format!(
        "Bearer x_redirect_server=\"{initiate_url}\", x_token_server=\"{token_url}\""
    );

    Ok((auth_id, www_authenticate))
}

/// GET /oauth2/token/initiate/{auth_id_hash}
/// Redirects the user's browser to the IdP's authorization endpoint.
pub async fn initiate_handler(
    State(state): State<Arc<OAuth2State>>,
    Path(_auth_id_hash): Path<String>,
) -> Response {
    match state.auth_code_service.start_challenge().await {
        Ok(challenge) => {
            state.pending_store.insert_pending(
                &challenge.auth_id,
                challenge.code_verifier,
                challenge.state,
            );
            Redirect::temporary(&challenge.authorization_url).into_response()
        }
        Err(e) => {
            warn!(error = %e, "Failed to generate authorization URL");
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal error").into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CallbackParams {
    pub code: String,
    pub state: String,
}

/// GET /oauth2/callback?code=...&state=...
/// Receives the IdP's redirect after user authentication.
pub async fn callback_handler(
    State(state): State<Arc<OAuth2State>>,
    Query(params): Query<CallbackParams>,
) -> Response {
    // State parameter IS the auth_id (we control both values).
    let auth_id = params.state.clone();

    let code_verifier = match state.pending_store.poll(&auth_id) {
        Some(PendingAuth::AwaitingCallback { code_verifier, .. }) => code_verifier,
        _ => {
            warn!(state = %params.state, "No pending auth session found for state");
            return (StatusCode::BAD_REQUEST, "Invalid or expired state parameter").into_response();
        }
    };

    match state.auth_code_service.exchange_code(&params.code, &code_verifier).await {
        Ok(tokens) => {
            debug!(auth_id = %auth_id, "Authorization code exchange succeeded");
            state.pending_store.complete(&auth_id, tokens);
            Html(SUCCESS_HTML).into_response()
        }
        Err(e) => {
            warn!(auth_id = %auth_id, error = %e, "Authorization code exchange failed");
            state.pending_store.fail(&auth_id, e.to_string());
            Html(FAILURE_HTML).into_response()
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum TokenPollResponse {
    Pending { #[serde(rename = "nextUri")] next_uri: String },
    Complete { token: String },
    Error { error: String },
}

/// GET /oauth2/token/{auth_id} — polled by the Trino JDBC driver.
pub async fn poll_token_handler(
    State(state): State<Arc<OAuth2State>>,
    Path(auth_id): Path<String>,
) -> Response {
    match state.pending_store.poll(&auth_id) {
        Some(PendingAuth::AwaitingCallback { .. }) => {
            let next_uri = format!("{}/oauth2/token/{}", state.base_url, auth_id);
            Json(TokenPollResponse::Pending { next_uri }).into_response()
        }
        Some(PendingAuth::Complete(tokens)) => {
            Json(TokenPollResponse::Complete { token: tokens.access_token }).into_response()
        }
        Some(PendingAuth::Failed(msg)) => {
            Json(TokenPollResponse::Error { error: msg }).into_response()
        }
        None => (StatusCode::NOT_FOUND, "Auth session not found or expired").into_response(),
    }
}

/// DELETE /oauth2/token/{auth_id} — cleanup after client receives token.
pub async fn delete_token_handler(
    State(state): State<Arc<OAuth2State>>,
    Path(auth_id): Path<String>,
) -> StatusCode {
    state.pending_store.remove(&auth_id);
    StatusCode::NO_CONTENT
}

const SUCCESS_HTML: &str = r#"<!DOCTYPE html>
<html><head><title>SQE — Authentication Complete</title></head>
<body style="font-family:system-ui;text-align:center;padding:60px">
<h2>Authentication successful</h2>
<p>You can close this tab and return to your application.</p>
</body></html>"#;

const FAILURE_HTML: &str = r#"<!DOCTYPE html>
<html><head><title>SQE — Authentication Failed</title></head>
<body style="font-family:system-ui;text-align:center;padding:60px">
<h2>Authentication failed</h2>
<p>Please close this tab and try again.</p>
</body></html>"#;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn token_poll_response_pending_serializes() {
        let resp = TokenPollResponse::Pending {
            next_uri: "https://sqe:8080/oauth2/token/abc".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("nextUri"));
        assert!(json.contains("abc"));
    }

    #[test]
    fn token_poll_response_complete_serializes() {
        let resp = TokenPollResponse::Complete { token: "eyJ...".to_string() };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"token\""));
        assert!(json.contains("eyJ..."));
    }

    #[test]
    fn token_poll_response_error_serializes() {
        let resp = TokenPollResponse::Error { error: "user denied".to_string() };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"error\""));
    }

    #[test]
    fn pending_store_lifecycle() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("test-id", "verifier".to_string(), "state".to_string());

        assert!(matches!(store.poll("test-id"), Some(PendingAuth::AwaitingCallback { .. })));

        store.complete("test-id", sqe_auth::pending_auth::TokenSet {
            access_token: "at".to_string(),
            id_token: None,
            refresh_token: None,
            expires_in: 3600,
        });

        assert!(matches!(store.poll("test-id"), Some(PendingAuth::Complete(_))));

        store.remove("test-id");
        assert!(store.poll("test-id").is_none());
    }
}
