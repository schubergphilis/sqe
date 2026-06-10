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
use sqe_core::SecretString;

/// Shared state for OAuth2 endpoints.
pub struct OAuth2State {
    pub auth_code_service: Arc<AuthCodeService>,
    pub pending_store: Arc<PendingAuthStore>,
    pub base_url: String,
}

/// Generate a 401 WWW-Authenticate challenge for Trino external auth.
/// Returns `(session_key, www_authenticate_header_value)`.
///
/// The pending session is keyed by the OAuth2 `state` value. The IdP echoes
/// only `state` (and `code`) back to the callback, so `state` is the single
/// high-entropy value that ties the issued request to its callback, serves as
/// the token-poll id, and acts as the anti-CSRF token verified in the callback
/// (RFC 6749 section 10.12).
pub async fn generate_challenge(state: &OAuth2State) -> Result<(String, String), StatusCode> {
    let challenge = state.auth_code_service.start_challenge().await.map_err(|e| {
        warn!(error = %e, "Failed to start auth code challenge");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let session_key = challenge.state.clone();
    let session_key_hash = hex::encode(Sha256::digest(session_key.as_bytes()));

    state
        .pending_store
        .insert_pending(&session_key, challenge.code_verifier, challenge.state);

    let initiate_url = format!("{}/oauth2/token/initiate/{}", state.base_url, session_key_hash);
    let token_url = format!("{}/oauth2/token/{}", state.base_url, session_key);

    let www_authenticate = format!(
        "Bearer x_redirect_server=\"{initiate_url}\", x_token_server=\"{token_url}\""
    );

    Ok((session_key, www_authenticate))
}

/// GET /oauth2/token/initiate/{auth_id_hash}
/// Redirects the user's browser to the IdP's authorization endpoint.
pub async fn initiate_handler(
    State(state): State<Arc<OAuth2State>>,
    Path(_auth_id_hash): Path<String>,
) -> Response {
    match state.auth_code_service.start_challenge().await {
        Ok(challenge) => {
            // Key by `state` so the callback can both locate and CSRF-verify the
            // session (the IdP echoes back only `state`).
            state.pending_store.insert_pending(
                &challenge.state.clone(),
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

/// Look up the pending session for an IdP-returned `state` and verify it.
///
/// The session is keyed by `state`, so an unknown or expired `state` simply
/// misses the store. When a session is found we still constant-time-compare the
/// returned `state` against the `state` stored when the flow began: this is the
/// anti-CSRF binding required by RFC 6749 section 10.12, and the explicit check
/// keeps the verification correct even if the keying scheme ever changes. On
/// success the `code_verifier` is returned for the token exchange; any
/// mismatch, missing session, or non-`AwaitingCallback` state is rejected.
fn verify_state_and_take_verifier(
    store: &PendingAuthStore,
    returned_state: &str,
) -> Result<String, &'static str> {
    match store.poll(returned_state) {
        Some(PendingAuth::AwaitingCallback { code_verifier, state, .. }) => {
            let stored = SecretString::new(state);
            let returned = SecretString::new(returned_state.to_string());
            if stored.ct_eq(&returned) {
                Ok(code_verifier)
            } else {
                Err("State parameter does not match the stored value")
            }
        }
        _ => Err("Invalid or expired state parameter"),
    }
}

/// GET /oauth2/callback?code=...&state=...
/// Receives the IdP's redirect after user authentication.
pub async fn callback_handler(
    State(state): State<Arc<OAuth2State>>,
    Query(params): Query<CallbackParams>,
) -> Response {
    // The session is keyed by `state`, which is also the anti-CSRF token. Verify
    // the IdP-returned `state` against the value stored when the flow started
    // before exchanging the code (RFC 6749 section 10.12). `state` and the
    // earlier `auth_id` are distinct values; we standardize on `state`.
    let code_verifier = match verify_state_and_take_verifier(&state.pending_store, &params.state) {
        Ok(verifier) => verifier,
        Err(reason) => {
            warn!(reason = %reason, "Rejected OAuth2 callback: state verification failed");
            return (StatusCode::BAD_REQUEST, reason).into_response();
        }
    };

    let session_key = &params.state;
    match state.auth_code_service.exchange_code(&params.code, &code_verifier).await {
        Ok(tokens) => {
            debug!("Authorization code exchange succeeded");
            state.pending_store.complete(session_key, tokens);
            Html(SUCCESS_HTML).into_response()
        }
        Err(e) => {
            warn!(error = %e, "Authorization code exchange failed");
            state.pending_store.fail(session_key, e.to_string());
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

    #[test]
    fn verify_state_accepts_matching_state() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        // Keyed by `state`, with the same `state` stored as the CSRF token.
        store.insert_pending("good-state", "verifier-1".to_string(), "good-state".to_string());

        let result = verify_state_and_take_verifier(&store, "good-state");
        assert_eq!(result, Ok("verifier-1".to_string()));
    }

    #[test]
    fn verify_state_rejects_forged_state() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        // Lookup key and stored CSRF token deliberately diverge to exercise the
        // mismatch branch: the entry is found by key but its stored `state`
        // does not match the value used to look it up.
        store.insert_pending("lookup-key", "verifier-2".to_string(), "real-state".to_string());

        let result = verify_state_and_take_verifier(&store, "lookup-key");
        assert!(result.is_err(), "mismatched state must be rejected");
    }

    #[test]
    fn verify_state_rejects_unknown_state() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("good-state", "verifier-3".to_string(), "good-state".to_string());

        // An attacker-supplied state that was never issued misses the store.
        let result = verify_state_and_take_verifier(&store, "forged-state");
        assert!(result.is_err(), "unknown state must be rejected");
    }
}
