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

/// Store key for a client-held `auth_id`: `hex(sha256(auth_id))`, which is
/// also the OAuth2 `state` the IdP echoes back. The client polls with
/// `auth_id`; the browser only ever sees the hash (initiate path, `state`),
/// so nothing browser-visible can fetch the token (#709). Mirrors Trino's
/// `OAuth2TokenExchange` (store keyed by `hash(authId)`).
fn poll_key(auth_id: &str) -> String {
    hex::encode(Sha256::digest(auth_id.as_bytes()))
}

/// Generate a 401 WWW-Authenticate challenge for Trino external auth.
/// Returns `(auth_id, www_authenticate_header_value)`.
///
/// One pending session, keyed by `challenge.state == poll_key(auth_id)`:
/// `initiate/{state}` redirects it, the callback (`?state=`) completes it, and
/// `token/{auth_id}` polls it. `state` doubles as the anti-CSRF token verified
/// in the callback (RFC 6749 section 10.12).
///
/// `requester_ip` is recorded so `initiate` can show the browser user which
/// client asked for the sign-in.
pub async fn generate_challenge(
    state: &OAuth2State,
    requester_ip: &str,
) -> Result<(String, String), StatusCode> {
    let challenge = state
        .auth_code_service
        .start_challenge()
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to start auth code challenge");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let auth_id = challenge.auth_id;
    let session_key = challenge.state.clone();
    debug_assert_eq!(session_key, poll_key(&auth_id));

    state.pending_store.insert_pending(
        &session_key,
        challenge.code_verifier,
        challenge.state,
        challenge.authorization_url,
        requester_ip.to_string(),
    );

    let initiate_url = format!("{}/oauth2/token/initiate/{}", state.base_url, session_key);
    let token_url = format!("{}/oauth2/token/{}", state.base_url, auth_id);

    let www_authenticate =
        format!("Bearer x_redirect_server=\"{initiate_url}\", x_token_server=\"{token_url}\"");

    Ok((auth_id, www_authenticate))
}

#[derive(Debug, Default, Deserialize)]
pub struct InitiateParams {
    #[serde(default)]
    pub confirm: Option<String>,
}

/// GET /oauth2/token/initiate/{auth_id_hash}
/// First shows an interstitial naming the client address that requested the
/// sign-in; its "continue" button (`?confirm=1`) redirects the browser to the
/// IdP authorization URL of the pending session issued in the 401 challenge.
/// A link someone else minted and sent to the user therefore shows a foreign
/// address before any sign-in happens. Unknown or already-used hash -> 404.
pub async fn initiate_handler(
    State(state): State<Arc<OAuth2State>>,
    Path(auth_id_hash): Path<String>,
    Query(params): Query<InitiateParams>,
) -> Response {
    match state.pending_store.poll(&auth_id_hash) {
        Some(PendingAuth::AwaitingCallback {
            authorization_url,
            requester_ip,
            ..
        }) => {
            if params.confirm.is_some() {
                Redirect::temporary(&authorization_url).into_response()
            } else {
                Html(confirm_html(&requester_ip)).into_response()
            }
        }
        _ => (StatusCode::NOT_FOUND, "Auth session not found or expired").into_response(),
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn confirm_html(requester_ip: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html><head><title>SQE — Confirm Sign-in</title></head>
<body style="font-family:system-ui;text-align:center;padding:60px">
<h2>Sign in to SQE</h2>
<p>A client at <strong>{}</strong> asked to sign in as you.</p>
<p>Continue only if you just started this sign-in yourself.</p>
<form method="get"><input type="hidden" name="confirm" value="1">
<button type="submit">Continue to sign in</button></form>
</body></html>"#,
        html_escape(requester_ip)
    )
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
    store.take_callback_verifier(returned_state)
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
    match state
        .auth_code_service
        .exchange_code(&params.code, &code_verifier)
        .await
    {
        Ok(tokens) => {
            debug!("Authorization code exchange succeeded");
            if state.pending_store.complete(session_key, tokens) {
                Html(SUCCESS_HTML).into_response()
            } else {
                (StatusCode::BAD_REQUEST, "Auth session not found or expired").into_response()
            }
        }
        Err(e) => {
            warn!(error = %e, "Authorization code exchange failed");
            state.pending_store.fail(session_key, e.to_string());
            Html(FAILURE_HTML).into_response()
        }
    }
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum TokenPollResponse {
    Pending {
        #[serde(rename = "nextUri")]
        next_uri: String,
    },
    Complete {
        token: String,
    },
    Error {
        error: String,
    },
}

/// GET /oauth2/token/{auth_id} — polled by the Trino JDBC driver.
/// A completed session is consumed on first read (one-shot), so the token
/// cannot be fetched again even by a party that later learns `auth_id`.
pub async fn poll_token_handler(
    State(state): State<Arc<OAuth2State>>,
    Path(auth_id): Path<String>,
) -> Response {
    let key = poll_key(&auth_id);
    match state.pending_store.poll_and_consume(&key) {
        Some(PendingAuth::AwaitingCallback { .. } | PendingAuth::Exchanging) => {
            let next_uri = format!("{}/oauth2/token/{}", state.base_url, auth_id);
            Json(TokenPollResponse::Pending { next_uri }).into_response()
        }
        Some(PendingAuth::Complete(tokens)) => Json(TokenPollResponse::Complete {
            token: tokens.access_token,
        })
        .into_response(),
        Some(PendingAuth::Failed(msg)) => {
            Json(TokenPollResponse::Error { error: msg }).into_response()
        }
        Some(PendingAuth::Consumed) | None => {
            (StatusCode::NOT_FOUND, "Auth session not found or expired").into_response()
        }
    }
}

/// DELETE /oauth2/token/{auth_id} — cleanup after client receives token.
pub async fn delete_token_handler(
    State(state): State<Arc<OAuth2State>>,
    Path(auth_id): Path<String>,
) -> StatusCode {
    state.pending_store.remove(&poll_key(&auth_id));
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
        let resp = TokenPollResponse::Complete {
            token: "eyJ...".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"token\""));
        assert!(json.contains("eyJ..."));
    }

    #[test]
    fn token_poll_response_error_serializes() {
        let resp = TokenPollResponse::Error {
            error: "user denied".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"error\""));
    }

    #[test]
    fn pending_store_lifecycle() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending(
            "test-id",
            "verifier".to_string(),
            "state".to_string(),
            String::new(),
            String::new(),
        );

        assert!(matches!(
            store.poll("test-id"),
            Some(PendingAuth::AwaitingCallback { .. })
        ));

        store.complete(
            "test-id",
            sqe_auth::pending_auth::TokenSet {
                access_token: "at".to_string(),
                id_token: None,
                refresh_token: None,
                expires_in: 3600,
            },
        );

        assert!(matches!(
            store.poll("test-id"),
            Some(PendingAuth::Complete(_))
        ));

        store.remove("test-id");
        assert!(store.poll("test-id").is_none());
    }

    #[test]
    fn verify_state_accepts_matching_state() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        // Keyed by `state`, with the same `state` stored as the CSRF token.
        store.insert_pending(
            "good-state",
            "verifier-1".to_string(),
            "good-state".to_string(),
            String::new(),
            String::new(),
        );

        let result = verify_state_and_take_verifier(&store, "good-state");
        assert_eq!(result, Ok("verifier-1".to_string()));
    }

    #[test]
    fn verify_state_rejects_forged_state() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        // Lookup key and stored CSRF token deliberately diverge to exercise the
        // mismatch branch: the entry is found by key but its stored `state`
        // does not match the value used to look it up.
        store.insert_pending(
            "lookup-key",
            "verifier-2".to_string(),
            "real-state".to_string(),
            String::new(),
            String::new(),
        );

        let result = verify_state_and_take_verifier(&store, "lookup-key");
        assert!(result.is_err(), "mismatched state must be rejected");
    }

    #[test]
    fn confirm_page_escapes_requester() {
        let html = confirm_html("<script>\"x'&");
        assert!(html.contains("&lt;script&gt;&quot;x&#39;&amp;"));
        assert!(!html.contains("<script>"));
    }

    #[test]
    fn verify_state_rejects_unknown_state() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending(
            "good-state",
            "verifier-3".to_string(),
            "good-state".to_string(),
            String::new(),
            String::new(),
        );

        // An attacker-supplied state that was never issued misses the store.
        let result = verify_state_and_take_verifier(&store, "forged-state");
        assert!(result.is_err(), "unknown state must be rejected");
    }

    /// #709: the browser sees only `state == poll_key(auth_id)`; that value
    /// must serve `initiate` but never the token. The client-held `auth_id`
    /// fetches the token exactly once.
    #[tokio::test]
    async fn browser_visible_state_initiates_but_cannot_poll_and_token_is_one_shot() {
        use sqe_auth::oidc_discovery::{OidcDiscovery, OidcDiscoveryConfig};

        let discovery = Arc::new(
            OidcDiscovery::new(OidcDiscoveryConfig {
                issuer: "http://127.0.0.1:1".to_string(),
                authorization_endpoint_override: None,
                token_endpoint_override: None,
                accept_invalid_certs: false,
            })
            .unwrap(),
        );
        let state = Arc::new(OAuth2State {
            auth_code_service: Arc::new(AuthCodeService::new(
                discovery,
                "cid",
                None,
                "http://sqe/oauth2/callback",
                vec![],
            )),
            pending_store: Arc::new(PendingAuthStore::new(Duration::from_secs(60))),
            base_url: "http://sqe:8080".to_string(),
        });

        let auth_id = "client-only-auth-id";
        let key = poll_key(auth_id);
        state.pending_store.insert_pending(
            &key,
            "verifier".to_string(),
            key.clone(),
            "http://idp/authorize?state=x".to_string(),
            "198.51.100.7".to_string(),
        );
        let confirmed = || InitiateParams {
            confirm: Some("1".to_string()),
        };

        // Without confirmation the browser gets the interstitial naming the
        // requesting client, not the IdP redirect.
        let resp = initiate_handler(
            State(state.clone()),
            Path(key.clone()),
            Query(InitiateParams::default()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("198.51.100.7"));
        assert!(body.contains("Continue to sign in"));
        assert!(!body.contains("idp/authorize"));

        // Browser-visible hash drives the redirect once confirmed...
        let resp = initiate_handler(
            State(state.clone()),
            Path(key.clone()),
            Query(confirmed()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            resp.headers().get("location").unwrap(),
            "http://idp/authorize?state=x"
        );
        // ...and an unknown hash does not mint a fresh session.
        let resp = initiate_handler(
            State(state.clone()),
            Path("nope".to_string()),
            Query(confirmed()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        state.pending_store.complete(
            &key,
            sqe_auth::pending_auth::TokenSet {
                access_token: "at".to_string(),
                id_token: None,
                refresh_token: None,
                expires_in: 3600,
            },
        );

        // The browser-visible value cannot fetch the token.
        let resp = poll_token_handler(State(state.clone()), Path(key.clone())).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // The client's auth_id can, exactly once.
        let resp = poll_token_handler(State(state.clone()), Path(auth_id.to_string())).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = poll_token_handler(State(state.clone()), Path(auth_id.to_string())).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
