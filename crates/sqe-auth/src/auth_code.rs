//! OAuth2 Authorization Code + PKCE flow.
//!
//! This module implements the Authorization Code grant with PKCE (RFC 7636),
//! suitable for interactive user-facing clients (web apps, CLI with browser redirect).
//!
//! # Flow
//! 1. Call [`AuthCodeService::start_challenge`] to generate a PKCE verifier/challenge
//!    pair and build the authorization URL to redirect the user to.
//! 2. Receive the `code` callback from the IdP (via a redirect URI handled externally).
//! 3. Call [`AuthCodeService::exchange_code`] with the `code` and the saved
//!    `code_verifier` from step 1 to obtain a [`TokenSet`].

use std::sync::Arc;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::Rng;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

use crate::oidc_discovery::OidcDiscovery;
use crate::pending_auth::TokenSet;
use crate::provider::AuthError;

// ─── Public types ────────────────────────────────────────────────────────────

/// Holds the PKCE challenge data returned by [`AuthCodeService::start_challenge`].
///
/// The caller is responsible for:
/// - Redirecting the user to `authorization_url`
/// - Persisting `auth_id`, `code_verifier`, and `state` until the callback arrives
///   (e.g. in [`crate::pending_auth::PendingAuthStore`])
#[derive(Debug, Clone)]
pub struct AuthCodeChallenge {
    /// Unique identifier for this auth session (opaque to the IdP).
    pub auth_id: String,
    /// Full URL to redirect the user to for authentication.
    pub authorization_url: String,
    /// The PKCE verifier — **keep secret, never sent to the browser**.
    /// Sent to the token endpoint in [`AuthCodeService::exchange_code`].
    pub code_verifier: String,
    /// Random state value to prevent CSRF.  Must be verified in the callback.
    pub state: String,
}

// ─── Internal serde type ─────────────────────────────────────────────────────

/// Raw token response body from the IdP's token endpoint.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    /// Seconds until the access token expires.  Defaults to 3600 if not present.
    #[serde(default = "default_expires_in")]
    expires_in: u64,
}

fn default_expires_in() -> u64 {
    3600
}

impl From<TokenResponse> for TokenSet {
    fn from(r: TokenResponse) -> Self {
        TokenSet {
            access_token: r.access_token,
            id_token: r.id_token,
            refresh_token: r.refresh_token,
            expires_in: r.expires_in,
        }
    }
}

// ─── Service ─────────────────────────────────────────────────────────────────

/// Service that drives the OAuth2 Authorization Code + PKCE flow.
pub struct AuthCodeService {
    discovery: Arc<OidcDiscovery>,
    client_id: String,
    client_secret: Option<String>,
    redirect_uri: String,
    scopes: Vec<String>,
    http: reqwest::Client,
}

impl AuthCodeService {
    /// Create a new `AuthCodeService`.
    ///
    /// # Parameters
    /// - `discovery` — shared OIDC discovery handle (lazy-fetched, cached)
    /// - `client_id` — OAuth2 client ID registered with the IdP
    /// - `client_secret` — optional client secret (public clients omit this)
    /// - `redirect_uri` — URI the IdP will redirect back to after authentication
    /// - `scopes` — requested OAuth2 scopes (e.g. `["openid", "profile", "email"]`)
    pub fn new(
        discovery: Arc<OidcDiscovery>,
        client_id: impl Into<String>,
        client_secret: Option<String>,
        redirect_uri: impl Into<String>,
        scopes: Vec<String>,
    ) -> Self {
        Self {
            discovery,
            client_id: client_id.into(),
            client_secret,
            redirect_uri: redirect_uri.into(),
            scopes,
            http: reqwest::Client::new(),
        }
    }

    /// Start an Authorization Code + PKCE challenge.
    ///
    /// Returns an [`AuthCodeChallenge`] containing the authorization URL to
    /// redirect the user to, plus the verifier and state values that must be
    /// persisted until the IdP callback is received.
    pub async fn start_challenge(&self) -> Result<AuthCodeChallenge, AuthError> {
        let auth_endpoint = self.discovery.authorization_endpoint().await?;

        let auth_id = generate_random_string(16);
        let code_verifier = generate_code_verifier();
        let code_challenge = compute_code_challenge(&code_verifier);
        let state = generate_random_string(32);

        let scope = self.scopes.join(" ");

        // Build the authorization URL query string manually so we control
        // percent-encoding and avoid pulling in a heavyweight OAuth crate.
        let params: Vec<(&str, String)> = vec![
            ("response_type", "code".to_string()),
            ("client_id", self.client_id.clone()),
            ("redirect_uri", self.redirect_uri.clone()),
            ("scope", scope),
            ("state", state.clone()),
            ("code_challenge", code_challenge),
            ("code_challenge_method", "S256".to_string()),
        ];

        let query_string: String = params
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, urlencoding(&v)))
            .collect::<Vec<_>>()
            .join("&");

        let authorization_url = format!("{}?{}", auth_endpoint, query_string);

        debug!(
            auth_id = %auth_id,
            authorization_url = %authorization_url,
            "Starting Authorization Code + PKCE challenge"
        );

        Ok(AuthCodeChallenge {
            auth_id,
            authorization_url,
            code_verifier,
            state,
        })
    }

    /// Exchange an authorization `code` for a [`TokenSet`].
    ///
    /// # Parameters
    /// - `code` — the authorization code received from the IdP callback
    /// - `code_verifier` — the PKCE verifier generated in [`Self::start_challenge`]
    pub async fn exchange_code(
        &self,
        code: &str,
        code_verifier: &str,
    ) -> Result<TokenSet, AuthError> {
        let token_endpoint = self.discovery.token_endpoint().await?;

        let mut params = vec![
            ("grant_type", "authorization_code"),
            ("client_id", &self.client_id),
            ("redirect_uri", &self.redirect_uri),
            ("code", code),
            ("code_verifier", code_verifier),
        ];

        // Confidential clients include client_secret; public clients do not.
        let secret_ref;
        if let Some(ref secret) = self.client_secret {
            secret_ref = secret.clone();
            params.push(("client_secret", &secret_ref));
        }

        debug!(token_endpoint = %token_endpoint, "Exchanging authorization code for tokens");

        let response = self
            .http
            .post(token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| AuthError::Internal(anyhow::anyhow!("token request failed: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            warn!(status = %status, body = %body, "Token endpoint rejected auth code exchange");
            return Err(AuthError::AuthFailed("Authentication failed".to_string()));
        }

        let token_response: TokenResponse = response.json().await.map_err(|e| {
            AuthError::Internal(anyhow::anyhow!("failed to parse token response: {e}"))
        })?;

        Ok(token_response.into())
    }
}

// ─── Private helpers ─────────────────────────────────────────────────────────

/// Generate a PKCE `code_verifier`: 32 random bytes, base64url-encoded (43 chars).
///
/// Per RFC 7636 §4.1: the verifier must be 43–128 unreserved ASCII characters.
fn generate_code_verifier() -> String {
    let bytes: Vec<u8> = (0..32).map(|_| rand::thread_rng().gen::<u8>()).collect();
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Compute the PKCE `code_challenge` using S256 (SHA-256 of verifier, base64url-encoded).
///
/// Per RFC 7636 §4.2: `code_challenge = BASE64URL(SHA256(ASCII(code_verifier)))`.
fn compute_code_challenge(verifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    let digest = hasher.finalize();
    URL_SAFE_NO_PAD.encode(digest)
}

/// Generate a random alphanumeric string of `len` characters.
fn generate_random_string(len: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..len)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Percent-encode a string suitable for use as a query parameter value.
///
/// Uses `url::form_urlencoded` so spaces become `+` and special chars are
/// percent-encoded (application/x-www-form-urlencoded encoding).
fn urlencoding(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_verifier_is_correct_length() {
        let verifier = generate_code_verifier();
        // 32 bytes → 43 base64url chars (no padding)
        assert_eq!(
            verifier.len(),
            43,
            "expected 43 chars, got {}",
            verifier.len()
        );
        // Only unreserved chars: A-Z a-z 0-9 - _ . ~
        // base64url (no pad) uses A-Z a-z 0-9 + - _
        assert!(
            verifier
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_'),
            "verifier contains reserved characters: {verifier}"
        );
    }

    #[test]
    fn code_challenge_is_s256() {
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let challenge = compute_code_challenge(verifier);

        // Manually compute expected: SHA-256 of verifier bytes, base64url-encoded
        let mut hasher = Sha256::new();
        hasher.update(verifier.as_bytes());
        let digest = hasher.finalize();
        let expected = URL_SAFE_NO_PAD.encode(digest);

        assert_eq!(challenge, expected);
        // Must only contain base64url chars
        assert!(
            challenge
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_'),
            "challenge contains invalid characters: {challenge}"
        );
    }

    #[test]
    fn code_challenge_deterministic() {
        let verifier = "some-fixed-verifier-value-for-testing";
        let c1 = compute_code_challenge(verifier);
        let c2 = compute_code_challenge(verifier);
        assert_eq!(c1, c2, "code_challenge must be deterministic");
    }

    #[test]
    fn random_string_correct_length() {
        for len in [8, 16, 32, 64] {
            let s = generate_random_string(len);
            assert_eq!(s.len(), len, "expected length {len}, got {}", s.len());
            assert!(
                s.chars().all(|c| c.is_alphanumeric()),
                "random string contains non-alphanumeric char: {s}"
            );
        }
    }

    #[test]
    fn random_string_not_constant() {
        // With 62^16 possible values the probability of collision is negligible.
        let s1 = generate_random_string(16);
        let s2 = generate_random_string(16);
        assert_ne!(s1, s2, "two random strings should almost certainly differ");
    }

    #[test]
    fn urlencoding_spaces_and_special_chars() {
        let encoded = urlencoding("hello world");
        assert!(
            encoded.contains('+') || encoded.contains("%20"),
            "space not encoded: {encoded}"
        );

        let encoded_special = urlencoding("a=b&c=d");
        assert!(
            !encoded_special.contains('=')
                || !encoded_special.contains('&')
                || encoded_special.contains("%3D")
                || encoded_special.contains("%26"),
            "special chars not encoded: {encoded_special}"
        );
    }

    #[test]
    fn parse_token_response() {
        let json = serde_json::json!({
            "access_token": "access-tok",
            "id_token": "id-tok",
            "refresh_token": "refresh-tok",
            "expires_in": 7200
        });
        let resp: TokenResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.access_token, "access-tok");
        assert_eq!(resp.id_token.as_deref(), Some("id-tok"));
        assert_eq!(resp.refresh_token.as_deref(), Some("refresh-tok"));
        assert_eq!(resp.expires_in, 7200);
    }

    #[test]
    fn parse_token_response_minimal() {
        let json = serde_json::json!({
            "access_token": "at-only"
        });
        let resp: TokenResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.access_token, "at-only");
        assert!(resp.id_token.is_none());
        assert!(resp.refresh_token.is_none());
        // Default expires_in applied
        assert_eq!(resp.expires_in, 3600);
    }
}
