// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use gcp_auth::TokenProvider;
use http::StatusCode;
use iceberg::{Error, ErrorKind, Result};
use reqwest::header::HeaderMap;
use reqwest::{Client, IntoUrl, Method, Request, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::types::{ErrorResponse, TokenResponse};
use crate::{GCP_CLOUD_PLATFORM_SCOPE, RestCatalogConfig};

pub(crate) struct HttpClient {
    client: Client,

    /// The token to be used for authentication.
    ///
    /// It's possible to fetch the token from the server while needed.
    token: Mutex<Option<String>>,
    /// The token endpoint to be used for authentication.
    token_endpoint: String,
    /// The credential to be used for authentication.
    credential: Option<(Option<String>, String)>,
    /// Extra headers to be added to each request.
    extra_headers: HeaderMap,
    /// Extra oauth parameters to be added to each authentication request.
    extra_oauth_params: HashMap<String, String>,
    /// Whether to disable header redaction in error logs (defaults to false for security).
    disable_header_redaction: bool,
    /// GCP service account JSON for authentication.
    gcp_credential: Option<String>,
    /// AWS SigV4 signer. `Some` when `rest.sigv4-enabled=true` was
    /// resolved from either the user config or the server's
    /// `/v1/config` defaults; in that case the OAuth/Bearer path is
    /// bypassed and outgoing requests are signed with SigV4 instead.
    #[cfg(feature = "aws-sigv4")]
    sigv4_signer: Option<crate::sigv4::SigV4Signer>,
    /// Whether this client was constructed with *any* form of
    /// authentication (token, credential, or GCP service account).
    /// When true, [`authenticate`] refuses to send a request with no
    /// `Authorization` header even if the live token has been cleared
    /// out from under it. This catches the regression in
    /// <https://sbp.gitlab.schubergphilis.com/.../sqlengine/-/issues/2>
    /// where some outbound REST calls were arriving at Polaris without
    /// the bearer header under concurrency.
    auth_required: bool,
}

impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("client", &self.client)
            .field("extra_headers", &self.extra_headers)
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Create a new http client.
    pub fn new(cfg: &RestCatalogConfig) -> Result<Self> {
        let token = cfg.token();
        let credential = cfg.credential();
        let gcp_credential = cfg.gcp_credential();
        // Track whether the caller intends this client to be
        // authenticated. Used by [`authenticate`] to refuse silent
        // unauthenticated requests if the live token is later lost.
        let auth_required =
            token.as_deref().is_some_and(|t| !t.is_empty())
                || credential.is_some()
                || gcp_credential.is_some();
        Ok(HttpClient {
            client: cfg.client().unwrap_or_default(),
            token: Mutex::new(token),
            token_endpoint: cfg.get_token_endpoint(),
            credential,
            extra_headers: cfg.extra_headers()?,
            extra_oauth_params: cfg.extra_oauth_params(),
            disable_header_redaction: cfg.disable_header_redaction(),
            gcp_credential,
            #[cfg(feature = "aws-sigv4")]
            sigv4_signer: build_sigv4_signer(cfg),
            auth_required,
        })
    }

    /// Update the http client with new configuration.
    ///
    /// If cfg carries new value, we will use cfg instead.
    /// Otherwise, we will keep the old value.
    pub fn update_with(self, cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = (!cfg.extra_headers()?.is_empty())
            .then(|| cfg.extra_headers())
            .transpose()?
            .unwrap_or(self.extra_headers);

        // Preserve the original auth-required flag across updates.
        // The merge in [`RestCatalogConfig::merge_with_config`] folds
        // in the server's `/v1/config` response; if a server-side
        // override silently clobbers the user-supplied token (e.g.
        // overrides "token" -> "" or omits it entirely while the user
        // had one), the live `cfg` would resolve to no auth. Without
        // this flag we'd construct a client that quietly drops the
        // bearer on every outbound request and 401s downstream.
        let merged_token = cfg.token().or_else(|| self.token.into_inner());
        let merged_credential = cfg.credential().or(self.credential);
        let merged_gcp = cfg.gcp_credential().or(self.gcp_credential);
        let auth_required = self.auth_required
            || merged_token.as_deref().is_some_and(|t| !t.is_empty())
            || merged_credential.is_some()
            || merged_gcp.is_some();

        Ok(HttpClient {
            client: cfg.client().unwrap_or(self.client),
            token: Mutex::new(merged_token),
            token_endpoint: if !cfg.get_token_endpoint().is_empty() {
                cfg.get_token_endpoint()
            } else {
                self.token_endpoint
            },
            credential: merged_credential,
            extra_headers,
            extra_oauth_params: if !cfg.extra_oauth_params().is_empty() {
                cfg.extra_oauth_params()
            } else {
                self.extra_oauth_params
            },
            disable_header_redaction: cfg.disable_header_redaction(),
            gcp_credential: merged_gcp,
            // The user config doesn't carry SigV4 props on the first
            // pass; the AWS endpoint advertises them in its
            // `/v1/config` response, which lands here through
            // `merge_with_config`. Rebuild the signer once we have
            // the merged props.
            #[cfg(feature = "aws-sigv4")]
            sigv4_signer: build_sigv4_signer(cfg).or(self.sigv4_signer),
            auth_required,
        })
    }

    /// This API is testing only to assert the token.
    #[cfg(test)]
    pub(crate) async fn token(&self) -> Option<String> {
        let mut req = self
            .request(Method::GET, &self.token_endpoint)
            .build()
            .unwrap();
        self.authenticate(&mut req).await.ok();
        self.token.lock().await.clone()
    }

    async fn exchange_credential_for_token(&self) -> Result<String> {
        // Credential must exist here.
        let (client_id, client_secret) = self.credential.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Credential must be provided for authentication",
            )
        })?;

        let mut params = HashMap::with_capacity(4);
        params.insert("grant_type", "client_credentials");
        if let Some(client_id) = client_id {
            params.insert("client_id", client_id);
        }
        params.insert("client_secret", client_secret);
        params.extend(
            self.extra_oauth_params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );

        let mut auth_req = self
            .request(Method::POST, &self.token_endpoint)
            .form(&params)
            .build()?;
        // extra headers add content-type application/json header it's necessary to override it with proper type
        // note that form call doesn't add content-type header if already present
        auth_req.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let auth_url = auth_req.url().clone();
        let auth_resp = self.client.execute(auth_req).await?;

        let auth_res: TokenResponse = if auth_resp.status() == StatusCode::OK {
            let text = auth_resp
                .bytes()
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;
            Ok(serde_json::from_slice(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("operation", "auth")
                .with_context("url", auth_url.to_string())
                .with_context("json", String::from_utf8_lossy(&text))
                .with_source(e)
            })?)
        } else {
            let code = auth_resp.status();
            let text = auth_resp
                .bytes()
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;
            let e: ErrorResponse = serde_json::from_slice(&text).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Received unexpected response")
                    .with_context("code", code.to_string())
                    .with_context("operation", "auth")
                    .with_context("url", auth_url.to_string())
                    .with_context("json", String::from_utf8_lossy(&text))
                    .with_source(e)
            })?;
            Err(Error::from(e))
        }?;
        Ok(auth_res.access_token)
    }

    /// Exchange GCP service account for access token using gcp_auth library.
    async fn exchange_gcp_credential_for_token(&self) -> Result<String> {
        let service_account_json = self.gcp_credential.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "GCP service account must be provided for authentication",
            )
        })?;

        // Use gcp_auth library to handle authentication
        let service_account = gcp_auth::CustomServiceAccount::from_json(service_account_json)
            .map_err(|e| {
                Error::new(ErrorKind::DataInvalid, "Invalid GCP service account JSON")
                    .with_source(e)
            })?;

        // Get access token with cloud-platform scope
        let scopes = &[GCP_CLOUD_PLATFORM_SCOPE];
        let token = service_account.token(scopes).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Failed to get GCP access token").with_source(e)
        })?;

        Ok(token.as_str().to_string())
    }

    /// Invalidate the current token without generating a new one. On the next request, the client
    /// will attempt to generate a new token.
    pub(crate) async fn invalidate_token(&self) -> Result<()> {
        *self.token.lock().await = None;
        Ok(())
    }

    /// Invalidate the current token and set a new one. Generates a new token before invalidating
    /// the current token, meaning the old token will be used until this function acquires the lock
    /// and overwrites the token.
    ///
    /// If credential is invalid, or the request fails, this method will return an error and leave
    /// the current token unchanged.
    pub(crate) async fn regenerate_token(&self) -> Result<()> {
        let new_token = self.exchange_credential_for_token().await?;
        *self.token.lock().await = Some(new_token.clone());
        Ok(())
    }

    /// Authenticates the request by adding a bearer token to the authorization header.
    ///
    /// This method supports four authentication modes:
    ///
    /// 1. **No authentication** - Skip authentication when no credentials are configured.
    /// 2. **Token authentication** - Use the provided `token` directly for authentication.
    /// 3. **OAuth authentication** - Exchange `credential` for a token, cache it, then use it for authentication.
    /// 4. **GCP Service Account** - Use GCP service account to get access token.
    ///
    /// When both `credential` and `token` are present, `token` takes precedence.
    /// When GCP service account is present, it takes precedence over credential-based auth.
    ///
    /// # TODO: Support automatic token refreshing.
    async fn authenticate(&self, req: &mut Request) -> Result<()> {
        // AWS SigV4 path: short-circuits the OAuth/Bearer flow entirely
        // because AWS's REST endpoints reject Bearer headers and need
        // the request to be signed with the resolved AWS credentials.
        // Triggered by `rest.sigv4-enabled=true` in either user or
        // server-supplied config.
        #[cfg(feature = "aws-sigv4")]
        if let Some(signer) = self.sigv4_signer.as_ref() {
            return signer.sign(req).await;
        }

        // Clone the token from lock without holding the lock for entire function.
        let token = self.token.lock().await.clone();

        if self.credential.is_none() && token.is_none() && self.gcp_credential.is_none() {
            if self.auth_required {
                // Defensive guard for issue #2. If this client was
                // constructed with a token / credential / GCP creds
                // (auth_required = true) but the live state has none,
                // the bearer was lost somewhere between session setup
                // and the outbound request. Refuse to send the
                // request unauthenticated; the catch downstream
                // (Polaris 401, mistranslated to TABLE_NOT_FOUND in
                // older builds) is exactly what we want to avoid.
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Refusing to send outbound REST request without \
                         Authorization: client was configured with \
                         authentication but the live token has been \
                         cleared. Method={} path={}",
                        req.method(),
                        req.url().path(),
                    ),
                ));
            }
            // Genuinely-unauthenticated path: the endpoint permits
            // anonymous access. Still log so an operator can correlate
            // against the server-side access log if a 401 surfaces.
            // Path only, never the full URL (would leak names of
            // catalogs/tables on the wire).
            warn!(
                method = %req.method(),
                path = req.url().path(),
                "Outbound REST request issued WITHOUT Authorization \
                 (no token, no credential, no GCP creds). If this is \
                 unexpected, the bearer was lost between session setup \
                 and outbound request."
            );
            return Ok(());
        }

        // Either use the provided token or exchange credential for token, cache and use that
        let token = match token {
            Some(token) => token,
            None => {
                let token = if self.gcp_credential.is_some() {
                    self.exchange_gcp_credential_for_token().await?
                } else {
                    self.exchange_credential_for_token().await?
                };
                // Update token so that we use it for next request instead of
                // exchanging credential for token from the server again
                *self.token.lock().await = Some(token.clone());
                token
            }
        };

        // Defensive guard: a token of length 0 must never go on the
        // wire as `Authorization: Bearer ` (empty bearer). That fails
        // 401 at the server with no useful diagnostic and used to be
        // mistranslated to TABLE_NOT_FOUND downstream.
        if token.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Refusing to send empty bearer token: catalog auth is \
                 misconfigured (token resolved to empty string).",
            ));
        }

        // Insert token in request.
        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Invalid token received from catalog server!",
                )
                .with_source(e)
            })?,
        );

        // Diagnostic trace at the moment the request gains its
        // Authorization header. Records token length only, never the
        // value, never any byte that could be reversed back into the
        // token. Combined with the warn! above, the SQE log alone
        // can answer "did this outbound call have a bearer?" without
        // cross-referencing the catalog's access log.
        debug!(
            method = %req.method(),
            path = req.url().path(),
            token_len = token.len(),
            "Outbound REST request authenticated with Bearer token"
        );

        Ok(())
    }

    #[inline]
    pub fn request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        self.client
            .request(method, url)
            .headers(self.extra_headers.clone())
    }

    /// Executes the given `Request` and returns a `Response`.
    pub async fn execute(&self, mut request: Request) -> Result<Response> {
        request.headers_mut().extend(self.extra_headers.clone());
        Ok(self.client.execute(request).await?)
    }

    // Queries the Iceberg REST catalog after authentication with the given `Request` and
    // returns a `Response`.
    pub async fn query_catalog(&self, mut request: Request) -> Result<Response> {
        self.authenticate(&mut request).await?;
        self.execute(request).await
    }

    /// Returns whether header redaction is disabled for this client.
    pub(crate) fn disable_header_redaction(&self) -> bool {
        self.disable_header_redaction
    }
}

/// Deserializes a catalog response into the given [`DeserializedOwned`] type.
///
/// Returns an error if unable to parse the response bytes.
pub(crate) async fn deserialize_catalog_response<R: DeserializeOwned>(
    response: Response,
) -> Result<R> {
    let bytes = response.bytes().await?;

    serde_json::from_slice::<R>(&bytes).map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to parse response from rest catalog server",
        )
        .with_context("json", String::from_utf8_lossy(&bytes))
        .with_source(e)
    })
}

/// Headers that contain sensitive information and should be excluded from logs.
const SENSITIVE_HEADERS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "set-cookie",
    "cookie",
    "x-api-key",
    "x-auth-token",
];

/// Returns true if the header name is considered sensitive.
fn is_sensitive_header(name: &str) -> bool {
    let name_lower = name.to_lowercase();
    SENSITIVE_HEADERS.iter().any(|h| name_lower == *h)
}

/// Redacts sensitive headers and returns a debug-formatted string.
///
/// If `disable_redaction` is true, returns all headers without redaction.
/// Otherwise, replaces sensitive header values with "[REDACTED]".
fn format_headers_redacted(headers: &HeaderMap, disable_redaction: bool) -> String {
    if disable_redaction {
        // Return all headers as-is without redaction
        let all: HashMap<&str, &str> = headers
            .iter()
            .filter_map(|(name, value)| value.to_str().ok().map(|v| (name.as_str(), v)))
            .collect();
        return format!("{all:?}");
    }

    // Redact sensitive headers by replacing their values with "[REDACTED]"
    let redacted: HashMap<&str, &str> = headers
        .iter()
        .filter_map(|(name, value)| {
            if is_sensitive_header(name.as_str()) {
                Some((name.as_str(), "[REDACTED]"))
            } else {
                value.to_str().ok().map(|v| (name.as_str(), v))
            }
        })
        .collect();
    format!("{redacted:?}")
}

/// Build a SigV4 signer from the merged catalog config when the
/// server (or the user) advertises `rest.sigv4-enabled=true`.
///
/// Returns `None` when SigV4 is off or when the required signing-name
/// or signing-region props are missing. The signer caches the AWS
/// credentials provider chain on first sign call, so building it is
/// cheap.
#[cfg(feature = "aws-sigv4")]
fn build_sigv4_signer(cfg: &RestCatalogConfig) -> Option<crate::sigv4::SigV4Signer> {
    if !cfg.sigv4_enabled() {
        return None;
    }
    let region = cfg.signing_region()?;
    let name = cfg.signing_name()?;
    Some(crate::sigv4::SigV4Signer::new(region, name))
}

/// Deserializes a unexpected catalog response into an error.
pub(crate) async fn deserialize_unexpected_catalog_error(
    response: Response,
    disable_header_redaction: bool,
) -> Error {
    let err = Error::new(
        ErrorKind::Unexpected,
        "Received response with unexpected status code",
    )
    .with_context("status", response.status().to_string())
    .with_context(
        "headers",
        format_headers_redacted(response.headers(), disable_header_redaction),
    );

    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => return err.into(),
    };

    if bytes.is_empty() {
        return err;
    }
    err.with_context("json", String::from_utf8_lossy(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_headers_redacted_empty() {
        let headers = HeaderMap::new();
        let result = format_headers_redacted(&headers, false);
        assert_eq!(result, "{}");
    }

    #[test]
    fn test_format_headers_redacted_non_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("x-request-id", "abc123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
        assert!(result.contains("x-request-id"));
        assert!(result.contains("abc123"));
    }

    #[test]
    fn test_format_headers_redacted_filters_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header should be present but with redacted value
        assert!(result.contains("authorization"));
        assert!(result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("secret-token"));
        // Non-sensitive header should be present with actual value
        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
    }

    #[test]
    fn test_format_headers_redacted_filters_set_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "set-cookie",
            "CF_Authorization=sensitive-session-token; Path=/; Secure;"
                .parse()
                .unwrap(),
        );
        headers.insert("server", "cloudflare".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header should be present but with redacted value
        assert!(result.contains("set-cookie"));
        assert!(result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("sensitive-session-token"));
        // Non-sensitive header should be present with actual value
        assert!(result.contains("server"));
        assert!(result.contains("cloudflare"));
    }

    #[test]
    fn test_format_headers_redacted_filters_all_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token".parse().unwrap());
        headers.insert("proxy-authorization", "Basic creds".parse().unwrap());
        headers.insert("set-cookie", "session=abc".parse().unwrap());
        headers.insert("cookie", "session=abc".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("x-auth-token", "auth-token-456".parse().unwrap());
        headers.insert("x-request-id", "req-123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // All sensitive headers should be present but with redacted values
        assert!(result.contains("authorization"));
        assert!(result.contains("proxy-authorization"));
        assert!(result.contains("set-cookie"));
        assert!(result.contains("cookie"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("x-auth-token"));
        assert!(result.contains("[REDACTED]"));

        // Ensure no sensitive values leaked
        assert!(!result.contains("Bearer token"));
        assert!(!result.contains("Basic creds"));
        assert!(!result.contains("session=abc"));
        assert!(!result.contains("api-key-123"));
        assert!(!result.contains("auth-token-456"));

        // Non-sensitive header should be present with actual value
        assert!(result.contains("x-request-id"));
        assert!(result.contains("req-123"));
    }

    #[test]
    fn test_format_headers_with_redaction_disabled() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, true);

        // When redaction is disabled, all headers and values should be present
        assert!(result.contains("authorization"));
        assert!(result.contains("Bearer secret-token"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("api-key-123"));
        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
        // [REDACTED] should NOT be present when redaction is disabled
        assert!(!result.contains("[REDACTED]"));
    }
}
