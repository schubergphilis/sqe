use std::fmt;

use async_trait::async_trait;

/// Raw credentials extracted from a Flight SQL handshake request.
///
/// Different auth providers inspect different fields:
/// - `OidcPasswordProvider` uses `username` + `password`
/// - `BearerTokenProvider` uses `bearer_token` (or `password` if it looks like a JWT)
/// - `ApiKeyProvider` uses `password` (if it matches the key prefix)
/// - `MtlsProvider` uses `client_cert_cn`
/// - `AnonymousProvider` accepts anything
///
/// `Debug` is hand-implemented to print only field presence for the secret
/// fields (`password`, `bearer_token`) — see issue #16. A panic handler or
/// `anyhow!` chain printing this struct with `{:?}` must never leak the
/// caller's password or bearer token.
#[derive(Clone, Default)]
pub struct FlightCredentials {
    pub username: Option<String>,
    pub password: Option<String>,
    pub bearer_token: Option<String>,
    pub client_cert_cn: Option<String>,
}

impl fmt::Debug for FlightCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlightCredentials")
            .field("username", &self.username)
            .field("password", &redacted_presence(&self.password))
            .field("bearer_token", &redacted_presence(&self.bearer_token))
            .field("client_cert_cn", &self.client_cert_cn)
            .finish()
    }
}

/// The authenticated identity produced by a successful `AuthProvider::authenticate` call.
///
/// Carried in the `Session` for the lifetime of the connection. The `catalog_token`
/// is forwarded to the Polaris REST catalog (and S3) so every request runs as the
/// authenticated user.
///
/// `Debug` is hand-implemented so `catalog_token` and `refresh_token` print as
/// presence sentinels (`<set>` / `None`) rather than the raw value (issue #16).
#[derive(Clone)]
pub struct Identity {
    pub user_id: String,
    pub display_name: String,
    pub roles: Vec<String>,
    pub catalog_token: Option<String>,
    /// Refresh token for obtaining new access tokens without re-authentication.
    /// Only populated by providers that support token refresh (e.g. OIDC password grant).
    pub refresh_token: Option<String>,
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity")
            .field("user_id", &self.user_id)
            .field("display_name", &self.display_name)
            .field("roles", &self.roles)
            .field("catalog_token", &redacted_presence(&self.catalog_token))
            .field("refresh_token", &redacted_presence(&self.refresh_token))
            .finish()
    }
}

/// Render an `Option<String>` as either `"<set>"` (any non-empty value) or
/// `"None"`. Used by hand-written `Debug` impls on secret-bearing structs so
/// that printing the struct via `{:?}` never reveals the underlying bytes.
fn redacted_presence(value: &Option<String>) -> &'static str {
    match value {
        Some(s) if !s.is_empty() => "<set>",
        _ => "None",
    }
}

/// Errors returned by `AuthProvider::authenticate`.
///
/// The three variants drive the `AuthChain` control flow:
/// - `NotMyCredentials` — this provider does not handle this credential type; try the next one.
/// - `AuthFailed` — definitive rejection; stop the chain immediately.
/// - `Internal` — unexpected error; stop the chain immediately.
#[derive(Debug)]
pub enum AuthError {
    /// This provider does not handle the given credential type.
    /// The chain should try the next provider.
    NotMyCredentials,
    /// Authentication was attempted but failed definitively (wrong password, revoked key, etc.).
    /// The chain should stop immediately.
    AuthFailed(String),
    /// An unexpected internal error occurred (network failure, config issue, etc.).
    Internal(anyhow::Error),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::NotMyCredentials => write!(f, "credentials not handled by this provider"),
            AuthError::AuthFailed(msg) => write!(f, "authentication failed: {msg}"),
            AuthError::Internal(err) => write!(f, "internal auth error: {err}"),
        }
    }
}

impl std::error::Error for AuthError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AuthError::Internal(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

/// Pluggable authentication provider.
///
/// Implementations validate raw `FlightCredentials` and produce an `Identity`
/// on success. The `AuthChain` tries providers in order until one succeeds or
/// definitively rejects the request.
#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Attempt to authenticate from raw Flight credentials.
    ///
    /// Returns:
    /// - `Ok(Identity)` on success
    /// - `Err(AuthError::NotMyCredentials)` if this provider does not handle this credential type
    /// - `Err(AuthError::AuthFailed)` on a definitive rejection
    /// - `Err(AuthError::Internal)` on unexpected errors
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError>;

    /// Optionally return a fresh catalog token for an existing identity.
    ///
    /// Called by `SessionManager` before catalog requests to ensure the token
    /// is still valid. Providers that issue short-lived tokens (e.g. OIDC with
    /// refresh tokens) should override this. The default implementation returns
    /// `Ok(None)`, meaning no refresh is needed.
    async fn refresh_catalog_token(
        &self,
        _identity: &Identity,
    ) -> Result<Option<String>, AuthError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_error_display_not_my_credentials() {
        let err = AuthError::NotMyCredentials;
        assert_eq!(err.to_string(), "credentials not handled by this provider");
    }

    #[test]
    fn auth_error_display_auth_failed() {
        let err = AuthError::AuthFailed("bad password".to_string());
        assert_eq!(err.to_string(), "authentication failed: bad password");
    }

    #[test]
    fn auth_error_display_internal() {
        let err = AuthError::Internal(anyhow::anyhow!("connection refused"));
        assert!(err.to_string().contains("connection refused"));
    }

    #[test]
    fn flight_credentials_default_is_all_none() {
        let creds = FlightCredentials::default();
        assert!(creds.username.is_none());
        assert!(creds.password.is_none());
        assert!(creds.bearer_token.is_none());
        assert!(creds.client_cert_cn.is_none());
    }

    #[test]
    fn identity_debug_does_not_leak_token() {
        let identity = Identity {
            user_id: "alice".to_string(),
            display_name: "Alice".to_string(),
            roles: vec!["analyst".to_string()],
            catalog_token: Some("ey-very-secret-jwt-value".to_string()),
            refresh_token: Some("very-secret-refresh-token".to_string()),
        };
        let debug = format!("{:?}", identity);

        // Identifiers and roles are fine to log.
        assert!(debug.contains("alice"), "user_id should appear: {debug}");
        assert!(debug.contains("Alice"), "display_name should appear: {debug}");
        assert!(debug.contains("analyst"), "role should appear: {debug}");

        // Secrets must NOT appear in any form.
        assert!(
            !debug.contains("ey-very-secret-jwt-value"),
            "catalog_token leaked to Debug output: {debug}"
        );
        assert!(
            !debug.contains("very-secret-refresh-token"),
            "refresh_token leaked to Debug output: {debug}"
        );
        // Presence sentinel is shown so operators can tell the field was set.
        assert!(debug.contains("<set>"), "presence sentinel missing: {debug}");
    }

    #[test]
    fn flight_credentials_debug_does_not_leak_password_or_bearer() {
        let creds = FlightCredentials {
            username: Some("alice".to_string()),
            password: Some("hunter2-very-private".to_string()),
            bearer_token: Some("ey-bearer-private".to_string()),
            client_cert_cn: Some("cn=alice".to_string()),
        };
        let debug = format!("{:?}", creds);

        assert!(debug.contains("alice"), "username should appear: {debug}");
        assert!(debug.contains("cn=alice"), "cert CN should appear: {debug}");

        assert!(
            !debug.contains("hunter2-very-private"),
            "password leaked to Debug output: {debug}"
        );
        assert!(
            !debug.contains("ey-bearer-private"),
            "bearer_token leaked to Debug output: {debug}"
        );
        assert!(debug.contains("<set>"), "presence sentinel missing: {debug}");
    }

    #[test]
    fn flight_credentials_debug_distinguishes_none_from_set() {
        // None should render as the string "None", not "<set>".
        let creds = FlightCredentials {
            username: Some("alice".to_string()),
            ..Default::default()
        };
        let debug = format!("{:?}", creds);
        // password and bearer_token are None — must NOT show "<set>" for them
        // (but the username is some, so the test guards against missing <set>
        // entirely is not appropriate here).
        // We instead check that the rendering contains "None" for the empty
        // fields. debug_struct prints "field: None" for &"None" values.
        assert!(debug.contains("password: \"None\""), "expected 'password: \"None\"': {debug}");
        assert!(debug.contains("bearer_token: \"None\""), "expected 'bearer_token: \"None\"': {debug}");
    }
}
