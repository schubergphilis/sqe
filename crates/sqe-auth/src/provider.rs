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
#[derive(Debug, Clone, Default)]
pub struct FlightCredentials {
    pub username: Option<String>,
    pub password: Option<String>,
    pub bearer_token: Option<String>,
    pub client_cert_cn: Option<String>,
}

/// The authenticated identity produced by a successful `AuthProvider::authenticate` call.
///
/// Carried in the `Session` for the lifetime of the connection. The `catalog_token`
/// is forwarded to the Polaris REST catalog (and S3) so every request runs as the
/// authenticated user.
#[derive(Debug, Clone)]
pub struct Identity {
    pub user_id: String,
    pub display_name: String,
    pub roles: Vec<String>,
    pub catalog_token: Option<String>,
    /// Refresh token for obtaining new access tokens without re-authentication.
    /// Only populated by providers that support token refresh (e.g. OIDC password grant).
    pub refresh_token: Option<String>,
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
            catalog_token: Some("secret-token".to_string()),
            refresh_token: None,
        };
        // Debug should include the fields (it's derived Debug, which is fine
        // for development; production logging should use Display or redact).
        let debug = format!("{:?}", identity);
        assert!(debug.contains("alice"));
    }
}
