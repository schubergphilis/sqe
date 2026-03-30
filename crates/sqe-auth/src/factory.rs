//! Factory function for constructing an `AuthChain` from configuration.
//!
//! Supports both the new `[[auth.providers]]` array and backward-compatible
//! fallback to the legacy `keycloak_url` / `token_endpoint` fields.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::info;

use sqe_core::config::{AuthConfig, AuthProviderConfig};

use crate::anonymous::{AnonymousProvider, AnonymousProviderConfig};
use crate::api_key::{ApiKeyProvider, ApiKeyProviderConfig};
use crate::aws_iam::{AwsIamProvider, AwsIamProviderConfig};
use crate::bearer_token::{BearerTokenProvider, BearerTokenProviderConfig};
use crate::chain::AuthChain;
use crate::mtls::{MtlsProvider, MtlsProviderConfig};
use crate::oidc_provider::{OidcPasswordProvider, OidcPasswordProviderConfig};
use crate::token_exchange::{TokenExchangeProvider, TokenExchangeConfig};
use crate::provider::AuthProvider;
use crate::authenticator::Authenticator;

/// Build an `AuthChain` from the given `AuthConfig`.
///
/// When `auth.providers` is non-empty, each entry is mapped to a concrete
/// `AuthProvider` implementation and assembled into a chain.
///
/// When `auth.providers` is empty (backward compatibility), the legacy
/// `Authenticator` (which uses `keycloak_url` / `token_endpoint`) is wrapped
/// in a single-provider chain.
pub async fn build_auth_chain(config: &AuthConfig) -> sqe_core::Result<AuthChain> {
    if !config.providers.is_empty() {
        info!(
            count = config.providers.len(),
            "Building auth chain from explicit provider config"
        );
        let mut providers: Vec<Arc<dyn AuthProvider>> = Vec::new();

        for (i, provider_config) in config.providers.iter().enumerate() {
            let provider: Arc<dyn AuthProvider> = match provider_config {
                AuthProviderConfig::OidcPassword {
                    token_url,
                    client_id,
                    client_secret,
                    roles_claim,
                } => {
                    info!(
                        index = i,
                        token_url = %token_url,
                        "Adding OidcPasswordProvider to chain"
                    );
                    let oidc_config = OidcPasswordProviderConfig {
                        token_url: token_url.clone(),
                        client_id: client_id.clone(),
                        client_secret: client_secret.clone(),
                        roles_claim: roles_claim.clone(),
                        accept_invalid_certs: !config.ssl_verification,
                    };
                    let provider = OidcPasswordProvider::new(oidc_config).map_err(|e| {
                        sqe_core::SqeError::Config(format!(
                            "Failed to create OidcPasswordProvider: {e}"
                        ))
                    })?;
                    Arc::new(provider)
                }
                AuthProviderConfig::ClientCredentials {
                    token_endpoint,
                    client_id,
                    client_secret,
                } => {
                    info!(
                        index = i,
                        token_endpoint = %token_endpoint,
                        "Adding OAuth2 ClientCredentials provider to chain"
                    );
                    // Wrap the existing OAuthClient inside an Authenticator-compatible provider.
                    // For now, create a legacy Authenticator configured for client_credentials.
                    let legacy_config = AuthConfig {
                        keycloak_url: String::new(),
                        realm: String::new(),
                        client_id: client_id.clone(),
                        client_secret: client_secret.clone(),
                        token_endpoint: token_endpoint.clone(),
                        token_refresh_buffer_secs: config.token_refresh_buffer_secs,
                        ssl_verification: config.ssl_verification,
                        providers: Vec::new(),
                        role_mappings: HashMap::new(),
                    };
                    let auth = Authenticator::new(&legacy_config).await?;
                    Arc::new(auth)
                }
                AuthProviderConfig::TokenExchange {
                    token_url,
                    client_id,
                    client_secret,
                    audience,
                    user_claim,
                    roles_claim,
                } => {
                    info!(
                        index = i,
                        token_url = %token_url,
                        "Adding TokenExchangeProvider to chain"
                    );
                    let te_config = TokenExchangeConfig {
                        token_url: token_url.clone(),
                        client_id: client_id.clone(),
                        client_secret: client_secret.clone(),
                        audience: audience.clone(),
                        user_claim: user_claim.clone(),
                        roles_claim: roles_claim.clone(),
                        accept_invalid_certs: !config.ssl_verification,
                    };
                    let provider = TokenExchangeProvider::new(te_config).map_err(|e| {
                        sqe_core::SqeError::Config(format!(
                            "Failed to create TokenExchangeProvider: {e}"
                        ))
                    })?;
                    Arc::new(provider)
                }
                AuthProviderConfig::BearerToken {
                    jwks_url,
                    issuer,
                    audience,
                    user_claim,
                    roles_claim,
                } => {
                    info!(
                        index = i,
                        jwks_url = %jwks_url,
                        "Adding BearerTokenProvider to chain"
                    );
                    let bt_config = BearerTokenProviderConfig {
                        jwks_url: jwks_url.clone(),
                        issuer: issuer.clone(),
                        audience: audience.clone(),
                        user_claim: user_claim.clone(),
                        roles_claim: roles_claim.clone(),
                    };
                    let provider = BearerTokenProvider::new(bt_config).map_err(|e| {
                        sqe_core::SqeError::Config(format!(
                            "Failed to create BearerTokenProvider: {e}"
                        ))
                    })?;
                    Arc::new(provider)
                }
                AuthProviderConfig::AwsIam {
                    region,
                    validate_with_sts,
                } => {
                    info!(
                        index = i,
                        region = %region,
                        validate_with_sts = validate_with_sts,
                        "Adding AwsIamProvider to chain"
                    );
                    let aws_config = AwsIamProviderConfig {
                        region: region.clone(),
                        validate_with_sts: *validate_with_sts,
                        role_mappings: config.role_mappings.clone(),
                        key_mappings: HashMap::new(),
                    };
                    Arc::new(AwsIamProvider::new(aws_config))
                }
                AuthProviderConfig::ApiKey {
                    keys_file,
                    key_prefix,
                } => {
                    info!(
                        index = i,
                        keys_file = %keys_file,
                        "Adding ApiKeyProvider to chain"
                    );
                    let ak_config = ApiKeyProviderConfig {
                        keys_file: keys_file.into(),
                        key_prefix: key_prefix.clone(),
                        role_mappings: config.role_mappings.clone(),
                        ..Default::default()
                    };
                    let provider = ApiKeyProvider::new(ak_config).map_err(|e| {
                        sqe_core::SqeError::Config(format!(
                            "Failed to create ApiKeyProvider: {e}"
                        ))
                    })?;
                    Arc::new(provider)
                }
                AuthProviderConfig::Mtls {
                    extract_ou,
                    extract_san,
                } => {
                    info!(
                        index = i,
                        extract_ou = extract_ou,
                        extract_san = extract_san,
                        "Adding MtlsProvider to chain"
                    );
                    let mtls_config = MtlsProviderConfig {
                        extract_ou: *extract_ou,
                        extract_san: *extract_san,
                        role_mappings: config.role_mappings.clone(),
                    };
                    Arc::new(MtlsProvider::new(mtls_config))
                }
                AuthProviderConfig::Anonymous { user, roles } => {
                    info!(
                        index = i,
                        user = %user,
                        "Adding AnonymousProvider to chain"
                    );
                    Arc::new(AnonymousProvider::new(AnonymousProviderConfig {
                        user: user.clone(),
                        roles: roles.clone(),
                    }))
                }
            };
            providers.push(provider);
        }

        Ok(AuthChain::new(providers))
    } else {
        // Backward compatibility: wrap the legacy Authenticator in a single-provider chain.
        info!("No explicit providers configured — falling back to legacy auth backend");
        let authenticator = Authenticator::new(config).await?;
        Ok(AuthChain::new(vec![Arc::new(authenticator)]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::FlightCredentials;

    // -----------------------------------------------------------------------
    // Anonymous provider via factory
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn build_chain_with_anonymous_provider() {
        let config = AuthConfig {
            keycloak_url: String::new(),
            realm: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            token_endpoint: String::new(),
            token_refresh_buffer_secs: 60,
            ssl_verification: true,
            providers: vec![AuthProviderConfig::Anonymous {
                user: "dev-user".to_string(),
                roles: vec!["admin".to_string()],
            }],
            role_mappings: HashMap::new(),
        };

        let chain = build_auth_chain(&config).await.expect("should build chain");
        assert_eq!(chain.len(), 1);

        let identity = chain
            .authenticate(&FlightCredentials::default())
            .await
            .expect("anonymous should always succeed");
        assert_eq!(identity.user_id, "dev-user");
        assert_eq!(identity.roles, vec!["admin"]);
    }

    // -----------------------------------------------------------------------
    // OIDC password provider via factory (construction only, no network)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn build_chain_with_oidc_provider() {
        let config = AuthConfig {
            keycloak_url: String::new(),
            realm: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            token_endpoint: String::new(),
            token_refresh_buffer_secs: 60,
            ssl_verification: true,
            providers: vec![AuthProviderConfig::OidcPassword {
                token_url: "http://localhost:8080/token".to_string(),
                client_id: "sqe".to_string(),
                client_secret: "secret".to_string(),
                roles_claim: "realm_access.roles".to_string(),
            }],
            role_mappings: HashMap::new(),
        };

        let chain = build_auth_chain(&config).await.expect("should build chain");
        assert_eq!(chain.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Multi-provider chain
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn build_chain_multi_provider() {
        let config = AuthConfig {
            keycloak_url: String::new(),
            realm: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            token_endpoint: String::new(),
            token_refresh_buffer_secs: 60,
            ssl_verification: true,
            providers: vec![
                AuthProviderConfig::OidcPassword {
                    token_url: "http://localhost:8080/token".to_string(),
                    client_id: "sqe".to_string(),
                    client_secret: "secret".to_string(),
                    roles_claim: "realm_access.roles".to_string(),
                },
                AuthProviderConfig::Anonymous {
                    user: "fallback".to_string(),
                    roles: vec!["public".to_string()],
                },
            ],
            role_mappings: HashMap::new(),
        };

        let chain = build_auth_chain(&config).await.expect("should build chain");
        assert_eq!(chain.len(), 2);

        // With empty credentials, OIDC should return NotMyCredentials,
        // and anonymous should catch everything.
        let identity = chain
            .authenticate(&FlightCredentials::default())
            .await
            .expect("anonymous fallback should succeed");
        assert_eq!(identity.user_id, "fallback");
    }

    // -----------------------------------------------------------------------
    // Legacy fallback (no providers array)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn build_chain_legacy_fallback() {
        let config = AuthConfig {
            keycloak_url: "http://localhost:8080".to_string(),
            realm: "test".to_string(),
            client_id: "sqe-client".to_string(),
            client_secret: "secret".to_string(),
            token_endpoint: String::new(),
            token_refresh_buffer_secs: 60,
            ssl_verification: false,
            providers: Vec::new(),
            role_mappings: HashMap::new(),
        };

        let chain = build_auth_chain(&config).await.expect("should build chain");
        assert_eq!(chain.len(), 1);
        assert!(!chain.is_empty());
    }
}
