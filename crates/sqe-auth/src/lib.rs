pub mod anonymous;
pub mod api_key;
pub mod auth_code;
pub mod authenticator;
pub mod aws_iam;
pub mod bearer_passthrough;
pub mod bearer_token;
pub mod chain;
pub mod device_code;
pub mod factory;
pub mod mtls;
pub mod oauth;
pub mod oidc_client_credentials;
pub mod oidc_discovery;
pub mod oidc_m2m;
pub mod oidc_password;
pub mod oidc_provider;
pub mod pending_auth;
pub mod per_catalog;
pub mod provider;
pub mod token_cache;
pub mod token_exchange;

/// Deprecated: use `oidc_password` instead.
#[deprecated(note = "renamed to oidc_password")]
pub mod keycloak {
    pub use crate::oidc_password::*;
}

pub use anonymous::{AnonymousProvider, AnonymousProviderConfig};
pub use api_key::{ApiKeyEntry, ApiKeyProvider, ApiKeyProviderConfig};
pub use auth_code::{AuthCodeChallenge, AuthCodeService};
pub use authenticator::Authenticator;
pub use aws_iam::{AwsIamProvider, AwsIamProviderConfig, KeyMapping as AwsKeyMapping};
pub use bearer_token::{BearerTokenProvider, BearerTokenProviderConfig};
pub use chain::AuthChain;
pub use device_code::{DeviceAuthSession, DeviceCodeService, DevicePollResult};
pub use factory::build_auth_chain;
pub use mtls::{MtlsProvider, MtlsProviderConfig};
pub use oidc_client_credentials::{OidcClientCredentialsConfig, OidcClientCredentialsProvider};
pub use oidc_discovery::{DiscoveredEndpoints, OidcDiscovery, OidcDiscoveryConfig};
pub use oidc_m2m::{OidcM2mConfig, OidcM2mProvider};
pub use oidc_provider::{OidcPasswordProvider, OidcPasswordProviderConfig};
pub use pending_auth::{PendingAuth, PendingAuthStore, TokenSet};
pub use provider::{AuthError, AuthProvider, FlightCredentials, Identity};
pub use token_exchange::{TokenExchangeConfig, TokenExchangeProvider};
