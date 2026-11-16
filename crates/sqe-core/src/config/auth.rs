use serde::Deserialize;

use crate::SecretString;

use super::{default_refresh_buffer, default_true};

/// Configuration for a single auth provider in the `[[auth.providers]]` array.
///
/// Each variant maps to a concrete `AuthProvider` implementation in `sqe-auth`.
/// The `type` field in TOML selects the variant via the serde tag.
#[derive(Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthProviderConfig {
    /// OIDC Resource Owner Password Credentials (ROPC) grant.
    /// Works with any OIDC provider (Keycloak, Auth0, Okta, Zitadel, etc.).
    OidcPassword {
        /// Full token endpoint URL.
        token_url: String,
        /// OAuth2 client_id.
        client_id: String,
        /// OAuth2 client_secret. Empty string for public clients.
        #[serde(default)]
        client_secret: String,
        /// Dot-separated JSON path to the roles array in the JWT payload.
        /// Default: `"realm_access.roles"`.
        #[serde(default = "default_roles_claim")]
        roles_claim: String,
        /// JWT claim for the subject identifier (`sub`). Default: `"sub"`.
        /// Distinct from `user_claim`/`user_id`; used to populate `Identity::subject`.
        #[serde(default = "default_user_claim")]
        subject_claim: String,
        /// JWT claim path for the user's email address. Empty string disables extraction.
        #[serde(default)]
        email_claim: String,
        /// Dot-separated JSON path to the groups array in the JWT payload.
        /// Empty string disables extraction. Separate from `roles_claim`.
        #[serde(default)]
        groups_claim: String,
        /// When `true`, a token-endpoint *rejection* of the ROPC grant returns
        /// `NotMyCredentials` (defer to the next provider) instead of
        /// `AuthFailed` (stop the chain). Set this on a mixed Basic-auth
        /// listener so a `client_id`/`client_secret` that is not a valid user
        /// falls through to `client_credentials_passthrough`. Default `false`
        /// preserves single-provider behavior. (#276)
        #[serde(default)]
        fallthrough_on_reject: bool,
    },
    /// Generic OAuth2 client_credentials grant (e.g. Polaris service token).
    ClientCredentials {
        /// OAuth2 token endpoint URL.
        token_endpoint: String,
        /// OAuth2 client_id.
        client_id: String,
        /// OAuth2 client_secret.
        client_secret: String,
    },
    /// Per-connection OAuth2 `client_credentials` passthrough. The end-user
    /// client presents its OWN `client_id`/`client_secret` on the connection
    /// (Basic auth: username = client_id, password = client_secret); SQE runs
    /// the grant per connection and forwards the resulting token to the catalog.
    /// No credentials live in config: that is the difference from
    /// `ClientCredentials`. It consumes username/password, so to share a
    /// listener with `OidcPassword` (mixed human + service-principal Basic
    /// auth) set `fallthrough_on_reject = true` on the `OidcPassword` provider
    /// that precedes it; otherwise deploy it as the sole username/password
    /// provider (service-principal-only access). (#276)
    ClientCredentialsPassthrough {
        /// Full OAuth2 token endpoint URL.
        token_url: String,
        /// Dot-separated JSON path to the roles array in the JWT payload.
        /// Default: `"realm_access.roles"`.
        #[serde(default = "default_roles_claim")]
        roles_claim: String,
        /// JWT claim for the subject identifier (`sub`). Default: `"sub"`.
        #[serde(default = "default_user_claim")]
        subject_claim: String,
        /// Optional OAuth `scope`, sent only when set. No Polaris-specific
        /// default.
        #[serde(default)]
        scope: Option<String>,
        /// When `true`, a token-endpoint *rejection* of the client_credentials
        /// grant returns `NotMyCredentials` instead of `AuthFailed`, so a
        /// human ROPC credential that is not a valid client falls through to a
        /// following `oidc_password` provider. Default `false`. (#276)
        #[serde(default)]
        fallthrough_on_reject: bool,
    },
    /// OAuth2 Token Exchange (RFC 8693) — exchanges an incoming credential for a
    /// user-scoped JWT via an OIDC token endpoint. Catch-all; place last in chain.
    TokenExchange {
        /// Full token endpoint URL.
        token_url: String,
        /// OAuth2 client_id.
        client_id: String,
        /// OAuth2 client_secret. Optional for public clients.
        #[serde(default)]
        client_secret: Option<String>,
        /// Target audience for the exchanged token (e.g. `"polaris"`).
        #[serde(default)]
        audience: Option<String>,
        /// JWT claim that carries the user identifier. Default: `"sub"`.
        #[serde(default = "default_user_claim")]
        user_claim: String,
        /// Dot-separated JSON path to the roles array in the JWT payload.
        /// Default: `"realm_access.roles"`.
        #[serde(default = "default_roles_claim")]
        roles_claim: String,
    },
    /// Pre-obtained JWT validated via JWKS. For programmatic clients, SSO flows, Airflow.
    BearerToken {
        /// JWKS endpoint URL for signature verification.
        jwks_url: String,
        /// Expected issuer (`iss` claim). Optional.
        #[serde(default)]
        issuer: Option<String>,
        /// Expected audience (`aud` claim). Required by default; see
        /// `allow_unbounded_audience` for the explicit opt-out. Without
        /// audience binding, any JWT signed by the configured JWKS issuer
        /// would be accepted (confused-deputy across SaaS apps sharing
        /// the IdP). Issue #8.
        #[serde(default)]
        audience: Option<String>,
        /// JWT claim for user identity. Default: `"sub"`.
        #[serde(default = "default_user_claim")]
        user_claim: String,
        /// Dot-separated JSON path to roles. Default: `"realm_access.roles"`.
        #[serde(default = "default_roles_claim")]
        roles_claim: String,
        /// JWT claim for the subject identifier (`sub`). Default: `"sub"`.
        /// Distinct from `user_claim`/`user_id`; used to populate `Identity::subject`.
        #[serde(default = "default_user_claim")]
        subject_claim: String,
        /// JWT claim path for the user's email address. Empty string disables extraction.
        #[serde(default)]
        email_claim: String,
        /// Dot-separated JSON path to the groups array in the JWT payload.
        /// Empty string disables extraction. Separate from `roles_claim`.
        #[serde(default)]
        groups_claim: String,
        /// Explicit opt-in to accept tokens with any audience. Default
        /// `false`: a missing/empty `audience` then errors at startup.
        /// Setting `true` acknowledges that tokens issued for any service
        /// sharing the IdP will be accepted.
        #[serde(default)]
        allow_unbounded_audience: bool,
        /// Explicit opt-in to allow a non-`https` `jwks_url`. Default
        /// `false`: an `http://` JWKS endpoint then errors at startup. The
        /// JWKS is the highest-trust input in the auth path; over plaintext
        /// an on-path attacker can substitute the signing keys and forge
        /// identities. Set `true` only for local/dev setups.
        #[serde(default)]
        allow_insecure_jwks: bool,
        /// Explicit opt-in to accept tokens from any issuer. Default
        /// `false`: a missing/empty `issuer` then errors at startup. A
        /// multi-tenant IdP serves one JWKS for several issuers, so audience
        /// binding alone does not stop a token minted by a sibling issuer
        /// for the same audience string. Finding 207.
        #[serde(default)]
        allow_unbounded_issuer: bool,
    },
    /// AWS IAM authentication via STS GetCallerIdentity.
    AwsIam {
        /// AWS region for STS endpoint. Default: `"us-east-1"`.
        #[serde(default = "default_aws_region")]
        region: String,
        /// Must stay `true` (the default): credentials are always validated
        /// via STS, and `false` is rejected at startup.
        #[serde(default = "default_true")]
        validate_with_sts: bool,
    },
    /// API key authentication from a keys file.
    ApiKey {
        /// Path to the TOML file containing API key entries.
        keys_file: String,
        /// Prefix that identifies an API key (default: `"sqe_"`).
        #[serde(default = "default_api_key_prefix")]
        key_prefix: String,
    },
    /// Reserved mutual TLS provider. Current listeners do not extract verified
    /// peer identity, so startup rejects this provider.
    Mtls {
        /// Whether to extract OU from the cert subject as a group.
        #[serde(default = "default_true")]
        extract_ou: bool,
        /// Whether to extract SAN DNS names as groups.
        #[serde(default)]
        extract_san: bool,
    },
    /// Fixed-identity provider for development/testing.
    Anonymous {
        /// User name to assign. Default: `"anonymous"`.
        #[serde(default = "default_anonymous_user")]
        user: String,
        /// Roles to assign. Default: empty.
        #[serde(default)]
        roles: Vec<String>,
    },
    /// Accepts any non-empty bearer and forwards it to the catalog without
    /// local validation. Intended for deployments where an upstream proxy
    /// has already validated the JWT, or for dev environments where the
    /// catalog itself is the source of truth.
    BearerPassthrough {
        /// User name to assign. Default: `"bearer-passthrough"`.
        #[serde(default = "default_bearer_passthrough_user")]
        user: String,
        /// Roles to assign. Default: empty.
        #[serde(default)]
        roles: Vec<String>,
    },
}

// Hand-written Debug so the OAuth `client_secret`s on the three secret-bearing
// variants are never printed by `{:?}`, an anyhow chain, or a panic message
// (CORE-01). Non-secret variants fall through to a variant-name-only summary
// (the catch-all also guards against a future variant leaking a secret without
// a matching arm). Note `AuthConfig`'s own Debug already summarizes the
// provider list as a count; this protects a stray `{:?}` of a single provider.
impl AuthProviderConfig {
    /// The provider's TOML `type` when it authenticates against an external
    /// identity provider, `None` for the credential-local providers.
    ///
    /// Used to refuse `mcp.auth.mode = "anonymous"` on a deployment that can
    /// authenticate real callers. `api_key`, `mtls` and `aws_iam` are local
    /// credential checks rather than an IdP, so they do not block it.
    pub fn identity_provider_kind(&self) -> Option<&'static str> {
        match self {
            Self::OidcPassword { .. } => Some("oidc_password"),
            Self::ClientCredentials { .. } => Some("client_credentials"),
            Self::ClientCredentialsPassthrough { .. } => Some("client_credentials_passthrough"),
            Self::TokenExchange { .. } => Some("token_exchange"),
            Self::BearerToken { .. } => Some("bearer_token"),
            Self::BearerPassthrough { .. } => Some("bearer_passthrough"),
            Self::AwsIam { .. }
            | Self::ApiKey { .. }
            | Self::Mtls { .. }
            | Self::Anonymous { .. } => None,
        }
    }
}

impl std::fmt::Debug for AuthProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthProviderConfig::OidcPassword {
                token_url,
                client_id,
                roles_claim,
                ..
            } => f
                .debug_struct("OidcPassword")
                .field("token_url", token_url)
                .field("client_id", client_id)
                .field("client_secret", &"[REDACTED]")
                .field("roles_claim", roles_claim)
                .finish(),
            AuthProviderConfig::ClientCredentials {
                token_endpoint,
                client_id,
                ..
            } => f
                .debug_struct("ClientCredentials")
                .field("token_endpoint", token_endpoint)
                .field("client_id", client_id)
                .field("client_secret", &"[REDACTED]")
                .finish(),
            AuthProviderConfig::TokenExchange {
                token_url,
                client_id,
                ..
            } => f
                .debug_struct("TokenExchange")
                .field("token_url", token_url)
                .field("client_id", client_id)
                .field("client_secret", &"[REDACTED]")
                .field("..", &"<other fields elided>")
                .finish(),
            other => {
                let name = match other {
                    AuthProviderConfig::BearerToken { .. } => "BearerToken",
                    AuthProviderConfig::AwsIam { .. } => "AwsIam",
                    AuthProviderConfig::ApiKey { .. } => "ApiKey",
                    AuthProviderConfig::Mtls { .. } => "Mtls",
                    AuthProviderConfig::Anonymous { .. } => "Anonymous",
                    AuthProviderConfig::ClientCredentialsPassthrough { .. } => {
                        "ClientCredentialsPassthrough"
                    }
                    AuthProviderConfig::BearerPassthrough { .. } => "BearerPassthrough",
                    // The three secret-bearing variants are handled above.
                    _ => "AuthProviderConfig",
                };
                f.debug_struct(name).finish_non_exhaustive()
            }
        }
    }
}

fn default_bearer_passthrough_user() -> String {
    "bearer-passthrough".to_string()
}

pub(super) fn default_roles_claim() -> String {
    "realm_access.roles".to_string()
}
fn default_aws_region() -> String {
    "us-east-1".to_string()
}
fn default_user_claim() -> String {
    "sub".to_string()
}
fn default_anonymous_user() -> String {
    "anonymous".to_string()
}
fn default_api_key_prefix() -> String {
    "sqe_".to_string()
}

#[derive(Deserialize, Clone)]
pub struct AuthConfig {
    /// Legacy: Keycloak base URL. Deprecated in favor of `[[auth.providers]]`.
    #[serde(default)]
    pub keycloak_url: String,
    /// Legacy: Keycloak realm. Deprecated in favor of `[[auth.providers]]`.
    #[serde(default)]
    pub realm: String,
    /// Legacy: OAuth2 client_id (used when `providers` is empty for backward compat).
    #[serde(default)]
    pub client_id: String,
    /// Legacy: OAuth2 client_secret.
    #[serde(default)]
    pub client_secret: SecretString,
    /// Legacy: Generic OAuth2 token endpoint for client_credentials grant.
    /// When set (and keycloak_url is empty), the engine uses client_credentials mode.
    #[serde(default)]
    pub token_endpoint: String,
    #[serde(default = "default_refresh_buffer")]
    pub token_refresh_buffer_secs: u64,
    /// Verify TLS certificates for OIDC/OAuth endpoints.
    /// Deprecated: use `tls_skip_verify = true` instead of `ssl_verification = false`.
    #[serde(default = "default_true")]
    pub ssl_verification: bool,
    /// Skip TLS certificate verification (default: false).
    /// When true, equivalent to `ssl_verification = false`.
    /// Clearer naming: `true` means "skip verification" (insecure).
    #[serde(default)]
    pub tls_skip_verify: bool,
    /// Dot-separated JSON path to the roles claim in the legacy OIDC password
    /// grant JWT payload. Default: `"realm_access.roles"` (Keycloak shape).
    /// Set to e.g. `"groups"` for Auth0/Okta/AzureAD whose tokens carry roles
    /// at the top level rather than under `realm_access`. Mirrors the
    /// per-provider `roles_claim` on `[[auth.providers]]` so the legacy
    /// authenticator path also reads custom claim paths (issue #13).
    #[serde(default = "default_roles_claim")]
    pub roles_claim: String,
    /// Explicit provider chain. When non-empty, takes precedence over the
    /// legacy `keycloak_url` / `token_endpoint` fields.
    #[serde(default)]
    pub providers: Vec<AuthProviderConfig>,
    /// Group/ARN → roles mapping shared across providers that support it.
    /// Keys are group names or ARN patterns, values are role lists.
    #[serde(default)]
    pub role_mappings: std::collections::HashMap<String, Vec<String>>,
    /// Interactive OIDC flows (auth code + PKCE, device code).
    /// Maps to `[auth.external]` in TOML.
    #[serde(default)]
    pub external: Option<ExternalAuthConfig>,
    /// Roles that may execute coordinator-wide DDL: ATTACH, DETACH,
    /// CREATE SECRET, DROP SECRET, SHOW SECRETS. Every authenticated
    /// session used to be able to mount arbitrary catalog backends or
    /// stash arbitrary credentials in process memory; that surface is
    /// now gated behind the roles listed here (issue #3). Default:
    /// `["service_admin", "catalog_admin"]`.
    #[serde(default = "default_admin_roles")]
    pub admin_roles: Vec<String>,
}

pub(super) fn default_admin_roles() -> Vec<String> {
    vec!["service_admin".to_string(), "catalog_admin".to_string()]
}

impl AuthConfig {
    /// Refuse a configured control that current transport adapters cannot supply.
    /// TLS client-certificate verification alone does not populate the identity
    /// consumed by MtlsProvider; never substitute request headers for that proof.
    pub fn validate_transport_providers(&self) -> Result<(), String> {
        if self
            .providers
            .iter()
            .any(|p| matches!(p, AuthProviderConfig::Mtls { .. }))
        {
            return Err("auth.providers: mtls requires verified TLS peer identity extraction, which current listeners do not support".into());
        }
        Ok(())
    }

    /// Returns true if TLS certificate verification should be skipped.
    /// Combines `tls_skip_verify` (new, clear) with `ssl_verification` (legacy, inverted).
    /// Either `tls_skip_verify = true` OR `ssl_verification = false` triggers skip.
    pub fn should_skip_tls_verify(&self) -> bool {
        self.tls_skip_verify || !self.ssl_verification
    }

    /// Returns `true` if any of the given role names is in `admin_roles`.
    /// Empty `admin_roles` (an operator who explicitly disabled the gate)
    /// returns `false` — admin-only statements then fail closed.
    pub fn has_admin_role(&self, roles: &[String]) -> bool {
        if self.admin_roles.is_empty() {
            return false;
        }
        roles
            .iter()
            .any(|r| self.admin_roles.iter().any(|admin| admin == r))
    }
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("keycloak_url", &self.keycloak_url)
            .field("realm", &self.realm)
            .field("client_id", &self.client_id)
            .field("client_secret", &self.client_secret)
            .field("token_endpoint", &self.token_endpoint)
            .field("token_refresh_buffer_secs", &self.token_refresh_buffer_secs)
            .field("ssl_verification", &self.ssl_verification)
            .field(
                "providers",
                &format!("[{} provider(s)]", self.providers.len()),
            )
            .field(
                "role_mappings",
                &format!("[{} mapping(s)]", self.role_mappings.len()),
            )
            .field(
                "external",
                &self
                    .external
                    .as_ref()
                    .map(|e| format!("issuer={}", e.issuer)),
            )
            .finish()
    }
}

/// Configuration for interactive OIDC flows (device code, Trino external auth).
#[derive(Debug, Deserialize, Clone)]
pub struct ExternalAuthConfig {
    pub issuer: String,
    pub client_id: String,
    #[serde(default)]
    pub client_secret: Option<String>,
    #[serde(default = "default_redirect_uri")]
    pub redirect_uri: String,
    #[serde(default = "default_external_scopes")]
    pub scopes: Vec<String>,
    #[serde(default = "default_challenge_timeout")]
    pub challenge_timeout_secs: u64,
    #[serde(default)]
    pub authorization_endpoint: Option<String>,
    #[serde(default)]
    pub token_endpoint: Option<String>,
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

fn default_redirect_uri() -> String {
    "http://localhost:8080/oauth2/callback".to_string()
}
fn default_external_scopes() -> Vec<String> {
    vec!["openid".to_string(), "profile".to_string()]
}
fn default_challenge_timeout() -> u64 {
    900
}
