use serde::Deserialize;

use crate::SecretString;

/// Streamable HTTP MCP resource-server configuration.
///
/// MCP never mints or exchanges tokens. It validates the caller's bearer with
/// SQE's existing auth chain and forwards that same user token through the SQE
/// session to the source catalog/storage layer.
#[derive(Debug, Deserialize, Clone)]
pub struct McpConfig {
    /// Enable MCP. A non-zero `port` also enables it for backwards-compatible
    /// convenience; `enabled = true, port = 0` mounts it on the web port.
    #[serde(default)]
    pub enabled: bool,
    /// Dedicated TCP port for `/mcp`; zero mounts on the existing web listener.
    #[serde(default)]
    pub port: u16,
    /// Interface the dedicated `/mcp` listener binds. Empty picks
    /// `127.0.0.1` under `[mcp.auth] mode = "anonymous"` and `0.0.0.0`
    /// otherwise, so an unauthenticated endpoint on a laptop or bastion is
    /// not reachable from the network unless this says so (#671). Ignored
    /// with `port = 0`, where `/mcp` rides the web listener's bind.
    #[serde(default)]
    pub bind_address: String,
    /// Canonical RFC 8707 resource identifier (normally the public MCP URL).
    #[serde(default)]
    pub resource_uri: String,
    /// OAuth/OIDC issuers advertised through RFC 9728 protected-resource metadata.
    #[serde(default)]
    pub authorization_servers: Vec<String>,
    /// Browser origins allowed to call `/mcp`. Requests without `Origin`
    /// (normal native MCP clients) are unaffected. Empty rejects all browser origins.
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    /// Scope required for all MCP discovery and read operations.
    #[serde(default = "default_mcp_read_scope")]
    pub read_scope: String,
    /// Additional scope required by the change/write tool.
    #[serde(default = "default_mcp_write_scope")]
    pub write_scope: String,
    /// Enables the change/write tool. It is absent from tools/list when false.
    #[serde(default)]
    pub allow_writes: bool,
    /// At least one of these SQE roles is also required for change/write calls.
    #[serde(default = "default_mcp_write_roles")]
    pub write_roles: Vec<String>,
    /// Maximum rows serialized into one MCP tool response.
    #[serde(default = "default_mcp_max_result_rows")]
    pub max_result_rows: usize,
    /// Maximum JSON bytes returned in one MCP tool response.
    #[serde(default = "default_mcp_max_response_bytes")]
    pub max_response_bytes: usize,
    /// Deadline for one statement run by an MCP tool call, in seconds; `0`
    /// leaves only `query.timeout_secs`. An MCP client waits about a minute
    /// and a half and then retries, so a statement that outlives the caller
    /// is work nobody collects: past this deadline it is cancelled and the
    /// tool reports the deadline instead. Set it below the client's own.
    #[serde(default = "default_mcp_request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// Catalogs the change/write tool may target. Empty means any catalog
    /// EXCEPT a `[[hive.roots]]` catalog: Hive roots are reached with the
    /// engine's own credentials, so an agent writing there acts as SQE, not
    /// as the user, and that needs naming here explicitly. Compared
    /// case-insensitively. A change statement whose target catalog cannot be
    /// determined (unqualified name with no default catalog, or a statement
    /// kind without a table target) is refused while this gate is active.
    #[serde(default)]
    pub write_catalogs: Vec<String>,
    /// Default catalog for MCP sessions, so agents can use two-part names.
    /// A static principal's own `default_catalog` wins over this one.
    #[serde(default)]
    pub default_catalog: Option<String>,
    /// Default schema for MCP sessions. Only meaningful with a catalog.
    #[serde(default)]
    pub default_schema: Option<String>,
    /// How callers authenticate to `/mcp`. Defaults to OAuth.
    #[serde(default)]
    pub auth: McpAuthConfig,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: 0,
            bind_address: String::new(),
            resource_uri: String::new(),
            authorization_servers: Vec::new(),
            allowed_origins: Vec::new(),
            read_scope: default_mcp_read_scope(),
            write_scope: default_mcp_write_scope(),
            allow_writes: false,
            write_roles: default_mcp_write_roles(),
            max_result_rows: default_mcp_max_result_rows(),
            max_response_bytes: default_mcp_max_response_bytes(),
            request_timeout_secs: default_mcp_request_timeout_secs(),
            write_catalogs: Vec::new(),
            default_catalog: None,
            default_schema: None,
            auth: McpAuthConfig::default(),
        }
    }
}

/// How callers authenticate to `/mcp`.
///
/// `Oauth` is the only mode that reaches an external identity provider. The
/// other two exist for deployments that have none: a catalog configured with
/// engine credentials (AWS Glue, S3 Tables, an attached warehouse) has no user
/// tokens to validate, so the MCP surface is gated on configured principals or
/// on nothing at all.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum McpAuthMode {
    /// Validate the caller's bearer through the auth chain and read OAuth
    /// scopes from its claims. Requires `authorization_servers`.
    #[default]
    Oauth,
    /// Match the caller's bearer against `[[mcp.auth.principals]]`. Roles come
    /// from the matched principal; there are no OAuth scopes.
    Static,
    /// Accept every caller as one fixed identity. Refused when an identity
    /// provider is configured, under `production_mode`, and with writes on.
    Anonymous,
}

impl McpAuthMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Oauth => "oauth",
            Self::Static => "static",
            Self::Anonymous => "anonymous",
        }
    }
}

/// MCP authentication settings.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct McpAuthConfig {
    #[serde(default)]
    pub mode: McpAuthMode,
    /// Static bearer principals, used when `mode = "static"`.
    #[serde(default)]
    pub principals: Vec<McpPrincipalConfig>,
    /// Identity assigned to every caller when `mode = "anonymous"`.
    #[serde(default)]
    pub anonymous: McpAnonymousConfig,
}

impl McpAuthConfig {
    /// Roles a principal must hold for the change/write tool, given the
    /// endpoint's `write_roles`.
    pub fn has_writer(&self, write_roles: &[String]) -> bool {
        self.principals
            .iter()
            .any(|p| p.roles.iter().any(|role| write_roles.contains(role)))
    }
}

/// One static MCP principal: a bearer token, a user name, and roles.
///
/// The token comes from `token_env` (preferred) or `token_b64`. Base64 is
/// encoding, not protection: anyone who can read the config can read the
/// token, so prefer the environment variable outside development.
#[derive(Debug, Deserialize, Clone)]
pub struct McpPrincipalConfig {
    /// Identity assigned to callers presenting this token.
    pub user: String,
    /// SQE roles for this principal. The write gate checks these against
    /// `mcp.write_roles`.
    #[serde(default)]
    pub roles: Vec<String>,
    /// Environment variable holding the bearer token. Wins over `token_b64`.
    #[serde(default)]
    pub token_env: String,
    /// Base64-encoded bearer token. Obfuscation only.
    #[serde(default)]
    pub token_b64: String,
    /// Default catalog for this principal's sessions; wins over
    /// `mcp.default_catalog`. Pins an agent to one warehouse without it
    /// having to qualify every name.
    #[serde(default)]
    pub default_catalog: Option<String>,
    /// Default schema for this principal's sessions; wins over
    /// `mcp.default_schema`.
    #[serde(default)]
    pub default_schema: Option<String>,
}

/// Minimum token length. Short tokens are guessable, and a static token is the
/// only thing standing between a caller and the engine's own credentials.
const MCP_MIN_TOKEN_LEN: usize = 16;

impl McpPrincipalConfig {
    /// Resolve this principal's bearer token, preferring `token_env`.
    ///
    /// Returns the message to surface at startup on failure, so validation and
    /// the serving path agree on what a usable token is.
    pub fn resolve_token(&self) -> Result<SecretString, String> {
        let user = &self.user;
        let token = if !self.token_env.trim().is_empty() {
            let var = self.token_env.trim();
            match std::env::var(var) {
                Ok(value) if !value.trim().is_empty() => value,
                _ => {
                    return Err(format!(
                        "mcp.auth.principals[{user}] token_env '{var}' is unset or empty"
                    ))
                }
            }
        } else if !self.token_b64.trim().is_empty() {
            let decoded = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                self.token_b64.trim(),
            )
            .map_err(|_| format!("mcp.auth.principals[{user}] token_b64 is not valid base64"))?;
            String::from_utf8(decoded).map_err(|_| {
                format!("mcp.auth.principals[{user}] token_b64 must decode to UTF-8")
            })?
        } else {
            return Err(format!(
                "mcp.auth.principals[{user}] needs either token_env or token_b64"
            ));
        };
        if token.len() < MCP_MIN_TOKEN_LEN {
            return Err(format!(
                "mcp.auth.principals[{user}] token must be at least {MCP_MIN_TOKEN_LEN} characters"
            ));
        }
        Ok(SecretString::new(token))
    }
}

/// Identity handed to every caller in anonymous mode.
#[derive(Debug, Deserialize, Clone)]
pub struct McpAnonymousConfig {
    #[serde(default = "default_mcp_anonymous_user")]
    pub user: String,
    #[serde(default)]
    pub roles: Vec<String>,
}

impl Default for McpAnonymousConfig {
    fn default() -> Self {
        Self {
            user: default_mcp_anonymous_user(),
            roles: Vec::new(),
        }
    }
}

fn default_mcp_anonymous_user() -> String {
    "mcp-anonymous".to_string()
}

impl McpConfig {
    /// `enabled` alone (#672): a leftover `port` line is not what opens the
    /// surface. `--mcp-port` and `--mcp-anonymous` set `enabled` themselves.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// `port > 0` with `enabled = false`: the surface is OFF; `validate`
    /// and the startup path warn so the operator learns why nothing listens.
    pub fn enabled_by_port_only(&self) -> bool {
        !self.enabled && self.port > 0
    }

    /// The interface the dedicated listener binds: `bind_address`, or the
    /// mode's default when it is empty (loopback for anonymous, every
    /// interface otherwise). `Err` names an address that does not parse.
    pub fn bind_ip(&self) -> Result<std::net::IpAddr, String> {
        let raw = self.bind_address.trim();
        if raw.is_empty() {
            return Ok(if self.auth.mode == McpAuthMode::Anonymous {
                std::net::Ipv4Addr::LOCALHOST.into()
            } else {
                std::net::Ipv4Addr::UNSPECIFIED.into()
            });
        }
        raw.parse()
            .map_err(|_| format!("mcp.bind_address is not an IP address: {raw:?}"))
    }
}

fn default_mcp_read_scope() -> String {
    "sqe:mcp:read".to_string()
}
fn default_mcp_write_scope() -> String {
    "sqe:mcp:write".to_string()
}
fn default_mcp_write_roles() -> Vec<String> {
    vec!["data_writer".to_string()]
}
fn default_mcp_max_result_rows() -> usize {
    1_000
}
fn default_mcp_max_response_bytes() -> usize {
    2 * 1024 * 1024
}
fn default_mcp_request_timeout_secs() -> u64 {
    75
}

pub(super) fn valid_oauth_scope(scope: &str) -> bool {
    !scope.is_empty()
        && scope
            .bytes()
            .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_auth_mode_defaults_to_oauth() {
        assert_eq!(McpAuthConfig::default().mode, McpAuthMode::Oauth);
    }

    /// #671: an unauthenticated listener stays on loopback unless the
    /// operator names another interface; authenticated modes keep 0.0.0.0.
    #[test]
    fn mcp_bind_defaults_to_loopback_only_for_anonymous() {
        let mut mcp = McpConfig::default();
        assert_eq!(mcp.bind_ip().unwrap(), std::net::IpAddr::from([0, 0, 0, 0]));
        mcp.auth.mode = McpAuthMode::Anonymous;
        assert_eq!(
            mcp.bind_ip().unwrap(),
            std::net::IpAddr::from([127, 0, 0, 1])
        );
        mcp.bind_address = "0.0.0.0".to_string();
        assert_eq!(mcp.bind_ip().unwrap(), std::net::IpAddr::from([0, 0, 0, 0]));
        mcp.bind_address = "eth0".to_string();
        assert!(mcp.bind_ip().unwrap_err().contains("mcp.bind_address"));
    }

    /// #672: `port` alone does NOT serve; it is flagged so the WARN fires.
    /// `enabled = true` is the only switch.
    #[test]
    fn mcp_port_only_does_not_enable_the_surface() {
        let mut mcp = McpConfig {
            port: 8081,
            ..Default::default()
        };
        assert!(!mcp.is_enabled() && mcp.enabled_by_port_only());
        mcp.enabled = true;
        assert!(mcp.is_enabled() && !mcp.enabled_by_port_only());
    }
}
