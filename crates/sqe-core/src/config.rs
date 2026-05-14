use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Clone)]
pub struct SqeConfig {
    pub coordinator: CoordinatorConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
    pub auth: AuthConfig,
    /// Legacy single-catalog block. Always present for backwards
    /// compatibility. Operators starting fresh should usually use
    /// `[catalogs.NAME]` blocks instead and leave `[catalog]` minimal
    /// (a placeholder REST URL satisfies the deserializer; flatten
    /// drops it when `catalogs` is non-empty and the operator hasn't
    /// named the legacy block via `default_catalog`).
    pub catalog: CatalogConfig,
    /// Named catalog map. Each entry is a full `CatalogConfig`,
    /// including its own `[catalogs.<name>.backend]` block, so you
    /// can attach Polaris alongside Nessie, AWS Glue, S3 Tables, HMS,
    /// JDBC, or Hadoop in the same coordinator. The map is keyed
    /// by the SQL identifier the catalog is exposed as. Empty
    /// (the default) means "use the legacy `[catalog]` block only".
    #[serde(default)]
    pub catalogs: HashMap<String, CatalogConfig>,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
    #[serde(default)]
    pub access_control: AccessControlConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub session: SessionConfig,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub query_cache: QueryCacheConfig,
    #[serde(default)]
    pub query_history: QueryHistoryConfig,
}

/// Controls adaptive sort stripping behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortMode {
    /// Always sort. Spill to disk if needed.
    Strict,
    /// Only sort when keys match Iceberg partition columns.
    PartitionOnly,
    /// Sort when memory allows; strip non-partition sorts under pressure.
    Adaptive,
}

impl SortMode {
    /// Parse from config string. Returns `Adaptive` for unknown values.
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "strict" => Self::Strict,
            "partition_only" | "partition-only" => Self::PartitionOnly,
            "adaptive" => Self::Adaptive,
            _ => {
                tracing::warn!(sort_mode = s, "Unknown sort_mode, defaulting to partition_only");
                Self::PartitionOnly
            }
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct QueryConfig {
    /// Default catalog for unqualified SQL identifiers. When unset
    /// (the common case), SQE picks `iceberg` if the legacy
    /// `[catalog]` block is the sole catalog, otherwise the first
    /// alphabetically-sorted name from `[catalogs.*]`. Set this
    /// when you've named the legacy `[catalog]` block via the
    /// flattened name (see `SqeConfig::flattened_catalogs`) and want
    /// it to be the default rather than the alphabetic winner.
    #[serde(default)]
    pub default_catalog: Option<String>,
    /// Maximum query execution time in seconds. Default: 300 (5 minutes).
    #[serde(default = "default_query_timeout")]
    pub timeout_secs: u64,
    /// Per-role timeout overrides. Keys are role names, values are timeout in
    /// seconds. When a user has multiple matching roles the highest value wins.
    #[serde(default)]
    pub role_overrides: std::collections::HashMap<String, u64>,
    /// Maximum number of rows returned per query. Default: 1_000_000. Set to 0 for unlimited.
    #[serde(default = "default_max_result_rows")]
    pub max_result_rows: usize,
    /// Maximum concurrent queries. Default: 100. Set to 0 for unlimited.
    #[serde(default = "default_max_concurrent_queries")]
    pub max_concurrent_queries: usize,
    /// Queries taking longer than this are logged at WARN level. Default: 30. Set to 0 to disable.
    #[serde(default = "default_slow_query_threshold")]
    pub slow_query_threshold_secs: u64,
    /// Maximum memory per query. Default: "256MB". Supports: B, KB, MB, GB. Set to "0" for unlimited.
    #[serde(default = "default_max_query_memory")]
    pub max_query_memory: String,
    /// Minimum total scan size to distribute across workers. Below this, execute on coordinator.
    /// Default: "128MB". Set to "0" to always distribute.
    #[serde(default = "default_distribution_threshold")]
    pub distribution_threshold: String,
    /// Minimum number of data files to distribute. Below this, execute locally on coordinator.
    /// Default: 4. Used as a fast check when file sizes are not yet available.
    #[serde(default = "default_distribution_file_threshold")]
    pub distribution_file_threshold: usize,
    /// Target size per scan task for bin-packing. Default: "256MB".
    #[serde(default = "default_target_task_size")]
    pub target_task_size: String,
    /// Controls when ORDER BY clauses are preserved vs stripped to save memory.
    ///
    /// - `"strict"`: Always sort. Spill to disk if needed.
    /// - `"partition_only"`: Only sort when keys match Iceberg partition columns.
    ///   Safest for TB-scale data from mixed writers (Spark, Trino, SQE).
    /// - `"adaptive"`: Sort when memory is available (Green); strip non-partition
    ///   sorts under memory pressure. Best default: correct for small data,
    ///   safe for large data.
    ///
    /// Default: `"adaptive"` — tries to sort in memory, falls back to
    /// partition-only under pressure. Never crashes from unbounded sorts.
    #[serde(default = "default_sort_mode")]
    pub sort_mode: String,
    /// Minimum number of projection-only columns required for late materialization
    /// to be applied. Late materialization uses a two-phase Parquet scan: Phase 1
    /// reads only predicate columns, Phase 2 reads remaining columns for surviving
    /// rows. When the number of deferrable columns is below this threshold, the
    /// overhead of two-phase scanning may exceed the I/O savings.
    ///
    /// Default: 1 (apply whenever there is at least one projection-only column).
    /// Set to 0 to disable late materialization entirely.
    #[serde(default = "default_late_mat_min_projection_cols")]
    pub late_materialization_min_projection_cols: usize,
    /// Enable star-schema join reordering. When enabled, chains of inner
    /// equi-joins are reordered so small dimension tables are joined first
    /// (building small hash tables) and the large fact table is probed last.
    ///
    /// Default: true.
    #[serde(default = "default_true")]
    pub star_schema_reorder: bool,
    /// Minimum ratio between the largest and smallest table row counts
    /// required to trigger star-schema join reordering. Only applies when
    /// `star_schema_reorder` is enabled.
    ///
    /// Default: 10 (fact table must be at least 10x larger than the smallest dimension).
    #[serde(default = "default_star_schema_min_ratio")]
    pub star_schema_min_ratio: usize,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            default_catalog: None,
            timeout_secs: default_query_timeout(),
            role_overrides: std::collections::HashMap::new(),
            max_result_rows: default_max_result_rows(),
            max_concurrent_queries: default_max_concurrent_queries(),
            slow_query_threshold_secs: default_slow_query_threshold(),
            max_query_memory: default_max_query_memory(),
            distribution_threshold: default_distribution_threshold(),
            distribution_file_threshold: default_distribution_file_threshold(),
            target_task_size: default_target_task_size(),
            sort_mode: default_sort_mode(),
            late_materialization_min_projection_cols: default_late_mat_min_projection_cols(),
            star_schema_reorder: default_true(),
            star_schema_min_ratio: default_star_schema_min_ratio(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct QueryCacheConfig {
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,
    #[serde(default = "default_cache_max_memory_mb")]
    pub max_memory_mb: u64,
    #[serde(default = "default_cache_max_entry_mb")]
    pub max_entry_mb: u64,
    #[serde(default = "default_cache_ttl_secs")]
    pub ttl_secs: u64,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_cache_enabled(),
            max_memory_mb: default_cache_max_memory_mb(),
            max_entry_mb: default_cache_max_entry_mb(),
            ttl_secs: default_cache_ttl_secs(),
        }
    }
}

fn default_cache_enabled() -> bool { true }
fn default_cache_max_memory_mb() -> u64 { 256 }
fn default_cache_max_entry_mb() -> u64 { 5 }
fn default_cache_ttl_secs() -> u64 { 300 }

#[derive(Debug, Deserialize, Clone)]
pub struct QueryHistoryConfig {
    #[serde(default = "default_history_max_entries")]
    pub max_entries: u64,
    #[serde(default = "default_history_ttl_secs")]
    pub ttl_secs: u64,
}

impl Default for QueryHistoryConfig {
    fn default() -> Self {
        Self {
            max_entries: default_history_max_entries(),
            ttl_secs: default_history_ttl_secs(),
        }
    }
}

fn default_history_max_entries() -> u64 { 10000 }
fn default_history_ttl_secs() -> u64 { 1800 }

#[derive(Debug, Deserialize, Clone)]
pub struct CoordinatorConfig {
    #[serde(default = "default_flight_port")]
    pub flight_sql_port: u16,
    #[serde(default = "default_trino_port")]
    pub trino_http_port: u16,
    #[serde(default = "default_mode")]
    pub mode: String,
    /// List of worker Flight server URLs for distributed execution.
    /// Empty = single-node mode (all queries execute locally).
    #[serde(default)]
    pub worker_urls: Vec<String>,
    /// When `true`, error responses include the full error chain (dev only).
    /// When `false` (default / production), only sanitised messages are returned.
    #[serde(default)]
    pub debug: bool,
    /// Optional TLS configuration for the Flight SQL listener.
    #[serde(default)]
    pub tls: TlsConfig,
    /// Shared secret that workers must supply in the `x-sqe-worker-secret`
    /// metadata header when sending heartbeats. Must be non-empty whenever
    /// `worker_urls` is non-empty unless `allow_unauthenticated_workers` is
    /// explicitly set. Validation rejects the empty case at startup —
    /// configuring distributed mode without a secret used to be a logged
    /// warning, but that let any client on the cluster network register as
    /// a worker and exfiltrate user bearers along with query plans.
    #[serde(default)]
    pub worker_secret: String,
    /// Opt-in escape hatch for the `worker_secret` requirement. Leaving this
    /// `false` (the default) makes the coordinator refuse to start when
    /// distributed mode is configured without a secret. Setting it `true`
    /// is visible in config diffs and acknowledges that any TCP-reachable
    /// client may register as a worker.
    #[serde(default)]
    pub allow_unauthenticated_workers: bool,
    /// Memory limit for the coordinator's DataFusion runtime.
    /// Accepts human-readable sizes: "8GB", "512MB", "4096MB".
    /// Default: "8GB". Applies to all query operator memory (sorts, joins, aggregates).
    #[serde(default = "default_coordinator_memory")]
    pub memory_limit: String,
    /// Enable spill-to-disk when memory limit is reached. Default: true.
    #[serde(default = "default_true")]
    pub spill_to_disk: bool,
    /// Directory for spill files. Must be on fast local storage (SSD recommended).
    /// Default: "/tmp/sqe-coordinator-spill".
    #[serde(default = "default_coordinator_spill_dir")]
    pub spill_dir: String,
    /// Compression for spill files. "none", "lz4" (default), or "zstd".
    #[serde(default = "default_spill_compression")]
    pub spill_compression: String,
    /// Arrow Flight IPC compression for client-facing DoGet responses.
    /// Supported values: `"lz4"` (default), `"zstd"`, `"none"`.
    #[serde(default = "default_flight_compression")]
    pub flight_compression: String,
    /// Arrow Flight IPC compression for internal shuffle (DoExchange) transfers.
    /// Supported values: `"zstd"` (default), `"lz4"`, `"none"`.
    #[serde(default = "default_shuffle_compression")]
    pub shuffle_compression: String,
}

/// IPC body compression codec for Arrow Flight transfers.
///
/// Maps directly to `arrow_ipc::CompressionType` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlightCompression {
    /// No compression.
    None,
    /// LZ4 Frame -- fast decompression, good for client-facing responses.
    Lz4,
    /// Zstandard -- better compression ratio, good for internal shuffle.
    Zstd,
}

impl FlightCompression {
    /// Parse a config string into a `FlightCompression`.
    ///
    /// Accepted values (case-insensitive): `"lz4"`, `"zstd"`, `"none"`.
    pub fn from_config(s: &str) -> crate::error::Result<Self> {
        match s.trim().to_lowercase().as_str() {
            "lz4" | "lz4_frame" => Ok(Self::Lz4),
            "zstd" | "zstandard" => Ok(Self::Zstd),
            "none" | "" => Ok(Self::None),
            other => Err(crate::error::SqeError::Config(format!(
                "Unknown Flight compression codec '{other}'. Expected: lz4, zstd, or none"
            ))),
        }
    }
}

/// TLS configuration for gRPC (Flight SQL) and worker listeners.
///
/// When `cert_file` and `key_file` are both set, the server enables TLS.
/// If omitted, the server runs in plaintext (suitable for development).
#[derive(Debug, Deserialize, Clone, Default)]
pub struct TlsConfig {
    /// Path to PEM-encoded server certificate.
    #[serde(default)]
    pub cert_file: String,
    /// Path to PEM-encoded private key.
    #[serde(default)]
    pub key_file: String,
    /// Path to PEM-encoded CA certificate for client verification (mTLS).
    /// When set, the server requires clients to present a valid certificate
    /// signed by this CA.
    #[serde(default)]
    pub ca_file: String,
}

impl TlsConfig {
    /// Returns `true` when both cert and key are configured.
    pub fn is_enabled(&self) -> bool {
        !self.cert_file.is_empty() && !self.key_file.is_empty()
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct WorkerConfig {
    #[serde(default)]
    pub coordinator_url: String,
    #[serde(default = "default_worker_flight_port")]
    pub flight_port: u16,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_memory")]
    pub memory_limit: String,
    #[serde(default = "default_true")]
    pub spill_to_disk: bool,
    #[serde(default = "default_spill_dir")]
    pub spill_dir: String,
    /// Maximum duration in seconds for a single scan task. Default: 600 (10 minutes).
    /// Set to 0 to disable the timeout.
    #[serde(default = "default_scan_timeout")]
    pub scan_timeout_secs: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            coordinator_url: String::new(),
            flight_port: default_worker_flight_port(),
            heartbeat_interval_secs: default_heartbeat(),
            memory_limit: default_memory(),
            spill_to_disk: true,
            spill_dir: default_spill_dir(),
            scan_timeout_secs: default_scan_timeout(),
        }
    }
}

/// Parse a human-readable memory size string (e.g. "1GB", "512MB", "1024") into bytes.
///
/// Supported suffixes (case-insensitive): `B`, `KB`/`K`, `MB`/`M`, `GB`/`G`, `TB`/`T`.
/// A bare number without a suffix is interpreted as bytes.
pub fn parse_memory_limit(s: &str) -> crate::error::Result<usize> {
    let s = s.trim();
    if s.is_empty() {
        return Err(crate::error::SqeError::Config(
            "Empty memory limit string".to_string(),
        ));
    }

    // Find where the numeric part ends and the suffix begins
    let (num_str, suffix) = match s.find(|c: char| !c.is_ascii_digit() && c != '.') {
        Some(idx) => (&s[..idx], s[idx..].trim().to_uppercase()),
        None => (s, String::new()),
    };

    let num: f64 = num_str.parse().map_err(|e| {
        crate::error::SqeError::Config(format!("Invalid memory limit number '{num_str}': {e}"))
    })?;

    let multiplier: f64 = match suffix.as_str() {
        "" | "B" => 1.0,
        "K" | "KB" => 1024.0,
        "M" | "MB" => 1024.0 * 1024.0,
        "G" | "GB" => 1024.0 * 1024.0 * 1024.0,
        "T" | "TB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        other => {
            return Err(crate::error::SqeError::Config(format!(
                "Unknown memory limit suffix '{other}' in '{s}'"
            )))
        }
    };

    Ok((num * multiplier) as usize)
}

/// Configuration for a single auth provider in the `[[auth.providers]]` array.
///
/// Each variant maps to a concrete `AuthProvider` implementation in `sqe-auth`.
/// The `type` field in TOML selects the variant via the serde tag.
#[derive(Debug, Deserialize, Clone)]
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
        /// Expected audience (`aud` claim). Required by default — see
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
        /// Explicit opt-in to accept tokens with any audience. Default
        /// `false`: a missing/empty `audience` then errors at startup.
        /// Setting `true` acknowledges that tokens issued for any service
        /// sharing the IdP will be accepted.
        #[serde(default)]
        allow_unbounded_audience: bool,
    },
    /// AWS IAM authentication via STS GetCallerIdentity.
    AwsIam {
        /// AWS region for STS endpoint. Default: `"us-east-1"`.
        #[serde(default = "default_aws_region")]
        region: String,
        /// Whether to validate credentials via STS call. Default: true.
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
    /// Mutual TLS client certificate authentication.
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
}

fn default_roles_claim() -> String {
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
    pub client_secret: String,
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

fn default_admin_roles() -> Vec<String> {
    vec!["service_admin".to_string(), "catalog_admin".to_string()]
}

impl AuthConfig {
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
            .field("client_secret", &"[REDACTED]")
            .field("token_endpoint", &self.token_endpoint)
            .field("token_refresh_buffer_secs", &self.token_refresh_buffer_secs)
            .field("ssl_verification", &self.ssl_verification)
            .field("providers", &format!("[{} provider(s)]", self.providers.len()))
            .field("role_mappings", &format!("[{} mapping(s)]", self.role_mappings.len()))
            .field("external", &self.external.as_ref().map(|e| format!("issuer={}", e.issuer)))
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
    pub device_authorization_endpoint: Option<String>,
    #[serde(default)]
    pub device: Option<DeviceAuthConfig>,
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DeviceAuthConfig {
    pub client_id: String,
    #[serde(default = "default_external_scopes")]
    pub scopes: Vec<String>,
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

/// Selectable catalog backend. Defaults to `Rest` so existing TOML
/// configurations keep working unchanged.
///
/// The non-REST variants point at the per-backend constructors in
/// `crates/sqe-catalog/src/backends/`; selecting one routes the
/// engine session manager through that backend's iceberg::Catalog
/// implementation instead of through the REST client. REST-specific
/// SessionCatalog methods (view DDL, commit_schema_update through
/// raw REST) error out under non-REST backends.
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum CatalogBackend {
    /// Iceberg REST (Polaris, Nessie, Unity OSS, AWS Glue REST,
    /// AWS S3 Tables). Configured via `catalog_url` + `warehouse` on
    /// CatalogConfig. AWS endpoints engage SigV4 automatically when
    /// the server's /v1/config response advertises
    /// `rest.sigv4-enabled=true`.
    #[default]
    Rest,
    /// Hive Metastore over Thrift. Requires the `hms` cargo feature
    /// on sqe-catalog. `uri` is `host:port` of the metastore.
    Hms {
        uri: String,
        #[serde(default)]
        warehouse: String,
    },
    /// AWS Glue Data Catalog over the AWS SDK. Requires the `glue`
    /// cargo feature. `region` mandatory; `endpoint` optional for
    /// LocalStack.
    Glue {
        region: String,
        #[serde(default)]
        warehouse: String,
        #[serde(default)]
        endpoint: Option<String>,
    },
    /// JDBC catalog (Postgres, MySQL, SQLite) via iceberg-catalog-sql.
    /// Requires the `sql-postgres` cargo feature. `url` is the JDBC
    /// connection string; `warehouse` is the on-disk path used for
    /// new tables when the DB doesn't carry one.
    Jdbc {
        url: String,
        #[serde(default)]
        warehouse: String,
    },
    /// AWS S3 Tables (managed Iceberg). Requires the `s3tables`
    /// cargo feature on sqe-catalog. `table_bucket_arn` is the
    /// fully-qualified ARN of the S3 Tables bucket
    /// (`arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME`).
    /// `endpoint_url` is optional and only needed when targeting a
    /// non-default S3 Tables endpoint (LocalStack, custom region
    /// override).
    S3tables {
        table_bucket_arn: String,
        #[serde(default)]
        endpoint_url: Option<String>,
    },
}

/// Per-catalog auth strategy. When present on a `CatalogConfig`,
/// overrides the session bearer token for that catalog only. Used
/// for federation where one catalog speaks to Polaris (session
/// token) and a sibling speaks to a partner Iceberg REST endpoint
/// behind its own OAuth client.
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogAuthConfig {
    /// Use the user's session bearer token from the top-level
    /// `[auth]` block. This is the default and matches V6
    /// behaviour. Configured explicitly only when the operator
    /// wants the intent visible in the TOML.
    #[default]
    SessionBearer,
    /// A pre-issued static bearer token. Read from a separate
    /// secrets file or env override at deploy time. Useful for
    /// internal gateways and integration tests.
    Static { token: String },
    /// No `Authorization` header at all. Used for public Nessie
    /// read-only access and other anonymous endpoints.
    Anonymous,
    /// OAuth2 `client_credentials` grant against a per-catalog
    /// token endpoint. The token is fetched at session-build time
    /// and reused for the session lifetime; refresh on expiry is
    /// a future change.
    ClientCredentials {
        /// Full URL of the token endpoint (e.g.
        /// `https://partner.com/oauth/tokens`).
        token_endpoint: String,
        client_id: String,
        client_secret: String,
        /// Optional scope passed in the form body. Defaults to
        /// `PRINCIPAL_ROLE:ALL` for Polaris compatibility.
        #[serde(default)]
        scope: Option<String>,
    },
    /// Rely on the AWS SDK provider chain. Used by `glue` and
    /// `s3tables` backends and by AWS REST endpoints that engage
    /// SigV4 based on `/v1/config`. No token is fetched at the
    /// SQE level; the AWS SDK handles credentials.
    Aws,
}


#[derive(Debug, Deserialize, Clone)]
pub struct CatalogConfig {
    /// REST catalog endpoint URL. Used by `CatalogBackend::Rest`
    /// (the default) for Polaris, Nessie, Unity OSS, AWS Glue REST,
    /// and AWS S3 Tables. Other backends (`Hms`, `Glue`, `Jdbc`,
    /// `S3tables`) carry their own connection details on the enum
    /// variant and ignore this field.
    ///
    /// Old configs that used `polaris_url` continue to deserialize
    /// thanks to the serde alias.
    #[serde(alias = "polaris_url")]
    pub catalog_url: String,
    #[serde(default)]
    pub warehouse: String,
    /// Backend selector. When omitted from TOML, deserialises to
    /// `CatalogBackend::Rest` so the existing `catalog_url` field
    /// keeps driving the engine. Non-REST variants source their own
    /// connection details from the enum.
    #[serde(default)]
    pub backend: CatalogBackend,
    #[serde(default = "default_cache_ttl")]
    pub metadata_cache_ttl_secs: u64,
    /// Default Iceberg table format version for new tables (2 or 3).
    #[serde(default = "default_table_format_version")]
    pub default_table_format_version: u8,
    /// Trust Iceberg sort order metadata for ALL columns, not just partition keys.
    /// When true, DataFusion may skip redundant sorts based on Iceberg metadata.
    /// Default false: safer for mixed-writer environments (Spark, Trino, SQE).
    /// Only enable when you know all data files are physically sorted.
    #[serde(default)]
    pub trust_sort_order: bool,
    /// Maximum file size in MB for the direct-read fast path.
    ///
    /// When all data files in a scan are smaller than this threshold, SQE reads
    /// each file entirely in a single S3 GET and parses Parquet from memory,
    /// bypassing iceberg-rust's `scan.to_arrow()` pipeline (which issues
    /// additional HEAD, footer, and manifest requests). For ClickBench-style
    /// queries this reduces per-query S3 overhead from 5–7 requests to 1.
    ///
    /// Set to 0 to disable the fast path and always use iceberg-rust's pipeline.
    /// Default: 3 MB.
    #[serde(default = "default_small_file_threshold_mb")]
    pub small_file_threshold_mb: u64,
    /// Parquet compression codec for writes (CTAS, INSERT, etc.).
    ///
    /// Supported values: `"zstd"` (default), `"lz4"`, `"snappy"`, `"none"`.
    /// ZSTD level 3 is used when `"zstd"` is selected — a good balance of
    /// compression ratio vs. speed for S3.
    #[serde(default = "default_parquet_compression")]
    pub parquet_compression: String,
    /// Concurrency for loading Iceberg manifests during query-time column
    /// statistics pruning and CoW write paths.
    ///
    /// Each manifest is a separate S3 GET. On wide snapshots the sequential
    /// walk dominates cold-cache plan latency; loading manifests in parallel
    /// collapses that to roughly one round trip. Warm-cache reads are served
    /// from the iceberg-rust `ObjectCache` and ignore this knob.
    ///
    /// Default: 64.
    #[serde(default = "default_manifest_concurrency")]
    pub manifest_concurrency: usize,
    /// Per-catalog auth override. When unset (the common case),
    /// SQE uses the user's session bearer token from the top-level
    /// `[auth]` block, matching V6 behaviour. Set this on
    /// `[catalogs.<name>.auth]` blocks when one catalog needs a
    /// different token source: a partner Polaris with its own
    /// OAuth client, an anonymous Nessie read-only endpoint, an
    /// AWS-SDK-provider-chain Glue endpoint, etc.
    #[serde(default)]
    pub auth: Option<CatalogAuthConfig>,
    /// Per-catalog storage override. When unset, the global
    /// `[storage]` block applies. Set this on
    /// `[catalogs.<name>.storage]` to point a single catalog at a
    /// different S3 endpoint, region, or credential pair (e.g.
    /// one Ceph cluster + one AWS S3, or a partner bucket with
    /// its own access key). Iceberg credential vending from REST
    /// catalogs still wins per-table over both this and the
    /// global block.
    #[serde(default)]
    pub storage: Option<StorageConfig>,
}

#[derive(Deserialize, Clone)]
pub struct StorageConfig {
    #[serde(default)]
    pub s3_endpoint: String,
    #[serde(default)]
    pub s3_region: String,
    #[serde(default)]
    pub s3_access_key: String,
    #[serde(default)]
    pub s3_secret_key: String,
    #[serde(default)]
    pub s3_path_style: bool,
    /// Allow plaintext HTTP for S3 endpoints. Only enable for dev/test (e.g., MinIO).
    #[serde(default)]
    pub s3_allow_http: bool,
    /// Byte-range coalescing threshold. Ranges separated by a gap of at most
    /// this many bytes are merged into a single S3 GET request. Default: "1MB".
    #[serde(default = "default_coalesce_threshold")]
    pub coalesce_threshold: String,
    /// Maximum size of the Parquet footer (metadata) cache. Default: "256MB".
    #[serde(default = "default_footer_cache_size")]
    pub footer_cache_size: String,
    /// Maximum number of concurrent byte-range requests per file. Default: 4.
    #[serde(default = "default_concurrent_requests_per_file")]
    pub concurrent_requests_per_file: usize,
    /// Maximum number of files fetched concurrently. Default: 8.
    #[serde(default = "default_max_concurrent_files")]
    pub max_concurrent_files: usize,
    /// Prefetch buffer size for overlapping footer reads. Default: "32MB".
    #[serde(default = "default_prefetch_buffer")]
    pub prefetch_buffer: String,
    /// Maximum number of S3 files to prefetch concurrently during scan.
    /// Higher values improve throughput on high-latency S3 connections.
    /// Default: 4. Set higher (8-16) for WAN or high-latency storage.
    #[serde(default = "default_prefetch_concurrency")]
    pub prefetch_concurrency: usize,

    // ── Azure ADLS Gen2 / Blob ──────────────────────────────────────────
    /// Azure storage account name (required for shared-key / SAS auth).
    #[serde(default)]
    pub azure_account: String,
    /// Azure storage account access key (shared-key auth).
    #[serde(default)]
    pub azure_access_key: String,
    /// Azure SAS token (alternative to shared key).
    #[serde(default)]
    pub azure_sas_token: String,
    /// Use the Azurite storage emulator (for local development).
    #[serde(default)]
    pub azure_use_emulator: bool,

    // ── Google Cloud Storage ────────────────────────────────────────────
    /// Path to a GCP service-account JSON key file.
    #[serde(default)]
    pub gcs_service_account_path: String,
    /// Inline GCP service-account JSON key contents.
    #[serde(default)]
    pub gcs_service_account_key: String,

    // ── Table-valued function security (issue #10) ──────────────────────
    /// File TVF (`read_parquet`, `read_csv`, `read_json`) policy. Defaults
    /// deny local-fs and arbitrary HTTP hosts so an authenticated user
    /// cannot exfiltrate `/etc/shadow`, `/proc/self/environ`, or the
    /// coordinator's service-account token, nor pivot to cloud-metadata
    /// endpoints (IMDS / GCP / Azure IMDS) on `http://169.254.169.254`.
    #[serde(default)]
    pub tvf: TvfPolicy,
}

/// Security policy for table-valued-function path arguments.
///
/// All defaults are fail-closed: cloud object stores (`s3://`, `abfss://`,
/// `gs://`, `hf://`) keep working out of the box; local paths and arbitrary
/// `http(s)://` hosts are rejected unless explicitly enabled.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct TvfPolicy {
    /// When `true`, file TVFs may read absolute paths on the coordinator /
    /// worker filesystem (e.g. `/var/data/foo.parquet`). Default: `false`
    /// because an authenticated user could read `/etc/shadow`, a mounted
    /// secret, or `/var/run/secrets/kubernetes.io/serviceaccount/token`.
    /// Enable on single-tenant deployments where every user is trusted to
    /// read the local filesystem (rare; production deployments should
    /// stage files in an object store instead).
    #[serde(default)]
    pub allow_local_paths: bool,
    /// When `true`, file TVFs may fetch from arbitrary `http(s)://` hosts.
    /// Default: `false` because IMDS lives at `http://169.254.169.254`
    /// and worker pods often run with cloud-side privileges higher than
    /// the user's. When `false`, only `allowed_http_hosts` are reachable
    /// (empty list = no HTTP at all).
    #[serde(default)]
    pub allow_http: bool,
    /// When `allow_http` is `false`, this allowlist names the hosts that
    /// file TVFs may reach. Compared case-insensitively against the URL's
    /// host (no port, no path). Examples: `["data.example.com",
    /// "huggingface.co"]`. Wildcards are not supported; add each fully-
    /// qualified host explicitly.
    #[serde(default)]
    pub allowed_http_hosts: Vec<String>,
}

impl TvfPolicy {
    /// Return `Ok(())` if a TVF may legitimately reference `path`, or an
    /// error describing why it cannot. Object-store schemes (`s3://`,
    /// `s3a://`, `abfss://`, `abfs://`, `azure://`, `az://`, `gs://`,
    /// `gcs://`, `hf://`) are always allowed because they go through
    /// SQE's credential-managed stores. Local paths (`/...`, `file://`,
    /// no scheme) need `allow_local_paths = true`. `http(s)://` needs
    /// either `allow_http = true` or an exact match in
    /// `allowed_http_hosts`.
    pub fn check(&self, path: &str) -> Result<(), String> {
        let lower = path.to_lowercase();
        // Object-store schemes — always allowed; credentials come from
        // `[storage.*]`, not the TVF user's filesystem.
        if lower.starts_with("s3://")
            || lower.starts_with("s3a://")
            || lower.starts_with("abfss://")
            || lower.starts_with("abfs://")
            || lower.starts_with("azure://")
            || lower.starts_with("az://")
            || lower.starts_with("gs://")
            || lower.starts_with("gcs://")
            || lower.starts_with("hf://")
        {
            return Ok(());
        }

        if lower.starts_with("http://") || lower.starts_with("https://") {
            if self.allow_http {
                return Ok(());
            }
            // Parse out the host (lowercased) and check the allowlist.
            // Keeping this local + total avoids panicking on malformed input.
            let after_scheme = lower
                .split_once("://")
                .map(|x| x.1)
                .unwrap_or("");
            let host_with_port = after_scheme.split('/').next().unwrap_or("");
            let host = host_with_port
                .split(':')
                .next()
                .unwrap_or("")
                .trim();
            if host.is_empty() {
                return Err(format!(
                    "TVF: malformed URL '{path}' (missing host)"
                ));
            }
            if self
                .allowed_http_hosts
                .iter()
                .any(|h| h.eq_ignore_ascii_case(host))
            {
                return Ok(());
            }
            return Err(format!(
                "TVF: HTTP host '{host}' is not in `[storage.tvf] allowed_http_hosts`. \
                 Add the host or set `allow_http = true` to permit arbitrary hosts."
            ));
        }

        // Everything else — bare path, `file://`, `/...` — is a local path.
        if self.allow_local_paths {
            return Ok(());
        }
        Err(format!(
            "TVF: local filesystem paths are disabled. \
             Path '{path}' is not an object-store URL (s3://, abfss://, gs://, hf://, ...). \
             Set `[storage.tvf] allow_local_paths = true` to permit local reads, \
             or stage the file in an object store."
        ))
    }

    /// Validate an S3-style endpoint override (the `endpoint =>` argument
    /// on `read_parquet` / `read_csv` / `read_json`). Issue #46 closed
    /// the SSRF gap left by issue #10: the path argument was checked but
    /// the endpoint override flowed straight into `AmazonS3Builder`, so
    /// `read_parquet('s3://x/y', endpoint => 'http://169.254.169.254/...')`
    /// still pivoted to IMDS.
    ///
    /// Empty endpoints are allowed (the operator's storage config kicks
    /// in). HTTP/HTTPS endpoints are validated against the same allowlist
    /// as path arguments. Bare-host endpoints (e.g. `minio.local:9000`)
    /// are allowed since they cannot reach IMDS over its expected scheme.
    pub fn check_endpoint(&self, endpoint: &str) -> Result<(), String> {
        if endpoint.is_empty() {
            return Ok(());
        }
        let lower = endpoint.to_lowercase();
        if !lower.starts_with("http://") && !lower.starts_with("https://") {
            return Ok(());
        }
        if self.allow_http {
            return Ok(());
        }
        let after_scheme = lower
            .split_once("://")
            .map(|x| x.1)
            .unwrap_or("");
        let host_with_port = after_scheme.split('/').next().unwrap_or("");
        let host = host_with_port
            .split(':')
            .next()
            .unwrap_or("")
            .trim();
        if host.is_empty() {
            return Err(format!(
                "TVF: malformed endpoint '{endpoint}' (missing host)"
            ));
        }
        if self
            .allowed_http_hosts
            .iter()
            .any(|h| h.eq_ignore_ascii_case(host))
        {
            return Ok(());
        }
        Err(format!(
            "TVF: endpoint host '{host}' is not in `[storage.tvf] allowed_http_hosts`. \
             Inline `endpoint => '{endpoint}'` was rejected to prevent SSRF \
             to metadata services. Add the host or set `allow_http = true` \
             to permit arbitrary hosts."
        ))
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_path_style: false,
            s3_allow_http: false,
            coalesce_threshold: default_coalesce_threshold(),
            footer_cache_size: default_footer_cache_size(),
            concurrent_requests_per_file: default_concurrent_requests_per_file(),
            max_concurrent_files: default_max_concurrent_files(),
            prefetch_buffer: default_prefetch_buffer(),
            prefetch_concurrency: default_prefetch_concurrency(),
            azure_account: String::new(),
            azure_access_key: String::new(),
            azure_sas_token: String::new(),
            azure_use_emulator: false,
            gcs_service_account_path: String::new(),
            gcs_service_account_key: String::new(),
            tvf: TvfPolicy::default(),
        }
    }
}

fn default_coalesce_threshold() -> String { "1MB".to_string() }
fn default_footer_cache_size() -> String { "256MB".to_string() }
fn default_concurrent_requests_per_file() -> usize { 4 }
fn default_max_concurrent_files() -> usize { 8 }
fn default_prefetch_buffer() -> String { "32MB".to_string() }
fn default_prefetch_concurrency() -> usize { 4 }

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageConfig")
            .field("s3_endpoint", &self.s3_endpoint)
            .field("s3_region", &self.s3_region)
            .field("s3_access_key", &"[REDACTED]")
            .field("s3_secret_key", &"[REDACTED]")
            .field("s3_path_style", &self.s3_path_style)
            .field("s3_allow_http", &self.s3_allow_http)
            .field("coalesce_threshold", &self.coalesce_threshold)
            .field("footer_cache_size", &self.footer_cache_size)
            .field("concurrent_requests_per_file", &self.concurrent_requests_per_file)
            .field("max_concurrent_files", &self.max_concurrent_files)
            .field("prefetch_buffer", &self.prefetch_buffer)
            .field("prefetch_concurrency", &self.prefetch_concurrency)
            .field("azure_account", &self.azure_account)
            .field("azure_access_key", &"[REDACTED]")
            .field("azure_sas_token", &"[REDACTED]")
            .field("azure_use_emulator", &self.azure_use_emulator)
            .field("gcs_service_account_path", &self.gcs_service_account_path)
            .field("gcs_service_account_key", &"[REDACTED]")
            .finish()
    }
}

/// Access control backend for GRANT/REVOKE/SHOW GRANTS SQL.
///
/// Supports multiple backends:
/// - `"chameleon"` -- Chameleon platform API (GROUP/USER grantees)
/// - `"polaris"` -- Apache Polaris 1.3 native (PRINCIPAL/PRINCIPAL_ROLE/CATALOG_ROLE)
/// - `"none"` -- disabled (default)
///
/// ```toml
/// [access_control]
/// backend = "chameleon"
/// url = "http://backend:8080/api/platform/v1/access"
/// ```
#[derive(Debug, Deserialize, Clone)]
pub struct AccessControlConfig {
    /// Backend type: "chameleon", "polaris", or "none" (disabled).
    #[serde(default = "default_access_control_backend")]
    pub backend: String,
    /// Backend API URL.
    /// Chameleon: http://backend:port/api/platform/v1/access
    /// Polaris: http://polaris:8181/api/management/v1 (Polaris management API)
    #[serde(default)]
    pub url: String,
    /// Request timeout in seconds.
    #[serde(default = "default_access_control_timeout")]
    pub timeout_secs: u64,
    /// Optional: Polaris service account client_id for management API.
    /// When absent, the user's passthrough OIDC token is used.
    #[serde(default)]
    pub client_id: Option<String>,
    /// Optional: Polaris service account client_secret for management API.
    /// When absent, the user's passthrough OIDC token is used.
    #[serde(default)]
    pub client_secret: Option<String>,
}

impl Default for AccessControlConfig {
    fn default() -> Self {
        Self {
            backend: "none".to_string(),
            url: String::new(),
            timeout_secs: 30,
            client_id: None,
            client_secret: None,
        }
    }
}

fn default_access_control_backend() -> String { "none".to_string() }
fn default_access_control_timeout() -> u64 { 30 }

#[derive(Debug, Deserialize, Clone)]
pub struct PolicyConfig {
    #[serde(default = "default_passthrough")]
    pub engine: String,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self { engine: "passthrough".to_string() }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,
    #[serde(default)]
    pub otlp_endpoint: String,
    #[serde(default)]
    pub audit_log_path: String,
    /// OTel trace sampling rate (0.0 to 1.0). Default: 0.01 (1%).
    /// Set to 1.0 to trace all queries (expensive). Set to 0.0 to disable tracing.
    #[serde(default = "default_trace_sample_rate")]
    pub trace_sample_rate: f64,
    #[serde(default)]
    pub openlineage: OpenLineageConfig,
}

fn default_trace_sample_rate() -> f64 {
    0.01
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            prometheus_port: 9090,
            otlp_endpoint: String::new(),
            audit_log_path: String::new(),
            trace_sample_rate: default_trace_sample_rate(),
            openlineage: OpenLineageConfig::default(),
        }
    }
}

/// OpenLineage emitter configuration.
///
/// Controls whether SQE emits OpenLineage events for executed statements
/// and where those events are delivered (file sink, HTTP endpoint, or both).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenLineageConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_ol_job_namespace")]
    pub job_namespace: String,
    #[serde(default)]
    pub producer: String,
    #[serde(default)]
    pub emit_selects: bool,
    #[serde(default)]
    pub file_path: String,
    #[serde(default)]
    pub http_endpoint: String,
    #[serde(default = "default_ol_auth_mode")]
    pub auth_mode: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_ol_http_timeout")]
    pub http_timeout_ms: u64,
    #[serde(default = "default_ol_http_retry")]
    pub http_retry_attempts: u32,
    #[serde(default)]
    pub spool_path: String,
    #[serde(default = "default_ol_spool_cap")]
    pub spool_max_bytes: u64,
    #[serde(default = "default_ol_replay_secs")]
    pub replay_interval_secs: u64,
    #[serde(default = "default_ol_channel_cap")]
    pub channel_capacity: usize,
}

impl Default for OpenLineageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            job_namespace: default_ol_job_namespace(),
            producer: String::new(),
            emit_selects: false,
            file_path: String::new(),
            http_endpoint: String::new(),
            auth_mode: default_ol_auth_mode(),
            api_key: String::new(),
            http_timeout_ms: default_ol_http_timeout(),
            http_retry_attempts: default_ol_http_retry(),
            spool_path: String::new(),
            spool_max_bytes: default_ol_spool_cap(),
            replay_interval_secs: default_ol_replay_secs(),
            channel_capacity: default_ol_channel_cap(),
        }
    }
}

impl OpenLineageConfig {
    /// Validate that the OpenLineage configuration is internally consistent.
    ///
    /// Rules:
    /// - When enabled, at least one sink (file_path or http_endpoint) must be set.
    /// - `auth_mode = "bearer"` requires `api_key` to be set.
    /// - `spool_path` only makes sense when `http_endpoint` is set (the spool
    ///   buffers events that fail to deliver over HTTP).
    /// - When enabled, `spool_max_bytes` must be at least 1 MiB so the spool
    ///   has room for at least a few events.
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled && self.file_path.is_empty() && self.http_endpoint.is_empty() {
            return Err(
                "openlineage.enabled requires at least one of file_path or http_endpoint".into(),
            );
        }
        if self.auth_mode == "bearer" && self.api_key.is_empty() {
            return Err(
                "openlineage.auth_mode = \"bearer\" requires api_key to be set".into(),
            );
        }
        if !self.spool_path.is_empty() && self.http_endpoint.is_empty() {
            return Err(
                "openlineage.spool_path requires http_endpoint to be set".into(),
            );
        }
        if self.enabled && self.spool_max_bytes < 1024 * 1024 {
            return Err("openlineage.spool_max_bytes must be at least 1 MiB".into());
        }
        Ok(())
    }
}

fn default_ol_job_namespace() -> String {
    "sqe".into()
}
fn default_ol_auth_mode() -> String {
    "none".into()
}
fn default_ol_http_timeout() -> u64 {
    5000
}
fn default_ol_http_retry() -> u32 {
    1
}
fn default_ol_spool_cap() -> u64 {
    100 * 1024 * 1024
}
fn default_ol_replay_secs() -> u64 {
    30
}
fn default_ol_channel_cap() -> usize {
    10_000
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_per_user_rpm")]
    pub per_user_queries_per_minute: u32,
    #[serde(default = "default_global_rpm")]
    pub global_queries_per_minute: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            per_user_queries_per_minute: default_per_user_rpm(),
            global_queries_per_minute: default_global_rpm(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct SessionConfig {
    /// Idle timeout in seconds. Sessions with no activity for this duration are expired.
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
    /// Absolute timeout in seconds. Sessions older than this are expired regardless of activity.
    #[serde(default = "default_absolute_timeout")]
    pub absolute_timeout_secs: u64,
    /// Session persistence backend. Options: "memory" (default), "file".
    /// "memory" = in-process only (lost on restart).
    /// "file" = periodic snapshot to disk (survives restart, best-effort).
    #[serde(default = "default_session_persistence")]
    pub persistence: String,
    /// Path for file-based session persistence. Default: "/tmp/sqe-sessions.json"
    #[serde(default = "default_session_persistence_path")]
    pub persistence_path: String,
    /// How often to snapshot sessions to disk (seconds). Default: 60.
    #[serde(default = "default_session_snapshot_interval")]
    pub snapshot_interval_secs: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            idle_timeout_secs: default_idle_timeout(),
            absolute_timeout_secs: default_absolute_timeout(),
            persistence: default_session_persistence(),
            persistence_path: default_session_persistence_path(),
            snapshot_interval_secs: default_session_snapshot_interval(),
        }
    }
}

fn default_idle_timeout() -> u64 { 900 }       // 15 minutes
fn default_absolute_timeout() -> u64 { 28800 }  // 8 hours
fn default_session_persistence() -> String { "memory".to_string() }
fn default_session_persistence_path() -> String { "/tmp/sqe-sessions.json".to_string() }
fn default_session_snapshot_interval() -> u64 { 60 }
fn default_query_timeout() -> u64 { 300 }       // 5 minutes
fn default_max_result_rows() -> usize { 1_000_000 }
fn default_max_concurrent_queries() -> usize { 100 }
fn default_slow_query_threshold() -> u64 { 30 }
fn default_max_query_memory() -> String { "256MB".to_string() }
fn default_distribution_threshold() -> String { "128MB".to_string() }
fn default_distribution_file_threshold() -> usize { 4 }
fn default_target_task_size() -> String { "256MB".to_string() }
fn default_sort_mode() -> String { "adaptive".to_string() }
fn default_late_mat_min_projection_cols() -> usize { 1 }
fn default_star_schema_min_ratio() -> usize { 10 }

fn default_coordinator_memory() -> String { "8GB".to_string() }
fn default_coordinator_spill_dir() -> String { "/tmp/sqe-coordinator-spill".to_string() }
fn default_spill_compression() -> String { "lz4".to_string() }
fn default_flight_compression() -> String { "lz4".to_string() }
fn default_shuffle_compression() -> String { "zstd".to_string() }

fn default_flight_port() -> u16 { 50051 }
fn default_trino_port() -> u16 { 8080 }
fn default_mode() -> String { "hybrid".to_string() }
fn default_worker_flight_port() -> u16 { 50052 }
fn default_heartbeat() -> u64 { 5 }
fn default_memory() -> String { "8GB".to_string() }
fn default_spill_dir() -> String { "/tmp/sqe-spill".to_string() }
fn default_scan_timeout() -> u64 { 600 }         // 10 minutes
fn default_refresh_buffer() -> u64 { 60 }
fn default_true() -> bool { true }
fn default_cache_ttl() -> u64 { 30 }
fn default_table_format_version() -> u8 { 2 }
fn default_small_file_threshold_mb() -> u64 { 3 }
fn default_parquet_compression() -> String { "zstd".to_string() }
fn default_manifest_concurrency() -> usize { 64 }
fn default_passthrough() -> String { "passthrough".to_string() }
fn default_prometheus_port() -> u16 { 9090 }
fn default_per_user_rpm() -> u32 { 60 }
fn default_global_rpm() -> u32 { 1000 }

impl SqeConfig {
    /// Default name for the legacy `[catalog]` block when the new
    /// `[catalogs.*]` map is also populated. Picked to match what
    /// embedded mode (`sqe-cli --warehouse <path>`) calls its
    /// single catalog so users moving between embedded and cluster
    /// see the same SQL identifier.
    pub const LEGACY_CATALOG_NAME: &'static str = "iceberg";

    /// Flatten the legacy `[catalog]` block plus the `[catalogs.*]`
    /// map into a single ordered list of named catalogs. Order is
    /// stable: legacy block first if it's the only one, otherwise
    /// `[catalogs.*]` entries in alphabetical order, with the legacy
    /// block joining only when explicitly named via
    /// `query.default_catalog == Self::LEGACY_CATALOG_NAME` or when
    /// `[catalogs.*]` is empty.
    ///
    /// Practical outcomes:
    /// - Legacy single-catalog config: returns one entry,
    ///   `("iceberg", &self.catalog)`.
    /// - Pure new-style config (only `[catalogs.*]` set, no
    ///   `default_catalog`): returns the named catalogs sorted by
    ///   name. The legacy `[catalog]` block exists in the struct but
    ///   is dropped — operators set it to a placeholder REST URL to
    ///   satisfy the deserializer and the runtime ignores it.
    /// - Mixed (`[catalogs.*]` populated AND `default_catalog =
    ///   "iceberg"`): legacy block joins under name "iceberg",
    ///   alongside the named entries.
    ///
    /// The returned `&CatalogConfig` references borrow from `self`,
    /// so the caller doesn't pay clone cost during dispatch.
    pub fn flattened_catalogs(&self) -> Vec<(String, &CatalogConfig)> {
        if self.catalogs.is_empty() {
            return vec![(Self::LEGACY_CATALOG_NAME.to_string(), &self.catalog)];
        }
        let mut out: Vec<(String, &CatalogConfig)> = self
            .catalogs
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect();
        // Stable order so DataFusion catalog registration produces a
        // deterministic information_schema ordering and the welcome
        // banner is reproducible.
        out.sort_by(|a, b| a.0.cmp(&b.0));

        // If the operator explicitly named the legacy block, fold it
        // in (or replace if the name collides).
        if let Some(name) = self
            .query
            .default_catalog
            .as_deref()
            .filter(|n| !n.is_empty())
        {
            // Remove any existing entry with the legacy name so the
            // legacy block wins (operator set it explicitly; that's
            // an opt-in to use the legacy block as that name).
            out.retain(|(n, _)| n != name);
            out.push((name.to_string(), &self.catalog));
            out.sort_by(|a, b| a.0.cmp(&b.0));
        }
        out
    }

    /// The name of the catalog that DataFusion treats as the
    /// "default" for unqualified table names. Resolution order:
    /// 1. `query.default_catalog` if set and non-empty.
    /// 2. The first entry from `flattened_catalogs()`.
    /// 3. `Self::LEGACY_CATALOG_NAME` as a last resort (no
    ///    catalogs flattened, which is a config error caught
    ///    elsewhere).
    pub fn resolve_default_catalog(&self) -> String {
        if let Some(name) = self
            .query
            .default_catalog
            .as_deref()
            .filter(|n| !n.is_empty())
        {
            return name.to_string();
        }
        self.flattened_catalogs()
            .first()
            .map(|(n, _)| n.clone())
            .unwrap_or_else(|| Self::LEGACY_CATALOG_NAME.to_string())
    }
}

impl SqeConfig {
    /// Validate configuration: required fields and port conflicts.
    pub fn validate(&self) -> crate::error::Result<()> {
        let mut errors = Vec::new();

        // Required fields
        //
        // When `auth.providers` is configured, the legacy fields (client_id,
        // keycloak_url, token_endpoint) are not required.
        let has_providers = !self.auth.providers.is_empty();

        if !has_providers && self.auth.client_id.trim().is_empty() {
            errors.push("auth.client_id is required".to_string());
        }
        if self.catalog.catalog_url.trim().is_empty() {
            errors.push(
                "catalog.catalog_url is required (TOML field; or env \
                 SQE_CATALOG__CATALOG_URL). Legacy names `polaris_url` / \
                 SQE_CATALOG__POLARIS_URL also work via serde alias."
                    .to_string(),
            );
        }
        if !has_providers
            && self.auth.keycloak_url.trim().is_empty()
            && self.auth.token_endpoint.trim().is_empty()
        {
            errors.push(
                "at least one of auth.keycloak_url or auth.token_endpoint must be set, or configure auth.providers"
                    .to_string(),
            );
        }

        // Port conflicts
        if self.coordinator.flight_sql_port == self.coordinator.trino_http_port
            && self.coordinator.trino_http_port > 0
        {
            errors.push(format!(
                "port conflict: coordinator.flight_sql_port and coordinator.trino_http_port are both {}",
                self.coordinator.flight_sql_port
            ));
        }
        if self.coordinator.flight_sql_port == self.metrics.prometheus_port {
            errors.push(format!(
                "port conflict: coordinator.flight_sql_port and metrics.prometheus_port are both {}",
                self.coordinator.flight_sql_port
            ));
        }

        // TLS validation: if one of cert/key is set, both must be set
        let tls = &self.coordinator.tls;
        if !tls.cert_file.is_empty() && tls.key_file.is_empty() {
            errors.push("tls.cert_file is set but tls.key_file is missing".to_string());
        }
        if tls.cert_file.is_empty() && !tls.key_file.is_empty() {
            errors.push("tls.key_file is set but tls.cert_file is missing".to_string());
        }
        if tls.is_enabled() {
            if !std::path::Path::new(&tls.cert_file).exists() {
                errors.push(format!("tls.cert_file '{}' not found", tls.cert_file));
            }
            if !std::path::Path::new(&tls.key_file).exists() {
                errors.push(format!("tls.key_file '{}' not found", tls.key_file));
            }
            if !tls.ca_file.is_empty() && !std::path::Path::new(&tls.ca_file).exists() {
                errors.push(format!("tls.ca_file '{}' not found", tls.ca_file));
            }
        }

        // Parquet compression validation
        let valid_compressions = ["zstd", "lz4", "snappy", "none"];
        if !valid_compressions.contains(&self.catalog.parquet_compression.to_lowercase().as_str()) {
            errors.push(format!(
                "catalog.parquet_compression '{}' is not supported; valid options: {}",
                self.catalog.parquet_compression,
                valid_compressions.join(", ")
            ));
        }

        // Distributed-mode worker secret is required unless explicitly waived.
        if !self.coordinator.worker_urls.is_empty()
            && self.coordinator.worker_secret.is_empty()
            && !self.coordinator.allow_unauthenticated_workers
        {
            errors.push(
                "coordinator.worker_urls is set but coordinator.worker_secret is empty. \
                 Any TCP-reachable client could register as a worker and receive query \
                 fragments with user bearer tokens. Set worker_secret (recommended), or \
                 explicitly set coordinator.allow_unauthenticated_workers = true to opt out."
                    .to_string(),
            );
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(crate::error::SqeError::Config(
                format!("config error: {}", errors.join("; ")),
            ))
        }
    }

    pub fn load(path: &str) -> crate::error::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| crate::error::SqeError::Config(format!("Failed to read {path}: {e}")))?;
        let mut config: Self = toml::from_str(&content)
            .map_err(|e| crate::error::SqeError::Config(format!("Failed to parse config: {e}")))?;
        config.apply_env_overrides();
        config.log_deprecation_warnings();
        Ok(config)
    }

    /// Log warnings for deprecated config keys that still work but will be removed.
    fn log_deprecation_warnings(&self) {
        if !self.auth.keycloak_url.is_empty() {
            tracing::warn!("config key 'auth.keycloak_url' is deprecated — the OIDC password grant provider works with any OIDC-compliant endpoint, not just Keycloak. This key will continue to work but may be renamed in a future release.");
        }
    }

    /// Apply environment variable overrides. Convention: `SQE_<SECTION>__<FIELD>`.
    /// E.g. `SQE_AUTH__KEYCLOAK_URL=https://...` overrides `auth.keycloak_url`.
    fn apply_env_overrides(&mut self) {
        // Coordinator
        env_override_u16("SQE_COORDINATOR__FLIGHT_SQL_PORT", &mut self.coordinator.flight_sql_port);
        env_override_u16("SQE_COORDINATOR__TRINO_HTTP_PORT", &mut self.coordinator.trino_http_port);
        env_override_str("SQE_COORDINATOR__MODE", &mut self.coordinator.mode);
        env_override_bool("SQE_COORDINATOR__DEBUG", &mut self.coordinator.debug);
        env_override_str("SQE_COORDINATOR__WORKER_SECRET", &mut self.coordinator.worker_secret);
        env_override_bool(
            "SQE_COORDINATOR__ALLOW_UNAUTHENTICATED_WORKERS",
            &mut self.coordinator.allow_unauthenticated_workers,
        );
        env_override_str("SQE_TLS__CERT_FILE", &mut self.coordinator.tls.cert_file);
        env_override_str("SQE_TLS__KEY_FILE", &mut self.coordinator.tls.key_file);
        env_override_str("SQE_TLS__CA_FILE", &mut self.coordinator.tls.ca_file);

        // Worker
        env_override_str("SQE_WORKER__COORDINATOR_URL", &mut self.worker.coordinator_url);
        env_override_u16("SQE_WORKER__FLIGHT_PORT", &mut self.worker.flight_port);
        env_override_u64("SQE_WORKER__HEARTBEAT_INTERVAL_SECS", &mut self.worker.heartbeat_interval_secs);
        env_override_str("SQE_WORKER__MEMORY_LIMIT", &mut self.worker.memory_limit);
        env_override_bool("SQE_WORKER__SPILL_TO_DISK", &mut self.worker.spill_to_disk);
        env_override_str("SQE_WORKER__SPILL_DIR", &mut self.worker.spill_dir);
        env_override_u64("SQE_WORKER__SCAN_TIMEOUT_SECS", &mut self.worker.scan_timeout_secs);

        // Auth
        env_override_str("SQE_AUTH__KEYCLOAK_URL", &mut self.auth.keycloak_url);
        env_override_str("SQE_AUTH__REALM", &mut self.auth.realm);
        env_override_str("SQE_AUTH__CLIENT_ID", &mut self.auth.client_id);
        env_override_str("SQE_AUTH__CLIENT_SECRET", &mut self.auth.client_secret);
        env_override_str("SQE_AUTH__TOKEN_ENDPOINT", &mut self.auth.token_endpoint);
        env_override_u64("SQE_AUTH__TOKEN_REFRESH_BUFFER_SECS", &mut self.auth.token_refresh_buffer_secs);
        env_override_bool("SQE_AUTH__SSL_VERIFICATION", &mut self.auth.ssl_verification);
        env_override_str("SQE_AUTH__ROLES_CLAIM", &mut self.auth.roles_claim);

        // Auth providers: secrets must be injectable via env so Kubernetes
        // Secret mounts, Vault Agent, and External Secrets Operator can rotate
        // without rewriting TOML. Convention: `SQE_AUTH__PROVIDERS__<N>__<FIELD>`
        // where `<N>` is the zero-based index in the `[[auth.providers]]`
        // array. Today only `client_secret` is wired through, matching the
        // fields where the bug bit (issue #14). Token endpoints and
        // discovery URLs stay in TOML.
        for (idx, provider) in self.auth.providers.iter_mut().enumerate() {
            let env_name = format!("SQE_AUTH__PROVIDERS__{idx}__CLIENT_SECRET");
            match provider {
                AuthProviderConfig::OidcPassword { client_secret, .. } => {
                    env_override_str(&env_name, client_secret);
                }
                AuthProviderConfig::ClientCredentials { client_secret, .. } => {
                    env_override_str(&env_name, client_secret);
                }
                AuthProviderConfig::TokenExchange { client_secret, .. } => {
                    if let Some(secret) = client_secret.as_mut() {
                        env_override_str(&env_name, secret);
                    } else if let Ok(v) = std::env::var(&env_name) {
                        if !v.is_empty() {
                            *client_secret = Some(v);
                        }
                    }
                }
                // Variants without a client_secret field: nothing to override.
                AuthProviderConfig::BearerToken { .. }
                | AuthProviderConfig::AwsIam { .. }
                | AuthProviderConfig::ApiKey { .. }
                | AuthProviderConfig::Mtls { .. }
                | AuthProviderConfig::Anonymous { .. } => {}
            }
        }

        // Catalog
        // SQE_CATALOG__CATALOG_URL is the canonical env var; the legacy
        // SQE_CATALOG__POLARIS_URL is honoured first for backwards-compat
        // and overridden by the new name when both are set.
        env_override_str("SQE_CATALOG__POLARIS_URL", &mut self.catalog.catalog_url);
        env_override_str("SQE_CATALOG__CATALOG_URL", &mut self.catalog.catalog_url);
        env_override_str("SQE_CATALOG__WAREHOUSE", &mut self.catalog.warehouse);
        env_override_u64("SQE_CATALOG__METADATA_CACHE_TTL_SECS", &mut self.catalog.metadata_cache_ttl_secs);
        env_override_u8("SQE_CATALOG__DEFAULT_TABLE_FORMAT_VERSION", &mut self.catalog.default_table_format_version);
        env_override_usize("SQE_CATALOG__MANIFEST_CONCURRENCY", &mut self.catalog.manifest_concurrency);

        // Storage
        env_override_str("SQE_STORAGE__S3_ENDPOINT", &mut self.storage.s3_endpoint);
        env_override_str("SQE_STORAGE__S3_REGION", &mut self.storage.s3_region);
        env_override_str("SQE_STORAGE__S3_ACCESS_KEY", &mut self.storage.s3_access_key);
        env_override_str("SQE_STORAGE__S3_SECRET_KEY", &mut self.storage.s3_secret_key);
        env_override_bool("SQE_STORAGE__S3_PATH_STYLE", &mut self.storage.s3_path_style);
        env_override_bool("SQE_STORAGE__S3_ALLOW_HTTP", &mut self.storage.s3_allow_http);
        env_override_usize("SQE_STORAGE__PREFETCH_CONCURRENCY", &mut self.storage.prefetch_concurrency);
        env_override_str("SQE_STORAGE__AZURE_ACCOUNT", &mut self.storage.azure_account);
        env_override_str("SQE_STORAGE__AZURE_ACCESS_KEY", &mut self.storage.azure_access_key);
        env_override_str("SQE_STORAGE__AZURE_SAS_TOKEN", &mut self.storage.azure_sas_token);
        env_override_bool("SQE_STORAGE__AZURE_USE_EMULATOR", &mut self.storage.azure_use_emulator);
        env_override_str("SQE_STORAGE__GCS_SERVICE_ACCOUNT_PATH", &mut self.storage.gcs_service_account_path);
        env_override_str("SQE_STORAGE__GCS_SERVICE_ACCOUNT_KEY", &mut self.storage.gcs_service_account_key);

        // Policy
        env_override_str("SQE_POLICY__ENGINE", &mut self.policy.engine);

        // Metrics
        env_override_u16("SQE_METRICS__PROMETHEUS_PORT", &mut self.metrics.prometheus_port);
        env_override_str("SQE_METRICS__OTLP_ENDPOINT", &mut self.metrics.otlp_endpoint);
        env_override_str("SQE_METRICS__AUDIT_LOG_PATH", &mut self.metrics.audit_log_path);

        // Metrics: OpenLineage
        env_override_bool(
            "SQE_METRICS__OPENLINEAGE__ENABLED",
            &mut self.metrics.openlineage.enabled,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__JOB_NAMESPACE",
            &mut self.metrics.openlineage.job_namespace,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__PRODUCER",
            &mut self.metrics.openlineage.producer,
        );
        env_override_bool(
            "SQE_METRICS__OPENLINEAGE__EMIT_SELECTS",
            &mut self.metrics.openlineage.emit_selects,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__FILE_PATH",
            &mut self.metrics.openlineage.file_path,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__HTTP_ENDPOINT",
            &mut self.metrics.openlineage.http_endpoint,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__AUTH_MODE",
            &mut self.metrics.openlineage.auth_mode,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__API_KEY",
            &mut self.metrics.openlineage.api_key,
        );
        env_override_u64(
            "SQE_METRICS__OPENLINEAGE__HTTP_TIMEOUT_MS",
            &mut self.metrics.openlineage.http_timeout_ms,
        );
        env_override_u32(
            "SQE_METRICS__OPENLINEAGE__HTTP_RETRY_ATTEMPTS",
            &mut self.metrics.openlineage.http_retry_attempts,
        );
        env_override_str(
            "SQE_METRICS__OPENLINEAGE__SPOOL_PATH",
            &mut self.metrics.openlineage.spool_path,
        );
        env_override_u64(
            "SQE_METRICS__OPENLINEAGE__SPOOL_MAX_BYTES",
            &mut self.metrics.openlineage.spool_max_bytes,
        );
        env_override_u64(
            "SQE_METRICS__OPENLINEAGE__REPLAY_INTERVAL_SECS",
            &mut self.metrics.openlineage.replay_interval_secs,
        );
        env_override_usize(
            "SQE_METRICS__OPENLINEAGE__CHANNEL_CAPACITY",
            &mut self.metrics.openlineage.channel_capacity,
        );

        // Rate limit
        env_override_bool("SQE_RATE_LIMIT__ENABLED", &mut self.rate_limit.enabled);
        env_override_u32("SQE_RATE_LIMIT__PER_USER_QUERIES_PER_MINUTE", &mut self.rate_limit.per_user_queries_per_minute);
        env_override_u32("SQE_RATE_LIMIT__GLOBAL_QUERIES_PER_MINUTE", &mut self.rate_limit.global_queries_per_minute);

        // Session
        env_override_u64("SQE_SESSION__IDLE_TIMEOUT_SECS", &mut self.session.idle_timeout_secs);
        env_override_u64("SQE_SESSION__ABSOLUTE_TIMEOUT_SECS", &mut self.session.absolute_timeout_secs);

        // Query
        env_override_u64("SQE_QUERY__TIMEOUT_SECS", &mut self.query.timeout_secs);
    }
}

fn env_override_str(key: &str, target: &mut String) {
    if let Ok(val) = std::env::var(key) {
        *target = val;
    }
}

fn env_override_u8(key: &str, target: &mut u8) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid u8, ignoring");
        }
    }
}

fn env_override_u32(key: &str, target: &mut u32) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid u32, ignoring");
        }
    }
}

fn env_override_u16(key: &str, target: &mut u16) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid u16, ignoring");
        }
    }
}

fn env_override_u64(key: &str, target: &mut u64) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid u64, ignoring");
        }
    }
}

fn env_override_bool(key: &str, target: &mut bool) {
    if let Ok(val) = std::env::var(key) {
        match val.to_lowercase().as_str() {
            "true" | "1" | "yes" => *target = true,
            "false" | "0" | "no" => *target = false,
            _ => tracing::warn!("{key}={val:?} is not a valid bool, ignoring"),
        }
    }
}

fn env_override_usize(key: &str, target: &mut usize) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid usize, ignoring");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_limit_bytes() {
        assert_eq!(parse_memory_limit("1024").unwrap(), 1024);
        assert_eq!(parse_memory_limit("1024B").unwrap(), 1024);
        assert_eq!(parse_memory_limit("1024b").unwrap(), 1024);
    }

    #[test]
    fn test_parse_memory_limit_kilobytes() {
        assert_eq!(parse_memory_limit("1K").unwrap(), 1024);
        assert_eq!(parse_memory_limit("1KB").unwrap(), 1024);
        assert_eq!(parse_memory_limit("2kb").unwrap(), 2048);
    }

    #[test]
    fn test_parse_memory_limit_megabytes() {
        assert_eq!(parse_memory_limit("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_limit("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_limit("512MB").unwrap(), 512 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_limit_gigabytes() {
        assert_eq!(parse_memory_limit("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_limit("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_limit("8GB").unwrap(), 8 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_limit_terabytes() {
        assert_eq!(
            parse_memory_limit("1TB").unwrap(),
            1024 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_parse_memory_limit_whitespace() {
        assert_eq!(parse_memory_limit("  1GB  ").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_limit_empty() {
        assert!(parse_memory_limit("").is_err());
        assert!(parse_memory_limit("   ").is_err());
    }

    #[test]
    fn test_parse_memory_limit_invalid() {
        assert!(parse_memory_limit("abc").is_err());
        assert!(parse_memory_limit("GB").is_err());
    }

    #[test]
    fn test_parse_memory_limit_unknown_suffix() {
        assert!(parse_memory_limit("100XB").is_err());
    }

    #[test]
    fn test_distribution_threshold_config() {
        let config = QueryConfig::default();
        assert_eq!(config.distribution_threshold, "128MB");
        assert_eq!(config.distribution_file_threshold, 4);
    }

    #[test]
    fn test_worker_config_defaults() {
        let config = WorkerConfig::default();
        assert_eq!(config.memory_limit, "8GB");
        assert!(config.spill_to_disk);
        assert_eq!(config.spill_dir, "/tmp/sqe-spill");
        assert_eq!(config.flight_port, 50052);
        assert_eq!(config.heartbeat_interval_secs, 5);
    }

    /// Helper to build a valid config for validation tests.
    fn valid_config() -> SqeConfig {
        SqeConfig {
            coordinator: CoordinatorConfig {
                flight_sql_port: 50051,
                trino_http_port: 8080,
                mode: "hybrid".to_string(),
                worker_urls: vec![],
                debug: false,
                tls: TlsConfig::default(),
                worker_secret: String::new(),
                allow_unauthenticated_workers: false,
                memory_limit: default_coordinator_memory(),
                spill_to_disk: true,
                spill_dir: default_coordinator_spill_dir(),
                spill_compression: default_spill_compression(),
                flight_compression: default_flight_compression(),
                shuffle_compression: default_shuffle_compression(),
            },
            worker: WorkerConfig::default(),
            auth: AuthConfig {
                keycloak_url: "https://keycloak.example.com".to_string(),
                realm: "sqe".to_string(),
                client_id: "sqe-client".to_string(),
                client_secret: String::new(),
                token_endpoint: String::new(),
                token_refresh_buffer_secs: 60,
                ssl_verification: true,
                tls_skip_verify: false,
                roles_claim: default_roles_claim(),
                providers: Vec::new(),
                role_mappings: std::collections::HashMap::new(),
                external: None,
                admin_roles: default_admin_roles(),
            },
            catalog: CatalogConfig {
                catalog_url: "https://polaris.example.com".to_string(),
                warehouse: "wh".to_string(),
                backend: CatalogBackend::default(),
                metadata_cache_ttl_secs: 30,
                default_table_format_version: 2,
                trust_sort_order: false,
                small_file_threshold_mb: 3,
                parquet_compression: "zstd".to_string(),
                manifest_concurrency: 64,
                auth: None,
                storage: None,
            },
            catalogs: HashMap::new(),
            storage: StorageConfig::default(),
            policy: PolicyConfig::default(),
            access_control: AccessControlConfig::default(),
            metrics: MetricsConfig::default(),
            rate_limit: RateLimitConfig::default(),
            session: SessionConfig::default(),
            query: QueryConfig::default(),
            query_cache: QueryCacheConfig::default(),
            query_history: QueryHistoryConfig::default(),
        }
    }

    #[test]
    fn test_validate_valid_config() {
        let config = valid_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_missing_client_id() {
        let mut config = valid_config();
        config.auth.client_id = String::new();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("auth.client_id is required"),
            "Expected client_id error, got: {err}"
        );
    }

    #[test]
    fn test_validate_missing_catalog_url() {
        let mut config = valid_config();
        config.catalog.catalog_url = String::new();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("catalog.catalog_url is required"),
            "Expected catalog_url error, got: {err}"
        );
    }

    #[test]
    fn test_validate_no_auth_backend() {
        let mut config = valid_config();
        config.auth.keycloak_url = String::new();
        config.auth.token_endpoint = String::new();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("auth.keycloak_url or auth.token_endpoint"),
            "Expected auth backend error, got: {err}"
        );
    }

    #[test]
    fn test_validate_token_endpoint_suffices() {
        let mut config = valid_config();
        config.auth.keycloak_url = String::new();
        config.auth.token_endpoint = "https://token.example.com/token".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_flight_trino_port_conflict() {
        let mut config = valid_config();
        config.coordinator.flight_sql_port = 8080;
        config.coordinator.trino_http_port = 8080;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("flight_sql_port") && err.contains("trino_http_port"),
            "Expected port conflict error, got: {err}"
        );
    }

    #[test]
    fn test_validate_flight_metrics_port_conflict() {
        let mut config = valid_config();
        config.coordinator.flight_sql_port = 9090;
        config.metrics.prometheus_port = 9090;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("flight_sql_port") && err.contains("prometheus_port"),
            "Expected port conflict error, got: {err}"
        );
    }

    #[test]
    fn test_validate_multiple_errors() {
        let mut config = valid_config();
        config.auth.client_id = String::new();
        config.catalog.catalog_url = "  ".to_string();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("auth.client_id") && err.contains("catalog.catalog_url"),
            "Expected multiple errors, got: {err}"
        );
    }

    /// Regression for issue #6: distributed mode with no worker_secret used
    /// to log a SECURITY error and continue booting, leaving the heartbeat
    /// handler open to any reachable client. validate() must now refuse.
    #[test]
    fn validate_rejects_distributed_without_worker_secret() {
        let mut config = valid_config();
        config.coordinator.worker_urls = vec!["http://worker-1:50051".to_string()];
        config.coordinator.worker_secret = String::new();
        config.coordinator.allow_unauthenticated_workers = false;
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("worker_urls") && err.contains("worker_secret"),
            "Expected worker_secret guard error, got: {err}"
        );
    }

    #[test]
    fn validate_accepts_distributed_when_explicitly_unauthenticated() {
        let mut config = valid_config();
        config.coordinator.worker_urls = vec!["http://worker-1:50051".to_string()];
        config.coordinator.worker_secret = String::new();
        config.coordinator.allow_unauthenticated_workers = true;
        // The explicit opt-in is allowed, visible in config diffs.
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_accepts_distributed_with_worker_secret() {
        let mut config = valid_config();
        config.coordinator.worker_urls = vec!["http://worker-1:50051".to_string()];
        config.coordinator.worker_secret = "shared-secret-value".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_accepts_single_node_without_worker_secret() {
        // No workers configured -> secret irrelevant, no error.
        let mut config = valid_config();
        config.coordinator.worker_urls.clear();
        config.coordinator.worker_secret = String::new();
        assert!(config.validate().is_ok());
    }

    /// Old configs that used `polaris_url` continue to deserialize via the
    /// serde alias. New configs use `catalog_url`. Both populate the same
    /// in-memory field, and a config built from a legacy TOML still passes
    /// validate() — guarding against any future addition of
    /// `#[serde(deny_unknown_fields)]` on `CatalogConfig` or its parent
    /// that would silently drop the alias path.
    #[test]
    fn legacy_polaris_url_alias_deserializes() {
        let new_toml = "[catalog]\ncatalog_url = \"http://new.example.com\"\nwarehouse = \"wh\"\n";
        let old_toml =
            "[catalog]\npolaris_url = \"http://old.example.com\"\nwarehouse = \"wh\"\n";

        #[derive(serde::Deserialize)]
        struct Wrap {
            catalog: CatalogConfig,
        }
        let new_w: Wrap = toml::from_str(new_toml).expect("new TOML deserializes");
        let old_w: Wrap = toml::from_str(old_toml).expect("legacy TOML deserializes");
        assert_eq!(new_w.catalog.catalog_url, "http://new.example.com");
        assert_eq!(old_w.catalog.catalog_url, "http://old.example.com");

        // Round-trip into a full SqeConfig and validate. Both must pass.
        let mut full = valid_config();
        full.catalog = old_w.catalog;
        assert!(
            full.validate().is_ok(),
            "legacy polaris_url config should pass validate()"
        );
    }

    /// Legacy single-catalog config: flattening yields one entry
    /// named `iceberg` (the canonical embedded-mode name) and
    /// `resolve_default_catalog()` returns it.
    #[test]
    fn flatten_with_only_legacy_catalog() {
        let cfg = valid_config();
        let flat = cfg.flattened_catalogs();
        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0].0, SqeConfig::LEGACY_CATALOG_NAME);
        assert_eq!(cfg.resolve_default_catalog(), "iceberg");
    }

    /// New-style config with a single named catalog: flattening
    /// uses that name and the legacy block is dropped.
    #[test]
    fn flatten_with_named_catalog_drops_legacy() {
        let mut cfg = valid_config();
        cfg.catalogs.insert(
            "polaris".to_string(),
            CatalogConfig {
                catalog_url: "http://polaris:8181".to_string(),
                warehouse: "main".to_string(),
                backend: CatalogBackend::default(),
                metadata_cache_ttl_secs: 30,
                default_table_format_version: 2,
                trust_sort_order: false,
                small_file_threshold_mb: 3,
                parquet_compression: "zstd".to_string(),
                manifest_concurrency: 64,
                auth: None,
                storage: None,
            },
        );
        let flat = cfg.flattened_catalogs();
        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0].0, "polaris");
        assert_eq!(cfg.resolve_default_catalog(), "polaris");
    }

    /// Two named catalogs: alphabetical order, first becomes default
    /// unless the operator names one explicitly.
    #[test]
    fn flatten_sorts_named_catalogs() {
        let mut cfg = valid_config();
        for name in ["zeta", "alpha", "mid"] {
            cfg.catalogs.insert(
                name.to_string(),
                CatalogConfig {
                    catalog_url: "http://x".to_string(),
                    warehouse: name.to_string(),
                    backend: CatalogBackend::default(),
                    metadata_cache_ttl_secs: 30,
                    default_table_format_version: 2,
                    trust_sort_order: false,
                    small_file_threshold_mb: 3,
                    parquet_compression: "zstd".to_string(),
                    manifest_concurrency: 64,
                    auth: None,
                    storage: None,
                },
            );
        }
        let names: Vec<String> = cfg
            .flattened_catalogs()
            .iter()
            .map(|(n, _)| n.clone())
            .collect();
        assert_eq!(names, vec!["alpha", "mid", "zeta"]);
        assert_eq!(cfg.resolve_default_catalog(), "alpha");
    }

    /// Operator-set `default_catalog` overrides alphabetical pick
    /// AND folds the legacy `[catalog]` block in under that name.
    /// The mixed-mode use case: one Polaris cluster `[catalog]`
    /// kept for backwards-compat, plus a Glue `[catalogs.glue]`
    /// for new tables, with Polaris remaining the default.
    #[test]
    fn default_catalog_can_promote_legacy_block() {
        let mut cfg = valid_config();
        cfg.query.default_catalog = Some("legacy_polaris".to_string());
        cfg.catalogs.insert(
            "glue".to_string(),
            CatalogConfig {
                catalog_url: "".to_string(),
                warehouse: "s3://wh/".to_string(),
                backend: CatalogBackend::default(),
                metadata_cache_ttl_secs: 30,
                default_table_format_version: 2,
                trust_sort_order: false,
                small_file_threshold_mb: 3,
                parquet_compression: "zstd".to_string(),
                manifest_concurrency: 64,
                auth: None,
                storage: None,
            },
        );
        let flat = cfg.flattened_catalogs();
        let names: Vec<String> = flat.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(names, vec!["glue", "legacy_polaris"]);
        // glue wins the alphabetic tiebreak; that's intentional.
        // The operator can pick legacy_polaris explicitly:
        assert_eq!(cfg.resolve_default_catalog(), "legacy_polaris");
    }

    /// Empty `default_catalog = ""` is treated as unset (operator
    /// may have left an empty placeholder in TOML during a rollout).
    #[test]
    fn empty_default_catalog_string_is_treated_as_unset() {
        let mut cfg = valid_config();
        cfg.query.default_catalog = Some("".to_string());
        // No named catalogs, only the legacy block: should still
        // resolve to the canonical legacy name.
        assert_eq!(cfg.resolve_default_catalog(), "iceberg");
    }

    /// End-to-end TOML deserialization: the documented shape with
    /// Polaris + Nessie + AWS Glue + S3 Tables in one config
    /// round-trips through serde and `flattened_catalogs()` returns
    /// the four named entries.
    #[test]
    fn multi_backend_toml_round_trips() {
        let toml = r#"
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
mode = "hybrid"

[auth]
keycloak_url = "https://kc.example.com"
client_id = "sqe-client"

# Legacy single-catalog block kept as a backwards-compat placeholder.
# Operators leaving this minimal must populate `[catalogs.*]` below.
[catalog]
catalog_url = ""

[catalogs.polaris]
catalog_url = "http://polaris:8181/api/catalog"
warehouse = "main"
[catalogs.polaris.backend]
type = "rest"

[catalogs.nessie]
catalog_url = "http://nessie:19120/iceberg"
warehouse = "lake"
[catalogs.nessie.backend]
type = "rest"

[catalogs.aws_glue]
catalog_url = ""
[catalogs.aws_glue.backend]
type = "glue"
region = "eu-central-1"
warehouse = "s3://my-bucket/wh"

[catalogs.aws_s3tables]
catalog_url = ""
[catalogs.aws_s3tables.backend]
type = "s3tables"
table_bucket_arn = "arn:aws:s3tables:eu-west-1:123456789012:bucket/my-bucket"
"#;
        let cfg: SqeConfig = toml::from_str(toml).expect("multi-backend TOML deserializes");
        let names: Vec<String> = cfg
            .flattened_catalogs()
            .iter()
            .map(|(n, _)| n.clone())
            .collect();
        assert_eq!(
            names,
            vec!["aws_glue", "aws_s3tables", "nessie", "polaris"],
            "named catalogs sorted alphabetically",
        );
        // The legacy [catalog] block has an empty catalog_url; with
        // no `default_catalog` set, it is dropped.
        assert!(!names.contains(&"iceberg".to_string()));
        // Each backend variant deserialised correctly:
        assert!(matches!(
            cfg.catalogs["polaris"].backend,
            CatalogBackend::Rest
        ));
        assert!(matches!(
            cfg.catalogs["aws_glue"].backend,
            CatalogBackend::Glue { .. }
        ));
        assert!(matches!(
            cfg.catalogs["aws_s3tables"].backend,
            CatalogBackend::S3tables { .. }
        ));
    }

    /// V7: per-catalog auth + storage overrides round-trip through
    /// the TOML deserializer, including all four `CatalogAuthConfig`
    /// variants and a per-catalog `[storage]` block that points at
    /// a different S3 endpoint.
    #[test]
    fn per_catalog_auth_and_storage_overrides_deserialize() {
        let toml = r#"
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
mode = "hybrid"

[auth]
keycloak_url = "https://kc.example.com"
client_id = "sqe-client"

[catalog]
catalog_url = ""

[catalogs.polaris]
catalog_url = "http://polaris:8181/api/catalog"
warehouse = "main"
[catalogs.polaris.backend]
type = "rest"
# auth omitted: defaults to SessionBearer

[catalogs.partner_polaris]
catalog_url = "https://partner.com/iceberg"
warehouse = "shared"
[catalogs.partner_polaris.backend]
type = "rest"
[catalogs.partner_polaris.auth]
type = "client_credentials"
token_endpoint = "https://partner.com/oauth/tokens"
client_id = "sqe-partner"
client_secret = "secret-from-env"
[catalogs.partner_polaris.storage]
s3_endpoint = "https://partner-s3.example.com"
s3_region = "us-east-1"
s3_access_key = "partner-key"
s3_secret_key = "partner-secret"

[catalogs.public_nessie]
catalog_url = "https://nessie.public.example.com/iceberg"
warehouse = "public"
[catalogs.public_nessie.backend]
type = "rest"
[catalogs.public_nessie.auth]
type = "anonymous"

[catalogs.aws_glue]
catalog_url = ""
[catalogs.aws_glue.backend]
type = "glue"
region = "eu-central-1"
warehouse = "s3://wh/"
[catalogs.aws_glue.auth]
type = "aws"
"#;
        let cfg: SqeConfig = toml::from_str(toml).expect("V7 TOML deserializes");

        // Default (omitted) is SessionBearer (None on the field).
        assert!(cfg.catalogs["polaris"].auth.is_none());
        assert!(cfg.catalogs["polaris"].storage.is_none());

        // Partner has both overrides.
        match &cfg.catalogs["partner_polaris"].auth {
            Some(CatalogAuthConfig::ClientCredentials {
                token_endpoint,
                client_id,
                client_secret,
                scope,
            }) => {
                assert_eq!(token_endpoint, "https://partner.com/oauth/tokens");
                assert_eq!(client_id, "sqe-partner");
                assert_eq!(client_secret, "secret-from-env");
                assert!(scope.is_none(), "scope is optional, defaults to None");
            }
            other => panic!("partner_polaris auth wrong variant: {other:?}"),
        }
        let partner_storage = cfg.catalogs["partner_polaris"]
            .storage
            .as_ref()
            .expect("partner has storage override");
        assert_eq!(partner_storage.s3_endpoint, "https://partner-s3.example.com");
        assert_eq!(partner_storage.s3_region, "us-east-1");
        assert_eq!(partner_storage.s3_access_key, "partner-key");

        // Anonymous + AWS variants:
        assert!(matches!(
            cfg.catalogs["public_nessie"].auth,
            Some(CatalogAuthConfig::Anonymous)
        ));
        assert!(matches!(
            cfg.catalogs["aws_glue"].auth,
            Some(CatalogAuthConfig::Aws)
        ));
    }

    #[test]
    fn test_tls_config_disabled_by_default() {
        let tls = TlsConfig::default();
        assert!(!tls.is_enabled());
    }

    #[test]
    fn test_tls_config_enabled_with_cert_and_key() {
        let tls = TlsConfig {
            cert_file: "/tmp/cert.pem".to_string(),
            key_file: "/tmp/key.pem".to_string(),
            ca_file: String::new(),
        };
        assert!(tls.is_enabled());
    }

    #[test]
    fn test_validate_tls_cert_without_key() {
        let mut config = valid_config();
        config.coordinator.tls.cert_file = "/tmp/cert.pem".to_string();
        // key_file is empty
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("tls.key_file is missing"),
            "Expected cert-without-key error, got: {err}"
        );
    }

    #[test]
    fn test_validate_tls_key_without_cert() {
        let mut config = valid_config();
        config.coordinator.tls.key_file = "/tmp/key.pem".to_string();
        // cert_file is empty
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("tls.cert_file is missing"),
            "Expected key-without-cert error, got: {err}"
        );
    }

    #[test]
    fn test_validate_tls_missing_files() {
        let mut config = valid_config();
        config.coordinator.tls.cert_file = "/nonexistent/cert.pem".to_string();
        config.coordinator.tls.key_file = "/nonexistent/key.pem".to_string();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("tls.cert_file") && err.contains("not found"),
            "Expected missing file error, got: {err}"
        );
    }

    #[test]
    fn test_storage_config_s3_allow_http_defaults_false() {
        let config = StorageConfig::default();
        assert!(!config.s3_allow_http, "s3_allow_http should default to false (secure by default)");
    }

    #[test]
    fn test_query_cache_defaults() {
        let config = QueryCacheConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_memory_mb, 256);
        assert_eq!(config.max_entry_mb, 5);
        assert_eq!(config.ttl_secs, 300);
    }

    #[test]
    fn test_query_history_defaults() {
        let config = QueryHistoryConfig::default();
        assert_eq!(config.max_entries, 10000);
        assert_eq!(config.ttl_secs, 1800);
    }

    // -----------------------------------------------------------------------
    // AuthProviderConfig parsing
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_oidc_password_provider() {
        let toml_str = r#"
            type = "oidc_password"
            token_url = "https://idp.example.com/token"
            client_id = "sqe"
            client_secret = "changeme"
            roles_claim = "custom.roles"
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::OidcPassword {
                token_url,
                client_id,
                client_secret,
                roles_claim,
            } => {
                assert_eq!(token_url, "https://idp.example.com/token");
                assert_eq!(client_id, "sqe");
                assert_eq!(client_secret, "changeme");
                assert_eq!(roles_claim, "custom.roles");
            }
            other => panic!("Expected OidcPassword, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_oidc_password_provider_defaults() {
        let toml_str = r#"
            type = "oidc_password"
            token_url = "https://idp.example.com/token"
            client_id = "sqe"
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::OidcPassword {
                client_secret,
                roles_claim,
                ..
            } => {
                assert_eq!(client_secret, "");
                assert_eq!(roles_claim, "realm_access.roles");
            }
            other => panic!("Expected OidcPassword, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_client_credentials_provider() {
        let toml_str = r#"
            type = "client_credentials"
            token_endpoint = "https://polaris.example.com/oauth/tokens"
            client_id = "polaris-client"
            client_secret = "polaris-secret"
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::ClientCredentials {
                token_endpoint,
                client_id,
                client_secret,
            } => {
                assert_eq!(token_endpoint, "https://polaris.example.com/oauth/tokens");
                assert_eq!(client_id, "polaris-client");
                assert_eq!(client_secret, "polaris-secret");
            }
            other => panic!("Expected ClientCredentials, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_anonymous_provider() {
        let toml_str = r#"
            type = "anonymous"
            user = "dev-user"
            roles = ["admin", "reader"]
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::Anonymous { user, roles } => {
                assert_eq!(user, "dev-user");
                assert_eq!(roles, vec!["admin", "reader"]);
            }
            other => panic!("Expected Anonymous, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_anonymous_provider_defaults() {
        let toml_str = r#"
            type = "anonymous"
        "#;

        let config: AuthProviderConfig = toml::from_str(toml_str).unwrap();
        match config {
            AuthProviderConfig::Anonymous { user, roles } => {
                assert_eq!(user, "anonymous");
                assert!(roles.is_empty());
            }
            other => panic!("Expected Anonymous, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_providers_array_in_auth_config() {
        let toml_str = r#"
            client_id = "sqe"

            [[providers]]
            type = "oidc_password"
            token_url = "https://idp.example.com/token"
            client_id = "sqe"

            [[providers]]
            type = "anonymous"
            user = "fallback"
        "#;

        let config: AuthConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.providers.len(), 2);
        assert!(matches!(config.providers[0], AuthProviderConfig::OidcPassword { .. }));
        assert!(matches!(config.providers[1], AuthProviderConfig::Anonymous { .. }));
    }

    #[test]
    fn test_parse_auth_config_no_providers_backward_compat() {
        let toml_str = r#"
            keycloak_url = "https://keycloak.example.com"
            realm = "sqe"
            client_id = "sqe-client"
            client_secret = "secret"
        "#;

        let config: AuthConfig = toml::from_str(toml_str).unwrap();
        assert!(config.providers.is_empty());
        assert_eq!(config.keycloak_url, "https://keycloak.example.com");
        assert_eq!(config.client_id, "sqe-client");
    }

    #[test]
    fn test_validate_with_providers_no_legacy_fields_needed() {
        let mut config = valid_config();
        config.auth.keycloak_url = String::new();
        config.auth.token_endpoint = String::new();
        config.auth.client_id = String::new();
        config.auth.providers = vec![AuthProviderConfig::Anonymous {
            user: "test".to_string(),
            roles: vec![],
        }];
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_parse_external_auth_config() {
        let toml_str = r#"
            issuer = "https://idp.example.com/realms/sqe"
            client_id = "sqe"
            client_secret = "secret"
            scopes = ["openid", "profile"]

            [device]
            client_id = "sqe-cli"
            scopes = ["openid", "profile", "offline_access"]
        "#;
        let config: ExternalAuthConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.issuer, "https://idp.example.com/realms/sqe");
        assert_eq!(config.client_id, "sqe");
        assert_eq!(config.client_secret, Some("secret".to_string()));
        assert_eq!(config.challenge_timeout_secs, 900);
        assert!(config.device.is_some());
        let device = config.device.unwrap();
        assert_eq!(device.client_id, "sqe-cli");
        assert_eq!(device.scopes, vec!["openid", "profile", "offline_access"]);
    }

    #[test]
    fn test_parse_external_auth_config_minimal() {
        let toml_str = r#"
            issuer = "https://idp.example.com"
            client_id = "sqe"
        "#;
        let config: ExternalAuthConfig = toml::from_str(toml_str).unwrap();
        assert!(config.client_secret.is_none());
        assert!(config.device.is_none());
        assert_eq!(config.scopes, vec!["openid", "profile"]);
    }

    // -----------------------------------------------------------------------
    // QueryConfig: defaults and custom values
    // -----------------------------------------------------------------------

    #[test]
    fn test_query_config_defaults() {
        let config = QueryConfig::default();
        assert_eq!(config.timeout_secs, 300);
        assert_eq!(config.max_result_rows, 1_000_000);
        assert_eq!(config.max_concurrent_queries, 100);
        assert_eq!(config.slow_query_threshold_secs, 30);
        assert_eq!(config.max_query_memory, "256MB");
    }

    #[test]
    fn test_query_config_custom_values() {
        let toml_str = r#"
            timeout_secs = 60
            max_result_rows = 500
            max_concurrent_queries = 50
            slow_query_threshold_secs = 10
            max_query_memory = "512MB"
        "#;
        let config: QueryConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.timeout_secs, 60);
        assert_eq!(config.max_result_rows, 500);
        assert_eq!(config.max_concurrent_queries, 50);
        assert_eq!(config.slow_query_threshold_secs, 10);
        assert_eq!(config.max_query_memory, "512MB");
    }

    // -----------------------------------------------------------------------
    // SessionConfig: defaults and file persistence
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_config_defaults() {
        let config = SessionConfig::default();
        assert_eq!(config.idle_timeout_secs, 900);
        assert_eq!(config.absolute_timeout_secs, 28800);
        assert_eq!(config.persistence, "memory");
        assert_eq!(config.persistence_path, "/tmp/sqe-sessions.json");
        assert_eq!(config.snapshot_interval_secs, 60);
    }

    #[test]
    fn test_session_config_file_persistence() {
        let toml_str = r#"
            persistence = "file"
            persistence_path = "/var/data/sessions.json"
            snapshot_interval_secs = 30
        "#;
        let config: SessionConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.persistence, "file");
        assert_eq!(config.persistence_path, "/var/data/sessions.json");
        assert_eq!(config.snapshot_interval_secs, 30);
    }

    // -----------------------------------------------------------------------
    // OpenLineage config tests
    // -----------------------------------------------------------------------

    /// Minimal skeleton TOML (coordinator/auth/catalog blocks) so a full
    /// `SqeConfig` deserialises in OpenLineage tests. Only `catalog.catalog_url`
    /// is structurally required; everything else has serde defaults.
    const OL_TEST_SKELETON: &str = r#"
[coordinator]
[auth]
[catalog]
catalog_url = "http://polaris.example/api/catalog"
"#;

    #[test]
    fn openlineage_config_parses_from_toml() {
        let body = r#"
[metrics]
prometheus_port = 9090
otlp_endpoint = ""

[metrics.openlineage]
enabled = true
job_namespace = "sqe-prod"
emit_selects = true
file_path = "/var/log/ol.jsonl"
http_endpoint = "https://marquez.example/api/v1/lineage"
auth_mode = "bearer"
api_key = "secret"
spool_path = "/var/spool/sqe-ol"
spool_max_bytes = 209715200
"#;
        let toml = format!("{OL_TEST_SKELETON}{body}");
        let cfg: SqeConfig = toml::from_str(&toml).unwrap();
        let ol = &cfg.metrics.openlineage;
        assert!(ol.enabled);
        assert_eq!(ol.job_namespace, "sqe-prod");
        assert!(ol.emit_selects);
        assert_eq!(ol.spool_max_bytes, 209715200);
    }

    #[test]
    fn openlineage_config_uses_defaults_when_omitted() {
        let body = r#"
[metrics]
prometheus_port = 9090
otlp_endpoint = ""
"#;
        let toml = format!("{OL_TEST_SKELETON}{body}");
        let cfg: SqeConfig = toml::from_str(&toml).unwrap();
        let ol = &cfg.metrics.openlineage;
        assert!(!ol.enabled);
        assert_eq!(ol.job_namespace, "sqe");
        assert_eq!(ol.spool_max_bytes, 100 * 1024 * 1024);
        assert_eq!(ol.channel_capacity, 10000);
    }

    #[test]
    fn env_overrides_apply_to_openlineage() {
        // Use unique env-var values per test scope to avoid cross-test leakage,
        // and guard with the same lock pattern as other env-touching tests if
        // any are added later. Set, override, assert, then clear.
        std::env::set_var("SQE_METRICS__OPENLINEAGE__ENABLED", "true");
        std::env::set_var("SQE_METRICS__OPENLINEAGE__SPOOL_MAX_BYTES", "999");
        std::env::set_var("SQE_METRICS__OPENLINEAGE__CHANNEL_CAPACITY", "42");

        let mut cfg = valid_config();
        cfg.apply_env_overrides();

        assert!(cfg.metrics.openlineage.enabled);
        assert_eq!(cfg.metrics.openlineage.spool_max_bytes, 999);
        assert_eq!(cfg.metrics.openlineage.channel_capacity, 42);

        // Cleanup so other tests in this process aren't affected.
        std::env::remove_var("SQE_METRICS__OPENLINEAGE__ENABLED");
        std::env::remove_var("SQE_METRICS__OPENLINEAGE__SPOOL_MAX_BYTES");
        std::env::remove_var("SQE_METRICS__OPENLINEAGE__CHANNEL_CAPACITY");
    }

    #[test]
    fn validate_rejects_enabled_without_sinks() {
        let cfg = OpenLineageConfig {
            enabled: true,
            ..OpenLineageConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("at least one of file_path or http_endpoint"));
    }

    #[test]
    fn validate_rejects_bearer_without_api_key() {
        let cfg = OpenLineageConfig {
            enabled: true,
            file_path: "/tmp/ol.jsonl".into(),
            auth_mode: "bearer".into(),
            ..OpenLineageConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("requires api_key"));
    }

    #[test]
    fn validate_rejects_spool_without_http() {
        let cfg = OpenLineageConfig {
            enabled: true,
            file_path: "/tmp/ol.jsonl".into(),
            spool_path: "/tmp/spool".into(),
            ..OpenLineageConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("spool_path requires http_endpoint"));
    }

    #[test]
    fn validate_rejects_tiny_spool_cap() {
        let cfg = OpenLineageConfig {
            enabled: true,
            file_path: "/tmp/ol.jsonl".into(),
            spool_max_bytes: 1024, // 1 KiB, well under 1 MiB
            ..OpenLineageConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("at least 1 MiB"));
    }

    #[test]
    fn validate_passes_with_valid_config() {
        let cfg = OpenLineageConfig {
            enabled: true,
            file_path: "/tmp/ol.jsonl".into(),
            ..OpenLineageConfig::default()
        };
        assert!(cfg.validate().is_ok());
    }

    // --- Provider env-var override (issue #14 regression test) ---

    /// Lock used to serialise env-var test mutation since std::env::set_var
    /// has process-wide effect.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn provider_client_secret_env_override_beats_toml() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let mut cfg = valid_config();
        cfg.auth.providers = vec![
            AuthProviderConfig::OidcPassword {
                token_url: "http://idp.example.com/token".to_string(),
                client_id: "sqe".to_string(),
                client_secret: "toml-secret".to_string(),
                roles_claim: "realm_access.roles".to_string(),
            },
            AuthProviderConfig::ClientCredentials {
                token_endpoint: "http://polaris:8181/oauth/tokens".to_string(),
                client_id: "polaris-sa".to_string(),
                client_secret: "toml-polaris".to_string(),
            },
        ];

        std::env::set_var(
            "SQE_AUTH__PROVIDERS__0__CLIENT_SECRET",
            "from-env-oidc",
        );
        std::env::set_var(
            "SQE_AUTH__PROVIDERS__1__CLIENT_SECRET",
            "from-env-ccg",
        );

        cfg.apply_env_overrides();

        match &cfg.auth.providers[0] {
            AuthProviderConfig::OidcPassword { client_secret, .. } => {
                assert_eq!(client_secret, "from-env-oidc");
            }
            other => panic!("expected OidcPassword, got {other:?}"),
        }
        match &cfg.auth.providers[1] {
            AuthProviderConfig::ClientCredentials { client_secret, .. } => {
                assert_eq!(client_secret, "from-env-ccg");
            }
            other => panic!("expected ClientCredentials, got {other:?}"),
        }

        std::env::remove_var("SQE_AUTH__PROVIDERS__0__CLIENT_SECRET");
        std::env::remove_var("SQE_AUTH__PROVIDERS__1__CLIENT_SECRET");
    }

    #[test]
    fn provider_client_secret_env_override_token_exchange_optional() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let mut cfg = valid_config();
        cfg.auth.providers = vec![AuthProviderConfig::TokenExchange {
            token_url: "http://idp.example.com/token".to_string(),
            client_id: "sqe".to_string(),
            client_secret: None,
            audience: Some("polaris".to_string()),
            user_claim: "sub".to_string(),
            roles_claim: "realm_access.roles".to_string(),
        }];

        std::env::set_var(
            "SQE_AUTH__PROVIDERS__0__CLIENT_SECRET",
            "exchange-secret",
        );

        cfg.apply_env_overrides();

        match &cfg.auth.providers[0] {
            AuthProviderConfig::TokenExchange { client_secret, .. } => {
                assert_eq!(client_secret.as_deref(), Some("exchange-secret"));
            }
            other => panic!("expected TokenExchange, got {other:?}"),
        }

        std::env::remove_var("SQE_AUTH__PROVIDERS__0__CLIENT_SECRET");
    }

    // --- TvfPolicy (issue #10 regression tests) ---

    #[test]
    fn tvf_default_allows_object_store_schemes() {
        let policy = TvfPolicy::default();
        assert!(policy.check("s3://my-bucket/data.parquet").is_ok());
        assert!(policy.check("s3a://my-bucket/data.parquet").is_ok());
        assert!(policy.check("abfss://c@a.dfs.core.windows.net/x").is_ok());
        assert!(policy.check("abfs://c@a.dfs.core.windows.net/x").is_ok());
        assert!(policy.check("azure://container/x").is_ok());
        assert!(policy.check("az://container/x").is_ok());
        assert!(policy.check("gs://bucket/x").is_ok());
        assert!(policy.check("gcs://bucket/x").is_ok());
        assert!(policy.check("hf://datasets/foo/bar/x").is_ok());
    }

    #[test]
    fn tvf_default_rejects_local_absolute_paths() {
        let policy = TvfPolicy::default();
        let err = policy.check("/etc/shadow").unwrap_err();
        assert!(err.contains("local filesystem paths are disabled"));
        let err = policy.check("/proc/self/environ").unwrap_err();
        assert!(err.contains("local filesystem"));
        let err = policy.check("file:///root/.aws/credentials").unwrap_err();
        assert!(err.contains("local filesystem"));
    }

    #[test]
    fn tvf_default_rejects_arbitrary_http_hosts() {
        let policy = TvfPolicy::default();
        // The IMDS scenario from the issue.
        let err = policy
            .check("http://169.254.169.254/latest/meta-data/iam/security-credentials/")
            .unwrap_err();
        assert!(err.contains("not in `[storage.tvf] allowed_http_hosts`"));
        assert!(err.contains("169.254.169.254"));
    }

    #[test]
    fn tvf_allowed_http_host_is_accepted_exact_match() {
        let policy = TvfPolicy {
            allow_local_paths: false,
            allow_http: false,
            allowed_http_hosts: vec![
                "data.example.com".to_string(),
                "huggingface.co".to_string(),
            ],
        };
        assert!(policy.check("https://data.example.com/file.parquet").is_ok());
        // Case-insensitive host comparison.
        assert!(policy.check("https://DATA.EXAMPLE.COM/file.parquet").is_ok());
        // Different port is still allowed (host match only).
        assert!(policy.check("https://data.example.com:8080/file.parquet").is_ok());
        // Subdomain that isn't allowlisted is rejected (no wildcards).
        assert!(policy
            .check("https://api.data.example.com/file.parquet")
            .is_err());
    }

    #[test]
    fn tvf_allow_http_true_bypasses_allowlist() {
        let policy = TvfPolicy {
            allow_local_paths: false,
            allow_http: true,
            allowed_http_hosts: Vec::new(),
        };
        assert!(policy.check("http://169.254.169.254/").is_ok());
        assert!(policy.check("https://anything.example/x").is_ok());
    }

    #[test]
    fn tvf_allow_local_paths_true_permits_filesystem() {
        let policy = TvfPolicy {
            allow_local_paths: true,
            allow_http: false,
            allowed_http_hosts: Vec::new(),
        };
        assert!(policy.check("/var/data/foo.parquet").is_ok());
        assert!(policy.check("file:///var/data/foo.parquet").is_ok());
    }

    #[test]
    fn tvf_malformed_http_url_returns_error() {
        let policy = TvfPolicy {
            allow_local_paths: false,
            allow_http: false,
            allowed_http_hosts: vec!["example.com".to_string()],
        };
        let err = policy.check("http:///just-a-path").unwrap_err();
        assert!(err.contains("malformed URL") || err.contains("missing host"));
    }

    // --- TvfPolicy::check_endpoint (issue #46 regression tests) ---

    #[test]
    fn tvf_endpoint_empty_is_allowed() {
        let policy = TvfPolicy::default();
        assert!(policy.check_endpoint("").is_ok());
    }

    #[test]
    fn tvf_endpoint_imds_url_is_rejected_by_default() {
        let policy = TvfPolicy::default();
        let err = policy
            .check_endpoint("http://169.254.169.254/latest/meta-data/")
            .unwrap_err();
        assert!(err.contains("not in `[storage.tvf] allowed_http_hosts`"));
        assert!(err.contains("169.254.169.254"));
    }

    #[test]
    fn tvf_endpoint_bare_host_is_allowed() {
        // MinIO / Ceph deployments use bare host:port endpoints. Allowed
        // because they cannot be the IMDS http://169.254.169.254 URL.
        let policy = TvfPolicy::default();
        assert!(policy.check_endpoint("minio.local:9000").is_ok());
        assert!(policy.check_endpoint("s3.example:9000").is_ok());
    }

    #[test]
    fn tvf_endpoint_allowed_http_host_passes() {
        let policy = TvfPolicy {
            allow_local_paths: false,
            allow_http: false,
            allowed_http_hosts: vec!["s3.us-east-1.amazonaws.com".to_string()],
        };
        assert!(policy
            .check_endpoint("https://s3.us-east-1.amazonaws.com")
            .is_ok());
        let err = policy
            .check_endpoint("https://other-region.amazonaws.com")
            .unwrap_err();
        assert!(err.contains("not in `[storage.tvf] allowed_http_hosts`"));
    }

    #[test]
    fn tvf_endpoint_allow_http_true_bypasses_allowlist() {
        let policy = TvfPolicy {
            allow_local_paths: false,
            allow_http: true,
            allowed_http_hosts: Vec::new(),
        };
        // Defense-in-depth surrenders when allow_http = true.
        assert!(policy.check_endpoint("http://169.254.169.254").is_ok());
    }

    // --- AuthConfig::has_admin_role (issue #3 regression tests) ---

    fn auth_with_admin_roles(roles: Vec<&str>) -> AuthConfig {
        let mut cfg = valid_config().auth;
        cfg.admin_roles = roles.into_iter().map(String::from).collect();
        cfg
    }

    #[test]
    fn admin_role_matches_when_caller_in_allowlist() {
        let auth = auth_with_admin_roles(vec!["service_admin", "catalog_admin"]);
        let caller_roles = vec!["analyst".to_string(), "catalog_admin".to_string()];
        assert!(auth.has_admin_role(&caller_roles));
    }

    #[test]
    fn admin_role_misses_when_caller_lacks_admin() {
        let auth = auth_with_admin_roles(vec!["service_admin", "catalog_admin"]);
        let caller_roles = vec!["analyst".to_string(), "viewer".to_string()];
        assert!(!auth.has_admin_role(&caller_roles));
    }

    #[test]
    fn admin_role_misses_when_caller_has_no_roles() {
        let auth = auth_with_admin_roles(vec!["service_admin"]);
        assert!(!auth.has_admin_role(&[]));
    }

    #[test]
    fn admin_role_fails_closed_when_allowlist_empty() {
        // Operator explicitly cleared the allowlist - every admin
        // statement is rejected, even for users who happen to hold
        // a role named "service_admin".
        let auth = auth_with_admin_roles(vec![]);
        let caller_roles = vec!["service_admin".to_string()];
        assert!(!auth.has_admin_role(&caller_roles));
    }

    #[test]
    fn admin_role_match_is_case_sensitive() {
        // Role names are exact-match. SERVICE_ADMIN != service_admin.
        let auth = auth_with_admin_roles(vec!["service_admin"]);
        let caller_roles = vec!["SERVICE_ADMIN".to_string()];
        assert!(!auth.has_admin_role(&caller_roles));
    }
}
