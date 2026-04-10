use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct SqeConfig {
    pub coordinator: CoordinatorConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
    pub auth: AuthConfig,
    pub catalog: CatalogConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
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
                tracing::warn!(sort_mode = s, "Unknown sort_mode, defaulting to adaptive");
                Self::Adaptive
            }
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct QueryConfig {
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
    /// - `"strict"`: Always sort. Spill to disk if needed. (backwards-compatible)
    /// - `"partition_only"`: Only sort when keys match Iceberg partition columns.
    /// - `"adaptive"`: Sort when memory is Green; strip non-partition sorts under pressure.
    ///
    /// Default: `"adaptive"`
    #[serde(default = "default_sort_mode")]
    pub sort_mode: String,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
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
    /// metadata header when sending heartbeats. An empty value disables the
    /// check (backwards compatible default).
    #[serde(default)]
    pub worker_secret: String,
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
        /// Expected audience (`aud` claim). Optional.
        #[serde(default)]
        audience: Option<String>,
        /// JWT claim for user identity. Default: `"sub"`.
        #[serde(default = "default_user_claim")]
        user_claim: String,
        /// Dot-separated JSON path to roles. Default: `"realm_access.roles"`.
        #[serde(default = "default_roles_claim")]
        roles_claim: String,
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
    #[serde(default = "default_true")]
    pub ssl_verification: bool,
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

#[derive(Debug, Deserialize, Clone)]
pub struct CatalogConfig {
    pub polaris_url: String,
    #[serde(default)]
    pub warehouse: String,
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
        }
    }
}

fn default_coalesce_threshold() -> String { "1MB".to_string() }
fn default_footer_cache_size() -> String { "256MB".to_string() }
fn default_concurrent_requests_per_file() -> usize { 4 }
fn default_max_concurrent_files() -> usize { 8 }
fn default_prefetch_buffer() -> String { "32MB".to_string() }

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
            .finish()
    }
}

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
        }
    }
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

fn default_coordinator_memory() -> String { "8GB".to_string() }
fn default_coordinator_spill_dir() -> String { "/tmp/sqe-coordinator-spill".to_string() }
fn default_spill_compression() -> String { "lz4".to_string() }

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
fn default_passthrough() -> String { "passthrough".to_string() }
fn default_prometheus_port() -> u16 { 9090 }
fn default_per_user_rpm() -> u32 { 60 }
fn default_global_rpm() -> u32 { 1000 }

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
        if self.catalog.polaris_url.trim().is_empty() {
            errors.push("catalog.polaris_url is required".to_string());
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
            eprintln!("WARN: config key 'auth.keycloak_url' is deprecated — the OIDC password grant provider works with any OIDC-compliant endpoint, not just Keycloak. This key will continue to work but may be renamed in a future release.");
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

        // Catalog
        env_override_str("SQE_CATALOG__POLARIS_URL", &mut self.catalog.polaris_url);
        env_override_str("SQE_CATALOG__WAREHOUSE", &mut self.catalog.warehouse);
        env_override_u64("SQE_CATALOG__METADATA_CACHE_TTL_SECS", &mut self.catalog.metadata_cache_ttl_secs);
        env_override_u8("SQE_CATALOG__DEFAULT_TABLE_FORMAT_VERSION", &mut self.catalog.default_table_format_version);

        // Storage
        env_override_str("SQE_STORAGE__S3_ENDPOINT", &mut self.storage.s3_endpoint);
        env_override_str("SQE_STORAGE__S3_REGION", &mut self.storage.s3_region);
        env_override_str("SQE_STORAGE__S3_ACCESS_KEY", &mut self.storage.s3_access_key);
        env_override_str("SQE_STORAGE__S3_SECRET_KEY", &mut self.storage.s3_secret_key);
        env_override_bool("SQE_STORAGE__S3_PATH_STYLE", &mut self.storage.s3_path_style);
        env_override_bool("SQE_STORAGE__S3_ALLOW_HTTP", &mut self.storage.s3_allow_http);

        // Policy
        env_override_str("SQE_POLICY__ENGINE", &mut self.policy.engine);

        // Metrics
        env_override_u16("SQE_METRICS__PROMETHEUS_PORT", &mut self.metrics.prometheus_port);
        env_override_str("SQE_METRICS__OTLP_ENDPOINT", &mut self.metrics.otlp_endpoint);
        env_override_str("SQE_METRICS__AUDIT_LOG_PATH", &mut self.metrics.audit_log_path);

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
                memory_limit: default_coordinator_memory(),
                spill_to_disk: true,
                spill_dir: default_coordinator_spill_dir(),
                spill_compression: default_spill_compression(),
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
                providers: Vec::new(),
                role_mappings: std::collections::HashMap::new(),
                external: None,
            },
            catalog: CatalogConfig {
                polaris_url: "https://polaris.example.com".to_string(),
                warehouse: "wh".to_string(),
                metadata_cache_ttl_secs: 30,
                default_table_format_version: 2,
            },
            storage: StorageConfig::default(),
            policy: PolicyConfig::default(),
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
    fn test_validate_missing_polaris_url() {
        let mut config = valid_config();
        config.catalog.polaris_url = String::new();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("catalog.polaris_url is required"),
            "Expected polaris_url error, got: {err}"
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
        config.catalog.polaris_url = "  ".to_string();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("auth.client_id") && err.contains("catalog.polaris_url"),
            "Expected multiple errors, got: {err}"
        );
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
}
