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

#[derive(Debug, Deserialize, Clone)]
pub struct QueryConfig {
    /// Maximum query execution time in seconds. Default: 300 (5 minutes).
    #[serde(default = "default_query_timeout")]
    pub timeout_secs: u64,
    /// Per-role timeout overrides. Keys are role names, values are timeout in
    /// seconds. When a user has multiple matching roles the highest value wins.
    #[serde(default)]
    pub role_overrides: std::collections::HashMap<String, u64>,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            timeout_secs: default_query_timeout(),
            role_overrides: std::collections::HashMap::new(),
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

#[derive(Deserialize, Clone)]
pub struct AuthConfig {
    #[serde(default)]
    pub keycloak_url: String,
    #[serde(default)]
    pub realm: String,
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    /// Generic OAuth2 token endpoint for client_credentials grant.
    /// When set (and keycloak_url is empty), the engine uses client_credentials mode.
    #[serde(default)]
    pub token_endpoint: String,
    #[serde(default = "default_refresh_buffer")]
    pub token_refresh_buffer_secs: u64,
    #[serde(default = "default_true")]
    pub ssl_verification: bool,
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
            .finish()
    }
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
}

#[derive(Deserialize, Clone, Default)]
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
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageConfig")
            .field("s3_endpoint", &self.s3_endpoint)
            .field("s3_region", &self.s3_region)
            .field("s3_access_key", &"[REDACTED]")
            .field("s3_secret_key", &"[REDACTED]")
            .field("s3_path_style", &self.s3_path_style)
            .field("s3_allow_http", &self.s3_allow_http)
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
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            prometheus_port: 9090,
            otlp_endpoint: String::new(),
            audit_log_path: String::new(),
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
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            idle_timeout_secs: default_idle_timeout(),
            absolute_timeout_secs: default_absolute_timeout(),
        }
    }
}

fn default_idle_timeout() -> u64 { 900 }       // 15 minutes
fn default_absolute_timeout() -> u64 { 28800 }  // 8 hours
fn default_query_timeout() -> u64 { 300 }       // 5 minutes

fn default_flight_port() -> u16 { 50051 }
fn default_trino_port() -> u16 { 8080 }
fn default_mode() -> String { "hybrid".to_string() }
fn default_worker_flight_port() -> u16 { 50052 }
fn default_heartbeat() -> u64 { 5 }
fn default_memory() -> String { "8GB".to_string() }
fn default_spill_dir() -> String { "/tmp/sqe-spill".to_string() }
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
        if self.auth.client_id.trim().is_empty() {
            errors.push("auth.client_id is required".to_string());
        }
        if self.catalog.polaris_url.trim().is_empty() {
            errors.push("catalog.polaris_url is required".to_string());
        }
        if self.auth.keycloak_url.trim().is_empty()
            && self.auth.token_endpoint.trim().is_empty()
        {
            errors.push(
                "at least one of auth.keycloak_url or auth.token_endpoint must be set"
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
}
