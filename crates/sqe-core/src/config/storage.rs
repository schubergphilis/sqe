use serde::Deserialize;

use crate::SecretString;

use super::TvfPolicy;

#[derive(Deserialize, Clone)]
pub struct StorageConfig {
    #[serde(default)]
    pub s3_endpoint: String,
    #[serde(default)]
    pub s3_region: String,
    #[serde(default)]
    pub s3_access_key: String,
    #[serde(default)]
    pub s3_secret_key: SecretString,
    /// STS session token, required whenever `s3_access_key` holds temporary
    /// credentials (SSO, assumed role, IMDS). Signing without it against
    /// temporary keys fails as `InvalidAccessKeyId`, which reads like a
    /// wrong key rather than a missing token.
    #[serde(default)]
    pub s3_session_token: SecretString,
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

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: SecretString::default(),
            s3_session_token: SecretString::default(),
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

fn default_coalesce_threshold() -> String {
    "1MB".to_string()
}
fn default_footer_cache_size() -> String {
    "256MB".to_string()
}
fn default_concurrent_requests_per_file() -> usize {
    4
}
fn default_max_concurrent_files() -> usize {
    8
}
fn default_prefetch_buffer() -> String {
    "32MB".to_string()
}
fn default_prefetch_concurrency() -> usize {
    4
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageConfig")
            .field("s3_endpoint", &self.s3_endpoint)
            .field("s3_region", &self.s3_region)
            .field("s3_access_key", &"[REDACTED]")
            .field("s3_secret_key", &self.s3_secret_key)
            .field("s3_path_style", &self.s3_path_style)
            .field("s3_allow_http", &self.s3_allow_http)
            .field("coalesce_threshold", &self.coalesce_threshold)
            .field("footer_cache_size", &self.footer_cache_size)
            .field(
                "concurrent_requests_per_file",
                &self.concurrent_requests_per_file,
            )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_config_s3_allow_http_defaults_false() {
        let config = StorageConfig::default();
        assert!(
            !config.s3_allow_http,
            "s3_allow_http should default to false (secure by default)"
        );
    }
}
