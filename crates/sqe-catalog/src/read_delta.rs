//! `read_delta(path, [named_args...])` table-valued function (V11).
//!
//! Closes the Delta Lake gap from the DuckDB compatibility audit. The
//! TVF wraps `deltalake::open_table` so users can query a Delta table
//! root directly:
//!
//! ```sql
//! SELECT count(*) FROM read_delta('/data/delta/sales');
//!
//! SELECT * FROM read_delta('s3://bucket/delta/orders',
//!     access_key => 'AKIA...',
//!     secret_key => '...');
//! ```
//!
//! Read-only. Writes (INSERT, UPDATE, DELETE, MERGE) require the full
//! deltalake transaction pipeline and are not exposed here.
//!
//! Path parsing reuses [`file_tvf_common`]: `s3://`, `s3a://`, and local
//! filesystem paths all flow through. HTTPS / hf:// URL support lands in
//! V10's lazy HTTP object store; once that MR is merged, deltalake will
//! pick up `rewrite_hf_path_in_place` and the lazy registry without further
//! changes here.
//!
//! Time-travel: pass `version => '<int>'` to pin a snapshot, or
//! `timestamp => '<RFC3339>'` to pin to the snapshot active at that
//! instant. Mutually exclusive — supplying both is a plan error.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion_expr::Expr;
use deltalake_core::delta_datafusion::{DeltaScanConfigBuilder, DeltaTableProvider};
use deltalake_core::open_table_with_storage_options;
use tracing::debug;

use sqe_core::config::StorageConfig;

use crate::file_tvf_common::{parse_file_tvf_args, FileTvfArgs};

const FN_NAME: &str = "read_delta";

#[derive(Debug, Default)]
struct DeltaOpts {
    version: Option<u64>,
    timestamp: Option<String>,
}

#[derive(Debug)]
pub struct ReadDeltaFunction {
    storage: StorageConfig,
}

impl ReadDeltaFunction {
    pub fn new(storage: StorageConfig) -> Self {
        Self { storage }
    }
}

impl TableFunctionImpl for ReadDeltaFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let mut delta_opts = DeltaOpts::default();
        let mut parse_err: Option<DataFusionError> = None;

        let args = parse_file_tvf_args(FN_NAME, exprs, |key, value| {
            match key {
                "version" => match value.parse::<u64>() {
                    Ok(v) => delta_opts.version = Some(v),
                    Err(e) => {
                        parse_err = Some(DataFusionError::Plan(format!(
                            "{FN_NAME}: 'version' must be a non-negative integer, got '{value}': {e}"
                        )));
                    }
                },
                "timestamp" => delta_opts.timestamp = Some(value.to_string()),
                _ => return false,
            }
            true
        })?;

        if let Some(e) = parse_err {
            return Err(e);
        }

        if delta_opts.version.is_some() && delta_opts.timestamp.is_some() {
            return Err(DataFusionError::Plan(format!(
                "{FN_NAME}: 'version' and 'timestamp' are mutually exclusive"
            )));
        }

        // Issue #10: TVF path / host policy check.
        self.storage.tvf.check(&args.path).map_err(|e| {
            DataFusionError::Plan(format!("{FN_NAME}: {e}"))
        })?;

        let storage = self.storage.clone();
        crate::runtime_bridge::block_on_compat(async move {
            build_delta_provider(args, delta_opts, storage).await
        })
        .ok_or_else(|| {
            DataFusionError::Plan(format!("{FN_NAME}: no tokio runtime available"))
        })?
    }
}

async fn build_delta_provider(
    args: FileTvfArgs,
    delta_opts: DeltaOpts,
    storage: StorageConfig,
) -> DFResult<Arc<dyn TableProvider>> {
    // Local filesystem paths need a `file://` prefix so deltalake's URL
    // parser accepts them. Anything that already has a scheme (s3, https,
    // hf-resolved-to-https, ...) flows through unchanged.
    let url_str = if args.path.contains("://") {
        args.path.clone()
    } else {
        let abs = std::fs::canonicalize(&args.path).unwrap_or_else(|_| args.path.clone().into());
        format!("file://{}", abs.display())
    };

    let table_url = url::Url::parse(&url_str).map_err(|e| {
        DataFusionError::Plan(format!(
            "{FN_NAME}: failed to parse URL '{url_str}': {e}"
        ))
    })?;

    let storage_options = build_storage_options(&args, &storage);

    let mut table = open_table_with_storage_options(table_url, storage_options)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Time travel: load_version reuses the same DeltaTable; load_with_datetime
    // does a fresh snapshot resolution against the log.
    if let Some(v) = delta_opts.version {
        table
            .load_version(v)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    } else if let Some(ts) = delta_opts.timestamp {
        let dt = chrono::DateTime::parse_from_rfc3339(&ts)
            .map_err(|e| {
                DataFusionError::Plan(format!(
                    "{FN_NAME}: 'timestamp' must be RFC3339, got '{ts}': {e}"
                ))
            })?
            .with_timezone(&chrono::Utc);
        table
            .load_with_datetime(dt)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    let snapshot = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .snapshot()
        .clone();
    let log_store = table.log_store();

    let scan_config = DeltaScanConfigBuilder::new()
        .build(&snapshot)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let provider = DeltaTableProvider::try_new(snapshot, log_store, scan_config)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    debug!(path = %args.path, "read_delta: built DeltaTableProvider");
    Ok(Arc::new(provider))
}

/// Construct deltalake's `storage_options` map from the TVF's S3
/// credentials + the engine's [`StorageConfig`] defaults.
///
/// deltalake's S3 backend reads keys with the `AWS_*` and
/// `AWS_ENDPOINT_URL` names; mirror what we already pass to
/// `read_parquet`'s S3 builder.
fn build_storage_options(
    args: &FileTvfArgs,
    storage: &StorageConfig,
) -> HashMap<String, String> {
    let mut out = HashMap::new();

    let access_key = args
        .access_key
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.s3_access_key.as_str());
    let secret_key = args
        .secret_key
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.s3_secret_key.expose());
    let endpoint = args
        .endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.s3_endpoint.as_str());
    let region = args
        .region
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.s3_region.as_str());

    if !access_key.is_empty() {
        out.insert("AWS_ACCESS_KEY_ID".to_string(), access_key.to_string());
    }
    if !secret_key.is_empty() {
        out.insert("AWS_SECRET_ACCESS_KEY".to_string(), secret_key.to_string());
    }
    if !endpoint.is_empty() {
        out.insert("AWS_ENDPOINT_URL".to_string(), endpoint.to_string());
    }
    if !region.is_empty() {
        out.insert("AWS_REGION".to_string(), region.to_string());
    }
    if storage.s3_allow_http {
        out.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
    }

    // Azure: deltalake reads AZURE_* keys.
    let azure_account = args
        .azure_account
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.azure_account.as_str());
    let azure_access_key = args
        .azure_access_key
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.azure_access_key.as_str());
    let azure_sas_token = args
        .azure_sas_token
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.azure_sas_token.as_str());
    if !azure_account.is_empty() {
        out.insert("AZURE_STORAGE_ACCOUNT_NAME".to_string(), azure_account.to_string());
    }
    if !azure_access_key.is_empty() {
        out.insert("AZURE_STORAGE_ACCOUNT_KEY".to_string(), azure_access_key.to_string());
    }
    if !azure_sas_token.is_empty() {
        out.insert("AZURE_STORAGE_SAS_TOKEN".to_string(), azure_sas_token.to_string());
    }
    if storage.azure_use_emulator {
        out.insert("AZURE_USE_EMULATOR".to_string(), "true".to_string());
    }

    // GCS: deltalake reads GOOGLE_* keys.
    let gcs_path = args
        .gcs_service_account_path
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.gcs_service_account_path.as_str());
    let gcs_key = args
        .gcs_service_account_key
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.gcs_service_account_key.as_str());
    if !gcs_path.is_empty() {
        out.insert(
            "GOOGLE_SERVICE_ACCOUNT_PATH".to_string(),
            gcs_path.to_string(),
        );
    }
    if !gcs_key.is_empty() {
        out.insert(
            "GOOGLE_SERVICE_ACCOUNT_KEY".to_string(),
            gcs_key.to_string(),
        );
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_can_be_created() {
        let f = ReadDeltaFunction::new(StorageConfig::default());
        assert!(matches!(f.storage, StorageConfig { .. }));
    }

    #[test]
    fn storage_options_populated_from_args() {
        let args = FileTvfArgs {
            path: "s3://bucket/delta".to_string(),
            access_key: Some("AKID".to_string()),
            secret_key: Some("SECRET".to_string()),
            endpoint: Some("http://minio:9000".to_string()),
            region: Some("eu-west-1".to_string()),
            ..FileTvfArgs::default()
        };
        let storage = StorageConfig {
            s3_allow_http: true,
            ..StorageConfig::default()
        };
        let opts = build_storage_options(&args, &storage);
        assert_eq!(opts.get("AWS_ACCESS_KEY_ID").map(|s| s.as_str()), Some("AKID"));
        assert_eq!(
            opts.get("AWS_SECRET_ACCESS_KEY").map(|s| s.as_str()),
            Some("SECRET")
        );
        assert_eq!(
            opts.get("AWS_ENDPOINT_URL").map(|s| s.as_str()),
            Some("http://minio:9000")
        );
        assert_eq!(opts.get("AWS_REGION").map(|s| s.as_str()), Some("eu-west-1"));
        assert_eq!(opts.get("AWS_ALLOW_HTTP").map(|s| s.as_str()), Some("true"));
    }

    #[test]
    fn storage_options_populated_for_azure() {
        let args = FileTvfArgs {
            path: "abfss://container@account.dfs.core.windows.net/delta".to_string(),
            azure_account: Some("account".to_string()),
            azure_access_key: Some("AZ_KEY".to_string()),
            ..FileTvfArgs::default()
        };
        let opts = build_storage_options(&args, &StorageConfig::default());
        assert_eq!(
            opts.get("AZURE_STORAGE_ACCOUNT_NAME").map(|s| s.as_str()),
            Some("account")
        );
        assert_eq!(
            opts.get("AZURE_STORAGE_ACCOUNT_KEY").map(|s| s.as_str()),
            Some("AZ_KEY")
        );
    }

    #[test]
    fn storage_options_populated_for_gcs() {
        let args = FileTvfArgs {
            path: "gs://bucket/delta".to_string(),
            gcs_service_account_path: Some("/var/secrets/gcs.json".to_string()),
            ..FileTvfArgs::default()
        };
        let opts = build_storage_options(&args, &StorageConfig::default());
        assert_eq!(
            opts.get("GOOGLE_SERVICE_ACCOUNT_PATH").map(|s| s.as_str()),
            Some("/var/secrets/gcs.json")
        );
    }

    #[test]
    fn storage_options_falls_back_to_storage_config() {
        let args = FileTvfArgs {
            path: "s3://bucket/delta".to_string(),
            access_key: None,
            secret_key: None,
            endpoint: None,
            region: None,
            ..FileTvfArgs::default()
        };
        let storage = StorageConfig {
            s3_access_key: "config-akid".to_string(),
            s3_secret_key: sqe_core::SecretString::new("config-secret".to_string()),
            s3_endpoint: "http://minio".to_string(),
            s3_region: "us-east-1".to_string(),
            ..StorageConfig::default()
        };
        let opts = build_storage_options(&args, &storage);
        assert_eq!(
            opts.get("AWS_ACCESS_KEY_ID").map(|s| s.as_str()),
            Some("config-akid")
        );
        assert_eq!(opts.get("AWS_REGION").map(|s| s.as_str()), Some("us-east-1"));
    }

    #[test]
    fn storage_options_omits_empty_values() {
        let args = FileTvfArgs::default();
        let storage = StorageConfig::default();
        let opts = build_storage_options(&args, &storage);
        assert!(opts.is_empty(), "no creds means no AWS_* keys, got {opts:?}");
    }
}
