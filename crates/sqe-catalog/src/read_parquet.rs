//! `read_parquet(path, [named_args...])` table-valued function.
//!
//! Registers a `ListingTable` backed by Parquet files so users can write:
//!
//! ```sql
//! -- Local files (glob supported)
//! SELECT * FROM read_parquet('/data/*.parquet');
//!
//! -- S3 with inline credentials
//! SELECT * FROM read_parquet('s3://bucket/prefix/*.parquet',
//!     access_key => 'AKIA...',
//!     secret_key => '...',
//!     endpoint   => 'http://localhost:9000',
//!     region     => 'us-east-1');
//!
//! -- S3 using defaults from sqe.toml [storage] section
//! SELECT * FROM read_parquet('s3://bucket/prefix/*.parquet');
//! ```
//!
//! The function implements [`TableFunctionImpl`] which DataFusion's planner
//! calls during query planning. Because `call` is synchronous but schema
//! inference is async we use `crate::runtime_bridge::block_on_compat` to
//! drive the async work, which works on both multi-thread and
//! current-thread tokio runtimes (issue #83).

use std::sync::Arc;

use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionContext;
use datafusion_expr::Expr;
use object_store::aws::AmazonS3Builder;
use tracing::debug;

use sqe_core::config::{StorageConfig, TvfCaller};

/// Named keyword arguments accepted by `read_parquet()`.
#[derive(Debug, Clone, Default)]
struct ReadParquetArgs {
    path: String,
    access_key: Option<String>,
    secret_key: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    azure_account: Option<String>,
    azure_access_key: Option<String>,
    azure_sas_token: Option<String>,
    gcs_service_account_path: Option<String>,
    gcs_service_account_key: Option<String>,
}

impl ReadParquetArgs {
    /// Construct a new `ReadParquetArgs` with `path` replaced. Used when
    /// the caller resolves an `hf://` URL to its HTTPS form before
    /// schema inference. Returns a fresh value rather than mutating
    /// the input so the original `args` borrow stays valid.
    #[allow(dead_code)]
    fn clone_with_path(&self, path: String) -> Self {
        let mut out = self.clone();
        out.path = path;
        out
    }

    /// Project this struct onto the shared [`FileTvfArgs`] shape so the
    /// common register helpers (Azure / GCS) can be called.
    fn as_file_tvf_args(&self) -> crate::file_tvf_common::FileTvfArgs {
        crate::file_tvf_common::FileTvfArgs {
            path: self.path.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            azure_account: self.azure_account.clone(),
            azure_access_key: self.azure_access_key.clone(),
            azure_sas_token: self.azure_sas_token.clone(),
            gcs_service_account_path: self.gcs_service_account_path.clone(),
            gcs_service_account_key: self.gcs_service_account_key.clone(),
        }
    }
}

/// Extract positional + named arguments from the raw `Expr` slice that
/// DataFusion passes to [`TableFunctionImpl::call`].
///
/// Delegates to [`crate::file_tvf_common::parse_file_tvf_args`] so both
/// named-arg form (`access_key => 'x'`) and the DF54 positional-literal form
/// (`'access_key=x'`) are accepted for all storage credential keys.
/// `read_parquet` has no format-specific extra args, so the `extra` closure
/// always returns `false` (unknown keys produce a plan error).
fn parse_args(exprs: &[Expr]) -> DFResult<ReadParquetArgs> {
    let f = crate::file_tvf_common::parse_file_tvf_args("read_parquet", exprs, |_, _| false)?;
    Ok(ReadParquetArgs {
        path: f.path,
        access_key: f.access_key,
        secret_key: f.secret_key,
        endpoint: f.endpoint,
        region: f.region,
        azure_account: f.azure_account,
        azure_access_key: f.azure_access_key,
        azure_sas_token: f.azure_sas_token,
        gcs_service_account_path: f.gcs_service_account_path,
        gcs_service_account_key: f.gcs_service_account_key,
    })
}

/// Returns `true` when the path looks like an S3 URL.
fn is_s3_path(path: &str) -> bool {
    path.starts_with("s3://") || path.starts_with("s3a://")
}

// ──────────────────────────────────────────────────────────────────────────────
// Public struct
// ──────────────────────────────────────────────────────────────────────────────

/// DataFusion table-valued function that exposes Parquet files as a scannable
/// table, supporting both local filesystem paths and S3-compatible object
/// storage.
///
/// Register once per [`SessionContext`] via
/// `ctx.register_udtf("read_parquet", Arc::new(ReadParquetFunction::new(cfg)))`.
#[derive(Debug)]
pub struct ReadParquetFunction {
    /// Default S3 connection parameters from `sqe.toml`.
    /// Inline named arguments in the TVF call take precedence over these.
    storage: StorageConfig,
    /// Authenticated caller identity for the object-store prefix gate.
    /// `TvfCaller::default()` (anonymous, untrusted) fails closed.
    caller: TvfCaller,
}

impl ReadParquetFunction {
    /// Create a new `ReadParquetFunction` with the given storage defaults
    /// and an anonymous, untrusted caller (engine-credentialed object-store
    /// reads denied unless `[storage.tvf]` allows them without `{user}`).
    pub fn new(storage: StorageConfig) -> Self {
        Self {
            storage,
            caller: TvfCaller::default(),
        }
    }

    /// Create a new `ReadParquetFunction` bound to an authenticated caller.
    pub fn with_caller(storage: StorageConfig, caller: TvfCaller) -> Self {
        Self { storage, caller }
    }
}

impl TableFunctionImpl for ReadParquetFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let args = parse_args(exprs)?;
        // Issue #10: reject local-path and arbitrary HTTP-host arguments
        // BEFORE constructing the object store. Without this guard,
        // `read_parquet('/etc/shadow')` or
        // `read_parquet('http://169.254.169.254/...')` reached the
        // filesystem / IMDS endpoint. Object-store paths are additionally
        // gated per caller identity (E2E-identity item 1): the engine's
        // static storage key must not read arbitrary `s3://` paths.
        crate::file_tvf_common::enforce_tvf_path_policy(
            "read_parquet",
            &args.as_file_tvf_args(),
            &self.storage,
            &self.caller,
        )?;
        let storage = self.storage.clone();

        // `TableFunctionImpl::call` is sync; schema inference is async.
        // block_on_compat drives the future on multi-thread (block_in_place)
        // or current-thread (off-thread) runtimes (issue #83).
        crate::runtime_bridge::block_on_compat(async move {
            build_listing_table(&args, &storage).await
        })
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "read_parquet: no tokio runtime available".to_string(),
            )
        })?
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Asynchronously build a [`ListingTable`] for the requested path.
async fn build_listing_table(
    args: &ReadParquetArgs,
    storage: &StorageConfig,
) -> DFResult<Arc<dyn TableProvider>> {
    // V10: resolve hf:// to its HTTPS form before URL parsing so HF
    // dataset / model paths flow through the same httpfs path as raw
    // HTTPS URLs.
    let mut args_local;
    let args = if crate::file_tvf_common::is_hf_path(&args.path) {
        args_local = args.clone_with_path(args.path.clone());
        let tmp = crate::file_tvf_common::resolve_hf_url(&args.path).ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!(
                "read_parquet: malformed HuggingFace URL '{}'",
                args.path
            ))
        })?;
        args_local.path = tmp;
        &args_local
    } else {
        args
    };

    let listing_url = ListingTableUrl::parse(&args.path)?;

    // Create a temporary session context so we can register an object store
    // and then use it for schema inference.
    let tmp_ctx = SessionContext::new();

    if is_s3_path(&args.path) {
        let s3 = build_s3_store(args, storage)?;
        // `register_object_store` expects a `&url::Url` with only the
        // scheme + host (bucket) portion, e.g. `s3://my-bucket`.
        let bucket = extract_bucket(&args.path).ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!(
                "read_parquet: could not parse bucket from S3 URL '{}'",
                args.path
            ))
        })?;
        let scheme = if args.path.starts_with("s3a://") { "s3a" } else { "s3" };
        let store_url = url::Url::parse(&format!("{scheme}://{bucket}"))
            .map_err(|e| datafusion::error::DataFusionError::Plan(format!(
                "read_parquet: failed to build object-store URL: {e}"
            )))?;
        tmp_ctx.register_object_store(&store_url, Arc::new(s3));
        debug!(path = %args.path, "Registered S3 object store for read_parquet");
    } else if crate::file_tvf_common::is_azure_path(&args.path) {
        let common_args = args.as_file_tvf_args();
        crate::file_tvf_common::register_azure_store_if_needed(
            "read_parquet",
            &tmp_ctx,
            &common_args,
            storage,
        )?;
        debug!(path = %args.path, "Registered Azure object store for read_parquet");
    } else if crate::file_tvf_common::is_gcs_path(&args.path) {
        let common_args = args.as_file_tvf_args();
        crate::file_tvf_common::register_gcs_store_if_needed(
            "read_parquet",
            &tmp_ctx,
            &common_args,
            storage,
        )?;
        debug!(path = %args.path, "Registered GCS object store for read_parquet");
    } else {
        // V10 httpfs: HTTPS / HTTP paths use the shared HttpStore
        // builder. Parquet metadata reads need range requests, which
        // object_store::http supports.
        crate::file_tvf_common::register_http_store_if_needed(
            "read_parquet",
            &tmp_ctx,
            &args.path,
        )?;
    }
    // For local paths DataFusion's default LocalFileSystem is used automatically.

    let format = Arc::new(ParquetFormat::default());
    let listing_options = ListingOptions::new(format)
        .with_file_extension(".parquet");

    // Infer schema from the files (async — reads Parquet footers).
    let state = tmp_ctx.state();
    crate::file_tvf_common::ensure_local_files_exist(
        "read_parquet",
        &state,
        &listing_url,
        ".parquet",
        &args.path,
    )
    .await?;
    let schema = listing_options
        .infer_schema(&state, &listing_url)
        .await?;

    let config = ListingTableConfig::new(listing_url)
        .with_listing_options(listing_options)
        .with_schema(schema);

    let table = ListingTable::try_new(config)?;
    Ok(Arc::new(table))
}

/// Build an [`object_store::aws::AmazonS3`] from inline TVF args with
/// fallback to `StorageConfig` defaults.
fn build_s3_store(
    args: &ReadParquetArgs,
    storage: &StorageConfig,
) -> DFResult<object_store::aws::AmazonS3> {
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

    // Issue #46: reject inline `endpoint =>` overrides that bypass the
    // path allowlist. Without this, the `s3://...` path passes the #10
    // check but the endpoint flows straight into AmazonS3Builder and
    // pivots the request to IMDS.
    if args.endpoint.as_deref().is_some_and(|s| !s.is_empty()) {
        storage.tvf.check_endpoint(endpoint).map_err(|e| {
            datafusion::error::DataFusionError::Plan(format!("read_parquet: {e}"))
        })?;
    }

    let region = args
        .region
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.s3_region.as_str());

    // Extract bucket name from the URL (s3://bucket/...).
    let bucket = extract_bucket(&args.path).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(format!(
            "read_parquet: could not parse bucket from S3 URL '{}'",
            args.path
        ))
    })?;

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket);

    if !access_key.is_empty() {
        builder = builder.with_access_key_id(access_key);
    }
    if !secret_key.is_empty() {
        builder = builder.with_secret_access_key(secret_key);
    }
    if !endpoint.is_empty() {
        builder = builder.with_endpoint(endpoint);
    }
    if !region.is_empty() {
        builder = builder.with_region(region);
    }
    if storage.s3_path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }
    if storage.s3_allow_http {
        builder = builder.with_allow_http(true);
    }

    builder
        .build()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
}

/// Extract the bucket name from an `s3://bucket/...` or `s3a://bucket/...` URL.
fn extract_bucket(path: &str) -> Option<&str> {
    let after_scheme = path
        .strip_prefix("s3://")
        .or_else(|| path.strip_prefix("s3a://"))?;
    let bucket = after_scheme.split('/').next()?;
    if bucket.is_empty() { None } else { Some(bucket) }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
    use sqe_core::config::StorageConfig;

    fn default_storage() -> StorageConfig {
        StorageConfig::default()
    }

    // ── struct construction ──────────────────────────────────────────────────

    #[test]
    fn test_new_can_be_created() {
        let f = ReadParquetFunction::new(default_storage());
        assert!(f.storage.s3_endpoint.is_empty());
        assert!(!f.storage.s3_path_style);
        assert!(!f.storage.s3_allow_http);
    }

    // ── S3 path detection ────────────────────────────────────────────────────

    #[test]
    fn test_is_s3_path_s3_scheme() {
        assert!(is_s3_path("s3://my-bucket/prefix/file.parquet"));
    }

    #[test]
    fn test_is_s3_path_s3a_scheme() {
        assert!(is_s3_path("s3a://my-bucket/prefix/"));
    }

    #[test]
    fn test_is_s3_path_local_absolute() {
        assert!(!is_s3_path("/data/file.parquet"));
    }

    #[test]
    fn test_is_s3_path_local_glob() {
        assert!(!is_s3_path("/data/*.parquet"));
    }

    #[test]
    fn test_is_s3_path_relative() {
        assert!(!is_s3_path("relative/path.parquet"));
    }

    // ── bucket extraction ────────────────────────────────────────────────────

    #[test]
    fn test_extract_bucket_s3() {
        assert_eq!(extract_bucket("s3://my-bucket/key/file.parquet"), Some("my-bucket"));
    }

    #[test]
    fn test_extract_bucket_s3a() {
        assert_eq!(extract_bucket("s3a://another-bucket/"), Some("another-bucket"));
    }

    #[test]
    fn test_extract_bucket_no_bucket() {
        assert_eq!(extract_bucket("s3:///key"), None);
    }

    #[test]
    fn test_extract_bucket_local_returns_none() {
        assert_eq!(extract_bucket("/local/path.parquet"), None);
    }

    // ── named argument parsing ───────────────────────────────────────────────

    fn make_str_literal(s: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None)
    }

    fn make_named_arg(key: &str, value: &str) -> Expr {
        use datafusion_expr::{BinaryExpr, Operator};
        use datafusion::common::Column;
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified(key))),
            op: Operator::Eq,
            right: Box::new(make_str_literal(value)),
        })
    }

    #[test]
    fn test_parse_args_path_only() {
        let exprs = vec![make_str_literal("s3://bucket/file.parquet")];
        let args = parse_args(&exprs).unwrap();
        assert_eq!(args.path, "s3://bucket/file.parquet");
        assert!(args.access_key.is_none());
        assert!(args.secret_key.is_none());
        assert!(args.endpoint.is_none());
        assert!(args.region.is_none());
    }

    #[test]
    fn test_parse_args_with_named_args() {
        let exprs = vec![
            make_str_literal("s3://bucket/prefix/*.parquet"),
            make_named_arg("access_key", "AKID"),
            make_named_arg("secret_key", "SECRET"),
            make_named_arg("endpoint", "http://minio:9000"),
            make_named_arg("region", "us-east-1"),
        ];
        let args = parse_args(&exprs).unwrap();
        assert_eq!(args.path, "s3://bucket/prefix/*.parquet");
        assert_eq!(args.access_key.as_deref(), Some("AKID"));
        assert_eq!(args.secret_key.as_deref(), Some("SECRET"));
        assert_eq!(args.endpoint.as_deref(), Some("http://minio:9000"));
        assert_eq!(args.region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn test_parse_args_no_args_is_error() {
        let result = parse_args(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("at least one argument"));
    }

    #[test]
    fn test_parse_args_unknown_named_arg_is_error() {
        let exprs = vec![
            make_str_literal("/data/file.parquet"),
            make_named_arg("unknown_param", "value"),
        ];
        let result = parse_args(&exprs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown named argument"));
    }

    #[test]
    fn test_parse_args_non_string_path_is_error() {
        let exprs = vec![Expr::Literal(ScalarValue::Int64(Some(42)), None)];
        let result = parse_args(&exprs);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_args_positional_kv_literals() {
        // DF54 rewrite produces positional 'key=value' string literals for
        // named TVF args. This is the exact shape the benchmark CTAS load uses:
        //   SELECT * FROM read_parquet('s3://...', 'access_key=s3admin', ...)
        let exprs = vec![
            make_str_literal("s3://bucket/data.parquet"),
            make_str_literal("access_key=s3admin"),
            make_str_literal("secret_key=s3secret"),
            make_str_literal("region=us-east-1"),
        ];
        let args = parse_args(&exprs).unwrap();
        assert_eq!(args.path, "s3://bucket/data.parquet");
        assert_eq!(args.access_key.as_deref(), Some("s3admin"));
        assert_eq!(args.secret_key.as_deref(), Some("s3secret"));
        assert_eq!(args.region.as_deref(), Some("us-east-1"));
        assert!(args.endpoint.is_none());
    }

    #[test]
    fn test_parse_args_positional_all_credential_keys() {
        // Verify every supported credential key is reachable via positional literals.
        let exprs = vec![
            make_str_literal("azure://container/data.parquet"),
            make_str_literal("azure_account=myaccount"),
            make_str_literal("azure_access_key=mykey"),
            make_str_literal("azure_sas_token=sv=2021"),
            make_str_literal("gcs_service_account_path=/tmp/sa.json"),
            make_str_literal("gcs_service_account_key={\"type\":\"service_account\"}"),
        ];
        let args = parse_args(&exprs).unwrap();
        assert_eq!(args.azure_account.as_deref(), Some("myaccount"));
        assert_eq!(args.azure_access_key.as_deref(), Some("mykey"));
        assert_eq!(args.azure_sas_token.as_deref(), Some("sv=2021"));
        assert_eq!(args.gcs_service_account_path.as_deref(), Some("/tmp/sa.json"));
        assert_eq!(
            args.gcs_service_account_key.as_deref(),
            Some("{\"type\":\"service_account\"}")
        );
    }

    #[test]
    fn test_parse_args_positional_unknown_key_is_error() {
        let exprs = vec![
            make_str_literal("/data/file.parquet"),
            make_str_literal("unknown_param=value"),
        ];
        let result = parse_args(&exprs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown named argument"));
    }

    // ── S3 builder (unit-level, no network) ─────────────────────────────────

    #[test]
    fn test_build_s3_store_with_inline_args() {
        let args = ReadParquetArgs {
            path: "s3://my-bucket/data/*.parquet".to_string(),
            access_key: Some("AKID".to_string()),
            secret_key: Some("SECRET".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            region: Some("us-east-1".to_string()),
            ..ReadParquetArgs::default()
        };
        let storage = StorageConfig {
            s3_allow_http: true,
            tvf: sqe_core::config::TvfPolicy {
                allowed_http_hosts: vec!["localhost".to_string()],
                ..Default::default()
            },
            ..StorageConfig::default()
        };
        // Should succeed (no network call at build time).
        let result = build_s3_store(&args, &storage);
        assert!(result.is_ok(), "build_s3_store failed: {:?}", result.err());
    }

    #[test]
    fn test_build_s3_store_rejects_imds_endpoint() {
        // Issue #46: inline endpoint override must not be able to
        // pivot the S3 client at IMDS.
        let args = ReadParquetArgs {
            path: "s3://my-bucket/innocent.parquet".to_string(),
            endpoint: Some("http://169.254.169.254/latest/meta-data/".to_string()),
            ..ReadParquetArgs::default()
        };
        let storage = StorageConfig::default();
        let result = build_s3_store(&args, &storage);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("169.254.169.254"));
    }

    #[test]
    fn test_build_s3_store_falls_back_to_storage_config() {
        let args = ReadParquetArgs {
            path: "s3://my-bucket/data/*.parquet".to_string(),
            access_key: None,
            secret_key: None,
            endpoint: None,
            region: None,
            ..ReadParquetArgs::default()
        };
        let storage = StorageConfig {
            s3_access_key: "config-akid".to_string(),
            s3_secret_key: sqe_core::SecretString::new("config-secret".to_string()),
            s3_endpoint: "http://minio:9000".to_string(),
            s3_region: "eu-west-1".to_string(),
            s3_allow_http: true,
            ..StorageConfig::default()
        };
        let result = build_s3_store(&args, &storage);
        assert!(result.is_ok(), "build_s3_store fallback failed: {:?}", result.err());
    }
}
