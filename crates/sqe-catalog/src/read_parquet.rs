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
    /// The executing session's runtime environment. Inline-credential S3
    /// stores must be registered here as well as on the inference context,
    /// because the scan resolves object stores through the session registry
    /// at execution time. `None` (tests, standalone use) skips that
    /// registration and execution falls back to the engine's `[storage]`
    /// config — correct only when the inline endpoint matches it.
    runtime_env: Option<Arc<datafusion::execution::runtime_env::RuntimeEnv>>,
}

impl ReadParquetFunction {
    /// Create a new `ReadParquetFunction` with the given storage defaults
    /// and an anonymous, untrusted caller (engine-credentialed object-store
    /// reads denied unless `[storage.tvf]` allows them without `{user}`).
    pub fn new(storage: StorageConfig) -> Self {
        Self {
            storage,
            caller: TvfCaller::default(),
            runtime_env: None,
        }
    }

    /// Create a new `ReadParquetFunction` bound to an authenticated caller.
    pub fn with_caller(storage: StorageConfig, caller: TvfCaller) -> Self {
        Self {
            storage,
            caller,
            runtime_env: None,
        }
    }

    /// Bind the executing session's runtime environment so inline-credential
    /// object stores are visible to the scan, not just to schema inference.
    pub fn with_runtime_env(
        mut self,
        env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> Self {
        self.runtime_env = Some(env);
        self
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
        let runtime_env = self.runtime_env.clone();

        // `TableFunctionImpl::call` is sync; schema inference is async.
        // block_on_compat drives the future on multi-thread (block_in_place)
        // or current-thread (off-thread) runtimes (issue #83).
        crate::runtime_bridge::block_on_compat(async move {
            build_listing_table(&args, &storage, runtime_env.as_deref()).await
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
    runtime_env: Option<&datafusion::execution::runtime_env::RuntimeEnv>,
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
        // Register on the inference context AND the executing session's
        // runtime env: the temp context never runs the scan, and without
        // the session registration execution falls through to the lazy
        // fallback store built from `[storage]` — the wrong endpoint and
        // credentials whenever the inline `endpoint =>` differs from it.
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(s3);
        tmp_ctx.register_object_store(&store_url, Arc::clone(&store));
        if let Some(env) = runtime_env {
            env.register_object_store(&store_url, store);
        }
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

    // ── #363: stale listing-cache regression ─────────────────────────────────

    /// Write a Parquet file at `path` with `num_rows` single-column rows. The
    /// row count controls the file size, so a larger `num_rows` produces a
    /// strictly larger file at the same location.
    fn write_parquet_file(path: &std::path::Path, num_rows: usize) {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Int64Array = (0..num_rows as i64).collect::<Vec<_>>().into();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    async fn count_dir(ctx: &SessionContext, dir_url: &str, table: &str) -> DFResult<i64> {
        let url = ListingTableUrl::parse(dir_url).unwrap();
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet");
        let state = ctx.state();
        let schema = options.infer_schema(&state, &url).await?;
        let provider = ListingTable::try_new(
            ListingTableConfig::new(url)
                .with_listing_options(options)
                .with_schema(schema),
        )?;
        // Re-register the table on every call so each read re-lists through the
        // session's `list_files_cache` (the cache under test), mirroring how a
        // fresh `read_parquet(...)` TVF call resolves the directory each query.
        let _ = ctx.deregister_table(table);
        ctx.register_table(table, Arc::new(provider)).unwrap();
        let batches = ctx
            .sql(&format!("SELECT count(*) AS n FROM {table}"))
            .await?
            .collect()
            .await?;
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Int64Type;
        Ok(batches[0]
            .column(0)
            .as_primitive::<Int64Type>()
            .value(0))
    }

    /// #363: reading a directory, then reading it again after a file at the
    /// same path grows, must reflect the new file — not a stale cached listing.
    ///
    /// DataFusion 54 enables `list_files_cache` (infinite TTL) and
    /// `file_statistics_cache` by default. When an external writer (dbt loading
    /// into the raw bucket) replaces `dir/part-0.parquet` with a larger file
    /// after SQE has listed the directory once, the cached `ObjectMeta.size`
    /// goes stale. The next directory read computes the Parquet footer offset
    /// from the stale (smaller) size and fails with "Corrupt footer" — while a
    /// single-file read of the same object, which does not use the listing
    /// cache, succeeds. This test reproduces that divergence.
    #[tokio::test]
    async fn directory_read_reflects_grown_file_not_stale_listing() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("customer");
        std::fs::create_dir_all(&sub).unwrap();
        let file = sub.join("part-0.parquet");

        // Small file first, then a much larger file at the same path.
        write_parquet_file(&file, 4);
        let dir_url = format!("file://{}/", sub.display());

        // Build the session with the same cache config SQE's coordinator and
        // embedded runtimes use (#363): the on-by-default, infinite-TTL
        // list_files_cache is disabled so a directory relists per read.
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        let runtime = RuntimeEnvBuilder::new()
            .with_cache_manager(crate::lazy_object_store::external_store_cache_config())
            .build_arc()
            .unwrap();
        let ctx = SessionContext::new_with_config_rt(Default::default(), runtime);
        let first = count_dir(&ctx, &dir_url, "t").await.unwrap();
        assert_eq!(first, 4, "baseline directory read");

        // External writer replaces the file with a strictly larger one.
        write_parquet_file(&file, 50_000);

        let second = count_dir(&ctx, &dir_url, "t").await.unwrap();
        assert_eq!(
            second, 50_000,
            "directory read after the file grew must see the new file, \
             not a stale cached listing/footer size"
        );
    }

    /// #363: the crash branch. Demonstrates that with DataFusion 54's default
    /// caches, a directory read after the file grows fails with the exact
    /// "Corrupt footer" error the issue reports.
    ///
    /// Same root cause as the test above (a stale `list_files_cache` entry),
    /// but a different downstream symptom depending on metadata-cache state:
    ///   * If the footer-metadata cache still holds a valid entry for the old
    ///     (small) `ObjectMeta`, `count(*)` is answered from cached statistics
    ///     and silently returns the STALE COUNT (the test above).
    ///   * If it does not (metadata cache empty/limited — the demo case), the
    ///     reader calls `fetch_metadata` with the stale, smaller size and reads
    ///     the "footer" from the middle of the now-larger file, producing
    ///     "Invalid Parquet file. Corrupt footer".
    /// Both branches vanish once `list_files_cache` is disabled, so the fix
    /// covers both; this test pins the crash symptom so a future "add a TTL
    /// instead of disabling" change cannot silently reintroduce it.
    #[tokio::test]
    async fn stale_listing_causes_corrupt_footer_with_default_caches() {
        use datafusion::execution::cache::cache_manager::CacheManagerConfig;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;

        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("customer");
        std::fs::create_dir_all(&sub).unwrap();
        let file = sub.join("part-0.parquet");
        write_parquet_file(&file, 4);
        let dir_url = format!("file://{}/", sub.display());

        // DataFusion 54 defaults: list_files_cache ON (infinite TTL). Disable
        // only the per-file metadata/statistics caches so `count(*)` must fetch
        // the footer (rather than answer from cached stats), driving the read
        // through the stale cached size — exactly the demo's code path.
        let runtime = RuntimeEnvBuilder::new()
            .with_cache_manager(
                CacheManagerConfig::default()
                    .with_metadata_cache_limit(0)
                    .with_file_statistics_cache_limit(0),
            )
            .build_arc()
            .unwrap();
        let ctx = SessionContext::new_with_config_rt(Default::default(), runtime);

        assert_eq!(count_dir(&ctx, &dir_url, "t").await.unwrap(), 4);
        write_parquet_file(&file, 50_000);
        let err = count_dir(&ctx, &dir_url, "t")
            .await
            .expect_err("stale list_files_cache size must corrupt the footer read");
        let msg = err.to_string();
        assert!(
            msg.contains("Corrupt footer") || msg.to_lowercase().contains("footer"),
            "expected a corrupt-footer error, got: {msg}"
        );

        // Same sequence with list_files_cache disabled (the #363 fix) succeeds.
        // Reset the file to the small size first — the crash branch above grew
        // it to 50k, and this section must start from the small file again.
        write_parquet_file(&file, 4);
        let fixed_runtime = RuntimeEnvBuilder::new()
            .with_cache_manager(
                crate::lazy_object_store::external_store_cache_config()
                    .with_metadata_cache_limit(0)
                    .with_file_statistics_cache_limit(0),
            )
            .build_arc()
            .unwrap();
        let fixed_ctx = SessionContext::new_with_config_rt(Default::default(), fixed_runtime);
        assert_eq!(count_dir(&fixed_ctx, &dir_url, "t").await.unwrap(), 4);
        write_parquet_file(&file, 50_000);
        assert_eq!(
            count_dir(&fixed_ctx, &dir_url, "t").await.unwrap(),
            50_000,
            "with list_files_cache disabled the grown file reads cleanly"
        );
    }
}
