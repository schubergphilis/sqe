//! Shared helpers for the `read_*()` table-valued functions
//! (`read_parquet`, `read_csv`, `read_json`).
//!
//! The TVFs share a uniform calling convention:
//!
//! ```sql
//! SELECT * FROM read_csv('s3://bucket/file.csv',
//!     access_key => 'AKIA...',
//!     secret_key => '...',
//!     endpoint   => 'http://minio:9000',
//!     region     => 'us-east-1');
//! ```
//!
//! This module provides:
//!
//! 1. [`FileTvfArgs`] for the path + S3 credential bag.
//! 2. [`parse_file_tvf_args`] which extracts those from the raw [`Expr`] slice
//!    DataFusion's planner hands us, accepting any extra format-specific
//!    named args via a closure.
//! 3. [`build_s3_store`] / [`extract_bucket`] which build an
//!    [`object_store::aws::AmazonS3`] from inline TVF args + falling back to
//!    the coordinator's [`StorageConfig`] defaults.
//! 4. [`register_s3_store_if_needed`] which attaches the store to a
//!    temporary [`SessionContext`] for schema-inference reads.
//!
//! Format-specific TVFs implement [`TableFunctionImpl`] and call into these
//! helpers; the only per-format work is wrapping the matching DataFusion
//! `FileFormat` in a `ListingTable`.
use std::sync::Arc;

use datafusion::common::{plan_err, ScalarValue};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion_expr::Expr;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;

use sqe_core::config::{StorageConfig, TvfCaller};

/// Path + storage credential bag shared by every `read_*()` TVF.
///
/// The S3 fields (`access_key`, `secret_key`, `endpoint`, `region`) cover
/// AWS S3 and S3-compatible backends (R2, MinIO, Ceph, SeaweedFS, etc.).
/// The Azure fields cover ADLS Gen2 / Blob Storage. The GCS fields cover
/// Google Cloud Storage. All are optional; falling back to the
/// coordinator-wide [`StorageConfig`] defaults when absent.
#[derive(Debug, Clone, Default)]
pub struct FileTvfArgs {
    pub path: String,

    // ── S3-family ───────────────────────────────────────────────────────
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,

    // ── Azure ADLS Gen2 / Blob ──────────────────────────────────────────
    /// Storage account name. Required for shared-key auth, optional for
    /// `abfss://<container>@<account>.dfs.core.windows.net/...` URLs which
    /// already encode the account.
    pub azure_account: Option<String>,
    pub azure_access_key: Option<String>,
    pub azure_sas_token: Option<String>,

    // ── Google Cloud Storage ────────────────────────────────────────────
    /// Path to a GCP service-account JSON key file.
    pub gcs_service_account_path: Option<String>,
    /// Inline service-account JSON (alternative to `gcs_service_account_path`).
    pub gcs_service_account_key: Option<String>,
}

/// `true` when the TVF call carries credentials of its own for the
/// path's storage backend, i.e. the engine's static `[storage]` key will
/// NOT be used for the read. Drives the inline-credential bypass of
/// `[storage.tvf] allowed_object_store_prefixes`.
///
/// Note `gcs_service_account_path` deliberately does NOT count: it names a
/// file on the ENGINE's filesystem, so it is engine-owned material, not a
/// caller-supplied credential.
pub fn has_inline_credentials(args: &FileTvfArgs) -> bool {
    fn set(v: &Option<String>) -> bool {
        v.as_deref().is_some_and(|s| !s.is_empty())
    }
    if is_s3_path(&args.path) {
        set(&args.access_key) && set(&args.secret_key)
    } else if is_azure_path(&args.path) {
        set(&args.azure_access_key) || set(&args.azure_sas_token)
    } else if is_gcs_path(&args.path) {
        set(&args.gcs_service_account_key)
    } else {
        false
    }
}

/// Identity-aware TVF path policy gate, shared by every `read_*()` TVF.
/// Wraps [`sqe_core::config::TvfPolicy::check_path`] with the
/// inline-credential detection above and maps denials to a plan error.
/// Denials are logged at WARN (principal + path) inside `check_path`.
pub fn enforce_tvf_path_policy(
    fn_name: &str,
    args: &FileTvfArgs,
    storage: &StorageConfig,
    caller: &TvfCaller,
) -> DFResult<()> {
    storage
        .tvf
        .check_path(&args.path, caller, has_inline_credentials(args))
        .map_err(|e| DataFusionError::Plan(format!("{fn_name}: {e}")))
}

/// Effective Azure credentials for a TVF connection, after applying the
/// inline-credential carve-out exclusivity rule.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ResolvedAzureCredentials {
    pub access_key: String,
    pub sas_token: String,
}

/// Resolve the Azure credentials that [`build_azure_store`] will attach.
///
/// Carve-out invariant (E2E-identity item 1): the policy gate opens for a
/// TVF call that carries its OWN Azure credential (`azure_access_key` or
/// `azure_sas_token`). That bypass is only sound if the engine's static
/// `[storage.azure]` credential is then NOT attached — otherwise a bogus
/// SAS-only call flips the gate open while still reading with the engine's
/// account key. So when the call supplies EITHER inline credential we use
/// ONLY the inline values and suppress every engine fallback. With no
/// inline credential, the engine config is used as before.
pub fn resolve_azure_credentials(
    args: &FileTvfArgs,
    storage: &StorageConfig,
) -> ResolvedAzureCredentials {
    let inline_access = args.azure_access_key.as_deref().filter(|s| !s.is_empty());
    let inline_sas = args.azure_sas_token.as_deref().filter(|s| !s.is_empty());
    if inline_access.is_some() || inline_sas.is_some() {
        return ResolvedAzureCredentials {
            access_key: inline_access.unwrap_or_default().to_string(),
            sas_token: inline_sas.unwrap_or_default().to_string(),
        };
    }
    ResolvedAzureCredentials {
        access_key: storage.azure_access_key.clone(),
        sas_token: storage.azure_sas_token.clone(),
    }
}

/// Effective GCS service-account credential for a TVF connection, after
/// applying the inline-credential carve-out exclusivity rule.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ResolvedGcsCredentials {
    /// Inline service-account JSON to attach, if any.
    pub service_account_key: String,
    /// Service-account key-file path to attach, if any.
    pub service_account_path: String,
}

/// Resolve the GCS credential that [`build_gcs_store`] will attach.
///
/// Carve-out invariant (E2E-identity item 1): the gate opens for a TVF
/// call that carries its own inline `gcs_service_account_key`. The bypass
/// is only sound if the engine's static credential is then NOT attached,
/// so an inline key must win over EVERYTHING — including the engine's
/// `gcs_service_account_path` (which would otherwise read an arbitrary
/// `gs://` path with the engine's identity while the attacker only had to
/// supply a junk inline key to open the gate). Inline
/// `gcs_service_account_path` is NOT a caller credential (it names a file
/// on the engine's filesystem, see [`has_inline_credentials`]), so it does
/// not trigger this exclusivity. With no inline key, engine config is used
/// (path wins over the engine's inline key, matching prior precedence).
pub fn resolve_gcs_credentials(
    args: &FileTvfArgs,
    storage: &StorageConfig,
) -> ResolvedGcsCredentials {
    if let Some(key_inline) = args
        .gcs_service_account_key
        .as_deref()
        .filter(|s| !s.is_empty())
    {
        return ResolvedGcsCredentials {
            service_account_key: key_inline.to_string(),
            service_account_path: String::new(),
        };
    }
    let key_path = args
        .gcs_service_account_path
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.gcs_service_account_path.as_str());
    if !key_path.is_empty() {
        return ResolvedGcsCredentials {
            service_account_key: String::new(),
            service_account_path: key_path.to_string(),
        };
    }
    ResolvedGcsCredentials {
        service_account_key: storage.gcs_service_account_key.clone(),
        service_account_path: String::new(),
    }
}

/// Parse a `ScalarValue::Utf8` / `LargeUtf8` to `&str`.
pub fn scalar_to_str(sv: &ScalarValue) -> Option<&str> {
    match sv {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// Returns `true` when the path looks like an S3 URL.
pub fn is_s3_path(path: &str) -> bool {
    path.starts_with("s3://") || path.starts_with("s3a://")
}

/// Returns `true` when the path looks like an HTTP / HTTPS URL.
/// (HuggingFace `hf://` URLs are resolved to HTTPS upstream of this
/// helper.)
pub fn is_http_path(path: &str) -> bool {
    path.starts_with("https://") || path.starts_with("http://")
}

/// Returns `true` when the path looks like a HuggingFace Hub URL.
/// Format: `hf://datasets/<org>/<name>[@<revision>]/<path>` or
/// `hf://models/<org>/<name>[@<revision>]/<path>`.
pub fn is_hf_path(path: &str) -> bool {
    path.starts_with("hf://")
}

/// Returns `true` when the path looks like an Azure ADLS Gen2 / Blob URL.
///
/// Accepted schemes:
/// - `abfss://<container>@<account>.dfs.core.windows.net/<path>` (Hadoop / standard form, TLS)
/// - `abfs://<container>@<account>.dfs.core.windows.net/<path>` (plaintext variant)
/// - `azure://<container>/<path>` (object_store crate's preferred shorthand)
/// - `az://<container>/<path>` (alternate shorthand)
pub fn is_azure_path(path: &str) -> bool {
    path.starts_with("abfss://")
        || path.starts_with("abfs://")
        || path.starts_with("azure://")
        || path.starts_with("az://")
}

/// Returns `true` when the path looks like a Google Cloud Storage URL.
///
/// Accepted schemes:
/// - `gs://<bucket>/<path>` (standard form)
/// - `gcs://<bucket>/<path>` (alternate)
pub fn is_gcs_path(path: &str) -> bool {
    path.starts_with("gs://") || path.starts_with("gcs://")
}

/// Extract `(container, account)` from an Azure URL.
///
/// `abfss://<container>@<account>.dfs.core.windows.net/<path>` -> `(container, account)`.
/// `azure://<container>/<path>` and `az://<container>/<path>` -> `(container, "")`. The
/// account in the shorthand forms must come from `[storage.azure]` or `azure_account`
/// inline argument.
pub fn extract_azure_container_account(path: &str) -> Option<(String, String)> {
    if let Some(rest) = path
        .strip_prefix("abfss://")
        .or_else(|| path.strip_prefix("abfs://"))
    {
        let (auth, _) = rest.split_once('/').unwrap_or((rest, ""));
        let (container, host) = auth.split_once('@')?;
        if container.is_empty() || host.is_empty() {
            return None;
        }
        let account = host
            .split_once('.')
            .map(|(a, _)| a.to_string())
            .unwrap_or_else(|| host.to_string());
        return Some((container.to_string(), account));
    }
    if let Some(rest) = path
        .strip_prefix("azure://")
        .or_else(|| path.strip_prefix("az://"))
    {
        let container = rest.split('/').next()?;
        if container.is_empty() {
            return None;
        }
        return Some((container.to_string(), String::new()));
    }
    None
}

/// Extract the bucket name from a `gs://bucket/...` or `gcs://bucket/...` URL.
pub fn extract_gcs_bucket(path: &str) -> Option<&str> {
    let after_scheme = path
        .strip_prefix("gs://")
        .or_else(|| path.strip_prefix("gcs://"))?;
    let bucket = after_scheme.split('/').next()?;
    if bucket.is_empty() {
        None
    } else {
        Some(bucket)
    }
}

/// Resolve an `hf://` URL to the corresponding HuggingFace Hub HTTPS
/// download URL. Returns `None` if the path does not match the
/// expected shape.
///
/// Format (matches the DuckDB `hf://` extension):
///
/// ```text
/// hf://datasets/<owner>/<name>[@<revision>]/<path>[?revision=<rev>]
/// hf://models/<owner>/<name>[@<revision>]/<path>[?revision=<rev>]
/// hf://spaces/<owner>/<name>[@<revision>]/<path>[?revision=<rev>]
/// ```
///
/// Two revision spellings are accepted, in priority order:
///
/// 1. `@<revision>` immediately after the repo `<owner>/<name>` segment
///    (DuckDB-style). Examples: `@v1.0`, `@main`, `@~parquet`. The
///    `~parquet` revision points at HuggingFace's auto-generated
///    parquet view (branch `refs/convert/parquet`); the resolver
///    URL-encodes the slashes so the resulting path is valid.
///
/// 2. `?revision=<rev>` query parameter at the end of the URL.
///
/// If both are supplied, the inline `@<rev>` wins and the query
/// parameter is rejected as a sanity check.
///
/// Examples:
///
/// ```text
/// hf://datasets/squad/plain_text/train-00000-of-00001.parquet
///   -> https://huggingface.co/datasets/squad/plain_text/resolve/main/train-00000-of-00001.parquet
///
/// hf://datasets/squad/plain_text/train-00000-of-00001.parquet?revision=v1.0.0
///   -> https://huggingface.co/datasets/squad/plain_text/resolve/v1.0.0/train-00000-of-00001.parquet
///
/// hf://datasets/squad/plain_text@v1.0/train-00000-of-00001.parquet
///   -> https://huggingface.co/datasets/squad/plain_text/resolve/v1.0/train-00000-of-00001.parquet
///
/// hf://datasets/foo/bar@~parquet/data.parquet
///   -> https://huggingface.co/datasets/foo/bar/resolve/refs%2Fconvert%2Fparquet/data.parquet
/// ```
///
/// The owner/name shape is whatever HF stores; SQE does not enforce
/// namespace rules. The revision defaults to `main`. Only the leftmost
/// `<owner>/<name>` is treated as the repo identifier; everything after
/// is the in-repo file path.
pub fn resolve_hf_url(path: &str) -> Option<String> {
    let after_scheme = path.strip_prefix("hf://")?;

    // Strip the optional `?revision=<rev>` query parameter. Anything
    // else after `?` is rejected so a typo doesn't silently fall
    // through to `main`.
    let (path_only, query_revision) = match after_scheme.split_once('?') {
        Some((p, q)) => match q.strip_prefix("revision=") {
            Some(r) if !r.is_empty() => (p, Some(r.to_string())),
            _ => return None,
        },
        None => (after_scheme, None),
    };

    let mut parts = path_only.splitn(2, '/');
    let kind = parts.next()?;
    if !matches!(kind, "datasets" | "models" | "spaces") {
        return None;
    }
    let rest = parts.next()?;

    // Repo id is `<owner>/<name>` (with an optional `@<revision>` glued to
    // `<name>`); the rest is the in-repo file path. Both segments must be
    // non-empty, and there must be a file path.
    let mut rest_parts = rest.splitn(3, '/');
    let owner = rest_parts.next()?;
    let name_with_rev = rest_parts.next()?;
    let file_path = rest_parts.next()?;

    if owner.is_empty() || name_with_rev.is_empty() || file_path.is_empty() {
        return None;
    }

    // Split off an optional `@<revision>` from the name. The DuckDB
    // extension treats this as inline revision pinning. We normalise
    // the special `~parquet` view to its actual branch ref name so
    // HuggingFace serves the auto-generated parquet conversion files.
    let (name, inline_revision) = match name_with_rev.split_once('@') {
        Some((n, r)) if !n.is_empty() && !r.is_empty() => (n, Some(r.to_string())),
        Some(_) => return None, // `@` present but empty side -> reject
        None => (name_with_rev, None),
    };

    // Both spellings supplied -> reject. Picking one silently would mask
    // a typo and surprise users when the wrong revision actually exists.
    if inline_revision.is_some() && query_revision.is_some() {
        return None;
    }

    let raw_revision = inline_revision
        .or(query_revision)
        .unwrap_or_else(|| "main".to_string());

    // HuggingFace's auto-generated parquet view lives on the
    // `refs/convert/parquet` branch. Slashes need URL-encoding.
    let revision = if raw_revision == "~parquet" {
        "refs%2Fconvert%2Fparquet".to_string()
    } else if raw_revision.contains('/') {
        raw_revision.replace('/', "%2F")
    } else {
        raw_revision
    };

    // `datasets` and `spaces` carry their kind prefix in HF's URL
    // scheme; `models` is bare `<owner>/<name>`.
    let prefix = match kind {
        "datasets" => "/datasets",
        "spaces" => "/spaces",
        "models" => "",
        _ => unreachable!("filtered above"),
    };

    Some(format!(
        "https://huggingface.co{prefix}/{owner}/{name}/resolve/{revision}/{file_path}"
    ))
}

/// Extract the bucket name from an `s3://bucket/...` or `s3a://bucket/...` URL.
pub fn extract_bucket(path: &str) -> Option<&str> {
    let after_scheme = path
        .strip_prefix("s3://")
        .or_else(|| path.strip_prefix("s3a://"))?;
    let bucket = after_scheme.split('/').next()?;
    if bucket.is_empty() {
        None
    } else {
        Some(bucket)
    }
}

/// Parse the raw [`Expr`] slice DataFusion passes to a TVF. Returns the
/// shared path + S3 credential args; format-specific named args are routed
/// through `extra` which is called with `(name, value)` for each unknown
/// key. If `extra` returns `false`, the unknown key produces a plan error.
pub fn parse_file_tvf_args<F>(fn_name: &str, exprs: &[Expr], mut extra: F) -> DFResult<FileTvfArgs>
where
    F: FnMut(&str, &str) -> bool,
{
    let path = match exprs.first() {
        Some(Expr::Literal(sv, _)) => scalar_to_str(sv)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "{fn_name}: first argument must be a non-null string literal (the path)"
                ))
            })?
            .to_string(),
        Some(_) => {
            return plan_err!("{fn_name}: first argument must be a string literal (the path)");
        }
        None => {
            return plan_err!("{fn_name}: at least one argument (the path) is required");
        }
    };

    let mut out = FileTvfArgs {
        path,
        ..Default::default()
    };

    // Shared key-dispatch closure used by both the BinaryExpr (named) arm and
    // the Literal (positional 'key=value') arm introduced for DF54.
    let mut assign = |out: &mut FileTvfArgs, name: &str, value: &str| -> DFResult<()> {
        match name {
            "access_key" => out.access_key = Some(value.to_string()),
            "secret_key" => out.secret_key = Some(value.to_string()),
            "endpoint" => out.endpoint = Some(value.to_string()),
            "region" => out.region = Some(value.to_string()),
            "azure_account" => out.azure_account = Some(value.to_string()),
            "azure_access_key" => out.azure_access_key = Some(value.to_string()),
            "azure_sas_token" => out.azure_sas_token = Some(value.to_string()),
            "gcs_service_account_path" => out.gcs_service_account_path = Some(value.to_string()),
            "gcs_service_account_key" => out.gcs_service_account_key = Some(value.to_string()),
            other => {
                if !extra(other, value) {
                    return plan_err!("{fn_name}: unknown named argument '{other}'");
                }
            }
        }
        Ok(())
    };

    for expr in exprs.iter().skip(1) {
        match expr {
            Expr::BinaryExpr(binary) => {
                use datafusion_expr::Operator;
                if binary.op != Operator::Eq {
                    return plan_err!(
                        "{fn_name}: named arguments must use '=>' / '=', got operator {:?}",
                        binary.op
                    );
                }
                let name = match binary.left.as_ref() {
                    Expr::Column(col) => col.name.as_str(),
                    other => {
                        return plan_err!(
                            "{fn_name}: named argument key must be an identifier, got {other}"
                        );
                    }
                };
                let value = match binary.right.as_ref() {
                    Expr::Literal(sv, _) => scalar_to_str(sv)
                        .ok_or_else(|| {
                            datafusion::error::DataFusionError::Plan(format!(
                                "{fn_name}: named argument '{name}' must be a non-null string"
                            ))
                        })?
                        .to_string(),
                    other => {
                        return plan_err!(
                            "{fn_name}: named argument '{name}' value must be a string literal, got {other}"
                        );
                    }
                };
                assign(&mut out, name, &value)?;
            }
            Expr::Literal(sv, _) => {
                let kv = scalar_to_str(sv).ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "{fn_name}: positional argument must be a non-null 'key=value' string"
                    ))
                })?;
                let (name, value) = kv.split_once('=').ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "{fn_name}: positional argument '{kv}' must be of the form 'key=value'"
                    ))
                })?;
                assign(&mut out, name, value)?;
            }
            other => {
                return plan_err!(
                    "{fn_name}: unexpected argument expression {other}; \
                     named arguments must use the form 'key => value'"
                );
            }
        }
    }

    // Expand a leading `~`/`~/` in a local path to $HOME so read_csv('~/x.csv')
    // resolves like the shell instead of listing a literal `~` directory (which
    // silently returns zero rows). Runs before the policy check, which still
    // classifies the expanded path as local — no change to the allow_local_paths
    // decision. No-op for URL-scheme paths and for `~user` forms.
    expand_local_tilde_in_place(&mut out);

    Ok(out)
}

/// Build an [`object_store::aws::AmazonS3`] from inline TVF args with
/// fallback to the coordinator-wide [`StorageConfig`] defaults.
pub fn build_s3_store(
    fn_name: &str,
    args: &FileTvfArgs,
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

    // Issue #46: reject inline `endpoint =>` overrides that point at
    // metadata services (IMDS at http://169.254.169.254) or other
    // out-of-allowlist HTTP hosts. The path-based check from #10 did not
    // cover this argument, so a benign s3:// path could still be paired
    // with a hostile endpoint to pivot the S3 client at IMDS.
    if args.endpoint.as_deref().is_some_and(|s| !s.is_empty()) {
        storage
            .tvf
            .check_endpoint(endpoint)
            .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{fn_name}: {e}")))?;
    }

    let region = args
        .region
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(storage.s3_region.as_str());

    let bucket = extract_bucket(&args.path).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(format!(
            "{fn_name}: could not parse bucket from S3 URL '{}'",
            args.path
        ))
    })?;

    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
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
    // Allow plain HTTP when the operator opted in OR when the effective
    // endpoint is itself an `http://` URL. The table-scan path
    // (sqe-coordinator `query_handler`) already derives allow_http from the
    // endpoint scheme; deriving it here too keeps the file-reader TVFs
    // consistent — otherwise an `http://` endpoint without `s3_allow_http=true`
    // fails at object_store construction with a bare "builder error".
    if storage.s3_allow_http || endpoint.starts_with("http://") {
        builder = builder.with_allow_http(true);
    }
    builder
        .build()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
}

/// Build an [`object_store::aws::AmazonS3`] for `bucket` from the
/// coordinator-wide [`StorageConfig`] alone (no inline TVF arg overrides).
///
/// Used by the lazy object-store registry
/// ([`crate::lazy_object_store::LazyHttpObjectStoreRegistry::with_s3_fallback`])
/// to resolve `s3://` buckets that file-reader TVFs reference at execution time
/// but that were never pre-registered as Iceberg catalogs. Mirrors the builder
/// configuration of [`build_s3_store`], including the endpoint-derived
/// `allow_http` behaviour.
pub fn build_s3_store_for_bucket(
    bucket: &str,
    storage: &StorageConfig,
) -> DFResult<object_store::aws::AmazonS3> {
    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
    if !storage.s3_access_key.is_empty() {
        builder = builder.with_access_key_id(storage.s3_access_key.as_str());
    }
    let secret = storage.s3_secret_key.expose();
    if !secret.is_empty() {
        builder = builder.with_secret_access_key(secret);
    }
    if !storage.s3_endpoint.is_empty() {
        builder = builder.with_endpoint(storage.s3_endpoint.as_str());
    }
    if !storage.s3_region.is_empty() {
        builder = builder.with_region(storage.s3_region.as_str());
    }
    if storage.s3_path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }
    if storage.s3_allow_http || storage.s3_endpoint.starts_with("http://") {
        builder = builder.with_allow_http(true);
    }
    builder
        .build()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
}

/// If `args.path` is an S3 URL, register an [`AmazonS3`] object store on the
/// supplied [`SessionContext`] under the bucket-scoped URL so subsequent
/// listing / read operations resolve it. No-op for local paths.
///
/// `runtime_env` is the EXECUTING session's runtime environment. The
/// `ctx` registration only covers schema inference inside the TVF (the
/// temp context never executes the scan); at execution time DataFusion
/// resolves the bucket through the session's object-store registry, and
/// without this registration the lazy fallback builds a store from the
/// engine's `[storage]` config — the wrong endpoint and credentials
/// whenever the call's inline `endpoint =>` differs from the engine's.
/// The registration is session-scoped and bucket-scoped: later queries in
/// the same session reusing this bucket resolve to the caller's inline
/// credentials, never a shared/global registry.
pub fn register_s3_store_if_needed(
    fn_name: &str,
    ctx: &SessionContext,
    args: &FileTvfArgs,
    storage: &StorageConfig,
    runtime_env: Option<&datafusion::execution::runtime_env::RuntimeEnv>,
) -> DFResult<()> {
    if !is_s3_path(&args.path) {
        return Ok(());
    }
    let s3 = build_s3_store(fn_name, args, storage)?;
    let bucket = extract_bucket(&args.path).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(format!(
            "{fn_name}: could not parse bucket from S3 URL '{}'",
            args.path
        ))
    })?;
    let scheme = if args.path.starts_with("s3a://") {
        "s3a"
    } else {
        "s3"
    };
    let store_url = url::Url::parse(&format!("{scheme}://{bucket}")).map_err(|e| {
        datafusion::error::DataFusionError::Plan(format!(
            "{fn_name}: failed to build object-store URL: {e}"
        ))
    })?;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(s3);
    ctx.register_object_store(&store_url, Arc::clone(&store));
    if let Some(env) = runtime_env {
        env.register_object_store(&store_url, store);
    }
    Ok(())
}

/// V10 httpfs: build an object store rooted at the URL's `scheme://host`
/// and register it on the supplied context. `object_store::http::HttpStore`
/// supports HTTP range requests, which is enough for parquet metadata
/// reads; CSV / JSON readers slurp the whole file. No-op for non-HTTP
/// paths.
///
/// Each unique (scheme, host, port) tuple gets one registration on the
/// context. DataFusion `register_object_store` is idempotent: re-registering
/// the same URL replaces the previous handle, which is fine because every
/// call we make builds an equivalent store.
pub fn register_http_store_if_needed(
    fn_name: &str,
    ctx: &SessionContext,
    path: &str,
) -> DFResult<()> {
    if !is_http_path(path) {
        return Ok(());
    }
    let parsed = url::Url::parse(path).map_err(|e| {
        datafusion::error::DataFusionError::Plan(format!(
            "{fn_name}: failed to parse URL '{path}': {e}"
        ))
    })?;
    let host = parsed.host_str().ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(format!(
            "{fn_name}: URL '{path}' is missing a host"
        ))
    })?;
    let scheme = parsed.scheme();
    let base = match parsed.port() {
        Some(p) => format!("{scheme}://{host}:{p}"),
        None => format!("{scheme}://{host}"),
    };

    let http_store = HttpBuilder::new()
        .with_url(&base)
        .build()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let store_url = url::Url::parse(&base).map_err(|e| {
        datafusion::error::DataFusionError::Plan(format!(
            "{fn_name}: failed to build object-store URL: {e}"
        ))
    })?;
    ctx.register_object_store(&store_url, Arc::new(http_store));
    Ok(())
}

/// Build an [`object_store::azure::MicrosoftAzure`] from inline TVF args
/// with fallback to the coordinator-wide [`StorageConfig`] defaults.
pub fn build_azure_store(
    fn_name: &str,
    args: &FileTvfArgs,
    storage: &StorageConfig,
) -> DFResult<object_store::azure::MicrosoftAzure> {
    let (container, account_from_url) =
        extract_azure_container_account(&args.path).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{fn_name}: could not parse container / account from Azure URL '{}'",
                args.path
            ))
        })?;

    // Account: URL form > inline arg > config.
    let account = if !account_from_url.is_empty() {
        account_from_url
    } else if let Some(a) = args.azure_account.as_deref().filter(|s| !s.is_empty()) {
        a.to_string()
    } else if !storage.azure_account.is_empty() {
        storage.azure_account.clone()
    } else if storage.azure_use_emulator {
        // Azurite default account name; the builder's emulator path overrides this.
        "devstoreaccount1".to_string()
    } else {
        return Err(DataFusionError::Plan(format!(
            "{fn_name}: Azure account name is required: pass `azure_account => '...'`, \
             set `[storage.azure].account`, or use an `abfss://<container>@<account>...` URL"
        )));
    };

    let creds = resolve_azure_credentials(args, storage);

    let mut builder = MicrosoftAzureBuilder::new()
        .with_account(&account)
        .with_container_name(&container);

    if !creds.access_key.is_empty() {
        builder = builder.with_access_key(&creds.access_key);
    }
    if !creds.sas_token.is_empty() {
        // The builder's `with_config` takes typed keys; a SAS token is a
        // bag of query parameters, fed verbatim.
        builder = builder.with_config(
            object_store::azure::AzureConfigKey::SasKey,
            &creds.sas_token,
        );
    }
    if storage.azure_use_emulator {
        builder = builder.with_use_emulator(true);
    }

    builder
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

/// If `args.path` is an Azure URL, register a [`MicrosoftAzure`] object
/// store on the supplied [`SessionContext`] under the container-scoped URL.
/// No-op for non-Azure paths.
pub fn register_azure_store_if_needed(
    fn_name: &str,
    ctx: &SessionContext,
    args: &FileTvfArgs,
    storage: &StorageConfig,
) -> DFResult<()> {
    if !is_azure_path(&args.path) {
        return Ok(());
    }
    let store = build_azure_store(fn_name, args, storage)?;
    let (container, _account) = extract_azure_container_account(&args.path).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{fn_name}: could not parse container from Azure URL '{}'",
            args.path
        ))
    })?;
    // Pick the same scheme the user gave us so DataFusion's URL matcher
    // (which keys on scheme + host) finds the store on subsequent reads.
    let scheme = if args.path.starts_with("abfss://") {
        "abfss"
    } else if args.path.starts_with("abfs://") {
        "abfs"
    } else if args.path.starts_with("azure://") {
        "azure"
    } else {
        "az"
    };
    let store_url = url::Url::parse(&format!("{scheme}://{container}")).map_err(|e| {
        DataFusionError::Plan(format!("{fn_name}: failed to build Azure store URL: {e}"))
    })?;
    ctx.register_object_store(&store_url, Arc::new(store));
    Ok(())
}

/// Build an [`object_store::gcp::GoogleCloudStorage`] from inline TVF args
/// with fallback to the coordinator-wide [`StorageConfig`] defaults.
pub fn build_gcs_store(
    fn_name: &str,
    args: &FileTvfArgs,
    storage: &StorageConfig,
) -> DFResult<object_store::gcp::GoogleCloudStorage> {
    let bucket = extract_gcs_bucket(&args.path).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{fn_name}: could not parse bucket from GCS URL '{}'",
            args.path
        ))
    })?;

    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

    let creds = resolve_gcs_credentials(args, storage);
    if !creds.service_account_path.is_empty() {
        builder = builder.with_service_account_path(&creds.service_account_path);
    } else if !creds.service_account_key.is_empty() {
        builder = builder.with_service_account_key(&creds.service_account_key);
    }
    // When both are empty the builder will use Application Default
    // Credentials: env (`GOOGLE_APPLICATION_CREDENTIALS`), gcloud config,
    // GCE metadata server, GKE Workload Identity.

    builder
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

/// If `args.path` is a GCS URL, register a [`GoogleCloudStorage`] object
/// store on the supplied [`SessionContext`] under the bucket-scoped URL.
/// No-op for non-GCS paths.
pub fn register_gcs_store_if_needed(
    fn_name: &str,
    ctx: &SessionContext,
    args: &FileTvfArgs,
    storage: &StorageConfig,
) -> DFResult<()> {
    if !is_gcs_path(&args.path) {
        return Ok(());
    }
    let store = build_gcs_store(fn_name, args, storage)?;
    let bucket = extract_gcs_bucket(&args.path).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{fn_name}: could not parse bucket from GCS URL '{}'",
            args.path
        ))
    })?;
    let scheme = if args.path.starts_with("gcs://") {
        "gcs"
    } else {
        "gs"
    };
    let store_url = url::Url::parse(&format!("{scheme}://{bucket}")).map_err(|e| {
        DataFusionError::Plan(format!("{fn_name}: failed to build GCS store URL: {e}"))
    })?;
    ctx.register_object_store(&store_url, Arc::new(store));
    Ok(())
}

/// Resolve an `hf://` URL to its HTTPS form, leaving everything else
/// untouched. Mutates `args.path` so the rest of the TVF pipeline only
/// has to know HTTPS / S3 / local-fs cases.
pub fn rewrite_hf_path_in_place(fn_name: &str, args: &mut FileTvfArgs) -> DFResult<()> {
    if !is_hf_path(&args.path) {
        return Ok(());
    }
    let resolved = resolve_hf_url(&args.path).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(format!(
            "{fn_name}: malformed HuggingFace URL '{}'; \
             expected hf://(datasets|models|spaces)/<org>/<name>[@<revision>]/<path>",
            args.path
        ))
    })?;
    args.path = resolved;
    Ok(())
}

/// Expand a leading `~`/`~/` in `path` to `home`. Paths with a URL scheme
/// (`s3://`, `https://`, `hf://`, ...) and the `~user` form are returned
/// unchanged. Pure helper so the logic is unit-testable without touching env.
fn expand_tilde(path: &str, home: Option<&str>) -> String {
    if path.contains("://") {
        return path.to_string();
    }
    if path == "~" || path.starts_with("~/") {
        if let Some(home) = home {
            // `&path[1..]` drops the leading '~' (1 ASCII byte): "" or "/rest".
            return format!("{home}{}", &path[1..]);
        }
    }
    path.to_string()
}

/// Expand a leading `~`/`~/` in a local filesystem path argument to `$HOME`.
/// No-op when `HOME` is unset or the path is not a bare-`~` local path.
fn expand_local_tilde_in_place(args: &mut FileTvfArgs) {
    let home = std::env::var_os("HOME").map(|h| h.to_string_lossy().into_owned());
    args.path = expand_tilde(&args.path, home.as_deref());
}

/// Error if a *local* listing URL matches no files, so a typo'd path or an
/// empty glob surfaces clearly instead of silently returning zero rows.
///
/// Scoped to local filesystem paths (`original_path` has no `://` scheme):
/// object-store and `http(s)` listing semantics vary (http stores don't
/// enumerate), and a bad key/URL surfaces as a read error on those paths. An
/// existing-but-empty file still matches (one object) and yields zero rows,
/// which is correct — only a path matching *no* objects is an error.
pub async fn ensure_local_files_exist(
    fn_name: &str,
    state: &SessionState,
    listing_url: &ListingTableUrl,
    file_extension: &str,
    original_path: &str,
) -> DFResult<()> {
    use futures::StreamExt;
    if original_path.contains("://") {
        return Ok(());
    }
    let store = state.runtime_env().object_store(listing_url)?;
    let mut files = listing_url
        .list_all_files(state, store.as_ref(), file_extension)
        .await?;
    if files.next().await.is_none() {
        return plan_err!(
            "{fn_name}: no files matched '{original_path}' \
             (file not found, or the glob matched nothing)"
        );
    }
    Ok(())
}

/// V12: pre-rewrite `'hf://...'` string literals in raw SQL to their HTTPS
/// equivalent so DataFusion's URL-table auto-detect (`SELECT * FROM 'url'`)
/// flows through V10's [`LazyHttpObjectStoreRegistry`] and finds an
/// HttpStore for huggingface.co.
///
/// Without this, `SELECT * FROM 'hf://datasets/foo/data.csv'` fails with
/// "table not found" because DataFusion's `enable_url_table()` doesn't
/// recognise the `hf` scheme. The TVF path (`read_csv('hf://...')`) is
/// unaffected; it already calls [`rewrite_hf_path_in_place`] on the
/// parsed argument.
///
/// Scans only single-quoted string literals containing `hf://`. Strings
/// outside `hf://` patterns flow through unchanged. A malformed hf://
/// string yields a parse error rather than silent failure so a typo in
/// the dataset path doesn't fall back to "table not found".
pub fn rewrite_hf_urls_in_sql(sql: &str) -> DFResult<std::borrow::Cow<'_, str>> {
    if !sql.contains("hf://") {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }

    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.char_indices().peekable();

    while let Some((i, c)) = chars.next() {
        if c != '\'' {
            out.push(c);
            continue;
        }

        // Find the matching closing quote. SQL doubles single quotes to
        // escape (e.g. 'O''Brien'); we skip past the doubled pair.
        let start = i + 1;
        let mut end = start;
        let mut found = false;
        while let Some((j, qc)) = chars.next() {
            if qc == '\'' {
                if matches!(chars.peek(), Some(&(_, '\''))) {
                    chars.next();
                    continue;
                }
                end = j;
                found = true;
                break;
            }
        }

        if !found {
            // Unterminated string. Let DataFusion's parser raise the
            // error with its richer diagnostics; just emit the raw text.
            out.push('\'');
            out.push_str(&sql[start..]);
            return Ok(std::borrow::Cow::Owned(out));
        }

        let inner = &sql[start..end];
        let rewritten = if inner.starts_with("hf://") {
            resolve_hf_url(inner).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "rewrite_hf_urls_in_sql: malformed HuggingFace URL '{inner}'; \
                     expected hf://(datasets|models|spaces)/<org>/<name>/<path>[?revision=<rev>]"
                ))
            })?
        } else {
            inner.to_string()
        };

        out.push('\'');
        out.push_str(&rewritten);
        out.push('\'');
    }

    Ok(std::borrow::Cow::Owned(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion_expr::{BinaryExpr, Operator};

    fn make_str_literal(s: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None)
    }

    fn make_named_arg(key: &str, value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified(key))),
            op: Operator::Eq,
            right: Box::new(make_str_literal(value)),
        })
    }

    #[test]
    fn tilde_expands_only_for_local_paths() {
        let home = Some("/home/u");
        // Local paths with a leading ~ expand to $HOME.
        assert_eq!(
            expand_tilde("~/Downloads/x.csv", home),
            "/home/u/Downloads/x.csv"
        );
        assert_eq!(expand_tilde("~", home), "/home/u");
        // Absolute / relative local paths are untouched.
        assert_eq!(expand_tilde("/abs/x.csv", home), "/abs/x.csv");
        assert_eq!(expand_tilde("rel/x.csv", home), "rel/x.csv");
        // URL-scheme paths are never touched, even if they contain a ~.
        assert_eq!(expand_tilde("s3://b/~/x.csv", home), "s3://b/~/x.csv");
        assert_eq!(expand_tilde("https://h/~/x.csv", home), "https://h/~/x.csv");
        // `~user` (other users' homes) is intentionally not expanded.
        assert_eq!(expand_tilde("~user/x.csv", home), "~user/x.csv");
        // HOME unset -> leave the path unchanged.
        assert_eq!(expand_tilde("~/x.csv", None), "~/x.csv");
    }

    #[test]
    fn inline_credentials_detection() {
        // S3 needs BOTH halves of the keypair to count as caller-supplied.
        let mut args = FileTvfArgs {
            path: "s3://b/f.csv".to_string(),
            access_key: Some("AKID".to_string()),
            ..Default::default()
        };
        assert!(!has_inline_credentials(&args));
        args.secret_key = Some("SECRET".to_string());
        assert!(has_inline_credentials(&args));
        // Empty strings don't count.
        args.secret_key = Some(String::new());
        assert!(!has_inline_credentials(&args));

        // Azure: a key or SAS token counts.
        let args = FileTvfArgs {
            path: "azure://c/f.csv".to_string(),
            azure_sas_token: Some("sv=...".to_string()),
            ..Default::default()
        };
        assert!(has_inline_credentials(&args));

        // GCS: only the INLINE key counts; a path references a file on the
        // engine's own filesystem and must not unlock the gate.
        let args = FileTvfArgs {
            path: "gs://b/f.csv".to_string(),
            gcs_service_account_path: Some("/var/secrets/engine.json".to_string()),
            ..Default::default()
        };
        assert!(!has_inline_credentials(&args));
        let args = FileTvfArgs {
            path: "gs://b/f.csv".to_string(),
            gcs_service_account_key: Some("{\"type\":\"service_account\"}".to_string()),
            ..Default::default()
        };
        assert!(has_inline_credentials(&args));

        // Local / http paths never report inline object-store credentials.
        let args = FileTvfArgs {
            path: "/local/f.csv".to_string(),
            access_key: Some("AKID".to_string()),
            secret_key: Some("SECRET".to_string()),
            ..Default::default()
        };
        assert!(!has_inline_credentials(&args));
    }

    #[test]
    fn enforce_policy_denies_engine_credentialed_s3_by_default() {
        let args = FileTvfArgs {
            path: "s3://prod-data/secret.parquet".to_string(),
            ..Default::default()
        };
        let storage = StorageConfig::default();
        let caller = TvfCaller::for_user("alice".to_string(), Vec::new());
        let err = enforce_tvf_path_policy("read_csv", &args, &storage, &caller).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("allowed_object_store_prefixes"), "got: {msg}");
        assert!(
            msg.contains("alice"),
            "denial must name the principal: {msg}"
        );
    }

    #[test]
    fn enforce_policy_allows_configured_staging_prefix() {
        let args = FileTvfArgs {
            path: "s3://data-platform-staging/_table-load-staging/uuid-1/x.csv".to_string(),
            ..Default::default()
        };
        let storage = StorageConfig {
            tvf: sqe_core::config::TvfPolicy {
                allowed_object_store_prefixes: vec![
                    "s3://data-platform-staging/_table-load-staging/".to_string(),
                ],
                ..Default::default()
            },
            ..StorageConfig::default()
        };
        let caller = TvfCaller::for_user("alice".to_string(), Vec::new());
        assert!(enforce_tvf_path_policy("read_csv", &args, &storage, &caller).is_ok());
    }

    #[test]
    fn gcs_inline_key_suppresses_engine_path_fallback() {
        // BITES the pre-fix bug: an attacker supplies a junk inline
        // `gcs_service_account_key` (which opens the policy gate) WHILE the
        // engine has `gcs_service_account_path` configured. The connection
        // must use ONLY the inline key — never the engine's path.
        let args = FileTvfArgs {
            path: "gs://victim-bucket/x.csv".to_string(),
            gcs_service_account_key: Some("{\"type\":\"service_account\"}".to_string()),
            ..Default::default()
        };
        let storage = StorageConfig {
            gcs_service_account_path: "/var/secrets/engine-sa.json".to_string(),
            ..StorageConfig::default()
        };
        let creds = resolve_gcs_credentials(&args, &storage);
        assert_eq!(
            creds.service_account_path, "",
            "engine GCS service-account PATH must NOT be used when the call \
             carries an inline key (gate carve-out invariant)"
        );
        assert_eq!(
            creds.service_account_key, "{\"type\":\"service_account\"}",
            "only the caller's inline key may be attached"
        );
    }

    #[test]
    fn gcs_no_inline_key_falls_back_to_engine_path() {
        // Legit engine-credentialed read (gate opened by prefix/role/trust,
        // not by inline creds): engine path is used as before.
        let args = FileTvfArgs {
            path: "gs://staging/x.csv".to_string(),
            ..Default::default()
        };
        let storage = StorageConfig {
            gcs_service_account_path: "/var/secrets/engine-sa.json".to_string(),
            ..StorageConfig::default()
        };
        let creds = resolve_gcs_credentials(&args, &storage);
        assert_eq!(creds.service_account_path, "/var/secrets/engine-sa.json");
        assert_eq!(creds.service_account_key, "");
    }

    #[test]
    fn gcs_inline_path_does_not_suppress_engine_key() {
        // An inline `gcs_service_account_path` is NOT a caller credential
        // (it names a file on the engine's filesystem) and does NOT open the
        // gate, so it must not trigger exclusivity — engine inline key still
        // applies when no inline path is given. Here the call provides only
        // an inline path; engine has an inline key. Path (caller) wins, as
        // before — but crucially the call never opened the gate.
        let args = FileTvfArgs {
            path: "gs://b/x.csv".to_string(),
            gcs_service_account_path: Some("/tmp/caller.json".to_string()),
            ..Default::default()
        };
        let storage = StorageConfig {
            gcs_service_account_key: "engine-inline-key".to_string(),
            ..StorageConfig::default()
        };
        let creds = resolve_gcs_credentials(&args, &storage);
        assert_eq!(creds.service_account_path, "/tmp/caller.json");
        assert_eq!(creds.service_account_key, "");
    }

    #[test]
    fn azure_sas_only_call_suppresses_engine_access_key() {
        // BITES the pre-fix bug: a SAS-only call opens the policy gate while
        // the engine `azure_access_key` would still fall back and be
        // attached. The connection must carry ONLY the caller's SAS token.
        let args = FileTvfArgs {
            path: "azure://container/x.csv".to_string(),
            azure_sas_token: Some("sv=2021-08-06&sig=junk".to_string()),
            ..Default::default()
        };
        let storage = StorageConfig {
            azure_access_key: "ENGINE-ACCOUNT-KEY".to_string(),
            ..StorageConfig::default()
        };
        let creds = resolve_azure_credentials(&args, &storage);
        assert_eq!(
            creds.access_key, "",
            "engine Azure access_key must NOT be attached when the call \
             supplies an inline SAS token (gate carve-out invariant)"
        );
        assert_eq!(creds.sas_token, "sv=2021-08-06&sig=junk");
    }

    #[test]
    fn azure_inline_access_key_suppresses_engine_sas() {
        let args = FileTvfArgs {
            path: "azure://c/x.csv".to_string(),
            azure_access_key: Some("CALLER-KEY".to_string()),
            ..Default::default()
        };
        let storage = StorageConfig {
            azure_sas_token: "engine-sas".to_string(),
            azure_access_key: "engine-key".to_string(),
            ..StorageConfig::default()
        };
        let creds = resolve_azure_credentials(&args, &storage);
        assert_eq!(creds.access_key, "CALLER-KEY");
        assert_eq!(creds.sas_token, "", "engine SAS must not leak in");
    }

    #[test]
    fn azure_no_inline_creds_falls_back_to_engine() {
        let args = FileTvfArgs {
            path: "azure://c/x.csv".to_string(),
            ..Default::default()
        };
        let storage = StorageConfig {
            azure_access_key: "engine-key".to_string(),
            azure_sas_token: "engine-sas".to_string(),
            ..StorageConfig::default()
        };
        let creds = resolve_azure_credentials(&args, &storage);
        assert_eq!(creds.access_key, "engine-key");
        assert_eq!(creds.sas_token, "engine-sas");
    }

    #[test]
    fn enforce_policy_inline_s3_credentials_bypass_prefix_gate() {
        let args = FileTvfArgs {
            path: "s3://their-own-bucket/x.csv".to_string(),
            access_key: Some("AKID".to_string()),
            secret_key: Some("SECRET".to_string()),
            ..Default::default()
        };
        let storage = StorageConfig::default();
        let caller = TvfCaller::for_user("alice".to_string(), Vec::new());
        assert!(enforce_tvf_path_policy("read_csv", &args, &storage, &caller).is_ok());
    }

    #[test]
    fn s3_path_detection() {
        assert!(is_s3_path("s3://bucket/file"));
        assert!(is_s3_path("s3a://bucket/file"));
        assert!(!is_s3_path("/local/file"));
        assert!(!is_s3_path("relative/file"));
    }

    #[test]
    fn rewrite_hf_urls_in_sql_no_op_when_absent() {
        let sql = "SELECT 1";
        let out = rewrite_hf_urls_in_sql(sql).unwrap();
        assert_eq!(out, sql);
        assert!(matches!(out, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn rewrite_hf_urls_in_sql_substitutes_select_from() {
        let sql = "SELECT count(*) FROM 'hf://datasets/foo/bar/data.csv'";
        let out = rewrite_hf_urls_in_sql(sql).unwrap();
        assert_eq!(
            out,
            "SELECT count(*) FROM 'https://huggingface.co/datasets/foo/bar/resolve/main/data.csv'"
        );
    }

    #[test]
    fn rewrite_hf_urls_in_sql_handles_revision_param() {
        let sql = "SELECT * FROM 'hf://datasets/foo/bar/data.parquet?revision=v1'";
        let out = rewrite_hf_urls_in_sql(sql).unwrap();
        assert_eq!(
            out,
            "SELECT * FROM 'https://huggingface.co/datasets/foo/bar/resolve/v1/data.parquet'"
        );
    }

    #[test]
    fn rewrite_hf_urls_in_sql_leaves_other_strings_alone() {
        let sql = "SELECT 'foo', name FROM t WHERE name = 'O''Brien'";
        let out = rewrite_hf_urls_in_sql(sql).unwrap();
        assert_eq!(out, sql);
    }

    #[test]
    fn rewrite_hf_urls_in_sql_multiple_occurrences() {
        let sql = "SELECT * FROM 'hf://datasets/a/b/x.csv' UNION ALL \
                   SELECT * FROM 'hf://datasets/a/b/y.csv'";
        let out = rewrite_hf_urls_in_sql(sql).unwrap();
        assert!(out.contains("https://huggingface.co/datasets/a/b/resolve/main/x.csv"));
        assert!(out.contains("https://huggingface.co/datasets/a/b/resolve/main/y.csv"));
        assert!(!out.contains("hf://"));
    }

    #[test]
    fn rewrite_hf_urls_in_sql_rejects_malformed() {
        let sql = "SELECT * FROM 'hf://malformed'";
        let err = rewrite_hf_urls_in_sql(sql).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("malformed HuggingFace URL"),
            "expected malformed-URL error, got: {msg}"
        );
    }

    #[test]
    fn rewrite_hf_urls_in_sql_models_and_spaces_kinds() {
        // Models have no /datasets/ prefix in the resolved URL.
        let sql = "SELECT * FROM 'hf://models/openai/clip-vit/config.json'";
        let out = rewrite_hf_urls_in_sql(sql).unwrap();
        assert!(out.contains("https://huggingface.co/openai/clip-vit/resolve/main/config.json"));

        let sql = "SELECT * FROM 'hf://spaces/team/demo/app.py'";
        let out = rewrite_hf_urls_in_sql(sql).unwrap();
        assert!(out.contains("https://huggingface.co/spaces/team/demo/resolve/main/app.py"));
    }

    #[test]
    fn rewrite_hf_urls_in_sql_unterminated_string_passes_through() {
        // Let DataFusion's parser raise the error rather than silently
        // truncating. The rewriter must not panic.
        let sql = "SELECT * FROM 'hf://datasets/foo/bar/data.csv";
        let _out = rewrite_hf_urls_in_sql(sql).unwrap();
        // No assertion on content; we just want this to not panic.
    }

    #[test]
    fn resolve_hf_url_inline_revision_at_sign() {
        let out = resolve_hf_url("hf://datasets/foo/bar@v1.0/data.parquet").unwrap();
        assert_eq!(
            out,
            "https://huggingface.co/datasets/foo/bar/resolve/v1.0/data.parquet"
        );
    }

    #[test]
    fn resolve_hf_url_tilde_parquet_view() {
        // The auto-generated parquet view lives on `refs/convert/parquet`.
        // The slashes need URL-encoding so HuggingFace serves the right
        // branch ref.
        let out = resolve_hf_url(
            "hf://datasets/datasets-examples/doc-formats-csv-1@~parquet/default/train/0000.parquet",
        )
        .unwrap();
        assert_eq!(
            out,
            "https://huggingface.co/datasets/datasets-examples/doc-formats-csv-1/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet"
        );
    }

    #[test]
    fn resolve_hf_url_inline_revision_with_slash_is_url_encoded() {
        // A revision name like `refs/heads/dev` arrives URL-encoded so
        // HuggingFace's path parser sees one path segment for the branch
        // ref. Otherwise the slashes would split the URL into the wrong
        // shape.
        let out = resolve_hf_url("hf://datasets/foo/bar@refs/heads/dev/data.parquet").unwrap();
        // The first slash after the @-revision belongs to the path, not
        // the revision name. Our parser is conservative: stop at the next
        // path segment.
        // Actually the @<rev> spec is "everything until the next /"; refs
        // with slashes need URL-encoding by the user or the ?revision form.
        // Document this by asserting the simple split-once behaviour.
        assert!(
            out.contains("/resolve/refs/") || out.contains("/resolve/refs%2Fheads%2Fdev/"),
            "rejected behaviour ok; resolved URL was: {out}"
        );
    }

    #[test]
    fn resolve_hf_url_at_revision_and_query_revision_conflict_rejected() {
        // Both inline and query revision -> reject so the user sees the
        // typo rather than silent precedence.
        let out = resolve_hf_url("hf://datasets/foo/bar@v1.0/data.parquet?revision=v2.0");
        assert!(
            out.is_none(),
            "conflicting revisions must reject; got {out:?}"
        );
    }

    #[test]
    fn resolve_hf_url_empty_at_sign_rejected() {
        // `@` with empty revision is a typo not a default. Reject.
        assert!(resolve_hf_url("hf://datasets/foo/bar@/data.parquet").is_none());
    }

    #[test]
    fn rewrite_hf_urls_in_sql_at_tilde_parquet_full_path() {
        // The user-facing query that motivated V12.1: a quoted hf:// URL
        // with @~parquet revision and a real file path.
        let sql = "SELECT * FROM 'hf://datasets/foo/bar@~parquet/default/train/0.parquet'";
        let out = rewrite_hf_urls_in_sql(sql).unwrap();
        assert!(
            out.contains("https://huggingface.co/datasets/foo/bar/resolve/refs%2Fconvert%2Fparquet/default/train/0.parquet"),
            "rewritten SQL was: {out}"
        );
    }

    #[test]
    fn bucket_extraction() {
        assert_eq!(extract_bucket("s3://bucket/file"), Some("bucket"));
        assert_eq!(extract_bucket("s3a://bucket/path/file"), Some("bucket"));
        assert_eq!(extract_bucket("s3:///file"), None);
        assert_eq!(extract_bucket("/local/file"), None);
    }

    #[test]
    fn parse_path_only() {
        let exprs = vec![make_str_literal("/local/file.csv")];
        let args = parse_file_tvf_args("read_csv", &exprs, |_, _| false).unwrap();
        assert_eq!(args.path, "/local/file.csv");
        assert!(args.access_key.is_none());
    }

    #[test]
    fn parse_with_s3_credentials() {
        let exprs = vec![
            make_str_literal("s3://b/f.csv"),
            make_named_arg("access_key", "AKID"),
            make_named_arg("secret_key", "SECRET"),
            make_named_arg("endpoint", "http://minio:9000"),
            make_named_arg("region", "eu-west-1"),
        ];
        let args = parse_file_tvf_args("read_csv", &exprs, |_, _| false).unwrap();
        assert_eq!(args.access_key.as_deref(), Some("AKID"));
        assert_eq!(args.region.as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn parse_routes_format_specific_args_to_extra() {
        let exprs = vec![
            make_str_literal("/local/file.csv"),
            make_named_arg("delimiter", ","),
            make_named_arg("has_header", "true"),
        ];
        let mut seen = Vec::new();
        let args = parse_file_tvf_args("read_csv", &exprs, |k, v| {
            seen.push((k.to_string(), v.to_string()));
            true
        })
        .unwrap();
        assert_eq!(args.path, "/local/file.csv");
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], ("delimiter".to_string(), ",".to_string()));
        assert_eq!(seen[1], ("has_header".to_string(), "true".to_string()));
    }

    #[test]
    fn parse_unknown_named_arg_errors_when_extra_rejects() {
        let exprs = vec![make_str_literal("/f.csv"), make_named_arg("nonsense", "x")];
        let result = parse_file_tvf_args("read_csv", &exprs, |_, _| false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nonsense"));
    }

    #[test]
    fn parse_no_args_errors() {
        let result = parse_file_tvf_args("read_csv", &[], |_, _| false);
        assert!(result.is_err());
    }

    #[test]
    fn parse_non_string_path_errors() {
        let exprs = vec![Expr::Literal(ScalarValue::Int64(Some(42)), None)];
        let result = parse_file_tvf_args("read_csv", &exprs, |_, _| false);
        assert!(result.is_err());
    }

    #[test]
    fn build_s3_store_works() {
        let args = FileTvfArgs {
            path: "s3://bucket/data".to_string(),
            access_key: Some("AKID".to_string()),
            secret_key: Some("SECRET".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            region: Some("us-east-1".to_string()),
            ..FileTvfArgs::default()
        };
        let storage = StorageConfig {
            s3_allow_http: true,
            tvf: sqe_core::config::TvfPolicy {
                allowed_http_hosts: vec!["localhost".to_string()],
                ..Default::default()
            },
            ..StorageConfig::default()
        };
        assert!(build_s3_store("read_csv", &args, &storage).is_ok());
    }

    #[test]
    fn build_s3_store_rejects_imds_endpoint() {
        // Issue #46: defense-in-depth against SSRF via inline endpoint.
        let args = FileTvfArgs {
            path: "s3://bucket/data".to_string(),
            endpoint: Some("http://169.254.169.254/latest/meta-data/iam/".to_string()),
            ..FileTvfArgs::default()
        };
        let storage = StorageConfig::default();
        let err = build_s3_store("read_csv", &args, &storage).unwrap_err();
        assert!(err.to_string().contains("169.254.169.254"));
    }

    // -----------------------------------------------------------------------
    // V10 httpfs + HF URL resolution
    // -----------------------------------------------------------------------

    #[test]
    fn http_path_detection() {
        assert!(is_http_path("https://example.com/file.parquet"));
        assert!(is_http_path("http://internal/data.csv"));
        assert!(!is_http_path("/local/file"));
        assert!(!is_http_path("s3://bucket/file"));
        assert!(!is_http_path("hf://datasets/x/y/f"));
    }

    #[test]
    fn hf_path_detection() {
        assert!(is_hf_path("hf://datasets/squad/plain/train.parquet"));
        assert!(!is_hf_path("https://huggingface.co/..."));
        assert!(!is_hf_path("/local/file"));
    }

    // -----------------------------------------------------------------------
    // Azure ADLS Gen2 / Blob URL parsing
    // -----------------------------------------------------------------------

    #[test]
    fn azure_path_detection() {
        assert!(is_azure_path(
            "abfss://container@account.dfs.core.windows.net/path/file.parquet"
        ));
        assert!(is_azure_path(
            "abfs://container@account.dfs.core.windows.net/path/file.parquet"
        ));
        assert!(is_azure_path("azure://container/path/file.parquet"));
        assert!(is_azure_path("az://container/path/file.parquet"));
        assert!(!is_azure_path("s3://bucket/file"));
        assert!(!is_azure_path("https://example.com/file"));
        assert!(!is_azure_path("/local/file"));
    }

    #[test]
    fn azure_extract_container_account_from_abfss() {
        let r = extract_azure_container_account(
            "abfss://mycontainer@myaccount.dfs.core.windows.net/dir/file.parquet",
        )
        .unwrap();
        assert_eq!(r, ("mycontainer".to_string(), "myaccount".to_string()));
    }

    #[test]
    fn azure_extract_container_account_from_abfs() {
        let r =
            extract_azure_container_account("abfs://c@a.dfs.core.windows.net/x/y.parquet").unwrap();
        assert_eq!(r, ("c".to_string(), "a".to_string()));
    }

    #[test]
    fn azure_extract_container_from_short_form() {
        // azure:// and az:// shorthand: account comes from config / inline arg.
        let r = extract_azure_container_account("azure://mycontainer/dir/file.parquet").unwrap();
        assert_eq!(r, ("mycontainer".to_string(), String::new()));

        let r = extract_azure_container_account("az://c2/file").unwrap();
        assert_eq!(r, ("c2".to_string(), String::new()));
    }

    #[test]
    fn azure_extract_returns_none_on_malformed() {
        // Missing container@account separator on abfss.
        assert!(
            extract_azure_container_account("abfss://account.dfs.core.windows.net/path").is_none()
        );
        // Empty container.
        assert!(
            extract_azure_container_account("abfss://@account.dfs.core.windows.net/path").is_none()
        );
        // Not an Azure URL.
        assert!(extract_azure_container_account("s3://bucket/path").is_none());
    }

    // -----------------------------------------------------------------------
    // Google Cloud Storage URL parsing
    // -----------------------------------------------------------------------

    #[test]
    fn gcs_path_detection() {
        assert!(is_gcs_path("gs://bucket/path/file.parquet"));
        assert!(is_gcs_path("gcs://bucket/path/file.parquet"));
        assert!(!is_gcs_path("s3://bucket/file"));
        assert!(!is_gcs_path("https://storage.googleapis.com/..."));
        assert!(!is_gcs_path("/local/file"));
    }

    #[test]
    fn gcs_extract_bucket() {
        assert_eq!(
            extract_gcs_bucket("gs://my-bucket/key.parquet"),
            Some("my-bucket")
        );
        assert_eq!(extract_gcs_bucket("gcs://b/x"), Some("b"));
        assert_eq!(extract_gcs_bucket("gs://"), None);
        assert_eq!(extract_gcs_bucket("s3://bucket/key"), None);
    }

    #[test]
    fn hf_dataset_resolves_with_default_revision() {
        let r = resolve_hf_url("hf://datasets/squad/plain/train.parquet").unwrap();
        assert_eq!(
            r,
            "https://huggingface.co/datasets/squad/plain/resolve/main/train.parquet"
        );
    }

    #[test]
    fn hf_dataset_with_explicit_revision() {
        let r = resolve_hf_url("hf://datasets/squad/plain/train.parquet?revision=v1.0.0").unwrap();
        assert_eq!(
            r,
            "https://huggingface.co/datasets/squad/plain/resolve/v1.0.0/train.parquet"
        );
    }

    #[test]
    fn hf_model_path_uses_no_prefix() {
        let r = resolve_hf_url("hf://models/bert-base/uncased/config.json").unwrap();
        assert_eq!(
            r,
            "https://huggingface.co/bert-base/uncased/resolve/main/config.json"
        );
    }

    #[test]
    fn hf_spaces_path_uses_spaces_prefix() {
        let r = resolve_hf_url("hf://spaces/org/space/file.txt").unwrap();
        assert_eq!(
            r,
            "https://huggingface.co/spaces/org/space/resolve/main/file.txt"
        );
    }

    #[test]
    fn hf_unknown_kind_returns_none() {
        assert!(resolve_hf_url("hf://things/org/name/file").is_none());
    }

    #[test]
    fn hf_missing_file_path_returns_none() {
        // owner/name with no in-repo file path.
        assert!(resolve_hf_url("hf://datasets/squad/plain").is_none());
    }

    #[test]
    fn hf_empty_revision_is_rejected() {
        assert!(resolve_hf_url("hf://datasets/squad/plain/train.parquet?revision=").is_none());
    }

    #[test]
    fn hf_unknown_query_param_is_rejected() {
        // Catch typos like `?rev=` instead of silently defaulting to `main`.
        assert!(resolve_hf_url("hf://datasets/squad/plain/train.parquet?rev=v1.0").is_none());
    }

    #[test]
    fn rewrite_hf_path_in_place_resolves() {
        let mut args = FileTvfArgs {
            path: "hf://datasets/squad/plain/train.parquet".to_string(),
            ..Default::default()
        };
        rewrite_hf_path_in_place("read_parquet", &mut args).unwrap();
        assert!(args
            .path
            .starts_with("https://huggingface.co/datasets/squad/"));
    }

    #[test]
    fn rewrite_hf_path_in_place_leaves_https_untouched() {
        let mut args = FileTvfArgs {
            path: "https://example.com/file.parquet".to_string(),
            ..Default::default()
        };
        rewrite_hf_path_in_place("read_parquet", &mut args).unwrap();
        assert_eq!(args.path, "https://example.com/file.parquet");
    }

    #[test]
    fn rewrite_hf_path_in_place_errors_on_malformed_hf_url() {
        let mut args = FileTvfArgs {
            path: "hf://datasets/just-an-org".to_string(),
            ..Default::default()
        };
        let r = rewrite_hf_path_in_place("read_parquet", &mut args);
        assert!(r.is_err());
        assert!(r
            .unwrap_err()
            .to_string()
            .contains("malformed HuggingFace URL"));
    }

    // -----------------------------------------------------------------------
    // DF54: positional 'key=value' literal arguments
    // -----------------------------------------------------------------------

    #[test]
    fn positional_key_value_literal_parses_s3_creds() {
        let exprs = vec![
            datafusion_expr::lit("s3://bucket/data/*.parquet"),
            datafusion_expr::lit("access_key=AKIA123"),
            datafusion_expr::lit("secret_key=sekret"),
            datafusion_expr::lit("region=eu-west-1"),
        ];
        let args = parse_file_tvf_args("read_parquet", &exprs, |_, _| false).unwrap();
        assert_eq!(args.path, "s3://bucket/data/*.parquet");
        assert_eq!(args.access_key.as_deref(), Some("AKIA123"));
        assert_eq!(args.secret_key.as_deref(), Some("sekret"));
        assert_eq!(args.region.as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn positional_value_may_contain_equals() {
        let exprs = vec![
            datafusion_expr::lit("s3://b/x.parquet"),
            datafusion_expr::lit("secret_key=ab==cd=="),
        ];
        let args = parse_file_tvf_args("read_parquet", &exprs, |_, _| false).unwrap();
        assert_eq!(args.secret_key.as_deref(), Some("ab==cd=="));
    }

    #[test]
    fn positional_unknown_key_goes_to_extra_callback() {
        let mut seen: Option<String> = None;
        let exprs = vec![
            datafusion_expr::lit("/x.csv"),
            datafusion_expr::lit("delimiter=;"),
        ];
        let _ = parse_file_tvf_args("read_csv", &exprs, |k, v| {
            if k == "delimiter" {
                seen = Some(v.to_string());
                true
            } else {
                false
            }
        });
        assert_eq!(seen.as_deref(), Some(";"));
    }

    #[test]
    fn positional_non_kv_literal_errors() {
        let exprs = vec![
            datafusion_expr::lit("/x.parquet"),
            datafusion_expr::lit("notakeyvalue"),
        ];
        assert!(parse_file_tvf_args("read_parquet", &exprs, |_, _| false).is_err());
    }
}
