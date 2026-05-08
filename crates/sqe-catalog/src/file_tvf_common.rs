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
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionContext;
use datafusion_expr::Expr;
use object_store::aws::AmazonS3Builder;
use object_store::http::HttpBuilder;

use sqe_core::config::StorageConfig;

/// Path + S3 credential bag shared by every `read_*()` TVF.
#[derive(Debug, Clone, Default)]
pub struct FileTvfArgs {
    pub path: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
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

/// Resolve an `hf://` URL to the corresponding HuggingFace Hub HTTPS
/// download URL. Returns `None` if the path does not match the
/// expected shape.
///
/// Format (matches the DuckDB `hf://` extension):
///
/// ```text
/// hf://datasets/<owner>/<name>/<path>[?revision=<rev>]
/// hf://models/<owner>/<name>/<path>[?revision=<rev>]
/// hf://spaces/<owner>/<name>/<path>[?revision=<rev>]
/// ```
///
/// Examples:
///
/// ```text
/// hf://datasets/squad/plain_text/train-00000-of-00001.parquet
///   -> https://huggingface.co/datasets/squad/plain_text/resolve/main/train-00000-of-00001.parquet
///
/// hf://datasets/squad/plain_text/train-00000-of-00001.parquet?revision=v1.0.0
///   -> https://huggingface.co/datasets/squad/plain_text/resolve/v1.0.0/train-00000-of-00001.parquet
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
    let (path_only, revision) = match after_scheme.split_once('?') {
        Some((p, q)) => match q.strip_prefix("revision=") {
            Some(r) if !r.is_empty() => (p, r.to_string()),
            _ => return None,
        },
        None => (after_scheme, "main".to_string()),
    };

    let mut parts = path_only.splitn(2, '/');
    let kind = parts.next()?;
    if !matches!(kind, "datasets" | "models" | "spaces") {
        return None;
    }
    let rest = parts.next()?;

    // Repo id is `<owner>/<name>`; the rest is the in-repo file path.
    // Both segments must be non-empty, and there must be a file path.
    let mut rest_parts = rest.splitn(3, '/');
    let owner = rest_parts.next()?;
    let name = rest_parts.next()?;
    let file_path = rest_parts.next()?;

    if owner.is_empty() || name.is_empty() || file_path.is_empty() {
        return None;
    }

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
pub fn parse_file_tvf_args<F>(
    fn_name: &str,
    exprs: &[Expr],
    mut extra: F,
) -> DFResult<FileTvfArgs>
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
                match name {
                    "access_key" => out.access_key = Some(value),
                    "secret_key" => out.secret_key = Some(value),
                    "endpoint" => out.endpoint = Some(value),
                    "region" => out.region = Some(value),
                    other => {
                        if !extra(other, &value) {
                            return plan_err!(
                                "{fn_name}: unknown named argument '{other}'"
                            );
                        }
                    }
                }
            }
            other => {
                return plan_err!(
                    "{fn_name}: unexpected argument expression {other}; \
                     named arguments must use the form 'key => value'"
                );
            }
        }
    }

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
        .unwrap_or(storage.s3_secret_key.as_str());

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
    if storage.s3_allow_http {
        builder = builder.with_allow_http(true);
    }
    builder
        .build()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
}

/// If `args.path` is an S3 URL, register an [`AmazonS3`] object store on the
/// supplied [`SessionContext`] under the bucket-scoped URL so subsequent
/// listing / read operations resolve it. No-op for local paths.
pub fn register_s3_store_if_needed(
    fn_name: &str,
    ctx: &SessionContext,
    args: &FileTvfArgs,
    storage: &StorageConfig,
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
    ctx.register_object_store(&store_url, Arc::new(s3));
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
        let exprs = vec![
            make_str_literal("/f.csv"),
            make_named_arg("nonsense", "x"),
        ];
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
        };
        let storage = StorageConfig {
            s3_allow_http: true,
            ..StorageConfig::default()
        };
        assert!(build_s3_store("read_csv", &args, &storage).is_ok());
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
        let r = resolve_hf_url(
            "hf://datasets/squad/plain/train.parquet?revision=v1.0.0",
        )
        .unwrap();
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
        assert!(
            resolve_hf_url("hf://datasets/squad/plain/train.parquet?revision=").is_none()
        );
    }

    #[test]
    fn hf_unknown_query_param_is_rejected() {
        // Catch typos like `?rev=` instead of silently defaulting to `main`.
        assert!(
            resolve_hf_url("hf://datasets/squad/plain/train.parquet?rev=v1.0").is_none()
        );
    }

    #[test]
    fn rewrite_hf_path_in_place_resolves() {
        let mut args = FileTvfArgs {
            path: "hf://datasets/squad/plain/train.parquet".to_string(),
            ..Default::default()
        };
        rewrite_hf_path_in_place("read_parquet", &mut args).unwrap();
        assert!(args.path.starts_with("https://huggingface.co/datasets/squad/"));
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
        assert!(r.unwrap_err().to_string().contains("malformed HuggingFace URL"));
    }
}
