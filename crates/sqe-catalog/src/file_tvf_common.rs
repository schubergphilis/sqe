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
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionContext;
use datafusion_expr::Expr;
use object_store::aws::AmazonS3Builder;

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
}
