//! `read_json(path, [named_args...])` table-valued function.
//!
//! Mirrors `read_csv` for newline-delimited JSON. DataFusion's
//! [`JsonFormat`] handles both NDJSON (one JSON object per line) and
//! line-by-line concatenated JSON; the `newline_delimited` named arg
//! controls which.
//!
//! ```sql
//! SELECT * FROM read_json('/data/events.jsonl');
//!
//! SELECT * FROM read_json('s3://bucket/events.json',
//!     access_key         => 'AKIA...',
//!     secret_key         => '...',
//!     newline_delimited  => 'true');
//! ```
//!
//! The JSON-specific named args are:
//!
//! - `newline_delimited`: bool. NDJSON mode. Default: true (DataFusion's
//!   built-in default; explicit pass-through here in case it ever flips).
//! - `file_extension`: override the listing file extension. Default `.json`.
use std::sync::Arc;

use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionContext;
use datafusion_expr::Expr;
use tracing::debug;

use sqe_core::config::StorageConfig;

use crate::file_tvf_common::{
    parse_file_tvf_args, register_azure_store_if_needed, register_gcs_store_if_needed,
    register_http_store_if_needed, register_s3_store_if_needed, rewrite_hf_path_in_place,
    FileTvfArgs,
};

const FN_NAME: &str = "read_json";

#[derive(Debug, Default)]
struct JsonOpts {
    newline_delimited: Option<bool>,
    file_extension: Option<String>,
}

fn parse_bool(key: &str, value: &str) -> DFResult<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: '{key}' must be a boolean (true/false), got '{value}'"
        ))),
    }
}

#[derive(Debug)]
pub struct ReadJsonFunction {
    storage: StorageConfig,
}

impl ReadJsonFunction {
    pub fn new(storage: StorageConfig) -> Self {
        Self { storage }
    }
}

impl TableFunctionImpl for ReadJsonFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let mut json_opts = JsonOpts::default();
        let mut parse_err: Option<DataFusionError> = None;

        let args = parse_file_tvf_args(FN_NAME, exprs, |key, value| {
            match key {
                "newline_delimited" => match parse_bool("newline_delimited", value) {
                    Ok(b) => json_opts.newline_delimited = Some(b),
                    Err(e) => parse_err = Some(e),
                },
                "file_extension" => json_opts.file_extension = Some(value.to_string()),
                _ => return false,
            }
            true
        })?;

        if let Some(e) = parse_err {
            return Err(e);
        }

        // Issue #10: TVF path / host policy check.
        self.storage.tvf.check(&args.path).map_err(|e| {
            DataFusionError::Plan(format!("{FN_NAME}: {e}"))
        })?;

        let storage = self.storage.clone();
        crate::runtime_bridge::block_on_compat(async move {
            build_json_listing_table(&args, &json_opts, &storage).await
        })
        .ok_or_else(|| {
            DataFusionError::Plan(format!("{FN_NAME}: no tokio runtime available"))
        })?
    }
}

async fn build_json_listing_table(
    args: &FileTvfArgs,
    json_opts: &JsonOpts,
    storage: &StorageConfig,
) -> DFResult<Arc<dyn TableProvider>> {
    let mut args = args.clone();
    rewrite_hf_path_in_place(FN_NAME, &mut args)?;

    let listing_url = ListingTableUrl::parse(&args.path)?;

    let tmp_ctx = SessionContext::new();
    register_s3_store_if_needed(FN_NAME, &tmp_ctx, &args, storage)?;
    register_azure_store_if_needed(FN_NAME, &tmp_ctx, &args, storage)?;
    register_gcs_store_if_needed(FN_NAME, &tmp_ctx, &args, storage)?;
    register_http_store_if_needed(FN_NAME, &tmp_ctx, &args.path)?;

    let mut format = JsonFormat::default();
    if let Some(nd) = json_opts.newline_delimited {
        format = format.with_newline_delimited(nd);
    }

    let extension = json_opts.file_extension.as_deref().unwrap_or(".json");
    let listing_options = ListingOptions::new(Arc::new(format)).with_file_extension(extension);

    let schema = listing_options
        .infer_schema(&tmp_ctx.state(), &listing_url)
        .await?;

    let config = ListingTableConfig::new(listing_url)
        .with_listing_options(listing_options)
        .with_schema(schema);

    let table = ListingTable::try_new(config)?;
    debug!(path = %args.path, "read_json: built ListingTable");
    Ok(Arc::new(table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_can_be_created() {
        let f = ReadJsonFunction::new(StorageConfig::default());
        assert!(f.storage.s3_endpoint.is_empty());
        assert!(!f.storage.s3_path_style);
        assert!(!f.storage.s3_allow_http);
    }

    #[test]
    fn parse_bool_accepts_common_forms() {
        assert!(parse_bool("newline_delimited", "true").unwrap());
        assert!(parse_bool("newline_delimited", "1").unwrap());
        assert!(!parse_bool("newline_delimited", "false").unwrap());
        assert!(parse_bool("newline_delimited", "garbage").is_err());
    }
}
