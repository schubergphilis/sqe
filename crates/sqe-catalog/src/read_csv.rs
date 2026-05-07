//! `read_csv(path, [named_args...])` table-valued function.
//!
//! Closes a DuckDB-parity gap. Today users have to register a CSV file as a
//! table via `CREATE EXTERNAL TABLE ... STORED AS CSV LOCATION ...`; with
//! `read_csv()` they can query it inline:
//!
//! ```sql
//! SELECT * FROM read_csv('/data/sales.csv');
//!
//! SELECT * FROM read_csv('s3://bucket/sales/*.csv',
//!     access_key  => 'AKIA...',
//!     secret_key  => '...',
//!     endpoint    => 'http://localhost:9000',
//!     region      => 'us-east-1',
//!     delimiter   => ',',
//!     has_header  => 'true');
//! ```
//!
//! The implementation wraps DataFusion 53's [`CsvFormat`] in a
//! [`ListingTable`], using the same `block_in_place`-driven async-from-sync
//! bridge as `read_parquet`. CSV-specific named args (`delimiter`,
//! `has_header`, `quote`, `escape`, `comment`, `null_regex`) flow into the
//! [`CsvFormat`] builder.
//!
//! Compression is not exposed yet; CSV reading from gzipped files works only
//! when DataFusion's `compression` feature is on (default).

use std::sync::Arc;

use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionContext;
use datafusion_expr::Expr;
use tracing::debug;

use sqe_core::config::StorageConfig;

use crate::file_tvf_common::{
    parse_file_tvf_args, register_s3_store_if_needed, FileTvfArgs,
};

const FN_NAME: &str = "read_csv";

/// CSV-specific options collected from named arguments.
#[derive(Debug, Default)]
struct CsvOpts {
    delimiter: Option<u8>,
    has_header: Option<bool>,
    quote: Option<u8>,
    escape: Option<u8>,
    comment: Option<u8>,
    null_regex: Option<String>,
    file_extension: Option<String>,
}

fn parse_single_byte(key: &str, value: &str) -> DFResult<u8> {
    let bytes = value.as_bytes();
    if bytes.len() != 1 {
        return Err(DataFusionError::Plan(format!(
            "{FN_NAME}: '{key}' must be a single ASCII character, got '{value}'"
        )));
    }
    Ok(bytes[0])
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
pub struct ReadCsvFunction {
    storage: StorageConfig,
}

impl ReadCsvFunction {
    pub fn new(storage: StorageConfig) -> Self {
        Self { storage }
    }
}

impl TableFunctionImpl for ReadCsvFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let mut csv_opts = CsvOpts::default();
        let mut parse_err: Option<DataFusionError> = None;

        let args = parse_file_tvf_args(FN_NAME, exprs, |key, value| {
            let res: DFResult<()> = match key {
                "delimiter" => parse_single_byte("delimiter", value).map(|b| {
                    csv_opts.delimiter = Some(b);
                }),
                "has_header" => parse_bool("has_header", value).map(|b| {
                    csv_opts.has_header = Some(b);
                }),
                "quote" => parse_single_byte("quote", value).map(|b| {
                    csv_opts.quote = Some(b);
                }),
                "escape" => parse_single_byte("escape", value).map(|b| {
                    csv_opts.escape = Some(b);
                }),
                "comment" => parse_single_byte("comment", value).map(|b| {
                    csv_opts.comment = Some(b);
                }),
                "null_regex" => {
                    csv_opts.null_regex = Some(value.to_string());
                    Ok(())
                }
                "file_extension" => {
                    csv_opts.file_extension = Some(value.to_string());
                    Ok(())
                }
                _ => return false,
            };
            if let Err(e) = res {
                parse_err = Some(e);
            }
            true
        })?;

        if let Some(e) = parse_err {
            return Err(e);
        }

        let storage = self.storage.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { build_csv_listing_table(&args, &csv_opts, &storage).await })
        })
    }
}

async fn build_csv_listing_table(
    args: &FileTvfArgs,
    csv_opts: &CsvOpts,
    storage: &StorageConfig,
) -> DFResult<Arc<dyn TableProvider>> {
    let listing_url = ListingTableUrl::parse(&args.path)?;

    let tmp_ctx = SessionContext::new();
    register_s3_store_if_needed(FN_NAME, &tmp_ctx, args, storage)?;

    let mut format = CsvFormat::default();
    if let Some(d) = csv_opts.delimiter {
        format = format.with_delimiter(d);
    }
    if let Some(h) = csv_opts.has_header {
        format = format.with_has_header(h);
    }
    if let Some(q) = csv_opts.quote {
        format = format.with_quote(q);
    }
    if let Some(e) = csv_opts.escape {
        format = format.with_escape(Some(e));
    }
    if let Some(c) = csv_opts.comment {
        format = format.with_comment(Some(c));
    }
    if let Some(re) = &csv_opts.null_regex {
        format = format.with_null_regex(Some(re.clone()));
    }

    let extension = csv_opts.file_extension.as_deref().unwrap_or(".csv");
    let listing_options = ListingOptions::new(Arc::new(format)).with_file_extension(extension);

    let schema = listing_options
        .infer_schema(&tmp_ctx.state(), &listing_url)
        .await?;

    let config = ListingTableConfig::new(listing_url)
        .with_listing_options(listing_options)
        .with_schema(schema);

    let table = ListingTable::try_new(config)?;
    debug!(path = %args.path, "read_csv: built ListingTable");
    Ok(Arc::new(table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_can_be_created() {
        let f = ReadCsvFunction::new(StorageConfig::default());
        assert!(matches!(f.storage, StorageConfig { .. }));
    }

    #[test]
    fn parse_single_byte_accepts_one_char() {
        assert_eq!(parse_single_byte("delimiter", ",").unwrap(), b',');
        assert_eq!(parse_single_byte("delimiter", "\t").unwrap(), b'\t');
    }

    #[test]
    fn parse_single_byte_rejects_multi_char() {
        assert!(parse_single_byte("delimiter", "->").is_err());
        assert!(parse_single_byte("delimiter", "").is_err());
    }

    #[test]
    fn parse_bool_accepts_common_forms() {
        assert!(parse_bool("has_header", "true").unwrap());
        assert!(parse_bool("has_header", "TRUE").unwrap());
        assert!(parse_bool("has_header", "1").unwrap());
        assert!(parse_bool("has_header", "yes").unwrap());
        assert!(!parse_bool("has_header", "false").unwrap());
        assert!(!parse_bool("has_header", "0").unwrap());
    }

    #[test]
    fn parse_bool_rejects_garbage() {
        assert!(parse_bool("has_header", "maybe").is_err());
    }
}
