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
//! ## Named arguments (DuckDB-parity)
//!
//! - `delimiter` (alias `delim`, `sep`): single ASCII byte. Default
//!   auto-detected from extension (`.tsv` -> tab, `.psv` -> pipe, `.ssv` -> semicolon, `.csv` -> comma).
//! - `has_header` (alias `header`): boolean. Default `true`.
//! - `quote`: single ASCII byte. DataFusion default `"`.
//! - `escape`: single ASCII byte.
//! - `comment`: single ASCII byte. Lines starting with this are skipped.
//! - `null_regex` (alias `nullstr`): regex for null values.
//! - `compression` (alias `compress`): `auto` (default), `gzip`, `bz2`, `xz`,
//!   `zstd`, or `none`. `auto` reads the file extension chain (`.csv.gz`,
//!   `.tsv.zst`, etc.).
//! - `file_extension`: glob extension for directory listings; default
//!   matches the path's outer extension.
//!
//! ## Implementation
//!
//! The TVF wraps DataFusion 53's [`CsvFormat`] in a [`ListingTable`], using
//! the runtime-flavor-aware `block_on_compat` bridge from
//! `crate::runtime_bridge` (issue #83) so the call works on both multi-thread
//! and current-thread tokio runtimes.
//! Hf:// URLs are resolved to HTTPS upstream; S3 stores get registered on
//! demand from inline credentials or [`StorageConfig`] defaults.

use std::sync::Arc;

use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionContext;
use datafusion_expr::Expr;
use tracing::debug;

use sqe_core::config::{StorageConfig, TvfCaller};

use crate::file_tvf_common::{
    parse_file_tvf_args, register_azure_store_if_needed, register_gcs_store_if_needed,
    register_http_store_if_needed, register_s3_store_if_needed, rewrite_hf_path_in_place,
    FileTvfArgs,
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
    /// `None` means use auto-detect from the path; `Some` overrides it.
    compression: Option<FileCompressionType>,
}

/// Detect a delimiter from a path's outer extension. Matches DuckDB's
/// `read_csv` heuristics.
fn delimiter_for_extension(path: &str) -> Option<u8> {
    // Strip a trailing compression extension first (.gz, .bz2, .xz, .zst).
    let stripped = strip_compression_ext(path);
    let lower = stripped.to_ascii_lowercase();
    if lower.ends_with(".tsv") {
        Some(b'\t')
    } else if lower.ends_with(".psv") {
        Some(b'|')
    } else if lower.ends_with(".ssv") {
        Some(b';')
    } else if lower.ends_with(".csv") {
        Some(b',')
    } else {
        None
    }
}

/// Strip a compression suffix from a path, returning the inner path.
/// `data.csv.gz` -> `data.csv`. Used so `delimiter_for_extension` and the
/// `file_extension` listing knob look at the format extension, not the
/// codec extension.
fn strip_compression_ext(path: &str) -> &str {
    for ext in [".gz", ".gzip", ".bz2", ".bzip2", ".xz", ".zst", ".zstd"] {
        if path.to_ascii_lowercase().ends_with(ext) {
            return &path[..path.len() - ext.len()];
        }
    }
    path
}

/// Parse a `compression => '<value>'` named arg. Accepts `auto`, `gzip`,
/// `bz2`, `xz`, `zstd`, `none`, plus DuckDB-friendly aliases.
fn parse_compression(value: &str) -> DFResult<Option<FileCompressionType>> {
    match value.to_ascii_lowercase().as_str() {
        "auto" | "" => Ok(None),
        "none" | "uncompressed" | "off" => Ok(Some(FileCompressionType::UNCOMPRESSED)),
        "gz" | "gzip" => Ok(Some(FileCompressionType::GZIP)),
        "bz2" | "bzip2" => Ok(Some(FileCompressionType::BZIP2)),
        "xz" => Ok(Some(FileCompressionType::XZ)),
        "zst" | "zstd" => Ok(Some(FileCompressionType::ZSTD)),
        other => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: 'compression' must be one of auto, none, gzip, bz2, xz, zstd; got '{other}'"
        ))),
    }
}

/// Map a path's compression extension to a [`FileCompressionType`].
/// Returns `Uncompressed` if the path has no recognised codec suffix.
fn compression_from_extension(path: &str) -> FileCompressionType {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".gz") || lower.ends_with(".gzip") {
        FileCompressionType::GZIP
    } else if lower.ends_with(".bz2") || lower.ends_with(".bzip2") {
        FileCompressionType::BZIP2
    } else if lower.ends_with(".xz") {
        FileCompressionType::XZ
    } else if lower.ends_with(".zst") || lower.ends_with(".zstd") {
        FileCompressionType::ZSTD
    } else {
        FileCompressionType::UNCOMPRESSED
    }
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
    /// Authenticated caller identity for the object-store prefix gate.
    /// `TvfCaller::default()` (anonymous, untrusted) fails closed.
    caller: TvfCaller,
    /// The executing session's runtime environment; see
    /// [`crate::read_parquet::ReadParquetFunction::with_runtime_env`].
    runtime_env: Option<Arc<datafusion::execution::runtime_env::RuntimeEnv>>,
}

impl ReadCsvFunction {
    pub fn new(storage: StorageConfig) -> Self {
        Self {
            storage,
            caller: TvfCaller::default(),
            runtime_env: None,
        }
    }

    /// Create a new `ReadCsvFunction` bound to an authenticated caller.
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

impl TableFunctionImpl for ReadCsvFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let mut csv_opts = CsvOpts::default();
        let mut parse_err: Option<DataFusionError> = None;

        let args = parse_file_tvf_args(FN_NAME, exprs, |key, value| {
            let res: DFResult<()> = match key {
                // DuckDB uses `delim` and `sep`; CSV traditionalists use
                // `delimiter`. Accept all three.
                "delimiter" | "delim" | "sep" => parse_single_byte(key, value).map(|b| {
                    csv_opts.delimiter = Some(b);
                }),
                // `header` is the DuckDB spelling; `has_header` is what
                // DataFusion uses. Either works.
                "has_header" | "header" => parse_bool(key, value).map(|b| {
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
                // `nullstr` is DuckDB; `null_regex` is the underlying knob.
                "null_regex" | "nullstr" => {
                    csv_opts.null_regex = Some(value.to_string());
                    Ok(())
                }
                "file_extension" => {
                    csv_opts.file_extension = Some(value.to_string());
                    Ok(())
                }
                "compression" | "compress" => parse_compression(value).map(|c| {
                    csv_opts.compression = c;
                }),
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

        // Issue #10: TVF path / host policy check before object-store
        // construction. Object-store paths are additionally gated per
        // caller identity (E2E-identity item 1).
        crate::file_tvf_common::enforce_tvf_path_policy(
            FN_NAME,
            &args,
            &self.storage,
            &self.caller,
        )?;

        let storage = self.storage.clone();
        let runtime_env = self.runtime_env.clone();
        crate::runtime_bridge::block_on_compat(async move {
            build_csv_listing_table(&args, &csv_opts, &storage, runtime_env.as_deref()).await
        })
        .ok_or_else(|| DataFusionError::Plan(format!("{FN_NAME}: no tokio runtime available")))?
    }
}

/// Pull the file extension from a path, including the compression suffix
/// if present. `'/data/x.csv.gz'` -> `'.csv.gz'`. Falls back to `'.csv'`
/// when the path is a glob or has no extension.
fn derive_file_extension(path: &str) -> String {
    // Drop directory parts.
    let basename = path.rsplit('/').next().unwrap_or(path);
    // If the basename contains glob characters we use the conservative
    // default; ListingTable's globbing will pick up files by that suffix.
    if basename.contains('*') || basename.contains('?') {
        return ".csv".to_string();
    }
    // Find the LAST dot for the codec suffix, then check if the byte
    // before that suffix is also a recognized format extension.
    let lower = basename.to_ascii_lowercase();
    let codecs = [".gz", ".gzip", ".bz2", ".bzip2", ".xz", ".zst", ".zstd"];
    for c in codecs {
        if let Some(stripped) = lower.strip_suffix(c) {
            if let Some(dot) = stripped.rfind('.') {
                return format!("{}{c}", &stripped[dot..]);
            }
        }
    }
    if let Some(dot) = basename.rfind('.') {
        return basename[dot..].to_string();
    }
    ".csv".to_string()
}

async fn build_csv_listing_table(
    args: &FileTvfArgs,
    csv_opts: &CsvOpts,
    storage: &StorageConfig,
    runtime_env: Option<&datafusion::execution::runtime_env::RuntimeEnv>,
) -> DFResult<Arc<dyn TableProvider>> {
    // Resolve hf:// to its HTTPS form, then drive HTTPS through the
    // shared httpfs builder. S3 paths still flow through the S3 helper.
    let mut args = args.clone();
    rewrite_hf_path_in_place(FN_NAME, &mut args)?;

    let listing_url = ListingTableUrl::parse(&args.path)?;

    let tmp_ctx = SessionContext::new();
    register_s3_store_if_needed(FN_NAME, &tmp_ctx, &args, storage, runtime_env)?;
    register_azure_store_if_needed(FN_NAME, &tmp_ctx, &args, storage)?;
    register_gcs_store_if_needed(FN_NAME, &tmp_ctx, &args, storage)?;
    register_http_store_if_needed(FN_NAME, &tmp_ctx, &args.path)?;

    let mut format = CsvFormat::default();

    // Delimiter: explicit > extension-based detection > DataFusion default.
    let delimiter = csv_opts
        .delimiter
        .or_else(|| delimiter_for_extension(&args.path));
    if let Some(d) = delimiter {
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

    // Compression: explicit > extension-based detection > UNCOMPRESSED.
    let compression = csv_opts
        .compression
        .unwrap_or_else(|| compression_from_extension(&args.path));
    format = format.with_file_compression_type(compression);

    // file_extension: explicit > derived from the path (skipping the
    // compression suffix). If a user passes 'data.tsv.gz' we still want
    // the listing to match '.tsv.gz' for directory globs.
    let derived_extension = derive_file_extension(&args.path);
    let extension = csv_opts
        .file_extension
        .as_deref()
        .unwrap_or(derived_extension.as_str());
    let listing_options = ListingOptions::new(Arc::new(format)).with_file_extension(extension);

    let state = tmp_ctx.state();
    crate::file_tvf_common::ensure_local_files_exist(
        FN_NAME,
        &state,
        &listing_url,
        extension,
        &args.path,
    )
    .await?;
    let schema = listing_options.infer_schema(&state, &listing_url).await?;

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
        assert!(f.storage.s3_endpoint.is_empty());
        assert!(!f.storage.s3_path_style);
        assert!(!f.storage.s3_allow_http);
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

    #[test]
    fn delimiter_for_extension_picks_per_format() {
        assert_eq!(delimiter_for_extension("data.csv"), Some(b','));
        assert_eq!(delimiter_for_extension("data.tsv"), Some(b'\t'));
        assert_eq!(delimiter_for_extension("data.psv"), Some(b'|'));
        assert_eq!(delimiter_for_extension("data.ssv"), Some(b';'));
        assert_eq!(delimiter_for_extension("data.txt"), None);
    }

    #[test]
    fn delimiter_for_extension_strips_compression() {
        // .csv.gz -> still ',' (TSV-and-gz means tab)
        assert_eq!(delimiter_for_extension("data.csv.gz"), Some(b','));
        assert_eq!(delimiter_for_extension("data.tsv.bz2"), Some(b'\t'));
        assert_eq!(delimiter_for_extension("data.tsv.zst"), Some(b'\t'));
        assert_eq!(delimiter_for_extension("data.psv.xz"), Some(b'|'));
    }

    #[test]
    fn compression_from_extension_recognises_codecs() {
        assert!(matches!(
            compression_from_extension("data.csv.gz"),
            FileCompressionType::GZIP
        ));
        assert!(matches!(
            compression_from_extension("data.csv.bz2"),
            FileCompressionType::BZIP2
        ));
        assert!(matches!(
            compression_from_extension("data.csv.xz"),
            FileCompressionType::XZ
        ));
        assert!(matches!(
            compression_from_extension("data.csv.zst"),
            FileCompressionType::ZSTD
        ));
        assert!(matches!(
            compression_from_extension("data.csv"),
            FileCompressionType::UNCOMPRESSED
        ));
    }

    #[test]
    fn parse_compression_accepts_aliases() {
        assert!(parse_compression("auto").unwrap().is_none());
        assert!(matches!(
            parse_compression("gzip").unwrap(),
            Some(FileCompressionType::GZIP)
        ));
        assert!(matches!(
            parse_compression("gz").unwrap(),
            Some(FileCompressionType::GZIP)
        ));
        assert!(matches!(
            parse_compression("zstd").unwrap(),
            Some(FileCompressionType::ZSTD)
        ));
        assert!(matches!(
            parse_compression("zst").unwrap(),
            Some(FileCompressionType::ZSTD)
        ));
        assert!(matches!(
            parse_compression("none").unwrap(),
            Some(FileCompressionType::UNCOMPRESSED)
        ));
    }

    #[test]
    fn parse_compression_rejects_garbage() {
        assert!(parse_compression("rar").is_err());
    }

    #[test]
    fn derive_file_extension_handles_compression() {
        assert_eq!(derive_file_extension("/data/x.csv"), ".csv");
        assert_eq!(derive_file_extension("/data/x.csv.gz"), ".csv.gz");
        assert_eq!(derive_file_extension("/data/x.tsv.zst"), ".tsv.zst");
        assert_eq!(derive_file_extension("/data/x.psv.xz"), ".psv.xz");
        assert_eq!(derive_file_extension("/data/glob/*.csv"), ".csv");
        assert_eq!(derive_file_extension("/data/no_extension"), ".csv");
    }
}
