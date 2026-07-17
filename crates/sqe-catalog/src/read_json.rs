//! `read_json(path, [named_args...])` table-valued function.
//!
//! Mirrors `read_csv` for JSON. Two named args select the shape and the
//! codec:
//!
//! - `format`: `auto` (default), `newline_delimited` (aliases `ndjson`,
//!   `nd`), or `array` (alias `json`). `auto` resolves to NDJSON; a
//!   top-level JSON array is only read as an array when `format =>
//!   'array'` is passed explicitly, or the source is a `.zip` archive
//!   (each entry auto-detects on its own). The legacy `newline_delimited`
//!   bool arg is kept as an alias: `newline_delimited => 'false'` means
//!   `format => 'array'`.
//! - `compression` (alias `compress`): `auto` (default, detected from the
//!   path extension), `none`, `gzip`, `zip`, `zstd`, `bz2`, `xz`.
//! - `file_extension`: override the listing file extension. Default is
//!   derived from the path, codec suffix included (e.g. `.jsonl.gz` for
//!   `events.jsonl.gz`), so the listing filter matches files as they sit
//!   on disk/object store.
//!
//! ```sql
//! -- Plain NDJSON, streamed line by line.
//! SELECT * FROM read_json('/data/events.jsonl');
//!
//! -- gzip NDJSON, still streamed (FileCompressionType handles the codec).
//! SELECT * FROM read_json('s3://bucket/events.jsonl.gz',
//!     access_key => 'AKIA...', secret_key => '...');
//!
//! -- Full JSON array: buffered, decompressed, and reshaped into NDJSON.
//! SELECT * FROM read_json('/data/events.json', format => 'array');
//!
//! -- Zip archive: every JSON/NDJSON entry is concatenated; also buffered.
//! SELECT * FROM read_json('/data/export.json.zip');
//! ```
//!
//! # Two execution paths
//!
//! The router in `ReadJsonFunction::call` picks one of two paths after
//! `enforce_tvf_path_policy` runs:
//!
//! - **Streaming path** (`build_json_listing_table`, this module): the
//!   default for NDJSON with no compression or a compression DataFusion's
//!   [`JsonFormat`] / [`FileCompressionType`] can stream (gzip, zstd,
//!   bzip2, xz). Handles arbitrarily large files without buffering the
//!   whole object.
//! - **Buffer path** ([`crate::read_json_buffer`]): used when `format =>
//!   'array'` is requested, or the compression is `zip`. Both cases fetch
//!   the whole object, decompress/unzip it, reshape it into NDJSON, and
//!   decode it into an in-memory `MemTable`. A size guard
//!   (`DEFAULT_MAX_BUFFER_BYTES`) rejects inputs above the cap instead of
//!   risking OOM on a mislabeled file.
use std::sync::Arc;

use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
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

const FN_NAME: &str = "read_json";

/// Default cap for the buffer path (zip / full-array). Mislabeled huge inputs
/// fail with a clear message instead of OOM. Overridable later via a named arg.
const DEFAULT_MAX_BUFFER_BYTES: usize = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonFraming {
    Auto,
    NewlineDelimited,
    Array,
}

/// Local compression enum, distinct from DataFusion's `FileCompressionType`
/// because it must also represent `Zip`, which `FileCompressionType` cannot.
/// The streaming path (Task 2) maps the non-zip variants onto
/// `FileCompressionType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonCompression {
    None,
    Gzip,
    Zip,
    Zstd,
    Bz2,
    Xz,
}

#[derive(Debug, Default)]
struct JsonOpts {
    framing: Option<JsonFraming>,
    compression: Option<JsonCompression>,
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

fn parse_framing(value: &str) -> DFResult<JsonFraming> {
    match value.to_ascii_lowercase().as_str() {
        "auto" | "" => Ok(JsonFraming::Auto),
        "newline_delimited" | "ndjson" | "nd" => Ok(JsonFraming::NewlineDelimited),
        "array" | "json" => Ok(JsonFraming::Array),
        other => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: 'format' must be one of auto, newline_delimited, array; got '{other}'"
        ))),
    }
}

fn parse_json_compression(value: &str) -> DFResult<Option<JsonCompression>> {
    match value.to_ascii_lowercase().as_str() {
        "auto" | "" => Ok(None),
        "none" | "uncompressed" | "off" => Ok(Some(JsonCompression::None)),
        "gz" | "gzip" => Ok(Some(JsonCompression::Gzip)),
        "zip" => Ok(Some(JsonCompression::Zip)),
        "zst" | "zstd" => Ok(Some(JsonCompression::Zstd)),
        "bz2" | "bzip2" => Ok(Some(JsonCompression::Bz2)),
        "xz" => Ok(Some(JsonCompression::Xz)),
        other => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: 'compression' must be one of auto, none, gzip, zip, zstd, bz2, xz; got '{other}'"
        ))),
    }
}

/// Map a path's trailing codec extension to a [`JsonCompression`].
fn compression_from_extension(path: &str) -> JsonCompression {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".gz") || lower.ends_with(".gzip") {
        JsonCompression::Gzip
    } else if lower.ends_with(".zip") {
        JsonCompression::Zip
    } else if lower.ends_with(".bz2") || lower.ends_with(".bzip2") {
        JsonCompression::Bz2
    } else if lower.ends_with(".xz") {
        JsonCompression::Xz
    } else if lower.ends_with(".zst") || lower.ends_with(".zstd") {
        JsonCompression::Zstd
    } else {
        JsonCompression::None
    }
}

/// Peek the first non-whitespace byte to decide framing. `[` => array.
pub(crate) fn detect_framing_from_bytes(bytes: &[u8]) -> JsonFraming {
    for &b in bytes {
        match b {
            b' ' | b'\t' | b'\r' | b'\n' => continue,
            b'[' => return JsonFraming::Array,
            _ => return JsonFraming::NewlineDelimited,
        }
    }
    JsonFraming::NewlineDelimited
}

/// Map the streaming-capable codecs onto DataFusion's `FileCompressionType`.
/// `Zip` never reaches here (it routes to the buffer path); treat it as a bug.
fn streaming_compression(c: JsonCompression) -> FileCompressionType {
    match c {
        JsonCompression::None => FileCompressionType::UNCOMPRESSED,
        JsonCompression::Gzip => FileCompressionType::GZIP,
        JsonCompression::Bz2 => FileCompressionType::BZIP2,
        JsonCompression::Xz => FileCompressionType::XZ,
        JsonCompression::Zstd => FileCompressionType::ZSTD,
        JsonCompression::Zip => FileCompressionType::UNCOMPRESSED, // unreachable via router
    }
}

#[derive(Debug)]
pub struct ReadJsonFunction {
    storage: StorageConfig,
    /// Authenticated caller identity for the object-store prefix gate.
    /// `TvfCaller::default()` (anonymous, untrusted) fails closed.
    caller: TvfCaller,
    /// The executing session's runtime environment; see
    /// [`crate::read_parquet::ReadParquetFunction::with_runtime_env`].
    runtime_env: Option<Arc<datafusion::execution::runtime_env::RuntimeEnv>>,
}

impl ReadJsonFunction {
    pub fn new(storage: StorageConfig) -> Self {
        Self {
            storage,
            caller: TvfCaller::default(),
            runtime_env: None,
        }
    }

    /// Create a new `ReadJsonFunction` bound to an authenticated caller.
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

impl TableFunctionImpl for ReadJsonFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let mut json_opts = JsonOpts::default();
        let mut parse_err: Option<DataFusionError> = None;

        let args = parse_file_tvf_args(FN_NAME, exprs, |key, value| {
            match key {
                "format" => match parse_framing(value) {
                    Ok(f) => json_opts.framing = Some(f),
                    Err(e) => parse_err = Some(e),
                },
                // Legacy alias: newline_delimited => format.
                "newline_delimited" => match parse_bool("newline_delimited", value) {
                    Ok(true) => json_opts.framing = Some(JsonFraming::NewlineDelimited),
                    Ok(false) => json_opts.framing = Some(JsonFraming::Array),
                    Err(e) => parse_err = Some(e),
                },
                "compression" | "compress" => match parse_json_compression(value) {
                    Ok(c) => json_opts.compression = c,
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

        // Issue #10: TVF path / host policy check. Object-store paths are
        // additionally gated per caller identity (E2E-identity item 1).
        crate::file_tvf_common::enforce_tvf_path_policy(
            FN_NAME,
            &args,
            &self.storage,
            &self.caller,
        )?;

        let framing = json_opts.framing.unwrap_or(JsonFraming::Auto);
        let compression = json_opts
            .compression
            .unwrap_or_else(|| compression_from_extension(&args.path));

        // Router: a top-level array (explicit `format => 'array'`) or a zip
        // archive cannot be served by DataFusion's streaming JSON listing
        // table, so both route to the buffer path (whole-object fetch +
        // decode). Everything else streams. Both branches run only after
        // `enforce_tvf_path_policy` above.
        let use_buffer =
            matches!(framing, JsonFraming::Array) || matches!(compression, JsonCompression::Zip);

        let storage = self.storage.clone();
        let runtime_env = self.runtime_env.clone();

        if use_buffer {
            let max_bytes = DEFAULT_MAX_BUFFER_BYTES;
            crate::runtime_bridge::block_on_compat(async move {
                crate::read_json_buffer::build_buffer_table(
                    &args,
                    framing,
                    compression,
                    max_bytes,
                    &storage,
                    runtime_env.as_deref(),
                )
                .await
            })
            .ok_or_else(|| DataFusionError::Plan(format!("{FN_NAME}: no tokio runtime available")))?
        } else {
            let df_compression = streaming_compression(compression);
            crate::runtime_bridge::block_on_compat(async move {
                build_json_listing_table(&args, &json_opts, df_compression, &storage, runtime_env.as_deref())
                    .await
            })
            .ok_or_else(|| DataFusionError::Plan(format!("{FN_NAME}: no tokio runtime available")))?
        }
    }
}

async fn build_json_listing_table(
    args: &FileTvfArgs,
    json_opts: &JsonOpts,
    compression: FileCompressionType,
    storage: &StorageConfig,
    runtime_env: Option<&datafusion::execution::runtime_env::RuntimeEnv>,
) -> DFResult<Arc<dyn TableProvider>> {
    let mut args = args.clone();
    rewrite_hf_path_in_place(FN_NAME, &mut args)?;

    let listing_url = ListingTableUrl::parse(&args.path)?;

    let tmp_ctx = SessionContext::new();
    register_s3_store_if_needed(FN_NAME, &tmp_ctx, &args, storage, runtime_env)?;
    register_azure_store_if_needed(FN_NAME, &tmp_ctx, &args, storage)?;
    register_gcs_store_if_needed(FN_NAME, &tmp_ctx, &args, storage)?;
    register_http_store_if_needed(FN_NAME, &tmp_ctx, &args.path)?;

    let mut format = JsonFormat::default();
    // NDJSON is the only framing the streaming path serves.
    format = format.with_file_compression_type(compression);

    // The listing extension must match the file as it sits on disk/object
    // store, codec suffix included: `events.jsonl.gz` lists as `.jsonl.gz`,
    // not `.jsonl` (the codec suffix is stripped from the *content*, not
    // from the *filename* ListingTable globs against).
    let derived_extension = derive_file_extension(&args.path);
    let extension = json_opts
        .file_extension
        .as_deref()
        .unwrap_or(derived_extension.as_str());
    let listing_options =
        ListingOptions::new(Arc::new(format)).with_file_extension(extension);

    let state = tmp_ctx.state();
    crate::file_tvf_common::ensure_local_files_exist(
        FN_NAME, &state, &listing_url, extension, &args.path,
    )
    .await?;
    let schema = listing_options.infer_schema(&state, &listing_url).await?;

    let config = ListingTableConfig::new(listing_url)
        .with_listing_options(listing_options)
        .with_schema(schema);

    let table = ListingTable::try_new(config)?;
    debug!(path = %args.path, "read_json: built ListingTable");
    Ok(Arc::new(table))
}

/// Pull the file extension from a path, including the compression suffix
/// if present. `'/data/x.jsonl.gz'` -> `'.jsonl.gz'`. Falls back to
/// `'.json'` when the path is a glob or has no extension. Mirrors
/// `read_csv::derive_file_extension`.
fn derive_file_extension(path: &str) -> String {
    // Drop directory parts.
    let basename = path.rsplit('/').next().unwrap_or(path);
    // If the basename contains glob characters we use the conservative
    // default; ListingTable's globbing will pick up files by that suffix.
    if basename.contains('*') || basename.contains('?') {
        return ".json".to_string();
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
    ".json".to_string()
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

    #[test]
    fn parse_framing_accepts_known_values() {
        assert!(matches!(parse_framing("auto").unwrap(), JsonFraming::Auto));
        assert!(matches!(parse_framing("array").unwrap(), JsonFraming::Array));
        assert!(matches!(
            parse_framing("newline_delimited").unwrap(),
            JsonFraming::NewlineDelimited
        ));
        assert!(matches!(parse_framing("ARRAY").unwrap(), JsonFraming::Array));
        assert!(parse_framing("bogus").is_err());
    }

    #[test]
    fn parse_json_compression_accepts_known_values() {
        assert!(parse_json_compression("auto").unwrap().is_none());
        assert!(matches!(
            parse_json_compression("gzip").unwrap(),
            Some(JsonCompression::Gzip)
        ));
        assert!(matches!(
            parse_json_compression("zip").unwrap(),
            Some(JsonCompression::Zip)
        ));
        assert!(matches!(
            parse_json_compression("none").unwrap(),
            Some(JsonCompression::None)
        ));
        assert!(parse_json_compression("rar").is_err());
    }

    #[test]
    fn compression_from_extension_maps_suffixes() {
        assert!(matches!(compression_from_extension("a.json.gz"), JsonCompression::Gzip));
        assert!(matches!(compression_from_extension("a.json.zip"), JsonCompression::Zip));
        assert!(matches!(compression_from_extension("a.json.zst"), JsonCompression::Zstd));
        assert!(matches!(compression_from_extension("a.json"), JsonCompression::None));
    }

    #[test]
    fn derive_file_extension_keeps_codec_suffix() {
        // The listing filter must match the file as it sits on disk, so
        // the codec suffix stays in the derived extension (regression
        // test for the streaming-gzip-NDJSON glob-miss bug).
        assert_eq!(derive_file_extension("/data/x.json"), ".json");
        assert_eq!(derive_file_extension("/data/x.jsonl.gz"), ".jsonl.gz");
        assert_eq!(derive_file_extension("/data/x.json.zst"), ".json.zst");
        assert_eq!(derive_file_extension("/data/x.ndjson.xz"), ".ndjson.xz");
        assert_eq!(derive_file_extension("/data/glob/*.json"), ".json");
        assert_eq!(derive_file_extension("/data/no_extension"), ".json");
    }

    #[test]
    fn detect_framing_peeks_first_nonws_byte() {
        assert!(matches!(detect_framing_from_bytes(b"   [ {\"a\":1} ]"), JsonFraming::Array));
        assert!(matches!(detect_framing_from_bytes(b"\n\t[1,2]"), JsonFraming::Array));
        assert!(matches!(detect_framing_from_bytes(b"{\"a\":1}\n"), JsonFraming::NewlineDelimited));
        assert!(matches!(detect_framing_from_bytes(b""), JsonFraming::NewlineDelimited));
    }

    #[tokio::test]
    async fn reads_plain_ndjson_local() {
        use datafusion::prelude::SessionContext;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        std::fs::write(&path, b"{\"a\":1}\n{\"a\":2}\n").unwrap();

        let storage = StorageConfig {
            tvf: sqe_core::config::TvfPolicy {
                allow_local_paths: true,
                ..Default::default()
            },
            ..StorageConfig::default()
        };
        let ctx = SessionContext::new();
        ctx.register_udtf("read_json", Arc::new(ReadJsonFunction::new(storage)));
        let df = ctx
            .sql(&format!("SELECT count(*) AS n FROM read_json('{}')", path.display()))
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        // 2 rows expected.
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn reads_full_array_local() {
        use datafusion::prelude::SessionContext;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.json");
        std::fs::write(&path, b"[{\"a\":1},{\"a\":2},{\"a\":3}]").unwrap();

        let storage = StorageConfig {
            tvf: sqe_core::config::TvfPolicy {
                allow_local_paths: true,
                ..Default::default()
            },
            ..StorageConfig::default()
        };
        let ctx = SessionContext::new();
        ctx.register_udtf("read_json", Arc::new(ReadJsonFunction::new(storage)));
        let df = ctx
            .sql(&format!(
                "SELECT count(*) AS n FROM read_json('{}', 'format=array')",
                path.display()
            ))
            .await
            .unwrap();
        let n = df.collect().await.unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn buffer_path_rejects_local_secret() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        ctx.register_udtf(
            "read_json",
            Arc::new(ReadJsonFunction::new(StorageConfig::default())),
        );
        // Anonymous caller + a sensitive absolute path must be denied by
        // policy, BEFORE any object-store fetch. format=array forces the
        // buffer path (the streaming path would also be denied the same
        // way, but this specifically exercises the buffer dispatch).
        //
        // The positional `'format=array'` form (rather than `format =>
        // 'array'`) is used because this test calls `ctx.sql()` on a raw
        // `SessionContext` that hasn't gone through the coordinator's
        // `rewrite_named_tvf_args` preprocessing (see
        // `crates/sqe-sql/src/tvf_named_args.rs`); DataFusion 54 rejects
        // `FunctionArg::Named` at the SQL-planner level before it ever
        // reaches this TVF's `call()`, so `format => 'array'` here would
        // fail on an unrelated parser error instead of exercising the
        // policy gate.
        let res = ctx
            .sql("SELECT * FROM read_json('/etc/shadow', 'format=array')")
            .await;
        // Either plan-time or collect-time error; assert it errors and never reads.
        let err = match res {
            Err(e) => e.to_string(),
            Ok(df) => format!("{:?}", df.collect().await.err()),
        };
        assert!(
            err.to_lowercase().contains("local filesystem paths are disabled")
                || err.to_lowercase().contains("allow_local_paths"),
            "expected a policy denial, got: {err}"
        );
    }

    #[tokio::test]
    async fn buffer_path_rejects_imds_url() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        ctx.register_udtf(
            "read_json",
            Arc::new(ReadJsonFunction::new(StorageConfig::default())),
        );
        // Anonymous caller + an IMDS/SSRF-style URL must be denied by
        // policy, BEFORE any object-store fetch. format=array forces the
        // buffer path (same positional-arg rationale as
        // `buffer_path_rejects_local_secret` above: a raw `SessionContext`
        // hasn't gone through `rewrite_named_tvf_args`, so `format=array`
        // must be passed positionally rather than as `format => 'array'`).
        let res = ctx
            .sql(
                "SELECT * FROM read_json('http://169.254.169.254/latest/meta-data/', \
                 'format=array')",
            )
            .await;
        let err = match res {
            Err(e) => e.to_string(),
            Ok(df) => format!("{:?}", df.collect().await.err()),
        };
        assert!(
            err.contains("169.254.169.254") && err.contains("allowed_http_hosts"),
            "expected an IMDS/SSRF policy denial, got: {err}"
        );
    }

    #[tokio::test]
    async fn reads_gzipped_ndjson_streaming() {
        use datafusion::prelude::SessionContext;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl.gz");
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n").unwrap();
        std::fs::write(&path, enc.finish().unwrap()).unwrap();

        let storage = StorageConfig {
            tvf: sqe_core::config::TvfPolicy {
                allow_local_paths: true,
                ..Default::default()
            },
            ..StorageConfig::default()
        };
        let ctx = SessionContext::new();
        ctx.register_udtf("read_json", Arc::new(ReadJsonFunction::new(storage)));
        // No `format` arg: plain NDJSON with a `.gz` suffix stays on the
        // streaming path (`build_json_listing_table` +
        // `FileCompressionType::GZIP`), unlike `reads_gzipped_array` below
        // which forces the buffer path via `format => 'array'`.
        let n = ctx
            .sql(&format!("SELECT count(*) AS n FROM read_json('{}')", path.display()))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn reads_gzipped_array() {
        use datafusion::prelude::SessionContext;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.json.gz");
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(b"[{\"a\":1},{\"a\":2}]").unwrap();
        std::fs::write(&path, enc.finish().unwrap()).unwrap();

        let storage = StorageConfig {
            tvf: sqe_core::config::TvfPolicy {
                allow_local_paths: true,
                ..Default::default()
            },
            ..StorageConfig::default()
        };
        let ctx = SessionContext::new();
        ctx.register_udtf("read_json", Arc::new(ReadJsonFunction::new(storage)));
        // .gz auto-detects gzip; format=array forces the buffer path (array
        // can't stream). Positional 'format=array' (not `format => 'array'`)
        // because this raw SessionContext hasn't gone through the
        // coordinator's named-TVF-arg rewrite; see
        // `buffer_path_rejects_local_secret` above for the full rationale.
        let n = ctx
            .sql(&format!(
                "SELECT count(*) AS n FROM read_json('{}', 'format=array')",
                path.display()
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn reads_zip_of_ndjson() {
        use datafusion::prelude::SessionContext;
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.json.zip");
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            zw.start_file("events.jsonl", SimpleFileOptions::default())
                .unwrap();
            zw.write_all(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n").unwrap();
            zw.finish().unwrap();
        }
        std::fs::write(&path, buf).unwrap();

        let storage = StorageConfig {
            tvf: sqe_core::config::TvfPolicy {
                allow_local_paths: true,
                ..Default::default()
            },
            ..StorageConfig::default()
        };
        let ctx = SessionContext::new();
        ctx.register_udtf("read_json", Arc::new(ReadJsonFunction::new(storage)));
        // Zip always routes to the buffer path regardless of `format`, so no
        // positional arg is needed here (unlike the array case above).
        let n = ctx
            .sql(&format!("SELECT count(*) AS n FROM read_json('{}')", path.display()))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn reads_full_array_via_newline_delimited_false_alias() {
        use datafusion::prelude::SessionContext;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.json");
        std::fs::write(&path, b"[{\"a\":1},{\"a\":2},{\"a\":3}]").unwrap();

        let storage = StorageConfig {
            tvf: sqe_core::config::TvfPolicy {
                allow_local_paths: true,
                ..Default::default()
            },
            ..StorageConfig::default()
        };
        let ctx = SessionContext::new();
        ctx.register_udtf("read_json", Arc::new(ReadJsonFunction::new(storage)));
        // `newline_delimited=false` is the legacy alias for `format=array`;
        // this locks the newline_delimited => framing::Array => buffer route.
        let df = ctx
            .sql(&format!(
                "SELECT count(*) AS n FROM read_json('{}', 'newline_delimited=false')",
                path.display()
            ))
            .await
            .unwrap();
        let n = df.collect().await.unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 3);
    }
}
