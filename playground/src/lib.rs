//! SQE playground: a DataFusion session compiled to WebAssembly.
//!
//! This is the read-only, single-node, browser-hosted slice of the engine.
//! It is deliberately NOT the SQE engine: there is no catalog, no policy
//! enforcement and no distributed execution. What runs here is DataFusion
//! reading parquet and CSV straight off HTTP or S3, plus copy-on-write
//! Iceberg tables through `iceberg.rs`, which reads the spec's JSON and Avro
//! directly because iceberg-rust cannot build for wasm32 (its FileIO goes
//! through opendal, whose `Access` trait uses boxed futures declared without
//! a `Send` bound).
//!
//! Everything the browser can reach must serve CORS headers, and parquet
//! additionally needs HTTP Range support (206 + `Accept-Ranges: bytes`).

mod iceberg;

use std::cell::RefCell;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::prelude::*;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::http::HttpBuilder;
use object_store::RetryConfig;
use serde::Serialize;
use wasm_bindgen::prelude::*;

/// Rows beyond this are dropped before crossing into JS. A browser table
/// stops being readable long before DataFusion stops being able to produce
/// rows, and serialising a million of them just wedges the tab.
const MAX_ROWS: usize = 2_000;

/// object_store defaults to retrying for three minutes. In a CLI that is
/// reasonable; in a UI a mistyped URL locks the page for three minutes behind
/// a spinner with nothing to read. Fail fast enough that the error is the
/// feedback.
fn retry() -> RetryConfig {
    RetryConfig {
        max_retries: 2,
        retry_timeout: std::time::Duration::from_secs(12),
        ..Default::default()
    }
}

thread_local! {
    static CTX: RefCell<Option<SessionContext>> = const { RefCell::new(None) };
}

#[derive(Serialize)]
struct Column {
    name: String,
    data_type: String,
    /// Lets the UI right-align numbers without reparsing every cell.
    numeric: bool,
}

#[derive(Serialize)]
struct QueryResult {
    columns: Vec<Column>,
    rows: Vec<Vec<Option<String>>>,
    /// Rows returned. When `truncated`, the scan was stopped at the cap and
    /// the true total is unknown, so this is a lower bound.
    row_count: u32,
    truncated: bool,
}

#[derive(Serialize)]
struct IcebergInfo {
    name: String,
    columns: Vec<Column>,
    snapshot_id: String,
    data_files: u32,
    location: String,
    schema_mismatch: bool,
}

#[derive(Serialize)]
struct TableInfo {
    name: String,
    columns: Vec<Column>,
}

fn err(context: &str, e: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&format!("{context}: {e}"))
}

fn with_ctx() -> Result<SessionContext, JsValue> {
    CTX.with(|c| {
        c.borrow()
            .clone()
            .ok_or_else(|| JsValue::from_str("engine not started; call start() first"))
    })
}

/// Boot the session. Safe to call more than once; the second call resets
/// every registered table, which is what the UI's "reset" affordance wants.
#[wasm_bindgen]
pub fn start() {
    console_error_panic_hook::set_once();
    let ctx = SessionContext::new();
    CTX.with(|c| *c.borrow_mut() = Some(ctx));
}

/// Wire up the object store that serves `url`, so DataFusion can reach it.
///
/// For `http(s)://` we register the origin, not the full path: object_store's
/// HTTP store treats its configured URL as the root, and DataFusion resolves
/// table paths against the registered scheme+authority.
fn register_store(ctx: &SessionContext, url: &url::Url) -> Result<(), JsValue> {
    match url.scheme() {
        "http" | "https" => {
            let origin = format!(
                "{}://{}",
                url.scheme(),
                url.host_str().unwrap_or_default()
            );
            let origin = match url.port() {
                Some(p) => format!("{origin}:{p}"),
                None => origin,
            };
            let store = HttpBuilder::new()
                .with_url(&origin)
                .with_retry(retry())
                .build()
                .map_err(|e| err("http store", e))?;
            ctx.register_object_store(url, Arc::new(store));
            Ok(())
        }
        // s3:// is registered up front by configure_s3, which is the only
        // place credentials exist.
        "s3" => Ok(()),
        other => Err(JsValue::from_str(&format!(
            "unsupported scheme '{other}': use http, https or s3"
        ))),
    }
}

/// Point the session at an S3-compatible bucket. Call before registering an
/// `s3://` table. `endpoint` is for MinIO/RustFS/Ceph; leave it empty for AWS.
///
/// The bucket must send CORS headers or the browser will refuse the request
/// before object_store ever sees it.
#[wasm_bindgen]
pub fn configure_s3(
    bucket: String,
    region: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
    allow_http: bool,
) -> Result<(), JsValue> {
    let ctx = with_ctx()?;
    let mut b = AmazonS3Builder::new()
        .with_bucket_name(&bucket)
        .with_region(if region.is_empty() {
            "us-east-1".to_string()
        } else {
            region
        })
        .with_allow_http(allow_http)
        .with_retry(retry());

    if !endpoint.is_empty() {
        b = b.with_endpoint(&endpoint).with_virtual_hosted_style_request(false);
    }
    if access_key.is_empty() {
        b = b.with_skip_signature(true);
    } else {
        b = b.with_access_key_id(&access_key).with_secret_access_key(&secret_key);
    }

    let store = b.build().map_err(|e| err("s3 store", e))?;
    let base = url::Url::parse(&format!("s3://{bucket}")).map_err(|e| err("bucket url", e))?;
    ctx.register_object_store(&base, Arc::new(store));
    Ok(())
}

/// Register `url` as queryable table `name`.
///
/// Format comes from the extension. A URL ending in `/` is treated as a
/// directory of same-format files, which is how partitioned exports and
/// Hive-style layouts land here.
#[wasm_bindgen]
pub async fn register(name: String, url: String) -> Result<JsValue, JsValue> {
    let ctx = with_ctx()?;
    let parsed = url::Url::parse(&url).map_err(|e| err("url", e))?;
    register_store(&ctx, &parsed)?;

    let is_dir = url.ends_with('/');
    let probe = url.trim_end_matches('/').to_ascii_lowercase();
    let csv = probe.ends_with(".csv") || (is_dir && probe.ends_with("csv"));

    let (format, ext): (Arc<dyn FileFormat>, &str) = if csv {
        (Arc::new(CsvFormat::default().with_has_header(true)), ".csv")
    } else {
        (Arc::new(ParquetFormat::default()), ".parquet")
    };

    let mut opts = ListingOptions::new(format);
    if is_dir {
        opts = opts.with_file_extension(ext);
    }

    ctx.register_listing_table(&name, &url, opts, None, None)
        .await
        .map_err(|e| err("register", e))?;

    // Schema inference over a file list that came back empty yields a table
    // with no columns and no error, which then fails much later as "No field
    // named x" and points at the query instead of the source. Refuse it here.
    // Observed intermittently against an S3 endpoint when a range read is
    // truncated, so this is a real state to guard, not a theoretical one.
    let info = describe(name.clone()).await?;
    let empty = ctx
        .table(&name)
        .await
        .map(|df| df.schema().fields().is_empty())
        .unwrap_or(true);
    if empty {
        let _ = ctx.deregister_table(&name);
        return Err(JsValue::from_str(&format!(
            "register: '{url}' resolved to a table with no columns. \
             Either nothing matched the path, or the reads that infer the \
             schema came back truncated. For an S3 host, check that its CORS \
             policy exposes content-range."
        )));
    }

    Ok(info)
}

/// Register the Iceberg table whose `metadata.json` lives at `url`.
///
/// Resolves the current snapshot to an explicit list of parquet files and
/// registers exactly those. Registering the data DIRECTORY instead would
/// appear to work and be wrong: it picks up files from every snapshot,
/// including ones the current snapshot has replaced.
#[wasm_bindgen]
pub async fn register_iceberg(name: String, url: String) -> Result<JsValue, JsValue> {
    let ctx = with_ctx()?;
    let parsed = url::Url::parse(&url).map_err(|e| err("url", e))?;
    register_store(&ctx, &parsed)?;

    let table = iceberg::load(&ctx, &parsed)
        .await
        .map_err(|e| JsValue::from_str(&format!("iceberg: {e}")))?;

    let paths = table
        .data_files
        .iter()
        .map(ListingTableUrl::parse)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| err("data file path", e))?;

    let opts = ListingOptions::new(Arc::new(ParquetFormat::default()));
    let config = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(opts)
        .infer_schema(&ctx.state())
        .await
        .map_err(|e| err("infer schema", e))?;
    let provider = ListingTable::try_new(config).map_err(|e| err("listing table", e))?;

    ctx.register_table(&name, Arc::new(provider))
        .map_err(|e| err("register", e))?;

    // Surface what the CATALOG says the schema is next to what the files say.
    // They should agree; when they do not, the parquet was written under an
    // older schema and column names are the thing most likely to be stale.
    let info = ctx.table(&name).await.map_err(|e| err("table", e))?;
    let file_names = info
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    let catalog_names = table.fields.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();
    let schema_mismatch = file_names != catalog_names;

    let out = IcebergInfo {
        name: name.clone(),
        columns: info
            .schema()
            .fields()
            .iter()
            .map(|f| Column {
                name: f.name().clone(),
                data_type: f.data_type().to_string(),
                numeric: f.data_type().is_numeric(),
            })
            .collect(),
        snapshot_id: table.snapshot_id.to_string(),
        data_files: table.data_files.len() as u32,
        location: table.location,
        schema_mismatch,
    };
    serde_wasm_bindgen::to_value(&out).map_err(|e| err("serialize", e))
}

/// Schema of an already-registered table, for the sidebar.
#[wasm_bindgen]
pub async fn describe(name: String) -> Result<JsValue, JsValue> {
    let ctx = with_ctx()?;
    let df = ctx.table(&name).await.map_err(|e| err("table", e))?;
    let info = TableInfo {
        name,
        columns: df
            .schema()
            .fields()
            .iter()
            .map(|f| Column {
                name: f.name().clone(),
                data_type: f.data_type().to_string(),
                numeric: f.data_type().is_numeric(),
            })
            .collect(),
    };
    serde_wasm_bindgen::to_value(&info).map_err(|e| err("serialize", e))
}

/// Names of every registered table.
#[wasm_bindgen]
pub fn table_names() -> Result<JsValue, JsValue> {
    let ctx = with_ctx()?;
    let names = ctx
        .catalog_names()
        .into_iter()
        .filter_map(|c| ctx.catalog(&c))
        .flat_map(|cat| {
            cat.schema_names()
                .into_iter()
                .filter_map(move |s| cat.schema(&s))
                .flat_map(|sch| sch.table_names())
        })
        .collect::<Vec<_>>();
    serde_wasm_bindgen::to_value(&names).map_err(|e| err("serialize", e))
}

/// Run `sql` and return columns plus stringified rows.
///
/// Cells are rendered with arrow's own display formatter, so timestamps,
/// decimals and nested types come out the way the CLI prints them rather
/// than however JSON would guess.
#[wasm_bindgen]
pub async fn run_sql(sql: String) -> Result<JsValue, JsValue> {
    let ctx = with_ctx()?;
    let df = ctx.sql(&sql).await.map_err(|e| err("plan", e))?;
    let result = to_result(df).await?;
    serde_wasm_bindgen::to_value(&result).map_err(|e| err("serialize", e))
}

/// The physical plan, for the "explain" affordance.
#[wasm_bindgen]
pub async fn explain(sql: String) -> Result<JsValue, JsValue> {
    let ctx = with_ctx()?;
    let df = ctx
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .map_err(|e| err("plan", e))?;
    let result = to_result(df).await?;
    serde_wasm_bindgen::to_value(&result).map_err(|e| err("serialize", e))
}

/// Stream `df` and keep the first `MAX_ROWS` rows.
///
/// The stream is dropped as soon as one row past the cap arrives, which
/// cancels the scan. Collecting first would make `SELECT *` without a LIMIT
/// on the 2 GB dataset pull every byte through the one wasm thread just to
/// show 2,000 rows. The price is that a truncated result does not know its
/// total, so `row_count` is then a lower bound.
async fn to_result(df: DataFrame) -> Result<QueryResult, JsValue> {
    let mut stream = df.execute_stream().await.map_err(|e| err("execute", e))?;

    // From the stream, not the first batch, so a query that returns no rows
    // still shows its column headers.
    let columns = stream
        .schema()
        .fields()
        .iter()
        .map(|f| Column {
            name: f.name().clone(),
            data_type: f.data_type().to_string(),
            numeric: f.data_type().is_numeric(),
        })
        .collect::<Vec<_>>();

    let opts = FormatOptions::default().with_display_error(true);
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut truncated = false;

    'outer: while let Some(batch) = stream.next().await {
        let batch: RecordBatch = batch.map_err(|e| err("execute", e))?;
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| err("format", e))?;

        for r in 0..batch.num_rows() {
            if rows.len() == MAX_ROWS {
                truncated = true;
                break 'outer;
            }
            rows.push(
                formatters
                    .iter()
                    .enumerate()
                    .map(|(c, f)| {
                        if batch.column(c).is_null(r) {
                            None
                        } else {
                            Some(f.value(r).to_string())
                        }
                    })
                    .collect(),
            );
        }
    }

    Ok(QueryResult {
        row_count: rows.len() as u32,
        truncated,
        columns,
        rows,
    })
}
