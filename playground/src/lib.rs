//! SQE playground: a DataFusion session compiled to WebAssembly.
//!
//! This is the read-only, single-node, browser-hosted slice of the engine.
//! It is deliberately NOT the SQE engine: there is no Iceberg, no catalog,
//! no policy enforcement. iceberg-rust cannot build for wasm32 because its
//! FileIO goes through opendal, whose `Access` trait uses boxed futures
//! declared without a `Send` bound. object_store has no such problem, so
//! what runs here is DataFusion reading parquet and CSV straight off HTTP
//! or S3.
//!
//! Everything the browser can reach must serve CORS headers, and parquet
//! additionally needs HTTP Range support (206 + `Accept-Ranges: bytes`).

use std::cell::RefCell;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use object_store::http::HttpBuilder;
use serde::Serialize;
use wasm_bindgen::prelude::*;

/// Rows beyond this are dropped before crossing into JS. A browser table
/// stops being readable long before DataFusion stops being able to produce
/// rows, and serialising a million of them just wedges the tab.
const MAX_ROWS: usize = 2_000;

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
    row_count: u32,
    truncated: bool,
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
        .with_allow_http(allow_http);

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

    describe(name).await
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
    let batches = ctx
        .sql(&sql)
        .await
        .map_err(|e| err("plan", e))?
        .collect()
        .await
        .map_err(|e| err("execute", e))?;

    let result = to_result(&batches)?;
    serde_wasm_bindgen::to_value(&result).map_err(|e| err("serialize", e))
}

/// The physical plan, for the "explain" affordance.
#[wasm_bindgen]
pub async fn explain(sql: String) -> Result<JsValue, JsValue> {
    let ctx = with_ctx()?;
    let batches = ctx
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .map_err(|e| err("plan", e))?
        .collect()
        .await
        .map_err(|e| err("execute", e))?;

    let result = to_result(&batches)?;
    serde_wasm_bindgen::to_value(&result).map_err(|e| err("serialize", e))
}

fn to_result(batches: &[RecordBatch]) -> Result<QueryResult, JsValue> {
    let Some(first) = batches.first() else {
        return Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            truncated: false,
        });
    };

    let columns = first
        .schema()
        .fields()
        .iter()
        .map(|f| Column {
            name: f.name().clone(),
            data_type: f.data_type().to_string(),
            numeric: f.data_type().is_numeric(),
        })
        .collect::<Vec<_>>();

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let opts = FormatOptions::default().with_display_error(true);
    let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(total.min(MAX_ROWS));

    'outer: for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| err("format", e))?;

        for r in 0..batch.num_rows() {
            if rows.len() == MAX_ROWS {
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
        truncated: total > rows.len(),
        row_count: total as u32,
        columns,
        rows,
    })
}
