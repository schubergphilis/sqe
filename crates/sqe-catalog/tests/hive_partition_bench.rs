//! Scale bench for hive cycle-2: listing vs index vs projection.
//!
//! Ignored in the default suite. Run with:
//! ```text
//! cargo test -p sqe-catalog --test hive_partition_bench --features hive-external \
//!   --profile dev-release -- --ignored --nocapture --test-threads=1
//! ```
//! Writes `benchmarks/results/hive-cycle2-partition-index-scale.json`.

#![cfg(feature = "hive-external")]

use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, AsArray, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema, UInt64Type};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use datafusion::prelude::SessionContext;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use serde::Serialize;
use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits};
use sqe_catalog::hive::partition::msck_repair;
use sqe_catalog::hive::store::HiveManifestStore;
use sqe_core::config::{TvfCaller, TvfPolicy};

#[allow(dead_code)]
mod hive_fixture;
use hive_fixture::{register_hive_root, Fixture};

fn store_for(dir: &Path) -> HiveManifestStore {
    let url = datafusion::datasource::listing::ListingTableUrl::parse(format!(
        "file://{}",
        dir.display()
    ))
    .expect("url");
    HiveManifestStore::new(
        url,
        Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>,
        ManifestLimits::default(),
        TvfPolicy {
            allow_local_paths: true,
            ..TvfPolicy::default()
        },
        TvfCaller::trusted_local(),
    )
}

fn dates(n: usize) -> Vec<String> {
    let start = NaiveDate::from_ymd_opt(2020, 1, 1).expect("date");
    (0..n)
        .map(|i| (start + chrono::Duration::days(i as i64)).to_string())
        .collect()
}

fn write_csv_partition(dir: &Path, dt: &str, rows: usize, start_id: i64) {
    let part = dir.join(format!("dt={dt}"));
    std::fs::create_dir_all(&part).expect("mkdir");
    let mut body = String::from("id,amount\n");
    for i in 0..rows {
        body.push_str(&format!("{},{}.0\n", start_id + i as i64, i));
    }
    std::fs::write(part.join("part-0.csv"), body).expect("csv");
}

fn write_parquet_partition(dir: &Path, dt: &str, rows: usize, start_id: i64) {
    let part = dir.join(format!("dt={dt}"));
    std::fs::create_dir_all(&part).expect("mkdir");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let ids: Vec<i64> = (0..rows as i64).map(|i| start_id + i).collect();
    let amounts: Vec<f64> = (0..rows as i64).map(|i| i as f64).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .expect("batch");
    let file = File::create(part.join("part-0.parquet")).expect("create");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

async fn put_manifest(root: &str, data: &str, format: &str) {
    let ddl = if format == "parquet" {
        format!(
            "CREATE EXTERNAL TABLE sales.orders (id BIGINT, amount DOUBLE) \
             PARTITIONED BY (dt date) STORED AS PARQUET LOCATION '{data}'"
        )
    } else {
        format!(
            "CREATE EXTERNAL TABLE sales.orders (id BIGINT, amount DOUBLE) \
             PARTITIONED BY (dt date) \
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
             STORED AS TEXTFILE LOCATION '{data}' \
             TBLPROPERTIES ('skip.header.line.count' = '1')"
        )
    };
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .unwrap_or_else(|e| panic!("parse {ddl}: {e}"));
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let m = HiveManifest::from_spec(&spec, "sales", "bench", "2026-08-23T00:00:00Z");
    store_for(Path::new(root.trim_start_matches("file://")))
        .put(&m, false)
        .await
        .expect("put");
}

fn count_num(batches: &[RecordBatch]) -> i64 {
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return 0;
    }
    let col = batches[0].column(0);
    if col.is_null(0) {
        return 0;
    }
    if let Some(a) = col.as_primitive_opt::<Int64Type>() {
        a.value(0)
    } else if let Some(a) = col.as_primitive_opt::<UInt64Type>() {
        a.value(0) as i64
    } else if let Some(a) = col.as_primitive_opt::<Float64Type>() {
        a.value(0) as i64
    } else {
        panic!("aggregate column type {:?}", col.data_type());
    }
}

async fn time_sql(ctx: &SessionContext, sql: &str) -> (Duration, i64) {
    let t0 = Instant::now();
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("collect {sql}: {e}"));
    (t0.elapsed(), count_num(&batches))
}

struct Run {
    cold_ms: f64,
    warm_ms: f64,
    rows: i64,
}

async fn cold_warm(f: &Fixture, sql: &str) -> Run {
    let ctx = SessionContext::new();
    register_hive_root(&ctx, f).await;
    let (cold, rows) = time_sql(&ctx, sql).await;
    let _ = time_sql(&ctx, sql).await;
    let (warm, rows2) = time_sql(&ctx, sql).await;
    assert_eq!(rows, rows2, "cold/warm row count drifted for {sql}");
    Run {
        cold_ms: cold.as_secs_f64() * 1000.0,
        warm_ms: warm.as_secs_f64() * 1000.0,
        rows,
    }
}

#[derive(Serialize)]
struct Case {
    name: String,
    format: String,
    partitions: usize,
    rows_per_partition: usize,
    predicate: Option<String>,
    listing_cold_ms: Option<f64>,
    listing_warm_ms: Option<f64>,
    indexed_cold_ms: Option<f64>,
    indexed_warm_ms: Option<f64>,
    projection_cold_ms: Option<f64>,
    projection_warm_ms: Option<f64>,
    msck_ms: Option<f64>,
    rows: Option<i64>,
}

async fn scale(format: &str, partitions: usize, rows: usize, pin_only: bool) -> Vec<Case> {
    let dts = dates(partitions);
    let dir = tempfile::tempdir().expect("tmp");
    let data_path = dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("mkdir");
    for (i, dt) in dts.iter().enumerate() {
        match format {
            "parquet" => write_parquet_partition(&data_path, dt, rows, (i * rows) as i64),
            _ => write_csv_partition(&data_path, dt, rows, (i * rows) as i64),
        }
    }
    let wh = dir.path().join("wh");
    std::fs::create_dir_all(&wh).expect("wh");
    let root = format!("file://{}", wh.display());
    let data = format!("file://{}", data_path.display());
    put_manifest(&root, &data, format).await;
    let f = Fixture {
        dir,
        root: root.clone(),
    };

    let pin = dts.last().cloned().unwrap();
    let full_sql = "SELECT count(*) FROM lake.sales.orders";
    let pin_sql = format!("SELECT count(*) FROM lake.sales.orders WHERE dt = '{pin}'");

    let mut out = Vec::new();
    if !pin_only {
        let listing = cold_warm(&f, full_sql).await;
        let listing_pin = cold_warm(&f, &pin_sql).await;
        let store = store_for(wh.as_path());
        let mut m = store.get("sales", "orders").await.expect("get");
        let t0 = Instant::now();
        let n = msck_repair(&store, &mut m, 100_000).await.expect("msck");
        let msck_ms = t0.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(n, partitions);

        let indexed = cold_warm(&f, full_sql).await;
        let indexed_pin = cold_warm(&f, &pin_sql).await;
        assert_eq!(listing.rows, indexed.rows);
        assert_eq!(listing_pin.rows, rows as i64, "listing pin row count");
        assert_eq!(indexed_pin.rows, rows as i64, "indexed pin row count");

        println!(
            "{format:7} n={partitions:<5} full  list {:7.1}/{:.1}  idx {:7.1}/{:.1}  msck {:7.1}  rows {}",
            listing.cold_ms, listing.warm_ms, indexed.cold_ms, indexed.warm_ms, msck_ms, listing.rows
        );
        println!(
            "{format:7} n={partitions:<5} pin   list {:7.1}/{:.1}  idx {:7.1}/{:.1}  rows {}",
            listing_pin.cold_ms,
            listing_pin.warm_ms,
            indexed_pin.cold_ms,
            indexed_pin.warm_ms,
            indexed_pin.rows
        );

        out.push(Case {
            name: format!("{format}_{partitions}_full"),
            format: format.into(),
            partitions,
            rows_per_partition: rows,
            predicate: None,
            listing_cold_ms: Some(listing.cold_ms),
            listing_warm_ms: Some(listing.warm_ms),
            indexed_cold_ms: Some(indexed.cold_ms),
            indexed_warm_ms: Some(indexed.warm_ms),
            projection_cold_ms: None,
            projection_warm_ms: None,
            msck_ms: Some(msck_ms),
            rows: Some(listing.rows),
        });
        out.push(Case {
            name: format!("{format}_{partitions}_pin"),
            format: format.into(),
            partitions,
            rows_per_partition: rows,
            predicate: Some(format!("dt = '{pin}'")),
            listing_cold_ms: Some(listing_pin.cold_ms),
            listing_warm_ms: Some(listing_pin.warm_ms),
            indexed_cold_ms: Some(indexed_pin.cold_ms),
            indexed_warm_ms: Some(indexed_pin.warm_ms),
            projection_cold_ms: None,
            projection_warm_ms: None,
            msck_ms: None,
            rows: Some(indexed_pin.rows),
        });
    } else {
        let listing_pin = cold_warm(&f, &pin_sql).await;
        let store = store_for(wh.as_path());
        let mut m = store.get("sales", "orders").await.expect("get");
        let t0 = Instant::now();
        let n = msck_repair(&store, &mut m, 100_000).await.expect("msck");
        let msck_ms = t0.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(n, partitions);
        let indexed_pin = cold_warm(&f, &pin_sql).await;
        println!(
            "{format:7} n={partitions:<5} pin   list {:7.1}/{:.1}  idx {:7.1}/{:.1}  msck {:7.1}",
            listing_pin.cold_ms,
            listing_pin.warm_ms,
            indexed_pin.cold_ms,
            indexed_pin.warm_ms,
            msck_ms
        );
        out.push(Case {
            name: format!("{format}_{partitions}_pin"),
            format: format.into(),
            partitions,
            rows_per_partition: rows,
            predicate: Some(format!("dt = '{pin}'")),
            listing_cold_ms: Some(listing_pin.cold_ms),
            listing_warm_ms: Some(listing_pin.warm_ms),
            indexed_cold_ms: Some(indexed_pin.cold_ms),
            indexed_warm_ms: Some(indexed_pin.warm_ms),
            projection_cold_ms: None,
            projection_warm_ms: None,
            msck_ms: Some(msck_ms),
            rows: Some(indexed_pin.rows),
        });
    }

    // Projection pin: same files, computed set, no prefix LIST.
    {
        let store = store_for(wh.as_path());
        let mut m = store.get("sales", "orders").await.expect("get");
        m.parameters
            .insert("projection.enabled".into(), "true".into());
        m.parameters
            .insert("projection.dt.type".into(), "date".into());
        m.parameters
            .insert("projection.dt.format".into(), "yyyy-MM-dd".into());
        m.parameters.insert(
            "projection.dt.range".into(),
            format!("{},{}", dts.first().unwrap(), dts.last().unwrap()),
        );
        store.put(&m, false).await.expect("put projection");
        let proj = cold_warm(&f, &pin_sql).await;
        println!(
            "{format:7} n={partitions:<5} pin   proj {:7.1}/{:.1}  rows {}",
            proj.cold_ms, proj.warm_ms, proj.rows
        );
        out.last_mut().unwrap().projection_cold_ms = Some(proj.cold_ms);
        out.last_mut().unwrap().projection_warm_ms = Some(proj.warm_ms);
    }
    out
}

fn block_on_prod_stack<F>(fut: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("runtime")
        .block_on(async { tokio::spawn(fut).await.expect("join") })
}

#[derive(Serialize)]
struct Report {
    plan: &'static str,
    generated_at_rfc3339: String,
    profile: &'static str,
    cases: Vec<Case>,
}

#[test]
#[ignore]
fn hive_cycle2_scale_bench() {
    block_on_prod_stack(async {
        let mut cases = Vec::new();
        cases.extend(scale("csv", 256, 50, false).await);
        cases.extend(scale("csv", 1024, 20, false).await);
        cases.extend(scale("csv", 4096, 2, true).await);
        cases.extend(scale("parquet", 256, 200, false).await);
        cases.extend(scale("parquet", 1024, 50, true).await);

        let report = Report {
            plan: "hive-external-cycle2-partition-index",
            generated_at_rfc3339: chrono::Utc::now().to_rfc3339(),
            profile: "dev-release",
            cases,
        };
        let out = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmarks/results/hive-cycle2-partition-index-scale.json");
        std::fs::write(&out, serde_json::to_string_pretty(&report).unwrap()).expect("write json");
        println!("wrote {}", out.display());
    });
}

/// Real AWS public blockchain parquet (`date=YYYY-MM-DD/` Hive layout),
/// copied locally. Default slice: 2009 + 2024 Q1 under
/// `HIVE_BTC_DATA` (default `/tmp/sqe-btc-hive/data`).
#[test]
#[ignore]
fn hive_btc_public_slice_bench() {
    let data_dir = std::env::var("HIVE_BTC_DATA")
        .unwrap_or_else(|_| "/tmp/sqe-btc-hive/data".to_string());
    assert!(
        Path::new(&data_dir).is_dir(),
        "missing {data_dir}; copy with:\n  aws s3 cp --no-sign-request --recursive \\\n    s3://aws-public-blockchain/v1.0/btc/blocks/ {data_dir}/ \\\n    --exclude '*' --include 'date=2009-*' --include 'date=2024-01-*' ..."
    );

    block_on_prod_stack(async move {
        let data_path = Path::new(&data_dir);
        let tmp = tempfile::tempdir().expect("wh");
        let wh = tmp.path().join("wh");
        std::fs::create_dir_all(&wh).expect("mkdir wh");
        let root = format!("file://{}", wh.display());
        let data = format!("file://{}", data_path.display());
        let ddl = format!(
            "CREATE EXTERNAL TABLE sales.orders (\
               hash STRING, version BIGINT, nonce BIGINT, bits STRING, \
               difficulty DOUBLE, chainwork STRING, size BIGINT, weight BIGINT, \
               coinbase_param STRING, \"number\" BIGINT, transaction_count BIGINT, \
               merkle_root STRING, stripped_size BIGINT\
             ) PARTITIONED BY (date STRING) STORED AS PARQUET LOCATION '{data}'"
        );
        let stmts =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
                .expect("parse");
        let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
            .expect("extract")
            .expect("external");
        let m = HiveManifest::from_spec(&spec, "sales", "bench", "2026-08-23T00:00:00Z");
        store_for(&wh).put(&m, false).await.expect("put");
        let f = Fixture {
            dir: tmp,
            root: root.clone(),
        };

        let n_parts = std::fs::read_dir(data_path)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .ok()
                    .and_then(|d| d.file_name().to_str().map(|s| s.starts_with("date=")))
                    .unwrap_or(false)
            })
            .count();

        let full = "SELECT count(*) FROM lake.sales.orders";
        let pin = "SELECT count(*) FROM lake.sales.orders WHERE date = '2024-01-01'";
        let pin_tx =
            "SELECT sum(transaction_count) FROM lake.sales.orders WHERE date = '2024-01-01'";
        let full_tx = "SELECT sum(transaction_count) FROM lake.sales.orders";

        let listing_full = cold_warm(&f, full).await;
        let listing_pin = cold_warm(&f, pin).await;
        let listing_pin_tx = cold_warm(&f, pin_tx).await;

        let store = store_for(&wh);
        let mut m = store.get("sales", "orders").await.expect("get");
        let t0 = Instant::now();
        let repaired = msck_repair(&store, &mut m, 100_000).await.expect("msck");
        let msck_ms = t0.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(repaired, n_parts, "MSCK should see every date= dir");

        let indexed_full = cold_warm(&f, full).await;
        let indexed_pin = cold_warm(&f, pin).await;
        let indexed_pin_tx = cold_warm(&f, pin_tx).await;
        let indexed_full_tx = cold_warm(&f, full_tx).await;

        println!(
            "btc slice partitions={n_parts} msck={msck_ms:.1}ms"
        );
        println!(
            "  count(*)        list {:7.1}/{:.1}  idx {:7.1}/{:.1}  rows {}",
            listing_full.cold_ms,
            listing_full.warm_ms,
            indexed_full.cold_ms,
            indexed_full.warm_ms,
            listing_full.rows
        );
        println!(
            "  pin 2024-01-01  list {:7.1}/{:.1}  idx {:7.1}/{:.1}  rows {}",
            listing_pin.cold_ms,
            listing_pin.warm_ms,
            indexed_pin.cold_ms,
            indexed_pin.warm_ms,
            indexed_pin.rows
        );
        println!(
            "  pin sum(tx)     list {:7.1}/{:.1}  idx {:7.1}/{:.1}  sum {}",
            listing_pin_tx.cold_ms,
            listing_pin_tx.warm_ms,
            indexed_pin_tx.cold_ms,
            indexed_pin_tx.warm_ms,
            indexed_pin_tx.rows
        );
        println!(
            "  sum(tx) all     idx {:7.1}/{:.1}  sum {}",
            indexed_full_tx.cold_ms, indexed_full_tx.warm_ms, indexed_full_tx.rows
        );

        // Projection over the actual min/max dates in the slice, pin only.
        {
            let mut dates: Vec<String> = std::fs::read_dir(data_path)
                .unwrap()
                .filter_map(|e| {
                    e.ok()?
                        .file_name()
                        .to_str()?
                        .strip_prefix("date=")
                        .map(str::to_string)
                })
                .collect();
            dates.sort();
            let mut m = store.get("sales", "orders").await.expect("get");
            m.parameters
                .insert("projection.enabled".into(), "true".into());
            m.parameters
                .insert("projection.date.type".into(), "date".into());
            m.parameters
                .insert("projection.date.format".into(), "yyyy-MM-dd".into());
            m.parameters.insert(
                "projection.date.range".into(),
                format!("{},{}", dates.first().unwrap(), dates.last().unwrap()),
            );
            store.put(&m, false).await.expect("put projection");
            let proj = cold_warm(&f, pin).await;
            println!(
                "  pin projection  {:7.1}/{:.1}  rows {}",
                proj.cold_ms, proj.warm_ms, proj.rows
            );

            let cases = vec![
                Case {
                    name: "btc_count_full".into(),
                    format: "parquet".into(),
                    partitions: n_parts,
                    rows_per_partition: 0,
                    predicate: None,
                    listing_cold_ms: Some(listing_full.cold_ms),
                    listing_warm_ms: Some(listing_full.warm_ms),
                    indexed_cold_ms: Some(indexed_full.cold_ms),
                    indexed_warm_ms: Some(indexed_full.warm_ms),
                    projection_cold_ms: None,
                    projection_warm_ms: None,
                    msck_ms: Some(msck_ms),
                    rows: Some(listing_full.rows),
                },
                Case {
                    name: "btc_count_pin_2024_01_01".into(),
                    format: "parquet".into(),
                    partitions: n_parts,
                    rows_per_partition: 0,
                    predicate: Some("date = '2024-01-01'".into()),
                    listing_cold_ms: Some(listing_pin.cold_ms),
                    listing_warm_ms: Some(listing_pin.warm_ms),
                    indexed_cold_ms: Some(indexed_pin.cold_ms),
                    indexed_warm_ms: Some(indexed_pin.warm_ms),
                    projection_cold_ms: Some(proj.cold_ms),
                    projection_warm_ms: Some(proj.warm_ms),
                    msck_ms: None,
                    rows: Some(indexed_pin.rows),
                },
                Case {
                    name: "btc_sum_tx_pin_2024_01_01".into(),
                    format: "parquet".into(),
                    partitions: n_parts,
                    rows_per_partition: 0,
                    predicate: Some("date = '2024-01-01'".into()),
                    listing_cold_ms: Some(listing_pin_tx.cold_ms),
                    listing_warm_ms: Some(listing_pin_tx.warm_ms),
                    indexed_cold_ms: Some(indexed_pin_tx.cold_ms),
                    indexed_warm_ms: Some(indexed_pin_tx.warm_ms),
                    projection_cold_ms: None,
                    projection_warm_ms: None,
                    msck_ms: None,
                    rows: Some(indexed_pin_tx.rows),
                },
                Case {
                    name: "btc_sum_tx_full".into(),
                    format: "parquet".into(),
                    partitions: n_parts,
                    rows_per_partition: 0,
                    predicate: None,
                    listing_cold_ms: None,
                    listing_warm_ms: None,
                    indexed_cold_ms: Some(indexed_full_tx.cold_ms),
                    indexed_warm_ms: Some(indexed_full_tx.warm_ms),
                    projection_cold_ms: None,
                    projection_warm_ms: None,
                    msck_ms: None,
                    rows: Some(indexed_full_tx.rows),
                },
            ];
            let report = Report {
                plan: "hive-external-cycle2-bitcoin-public-slice",
                generated_at_rfc3339: chrono::Utc::now().to_rfc3339(),
                profile: "dev-release",
                cases,
            };
            let out = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../benchmarks/results/hive-btc-slice.json");
            std::fs::write(&out, serde_json::to_string_pretty(&report).unwrap()).expect("write");
            println!("wrote {}", out.display());
        }
    });
}
