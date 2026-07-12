use std::sync::Arc;

use arrow_array::{Date32Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{parquet_writer, BenchmarkGenerator, GenerateStats, TableDef};

/// TPC-BB (BigBench) generator — additional tables only.
///
/// TPC-BB reuses all TPC-DS dimension and fact tables.  Those tables must be
/// generated separately with `sqe-bench generate tpcds`.  This generator
/// produces only the two TPC-BB–specific tables:
///
///   - `web_clickstreams`  (SF × 4,000,000 rows)
///   - `product_reviews`   (SF × 100,000 rows)
pub struct TpcbbGenerator;

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

fn web_clickstreams_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("wcs_click_date_sk", DataType::Int32, true),
        Field::new("wcs_click_time_sk", DataType::Int32, true),
        Field::new("wcs_sales_sk", DataType::Int64, true),
        Field::new("wcs_item_sk", DataType::Int32, true),
        Field::new("wcs_web_page_sk", DataType::Int32, true),
        Field::new("wcs_user_sk", DataType::Int32, true),
        Field::new("wcs_referrer_url", DataType::Utf8, true),
        Field::new("wcs_search_keywords", DataType::Utf8, true),
    ]))
}

fn product_reviews_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pr_review_sk", DataType::Int64, false),
        Field::new("pr_review_date", DataType::Date32, true),
        Field::new("pr_review_time", DataType::Utf8, true),
        Field::new("pr_review_rating", DataType::Int32, false),
        Field::new("pr_item_sk", DataType::Int32, true),
        Field::new("pr_user_sk", DataType::Int32, true),
        Field::new("pr_order_sk", DataType::Int64, true),
        Field::new("pr_review_content", DataType::Utf8, true),
        Field::new("pr_title", DataType::Utf8, true),
    ]))
}

// ---------------------------------------------------------------------------
// Seed derivation (same algorithm as tpch.rs for consistency)
// ---------------------------------------------------------------------------

fn seed_for_table(name: &str) -> u64 {
    name.bytes()
        .enumerate()
        .fold(0u64, |acc, (i, b)| {
            acc ^ ((b as u64).wrapping_shl(i as u32 % 64))
        })
        .wrapping_add(0xBB00_BB00_BB00_BB00)
}

// ---------------------------------------------------------------------------
// Random helpers
// ---------------------------------------------------------------------------

const BATCH_SIZE: usize = 10_000;

fn random_word(rng: &mut StdRng, len: usize) -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    (0..len)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

fn random_sentence(rng: &mut StdRng) -> String {
    let words = rng.gen_range(4..12usize);
    let mut parts = Vec::with_capacity(words);
    for _ in 0..words {
        let len = rng.gen_range(3..10usize);
        parts.push(random_word(rng, len));
    }
    parts.join(" ")
}

const REFERRER_DOMAINS: &[&str] = &[
    "https://search.example.com",
    "https://ads.example.net",
    "https://social.example.org",
    "https://email.example.com",
    "https://partner.example.io",
    "https://direct.example.com",
    "",
];

const SEARCH_KEYWORDS: &[&str] = &[
    "cheap electronics",
    "best price laptop",
    "discount shoes",
    "sale clothing",
    "free shipping books",
    "top rated camera",
    "affordable furniture",
    "new arrivals fashion",
    "sports equipment",
    "kitchen appliances",
];

const REVIEW_TITLES: &[&str] = &[
    "Great product",
    "Would recommend",
    "Not as described",
    "Excellent quality",
    "Good value for money",
    "Disappointed",
    "Exceeded expectations",
    "Average product",
    "Five stars",
    "Would not buy again",
];

// TPC-DS date range: 1998-01-01 to 2003-12-31 (approx 6 years)
// Days since epoch for 1998-01-01: 10227
const DATE_START: i32 = 10227;
const DATE_RANGE: i32 = 2190; // ~6 years

fn random_date(rng: &mut StdRng) -> i32 {
    DATE_START + rng.gen_range(0..DATE_RANGE)
}

fn random_time_str(rng: &mut StdRng) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        rng.gen_range(0..24u32),
        rng.gen_range(0..60u32),
        rng.gen_range(0..60u32),
    )
}

// ---------------------------------------------------------------------------
// Table generators
// ---------------------------------------------------------------------------

fn generate_web_clickstreams(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = web_clickstreams_schema();
    let total = super::scaled(scale, 4_000_000.0);
    let total = total.max(1);
    // Use a realistic number of items / web pages / users tied to scale factor.
    // TPC-DS SF1 has ~18,000 items, ~2,040 web pages, ~100,000 customers.
    let num_items = ((scale * 18_000.0) as i32).max(1);
    let num_web_pages = ((scale * 2_040.0) as i32).max(1);
    let num_users = ((scale * 100_000.0) as i32).max(1);
    let num_date_sks = ((scale * 73_049.0) as i32).max(1); // TPC-DS date_dim rows at SF1
    let num_time_sks = 86_400i32; // seconds in a day
    let num_sales = ((scale * 720_000.0) as i64).max(1); // web_sales rows at SF1

    let mut rng = StdRng::seed_from_u64(seed_for_table("web_clickstreams"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    // q07 (product affinity) pairs items co-viewed in one (user, date)
    // session. Drawing user and date independently per row makes every
    // (user, date) unique, so no pair ever recurs and the HAVING is empty.
    // Group consecutive rows into sessions that share one (user, date);
    // items stay per-row so a session is a basket of co-viewed items.
    let mut session_remaining = 0u32;
    let mut session_user: Option<i32> = None;
    let mut session_date: i32 = 1;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);

        let mut wcs_click_date_sk: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut wcs_click_time_sk: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut wcs_sales_sk: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut wcs_item_sk: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut wcs_web_page_sk: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut wcs_user_sk: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut wcs_referrer_url: Vec<Option<String>> = Vec::with_capacity(n);
        let mut wcs_search_keywords: Vec<Option<String>> = Vec::with_capacity(n);

        for _ in 0..n {
            if session_remaining == 0 {
                // A session is 2..=6 clicks under one (user, date). The user
                // is null at the same ~30% rate as before (logged-out clicks).
                session_remaining = rng.gen_range(2..=6);
                session_user = if rng.gen_bool(0.70) {
                    Some(rng.gen_range(1..=num_users))
                } else {
                    None
                };
                session_date = rng.gen_range(1..=num_date_sks);
            }
            session_remaining -= 1;

            // ~5% of clicks have nulls in optional FK columns (abandoned sessions)
            let is_sale = rng.gen_bool(0.60);
            let has_keyword = rng.gen_bool(0.30);

            wcs_click_date_sk.push(Some(session_date));
            wcs_click_time_sk.push(Some(rng.gen_range(0..num_time_sks)));
            wcs_sales_sk.push(if is_sale {
                Some(rng.gen_range(1..=num_sales))
            } else {
                None
            });
            wcs_item_sk.push(Some(rng.gen_range(1..=num_items)));
            wcs_web_page_sk.push(Some(rng.gen_range(1..=num_web_pages)));
            wcs_user_sk.push(session_user);
            wcs_referrer_url.push(Some(
                REFERRER_DOMAINS[rng.gen_range(0..REFERRER_DOMAINS.len())].to_string(),
            ));
            wcs_search_keywords.push(if has_keyword {
                Some(SEARCH_KEYWORDS[rng.gen_range(0..SEARCH_KEYWORDS.len())].to_string())
            } else {
                None
            });
        }

        let ref_refs: Vec<Option<&str>> = wcs_referrer_url.iter().map(|o| o.as_deref()).collect();
        let kw_refs: Vec<Option<&str>> = wcs_search_keywords.iter().map(|o| o.as_deref()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(wcs_click_date_sk)),
                    Arc::new(Int32Array::from(wcs_click_time_sk)),
                    Arc::new(Int64Array::from(wcs_sales_sk)),
                    Arc::new(Int32Array::from(wcs_item_sk)),
                    Arc::new(Int32Array::from(wcs_web_page_sk)),
                    Arc::new(Int32Array::from(wcs_user_sk)),
                    Arc::new(StringArray::from(ref_refs)),
                    Arc::new(StringArray::from(kw_refs)),
                ],
            )
            .expect("web_clickstreams batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_product_reviews(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = product_reviews_schema();
    let total = super::scaled(scale, 100_000.0);
    let total = total.max(1);
    let num_items = ((scale * 18_000.0) as i32).max(1);
    let num_users = ((scale * 100_000.0) as i32).max(1);
    let num_orders = ((scale * 720_000.0) as i64).max(1);

    let mut rng = StdRng::seed_from_u64(seed_for_table("product_reviews"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);

        let mut pr_review_sk: Vec<i64> = Vec::with_capacity(n);
        let mut pr_review_date: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut pr_review_time: Vec<Option<String>> = Vec::with_capacity(n);
        let mut pr_review_rating: Vec<i32> = Vec::with_capacity(n);
        let mut pr_item_sk: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut pr_user_sk: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut pr_order_sk: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut pr_review_content: Vec<Option<String>> = Vec::with_capacity(n);
        let mut pr_title: Vec<Option<String>> = Vec::with_capacity(n);

        for i in 0..n {
            let sk = (offset + i + 1) as i64;
            let has_order = rng.gen_bool(0.85);

            pr_review_sk.push(sk);
            pr_review_date.push(Some(random_date(&mut rng)));
            pr_review_time.push(Some(random_time_str(&mut rng)));
            pr_review_rating.push(rng.gen_range(1..=5i32));
            pr_item_sk.push(Some(rng.gen_range(1..=num_items)));
            pr_user_sk.push(Some(rng.gen_range(1..=num_users)));
            pr_order_sk.push(if has_order {
                Some(rng.gen_range(1..=num_orders))
            } else {
                None
            });
            pr_review_content.push(Some(random_sentence(&mut rng)));
            pr_title.push(Some(
                REVIEW_TITLES[rng.gen_range(0..REVIEW_TITLES.len())].to_string(),
            ));
        }

        let time_refs: Vec<Option<&str>> = pr_review_time.iter().map(|o| o.as_deref()).collect();
        let content_refs: Vec<Option<&str>> =
            pr_review_content.iter().map(|o| o.as_deref()).collect();
        let title_refs: Vec<Option<&str>> = pr_title.iter().map(|o| o.as_deref()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(pr_review_sk)),
                    Arc::new(Date32Array::from(pr_review_date)),
                    Arc::new(StringArray::from(time_refs)),
                    Arc::new(Int32Array::from(pr_review_rating)),
                    Arc::new(Int32Array::from(pr_item_sk)),
                    Arc::new(Int32Array::from(pr_user_sk)),
                    Arc::new(Int64Array::from(pr_order_sk)),
                    Arc::new(StringArray::from(content_refs)),
                    Arc::new(StringArray::from(title_refs)),
                ],
            )
            .expect("product_reviews batch"),
        );
        offset += n;
    }

    (schema, batches)
}

// ---------------------------------------------------------------------------
// BenchmarkGenerator impl
// ---------------------------------------------------------------------------

impl BenchmarkGenerator for TpcbbGenerator {
    fn name(&self) -> &str {
        "tpcbb"
    }

    fn tables(&self) -> Vec<TableDef> {
        // NOTE: TPC-BB also requires all TPC-DS tables (store_sales, store_returns,
        // item, customer, date_dim, web_sales, catalog_sales, …).  Those are
        // generated separately via `sqe-bench generate tpcds`.  This generator
        // only produces the two TPC-BB–specific additional tables.
        vec![
            TableDef {
                name: "web_clickstreams".into(),
                schema: web_clickstreams_schema(),
                row_count: |sf| (sf * 4_000_000.0) as usize,
            },
            TableDef {
                name: "product_reviews".into(),
                schema: product_reviews_schema(),
                row_count: |sf| (sf * 100_000.0) as usize,
            },
        ]
    }

    fn generate_table(
        &self,
        table: &str,
        scale: f64,
        output_dir: &str,
        _config: &super::GenerateConfig,
    ) -> anyhow::Result<GenerateStats> {
        let start = std::time::Instant::now();

        let (schema, batches) = match table {
            "web_clickstreams" => generate_web_clickstreams(scale),
            "product_reviews" => generate_product_reviews(scale),
            _ => anyhow::bail!(
                "Unknown TPC-BB table: {table}. \
                 TPC-DS tables (store_sales, item, customer, …) must be generated \
                 with `sqe-bench generate tpcds`."
            ),
        };

        let full_output = format!("{output_dir}/tpcbb/sf{scale}");
        let (files, bytes) =
            parquet_writer::write_parquet_files(&batches, schema, &full_output, table)?;
        let rows = batches.iter().map(|b| b.num_rows()).sum();

        Ok(GenerateStats {
            table: table.to_string(),
            rows,
            bytes: bytes as usize,
            files,
            duration: start.elapsed(),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::BenchmarkGenerator;

    #[test]
    fn test_tables_list() {
        let gen = TpcbbGenerator;
        let tables = gen.tables();
        assert_eq!(tables.len(), 2);
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"web_clickstreams"));
        assert!(names.contains(&"product_reviews"));
    }

    #[test]
    fn test_row_counts_sf01() {
        let gen = TpcbbGenerator;
        let sf = 0.01_f64;
        for t in gen.tables() {
            let expected = (t.row_count)(sf);
            match t.name.as_str() {
                "web_clickstreams" => assert_eq!(expected, 40_000),
                "product_reviews" => assert_eq!(expected, 1_000),
                _ => {}
            }
        }
    }

    #[test]
    fn test_web_clickstreams_schema() {
        let schema = web_clickstreams_schema();
        assert_eq!(schema.fields().len(), 8);
        assert_eq!(schema.field(0).name(), "wcs_click_date_sk");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(2).name(), "wcs_sales_sk");
        assert_eq!(schema.field(2).data_type(), &DataType::Int64);
        assert_eq!(schema.field(6).name(), "wcs_referrer_url");
        assert_eq!(schema.field(6).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_product_reviews_schema() {
        let schema = product_reviews_schema();
        assert_eq!(schema.fields().len(), 9);
        assert_eq!(schema.field(0).name(), "pr_review_sk");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).name(), "pr_review_date");
        assert_eq!(schema.field(1).data_type(), &DataType::Date32);
        assert_eq!(schema.field(3).name(), "pr_review_rating");
        assert_eq!(schema.field(3).data_type(), &DataType::Int32);
    }

    #[test]
    fn test_generate_web_clickstreams_sf001() {
        let (schema, batches) = generate_web_clickstreams(0.01);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 40_000);
        assert_eq!(batches[0].schema(), schema);
        // Verify nulls are present in wcs_sales_sk (col 2) — not every click is a sale
        let first_batch = &batches[0];
        let sales_col = first_batch.column(2);
        assert!(
            sales_col.null_count() > 0,
            "expected some null sales_sk values"
        );
    }

    #[test]
    fn test_web_clickstreams_is_sessionized() {
        // q07 needs (user, date) sessions to repeat items, so distinct
        // (user, date) combos must sit well below the non-null-user row
        // count rather than tracking it one-to-one.
        use arrow_array::Array;
        use std::collections::HashSet;
        let (_schema, batches) = generate_web_clickstreams(0.01);
        let mut combos: HashSet<(i32, i32)> = HashSet::new();
        let mut non_null_rows = 0usize;
        for b in &batches {
            let dates = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let users = b.column(5).as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..b.num_rows() {
                if users.is_valid(i) {
                    non_null_rows += 1;
                    combos.insert((users.value(i), dates.value(i)));
                }
            }
        }
        assert!(non_null_rows > 0, "expected some logged-in rows");
        assert!(
            (combos.len() as f64) < 0.6 * non_null_rows as f64,
            "distinct (user, date) combos {} not below 0.6x non-null rows {}",
            combos.len(),
            non_null_rows
        );
    }

    #[test]
    fn test_generate_product_reviews_sf001() {
        let (schema, batches) = generate_product_reviews(0.01);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1_000);
        assert_eq!(batches[0].schema(), schema);
        // Ratings must be 1–5
        use arrow_array::Int32Array;
        let first_batch = &batches[0];
        let rating_col = first_batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..rating_col.len() {
            let v = rating_col.value(i);
            assert!((1..=5).contains(&v), "rating {v} out of range");
        }
    }

    #[test]
    fn test_generate_table_to_parquet() {
        let gen = TpcbbGenerator;
        let output = "/tmp/sqe-bench-test-tpcbb-parquet";

        let stats = gen
            .generate_table("web_clickstreams", 0.001, output, &Default::default())
            .unwrap();
        assert_eq!(stats.rows, 4_000);
        assert_eq!(stats.files, 1);

        let stats = gen
            .generate_table("product_reviews", 0.001, output, &Default::default())
            .unwrap();
        assert_eq!(stats.rows, 100);
        assert_eq!(stats.files, 1);
    }

    #[test]
    fn test_unknown_table_errors() {
        let gen = TpcbbGenerator;
        let result = gen.generate_table("store_sales", 1.0, "/tmp", &Default::default());
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("tpcds"), "error should mention tpcds");
    }
}
