use std::sync::Arc;

use arrow_array::{Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{parquet_writer, BenchmarkGenerator, GenerateStats, TableDef};

pub struct TpceGenerator;

// ---------------------------------------------------------------------------
// Seed derivation (same algorithm as TPC-H generator)
// ---------------------------------------------------------------------------

fn seed_for_table(name: &str) -> u64 {
    name.bytes()
        .enumerate()
        .fold(0u64, |acc, (i, b)| {
            acc ^ ((b as u64).wrapping_shl(i as u32 % 64))
        })
        .wrapping_add(0xC0DE_CAFE_DEAD_BEEF)
}

// ---------------------------------------------------------------------------
// Dimension-table cardinality floor
// ---------------------------------------------------------------------------
//
// Several TPC-E reference dimensions scale with SF at very low rates:
// customer_account (5×SF), broker (10×SF), company (5×SF), security (6.85×SF).
// At SF < 0.2 the raw formulas collapse each to a single row, which makes
// every composite foreign-key join between fact tables degenerate into a
// cross product. The canonical example is `trade × holding × holding_summary`
// in `trade_result.sql`: at SF0.1 the query produced ~270M rows against
// 1728 input trades because (ca_id, s_symb) had one value on every side.
//
// `DIM_MIN` floors both the dimension-table row count and the matching FK
// sampling range used by consumer tables. The floor keeps small-scale runs
// relationally meaningful without changing behaviour at SF ≥ 2.

const DIM_MIN: usize = 10;

/// Floored cardinality for a dimension table or its FK-sampling pool.
fn dim_card_usize(scale: f64, base: f64) -> usize {
    ((scale * base) as usize).max(DIM_MIN)
}

/// Same as [`dim_card_usize`] but typed for `rng.gen_range(1..=n)` callers.
fn dim_card_i64(scale: f64, base: f64) -> i64 {
    dim_card_usize(scale, base) as i64
}

// ---------------------------------------------------------------------------
// Date helpers
// ---------------------------------------------------------------------------

// TPC-E date range: 2000-01-01 to 2005-12-31
const DATE_START: i32 = 10957; // days since 1970-01-01 to 2000-01-01
const DATE_RANGE: i32 = 2192; // ~6 years in days

fn random_date(rng: &mut StdRng) -> i32 {
    DATE_START + rng.gen_range(0..DATE_RANGE)
}

// ---------------------------------------------------------------------------
// Random string helpers
// ---------------------------------------------------------------------------

const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

fn random_word(rng: &mut StdRng, len: usize) -> String {
    (0..len)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

fn random_name(rng: &mut StdRng) -> String {
    let len = rng.gen_range(4..12usize);
    let mut s = random_word(rng, len);
    if let Some(c) = s.get_mut(0..1) {
        c.make_ascii_uppercase();
    }
    s
}

fn random_email(rng: &mut StdRng, id: i64) -> String {
    let domain = ["example.com", "mail.net", "broker.org", "finance.io"];
    format!("user{}@{}", id, domain[rng.gen_range(0..domain.len())])
}

fn random_phone(rng: &mut StdRng) -> String {
    format!(
        "{:03}-{:03}-{:04}",
        rng.gen_range(200..999u32),
        rng.gen_range(100..999u32),
        rng.gen_range(1000..9999u32),
    )
}

fn random_text(rng: &mut StdRng, min_words: usize, max_words: usize) -> String {
    let words = rng.gen_range(min_words..=max_words);
    let mut parts = Vec::with_capacity(words);
    for _ in 0..words {
        let len = rng.gen_range(3..10usize);
        parts.push(random_word(rng, len));
    }
    parts.join(" ")
}

// ---------------------------------------------------------------------------
// Batch size
// ---------------------------------------------------------------------------

const BATCH_SIZE: usize = 10_000;

// ---------------------------------------------------------------------------
// Schema definitions
// ---------------------------------------------------------------------------

// Customer domain

fn customer_account_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ca_id", DataType::Int64, false),
        Field::new("ca_b_id", DataType::Int64, false),
        Field::new("ca_c_id", DataType::Int64, false),
        Field::new("ca_name", DataType::Utf8, false),
        Field::new("ca_tax_st", DataType::Int32, false),
        Field::new("ca_bal", DataType::Float64, false),
    ]))
}

fn customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c_id", DataType::Int64, false),
        Field::new("c_tax_id", DataType::Utf8, false),
        Field::new("c_st_id", DataType::Utf8, false),
        Field::new("c_l_name", DataType::Utf8, false),
        Field::new("c_f_name", DataType::Utf8, false),
        Field::new("c_m_name", DataType::Utf8, false),
        Field::new("c_gndr", DataType::Utf8, false),
        Field::new("c_tier", DataType::Int32, false),
        Field::new("c_dob", DataType::Date32, false),
        Field::new("c_ad_id", DataType::Int64, false),
        Field::new("c_ctry_1", DataType::Utf8, false),
        Field::new("c_area_1", DataType::Utf8, false),
        Field::new("c_local_1", DataType::Utf8, false),
        Field::new("c_ext_1", DataType::Utf8, false),
        Field::new("c_email_1", DataType::Utf8, false),
        Field::new("c_email_2", DataType::Utf8, false),
    ]))
}

fn customer_taxrate_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("cx_tx_id", DataType::Utf8, false),
        Field::new("cx_c_id", DataType::Int64, false),
    ]))
}

fn account_permission_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ap_ca_id", DataType::Int64, false),
        Field::new("ap_acl", DataType::Utf8, false),
        Field::new("ap_tax_id", DataType::Utf8, false),
        Field::new("ap_l_name", DataType::Utf8, false),
        Field::new("ap_f_name", DataType::Utf8, false),
    ]))
}

fn holding_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("h_t_id", DataType::Int64, false),
        Field::new("h_ca_id", DataType::Int64, false),
        Field::new("h_s_symb", DataType::Utf8, false),
        Field::new("h_dts", DataType::Date32, false),
        Field::new("h_price", DataType::Float64, false),
        Field::new("h_qty", DataType::Int32, false),
    ]))
}

fn holding_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("hh_h_t_id", DataType::Int64, false),
        Field::new("hh_t_id", DataType::Int64, false),
        Field::new("hh_before_qty", DataType::Int32, false),
        Field::new("hh_after_qty", DataType::Int32, false),
    ]))
}

fn holding_summary_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("hs_ca_id", DataType::Int64, false),
        Field::new("hs_s_symb", DataType::Utf8, false),
        Field::new("hs_qty", DataType::Int32, false),
    ]))
}

fn watch_item_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("wi_wl_id", DataType::Int64, false),
        Field::new("wi_s_symb", DataType::Utf8, false),
    ]))
}

fn watch_list_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("wl_id", DataType::Int64, false),
        Field::new("wl_c_id", DataType::Int64, false),
    ]))
}

// Broker domain

fn broker_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("b_id", DataType::Int64, false),
        Field::new("b_st_id", DataType::Utf8, false),
        Field::new("b_name", DataType::Utf8, false),
        Field::new("b_num_trades", DataType::Int32, false),
        Field::new("b_comm_total", DataType::Float64, false),
    ]))
}

// Market domain

fn trade_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("t_id", DataType::Int64, false),
        Field::new("t_dts", DataType::Date32, false),
        Field::new("t_st_id", DataType::Utf8, false),
        Field::new("t_tt_id", DataType::Utf8, false),
        Field::new("t_is_cash", DataType::Int32, false),
        Field::new("t_s_symb", DataType::Utf8, false),
        Field::new("t_qty", DataType::Int32, false),
        Field::new("t_bid_price", DataType::Float64, false),
        Field::new("t_ca_id", DataType::Int64, false),
        Field::new("t_exec_name", DataType::Utf8, false),
        Field::new("t_trade_price", DataType::Float64, false),
        Field::new("t_chrg", DataType::Float64, false),
        Field::new("t_comm", DataType::Float64, false),
        Field::new("t_tax", DataType::Float64, false),
        Field::new("t_lifo", DataType::Int32, false),
    ]))
}

fn trade_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("th_t_id", DataType::Int64, false),
        Field::new("th_dts", DataType::Date32, false),
        Field::new("th_st_id", DataType::Utf8, false),
    ]))
}

fn trade_request_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tr_t_id", DataType::Int64, false),
        Field::new("tr_tt_id", DataType::Utf8, false),
        Field::new("tr_s_symb", DataType::Utf8, false),
        Field::new("tr_qty", DataType::Int32, false),
        Field::new("tr_bid_price", DataType::Float64, false),
        Field::new("tr_b_id", DataType::Int64, false),
    ]))
}

fn trade_type_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tt_id", DataType::Utf8, false),
        Field::new("tt_name", DataType::Utf8, false),
        Field::new("tt_is_sell", DataType::Int32, false),
        Field::new("tt_is_mrkt", DataType::Int32, false),
    ]))
}

fn settlement_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("se_t_id", DataType::Int64, false),
        Field::new("se_cash_type", DataType::Utf8, false),
        Field::new("se_cash_due_date", DataType::Date32, false),
        Field::new("se_amt", DataType::Float64, false),
    ]))
}

fn cash_transaction_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ct_t_id", DataType::Int64, false),
        Field::new("ct_dts", DataType::Date32, false),
        Field::new("ct_amt", DataType::Float64, false),
        Field::new("ct_name", DataType::Utf8, false),
    ]))
}

fn commission_rate_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("cr_c_tier", DataType::Int32, false),
        Field::new("cr_tt_id", DataType::Utf8, false),
        Field::new("cr_ex_id", DataType::Utf8, false),
        Field::new("cr_from_qty", DataType::Int32, false),
        Field::new("cr_to_qty", DataType::Int32, false),
        Field::new("cr_rate", DataType::Float64, false),
    ]))
}

// Company domain

fn company_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("co_id", DataType::Int64, false),
        Field::new("co_st_id", DataType::Utf8, false),
        Field::new("co_name", DataType::Utf8, false),
        Field::new("co_in_id", DataType::Utf8, false),
        Field::new("co_sp_rate", DataType::Utf8, false),
        Field::new("co_ceo", DataType::Utf8, false),
        Field::new("co_ad_id", DataType::Int64, false),
        Field::new("co_desc", DataType::Utf8, false),
        Field::new("co_open_date", DataType::Date32, false),
    ]))
}

fn company_competitor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("cp_co_id", DataType::Int64, false),
        Field::new("cp_comp_co_id", DataType::Int64, false),
        Field::new("cp_in_id", DataType::Utf8, false),
    ]))
}

fn security_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("s_symb", DataType::Utf8, false),
        Field::new("s_issue", DataType::Utf8, false),
        Field::new("s_st_id", DataType::Utf8, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_ex_id", DataType::Utf8, false),
        Field::new("s_co_id", DataType::Int64, false),
        Field::new("s_num_out", DataType::Int64, false),
        Field::new("s_start_date", DataType::Date32, false),
        Field::new("s_exch_date", DataType::Date32, false),
        Field::new("s_pe", DataType::Float64, false),
        Field::new("s_52wk_high", DataType::Float64, false),
        Field::new("s_52wk_high_date", DataType::Date32, false),
        Field::new("s_52wk_low", DataType::Float64, false),
        Field::new("s_52wk_low_date", DataType::Date32, false),
        Field::new("s_dividend", DataType::Float64, false),
        Field::new("s_yield", DataType::Float64, false),
    ]))
}

fn daily_market_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("dm_date", DataType::Date32, false),
        Field::new("dm_s_symb", DataType::Utf8, false),
        Field::new("dm_close", DataType::Float64, false),
        Field::new("dm_high", DataType::Float64, false),
        Field::new("dm_low", DataType::Float64, false),
        Field::new("dm_vol", DataType::Int64, false),
    ]))
}

fn financial_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("fi_co_id", DataType::Int64, false),
        Field::new("fi_year", DataType::Int32, false),
        Field::new("fi_qtr", DataType::Int32, false),
        Field::new("fi_qtr_start_date", DataType::Date32, false),
        Field::new("fi_revenue", DataType::Float64, false),
        Field::new("fi_net_earn", DataType::Float64, false),
        Field::new("fi_basic_eps", DataType::Float64, false),
        Field::new("fi_dilut_eps", DataType::Float64, false),
        Field::new("fi_margin", DataType::Float64, false),
        Field::new("fi_inventory", DataType::Float64, false),
        Field::new("fi_assets", DataType::Float64, false),
        Field::new("fi_liability", DataType::Float64, false),
        Field::new("fi_out_basic", DataType::Int64, false),
        Field::new("fi_out_dilut", DataType::Int64, false),
    ]))
}

fn last_trade_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("lt_s_symb", DataType::Utf8, false),
        Field::new("lt_dts", DataType::Date32, false),
        Field::new("lt_price", DataType::Float64, false),
        Field::new("lt_open_price", DataType::Float64, false),
        Field::new("lt_vol", DataType::Int64, false),
    ]))
}

fn news_item_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ni_id", DataType::Int64, false),
        Field::new("ni_headline", DataType::Utf8, false),
        Field::new("ni_summary", DataType::Utf8, false),
        Field::new("ni_item", DataType::Utf8, false),
        Field::new("ni_dts", DataType::Date32, false),
        Field::new("ni_source", DataType::Utf8, false),
        Field::new("ni_author", DataType::Utf8, false),
    ]))
}

fn news_xref_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("nx_ni_id", DataType::Int64, false),
        Field::new("nx_co_id", DataType::Int64, false),
    ]))
}

// Reference tables

fn address_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ad_id", DataType::Int64, false),
        Field::new("ad_line1", DataType::Utf8, false),
        Field::new("ad_line2", DataType::Utf8, false),
        Field::new("ad_zc_code", DataType::Utf8, false),
        Field::new("ad_ctry", DataType::Utf8, false),
    ]))
}

fn zip_code_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("zc_code", DataType::Utf8, false),
        Field::new("zc_town", DataType::Utf8, false),
        Field::new("zc_div", DataType::Utf8, false),
    ]))
}

fn status_type_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("st_id", DataType::Utf8, false),
        Field::new("st_name", DataType::Utf8, false),
    ]))
}

fn taxrate_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tx_id", DataType::Utf8, false),
        Field::new("tx_name", DataType::Utf8, false),
        Field::new("tx_rate", DataType::Float64, false),
    ]))
}

fn exchange_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ex_id", DataType::Utf8, false),
        Field::new("ex_name", DataType::Utf8, false),
        Field::new("ex_num_symb", DataType::Int32, false),
        Field::new("ex_open", DataType::Int32, false),
        Field::new("ex_close", DataType::Int32, false),
        Field::new("ex_desc", DataType::Utf8, false),
        Field::new("ex_ad_id", DataType::Int64, false),
    ]))
}

fn industry_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("in_id", DataType::Utf8, false),
        Field::new("in_name", DataType::Utf8, false),
        Field::new("in_sc_id", DataType::Utf8, false),
    ]))
}

fn sector_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sc_id", DataType::Utf8, false),
        Field::new("sc_name", DataType::Utf8, false),
    ]))
}

fn charge_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ch_tt_id", DataType::Utf8, false),
        Field::new("ch_chrg", DataType::Float64, false),
    ]))
}

// ---------------------------------------------------------------------------
// Fixed reference data
// ---------------------------------------------------------------------------

const STATUS_TYPES: &[(&str, &str)] = &[
    ("ACTV", "Active"),
    ("CMPT", "Completed"),
    ("CNCL", "Cancelled"),
    ("PNDG", "Pending"),
    ("SBMT", "Submitted"),
];

const TRADE_TYPES: &[(&str, &str, i32, i32)] = &[
    ("TMB", "Market Buy", 0, 1),
    ("TMS", "Market Sell", 1, 1),
    ("TSL", "Stop Loss", 1, 0),
    ("TLB", "Limit Buy", 0, 0),
    ("TLS", "Limit Sell", 1, 0),
];

const EXCHANGES: &[(&str, &str, i32, i32, i32, &str)] = &[
    (
        "NYSE",
        "New York Stock Exchange",
        1366,
        930,
        1600,
        "Primary US equity market",
    ),
    (
        "NASDAQ",
        "NASDAQ Stock Market",
        3286,
        930,
        1600,
        "Electronic equity market",
    ),
    (
        "AMEX",
        "American Stock Exchange",
        712,
        930,
        1600,
        "Equities and ETFs",
    ),
    (
        "NYSE_MKT",
        "NYSE MKT LLC",
        480,
        930,
        1600,
        "Small and mid cap equities",
    ),
];

const SECTORS: &[(&str, &str)] = &[
    ("SC0001", "Basic Materials"),
    ("SC0002", "Consumer Cyclical"),
    ("SC0003", "Financial Services"),
    ("SC0004", "Real Estate"),
    ("SC0005", "Consumer Defensive"),
    ("SC0006", "Healthcare"),
    ("SC0007", "Utilities"),
    ("SC0008", "Communication Services"),
    ("SC0009", "Energy"),
    ("SC0010", "Industrials"),
    ("SC0011", "Technology"),
    ("SC0012", "Transportation"),
];

const SP_RATES: &[&str] = &[
    "AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BB",
];

const CASH_TYPES: &[&str] = &["Cash", "Margin"];

const GENDERS: &[&str] = &["M", "F", "U"];

const COUNTRIES: &[&str] = &["US", "CA", "GB", "DE", "FR", "JP", "AU", "CH", "NL", "SE"];

const AREA_CODES: &[&str] = &[
    "212", "415", "312", "713", "617", "305", "404", "206", "303", "702",
];

// Generate a stock symbol from an index
fn symb_for_idx(idx: usize) -> String {
    // Produce symbols like AAAA, AAAB, ..., ZZZZ
    let mut n = idx;
    let mut chars = [b'A'; 4];
    for c in chars.iter_mut().rev() {
        *c = b'A' + (n % 26) as u8;
        n /= 26;
    }
    String::from_utf8(chars.to_vec()).unwrap_or_else(|_| format!("S{idx:04}"))
}

// Generate a tax ID string
fn tax_id_for(id: i64) -> String {
    format!("{:03}-{:02}-{:04}", id % 999, (id / 1000) % 99, id % 9999)
}

// ---------------------------------------------------------------------------
// Fixed table generators
// ---------------------------------------------------------------------------

fn generate_status_type() -> (SchemaRef, Vec<RecordBatch>) {
    let schema = status_type_schema();
    let ids: Vec<&str> = STATUS_TYPES.iter().map(|r| r.0).collect();
    let names: Vec<&str> = STATUS_TYPES.iter().map(|r| r.1).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("status_type batch");
    (schema, vec![batch])
}

fn generate_trade_type() -> (SchemaRef, Vec<RecordBatch>) {
    let schema = trade_type_schema();
    let ids: Vec<&str> = TRADE_TYPES.iter().map(|r| r.0).collect();
    let names: Vec<&str> = TRADE_TYPES.iter().map(|r| r.1).collect();
    let is_sell: Vec<i32> = TRADE_TYPES.iter().map(|r| r.2).collect();
    let is_mrkt: Vec<i32> = TRADE_TYPES.iter().map(|r| r.3).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(is_sell)),
            Arc::new(Int32Array::from(is_mrkt)),
        ],
    )
    .expect("trade_type batch");
    (schema, vec![batch])
}

fn generate_exchange() -> (SchemaRef, Vec<RecordBatch>) {
    let schema = exchange_schema();
    let ex_ids: Vec<&str> = EXCHANGES.iter().map(|r| r.0).collect();
    let ex_names: Vec<&str> = EXCHANGES.iter().map(|r| r.1).collect();
    let ex_num_symb: Vec<i32> = EXCHANGES.iter().map(|r| r.2).collect();
    let ex_open: Vec<i32> = EXCHANGES.iter().map(|r| r.3).collect();
    let ex_close: Vec<i32> = EXCHANGES.iter().map(|r| r.4).collect();
    let ex_desc: Vec<&str> = EXCHANGES.iter().map(|r| r.5).collect();
    // ad_id: synthetic address IDs 1..=4
    let ex_ad_id: Vec<i64> = (1..=4i64).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ex_ids)),
            Arc::new(StringArray::from(ex_names)),
            Arc::new(Int32Array::from(ex_num_symb)),
            Arc::new(Int32Array::from(ex_open)),
            Arc::new(Int32Array::from(ex_close)),
            Arc::new(StringArray::from(ex_desc)),
            Arc::new(Int64Array::from(ex_ad_id)),
        ],
    )
    .expect("exchange batch");
    (schema, vec![batch])
}

fn generate_sector() -> (SchemaRef, Vec<RecordBatch>) {
    let schema = sector_schema();
    let ids: Vec<&str> = SECTORS.iter().map(|r| r.0).collect();
    let names: Vec<&str> = SECTORS.iter().map(|r| r.1).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("sector batch");
    (schema, vec![batch])
}

fn generate_taxrate() -> (SchemaRef, Vec<RecordBatch>) {
    // 320 fixed taxrate rows: 64 jurisdictions × 5 rate tiers
    let schema = taxrate_schema();
    let total = 320usize;
    let mut rng = StdRng::seed_from_u64(seed_for_table("taxrate"));

    let mut tx_id = Vec::with_capacity(total);
    let mut tx_name = Vec::with_capacity(total);
    let mut tx_rate = Vec::with_capacity(total);

    for i in 0..total {
        tx_id.push(format!("TX{i:04}"));
        tx_name.push(format!("Tax Rate {i}"));
        tx_rate.push((rng.gen_range(5..35u32) as f64) / 100.0);
    }

    let id_refs: Vec<&str> = tx_id.iter().map(|s| s.as_str()).collect();
    let name_refs: Vec<&str> = tx_name.iter().map(|s| s.as_str()).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(id_refs)),
            Arc::new(StringArray::from(name_refs)),
            Arc::new(Float64Array::from(tx_rate)),
        ],
    )
    .expect("taxrate batch");
    (schema, vec![batch])
}

fn generate_industry() -> (SchemaRef, Vec<RecordBatch>) {
    // 102 fixed industry rows: 8-9 per sector
    let schema = industry_schema();
    let total = 102usize;

    let mut in_id = Vec::with_capacity(total);
    let mut in_name = Vec::with_capacity(total);
    let mut in_sc_id = Vec::with_capacity(total);

    for i in 0..total {
        let sector_idx = i % SECTORS.len();
        in_id.push(format!("IN{i:04}"));
        in_name.push(format!("Industry {i}"));
        in_sc_id.push(SECTORS[sector_idx].0.to_string());
    }

    let id_refs: Vec<&str> = in_id.iter().map(|s| s.as_str()).collect();
    let name_refs: Vec<&str> = in_name.iter().map(|s| s.as_str()).collect();
    let sc_refs: Vec<&str> = in_sc_id.iter().map(|s| s.as_str()).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(id_refs)),
            Arc::new(StringArray::from(name_refs)),
            Arc::new(StringArray::from(sc_refs)),
        ],
    )
    .expect("industry batch");
    (schema, vec![batch])
}

fn generate_charge() -> (SchemaRef, Vec<RecordBatch>) {
    // 15 fixed charge rows: 5 trade types × 3 tiers
    let schema = charge_schema();
    let mut ch_tt_id = Vec::with_capacity(15);
    let mut ch_chrg = Vec::with_capacity(15);

    let rates = [1.99f64, 4.99, 9.99];
    for tt in TRADE_TYPES {
        for &rate in &rates {
            ch_tt_id.push(tt.0.to_string());
            ch_chrg.push(rate);
        }
    }

    let tt_refs: Vec<&str> = ch_tt_id.iter().map(|s| s.as_str()).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(tt_refs)),
            Arc::new(Float64Array::from(ch_chrg)),
        ],
    )
    .expect("charge batch");
    (schema, vec![batch])
}

fn generate_commission_rate() -> (SchemaRef, Vec<RecordBatch>) {
    // 240 fixed rows: 3 tiers × 5 trade types × 4 exchanges × 4 qty ranges
    let schema = commission_rate_schema();
    let mut rng = StdRng::seed_from_u64(seed_for_table("commission_rate"));

    let mut cr_c_tier = Vec::new();
    let mut cr_tt_id = Vec::new();
    let mut cr_ex_id = Vec::new();
    let mut cr_from_qty = Vec::new();
    let mut cr_to_qty = Vec::new();
    let mut cr_rate = Vec::new();

    let qty_ranges = [(0i32, 499i32), (500, 4999), (5000, 9999), (10000, i32::MAX)];

    for tier in 1..=3i32 {
        for tt in TRADE_TYPES {
            for ex in EXCHANGES {
                for &(from_qty, to_qty) in &qty_ranges {
                    cr_c_tier.push(tier);
                    cr_tt_id.push(tt.0.to_string());
                    cr_ex_id.push(ex.0.to_string());
                    cr_from_qty.push(from_qty);
                    cr_to_qty.push(to_qty);
                    cr_rate.push((rng.gen_range(10..500u32) as f64) / 10000.0);
                }
            }
        }
    }

    let tt_refs: Vec<&str> = cr_tt_id.iter().map(|s| s.as_str()).collect();
    let ex_refs: Vec<&str> = cr_ex_id.iter().map(|s| s.as_str()).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(cr_c_tier)),
            Arc::new(StringArray::from(tt_refs)),
            Arc::new(StringArray::from(ex_refs)),
            Arc::new(Int32Array::from(cr_from_qty)),
            Arc::new(Int32Array::from(cr_to_qty)),
            Arc::new(Float64Array::from(cr_rate)),
        ],
    )
    .expect("commission_rate batch");
    (schema, vec![batch])
}

fn generate_zip_code() -> (SchemaRef, Vec<RecordBatch>) {
    // 14,741 fixed rows (we synthesize them deterministically)
    let schema = zip_code_schema();
    let total = 14_741usize;
    let mut rng = StdRng::seed_from_u64(seed_for_table("zip_code"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut zc_code = Vec::with_capacity(n);
        let mut zc_town = Vec::with_capacity(n);
        let mut zc_div = Vec::with_capacity(n);

        for i in 0..n {
            zc_code.push(format!("{:05}", offset + i));
            zc_town.push(random_name(&mut rng));
            zc_div.push(random_name(&mut rng));
        }

        let code_refs: Vec<&str> = zc_code.iter().map(|s| s.as_str()).collect();
        let town_refs: Vec<&str> = zc_town.iter().map(|s| s.as_str()).collect();
        let div_refs: Vec<&str> = zc_div.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(code_refs)),
                    Arc::new(StringArray::from(town_refs)),
                    Arc::new(StringArray::from(div_refs)),
                ],
            )
            .expect("zip_code batch"),
        );
        offset += n;
    }

    (schema, batches)
}

// ---------------------------------------------------------------------------
// Scaled table generators
// ---------------------------------------------------------------------------

fn generate_address(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 5,500
    let schema = address_schema();
    let total = super::scaled(scale, 5_500.0);
    let total = total.max(1);
    let mut rng = StdRng::seed_from_u64(seed_for_table("address"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut ad_id = Vec::with_capacity(n);
        let mut ad_line1 = Vec::with_capacity(n);
        let mut ad_line2 = Vec::with_capacity(n);
        let mut ad_zc_code = Vec::with_capacity(n);
        let mut ad_ctry = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i + 1) as i64;
            ad_id.push(id);
            ad_line1.push(format!(
                "{} {}",
                rng.gen_range(1..9999u32),
                random_name(&mut rng)
            ));
            ad_line2.push(format!("Apt {}", rng.gen_range(1..500u32)));
            ad_zc_code.push(format!("{:05}", rng.gen_range(0..14741u32)));
            ad_ctry.push(COUNTRIES[rng.gen_range(0..COUNTRIES.len())].to_string());
        }

        let line1_refs: Vec<&str> = ad_line1.iter().map(|s| s.as_str()).collect();
        let line2_refs: Vec<&str> = ad_line2.iter().map(|s| s.as_str()).collect();
        let zc_refs: Vec<&str> = ad_zc_code.iter().map(|s| s.as_str()).collect();
        let ctry_refs: Vec<&str> = ad_ctry.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ad_id)),
                    Arc::new(StringArray::from(line1_refs)),
                    Arc::new(StringArray::from(line2_refs)),
                    Arc::new(StringArray::from(zc_refs)),
                    Arc::new(StringArray::from(ctry_refs)),
                ],
            )
            .expect("address batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_customer(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 1,000
    let schema = customer_schema();
    let total = super::scaled(scale, 1_000.0);
    let total = total.max(1);
    let num_addr = (scale * 5_500.0).max(1.0) as i64;
    let mut rng = StdRng::seed_from_u64(seed_for_table("customer"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut c_id = Vec::with_capacity(n);
        let mut c_tax_id = Vec::with_capacity(n);
        let mut c_st_id = Vec::with_capacity(n);
        let mut c_l_name = Vec::with_capacity(n);
        let mut c_f_name = Vec::with_capacity(n);
        let mut c_m_name = Vec::with_capacity(n);
        let mut c_gndr = Vec::with_capacity(n);
        let mut c_tier = Vec::with_capacity(n);
        let mut c_dob = Vec::with_capacity(n);
        let mut c_ad_id = Vec::with_capacity(n);
        let mut c_ctry_1 = Vec::with_capacity(n);
        let mut c_area_1 = Vec::with_capacity(n);
        let mut c_local_1 = Vec::with_capacity(n);
        let mut c_ext_1 = Vec::with_capacity(n);
        let mut c_email_1 = Vec::with_capacity(n);
        let mut c_email_2 = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i + 1) as i64;
            c_id.push(id);
            c_tax_id.push(tax_id_for(id));
            c_st_id.push(
                STATUS_TYPES[rng.gen_range(0..STATUS_TYPES.len())]
                    .0
                    .to_string(),
            );
            c_l_name.push(random_name(&mut rng));
            c_f_name.push(random_name(&mut rng));
            c_m_name.push(random_word(&mut rng, 1).to_uppercase());
            c_gndr.push(GENDERS[rng.gen_range(0..GENDERS.len())].to_string());
            c_tier.push(rng.gen_range(1..=3i32));
            // DOB: 1940-2000 range
            c_dob.push(DATE_START - 10957 + rng.gen_range(0..21915i32));
            c_ad_id.push(rng.gen_range(1..=num_addr));
            c_ctry_1.push(COUNTRIES[rng.gen_range(0..COUNTRIES.len())].to_string());
            c_area_1.push(AREA_CODES[rng.gen_range(0..AREA_CODES.len())].to_string());
            c_local_1.push(random_phone(&mut rng));
            c_ext_1.push(format!("{}", rng.gen_range(1000..9999u32)));
            c_email_1.push(random_email(&mut rng, id));
            c_email_2.push(random_email(&mut rng, id + 1_000_000));
        }

        let tax_refs: Vec<&str> = c_tax_id.iter().map(|s| s.as_str()).collect();
        let st_refs: Vec<&str> = c_st_id.iter().map(|s| s.as_str()).collect();
        let ln_refs: Vec<&str> = c_l_name.iter().map(|s| s.as_str()).collect();
        let fn_refs: Vec<&str> = c_f_name.iter().map(|s| s.as_str()).collect();
        let mn_refs: Vec<&str> = c_m_name.iter().map(|s| s.as_str()).collect();
        let gn_refs: Vec<&str> = c_gndr.iter().map(|s| s.as_str()).collect();
        let ctry_refs: Vec<&str> = c_ctry_1.iter().map(|s| s.as_str()).collect();
        let area_refs: Vec<&str> = c_area_1.iter().map(|s| s.as_str()).collect();
        let local_refs: Vec<&str> = c_local_1.iter().map(|s| s.as_str()).collect();
        let ext_refs: Vec<&str> = c_ext_1.iter().map(|s| s.as_str()).collect();
        let em1_refs: Vec<&str> = c_email_1.iter().map(|s| s.as_str()).collect();
        let em2_refs: Vec<&str> = c_email_2.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(c_id)),
                    Arc::new(StringArray::from(tax_refs)),
                    Arc::new(StringArray::from(st_refs)),
                    Arc::new(StringArray::from(ln_refs)),
                    Arc::new(StringArray::from(fn_refs)),
                    Arc::new(StringArray::from(mn_refs)),
                    Arc::new(StringArray::from(gn_refs)),
                    Arc::new(Int32Array::from(c_tier)),
                    Arc::new(Date32Array::from(c_dob)),
                    Arc::new(Int64Array::from(c_ad_id)),
                    Arc::new(StringArray::from(ctry_refs)),
                    Arc::new(StringArray::from(area_refs)),
                    Arc::new(StringArray::from(local_refs)),
                    Arc::new(StringArray::from(ext_refs)),
                    Arc::new(StringArray::from(em1_refs)),
                    Arc::new(StringArray::from(em2_refs)),
                ],
            )
            .expect("customer batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_customer_account(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 5 (floored to DIM_MIN — see DIM_MIN comment near top of file)
    let schema = customer_account_schema();
    let total = dim_card_usize(scale, 5.0);
    let num_customers = (scale * 1_000.0).max(1.0) as i64;
    let num_brokers = dim_card_i64(scale, 10.0);
    let mut rng = StdRng::seed_from_u64(seed_for_table("customer_account"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut ca_id = Vec::with_capacity(n);
        let mut ca_b_id = Vec::with_capacity(n);
        let mut ca_c_id = Vec::with_capacity(n);
        let mut ca_name = Vec::with_capacity(n);
        let mut ca_tax_st = Vec::with_capacity(n);
        let mut ca_bal = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i + 1) as i64;
            ca_id.push(id);
            ca_b_id.push(rng.gen_range(1..=num_brokers));
            ca_c_id.push(rng.gen_range(1..=num_customers));
            ca_name.push(format!("Account#{id:08}"));
            ca_tax_st.push(rng.gen_range(0..=2i32));
            ca_bal.push((rng.gen_range(0..1_000_000_i64) as f64) / 100.0);
        }

        let name_refs: Vec<&str> = ca_name.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ca_id)),
                    Arc::new(Int64Array::from(ca_b_id)),
                    Arc::new(Int64Array::from(ca_c_id)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(Int32Array::from(ca_tax_st)),
                    Arc::new(Float64Array::from(ca_bal)),
                ],
            )
            .expect("customer_account batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_customer_taxrate(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 2,000
    let schema = customer_taxrate_schema();
    let total = super::scaled(scale, 2_000.0);
    let total = total.max(1);
    let num_customers = (scale * 1_000.0).max(1.0) as i64;
    let num_taxrates = 320i64;
    let mut rng = StdRng::seed_from_u64(seed_for_table("customer_taxrate"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut cx_tx_id = Vec::with_capacity(n);
        let mut cx_c_id = Vec::with_capacity(n);

        for _ in 0..n {
            cx_tx_id.push(format!("TX{:04}", rng.gen_range(0..num_taxrates)));
            cx_c_id.push(rng.gen_range(1..=num_customers));
        }

        let tx_refs: Vec<&str> = cx_tx_id.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(tx_refs)),
                    Arc::new(Int64Array::from(cx_c_id)),
                ],
            )
            .expect("customer_taxrate batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_account_permission(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 5,000
    let schema = account_permission_schema();
    let total = super::scaled(scale, 5_000.0);
    let total = total.max(1);
    let num_accounts = dim_card_i64(scale, 5.0);
    let mut rng = StdRng::seed_from_u64(seed_for_table("account_permission"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut ap_ca_id = Vec::with_capacity(n);
        let mut ap_acl = Vec::with_capacity(n);
        let mut ap_tax_id = Vec::with_capacity(n);
        let mut ap_l_name = Vec::with_capacity(n);
        let mut ap_f_name = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i + 1) as i64;
            ap_ca_id.push(rng.gen_range(1..=num_accounts));
            ap_acl.push(if rng.gen_bool(0.5) { "0" } else { "1" }.to_string());
            ap_tax_id.push(tax_id_for(id));
            ap_l_name.push(random_name(&mut rng));
            ap_f_name.push(random_name(&mut rng));
        }

        let acl_refs: Vec<&str> = ap_acl.iter().map(|s| s.as_str()).collect();
        let tax_refs: Vec<&str> = ap_tax_id.iter().map(|s| s.as_str()).collect();
        let ln_refs: Vec<&str> = ap_l_name.iter().map(|s| s.as_str()).collect();
        let fn_refs: Vec<&str> = ap_f_name.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ap_ca_id)),
                    Arc::new(StringArray::from(acl_refs)),
                    Arc::new(StringArray::from(tax_refs)),
                    Arc::new(StringArray::from(ln_refs)),
                    Arc::new(StringArray::from(fn_refs)),
                ],
            )
            .expect("account_permission batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_broker(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 10 (floored to DIM_MIN — see DIM_MIN comment near top of file)
    let schema = broker_schema();
    let total = dim_card_usize(scale, 10.0);
    let mut rng = StdRng::seed_from_u64(seed_for_table("broker"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut b_id = Vec::with_capacity(n);
        let mut b_st_id = Vec::with_capacity(n);
        let mut b_name = Vec::with_capacity(n);
        let mut b_num_trades = Vec::with_capacity(n);
        let mut b_comm_total = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i + 1) as i64;
            b_id.push(id);
            b_st_id.push(
                STATUS_TYPES[rng.gen_range(0..STATUS_TYPES.len())]
                    .0
                    .to_string(),
            );
            b_name.push(format!(
                "Broker {} {}",
                random_name(&mut rng),
                random_name(&mut rng)
            ));
            b_num_trades.push(rng.gen_range(0..100_000i32));
            b_comm_total.push((rng.gen_range(0..10_000_000_i64) as f64) / 100.0);
        }

        let st_refs: Vec<&str> = b_st_id.iter().map(|s| s.as_str()).collect();
        let name_refs: Vec<&str> = b_name.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(b_id)),
                    Arc::new(StringArray::from(st_refs)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(Int32Array::from(b_num_trades)),
                    Arc::new(Float64Array::from(b_comm_total)),
                ],
            )
            .expect("broker batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_company(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 5 (floored to DIM_MIN — see DIM_MIN comment near top of file)
    let schema = company_schema();
    let total = dim_card_usize(scale, 5.0);
    let num_addr = (scale * 5_500.0).max(1.0) as i64;
    let mut rng = StdRng::seed_from_u64(seed_for_table("company"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut co_id = Vec::with_capacity(n);
        let mut co_st_id = Vec::with_capacity(n);
        let mut co_name = Vec::with_capacity(n);
        let mut co_in_id = Vec::with_capacity(n);
        let mut co_sp_rate = Vec::with_capacity(n);
        let mut co_ceo = Vec::with_capacity(n);
        let mut co_ad_id = Vec::with_capacity(n);
        let mut co_desc = Vec::with_capacity(n);
        let mut co_open_date = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i + 1) as i64;
            co_id.push(id);
            co_st_id.push(
                STATUS_TYPES[rng.gen_range(0..STATUS_TYPES.len())]
                    .0
                    .to_string(),
            );
            co_name.push(format!(
                "{} {} Corp",
                random_name(&mut rng),
                random_name(&mut rng)
            ));
            co_in_id.push(format!("IN{:04}", rng.gen_range(0..102u32)));
            co_sp_rate.push(SP_RATES[rng.gen_range(0..SP_RATES.len())].to_string());
            co_ceo.push(format!(
                "{} {}",
                random_name(&mut rng),
                random_name(&mut rng)
            ));
            co_ad_id.push(rng.gen_range(1..=num_addr));
            co_desc.push(random_text(&mut rng, 5, 15));
            co_open_date.push(random_date(&mut rng));
        }

        let st_refs: Vec<&str> = co_st_id.iter().map(|s| s.as_str()).collect();
        let name_refs: Vec<&str> = co_name.iter().map(|s| s.as_str()).collect();
        let in_refs: Vec<&str> = co_in_id.iter().map(|s| s.as_str()).collect();
        let sp_refs: Vec<&str> = co_sp_rate.iter().map(|s| s.as_str()).collect();
        let ceo_refs: Vec<&str> = co_ceo.iter().map(|s| s.as_str()).collect();
        let desc_refs: Vec<&str> = co_desc.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(co_id)),
                    Arc::new(StringArray::from(st_refs)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(StringArray::from(in_refs)),
                    Arc::new(StringArray::from(sp_refs)),
                    Arc::new(StringArray::from(ceo_refs)),
                    Arc::new(Int64Array::from(co_ad_id)),
                    Arc::new(StringArray::from(desc_refs)),
                    Arc::new(Date32Array::from(co_open_date)),
                ],
            )
            .expect("company batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_company_competitor(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 15
    let schema = company_competitor_schema();
    let total = super::scaled(scale, 15.0);
    let total = total.max(1);
    let num_co = dim_card_i64(scale, 5.0);
    let mut rng = StdRng::seed_from_u64(seed_for_table("company_competitor"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut cp_co_id = Vec::with_capacity(n);
        let mut cp_comp_co_id = Vec::with_capacity(n);
        let mut cp_in_id = Vec::with_capacity(n);

        for _ in 0..n {
            cp_co_id.push(rng.gen_range(1..=num_co));
            cp_comp_co_id.push(rng.gen_range(1..=num_co));
            cp_in_id.push(format!("IN{:04}", rng.gen_range(0..102u32)));
        }

        let in_refs: Vec<&str> = cp_in_id.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(cp_co_id)),
                    Arc::new(Int64Array::from(cp_comp_co_id)),
                    Arc::new(StringArray::from(in_refs)),
                ],
            )
            .expect("company_competitor batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_security(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 6.85 (floored to DIM_MIN — see DIM_MIN comment near top of file)
    let schema = security_schema();
    let total = dim_card_usize(scale, 6.85);
    let num_co = dim_card_i64(scale, 5.0);
    let mut rng = StdRng::seed_from_u64(seed_for_table("security"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut s_symb = Vec::with_capacity(n);
        let mut s_issue = Vec::with_capacity(n);
        let mut s_st_id = Vec::with_capacity(n);
        let mut s_name = Vec::with_capacity(n);
        let mut s_ex_id = Vec::with_capacity(n);
        let mut s_co_id = Vec::with_capacity(n);
        let mut s_num_out = Vec::with_capacity(n);
        let mut s_start_date = Vec::with_capacity(n);
        let mut s_exch_date = Vec::with_capacity(n);
        let mut s_pe = Vec::with_capacity(n);
        let mut s_52wk_high = Vec::with_capacity(n);
        let mut s_52wk_high_date = Vec::with_capacity(n);
        let mut s_52wk_low = Vec::with_capacity(n);
        let mut s_52wk_low_date = Vec::with_capacity(n);
        let mut s_dividend = Vec::with_capacity(n);
        let mut s_yield = Vec::with_capacity(n);

        for i in 0..n {
            let idx = offset + i;
            let symb = symb_for_idx(idx);
            let high =
                (rng.gen_range(50..500u32) as f64) + (rng.gen_range(0..100u32) as f64) / 100.0;
            let low = high * (0.5 + rng.gen_range(0..50u32) as f64 / 100.0);
            s_symb.push(symb);
            s_issue.push(
                if rng.gen_bool(0.8) {
                    "COMMON"
                } else {
                    "PREFERRED"
                }
                .to_string(),
            );
            s_st_id.push(
                STATUS_TYPES[rng.gen_range(0..STATUS_TYPES.len())]
                    .0
                    .to_string(),
            );
            s_name.push(format!("{} Inc", random_name(&mut rng)));
            s_ex_id.push(EXCHANGES[rng.gen_range(0..EXCHANGES.len())].0.to_string());
            s_co_id.push(rng.gen_range(1..=num_co));
            s_num_out.push(rng.gen_range(1_000_000..1_000_000_000_i64));
            s_start_date.push(random_date(&mut rng));
            s_exch_date.push(random_date(&mut rng));
            s_pe.push((rng.gen_range(5..50u32) as f64) + (rng.gen_range(0..100u32) as f64) / 100.0);
            s_52wk_high.push(high);
            s_52wk_high_date.push(random_date(&mut rng));
            s_52wk_low.push(low);
            s_52wk_low_date.push(random_date(&mut rng));
            s_dividend.push((rng.gen_range(0..500u32) as f64) / 100.0);
            s_yield.push((rng.gen_range(0..800u32) as f64) / 10000.0);
        }

        let symb_refs: Vec<&str> = s_symb.iter().map(|s| s.as_str()).collect();
        let issue_refs: Vec<&str> = s_issue.iter().map(|s| s.as_str()).collect();
        let st_refs: Vec<&str> = s_st_id.iter().map(|s| s.as_str()).collect();
        let name_refs: Vec<&str> = s_name.iter().map(|s| s.as_str()).collect();
        let ex_refs: Vec<&str> = s_ex_id.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(symb_refs)),
                    Arc::new(StringArray::from(issue_refs)),
                    Arc::new(StringArray::from(st_refs)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(StringArray::from(ex_refs)),
                    Arc::new(Int64Array::from(s_co_id)),
                    Arc::new(Int64Array::from(s_num_out)),
                    Arc::new(Date32Array::from(s_start_date)),
                    Arc::new(Date32Array::from(s_exch_date)),
                    Arc::new(Float64Array::from(s_pe)),
                    Arc::new(Float64Array::from(s_52wk_high)),
                    Arc::new(Date32Array::from(s_52wk_high_date)),
                    Arc::new(Float64Array::from(s_52wk_low)),
                    Arc::new(Date32Array::from(s_52wk_low_date)),
                    Arc::new(Float64Array::from(s_dividend)),
                    Arc::new(Float64Array::from(s_yield)),
                ],
            )
            .expect("security batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_daily_market(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 17,136
    let schema = daily_market_schema();
    let total = super::scaled(scale, 17_136.0);
    let total = total.max(1);
    let num_symb = dim_card_usize(scale, 6.85);
    let mut rng = StdRng::seed_from_u64(seed_for_table("daily_market"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut dm_date = Vec::with_capacity(n);
        let mut dm_s_symb = Vec::with_capacity(n);
        let mut dm_close = Vec::with_capacity(n);
        let mut dm_high = Vec::with_capacity(n);
        let mut dm_low = Vec::with_capacity(n);
        let mut dm_vol = Vec::with_capacity(n);

        for _ in 0..n {
            let close =
                (rng.gen_range(10..500u32) as f64) + (rng.gen_range(0..100u32) as f64) / 100.0;
            let high = close * (1.0 + rng.gen_range(0..10u32) as f64 / 100.0);
            let low = close * (1.0 - rng.gen_range(0..10u32) as f64 / 100.0);
            dm_date.push(random_date(&mut rng));
            dm_s_symb.push(symb_for_idx(rng.gen_range(0..num_symb)));
            dm_close.push(close);
            dm_high.push(high);
            dm_low.push(low);
            dm_vol.push(rng.gen_range(10_000..10_000_000_i64));
        }

        let symb_refs: Vec<&str> = dm_s_symb.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Date32Array::from(dm_date)),
                    Arc::new(StringArray::from(symb_refs)),
                    Arc::new(Float64Array::from(dm_close)),
                    Arc::new(Float64Array::from(dm_high)),
                    Arc::new(Float64Array::from(dm_low)),
                    Arc::new(Int64Array::from(dm_vol)),
                ],
            )
            .expect("daily_market batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_financial(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 100
    let schema = financial_schema();
    let total = super::scaled(scale, 100.0);
    let total = total.max(1);
    let num_co = dim_card_i64(scale, 5.0);
    let mut rng = StdRng::seed_from_u64(seed_for_table("financial"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut fi_co_id = Vec::with_capacity(n);
        let mut fi_year = Vec::with_capacity(n);
        let mut fi_qtr = Vec::with_capacity(n);
        let mut fi_qtr_start_date = Vec::with_capacity(n);
        let mut fi_revenue = Vec::with_capacity(n);
        let mut fi_net_earn = Vec::with_capacity(n);
        let mut fi_basic_eps = Vec::with_capacity(n);
        let mut fi_dilut_eps = Vec::with_capacity(n);
        let mut fi_margin = Vec::with_capacity(n);
        let mut fi_inventory = Vec::with_capacity(n);
        let mut fi_assets = Vec::with_capacity(n);
        let mut fi_liability = Vec::with_capacity(n);
        let mut fi_out_basic = Vec::with_capacity(n);
        let mut fi_out_dilut = Vec::with_capacity(n);

        for _ in 0..n {
            let rev = (rng.gen_range(1_000_000..1_000_000_000_i64) as f64) / 100.0;
            let earn = rev * (rng.gen_range(5..25u32) as f64 / 100.0);
            fi_co_id.push(rng.gen_range(1..=num_co));
            fi_year.push(rng.gen_range(2000..=2005i32));
            fi_qtr.push(rng.gen_range(1..=4i32));
            fi_qtr_start_date.push(random_date(&mut rng));
            fi_revenue.push(rev);
            fi_net_earn.push(earn);
            fi_basic_eps.push((rng.gen_range(100..2000u32) as f64) / 100.0);
            fi_dilut_eps.push((rng.gen_range(100..2000u32) as f64) / 100.0);
            fi_margin.push(earn / rev);
            fi_inventory.push((rng.gen_range(0..100_000_000_i64) as f64) / 100.0);
            fi_assets.push((rng.gen_range(10_000_000..10_000_000_000_i64) as f64) / 100.0);
            fi_liability.push((rng.gen_range(1_000_000..5_000_000_000_i64) as f64) / 100.0);
            fi_out_basic.push(rng.gen_range(1_000_000..1_000_000_000_i64));
            fi_out_dilut.push(rng.gen_range(1_000_000..1_000_000_000_i64));
        }

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(fi_co_id)),
                    Arc::new(Int32Array::from(fi_year)),
                    Arc::new(Int32Array::from(fi_qtr)),
                    Arc::new(Date32Array::from(fi_qtr_start_date)),
                    Arc::new(Float64Array::from(fi_revenue)),
                    Arc::new(Float64Array::from(fi_net_earn)),
                    Arc::new(Float64Array::from(fi_basic_eps)),
                    Arc::new(Float64Array::from(fi_dilut_eps)),
                    Arc::new(Float64Array::from(fi_margin)),
                    Arc::new(Float64Array::from(fi_inventory)),
                    Arc::new(Float64Array::from(fi_assets)),
                    Arc::new(Float64Array::from(fi_liability)),
                    Arc::new(Int64Array::from(fi_out_basic)),
                    Arc::new(Int64Array::from(fi_out_dilut)),
                ],
            )
            .expect("financial batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_last_trade(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 6.85 — one row per security (floored to DIM_MIN so it matches
    // the security table after the dim-cardinality floor is applied).
    let schema = last_trade_schema();
    let total = dim_card_usize(scale, 6.85);
    let mut rng = StdRng::seed_from_u64(seed_for_table("last_trade"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut lt_s_symb = Vec::with_capacity(n);
        let mut lt_dts = Vec::with_capacity(n);
        let mut lt_price = Vec::with_capacity(n);
        let mut lt_open_price = Vec::with_capacity(n);
        let mut lt_vol = Vec::with_capacity(n);

        for i in 0..n {
            let idx = offset + i;
            let price =
                (rng.gen_range(10..500u32) as f64) + (rng.gen_range(0..100u32) as f64) / 100.0;
            lt_s_symb.push(symb_for_idx(idx));
            lt_dts.push(random_date(&mut rng));
            lt_price.push(price);
            lt_open_price.push(price * (0.95 + rng.gen_range(0..10u32) as f64 / 100.0));
            lt_vol.push(rng.gen_range(0..10_000_000_i64));
        }

        let symb_refs: Vec<&str> = lt_s_symb.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(symb_refs)),
                    Arc::new(Date32Array::from(lt_dts)),
                    Arc::new(Float64Array::from(lt_price)),
                    Arc::new(Float64Array::from(lt_open_price)),
                    Arc::new(Int64Array::from(lt_vol)),
                ],
            )
            .expect("last_trade batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_news_item(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 100
    let schema = news_item_schema();
    let total = super::scaled(scale, 100.0);
    let total = total.max(1);
    let mut rng = StdRng::seed_from_u64(seed_for_table("news_item"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut ni_id = Vec::with_capacity(n);
        let mut ni_headline = Vec::with_capacity(n);
        let mut ni_summary = Vec::with_capacity(n);
        let mut ni_item = Vec::with_capacity(n);
        let mut ni_dts = Vec::with_capacity(n);
        let mut ni_source = Vec::with_capacity(n);
        let mut ni_author = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i + 1) as i64;
            ni_id.push(id);
            ni_headline.push(random_text(&mut rng, 4, 10));
            ni_summary.push(random_text(&mut rng, 10, 30));
            ni_item.push(random_text(&mut rng, 50, 100));
            ni_dts.push(random_date(&mut rng));
            ni_source.push(format!("Source{}", rng.gen_range(1..=20u32)));
            ni_author.push(format!(
                "{} {}",
                random_name(&mut rng),
                random_name(&mut rng)
            ));
        }

        let hl_refs: Vec<&str> = ni_headline.iter().map(|s| s.as_str()).collect();
        let sum_refs: Vec<&str> = ni_summary.iter().map(|s| s.as_str()).collect();
        let item_refs: Vec<&str> = ni_item.iter().map(|s| s.as_str()).collect();
        let src_refs: Vec<&str> = ni_source.iter().map(|s| s.as_str()).collect();
        let auth_refs: Vec<&str> = ni_author.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ni_id)),
                    Arc::new(StringArray::from(hl_refs)),
                    Arc::new(StringArray::from(sum_refs)),
                    Arc::new(StringArray::from(item_refs)),
                    Arc::new(Date32Array::from(ni_dts)),
                    Arc::new(StringArray::from(src_refs)),
                    Arc::new(StringArray::from(auth_refs)),
                ],
            )
            .expect("news_item batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_news_xref(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 100
    let schema = news_xref_schema();
    let total = super::scaled(scale, 100.0);
    let total = total.max(1);
    let num_news = (scale * 100.0).max(1.0) as i64;
    let num_co = dim_card_i64(scale, 5.0);
    let mut rng = StdRng::seed_from_u64(seed_for_table("news_xref"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut nx_ni_id = Vec::with_capacity(n);
        let mut nx_co_id = Vec::with_capacity(n);

        for _ in 0..n {
            nx_ni_id.push(rng.gen_range(1..=num_news));
            nx_co_id.push(rng.gen_range(1..=num_co));
        }

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(nx_ni_id)),
                    Arc::new(Int64Array::from(nx_co_id)),
                ],
            )
            .expect("news_xref batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_trade(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 17,280
    let schema = trade_schema();
    let total = super::scaled(scale, 17_280.0);
    let total = total.max(1);
    let num_symb = dim_card_usize(scale, 6.85);
    let num_accounts = dim_card_i64(scale, 5.0);
    let mut rng = StdRng::seed_from_u64(seed_for_table("trade"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut t_id = Vec::with_capacity(n);
        let mut t_dts = Vec::with_capacity(n);
        let mut t_st_id = Vec::with_capacity(n);
        let mut t_tt_id = Vec::with_capacity(n);
        let mut t_is_cash = Vec::with_capacity(n);
        let mut t_s_symb = Vec::with_capacity(n);
        let mut t_qty = Vec::with_capacity(n);
        let mut t_bid_price = Vec::with_capacity(n);
        let mut t_ca_id = Vec::with_capacity(n);
        let mut t_exec_name = Vec::with_capacity(n);
        let mut t_trade_price = Vec::with_capacity(n);
        let mut t_chrg = Vec::with_capacity(n);
        let mut t_comm = Vec::with_capacity(n);
        let mut t_tax = Vec::with_capacity(n);
        let mut t_lifo = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i + 1) as i64;
            let tt_idx = rng.gen_range(0..TRADE_TYPES.len());
            let price = (rng.gen_range(100..50000u32) as f64) / 100.0;
            t_id.push(id);
            t_dts.push(random_date(&mut rng));
            t_st_id.push(
                STATUS_TYPES[rng.gen_range(0..STATUS_TYPES.len())]
                    .0
                    .to_string(),
            );
            t_tt_id.push(TRADE_TYPES[tt_idx].0.to_string());
            t_is_cash.push(rng.gen_range(0..=1i32));
            t_s_symb.push(symb_for_idx(rng.gen_range(0..num_symb)));
            t_qty.push(rng.gen_range(1..10000i32));
            t_bid_price.push(price);
            t_ca_id.push(rng.gen_range(1..=num_accounts));
            t_exec_name.push(format!(
                "{} {}",
                random_name(&mut rng),
                random_name(&mut rng)
            ));
            t_trade_price.push(price * (0.99 + rng.gen_range(0..3u32) as f64 / 100.0));
            t_chrg.push((rng.gen_range(0..2000u32) as f64) / 100.0);
            t_comm.push((rng.gen_range(0..1000u32) as f64) / 100.0);
            t_tax.push((rng.gen_range(0..500u32) as f64) / 100.0);
            t_lifo.push(rng.gen_range(0..=1i32));
        }

        let st_refs: Vec<&str> = t_st_id.iter().map(|s| s.as_str()).collect();
        let tt_refs: Vec<&str> = t_tt_id.iter().map(|s| s.as_str()).collect();
        let symb_refs: Vec<&str> = t_s_symb.iter().map(|s| s.as_str()).collect();
        let exec_refs: Vec<&str> = t_exec_name.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(t_id)),
                    Arc::new(Date32Array::from(t_dts)),
                    Arc::new(StringArray::from(st_refs)),
                    Arc::new(StringArray::from(tt_refs)),
                    Arc::new(Int32Array::from(t_is_cash)),
                    Arc::new(StringArray::from(symb_refs)),
                    Arc::new(Int32Array::from(t_qty)),
                    Arc::new(Float64Array::from(t_bid_price)),
                    Arc::new(Int64Array::from(t_ca_id)),
                    Arc::new(StringArray::from(exec_refs)),
                    Arc::new(Float64Array::from(t_trade_price)),
                    Arc::new(Float64Array::from(t_chrg)),
                    Arc::new(Float64Array::from(t_comm)),
                    Arc::new(Float64Array::from(t_tax)),
                    Arc::new(Int32Array::from(t_lifo)),
                ],
            )
            .expect("trade batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_trade_history(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 51,840 (3 history rows per trade)
    let schema = trade_history_schema();
    let total = super::scaled(scale, 51_840.0);
    let total = total.max(1);
    let num_trades = (scale * 17_280.0).max(1.0) as i64;
    let mut rng = StdRng::seed_from_u64(seed_for_table("trade_history"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut th_t_id = Vec::with_capacity(n);
        let mut th_dts = Vec::with_capacity(n);
        let mut th_st_id = Vec::with_capacity(n);

        for _ in 0..n {
            th_t_id.push(rng.gen_range(1..=num_trades));
            th_dts.push(random_date(&mut rng));
            th_st_id.push(
                STATUS_TYPES[rng.gen_range(0..STATUS_TYPES.len())]
                    .0
                    .to_string(),
            );
        }

        let st_refs: Vec<&str> = th_st_id.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(th_t_id)),
                    Arc::new(Date32Array::from(th_dts)),
                    Arc::new(StringArray::from(st_refs)),
                ],
            )
            .expect("trade_history batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_trade_request(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 100
    let schema = trade_request_schema();
    let total = super::scaled(scale, 100.0);
    let total = total.max(1);
    let num_symb = dim_card_usize(scale, 6.85);
    let num_brokers = dim_card_i64(scale, 10.0);
    let num_trades = (scale * 17_280.0).max(1.0) as i64;
    let mut rng = StdRng::seed_from_u64(seed_for_table("trade_request"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut tr_t_id = Vec::with_capacity(n);
        let mut tr_tt_id = Vec::with_capacity(n);
        let mut tr_s_symb = Vec::with_capacity(n);
        let mut tr_qty = Vec::with_capacity(n);
        let mut tr_bid_price = Vec::with_capacity(n);
        let mut tr_b_id = Vec::with_capacity(n);

        for _ in 0..n {
            tr_t_id.push(rng.gen_range(1..=num_trades));
            tr_tt_id.push(
                TRADE_TYPES[rng.gen_range(0..TRADE_TYPES.len())]
                    .0
                    .to_string(),
            );
            tr_s_symb.push(symb_for_idx(rng.gen_range(0..num_symb)));
            tr_qty.push(rng.gen_range(1..10000i32));
            tr_bid_price.push((rng.gen_range(100..50000u32) as f64) / 100.0);
            tr_b_id.push(rng.gen_range(1..=num_brokers));
        }

        let tt_refs: Vec<&str> = tr_tt_id.iter().map(|s| s.as_str()).collect();
        let symb_refs: Vec<&str> = tr_s_symb.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(tr_t_id)),
                    Arc::new(StringArray::from(tt_refs)),
                    Arc::new(StringArray::from(symb_refs)),
                    Arc::new(Int32Array::from(tr_qty)),
                    Arc::new(Float64Array::from(tr_bid_price)),
                    Arc::new(Int64Array::from(tr_b_id)),
                ],
            )
            .expect("trade_request batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_settlement(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 17,280 (one per trade)
    let schema = settlement_schema();
    let total = super::scaled(scale, 17_280.0);
    let total = total.max(1);
    let num_trades = (scale * 17_280.0).max(1.0) as i64;
    let mut rng = StdRng::seed_from_u64(seed_for_table("settlement"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut se_t_id = Vec::with_capacity(n);
        let mut se_cash_type = Vec::with_capacity(n);
        let mut se_cash_due_date = Vec::with_capacity(n);
        let mut se_amt = Vec::with_capacity(n);

        for _ in 0..n {
            se_t_id.push(rng.gen_range(1..=num_trades));
            se_cash_type.push(CASH_TYPES[rng.gen_range(0..CASH_TYPES.len())].to_string());
            se_cash_due_date.push(random_date(&mut rng));
            se_amt.push((rng.gen_range(100..1_000_000_i64) as f64) / 100.0);
        }

        let ct_refs: Vec<&str> = se_cash_type.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(se_t_id)),
                    Arc::new(StringArray::from(ct_refs)),
                    Arc::new(Date32Array::from(se_cash_due_date)),
                    Arc::new(Float64Array::from(se_amt)),
                ],
            )
            .expect("settlement batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_cash_transaction(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 13,824 (80% of trades have a cash transaction)
    let schema = cash_transaction_schema();
    let total = super::scaled(scale, 13_824.0);
    let total = total.max(1);
    let num_trades = (scale * 17_280.0).max(1.0) as i64;
    let mut rng = StdRng::seed_from_u64(seed_for_table("cash_transaction"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut ct_t_id = Vec::with_capacity(n);
        let mut ct_dts = Vec::with_capacity(n);
        let mut ct_amt = Vec::with_capacity(n);
        let mut ct_name = Vec::with_capacity(n);

        for _ in 0..n {
            ct_t_id.push(rng.gen_range(1..=num_trades));
            ct_dts.push(random_date(&mut rng));
            ct_amt.push((rng.gen_range(100..1_000_000_i64) as f64) / 100.0);
            ct_name.push(format!("Transaction {}", random_text(&mut rng, 2, 5)));
        }

        let name_refs: Vec<&str> = ct_name.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ct_t_id)),
                    Arc::new(Date32Array::from(ct_dts)),
                    Arc::new(Float64Array::from(ct_amt)),
                    Arc::new(StringArray::from(name_refs)),
                ],
            )
            .expect("cash_transaction batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_holding(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 12,500
    let schema = holding_schema();
    let total = super::scaled(scale, 12_500.0);
    let total = total.max(1);
    let num_trades = (scale * 17_280.0).max(1.0) as i64;
    let num_accounts = dim_card_i64(scale, 5.0);
    let num_symb = dim_card_usize(scale, 6.85);
    let mut rng = StdRng::seed_from_u64(seed_for_table("holding"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut h_t_id = Vec::with_capacity(n);
        let mut h_ca_id = Vec::with_capacity(n);
        let mut h_s_symb = Vec::with_capacity(n);
        let mut h_dts = Vec::with_capacity(n);
        let mut h_price = Vec::with_capacity(n);
        let mut h_qty = Vec::with_capacity(n);

        for _ in 0..n {
            h_t_id.push(rng.gen_range(1..=num_trades));
            h_ca_id.push(rng.gen_range(1..=num_accounts));
            h_s_symb.push(symb_for_idx(rng.gen_range(0..num_symb)));
            h_dts.push(random_date(&mut rng));
            h_price.push((rng.gen_range(100..50000u32) as f64) / 100.0);
            h_qty.push(rng.gen_range(1..10000i32));
        }

        let symb_refs: Vec<&str> = h_s_symb.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(h_t_id)),
                    Arc::new(Int64Array::from(h_ca_id)),
                    Arc::new(StringArray::from(symb_refs)),
                    Arc::new(Date32Array::from(h_dts)),
                    Arc::new(Float64Array::from(h_price)),
                    Arc::new(Int32Array::from(h_qty)),
                ],
            )
            .expect("holding batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_holding_history(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 25,000
    let schema = holding_history_schema();
    let total = super::scaled(scale, 25_000.0);
    let total = total.max(1);
    let num_trades = (scale * 17_280.0).max(1.0) as i64;
    let mut rng = StdRng::seed_from_u64(seed_for_table("holding_history"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut hh_h_t_id = Vec::with_capacity(n);
        let mut hh_t_id = Vec::with_capacity(n);
        let mut hh_before_qty = Vec::with_capacity(n);
        let mut hh_after_qty = Vec::with_capacity(n);

        for _ in 0..n {
            let before = rng.gen_range(0..10000i32);
            let delta = rng.gen_range(-before..=10000i32 - before);
            hh_h_t_id.push(rng.gen_range(1..=num_trades));
            hh_t_id.push(rng.gen_range(1..=num_trades));
            hh_before_qty.push(before);
            hh_after_qty.push(before + delta);
        }

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(hh_h_t_id)),
                    Arc::new(Int64Array::from(hh_t_id)),
                    Arc::new(Int32Array::from(hh_before_qty)),
                    Arc::new(Int32Array::from(hh_after_qty)),
                ],
            )
            .expect("holding_history batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_holding_summary(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 5,000
    let schema = holding_summary_schema();
    let total = super::scaled(scale, 5_000.0);
    let total = total.max(1);
    let num_accounts = dim_card_i64(scale, 5.0);
    let num_symb = dim_card_usize(scale, 6.85);
    let mut rng = StdRng::seed_from_u64(seed_for_table("holding_summary"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut hs_ca_id = Vec::with_capacity(n);
        let mut hs_s_symb = Vec::with_capacity(n);
        let mut hs_qty = Vec::with_capacity(n);

        for _ in 0..n {
            hs_ca_id.push(rng.gen_range(1..=num_accounts));
            hs_s_symb.push(symb_for_idx(rng.gen_range(0..num_symb)));
            hs_qty.push(rng.gen_range(-50000..50000i32));
        }

        let symb_refs: Vec<&str> = hs_s_symb.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(hs_ca_id)),
                    Arc::new(StringArray::from(symb_refs)),
                    Arc::new(Int32Array::from(hs_qty)),
                ],
            )
            .expect("holding_summary batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_watch_list(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 5,000
    let schema = watch_list_schema();
    let total = super::scaled(scale, 5_000.0);
    let total = total.max(1);
    let num_customers = (scale * 1_000.0).max(1.0) as i64;
    let mut rng = StdRng::seed_from_u64(seed_for_table("watch_list"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut wl_id = Vec::with_capacity(n);
        let mut wl_c_id = Vec::with_capacity(n);

        for i in 0..n {
            wl_id.push((offset + i + 1) as i64);
            wl_c_id.push(rng.gen_range(1..=num_customers));
        }

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(wl_id)),
                    Arc::new(Int64Array::from(wl_c_id)),
                ],
            )
            .expect("watch_list batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_watch_item(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    // SF × 50,000
    let schema = watch_item_schema();
    let total = super::scaled(scale, 50_000.0);
    let total = total.max(1);
    let num_wl = (scale * 5_000.0).max(1.0) as i64;
    let num_symb = dim_card_usize(scale, 6.85);
    let mut rng = StdRng::seed_from_u64(seed_for_table("watch_item"));
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut wi_wl_id = Vec::with_capacity(n);
        let mut wi_s_symb = Vec::with_capacity(n);

        for _ in 0..n {
            wi_wl_id.push(rng.gen_range(1..=num_wl));
            wi_s_symb.push(symb_for_idx(rng.gen_range(0..num_symb)));
        }

        let symb_refs: Vec<&str> = wi_s_symb.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(wi_wl_id)),
                    Arc::new(StringArray::from(symb_refs)),
                ],
            )
            .expect("watch_item batch"),
        );
        offset += n;
    }

    (schema, batches)
}

// ---------------------------------------------------------------------------
// BenchmarkGenerator impl
// ---------------------------------------------------------------------------

impl BenchmarkGenerator for TpceGenerator {
    fn name(&self) -> &str {
        "tpce"
    }

    fn tables(&self) -> Vec<TableDef> {
        vec![
            // Customer domain
            TableDef {
                name: "customer_account".into(),
                schema: customer_account_schema(),
                row_count: |sf| dim_card_usize(sf, 5.0),
            },
            TableDef {
                name: "customer".into(),
                schema: customer_schema(),
                row_count: |sf| (sf * 1_000.0) as usize,
            },
            TableDef {
                name: "customer_taxrate".into(),
                schema: customer_taxrate_schema(),
                row_count: |sf| (sf * 2_000.0) as usize,
            },
            TableDef {
                name: "account_permission".into(),
                schema: account_permission_schema(),
                row_count: |sf| (sf * 5_000.0) as usize,
            },
            TableDef {
                name: "holding".into(),
                schema: holding_schema(),
                row_count: |sf| (sf * 12_500.0) as usize,
            },
            TableDef {
                name: "holding_history".into(),
                schema: holding_history_schema(),
                row_count: |sf| (sf * 25_000.0) as usize,
            },
            TableDef {
                name: "holding_summary".into(),
                schema: holding_summary_schema(),
                row_count: |sf| (sf * 5_000.0) as usize,
            },
            TableDef {
                name: "watch_item".into(),
                schema: watch_item_schema(),
                row_count: |sf| (sf * 50_000.0) as usize,
            },
            TableDef {
                name: "watch_list".into(),
                schema: watch_list_schema(),
                row_count: |sf| (sf * 5_000.0) as usize,
            },
            // Broker domain
            TableDef {
                name: "broker".into(),
                schema: broker_schema(),
                row_count: |sf| dim_card_usize(sf, 10.0),
            },
            // Market domain
            TableDef {
                name: "trade".into(),
                schema: trade_schema(),
                row_count: |sf| (sf * 17_280.0) as usize,
            },
            TableDef {
                name: "trade_history".into(),
                schema: trade_history_schema(),
                row_count: |sf| (sf * 51_840.0) as usize,
            },
            TableDef {
                name: "trade_request".into(),
                schema: trade_request_schema(),
                row_count: |sf| (sf * 100.0) as usize,
            },
            TableDef {
                name: "trade_type".into(),
                schema: trade_type_schema(),
                row_count: |_| 5,
            },
            TableDef {
                name: "settlement".into(),
                schema: settlement_schema(),
                row_count: |sf| (sf * 17_280.0) as usize,
            },
            TableDef {
                name: "cash_transaction".into(),
                schema: cash_transaction_schema(),
                row_count: |sf| (sf * 13_824.0) as usize,
            },
            TableDef {
                name: "commission_rate".into(),
                schema: commission_rate_schema(),
                row_count: |_| 240,
            },
            // Company domain
            TableDef {
                name: "company".into(),
                schema: company_schema(),
                row_count: |sf| dim_card_usize(sf, 5.0),
            },
            TableDef {
                name: "company_competitor".into(),
                schema: company_competitor_schema(),
                row_count: |sf| (sf * 15.0) as usize,
            },
            TableDef {
                name: "security".into(),
                schema: security_schema(),
                row_count: |sf| dim_card_usize(sf, 6.85),
            },
            TableDef {
                name: "daily_market".into(),
                schema: daily_market_schema(),
                row_count: |sf| (sf * 17_136.0) as usize,
            },
            TableDef {
                name: "financial".into(),
                schema: financial_schema(),
                row_count: |sf| (sf * 100.0) as usize,
            },
            TableDef {
                name: "last_trade".into(),
                schema: last_trade_schema(),
                row_count: |sf| dim_card_usize(sf, 6.85),
            },
            TableDef {
                name: "news_item".into(),
                schema: news_item_schema(),
                row_count: |sf| (sf * 100.0) as usize,
            },
            TableDef {
                name: "news_xref".into(),
                schema: news_xref_schema(),
                row_count: |sf| (sf * 100.0) as usize,
            },
            // Reference tables
            TableDef {
                name: "address".into(),
                schema: address_schema(),
                row_count: |sf| (sf * 5_500.0) as usize,
            },
            TableDef {
                name: "zip_code".into(),
                schema: zip_code_schema(),
                row_count: |_| 14_741,
            },
            TableDef {
                name: "status_type".into(),
                schema: status_type_schema(),
                row_count: |_| 5,
            },
            TableDef {
                name: "taxrate".into(),
                schema: taxrate_schema(),
                row_count: |_| 320,
            },
            TableDef {
                name: "exchange".into(),
                schema: exchange_schema(),
                row_count: |_| 4,
            },
            TableDef {
                name: "industry".into(),
                schema: industry_schema(),
                row_count: |_| 102,
            },
            TableDef {
                name: "sector".into(),
                schema: sector_schema(),
                row_count: |_| 12,
            },
            TableDef {
                name: "charge".into(),
                schema: charge_schema(),
                row_count: |_| 15,
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
            // Customer domain
            "customer_account" => generate_customer_account(scale),
            "customer" => generate_customer(scale),
            "customer_taxrate" => generate_customer_taxrate(scale),
            "account_permission" => generate_account_permission(scale),
            "holding" => generate_holding(scale),
            "holding_history" => generate_holding_history(scale),
            "holding_summary" => generate_holding_summary(scale),
            "watch_item" => generate_watch_item(scale),
            "watch_list" => generate_watch_list(scale),
            // Broker domain
            "broker" => generate_broker(scale),
            // Market domain
            "trade" => generate_trade(scale),
            "trade_history" => generate_trade_history(scale),
            "trade_request" => generate_trade_request(scale),
            "trade_type" => generate_trade_type(),
            "settlement" => generate_settlement(scale),
            "cash_transaction" => generate_cash_transaction(scale),
            "commission_rate" => generate_commission_rate(),
            // Company domain
            "company" => generate_company(scale),
            "company_competitor" => generate_company_competitor(scale),
            "security" => generate_security(scale),
            "daily_market" => generate_daily_market(scale),
            "financial" => generate_financial(scale),
            "last_trade" => generate_last_trade(scale),
            "news_item" => generate_news_item(scale),
            "news_xref" => generate_news_xref(scale),
            // Reference tables
            "address" => generate_address(scale),
            "zip_code" => generate_zip_code(),
            "status_type" => generate_status_type(),
            "taxrate" => generate_taxrate(),
            "exchange" => generate_exchange(),
            "industry" => generate_industry(),
            "sector" => generate_sector(),
            "charge" => generate_charge(),
            _ => anyhow::bail!("Unknown TPC-E table: {table}"),
        };

        let full_output = format!("{output_dir}/tpce/sf{scale}");
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
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tpce_has_33_tables() {
        let gen = TpceGenerator;
        assert_eq!(gen.tables().len(), 33, "expected 33 TPC-E tables");
    }

    #[test]
    fn tpce_table_names_unique() {
        let gen = TpceGenerator;
        let mut names: Vec<String> = gen.tables().iter().map(|t| t.name.clone()).collect();
        let original_len = names.len();
        names.dedup();
        assert_eq!(names.len(), original_len, "duplicate table names found");
    }

    #[test]
    fn fixed_tables_have_correct_row_counts() {
        let gen = TpceGenerator;
        let tables = gen.tables();
        let fixed = |name: &str| {
            tables
                .iter()
                .find(|t| t.name == name)
                .map(|t| (t.row_count)(1.0))
                .unwrap_or(0)
        };
        assert_eq!(fixed("status_type"), 5);
        assert_eq!(fixed("trade_type"), 5);
        assert_eq!(fixed("exchange"), 4);
        assert_eq!(fixed("sector"), 12);
        assert_eq!(fixed("industry"), 102);
        assert_eq!(fixed("taxrate"), 320);
        assert_eq!(fixed("commission_rate"), 240);
        assert_eq!(fixed("zip_code"), 14_741);
        assert_eq!(fixed("charge"), 15);
    }

    #[test]
    fn scaled_tables_row_counts_at_sf1() {
        let gen = TpceGenerator;
        let tables = gen.tables();
        let count = |name: &str| {
            tables
                .iter()
                .find(|t| t.name == name)
                .map(|t| (t.row_count)(1.0))
                .unwrap_or(0)
        };
        assert_eq!(count("customer"), 1_000);
        assert_eq!(count("broker"), 10);
        assert_eq!(count("trade"), 17_280);
        assert_eq!(count("trade_history"), 51_840);
        // customer_account raw formula is 5×SF, but the DIM_MIN floor (10)
        // keeps it from collapsing at low SF — see DIM_MIN near top of file.
        // At SF1 the floor binds (5 < 10) so we get 10 rows.
        assert_eq!(count("customer_account"), 10);
        assert_eq!(count("holding"), 12_500);
        assert_eq!(count("watch_item"), 50_000);
    }

    #[test]
    fn generate_status_type_produces_5_rows() {
        let (_, batches) = generate_status_type();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);
    }

    #[test]
    fn generate_trade_type_produces_5_rows() {
        let (_, batches) = generate_trade_type();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);
    }

    #[test]
    fn generate_exchange_produces_4_rows() {
        let (_, batches) = generate_exchange();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
    }

    #[test]
    fn generate_sector_produces_12_rows() {
        let (_, batches) = generate_sector();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 12);
    }

    #[test]
    fn generate_industry_produces_102_rows() {
        let (_, batches) = generate_industry();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 102);
    }

    #[test]
    fn generate_taxrate_produces_320_rows() {
        let (_, batches) = generate_taxrate();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 320);
    }

    #[test]
    fn generate_commission_rate_produces_240_rows() {
        let (_, batches) = generate_commission_rate();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 240);
    }

    #[test]
    fn generate_charge_produces_15_rows() {
        let (_, batches) = generate_charge();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 15);
    }

    #[test]
    fn generate_customer_sf1_produces_1000_rows() {
        let (_, batches) = generate_customer(1.0);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1_000);
    }

    #[test]
    fn generate_trade_sf1_produces_17280_rows() {
        let (_, batches) = generate_trade(1.0);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 17_280);
    }

    #[test]
    fn generate_broker_sf1_produces_10_rows() {
        let (_, batches) = generate_broker(1.0);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 10);
    }

    #[test]
    fn generate_zip_code_produces_14741_rows() {
        let (_, batches) = generate_zip_code();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 14_741);
    }

    #[test]
    fn generate_security_sf1_correct_count() {
        let (_, batches) = generate_security(1.0);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Raw formula at SF1 is 1.0 * 6.85 = 6, but DIM_MIN (10) floors it.
        assert_eq!(rows, 10);
    }

    #[test]
    fn schema_column_counts_match_spec() {
        assert_eq!(customer_schema().fields().len(), 16);
        assert_eq!(trade_schema().fields().len(), 15);
        assert_eq!(security_schema().fields().len(), 16);
        assert_eq!(financial_schema().fields().len(), 14);
        assert_eq!(daily_market_schema().fields().len(), 6);
        assert_eq!(commission_rate_schema().fields().len(), 6);
    }

    #[test]
    fn symb_for_idx_produces_4_char_strings() {
        for i in [0, 1, 25, 26, 100, 1000, 456975] {
            let s = symb_for_idx(i);
            assert_eq!(s.len(), 4, "symbol {s} for idx {i} is not 4 chars");
            assert!(
                s.chars().all(|c| c.is_ascii_uppercase()),
                "non-uppercase: {s}"
            );
        }
    }

    #[test]
    fn get_generator_returns_tpce() {
        let gen = crate::generate::get_generator("tpce").expect("tpce generator");
        assert_eq!(gen.name(), "tpce");
        assert_eq!(gen.tables().len(), 33);
    }
}
