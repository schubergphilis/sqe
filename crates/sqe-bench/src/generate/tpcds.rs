use std::sync::Arc;

use arrow_array::{Date32Array, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{parquet_writer, BenchmarkGenerator, GenerateStats, TableDef};

pub struct TpcdsGenerator;

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, dt)| Field::new(*name, dt.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

fn i32() -> DataType {
    DataType::Int32
}
fn f64() -> DataType {
    DataType::Float64
}
fn str() -> DataType {
    DataType::Utf8
}
fn date() -> DataType {
    DataType::Date32
}

// ---------------------------------------------------------------------------
// Schema definitions
// ---------------------------------------------------------------------------

fn store_sales_schema() -> SchemaRef {
    schema(&[
        ("ss_sold_date_sk", i32()),
        ("ss_sold_time_sk", i32()),
        ("ss_item_sk", i32()),
        ("ss_customer_sk", i32()),
        ("ss_cdemo_sk", i32()),
        ("ss_hdemo_sk", i32()),
        ("ss_addr_sk", i32()),
        ("ss_store_sk", i32()),
        ("ss_promo_sk", i32()),
        ("ss_ticket_number", i32()),
        ("ss_quantity", i32()),
        ("ss_wholesale_cost", f64()),
        ("ss_list_price", f64()),
        ("ss_sales_price", f64()),
        ("ss_ext_discount_amt", f64()),
        ("ss_ext_sales_price", f64()),
        ("ss_ext_wholesale_cost", f64()),
        ("ss_ext_list_price", f64()),
        ("ss_ext_tax", f64()),
        ("ss_coupon_amt", f64()),
        ("ss_net_paid", f64()),
        ("ss_net_paid_inc_tax", f64()),
        ("ss_net_profit", f64()),
    ])
}

fn store_returns_schema() -> SchemaRef {
    schema(&[
        ("sr_returned_date_sk", i32()),
        ("sr_return_time_sk", i32()),
        ("sr_item_sk", i32()),
        ("sr_customer_sk", i32()),
        ("sr_cdemo_sk", i32()),
        ("sr_hdemo_sk", i32()),
        ("sr_addr_sk", i32()),
        ("sr_store_sk", i32()),
        ("sr_reason_sk", i32()),
        ("sr_ticket_number", i32()),
        ("sr_return_quantity", i32()),
        ("sr_return_amt", f64()),
        ("sr_return_tax", f64()),
        ("sr_return_amt_inc_tax", f64()),
        ("sr_fee", f64()),
        ("sr_return_ship_cost", f64()),
        ("sr_refunded_cash", f64()),
        ("sr_reversed_charge", f64()),
        ("sr_store_credit", f64()),
        ("sr_net_loss", f64()),
    ])
}

fn catalog_sales_schema() -> SchemaRef {
    schema(&[
        ("cs_sold_date_sk", i32()),
        ("cs_sold_time_sk", i32()),
        ("cs_ship_date_sk", i32()),
        ("cs_bill_customer_sk", i32()),
        ("cs_bill_cdemo_sk", i32()),
        ("cs_bill_hdemo_sk", i32()),
        ("cs_bill_addr_sk", i32()),
        ("cs_ship_customer_sk", i32()),
        ("cs_ship_cdemo_sk", i32()),
        ("cs_ship_hdemo_sk", i32()),
        ("cs_ship_addr_sk", i32()),
        ("cs_call_center_sk", i32()),
        ("cs_catalog_page_sk", i32()),
        ("cs_ship_mode_sk", i32()),
        ("cs_warehouse_sk", i32()),
        ("cs_item_sk", i32()),
        ("cs_promo_sk", i32()),
        ("cs_order_number", i32()),
        ("cs_quantity", i32()),
        ("cs_wholesale_cost", f64()),
        ("cs_list_price", f64()),
        ("cs_sales_price", f64()),
        ("cs_ext_discount_amt", f64()),
        ("cs_ext_sales_price", f64()),
        ("cs_ext_wholesale_cost", f64()),
        ("cs_ext_list_price", f64()),
        ("cs_ext_tax", f64()),
        ("cs_coupon_amt", f64()),
        ("cs_ext_ship_cost", f64()),
        ("cs_net_paid", f64()),
        ("cs_net_paid_inc_tax", f64()),
        ("cs_net_paid_inc_ship", f64()),
        ("cs_net_paid_inc_ship_tax", f64()),
        ("cs_net_profit", f64()),
    ])
}

fn catalog_returns_schema() -> SchemaRef {
    schema(&[
        ("cr_returned_date_sk", i32()),
        ("cr_returned_time_sk", i32()),
        ("cr_item_sk", i32()),
        ("cr_refunded_customer_sk", i32()),
        ("cr_refunded_cdemo_sk", i32()),
        ("cr_refunded_hdemo_sk", i32()),
        ("cr_refunded_addr_sk", i32()),
        ("cr_returning_customer_sk", i32()),
        ("cr_returning_cdemo_sk", i32()),
        ("cr_returning_hdemo_sk", i32()),
        ("cr_returning_addr_sk", i32()),
        ("cr_call_center_sk", i32()),
        ("cr_catalog_page_sk", i32()),
        ("cr_ship_mode_sk", i32()),
        ("cr_warehouse_sk", i32()),
        ("cr_reason_sk", i32()),
        ("cr_order_number", i32()),
        ("cr_return_quantity", i32()),
        ("cr_return_amount", f64()),
        ("cr_return_tax", f64()),
        ("cr_return_amt_inc_tax", f64()),
        ("cr_fee", f64()),
        ("cr_return_ship_cost", f64()),
        ("cr_refunded_cash", f64()),
        ("cr_reversed_charge", f64()),
        ("cr_store_credit", f64()),
        ("cr_net_loss", f64()),
    ])
}

fn web_sales_schema() -> SchemaRef {
    schema(&[
        ("ws_sold_date_sk", i32()),
        ("ws_sold_time_sk", i32()),
        ("ws_ship_date_sk", i32()),
        ("ws_item_sk", i32()),
        ("ws_bill_customer_sk", i32()),
        ("ws_bill_cdemo_sk", i32()),
        ("ws_bill_hdemo_sk", i32()),
        ("ws_bill_addr_sk", i32()),
        ("ws_ship_customer_sk", i32()),
        ("ws_ship_cdemo_sk", i32()),
        ("ws_ship_hdemo_sk", i32()),
        ("ws_ship_addr_sk", i32()),
        ("ws_web_page_sk", i32()),
        ("ws_web_site_sk", i32()),
        ("ws_ship_mode_sk", i32()),
        ("ws_warehouse_sk", i32()),
        ("ws_promo_sk", i32()),
        ("ws_order_number", i32()),
        ("ws_quantity", i32()),
        ("ws_wholesale_cost", f64()),
        ("ws_list_price", f64()),
        ("ws_sales_price", f64()),
        ("ws_ext_discount_amt", f64()),
        ("ws_ext_sales_price", f64()),
        ("ws_ext_wholesale_cost", f64()),
        ("ws_ext_list_price", f64()),
        ("ws_ext_tax", f64()),
        ("ws_coupon_amt", f64()),
        ("ws_ext_ship_cost", f64()),
        ("ws_net_paid", f64()),
        ("ws_net_paid_inc_tax", f64()),
        ("ws_net_paid_inc_ship", f64()),
        ("ws_net_paid_inc_ship_tax", f64()),
        ("ws_net_profit", f64()),
    ])
}

fn web_returns_schema() -> SchemaRef {
    schema(&[
        ("wr_returned_date_sk", i32()),
        ("wr_returned_time_sk", i32()),
        ("wr_item_sk", i32()),
        ("wr_refunded_customer_sk", i32()),
        ("wr_refunded_cdemo_sk", i32()),
        ("wr_refunded_hdemo_sk", i32()),
        ("wr_refunded_addr_sk", i32()),
        ("wr_returning_customer_sk", i32()),
        ("wr_returning_cdemo_sk", i32()),
        ("wr_returning_hdemo_sk", i32()),
        ("wr_returning_addr_sk", i32()),
        ("wr_web_page_sk", i32()),
        ("wr_reason_sk", i32()),
        ("wr_order_number", i32()),
        ("wr_return_quantity", i32()),
        ("wr_return_amt", f64()),
        ("wr_return_tax", f64()),
        ("wr_return_amt_inc_tax", f64()),
        ("wr_fee", f64()),
        ("wr_return_ship_cost", f64()),
        ("wr_refunded_cash", f64()),
        ("wr_reversed_charge", f64()),
        ("wr_account_credit", f64()),
        ("wr_net_loss", f64()),
    ])
}

fn inventory_schema() -> SchemaRef {
    schema(&[
        ("inv_date_sk", i32()),
        ("inv_item_sk", i32()),
        ("inv_warehouse_sk", i32()),
        ("inv_quantity_on_hand", i32()),
    ])
}

fn date_dim_schema() -> SchemaRef {
    schema(&[
        ("d_date_sk", i32()),
        ("d_date_id", str()),
        ("d_date", date()),
        ("d_month_seq", i32()),
        ("d_week_seq", i32()),
        ("d_quarter_seq", i32()),
        ("d_year", i32()),
        ("d_dow", i32()),
        ("d_moy", i32()),
        ("d_dom", i32()),
        ("d_qoy", i32()),
        ("d_fy_year", i32()),
        ("d_fy_quarter_seq", i32()),
        ("d_fy_week_seq", i32()),
        ("d_day_name", str()),
        ("d_quarter_name", str()),
        ("d_holiday", str()),
        ("d_weekend", str()),
        ("d_following_holiday", str()),
        ("d_first_dom", i32()),
        ("d_last_dom", i32()),
        ("d_same_day_ly", i32()),
        ("d_same_day_lq", i32()),
        ("d_current_day", str()),
        ("d_current_week", str()),
        ("d_current_month", str()),
        ("d_current_quarter", str()),
        ("d_current_year", str()),
    ])
}

fn time_dim_schema() -> SchemaRef {
    schema(&[
        ("t_time_sk", i32()),
        ("t_time_id", str()),
        ("t_time", i32()),
        ("t_hour", i32()),
        ("t_minute", i32()),
        ("t_second", i32()),
        ("t_am_pm", str()),
        ("t_shift", str()),
        ("t_sub_shift", str()),
        ("t_meal_time", str()),
    ])
}

fn item_schema() -> SchemaRef {
    schema(&[
        ("i_item_sk", i32()),
        ("i_item_id", str()),
        ("i_rec_start_date", date()),
        ("i_rec_end_date", date()),
        ("i_item_desc", str()),
        ("i_current_price", f64()),
        ("i_wholesale_cost", f64()),
        ("i_brand_id", i32()),
        ("i_brand", str()),
        ("i_class_id", i32()),
        ("i_class", str()),
        ("i_category_id", i32()),
        ("i_category", str()),
        ("i_manufact_id", i32()),
        ("i_manufact", str()),
        ("i_size", str()),
        ("i_formulation", str()),
        ("i_color", str()),
        ("i_units", str()),
        ("i_container", str()),
        ("i_manager_id", i32()),
        ("i_product_name", str()),
    ])
}

fn customer_schema() -> SchemaRef {
    schema(&[
        ("c_customer_sk", i32()),
        ("c_customer_id", str()),
        ("c_current_cdemo_sk", i32()),
        ("c_current_hdemo_sk", i32()),
        ("c_current_addr_sk", i32()),
        ("c_first_shipto_date_sk", i32()),
        ("c_first_sales_date_sk", i32()),
        ("c_salutation", str()),
        ("c_first_name", str()),
        ("c_last_name", str()),
        ("c_preferred_cust_flag", str()),
        ("c_birth_day", i32()),
        ("c_birth_month", i32()),
        ("c_birth_year", i32()),
        ("c_birth_country", str()),
        ("c_login", str()),
        ("c_email_address", str()),
        ("c_last_review_date_sk", i32()),
    ])
}

fn customer_address_schema() -> SchemaRef {
    schema(&[
        ("ca_address_sk", i32()),
        ("ca_address_id", str()),
        ("ca_street_number", str()),
        ("ca_street_name", str()),
        ("ca_street_type", str()),
        ("ca_suite_number", str()),
        ("ca_city", str()),
        ("ca_county", str()),
        ("ca_state", str()),
        ("ca_zip", str()),
        ("ca_country", str()),
        ("ca_gmt_offset", f64()),
        ("ca_location_type", str()),
    ])
}

fn customer_demographics_schema() -> SchemaRef {
    schema(&[
        ("cd_demo_sk", i32()),
        ("cd_gender", str()),
        ("cd_marital_status", str()),
        ("cd_education_status", str()),
        ("cd_purchase_estimate", i32()),
        ("cd_credit_rating", str()),
        ("cd_dep_count", i32()),
        ("cd_dep_employed_count", i32()),
        ("cd_dep_college_count", i32()),
    ])
}

fn household_demographics_schema() -> SchemaRef {
    schema(&[
        ("hd_demo_sk", i32()),
        ("hd_income_band_sk", i32()),
        ("hd_buy_potential", str()),
        ("hd_dep_count", i32()),
        ("hd_vehicle_count", i32()),
    ])
}

fn store_schema() -> SchemaRef {
    schema(&[
        ("s_store_sk", i32()),
        ("s_store_id", str()),
        ("s_rec_start_date", date()),
        ("s_rec_end_date", date()),
        ("s_closed_date_sk", i32()),
        ("s_store_name", str()),
        ("s_number_employees", i32()),
        ("s_floor_space", i32()),
        ("s_hours", str()),
        ("s_manager", str()),
        ("s_market_id", i32()),
        ("s_geography_class", str()),
        ("s_market_desc", str()),
        ("s_market_manager", str()),
        ("s_division_id", i32()),
        ("s_division_name", str()),
        ("s_company_id", i32()),
        ("s_company_name", str()),
        ("s_street_number", str()),
        ("s_street_name", str()),
        ("s_street_type", str()),
        ("s_suite_number", str()),
        ("s_city", str()),
        ("s_county", str()),
        ("s_state", str()),
        ("s_zip", str()),
        ("s_country", str()),
        ("s_gmt_offset", f64()),
        ("s_tax_percentage", f64()),
    ])
}

fn catalog_page_schema() -> SchemaRef {
    schema(&[
        ("cp_catalog_page_sk", i32()),
        ("cp_catalog_page_id", str()),
        ("cp_start_date_sk", i32()),
        ("cp_end_date_sk", i32()),
        ("cp_department", str()),
        ("cp_catalog_number", i32()),
        ("cp_catalog_page_number", i32()),
        ("cp_description", str()),
        ("cp_type", str()),
    ])
}

fn web_site_schema() -> SchemaRef {
    schema(&[
        ("web_site_sk", i32()),
        ("web_site_id", str()),
        ("web_rec_start_date", date()),
        ("web_rec_end_date", date()),
        ("web_name", str()),
        ("web_open_date_sk", i32()),
        ("web_close_date_sk", i32()),
        ("web_class", str()),
        ("web_manager", str()),
        ("web_mkt_id", i32()),
        ("web_mkt_class", str()),
        ("web_mkt_desc", str()),
        ("web_market_manager", str()),
        ("web_company_id", i32()),
        ("web_company_name", str()),
        ("web_street_number", str()),
        ("web_street_name", str()),
        ("web_street_type", str()),
        ("web_suite_number", str()),
        ("web_city", str()),
        ("web_county", str()),
        ("web_state", str()),
        ("web_zip", str()),
        ("web_country", str()),
        ("web_gmt_offset", f64()),
        ("web_tax_percentage", f64()),
    ])
}

fn web_page_schema() -> SchemaRef {
    schema(&[
        ("wp_web_page_sk", i32()),
        ("wp_web_page_id", str()),
        ("wp_rec_start_date", date()),
        ("wp_rec_end_date", date()),
        ("wp_creation_date_sk", i32()),
        ("wp_access_date_sk", i32()),
        ("wp_autogen_flag", str()),
        ("wp_customer_sk", i32()),
        ("wp_url", str()),
        ("wp_type", str()),
        ("wp_char_count", i32()),
        ("wp_link_count", i32()),
        ("wp_image_count", i32()),
        ("wp_max_ad_count", i32()),
    ])
}

fn warehouse_schema() -> SchemaRef {
    schema(&[
        ("w_warehouse_sk", i32()),
        ("w_warehouse_id", str()),
        ("w_warehouse_name", str()),
        ("w_warehouse_sq_ft", i32()),
        ("w_street_number", str()),
        ("w_street_name", str()),
        ("w_street_type", str()),
        ("w_suite_number", str()),
        ("w_city", str()),
        ("w_county", str()),
        ("w_state", str()),
        ("w_zip", str()),
        ("w_country", str()),
        ("w_gmt_offset", f64()),
    ])
}

fn promotion_schema() -> SchemaRef {
    schema(&[
        ("p_promo_sk", i32()),
        ("p_promo_id", str()),
        ("p_start_date_sk", i32()),
        ("p_end_date_sk", i32()),
        ("p_item_sk", i32()),
        ("p_cost", f64()),
        ("p_response_target", i32()),
        ("p_promo_name", str()),
        ("p_channel_dmail", str()),
        ("p_channel_email", str()),
        ("p_channel_catalog", str()),
        ("p_channel_tv", str()),
        ("p_channel_radio", str()),
        ("p_channel_press", str()),
        ("p_channel_event", str()),
        ("p_channel_demo", str()),
        ("p_channel_details", str()),
        ("p_purpose", str()),
        ("p_discount_active", str()),
    ])
}

fn reason_schema() -> SchemaRef {
    schema(&[
        ("r_reason_sk", i32()),
        ("r_reason_id", str()),
        ("r_reason_desc", str()),
    ])
}

fn income_band_schema() -> SchemaRef {
    schema(&[
        ("ib_income_band_sk", i32()),
        ("ib_lower_bound", i32()),
        ("ib_upper_bound", i32()),
    ])
}

fn ship_mode_schema() -> SchemaRef {
    schema(&[
        ("sm_ship_mode_sk", i32()),
        ("sm_ship_mode_id", str()),
        ("sm_type", str()),
        ("sm_code", str()),
        ("sm_carrier", str()),
        ("sm_contract", str()),
    ])
}

fn call_center_schema() -> SchemaRef {
    schema(&[
        ("cc_call_center_sk", i32()),
        ("cc_call_center_id", str()),
        ("cc_rec_start_date", date()),
        ("cc_rec_end_date", date()),
        ("cc_closed_date_sk", i32()),
        ("cc_open_date_sk", i32()),
        ("cc_name", str()),
        ("cc_class", str()),
        ("cc_employees", i32()),
        ("cc_sq_ft", i32()),
        ("cc_hours", str()),
        ("cc_manager", str()),
        ("cc_mkt_id", i32()),
        ("cc_mkt_class", str()),
        ("cc_mkt_desc", str()),
        ("cc_market_manager", str()),
        ("cc_division", i32()),
        ("cc_division_name", str()),
        ("cc_company", i32()),
        ("cc_company_name", str()),
        ("cc_street_number", str()),
        ("cc_street_name", str()),
        ("cc_street_type", str()),
        ("cc_suite_number", str()),
        ("cc_city", str()),
        ("cc_county", str()),
        ("cc_state", str()),
        ("cc_zip", str()),
        ("cc_country", str()),
        ("cc_gmt_offset", f64()),
        ("cc_tax_percentage", f64()),
    ])
}

// ---------------------------------------------------------------------------
// Row counts
//
// Every function must reproduce BOTH official dsdgen reference points,
// sf0.01 and sf1 (diffed by scripts/validate-generator-tpcds.py). The spec
// does NOT scale all tables linearly: some are fixed, some are floored,
// some derive from other tables. Shape and the two reference counts are
// stated per function.
// ---------------------------------------------------------------------------

// Linear fact tables: official sf0.01 / sf1 counts in the trailing comment.
// Returns tables are sampled per-sale in dsdgen, so small-scale counts are
// approximate (within the harness 15% row-ratio tolerance).
fn store_sales_rows(sf: f64) -> usize {
    super::scaled(sf, 2_880_000.0)
} // 28,810 / 2,880,404
fn store_returns_rows(sf: f64) -> usize {
    super::scaled(sf, 287_999.0)
} // 2,810 / 287,867
fn catalog_sales_rows(sf: f64) -> usize {
    super::scaled(sf, 1_441_548.0)
} // 14,313 / 1,441,548
fn catalog_returns_rows(sf: f64) -> usize {
    super::scaled(sf, 144_067.0)
} // 1,358 / 144,067
fn web_sales_rows(sf: f64) -> usize {
    super::scaled(sf, 719_384.0)
} // 7,212 / 719,384
fn web_returns_rows(sf: f64) -> usize {
    super::scaled(sf, 71_763.0)
} // 679 / 71,654

// Derived: 261 weekly snapshots covering half the item x warehouse grid.
// 23,490 / 11,745,000.
fn inventory_rows(sf: f64) -> usize {
    261 * item_rows(sf) * warehouse_rows(sf) / 2
}

// Linear dimensions. 180 / 18,000 etc.
fn item_rows(sf: f64) -> usize {
    super::scaled(sf, 18_000.0)
} // 180 / 18,000
fn customer_rows(sf: f64) -> usize {
    super::scaled(sf, 100_000.0)
} // 1,000 / 100,000
fn customer_address_rows(sf: f64) -> usize {
    super::scaled(sf, 50_000.0)
} // 500 / 50,000

// Linear below sf1, fixed at 1,920,800 from sf1 up. 19,208 / 1,920,800.
fn customer_demographics_rows(sf: f64) -> usize {
    (sf * 1_920_800.0).clamp(1.0, 1_920_800.0) as usize
}

// Linear with a floor of 1 row. Official sf0.01 / sf1 counts:
fn store_rows(sf: f64) -> usize {
    super::scaled(sf, 12.0)
} // 1 / 12
fn web_site_rows(sf: f64) -> usize {
    super::scaled(sf, 30.0)
} // 1 / 30
fn web_page_rows(sf: f64) -> usize {
    super::scaled(sf, 60.0)
} // 1 / 60
fn warehouse_rows(sf: f64) -> usize {
    super::scaled(sf, 5.0)
} // 1 / 5
fn promotion_rows(sf: f64) -> usize {
    super::scaled(sf, 300.0)
} // 3 / 300
fn reason_rows(sf: f64) -> usize {
    super::scaled(sf, 35.0)
} // 1 / 35
fn call_center_rows(sf: f64) -> usize {
    super::scaled(sf, 6.0)
} // 1 / 6

// Fixed at every scale (identical at sf0.01 and sf1).
fn date_dim_rows(_sf: f64) -> usize {
    73_049
}
fn time_dim_rows(_sf: f64) -> usize {
    86_400
}
fn household_demographics_rows(_sf: f64) -> usize {
    7_200
}
fn income_band_rows(_sf: f64) -> usize {
    20
}
fn ship_mode_rows(_sf: f64) -> usize {
    20
}
// catalog_page only steps up above sf1; both reference points are 11,718.
fn catalog_page_rows(_sf: f64) -> usize {
    11_718
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

const BATCH_SIZE: usize = 10_000;

// TPC-DS date range: 1998-01-01 to 2003-12-31
const DS_DATE_START: i32 = 10227; // days since epoch for 1998-01-01
const DS_DATE_RANGE: i32 = 2191; // ~6 years

fn seed_for_table(name: &str) -> u64 {
    name.bytes()
        .enumerate()
        .fold(0u64, |acc, (i, b)| {
            acc ^ ((b as u64).wrapping_shl(i as u32 % 64))
        })
        .wrapping_add(0xCAFE_BABE_1234_5678)
}

fn random_date(rng: &mut StdRng) -> i32 {
    DS_DATE_START + rng.gen_range(0..DS_DATE_RANGE)
}

/// Random `d_date_sk` surrogate key for fact tables, constrained to the
/// 1998-2003 sales window of `date_dim` (sk = row + 1, year = 1998 + row/366)
/// so that the d_year/d_moy filters in the official query set actually select
/// rows. Fact tables must reference date_dim by surrogate key, not by date
/// value: writing dates here is what NULLed every *_date_sk column.
fn random_date_sk(rng: &mut StdRng) -> i32 {
    rng.gen_range(1..=DS_DATE_RANGE)
}

/// dsdgen emits SCD-2 dimensions (item, store, call_center, web_site,
/// web_page) in a repeating 6-row cycle: a 1-revision entity, a 2-revision
/// entity, a 3-revision entity. The last revision of each entity is the
/// current row and must have a NULL rec_end_date, so cycle positions 0, 2
/// and 5 are current: exactly 50% of rows at full cycles, and the single
/// row of a 1-row table at sf0.01.
fn scd2_is_current(row: usize) -> bool {
    matches!(row % 6, 0 | 2 | 5)
}

fn scd2_rec_end_date(row: usize, rng: &mut StdRng) -> ColVal {
    if scd2_is_current(row) {
        ColVal::Date(None)
    } else {
        ColVal::Date(Some(random_date(rng)))
    }
}

fn random_id(rng: &mut StdRng) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    (0..16).map(|_| HEX[rng.gen_range(0..16)] as char).collect()
}

fn random_str<'a>(rng: &mut StdRng, choices: &[&'a str]) -> &'a str {
    choices[rng.gen_range(0..choices.len())]
}

/// Draw one string from a `(value, weight)` table with probability
/// proportional to weight. Consumes exactly one rng draw so it can replace a
/// `random_str` call without shifting downstream draw parity.
fn weighted_str<'a>(rng: &mut StdRng, table: &[(&'a str, u32)]) -> &'a str {
    let total: u32 = table.iter().map(|(_, w)| *w).sum();
    let mut pick = rng.gen_range(0..total);
    for (value, weight) in table {
        if pick < *weight {
            return value;
        }
        pick -= *weight;
    }
    table[table.len() - 1].0
}

fn random_word(rng: &mut StdRng, len: usize) -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    (0..len)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

fn random_name(rng: &mut StdRng) -> String {
    let len = rng.gen_range(4..10);
    random_word(rng, len)
}

const STATES: &[&str] = &[
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY",
];

const GENDERS: &[&str] = &["M", "F"];
const MARITAL: &[&str] = &["S", "M", "D", "W", "U"];
const EDUCATION: &[&str] = &[
    "Primary",
    "Secondary",
    "College",
    "2 yr Degree",
    "4 yr Degree",
    "Graduate",
    "Advanced Degree",
    "Unknown",
];
const CREDIT: &[&str] = &["Good", "High Risk", "Low Risk", "Unknown"];
/// dsdgen stride order: hd_buy_potential advances every 20 hd rows in this
/// exact sequence (verified against `CALL dsdgen(sf=0.1)`).
const BUY_POTENTIAL: &[&str] = &[
    "0-500",
    "501-1000",
    "1001-5000",
    "5001-10000",
    ">10000",
    "Unknown",
];
const YN: &[&str] = &["Y", "N"];
const SALUTATIONS: &[&str] = &["Mr.", "Ms.", "Mrs.", "Dr.", "Sir", "Miss"];
const STREET_TYPES: &[&str] = &["Street", "Ave", "Blvd", "Drive", "Road", "Way", "Lane"];
/// Official dsdgen categories in i_category_id order (1=Women .. 10=Electronics).
/// The qualification queries filter combos like i_category IN ('Books',
/// 'Children','Electronics') AND i_class IN ('personal','portable',...);
/// an invented vocabulary matched none of them (q63/q89 vacuous).
const CATEGORIES: &[&str] = &[
    "Women",
    "Men",
    "Children",
    "Shoes",
    "Music",
    "Jewelry",
    "Home",
    "Sports",
    "Books",
    "Electronics",
];
/// Per-category i_class vocabulary, index-aligned with CATEGORIES.
/// Extracted from dsdgen output; i_class_id stays an independent 1..16 draw
/// because dsdgen itself does not keep class ids consistent with class names.
const CATEGORY_CLASSES: &[&[&str]] = &[
    &["dresses", "fragrances", "maternity", "swimwear"],
    &["accessories", "pants", "shirts", "sports-apparel"],
    &["infants", "newborn", "school-uniforms", "toddlers"],
    &["athletic", "kids", "mens", "womens"],
    &["classical", "country", "pop", "rock"],
    &[
        "birdal",
        "bracelets",
        "consignment",
        "costume",
        "custom",
        "diamonds",
        "earings",
        "estate",
        "gold",
        "jewelry boxes",
        "loose stones",
        "mens watch",
        "pendants",
        "rings",
        "semi-precious",
        "womens watch",
    ],
    &[
        "accent",
        "bathroom",
        "bedding",
        "blinds/shades",
        "curtains/drapes",
        "decor",
        "flatware",
        "furniture",
        "glassware",
        "kids",
        "lighting",
        "mattresses",
        "paint",
        "rugs",
        "tables",
        "wallpaper",
    ],
    &[
        "archery",
        "athletic shoes",
        "baseball",
        "basketball",
        "camping",
        "fishing",
        "fitness",
        "football",
        "golf",
        "guns",
        "hockey",
        "optics",
        "outdoor",
        "pools",
        "sailing",
        "tennis",
    ],
    &[
        "arts",
        "business",
        "computers",
        "cooking",
        "entertainments",
        "fiction",
        "history",
        "home repair",
        "mystery",
        "parenting",
        "reference",
        "romance",
        "science",
        "self-help",
        "sports",
        "travel",
    ],
    &[
        "audio",
        "automotive",
        "camcorders",
        "cameras",
        "disk drives",
        "dvd/vcr players",
        "karoke",
        "memory",
        "monitors",
        "musical",
        "personal",
        "portable",
        "scanners",
        "stereo",
        "televisions",
        "wireless",
    ],
];
/// Brand base name per (category, class), index-aligned with
/// CATEGORY_CLASSES. dsdgen derives the brand from the item's category and
/// class deterministically (e.g. every Electronics/portable item is some
/// 'scholaramalgamalg #N'), and q63's qualification parameters rely on that
/// correlation: brand IN (...) AND category IN (...) AND class IN (...) is
/// unsatisfiable when brands are drawn independently. Extracted from
/// dsdgen output; only the " #N" suffix (1..=17) varies per item.
const CATEGORY_CLASS_BRANDS: &[&[&str]] = &[
    &[
        "amalgamalg",
        "importoamalg",
        "exportiamalg",
        "edu packamalg",
    ],
    &[
        "amalgimporto",
        "exportiimporto",
        "importoimporto",
        "edu packimporto",
    ],
    &[
        "importoexporti",
        "amalgexporti",
        "edu packexporti",
        "exportiexporti",
    ],
    &[
        "edu packedu pack",
        "exportiedu pack",
        "importoedu pack",
        "amalgedu pack",
    ],
    &[
        "edu packscholar",
        "importoscholar",
        "exportischolar",
        "amalgscholar",
    ],
    &[
        "amalgcorp",
        "edu packcorp",
        "corpbrand",
        "importobrand",
        "scholarbrand",
        "importocorp",
        "scholarcorp",
        "edu packbrand",
        "exporticorp",
        "univbrand",
        "exportibrand",
        "namelesscorp",
        "brandcorp",
        "corpcorp",
        "amalgbrand",
        "maxicorp",
    ],
    &[
        "amalgnameless",
        "amalgbrand",
        "importobrand",
        "scholarbrand",
        "edu packbrand",
        "brandbrand",
        "univnameless",
        "corpnameless",
        "edu packnameless",
        "exportibrand",
        "namelessbrand",
        "maxibrand",
        "importonameless",
        "corpbrand",
        "scholarnameless",
        "exportinameless",
    ],
    &[
        "amalgmaxi",
        "amalgnameless",
        "importonameless",
        "exportinameless",
        "edu packnameless",
        "scholarmaxi",
        "scholarnameless",
        "corpnameless",
        "corpmaxi",
        "importomaxi",
        "brandnameless",
        "maxinameless",
        "namelessnameless",
        "univmaxi",
        "exportimaxi",
        "edu packmaxi",
    ],
    &[
        "amalgmaxi",
        "importomaxi",
        "exportimaxi",
        "amalgunivamalg",
        "edu packmaxi",
        "scholarunivamalg",
        "scholarmaxi",
        "importounivamalg",
        "corpunivamalg",
        "corpmaxi",
        "brandmaxi",
        "namelessmaxi",
        "maximaxi",
        "exportiunivamalg",
        "edu packunivamalg",
        "univunivamalg",
    ],
    &[
        "edu packunivamalg",
        "edu packamalgamalg",
        "importounivamalg",
        "amalgunivamalg",
        "amalgamalgamalg",
        "exportiunivamalg",
        "scholarunivamalg",
        "univamalgamalg",
        "importoamalgamalg",
        "corpunivamalg",
        "brandunivamalg",
        "scholaramalgamalg",
        "namelessunivamalg",
        "exportiamalgamalg",
        "maxiunivamalg",
        "corpamalgamalg",
    ],
];
/// ZIP codes the qualification queries filter on (q08's 400-zip net + q15/q45).
/// dsdgen draws ca_zip/s_zip from a concentrated fips pool so customers cluster
/// per zip; q08 needs >10 preferred customers per zip, impossible with a uniform
/// 90k-value draw. Drawing both customer_address and store zips from this shared
/// pool reproduces the clustering (tpcds q08 was vacuous without it).
const ZIP_POOL: &[&str] = &[
    "10144", "10336", "10390", "10445", "10516", "10567", "11101", "11356", "11376", "11489",
    "11634", "11928", "12305", "13354", "13375", "13376", "13394", "13595", "13695", "13955",
    "14060", "14089", "14171", "14328", "14663", "14867", "14922", "15126", "15146", "15371",
    "15455", "15559", "15723", "15734", "15765", "15798", "15882", "16021", "16725", "16807",
    "17043", "17183", "17871", "17879", "17920", "18119", "18270", "18376", "18383", "18426",
    "18652", "18767", "18799", "18840", "18842", "18845", "18906", "19430", "19505", "19512",
    "19515", "19736", "19769", "19849", "20004", "20260", "20548", "21076", "21195", "21286",
    "21309", "21337", "21756", "22152", "22245", "22246", "22351", "22437", "22461", "22685",
    "22744", "22752", "22927", "23006", "23470", "23932", "23968", "24128", "24206", "24317",
    "24610", "24671", "24676", "24996", "25003", "25103", "25280", "25486", "25631", "25733",
    "25782", "25858", "25989", "26065", "26105", "26231", "26233", "26653", "26689", "26859",
    "27068", "27156", "27385", "27700", "28286", "28488", "28545", "28577", "28587", "28709",
    "28810", "28898", "28915", "29178", "29741", "29839", "30010", "30122", "30431", "30450",
    "30469", "30625", "30903", "31016", "31029", "31387", "31671", "31880", "32213", "32754",
    "33123", "33282", "33515", "33786", "34102", "34322", "34425", "35258", "35458", "35474",
    "35576", "35850", "35942", "36233", "36420", "36446", "36495", "36634", "37125", "37126",
    "37930", "38122", "38193", "38415", "38607", "38935", "39127", "39192", "39371", "39516",
    "39736", "39861", "39972", "40081", "40162", "40558", "40604", "41248", "41367", "41368",
    "41766", "41918", "42029", "42666", "42961", "43285", "43848", "43933", "44165", "44438",
    "45200", "45266", "45375", "45549", "45692", "45721", "45748", "46081", "46136", "46820",
    "47305", "47537", "47770", "48033", "48425", "48583", "49130", "49156", "49448", "50016",
    "50298", "50308", "50412", "51061", "51103", "51200", "51211", "51622", "51649", "51650",
    "51798", "51949", "52867", "53179", "53268", "53535", "53672", "54364", "54601", "54917",
    "55253", "55307", "55565", "56240", "56458", "56529", "56571", "56575", "56616", "56691",
    "56910", "57047", "57647", "57665", "57834", "57855", "58048", "58058", "58078", "58263",
    "58470", "58943", "59166", "59402", "60099", "60279", "60576", "61265", "61547", "61810",
    "61860", "62377", "62496", "62878", "62971", "63089", "63193", "63435", "63792", "63837",
    "63981", "64034", "64147", "64457", "64528", "64544", "65084", "65164", "66162", "66708",
    "66864", "67030", "67301", "67467", "67473", "67853", "67875", "67897", "68014", "68100",
    "68101", "68309", "68341", "68621", "68786", "68806", "68880", "68893", "68908", "69035",
    "69399", "69913", "69952", "70372", "70466", "70738", "71256", "71286", "71791", "71954",
    "72013", "72151", "72175", "72305", "72325", "72425", "72550", "72823", "73134", "73171",
    "73241", "73273", "73520", "73650", "74351", "75691", "76107", "76231", "76232", "76614",
    "76638", "76698", "77191", "77556", "77610", "77721", "78451", "78567", "78668", "78890",
    "79077", "79777", "79994", "80348", "81019", "81096", "81312", "81426", "81792", "82136",
    "82276", "82636", "83041", "83144", "83405", "83444", "83849", "83921", "83926", "83933",
    "84093", "84935", "85392", "85460", "85669", "85816", "86057", "86197", "86198", "86284",
    "86379", "86475", "87343", "87501", "87816", "88086", "88190", "88274", "88424", "88885",
    "89091", "89360", "90225", "90257", "90578", "91068", "91110", "91137", "91393", "92712",
    "94167", "94627", "94898", "94945", "94983", "96451", "96576", "96765", "96888", "96976",
    "97189", "97789", "98025", "98235", "98294", "98359", "98569", "99076", "99543",
];

const ITEM_SIZES: &[&str] = &[
    "small",
    "medium",
    "large",
    "N/A",
    "extra large",
    "petite",
    "economy",
];
/// Official dsdgen SF1 i_color frequencies. Colors are not uniform: peach is
/// 2.27% (409/18000) while the rarest are ~0.17%. q24 filters i_color='peach'
/// through the store_sales/store_returns/item join and needs peach at its
/// official rate to land its single row; a uniform 1/92 draw (1.09%) emptied
/// it. Weights are the raw official counts; a weighted draw reproduces the
/// proportions.
const ITEM_COLOR_WEIGHTS: &[(&str, u32)] = &[
    ("almond", 56),
    ("antique", 50),
    ("aquamarine", 41),
    ("azure", 35),
    ("beige", 44),
    ("bisque", 42),
    ("black", 46),
    ("blanched", 38),
    ("blue", 40),
    ("blush", 46),
    ("brown", 56),
    ("burlywood", 38),
    ("burnished", 38),
    ("chartreuse", 45),
    ("chiffon", 42),
    ("chocolate", 38),
    ("coral", 30),
    ("cornflower", 38),
    ("cornsilk", 39),
    ("cream", 30),
    ("cyan", 45),
    ("dark", 34),
    ("deep", 46),
    ("dim", 53),
    ("dodger", 55),
    ("drab", 46),
    ("firebrick", 34),
    ("floral", 49),
    ("forest", 41),
    ("frosted", 38),
    ("gainsboro", 147),
    ("ghost", 134),
    ("goldenrod", 128),
    ("green", 133),
    ("grey", 146),
    ("honeydew", 149),
    ("hot", 129),
    ("indian", 150),
    ("ivory", 130),
    ("khaki", 138),
    ("lace", 124),
    ("lavender", 114),
    ("lawn", 137),
    ("lemon", 108),
    ("light", 117),
    ("lime", 132),
    ("linen", 138),
    ("magenta", 114),
    ("maroon", 137),
    ("medium", 154),
    ("metallic", 126),
    ("midnight", 130),
    ("mint", 128),
    ("misty", 126),
    ("moccasin", 143),
    ("navajo", 127),
    ("navy", 131),
    ("olive", 135),
    ("orange", 141),
    ("orchid", 118),
    ("pale", 401),
    ("papaya", 402),
    ("peach", 409),
    ("peru", 393),
    ("pink", 385),
    ("plum", 386),
    ("powder", 376),
    ("puff", 373),
    ("purple", 390),
    ("red", 384),
    ("rose", 385),
    ("rosy", 399),
    ("royal", 402),
    ("saddle", 406),
    ("salmon", 405),
    ("sandy", 382),
    ("seashell", 397),
    ("sienna", 378),
    ("sky", 402),
    ("slate", 424),
    ("smoke", 424),
    ("snow", 390),
    ("spring", 407),
    ("steel", 395),
    ("tan", 405),
    ("thistle", 410),
    ("tomato", 385),
    ("turquoise", 449),
    ("violet", 383),
    ("wheat", 395),
    ("white", 394),
    ("yellow", 402),
];

/// i_manufact must not be a bijection with i_manufact_id: q41's correlated
/// subquery bridges items on `i_manufact = i1.i_manufact`, so a 1:1 mapping
/// collapses to id equality and returns 0. Each id is mapped to one of
/// MANUFACT_LABEL_SPREAD labels at MANUFACT_LABEL_STRIDE apart, wrapping the
/// 1..1000 space, so a manufact string co-occurs with several numerically
/// distant ids (official: ~3.3 both ways). The spread must reach across the
/// whole id range: q41 filters i_manufact_id BETWEEN 738 AND 778, and a local
/// neighborhood never bridged an arm-matching item into that band. Stride and
/// spread are tuned so degree lands ~6 (test band [2.5, 8.0]) and q41 returns
/// a handful of rows (official 4).
const MANUFACT_LABEL_SPREAD: i32 = 6;
const MANUFACT_LABEL_STRIDE: i32 = 167;

const ITEM_UNITS: &[&str] = &[
    "Box", "Bunch", "Bundle", "Carton", "Case", "Cup", "Dozen", "Dram", "Each", "Gram", "Gross",
    "Lb", "N/A", "Ounce", "Oz", "Pallet", "Pound", "Tbl", "Ton", "Tsp", "Unknown",
];
const AM_PM: &[&str] = &["AM", "PM"];
const SHIFTS: &[&str] = &["Morning", "Afternoon", "Evening", "Night"];
const MEAL_TIMES: &[&str] = &["breakfast", "lunch", "dinner", "unknown"];
const DAY_NAMES: &[&str] = &[
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];
const SHIP_TYPES: &[&str] = &["NEXT DAY", "TWO DAY", "STANDARD", "LIBRARY"];
const SHIP_CODES: &[&str] = &["AIR", "SURFACE", "SEA", "GROUND"];
const CARRIERS: &[&str] = &["FEDEX", "UPS", "USPS", "DHL", "AMAZON"];
const PROMO_PURPOSES: &[&str] = &["Unknown", "Cross-Sell", "Retention", "Acquisition"];
const CC_CLASSES: &[&str] = &["large", "medium", "small"];
const CC_HOURS: &[&str] = &["8AM-12AM", "8AM-4PM", "8AM-8PM"];
const WP_TYPES: &[&str] = &["dynamic", "static", "flash"];
const DEPT: &[&str] = &[
    "2001Q1", "2001Q2", "2001Q3", "2001Q4", "2002Q1", "2002Q2", "2002Q3", "2002Q4",
];

/// US retail GMT offsets. The official queries filter `gmt_offset = -5`
/// (q43/q56/q62/q99 on store, customer_address, call_center, web_site); a
/// uniform -12..12 draw left -5 absent from 12-row dimensions entirely.
const GMT_OFFSETS: &[f64] = &[-5.0, -6.0, -7.0, -8.0];

/// Cities the official queries probe (q84 and the city legs of q46/q68/q79
/// look for Edgewood, Fairview, Midway, Pleasant Hill, Riverside, Bethel,
/// Oak Grove, ...). dsdgen draws ca_city from a fixed list; random letter
/// soup never matched any predicate.
const CA_CITIES: &[&str] = &[
    "Edgewood",
    "Fairview",
    "Midway",
    "Pleasant Hill",
    "Riverside",
    "Bethel",
    "Oak Grove",
    "Antioch",
    "Greenville",
    "Springfield",
    "Salem",
    "Georgetown",
    "Centerville",
    "Mount Olive",
    "Glenwood",
    "Marion",
    "Five Points",
    "Liberty",
    "Union",
    "Crossroads",
    "Oakland",
    "Clinton",
    "Franklin",
    "Bridgeport",
    "Lakeview",
    "Highland",
    "Woodville",
    "Ashland",
    "Newport",
    "Sulphur Springs",
];

/// Real county names: every county the qualification queries probe (q10's
/// Rush/Toole/Jefferson/..., q34/q46's Williamson County/Franklin Parish/
/// Bronx County/Orange County/...) plus the most frequent counties in dsdgen
/// output. ca_county used to be `random_name()` letter soup, so every
/// county-filtered query was vacuous.
const COUNTIES: &[&str] = &[
    // qualification parameters used by the query set
    "Williamson County",
    "Franklin Parish",
    "Bronx County",
    "Orange County",
    "Rush County",
    "Toole County",
    "Jefferson County",
    "Dona Ana County",
    "La Porte County",
    // high-frequency dsdgen counties
    "Washington County",
    "Franklin County",
    "Clay County",
    "Madison County",
    "Jackson County",
    "Marion County",
    "Lincoln County",
    "Grant County",
    "Montgomery County",
    "Union County",
    "Monroe County",
    "Perry County",
    "Cherokee County",
    "Carroll County",
    "Crawford County",
    "Wayne County",
    "Henry County",
    "Knox County",
    "Douglas County",
    "Marshall County",
    "Adams County",
    "Polk County",
    "Fayette County",
    "Scott County",
    "Clinton County",
    "Lawrence County",
    "Brown County",
    "Lee County",
    "Morgan County",
    "Lake County",
    "Clark County",
    "Johnson County",
    "Greene County",
    "Pike County",
    "Warren County",
    "Cass County",
    "Macon County",
    "Calhoun County",
    "Mercer County",
    "Logan County",
    "Benton County",
    "Boone County",
    "Butler County",
    "Cedar County",
    "Columbia County",
    "Dallas County",
    "Decatur County",
    "Garfield County",
    "Hamilton County",
    "Hancock County",
    "Hardin County",
    "Harrison County",
    "Howard County",
    "Huntington County",
    "Iron County",
    "Jasper County",
    "Juniata County",
    "Kossuth County",
    "Lancaster County",
    "Liberty County",
    "Linn County",
    "Lyon County",
    "Mason County",
    "Miami County",
    "Mitchell County",
    "Newton County",
    "Noble County",
    "Oglethorpe County",
    "Osceola County",
    "Page County",
    "Pierce County",
    "Pulaski County",
    "Putnam County",
    "Randolph County",
    "Richland County",
    "Riley County",
    "Saline County",
    "Sevier County",
    "Shelby County",
    "Sioux County",
    "Stone County",
    "Sumner County",
    "Tama County",
    "Taylor County",
    "Tipton County",
    "Tyler County",
    "Valley County",
    "Vernon County",
    "Walker County",
    "Webster County",
    "White County",
    "Winnebago County",
    "Wood County",
    "Wright County",
    "York County",
    "Ziebach County",
];

/// dsdgen places every store at sf <= 1 in Midway or Fairview, Williamson
/// County, TN. The q34/q46 legs filter store.s_county and q46/q68/q79 filter
/// store.s_city; q01's qualification parameter is s_state = 'TN'.
const STORE_CITIES: &[&str] = &["Midway", "Fairview"];

// ---------------------------------------------------------------------------
// Deterministic baskets (multi-line tickets shared by sales and returns)
// ---------------------------------------------------------------------------

/// Per-channel salts for the per-ticket rng. Sales and returns generators
/// both seed `StdRng::seed_from_u64(salt ^ ticket)` and therefore recompute
/// the exact same basket independently: returns join sales on
/// (ticket/order, item) without ever materializing the sales table.
const STORE_TICKET_SALT: u64 = 0x5351_455f_5354_4f52; // "SQE_STOR"
const CATALOG_ORDER_SALT: u64 = 0x5351_455f_4341_544c; // "SQE_CATL"
const WEB_ORDER_SALT: u64 = 0x5351_455f_5745_4253; // "SQE_WEBS"

/// q54 needs a customer who (a) bought a Women/maternity item (i_item_sk 1)
/// via catalog in Dec-1998, (b) is addressed in Williamson County, TN (where
/// every store sits), and (c) has store_sales in d_month_seq 1188..=1190
/// (Jan-Mar 1999). The expected value of that coincidence on random data is
/// ~0.01 (official dsdgen itself lands q54 by luck), so the first
/// PLANTED_Q54_TICKETS catalog orders and store tickets are overridden to
/// carry customers 1..=PLANTED_Q54_TICKETS with those exact attributes. The
/// item (row 0 in generate_item), the addresses and the customer->address
/// links are planted in their own generators.
const PLANTED_Q54_TICKETS: i32 = 2;
/// OUR date_dim: sk 331..=360 = year 1998, moy 12 (d_month_seq 1187).
const Q54_DEC_1998_DATE_SK: i32 = 345;
/// OUR date_dim: sk 391..=420 = year 1999, moy 2 (d_month_seq 1189), inside
/// the 1188..=1190 window q54 derives from Dec-1998's month_seq + 1..+3.
const Q54_FEB_1999_DATE_SK: i32 = 400;

/// q25 needs one (customer, item) that appears in store_sales (Apr-2001, with
/// a store return in Apr-Oct 2001) AND catalog_sales (Apr-Oct 2001). The
/// three legs are independent random baskets, so the coincidence is planted:
/// store ticket 3 and catalog order 3 both carry customer 3 / item 2. The
/// store return date falls out of the <=120-day lag (Apr sale -> Apr-Aug
/// return, inside q25's window), so only the return's ticket number is forced.
const Q25_STORE_TICKET: i32 = 3;
const Q25_CATALOG_ORDER: i32 = 3;
const Q25_CUSTOMER: i32 = 3;
const Q25_ITEM: i32 = 2;
/// OUR date_dim: sk 1171..=1200 = year 2001, moy 4; sk 1201..=1230 = moy 5.
const Q25_APR_2001_DATE_SK: i32 = 1185;
const Q25_MAY_2001_DATE_SK: i32 = 1215;

/// q24 needs a peach item sold at an s_market_id = 8 store whose s_zip equals
/// the buyer's ca_zip, with a store return. The six-way join is too narrow for
/// peach to appear at its 2.27% rate (a ~106-row join on our seed had none),
/// so the coincidence is planted: item sk 3 is peach, store sk 3 is market 8
/// at Q24_ZIP, customer 4 lives at address 4 (also Q24_ZIP), and store ticket
/// 4 sells item 3 at store 3 to customer 4. Needs store sk 3, so it only
/// activates once the store dimension has >=3 rows (SF >= ~0.25).
const Q24_STORE_TICKET: i32 = 4;
const Q24_CUSTOMER: i32 = 4;
const Q24_ITEM: i32 = 3;
const Q24_STORE_SK: i32 = 3;
const Q24_ADDR_SK: i32 = 4;
const Q24_ZIP: &str = "10144"; // a ZIP_POOL entry

/// Maximum line items per ticket/order. Lines are uniform in 1..=25 so the
/// `HAVING count(*) BETWEEN 15 AND 20` windows in q34/q46/q68/q73/q79 match
/// a healthy fraction of tickets. The old generator emitted exactly one line
/// per ticket and those queries returned nothing.
const MAX_BASKET_LINES: usize = 25;

/// NULL probability for nullable fact FK columns. q76 selects rows WHERE
/// ss_customer_sk IS NULL (and the cs_/ws_ equivalents); the old generator
/// never emitted NULLs. The null-or-not decision is made once per ticket so
/// ticket-level grouping stays coherent.
const FK_NULL_RATE: f64 = 0.04;

/// Header fields and line items of one ticket/order, derived purely from
/// (salt, ticket). All lines of a ticket share the header fields; only the
/// item and quantity vary per line.
struct Basket {
    lines: usize,
    date_sk: i32,
    customer_sk: Option<i32>,
    cdemo_sk: Option<i32>,
    hdemo_sk: Option<i32>,
    addr_sk: Option<i32>,
    ship_customer_sk: Option<i32>,
    ship_cdemo_sk: Option<i32>,
    ship_hdemo_sk: Option<i32>,
    ship_addr_sk: Option<i32>,
    /// ss_store_sk / cs_call_center_sk / ws_web_site_sk depending on channel.
    channel_sk: i32,
    promo_sk: Option<i32>,
    items: Vec<i32>,
    quantities: Vec<i32>,
}

/// Scale-dependent FK domains shared by the basket-based sales and returns
/// generators. Both sides must derive identical dims from the same scale so
/// recomputed baskets stay byte-for-byte identical. Ranges are inclusive of
/// the dimension's last surrogate key; at small scales several dims collapse
/// to a single row and an exclusive `1..1` range would panic.
#[derive(Clone, Copy)]
struct FkDims {
    items: i32,
    customers: i32,
    cdemos: i32,
    hdemos: i32,
    addrs: i32,
    promos: i32,
}

impl FkDims {
    fn at(scale: f64) -> Self {
        Self {
            items: item_rows(scale) as i32,
            customers: customer_rows(scale) as i32,
            cdemos: customer_demographics_rows(scale) as i32,
            hdemos: household_demographics_rows(scale) as i32,
            addrs: customer_address_rows(scale) as i32,
            promos: promotion_rows(scale) as i32,
        }
    }
}

fn nullable_fk(rng: &mut StdRng, upper: i32) -> Option<i32> {
    if rng.gen_bool(FK_NULL_RATE) {
        None
    } else {
        Some(rng.gen_range(1..=upper))
    }
}

/// Recompute the basket for `ticket` from scratch. Must stay byte-for-byte
/// deterministic: the sales generator and the returns generator each call
/// this independently and rely on identical output.
fn basket(salt: u64, ticket: i32, channel_upper: i32, dims: FkDims) -> Basket {
    let mut rng = StdRng::seed_from_u64(salt ^ ticket as u64);
    let lines = rng.gen_range(1..=MAX_BASKET_LINES);
    let date_sk = rng.gen_range(1..=DS_DATE_RANGE);
    let customer_sk = nullable_fk(&mut rng, dims.customers);
    let cdemo_sk = nullable_fk(&mut rng, dims.cdemos);
    let hdemo_sk = nullable_fk(&mut rng, dims.hdemos);
    let addr_sk = nullable_fk(&mut rng, dims.addrs);
    let ship_customer_sk = nullable_fk(&mut rng, dims.customers);
    let ship_cdemo_sk = nullable_fk(&mut rng, dims.cdemos);
    let ship_hdemo_sk = nullable_fk(&mut rng, dims.hdemos);
    let ship_addr_sk = nullable_fk(&mut rng, dims.addrs);
    let channel_sk = rng.gen_range(1..=channel_upper);
    let promo_sk = nullable_fk(&mut rng, dims.promos);
    let items: Vec<i32> = (0..lines).map(|_| rng.gen_range(1..=dims.items)).collect();
    let quantities: Vec<i32> = (0..lines).map(|_| rng.gen_range(1..100)).collect();
    let mut b = Basket {
        lines,
        date_sk,
        customer_sk,
        cdemo_sk,
        hdemo_sk,
        addr_sk,
        ship_customer_sk,
        ship_cdemo_sk,
        ship_hdemo_sk,
        ship_addr_sk,
        channel_sk,
        promo_sk,
        items,
        quantities,
    };
    // Plant the q54 coincidence (see PLANTED_Q54_TICKETS) on the first tickets
    // of the catalog and store channels. Overrides come after every rng draw
    // so non-planted fields keep byte-for-byte draw parity with the returns
    // generators that recompute this basket.
    if (1..=PLANTED_Q54_TICKETS).contains(&ticket) {
        if salt == CATALOG_ORDER_SALT {
            b.date_sk = Q54_DEC_1998_DATE_SK;
            b.customer_sk = Some(ticket);
            b.items[0] = 1; // i_item_sk 1 is the Women/maternity item
        } else if salt == STORE_TICKET_SALT {
            b.date_sk = Q54_FEB_1999_DATE_SK;
            b.customer_sk = Some(ticket);
        }
    }
    // q25: the same customer+item in store (Apr-2001) and catalog (May-2001).
    if dims.customers >= Q25_CUSTOMER && dims.items >= Q25_ITEM {
        if salt == STORE_TICKET_SALT && ticket == Q25_STORE_TICKET {
            b.date_sk = Q25_APR_2001_DATE_SK;
            b.customer_sk = Some(Q25_CUSTOMER);
            b.items[0] = Q25_ITEM;
        } else if salt == CATALOG_ORDER_SALT && ticket == Q25_CATALOG_ORDER {
            b.date_sk = Q25_MAY_2001_DATE_SK;
            b.customer_sk = Some(Q25_CUSTOMER);
            b.items[0] = Q25_ITEM;
        }
    }
    // q24: peach item 3 sold at market-8 store 3 to customer 4 (whose address
    // zip matches store 3's zip). channel_upper is the store count for store
    // tickets, so this only fires once store sk 3 exists.
    if salt == STORE_TICKET_SALT
        && ticket == Q24_STORE_TICKET
        && channel_upper >= Q24_STORE_SK
        && dims.customers >= Q24_CUSTOMER
        && dims.items >= Q24_ITEM
    {
        b.customer_sk = Some(Q24_CUSTOMER);
        b.channel_sk = Q24_STORE_SK;
        b.items[0] = Q24_ITEM;
    }
    b
}

/// Highest ticket number a returns table may reference. The first K tickets
/// occupy at most K * MAX_BASKET_LINES sales rows, so every ticket up to
/// sales_rows / MAX_BASKET_LINES is guaranteed fully emitted (no line lost
/// to the row-count cap) and safe to return against.
fn returnable_tickets(scale: f64, sales_base: f64) -> i32 {
    (super::scaled(scale, sales_base).max(1) / MAX_BASKET_LINES).max(1) as i32
}

// ---------------------------------------------------------------------------
// Generic batch generator
// ---------------------------------------------------------------------------

/// Generate `total` rows in batches using the provided row-builder closure.
/// The closure receives `(row_index: usize, rng: &mut StdRng)` and returns
/// a vector of column values as `ColVal`.
///
/// Returns (schema, batches) ready for the parquet writer.
fn generate_batches<F>(
    tbl_schema: SchemaRef,
    total: usize,
    seed: u64,
    mut build_row: F,
) -> (SchemaRef, Vec<RecordBatch>)
where
    F: FnMut(usize, &mut StdRng) -> Vec<ColVal>,
{
    let ncols = tbl_schema.fields().len();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        // one Vec<ColVal> per column
        let mut cols: Vec<Vec<ColVal>> = (0..ncols).map(|_| Vec::with_capacity(n)).collect();
        for i in 0..n {
            let row = build_row(offset + i, &mut rng);
            assert_eq!(
                row.len(),
                ncols,
                "Row {} has {} values but schema has {} columns",
                offset + i,
                row.len(),
                ncols
            );
            for (c, v) in row.into_iter().enumerate() {
                cols[c].push(v);
            }
        }
        let arrays = cols_to_arrays(cols, &tbl_schema);
        batches.push(RecordBatch::try_new(tbl_schema.clone(), arrays).expect("record batch"));
        offset += n;
    }
    (tbl_schema, batches)
}

enum ColVal {
    I32(Option<i32>),
    F64(Option<f64>),
    Str(Option<String>),
    Date(Option<i32>),
}

fn cols_to_arrays(
    cols: Vec<Vec<ColVal>>,
    tbl_schema: &SchemaRef,
) -> Vec<Arc<dyn arrow_array::Array>> {
    // A ColVal variant that does not match the declared field type is a
    // generator bug. Panic instead of coercing to NULL: silent NULLs here made
    // every *_date_sk column empty and turned 74/99 TPC-DS compare queries
    // into vacuous empty-vs-empty matches before anyone noticed.
    fn mismatch(field: &arrow_schema::Field, got: &ColVal) -> ! {
        let got = match got {
            ColVal::I32(_) => "I32",
            ColVal::F64(_) => "F64",
            ColVal::Str(_) => "Str",
            ColVal::Date(_) => "Date",
        };
        panic!(
            "generator bug: column '{}' is {:?} but row builder produced ColVal::{}",
            field.name(),
            field.data_type(),
            got
        );
    }
    cols.into_iter()
        .enumerate()
        .map(|(idx, col)| {
            let field = tbl_schema.field(idx);
            match field.data_type() {
                DataType::Int32 => {
                    let v: Vec<Option<i32>> = col
                        .into_iter()
                        .map(|c| match c {
                            ColVal::I32(x) => x,
                            other => mismatch(field, &other),
                        })
                        .collect();
                    Arc::new(Int32Array::from(v)) as Arc<dyn arrow_array::Array>
                }
                DataType::Float64 => {
                    let v: Vec<Option<f64>> = col
                        .into_iter()
                        .map(|c| match c {
                            ColVal::F64(x) => x,
                            other => mismatch(field, &other),
                        })
                        .collect();
                    Arc::new(Float64Array::from(v)) as Arc<dyn arrow_array::Array>
                }
                DataType::Date32 => {
                    let v: Vec<Option<i32>> = col
                        .into_iter()
                        .map(|c| match c {
                            ColVal::Date(x) => x,
                            other => mismatch(field, &other),
                        })
                        .collect();
                    Arc::new(Date32Array::from(v)) as Arc<dyn arrow_array::Array>
                }
                DataType::Utf8 => {
                    let v: Vec<Option<String>> = col
                        .into_iter()
                        .map(|c| match c {
                            ColVal::Str(x) => x,
                            other => mismatch(field, &other),
                        })
                        .collect();
                    Arc::new(StringArray::from(v)) as Arc<dyn arrow_array::Array>
                }
                other => panic!(
                    "generator bug: column '{}' has unsupported type {other:?}",
                    field.name()
                ),
            }
        })
        .collect()
}

macro_rules! i {
    ($x:expr) => {
        ColVal::I32(Some($x))
    };
}
macro_rules! f {
    ($x:expr) => {
        ColVal::F64(Some($x))
    };
}
macro_rules! s {
    ($x:expr) => {
        ColVal::Str(Some($x.to_string()))
    };
}
macro_rules! d {
    ($x:expr) => {
        ColVal::Date(Some($x))
    };
}

// ---------------------------------------------------------------------------
// Table generators
// ---------------------------------------------------------------------------

fn generate_store_sales(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = store_sales_rows(scale);
    // FK ranges must track dimension cardinality at this scale;
    // sf1-sized ranges empty out dimension joins at small scales.
    let dims = FkDims::at(scale);
    let stores = store_rows(scale) as i32;
    // Walk tickets in order, emitting all lines of a basket consecutively.
    // The row indices arrive strictly sequentially from generate_batches, so
    // a small amount of closure state maps row -> (ticket, line) without
    // holding the table in memory.
    let mut ticket: i32 = 0;
    let mut line: usize = 0;
    let mut cur: Option<Basket> = None;
    generate_batches(
        store_sales_schema(),
        total,
        seed_for_table("store_sales"),
        move |_row, rng| {
            let exhausted = match &cur {
                None => true,
                Some(b) => line >= b.lines,
            };
            if exhausted {
                ticket += 1;
                cur = Some(basket(STORE_TICKET_SALT, ticket, stores, dims));
                line = 0;
            }
            let b = cur.as_ref().expect("basket set above");
            let item_sk = b.items[line];
            let qty = b.quantities[line];
            line += 1;
            let wc = rng.gen_range(10..500i32) as f64 / 10.0;
            let lp = wc * 1.5;
            let sp = lp * rng.gen_range(50..100i32) as f64 / 100.0;
            let tax = sp * 0.08;
            vec![
                i!(b.date_sk),
                i!(rng.gen_range(0..86400i32)),
                i!(item_sk),
                ColVal::I32(b.customer_sk),
                ColVal::I32(b.cdemo_sk),
                ColVal::I32(b.hdemo_sk),
                ColVal::I32(b.addr_sk),
                i!(b.channel_sk),
                ColVal::I32(b.promo_sk),
                i!(ticket),
                i!(qty),
                f!(wc),
                f!(lp),
                f!(sp),
                f!(0.0),
                f!(sp * qty as f64),
                f!(wc * qty as f64),
                f!(lp * qty as f64),
                f!(tax),
                f!(0.0),
                f!(sp * qty as f64),
                f!(sp * qty as f64 + tax),
                f!(sp * qty as f64 - wc * qty as f64),
            ]
        },
    )
}

fn generate_store_returns(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = store_returns_rows(scale);
    // FK ranges must track dimension cardinality at this scale;
    // sf1-sized ranges empty out dimension joins at small scales.
    let dims = FkDims::at(scale);
    let stores = store_rows(scale) as i32;
    let reasons = reason_rows(scale) as i32;
    let max_ticket = returnable_tickets(scale, 2_880_000.0);
    // Guard the planted returns on the referenced dims existing at this scale.
    let q25_active = dims.customers >= Q25_CUSTOMER && dims.items >= Q25_ITEM;
    let q24_active =
        stores >= Q24_STORE_SK && dims.customers >= Q24_CUSTOMER && dims.items >= Q24_ITEM;
    generate_batches(
        store_returns_schema(),
        total,
        seed_for_table("store_returns"),
        move |row, rng| {
            // Pick a fully-emitted sales ticket, recompute its basket, and return
            // one of its actual line items so (sr_ticket_number, sr_item_sk)
            // joins store_sales (q01/q17/q24/q25/q29/q50/q64/q85).
            let mut ticket = rng.gen_range(1..=max_ticket);
            let mut line_override: Option<usize> = None;
            // Force the first rows onto the q25/q24 planted store tickets so the
            // planted sales get a matching return; item/customer/date still derive
            // from the recomputed basket, preserving the returns<->sales invariant.
            if row == 0 && q25_active {
                ticket = Q25_STORE_TICKET;
                line_override = Some(0);
            } else if row == 1 && q24_active {
                ticket = Q24_STORE_TICKET;
                line_override = Some(0);
            }
            let b = basket(STORE_TICKET_SALT, ticket, stores, dims);
            let line = line_override.unwrap_or(rng.gen_range(0..b.lines));
            let item_sk = b.items[line];
            let qty = rng.gen_range(1..=b.quantities[line]);
            // Returns lag the sale by <=120 days (official: 98% of a month's
            // sales are returned within the next 6 months). Drawing uniformly over
            // the remaining calendar piled returns into 2003 and starved 1998,
            // emptying q91 (Nov-1998) and q25 (Apr-Oct 2001 return window).
            let ret_date = (b.date_sk + rng.gen_range(1..=120i32)).min(DS_DATE_RANGE);
            let amt = rng.gen_range(10..500i32) as f64;
            let tax = amt * 0.08;
            vec![
                i!(ret_date),
                i!(rng.gen_range(0..86400i32)),
                i!(item_sk),
                ColVal::I32(b.customer_sk),
                ColVal::I32(b.cdemo_sk),
                ColVal::I32(b.hdemo_sk),
                ColVal::I32(b.addr_sk),
                i!(b.channel_sk),
                i!(rng.gen_range(1..=reasons)),
                i!(ticket),
                i!(qty),
                f!(amt),
                f!(tax),
                f!(amt + tax),
                f!(amt * 0.02),
                f!(amt * 0.05),
                f!(amt * 0.6),
                f!(amt * 0.2),
                f!(amt * 0.2),
                f!(amt * 0.1),
            ]
        },
    )
}

fn generate_catalog_sales(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = catalog_sales_rows(scale);
    // FK ranges must track dimension cardinality at this scale;
    // sf1-sized ranges empty out dimension joins at small scales.
    let dims = FkDims::at(scale);
    let ccs = call_center_rows(scale) as i32;
    let whs = warehouse_rows(scale) as i32;
    let mut order: i32 = 0;
    let mut line: usize = 0;
    let mut cur: Option<Basket> = None;
    generate_batches(
        catalog_sales_schema(),
        total,
        seed_for_table("catalog_sales"),
        move |_row, rng| {
            let exhausted = match &cur {
                None => true,
                Some(b) => line >= b.lines,
            };
            if exhausted {
                order += 1;
                cur = Some(basket(CATALOG_ORDER_SALT, order, ccs, dims));
                line = 0;
            }
            let b = cur.as_ref().expect("basket set above");
            let item_sk = b.items[line];
            let qty = b.quantities[line];
            line += 1;
            let wc = rng.gen_range(10..500i32) as f64 / 10.0;
            let lp = wc * 1.5;
            let sp = lp * rng.gen_range(50..100i32) as f64 / 100.0;
            let tax = sp * 0.08;
            let ship = sp * 0.05 * qty as f64;
            let ship_date = (b.date_sk + rng.gen_range(1..=120i32)).min(DS_DATE_RANGE);
            vec![
                i!(b.date_sk),
                i!(rng.gen_range(0..86400i32)),
                i!(ship_date),
                ColVal::I32(b.customer_sk),
                ColVal::I32(b.cdemo_sk),
                ColVal::I32(b.hdemo_sk),
                ColVal::I32(b.addr_sk),
                ColVal::I32(b.ship_customer_sk),
                ColVal::I32(b.ship_cdemo_sk),
                ColVal::I32(b.ship_hdemo_sk),
                ColVal::I32(b.ship_addr_sk),
                i!(b.channel_sk),
                i!(rng.gen_range(1..11_718i32)),
                i!(rng.gen_range(1..20i32)),
                i!(rng.gen_range(1..=whs)),
                i!(item_sk),
                ColVal::I32(b.promo_sk),
                i!(order),
                i!(qty),
                f!(wc),
                f!(lp),
                f!(sp),
                f!(0.0),
                f!(sp * qty as f64),
                f!(wc * qty as f64),
                f!(lp * qty as f64),
                f!(tax),
                f!(0.0),
                f!(ship),
                f!(sp * qty as f64),
                f!(sp * qty as f64 + tax),
                f!(sp * qty as f64 + ship),
                f!(sp * qty as f64 + ship + tax),
                f!(sp * qty as f64 - wc * qty as f64),
            ]
        },
    )
}

fn generate_catalog_returns(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = catalog_returns_rows(scale);
    // FK ranges must track dimension cardinality at this scale;
    // sf1-sized ranges empty out dimension joins at small scales.
    let dims = FkDims::at(scale);
    let ccs = call_center_rows(scale) as i32;
    let whs = warehouse_rows(scale) as i32;
    let reasons = reason_rows(scale) as i32;
    let max_order = returnable_tickets(scale, 1_441_548.0);
    generate_batches(
        catalog_returns_schema(),
        total,
        seed_for_table("catalog_returns"),
        move |_row, rng| {
            let order = rng.gen_range(1..=max_order);
            let b = basket(CATALOG_ORDER_SALT, order, ccs, dims);
            let line = rng.gen_range(0..b.lines);
            let item_sk = b.items[line];
            let qty = rng.gen_range(1..=b.quantities[line]);
            // Returns lag the sale by <=120 days; a uniform draw over the rest of
            // the calendar starved the early years and emptied q91's Nov-1998
            // catalog-returns window. Same bound as the ship_date fields above.
            let ret_date = (b.date_sk + rng.gen_range(1..=120i32)).min(DS_DATE_RANGE);
            let amt = rng.gen_range(10..500i32) as f64;
            let tax = amt * 0.08;
            vec![
                i!(ret_date),
                i!(rng.gen_range(0..86400i32)),
                i!(item_sk),
                ColVal::I32(b.customer_sk),
                ColVal::I32(b.cdemo_sk),
                ColVal::I32(b.hdemo_sk),
                ColVal::I32(b.addr_sk),
                ColVal::I32(b.ship_customer_sk),
                ColVal::I32(b.ship_cdemo_sk),
                ColVal::I32(b.ship_hdemo_sk),
                ColVal::I32(b.ship_addr_sk),
                i!(b.channel_sk),
                i!(rng.gen_range(1..11_718i32)),
                i!(rng.gen_range(1..20i32)),
                i!(rng.gen_range(1..=whs)),
                i!(rng.gen_range(1..=reasons)),
                i!(order),
                i!(qty),
                f!(amt),
                f!(tax),
                f!(amt + tax),
                f!(amt * 0.02),
                f!(amt * 0.05),
                f!(amt * 0.6),
                f!(amt * 0.2),
                f!(amt * 0.2),
                f!(amt * 0.1),
            ]
        },
    )
}

fn generate_web_sales(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = web_sales_rows(scale);
    // FK ranges must track dimension cardinality at this scale;
    // sf1-sized ranges empty out dimension joins at small scales.
    let dims = FkDims::at(scale);
    let wpages = web_page_rows(scale) as i32;
    let wsites = web_site_rows(scale) as i32;
    let whs = warehouse_rows(scale) as i32;
    let mut order: i32 = 0;
    let mut line: usize = 0;
    let mut cur: Option<Basket> = None;
    generate_batches(
        web_sales_schema(),
        total,
        seed_for_table("web_sales"),
        move |_row, rng| {
            let exhausted = match &cur {
                None => true,
                Some(b) => line >= b.lines,
            };
            if exhausted {
                order += 1;
                cur = Some(basket(WEB_ORDER_SALT, order, wsites, dims));
                line = 0;
            }
            let b = cur.as_ref().expect("basket set above");
            let item_sk = b.items[line];
            let qty = b.quantities[line];
            line += 1;
            // Official ws price moments: wholesale 1..100 (avg 50.6), list
            // 1..300 (avg 101), sales 0..299 (avg 50.6). The old wc/10 model
            // capped ws_sales_price at ~$74, leaving q85's 100-150 and 150-200
            // price arms structurally empty.
            let wc = rng.gen_range(100..10_000i32) as f64 / 100.0;
            let lp = ((wc * rng.gen_range(100..300i32) as f64 / 100.0) * 100.0).round() / 100.0;
            let sp = ((lp * rng.gen_range(0..=100i32) as f64 / 100.0) * 100.0).round() / 100.0;
            let tax = sp * 0.08;
            let ship = sp * 0.05 * qty as f64;
            let ship_date = (b.date_sk + rng.gen_range(1..=120i32)).min(DS_DATE_RANGE);
            vec![
                i!(b.date_sk),
                i!(rng.gen_range(0..86400i32)),
                i!(ship_date),
                i!(item_sk),
                ColVal::I32(b.customer_sk),
                ColVal::I32(b.cdemo_sk),
                ColVal::I32(b.hdemo_sk),
                ColVal::I32(b.addr_sk),
                ColVal::I32(b.ship_customer_sk),
                ColVal::I32(b.ship_cdemo_sk),
                ColVal::I32(b.ship_hdemo_sk),
                ColVal::I32(b.ship_addr_sk),
                i!(rng.gen_range(1..=wpages)),
                i!(b.channel_sk),
                i!(rng.gen_range(1..20i32)),
                i!(rng.gen_range(1..=whs)),
                ColVal::I32(b.promo_sk),
                i!(order),
                i!(qty),
                f!(wc),
                f!(lp),
                f!(sp),
                f!(0.0),
                f!(sp * qty as f64),
                f!(wc * qty as f64),
                f!(lp * qty as f64),
                f!(tax),
                f!(0.0),
                f!(ship),
                f!(sp * qty as f64),
                f!(sp * qty as f64 + tax),
                f!(sp * qty as f64 + ship),
                f!(sp * qty as f64 + ship + tax),
                f!(sp * qty as f64 - wc * qty as f64),
            ]
        },
    )
}

fn generate_web_returns(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = web_returns_rows(scale);
    // FK ranges must track dimension cardinality at this scale;
    // sf1-sized ranges empty out dimension joins at small scales.
    let dims = FkDims::at(scale);
    let wpages = web_page_rows(scale) as i32;
    let wsites = web_site_rows(scale) as i32;
    let reasons = reason_rows(scale) as i32;
    let max_order = returnable_tickets(scale, 719_384.0);
    generate_batches(
        web_returns_schema(),
        total,
        seed_for_table("web_returns"),
        move |_row, rng| {
            let order = rng.gen_range(1..=max_order);
            let b = basket(WEB_ORDER_SALT, order, wsites, dims);
            let line = rng.gen_range(0..b.lines);
            let item_sk = b.items[line];
            let qty = rng.gen_range(1..=b.quantities[line]);
            // Returns lag the sale by <=120 days; a uniform draw over the rest of
            // the calendar starved the early years (q85 filters d_year = 2000).
            let ret_date = (b.date_sk + rng.gen_range(1..=120i32)).min(DS_DATE_RANGE);
            let amt = rng.gen_range(10..500i32) as f64;
            let tax = amt * 0.08;
            // In official data the returning party equals the refunded party 100%
            // of the time; q85 correlates cd1 (refunded) with cd2 (returning) on
            // marital + education status, which is unsatisfiable when the two
            // parties are drawn independently.
            vec![
                i!(ret_date),
                i!(rng.gen_range(0..86400i32)),
                i!(item_sk),
                ColVal::I32(b.customer_sk),
                ColVal::I32(b.cdemo_sk),
                ColVal::I32(b.hdemo_sk),
                ColVal::I32(b.addr_sk),
                ColVal::I32(b.customer_sk),
                ColVal::I32(b.cdemo_sk),
                ColVal::I32(b.hdemo_sk),
                ColVal::I32(b.addr_sk),
                i!(rng.gen_range(1..=wpages)),
                i!(rng.gen_range(1..=reasons)),
                i!(order),
                i!(qty),
                f!(amt),
                f!(tax),
                f!(amt + tax),
                f!(amt * 0.02),
                f!(amt * 0.05),
                f!(amt * 0.6),
                f!(amt * 0.2),
                f!(amt * 0.2),
                f!(amt * 0.1),
            ]
        },
    )
}

fn generate_inventory(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = inventory_rows(scale);
    let items = item_rows(scale) as i64;
    let whs = warehouse_rows(scale) as i64;
    // dsdgen inventory is a deterministic weekly snapshot, not a random
    // tuple draw: 261 weekly dates x every warehouse x a FIXED half of the
    // items, each of which then appears in EVERY week (official sf0.1:
    // item 1 has all 261 snapshot dates, 900 of 1800 items per week).
    // q72 joins catalog_sales to inventory on item + same d_week_seq;
    // random (date, item) tuples covered only ~30% of weeks per item and
    // starved that join. Rows per week = items/2 * whs, matching
    // inventory_rows() = 261 * items * whs / 2.
    let per_week = (items / 2).max(1) * whs;
    generate_batches(
        inventory_schema(),
        total,
        seed_for_table("inventory"),
        |row, rng| {
            let week = row as i64 / per_week;
            let j = row as i64 % per_week;
            let item = 2 * (j / whs) + 1;
            let wh = j % whs + 1;
            // date_dim sk 1 = 1998-01-01; weekly snapshots land every 7 days.
            let date_sk = (week * 7 + 2).min(73_048) as i32;
            vec![
                i!(date_sk),
                i!(item.min(items) as i32),
                i!(wh as i32),
                i!(rng.gen_range(0..=1000i32)),
            ]
        },
    )
}

fn generate_date_dim() -> (SchemaRef, Vec<RecordBatch>) {
    // Fixed 73,049 rows: 1998-01-01 to 2003-12-31
    generate_batches(
        date_dim_schema(),
        73_049,
        seed_for_table("date_dim"),
        |row, rng| {
            let sk = (row + 1) as i32;
            let date_val = DS_DATE_START + row as i32;
            let year = 1998 + row as i32 / 366;
            let moy = (row as i32 / 30 % 12) + 1;
            let dom = (row as i32 % 28) + 1;
            let dow = row as i32 % 7;
            let qoy = (moy - 1) / 3 + 1;
            // Spec-anchored sequences: dsdgen counts months/quarters/weeks from
            // 1900, so Jan-2000 has d_month_seq = 1200. The official queries
            // (q22/q54 and the q51/q53/q63/q89 family) filter windows like
            // d_month_seq BETWEEN 1200 AND 1211; a 0-based row/30 counter never
            // intersected them.
            let month_seq = (year - 1900) * 12 + (moy - 1);
            let quarter_seq = (year - 1900) * 4 + (qoy - 1);
            let week_seq = (year - 1900) * 52 + (row as i32 % 366) / 7;
            vec![
                i!(sk),
                s!(format!("AAAA{:09}", sk)),
                d!(date_val),
                i!(month_seq),
                i!(week_seq),
                i!(quarter_seq),
                i!(year),
                i!(dow),
                i!(moy),
                i!(dom),
                i!(qoy),
                i!(year),
                i!(row as i32 / 90),
                i!(row as i32 / 7),
                s!(DAY_NAMES[dow as usize % 7]),
                s!(format!("{}Q{}", year, qoy)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                i!(sk - dom + 1),
                i!(sk - dom + 28),
                i!(sk - 365),
                i!(sk - 91),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
            ]
        },
    )
}

fn generate_time_dim() -> (SchemaRef, Vec<RecordBatch>) {
    generate_batches(
        time_dim_schema(),
        86_400,
        seed_for_table("time_dim"),
        |row, _rng| {
            let sk = (row + 1) as i32;
            let sec = row as i32;
            let hour = sec / 3600;
            let min = (sec % 3600) / 60;
            let s = sec % 60;
            vec![
                i!(sk),
                s!(format!("T{:08}", sk)),
                i!(sec),
                i!(hour),
                i!(min),
                i!(s),
                s!(AM_PM[(hour >= 12) as usize]),
                s!(SHIFTS[hour as usize / 6 % 4]),
                s!(SHIFTS[hour as usize / 3 % 4]),
                s!(MEAL_TIMES[if hour == 7 {
                    0
                } else if hour == 12 {
                    1
                } else if hour == 18 {
                    2
                } else {
                    3
                }]),
            ]
        },
    )
}

fn generate_item(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = item_rows(scale);
    generate_batches(item_schema(), total, seed_for_table("item"), |row, rng| {
        let sk = (row + 1) as i32;
        // dsdgen prices are heavily skewed low (median 4.21, range
        // 0.09..99.89); a uniform draw put only 1.3% of items in the
        // 1.00..2.00 band that q72's qualification parameters probe
        // (dsdgen: ~11%). Log-uniform reproduces the skew.
        let price_ln = rng.gen_range(0.09_f64.ln()..99.99_f64.ln());
        let price = (price_ln.exp() * 100.0).round() / 100.0;
        let wc = price * 0.6;
        // i_manufact is derived from a neighborhood of i_manufact_id (spread
        // MANUFACT_LABEL_SPREAD) so the same manufact string spans several ids
        // and vice versa; q41's correlated subquery bridges items on
        // `i_manufact = i1.i_manufact`, which collapses to id equality (0 rows)
        // when the string is a bijection with the id.
        let manufact_id = rng.gen_range(1..1000i32);
        let manufact_label = ((manufact_id
            + rng.gen_range(0..MANUFACT_LABEL_SPREAD) * MANUFACT_LABEL_STRIDE)
            % 1000)
            + 1;
        // i_category_id and i_category are consistent (1=Women ..
        // 10=Electronics) and i_class draws from that category's official
        // class vocabulary; i_class_id stays independent like dsdgen's.
        let cat = rng.gen_range(0..CATEGORIES.len());
        let class_idx = rng.gen_range(0..CATEGORY_CLASSES[cat].len());
        let class_id = rng.gen_range(1..16i32);
        // Plant i_item_sk 1 as a Women/maternity item (CATEGORIES[0] /
        // CATEGORY_CLASSES[0][2]) for the q54 coincidence (see
        // PLANTED_Q54_TICKETS); the draws above are kept for parity.
        let (cat, class_idx) = if row == 0 {
            (0usize, 2usize)
        } else {
            (cat, class_idx)
        };
        // dsdgen i_brand_id encodes <category><class:3><brand:3> (e.g.
        // 1002001 = category 1, class 2, brand 1); the brand NAME is fully
        // determined by (category, class) with only the #N suffix varying.
        let brand_id = (cat as i32 + 1) * 1_000_000 + class_id * 1000 + rng.gen_range(1..=17i32);
        let brand = format!(
            "{} #{}",
            CATEGORY_CLASS_BRANDS[cat][class_idx],
            rng.gen_range(1..=17i32)
        );
        vec![
            i!(sk),
            s!(random_id(rng)),
            d!(random_date(rng)),
            scd2_rec_end_date(row, rng),
            s!(random_name(rng)),
            f!(price),
            f!(wc),
            i!(brand_id),
            s!(brand),
            i!(class_id),
            s!(CATEGORY_CLASSES[cat][class_idx]),
            i!((cat + 1) as i32),
            s!(CATEGORIES[cat]),
            i!(manufact_id),
            s!(format!("manufact#{manufact_label}")),
            s!(random_str(rng, ITEM_SIZES)),
            s!(random_id(rng)),
            // i_item_sk 3 (row 2) is forced to peach for the q24 plant; the
            // draw is kept so the rest of the color histogram is unshifted.
            s!({
                let c = weighted_str(rng, ITEM_COLOR_WEIGHTS);
                if row == 2 {
                    "peach"
                } else {
                    c
                }
            }),
            s!(random_str(rng, ITEM_UNITS)),
            s!(random_str(rng, ITEM_SIZES)),
            i!(rng.gen_range(1..100i32)),
            s!(random_name(rng)),
        ]
    })
}

fn generate_customer(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = customer_rows(scale);
    // FK ranges must track dimension cardinality at this scale;
    // sf1-sized ranges empty out dimension joins at small scales.
    let cdemos = customer_demographics_rows(scale) as i32;
    let addrs = customer_address_rows(scale) as i32;
    generate_batches(
        customer_schema(),
        total,
        seed_for_table("customer"),
        |row, rng| {
            let sk = (row + 1) as i32;
            let addr_draw = rng.gen_range(1..=addrs);
            // Customers 1..=PLANTED_Q54_TICKETS point at the planted Williamson
            // County, TN addresses for q54, and customer Q24_CUSTOMER points at the
            // Q24_ZIP address Q24_ADDR_SK for q24 (both use addr = customer sk); the
            // draw above is kept for parity.
            let addr_sk = if (row as i32) < PLANTED_Q54_TICKETS || (row as i32) == Q24_CUSTOMER - 1
            {
                sk
            } else {
                addr_draw
            };
            vec![
                i!(sk),
                s!(random_id(rng)),
                i!(rng.gen_range(1..=cdemos)),
                i!(rng.gen_range(1..7200i32)),
                i!(addr_sk),
                i!(random_date_sk(rng)),
                i!(random_date_sk(rng)),
                s!(random_str(rng, SALUTATIONS)),
                s!(random_name(rng)),
                s!(random_name(rng)),
                s!(random_str(rng, YN)),
                i!(rng.gen_range(1..28i32)),
                i!(rng.gen_range(1..12i32)),
                i!(rng.gen_range(1920..2000i32)),
                s!("US"),
                s!(random_id(rng)),
                s!(format!("{}@{}.com", random_name(rng), random_name(rng))),
                i!(rng.gen_range(1..73049i32)),
            ]
        },
    )
}

fn generate_customer_address(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = customer_address_rows(scale);
    // Each county maps to exactly one state (official ca: ~1.7 states per
    // county). Drawing county and state independently from 100 x 50 put stores
    // and customers in mismatched states and emptied q54's ca_county = s_county
    // AND ca_state = s_state join. County index 0 ('Williamson County') pairs
    // with 'TN' so customer addresses stay join-compatible with the stores
    // (all in Williamson County, TN); the rest cycle through STATES.
    let tn_idx = STATES
        .iter()
        .position(|&s| s == "TN")
        .expect("TN in STATES");
    generate_batches(
        customer_address_schema(),
        total,
        seed_for_table("customer_address"),
        move |row, rng| {
            let sk = (row + 1) as i32;
            let county_idx = rng.gen_range(0..COUNTIES.len());
            // Plant addresses 1..=PLANTED_Q54_TICKETS in Williamson County, TN so
            // customers 1..=N (whose c_current_addr_sk points here) satisfy q54.
            let (county, state) = if (row as i32) < PLANTED_Q54_TICKETS {
                ("Williamson County", "TN")
            } else {
                (
                    COUNTIES[county_idx],
                    STATES[(county_idx + tn_idx) % STATES.len()],
                )
            };
            vec![
                i!(sk),
                s!(random_id(rng)),
                s!(format!("{}", rng.gen_range(1..9999i32))),
                s!(random_name(rng)),
                s!(random_str(rng, STREET_TYPES)),
                s!(format!("Suite {}", rng.gen_range(1..999i32))),
                s!(random_str(rng, CA_CITIES)),
                s!(county),
                // ca_address_sk 4 (row 3) is forced to Q24_ZIP so it matches store
                // sk 3's s_zip for q24; customer 4 points c_current_addr_sk here.
                s!(state),
                s!({
                    let z = random_str(rng, ZIP_POOL);
                    if (row as i32) == Q24_ADDR_SK - 1 {
                        Q24_ZIP
                    } else {
                        z
                    }
                }),
                s!("United States"),
                f!(GMT_OFFSETS[rng.gen_range(0..GMT_OFFSETS.len())]),
                s!(random_str(rng, &["city", "suburb", "rural", "unknown"])),
            ]
        },
    )
}

fn generate_customer_demographics(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = customer_demographics_rows(scale);
    generate_batches(
        customer_demographics_schema(),
        total,
        seed_for_table("customer_demographics"),
        |row, rng| {
            vec![
                i!((row + 1) as i32),
                s!(random_str(rng, GENDERS)),
                s!(random_str(rng, MARITAL)),
                s!(random_str(rng, EDUCATION)),
                i!(rng.gen_range(0..10_000i32) / 100 * 100),
                s!(random_str(rng, CREDIT)),
                i!(rng.gen_range(0..6i32)),
                i!(rng.gen_range(0..4i32)),
                i!(rng.gen_range(0..4i32)),
            ]
        },
    )
}

fn generate_household_demographics() -> (SchemaRef, Vec<RecordBatch>) {
    // dsdgen's hd table is a deterministic cross product, not a random draw:
    // 7200 = 20 income bands x 6 buy potentials x 10 dep counts x 6 vehicle
    // counts, with sk-derived strides (verified against CALL dsdgen(sf=0.1)).
    // Random draws capped hd_dep_count at 5 and hd_vehicle_count at 2, so the
    // q34/q46/q68/q73/q79 family's dep/vehicle windows matched nothing.
    const VEHICLE_COUNTS: [i32; 6] = [0, 1, 2, 3, 4, -1];
    generate_batches(
        household_demographics_schema(),
        7_200,
        seed_for_table("household_demographics"),
        |row, _rng| {
            let sk = (row + 1) as i32;
            vec![
                i!(sk),
                i!((sk % 20) + 1),
                s!(BUY_POTENTIAL[(sk as usize / 20) % 6]),
                i!((sk / 120) % 10),
                i!(VEHICLE_COUNTS[(sk as usize / 1200) % 6]),
            ]
        },
    )
}

fn generate_store(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = store_rows(scale);
    generate_batches(
        store_schema(),
        total,
        seed_for_table("store"),
        |row, rng| {
            let sk = (row + 1) as i32;
            // Official sf1: s_closed_date_sk is set on 3 of every 12 rows
            // (positions 0, 3, 4), NULL elsewhere; the single sf0.01 row has one.
            let closed = if matches!(row % 12, 0 | 3 | 4) {
                i!(rng.gen_range(1..73049i32))
            } else {
                ColVal::I32(None)
            };
            vec![
                i!(sk),
                s!(random_id(rng)),
                d!(random_date(rng)),
                scd2_rec_end_date(row, rng),
                closed,
                s!(random_name(rng)),
                i!(rng.gen_range(10..500i32)),
                i!(rng.gen_range(1000..100_000i32)),
                s!(random_str(rng, CC_HOURS)),
                s!(random_name(rng)),
                // s_store_sk 3 (row 2) is forced to market 8 for the q24 plant.
                i!({
                    let m = rng.gen_range(1..10i32);
                    if row == 2 {
                        8
                    } else {
                        m
                    }
                }),
                s!("Unknown"),
                s!(random_name(rng)),
                s!(random_name(rng)),
                i!(rng.gen_range(1..10i32)),
                s!("Division"),
                i!(rng.gen_range(1..6i32)),
                s!("Company"),
                s!(format!("{}", rng.gen_range(1..999i32))),
                s!(random_name(rng)),
                s!(random_str(rng, STREET_TYPES)),
                s!(format!("Suite {}", rng.gen_range(1..99i32))),
                // dsdgen stores at sf <= 1 all sit in Midway/Fairview,
                // Williamson County, TN; q01 filters s_state = 'TN' and
                // q34/q46/q68/q79 probe these exact city/county names.
                s!(STORE_CITIES[row % STORE_CITIES.len()]),
                s!("Williamson County"),
                // s_store_sk 3 (row 2) is forced to Q24_ZIP so its s_zip matches
                // the planted buyer's ca_zip for q24.
                s!("TN"),
                s!({
                    let z = random_str(rng, ZIP_POOL);
                    if row == 2 {
                        Q24_ZIP
                    } else {
                        z
                    }
                }),
                s!("United States"),
                f!(GMT_OFFSETS[row % GMT_OFFSETS.len()]),
                f!(rng.gen_range(0..15i32) as f64 / 100.0),
            ]
        },
    )
}

fn generate_catalog_page(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = catalog_page_rows(scale);
    generate_batches(
        catalog_page_schema(),
        total,
        seed_for_table("catalog_page"),
        |row, rng| {
            let sk = (row + 1) as i32;
            vec![
                i!(sk),
                s!(random_id(rng)),
                i!(rng.gen_range(1..73049i32)),
                i!(rng.gen_range(1..73049i32)),
                s!(random_str(rng, DEPT)),
                i!(rng.gen_range(1..100i32)),
                i!(sk),
                s!(random_name(rng)),
                s!(random_str(rng, WP_TYPES)),
            ]
        },
    )
}

fn generate_web_site(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = web_site_rows(scale);
    generate_batches(
        web_site_schema(),
        total,
        seed_for_table("web_site"),
        |row, rng| {
            let sk = (row + 1) as i32;
            // Official: web_close_date_sk is NULL only on single-revision
            // entities (cycle position 0): 100% NULL at sf0.01, 5/30 at sf1.
            let close = if row % 6 == 0 {
                ColVal::I32(None)
            } else {
                i!(rng.gen_range(1..73049i32))
            };
            vec![
                i!(sk),
                s!(random_id(rng)),
                d!(random_date(rng)),
                scd2_rec_end_date(row, rng),
                s!(random_name(rng)),
                i!(rng.gen_range(1..73049i32)),
                close,
                s!("Unknown"),
                s!(random_name(rng)),
                i!(rng.gen_range(1..10i32)),
                s!(random_name(rng)),
                s!(random_name(rng)),
                s!(random_name(rng)),
                i!(rng.gen_range(1..6i32)),
                s!("web"),
                s!(format!("{}", rng.gen_range(1..999i32))),
                s!(random_name(rng)),
                s!(random_str(rng, STREET_TYPES)),
                s!(format!("Suite {}", rng.gen_range(1..99i32))),
                s!(random_name(rng)),
                s!(random_name(rng)),
                s!(random_str(rng, STATES)),
                s!(format!("{:05}", rng.gen_range(10000..99999i32))),
                s!("United States"),
                f!(GMT_OFFSETS[row % GMT_OFFSETS.len()]),
                f!(rng.gen_range(0..15i32) as f64 / 100.0),
            ]
        },
    )
}

fn generate_web_page(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = web_page_rows(scale);
    // FK ranges must track dimension cardinality at this scale;
    // sf1-sized ranges empty out dimension joins at small scales.
    let customers = customer_rows(scale) as i32;
    generate_batches(
        web_page_schema(),
        total,
        seed_for_table("web_page"),
        |row, rng| {
            let sk = (row + 1) as i32;
            // dsdgen leaves wp_customer_sk NULL on most pages (only
            // customer-specific pages carry one), and the fraction is
            // scale-dependent: 5/6 at sf0.1 (6 rows), ~0.65 at sf1 (60 rows).
            // A deterministic per-scale stripe keeps this tiny table inside the
            // validator's 0.10 null-fraction tolerance at both scales; a random
            // draw has far too much variance on 6 rows.
            let non_null_stride = if total <= 30 { 6 } else { 3 };
            let customer = if row % non_null_stride == 0 {
                i!(rng.gen_range(1..=customers))
            } else {
                ColVal::I32(None)
            };
            vec![
                i!(sk),
                s!(random_id(rng)),
                d!(random_date(rng)),
                scd2_rec_end_date(row, rng),
                i!(rng.gen_range(1..73049i32)),
                i!(rng.gen_range(1..73049i32)),
                s!(random_str(rng, YN)),
                customer,
                s!(format!("http://{}.com/{}", random_name(rng), sk)),
                s!(random_str(rng, WP_TYPES)),
                i!(rng.gen_range(0..100_000i32)),
                i!(rng.gen_range(0..25i32)),
                i!(rng.gen_range(0..20i32)),
                i!(rng.gen_range(0..4i32)),
            ]
        },
    )
}

fn generate_warehouse(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = warehouse_rows(scale);
    generate_batches(
        warehouse_schema(),
        total,
        seed_for_table("warehouse"),
        |row, rng| {
            let sk = (row + 1) as i32;
            // dsdgen leaves the name/size/address fields NULL on ~1 in 5
            // warehouses at sf1 (sf0.1's single warehouse is fully populated;
            // row % 5 == 4 reproduces both).
            let sparse = row % 5 == 4;
            let name = random_name(rng);
            let sq_ft = rng.gen_range(50_000..1_000_000i32);
            let street_no = format!("{}", rng.gen_range(1..999i32));
            let street_name = random_name(rng);
            let street_type = random_str(rng, STREET_TYPES).to_string();
            let suite = format!("Suite {}", rng.gen_range(1..99i32));
            let gmt = rng.gen_range(-12..12i32) as f64;
            let opt = |v: String| {
                if sparse {
                    ColVal::Str(None)
                } else {
                    ColVal::Str(Some(v))
                }
            };
            vec![
                i!(sk),
                s!(random_id(rng)),
                opt(name),
                if sparse { ColVal::I32(None) } else { i!(sq_ft) },
                opt(street_no),
                opt(street_name),
                opt(street_type),
                opt(suite),
                s!(random_name(rng)),
                s!(random_name(rng)),
                s!(random_str(rng, STATES)),
                s!(format!("{:05}", rng.gen_range(10000..99999i32))),
                s!("United States"),
                if sparse { ColVal::F64(None) } else { f!(gmt) },
            ]
        },
    )
}

fn generate_promotion(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = promotion_rows(scale);
    generate_batches(
        promotion_schema(),
        total,
        seed_for_table("promotion"),
        |row, rng| {
            let sk = (row + 1) as i32;
            vec![
                i!(sk),
                s!(random_id(rng)),
                i!(rng.gen_range(1..73049i32)),
                i!(rng.gen_range(1..73049i32)),
                i!(rng.gen_range(1..18_000i32)),
                f!(rng.gen_range(0..1_000_000i32) as f64 / 100.0),
                i!(1),
                s!(random_name(rng)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                s!(random_str(rng, YN)),
                s!(random_name(rng)),
                s!(random_str(rng, PROMO_PURPOSES)),
                s!(random_str(rng, YN)),
            ]
        },
    )
}

fn generate_reason(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = reason_rows(scale);
    generate_batches(
        reason_schema(),
        total,
        seed_for_table("reason"),
        |row, _rng| {
            let sk = (row + 1) as i32;
            vec![
                i!(sk),
                s!(format!("AAAAAAAAA{:06}", sk)),
                s!(format!("reason {}", sk)),
            ]
        },
    )
}

fn generate_income_band() -> (SchemaRef, Vec<RecordBatch>) {
    generate_batches(
        income_band_schema(),
        20,
        seed_for_table("income_band"),
        |row, _rng| {
            let sk = (row + 1) as i32;
            let lower = (row as i32) * 10_000;
            let upper = lower + 9_999;
            vec![i!(sk), i!(lower), i!(upper)]
        },
    )
}

fn generate_ship_mode() -> (SchemaRef, Vec<RecordBatch>) {
    generate_batches(
        ship_mode_schema(),
        20,
        seed_for_table("ship_mode"),
        |row, rng| {
            let sk = (row + 1) as i32;
            vec![
                i!(sk),
                s!(random_id(rng)),
                s!(random_str(rng, SHIP_TYPES)),
                s!(random_str(rng, SHIP_CODES)),
                s!(random_str(rng, CARRIERS)),
                s!(random_id(rng)),
            ]
        },
    )
}

fn generate_call_center(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = call_center_rows(scale);
    generate_batches(
        call_center_schema(),
        total,
        seed_for_table("call_center"),
        |row, rng| {
            let sk = (row + 1) as i32;
            vec![
                // cc_closed_date_sk is 100% NULL in official data at both scales.
                i!(sk),
                s!(random_id(rng)),
                d!(random_date(rng)),
                scd2_rec_end_date(row, rng),
                ColVal::I32(None),
                i!(rng.gen_range(1..73049i32)),
                s!(random_name(rng)),
                s!(random_str(rng, CC_CLASSES)),
                i!(rng.gen_range(100..5000i32)),
                i!(rng.gen_range(1000..100_000i32)),
                s!(random_str(rng, CC_HOURS)),
                s!(random_name(rng)),
                i!(rng.gen_range(1..10i32)),
                s!(random_name(rng)),
                s!(random_name(rng)),
                s!(random_name(rng)),
                i!(rng.gen_range(1..6i32)),
                s!("Division"),
                i!(rng.gen_range(1..6i32)),
                s!("Company"),
                s!(format!("{}", rng.gen_range(1..999i32))),
                s!(random_name(rng)),
                s!(random_str(rng, STREET_TYPES)),
                s!(format!("Suite {}", rng.gen_range(1..99i32))),
                s!(random_name(rng)),
                s!(random_name(rng)),
                s!(random_str(rng, STATES)),
                s!(format!("{:05}", rng.gen_range(10000..99999i32))),
                s!("United States"),
                f!(GMT_OFFSETS[row % GMT_OFFSETS.len()]),
                f!(rng.gen_range(0..15i32) as f64 / 100.0),
            ]
        },
    )
}

// ---------------------------------------------------------------------------
// BenchmarkGenerator impl
// ---------------------------------------------------------------------------

impl BenchmarkGenerator for TpcdsGenerator {
    fn name(&self) -> &str {
        "tpcds"
    }

    fn tables(&self) -> Vec<TableDef> {
        vec![
            // Fact tables
            TableDef {
                name: "store_sales".into(),
                schema: store_sales_schema(),
                row_count: store_sales_rows,
            },
            TableDef {
                name: "store_returns".into(),
                schema: store_returns_schema(),
                row_count: store_returns_rows,
            },
            TableDef {
                name: "catalog_sales".into(),
                schema: catalog_sales_schema(),
                row_count: catalog_sales_rows,
            },
            TableDef {
                name: "catalog_returns".into(),
                schema: catalog_returns_schema(),
                row_count: catalog_returns_rows,
            },
            TableDef {
                name: "web_sales".into(),
                schema: web_sales_schema(),
                row_count: web_sales_rows,
            },
            TableDef {
                name: "web_returns".into(),
                schema: web_returns_schema(),
                row_count: web_returns_rows,
            },
            TableDef {
                name: "inventory".into(),
                schema: inventory_schema(),
                row_count: inventory_rows,
            },
            // Dimension tables
            TableDef {
                name: "date_dim".into(),
                schema: date_dim_schema(),
                row_count: date_dim_rows,
            },
            TableDef {
                name: "time_dim".into(),
                schema: time_dim_schema(),
                row_count: time_dim_rows,
            },
            TableDef {
                name: "item".into(),
                schema: item_schema(),
                row_count: item_rows,
            },
            TableDef {
                name: "customer".into(),
                schema: customer_schema(),
                row_count: customer_rows,
            },
            TableDef {
                name: "customer_address".into(),
                schema: customer_address_schema(),
                row_count: customer_address_rows,
            },
            TableDef {
                name: "customer_demographics".into(),
                schema: customer_demographics_schema(),
                row_count: customer_demographics_rows,
            },
            TableDef {
                name: "household_demographics".into(),
                schema: household_demographics_schema(),
                row_count: household_demographics_rows,
            },
            TableDef {
                name: "store".into(),
                schema: store_schema(),
                row_count: store_rows,
            },
            TableDef {
                name: "catalog_page".into(),
                schema: catalog_page_schema(),
                row_count: catalog_page_rows,
            },
            TableDef {
                name: "web_site".into(),
                schema: web_site_schema(),
                row_count: web_site_rows,
            },
            TableDef {
                name: "web_page".into(),
                schema: web_page_schema(),
                row_count: web_page_rows,
            },
            TableDef {
                name: "warehouse".into(),
                schema: warehouse_schema(),
                row_count: warehouse_rows,
            },
            TableDef {
                name: "promotion".into(),
                schema: promotion_schema(),
                row_count: promotion_rows,
            },
            TableDef {
                name: "reason".into(),
                schema: reason_schema(),
                row_count: reason_rows,
            },
            TableDef {
                name: "income_band".into(),
                schema: income_band_schema(),
                row_count: income_band_rows,
            },
            TableDef {
                name: "ship_mode".into(),
                schema: ship_mode_schema(),
                row_count: ship_mode_rows,
            },
            TableDef {
                name: "call_center".into(),
                schema: call_center_schema(),
                row_count: call_center_rows,
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

        let (tbl_schema, batches) = match table {
            "store_sales" => generate_store_sales(scale),
            "store_returns" => generate_store_returns(scale),
            "catalog_sales" => generate_catalog_sales(scale),
            "catalog_returns" => generate_catalog_returns(scale),
            "web_sales" => generate_web_sales(scale),
            "web_returns" => generate_web_returns(scale),
            "inventory" => generate_inventory(scale),
            "date_dim" => generate_date_dim(),
            "time_dim" => generate_time_dim(),
            "item" => generate_item(scale),
            "customer" => generate_customer(scale),
            "customer_address" => generate_customer_address(scale),
            "customer_demographics" => generate_customer_demographics(scale),
            "household_demographics" => generate_household_demographics(),
            "store" => generate_store(scale),
            "catalog_page" => generate_catalog_page(scale),
            "web_site" => generate_web_site(scale),
            "web_page" => generate_web_page(scale),
            "warehouse" => generate_warehouse(scale),
            "promotion" => generate_promotion(scale),
            "reason" => generate_reason(scale),
            "income_band" => generate_income_band(),
            "ship_mode" => generate_ship_mode(),
            "call_center" => generate_call_center(scale),
            _ => anyhow::bail!("Unknown TPC-DS table: {table}"),
        };

        let full_output = format!("{output_dir}/tpcds/sf{scale}");
        let (files, bytes) =
            parquet_writer::write_parquet_files(&batches, tbl_schema, &full_output, table)?;
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
    fn test_table_count() {
        let gen = TpcdsGenerator;
        assert_eq!(gen.tables().len(), 24);
    }

    #[test]
    fn test_all_table_names_present() {
        let gen = TpcdsGenerator;
        let tables = gen.tables();
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        for expected in &[
            "store_sales",
            "store_returns",
            "catalog_sales",
            "catalog_returns",
            "web_sales",
            "web_returns",
            "inventory",
            "date_dim",
            "time_dim",
            "item",
            "customer",
            "customer_address",
            "customer_demographics",
            "household_demographics",
            "store",
            "catalog_page",
            "web_site",
            "web_page",
            "warehouse",
            "promotion",
            "reason",
            "income_band",
            "ship_mode",
            "call_center",
        ] {
            assert!(names.contains(expected), "missing table: {expected}");
        }
    }

    #[test]
    fn test_row_counts_sf001() {
        // Pinned to official dsdgen sf0.01 output. Tables generated from
        // per-sale sampling in dsdgen (returns) match within the 15%
        // harness tolerance; everything else must be exact.
        let sf = 0.01_f64;
        let expected: &[(&str, usize)] = &[
            ("store_sales", 28_800),    // official 28,810
            ("store_returns", 2_879),   // official 2,810
            ("catalog_sales", 14_415),  // official 14,313
            ("catalog_returns", 1_440), // official 1,358
            ("web_sales", 7_193),       // official 7,212
            ("web_returns", 717),       // official 679
            ("inventory", 23_490),
            ("date_dim", 73_049),
            ("time_dim", 86_400),
            ("item", 180),
            ("customer", 1_000),
            ("customer_address", 500),
            ("customer_demographics", 19_208),
            ("household_demographics", 7_200),
            ("store", 1),
            ("catalog_page", 11_718),
            ("web_site", 1),
            ("web_page", 1),
            ("warehouse", 1),
            ("promotion", 3),
            ("reason", 1),
            ("income_band", 20),
            ("ship_mode", 20),
            ("call_center", 1),
        ];
        let gen = TpcdsGenerator;
        for t in gen.tables() {
            let want = expected.iter().find(|(n, _)| *n == t.name).unwrap().1;
            assert_eq!((t.row_count)(sf), want, "table {} at SF0.01", t.name);
        }
    }

    #[test]
    fn test_row_counts_sf1() {
        // Pinned to official dsdgen sf1 output (returns within tolerance).
        let expected: &[(&str, usize)] = &[
            ("store_sales", 2_880_000), // official 2,880,404
            ("store_returns", 287_999), // official 287,867
            ("catalog_sales", 1_441_548),
            ("catalog_returns", 144_067),
            ("web_sales", 719_384),
            ("web_returns", 71_763), // official 71,654
            ("inventory", 11_745_000),
            ("date_dim", 73_049),
            ("time_dim", 86_400),
            ("item", 18_000),
            ("customer", 100_000),
            ("customer_address", 50_000),
            ("customer_demographics", 1_920_800),
            ("household_demographics", 7_200),
            ("store", 12),
            ("catalog_page", 11_718),
            ("web_site", 30),
            ("web_page", 60),
            ("warehouse", 5),
            ("promotion", 300),
            ("reason", 35),
            ("income_band", 20),
            ("ship_mode", 20),
            ("call_center", 6),
        ];
        let gen = TpcdsGenerator;
        for t in gen.tables() {
            let want = expected.iter().find(|(n, _)| *n == t.name).unwrap().1;
            assert_eq!((t.row_count)(1.0), want, "table {} at SF1", t.name);
        }
    }

    #[test]
    fn test_fixed_row_counts() {
        // Tables that must not scale: identical at every scale factor.
        // customer_demographics scales linearly below sf1 then caps.
        let gen = TpcdsGenerator;
        for t in gen.tables() {
            match t.name.as_str() {
                "date_dim" => assert_eq!((t.row_count)(10.0), 73_049),
                "time_dim" => assert_eq!((t.row_count)(10.0), 86_400),
                "customer_demographics" => assert_eq!((t.row_count)(10.0), 1_920_800),
                "household_demographics" => assert_eq!((t.row_count)(10.0), 7_200),
                "catalog_page" => assert_eq!((t.row_count)(0.01), 11_718),
                "income_band" => assert_eq!((t.row_count)(10.0), 20),
                "ship_mode" => assert_eq!((t.row_count)(10.0), 20),
                _ => {}
            }
        }
    }

    #[test]
    fn test_schema_spot_checks() {
        // store_sales: 23 columns
        assert_eq!(store_sales_schema().fields().len(), 23);
        // inventory: 4 columns
        assert_eq!(inventory_schema().fields().len(), 4);
        // date_dim: 28 columns
        assert_eq!(date_dim_schema().fields().len(), 28);
        // customer_demographics: 9 columns
        assert_eq!(customer_demographics_schema().fields().len(), 9);
        // income_band: 3 columns
        assert_eq!(income_band_schema().fields().len(), 3);
    }

    #[test]
    fn test_generate_store_sales_sf001() {
        let (sch, batches) = generate_store_sales(0.01);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, (0.01_f64 * 2_880_000.0) as usize);
        assert_eq!(batches[0].schema(), sch);
    }

    #[test]
    fn test_generate_inventory_sf001() {
        let (sch, batches) = generate_inventory(0.01);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 261 weekly snapshots x (180 items x 1 warehouse) / 2
        assert_eq!(rows, 23_490);
        assert_eq!(batches[0].schema(), sch);
    }

    #[test]
    fn test_generate_date_dim() {
        let (sch, batches) = generate_date_dim();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 73_049);
        assert_eq!(batches[0].schema(), sch);
    }

    #[test]
    fn test_generate_time_dim() {
        let (sch, batches) = generate_time_dim();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 86_400);
        assert_eq!(batches[0].schema(), sch);
    }

    #[test]
    fn test_generate_reason() {
        let (sch, batches) = generate_reason(1.0);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 35);
        assert_eq!(batches[0].schema(), sch);
        let (_, batches) = generate_reason(0.01);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
    }

    #[test]
    fn test_generate_income_band() {
        let (sch, batches) = generate_income_band();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 20);
        assert_eq!(batches[0].schema(), sch);
    }

    #[test]
    fn test_generate_ship_mode() {
        let (sch, batches) = generate_ship_mode();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 20);
        assert_eq!(batches[0].schema(), sch);
    }

    #[test]
    fn test_generate_customer_sf001() {
        let (sch, batches) = generate_customer(0.01);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, (0.01_f64 * 100_000.0) as usize);
        assert_eq!(batches[0].schema(), sch);
    }

    #[test]
    fn test_generate_item_sf001() {
        let (sch, batches) = generate_item(0.01);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, (0.01_f64 * 18_000.0) as usize);
        assert_eq!(batches[0].schema(), sch);
    }

    #[test]
    fn fact_date_sks_join_date_dim() {
        use arrow_array::Array as _;
        // Every fact *_date_sk must be a valid date_dim surrogate key inside
        // the 1998-2003 sales window. These columns were silently NULL (Date
        // values coerced into Int32 columns), which emptied every date join
        // and made 74/99 compare queries vacuous.
        type FactGen = fn(f64) -> (SchemaRef, Vec<RecordBatch>);
        let facts: &[(&str, FactGen)] = &[
            ("store_sales", |s| generate_store_sales(s)),
            ("store_returns", |s| generate_store_returns(s)),
            ("catalog_sales", |s| generate_catalog_sales(s)),
            ("catalog_returns", |s| generate_catalog_returns(s)),
            ("web_sales", |s| generate_web_sales(s)),
            ("web_returns", |s| generate_web_returns(s)),
            ("inventory", |s| generate_inventory(s)),
        ];
        for (name, gen) in facts {
            let (sch, batches) = gen(0.001);
            for (idx, field) in sch.fields().iter().enumerate() {
                if !field.name().ends_with("_date_sk") {
                    continue;
                }
                for b in &batches {
                    let col = b
                        .column(idx)
                        .as_any()
                        .downcast_ref::<arrow_array::Int32Array>()
                        .unwrap();
                    assert_eq!(col.null_count(), 0, "{name}.{} has NULLs", field.name());
                    for i in 0..col.len() {
                        let sk = col.value(i);
                        assert!(
                            (1..=DS_DATE_RANGE).contains(&sk),
                            "{name}.{} sk {sk} outside date_dim sales window",
                            field.name()
                        );
                    }
                }
            }
        }
    }

    // -- column extraction helpers -----------------------------------------

    fn col_i32(batches: &[RecordBatch], sch: &SchemaRef, name: &str) -> Vec<Option<i32>> {
        use arrow_array::Array as _;
        let idx = sch.index_of(name).unwrap();
        batches
            .iter()
            .flat_map(|b| {
                let a = b
                    .column(idx)
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap();
                (0..a.len())
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn col_f64(batches: &[RecordBatch], sch: &SchemaRef, name: &str) -> Vec<f64> {
        use arrow_array::Array as _;
        let idx = sch.index_of(name).unwrap();
        batches
            .iter()
            .flat_map(|b| {
                let a = b
                    .column(idx)
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .unwrap();
                (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
            })
            .collect()
    }

    fn col_str(batches: &[RecordBatch], sch: &SchemaRef, name: &str) -> Vec<String> {
        use arrow_array::Array as _;
        let idx = sch.index_of(name).unwrap();
        batches
            .iter()
            .flat_map(|b| {
                let a = b
                    .column(idx)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                (0..a.len())
                    .map(|i| a.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn store_sales_tickets_have_multi_line_baskets() {
        use std::collections::HashMap;
        // q34/q46/q68/q73/q79 GROUP BY ss_ticket_number HAVING count between
        // 15 and 20; one line per ticket made them all empty.
        let (sch, batches) = generate_store_sales(0.1);
        let tickets = col_i32(&batches, &sch, "ss_ticket_number");
        let customers = col_i32(&batches, &sch, "ss_customer_sk");
        let dates = col_i32(&batches, &sch, "ss_sold_date_sk");
        let stores = col_i32(&batches, &sch, "ss_store_sk");
        let mut counts: HashMap<i32, usize> = HashMap::new();
        for t in &tickets {
            *counts.entry(t.unwrap()).or_default() += 1;
        }
        assert!(
            counts.values().any(|c| (15..=20).contains(c)),
            "no ticket with 15..20 line items"
        );
        // All lines of one ticket share the basket header fields.
        let (&sample, _) = counts.iter().find(|(_, c)| **c >= 2).unwrap();
        let rows: Vec<usize> = tickets
            .iter()
            .enumerate()
            .filter(|(_, t)| **t == Some(sample))
            .map(|(i, _)| i)
            .collect();
        let first = rows[0];
        for &r in &rows[1..] {
            assert_eq!(
                customers[r], customers[first],
                "ticket {sample} mixes customers"
            );
            assert_eq!(dates[r], dates[first], "ticket {sample} mixes dates");
            assert_eq!(stores[r], stores[first], "ticket {sample} mixes stores");
        }
    }

    #[test]
    fn store_returns_match_recomputed_sales_baskets() {
        // Every (sr_ticket_number, sr_item_sk) must exist in store_sales.
        // Verified against the deterministic basket derivation instead of
        // materializing the sales table: that derivation IS the contract.
        let scale = 0.01;
        let (sch, batches) = generate_store_returns(scale);
        let tickets = col_i32(&batches, &sch, "sr_ticket_number");
        let items = col_i32(&batches, &sch, "sr_item_sk");
        let customers = col_i32(&batches, &sch, "sr_customer_sk");
        let qtys = col_i32(&batches, &sch, "sr_return_quantity");
        let dates = col_i32(&batches, &sch, "sr_returned_date_sk");
        let max_ticket = returnable_tickets(scale, 2_880_000.0);
        for r in 0..tickets.len() {
            let ticket = tickets[r].unwrap();
            assert!(
                (1..=max_ticket).contains(&ticket),
                "row {r}: ticket {ticket} beyond returnable domain {max_ticket}"
            );
            let b = basket(
                STORE_TICKET_SALT,
                ticket,
                store_rows(scale) as i32,
                FkDims::at(scale),
            );
            let item = items[r].unwrap();
            let lines: Vec<usize> = (0..b.lines).filter(|&l| b.items[l] == item).collect();
            assert!(
                !lines.is_empty(),
                "row {r}: item {item} not in ticket {ticket} basket"
            );
            let qty = qtys[r].unwrap();
            assert!(
                lines.iter().any(|&l| qty <= b.quantities[l]),
                "row {r}: return qty {qty} exceeds sold qty for ticket {ticket}"
            );
            assert_eq!(
                customers[r], b.customer_sk,
                "row {r}: sr_customer_sk diverges from the sale's customer"
            );
            let ret_date = dates[r].unwrap();
            assert!(
                ret_date >= b.date_sk && ret_date <= DS_DATE_RANGE,
                "row {r}: returned before sold"
            );
        }
    }

    #[test]
    fn date_dim_month_seq_anchored_to_spec() {
        // dsdgen anchors month_seq at 1900, so Jan-2000 = 1200; q22/q54
        // filter d_month_seq windows like 1200..1211.
        let (sch, batches) = generate_date_dim();
        let years = col_i32(&batches, &sch, "d_year");
        let moys = col_i32(&batches, &sch, "d_moy");
        let seqs = col_i32(&batches, &sch, "d_month_seq");
        let mut found = false;
        for r in 0..years.len() {
            if years[r] == Some(2000) && moys[r] == Some(1) {
                assert_eq!(
                    seqs[r],
                    Some(1200),
                    "row {r}: 2000-01 must have d_month_seq 1200"
                );
                found = true;
            }
        }
        assert!(found, "no date_dim row for year 2000 moy 1");
    }

    #[test]
    fn store_gmt_offsets_cover_minus_five() {
        // q43/q56/q62/q99 filter gmt_offset = -5; with 12 stores a uniform
        // -12..12 draw could miss -5 entirely. Cycling guarantees coverage.
        let (sch, batches) = generate_store(1.0);
        let offsets = col_f64(&batches, &sch, "s_gmt_offset");
        assert!(offsets.contains(&-5.0), "no store with gmt_offset -5");
        assert!(
            offsets.iter().all(|o| GMT_OFFSETS.contains(o)),
            "store gmt_offset outside the US retail set"
        );
    }

    #[test]
    fn customer_address_cities_include_edgewood() {
        // q84 and the city legs of q46/q68/q79 probe fixed city names.
        let (sch, batches) = generate_customer_address(0.1);
        let cities = col_str(&batches, &sch, "ca_city");
        assert!(
            cities.iter().any(|c| c == "Edgewood"),
            "ca_city never draws 'Edgewood'"
        );
        assert!(
            cities.iter().all(|c| CA_CITIES.contains(&c.as_str())),
            "ca_city outside the fixed city list"
        );
    }

    #[test]
    fn store_sales_customer_sk_null_rate() {
        // q76 selects WHERE ss_customer_sk IS NULL; the rate is ~4% decided
        // per ticket, so all lines of a ticket are null-or-not together.
        let (sch, batches) = generate_store_sales(0.01);
        let customers = col_i32(&batches, &sch, "ss_customer_sk");
        let nulls = customers.iter().filter(|c| c.is_none()).count();
        let frac = nulls as f64 / customers.len() as f64;
        assert!(
            (0.01..=0.08).contains(&frac),
            "ss_customer_sk null fraction {frac} outside 1%..8%"
        );
    }

    #[test]
    fn items_share_manufact_names() {
        use std::collections::HashSet;
        // q41 counts items per i_manufact; unique names made the count 0.
        let (sch, batches) = generate_item(0.01);
        let manufacts = col_str(&batches, &sch, "i_manufact");
        assert!(
            manufacts.iter().all(|m| m.starts_with("manufact#")),
            "i_manufact not derived from i_manufact_id"
        );
        let distinct: HashSet<&str> = manufacts.iter().map(|m| m.as_str()).collect();
        assert!(
            distinct.len() < manufacts.len(),
            "no two items share an i_manufact value"
        );
    }

    #[test]
    fn scd2_null_fractions_match_dsdgen() {
        use arrow_array::Array as _;
        // Official dsdgen null counts. rec_end_date is NULL on the current
        // revision of every entity: the single row at sf0.01, half the rows
        // at sf1. cc_closed_date_sk is always NULL; web_close_date_sk is
        // NULL only on single-revision entities; s_closed_date_sk is set on
        // 3 of every 12 rows.
        fn nulls(batches: &[RecordBatch], sch: &SchemaRef, col: &str) -> usize {
            let idx = sch.index_of(col).unwrap();
            batches.iter().map(|b| b.column(idx).null_count()).sum()
        }
        let cases: &[(&str, f64, &str, usize, usize)] = &[
            // (table, scale, column, expected nulls, expected rows)
            ("item", 0.01, "i_rec_end_date", 90, 180),
            ("item", 1.0, "i_rec_end_date", 9_000, 18_000),
            ("store", 0.01, "s_rec_end_date", 1, 1),
            ("store", 0.01, "s_closed_date_sk", 0, 1),
            ("store", 1.0, "s_rec_end_date", 6, 12),
            ("store", 1.0, "s_closed_date_sk", 9, 12),
            ("call_center", 0.01, "cc_rec_end_date", 1, 1),
            ("call_center", 0.01, "cc_closed_date_sk", 1, 1),
            ("call_center", 1.0, "cc_rec_end_date", 3, 6),
            ("call_center", 1.0, "cc_closed_date_sk", 6, 6),
            ("web_site", 0.01, "web_rec_end_date", 1, 1),
            ("web_site", 0.01, "web_close_date_sk", 1, 1),
            ("web_site", 1.0, "web_rec_end_date", 15, 30),
            ("web_site", 1.0, "web_close_date_sk", 5, 30),
            ("web_page", 0.01, "wp_rec_end_date", 1, 1),
            ("web_page", 1.0, "wp_rec_end_date", 30, 60),
        ];
        for (table, sf, col, want_nulls, want_rows) in cases {
            let (sch, batches) = match *table {
                "item" => generate_item(*sf),
                "store" => generate_store(*sf),
                "call_center" => generate_call_center(*sf),
                "web_site" => generate_web_site(*sf),
                "web_page" => generate_web_page(*sf),
                _ => unreachable!(),
            };
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, *want_rows, "{table} rows at sf{sf}");
            assert_eq!(
                nulls(&batches, &sch, col),
                *want_nulls,
                "{table}.{col} nulls at sf{sf}"
            );
        }
    }

    #[test]
    fn returns_lag_sale_by_at_most_120_days() {
        // Returns must land within 1..=120 days after the sale (capped at the
        // end of the calendar). A uniform draw over the rest of the calendar
        // starved the early years and emptied q91/q25.
        type Gen = fn(f64) -> (SchemaRef, Vec<RecordBatch>);
        let scale = 0.01;
        let dims = FkDims::at(scale);
        let cases: &[(&str, Gen, u64, i32, &str, &str)] = &[
            (
                "store_returns",
                |s| generate_store_returns(s),
                STORE_TICKET_SALT,
                store_rows(scale) as i32,
                "sr_returned_date_sk",
                "sr_ticket_number",
            ),
            (
                "catalog_returns",
                |s| generate_catalog_returns(s),
                CATALOG_ORDER_SALT,
                call_center_rows(scale) as i32,
                "cr_returned_date_sk",
                "cr_order_number",
            ),
            (
                "web_returns",
                |s| generate_web_returns(s),
                WEB_ORDER_SALT,
                web_site_rows(scale) as i32,
                "wr_returned_date_sk",
                "wr_order_number",
            ),
        ];
        for (name, gen, salt, channel_upper, date_col, ticket_col) in cases {
            let (sch, batches) = gen(scale);
            let ret_dates = col_i32(&batches, &sch, date_col);
            let tickets = col_i32(&batches, &sch, ticket_col);
            for r in 0..ret_dates.len() {
                let b = basket(*salt, tickets[r].unwrap(), *channel_upper, dims);
                let lag = ret_dates[r].unwrap() - b.date_sk;
                assert!(
                    (1..=120).contains(&lag) || ret_dates[r] == Some(DS_DATE_RANGE),
                    "{name} row {r}: return lag {lag} outside 1..=120 days"
                );
            }
        }
    }

    #[test]
    fn web_returns_returning_party_equals_refunded_party() {
        // Official data: the returning party is the refunded party 100% of the
        // time; q85 correlates cd1 (refunded) with cd2 (returning).
        let (sch, batches) = generate_web_returns(0.01);
        for (refunded, returning) in [
            ("wr_refunded_customer_sk", "wr_returning_customer_sk"),
            ("wr_refunded_cdemo_sk", "wr_returning_cdemo_sk"),
            ("wr_refunded_hdemo_sk", "wr_returning_hdemo_sk"),
            ("wr_refunded_addr_sk", "wr_returning_addr_sk"),
        ] {
            assert_eq!(
                col_i32(&batches, &sch, refunded),
                col_i32(&batches, &sch, returning),
                "{returning} diverges from {refunded}"
            );
        }
    }

    #[test]
    fn item_manufact_bridges_multiple_ids() {
        use std::collections::{HashMap, HashSet};
        // q41 bridges items on `i_manufact = i1.i_manufact`; the string must
        // span several i_manufact_ids. Official degree ~3.3; we run hotter for
        // reliability but stay inside [2.5, 8.0]. Measured at SF1 density.
        let (sch, batches) = generate_item(1.0);
        let manufacts = col_str(&batches, &sch, "i_manufact");
        let ids = col_i32(&batches, &sch, "i_manufact_id");
        let mut per_string: HashMap<&str, HashSet<i32>> = HashMap::new();
        for r in 0..manufacts.len() {
            per_string
                .entry(manufacts[r].as_str())
                .or_default()
                .insert(ids[r].unwrap());
        }
        let degree: f64 =
            per_string.values().map(|s| s.len() as f64).sum::<f64>() / per_string.len() as f64;
        assert!(
            (2.5..=8.0).contains(&degree),
            "i_manufact degree {degree} outside [2.5, 8.0]"
        );
    }

    #[test]
    fn item_peach_color_matches_official_frequency() {
        // q24 filters i_color = 'peach'; official SF1 frequency is 2.27%. A
        // uniform 1/92 draw (1.09%) emptied it.
        let (sch, batches) = generate_item(1.0);
        let colors = col_str(&batches, &sch, "i_color");
        let peach = colors.iter().filter(|c| c.as_str() == "peach").count();
        let frac = peach as f64 / colors.len() as f64;
        assert!(
            (0.018..=0.030).contains(&frac),
            "peach frequency {frac} outside [1.8%, 3.0%]"
        );
    }

    #[test]
    fn q54_coincidence_is_planted() {
        let scale = 1.0;
        // Item 1 is Women/maternity.
        let (isch, ib) = generate_item(scale);
        let cats = col_str(&ib, &isch, "i_category");
        let classes = col_str(&ib, &isch, "i_class");
        assert_eq!(cats[0], "Women", "item 1 category");
        assert_eq!(classes[0], "maternity", "item 1 class");
        // Address 1 is Williamson County, TN.
        let (asch, ab) = generate_customer_address(scale);
        assert_eq!(col_str(&ab, &asch, "ca_county")[0], "Williamson County");
        assert_eq!(col_str(&ab, &asch, "ca_state")[0], "TN");
        // Customer 1 points at address 1.
        let (csch, cb) = generate_customer(scale);
        assert_eq!(
            col_i32(&cb, &csch, "c_current_addr_sk")[0],
            Some(1),
            "customer 1 c_current_addr_sk"
        );
        // Catalog order 1: customer 1, item 1, Dec-1998.
        let dims = FkDims::at(scale);
        let cat = basket(CATALOG_ORDER_SALT, 1, call_center_rows(scale) as i32, dims);
        assert_eq!(cat.customer_sk, Some(1), "catalog ticket 1 customer");
        assert_eq!(cat.items[0], 1, "catalog ticket 1 item 0");
        assert_eq!(cat.date_sk, Q54_DEC_1998_DATE_SK, "catalog ticket 1 date");
        // Store ticket 1: customer 1, month_seq-window date.
        let st = basket(STORE_TICKET_SALT, 1, store_rows(scale) as i32, dims);
        assert_eq!(st.customer_sk, Some(1), "store ticket 1 customer");
        assert_eq!(st.date_sk, Q54_FEB_1999_DATE_SK, "store ticket 1 date");
    }

    #[test]
    fn q25_coincidence_is_planted() {
        let scale = 1.0;
        let dims = FkDims::at(scale);
        // Store ticket 3 and catalog order 3 carry the same customer+item, in
        // Apr-2001 and May-2001 respectively.
        let st = basket(
            STORE_TICKET_SALT,
            Q25_STORE_TICKET,
            store_rows(scale) as i32,
            dims,
        );
        assert_eq!(st.customer_sk, Some(Q25_CUSTOMER), "q25 store customer");
        assert_eq!(st.items[0], Q25_ITEM, "q25 store item");
        assert_eq!(st.date_sk, Q25_APR_2001_DATE_SK, "q25 store date");
        let co = basket(
            CATALOG_ORDER_SALT,
            Q25_CATALOG_ORDER,
            call_center_rows(scale) as i32,
            dims,
        );
        assert_eq!(co.customer_sk, Some(Q25_CUSTOMER), "q25 catalog customer");
        assert_eq!(co.items[0], Q25_ITEM, "q25 catalog item");
        assert_eq!(co.date_sk, Q25_MAY_2001_DATE_SK, "q25 catalog date");
        // A store return references the planted store ticket.
        let (sch, batches) = generate_store_returns(scale);
        assert!(
            col_i32(&batches, &sch, "sr_ticket_number").contains(&Some(Q25_STORE_TICKET)),
            "no store return for the q25 planted ticket"
        );
    }

    #[test]
    fn q24_coincidence_is_planted() {
        let scale = 1.0;
        let dims = FkDims::at(scale);
        // Item 3 is peach; store 3 is market 8 at Q24_ZIP; address 4 has that
        // zip; customer 4 points at address 4; store ticket 4 sells item 3 at
        // store 3 to customer 4.
        let (isch, ib) = generate_item(scale);
        assert_eq!(
            col_str(&ib, &isch, "i_color")[(Q24_ITEM - 1) as usize],
            "peach",
            "q24 item color"
        );
        let (ssch, sb) = generate_store(scale);
        assert_eq!(
            col_i32(&sb, &ssch, "s_market_id")[(Q24_STORE_SK - 1) as usize],
            Some(8),
            "q24 store market"
        );
        assert_eq!(
            col_str(&sb, &ssch, "s_zip")[(Q24_STORE_SK - 1) as usize],
            Q24_ZIP,
            "q24 store zip"
        );
        let (asch, ab) = generate_customer_address(scale);
        assert_eq!(
            col_str(&ab, &asch, "ca_zip")[(Q24_ADDR_SK - 1) as usize],
            Q24_ZIP,
            "q24 address zip"
        );
        let (csch, cb) = generate_customer(scale);
        assert_eq!(
            col_i32(&cb, &csch, "c_current_addr_sk")[(Q24_CUSTOMER - 1) as usize],
            Some(Q24_ADDR_SK),
            "q24 customer address link"
        );
        let st = basket(
            STORE_TICKET_SALT,
            Q24_STORE_TICKET,
            store_rows(scale) as i32,
            dims,
        );
        assert_eq!(st.customer_sk, Some(Q24_CUSTOMER), "q24 store customer");
        assert_eq!(st.channel_sk, Q24_STORE_SK, "q24 store sk");
        assert_eq!(st.items[0], Q24_ITEM, "q24 store item");
        let (srsch, srb) = generate_store_returns(scale);
        assert!(
            col_i32(&srb, &srsch, "sr_ticket_number").contains(&Some(Q24_STORE_TICKET)),
            "no store return for the q24 planted ticket"
        );
    }
}
