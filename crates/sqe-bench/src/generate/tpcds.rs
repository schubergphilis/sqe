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

fn i32() -> DataType { DataType::Int32 }
fn f64() -> DataType { DataType::Float64 }
fn str() -> DataType { DataType::Utf8 }
fn date() -> DataType { DataType::Date32 }

// ---------------------------------------------------------------------------
// Schema definitions
// ---------------------------------------------------------------------------

fn store_sales_schema() -> SchemaRef {
    schema(&[
        ("ss_sold_date_sk", i32()), ("ss_sold_time_sk", i32()), ("ss_item_sk", i32()),
        ("ss_customer_sk", i32()), ("ss_cdemo_sk", i32()), ("ss_hdemo_sk", i32()),
        ("ss_addr_sk", i32()), ("ss_store_sk", i32()), ("ss_promo_sk", i32()),
        ("ss_ticket_number", i32()), ("ss_quantity", i32()), ("ss_wholesale_cost", f64()),
        ("ss_list_price", f64()), ("ss_sales_price", f64()), ("ss_ext_discount_amt", f64()),
        ("ss_ext_sales_price", f64()), ("ss_ext_wholesale_cost", f64()),
        ("ss_ext_list_price", f64()), ("ss_ext_tax", f64()), ("ss_coupon_amt", f64()),
        ("ss_net_paid", f64()), ("ss_net_paid_inc_tax", f64()), ("ss_net_profit", f64()),
    ])
}

fn store_returns_schema() -> SchemaRef {
    schema(&[
        ("sr_returned_date_sk", i32()), ("sr_return_time_sk", i32()), ("sr_item_sk", i32()),
        ("sr_customer_sk", i32()), ("sr_cdemo_sk", i32()), ("sr_hdemo_sk", i32()),
        ("sr_addr_sk", i32()), ("sr_store_sk", i32()), ("sr_reason_sk", i32()),
        ("sr_ticket_number", i32()), ("sr_return_quantity", i32()),
        ("sr_return_amt", f64()), ("sr_return_tax", f64()), ("sr_return_amt_inc_tax", f64()),
        ("sr_fee", f64()), ("sr_return_ship_cost", f64()), ("sr_refunded_cash", f64()),
        ("sr_reversed_charge", f64()), ("sr_store_credit", f64()), ("sr_net_loss", f64()),
    ])
}

fn catalog_sales_schema() -> SchemaRef {
    schema(&[
        ("cs_sold_date_sk", i32()), ("cs_sold_time_sk", i32()), ("cs_ship_date_sk", i32()),
        ("cs_bill_customer_sk", i32()), ("cs_bill_cdemo_sk", i32()), ("cs_bill_hdemo_sk", i32()),
        ("cs_bill_addr_sk", i32()), ("cs_ship_customer_sk", i32()), ("cs_ship_cdemo_sk", i32()),
        ("cs_ship_hdemo_sk", i32()), ("cs_ship_addr_sk", i32()), ("cs_call_center_sk", i32()),
        ("cs_catalog_page_sk", i32()), ("cs_ship_mode_sk", i32()), ("cs_warehouse_sk", i32()),
        ("cs_item_sk", i32()), ("cs_promo_sk", i32()), ("cs_order_number", i32()),
        ("cs_quantity", i32()), ("cs_wholesale_cost", f64()), ("cs_list_price", f64()),
        ("cs_sales_price", f64()), ("cs_ext_discount_amt", f64()), ("cs_ext_sales_price", f64()),
        ("cs_ext_wholesale_cost", f64()), ("cs_ext_list_price", f64()), ("cs_ext_tax", f64()),
        ("cs_coupon_amt", f64()), ("cs_ext_ship_cost", f64()), ("cs_net_paid", f64()),
        ("cs_net_paid_inc_tax", f64()), ("cs_net_paid_inc_ship", f64()),
        ("cs_net_paid_inc_ship_tax", f64()), ("cs_net_profit", f64()),
    ])
}

fn catalog_returns_schema() -> SchemaRef {
    schema(&[
        ("cr_returned_date_sk", i32()), ("cr_returned_time_sk", i32()), ("cr_item_sk", i32()),
        ("cr_refunded_customer_sk", i32()), ("cr_refunded_cdemo_sk", i32()),
        ("cr_refunded_hdemo_sk", i32()), ("cr_refunded_addr_sk", i32()),
        ("cr_returning_customer_sk", i32()), ("cr_returning_cdemo_sk", i32()),
        ("cr_returning_hdemo_sk", i32()), ("cr_returning_addr_sk", i32()),
        ("cr_call_center_sk", i32()), ("cr_catalog_page_sk", i32()),
        ("cr_ship_mode_sk", i32()), ("cr_warehouse_sk", i32()), ("cr_reason_sk", i32()),
        ("cr_order_number", i32()), ("cr_return_quantity", i32()),
        ("cr_return_amount", f64()), ("cr_return_tax", f64()), ("cr_return_amt_inc_tax", f64()),
        ("cr_fee", f64()), ("cr_return_ship_cost", f64()), ("cr_refunded_cash", f64()),
        ("cr_reversed_charge", f64()), ("cr_store_credit", f64()), ("cr_net_loss", f64()),
    ])
}

fn web_sales_schema() -> SchemaRef {
    schema(&[
        ("ws_sold_date_sk", i32()), ("ws_sold_time_sk", i32()), ("ws_ship_date_sk", i32()),
        ("ws_item_sk", i32()), ("ws_bill_customer_sk", i32()), ("ws_bill_cdemo_sk", i32()),
        ("ws_bill_hdemo_sk", i32()), ("ws_bill_addr_sk", i32()), ("ws_ship_customer_sk", i32()),
        ("ws_ship_cdemo_sk", i32()), ("ws_ship_hdemo_sk", i32()), ("ws_ship_addr_sk", i32()),
        ("ws_web_page_sk", i32()), ("ws_web_site_sk", i32()), ("ws_ship_mode_sk", i32()),
        ("ws_warehouse_sk", i32()), ("ws_promo_sk", i32()), ("ws_order_number", i32()),
        ("ws_quantity", i32()), ("ws_wholesale_cost", f64()), ("ws_list_price", f64()),
        ("ws_sales_price", f64()), ("ws_ext_discount_amt", f64()), ("ws_ext_sales_price", f64()),
        ("ws_ext_wholesale_cost", f64()), ("ws_ext_list_price", f64()), ("ws_ext_tax", f64()),
        ("ws_coupon_amt", f64()), ("ws_ext_ship_cost", f64()), ("ws_net_paid", f64()),
        ("ws_net_paid_inc_tax", f64()), ("ws_net_paid_inc_ship", f64()),
        ("ws_net_paid_inc_ship_tax", f64()), ("ws_net_profit", f64()),
    ])
}

fn web_returns_schema() -> SchemaRef {
    schema(&[
        ("wr_returned_date_sk", i32()), ("wr_returned_time_sk", i32()), ("wr_item_sk", i32()),
        ("wr_refunded_customer_sk", i32()), ("wr_refunded_cdemo_sk", i32()),
        ("wr_refunded_hdemo_sk", i32()), ("wr_refunded_addr_sk", i32()),
        ("wr_returning_customer_sk", i32()), ("wr_returning_cdemo_sk", i32()),
        ("wr_returning_hdemo_sk", i32()), ("wr_returning_addr_sk", i32()),
        ("wr_web_page_sk", i32()), ("wr_reason_sk", i32()), ("wr_order_number", i32()),
        ("wr_return_quantity", i32()), ("wr_return_amt", f64()), ("wr_return_tax", f64()),
        ("wr_return_amt_inc_tax", f64()), ("wr_fee", f64()), ("wr_return_ship_cost", f64()),
        ("wr_refunded_cash", f64()), ("wr_reversed_charge", f64()),
        ("wr_account_credit", f64()), ("wr_net_loss", f64()),
    ])
}

fn inventory_schema() -> SchemaRef {
    schema(&[
        ("inv_date_sk", i32()), ("inv_item_sk", i32()),
        ("inv_warehouse_sk", i32()), ("inv_quantity_on_hand", i32()),
    ])
}

fn date_dim_schema() -> SchemaRef {
    schema(&[
        ("d_date_sk", i32()), ("d_date_id", str()), ("d_date", date()),
        ("d_month_seq", i32()), ("d_week_seq", i32()), ("d_quarter_seq", i32()),
        ("d_year", i32()), ("d_dow", i32()), ("d_moy", i32()), ("d_dom", i32()),
        ("d_qoy", i32()), ("d_fy_year", i32()), ("d_fy_quarter_seq", i32()),
        ("d_fy_week_seq", i32()), ("d_day_name", str()), ("d_quarter_name", str()),
        ("d_holiday", str()), ("d_weekend", str()), ("d_following_holiday", str()),
        ("d_first_dom", i32()), ("d_last_dom", i32()), ("d_same_day_ly", i32()),
        ("d_same_day_lq", i32()), ("d_current_day", str()), ("d_current_week", str()),
        ("d_current_month", str()), ("d_current_quarter", str()), ("d_current_year", str()),
    ])
}

fn time_dim_schema() -> SchemaRef {
    schema(&[
        ("t_time_sk", i32()), ("t_time_id", str()), ("t_time", i32()),
        ("t_hour", i32()), ("t_minute", i32()), ("t_second", i32()),
        ("t_am_pm", str()), ("t_shift", str()), ("t_sub_shift", str()), ("t_meal_time", str()),
    ])
}

fn item_schema() -> SchemaRef {
    schema(&[
        ("i_item_sk", i32()), ("i_item_id", str()), ("i_rec_start_date", date()),
        ("i_rec_end_date", date()), ("i_item_desc", str()), ("i_current_price", f64()),
        ("i_wholesale_cost", f64()), ("i_brand_id", i32()), ("i_brand", str()),
        ("i_class_id", i32()), ("i_class", str()), ("i_category_id", i32()),
        ("i_category", str()), ("i_manufact_id", i32()), ("i_manufact", str()),
        ("i_size", str()), ("i_formulation", str()), ("i_color", str()),
        ("i_units", str()), ("i_container", str()), ("i_manager_id", i32()),
        ("i_product_name", str()),
    ])
}

fn customer_schema() -> SchemaRef {
    schema(&[
        ("c_customer_sk", i32()), ("c_customer_id", str()), ("c_current_cdemo_sk", i32()),
        ("c_current_hdemo_sk", i32()), ("c_current_addr_sk", i32()),
        ("c_first_shipto_date_sk", i32()), ("c_first_sales_date_sk", i32()),
        ("c_salutation", str()), ("c_first_name", str()), ("c_last_name", str()),
        ("c_preferred_cust_flag", str()), ("c_birth_day", i32()), ("c_birth_month", i32()),
        ("c_birth_year", i32()), ("c_birth_country", str()), ("c_login", str()),
        ("c_email_address", str()), ("c_last_review_date_sk", i32()),
    ])
}

fn customer_address_schema() -> SchemaRef {
    schema(&[
        ("ca_address_sk", i32()), ("ca_address_id", str()), ("ca_street_number", str()),
        ("ca_street_name", str()), ("ca_street_type", str()), ("ca_suite_number", str()),
        ("ca_city", str()), ("ca_county", str()), ("ca_state", str()), ("ca_zip", str()),
        ("ca_country", str()), ("ca_gmt_offset", f64()), ("ca_location_type", str()),
    ])
}

fn customer_demographics_schema() -> SchemaRef {
    schema(&[
        ("cd_demo_sk", i32()), ("cd_gender", str()), ("cd_marital_status", str()),
        ("cd_education_status", str()), ("cd_purchase_estimate", i32()),
        ("cd_credit_rating", str()), ("cd_dep_count", i32()),
        ("cd_dep_employed_count", i32()), ("cd_dep_college_count", i32()),
    ])
}

fn household_demographics_schema() -> SchemaRef {
    schema(&[
        ("hd_demo_sk", i32()), ("hd_income_band_sk", i32()),
        ("hd_buy_potential", str()), ("hd_dep_count", i32()), ("hd_vehicle_count", i32()),
    ])
}

fn store_schema() -> SchemaRef {
    schema(&[
        ("s_store_sk", i32()), ("s_store_id", str()), ("s_rec_start_date", date()),
        ("s_rec_end_date", date()), ("s_closed_date_sk", i32()), ("s_store_name", str()),
        ("s_number_employees", i32()), ("s_floor_space", i32()), ("s_hours", str()),
        ("s_manager", str()), ("s_market_id", i32()), ("s_geography_class", str()),
        ("s_market_desc", str()), ("s_market_manager", str()), ("s_division_id", i32()),
        ("s_division_name", str()), ("s_company_id", i32()), ("s_company_name", str()),
        ("s_street_number", str()), ("s_street_name", str()), ("s_street_type", str()),
        ("s_suite_number", str()), ("s_city", str()), ("s_county", str()),
        ("s_state", str()), ("s_zip", str()), ("s_country", str()),
        ("s_gmt_offset", f64()), ("s_tax_percentage", f64()),
    ])
}

fn catalog_page_schema() -> SchemaRef {
    schema(&[
        ("cp_catalog_page_sk", i32()), ("cp_catalog_page_id", str()),
        ("cp_start_date_sk", i32()), ("cp_end_date_sk", i32()),
        ("cp_department", str()), ("cp_catalog_number", i32()),
        ("cp_catalog_page_number", i32()), ("cp_description", str()), ("cp_type", str()),
    ])
}

fn web_site_schema() -> SchemaRef {
    schema(&[
        ("web_site_sk", i32()), ("web_site_id", str()), ("web_rec_start_date", date()),
        ("web_rec_end_date", date()), ("web_name", str()), ("web_open_date_sk", i32()),
        ("web_close_date_sk", i32()), ("web_class", str()), ("web_manager", str()),
        ("web_mkt_id", i32()), ("web_mkt_class", str()), ("web_mkt_desc", str()),
        ("web_market_manager", str()), ("web_company_id", i32()), ("web_company_name", str()),
        ("web_street_number", str()), ("web_street_name", str()), ("web_street_type", str()),
        ("web_suite_number", str()), ("web_city", str()), ("web_county", str()),
        ("web_state", str()), ("web_zip", str()), ("web_country", str()),
        ("web_gmt_offset", f64()), ("web_tax_percentage", f64()),
    ])
}

fn web_page_schema() -> SchemaRef {
    schema(&[
        ("wp_web_page_sk", i32()), ("wp_web_page_id", str()),
        ("wp_rec_start_date", date()), ("wp_rec_end_date", date()),
        ("wp_creation_date_sk", i32()), ("wp_access_date_sk", i32()),
        ("wp_autogen_flag", str()), ("wp_customer_sk", i32()),
        ("wp_url", str()), ("wp_type", str()), ("wp_char_count", i32()),
        ("wp_link_count", i32()), ("wp_image_count", i32()), ("wp_max_ad_count", i32()),
    ])
}

fn warehouse_schema() -> SchemaRef {
    schema(&[
        ("w_warehouse_sk", i32()), ("w_warehouse_id", str()), ("w_warehouse_name", str()),
        ("w_warehouse_sq_ft", i32()), ("w_street_number", str()), ("w_street_name", str()),
        ("w_street_type", str()), ("w_suite_number", str()), ("w_city", str()),
        ("w_county", str()), ("w_state", str()), ("w_zip", str()), ("w_country", str()),
        ("w_gmt_offset", f64()),
    ])
}

fn promotion_schema() -> SchemaRef {
    schema(&[
        ("p_promo_sk", i32()), ("p_promo_id", str()), ("p_start_date_sk", i32()),
        ("p_end_date_sk", i32()), ("p_item_sk", i32()), ("p_cost", f64()),
        ("p_response_target", i32()), ("p_promo_name", str()),
        ("p_channel_dmail", str()), ("p_channel_email", str()), ("p_channel_catalog", str()),
        ("p_channel_tv", str()), ("p_channel_radio", str()), ("p_channel_press", str()),
        ("p_channel_event", str()), ("p_channel_demo", str()), ("p_channel_details", str()),
        ("p_purpose", str()), ("p_discount_active", str()),
    ])
}

fn reason_schema() -> SchemaRef {
    schema(&[
        ("r_reason_sk", i32()), ("r_reason_id", str()), ("r_reason_desc", str()),
    ])
}

fn income_band_schema() -> SchemaRef {
    schema(&[
        ("ib_income_band_sk", i32()), ("ib_lower_bound", i32()), ("ib_upper_bound", i32()),
    ])
}

fn ship_mode_schema() -> SchemaRef {
    schema(&[
        ("sm_ship_mode_sk", i32()), ("sm_ship_mode_id", str()), ("sm_type", str()),
        ("sm_code", str()), ("sm_carrier", str()), ("sm_contract", str()),
    ])
}

fn call_center_schema() -> SchemaRef {
    schema(&[
        ("cc_call_center_sk", i32()), ("cc_call_center_id", str()),
        ("cc_rec_start_date", date()), ("cc_rec_end_date", date()),
        ("cc_closed_date_sk", i32()), ("cc_open_date_sk", i32()),
        ("cc_name", str()), ("cc_class", str()), ("cc_employees", i32()),
        ("cc_sq_ft", i32()), ("cc_hours", str()), ("cc_manager", str()),
        ("cc_mkt_id", i32()), ("cc_mkt_class", str()), ("cc_mkt_desc", str()),
        ("cc_market_manager", str()), ("cc_division", i32()), ("cc_division_name", str()),
        ("cc_company", i32()), ("cc_company_name", str()), ("cc_street_number", str()),
        ("cc_street_name", str()), ("cc_street_type", str()), ("cc_suite_number", str()),
        ("cc_city", str()), ("cc_county", str()), ("cc_state", str()), ("cc_zip", str()),
        ("cc_country", str()), ("cc_gmt_offset", f64()), ("cc_tax_percentage", f64()),
    ])
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

const BATCH_SIZE: usize = 10_000;

// TPC-DS date range: 1998-01-01 to 2003-12-31
const DS_DATE_START: i32 = 10227; // days since epoch for 1998-01-01
const DS_DATE_RANGE: i32 = 2191;  // ~6 years

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

fn random_id(rng: &mut StdRng) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    (0..16).map(|_| HEX[rng.gen_range(0..16)] as char).collect()
}

fn random_str<'a>(rng: &mut StdRng, choices: &[&'a str]) -> &'a str {
    choices[rng.gen_range(0..choices.len())]
}

fn random_word(rng: &mut StdRng, len: usize) -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    (0..len).map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char).collect()
}

fn random_name(rng: &mut StdRng) -> String {
    let len = rng.gen_range(4..10);
    random_word(rng, len)
}

const STATES: &[&str] = &[
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
];

const GENDERS: &[&str] = &["M", "F"];
const MARITAL: &[&str] = &["S", "M", "D", "W", "U"];
const EDUCATION: &[&str] = &[
    "Primary", "Secondary", "College", "2 yr Degree", "4 yr Degree",
    "Graduate", "Advanced Degree", "Unknown",
];
const CREDIT: &[&str] = &["Good", "High Risk", "Low Risk", "Unknown"];
const BUY_POTENTIAL: &[&str] = &["1001-5000", "501-1000", "0-500", ">10000", "5001-10000", "Unknown"];
const YN: &[&str] = &["Y", "N"];
const SALUTATIONS: &[&str] = &["Mr.", "Ms.", "Mrs.", "Dr.", "Sir", "Miss"];
const STREET_TYPES: &[&str] = &["Street", "Ave", "Blvd", "Drive", "Road", "Way", "Lane"];
const CATEGORIES: &[&str] = &["Electronics", "Clothing", "Sports", "Home", "Books", "Toys", "Music", "Food"];
const BRANDS: &[&str] = &["Brand1", "Brand2", "Brand3", "Brand4", "Brand5", "Brand6"];
const ITEM_CLASSES: &[&str] = &["Class1", "Class2", "Class3", "Class4", "Class5"];
const ITEM_SIZES: &[&str] = &["small", "medium", "large", "N/A", "extra large", "petite"];
const ITEM_COLORS: &[&str] = &["red", "blue", "green", "black", "white", "yellow", "purple", "orange"];
const ITEM_UNITS: &[&str] = &["Ounce", "Pound", "Dozen", "Gram", "Bundle", "Each", "Tbl", "Cup"];
const AM_PM: &[&str] = &["AM", "PM"];
const SHIFTS: &[&str] = &["Morning", "Afternoon", "Evening", "Night"];
const MEAL_TIMES: &[&str] = &["breakfast", "lunch", "dinner", "unknown"];
const DAY_NAMES: &[&str] = &["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
const SHIP_TYPES: &[&str] = &["NEXT DAY", "TWO DAY", "STANDARD", "LIBRARY"];
const SHIP_CODES: &[&str] = &["AIR", "SURFACE", "SEA", "GROUND"];
const CARRIERS: &[&str] = &["FEDEX", "UPS", "USPS", "DHL", "AMAZON"];
const PROMO_PURPOSES: &[&str] = &["Unknown", "Cross-Sell", "Retention", "Acquisition"];
const CC_CLASSES: &[&str] = &["large", "medium", "small"];
const CC_HOURS: &[&str] = &["8AM-12AM", "8AM-4PM", "8AM-8PM"];
const WP_TYPES: &[&str] = &["dynamic", "static", "flash"];
const DEPT: &[&str] = &["2001Q1", "2001Q2", "2001Q3", "2001Q4",
                         "2002Q1", "2002Q2", "2002Q3", "2002Q4"];

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
            assert_eq!(row.len(), ncols, "Row {} has {} values but schema has {} columns", offset + i, row.len(), ncols);
            for (c, v) in row.into_iter().enumerate() {
                cols[c].push(v);
            }
        }
        let arrays = cols_to_arrays(cols, &tbl_schema);
        batches.push(
            RecordBatch::try_new(tbl_schema.clone(), arrays).expect("record batch"),
        );
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

fn cols_to_arrays(cols: Vec<Vec<ColVal>>, tbl_schema: &SchemaRef) -> Vec<Arc<dyn arrow_array::Array>> {
    cols.into_iter()
        .enumerate()
        .map(|(idx, col)| {
            match tbl_schema.field(idx).data_type() {
                DataType::Int32 => {
                    let v: Vec<Option<i32>> = col.into_iter().map(|c| match c {
                        ColVal::I32(x) => x,
                        _ => None,
                    }).collect();
                    Arc::new(Int32Array::from(v)) as Arc<dyn arrow_array::Array>
                }
                DataType::Float64 => {
                    let v: Vec<Option<f64>> = col.into_iter().map(|c| match c {
                        ColVal::F64(x) => x,
                        _ => None,
                    }).collect();
                    Arc::new(Float64Array::from(v)) as Arc<dyn arrow_array::Array>
                }
                DataType::Date32 => {
                    let v: Vec<Option<i32>> = col.into_iter().map(|c| match c {
                        ColVal::Date(x) => x,
                        ColVal::I32(x) => x,
                        _ => None,
                    }).collect();
                    Arc::new(Date32Array::from(v)) as Arc<dyn arrow_array::Array>
                }
                _ => {
                    let v: Vec<Option<String>> = col.into_iter().map(|c| match c {
                        ColVal::Str(x) => x,
                        _ => None,
                    }).collect();
                    Arc::new(StringArray::from(v)) as Arc<dyn arrow_array::Array>
                }
            }
        })
        .collect()
}

macro_rules! i { ($x:expr) => { ColVal::I32(Some($x)) }; }
macro_rules! f { ($x:expr) => { ColVal::F64(Some($x)) }; }
macro_rules! s { ($x:expr) => { ColVal::Str(Some($x.to_string())) }; }
macro_rules! d { ($x:expr) => { ColVal::Date(Some($x)) }; }

// ---------------------------------------------------------------------------
// Table generators
// ---------------------------------------------------------------------------

fn generate_store_sales(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = super::scaled(scale, 2_880_000.0);
    let total = total.max(1);
    generate_batches(store_sales_schema(), total, seed_for_table("store_sales"), |row, rng| {
        let qty = rng.gen_range(1..100i32);
        let wc  = rng.gen_range(10..500i32) as f64 / 10.0;
        let lp  = wc * 1.5;
        let sp  = lp * rng.gen_range(50..100i32) as f64 / 100.0;
        let tax = sp * 0.08;
        vec![
            d!(random_date(rng)), i!(rng.gen_range(0..86400i32)),
            i!(rng.gen_range(1..18_000i32)), i!(rng.gen_range(1..100_000i32)),
            i!(rng.gen_range(1..1_920_800i32)), i!(rng.gen_range(1..7200i32)),
            i!(rng.gen_range(1..50_000i32)), i!(rng.gen_range(1..12i32)),
            i!(rng.gen_range(1..300i32)), i!((row + 1) as i32), i!(qty),
            f!(wc), f!(lp), f!(sp), f!(0.0), f!(sp * qty as f64),
            f!(wc * qty as f64), f!(lp * qty as f64), f!(tax), f!(0.0),
            f!(sp * qty as f64), f!(sp * qty as f64 + tax),
            f!(sp * qty as f64 - wc * qty as f64),
        ]
    })
}

fn generate_store_returns(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = super::scaled(scale, 287_999.0);
    let total = total.max(1);
    generate_batches(store_returns_schema(), total, seed_for_table("store_returns"), |row, rng| {
        let qty = rng.gen_range(1..50i32);
        let amt = rng.gen_range(10..500i32) as f64;
        let tax = amt * 0.08;
        vec![
            d!(random_date(rng)), i!(rng.gen_range(0..86400i32)),
            i!(rng.gen_range(1..18_000i32)), i!(rng.gen_range(1..100_000i32)),
            i!(rng.gen_range(1..1_920_800i32)), i!(rng.gen_range(1..7200i32)),
            i!(rng.gen_range(1..50_000i32)), i!(rng.gen_range(1..12i32)),
            i!(rng.gen_range(1..35i32)), i!((row + 1) as i32), i!(qty),
            f!(amt), f!(tax), f!(amt + tax), f!(amt * 0.02), f!(amt * 0.05),
            f!(amt * 0.6), f!(amt * 0.2), f!(amt * 0.2), f!(amt * 0.1),
        ]
    })
}

fn generate_catalog_sales(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = super::scaled(scale, 1_441_548.0);
    let total = total.max(1);
    generate_batches(catalog_sales_schema(), total, seed_for_table("catalog_sales"), |row, rng| {
        let qty = rng.gen_range(1..100i32);
        let wc  = rng.gen_range(10..500i32) as f64 / 10.0;
        let lp  = wc * 1.5;
        let sp  = lp * rng.gen_range(50..100i32) as f64 / 100.0;
        let tax = sp * 0.08;
        let ship = sp * 0.05 * qty as f64;
        vec![
            d!(random_date(rng)), i!(rng.gen_range(0..86400i32)), d!(random_date(rng)),
            i!(rng.gen_range(1..100_000i32)), i!(rng.gen_range(1..1_920_800i32)),
            i!(rng.gen_range(1..7200i32)), i!(rng.gen_range(1..50_000i32)),
            i!(rng.gen_range(1..100_000i32)), i!(rng.gen_range(1..1_920_800i32)),
            i!(rng.gen_range(1..7200i32)), i!(rng.gen_range(1..50_000i32)),
            i!(rng.gen_range(1..6i32)), i!(rng.gen_range(1..11_718i32)),
            i!(rng.gen_range(1..20i32)), i!(rng.gen_range(1..5i32)),
            i!(rng.gen_range(1..18_000i32)), i!(rng.gen_range(1..300i32)),
            i!((row + 1) as i32), i!(qty), f!(wc), f!(lp), f!(sp),
            f!(0.0), f!(sp * qty as f64), f!(wc * qty as f64), f!(lp * qty as f64),
            f!(tax), f!(0.0), f!(ship), f!(sp * qty as f64),
            f!(sp * qty as f64 + tax), f!(sp * qty as f64 + ship),
            f!(sp * qty as f64 + ship + tax), f!(sp * qty as f64 - wc * qty as f64),
        ]
    })
}

fn generate_catalog_returns(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = super::scaled(scale, 144_067.0);
    let total = total.max(1);
    generate_batches(catalog_returns_schema(), total, seed_for_table("catalog_returns"), |row, rng| {
        let qty = rng.gen_range(1..50i32);
        let amt = rng.gen_range(10..500i32) as f64;
        let tax = amt * 0.08;
        vec![
            d!(random_date(rng)), i!(rng.gen_range(0..86400i32)),
            i!(rng.gen_range(1..18_000i32)), i!(rng.gen_range(1..100_000i32)),
            i!(rng.gen_range(1..1_920_800i32)), i!(rng.gen_range(1..7200i32)),
            i!(rng.gen_range(1..50_000i32)), i!(rng.gen_range(1..100_000i32)),
            i!(rng.gen_range(1..1_920_800i32)), i!(rng.gen_range(1..7200i32)),
            i!(rng.gen_range(1..50_000i32)), i!(rng.gen_range(1..6i32)),
            i!(rng.gen_range(1..11_718i32)), i!(rng.gen_range(1..20i32)),
            i!(rng.gen_range(1..5i32)), i!(rng.gen_range(1..35i32)),
            i!((row + 1) as i32), i!(qty),
            f!(amt), f!(tax), f!(amt + tax), f!(amt * 0.02), f!(amt * 0.05),
            f!(amt * 0.6), f!(amt * 0.2), f!(amt * 0.2), f!(amt * 0.1),
        ]
    })
}

fn generate_web_sales(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = super::scaled(scale, 719_384.0);
    let total = total.max(1);
    generate_batches(web_sales_schema(), total, seed_for_table("web_sales"), |row, rng| {
        let qty = rng.gen_range(1..100i32);
        let wc  = rng.gen_range(10..500i32) as f64 / 10.0;
        let lp  = wc * 1.5;
        let sp  = lp * rng.gen_range(50..100i32) as f64 / 100.0;
        let tax = sp * 0.08;
        let ship = sp * 0.05 * qty as f64;
        vec![
            d!(random_date(rng)), i!(rng.gen_range(0..86400i32)), d!(random_date(rng)),
            i!(rng.gen_range(1..18_000i32)), i!(rng.gen_range(1..100_000i32)),
            i!(rng.gen_range(1..1_920_800i32)), i!(rng.gen_range(1..7200i32)),
            i!(rng.gen_range(1..50_000i32)), i!(rng.gen_range(1..100_000i32)),
            i!(rng.gen_range(1..1_920_800i32)), i!(rng.gen_range(1..7200i32)),
            i!(rng.gen_range(1..50_000i32)), i!(rng.gen_range(1..60i32)),
            i!(rng.gen_range(1..30i32)), i!(rng.gen_range(1..20i32)),
            i!(rng.gen_range(1..5i32)), i!(rng.gen_range(1..300i32)),
            i!((row + 1) as i32), i!(qty), f!(wc), f!(lp), f!(sp),
            f!(0.0), f!(sp * qty as f64), f!(wc * qty as f64), f!(lp * qty as f64),
            f!(tax), f!(0.0), f!(ship), f!(sp * qty as f64),
            f!(sp * qty as f64 + tax), f!(sp * qty as f64 + ship),
            f!(sp * qty as f64 + ship + tax), f!(sp * qty as f64 - wc * qty as f64),
        ]
    })
}

fn generate_web_returns(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = super::scaled(scale, 71_763.0);
    let total = total.max(1);
    generate_batches(web_returns_schema(), total, seed_for_table("web_returns"), |row, rng| {
        let qty = rng.gen_range(1..50i32);
        let amt = rng.gen_range(10..500i32) as f64;
        let tax = amt * 0.08;
        vec![
            d!(random_date(rng)), i!(rng.gen_range(0..86400i32)),
            i!(rng.gen_range(1..18_000i32)), i!(rng.gen_range(1..100_000i32)),
            i!(rng.gen_range(1..1_920_800i32)), i!(rng.gen_range(1..7200i32)),
            i!(rng.gen_range(1..50_000i32)), i!(rng.gen_range(1..100_000i32)),
            i!(rng.gen_range(1..1_920_800i32)), i!(rng.gen_range(1..7200i32)),
            i!(rng.gen_range(1..50_000i32)), i!(rng.gen_range(1..60i32)),
            i!(rng.gen_range(1..35i32)), i!((row + 1) as i32), i!(qty),
            f!(amt), f!(tax), f!(amt + tax), f!(amt * 0.02), f!(amt * 0.05),
            f!(amt * 0.6), f!(amt * 0.2), f!(amt * 0.2), f!(amt * 0.1),
        ]
    })
}

fn generate_inventory(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = super::scaled(scale, 11_745_000.0);
    let total = total.max(1);
    generate_batches(inventory_schema(), total, seed_for_table("inventory"), |_row, rng| {
        vec![
            d!(random_date(rng)),
            i!(rng.gen_range(1..18_000i32)),
            i!(rng.gen_range(1..5i32)),
            i!(rng.gen_range(0..1000i32)),
        ]
    })
}

fn generate_date_dim() -> (SchemaRef, Vec<RecordBatch>) {
    // Fixed 73,049 rows: 1998-01-01 to 2003-12-31
    generate_batches(date_dim_schema(), 73_049, seed_for_table("date_dim"), |row, rng| {
        let sk = (row + 1) as i32;
        let date_val = DS_DATE_START + row as i32;
        let year = 1998 + row as i32 / 366;
        let moy  = (row as i32 / 30 % 12) + 1;
        let dom  = (row as i32 % 28) + 1;
        let dow  = row as i32 % 7;
        vec![
            i!(sk), s!(format!("AAAA{:09}", sk)), d!(date_val),
            i!(row as i32 / 30), i!(row as i32 / 7), i!(row as i32 / 90),
            i!(year), i!(dow), i!(moy), i!(dom),
            i!((moy - 1) / 3 + 1), i!(year), i!(row as i32 / 90),
            i!(row as i32 / 7),
            s!(DAY_NAMES[dow as usize % 7]),
            s!(format!("{}Q{}", year, (moy - 1) / 3 + 1)),
            s!(random_str(rng, YN)), s!(random_str(rng, YN)),
            s!(random_str(rng, YN)),
            i!(sk - dom + 1), i!(sk - dom + 28),
            i!(sk - 365), i!(sk - 91),
            s!(random_str(rng, YN)), s!(random_str(rng, YN)),
            s!(random_str(rng, YN)), s!(random_str(rng, YN)),
            s!(random_str(rng, YN)),
        ]
    })
}

fn generate_time_dim() -> (SchemaRef, Vec<RecordBatch>) {
    generate_batches(time_dim_schema(), 86_400, seed_for_table("time_dim"), |row, _rng| {
        let sk   = (row + 1) as i32;
        let sec  = row as i32;
        let hour = sec / 3600;
        let min  = (sec % 3600) / 60;
        let s    = sec % 60;
        vec![
            i!(sk), s!(format!("T{:08}", sk)), i!(sec),
            i!(hour), i!(min), i!(s),
            s!(AM_PM[(hour >= 12) as usize]),
            s!(SHIFTS[hour as usize / 6 % 4]),
            s!(SHIFTS[hour as usize / 3 % 4]),
            s!(MEAL_TIMES[if hour == 7 { 0 } else if hour == 12 { 1 } else if hour == 18 { 2 } else { 3 }]),
        ]
    })
}

fn generate_item(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = super::scaled(scale, 18_000.0);
    let total = total.max(1);
    generate_batches(item_schema(), total, seed_for_table("item"), |row, rng| {
        let sk = (row + 1) as i32;
        let price = rng.gen_range(100..10_000i32) as f64 / 100.0;
        let wc = price * 0.6;
        vec![
            i!(sk), s!(random_id(rng)), d!(random_date(rng)), d!(random_date(rng)),
            s!(random_name(rng)), f!(price), f!(wc),
            i!(rng.gen_range(1..1000i32)), s!(random_str(rng, BRANDS)),
            i!(rng.gen_range(1..16i32)), s!(random_str(rng, ITEM_CLASSES)),
            i!(rng.gen_range(1..8i32)), s!(random_str(rng, CATEGORIES)),
            i!(rng.gen_range(1..1000i32)), s!(random_name(rng)),
            s!(random_str(rng, ITEM_SIZES)), s!(random_id(rng)),
            s!(random_str(rng, ITEM_COLORS)), s!(random_str(rng, ITEM_UNITS)),
            s!(random_str(rng, ITEM_SIZES)), i!(rng.gen_range(1..100i32)),
            s!(random_name(rng)),
        ]
    })
}

fn generate_customer(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = super::scaled(scale, 100_000.0);
    let total = total.max(1);
    generate_batches(customer_schema(), total, seed_for_table("customer"), |row, rng| {
        let sk = (row + 1) as i32;
        vec![
            i!(sk), s!(random_id(rng)),
            i!(rng.gen_range(1..1_920_800i32)), i!(rng.gen_range(1..7200i32)),
            i!(rng.gen_range(1..50_000i32)), d!(random_date(rng)), d!(random_date(rng)),
            s!(random_str(rng, SALUTATIONS)), s!(random_name(rng)), s!(random_name(rng)),
            s!(random_str(rng, YN)), i!(rng.gen_range(1..28i32)),
            i!(rng.gen_range(1..12i32)), i!(rng.gen_range(1920..2000i32)),
            s!("US"), s!(random_id(rng)),
            s!(format!("{}@{}.com", random_name(rng), random_name(rng))),
            i!(rng.gen_range(1..73049i32)),
        ]
    })
}

fn generate_customer_address(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = super::scaled(scale, 50_000.0);
    let total = total.max(1);
    generate_batches(customer_address_schema(), total, seed_for_table("customer_address"), |row, rng| {
        let sk = (row + 1) as i32;
        vec![
            i!(sk), s!(random_id(rng)),
            s!(format!("{}", rng.gen_range(1..9999i32))), s!(random_name(rng)),
            s!(random_str(rng, STREET_TYPES)), s!(format!("Suite {}", rng.gen_range(1..999i32))),
            s!(random_name(rng)), s!(random_name(rng)),
            s!(random_str(rng, STATES)), s!(format!("{:05}", rng.gen_range(10000..99999i32))),
            s!("United States"), f!(rng.gen_range(-12..12i32) as f64),
            s!(random_str(rng, &["city", "suburb", "rural", "unknown"])),
        ]
    })
}

fn generate_customer_demographics() -> (SchemaRef, Vec<RecordBatch>) {
    generate_batches(customer_demographics_schema(), 1_920_800, seed_for_table("customer_demographics"), |row, rng| {
        vec![
            i!((row + 1) as i32), s!(random_str(rng, GENDERS)),
            s!(random_str(rng, MARITAL)), s!(random_str(rng, EDUCATION)),
            i!(rng.gen_range(0..10_000i32) / 100 * 100),
            s!(random_str(rng, CREDIT)),
            i!(rng.gen_range(0..6i32)), i!(rng.gen_range(0..4i32)), i!(rng.gen_range(0..4i32)),
        ]
    })
}

fn generate_household_demographics() -> (SchemaRef, Vec<RecordBatch>) {
    generate_batches(household_demographics_schema(), 7_200, seed_for_table("household_demographics"), |row, rng| {
        vec![
            i!((row + 1) as i32), i!(rng.gen_range(1..20i32)),
            s!(random_str(rng, BUY_POTENTIAL)),
            i!(rng.gen_range(0..6i32)), i!(rng.gen_range(-1..3i32)),
        ]
    })
}

fn generate_store(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = (scale * 12.0).max(1.0) as usize;
    generate_batches(store_schema(), total, seed_for_table("store"), |row, rng| {
        let sk = (row + 1) as i32;
        vec![
            i!(sk), s!(random_id(rng)), d!(random_date(rng)), d!(random_date(rng)),
            i!(rng.gen_range(1..73049i32)), s!(random_name(rng)),
            i!(rng.gen_range(10..500i32)), i!(rng.gen_range(1000..100_000i32)),
            s!(random_str(rng, CC_HOURS)), s!(random_name(rng)),
            i!(rng.gen_range(1..10i32)), s!("Unknown"), s!(random_name(rng)),
            s!(random_name(rng)), i!(rng.gen_range(1..10i32)), s!("Division"),
            i!(rng.gen_range(1..6i32)), s!("Company"), s!(format!("{}", rng.gen_range(1..999i32))),
            s!(random_name(rng)), s!(random_str(rng, STREET_TYPES)),
            s!(format!("Suite {}", rng.gen_range(1..99i32))),
            s!(random_name(rng)), s!(random_name(rng)),
            s!(random_str(rng, STATES)), s!(format!("{:05}", rng.gen_range(10000..99999i32))),
            s!("United States"), f!(rng.gen_range(-12..12i32) as f64),
            f!(rng.gen_range(0..15i32) as f64 / 100.0),
        ]
    })
}

fn generate_catalog_page(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = (scale * 11_718.0).max(1.0) as usize;
    generate_batches(catalog_page_schema(), total, seed_for_table("catalog_page"), |row, rng| {
        let sk = (row + 1) as i32;
        vec![
            i!(sk), s!(random_id(rng)), i!(rng.gen_range(1..73049i32)),
            i!(rng.gen_range(1..73049i32)), s!(random_str(rng, DEPT)),
            i!(rng.gen_range(1..100i32)), i!(sk),
            s!(random_name(rng)), s!(random_str(rng, WP_TYPES)),
        ]
    })
}

fn generate_web_site(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = (scale * 30.0).max(1.0) as usize;
    generate_batches(web_site_schema(), total, seed_for_table("web_site"), |row, rng| {
        let sk = (row + 1) as i32;
        vec![
            i!(sk), s!(random_id(rng)), d!(random_date(rng)), d!(random_date(rng)),
            s!(random_name(rng)), i!(rng.gen_range(1..73049i32)),
            i!(rng.gen_range(1..73049i32)), s!("Unknown"), s!(random_name(rng)),
            i!(rng.gen_range(1..10i32)), s!(random_name(rng)), s!(random_name(rng)),
            s!(random_name(rng)), i!(rng.gen_range(1..6i32)), s!("web"),
            s!(format!("{}", rng.gen_range(1..999i32))), s!(random_name(rng)),
            s!(random_str(rng, STREET_TYPES)), s!(format!("Suite {}", rng.gen_range(1..99i32))),
            s!(random_name(rng)), s!(random_name(rng)),
            s!(random_str(rng, STATES)), s!(format!("{:05}", rng.gen_range(10000..99999i32))),
            s!("United States"), f!(rng.gen_range(-12..12i32) as f64),
            f!(rng.gen_range(0..15i32) as f64 / 100.0),
        ]
    })
}

fn generate_web_page(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = (scale * 60.0).max(1.0) as usize;
    generate_batches(web_page_schema(), total, seed_for_table("web_page"), |row, rng| {
        let sk = (row + 1) as i32;
        vec![
            i!(sk), s!(random_id(rng)), d!(random_date(rng)), d!(random_date(rng)),
            i!(rng.gen_range(1..73049i32)), i!(rng.gen_range(1..73049i32)),
            s!(random_str(rng, YN)), i!(rng.gen_range(1..100_000i32)),
            s!(format!("http://{}.com/{}", random_name(rng), sk)),
            s!(random_str(rng, WP_TYPES)), i!(rng.gen_range(0..100_000i32)),
            i!(rng.gen_range(0..25i32)), i!(rng.gen_range(0..20i32)),
            i!(rng.gen_range(0..4i32)),
        ]
    })
}

fn generate_warehouse(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = (scale * 5.0).max(1.0) as usize;
    generate_batches(warehouse_schema(), total, seed_for_table("warehouse"), |row, rng| {
        let sk = (row + 1) as i32;
        vec![
            i!(sk), s!(random_id(rng)), s!(random_name(rng)),
            i!(rng.gen_range(50_000..1_000_000i32)),
            s!(format!("{}", rng.gen_range(1..999i32))), s!(random_name(rng)),
            s!(random_str(rng, STREET_TYPES)), s!(format!("Suite {}", rng.gen_range(1..99i32))),
            s!(random_name(rng)), s!(random_name(rng)),
            s!(random_str(rng, STATES)), s!(format!("{:05}", rng.gen_range(10000..99999i32))),
            s!("United States"), f!(rng.gen_range(-12..12i32) as f64),
        ]
    })
}

fn generate_promotion(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = (scale * 300.0).max(1.0) as usize;
    generate_batches(promotion_schema(), total, seed_for_table("promotion"), |row, rng| {
        let sk = (row + 1) as i32;
        vec![
            i!(sk), s!(random_id(rng)), i!(rng.gen_range(1..73049i32)),
            i!(rng.gen_range(1..73049i32)), i!(rng.gen_range(1..18_000i32)),
            f!(rng.gen_range(0..1_000_000i32) as f64 / 100.0), i!(1),
            s!(random_name(rng)), s!(random_str(rng, YN)), s!(random_str(rng, YN)),
            s!(random_str(rng, YN)), s!(random_str(rng, YN)), s!(random_str(rng, YN)),
            s!(random_str(rng, YN)), s!(random_str(rng, YN)), s!(random_str(rng, YN)),
            s!(random_name(rng)), s!(random_str(rng, PROMO_PURPOSES)),
            s!(random_str(rng, YN)),
        ]
    })
}

fn generate_reason() -> (SchemaRef, Vec<RecordBatch>) {
    generate_batches(reason_schema(), 35, seed_for_table("reason"), |row, _rng| {
        let sk = (row + 1) as i32;
        vec![
            i!(sk),
            s!(format!("AAAAAAAAA{:06}", sk)),
            s!(format!("reason {}", sk)),
        ]
    })
}

fn generate_income_band() -> (SchemaRef, Vec<RecordBatch>) {
    generate_batches(income_band_schema(), 20, seed_for_table("income_band"), |row, _rng| {
        let sk = (row + 1) as i32;
        let lower = (row as i32) * 10_000;
        let upper = lower + 9_999;
        vec![i!(sk), i!(lower), i!(upper)]
    })
}

fn generate_ship_mode() -> (SchemaRef, Vec<RecordBatch>) {
    generate_batches(ship_mode_schema(), 20, seed_for_table("ship_mode"), |row, rng| {
        let sk = (row + 1) as i32;
        vec![
            i!(sk), s!(random_id(rng)),
            s!(random_str(rng, SHIP_TYPES)), s!(random_str(rng, SHIP_CODES)),
            s!(random_str(rng, CARRIERS)), s!(random_id(rng)),
        ]
    })
}

fn generate_call_center(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let total = (scale * 6.0).max(1.0) as usize;
    generate_batches(call_center_schema(), total, seed_for_table("call_center"), |row, rng| {
        let sk = (row + 1) as i32;
        vec![
            i!(sk), s!(random_id(rng)), d!(random_date(rng)), d!(random_date(rng)),
            i!(rng.gen_range(1..73049i32)), i!(rng.gen_range(1..73049i32)),
            s!(random_name(rng)), s!(random_str(rng, CC_CLASSES)),
            i!(rng.gen_range(100..5000i32)), i!(rng.gen_range(1000..100_000i32)),
            s!(random_str(rng, CC_HOURS)), s!(random_name(rng)),
            i!(rng.gen_range(1..10i32)), s!(random_name(rng)), s!(random_name(rng)),
            s!(random_name(rng)), i!(rng.gen_range(1..6i32)), s!("Division"),
            i!(rng.gen_range(1..6i32)), s!("Company"),
            s!(format!("{}", rng.gen_range(1..999i32))), s!(random_name(rng)),
            s!(random_str(rng, STREET_TYPES)), s!(format!("Suite {}", rng.gen_range(1..99i32))),
            s!(random_name(rng)), s!(random_name(rng)),
            s!(random_str(rng, STATES)), s!(format!("{:05}", rng.gen_range(10000..99999i32))),
            s!("United States"), f!(rng.gen_range(-12..12i32) as f64),
            f!(rng.gen_range(0..15i32) as f64 / 100.0),
        ]
    })
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
            TableDef { name: "store_sales".into(),           schema: store_sales_schema(),           row_count: |sf| (sf * 2_880_000.0) as usize },
            TableDef { name: "store_returns".into(),         schema: store_returns_schema(),         row_count: |sf| (sf * 287_999.0) as usize },
            TableDef { name: "catalog_sales".into(),         schema: catalog_sales_schema(),         row_count: |sf| (sf * 1_441_548.0) as usize },
            TableDef { name: "catalog_returns".into(),       schema: catalog_returns_schema(),       row_count: |sf| (sf * 144_067.0) as usize },
            TableDef { name: "web_sales".into(),             schema: web_sales_schema(),             row_count: |sf| (sf * 719_384.0) as usize },
            TableDef { name: "web_returns".into(),           schema: web_returns_schema(),           row_count: |sf| (sf * 71_763.0) as usize },
            TableDef { name: "inventory".into(),             schema: inventory_schema(),             row_count: |sf| (sf * 11_745_000.0) as usize },
            // Dimension tables
            TableDef { name: "date_dim".into(),              schema: date_dim_schema(),              row_count: |_| 73_049 },
            TableDef { name: "time_dim".into(),              schema: time_dim_schema(),              row_count: |_| 86_400 },
            TableDef { name: "item".into(),                  schema: item_schema(),                  row_count: |sf| (sf * 18_000.0) as usize },
            TableDef { name: "customer".into(),              schema: customer_schema(),              row_count: |sf| (sf * 100_000.0) as usize },
            TableDef { name: "customer_address".into(),      schema: customer_address_schema(),      row_count: |sf| (sf * 50_000.0) as usize },
            TableDef { name: "customer_demographics".into(), schema: customer_demographics_schema(), row_count: |_| 1_920_800 },
            TableDef { name: "household_demographics".into(),schema: household_demographics_schema(),row_count: |_| 7_200 },
            TableDef { name: "store".into(),                 schema: store_schema(),                 row_count: |sf| (sf * 12.0).max(1.0) as usize },
            TableDef { name: "catalog_page".into(),          schema: catalog_page_schema(),          row_count: |sf| (sf * 11_718.0).max(1.0) as usize },
            TableDef { name: "web_site".into(),              schema: web_site_schema(),              row_count: |sf| (sf * 30.0).max(1.0) as usize },
            TableDef { name: "web_page".into(),              schema: web_page_schema(),              row_count: |sf| (sf * 60.0).max(1.0) as usize },
            TableDef { name: "warehouse".into(),             schema: warehouse_schema(),             row_count: |sf| (sf * 5.0).max(1.0) as usize },
            TableDef { name: "promotion".into(),             schema: promotion_schema(),             row_count: |sf| (sf * 300.0).max(1.0) as usize },
            TableDef { name: "reason".into(),                schema: reason_schema(),                row_count: |_| 35 },
            TableDef { name: "income_band".into(),           schema: income_band_schema(),           row_count: |_| 20 },
            TableDef { name: "ship_mode".into(),             schema: ship_mode_schema(),             row_count: |_| 20 },
            TableDef { name: "call_center".into(),           schema: call_center_schema(),           row_count: |sf| (sf * 6.0).max(1.0) as usize },
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
            "store_sales"            => generate_store_sales(scale),
            "store_returns"          => generate_store_returns(scale),
            "catalog_sales"          => generate_catalog_sales(scale),
            "catalog_returns"        => generate_catalog_returns(scale),
            "web_sales"              => generate_web_sales(scale),
            "web_returns"            => generate_web_returns(scale),
            "inventory"              => generate_inventory(scale),
            "date_dim"               => generate_date_dim(),
            "time_dim"               => generate_time_dim(),
            "item"                   => generate_item(scale),
            "customer"               => generate_customer(scale),
            "customer_address"       => generate_customer_address(scale),
            "customer_demographics"  => generate_customer_demographics(),
            "household_demographics" => generate_household_demographics(),
            "store"                  => generate_store(scale),
            "catalog_page"           => generate_catalog_page(scale),
            "web_site"               => generate_web_site(scale),
            "web_page"               => generate_web_page(scale),
            "warehouse"              => generate_warehouse(scale),
            "promotion"              => generate_promotion(scale),
            "reason"                 => generate_reason(),
            "income_band"            => generate_income_band(),
            "ship_mode"              => generate_ship_mode(),
            "call_center"            => generate_call_center(scale),
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
            "store_sales", "store_returns", "catalog_sales", "catalog_returns",
            "web_sales", "web_returns", "inventory",
            "date_dim", "time_dim", "item", "customer", "customer_address",
            "customer_demographics", "household_demographics",
            "store", "catalog_page", "web_site", "web_page",
            "warehouse", "promotion", "reason", "income_band", "ship_mode", "call_center",
        ] {
            assert!(names.contains(expected), "missing table: {expected}");
        }
    }

    #[test]
    fn test_row_counts_sf001() {
        let sf = 0.01_f64;
        let gen = TpcdsGenerator;
        for t in gen.tables() {
            let n = (t.row_count)(sf);
            // Every table must yield at least 1 row at SF0.01
            assert!(n >= 1, "table {} yielded 0 rows at SF0.01", t.name);
        }
    }

    #[test]
    fn test_fixed_row_counts() {
        let gen = TpcdsGenerator;
        for t in gen.tables() {
            match t.name.as_str() {
                "date_dim"               => assert_eq!((t.row_count)(1.0), 73_049),
                "time_dim"               => assert_eq!((t.row_count)(1.0), 86_400),
                "customer_demographics"  => assert_eq!((t.row_count)(1.0), 1_920_800),
                "household_demographics" => assert_eq!((t.row_count)(1.0), 7_200),
                "reason"                 => assert_eq!((t.row_count)(1.0), 35),
                "income_band"            => assert_eq!((t.row_count)(1.0), 20),
                "ship_mode"              => assert_eq!((t.row_count)(1.0), 20),
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
        assert_eq!(rows, (0.01_f64 * 11_745_000.0) as usize);
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
        let (sch, batches) = generate_reason();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 35);
        assert_eq!(batches[0].schema(), sch);
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
}
