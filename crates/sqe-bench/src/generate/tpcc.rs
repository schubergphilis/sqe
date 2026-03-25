use std::sync::Arc;

use arrow_array::{Date32Array, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{parquet_writer, BenchmarkGenerator, GenerateStats, TableDef};

pub struct TpccGenerator;

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

fn warehouse_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("w_id", DataType::Int32, false),
        Field::new("w_name", DataType::Utf8, false),
        Field::new("w_street_1", DataType::Utf8, false),
        Field::new("w_street_2", DataType::Utf8, false),
        Field::new("w_city", DataType::Utf8, false),
        Field::new("w_state", DataType::Utf8, false),
        Field::new("w_zip", DataType::Utf8, false),
        Field::new("w_tax", DataType::Float64, false),
        Field::new("w_ytd", DataType::Float64, false),
    ]))
}

fn district_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("d_id", DataType::Int32, false),
        Field::new("d_w_id", DataType::Int32, false),
        Field::new("d_name", DataType::Utf8, false),
        Field::new("d_street_1", DataType::Utf8, false),
        Field::new("d_street_2", DataType::Utf8, false),
        Field::new("d_city", DataType::Utf8, false),
        Field::new("d_state", DataType::Utf8, false),
        Field::new("d_zip", DataType::Utf8, false),
        Field::new("d_tax", DataType::Float64, false),
        Field::new("d_ytd", DataType::Float64, false),
        Field::new("d_next_o_id", DataType::Int32, false),
    ]))
}

fn customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c_id", DataType::Int32, false),
        Field::new("c_d_id", DataType::Int32, false),
        Field::new("c_w_id", DataType::Int32, false),
        Field::new("c_first", DataType::Utf8, false),
        Field::new("c_middle", DataType::Utf8, false),
        Field::new("c_last", DataType::Utf8, false),
        Field::new("c_street_1", DataType::Utf8, false),
        Field::new("c_street_2", DataType::Utf8, false),
        Field::new("c_city", DataType::Utf8, false),
        Field::new("c_state", DataType::Utf8, false),
        Field::new("c_zip", DataType::Utf8, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_since", DataType::Date32, false),
        Field::new("c_credit", DataType::Utf8, false),
        Field::new("c_credit_lim", DataType::Float64, false),
        Field::new("c_discount", DataType::Float64, false),
        Field::new("c_balance", DataType::Float64, false),
        Field::new("c_ytd_payment", DataType::Float64, false),
        Field::new("c_payment_cnt", DataType::Int32, false),
        Field::new("c_delivery_cnt", DataType::Int32, false),
        Field::new("c_data", DataType::Utf8, false),
    ]))
}

fn history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("h_c_id", DataType::Int32, false),
        Field::new("h_c_d_id", DataType::Int32, false),
        Field::new("h_c_w_id", DataType::Int32, false),
        Field::new("h_d_id", DataType::Int32, false),
        Field::new("h_w_id", DataType::Int32, false),
        Field::new("h_date", DataType::Date32, false),
        Field::new("h_amount", DataType::Float64, false),
        Field::new("h_data", DataType::Utf8, false),
    ]))
}

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("o_id", DataType::Int32, false),
        Field::new("o_d_id", DataType::Int32, false),
        Field::new("o_w_id", DataType::Int32, false),
        Field::new("o_c_id", DataType::Int32, false),
        Field::new("o_entry_d", DataType::Date32, false),
        Field::new("o_carrier_id", DataType::Int32, true),
        Field::new("o_ol_cnt", DataType::Int32, false),
        Field::new("o_all_local", DataType::Int32, false),
    ]))
}

fn new_order_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("no_o_id", DataType::Int32, false),
        Field::new("no_d_id", DataType::Int32, false),
        Field::new("no_w_id", DataType::Int32, false),
    ]))
}

fn order_line_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ol_o_id", DataType::Int32, false),
        Field::new("ol_d_id", DataType::Int32, false),
        Field::new("ol_w_id", DataType::Int32, false),
        Field::new("ol_number", DataType::Int32, false),
        Field::new("ol_i_id", DataType::Int32, false),
        Field::new("ol_supply_w_id", DataType::Int32, false),
        Field::new("ol_delivery_d", DataType::Date32, true),
        Field::new("ol_quantity", DataType::Int32, false),
        Field::new("ol_amount", DataType::Float64, false),
        Field::new("ol_dist_info", DataType::Utf8, false),
    ]))
}

fn item_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("i_id", DataType::Int32, false),
        Field::new("i_im_id", DataType::Int32, false),
        Field::new("i_name", DataType::Utf8, false),
        Field::new("i_price", DataType::Float64, false),
        Field::new("i_data", DataType::Utf8, false),
    ]))
}

fn stock_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("s_i_id", DataType::Int32, false),
        Field::new("s_w_id", DataType::Int32, false),
        Field::new("s_quantity", DataType::Int32, false),
        Field::new("s_dist_01", DataType::Utf8, false),
        Field::new("s_dist_02", DataType::Utf8, false),
        Field::new("s_dist_03", DataType::Utf8, false),
        Field::new("s_dist_04", DataType::Utf8, false),
        Field::new("s_dist_05", DataType::Utf8, false),
        Field::new("s_dist_06", DataType::Utf8, false),
        Field::new("s_dist_07", DataType::Utf8, false),
        Field::new("s_dist_08", DataType::Utf8, false),
        Field::new("s_dist_09", DataType::Utf8, false),
        Field::new("s_dist_10", DataType::Utf8, false),
        Field::new("s_ytd", DataType::Int32, false),
        Field::new("s_order_cnt", DataType::Int32, false),
        Field::new("s_remote_cnt", DataType::Int32, false),
        Field::new("s_data", DataType::Utf8, false),
    ]))
}

// ---------------------------------------------------------------------------
// Date utilities
// ---------------------------------------------------------------------------

// TPC-C date range: 2020-01-01 to 2024-12-31
const DATE_START: i32 = 18262; // days since 1970-01-01 for 2020-01-01
const DATE_RANGE: i32 = 1827; // ~5 years in days

fn random_date(rng: &mut StdRng) -> i32 {
    DATE_START + rng.gen_range(0..DATE_RANGE)
}

// ---------------------------------------------------------------------------
// Seed derivation
// ---------------------------------------------------------------------------

fn seed_for_table(name: &str) -> u64 {
    name.bytes()
        .enumerate()
        .fold(0u64, |acc, (i, b)| {
            acc ^ ((b as u64).wrapping_shl(i as u32 % 64))
        })
        .wrapping_add(0xCAFE_BABE_1234_5678)
}

// ---------------------------------------------------------------------------
// Random data helpers
// ---------------------------------------------------------------------------

const CHARS_ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
const CHARS_ALNUM: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
const CHARS_UPPER: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const CHARS_DIGIT: &[u8] = b"0123456789";

fn random_str(rng: &mut StdRng, chars: &[u8], len: usize) -> String {
    (0..len)
        .map(|_| chars[rng.gen_range(0..chars.len())] as char)
        .collect()
}

fn random_name(rng: &mut StdRng) -> String {
    let len = rng.gen_range(6..=10usize);
    random_str(rng, CHARS_ALPHA, len)
}

fn random_street(rng: &mut StdRng) -> String {
    let num = rng.gen_range(1..=999i32);
    let len = rng.gen_range(4..=10usize);
    format!("{} {}", num, random_str(rng, CHARS_ALPHA, len))
}

fn random_city(rng: &mut StdRng) -> String {
    let len = rng.gen_range(6..=12usize);
    random_str(rng, CHARS_ALPHA, len)
}

fn random_state(rng: &mut StdRng) -> String {
    random_str(rng, CHARS_UPPER, 2)
}

fn random_zip(rng: &mut StdRng) -> String {
    format!("{:05}1111", rng.gen_range(0..=99999i32))
}

fn random_phone(rng: &mut StdRng) -> String {
    random_str(rng, CHARS_DIGIT, 16)
}

fn random_data(rng: &mut StdRng) -> String {
    let len = rng.gen_range(26..=50usize);
    random_str(rng, CHARS_ALNUM, len)
}

fn random_dist_info(rng: &mut StdRng) -> String {
    random_str(rng, CHARS_ALNUM, 24)
}

// TPC-C last-name syllables
const LAST_SYLLABLES: &[&str] = &[
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
];

fn make_last_name(n: usize) -> String {
    let a = LAST_SYLLABLES[n / 100];
    let b = LAST_SYLLABLES[(n / 10) % 10];
    let c = LAST_SYLLABLES[n % 10];
    format!("{a}{b}{c}")
}

// ---------------------------------------------------------------------------
// Batch size for chunked generation
// ---------------------------------------------------------------------------

const BATCH_SIZE: usize = 10_000;

// ---------------------------------------------------------------------------
// Table generators
// ---------------------------------------------------------------------------

fn generate_warehouse(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = warehouse_schema();
    let total = scale as usize;
    let mut rng = StdRng::seed_from_u64(seed_for_table("warehouse"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut w_id = Vec::with_capacity(n);
        let mut w_name = Vec::with_capacity(n);
        let mut w_street_1 = Vec::with_capacity(n);
        let mut w_street_2 = Vec::with_capacity(n);
        let mut w_city = Vec::with_capacity(n);
        let mut w_state = Vec::with_capacity(n);
        let mut w_zip = Vec::with_capacity(n);
        let mut w_tax = Vec::with_capacity(n);
        let mut w_ytd = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i + 1) as i32;
            w_id.push(id);
            w_name.push(random_name(&mut rng));
            w_street_1.push(random_street(&mut rng));
            w_street_2.push(random_street(&mut rng));
            w_city.push(random_city(&mut rng));
            w_state.push(random_state(&mut rng));
            w_zip.push(random_zip(&mut rng));
            w_tax.push((rng.gen_range(0..=2000i32) as f64) / 10000.0);
            w_ytd.push(300_000.0_f64);
        }

        let name_refs: Vec<&str> = w_name.iter().map(|s| s.as_str()).collect();
        let st1_refs: Vec<&str> = w_street_1.iter().map(|s| s.as_str()).collect();
        let st2_refs: Vec<&str> = w_street_2.iter().map(|s| s.as_str()).collect();
        let city_refs: Vec<&str> = w_city.iter().map(|s| s.as_str()).collect();
        let state_refs: Vec<&str> = w_state.iter().map(|s| s.as_str()).collect();
        let zip_refs: Vec<&str> = w_zip.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(w_id)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(StringArray::from(st1_refs)),
                    Arc::new(StringArray::from(st2_refs)),
                    Arc::new(StringArray::from(city_refs)),
                    Arc::new(StringArray::from(state_refs)),
                    Arc::new(StringArray::from(zip_refs)),
                    Arc::new(Float64Array::from(w_tax)),
                    Arc::new(Float64Array::from(w_ytd)),
                ],
            )
            .expect("warehouse batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_district(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = district_schema();
    let num_warehouses = scale as i32;
    // 10 districts per warehouse
    let total = (scale * 10.0) as usize;
    let total = total.max(1);
    let mut rng = StdRng::seed_from_u64(seed_for_table("district"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut d_id = Vec::with_capacity(n);
        let mut d_w_id = Vec::with_capacity(n);
        let mut d_name = Vec::with_capacity(n);
        let mut d_street_1 = Vec::with_capacity(n);
        let mut d_street_2 = Vec::with_capacity(n);
        let mut d_city = Vec::with_capacity(n);
        let mut d_state = Vec::with_capacity(n);
        let mut d_zip = Vec::with_capacity(n);
        let mut d_tax = Vec::with_capacity(n);
        let mut d_ytd = Vec::with_capacity(n);
        let mut d_next_o_id = Vec::with_capacity(n);

        for i in 0..n {
            let idx = offset + i;
            let wid = (idx / 10) as i32 + 1;
            let did = (idx % 10) as i32 + 1;
            d_id.push(did);
            d_w_id.push(wid.min(num_warehouses));
            d_name.push(random_name(&mut rng));
            d_street_1.push(random_street(&mut rng));
            d_street_2.push(random_street(&mut rng));
            d_city.push(random_city(&mut rng));
            d_state.push(random_state(&mut rng));
            d_zip.push(random_zip(&mut rng));
            d_tax.push((rng.gen_range(0..=2000i32) as f64) / 10000.0);
            d_ytd.push(30_000.0_f64);
            // next order id starts at 3001 (per TPC-C spec: 3000 orders loaded)
            d_next_o_id.push(3001i32);
        }

        let name_refs: Vec<&str> = d_name.iter().map(|s| s.as_str()).collect();
        let st1_refs: Vec<&str> = d_street_1.iter().map(|s| s.as_str()).collect();
        let st2_refs: Vec<&str> = d_street_2.iter().map(|s| s.as_str()).collect();
        let city_refs: Vec<&str> = d_city.iter().map(|s| s.as_str()).collect();
        let state_refs: Vec<&str> = d_state.iter().map(|s| s.as_str()).collect();
        let zip_refs: Vec<&str> = d_zip.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(d_id)),
                    Arc::new(Int32Array::from(d_w_id)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(StringArray::from(st1_refs)),
                    Arc::new(StringArray::from(st2_refs)),
                    Arc::new(StringArray::from(city_refs)),
                    Arc::new(StringArray::from(state_refs)),
                    Arc::new(StringArray::from(zip_refs)),
                    Arc::new(Float64Array::from(d_tax)),
                    Arc::new(Float64Array::from(d_ytd)),
                    Arc::new(Int32Array::from(d_next_o_id)),
                ],
            )
            .expect("district batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_customer(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = customer_schema();
    let num_warehouses = scale as i32;
    // 3000 customers per district, 10 districts per warehouse
    let total = (scale * 30_000.0) as usize;
    let total = total.max(1);
    let mut rng = StdRng::seed_from_u64(seed_for_table("customer"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut c_id = Vec::with_capacity(n);
        let mut c_d_id = Vec::with_capacity(n);
        let mut c_w_id = Vec::with_capacity(n);
        let mut c_first = Vec::with_capacity(n);
        let mut c_middle = Vec::with_capacity(n);
        let mut c_last = Vec::with_capacity(n);
        let mut c_street_1 = Vec::with_capacity(n);
        let mut c_street_2 = Vec::with_capacity(n);
        let mut c_city = Vec::with_capacity(n);
        let mut c_state = Vec::with_capacity(n);
        let mut c_zip = Vec::with_capacity(n);
        let mut c_phone = Vec::with_capacity(n);
        let mut c_since = Vec::with_capacity(n);
        let mut c_credit = Vec::with_capacity(n);
        let mut c_credit_lim = Vec::with_capacity(n);
        let mut c_discount = Vec::with_capacity(n);
        let mut c_balance = Vec::with_capacity(n);
        let mut c_ytd_payment = Vec::with_capacity(n);
        let mut c_payment_cnt = Vec::with_capacity(n);
        let mut c_delivery_cnt = Vec::with_capacity(n);
        let mut c_data = Vec::with_capacity(n);

        for i in 0..n {
            let idx = offset + i;
            // customer layout: 3000 per district, 10 districts per warehouse
            let cid = (idx % 3000) as i32 + 1;
            let did = ((idx / 3000) % 10) as i32 + 1;
            let wid = ((idx / 30_000) as i32 + 1).min(num_warehouses);

            // 10% bad credit
            let credit = if rng.gen_range(0..10) == 0 { "BC" } else { "GC" };
            // last name for first 1000 customers is deterministic (TPC-C spec)
            let last = if cid <= 1000 {
                make_last_name((cid - 1) as usize)
            } else {
                make_last_name(rng.gen_range(0..1000usize))
            };

            c_id.push(cid);
            c_d_id.push(did);
            c_w_id.push(wid);
            c_first.push(random_name(&mut rng));
            c_middle.push("OE".to_string());
            c_last.push(last);
            c_street_1.push(random_street(&mut rng));
            c_street_2.push(random_street(&mut rng));
            c_city.push(random_city(&mut rng));
            c_state.push(random_state(&mut rng));
            c_zip.push(random_zip(&mut rng));
            c_phone.push(random_phone(&mut rng));
            c_since.push(random_date(&mut rng));
            c_credit.push(credit.to_string());
            c_credit_lim.push(50_000.0_f64);
            c_discount.push((rng.gen_range(0..=5000i32) as f64) / 10000.0);
            c_balance.push(-10.0_f64);
            c_ytd_payment.push(10.0_f64);
            c_payment_cnt.push(1i32);
            c_delivery_cnt.push(0i32);
            c_data.push(random_data(&mut rng));
        }

        let first_refs: Vec<&str> = c_first.iter().map(|s| s.as_str()).collect();
        let middle_refs: Vec<&str> = c_middle.iter().map(|s| s.as_str()).collect();
        let last_refs: Vec<&str> = c_last.iter().map(|s| s.as_str()).collect();
        let st1_refs: Vec<&str> = c_street_1.iter().map(|s| s.as_str()).collect();
        let st2_refs: Vec<&str> = c_street_2.iter().map(|s| s.as_str()).collect();
        let city_refs: Vec<&str> = c_city.iter().map(|s| s.as_str()).collect();
        let state_refs: Vec<&str> = c_state.iter().map(|s| s.as_str()).collect();
        let zip_refs: Vec<&str> = c_zip.iter().map(|s| s.as_str()).collect();
        let phone_refs: Vec<&str> = c_phone.iter().map(|s| s.as_str()).collect();
        let credit_refs: Vec<&str> = c_credit.iter().map(|s| s.as_str()).collect();
        let data_refs: Vec<&str> = c_data.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(c_id)),
                    Arc::new(Int32Array::from(c_d_id)),
                    Arc::new(Int32Array::from(c_w_id)),
                    Arc::new(StringArray::from(first_refs)),
                    Arc::new(StringArray::from(middle_refs)),
                    Arc::new(StringArray::from(last_refs)),
                    Arc::new(StringArray::from(st1_refs)),
                    Arc::new(StringArray::from(st2_refs)),
                    Arc::new(StringArray::from(city_refs)),
                    Arc::new(StringArray::from(state_refs)),
                    Arc::new(StringArray::from(zip_refs)),
                    Arc::new(StringArray::from(phone_refs)),
                    Arc::new(Date32Array::from(c_since)),
                    Arc::new(StringArray::from(credit_refs)),
                    Arc::new(Float64Array::from(c_credit_lim)),
                    Arc::new(Float64Array::from(c_discount)),
                    Arc::new(Float64Array::from(c_balance)),
                    Arc::new(Float64Array::from(c_ytd_payment)),
                    Arc::new(Int32Array::from(c_payment_cnt)),
                    Arc::new(Int32Array::from(c_delivery_cnt)),
                    Arc::new(StringArray::from(data_refs)),
                ],
            )
            .expect("customer batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_history(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = history_schema();
    let num_warehouses = scale as i32;
    // 1 history record per customer: SF * 30,000
    let total = (scale * 30_000.0) as usize;
    let total = total.max(1);
    let mut rng = StdRng::seed_from_u64(seed_for_table("history"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut h_c_id = Vec::with_capacity(n);
        let mut h_c_d_id = Vec::with_capacity(n);
        let mut h_c_w_id = Vec::with_capacity(n);
        let mut h_d_id = Vec::with_capacity(n);
        let mut h_w_id = Vec::with_capacity(n);
        let mut h_date = Vec::with_capacity(n);
        let mut h_amount = Vec::with_capacity(n);
        let mut h_data = Vec::with_capacity(n);

        for i in 0..n {
            let idx = offset + i;
            let cid = (idx % 3000) as i32 + 1;
            let did = ((idx / 3000) % 10) as i32 + 1;
            let wid = ((idx / 30_000) as i32 + 1).min(num_warehouses);

            h_c_id.push(cid);
            h_c_d_id.push(did);
            h_c_w_id.push(wid);
            h_d_id.push(did);
            h_w_id.push(wid);
            h_date.push(random_date(&mut rng));
            h_amount.push(10.0_f64);
            h_data.push(random_data(&mut rng));
        }

        let data_refs: Vec<&str> = h_data.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(h_c_id)),
                    Arc::new(Int32Array::from(h_c_d_id)),
                    Arc::new(Int32Array::from(h_c_w_id)),
                    Arc::new(Int32Array::from(h_d_id)),
                    Arc::new(Int32Array::from(h_w_id)),
                    Arc::new(Date32Array::from(h_date)),
                    Arc::new(Float64Array::from(h_amount)),
                    Arc::new(StringArray::from(data_refs)),
                ],
            )
            .expect("history batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_orders(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = orders_schema();
    let num_warehouses = scale as i32;
    // 3000 orders per district, 10 districts per warehouse
    let total = (scale * 30_000.0) as usize;
    let total = total.max(1);
    let num_customers_per_district = 3000i32;
    let mut rng = StdRng::seed_from_u64(seed_for_table("orders"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut o_id = Vec::with_capacity(n);
        let mut o_d_id = Vec::with_capacity(n);
        let mut o_w_id = Vec::with_capacity(n);
        let mut o_c_id = Vec::with_capacity(n);
        let mut o_entry_d = Vec::with_capacity(n);
        let mut o_carrier_id: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut o_ol_cnt = Vec::with_capacity(n);
        let mut o_all_local = Vec::with_capacity(n);

        for i in 0..n {
            let idx = offset + i;
            let oid = (idx % 3000) as i32 + 1;
            let did = ((idx / 3000) % 10) as i32 + 1;
            let wid = ((idx / 30_000) as i32 + 1).min(num_warehouses);

            // Last 900 orders per district are new orders (no carrier yet)
            let carrier = if oid <= 2100 {
                Some(rng.gen_range(1..=10i32))
            } else {
                None
            };

            o_id.push(oid);
            o_d_id.push(did);
            o_w_id.push(wid);
            o_c_id.push(rng.gen_range(1..=num_customers_per_district));
            o_entry_d.push(random_date(&mut rng));
            o_carrier_id.push(carrier);
            o_ol_cnt.push(rng.gen_range(5..=15i32));
            o_all_local.push(1i32);
        }

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(o_id)),
                    Arc::new(Int32Array::from(o_d_id)),
                    Arc::new(Int32Array::from(o_w_id)),
                    Arc::new(Int32Array::from(o_c_id)),
                    Arc::new(Date32Array::from(o_entry_d)),
                    Arc::new(Int32Array::from(o_carrier_id)),
                    Arc::new(Int32Array::from(o_ol_cnt)),
                    Arc::new(Int32Array::from(o_all_local)),
                ],
            )
            .expect("orders batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_new_order(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = new_order_schema();
    let num_warehouses = scale as i32;
    // last 900 orders per district are new orders: SF * 10 districts * 900 = SF * 9000
    let total = (scale * 9_000.0) as usize;
    let total = total.max(1);
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut no_o_id = Vec::with_capacity(n);
        let mut no_d_id = Vec::with_capacity(n);
        let mut no_w_id = Vec::with_capacity(n);

        for i in 0..n {
            let idx = offset + i;
            // new orders start at order id 2101
            let oid = (idx % 900) as i32 + 2101;
            let did = ((idx / 900) % 10) as i32 + 1;
            let wid = ((idx / 9_000) as i32 + 1).min(num_warehouses);

            no_o_id.push(oid);
            no_d_id.push(did);
            no_w_id.push(wid);
        }

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(no_o_id)),
                    Arc::new(Int32Array::from(no_d_id)),
                    Arc::new(Int32Array::from(no_w_id)),
                ],
            )
            .expect("new_order batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_order_line(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = order_line_schema();
    let num_warehouses = scale as i32;
    // average 10 lines per order (range 5-15), 30,000 orders per warehouse
    // TPC-C spec uses exactly 300,000 per warehouse for estimation
    let total = (scale * 300_000.0) as usize;
    let total = total.max(1);
    let num_items = 100_000i32;
    let mut rng = StdRng::seed_from_u64(seed_for_table("order_line"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut ol_o_id = Vec::with_capacity(n);
        let mut ol_d_id = Vec::with_capacity(n);
        let mut ol_w_id = Vec::with_capacity(n);
        let mut ol_number = Vec::with_capacity(n);
        let mut ol_i_id = Vec::with_capacity(n);
        let mut ol_supply_w_id = Vec::with_capacity(n);
        let mut ol_delivery_d: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut ol_quantity = Vec::with_capacity(n);
        let mut ol_amount = Vec::with_capacity(n);
        let mut ol_dist_info = Vec::with_capacity(n);

        for i in 0..n {
            let idx = offset + i;
            // 10 lines per order on average; use modulo to assign
            let line_num = (idx % 10) as i32 + 1;
            let order_idx = idx / 10;
            let oid = (order_idx % 3000) as i32 + 1;
            let did = ((order_idx / 3000) % 10) as i32 + 1;
            let wid = ((order_idx / 30_000) as i32 + 1).min(num_warehouses);

            // orders 2101-3000 are new orders: no delivery date
            let delivery_d = if oid <= 2100 {
                Some(random_date(&mut rng))
            } else {
                None
            };
            // new orders have amount 0, delivered orders have non-zero amount
            let amount = if oid <= 2100 {
                (rng.gen_range(1..=999999i32) as f64) / 100.0
            } else {
                0.0
            };

            ol_o_id.push(oid);
            ol_d_id.push(did);
            ol_w_id.push(wid);
            ol_number.push(line_num);
            ol_i_id.push(rng.gen_range(1..=num_items));
            ol_supply_w_id.push(wid);
            ol_delivery_d.push(delivery_d);
            ol_quantity.push(5i32);
            ol_amount.push(amount);
            ol_dist_info.push(random_dist_info(&mut rng));
        }

        let dist_refs: Vec<&str> = ol_dist_info.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ol_o_id)),
                    Arc::new(Int32Array::from(ol_d_id)),
                    Arc::new(Int32Array::from(ol_w_id)),
                    Arc::new(Int32Array::from(ol_number)),
                    Arc::new(Int32Array::from(ol_i_id)),
                    Arc::new(Int32Array::from(ol_supply_w_id)),
                    Arc::new(Date32Array::from(ol_delivery_d)),
                    Arc::new(Int32Array::from(ol_quantity)),
                    Arc::new(Float64Array::from(ol_amount)),
                    Arc::new(StringArray::from(dist_refs)),
                ],
            )
            .expect("order_line batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_item() -> (SchemaRef, Vec<RecordBatch>) {
    let schema = item_schema();
    // Item table is fixed at 100,000 rows regardless of scale
    let total = 100_000usize;
    let mut rng = StdRng::seed_from_u64(seed_for_table("item"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut i_id = Vec::with_capacity(n);
        let mut i_im_id = Vec::with_capacity(n);
        let mut i_name = Vec::with_capacity(n);
        let mut i_price = Vec::with_capacity(n);
        let mut i_data = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i + 1) as i32;
            i_id.push(id);
            i_im_id.push(rng.gen_range(1..=10_000i32));
            i_name.push(random_name(&mut rng));
            i_price.push((rng.gen_range(100..=10_000i32) as f64) / 100.0);
            // 10% of items have "ORIGINAL" in data (TPC-C spec)
            let data = if rng.gen_range(0..10) == 0 {
                let pos = rng.gen_range(0..=14usize);
                let base = random_data(&mut rng);
                format!("{}ORIGINAL{}", &base[..pos.min(base.len())], &base[pos.min(base.len())..])
            } else {
                random_data(&mut rng)
            };
            i_data.push(data);
        }

        let name_refs: Vec<&str> = i_name.iter().map(|s| s.as_str()).collect();
        let data_refs: Vec<&str> = i_data.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(i_id)),
                    Arc::new(Int32Array::from(i_im_id)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(Float64Array::from(i_price)),
                    Arc::new(StringArray::from(data_refs)),
                ],
            )
            .expect("item batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_stock(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = stock_schema();
    let num_warehouses = scale as i32;
    // 100,000 stock rows per warehouse
    let total = (scale * 100_000.0) as usize;
    let total = total.max(1);
    let num_items = 100_000i32;
    let mut rng = StdRng::seed_from_u64(seed_for_table("stock"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut s_i_id = Vec::with_capacity(n);
        let mut s_w_id = Vec::with_capacity(n);
        let mut s_quantity = Vec::with_capacity(n);
        let mut s_dist_01 = Vec::with_capacity(n);
        let mut s_dist_02 = Vec::with_capacity(n);
        let mut s_dist_03 = Vec::with_capacity(n);
        let mut s_dist_04 = Vec::with_capacity(n);
        let mut s_dist_05 = Vec::with_capacity(n);
        let mut s_dist_06 = Vec::with_capacity(n);
        let mut s_dist_07 = Vec::with_capacity(n);
        let mut s_dist_08 = Vec::with_capacity(n);
        let mut s_dist_09 = Vec::with_capacity(n);
        let mut s_dist_10 = Vec::with_capacity(n);
        let mut s_ytd = Vec::with_capacity(n);
        let mut s_order_cnt = Vec::with_capacity(n);
        let mut s_remote_cnt = Vec::with_capacity(n);
        let mut s_data = Vec::with_capacity(n);

        for i in 0..n {
            let idx = offset + i;
            let item_id = (idx % num_items as usize) as i32 + 1;
            let wid = (idx / num_items as usize) as i32 + 1;

            s_i_id.push(item_id);
            s_w_id.push(wid.min(num_warehouses));
            s_quantity.push(rng.gen_range(10..=100i32));
            s_dist_01.push(random_dist_info(&mut rng));
            s_dist_02.push(random_dist_info(&mut rng));
            s_dist_03.push(random_dist_info(&mut rng));
            s_dist_04.push(random_dist_info(&mut rng));
            s_dist_05.push(random_dist_info(&mut rng));
            s_dist_06.push(random_dist_info(&mut rng));
            s_dist_07.push(random_dist_info(&mut rng));
            s_dist_08.push(random_dist_info(&mut rng));
            s_dist_09.push(random_dist_info(&mut rng));
            s_dist_10.push(random_dist_info(&mut rng));
            s_ytd.push(0i32);
            s_order_cnt.push(0i32);
            s_remote_cnt.push(0i32);
            // 10% have ORIGINAL in data
            let data = if rng.gen_range(0..10) == 0 {
                let pos = rng.gen_range(0..=14usize);
                let base = random_data(&mut rng);
                format!("{}ORIGINAL{}", &base[..pos.min(base.len())], &base[pos.min(base.len())..])
            } else {
                random_data(&mut rng)
            };
            s_data.push(data);
        }

        let d01_refs: Vec<&str> = s_dist_01.iter().map(|s| s.as_str()).collect();
        let d02_refs: Vec<&str> = s_dist_02.iter().map(|s| s.as_str()).collect();
        let d03_refs: Vec<&str> = s_dist_03.iter().map(|s| s.as_str()).collect();
        let d04_refs: Vec<&str> = s_dist_04.iter().map(|s| s.as_str()).collect();
        let d05_refs: Vec<&str> = s_dist_05.iter().map(|s| s.as_str()).collect();
        let d06_refs: Vec<&str> = s_dist_06.iter().map(|s| s.as_str()).collect();
        let d07_refs: Vec<&str> = s_dist_07.iter().map(|s| s.as_str()).collect();
        let d08_refs: Vec<&str> = s_dist_08.iter().map(|s| s.as_str()).collect();
        let d09_refs: Vec<&str> = s_dist_09.iter().map(|s| s.as_str()).collect();
        let d10_refs: Vec<&str> = s_dist_10.iter().map(|s| s.as_str()).collect();
        let data_refs: Vec<&str> = s_data.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(s_i_id)),
                    Arc::new(Int32Array::from(s_w_id)),
                    Arc::new(Int32Array::from(s_quantity)),
                    Arc::new(StringArray::from(d01_refs)),
                    Arc::new(StringArray::from(d02_refs)),
                    Arc::new(StringArray::from(d03_refs)),
                    Arc::new(StringArray::from(d04_refs)),
                    Arc::new(StringArray::from(d05_refs)),
                    Arc::new(StringArray::from(d06_refs)),
                    Arc::new(StringArray::from(d07_refs)),
                    Arc::new(StringArray::from(d08_refs)),
                    Arc::new(StringArray::from(d09_refs)),
                    Arc::new(StringArray::from(d10_refs)),
                    Arc::new(Int32Array::from(s_ytd)),
                    Arc::new(Int32Array::from(s_order_cnt)),
                    Arc::new(Int32Array::from(s_remote_cnt)),
                    Arc::new(StringArray::from(data_refs)),
                ],
            )
            .expect("stock batch"),
        );
        offset += n;
    }

    (schema, batches)
}

// ---------------------------------------------------------------------------
// BenchmarkGenerator impl
// ---------------------------------------------------------------------------

impl BenchmarkGenerator for TpccGenerator {
    fn name(&self) -> &str {
        "tpcc"
    }

    fn tables(&self) -> Vec<TableDef> {
        vec![
            TableDef {
                name: "warehouse".into(),
                schema: warehouse_schema(),
                row_count: |sf| sf as usize,
            },
            TableDef {
                name: "district".into(),
                schema: district_schema(),
                row_count: |sf| (sf * 10.0) as usize,
            },
            TableDef {
                name: "customer".into(),
                schema: customer_schema(),
                row_count: |sf| (sf * 30_000.0) as usize,
            },
            TableDef {
                name: "hist".into(),
                schema: history_schema(),
                row_count: |sf| (sf * 30_000.0) as usize,
            },
            TableDef {
                name: "orders".into(),
                schema: orders_schema(),
                row_count: |sf| (sf * 30_000.0) as usize,
            },
            TableDef {
                name: "new_order".into(),
                schema: new_order_schema(),
                row_count: |sf| (sf * 9_000.0) as usize,
            },
            TableDef {
                name: "order_line".into(),
                schema: order_line_schema(),
                row_count: |sf| (sf * 300_000.0) as usize,
            },
            TableDef {
                name: "item".into(),
                schema: item_schema(),
                row_count: |_| 100_000,
            },
            TableDef {
                name: "stock".into(),
                schema: stock_schema(),
                row_count: |sf| (sf * 100_000.0) as usize,
            },
        ]
    }

    fn generate_table(
        &self,
        table: &str,
        scale: f64,
        output_dir: &str,
    ) -> anyhow::Result<GenerateStats> {
        let start = std::time::Instant::now();

        let (schema, batches) = match table {
            "warehouse" => generate_warehouse(scale),
            "district" => generate_district(scale),
            "customer" => generate_customer(scale),
            "hist" => generate_history(scale),
            "orders" => generate_orders(scale),
            "new_order" => generate_new_order(scale),
            "order_line" => generate_order_line(scale),
            "item" => generate_item(),
            "stock" => generate_stock(scale),
            _ => anyhow::bail!("Unknown TPC-C table: {table}"),
        };

        let full_output = format!("{output_dir}/tpcc/sf{scale}");
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
        let gen = TpccGenerator;
        let tables = gen.tables();
        assert_eq!(tables.len(), 9);
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"warehouse"));
        assert!(names.contains(&"district"));
        assert!(names.contains(&"customer"));
        assert!(names.contains(&"hist"));
        assert!(names.contains(&"orders"));
        assert!(names.contains(&"new_order"));
        assert!(names.contains(&"order_line"));
        assert!(names.contains(&"item"));
        assert!(names.contains(&"stock"));
    }

    #[test]
    fn test_row_counts_sf1() {
        let gen = TpccGenerator;
        let sf = 1.0_f64;
        for t in gen.tables() {
            let expected = (t.row_count)(sf);
            match t.name.as_str() {
                "warehouse" => assert_eq!(expected, 1),
                "district" => assert_eq!(expected, 10),
                "customer" => assert_eq!(expected, 30_000),
                "hist" => assert_eq!(expected, 30_000),
                "orders" => assert_eq!(expected, 30_000),
                "new_order" => assert_eq!(expected, 9_000),
                "order_line" => assert_eq!(expected, 300_000),
                "item" => assert_eq!(expected, 100_000),
                "stock" => assert_eq!(expected, 100_000),
                _ => {}
            }
        }
    }

    #[test]
    fn test_row_counts_sf10() {
        let gen = TpccGenerator;
        let sf = 10.0_f64;
        for t in gen.tables() {
            let expected = (t.row_count)(sf);
            match t.name.as_str() {
                "warehouse" => assert_eq!(expected, 10),
                "district" => assert_eq!(expected, 100),
                "customer" => assert_eq!(expected, 300_000),
                "hist" => assert_eq!(expected, 300_000),
                "orders" => assert_eq!(expected, 300_000),
                "new_order" => assert_eq!(expected, 90_000),
                "order_line" => assert_eq!(expected, 3_000_000),
                "item" => assert_eq!(expected, 100_000),
                "stock" => assert_eq!(expected, 1_000_000),
                _ => {}
            }
        }
    }

    #[test]
    fn test_generate_warehouse_sf1() {
        let (schema, batches) = generate_warehouse(1.0);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].schema(), schema);
        assert_eq!(schema.fields().len(), 9);
    }

    #[test]
    fn test_generate_district_sf1() {
        let (schema, batches) = generate_district(1.0);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10);
        assert_eq!(schema.fields().len(), 11);
    }

    #[test]
    fn test_generate_item_fixed() {
        let (schema, batches) = generate_item();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100_000);
        assert_eq!(schema.fields().len(), 5);
    }

    #[test]
    fn test_generate_new_order_sf1() {
        let (schema, batches) = generate_new_order(1.0);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 9_000);
        assert_eq!(schema.fields().len(), 3);
    }

    #[test]
    fn test_schema_field_counts() {
        assert_eq!(warehouse_schema().fields().len(), 9);
        assert_eq!(district_schema().fields().len(), 11);
        assert_eq!(customer_schema().fields().len(), 21);
        assert_eq!(history_schema().fields().len(), 8);
        assert_eq!(orders_schema().fields().len(), 8);
        assert_eq!(new_order_schema().fields().len(), 3);
        assert_eq!(order_line_schema().fields().len(), 10);
        assert_eq!(item_schema().fields().len(), 5);
        assert_eq!(stock_schema().fields().len(), 17);
    }

    #[test]
    fn test_make_last_name() {
        assert_eq!(make_last_name(0), "BARBARBAR");
        assert_eq!(make_last_name(1), "BARBAROUGHT");
        assert_eq!(make_last_name(10), "BAROUGHTBAR");
        assert_eq!(make_last_name(999), "EINGEINGEING");
    }

    #[test]
    fn test_name_is_tpcc() {
        assert_eq!(TpccGenerator.name(), "tpcc");
    }
}
