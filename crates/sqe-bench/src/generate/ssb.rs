use std::sync::Arc;

use arrow_array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{parquet_writer, BenchmarkGenerator, GenerateStats, TableDef};

pub struct SsbGenerator;

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

fn dim_date_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("d_datekey", DataType::Int32, false),
        Field::new("d_date", DataType::Utf8, false),
        Field::new("d_dayofweek", DataType::Utf8, false),
        Field::new("d_month", DataType::Utf8, false),
        Field::new("d_year", DataType::Int32, false),
        Field::new("d_yearmonthnum", DataType::Int32, false),
        Field::new("d_yearmonth", DataType::Utf8, false),
        Field::new("d_daynuminweek", DataType::Int32, false),
        Field::new("d_daynuminmonth", DataType::Int32, false),
        Field::new("d_daynuminyear", DataType::Int32, false),
        Field::new("d_monthnuminyear", DataType::Int32, false),
        Field::new("d_weeknuminyear", DataType::Int32, false),
        Field::new("d_sellingseason", DataType::Utf8, false),
        Field::new("d_lastdayinweekfl", DataType::Int32, false),
        Field::new("d_lastdayinmonthfl", DataType::Int32, false),
        Field::new("d_holidayfl", DataType::Int32, false),
        Field::new("d_weekdayfl", DataType::Int32, false),
    ]))
}

fn customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int32, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_city", DataType::Utf8, false),
        Field::new("c_nation", DataType::Utf8, false),
        Field::new("c_region", DataType::Utf8, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
    ]))
}

fn supplier_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int32, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_city", DataType::Utf8, false),
        Field::new("s_nation", DataType::Utf8, false),
        Field::new("s_region", DataType::Utf8, false),
        Field::new("s_phone", DataType::Utf8, false),
    ]))
}

fn part_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int32, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_category", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_color", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
    ]))
}

fn lineorder_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("lo_orderkey", DataType::Int64, false),
        Field::new("lo_linenumber", DataType::Int32, false),
        Field::new("lo_custkey", DataType::Int32, false),
        Field::new("lo_partkey", DataType::Int32, false),
        Field::new("lo_suppkey", DataType::Int32, false),
        Field::new("lo_orderdate", DataType::Int32, false),
        Field::new("lo_orderpriority", DataType::Utf8, false),
        Field::new("lo_shippriority", DataType::Int32, false),
        Field::new("lo_quantity", DataType::Int32, false),
        Field::new("lo_extendedprice", DataType::Float64, false),
        Field::new("lo_ordtotalprice", DataType::Float64, false),
        Field::new("lo_discount", DataType::Int32, false),
        Field::new("lo_revenue", DataType::Float64, false),
        Field::new("lo_supplycost", DataType::Float64, false),
        Field::new("lo_tax", DataType::Int32, false),
        Field::new("lo_commitdate", DataType::Int32, false),
        Field::new("lo_shipmode", DataType::Utf8, false),
    ]))
}

// ---------------------------------------------------------------------------
// Reference data
// ---------------------------------------------------------------------------

/// Region names indexed 0=AMERICA, 1=EUROPE, 2=ASIA, 3=MIDDLE EAST, 4=AFRICA
const REGIONS: &[&str] = &["AMERICA", "EUROPE", "ASIA", "MIDDLE EAST", "AFRICA"];

/// (nation_name, region_index)
const NATIONS: &[(&str, usize)] = &[
    ("UNITED STATES", 0),
    ("CANADA", 0),
    ("BRAZIL", 0),
    ("ARGENTINA", 0),
    ("PERU", 0),
    ("FRANCE", 1),
    ("GERMANY", 1),
    ("UNITED KINGDOM", 1),
    ("RUSSIA", 1),
    ("ROMANIA", 1),
    ("CHINA", 2),
    ("INDIA", 2),
    ("INDONESIA", 2),
    ("JAPAN", 2),
    ("VIETNAM", 2),
    ("EGYPT", 3),
    ("IRAN", 3),
    ("IRAQ", 3),
    ("JORDAN", 3),
    ("SAUDI ARABIA", 3),
    ("ALGERIA", 4),
    ("ETHIOPIA", 4),
    ("KENYA", 4),
    ("MOROCCO", 4),
    ("MOZAMBIQUE", 4),
];

const MARKET_SEGMENTS: &[&str] = &[
    "AUTOMOBILE",
    "BUILDING",
    "FURNITURE",
    "MACHINERY",
    "HOUSEHOLD",
];

const ORDER_PRIORITIES: &[&str] = &[
    "1-URGENT",
    "2-HIGH",
    "3-MEDIUM",
    "4-NOT SPECIFIED",
    "5-LOW",
];

const SHIP_MODES: &[&str] = &["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];

const PART_COLORS: &[&str] = &[
    "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched",
    "blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate",
    "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger",
    "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod",
    "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender",
    "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium",
];

const PART_TYPES: &[&str] = &[
    "STANDARD ANODIZED TIN",
    "STANDARD ANODIZED NICKEL",
    "STANDARD ANODIZED BRASS",
    "STANDARD ANODIZED STEEL",
    "STANDARD BURNISHED NICKEL",
    "ECONOMY ANODIZED TIN",
    "ECONOMY BURNISHED STEEL",
    "PROMO BURNISHED COPPER",
    "LARGE BURNISHED BRASS",
    "MEDIUM POLISHED COPPER",
    "SMALL POLISHED STEEL",
    "ECONOMY PLATED BRASS",
];

const PART_CONTAINERS: &[&str] = &[
    "SM CASE", "SM BOX", "SM BAG", "SM JAR", "SM PACK",
    "LG CASE", "LG BOX", "LG BAG", "LG JAR", "LG PACK",
    "MED CASE", "MED BOX", "MED BAG", "MED JAR", "MED PACK",
    "JUMBO CASE", "JUMBO BOX", "JUMBO BAG", "JUMBO JAR", "JUMBO PACK",
    "WRAP CASE", "WRAP BOX", "WRAP BAG", "WRAP JAR", "WRAP PACK",
];

/// SSB date range: 1992-01-01 to 1998-12-31
const SSB_YEAR_START: i32 = 1992;
const SSB_YEAR_END: i32 = 1998;

const DAYS_IN_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const MONTH_NAMES: [&str; 12] = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
];
const DAY_OF_WEEK_NAMES: [&str; 7] = [
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
];

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    if month == 2 && is_leap_year(year) {
        29
    } else {
        DAYS_IN_MONTH[(month - 1) as usize]
    }
}

fn days_in_year(year: i32) -> u32 {
    if is_leap_year(year) { 366 } else { 365 }
}

fn selling_season(month: u32) -> &'static str {
    match month {
        12 | 1 | 2 => "Winter",
        3..=5 => "Spring",
        6..=8 => "Summer",
        _ => "Autumn",
    }
}

/// Returns the day-of-week index (Mon=0 .. Sun=6) for a given (year, month, day).
/// Uses the fact that 1970-01-01 was a Thursday (index 3).
fn day_of_week_index(year: i32, month: u32, day: u32) -> usize {
    let mut total_days: i64 = 0;
    for y in 1970..year {
        total_days += days_in_year(y) as i64;
    }
    for m in 1..month {
        total_days += days_in_month(year, m) as i64;
    }
    total_days += (day - 1) as i64;
    // Thursday = 3, so (3 + total_days) % 7 gives Mon=0 index
    ((3 + total_days).rem_euclid(7)) as usize
}

/// Convert YYYYMMDD integer datekey to (year, month, day).
fn datekey_to_ymd(datekey: i32) -> (i32, u32, u32) {
    let year = datekey / 10000;
    let month = ((datekey % 10000) / 100) as u32;
    let day = (datekey % 100) as u32;
    (year, month, day)
}

/// Generate all SSB date keys in range [1992-01-01 .. 1998-12-31].
pub(crate) fn all_date_keys() -> Vec<i32> {
    let mut keys = Vec::new();
    for year in SSB_YEAR_START..=SSB_YEAR_END {
        for month in 1u32..=12 {
            for day in 1..=days_in_month(year, month) {
                keys.push(year * 10000 + month as i32 * 100 + day as i32);
            }
        }
    }
    keys
}

// ---------------------------------------------------------------------------
// Seed derivation
// ---------------------------------------------------------------------------

fn seed_for_table(name: &str) -> u64 {
    name.bytes()
        .enumerate()
        .fold(0u64, |acc, (i, b)| acc ^ ((b as u64).wrapping_shl(i as u32 % 64)))
        .wrapping_add(0xBEEF_CAFE_1234_5678)
}

// ---------------------------------------------------------------------------
// String helpers
// ---------------------------------------------------------------------------

fn random_word(rng: &mut StdRng, len: usize) -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    (0..len)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

fn random_address(rng: &mut StdRng) -> String {
    let len = rng.gen_range(10..40usize);
    random_word(rng, len)
}

fn random_phone(rng: &mut StdRng, nation_idx: usize) -> String {
    format!(
        "{:02}-{:03}-{:03}-{:04}",
        10 + (nation_idx % 25),
        rng.gen_range(100..999i32),
        rng.gen_range(100..999i32),
        rng.gen_range(1000..9999i32),
    )
}

/// City name format per ssb-dbgen (`"%-9.9s%d"`): the nation name truncated
/// or space-padded to exactly 9 characters, followed by one digit. Queries
/// q3.3/q3.4 probe literals like 'UNITED KI1', so any other format makes them
/// select nothing.
fn random_city(rng: &mut StdRng, nation_name: &str) -> String {
    format!("{:<9.9}{}", nation_name, rng.gen_range(0..10u32))
}

// ---------------------------------------------------------------------------
// Table generators
// ---------------------------------------------------------------------------

const BATCH_SIZE: usize = 10_000;

fn generate_dim_date() -> (SchemaRef, Vec<RecordBatch>) {
    let schema = dim_date_schema();
    let keys = all_date_keys();

    let mut d_datekey = Vec::with_capacity(keys.len());
    let mut d_date = Vec::with_capacity(keys.len());
    let mut d_dayofweek = Vec::with_capacity(keys.len());
    let mut d_month = Vec::with_capacity(keys.len());
    let mut d_year = Vec::with_capacity(keys.len());
    let mut d_yearmonthnum = Vec::with_capacity(keys.len());
    let mut d_yearmonth = Vec::with_capacity(keys.len());
    let mut d_daynuminweek = Vec::with_capacity(keys.len());
    let mut d_daynuminmonth = Vec::with_capacity(keys.len());
    let mut d_daynuminyear = Vec::with_capacity(keys.len());
    let mut d_monthnuminyear = Vec::with_capacity(keys.len());
    let mut d_weeknuminyear = Vec::with_capacity(keys.len());
    let mut d_sellingseason = Vec::with_capacity(keys.len());
    let mut d_lastdayinweekfl = Vec::with_capacity(keys.len());
    let mut d_lastdayinmonthfl = Vec::with_capacity(keys.len());
    let mut d_holidayfl = Vec::with_capacity(keys.len());
    let mut d_weekdayfl = Vec::with_capacity(keys.len());

    let mut prev_year = 0i32;
    let mut day_in_year = 0u32;

    for &key in &keys {
        let (year, month, day) = datekey_to_ymd(key);

        if year != prev_year {
            day_in_year = 0;
            prev_year = year;
        }
        day_in_year += 1;

        let dow_idx = day_of_week_index(year, month, day); // Mon=0 .. Sun=6
        let dow_name = DAY_OF_WEEK_NAMES[dow_idx];
        let month_name = MONTH_NAMES[(month - 1) as usize];
        // Abbreviated month + full 4-digit year, e.g. "Jan1992". Query q3.4
        // probes d_yearmonth = 'Dec1997'; a 2-digit year ("Dec97") makes it
        // vacuous.
        let yearmonth = format!("{}{}", &month_name[..3], year);
        let week_num = ((day_in_year - 1) / 7 + 1) as i32;
        let last_day_in_month = if day == days_in_month(year, month) { 1i32 } else { 0 };
        let last_day_in_week = if dow_idx == 6 { 1i32 } else { 0 }; // Sunday
        let is_weekday = if dow_idx < 5 { 1i32 } else { 0 };
        let is_holiday = if (month == 1 && day == 1) || (month == 12 && day == 25) {
            1i32
        } else {
            0
        };

        d_datekey.push(key);
        d_date.push(format!("{:04}-{:02}-{:02}", year, month, day));
        d_dayofweek.push(dow_name.to_string());
        d_month.push(month_name.to_string());
        d_year.push(year);
        d_yearmonthnum.push(year * 100 + month as i32);
        d_yearmonth.push(yearmonth);
        d_daynuminweek.push(dow_idx as i32 + 1); // 1=Mon .. 7=Sun
        d_daynuminmonth.push(day as i32);
        d_daynuminyear.push(day_in_year as i32);
        d_monthnuminyear.push(month as i32);
        d_weeknuminyear.push(week_num);
        d_sellingseason.push(selling_season(month).to_string());
        d_lastdayinweekfl.push(last_day_in_week);
        d_lastdayinmonthfl.push(last_day_in_month);
        d_holidayfl.push(is_holiday);
        d_weekdayfl.push(is_weekday);
    }

    let date_refs: Vec<&str> = d_date.iter().map(|s| s.as_str()).collect();
    let dow_refs: Vec<&str> = d_dayofweek.iter().map(|s| s.as_str()).collect();
    let month_refs: Vec<&str> = d_month.iter().map(|s| s.as_str()).collect();
    let yearmonth_refs: Vec<&str> = d_yearmonth.iter().map(|s| s.as_str()).collect();
    let season_refs: Vec<&str> = d_sellingseason.iter().map(|s| s.as_str()).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(d_datekey)),
            Arc::new(StringArray::from(date_refs)),
            Arc::new(StringArray::from(dow_refs)),
            Arc::new(StringArray::from(month_refs)),
            Arc::new(Int32Array::from(d_year)),
            Arc::new(Int32Array::from(d_yearmonthnum)),
            Arc::new(StringArray::from(yearmonth_refs)),
            Arc::new(Int32Array::from(d_daynuminweek)),
            Arc::new(Int32Array::from(d_daynuminmonth)),
            Arc::new(Int32Array::from(d_daynuminyear)),
            Arc::new(Int32Array::from(d_monthnuminyear)),
            Arc::new(Int32Array::from(d_weeknuminyear)),
            Arc::new(StringArray::from(season_refs)),
            Arc::new(Int32Array::from(d_lastdayinweekfl)),
            Arc::new(Int32Array::from(d_lastdayinmonthfl)),
            Arc::new(Int32Array::from(d_holidayfl)),
            Arc::new(Int32Array::from(d_weekdayfl)),
        ],
    )
    .expect("dim_date batch");

    (schema, vec![batch])
}

fn generate_customer(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = customer_schema();
    let total = super::scaled(scale, 30_000.0);
    let total = total.max(1);
    let mut rng = StdRng::seed_from_u64(seed_for_table("customer"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut c_custkey = Vec::with_capacity(n);
        let mut c_name = Vec::with_capacity(n);
        let mut c_address = Vec::with_capacity(n);
        let mut c_city = Vec::with_capacity(n);
        let mut c_nation = Vec::with_capacity(n);
        let mut c_region = Vec::with_capacity(n);
        let mut c_phone = Vec::with_capacity(n);
        let mut c_mktsegment = Vec::with_capacity(n);

        for i in 0..n {
            let key = (offset + i + 1) as i32;
            let nation_idx = rng.gen_range(0..NATIONS.len());
            let (nation_name, region_idx) = NATIONS[nation_idx];
            let region_name = REGIONS[region_idx];

            c_custkey.push(key);
            c_name.push(format!("Customer#{:09}", key));
            c_address.push(random_address(&mut rng));
            c_city.push(random_city(&mut rng, nation_name));
            c_nation.push(nation_name.to_string());
            c_region.push(region_name.to_string());
            c_phone.push(random_phone(&mut rng, nation_idx));
            c_mktsegment
                .push(MARKET_SEGMENTS[rng.gen_range(0..MARKET_SEGMENTS.len())].to_string());
        }

        let name_refs: Vec<&str> = c_name.iter().map(|s| s.as_str()).collect();
        let addr_refs: Vec<&str> = c_address.iter().map(|s| s.as_str()).collect();
        let city_refs: Vec<&str> = c_city.iter().map(|s| s.as_str()).collect();
        let nation_refs: Vec<&str> = c_nation.iter().map(|s| s.as_str()).collect();
        let region_refs: Vec<&str> = c_region.iter().map(|s| s.as_str()).collect();
        let phone_refs: Vec<&str> = c_phone.iter().map(|s| s.as_str()).collect();
        let seg_refs: Vec<&str> = c_mktsegment.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(c_custkey)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(StringArray::from(addr_refs)),
                    Arc::new(StringArray::from(city_refs)),
                    Arc::new(StringArray::from(nation_refs)),
                    Arc::new(StringArray::from(region_refs)),
                    Arc::new(StringArray::from(phone_refs)),
                    Arc::new(StringArray::from(seg_refs)),
                ],
            )
            .expect("customer batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_supplier(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = supplier_schema();
    let total = super::scaled(scale, 2_000.0);
    let total = total.max(1);
    let mut rng = StdRng::seed_from_u64(seed_for_table("supplier"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut s_suppkey = Vec::with_capacity(n);
        let mut s_name = Vec::with_capacity(n);
        let mut s_address = Vec::with_capacity(n);
        let mut s_city = Vec::with_capacity(n);
        let mut s_nation = Vec::with_capacity(n);
        let mut s_region = Vec::with_capacity(n);
        let mut s_phone = Vec::with_capacity(n);

        for i in 0..n {
            let key = (offset + i + 1) as i32;
            let nation_idx = rng.gen_range(0..NATIONS.len());
            let (nation_name, region_idx) = NATIONS[nation_idx];
            let region_name = REGIONS[region_idx];

            s_suppkey.push(key);
            s_name.push(format!("Supplier#{:09}", key));
            s_address.push(random_address(&mut rng));
            s_city.push(random_city(&mut rng, nation_name));
            s_nation.push(nation_name.to_string());
            s_region.push(region_name.to_string());
            s_phone.push(random_phone(&mut rng, nation_idx));
        }

        let name_refs: Vec<&str> = s_name.iter().map(|s| s.as_str()).collect();
        let addr_refs: Vec<&str> = s_address.iter().map(|s| s.as_str()).collect();
        let city_refs: Vec<&str> = s_city.iter().map(|s| s.as_str()).collect();
        let nation_refs: Vec<&str> = s_nation.iter().map(|s| s.as_str()).collect();
        let region_refs: Vec<&str> = s_region.iter().map(|s| s.as_str()).collect();
        let phone_refs: Vec<&str> = s_phone.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(s_suppkey)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(StringArray::from(addr_refs)),
                    Arc::new(StringArray::from(city_refs)),
                    Arc::new(StringArray::from(nation_refs)),
                    Arc::new(StringArray::from(region_refs)),
                    Arc::new(StringArray::from(phone_refs)),
                ],
            )
            .expect("supplier batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_part(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = part_schema();
    // SSB spec: SF × 200,000 × 0.4 = SF × 80,000
    let total = super::scaled(scale, 80_000.0);
    let total = total.max(1);
    let mut rng = StdRng::seed_from_u64(seed_for_table("part"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut p_partkey = Vec::with_capacity(n);
        let mut p_name = Vec::with_capacity(n);
        let mut p_mfgr = Vec::with_capacity(n);
        let mut p_category = Vec::with_capacity(n);
        let mut p_brand = Vec::with_capacity(n);
        let mut p_color = Vec::with_capacity(n);
        let mut p_type = Vec::with_capacity(n);
        let mut p_size = Vec::with_capacity(n);
        let mut p_container = Vec::with_capacity(n);

        for i in 0..n {
            let key = (offset + i + 1) as i32;
            let mfgr_num = rng.gen_range(1..=5i32);
            let cat_num = rng.gen_range(1..=5i32);
            // ssb-dbgen brand = category + zero-padded 01..40, e.g.
            // 'MFGR#2221'. Queries q2.2/q2.3 probe 4-digit brand literals.
            let brand_num = rng.gen_range(1..=40i32);
            let color = PART_COLORS[rng.gen_range(0..PART_COLORS.len())];

            p_partkey.push(key);
            p_name.push(format!(
                "{} {}",
                color,
                PART_COLORS[rng.gen_range(0..PART_COLORS.len())]
            ));
            p_mfgr.push(format!("MFGR#{}", mfgr_num));
            p_category.push(format!("MFGR#{}{}", mfgr_num, cat_num));
            p_brand.push(format!("MFGR#{}{}{:02}", mfgr_num, cat_num, brand_num));
            p_color.push(color.to_string());
            p_type.push(PART_TYPES[rng.gen_range(0..PART_TYPES.len())].to_string());
            p_size.push(rng.gen_range(1..=50i32));
            p_container.push(PART_CONTAINERS[rng.gen_range(0..PART_CONTAINERS.len())].to_string());
        }

        let name_refs: Vec<&str> = p_name.iter().map(|s| s.as_str()).collect();
        let mfgr_refs: Vec<&str> = p_mfgr.iter().map(|s| s.as_str()).collect();
        let cat_refs: Vec<&str> = p_category.iter().map(|s| s.as_str()).collect();
        let brand_refs: Vec<&str> = p_brand.iter().map(|s| s.as_str()).collect();
        let color_refs: Vec<&str> = p_color.iter().map(|s| s.as_str()).collect();
        let type_refs: Vec<&str> = p_type.iter().map(|s| s.as_str()).collect();
        let container_refs: Vec<&str> = p_container.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(p_partkey)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(StringArray::from(mfgr_refs)),
                    Arc::new(StringArray::from(cat_refs)),
                    Arc::new(StringArray::from(brand_refs)),
                    Arc::new(StringArray::from(color_refs)),
                    Arc::new(StringArray::from(type_refs)),
                    Arc::new(Int32Array::from(p_size)),
                    Arc::new(StringArray::from(container_refs)),
                ],
            )
            .expect("part batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_lineorder(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = lineorder_schema();
    let total = super::scaled(scale, 6_000_000.0);
    let total = total.max(1);
    let num_customers = (scale * 30_000.0) as i32;
    let num_parts = (scale * 80_000.0) as i32;
    let num_suppliers = (scale * 2_000.0) as i32;
    let mut rng = StdRng::seed_from_u64(seed_for_table("lineorder"));
    let mut batches = Vec::new();

    // Ordered list of every valid date key, so a commit date can be expressed as
    // the order date advanced by a number of days (datekeys are YYYYMMDD, which
    // can't be added directly).
    let date_keys = all_date_keys();
    let n_dates = date_keys.len();

    // State for multi-line orders
    let mut order_key: i64 = 0;
    let mut line_number: i32 = 1;
    let mut order_idx: usize = rng.gen_range(0..n_dates);
    let mut order_date: i32 = date_keys[order_idx];
    let mut order_total: f64 = (rng.gen_range(10_000..5_000_000_i64) as f64) / 100.0;

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut lo_orderkey = Vec::with_capacity(n);
        let mut lo_linenumber = Vec::with_capacity(n);
        let mut lo_custkey = Vec::with_capacity(n);
        let mut lo_partkey = Vec::with_capacity(n);
        let mut lo_suppkey = Vec::with_capacity(n);
        let mut lo_orderdate = Vec::with_capacity(n);
        let mut lo_orderpriority = Vec::with_capacity(n);
        let mut lo_shippriority = Vec::with_capacity(n);
        let mut lo_quantity = Vec::with_capacity(n);
        let mut lo_extendedprice = Vec::with_capacity(n);
        let mut lo_ordtotalprice = Vec::with_capacity(n);
        let mut lo_discount = Vec::with_capacity(n);
        let mut lo_revenue = Vec::with_capacity(n);
        let mut lo_supplycost = Vec::with_capacity(n);
        let mut lo_tax = Vec::with_capacity(n);
        let mut lo_commitdate = Vec::with_capacity(n);
        let mut lo_shipmode = Vec::with_capacity(n);

        for _ in 0..n {
            if line_number == 1 {
                order_key += 1;
                order_idx = rng.gen_range(0..n_dates);
                order_date = date_keys[order_idx];
                order_total = (rng.gen_range(10_000..5_000_000_i64) as f64) / 100.0;
            }

            let partkey = rng.gen_range(1..=num_parts.max(1));
            let suppkey = 1 + (partkey + rng.gen_range(0..4i32)) % num_suppliers.max(1);
            let quantity = rng.gen_range(1..=50i32);
            let base_price = 900.0 + (partkey as f64 / 10.0) % 2000.0;
            let extended_price = quantity as f64 * base_price;
            let discount_pct = rng.gen_range(0..=10i32); // integer percent
            let revenue = extended_price * (1.0 - discount_pct as f64 / 100.0);
            let supply_cost = base_price * 0.6 + (rng.gen_range(0..100) as f64);
            let tax_pct = rng.gen_range(0..=8i32);
            // Commit date is the order date advanced 1..121 days (SSB inherits the
            // TPC-H ship-window), expressed via the ordered date-key list.
            let commit_date = date_keys[(order_idx + rng.gen_range(1..=121usize)).min(n_dates - 1)];

            lo_orderkey.push(order_key);
            lo_linenumber.push(line_number);
            lo_custkey.push(rng.gen_range(1..=num_customers.max(1)));
            lo_partkey.push(partkey);
            lo_suppkey.push(suppkey);
            lo_orderdate.push(order_date);
            lo_orderpriority
                .push(ORDER_PRIORITIES[rng.gen_range(0..ORDER_PRIORITIES.len())].to_string());
            lo_shippriority.push(0i32);
            lo_quantity.push(quantity);
            lo_extendedprice.push(extended_price);
            lo_ordtotalprice.push(order_total);
            lo_discount.push(discount_pct);
            lo_revenue.push(revenue);
            lo_supplycost.push(supply_cost);
            lo_tax.push(tax_pct);
            lo_commitdate.push(commit_date);
            lo_shipmode.push(SHIP_MODES[rng.gen_range(0..SHIP_MODES.len())].to_string());

            // Advance line_number; when it exceeds a random 1..=7 bound, reset
            line_number += 1;
            if line_number > rng.gen_range(1..=7i32) {
                line_number = 1;
            }
        }

        let priority_refs: Vec<&str> = lo_orderpriority.iter().map(|s| s.as_str()).collect();
        let mode_refs: Vec<&str> = lo_shipmode.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(lo_orderkey)),
                    Arc::new(Int32Array::from(lo_linenumber)),
                    Arc::new(Int32Array::from(lo_custkey)),
                    Arc::new(Int32Array::from(lo_partkey)),
                    Arc::new(Int32Array::from(lo_suppkey)),
                    Arc::new(Int32Array::from(lo_orderdate)),
                    Arc::new(StringArray::from(priority_refs)),
                    Arc::new(Int32Array::from(lo_shippriority)),
                    Arc::new(Int32Array::from(lo_quantity)),
                    Arc::new(Float64Array::from(lo_extendedprice)),
                    Arc::new(Float64Array::from(lo_ordtotalprice)),
                    Arc::new(Int32Array::from(lo_discount)),
                    Arc::new(Float64Array::from(lo_revenue)),
                    Arc::new(Float64Array::from(lo_supplycost)),
                    Arc::new(Int32Array::from(lo_tax)),
                    Arc::new(Int32Array::from(lo_commitdate)),
                    Arc::new(StringArray::from(mode_refs)),
                ],
            )
            .expect("lineorder batch"),
        );
        offset += n;
    }

    (schema, batches)
}

// ---------------------------------------------------------------------------
// BenchmarkGenerator impl
// ---------------------------------------------------------------------------

impl BenchmarkGenerator for SsbGenerator {
    fn name(&self) -> &str {
        "ssb"
    }

    fn tables(&self) -> Vec<TableDef> {
        vec![
            TableDef {
                name: "dim_date".into(),
                schema: dim_date_schema(),
                row_count: |_| all_date_keys().len(),
            },
            TableDef {
                name: "customer".into(),
                schema: customer_schema(),
                row_count: |sf| (sf * 30_000.0) as usize,
            },
            TableDef {
                name: "supplier".into(),
                schema: supplier_schema(),
                row_count: |sf| (sf * 2_000.0) as usize,
            },
            TableDef {
                name: "part".into(),
                schema: part_schema(),
                row_count: |sf| (sf * 80_000.0) as usize,
            },
            TableDef {
                name: "lineorder".into(),
                schema: lineorder_schema(),
                row_count: |sf| (sf * 6_000_000.0) as usize,
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
            "dim_date" => generate_dim_date(),
            "customer" => generate_customer(scale),
            "supplier" => generate_supplier(scale),
            "part" => generate_part(scale),
            "lineorder" => generate_lineorder(scale),
            _ => anyhow::bail!("Unknown SSB table: {table}"),
        };

        let full_output = format!("{output_dir}/ssb/sf{scale}");
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
        let gen = SsbGenerator;
        let tables = gen.tables();
        assert_eq!(tables.len(), 5);
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"dim_date"));
        assert!(names.contains(&"customer"));
        assert!(names.contains(&"supplier"));
        assert!(names.contains(&"part"));
        assert!(names.contains(&"lineorder"));
    }

    #[test]
    fn test_dim_date_schema() {
        let schema = dim_date_schema();
        assert_eq!(schema.fields().len(), 17);
        assert_eq!(schema.field(0).name(), "d_datekey");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(4).name(), "d_year");
        assert_eq!(schema.field(4).data_type(), &DataType::Int32);
    }

    #[test]
    fn test_generate_dim_date() {
        let (schema, batches) = generate_dim_date();
        // 1992 (leap 366) + 1993-1995 (365 each) + 1996 (leap 366) + 1997-1998 (365 each) = 2557
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2557);
        assert_eq!(batches[0].schema(), schema);
        // Verify first key is 19920101
        let date_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(date_col.value(0), 19920101);
    }

    #[test]
    fn test_dim_date_last_key() {
        let (_, batches) = generate_dim_date();
        let last_batch = batches.last().unwrap();
        let date_col = last_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let last = date_col.value(last_batch.num_rows() - 1);
        assert_eq!(last, 19981231);
    }

    #[test]
    fn test_row_counts_sf001() {
        let gen = SsbGenerator;
        let sf = 0.01_f64;
        for t in gen.tables() {
            let expected = (t.row_count)(sf);
            match t.name.as_str() {
                "dim_date" => assert_eq!(expected, 2557),
                "customer" => assert_eq!(expected, 300),
                "supplier" => assert_eq!(expected, 20),
                "part" => assert_eq!(expected, 800),
                "lineorder" => assert_eq!(expected, 60_000),
                _ => {}
            }
        }
    }

    #[test]
    fn test_generate_customer_sf001() {
        let (schema, batches) = generate_customer(0.01);
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 300);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_generate_supplier_sf001() {
        let (schema, batches) = generate_supplier(0.01);
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 20);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_generate_part_sf001() {
        let (schema, batches) = generate_part(0.01);
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 800);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_generate_lineorder_sf001() {
        let (schema, batches) = generate_lineorder(0.01);
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 60_000);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_customer_schema_fields() {
        let schema = customer_schema();
        assert_eq!(schema.fields().len(), 8);
        assert_eq!(schema.field(0).name(), "c_custkey");
        assert_eq!(schema.field(3).name(), "c_city");
        assert_eq!(schema.field(4).name(), "c_nation");
        assert_eq!(schema.field(5).name(), "c_region");
    }

    #[test]
    fn test_supplier_schema_fields() {
        let schema = supplier_schema();
        assert_eq!(schema.fields().len(), 7);
        assert_eq!(schema.field(0).name(), "s_suppkey");
        assert_eq!(schema.field(3).name(), "s_city");
    }

    #[test]
    fn test_part_schema_fields() {
        let schema = part_schema();
        assert_eq!(schema.fields().len(), 9);
        assert_eq!(schema.field(0).name(), "p_partkey");
        assert_eq!(schema.field(3).name(), "p_category");
        assert_eq!(schema.field(4).name(), "p_brand");
    }

    #[test]
    fn test_lineorder_schema_fields() {
        let schema = lineorder_schema();
        assert_eq!(schema.fields().len(), 17);
        assert_eq!(schema.field(0).name(), "lo_orderkey");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        // lo_orderdate and lo_commitdate must be Int32 (YYYYMMDD), not Date32
        assert_eq!(schema.field(5).name(), "lo_orderdate");
        assert_eq!(schema.field(5).data_type(), &DataType::Int32);
        assert_eq!(schema.field(15).name(), "lo_commitdate");
        assert_eq!(schema.field(15).data_type(), &DataType::Int32);
    }

    #[test]
    fn test_generate_table_to_parquet() {
        let gen = SsbGenerator;
        let output = "/tmp/sqe-bench-test-ssb-parquet";

        let stats = gen.generate_table("dim_date", 1.0, output, &Default::default()).unwrap();
        assert_eq!(stats.rows, 2557);
        assert_eq!(stats.files, 1);
        assert!(stats.bytes > 0);
    }

    #[test]
    fn test_generate_all_tables_sf001() {
        let gen = SsbGenerator;
        let output = "/tmp/sqe-bench-test-ssb-all";
        let sf = 0.01_f64;

        for table in gen.tables() {
            let stats = gen.generate_table(&table.name, sf, output, &Default::default()).unwrap();
            let expected = (table.row_count)(sf);
            assert_eq!(
                stats.rows, expected,
                "row count mismatch for {}",
                table.name
            );
            assert!(stats.bytes > 0, "no bytes written for {}", table.name);
        }
    }

    #[test]
    fn test_lineorder_datekeys_are_integers() {
        let (_, batches) = generate_lineorder(0.001);
        let batch = &batches[0];
        // lo_orderdate column (index 5) must be Int32Array
        let col = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("lo_orderdate must be Int32Array");
        // Verify sample values are in YYYYMMDD format within the SSB date range
        for i in 0..col.len().min(10) {
            let v = col.value(i);
            assert!((19920101..=19981231).contains(&v), "datekey {v} out of range");
        }
    }

    #[test]
    fn test_all_date_keys_count() {
        let keys = all_date_keys();
        // 1992 (366) + 1993 (365) + 1994 (365) + 1995 (365) + 1996 (366) + 1997 (365) + 1998 (365)
        assert_eq!(keys.len(), 2557);
    }

    #[test]
    fn test_generator_name() {
        assert_eq!(SsbGenerator.name(), "ssb");
    }

    #[test]
    fn test_unknown_table_errors() {
        let gen = SsbGenerator;
        assert!(gen.generate_table("no_such_table", 1.0, "/tmp", &Default::default()).is_err());
    }

    #[test]
    fn p_brand_matches_dbgen_four_digit_format() {
        use arrow_array::Array as _;
        // q2.2 probes BETWEEN 'MFGR#2221' AND 'MFGR#2228'; 3-digit brand
        // suffixes made that range empty.
        let (sch, batches) = generate_part(0.1);
        let idx = sch.index_of("p_brand").unwrap();
        let mut in_q22_range = 0usize;
        for b in &batches {
            let col = b.column(idx).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..col.len() {
                let v = col.value(i);
                assert_eq!(v.len(), 9, "brand must be MFGR#mcnn, got '{v}'");
                let digits = &v[5..];
                let m: u32 = digits[0..1].parse().unwrap();
                let c: u32 = digits[1..2].parse().unwrap();
                let nn: u32 = digits[2..4].parse().unwrap();
                assert!((1..=5).contains(&m) && (1..=5).contains(&c));
                assert!((1..=40).contains(&nn), "brand number {nn} outside 01..40 in '{v}'");
                if ("MFGR#2221".."MFGR#2229").contains(&v) {
                    in_q22_range += 1;
                }
            }
        }
        assert!(in_q22_range > 0, "q2.2 brand range matched no parts");
    }

    #[test]
    fn d_yearmonth_uses_three_letter_month_and_full_year() {
        use arrow_array::Array as _;
        // q3.4 probes d_yearmonth = 'Dec1997'; the old 'Dec97' format made
        // the query vacuous.
        let (sch, batches) = generate_dim_date();
        let key_idx = sch.index_of("d_datekey").unwrap();
        let ym_idx = sch.index_of("d_yearmonth").unwrap();
        let mut dec_1997_rows = 0usize;
        for b in &batches {
            let keys = b.column(key_idx).as_any().downcast_ref::<Int32Array>().unwrap();
            let yms = b.column(ym_idx).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..keys.len() {
                let v = yms.value(i);
                assert_eq!(v.len(), 7, "d_yearmonth must be MonYYYY, got '{v}'");
                if (19971201..=19971231).contains(&keys.value(i)) {
                    assert_eq!(v, "Dec1997");
                    dec_1997_rows += 1;
                }
            }
        }
        assert_eq!(dec_1997_rows, 31, "December 1997 must have 31 days");
    }

    #[test]
    fn cities_use_nine_char_padded_nation_plus_digit() {
        use arrow_array::Array as _;
        // q3.3/q3.4 probe 'UNITED KI1'-style literals: nation truncated or
        // space-padded to 9 chars, then one digit (ssb-dbgen "%-9.9s%d").
        let (sch, batches) = generate_customer(0.1);
        let idx = sch.index_of("c_city").unwrap();
        let mut united_ki = false;
        for b in &batches {
            let col = b.column(idx).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..col.len() {
                let v = col.value(i);
                assert_eq!(v.chars().count(), 10, "city must be exactly 10 chars, got '{v}'");
                assert!(v.chars().last().unwrap().is_ascii_digit());
                if v.starts_with("UNITED KI") {
                    united_ki = true;
                }
            }
        }
        assert!(united_ki, "no 'UNITED KI*' city generated; q3.3/q3.4 stay vacuous");
    }
}
