use std::sync::Arc;

use arrow_array::{
    Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{parquet_writer, BenchmarkGenerator, GenerateStats, TableDef};

pub struct TpchGenerator;

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

fn region_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int32, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, true),
    ]))
}

fn nation_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int32, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int32, false),
        Field::new("n_comment", DataType::Utf8, true),
    ]))
}

fn supplier_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int32, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, true),
        Field::new("s_nationkey", DataType::Int32, false),
        Field::new("s_phone", DataType::Utf8, true),
        Field::new("s_acctbal", DataType::Float64, false),
        Field::new("s_comment", DataType::Utf8, true),
    ]))
}

fn customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int32, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, true),
        Field::new("c_nationkey", DataType::Int32, false),
        Field::new("c_phone", DataType::Utf8, true),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_mktsegment", DataType::Utf8, true),
        Field::new("c_comment", DataType::Utf8, true),
    ]))
}

fn part_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int32, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, true),
        Field::new("p_brand", DataType::Utf8, true),
        Field::new("p_type", DataType::Utf8, true),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, true),
        Field::new("p_retailprice", DataType::Float64, false),
        Field::new("p_comment", DataType::Utf8, true),
    ]))
}

fn partsupp_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int32, false),
        Field::new("ps_suppkey", DataType::Int32, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new("ps_supplycost", DataType::Float64, false),
        Field::new("ps_comment", DataType::Utf8, true),
    ]))
}

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int32, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, true),
        Field::new("o_clerk", DataType::Utf8, true),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, true),
    ]))
}

fn lineitem_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int32, false),
        Field::new("l_suppkey", DataType::Int32, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, true),
        Field::new("l_linestatus", DataType::Utf8, true),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, true),
        Field::new("l_shipmode", DataType::Utf8, true),
        Field::new("l_comment", DataType::Utf8, true),
    ]))
}

// ---------------------------------------------------------------------------
// Date utilities
// ---------------------------------------------------------------------------

#[cfg(test)]
fn days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
    use chrono::NaiveDate;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
    (date - epoch).num_days() as i32
}

// TPC-H date range: 1992-01-01 to 1998-12-31
const DATE_START: i32 = 8035; // days_since_epoch(1992, 1, 1)
const DATE_RANGE: i32 = 2556; // ~7 years in days

fn random_date(rng: &mut StdRng) -> i32 {
    DATE_START + rng.gen_range(0..DATE_RANGE)
}

// ---------------------------------------------------------------------------
// Seed derivation
// ---------------------------------------------------------------------------

fn seed_for_table(name: &str) -> u64 {
    // Simple deterministic hash of the table name
    name.bytes()
        .enumerate()
        .fold(0u64, |acc, (i, b)| acc ^ ((b as u64).wrapping_shl(i as u32 % 64)))
        .wrapping_add(0xDEAD_BEEF_CAFE_1234)
}

// ---------------------------------------------------------------------------
// Fixed TPC-H reference data
// ---------------------------------------------------------------------------

const REGIONS: &[(&str, &str)] = &[
    ("AFRICA", "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to"),
    ("AMERICA", "hs use ironic, even requests. s"),
    ("ASIA", "ges. thinly even pinto beans ca"),
    ("EUROPE", "ly final courts cajole furiously final excuse"),
    ("MIDDLE EAST", "uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl"),
];

const NATIONS: &[(&str, i32)] = &[
    ("ALGERIA", 0),
    ("ARGENTINA", 1),
    ("BRAZIL", 1),
    ("CANADA", 1),
    ("EGYPT", 4),
    ("ETHIOPIA", 0),
    ("FRANCE", 3),
    ("GERMANY", 3),
    ("INDIA", 2),
    ("INDONESIA", 2),
    ("IRAN", 4),
    ("IRAQ", 4),
    ("JAPAN", 2),
    ("JORDAN", 4),
    ("KENYA", 0),
    ("MOROCCO", 0),
    ("MOZAMBIQUE", 0),
    ("PERU", 1),
    ("CHINA", 2),
    ("ROMANIA", 3),
    ("SAUDI ARABIA", 4),
    ("VIETNAM", 2),
    ("RUSSIA", 3),
    ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
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

const RETURN_FLAGS: &[&str] = &["R", "A", "N"];

const LINE_STATUSES: &[&str] = &["O", "F"];

const SHIP_INSTRUCTS: &[&str] = &[
    "DELIVER IN PERSON",
    "COLLECT COD",
    "NONE",
    "TAKE BACK RETURN",
];

const PART_TYPES: &[&str] = &[
    "STANDARD ANODIZED TIN",
    "STANDARD ANODIZED NICKEL",
    "STANDARD ANODIZED BRASS",
    "STANDARD ANODIZED STEEL",
    "STANDARD ANODIZED COPPER",
    "STANDARD BURNISHED TIN",
    "STANDARD BURNISHED NICKEL",
    "STANDARD BURNISHED BRASS",
    "ECONOMY ANODIZED TIN",
    "ECONOMY ANODIZED NICKEL",
    "ECONOMY BURNISHED TIN",
    "PROMO BURNISHED COPPER",
    "LARGE BURNISHED BRASS",
    "MEDIUM BURNISHED STEEL",
    "SMALL POLISHED COPPER",
];

const PART_CONTAINERS: &[&str] = &[
    "SM CASE", "SM BOX", "SM BAG", "SM JAR", "SM PACK", "SM CAN",
    "LG CASE", "LG BOX", "LG BAG", "LG JAR", "LG PACK", "LG CAN",
    "MED CASE", "MED BOX", "MED BAG", "MED JAR", "MED PACK", "MED CAN",
    "JUMBO CASE", "JUMBO BOX", "JUMBO BAG", "JUMBO JAR", "JUMBO PACK", "JUMBO CAN",
    "WRAP CASE", "WRAP BOX", "WRAP BAG", "WRAP JAR", "WRAP PACK", "WRAP CAN",
];

const PART_COLORS: &[&str] = &[
    "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched",
    "blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate",
    "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim",
    "dodger", "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost",
    "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki",
    "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta",
    "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
    "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru",
    "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy",
    "royal", "saddle", "salmon", "sandy", "sea", "seashell", "sienna", "sky",
    "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato",
    "turquoise", "violet", "wheat", "white", "yellow",
];

// ---------------------------------------------------------------------------
// Random word generators
// ---------------------------------------------------------------------------

fn random_word(rng: &mut StdRng, len: usize) -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    (0..len).map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char).collect()
}

fn random_address(rng: &mut StdRng) -> String {
    let len = rng.gen_range(10..40usize);
    random_word(rng, len)
}

fn random_phone(rng: &mut StdRng, nationkey: i32) -> String {
    format!(
        "{:02}-{:03}-{:03}-{:04}",
        10 + (nationkey % 25),
        rng.gen_range(100..999i32),
        rng.gen_range(100..999i32),
        rng.gen_range(1000..9999i32),
    )
}

fn random_comment(rng: &mut StdRng) -> String {
    let words = rng.gen_range(3..8usize);
    let mut parts = Vec::with_capacity(words);
    for _ in 0..words {
        let len = rng.gen_range(3..10usize);
        parts.push(random_word(rng, len));
    }
    parts.join(" ")
}

// ---------------------------------------------------------------------------
// Fixed table generators (region, nation)
// ---------------------------------------------------------------------------

fn generate_region() -> (SchemaRef, Vec<RecordBatch>) {
    let schema = region_schema();
    let keys: Vec<i32> = (0..5).collect();
    let names: Vec<&str> = REGIONS.iter().map(|r| r.0).collect();
    let comments: Vec<&str> = REGIONS.iter().map(|r| r.1).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(comments)),
        ],
    )
    .expect("region batch");

    (schema, vec![batch])
}

fn generate_nation() -> (SchemaRef, Vec<RecordBatch>) {
    let schema = nation_schema();
    let keys: Vec<i32> = (0..25).collect();
    let names: Vec<&str> = NATIONS.iter().map(|n| n.0).collect();
    let regionkeys: Vec<i32> = NATIONS.iter().map(|n| n.1).collect();

    let mut rng = StdRng::seed_from_u64(seed_for_table("nation"));
    let comments: Vec<String> = (0..25).map(|_| random_comment(&mut rng)).collect();
    let comment_refs: Vec<&str> = comments.iter().map(|s| s.as_str()).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(regionkeys)),
            Arc::new(StringArray::from(comment_refs)),
        ],
    )
    .expect("nation batch");

    (schema, vec![batch])
}

// ---------------------------------------------------------------------------
// Scaled table generators
// ---------------------------------------------------------------------------

const BATCH_SIZE: usize = 10_000;

fn generate_supplier(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = supplier_schema();
    let total = (scale * 10_000.0) as usize;
    let mut rng = StdRng::seed_from_u64(seed_for_table("supplier"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut s_suppkey = Vec::with_capacity(n);
        let mut s_name = Vec::with_capacity(n);
        let mut s_address = Vec::with_capacity(n);
        let mut s_nationkey = Vec::with_capacity(n);
        let mut s_phone = Vec::with_capacity(n);
        let mut s_acctbal = Vec::with_capacity(n);
        let mut s_comment = Vec::with_capacity(n);

        for i in 0..n {
            let key = (offset + i + 1) as i32;
            let nk = rng.gen_range(0..25i32);
            s_suppkey.push(key);
            s_name.push(format!("Supplier#{:09}", key));
            s_address.push(random_address(&mut rng));
            s_nationkey.push(nk);
            s_phone.push(random_phone(&mut rng, nk));
            s_acctbal.push((rng.gen_range(-99_999..99_999_i32) as f64) / 100.0);
            s_comment.push(random_comment(&mut rng));
        }

        let name_refs: Vec<&str> = s_name.iter().map(|s| s.as_str()).collect();
        let addr_refs: Vec<&str> = s_address.iter().map(|s| s.as_str()).collect();
        let phone_refs: Vec<&str> = s_phone.iter().map(|s| s.as_str()).collect();
        let comment_refs: Vec<&str> = s_comment.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(s_suppkey)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(StringArray::from(addr_refs)),
                    Arc::new(Int32Array::from(s_nationkey)),
                    Arc::new(StringArray::from(phone_refs)),
                    Arc::new(Float64Array::from(s_acctbal)),
                    Arc::new(StringArray::from(comment_refs)),
                ],
            )
            .expect("supplier batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_customer(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = customer_schema();
    let total = (scale * 150_000.0) as usize;
    let mut rng = StdRng::seed_from_u64(seed_for_table("customer"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut c_custkey = Vec::with_capacity(n);
        let mut c_name = Vec::with_capacity(n);
        let mut c_address = Vec::with_capacity(n);
        let mut c_nationkey = Vec::with_capacity(n);
        let mut c_phone = Vec::with_capacity(n);
        let mut c_acctbal = Vec::with_capacity(n);
        let mut c_mktsegment = Vec::with_capacity(n);
        let mut c_comment = Vec::with_capacity(n);

        for i in 0..n {
            let key = (offset + i + 1) as i32;
            let nk = rng.gen_range(0..25i32);
            c_custkey.push(key);
            c_name.push(format!("Customer#{:09}", key));
            c_address.push(random_address(&mut rng));
            c_nationkey.push(nk);
            c_phone.push(random_phone(&mut rng, nk));
            c_acctbal.push((rng.gen_range(-99_999..99_999_i32) as f64) / 100.0);
            c_mktsegment.push(MARKET_SEGMENTS[rng.gen_range(0..MARKET_SEGMENTS.len())].to_string());
            c_comment.push(random_comment(&mut rng));
        }

        let name_refs: Vec<&str> = c_name.iter().map(|s| s.as_str()).collect();
        let addr_refs: Vec<&str> = c_address.iter().map(|s| s.as_str()).collect();
        let phone_refs: Vec<&str> = c_phone.iter().map(|s| s.as_str()).collect();
        let seg_refs: Vec<&str> = c_mktsegment.iter().map(|s| s.as_str()).collect();
        let comment_refs: Vec<&str> = c_comment.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(c_custkey)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(StringArray::from(addr_refs)),
                    Arc::new(Int32Array::from(c_nationkey)),
                    Arc::new(StringArray::from(phone_refs)),
                    Arc::new(Float64Array::from(c_acctbal)),
                    Arc::new(StringArray::from(seg_refs)),
                    Arc::new(StringArray::from(comment_refs)),
                ],
            )
            .expect("customer batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_part(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = part_schema();
    let total = (scale * 200_000.0) as usize;
    let mut rng = StdRng::seed_from_u64(seed_for_table("part"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut p_partkey = Vec::with_capacity(n);
        let mut p_name = Vec::with_capacity(n);
        let mut p_mfgr = Vec::with_capacity(n);
        let mut p_brand = Vec::with_capacity(n);
        let mut p_type = Vec::with_capacity(n);
        let mut p_size = Vec::with_capacity(n);
        let mut p_container = Vec::with_capacity(n);
        let mut p_retailprice = Vec::with_capacity(n);
        let mut p_comment = Vec::with_capacity(n);

        for i in 0..n {
            let key = (offset + i + 1) as i32;
            let mfgr_num = rng.gen_range(1..=5i32);
            let brand_num = rng.gen_range(1..=5i32);

            // Part name: 5 random colours joined by spaces (TPC-H spec)
            let color1 = PART_COLORS[rng.gen_range(0..PART_COLORS.len())];
            let color2 = PART_COLORS[rng.gen_range(0..PART_COLORS.len())];
            let color3 = PART_COLORS[rng.gen_range(0..PART_COLORS.len())];
            let color4 = PART_COLORS[rng.gen_range(0..PART_COLORS.len())];
            let color5 = PART_COLORS[rng.gen_range(0..PART_COLORS.len())];

            p_partkey.push(key);
            p_name.push(format!("{color1} {color2} {color3} {color4} {color5}"));
            p_mfgr.push(format!("Manufacturer#{mfgr_num}"));
            p_brand.push(format!("Brand#{mfgr_num}{brand_num}"));
            p_type.push(PART_TYPES[rng.gen_range(0..PART_TYPES.len())].to_string());
            p_size.push(rng.gen_range(1..=50i32));
            p_container.push(PART_CONTAINERS[rng.gen_range(0..PART_CONTAINERS.len())].to_string());
            // Retail price: 90001 + (key/10) mod 20001 + 0.nn
            let base = 90001 + (key / 10) % 20001;
            p_retailprice.push(base as f64 + (rng.gen_range(0..100) as f64) / 100.0);
            p_comment.push(random_comment(&mut rng));
        }

        let name_refs: Vec<&str> = p_name.iter().map(|s| s.as_str()).collect();
        let mfgr_refs: Vec<&str> = p_mfgr.iter().map(|s| s.as_str()).collect();
        let brand_refs: Vec<&str> = p_brand.iter().map(|s| s.as_str()).collect();
        let type_refs: Vec<&str> = p_type.iter().map(|s| s.as_str()).collect();
        let container_refs: Vec<&str> = p_container.iter().map(|s| s.as_str()).collect();
        let comment_refs: Vec<&str> = p_comment.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(p_partkey)),
                    Arc::new(StringArray::from(name_refs)),
                    Arc::new(StringArray::from(mfgr_refs)),
                    Arc::new(StringArray::from(brand_refs)),
                    Arc::new(StringArray::from(type_refs)),
                    Arc::new(Int32Array::from(p_size)),
                    Arc::new(StringArray::from(container_refs)),
                    Arc::new(Float64Array::from(p_retailprice)),
                    Arc::new(StringArray::from(comment_refs)),
                ],
            )
            .expect("part batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_partsupp(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = partsupp_schema();
    let num_parts = (scale * 200_000.0) as i32;
    let num_suppliers = (scale * 10_000.0) as i32;
    // 4 suppliers per part → SF * 800,000 rows
    let total = (scale * 800_000.0) as usize;
    let mut rng = StdRng::seed_from_u64(seed_for_table("partsupp"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    // Iterate: for each partkey, 4 suppkeys
    let mut part_idx = 1i32;
    let mut supp_offset_idx = 0usize;

    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut ps_partkey = Vec::with_capacity(n);
        let mut ps_suppkey = Vec::with_capacity(n);
        let mut ps_availqty = Vec::with_capacity(n);
        let mut ps_supplycost = Vec::with_capacity(n);
        let mut ps_comment = Vec::with_capacity(n);

        for _ in 0..n {
            let suppkey = 1 + ((part_idx - 1 + supp_offset_idx as i32) % num_suppliers.max(1));
            ps_partkey.push(part_idx);
            ps_suppkey.push(suppkey);
            ps_availqty.push(rng.gen_range(1..=9999i32));
            ps_supplycost.push((rng.gen_range(100..100000i32) as f64) / 100.0);
            ps_comment.push(random_comment(&mut rng));

            supp_offset_idx += 1;
            if supp_offset_idx >= 4 {
                supp_offset_idx = 0;
                part_idx += 1;
                if part_idx > num_parts {
                    part_idx = 1;
                }
            }
        }

        let comment_refs: Vec<&str> = ps_comment.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ps_partkey)),
                    Arc::new(Int32Array::from(ps_suppkey)),
                    Arc::new(Int32Array::from(ps_availqty)),
                    Arc::new(Float64Array::from(ps_supplycost)),
                    Arc::new(StringArray::from(comment_refs)),
                ],
            )
            .expect("partsupp batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_orders(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = orders_schema();
    let total = (scale * 1_500_000.0) as usize;
    let num_customers = (scale * 150_000.0) as i32;
    let mut rng = StdRng::seed_from_u64(seed_for_table("orders"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut o_orderkey = Vec::with_capacity(n);
        let mut o_custkey = Vec::with_capacity(n);
        let mut o_orderstatus = Vec::with_capacity(n);
        let mut o_totalprice = Vec::with_capacity(n);
        let mut o_orderdate = Vec::with_capacity(n);
        let mut o_orderpriority = Vec::with_capacity(n);
        let mut o_clerk = Vec::with_capacity(n);
        let mut o_shippriority = Vec::with_capacity(n);
        let mut o_comment = Vec::with_capacity(n);

        for i in 0..n {
            let key = (offset + i + 1) as i64;
            // status: O or F (P is not used in simple generators)
            let status = LINE_STATUSES[rng.gen_range(0..LINE_STATUSES.len())];
            let clerk_num = rng.gen_range(1..=1000i32);

            o_orderkey.push(key);
            o_custkey.push(rng.gen_range(1..=num_customers.max(1)));
            o_orderstatus.push(status.to_string());
            o_totalprice.push((rng.gen_range(10_000..50_000_000_i64) as f64) / 100.0);
            o_orderdate.push(random_date(&mut rng));
            o_orderpriority.push(ORDER_PRIORITIES[rng.gen_range(0..ORDER_PRIORITIES.len())].to_string());
            o_clerk.push(format!("Clerk#{clerk_num:09}"));
            o_shippriority.push(0i32);
            o_comment.push(random_comment(&mut rng));
        }

        let status_refs: Vec<&str> = o_orderstatus.iter().map(|s| s.as_str()).collect();
        let priority_refs: Vec<&str> = o_orderpriority.iter().map(|s| s.as_str()).collect();
        let clerk_refs: Vec<&str> = o_clerk.iter().map(|s| s.as_str()).collect();
        let comment_refs: Vec<&str> = o_comment.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(o_orderkey)),
                    Arc::new(Int32Array::from(o_custkey)),
                    Arc::new(StringArray::from(status_refs)),
                    Arc::new(Float64Array::from(o_totalprice)),
                    Arc::new(Date32Array::from(o_orderdate)),
                    Arc::new(StringArray::from(priority_refs)),
                    Arc::new(StringArray::from(clerk_refs)),
                    Arc::new(Int32Array::from(o_shippriority)),
                    Arc::new(StringArray::from(comment_refs)),
                ],
            )
            .expect("orders batch"),
        );
        offset += n;
    }

    (schema, batches)
}

fn generate_lineitem(scale: f64) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = lineitem_schema();
    let total = (scale * 6_000_000.0) as usize;
    let num_orders = (scale * 1_500_000.0) as i64;
    let num_parts = (scale * 200_000.0) as i32;
    let num_suppliers = (scale * 10_000.0) as i32;
    let mut rng = StdRng::seed_from_u64(seed_for_table("lineitem"));
    let mut batches = Vec::new();

    let mut offset = 0usize;
    while offset < total {
        let n = BATCH_SIZE.min(total - offset);
        let mut l_orderkey = Vec::with_capacity(n);
        let mut l_partkey = Vec::with_capacity(n);
        let mut l_suppkey = Vec::with_capacity(n);
        let mut l_linenumber = Vec::with_capacity(n);
        let mut l_quantity = Vec::with_capacity(n);
        let mut l_extendedprice = Vec::with_capacity(n);
        let mut l_discount = Vec::with_capacity(n);
        let mut l_tax = Vec::with_capacity(n);
        let mut l_returnflag = Vec::with_capacity(n);
        let mut l_linestatus = Vec::with_capacity(n);
        let mut l_shipdate = Vec::with_capacity(n);
        let mut l_commitdate = Vec::with_capacity(n);
        let mut l_receiptdate = Vec::with_capacity(n);
        let mut l_shipinstruct = Vec::with_capacity(n);
        let mut l_shipmode = Vec::with_capacity(n);
        let mut l_comment = Vec::with_capacity(n);

        for i in 0..n {
            let orderkey = rng.gen_range(1..=num_orders.max(1));
            let partkey = rng.gen_range(1..=num_parts.max(1));
            let suppkey = 1 + (partkey + rng.gen_range(0..4i32)) % num_suppliers.max(1);
            let linenumber = ((offset + i) % 7 + 1) as i32;
            let quantity = rng.gen_range(1..=50i32) as f64;
            let retailprice = 90001.0 + (partkey as f64 / 10.0) % 20001.0;
            let discount = rng.gen_range(0..=10i32) as f64 / 100.0;
            let tax = rng.gen_range(0..=8i32) as f64 / 100.0;
            let extendedprice = quantity * retailprice * (1.0 - discount);
            let shipdate = random_date(&mut rng);
            let commitdate = random_date(&mut rng);
            let receiptdate = random_date(&mut rng);

            l_orderkey.push(orderkey);
            l_partkey.push(partkey);
            l_suppkey.push(suppkey);
            l_linenumber.push(linenumber);
            l_quantity.push(quantity);
            l_extendedprice.push(extendedprice);
            l_discount.push(discount);
            l_tax.push(tax);
            l_returnflag.push(RETURN_FLAGS[rng.gen_range(0..RETURN_FLAGS.len())].to_string());
            l_linestatus.push(LINE_STATUSES[rng.gen_range(0..LINE_STATUSES.len())].to_string());
            l_shipdate.push(shipdate);
            l_commitdate.push(commitdate);
            l_receiptdate.push(receiptdate);
            l_shipinstruct.push(SHIP_INSTRUCTS[rng.gen_range(0..SHIP_INSTRUCTS.len())].to_string());
            l_shipmode.push(SHIP_MODES[rng.gen_range(0..SHIP_MODES.len())].to_string());
            l_comment.push(random_comment(&mut rng));
        }

        let rflag_refs: Vec<&str> = l_returnflag.iter().map(|s| s.as_str()).collect();
        let lstatus_refs: Vec<&str> = l_linestatus.iter().map(|s| s.as_str()).collect();
        let instruct_refs: Vec<&str> = l_shipinstruct.iter().map(|s| s.as_str()).collect();
        let mode_refs: Vec<&str> = l_shipmode.iter().map(|s| s.as_str()).collect();
        let comment_refs: Vec<&str> = l_comment.iter().map(|s| s.as_str()).collect();

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(l_orderkey)),
                    Arc::new(Int32Array::from(l_partkey)),
                    Arc::new(Int32Array::from(l_suppkey)),
                    Arc::new(Int32Array::from(l_linenumber)),
                    Arc::new(Float64Array::from(l_quantity)),
                    Arc::new(Float64Array::from(l_extendedprice)),
                    Arc::new(Float64Array::from(l_discount)),
                    Arc::new(Float64Array::from(l_tax)),
                    Arc::new(StringArray::from(rflag_refs)),
                    Arc::new(StringArray::from(lstatus_refs)),
                    Arc::new(Date32Array::from(l_shipdate)),
                    Arc::new(Date32Array::from(l_commitdate)),
                    Arc::new(Date32Array::from(l_receiptdate)),
                    Arc::new(StringArray::from(instruct_refs)),
                    Arc::new(StringArray::from(mode_refs)),
                    Arc::new(StringArray::from(comment_refs)),
                ],
            )
            .expect("lineitem batch"),
        );
        offset += n;
    }

    (schema, batches)
}

// ---------------------------------------------------------------------------
// BenchmarkGenerator impl
// ---------------------------------------------------------------------------

impl BenchmarkGenerator for TpchGenerator {
    fn name(&self) -> &str {
        "tpch"
    }

    fn tables(&self) -> Vec<TableDef> {
        vec![
            TableDef {
                name: "region".into(),
                schema: region_schema(),
                row_count: |_| 5,
            },
            TableDef {
                name: "nation".into(),
                schema: nation_schema(),
                row_count: |_| 25,
            },
            TableDef {
                name: "supplier".into(),
                schema: supplier_schema(),
                row_count: |sf| (sf * 10_000.0) as usize,
            },
            TableDef {
                name: "customer".into(),
                schema: customer_schema(),
                row_count: |sf| (sf * 150_000.0) as usize,
            },
            TableDef {
                name: "part".into(),
                schema: part_schema(),
                row_count: |sf| (sf * 200_000.0) as usize,
            },
            TableDef {
                name: "partsupp".into(),
                schema: partsupp_schema(),
                row_count: |sf| (sf * 800_000.0) as usize,
            },
            TableDef {
                name: "orders".into(),
                schema: orders_schema(),
                row_count: |sf| (sf * 1_500_000.0) as usize,
            },
            TableDef {
                name: "lineitem".into(),
                schema: lineitem_schema(),
                row_count: |sf| (sf * 6_000_000.0) as usize,
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
            "region" => generate_region(),
            "nation" => generate_nation(),
            "supplier" => generate_supplier(scale),
            "customer" => generate_customer(scale),
            "part" => generate_part(scale),
            "partsupp" => generate_partsupp(scale),
            "orders" => generate_orders(scale),
            "lineitem" => generate_lineitem(scale),
            _ => anyhow::bail!("Unknown TPC-H table: {table}"),
        };

        let full_output = format!("{output_dir}/tpch/sf{scale}");
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
        let gen = TpchGenerator;
        let tables = gen.tables();
        assert_eq!(tables.len(), 8);
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"region"));
        assert!(names.contains(&"nation"));
        assert!(names.contains(&"supplier"));
        assert!(names.contains(&"customer"));
        assert!(names.contains(&"part"));
        assert!(names.contains(&"partsupp"));
        assert!(names.contains(&"orders"));
        assert!(names.contains(&"lineitem"));
    }

    #[test]
    fn test_row_counts_sf01() {
        let gen = TpchGenerator;
        let sf = 0.01_f64;
        for t in gen.tables() {
            let expected = (t.row_count)(sf);
            match t.name.as_str() {
                "region" => assert_eq!(expected, 5),
                "nation" => assert_eq!(expected, 25),
                "supplier" => assert_eq!(expected, 100),
                "customer" => assert_eq!(expected, 1_500),
                "part" => assert_eq!(expected, 2_000),
                "partsupp" => assert_eq!(expected, 8_000),
                "orders" => assert_eq!(expected, 15_000),
                "lineitem" => assert_eq!(expected, 60_000),
                _ => {}
            }
        }
    }

    #[test]
    fn test_generate_region() {
        let (schema, batches) = generate_region();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_generate_nation() {
        let (schema, batches) = generate_nation();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 25);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_generate_supplier_sf001() {
        let (schema, batches) = generate_supplier(0.01);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_generate_customer_sf001() {
        let (schema, batches) = generate_customer(0.01);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1_500);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_generate_part_sf001() {
        let (schema, batches) = generate_part(0.01);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2_000);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_generate_partsupp_sf001() {
        let (schema, batches) = generate_partsupp(0.01);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 8_000);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_generate_orders_sf001() {
        let (schema, batches) = generate_orders(0.01);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 15_000);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_generate_lineitem_sf001() {
        let (schema, batches) = generate_lineitem(0.01);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 60_000);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_date_epoch() {
        // 1992-01-01 should be 8035 days after 1970-01-01
        let days = days_since_epoch(1992, 1, 1);
        assert_eq!(days, DATE_START);
    }

    #[test]
    fn test_generate_table_to_parquet() {
        let gen = TpchGenerator;
        // Use a stable path under /tmp; parquet_writer creates subdirs
        let output = "/tmp/sqe-bench-test-tpch-parquet";

        let stats = gen.generate_table("region", 1.0, output).unwrap();
        assert_eq!(stats.rows, 5);
        assert_eq!(stats.files, 1);

        let stats = gen.generate_table("nation", 1.0, output).unwrap();
        assert_eq!(stats.rows, 25);

        let stats = gen.generate_table("supplier", 0.01, output).unwrap();
        assert_eq!(stats.rows, 100);
    }

    #[test]
    fn test_unknown_table_errors() {
        let gen = TpchGenerator;
        let result = gen.generate_table("nonexistent", 1.0, "/tmp");
        assert!(result.is_err());
    }
}
