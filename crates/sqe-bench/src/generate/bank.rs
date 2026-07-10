//! Bank benchmark generator.
//!
//! A purpose-built financial dataset for large-scale, time-windowed demos:
//! three dimensions (`customer`, `account`, `kyc_profile`) and two facts
//! partitioned by calendar day (`transaction`, `account_balance`). The demo
//! query shape is "scan the last 14 days out of N", so facts carry a `date`
//! partition column and generation is organized per `(table, day, shard)`
//! unit.
//!
//! Every unit is independently deterministic: its seed derives from
//! `(table, day_index, shard_index)` alone, so any unit can be regenerated
//! on any machine without coordination. Transaction shards own a disjoint
//! account-id range and emit rows time-ordered within the day, which gives
//! written files tight min/max stats on both `t_ts` and `t_a_id` without a
//! sort step.
//!
//! All generators stream fixed-size batches through `iter::from_fn`; no
//! path accumulates a table (or a day) in memory.
//!
//! Arrow schemas carry explicit Iceberg field ids in field metadata
//! (`PARQUET:field_id`) so the direct-to-Iceberg sink can convert them to
//! Iceberg schemas losslessly and written Parquet is Iceberg-conformant.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    BooleanArray, Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{parallel_generate_table, BenchmarkGenerator, GenerateConfig, GenerateStats, TableDef};

pub struct BankGenerator;

/// Rows per generated batch. Large enough to amortize per-batch overhead,
/// small enough that one batch (plus writer buffers) bounds worker memory.
pub const BATCH_SIZE: usize = 65_536;

/// Microseconds in one day.
const DAY_MICROS: i64 = 86_400_000_000;

/// Default first trading day: 2026-06-01 as days since Unix epoch.
pub const DEFAULT_START_DAY: i32 = 20_605;

/// Fixed day count when the bank benchmark runs through the scale-factor
/// (local Parquet) path. The Iceberg sink passes an explicit day count.
const SCALE_MODE_DAYS: u32 = 3;

/// Fraud ring for bank q03 (high-velocity account screen). Uniform account
/// draws give every account only a few dozen debits in a 3-day window, so
/// `HAVING COUNT(*) > 100` never fires. These fixed low-id accounts absorb
/// a global stride of debits on the first three trading days so the screen
/// has accounts above the absolute threshold at benchmark scales.
const FRAUD_RING_START: i64 = 1;
/// Number of accounts in the ring; q03 returns one row per ring account.
const FRAUD_RING_COUNT: i64 = 10;
/// Every `FRAUD_RING_STRIDE`-th transaction id on a ring day is redirected
/// (as a debit) to a ring account. Keyed on the global `t_id` so the redirect
/// is shard-independent and total row counts stay unchanged; sized so the
/// per-account debit count lands in the hundreds at sf0.1 and grows with
/// scale, always clearing the 100-row threshold.
const FRAUD_RING_STRIDE: i64 = 200;
/// Trading days the ring is active on: the first three days from the start,
/// matching the q03 window `DATE '2026-06-01' AND DATE '2026-06-03'`.
const FRAUD_RING_DAYS: std::ops::Range<i32> = DEFAULT_START_DAY..DEFAULT_START_DAY + 3;

// ---------------------------------------------------------------------------
// Plan: the sizing knobs shared by every generation unit
// ---------------------------------------------------------------------------

/// Sizing for one bank dataset. All row counts derive from these fields so
/// foreign keys stay consistent across independently generated units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BankPlan {
    /// Number of customers (dimension size driver).
    pub customers: u64,
    /// First trading day, as days since Unix epoch.
    pub start_day: i32,
    /// Number of trading days.
    pub days: u32,
    /// Transaction rows per trading day.
    pub txn_rows_per_day: u64,
}

impl BankPlan {
    /// Accounts per dataset: 2.5 per customer.
    pub fn accounts(&self) -> u64 {
        (self.customers * 5) / 2
    }

    /// Map a scale factor to a small local plan. SF1 lands around a few GB
    /// so local smoke runs and the generator sweep stay fast.
    pub fn from_scale(scale: f64) -> Self {
        Self {
            customers: super::scaled(scale, 100_000.0) as u64,
            start_day: DEFAULT_START_DAY,
            days: SCALE_MODE_DAYS,
            txn_rows_per_day: super::scaled(scale, 2_000_000.0) as u64,
        }
    }
}

/// Deterministic seed for one `(table, day, shard)` generation unit.
/// FNV-1a over the qualified unit name; stable across platforms and runs.
pub fn unit_seed(table: &str, day_idx: u32, shard_idx: u32) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in format!("bank|{table}|{day_idx}|{shard_idx}").bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

// ---------------------------------------------------------------------------
// Schemas (field metadata carries Iceberg field ids)
// ---------------------------------------------------------------------------

fn fid(id: i32, name: &str, dt: DataType, nullable: bool) -> Field {
    Field::new(name, dt, nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        id.to_string(),
    )]))
}

fn decimal_15_2(cents: Vec<i64>) -> Decimal128Array {
    Decimal128Array::from_iter_values(cents.into_iter().map(|c| c as i128))
        .with_precision_and_scale(15, 2)
        .expect("Decimal128(15, 2) is valid for bank money columns")
}

pub fn customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fid(1, "c_id", DataType::Int64, false),
        fid(2, "c_name", DataType::Utf8, false),
        fid(3, "c_dob", DataType::Date32, false),
        fid(4, "c_country", DataType::Utf8, false),
        fid(5, "c_segment", DataType::Utf8, false),
        fid(6, "c_created", DataType::Date32, false),
    ]))
}

pub fn account_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fid(1, "a_id", DataType::Int64, false),
        fid(2, "a_c_id", DataType::Int64, false),
        fid(3, "a_iban", DataType::Utf8, false),
        fid(4, "a_type", DataType::Utf8, false),
        fid(5, "a_currency", DataType::Utf8, false),
        fid(6, "a_status", DataType::Utf8, false),
        fid(7, "a_opened", DataType::Date32, false),
    ]))
}

pub fn kyc_profile_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fid(1, "k_c_id", DataType::Int64, false),
        fid(2, "k_risk_rating", DataType::Utf8, false),
        fid(3, "k_pep", DataType::Boolean, false),
        fid(4, "k_sanctions_hit", DataType::Boolean, false),
        fid(5, "k_last_review", DataType::Date32, false),
        fid(6, "k_next_review", DataType::Date32, false),
        fid(7, "k_source_of_funds", DataType::Utf8, false),
    ]))
}

pub fn transaction_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fid(1, "t_id", DataType::Int64, false),
        fid(2, "t_day", DataType::Date32, false),
        fid(3, "t_ts", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        fid(4, "t_a_id", DataType::Int64, false),
        fid(5, "t_counterparty_iban", DataType::Utf8, false),
        fid(6, "t_counterparty_bic", DataType::Utf8, false),
        fid(7, "t_amount", DataType::Decimal128(15, 2), false),
        fid(8, "t_currency", DataType::Utf8, false),
        fid(9, "t_direction", DataType::Utf8, false),
        fid(10, "t_channel", DataType::Utf8, false),
        fid(11, "t_category", DataType::Utf8, false),
        fid(12, "t_status", DataType::Utf8, false),
        fid(13, "t_description", DataType::Utf8, false),
        fid(14, "t_balance_after", DataType::Decimal128(15, 2), false),
        fid(15, "t_country", DataType::Utf8, false),
    ]))
}

pub fn account_balance_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fid(1, "b_day", DataType::Date32, false),
        fid(2, "b_a_id", DataType::Int64, false),
        fid(3, "b_balance", DataType::Decimal128(15, 2), false),
        fid(4, "b_currency", DataType::Utf8, false),
        fid(5, "b_txn_count", DataType::Int32, false),
    ]))
}

/// Partition source column for a bank table, if it is day-partitioned.
pub fn partition_column(table: &str) -> Option<&'static str> {
    match table {
        "transaction" => Some("t_day"),
        "account_balance" => Some("b_day"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Value pools
// ---------------------------------------------------------------------------

const FIRST_NAMES: &[&str] = &[
    "Emma", "Daan", "Sofia", "Lucas", "Julia", "Milan", "Anna", "Levi", "Sara", "Finn", "Nora",
    "Sem", "Eva", "Noah", "Lotte", "Adam", "Mila", "Thomas", "Zoe", "David",
];

const SURNAMES: &[&str] = &[
    "Jansen", "Visser", "Smit", "Meyer", "Mulder", "Bakker", "Petersen", "Novak", "Weber",
    "Fischer", "Rossi", "Costa", "Dubois", "Laurent", "Andersen", "Kowalski", "Silva", "Moreau",
    "Schmidt", "Peeters",
];

const COUNTRIES: &[&str] = &[
    "NL", "NL", "NL", "NL", "DE", "DE", "BE", "BE", "FR", "GB", "ES", "IT", "PL", "CH", "US",
];

const SEGMENTS: &[&str] = &["retail", "retail", "retail", "retail", "retail", "retail", "sme", "sme", "private", "corporate"];

const ACCOUNT_TYPES: &[&str] = &[
    "current", "current", "current", "current", "current", "current", "savings", "savings",
    "savings", "brokerage",
];

const CURRENCIES: &[&str] = &["EUR", "EUR", "EUR", "EUR", "USD", "GBP", "CHF"];

const STATUSES: &[&str] = &[
    "open", "open", "open", "open", "open", "open", "open", "open", "open", "open", "open",
    "open", "dormant", "closed",
];

const RISK_RATINGS: &[&str] = &[
    "low", "low", "low", "low", "low", "low", "low", "low", "medium", "medium", "medium", "high",
];

const SOURCES_OF_FUNDS: &[&str] = &[
    "salary", "salary", "salary", "salary", "business income", "savings", "pension",
    "investments", "property sale", "inheritance",
];

const CHANNELS: &[&str] = &[
    "sepa", "sepa", "sepa", "sepa", "sepa", "sepa", "sepa", "sepa", "card", "card", "card",
    "card", "card", "card", "instant", "instant", "instant", "internal", "internal", "swift",
];

const CATEGORIES: &[&str] = &[
    "groceries", "utilities", "rent", "mortgage", "salary", "restaurants", "transport",
    "insurance", "healthcare", "entertainment", "travel", "online retail", "fuel", "telecom",
    "subscriptions", "cash withdrawal", "transfer", "investment", "fees", "taxes",
];

const TXN_STATUSES_100: &[&str] = &["pending", "pending", "rejected"];

const BICS: &[&str] = &[
    "SQEBNL2A", "INGBNL2A", "RABONL2U", "ABNANL2A", "DEUTDEFF", "BNPAFRPP", "GEBABEBB",
    "BARCGB22", "UBSWCHZH", "CAIXESBB", "UNCRITMM", "PKOPPLPW",
];

const DESC_VERBS: &[&str] = &[
    "payment", "transfer", "purchase", "settlement", "refund", "invoice", "order", "donation",
    "installment", "top-up",
];

const DESC_MERCHANTS: &[&str] = &[
    "Albert Heijn", "Shell Station", "Deutsche Bahn", "Amazon Marketplace", "IKEA", "Bol.com",
    "Carrefour", "Zalando", "Spotify", "NS International", "Vattenfall", "KPN", "Lidl",
    "Booking.com", "Airbnb Payments", "Uber", "Apple Services", "Steam Games", "H&M", "Decathlon",
];

// ---------------------------------------------------------------------------
// Dimension generators
// ---------------------------------------------------------------------------

/// A high-entropy uppercase-alphanumeric reference of length `N`, like the
/// EndToEndId on a real payment.
fn rand_ref<const N: usize>(rng: &mut StdRng) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    (0..N)
        .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
        .collect()
}

/// Currency of account `a_id`, consistent across every table that mentions
/// the account without any lookup table.
fn account_currency(a_id: i64) -> &'static str {
    CURRENCIES[(a_id as u64 % CURRENCIES.len() as u64) as usize]
}

/// Owning customer of account `a_id`: a monotone spread of accounts over
/// customers so the FK is valid for any plan without materializing a map.
fn account_customer(a_id: i64, plan: &BankPlan) -> i64 {
    ((a_id as u64) * plan.customers / plan.accounts()) as i64
}

pub fn customer_range(
    range: std::ops::Range<usize>,
    seed: u64,
) -> impl Iterator<Item = RecordBatch> + Send {
    let schema = customer_schema();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut offset = range.start;
    let end = range.end;

    std::iter::from_fn(move || {
        if offset >= end {
            return None;
        }
        let n = BATCH_SIZE.min(end - offset);
        let mut c_id = Vec::with_capacity(n);
        let mut c_name = Vec::with_capacity(n);
        let mut c_dob = Vec::with_capacity(n);
        let mut c_country = Vec::with_capacity(n);
        let mut c_segment = Vec::with_capacity(n);
        let mut c_created = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i) as i64;
            c_id.push(id);
            c_name.push(format!(
                "{} {}",
                FIRST_NAMES[rng.gen_range(0..FIRST_NAMES.len())],
                SURNAMES[rng.gen_range(0..SURNAMES.len())]
            ));
            // Date of birth between 1940-01-01 and 2004-12-31.
            c_dob.push(rng.gen_range(-10_957..12_784i32));
            c_country.push(COUNTRIES[rng.gen_range(0..COUNTRIES.len())]);
            c_segment.push(SEGMENTS[rng.gen_range(0..SEGMENTS.len())]);
            // Customer since between 2005-01-01 and 2026-05-31.
            c_created.push(rng.gen_range(12_784..20_604i32));
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(c_id)),
                Arc::new(StringArray::from(
                    c_name.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
                Arc::new(Date32Array::from(c_dob)),
                Arc::new(StringArray::from(c_country)),
                Arc::new(StringArray::from(c_segment)),
                Arc::new(Date32Array::from(c_created)),
            ],
        )
        .expect("customer batch");
        offset += n;
        Some(batch)
    })
}

pub fn account_range(
    range: std::ops::Range<usize>,
    plan: BankPlan,
    seed: u64,
) -> impl Iterator<Item = RecordBatch> + Send {
    let schema = account_schema();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut offset = range.start;
    let end = range.end;

    std::iter::from_fn(move || {
        if offset >= end {
            return None;
        }
        let n = BATCH_SIZE.min(end - offset);
        let mut a_id = Vec::with_capacity(n);
        let mut a_c_id = Vec::with_capacity(n);
        let mut a_iban = Vec::with_capacity(n);
        let mut a_type = Vec::with_capacity(n);
        let mut a_currency = Vec::with_capacity(n);
        let mut a_status = Vec::with_capacity(n);
        let mut a_opened = Vec::with_capacity(n);

        for i in 0..n {
            let id = (offset + i) as i64;
            a_id.push(id);
            a_c_id.push(account_customer(id, &plan));
            a_iban.push(format!("NL{:02}SQEB{:010}", id % 89 + 10, id));
            a_type.push(ACCOUNT_TYPES[rng.gen_range(0..ACCOUNT_TYPES.len())]);
            a_currency.push(account_currency(id));
            a_status.push(STATUSES[rng.gen_range(0..STATUSES.len())]);
            // Opened between 2010-01-01 and 2026-05-31.
            a_opened.push(rng.gen_range(14_610..20_604i32));
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(a_id)),
                Arc::new(Int64Array::from(a_c_id)),
                Arc::new(StringArray::from(
                    a_iban.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(a_type)),
                Arc::new(StringArray::from(a_currency)),
                Arc::new(StringArray::from(a_status)),
                Arc::new(Date32Array::from(a_opened)),
            ],
        )
        .expect("account batch");
        offset += n;
        Some(batch)
    })
}

pub fn kyc_profile_range(
    range: std::ops::Range<usize>,
    seed: u64,
) -> impl Iterator<Item = RecordBatch> + Send {
    let schema = kyc_profile_schema();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut offset = range.start;
    let end = range.end;

    std::iter::from_fn(move || {
        if offset >= end {
            return None;
        }
        let n = BATCH_SIZE.min(end - offset);
        let mut k_c_id = Vec::with_capacity(n);
        let mut k_risk = Vec::with_capacity(n);
        let mut k_pep = Vec::with_capacity(n);
        let mut k_sanctions = Vec::with_capacity(n);
        let mut k_last = Vec::with_capacity(n);
        let mut k_next = Vec::with_capacity(n);
        let mut k_source = Vec::with_capacity(n);

        for i in 0..n {
            k_c_id.push((offset + i) as i64);
            k_risk.push(RISK_RATINGS[rng.gen_range(0..RISK_RATINGS.len())]);
            // 0.5% politically exposed, 0.1% sanctions hits.
            k_pep.push(rng.gen_range(0..1000u32) < 5);
            k_sanctions.push(rng.gen_range(0..1000u32) < 1);
            // Last review between 2024-01-01 and 2026-05-31; next one year on.
            let last = rng.gen_range(19_723..20_604i32);
            k_last.push(last);
            k_next.push(last + 365);
            k_source.push(SOURCES_OF_FUNDS[rng.gen_range(0..SOURCES_OF_FUNDS.len())]);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(k_c_id)),
                Arc::new(StringArray::from(k_risk)),
                Arc::new(BooleanArray::from(k_pep)),
                Arc::new(BooleanArray::from(k_sanctions)),
                Arc::new(Date32Array::from(k_last)),
                Arc::new(Date32Array::from(k_next)),
                Arc::new(StringArray::from(k_source)),
            ],
        )
        .expect("kyc_profile batch");
        offset += n;
        Some(batch)
    })
}

// ---------------------------------------------------------------------------
// Fact generators (per day, per shard)
// ---------------------------------------------------------------------------

/// One transaction generation unit: `rows` rows of day `day` (days since
/// epoch), drawing account ids from the disjoint `accounts` range, with
/// timestamps ascending across the unit. `t_id_start` is the unit's global
/// id offset so ids stay unique across shards and days.
pub fn transaction_day_shard(
    day: i32,
    rows: u64,
    t_id_start: i64,
    accounts: std::ops::Range<u64>,
    seed: u64,
) -> impl Iterator<Item = RecordBatch> + Send {
    let schema = transaction_schema();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut offset: u64 = 0;
    let day_start_micros = day as i64 * DAY_MICROS;

    std::iter::from_fn(move || {
        if offset >= rows {
            return None;
        }
        let n = (BATCH_SIZE as u64).min(rows - offset) as usize;
        let mut t_id = Vec::with_capacity(n);
        let mut t_day = Vec::with_capacity(n);
        let mut t_ts = Vec::with_capacity(n);
        let mut t_a_id = Vec::with_capacity(n);
        let mut t_cp_iban = Vec::with_capacity(n);
        let mut t_cp_bic = Vec::with_capacity(n);
        let mut t_amount = Vec::with_capacity(n);
        let mut t_currency = Vec::with_capacity(n);
        let mut t_direction = Vec::with_capacity(n);
        let mut t_channel = Vec::with_capacity(n);
        let mut t_category = Vec::with_capacity(n);
        let mut t_status = Vec::with_capacity(n);
        let mut t_description = Vec::with_capacity(n);
        let mut t_balance_after = Vec::with_capacity(n);
        let mut t_country = Vec::with_capacity(n);

        for i in 0..n {
            let row = offset + i as u64;
            let id = t_id_start + row as i64;
            t_id.push(id);
            t_day.push(day);
            // Ascending across the unit: each row advances by the day span
            // divided by the unit's row count. Files cut from this stream
            // cover contiguous time windows, so ts min/max stats are tight.
            t_ts.push(day_start_micros + (row as i128 * DAY_MICROS as i128 / rows.max(1) as i128) as i64);
            // Draw unconditionally so the rng sequence (and every non-ring
            // row) is identical whether or not this row joins the ring.
            let drawn_a_id =
                rng.gen_range(accounts.start..accounts.end.max(accounts.start + 1)) as i64;
            let ring_account =
                FRAUD_RING_START + (id / FRAUD_RING_STRIDE).rem_euclid(FRAUD_RING_COUNT);
            let use_ring = FRAUD_RING_DAYS.contains(&day)
                && id % FRAUD_RING_STRIDE == 0
                && accounts.contains(&(ring_account as u64));
            let a_id = if use_ring { ring_account } else { drawn_a_id };
            t_a_id.push(a_id);
            t_cp_iban.push(format!(
                "{}{:02}BANK{:010}",
                COUNTRIES[rng.gen_range(0..COUNTRIES.len())],
                rng.gen_range(10..99),
                rng.gen_range(0..9_999_999_999u64)
            ));
            t_cp_bic.push(BICS[rng.gen_range(0..BICS.len())]);
            // Skewed magnitudes: cents in [10^2, 10^7) with small amounts
            // dominating, resembling retail payment flows.
            let mag = rng.gen_range(0..5u32);
            let base = 10i64.pow(mag + 2);
            t_amount.push(rng.gen_range(base / 2..base * 5));
            t_currency.push(account_currency(a_id));
            // Draw unconditionally to keep the rng aligned; ring rows are
            // forced to debit so they land in q03's outgoing-transaction filter.
            let debit = rng.gen_range(0..2) == 0 || use_ring;
            t_direction.push(if debit { "debit" } else { "credit" });
            t_channel.push(CHANNELS[rng.gen_range(0..CHANNELS.len())]);
            t_category.push(CATEGORIES[rng.gen_range(0..CATEGORIES.len())]);
            // 97% settled, 2% pending, 1% rejected.
            let s = rng.gen_range(0..100u32);
            t_status.push(if s < 97 { "settled" } else { TXN_STATUSES_100[(s - 97) as usize] });
            // The end-to-end reference mirrors SEPA EndToEndId: unique
            // high-entropy identifiers that give the row a realistic
            // compressed footprint (all other columns dictionary-compress
            // to near nothing, which would make a "4 TB day" mean an
            // implausible number of rows).
            t_description.push(format!(
                "{} {} e2e {}",
                DESC_MERCHANTS[rng.gen_range(0..DESC_MERCHANTS.len())],
                DESC_VERBS[rng.gen_range(0..DESC_VERBS.len())],
                rand_ref::<24>(&mut rng)
            ));
            t_balance_after.push(rng.gen_range(-10_000_000..1_000_000_000i64));
            t_country.push(COUNTRIES[rng.gen_range(0..COUNTRIES.len())]);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(t_id)),
                Arc::new(Date32Array::from(t_day)),
                Arc::new(TimestampMicrosecondArray::from(t_ts)),
                Arc::new(Int64Array::from(t_a_id)),
                Arc::new(StringArray::from(
                    t_cp_iban.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(t_cp_bic)),
                Arc::new(decimal_15_2(t_amount)),
                Arc::new(StringArray::from(t_currency)),
                Arc::new(StringArray::from(t_direction)),
                Arc::new(StringArray::from(t_channel)),
                Arc::new(StringArray::from(t_category)),
                Arc::new(StringArray::from(t_status)),
                Arc::new(StringArray::from(
                    t_description.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
                Arc::new(decimal_15_2(t_balance_after)),
                Arc::new(StringArray::from(t_country)),
            ],
        )
        .expect("transaction batch");
        offset += n as u64;
        Some(batch)
    })
}

/// One account_balance unit: the end-of-day snapshot rows for accounts in
/// the disjoint `accounts` range on day `day`.
pub fn account_balance_day_shard(
    day: i32,
    accounts: std::ops::Range<u64>,
    seed: u64,
) -> impl Iterator<Item = RecordBatch> + Send {
    let schema = account_balance_schema();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut next = accounts.start;
    let end = accounts.end;

    std::iter::from_fn(move || {
        if next >= end {
            return None;
        }
        let n = (BATCH_SIZE as u64).min(end - next) as usize;
        let mut b_day = Vec::with_capacity(n);
        let mut b_a_id = Vec::with_capacity(n);
        let mut b_balance = Vec::with_capacity(n);
        let mut b_currency = Vec::with_capacity(n);
        let mut b_txn_count = Vec::with_capacity(n);

        for i in 0..n {
            let a_id = (next + i as u64) as i64;
            b_day.push(day);
            b_a_id.push(a_id);
            b_balance.push(rng.gen_range(-50_000_000..5_000_000_000i64));
            b_currency.push(account_currency(a_id));
            b_txn_count.push(rng.gen_range(0..200i32));
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Date32Array::from(b_day)),
                Arc::new(Int64Array::from(b_a_id)),
                Arc::new(decimal_15_2(b_balance)),
                Arc::new(StringArray::from(b_currency)),
                Arc::new(Int32Array::from(b_txn_count)),
            ],
        )
        .expect("account_balance batch");
        next += n as u64;
        Some(batch)
    })
}

// ---------------------------------------------------------------------------
// BenchmarkGenerator (local Parquet / scale-factor path)
// ---------------------------------------------------------------------------

impl BenchmarkGenerator for BankGenerator {
    fn name(&self) -> &str {
        "bank"
    }

    fn tables(&self) -> Vec<TableDef> {
        vec![
            TableDef {
                name: "customer".to_string(),
                schema: customer_schema(),
                row_count: |scale| BankPlan::from_scale(scale).customers as usize,
            },
            TableDef {
                name: "account".to_string(),
                schema: account_schema(),
                row_count: |scale| BankPlan::from_scale(scale).accounts() as usize,
            },
            TableDef {
                name: "kyc_profile".to_string(),
                schema: kyc_profile_schema(),
                row_count: |scale| BankPlan::from_scale(scale).customers as usize,
            },
            TableDef {
                name: "transaction".to_string(),
                schema: transaction_schema(),
                row_count: |scale| {
                    let p = BankPlan::from_scale(scale);
                    (p.txn_rows_per_day * p.days as u64) as usize
                },
            },
            TableDef {
                name: "account_balance".to_string(),
                schema: account_balance_schema(),
                row_count: |scale| {
                    let p = BankPlan::from_scale(scale);
                    (p.accounts() * p.days as u64) as usize
                },
            },
        ]
    }

    fn generate_table(
        &self,
        table: &str,
        scale: f64,
        output_dir: &str,
        config: &GenerateConfig,
    ) -> anyhow::Result<GenerateStats> {
        let plan = BankPlan::from_scale(scale);
        match table {
            "customer" => parallel_generate_table(
                table,
                customer_schema(),
                plan.customers as usize,
                unit_seed("customer", 0, 0),
                output_dir,
                config,
                customer_range,
            ),
            "account" => parallel_generate_table(
                table,
                account_schema(),
                plan.accounts() as usize,
                unit_seed("account", 0, 0),
                output_dir,
                config,
                move |range, seed| account_range(range, plan, seed),
            ),
            "kyc_profile" => parallel_generate_table(
                table,
                kyc_profile_schema(),
                plan.customers as usize,
                unit_seed("kyc_profile", 0, 0),
                output_dir,
                config,
                kyc_profile_range,
            ),
            // Facts run through the same day/shard units as the Iceberg
            // sink: the global row range maps onto per-day slices so scale
            // mode exercises identical code and stays deterministic.
            "transaction" => {
                let per_day = plan.txn_rows_per_day;
                let total = (per_day * plan.days as u64) as usize;
                parallel_generate_table(
                    table,
                    transaction_schema(),
                    total,
                    unit_seed("transaction", 0, 0),
                    output_dir,
                    config,
                    move |range, seed| {
                        day_slices(range, per_day).flat_map(move |(day_idx, rows, id_start)| {
                            transaction_day_shard(
                                plan.start_day + day_idx as i32,
                                rows,
                                id_start,
                                0..plan.accounts(),
                                super::config::seed_for_table_partition(seed, day_idx as usize),
                            )
                        })
                    },
                )
            }
            "account_balance" => {
                let per_day = plan.accounts();
                let total = (per_day * plan.days as u64) as usize;
                parallel_generate_table(
                    table,
                    account_balance_schema(),
                    total,
                    unit_seed("account_balance", 0, 0),
                    output_dir,
                    config,
                    move |range, seed| {
                        day_slices(range, per_day).flat_map(move |(day_idx, rows, id_start)| {
                            let start = (id_start as u64) % per_day;
                            account_balance_day_shard(
                                plan.start_day + day_idx as i32,
                                start..start + rows,
                                super::config::seed_for_table_partition(seed, day_idx as usize),
                            )
                        })
                    },
                )
            }
            other => anyhow::bail!("unknown bank table: {other}"),
        }
    }
}

/// Split a global fact row range into per-day slices.
///
/// Returns `(day_index, rows_in_slice, global_start_row)` for each day the
/// range touches. `global_start_row` doubles as the unique-id offset.
fn day_slices(
    range: std::ops::Range<usize>,
    rows_per_day: u64,
) -> impl Iterator<Item = (u32, u64, i64)> {
    let per_day = rows_per_day.max(1);
    let mut cursor = range.start as u64;
    let end = range.end as u64;
    std::iter::from_fn(move || {
        if cursor >= end {
            return None;
        }
        let day_idx = (cursor / per_day) as u32;
        let day_end = (day_idx as u64 + 1) * per_day;
        let slice_end = day_end.min(end);
        let out = (day_idx, slice_end - cursor, cursor as i64);
        cursor = slice_end;
        Some(out)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_seed_is_stable_and_distinct() {
        assert_eq!(unit_seed("transaction", 3, 7), unit_seed("transaction", 3, 7));
        assert_ne!(unit_seed("transaction", 3, 7), unit_seed("transaction", 3, 8));
        assert_ne!(unit_seed("transaction", 3, 7), unit_seed("transaction", 4, 7));
        assert_ne!(unit_seed("transaction", 3, 7), unit_seed("account_balance", 3, 7));
    }

    #[test]
    fn transaction_unit_is_deterministic() {
        let a: Vec<RecordBatch> =
            transaction_day_shard(DEFAULT_START_DAY, 1000, 0, 0..500, 42).collect();
        let b: Vec<RecordBatch> =
            transaction_day_shard(DEFAULT_START_DAY, 1000, 0, 0..500, 42).collect();
        assert_eq!(a, b);
        let c: Vec<RecordBatch> =
            transaction_day_shard(DEFAULT_START_DAY, 1000, 0, 0..500, 43).collect();
        assert_ne!(a, c);
    }

    #[test]
    fn transaction_unit_respects_day_shard_and_ordering() {
        let day = DEFAULT_START_DAY + 5;
        let batches: Vec<RecordBatch> =
            transaction_day_shard(day, 200_000, 10_000, 1000..2000, 7).collect();
        assert!(batches.len() > 1, "expected multiple batches");
        let mut last_ts = i64::MIN;
        let mut rows = 0u64;
        for b in &batches {
            assert!(b.num_rows() <= BATCH_SIZE);
            let days = b.column(1).as_any().downcast_ref::<Date32Array>().unwrap();
            let ts = b
                .column(2)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let a_ids = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                assert_eq!(days.value(i), day);
                assert!(ts.value(i) >= last_ts, "timestamps must ascend");
                last_ts = ts.value(i);
                assert!((1000..2000).contains(&a_ids.value(i)));
                let day_start = day as i64 * DAY_MICROS;
                assert!(ts.value(i) >= day_start && ts.value(i) < day_start + DAY_MICROS);
            }
            rows += b.num_rows() as u64;
        }
        assert_eq!(rows, 200_000);
    }

    #[test]
    fn fraud_ring_crosses_q03_threshold_at_sf01() {
        // q03's high-velocity screen (HAVING COUNT(*) > 100 debits in the
        // 3-day window) must fire for ring accounts and stay silent for
        // ordinary accounts, which uniform draws keep near a dozen debits.
        use std::collections::HashMap;
        let plan = BankPlan::from_scale(0.1);
        let per_day = plan.txn_rows_per_day;
        let mut debits: HashMap<i64, u64> = HashMap::new();
        for day_idx in 0..plan.days {
            let day = plan.start_day + day_idx as i32;
            let t_id_start = day_idx as i64 * per_day as i64;
            let seed = crate::generate::config::seed_for_table_partition(
                unit_seed("transaction", 0, 0),
                day_idx as usize,
            );
            for batch in
                transaction_day_shard(day, per_day, t_id_start, 0..plan.accounts(), seed)
            {
                let a_ids = batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
                let dirs = batch.column(8).as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..batch.num_rows() {
                    if dirs.value(i) == "debit" {
                        *debits.entry(a_ids.value(i)).or_default() += 1;
                    }
                }
            }
        }
        for acct in FRAUD_RING_START..FRAUD_RING_START + FRAUD_RING_COUNT {
            let c = debits.get(&acct).copied().unwrap_or(0);
            assert!(c > 100, "ring account {acct} has {c} debits, expected > 100");
        }
        for acct in [500i64, 5_000, 20_000] {
            let c = debits.get(&acct).copied().unwrap_or(0);
            assert!(c <= 100, "non-ring account {acct} has {c} debits, expected <= 100");
        }
    }

    #[test]
    fn account_fk_spread_is_valid_and_monotone() {
        let plan = BankPlan {
            customers: 1000,
            start_day: DEFAULT_START_DAY,
            days: 2,
            txn_rows_per_day: 100,
        };
        let mut last = -1i64;
        for a in 0..plan.accounts() as i64 {
            let c = account_customer(a, &plan);
            assert!((0..plan.customers as i64).contains(&c));
            assert!(c >= last);
            last = c;
        }
        // Every customer owns at least one account (2.5x spread).
        assert_eq!(account_customer(0, &plan), 0);
        assert_eq!(
            account_customer(plan.accounts() as i64 - 1, &plan),
            plan.customers as i64 - 1
        );
    }

    #[test]
    fn day_slices_cover_range_exactly() {
        let slices: Vec<_> = day_slices(150..1050, 400).collect();
        assert_eq!(slices, vec![(0, 250, 150), (1, 400, 400), (2, 250, 800)]);
        let total: u64 = slices.iter().map(|(_, n, _)| n).sum();
        assert_eq!(total, 900);
    }

    #[test]
    fn schemas_carry_field_ids() {
        for schema in [
            customer_schema(),
            account_schema(),
            kyc_profile_schema(),
            transaction_schema(),
            account_balance_schema(),
        ] {
            for (i, field) in schema.fields().iter().enumerate() {
                let id = field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .unwrap_or_else(|| panic!("{} missing field id", field.name()));
                assert_eq!(id, &(i + 1).to_string(), "{} id mismatch", field.name());
            }
        }
    }
}
