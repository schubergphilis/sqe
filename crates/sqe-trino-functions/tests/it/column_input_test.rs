//! Column-fed UDF tests (review finding 703 / #623).
//!
//! `SELECT f('literal')` is constant-folded before `invoke_with_args` ever
//! sees an array, so a UDF that only implements the scalar arm passes every
//! literal test while returning NULL for every row of a real column. The two
//! halves of this file close that hole:
//!
//! 1. A `MemTable` harness: every extended Trino UDF (plus the base UDFs that
//!    were rewritten on the same helpers) is called as `SELECT f(col) FROM t`
//!    with a three-row column and checked against expected values row by row.
//! 2. An invariant test that walks every scalar UDF registered by
//!    `register_trino_functions` / `register_extended_trino_functions`,
//!    invokes it directly with three-row arrays, and asserts the result is a
//!    `ColumnarValue::Array` of three rows whose type matches the declared
//!    return field. A skip-list (with reasons) covers the handful of UDFs
//!    whose inputs cannot be synthesised generically.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch, StringArray, StringViewArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use datafusion::prelude::*;

// ═══════════════════════════════════════════════════════════════════
// Part 1: MemTable harness
// ═══════════════════════════════════════════════════════════════════

/// 2024-03-15T14:30:45.123 and 2020-01-02T03:04:05 as UTC epoch micros.
const TS0_US: i64 = 1_710_513_045_123_000;
const TS1_US: i64 = 1_577_934_245_000_000;
/// 2024-03-15 and 2024-02-10 as days since epoch.
const D0: i32 = 19_797;
const D1: i32 = 19_763;

fn harness_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("rid", DataType::Int32, false),
        Field::new("str_a", DataType::Utf8, true),
        Field::new("str_b", DataType::Utf8, true),
        Field::new("view_a", DataType::Utf8View, true),
        Field::new("json_a", DataType::Utf8, true),
        Field::new("json_path", DataType::Utf8, true),
        Field::new("int_a", DataType::Int64, true),
        Field::new("int_b", DataType::Int32, true),
        Field::new("dbl_a", DataType::Float64, true),
        Field::new("date_a", DataType::Date32, true),
        Field::new(
            "ts_a",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("iso_ts", DataType::Utf8, true),
        Field::new("iso_date", DataType::Utf8, true),
        Field::new("url_a", DataType::Utf8, true),
        Field::new("b64_a", DataType::Utf8, true),
        Field::new("fmt_a", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2])),
            Arc::new(StringArray::from(vec![
                Some("karolin"),
                Some("hello123world"),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("kathrin"),
                Some("hello123xorld"),
                None,
            ])),
            Arc::new(StringViewArray::from(vec![
                Some("Robert"),
                Some("Tymczak"),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("[1,2,3]"),
                Some(r#"{"a":1,"b":{"c":[10,20]}}"#),
                None,
            ])),
            Arc::new(StringArray::from(vec![Some("$"), Some("$.b.c"), None])),
            Arc::new(Int64Array::from(vec![Some(255), Some(-10), None])),
            Arc::new(Int32Array::from(vec![Some(16), Some(2), None])),
            Arc::new(Float64Array::from(vec![Some(3661.0), Some(45.5), None])),
            Arc::new(Date32Array::from(vec![Some(D0), Some(D1), None])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(TS0_US),
                Some(TS1_US),
                None,
            ])),
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(TS0_US * 1000),
                Some(TS1_US * 1000),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("2024-03-15T14:30:45.123"),
                Some("2020-01-02 03:04:05"),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("2024-03-15"),
                Some("20240210"),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("https://example.com:8080/docs/index.html?q=1&x=y"),
                Some("http://h/p"),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("aGVsbG8="),
                Some("d29ybGQ="),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("yyyy-MM-dd"),
                Some("HH:mm:ss"),
                None,
            ])),
        ],
    )
    .expect("harness batch")
}

async fn harness_ctx() -> SessionContext {
    let mut ctx = SessionContext::new();
    sqe_trino_functions::register_trino_functions(&ctx);
    datafusion_functions_json::register_all(&mut ctx).expect("register JSON functions");
    sqe_trino_functions::register_extended_trino_functions(&ctx);
    let batch = harness_batch();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("memtable");
    ctx.register_table("t", Arc::new(table))
        .expect("register t");
    ctx
}

/// Evaluate `expr` over the harness table and return column 0 for the three
/// rows in `rid` order, rendered with `array_value_to_string` (NULL -> None).
async fn col_eval(expr: &str) -> Vec<Option<String>> {
    let ctx = harness_ctx().await;
    let sql = format!("SELECT {expr} FROM t ORDER BY rid");
    let df = ctx.sql(&sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let batches = df.collect().await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &batches {
        let col = b.column(0);
        for i in 0..b.num_rows() {
            out.push(if col.is_null(i) {
                None
            } else {
                Some(array_value_to_string(col, i).unwrap())
            });
        }
    }
    assert_eq!(out.len(), 3, "{sql}: expected 3 rows, got {out:?}");
    out
}

/// Evaluate `expr` over the harness table and return the result data type.
async fn col_type(expr: &str) -> DataType {
    let ctx = harness_ctx().await;
    let sql = format!("SELECT {expr} FROM t ORDER BY rid");
    let df = ctx.sql(&sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let batches = df.collect().await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    batches[0].column(0).data_type().clone()
}

async fn col_err(expr: &str) -> String {
    let ctx = harness_ctx().await;
    let sql = format!("SELECT {expr} FROM t ORDER BY rid");
    match ctx.sql(&sql).await {
        Err(e) => e.to_string(),
        Ok(df) => match df.collect().await {
            Err(e) => e.to_string(),
            Ok(_) => panic!("{sql}: expected an error"),
        },
    }
}

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

macro_rules! col_case {
    ($name:ident, $expr:expr, [$a:expr, $b:expr, $c:expr]) => {
        #[tokio::test]
        async fn $name() {
            let got = col_eval($expr).await;
            let want: Vec<Option<String>> = vec![$a, $b, $c];
            assert_eq!(got, want, "expression: {}", $expr);
        }
    };
}

// ── the eleven functions from the finding ────────────────────────────

col_case!(
    col_hamming_distance,
    "hamming_distance(str_a, str_b)",
    [s("3"), s("1"), None]
);
col_case!(
    col_hamming_distance_literal_rhs,
    "hamming_distance(substr(str_a, 1, 3), 'kar')",
    [s("0"), s("3"), None]
);
// "kar" and "hel" read as base-36 numbers.
col_case!(
    col_from_base,
    "from_base(substr(str_a, 1, 3), 36)",
    [s("26307"), s("22557"), None]
);
col_case!(
    col_from_base_col_radix,
    "from_base('11', int_b)",
    [s("17"), s("3"), None]
);
col_case!(col_to_base, "to_base(int_a, 16)", [s("ff"), s("-a"), None]);
col_case!(
    col_to_base_both_cols,
    "to_base(int_a, int_b)",
    [s("ff"), s("-1010"), None]
);
col_case!(
    col_from_iso8601_timestamp,
    "from_iso8601_timestamp(iso_ts)",
    [s("2024-03-15T14:30:45.123"), s("2020-01-02T03:04:05"), None]
);
col_case!(
    col_to_iso8601_ts,
    "to_iso8601(ts_a)",
    [s("2024-03-15T14:30:45.123"), s("2020-01-02T03:04:05"), None]
);
col_case!(
    col_to_iso8601_ns,
    "to_iso8601(ts_ns)",
    [s("2024-03-15T14:30:45.123"), s("2020-01-02T03:04:05"), None]
);
col_case!(
    col_to_iso8601_date,
    "to_iso8601(date_a)",
    [s("2024-03-15"), s("2024-02-10"), None]
);
col_case!(
    col_with_timezone,
    "with_timezone(ts_a, 'Europe/Amsterdam')",
    // Wall clock is preserved; the zone tag makes the offset visible.
    [
        s("2024-03-15T14:30:45.123+01:00"),
        s("2020-01-02T03:04:05+01:00"),
        None
    ]
);
col_case!(
    col_at_timezone,
    "at_timezone(ts_a, 'Europe/Amsterdam')",
    // Same instant, displayed in the zone (CET = UTC+1 on both dates).
    [
        s("2024-03-15T15:30:45.123+01:00"),
        s("2020-01-02T04:04:05+01:00"),
        None
    ]
);
col_case!(
    col_at_timezone_fixed_offset,
    "at_timezone(ts_a, '-05:00')",
    [
        s("2024-03-15T09:30:45.123-05:00"),
        s("2020-01-01T22:04:05-05:00"),
        None
    ]
);
col_case!(
    col_format_datetime,
    "format_datetime(ts_a, 'yyyy-MM-dd HH:mm:ss')",
    [s("2024-03-15 14:30:45"), s("2020-01-02 03:04:05"), None]
);
col_case!(
    col_format_datetime_col_pattern,
    "format_datetime(ts_a, fmt_a)",
    [s("2024-03-15"), s("03:04:05"), None]
);
col_case!(
    col_format_datetime_zoned_input,
    "format_datetime(at_timezone(ts_a, 'Asia/Tokyo'), 'yyyy-MM-dd HH:mm')",
    [s("2024-03-15 23:30"), s("2020-01-02 12:04"), None]
);
col_case!(
    col_parse_datetime,
    "parse_datetime(iso_date, 'yyyyMMdd')",
    [None, s("2024-02-10T00:00:00"), None]
);
col_case!(
    col_parse_datetime_col_pattern,
    "parse_datetime(iso_date, CASE WHEN rid = 0 THEN 'yyyy-MM-dd' ELSE 'yyyyMMdd' END)",
    [s("2024-03-15T00:00:00"), s("2024-02-10T00:00:00"), None]
);
col_case!(
    col_json_array_contains,
    "json_array_contains(json_a, 2)",
    [s("true"), s("false"), None]
);
col_case!(
    col_json_array_contains_col_value,
    "json_array_contains('[1,2,255]', int_a)",
    [s("true"), s("false"), None]
);
col_case!(
    col_json_size,
    "json_size(json_a, json_path)",
    [s("3"), s("2"), None]
);
col_case!(
    col_json_size_literal_path,
    "json_size(json_a, '$')",
    [s("3"), s("2"), None]
);
col_case!(
    col_json_array_get,
    "json_array_get(json_a, 1)",
    [s("2"), None, None]
);
col_case!(
    col_json_array_get_col_index,
    "json_array_get('[10,20,30]', int_b - 15)",
    [s("20"), None, None]
);

// ── the other extended UDFs that had scalar-only or StringArray-only paths ──

col_case!(
    col_millisecond,
    "millisecond(ts_a)",
    [s("123"), s("0"), None]
);
col_case!(
    col_millisecond_ns,
    "millisecond(ts_ns)",
    [s("123"), s("0"), None]
);
col_case!(
    col_is_json_scalar,
    "is_json_scalar(json_a)",
    [s("false"), s("false"), None]
);
col_case!(
    col_is_json_scalar_view,
    "is_json_scalar(view_a)",
    [s("false"), s("false"), None]
);
col_case!(
    col_soundex_utf8view,
    "soundex(view_a)",
    [s("R163"), s("T522"), None]
);
col_case!(
    col_from_iso8601_date,
    "from_iso8601_date(iso_date)",
    [s("2024-03-15"), s("2024-02-10"), None]
);
col_case!(
    col_human_readable_seconds,
    "human_readable_seconds(dbl_a)",
    [
        s("1 hours, 1 minutes, 1.00 seconds"),
        s("45.50 seconds"),
        None
    ]
);
col_case!(
    col_human_readable_seconds_int,
    "human_readable_seconds(int_a)",
    [s("4 minutes, 15.00 seconds"), s("10.00 seconds"), None]
);
col_case!(
    col_last_day_of_month,
    "last_day_of_month(date_a)",
    [s("2024-03-31"), s("2024-02-29"), None]
);
col_case!(
    col_last_day_of_month_ts,
    "last_day_of_month(ts_a)",
    [s("2024-03-31"), s("2020-01-31"), None]
);
col_case!(
    col_regexp_extract,
    r"regexp_extract(str_a, '(\d+)')",
    [None, s("123"), None]
);
col_case!(
    col_regexp_extract_col_pattern,
    r"regexp_extract(str_a, str_b)",
    [None, None, None]
);
col_case!(
    col_regexp_extract_all,
    r"regexp_extract_all(str_a, '[lo]')",
    [s("[o, l]"), s("[l, l, o, o, l]"), None]
);
col_case!(
    col_regexp_split,
    r"regexp_split(str_a, '\d+')",
    [s("[karolin]"), s("[hello, world]"), None]
);
col_case!(
    col_regexp_split_view,
    r"regexp_split(view_a, 'e')",
    [s("[Rob, rt]"), s("[Tymczak]"), None]
);
col_case!(
    col_normalize,
    "normalize(view_a, 'NFC')",
    [s("Robert"), s("Tymczak"), None]
);
col_case!(
    col_word_stem,
    "word_stem(str_a)",
    [s("karolin"), s("hello123world"), None]
);
col_case!(
    col_word_stem_lang,
    "word_stem(str_a, 'de')",
    [s("karolin"), s("hello123world"), None]
);
col_case!(
    col_checksum_is_per_row,
    "length(checksum(str_a))",
    [s("16"), s("16"), None]
);
col_case!(
    col_timezone_hour,
    "timezone_hour(ts_a)",
    [s("0"), s("0"), s("0")]
);
col_case!(
    col_timezone_minute,
    "timezone_minute(ts_a)",
    [s("0"), s("0"), s("0")]
);
col_case!(
    col_try_passthrough,
    "try(int_a)",
    [s("255"), s("-10"), None]
);
col_case!(
    col_format,
    "format('%s=%d', str_a, int_a)",
    [s("karolin=255"), s("hello123world=-10"), s("null=null")]
);
col_case!(
    col_format_col_pattern,
    "format(fmt_a, int_a)",
    [s("yyyy-MM-dd"), s("HH:mm:ss"), None]
);
col_case!(
    col_to_json,
    "to_json(int_a)",
    [s("255"), s("-10"), s("null")]
);
col_case!(
    col_json_object_col_value,
    "json_object('id', int_a, 'name', str_a)",
    [
        s(r#"{"id":255,"name":"karolin"}"#),
        s(r#"{"id":-10,"name":"hello123world"}"#),
        s(r#"{"id":null,"name":null}"#)
    ]
);

// ── base UDFs rewritten on the shared helpers ────────────────────────

col_case!(
    col_url_extract_host,
    "url_extract_host(url_a)",
    [s("example.com"), s("h"), None]
);
col_case!(
    col_url_extract_port,
    "url_extract_port(url_a)",
    [s("8080"), None, None]
);
col_case!(
    col_url_extract_parameter,
    "url_extract_parameter(url_a, 'x')",
    [s("y"), None, None]
);
col_case!(
    col_url_extract_parameter_col_name,
    "url_extract_parameter(url_a, CASE WHEN rid = 0 THEN 'q' ELSE 'zz' END)",
    [s("1"), None, None]
);
col_case!(
    col_url_encode_view,
    "url_encode(view_a)",
    [s("Robert"), s("Tymczak"), None]
);
col_case!(
    col_from_base64,
    "from_base64(b64_a)",
    [s("hello"), s("world"), None]
);
col_case!(
    col_json_extract_scalar,
    "json_extract_scalar(json_a, '$.a')",
    [None, s("1"), None]
);
col_case!(
    col_json_parse_view,
    "json_parse(view_a)",
    [s("null"), s("null"), None]
);
col_case!(
    col_date_diff_mixed_scalar_array,
    "date_diff('day', date_a, DATE '2024-03-20')",
    [s("5"), s("39"), None]
);
col_case!(
    col_date_diff_hours_exact,
    "date_diff('hour', ts_a, TIMESTAMP '2024-03-16 00:00:00')",
    // 1535 days minus 03:04:05 = 36836.93 h, truncated (was 24 * whole days).
    [s("9"), s("36836"), None]
);
col_case!(
    col_date_diff_col_unit,
    "date_diff(CASE WHEN rid = 0 THEN 'month' ELSE 'year' END, date_a, DATE '2025-01-01')",
    [s("10"), s("1"), None]
);

// ── return types and errors ──────────────────────────────────────────

#[tokio::test]
async fn at_timezone_declared_type_matches_runtime_type() {
    let dt = col_type("at_timezone(ts_a, 'Europe/Amsterdam')").await;
    assert_eq!(
        dt,
        DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Amsterdam".into()))
    );
    let dt = col_type("with_timezone(ts_a, 'Asia/Tokyo')").await;
    assert_eq!(
        dt,
        DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Tokyo".into()))
    );
}

#[tokio::test]
async fn at_timezone_requires_constant_zone() {
    let err = col_err("at_timezone(ts_a, str_a)").await;
    assert!(
        err.contains("time zone must be a constant"),
        "unexpected error: {err}"
    );
    let err = col_err("with_timezone(ts_a, 'Mars/Olympus')").await;
    assert!(
        err.contains("Unknown time zone: Mars/Olympus"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn unsupported_input_type_is_plan_error_not_null() {
    for (expr, needle) in [
        (
            "hamming_distance(int_a, str_b)",
            "Unexpected parameters (bigint, varchar) for function hamming_distance. Expected: hamming_distance(varchar, varchar)",
        ),
        (
            "format_datetime(str_a, 'yyyy')",
            "Unexpected parameters (varchar, varchar) for function format_datetime. Expected: format_datetime(timestamp, varchar)",
        ),
        (
            "json_size(int_a, '$')",
            "Unexpected parameters (bigint, varchar) for function json_size",
        ),
        (
            "to_base(str_a, 16)",
            "Unexpected parameters (varchar, bigint) for function to_base. Expected: to_base(bigint, bigint)",
        ),
        (
            "to_iso8601(str_a)",
            "Unexpected parameters (varchar) for function to_iso8601",
        ),
        (
            "soundex(int_a)",
            "Unexpected parameters (bigint) for function soundex. Expected: soundex(varchar)",
        ),
    ] {
        let err = col_err(expr).await;
        assert!(err.contains("Error during planning"), "{expr}: not a Plan error: {err}");
        assert!(err.contains(needle), "{expr}: unexpected message: {err}");
    }
}

#[tokio::test]
async fn hamming_distance_unequal_length_is_an_error_per_row() {
    let err = col_err("hamming_distance(str_a, 'abc')").await;
    assert!(
        err.contains("must have the same length"),
        "unexpected error: {err}"
    );
}

// ═══════════════════════════════════════════════════════════════════
// Part 2: array-shape invariant over every registered UDF
// ═══════════════════════════════════════════════════════════════════

/// How to synthesise one argument for the invariant test.
#[derive(Clone, Copy)]
enum ArgSpec {
    /// A three-row array of the given kind.
    Col(Kind),
    /// A constant string (for parameters that must be literals, e.g. a
    /// time zone or a `date_format` pattern).
    LitStr(&'static str),
    /// A constant integer.
    LitInt(i64),
}

#[derive(Clone, Copy)]
enum Kind {
    Str,
    View,
    Json,
    Url,
    Int,
    Int32,
    Float,
    Bool,
    Date,
    Ts,
    TsNs,
    IsoTs,
    B64,
    Hex,
    Char,
    ListInt,
}

fn make_array(kind: Kind) -> ArrayRef {
    match kind {
        Kind::Str => Arc::new(StringArray::from(vec!["karolin", "kathrin", "running"])),
        Kind::Char => Arc::new(StringArray::from(vec!["a", "b", "c"])),
        Kind::View => Arc::new(StringViewArray::from(vec!["Robert", "Rupert", "Tymczak"])),
        Kind::Json => Arc::new(StringArray::from(vec![
            "[1,2,3]",
            r#"{"a":1,"b":2}"#,
            r#"{"a":{"b":[true]}}"#,
        ])),
        Kind::Url => Arc::new(StringArray::from(vec![
            "https://example.com:8080/p?q=1",
            "http://h/p",
            "ftp://x.org/f",
        ])),
        Kind::Int => Arc::new(Int64Array::from(vec![255, 10, 7])),
        Kind::Int32 => Arc::new(Int32Array::from(vec![2, 1, 0])),
        Kind::Float => Arc::new(Float64Array::from(vec![3661.0, 45.5, 0.25])),
        Kind::Bool => Arc::new(BooleanArray::from(vec![true, false, true])),
        Kind::Date => Arc::new(Date32Array::from(vec![D0, D1, 0])),
        Kind::Ts => Arc::new(TimestampMicrosecondArray::from(vec![TS0_US, TS1_US, 0])),
        Kind::TsNs => Arc::new(TimestampNanosecondArray::from(vec![
            TS0_US * 1000,
            TS1_US * 1000,
            0,
        ])),
        Kind::IsoTs => Arc::new(StringArray::from(vec![
            "2024-03-15T14:30:45.123",
            "2020-01-02 03:04:05",
            "1970-01-01T00:00:00",
        ])),
        Kind::B64 => Arc::new(StringArray::from(vec!["aGVsbG8=", "d29ybGQ=", "eA=="])),
        Kind::Hex => Arc::new(StringArray::from(vec!["68656c6c6f", "776f726c64", "78"])),
        Kind::ListInt => Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![Some(6)]),
        ])),
    }
}

/// Argument recipe per UDF name. Every scalar UDF registered by this crate
/// must appear here or in [`skip_reason`]; the invariant test fails on an
/// unknown name so a newly added UDF cannot dodge the check.
fn arg_specs(name: &str) -> Option<Vec<ArgSpec>> {
    use ArgSpec::*;
    use Kind::*;
    let v = match name {
        // ── extended ──
        "millisecond" | "timezone_hour" | "timezone_minute" => vec![Col(Ts)],
        // Nanosecond input (what `CAST(x AS TIMESTAMP)` produces) must be
        // accepted and normalised to microseconds.
        "to_iso8601" => vec![Col(TsNs)],
        "is_json_scalar" => vec![Col(Json)],
        "json_array_contains" => vec![Col(Json), Col(Int)],
        "soundex" | "word_stem" | "word_stem_lang" | "checksum" | "to_json" | "try" => {
            vec![Col(Str)]
        }
        "hamming_distance" => vec![Col(Str), Col(Str)],
        "from_base" => vec![Col(Str), LitInt(36)],
        "to_base" => vec![Col(Int), LitInt(16)],
        "from_iso8601_date" => vec![Col(IsoTs)],
        "from_iso8601_timestamp" => vec![Col(IsoTs)],
        "human_readable_seconds" => vec![Col(Float)],
        "last_day_of_month" => vec![Col(Date)],
        "regexp_extract" | "regexp_extract_all" | "regexp_split" => {
            vec![Col(Str), LitStr("[aeiou]")]
        }
        "normalize" => vec![Col(View), LitStr("NFC")],
        "with_timezone" | "at_timezone" => vec![Col(Ts), LitStr("Europe/Amsterdam")],
        "format_datetime" => vec![Col(Ts), LitStr("yyyy-MM-dd")],
        "parse_datetime" => vec![Col(IsoTs), LitStr("yyyy-MM-dd'T'HH:mm:ss")],
        "json_size" => vec![Col(Json), LitStr("$")],
        "json_array_get" => vec![Col(Json), Col(Int32)],
        "format" => vec![LitStr("%s-%d"), Col(Str), Col(Int)],
        // ── base ──
        "year" | "month" | "day" | "hour" | "minute" | "second" | "day_of_week" | "dow"
        | "day_of_year" | "doy" | "quarter" | "week" | "week_of_year" | "to_unixtime" | "date" => {
            vec![Col(Ts)]
        }
        "date_add" => vec![LitStr("day"), LitInt(1), Col(Ts)],
        "date_diff" => vec![LitStr("day"), Col(Ts), Col(Date)],
        "from_unixtime" => vec![Col(Int)],
        "if" | "iff" => vec![Col(Bool), Col(Int), Col(Int)],
        "date_format" => vec![Col(Ts), LitStr("%Y-%m-%d")],
        "date_parse" => vec![Col(IsoTs), LitStr("%Y-%m-%dT%H:%i:%s")],
        "json_object" => vec![LitStr("k"), Col(Int)],
        "json_format" | "json_parse" | "json_array_length" => vec![Col(Json)],
        "url_extract_host"
        | "url_extract_path"
        | "url_extract_port"
        | "url_extract_protocol"
        | "url_extract_query" => vec![Col(Url)],
        "url_extract_parameter" => vec![Col(Url), LitStr("q")],
        "url_encode" | "url_decode" | "to_base64" | "to_utf8" => vec![Col(Str)],
        "from_base64" => vec![Col(B64)],
        "from_hex" | "from_utf8" => vec![Col(Hex)],
        "json_extract" | "json_extract_scalar" => vec![Col(Json), LitStr("$.a")],
        "mod" => vec![Col(Int), Col(Int32)],
        "truncate" => vec![Col(Float)],
        "sign" => vec![Col(Int)],
        "codepoint" => vec![Col(Char)],
        // The bitwise signature is uniform Int64; the SQL planner coerces
        // narrower integers before the UDF sees them.
        "bitwise_and"
        | "bitwise_or"
        | "bitwise_xor"
        | "bitwise_left_shift"
        | "bitwise_right_shift" => vec![Col(Int), Col(Int)],
        "bitwise_not" => vec![Col(Int)],
        "slice" => vec![Col(ListInt), LitInt(1), LitInt(2)],
        "element_at" => vec![Col(ListInt), LitInt(1)],
        "contains" => vec![Col(ListInt), Col(Int32)],
        "split" => vec![Col(Str), LitStr("r")],
        "sequence" => vec![Col(Int), LitInt(300)],
        _ => return None,
    };
    Some(v)
}

/// UDFs the invariant deliberately does not cover, with the reason.
fn skip_reason(name: &str) -> Option<&'static str> {
    Some(match name {
        "infinity" | "nan" | "e" | "current_timezone" | "now" | "localtime" | "localtimestamp" => {
            "zero-arity: no array input exists"
        }
        "arbitrary" => {
            "aggregate-alias stub: collapses a column to its first non-null value by design"
        }
        "typeof" => "returns the static type name; a broadcast scalar is the correct shape",
        "all_match" | "none_match" | "reduce" | "array_all_match" | "array_none_match"
        | "array_reduce" | "filter" | "transform" | "any_match" | "array_any_match"
        | "array_filter" | "array_transform" => "lambda argument cannot be synthesised as an array",
        _ => return None,
    })
}

fn scalar_arg(spec: ArgSpec) -> ColumnarValue {
    match spec {
        ArgSpec::Col(kind) => ColumnarValue::Array(make_array(kind)),
        ArgSpec::LitStr(s) => ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.to_string()))),
        ArgSpec::LitInt(i) => ColumnarValue::Scalar(ScalarValue::Int64(Some(i))),
    }
}

/// Names registered by our two entry points, excluding DataFusion built-ins.
fn our_scalar_udf_names(ctx: &SessionContext) -> BTreeSet<String> {
    let builtin: BTreeSet<String> = SessionContext::new()
        .state()
        .scalar_functions()
        .keys()
        .cloned()
        .collect();
    ctx.state()
        .scalar_functions()
        .keys()
        .filter(|k| !builtin.contains(*k))
        .cloned()
        .collect()
}

#[tokio::test]
async fn every_udf_returns_an_array_for_array_input() {
    let ctx = SessionContext::new();
    sqe_trino_functions::register_trino_functions(&ctx);
    sqe_trino_functions::register_extended_trino_functions(&ctx);

    let names = our_scalar_udf_names(&ctx);
    assert!(names.len() > 60, "registry looks too small: {names:?}");

    let mut failures: Vec<String> = Vec::new();
    let mut covered = 0usize;
    let mut skipped: Vec<String> = Vec::new();

    for name in &names {
        if let Some(reason) = skip_reason(name) {
            skipped.push(format!("{name}: {reason}"));
            continue;
        }
        let Some(specs) = arg_specs(name) else {
            failures.push(format!(
                "{name}: no argument recipe in arg_specs() and no skip_reason(); add one"
            ));
            continue;
        };
        let udf = ctx.state().scalar_functions()[name].clone();
        let args: Vec<ColumnarValue> = specs.iter().map(|s| scalar_arg(*s)).collect();
        let arg_fields: Vec<Arc<Field>> = args
            .iter()
            .enumerate()
            .map(|(i, a)| Arc::new(Field::new(format!("arg{i}"), a.data_type(), true)))
            .collect();
        let scalar_args: Vec<Option<&ScalarValue>> = args
            .iter()
            .map(|a| match a {
                ColumnarValue::Scalar(sv) => Some(sv),
                ColumnarValue::Array(_) => None,
            })
            .collect();
        let return_field = match udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_args,
        }) {
            Ok(f) => f,
            Err(e) => {
                failures.push(format!("{name}: return_field_from_args failed: {e}"));
                continue;
            }
        };
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 3,
            return_field: return_field.clone(),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        });
        match result {
            Err(e) => failures.push(format!("{name}: invoke failed: {e}")),
            Ok(ColumnarValue::Scalar(sv)) => failures.push(format!(
                "{name}: returned Scalar({sv:?}) for 3-row array input (would broadcast one value to every row)"
            )),
            Ok(ColumnarValue::Array(arr)) => {
                if arr.len() != 3 {
                    failures.push(format!("{name}: returned {} rows, expected 3", arr.len()));
                } else if arr.data_type() != return_field.data_type() {
                    failures.push(format!(
                        "{name}: runtime type {} != declared return type {}",
                        arr.data_type(),
                        return_field.data_type()
                    ));
                } else {
                    covered += 1;
                }
            }
        }
    }

    // Visible with `--nocapture`; documents what the invariant actually ran.
    eprintln!(
        "array-shape invariant: covered={covered} skipped={}\n  {}",
        skipped.len(),
        skipped.join("\n  ")
    );
    assert!(
        failures.is_empty(),
        "{} UDF(s) violate the array-shape invariant:\n  {}\n\ncovered={covered} skipped={}:\n  {}",
        failures.len(),
        failures.join("\n  "),
        skipped.len(),
        skipped.join("\n  ")
    );
    // Sanity: the eleven functions from the finding are all covered.
    for must in [
        "hamming_distance",
        "from_base",
        "to_base",
        "from_iso8601_timestamp",
        "to_iso8601",
        "with_timezone",
        "at_timezone",
        "format_datetime",
        "parse_datetime",
        "json_array_contains",
        "json_size",
        "json_array_get",
    ] {
        assert!(names.contains(must), "{must} not registered");
        assert!(skip_reason(must).is_none(), "{must} must not be skipped");
    }
}
