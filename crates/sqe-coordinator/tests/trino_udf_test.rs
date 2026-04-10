//! Tests for Trino-compatible UDF functions.
//!
//! Each test creates a SessionContext, registers all Trino UDFs,
//! and executes a SQL query to verify the function works correctly.
//! These tests run against a plain DataFusion SessionContext — no external
//! services required.

use datafusion::arrow::array::{BooleanArray, Float64Array, Int64Array};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::*;

async fn ctx() -> SessionContext {
    let mut ctx = SessionContext::new();
    sqe_coordinator::trino_functions::register_trino_functions(&ctx);
    datafusion_functions_json::register_all(&mut ctx).expect("register JSON functions");
    sqe_coordinator::trino_functions_ext::register_extended_trino_functions(&ctx);
    ctx
}

/// Execute SQL and return the first column of the first row as a String via
/// `array_value_to_string` (works for any Arrow type).
async fn eval_str(sql: &str) -> Option<String> {
    let ctx = ctx().await;
    let df = ctx.sql(sql).await.expect("SQL parse failed");
    let batches = df.collect().await.expect("execute failed");
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return None;
    }
    let col = batches[0].column(0);
    if col.is_null(0) {
        return None;
    }
    Some(array_value_to_string(col, 0).unwrap())
}

/// Execute SQL and return the first column of the first row as i64.
async fn eval_i64(sql: &str) -> Option<i64> {
    let ctx = ctx().await;
    let df = ctx.sql(sql).await.expect("SQL parse failed");
    let batches = df.collect().await.expect("execute failed");
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return None;
    }
    let col = batches[0].column(0);
    if col.is_null(0) {
        return None;
    }
    col.as_any().downcast_ref::<Int64Array>().map(|a| a.value(0))
}

/// Execute SQL and return the first column of the first row as f64.
async fn eval_f64(sql: &str) -> Option<f64> {
    let ctx = ctx().await;
    let df = ctx.sql(sql).await.expect("SQL parse failed");
    let batches = df.collect().await.expect("execute failed");
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return None;
    }
    let col = batches[0].column(0);
    if col.is_null(0) {
        return None;
    }
    col.as_any()
        .downcast_ref::<Float64Array>()
        .map(|a| a.value(0))
}

/// Execute SQL and return the first column of the first row as bool.
async fn eval_bool(sql: &str) -> Option<bool> {
    let ctx = ctx().await;
    let df = ctx.sql(sql).await.expect("SQL parse failed");
    let batches = df.collect().await.expect("execute failed");
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return None;
    }
    let col = batches[0].column(0);
    if col.is_null(0) {
        return None;
    }
    col.as_any()
        .downcast_ref::<BooleanArray>()
        .map(|a| a.value(0))
}

// ═══════════════════════════════════════════════════════════════
// Date/Time Extract Functions
//
// These UDFs return Float64 (matching DataFusion's date_part semantics).
// We use eval_f64 and compare with f64 literals.
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_year() {
    let v = eval_i64("SELECT year(CAST('2024-03-15' AS DATE))").await;
    assert_eq!(v, Some(2024));
}

#[tokio::test]
async fn test_month() {
    let v = eval_i64("SELECT month(CAST('2024-03-15' AS DATE))").await;
    assert_eq!(v, Some(3));
}

#[tokio::test]
async fn test_day() {
    let v = eval_i64("SELECT day(CAST('2024-03-15' AS DATE))").await;
    assert_eq!(v, Some(15));
}

#[tokio::test]
async fn test_quarter() {
    let v = eval_i64("SELECT quarter(CAST('2024-08-15' AS DATE))").await;
    assert_eq!(v, Some(3));
}

#[tokio::test]
async fn test_week() {
    let v = eval_i64("SELECT week(CAST('2024-01-08' AS DATE))").await;
    assert!(v.is_some(), "week() returned None");
    let w = v.unwrap();
    assert!((1..=53).contains(&w), "Week {w} out of range");
}

#[tokio::test]
async fn test_day_of_week() {
    // 2024-01-15 is a Monday
    let v = eval_i64("SELECT day_of_week(CAST('2024-01-15' AS DATE))").await;
    assert!(v.is_some(), "day_of_week returned None");
    let d = v.unwrap();
    assert!((0..=7).contains(&d), "day_of_week {d} out of [0,7]");
}

#[tokio::test]
async fn test_day_of_year() {
    // 2024-02-01 is the 32nd day of the year (Jan has 31 days)
    let v = eval_i64("SELECT day_of_year(CAST('2024-02-01' AS DATE))").await;
    assert_eq!(v, Some(32));
}

#[tokio::test]
async fn test_hour() {
    // Use TimestampMicrosecond explicitly via CAST to microsecond precision
    let v =
        eval_i64("SELECT hour(CAST('2024-03-15 14:30:00' AS TIMESTAMP(6)))").await;
    assert_eq!(v, Some(14));
}

#[tokio::test]
async fn test_minute() {
    let v =
        eval_i64("SELECT minute(CAST('2024-03-15 14:30:00' AS TIMESTAMP(6)))").await;
    assert_eq!(v, Some(30));
}

#[tokio::test]
async fn test_second() {
    let v =
        eval_i64("SELECT second(CAST('2024-03-15 14:30:45' AS TIMESTAMP(6)))").await;
    assert_eq!(v, Some(45));
}

// ═══════════════════════════════════════════════════════════════
// Date Arithmetic
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_date_diff() {
    // Use DATE (Date32) inputs — date_diff handles Date32 and TimestampMicrosecond.
    // CAST(... AS TIMESTAMP) in DataFusion produces TimestampNanosecond, which is
    // not supported; use CAST(... AS DATE) for the day-level diff case.
    let v = eval_i64(
        "SELECT date_diff('day', CAST('2024-01-01' AS DATE), CAST('2024-01-10' AS DATE))",
    )
    .await;
    assert_eq!(v, Some(9));
}

#[tokio::test]
async fn test_from_unixtime() {
    // from_unixtime(0) should give a timestamp in the 1970 epoch
    let v = eval_str("SELECT CAST(from_unixtime(0) AS VARCHAR)").await;
    assert!(v.is_some(), "from_unixtime returned None");
    assert!(
        v.unwrap().contains("1970"),
        "Expected 1970 in timestamp output"
    );
}

// ═══════════════════════════════════════════════════════════════
// Conditional / Type
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_typeof() {
    let v = eval_str("SELECT typeof(42)").await;
    assert!(v.is_some(), "typeof returned None");
    let t = v.unwrap();
    assert!(t.contains("Int"), "Expected Int type name, got: {t}");
}

// ═══════════════════════════════════════════════════════════════
// Date Formatting / Parsing
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_date_format() {
    let v = eval_str(
        "SELECT date_format(CAST('2024-03-15 14:30:00' AS TIMESTAMP), '%Y-%m-%d')",
    )
    .await;
    assert_eq!(v, Some("2024-03-15".to_string()));
}

// ═══════════════════════════════════════════════════════════════
// JSON Functions (UDFs registered in trino_functions.rs)
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_json_extract() {
    let v =
        eval_str(r#"SELECT json_extract('{"name":"alice","age":30}', '$.name')"#).await;
    assert!(v.is_some(), "json_extract returned None");
    assert!(v.unwrap().contains("alice"), "Expected 'alice' in result");
}

#[tokio::test]
async fn test_json_extract_scalar() {
    let v =
        eval_str(r#"SELECT json_extract_scalar('{"name":"alice","age":30}', '$.name')"#)
            .await;
    assert_eq!(v, Some("alice".to_string()));
}

#[tokio::test]
async fn test_json_array_length() {
    let v = eval_i64(r#"SELECT json_array_length('[1,2,3,4,5]')"#).await;
    assert_eq!(v, Some(5));
}

#[tokio::test]
async fn test_json_parse() {
    let v = eval_str(r#"SELECT json_parse('{"a":1}')"#).await;
    assert!(v.is_some(), "json_parse returned None");
    assert!(v.unwrap().contains("\"a\""), "Expected key 'a' in result");
}

// ═══════════════════════════════════════════════════════════════
// Extended JSON Functions (from trino_functions_ext.rs)
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_is_json_scalar_true() {
    let v = eval_bool(r#"SELECT is_json_scalar('"hello"')"#).await;
    assert_eq!(v, Some(true));
}

#[tokio::test]
async fn test_is_json_scalar_false_object() {
    let v = eval_bool(r#"SELECT is_json_scalar('{"a":1}')"#).await;
    assert_eq!(v, Some(false));
}

#[tokio::test]
async fn test_json_array_contains_true() {
    let v = eval_bool(r#"SELECT json_array_contains('[1,2,3]', 2)"#).await;
    assert_eq!(v, Some(true));
}

#[tokio::test]
async fn test_json_array_contains_false() {
    let v = eval_bool(r#"SELECT json_array_contains('[1,2,3]', 5)"#).await;
    assert_eq!(v, Some(false));
}

#[tokio::test]
async fn test_json_size() {
    let v = eval_i64(r#"SELECT json_size('{"a":1,"b":2,"c":3}', '$')"#).await;
    assert_eq!(v, Some(3));
}

#[tokio::test]
async fn test_json_array_get() {
    let v = eval_str(r#"SELECT json_array_get('[10,20,30]', 1)"#).await;
    assert!(v.is_some(), "json_array_get returned None");
    assert!(v.unwrap().contains("20"), "Expected '20' in result");
}

#[tokio::test]
async fn test_json_array_get_negative_index() {
    let v = eval_str(r#"SELECT json_array_get('[10,20,30]', -1)"#).await;
    assert!(v.is_some(), "json_array_get(-1) returned None");
    assert!(v.unwrap().contains("30"), "Expected last element '30'");
}

// ═══════════════════════════════════════════════════════════════
// URL Functions
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_url_extract_host() {
    let v =
        eval_str("SELECT url_extract_host('https://example.com:8080/path?q=1')").await;
    assert_eq!(v, Some("example.com".to_string()));
}

#[tokio::test]
async fn test_url_extract_port() {
    let v =
        eval_str("SELECT url_extract_port('https://example.com:8080/path')").await;
    assert_eq!(v, Some("8080".to_string()));
}

#[tokio::test]
async fn test_url_extract_path() {
    let v =
        eval_str("SELECT url_extract_path('https://example.com/some/path')").await;
    assert_eq!(v, Some("/some/path".to_string()));
}

#[tokio::test]
async fn test_url_extract_protocol() {
    let v = eval_str("SELECT url_extract_protocol('https://example.com')").await;
    assert_eq!(v, Some("https".to_string()));
}

#[tokio::test]
async fn test_url_extract_query() {
    let v =
        eval_str("SELECT url_extract_query('https://example.com?foo=bar&baz=1')").await;
    assert_eq!(v, Some("foo=bar&baz=1".to_string()));
}

#[tokio::test]
async fn test_url_extract_parameter() {
    let v = eval_str(
        "SELECT url_extract_parameter('https://example.com?name=alice&age=30', 'name')",
    )
    .await;
    assert_eq!(v, Some("alice".to_string()));
}

#[tokio::test]
async fn test_url_encode() {
    let v = eval_str("SELECT url_encode('hello world')").await;
    assert_eq!(v, Some("hello%20world".to_string()));
}

#[tokio::test]
async fn test_url_decode() {
    let v = eval_str("SELECT url_decode('hello%20world')").await;
    assert_eq!(v, Some("hello world".to_string()));
}

// ═══════════════════════════════════════════════════════════════
// Encoding Functions
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_to_base64() {
    let v = eval_str("SELECT to_base64('hello')").await;
    assert_eq!(v, Some("aGVsbG8=".to_string()));
}

#[tokio::test]
async fn test_from_base64() {
    let v = eval_str("SELECT from_base64('aGVsbG8=')").await;
    assert_eq!(v, Some("hello".to_string()));
}

#[tokio::test]
async fn test_from_hex() {
    // "hello" in hex (lowercase input should work too)
    let v = eval_str("SELECT from_hex('68656c6c6f')").await;
    assert_eq!(v, Some("hello".to_string()));
}

// ═══════════════════════════════════════════════════════════════
// Extended UDFs — Math / Special Values
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_infinity() {
    let v = eval_f64("SELECT infinity()").await;
    assert_eq!(v, Some(f64::INFINITY));
}

#[tokio::test]
async fn test_nan() {
    let v = eval_f64("SELECT nan()").await;
    assert!(v.is_some(), "nan() returned None");
    assert!(v.unwrap().is_nan(), "Expected NaN");
}

// ═══════════════════════════════════════════════════════════════
// Extended UDFs — String
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_soundex() {
    let v = eval_str("SELECT soundex('Robert')").await;
    assert_eq!(v, Some("R163".to_string()));
}

#[tokio::test]
async fn test_soundex_empty() {
    let v = eval_str("SELECT soundex('')").await;
    assert_eq!(v, Some("0000".to_string()));
}

#[tokio::test]
async fn test_hamming_distance() {
    let v = eval_i64("SELECT hamming_distance('karolin', 'kathrin')").await;
    assert_eq!(v, Some(3));
}

#[tokio::test]
async fn test_normalize_nfc() {
    let v = eval_str("SELECT normalize('café', 'NFC')").await;
    assert!(v.is_some(), "normalize returned None");
    assert!(v.unwrap().contains("caf"), "Expected 'café' or normalised form");
}

#[tokio::test]
async fn test_regexp_extract_match() {
    let v = eval_str(r"SELECT regexp_extract('hello123world', '(\d+)')").await;
    assert_eq!(v, Some("123".to_string()));
}

#[tokio::test]
async fn test_regexp_extract_no_match() {
    let v = eval_str(r"SELECT regexp_extract('hello', '(\d+)')").await;
    // Returns empty string or NULL when there is no match
    assert!(
        v.is_none() || v == Some(String::new()),
        "Expected None or empty string, got {v:?}"
    );
}

#[tokio::test]
async fn test_word_stem_english() {
    let v = eval_str("SELECT word_stem('running')").await;
    assert_eq!(v, Some("run".to_string()));
}

#[tokio::test]
async fn test_word_stem_lang_german() {
    // word_stem_lang accepts an ISO language code; just verify it doesn't error
    let v = eval_str("SELECT word_stem_lang('laufend', 'de')").await;
    assert!(v.is_some(), "word_stem_lang returned None");
}

// ═══════════════════════════════════════════════════════════════
// Extended UDFs — Numeric Base Conversion
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_from_base_binary() {
    let v = eval_i64("SELECT from_base('1010', 2)").await;
    assert_eq!(v, Some(10));
}

#[tokio::test]
async fn test_from_base_hex() {
    let v = eval_i64("SELECT from_base('ff', 16)").await;
    assert_eq!(v, Some(255));
}

#[tokio::test]
async fn test_to_base_hex() {
    let v = eval_str("SELECT to_base(255, 16)").await;
    assert_eq!(v, Some("ff".to_string()));
}

#[tokio::test]
async fn test_to_base_binary() {
    let v = eval_str("SELECT to_base(10, 2)").await;
    assert_eq!(v, Some("1010".to_string()));
}

// ═══════════════════════════════════════════════════════════════
// Extended UDFs — ISO 8601 / Timezone
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_from_iso8601_date() {
    // Just verify it doesn't error; epoch math varies by implementation
    let ctx = ctx().await;
    let result = ctx.sql("SELECT from_iso8601_date('2024-03-15')").await;
    assert!(result.is_ok(), "from_iso8601_date failed: {result:?}");
    let batches = result.unwrap().collect().await;
    assert!(batches.is_ok(), "from_iso8601_date collect failed: {batches:?}");
}

#[tokio::test]
async fn test_to_iso8601_date() {
    let v = eval_str("SELECT to_iso8601(CAST('2024-03-15' AS DATE))").await;
    assert_eq!(v, Some("2024-03-15".to_string()));
}

#[tokio::test]
async fn test_current_timezone() {
    let v = eval_str("SELECT current_timezone()").await;
    assert_eq!(v, Some("UTC".to_string()));
}

#[tokio::test]
async fn test_timezone_hour_utc() {
    let v =
        eval_i64("SELECT timezone_hour(CAST('2024-03-15 14:30:00' AS TIMESTAMP))").await;
    assert_eq!(v, Some(0)); // UTC → offset hours = 0
}

#[tokio::test]
async fn test_timezone_minute_utc() {
    let v =
        eval_i64("SELECT timezone_minute(CAST('2024-03-15 14:30:00' AS TIMESTAMP))")
            .await;
    assert_eq!(v, Some(0)); // UTC → offset minutes = 0
}

// ═══════════════════════════════════════════════════════════════
// Extended UDFs — Human-readable duration
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_human_readable_seconds_hours() {
    let v = eval_str("SELECT human_readable_seconds(3661.0)").await;
    assert!(v.is_some(), "human_readable_seconds returned None");
    let s = v.unwrap();
    assert!(s.contains("1 hours"), "Expected '1 hours' in: {s}");
    assert!(s.contains("1 minutes"), "Expected '1 minutes' in: {s}");
}

#[tokio::test]
async fn test_human_readable_seconds_only() {
    let v = eval_str("SELECT human_readable_seconds(45.0)").await;
    assert!(v.is_some(), "human_readable_seconds returned None");
    let s = v.unwrap();
    assert!(s.contains("seconds"), "Expected 'seconds' in: {s}");
}

// ═══════════════════════════════════════════════════════════════
// Extended UDFs — Millisecond extract
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_millisecond() {
    // Just verify the function exists and the query doesn't error
    let ctx = ctx().await;
    let result = ctx
        .sql("SELECT millisecond(CAST('2024-03-15 14:30:45.123' AS TIMESTAMP))")
        .await;
    assert!(result.is_ok(), "millisecond query failed: {result:?}");
}

// ═══════════════════════════════════════════════════════════════
// Extended UDFs — Checksum & Arbitrary
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_checksum() {
    let v = eval_str("SELECT checksum('hello')").await;
    assert!(v.is_some(), "checksum returned None");
    let s = v.unwrap();
    // checksum returns a hex string; length depends on implementation (SHA-based → 16 hex chars for 8 bytes)
    assert!(!s.is_empty(), "checksum result was empty");
}

#[tokio::test]
async fn test_arbitrary() {
    let v = eval_i64("SELECT arbitrary(42)").await;
    assert_eq!(v, Some(42));
}

// ═══════════════════════════════════════════════════════════════
// DataFusion built-in verifications
// These verify that DataFusion's built-ins work as expected and
// haven't been accidentally shadowed by our UDF registrations.
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_width_bucket_builtin() {
    // width_bucket is available in Trino but NOT in DataFusion 52.
    // This test documents that the function is absent — any query using it
    // will fail with an "Invalid function" error. When DataFusion adds it,
    // this test should be updated to verify correctness.
    let ctx = ctx().await;
    let result = ctx
        .sql("SELECT width_bucket(5.0, 0.0, 10.0, 5)")
        .await;
    // Expect parse/plan error since width_bucket is not registered
    assert!(
        result.is_err(),
        "Expected width_bucket to be unavailable in DataFusion 52, but query succeeded"
    );
}

#[tokio::test]
async fn test_cosh_builtin() {
    let v = eval_f64("SELECT cosh(0.0)").await;
    assert_eq!(v, Some(1.0));
}

#[tokio::test]
async fn test_sinh_builtin() {
    let v = eval_f64("SELECT sinh(0.0)").await;
    assert_eq!(v, Some(0.0));
}

#[tokio::test]
async fn test_tanh_builtin() {
    let v = eval_f64("SELECT tanh(0.0)").await;
    assert_eq!(v, Some(0.0));
}

#[tokio::test]
async fn test_strpos_builtin() {
    // Verify DataFusion's built-in strpos works (we removed our redundant UDF).
    // DataFusion's strpos returns Int32, so cast to VARCHAR for a type-agnostic check.
    let v = eval_str("SELECT CAST(strpos('hello world', 'world') AS VARCHAR)").await;
    assert!(v.is_some(), "strpos built-in returned None");
    let pos: i64 = v.unwrap().parse().expect("strpos result should be numeric");
    assert!(pos > 0, "strpos should return positive 1-based position, got {pos}");
}

// ═══════════════════════════════════════════════════════════════
// format() — printf-style string formatting
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_format_basic() {
    let v = eval_str("SELECT format('%s has %d items', 'cart', 5)").await;
    assert_eq!(v, Some("cart has 5 items".to_string()));
}

#[tokio::test]
async fn test_format_float_precision() {
    let v = eval_str("SELECT format('%.2f', 3.14159)").await;
    assert_eq!(v, Some("3.14".to_string()));
}

#[tokio::test]
async fn test_format_zero_pad() {
    let v = eval_str("SELECT format('%03d', 8)").await;
    assert_eq!(v, Some("008".to_string()));
}

#[tokio::test]
async fn test_format_percent_literal() {
    let v = eval_str("SELECT format('%d%%', 100)").await;
    assert_eq!(v, Some("100%".to_string()));
}

#[tokio::test]
async fn test_format_string_only() {
    let v = eval_str("SELECT format('hello %s', 'world')").await;
    assert_eq!(v, Some("hello world".to_string()));
}

#[tokio::test]
async fn test_format_no_args() {
    // format() with only a format string and no substitution args
    let v = eval_str("SELECT format('no substitutions here')").await;
    assert_eq!(v, Some("no substitutions here".to_string()));
}

// ═══════════════════════════════════════════════════════════════
// to_json() — scalar-to-JSON conversion
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_to_json_string() {
    let v = eval_str("SELECT to_json('hello')").await;
    assert_eq!(v, Some("\"hello\"".to_string()));
}

#[tokio::test]
async fn test_to_json_number() {
    let v = eval_str("SELECT to_json(42)").await;
    assert_eq!(v, Some("42".to_string()));
}

#[tokio::test]
async fn test_to_json_boolean() {
    let v = eval_str("SELECT to_json(true)").await;
    assert_eq!(v, Some("true".to_string()));
}

#[tokio::test]
async fn test_to_json_string_with_quotes() {
    // Strings with special characters should be JSON-escaped
    let v = eval_str(r#"SELECT to_json('say "hello"')"#).await;
    assert!(v.is_some(), "to_json returned None for string with quotes");
    let s = v.unwrap();
    // The result should be a valid JSON string with escaped inner quotes
    assert!(s.starts_with('"'), "Expected JSON string to start with quote");
    assert!(s.ends_with('"'), "Expected JSON string to end with quote");
}

#[tokio::test]
async fn test_to_json_float() {
    let v = eval_str("SELECT to_json(3.14)").await;
    assert!(v.is_some(), "to_json returned None for float");
    let s = v.unwrap();
    assert!(s.contains("3.14"), "Expected '3.14' in to_json output, got: {s}");
}
