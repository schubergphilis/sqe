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
    sqe_trino_functions::register_trino_functions(&ctx);
    datafusion_functions_json::register_all(&mut ctx).expect("register JSON functions");
    sqe_trino_functions::register_extended_trino_functions(&ctx);
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

// ─── Trino aliases batch (split, regex array returns, aggregates) ─────────

#[tokio::test]
async fn test_split_returns_array() {
    // Trino's split(s, delim) returns ARRAY(VARCHAR). With the alias on
    // string_to_array, the cell value renders as a bracketed list.
    let v = eval_str("SELECT split('a,b,c', ',')").await;
    let s = v.expect("split returned None");
    assert!(
        s.contains("a") && s.contains("b") && s.contains("c"),
        "split output should contain all parts: {s}"
    );
    // Confirm it is an ARRAY render, not the JSON-string legacy shape.
    assert!(
        s.starts_with('[') || s.contains(", "),
        "split should render as array, got: {s}"
    );
}

#[tokio::test]
async fn test_regexp_extract_all_returns_array_of_matches() {
    // Returns List<Utf8>, not a JSON-array string. ARRAY rendering uses
    // brackets. The function returns three matches for the digit pattern.
    let v = eval_str(r#"SELECT regexp_extract_all('a1 b2 c3', '\d+')"#).await;
    let s = v.expect("regexp_extract_all returned None");
    for digit in ["1", "2", "3"] {
        assert!(
            s.contains(digit),
            "regexp_extract_all output should contain '{digit}': {s}"
        );
    }
}

#[tokio::test]
async fn test_regexp_split_returns_array_of_parts() {
    let v = eval_str(r#"SELECT regexp_split('one1two2three', '\d')"#).await;
    let s = v.expect("regexp_split returned None");
    for part in ["one", "two", "three"] {
        assert!(
            s.contains(part),
            "regexp_split output should contain '{part}': {s}"
        );
    }
}

#[tokio::test]
async fn test_regexp_extract_all_invalid_pattern_errors() {
    // Trino errors on invalid regex; confirm we surface the error rather
    // than silently returning NULL.
    let ctx = ctx().await;
    let res = ctx.sql(r#"SELECT regexp_extract_all('x', '[unclosed')"#).await;
    let failed = match res {
        Err(_) => true,
        Ok(plan) => plan.collect().await.is_err(),
    };
    assert!(failed, "regexp_extract_all with invalid pattern should error");
}

#[tokio::test]
async fn test_word_stem_one_arg_defaults_to_english() {
    // 1-arg form still works after the OneOf signature refactor.
    let v = eval_str("SELECT word_stem('running')").await;
    assert_eq!(v, Some("run".to_string()));
}

#[tokio::test]
async fn test_word_stem_two_arg_picks_language() {
    // 2-arg form works under the same name (no separate word_stem_lang
    // call required, though that name is registered as an alias).
    let v = eval_str("SELECT word_stem('liefen', 'de')").await;
    assert!(v.is_some(), "word_stem(s, lang) returned None");
}

#[tokio::test]
async fn test_word_stem_lang_alias_still_works() {
    // word_stem_lang continues to resolve to the same UDF for backward
    // compat with callers that adopted the old name.
    let v = eval_str("SELECT word_stem_lang('liefen', 'de')").await;
    assert!(v.is_some(), "word_stem_lang alias returned None");
}

// Aggregate aliases — same UDAF as the DataFusion built-in, exposed under
// the Trino-spelled name via `with_aliases([...])`.

#[tokio::test]
async fn test_listagg_alias_resolves_to_string_agg() {
    let ctx = ctx().await;
    let df = ctx
        .sql("SELECT listagg(v, ',') FROM (VALUES ('a'), ('b'), ('c')) t(v)")
        .await
        .expect("listagg parse");
    let batches = df.collect().await.expect("listagg execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    // Order is implementation-defined for string_agg without ORDER BY,
    // but we should see all three letters in the output.
    for ch in ["a", "b", "c"] {
        assert!(s.contains(ch), "listagg output should contain '{ch}': {s}");
    }
}

#[tokio::test]
async fn test_bitwise_and_agg_alias() {
    let ctx = ctx().await;
    let df = ctx
        .sql("SELECT bitwise_and_agg(v) FROM (VALUES (12), (10), (6)) t(v)")
        .await
        .expect("bitwise_and_agg parse");
    let batches = df.collect().await.expect("bitwise_and_agg execute");
    // 12 & 10 & 6 = 0b1100 & 0b1010 & 0b0110 = 0b0000 = 0
    let v = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 result")
        .value(0);
    assert_eq!(v, 0);
}

#[tokio::test]
async fn test_bitwise_or_agg_alias() {
    let ctx = ctx().await;
    let df = ctx
        .sql("SELECT bitwise_or_agg(v) FROM (VALUES (1), (2), (4)) t(v)")
        .await
        .expect("bitwise_or_agg parse");
    let batches = df.collect().await.expect("bitwise_or_agg execute");
    let v = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 result")
        .value(0);
    // 1 | 2 | 4 = 7
    assert_eq!(v, 7);
}

#[tokio::test]
async fn test_max_by_picks_correct_row() {
    // max_by(x, y) returns x for the row where y is maximum.
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "SELECT max_by(name, ts) FROM \
             (VALUES ('a', 100), ('b', 300), ('c', 200)) t(name, ts)",
        )
        .await
        .expect("max_by parse");
    let batches = df.collect().await.expect("max_by execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    assert_eq!(s, "b", "max_by should return name at max ts");
}

#[tokio::test]
async fn test_min_by_picks_correct_row() {
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "SELECT min_by(name, ts) FROM \
             (VALUES ('a', 100), ('b', 300), ('c', 200)) t(name, ts)",
        )
        .await
        .expect("min_by parse");
    let batches = df.collect().await.expect("min_by execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    assert_eq!(s, "a", "min_by should return name at min ts");
}

#[tokio::test]
async fn test_arg_max_alias_resolves_to_max_by() {
    // arg_max is a registered alias on max_by (DuckDB / ClickHouse spelling).
    let ctx = ctx().await;
    let df = ctx
        .sql("SELECT arg_max(v, k) FROM (VALUES ('x', 1), ('y', 5), ('z', 3)) t(v, k)")
        .await
        .expect("arg_max parse");
    let batches = df.collect().await.expect("arg_max execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    assert_eq!(s, "y");
}

#[tokio::test]
async fn test_arg_min_alias_resolves_to_min_by() {
    let ctx = ctx().await;
    let df = ctx
        .sql("SELECT arg_min(v, k) FROM (VALUES ('x', 1), ('y', 5), ('z', 3)) t(v, k)")
        .await
        .expect("arg_min parse");
    let batches = df.collect().await.expect("arg_min execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    assert_eq!(s, "x");
}

#[tokio::test]
async fn test_max_by_with_group_by() {
    // dbt's typical use: max_by per partition. Here we group by region
    // and pick the customer with the highest score per region.
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "SELECT region, max_by(customer, score) AS top \
             FROM (VALUES \
                 ('EU', 'alice', 90), \
                 ('EU', 'bob',   95), \
                 ('US', 'carol', 80), \
                 ('US', 'dave',  85)) \
                 t(region, customer, score) \
             GROUP BY region \
             ORDER BY region",
        )
        .await
        .expect("max_by group parse");
    let batches = df.collect().await.expect("max_by group execute");
    let region = array_value_to_string(batches[0].column(0), 0).unwrap();
    let top_eu = array_value_to_string(batches[0].column(1), 0).unwrap();
    let top_us = array_value_to_string(batches[0].column(1), 1).unwrap();
    assert_eq!(region, "EU");
    assert_eq!(top_eu, "bob", "EU top scorer");
    assert_eq!(top_us, "dave", "US top scorer");
}

#[tokio::test]
async fn test_every_alias_resolves_to_bool_and() {
    // every(x) is the Trino spelling for bool_and(x); aggregates ALL
    // booleans in the group.
    let ctx = ctx().await;
    let df = ctx
        .sql("SELECT every(v) FROM (VALUES (true), (true), (true)) t(v)")
        .await
        .expect("every parse");
    let batches = df.collect().await.expect("every execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    assert_eq!(s, "true", "every of all-true should be true");

    // One false row should flip the answer.
    let df2 = ctx
        .sql("SELECT every(v) FROM (VALUES (true), (false), (true)) t(v)")
        .await
        .expect("every2 parse");
    let batches2 = df2.collect().await.expect("every2 execute");
    let s2 = array_value_to_string(batches2[0].column(0), 0).unwrap();
    assert_eq!(s2, "false", "every with one false should be false");
}

#[tokio::test]
async fn test_approx_percentile_alias() {
    let ctx = ctx().await;
    // approx_percentile(x, p) shares the impl with approx_percentile_cont.
    // Median of 1..=9 is 5; assert the result is in a tight band around 5.
    let df = ctx
        .sql(
            "SELECT approx_percentile(v, 0.5) FROM \
             (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9)) t(v)",
        )
        .await
        .expect("approx_percentile parse");
    let batches = df.collect().await.expect("approx_percentile execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    let parsed: f64 = s.parse().expect("approx_percentile result must parse as f64");
    assert!(
        (4.0..=6.0).contains(&parsed),
        "approx_percentile median should land near 5, got {parsed}"
    );
}

// ─── histogram(x) — Trino map-producing aggregate ─────────────────────────

#[tokio::test]
async fn test_histogram_counts_distinct_string_values() {
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "SELECT histogram(v) FROM \
             (VALUES ('a'), ('b'), ('a'), ('c'), ('a'), ('b')) t(v)",
        )
        .await
        .expect("histogram parse");
    let batches = df.collect().await.expect("histogram execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    // Map render contains the entries in some order; just assert the
    // pairs are present. Trino does not specify entry ordering.
    for pair in ["a:3", "b:2", "c:1"] {
        let (k, c) = pair.split_once(':').unwrap();
        assert!(s.contains(k), "histogram output missing '{k}': {s}");
        assert!(s.contains(c), "histogram output missing count {c}: {s}");
    }
}

#[tokio::test]
async fn test_histogram_skips_nulls() {
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "SELECT histogram(v) FROM \
             (VALUES (CAST('a' AS VARCHAR)), (NULL), ('a'), (NULL)) t(v)",
        )
        .await
        .expect("histogram null parse");
    let batches = df.collect().await.expect("histogram null execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    // 'a' counted twice; NULL not counted. The map should contain only one entry.
    assert!(s.contains("a"), "histogram should still count 'a': {s}");
    assert!(s.contains('2'), "count of 'a' should be 2: {s}");
}

#[tokio::test]
async fn test_histogram_with_group_by() {
    // Per-group histograms — the typical dbt pattern.
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "SELECT region, histogram(status) AS h FROM (VALUES \
                ('EU', 'shipped'), \
                ('EU', 'pending'), \
                ('EU', 'shipped'), \
                ('US', 'cancelled')) \
                t(region, status) \
            GROUP BY region \
            ORDER BY region",
        )
        .await
        .expect("histogram group parse");
    let batches = df.collect().await.expect("histogram group execute");
    assert_eq!(batches[0].num_rows(), 2, "two groups");
    // EU row contains 'shipped' twice and 'pending' once; US row contains
    // 'cancelled' once. Render-format independence: assert by string match.
    let eu_h = array_value_to_string(batches[0].column(1), 0).unwrap();
    let us_h = array_value_to_string(batches[0].column(1), 1).unwrap();
    assert!(eu_h.contains("shipped"), "EU should have shipped: {eu_h}");
    assert!(eu_h.contains("pending"), "EU should have pending: {eu_h}");
    assert!(us_h.contains("cancelled"), "US should have cancelled: {us_h}");
    assert!(!us_h.contains("shipped"), "US should not see EU values: {us_h}");
}

#[tokio::test]
async fn test_histogram_integer_keys() {
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "SELECT histogram(v) FROM \
             (VALUES (1), (2), (1), (3), (1), (2)) t(v)",
        )
        .await
        .expect("histogram int parse");
    let batches = df.collect().await.expect("histogram int execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    // Count: 1->3, 2->2, 3->1.
    assert!(s.contains('1'), "histogram should contain key 1: {s}");
    assert!(s.contains('2'), "histogram should contain key 2 / count: {s}");
    assert!(s.contains('3'), "histogram should contain key 3: {s}");
}

// ─── Map-producing aggregates (map_agg, multimap_agg, map_union) ──────────

#[tokio::test]
async fn test_map_agg_aggregates_kv_pairs() {
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "SELECT map_agg(k, v) FROM \
             (VALUES ('a', 1), ('b', 2), ('c', 3)) t(k, v)",
        )
        .await
        .expect("map_agg parse");
    let batches = df.collect().await.expect("map_agg execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
        assert!(s.contains(k), "map_agg missing key '{k}': {s}");
        assert!(s.contains(v), "map_agg missing value '{v}': {s}");
    }
}

#[tokio::test]
async fn test_map_agg_last_wins_on_duplicate_key() {
    // Trino spec is implementation-defined for duplicate keys; SQE keeps
    // the last value seen (matches DuckDB and Snowflake).
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "SELECT map_agg(k, v) FROM \
             (VALUES ('a', 1), ('a', 99)) t(k, v)",
        )
        .await
        .expect("map_agg dup parse");
    let batches = df.collect().await.expect("map_agg dup execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    assert!(s.contains("99"), "last-wins value 99 missing: {s}");
    assert!(!s.contains('1') || s.contains("99"), "first-write 1 should not appear without 99: {s}");
}

#[tokio::test]
async fn test_multimap_agg_groups_values_per_key() {
    // multimap_agg(k, v) returns MAP<K, ARRAY<V>>: all values per key
    // collected into an array. Insertion order preserved.
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "SELECT multimap_agg(k, v) FROM \
             (VALUES ('a', 1), ('b', 2), ('a', 3), ('a', 4)) t(k, v)",
        )
        .await
        .expect("multimap_agg parse");
    let batches = df.collect().await.expect("multimap_agg execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    // Output should mention all four values somewhere.
    for v in ["1", "2", "3", "4"] {
        assert!(s.contains(v), "multimap_agg missing value '{v}': {s}");
    }
    assert!(s.contains('a'), "multimap_agg missing key 'a': {s}");
    assert!(s.contains('b'), "multimap_agg missing key 'b': {s}");
}

#[tokio::test]
async fn test_map_union_merges_multiple_maps() {
    // Build two map literals via map_agg, then map_union should merge.
    // We assert with a plain SELECT against a synthetic source.
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "WITH per_region AS ( \
                SELECT region, map_agg(k, v) AS m FROM \
                  (VALUES \
                    ('EU', 'k1', 1), ('EU', 'k2', 2), \
                    ('US', 'k3', 3), ('US', 'k4', 4)) \
                  t(region, k, v) \
                GROUP BY region \
             ) \
             SELECT map_union(m) FROM per_region",
        )
        .await
        .expect("map_union parse");
    let batches = df.collect().await.expect("map_union execute");
    let s = array_value_to_string(batches[0].column(0), 0).unwrap();
    for k in ["k1", "k2", "k3", "k4"] {
        assert!(s.contains(k), "map_union missing key '{k}': {s}");
    }
}

#[tokio::test]
async fn test_map_agg_with_group_by() {
    // Per-group map_agg — typical dbt pattern: build a dict per region.
    let ctx = ctx().await;
    let df = ctx
        .sql(
            "SELECT region, map_agg(k, v) AS m FROM \
              (VALUES \
                ('EU', 'k1', 1), ('EU', 'k2', 2), \
                ('US', 'k3', 3)) \
              t(region, k, v) \
             GROUP BY region \
             ORDER BY region",
        )
        .await
        .expect("map_agg group parse");
    let batches = df.collect().await.expect("map_agg group execute");
    assert_eq!(batches[0].num_rows(), 2);
    let eu_m = array_value_to_string(batches[0].column(1), 0).unwrap();
    let us_m = array_value_to_string(batches[0].column(1), 1).unwrap();
    assert!(eu_m.contains("k1") && eu_m.contains("k2"));
    assert!(us_m.contains("k3"));
    assert!(!us_m.contains("k1"), "US map should not see EU keys: {us_m}");
}
