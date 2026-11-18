//! Tests for the Trino date and time functions: the value and the Arrow type
//! of every signature, a NULL case, and a column-fed case.

use arrow::array::Array;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::util::display::array_value_to_string;

async fn q(sql: &str) -> (DataType, Vec<String>) {
    let ctx = crate::duckdb_test_ctx();
    crate::register_trino_functions(&ctx);
    crate::register_extended_trino_functions(&ctx);
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let dt = batches[0].column(0).data_type().clone();
    let mut out = Vec::new();
    for b in &batches {
        let c = b.column(0);
        for i in 0..c.len() {
            out.push(if c.is_null(i) {
                "NULL".to_string()
            } else {
                array_value_to_string(c, i).unwrap()
            });
        }
    }
    (dt, out)
}

async fn fails(sql: &str) -> bool {
    let ctx = crate::duckdb_test_ctx();
    crate::register_trino_functions(&ctx);
    crate::register_extended_trino_functions(&ctx);
    match ctx.sql(sql).await {
        Ok(df) => df.collect().await.is_err(),
        Err(_) => true,
    }
}

async fn one(sql: &str) -> String {
    q(sql).await.1.remove(0)
}

fn ts(unit: TimeUnit, tz: Option<&str>) -> DataType {
    DataType::Timestamp(unit, tz.map(Into::into))
}

#[tokio::test]
async fn extractor_aliases_are_bigint() {
    for (f, want) in [
        ("dow", "1"),
        ("day_of_week", "1"),
        ("doy", "15"),
        ("day_of_year", "15"),
        ("day_of_month", "15"),
        ("week_of_year", "3"),
        ("yow", "2024"),
        ("year_of_week", "2024"),
    ] {
        let (t, v) = q(&format!("SELECT {f}(DATE '2024-01-15')")).await;
        assert_eq!(t, DataType::Int64, "{f}");
        assert_eq!(v, [want], "{f}");
    }
    // 2021-01-03 is a Sunday in ISO week 53 of 2020.
    assert_eq!(one("SELECT yow(DATE '2021-01-03')").await, "2020");
    assert_eq!(one("SELECT year_of_week(CAST(NULL AS DATE))").await, "NULL");
}

#[tokio::test]
async fn millisecond_extracts_from_every_timestamp_unit() {
    let (t, v) = q("SELECT millisecond(TIMESTAMP '2024-01-15 10:30:45.123')").await;
    assert_eq!((t, v), (DataType::Int64, vec!["123".to_string()]));
    assert_eq!(
        one("SELECT millisecond(from_unixtime(1700000000.789))").await,
        "789"
    );
    assert_eq!(
        one("SELECT millisecond(CAST(NULL AS TIMESTAMP))").await,
        "NULL"
    );
    let v = q(
        "SELECT millisecond(ts) FROM (VALUES (TIMESTAMP '2024-01-01 00:00:00.001'), \
              (TIMESTAMP '2024-01-01 00:00:00.999'), (CAST(NULL AS TIMESTAMP))) t(ts)",
    )
    .await
    .1;
    assert_eq!(v, ["1", "999", "NULL"]);
}

#[tokio::test]
async fn interval_extractors() {
    let cases = [
        ("day", "INTERVAL '3' DAY", "3"),
        ("hour", "INTERVAL '4' HOUR", "4"),
        ("minute", "INTERVAL '5' MINUTE", "5"),
        ("second", "INTERVAL '6' SECOND", "6"),
        ("year", "INTERVAL '2' YEAR", "2"),
        ("month", "INTERVAL '7' MONTH", "7"),
    ];
    for (f, iv, want) in cases {
        let (t, v) = q(&format!("SELECT {f}({iv})")).await;
        assert_eq!(t, DataType::Int64, "{f}");
        assert_eq!(v, [want], "{f}");
    }
    let (t, v) = q("SELECT to_milliseconds(INTERVAL '1' DAY)").await;
    assert_eq!((t, v), (DataType::Int64, vec!["86400000".to_string()]));
    assert_eq!(
        one("SELECT to_milliseconds(CASE WHEN 1 = 2 THEN INTERVAL '1' DAY END)").await,
        "NULL"
    );
}

#[tokio::test]
async fn timezone_functions_report_the_real_offset() {
    assert_eq!(
        one("SELECT timezone_hour(from_unixtime(0, '+05:30'))").await,
        "5"
    );
    assert_eq!(
        one("SELECT timezone_minute(from_unixtime(0, '+05:30'))").await,
        "30"
    );
    assert_eq!(
        one("SELECT timezone_hour(from_unixtime(0, '-03:30'))").await,
        "-3"
    );
    assert_eq!(
        one("SELECT timezone_minute(from_unixtime(0, '-03:30'))").await,
        "-30"
    );
    assert_eq!(one("SELECT timezone_hour(from_unixtime(0))").await, "0");
    let (t, v) = q("SELECT timezone(from_unixtime(0, '+05:30'))").await;
    assert_eq!((t, v), (DataType::Utf8, vec!["+05:30".to_string()]));
    assert_eq!(
        one("SELECT timezone(from_unixtime(0, 'Europe/Amsterdam'))").await,
        "Europe/Amsterdam"
    );
    assert_eq!(
        one("SELECT timezone_hour(CAST(NULL AS TIMESTAMP))").await,
        "NULL"
    );
}

#[tokio::test]
async fn from_unixtime_types_and_values() {
    let (t, v) = q("SELECT from_unixtime(1700000000.5)").await;
    assert_eq!(t, ts(TimeUnit::Millisecond, Some("UTC")));
    assert_eq!(v, ["2023-11-14T22:13:20.500Z"]);

    // A named zone keeps the instant and changes the wall clock.
    let (t, v) = q("SELECT from_unixtime(1700000000, 'Europe/Amsterdam')").await;
    assert_eq!(t, ts(TimeUnit::Millisecond, Some("Europe/Amsterdam")));
    assert_eq!(v, ["2023-11-14T23:13:20+01:00"]);

    // Hours and minutes form.
    let (t, v) = q("SELECT from_unixtime(1700000000, 5, 30)").await;
    assert_eq!(t, ts(TimeUnit::Millisecond, Some("+05:30")));
    assert_eq!(v, ["2023-11-15T03:43:20+05:30"]);

    assert_eq!(
        one("SELECT from_unixtime(CAST(NULL AS DOUBLE))").await,
        "NULL"
    );
    let v = q(
        "SELECT from_unixtime(x) FROM (VALUES (CAST(0.0 AS DOUBLE)), (1.5), \
              (CAST(NULL AS DOUBLE))) t(x)",
    )
    .await
    .1;
    assert_eq!(
        v,
        ["1970-01-01T00:00:00Z", "1970-01-01T00:00:01.500Z", "NULL"]
    );
}

#[tokio::test]
async fn from_unixtime_nanos_is_timestamp_ns_with_zone() {
    let (t, v) = q("SELECT from_unixtime_nanos(1700000000123456789)").await;
    assert_eq!(t, ts(TimeUnit::Nanosecond, Some("UTC")));
    assert_eq!(v, ["2023-11-14T22:13:20.123456789Z"]);
    assert_eq!(
        one("SELECT from_unixtime_nanos(CAST(NULL AS BIGINT))").await,
        "NULL"
    );
    let v = q(
        "SELECT from_unixtime_nanos(x) FROM (VALUES (CAST(0 AS BIGINT)), (1), \
              (CAST(NULL AS BIGINT))) t(x)",
    )
    .await
    .1;
    assert_eq!(
        v,
        [
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00.000000001Z",
            "NULL"
        ]
    );
}

#[tokio::test]
async fn to_unixtime_is_double() {
    let (t, v) = q("SELECT to_unixtime(TIMESTAMP '1970-01-01 00:00:01.500')").await;
    assert_eq!(t, DataType::Float64);
    assert_eq!(v, ["1.5"]);
    assert_eq!(
        one("SELECT to_unixtime(from_unixtime(1700000000.5))").await,
        "1700000000.5"
    );
    assert_eq!(
        one("SELECT to_unixtime(CAST(NULL AS TIMESTAMP))").await,
        "NULL"
    );
}

#[tokio::test]
async fn from_iso8601_timestamp_types_and_values() {
    let (t, v) = q("SELECT from_iso8601_timestamp('2024-03-15T15:30:45.123+01:00')").await;
    assert_eq!(t, ts(TimeUnit::Millisecond, Some("+01:00")));
    assert_eq!(v, ["2024-03-15T15:30:45.123+01:00"]);

    let (t, v) = q("SELECT from_iso8601_timestamp('2024-03-15T15:30:45Z')").await;
    assert_eq!(t, ts(TimeUnit::Millisecond, Some("UTC")));
    assert_eq!(v, ["2024-03-15T15:30:45Z"]);

    // No designator: the session zone applies, and a bare date is midnight.
    assert_eq!(
        one("SELECT from_iso8601_timestamp('2024-03-15')").await,
        "2024-03-15T00:00:00Z"
    );
    assert_eq!(
        one("SELECT from_iso8601_timestamp(CAST(NULL AS VARCHAR))").await,
        "NULL"
    );
    assert!(fails("SELECT from_iso8601_timestamp('not a timestamp')").await);

    let v = q(
        "SELECT from_iso8601_timestamp(s) FROM (VALUES ('2024-01-01T00:00:00Z'), \
              ('2024-01-01T01:00:00+01:00'), (CAST(NULL AS VARCHAR))) t(s)",
    )
    .await
    .1;
    assert_eq!(v.len(), 3);
    assert_eq!(v[2], "NULL");
}

#[tokio::test]
async fn from_iso8601_timestamp_nanos_keeps_nine_digits() {
    let (t, v) = q("SELECT from_iso8601_timestamp_nanos('2024-03-15T15:30:45.123456789Z')").await;
    assert_eq!(t, ts(TimeUnit::Nanosecond, Some("UTC")));
    assert_eq!(v, ["2024-03-15T15:30:45.123456789Z"]);
    assert_eq!(
        one("SELECT from_iso8601_timestamp_nanos(CAST(NULL AS VARCHAR))").await,
        "NULL"
    );
}

#[tokio::test]
async fn from_iso8601_date_is_date() {
    let (t, v) = q("SELECT from_iso8601_date('2024-03-15')").await;
    assert_eq!((t, v), (DataType::Date32, vec!["2024-03-15".to_string()]));
    assert_eq!(
        one("SELECT from_iso8601_date(CAST(NULL AS VARCHAR))").await,
        "NULL"
    );
}

#[tokio::test]
async fn to_iso8601_by_input_type() {
    let (t, v) = q("SELECT to_iso8601(DATE '2024-03-15')").await;
    assert_eq!((t, v), (DataType::Utf8, vec!["2024-03-15".to_string()]));
    // A naive timestamp has no designator.
    assert_eq!(
        one("SELECT to_iso8601(TIMESTAMP '2024-03-15 15:30:45')").await,
        "2024-03-15T15:30:45"
    );
    // UTC renders Z, any other zone its offset.
    assert_eq!(
        one("SELECT to_iso8601(from_unixtime(1700000000.123))").await,
        "2023-11-14T22:13:20.123Z"
    );
    assert_eq!(
        one("SELECT to_iso8601(from_unixtime(1700000000.123, 'Europe/Amsterdam'))").await,
        "2023-11-14T23:13:20.123+01:00"
    );
    assert_eq!(one("SELECT to_iso8601(CAST(NULL AS DATE))").await, "NULL");
    assert_eq!(
        one("SELECT to_iso8601(CAST(NULL AS TIMESTAMP))").await,
        "NULL"
    );
    let v = q(
        "SELECT to_iso8601(d) FROM (VALUES (DATE '2024-01-01'), (CAST(NULL AS DATE)), \
              (DATE '1999-12-31')) t(d)",
    )
    .await
    .1;
    assert_eq!(v, ["2024-01-01", "NULL", "1999-12-31"]);
}

#[tokio::test]
async fn date_function_forms() {
    let (t, v) = q("SELECT date('2024-03-15')").await;
    assert_eq!((t, v), (DataType::Date32, vec!["2024-03-15".to_string()]));
    let (t, v) = q("SELECT date(TIMESTAMP '2024-03-15 23:59:59')").await;
    assert_eq!((t, v), (DataType::Date32, vec!["2024-03-15".to_string()]));
    assert_eq!(one("SELECT date(DATE '2024-03-15')").await, "2024-03-15");
    assert_eq!(one("SELECT date(CAST(NULL AS VARCHAR))").await, "NULL");
    assert!(fails("SELECT date('15/03/2024')").await);
    let v = q("SELECT date(s) FROM (VALUES ('2024-01-31'), (CAST(NULL AS VARCHAR))) t(s)")
        .await
        .1;
    assert_eq!(v, ["2024-01-31", "NULL"]);
}

#[tokio::test]
async fn date_trunc_keeps_the_input_type() {
    let (t, v) = q("SELECT date_trunc('month', DATE '2024-03-15')").await;
    assert_eq!((t, v), (DataType::Date32, vec!["2024-03-01".to_string()]));
    assert_eq!(
        one("SELECT date_trunc('week', DATE '2024-03-15')").await,
        "2024-03-11"
    );
    assert_eq!(
        one("SELECT date_trunc('year', DATE '2024-03-15')").await,
        "2024-01-01"
    );
    let (t, v) = q("SELECT date_trunc('hour', TIMESTAMP '2024-03-15 15:30:45.123')").await;
    assert_eq!(t, ts(TimeUnit::Nanosecond, None));
    assert_eq!(v, ["2024-03-15T15:00:00"]);
    let (t, v) = q("SELECT date_trunc('day', from_unixtime(1700000000, 'Europe/Amsterdam'))").await;
    assert_eq!(t, ts(TimeUnit::Millisecond, Some("Europe/Amsterdam")));
    assert_eq!(v, ["2023-11-14T00:00:00+01:00"]);
    // A varchar is still coerced to a timestamp.
    let (t, v) = q("SELECT date_trunc('month', '2024-03-15 10:20:30')").await;
    assert_eq!(t, ts(TimeUnit::Microsecond, None));
    assert_eq!(v, ["2024-03-01T00:00:00"]);
    assert_eq!(
        one("SELECT date_trunc('month', CAST(NULL AS DATE))").await,
        "NULL"
    );
    let v = q(
        "SELECT date_trunc('month', d) FROM (VALUES (DATE '2024-02-29'), \
              (CAST(NULL AS DATE))) t(d)",
    )
    .await
    .1;
    assert_eq!(v, ["2024-02-01", "NULL"]);
}

#[tokio::test]
async fn format_datetime_walks_joda_patterns() {
    let (t, v) = q(
        "SELECT format_datetime(TIMESTAMP '2024-03-05 15:04:09.123', 'yyyy-MM-dd HH:mm:ss.SSS')",
    )
    .await;
    assert_eq!(t, DataType::Utf8);
    assert_eq!(v, ["2024-03-05 15:04:09.123"]);
    assert_eq!(
        one("SELECT format_datetime(TIMESTAMP '2024-03-05 15:04:09', 'EEEE, d MMMM yyyy')").await,
        "Tuesday, 5 March 2024"
    );
    assert_eq!(
        one("SELECT format_datetime(TIMESTAMP '2024-03-05 15:04:09', 'h:mm a')").await,
        "3:04 PM"
    );
    // Quoted text is literal.
    assert_eq!(
        one("SELECT format_datetime(TIMESTAMP '2024-03-05 15:04:09', 'yyyy''T''HH')").await,
        "2024T15"
    );
    assert_eq!(
        one(
            "SELECT format_datetime(from_unixtime(1700000000, 'Europe/Amsterdam'), \
             'yyyy-MM-dd HH:mm ZZ')"
        )
        .await,
        "2023-11-14 23:13 +01:00"
    );
    assert_eq!(
        one("SELECT format_datetime(CAST(NULL AS TIMESTAMP), 'yyyy')").await,
        "NULL"
    );
    assert!(
        fails("SELECT format_datetime(TIMESTAMP '2024-01-15 10:30:45', 'yyyy %q')").await
            || one("SELECT format_datetime(TIMESTAMP '2024-01-15 10:30:45', 'yyyy %')").await
                == "2024 %"
    );
    assert!(fails("SELECT format_datetime(TIMESTAMP '2024-01-15 10:30:45', 'yyyy qq')").await);
    let v = q(
        "SELECT format_datetime(ts, 'yyyy') FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00'), \
              (CAST(NULL AS TIMESTAMP))) t(ts)",
    )
    .await
    .1;
    assert_eq!(v, ["2020", "NULL"]);
}

#[tokio::test]
async fn parse_datetime_is_timestamp_ms_with_zone() {
    let (t, v) = q("SELECT parse_datetime('2024-03-05 15:04:09', 'yyyy-MM-dd HH:mm:ss')").await;
    assert_eq!(t, ts(TimeUnit::Millisecond, Some("UTC")));
    assert_eq!(v, ["2024-03-05T15:04:09Z"]);
    // A date-only pattern gives midnight.
    assert_eq!(
        one("SELECT parse_datetime('2020-01-02', 'yyyy-MM-dd')").await,
        "2020-01-02T00:00:00Z"
    );
    assert_eq!(
        one("SELECT parse_datetime(CAST(NULL AS VARCHAR), 'yyyy')").await,
        "NULL"
    );
    assert_eq!(
        one("SELECT parse_datetime('2024-03-05 15:04:09 +01:00', 'yyyy-MM-dd HH:mm:ss ZZ')").await,
        "2024-03-05T14:04:09Z"
    );
    let v = q(
        "SELECT parse_datetime(s, 'yyyy-MM-dd') FROM (VALUES ('2020-01-02'), \
              (CAST(NULL AS VARCHAR))) t(s)",
    )
    .await
    .1;
    assert_eq!(v, ["2020-01-02T00:00:00Z", "NULL"]);
}

#[tokio::test]
async fn date_parse_and_date_format_use_the_mysql_table() {
    let (t, v) = q("SELECT date_parse('2024-03-05 15:04:09', '%Y-%m-%d %H:%i:%s')").await;
    assert_eq!(t, ts(TimeUnit::Millisecond, None));
    assert_eq!(v, ["2024-03-05T15:04:09"]);
    assert_eq!(
        one("SELECT date_parse('202403', '%Y%m')").await,
        "2024-03-01T00:00:00"
    );
    assert_eq!(
        one("SELECT date_parse('Mar 5 2024 3:04 PM', '%b %e %Y %l:%i %p')").await,
        "2024-03-05T15:04:00"
    );
    assert_eq!(
        one("SELECT date_parse('2024-03-05 15:04:09.5', '%Y-%m-%d %H:%i:%s.%f')").await,
        "2024-03-05T15:04:09.500"
    );
    assert_eq!(
        one("SELECT date_parse(CAST(NULL AS VARCHAR), '%Y')").await,
        "NULL"
    );
    assert!(fails("SELECT date_parse('garbage', '%Y-%m-%d')").await);
    // The array path errors on a failure too.
    assert!(
        fails("SELECT date_parse(s, '%Y-%m-%d') FROM (VALUES ('2024-01-01'), ('x')) t(s)").await
    );

    let (t, v) =
        q("SELECT date_format(TIMESTAMP '2024-03-05 15:04:09', '%a %b %e %Y %H:%i:%s %p')").await;
    assert_eq!(t, DataType::Utf8);
    assert_eq!(v, ["Tue Mar 5 2024 15:04:09 PM"]);
    assert_eq!(
        one("SELECT date_format(DATE '2024-03-05', '%W %M %D %j')").await,
        "Tuesday March 5th 065"
    );
    assert_eq!(
        one("SELECT date_format(CAST(NULL AS TIMESTAMP), '%Y')").await,
        "NULL"
    );
    assert!(fails("SELECT date_format(TIMESTAMP '2024-01-15 10:30:45', '%U')").await);
    let v = q(
        "SELECT date_format(ts, '%Y-%m') FROM (VALUES (TIMESTAMP '2020-01-01 00:00:00'), \
              (CAST(NULL AS TIMESTAMP))) t(ts)",
    )
    .await
    .1;
    assert_eq!(v, ["2020-01", "NULL"]);
}

#[tokio::test]
async fn parse_duration_is_interval_day_to_second() {
    let (t, v) = q("SELECT parse_duration('3.5m')").await;
    assert_eq!(
        t,
        DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
    );
    assert_eq!(v.len(), 1);
    assert_eq!(
        one("SELECT to_milliseconds(parse_duration('3.5m'))").await,
        "210000"
    );
    assert_eq!(
        one("SELECT to_milliseconds(parse_duration('2d'))").await,
        "172800000"
    );
    assert_eq!(
        one("SELECT to_milliseconds(parse_duration('150ms'))").await,
        "150"
    );
    assert_eq!(
        one("SELECT parse_duration(CAST(NULL AS VARCHAR)) IS NULL").await,
        "true"
    );
    assert!(fails("SELECT parse_duration('5 parsecs')").await);
}

#[tokio::test]
async fn sequence_over_dates_and_timestamps() {
    // Two dates: an implicit one-day step, ascending or descending.
    let (t, v) = q("SELECT sequence(DATE '2024-01-30', DATE '2024-02-02')").await;
    assert!(matches!(t, DataType::List(_)), "{t:?}");
    assert_eq!(v, ["[2024-01-30, 2024-01-31, 2024-02-01, 2024-02-02]"]);
    assert_eq!(
        one("SELECT sequence(DATE '2024-01-03', DATE '2024-01-01')").await,
        "[2024-01-03, 2024-01-02, 2024-01-01]"
    );
    assert_eq!(
        one("SELECT sequence(DATE '2024-01-31', DATE '2024-04-30', INTERVAL '1' MONTH)").await,
        one("SELECT generate_series(DATE '2024-01-31', DATE '2024-04-30', INTERVAL '1' MONTH)")
            .await
    );
    assert_eq!(
        one(
            "SELECT sequence(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 02:00:00', \
             INTERVAL '1' HOUR)"
        )
        .await,
        "[2024-01-01T00:00:00, 2024-01-01T01:00:00, 2024-01-01T02:00:00]"
    );
    assert_eq!(one("SELECT sequence(1, 3)").await, "[1, 2, 3]");
    assert_eq!(
        one("SELECT sequence(CAST(NULL AS DATE), DATE '2024-01-01') IS NULL").await,
        "true"
    );
    // One row per input row.
    let v = q(
        "SELECT sequence(a, b) FROM (VALUES (DATE '2024-01-01', DATE '2024-01-02'), \
              (DATE '2024-01-05', DATE '2024-01-03')) t(a, b)",
    )
    .await
    .1;
    assert_eq!(
        v,
        [
            "[2024-01-01, 2024-01-02]",
            "[2024-01-05, 2024-01-04, 2024-01-03]"
        ]
    );
}

#[tokio::test]
async fn sequence_keeps_the_size_cap() {
    // 100M elements is the cap; a day step over ~300k years is far past it.
    assert!(
        fails("SELECT sequence(DATE '0001-01-01', DATE '9999-12-31', INTERVAL '1' SECOND)").await
    );
    assert!(
        fails(
            "SELECT sequence(TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2999-12-31 00:00:00', \
               INTERVAL '1' SECOND)"
        )
        .await
    );
}

#[tokio::test]
async fn date_add_on_second_and_millisecond_timestamps_keeps_the_type() {
    let (t, v) = q("SELECT date_add('hour', 1, from_unixtime(0))").await;
    assert_eq!(t, ts(TimeUnit::Millisecond, Some("UTC")));
    assert_eq!(v, ["1970-01-01T01:00:00Z"]);
    assert_eq!(one("SELECT year(from_unixtime(0))").await, "1970");
}
