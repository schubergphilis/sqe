//! Trino-compatible function aliases for DataFusion.
//!
//! DataFusion uses `extract(YEAR FROM d)` / `date_part('year', d)` while
//! Trino provides standalone functions like `year(d)`, `month(d)`, etc.
//! These UDFs bridge the gap so Trino SQL and dbt models work unmodified.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Date32Array, Float64Array, TimestampMicrosecondArray, TimestampNanosecondArray};
use arrow::datatypes::DataType;
use arrow::temporal_conversions;
use chrono::{Datelike, Timelike, NaiveDate};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Register all Trino-compatible function aliases on a SessionContext.
pub fn register_trino_functions(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udf(ScalarUDF::from(ExtractYear));
    ctx.register_udf(ScalarUDF::from(ExtractMonth));
    ctx.register_udf(ScalarUDF::from(ExtractDay));
    ctx.register_udf(ScalarUDF::from(ExtractHour));
    ctx.register_udf(ScalarUDF::from(ExtractMinute));
    ctx.register_udf(ScalarUDF::from(ExtractSecond));
    ctx.register_udf(ScalarUDF::from(DayOfWeek));
    ctx.register_udf(ScalarUDF::from(DayOfYear));
    ctx.register_udf(ScalarUDF::from(Quarter));
    ctx.register_udf(ScalarUDF::from(Week));
}

/// Extract a chrono component from a Date32 or Timestamp array.
fn extract_component(
    arg: &ColumnarValue,
    f_date: fn(NaiveDate) -> f64,
    f_ts: fn(i64) -> f64,
    f_ts_ns: fn(i64) -> f64,
) -> DFResult<ColumnarValue> {
    match arg {
        ColumnarValue::Array(array) => {
            let result: Float64Array = if let Some(date_arr) = array.as_any().downcast_ref::<Date32Array>() {
                date_arr.iter().map(|opt| opt.map(|days| {
                    let date = temporal_conversions::date32_to_datetime(days).unwrap().date();
                    f_date(date)
                })).collect()
            } else if let Some(ts_arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                ts_arr.iter().map(|opt| opt.map(f_ts)).collect()
            } else if let Some(ts_arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                ts_arr.iter().map(|opt| opt.map(f_ts_ns)).collect()
            } else {
                return Err(DataFusionError::Internal(format!(
                    "Expected Date32 or Timestamp, got {:?}", array.data_type()
                )));
            };
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
        ColumnarValue::Scalar(scalar) => {
            use datafusion::common::ScalarValue;
            let val = match scalar {
                ScalarValue::Date32(Some(days)) => {
                    let date = temporal_conversions::date32_to_datetime(*days).unwrap().date();
                    f_date(date)
                }
                ScalarValue::TimestampMicrosecond(Some(us), _) => f_ts(*us),
                ScalarValue::TimestampNanosecond(Some(ns), _) => f_ts_ns(*ns),
                _ => return Err(DataFusionError::Internal(format!(
                    "Expected date or timestamp scalar, got {scalar:?}"
                ))),
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(val))))
        }
    }
}

fn us_to_naive(us: i64) -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp_micros(us)
        .unwrap_or_default()
        .naive_utc()
}

fn ns_to_naive(ns: i64) -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp_nanos(ns).naive_utc()
}

/// Macro to define a Trino date-extract function using direct chrono extraction.
macro_rules! trino_extract_fn {
    ($struct_name:ident, $fn_name:expr, $f_date:expr, $f_us:expr, $f_ns:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct $struct_name;

        impl ScalarUDFImpl for $struct_name {
            fn as_any(&self) -> &dyn std::any::Any { self }

            fn name(&self) -> &str { $fn_name }

            fn signature(&self) -> &Signature {
                static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
                    Signature::any(1, Volatility::Immutable)
                });
                &SIG
            }

            fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
                Ok(DataType::Float64)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                extract_component(&args.args[0], $f_date, $f_us, $f_ns)
            }
        }
    };
}

trino_extract_fn!(ExtractYear, "year",
    |d: NaiveDate| d.year() as f64,
    |us| us_to_naive(us).year() as f64,
    |ns| ns_to_naive(ns).year() as f64
);
trino_extract_fn!(ExtractMonth, "month",
    |d: NaiveDate| d.month() as f64,
    |us| us_to_naive(us).month() as f64,
    |ns| ns_to_naive(ns).month() as f64
);
trino_extract_fn!(ExtractDay, "day",
    |d: NaiveDate| d.day() as f64,
    |us| us_to_naive(us).day() as f64,
    |ns| ns_to_naive(ns).day() as f64
);
trino_extract_fn!(ExtractHour, "hour",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).hour() as f64,
    |ns| ns_to_naive(ns).hour() as f64
);
trino_extract_fn!(ExtractMinute, "minute",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).minute() as f64,
    |ns| ns_to_naive(ns).minute() as f64
);
trino_extract_fn!(ExtractSecond, "second",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).second() as f64,
    |ns| ns_to_naive(ns).second() as f64
);
trino_extract_fn!(DayOfWeek, "day_of_week",
    |d: NaiveDate| d.weekday().num_days_from_sunday() as f64,
    |us| us_to_naive(us).weekday().num_days_from_sunday() as f64,
    |ns| ns_to_naive(ns).weekday().num_days_from_sunday() as f64
);
trino_extract_fn!(DayOfYear, "day_of_year",
    |d: NaiveDate| d.ordinal() as f64,
    |us| us_to_naive(us).ordinal() as f64,
    |ns| ns_to_naive(ns).ordinal() as f64
);
trino_extract_fn!(Quarter, "quarter",
    |d: NaiveDate| ((d.month() - 1) / 3 + 1) as f64,
    |us| { let m = us_to_naive(us).month(); ((m - 1) / 3 + 1) as f64 },
    |ns| { let m = ns_to_naive(ns).month(); ((m - 1) / 3 + 1) as f64 }
);
trino_extract_fn!(Week, "week",
    |d: NaiveDate| d.iso_week().week() as f64,
    |us| us_to_naive(us).iso_week().week() as f64,
    |ns| ns_to_naive(ns).iso_week().week() as f64
);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn run_query(sql: &str) -> f64 {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        col.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    }

    #[tokio::test]
    async fn year_returns_correct_value() {
        assert_eq!(run_query("SELECT year(DATE '2026-03-30')").await, 2026.0);
    }

    #[tokio::test]
    async fn month_returns_correct_value() {
        assert_eq!(run_query("SELECT month(DATE '2026-03-30')").await, 3.0);
    }

    #[tokio::test]
    async fn day_returns_correct_value() {
        assert_eq!(run_query("SELECT day(DATE '2026-03-30')").await, 30.0);
    }

    #[tokio::test]
    async fn day_of_week_monday() {
        // 2026-03-30 is Monday. num_days_from_sunday: Sunday=0, Monday=1
        assert_eq!(run_query("SELECT day_of_week(DATE '2026-03-30')").await, 1.0);
    }

    #[tokio::test]
    async fn quarter_returns_correct_value() {
        assert_eq!(run_query("SELECT quarter(DATE '2026-03-30')").await, 1.0);
        assert_eq!(run_query("SELECT quarter(DATE '2026-06-15')").await, 2.0);
    }

    #[tokio::test]
    async fn day_of_year_returns_correct_value() {
        // 2026-03-30: Jan=31 + Feb=28 + 30 = 89
        assert_eq!(run_query("SELECT day_of_year(DATE '2026-03-30')").await, 89.0);
    }

    #[tokio::test]
    async fn year_works_with_timestamp() {
        assert_eq!(
            run_query("SELECT year(TIMESTAMP '2026-03-30 14:30:00')").await,
            2026.0,
        );
    }

    #[tokio::test]
    async fn hour_works_with_timestamp() {
        assert_eq!(
            run_query("SELECT hour(TIMESTAMP '2026-03-30 14:30:00')").await,
            14.0,
        );
    }

    #[tokio::test]
    async fn week_iso() {
        // 2026-01-05 is Monday of ISO week 2
        assert_eq!(run_query("SELECT week(DATE '2026-01-05')").await, 2.0);
    }
}
