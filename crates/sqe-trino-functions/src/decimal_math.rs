//! `round` over DECIMAL with Trino 484's result types.
//!
//! | call | Trino result |
//! |---|---|
//! | `round(decimal(p,s))` | `decimal(min(38, p - s + min(1, s)), 0)` |
//! | `round(decimal(p,s), d)` | `decimal(min(38, p + 1), s)` |
//!
//! Rounding is half away from zero. DataFusion's `round` returns a narrower
//! or differently scaled type. Any other argument type is passed to
//! DataFusion's `round`. (`truncate` over decimals is the math-functions
//! change's.)

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, Int64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use datafusion::prelude::SessionContext;

/// Register decimal-aware `round`, replacing DataFusion's.
pub fn register_decimal_math(ctx: &SessionContext) {
    // Shadow DataFusion's `round`: Trino's decimal result types.
    let round = datafusion::functions::math::round();
    let signature = round.signature().clone();
    ctx.register_udf(ScalarUDF::new_from_impl(DecimalRound {
        inner: round,
        signature,
    }));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DecimalRound {
    inner: Arc<ScalarUDF>,
    signature: Signature,
}

/// `(precision, scale)` of a result, per the table above.
fn result_type(p: u8, s: i8, has_d: bool) -> (u8, i8) {
    let (p, s) = (i32::from(p), i32::from(s));
    let (rp, rs) = if has_d {
        (38.min(p + 1), s)
    } else {
        (38.min(p - s + 1.min(s)), 0)
    };
    (rp as u8, rs as i8)
}

fn pow10(k: u32) -> Option<i128> {
    10i128.checked_pow(k)
}

/// `v` (at scale `s`) rounded half away from zero to `d` places, as an
/// integer at scale `s` (so `d >= s` leaves it alone).
fn apply(v: i128, s: i8, d: i64) -> Option<i128> {
    let k = i64::from(s) - d;
    if k <= 0 {
        return Some(v);
    }
    let Some(unit) = u32::try_from(k).ok().and_then(pow10) else {
        // More than 38 digits dropped: only zero is left.
        return Some(0);
    };
    let (q, r) = (v / unit, v % unit);
    let q = if r.unsigned_abs().checked_mul(2)? >= unit.unsigned_abs() {
        q + v.signum()
    } else {
        q
    };
    q.checked_mul(unit)
}

impl DecimalRound {
    fn decimal_args(arg_fields: &[FieldRef]) -> Option<(u8, i8)> {
        match arg_fields.first()?.data_type() {
            DataType::Decimal128(p, s) => Some((*p, *s)),
            _ => None,
        }
    }

    fn compute(&self, args: &ScalarFunctionArgs, p: u8, s: i8) -> Result<ColumnarValue> {
        let all_scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let has_d = arrays.len() == 2;
        let x = arrays[0].as_primitive::<Decimal128Type>();
        let d: Option<Int64Array> = match arrays.get(1) {
            Some(d) => Some(
                cast(d, &DataType::Int64)?
                    .as_primitive::<Int64Type>()
                    .clone(),
            ),
            None => None,
        };
        let (rp, rs) = result_type(p, s, has_d);
        let limit = pow10(u32::from(rp));
        let mut out: Vec<Option<i128>> = Vec::with_capacity(x.len());
        for i in 0..x.len() {
            let dv = match &d {
                Some(d) if d.is_null(i) => {
                    out.push(None);
                    continue;
                }
                Some(d) => d.value(i),
                None => 0,
            };
            if x.is_null(i) {
                out.push(None);
                continue;
            }
            let Some(at_s) = apply(x.value(i), s, dv) else {
                return exec_err!("decimal overflow in round");
            };
            // Back to the result scale: the integer part for scale 0.
            let v = if rs == s {
                at_s
            } else {
                at_s / pow10(u32::try_from(s - rs).unwrap_or(0)).unwrap_or(1)
            };
            if limit.is_some_and(|l| v.abs() >= l) {
                return exec_err!("decimal overflow in round");
            }
            out.push(Some(v));
        }
        let arr: ArrayRef = Arc::new(Decimal128Array::from(out).with_precision_and_scale(rp, rs)?);
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?))
        } else {
            Ok(ColumnarValue::Array(arr))
        }
    }
}

impl ScalarUDFImpl for DecimalRound {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        exec_err!("use return_field_from_args instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        match Self::decimal_args(args.arg_fields) {
            Some((p, s)) => {
                let (rp, rs) = result_type(p, s, args.arg_fields.len() == 2);
                let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
                Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::Decimal128(rp, rs),
                    nullable,
                )))
            }
            None => self.inner.return_field_from_args(args),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match Self::decimal_args(&args.arg_fields) {
            Some((p, s)) => self.compute(&args, p, s),
            None => self.inner.invoke_with_args(args),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Decimal128Array, Float64Array};
    use arrow::datatypes::DataType;
    use datafusion::prelude::SessionContext;

    use super::register_decimal_math;

    fn ctx() -> SessionContext {
        let cfg = datafusion::prelude::SessionConfig::new()
            .set_str("datafusion.sql_parser.dialect", "DuckDB")
            .set_bool("datafusion.sql_parser.parse_float_as_decimal", true);
        let ctx = SessionContext::new_with_config(cfg);
        crate::register_trino_functions(&ctx);
        register_decimal_math(&ctx);
        ctx
    }

    /// Type and the unscaled values of the first column.
    async fn dec(ctx: &SessionContext, sql: &str) -> (DataType, Vec<Option<i128>>) {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut vals = vec![];
        let mut ty = None;
        for b in &batches {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap_or_else(|| panic!("not decimal: {}", b.column(0).data_type()));
            ty = Some(a.data_type().clone());
            vals.extend(a.iter());
        }
        (ty.unwrap(), vals)
    }

    #[tokio::test]
    async fn round_one_arg_type_and_value() {
        let ctx = ctx();
        let (t, v) = dec(&ctx, "SELECT round(CAST('12.34' AS DECIMAL(4,2)))").await;
        assert_eq!((t, v), (DataType::Decimal128(3, 0), vec![Some(12)]));
        let (t, v) = dec(&ctx, "SELECT round(CAST('-12.50' AS DECIMAL(4,2)))").await;
        assert_eq!((t, v), (DataType::Decimal128(3, 0), vec![Some(-13)]));
        let (t, v) = dec(&ctx, "SELECT round(CAST('99.99' AS DECIMAL(4,2)))").await;
        assert_eq!((t, v), (DataType::Decimal128(3, 0), vec![Some(100)]));
        // Scale 0: precision unchanged.
        let (t, v) = dec(&ctx, "SELECT round(CAST('12' AS DECIMAL(4,0)))").await;
        assert_eq!((t, v), (DataType::Decimal128(4, 0), vec![Some(12)]));
        // decimal(1,1): 0.5 -> 1 fits decimal(1,0).
        let (t, v) = dec(&ctx, "SELECT round(CAST('0.5' AS DECIMAL(1,1)))").await;
        assert_eq!((t, v), (DataType::Decimal128(1, 0), vec![Some(1)]));
    }

    #[tokio::test]
    async fn round_two_args_keeps_scale_and_adds_a_digit() {
        let ctx = ctx();
        let (t, v) = dec(&ctx, "SELECT round(CAST('12.34' AS DECIMAL(4,2)), 1)").await;
        assert_eq!((t, v), (DataType::Decimal128(5, 2), vec![Some(1230)]));
        let (t, v) = dec(&ctx, "SELECT round(CAST('99.99' AS DECIMAL(4,2)), 1)").await;
        assert_eq!((t, v), (DataType::Decimal128(5, 2), vec![Some(10000)]));
        let (_, v) = dec(&ctx, "SELECT round(CAST('-12.35' AS DECIMAL(4,2)), 1)").await;
        assert_eq!(v, vec![Some(-1240)]);
        let (_, v) = dec(&ctx, "SELECT round(CAST('1234.56' AS DECIMAL(6,2)), -2)").await;
        assert_eq!(v, vec![Some(120_000)]);
    }

    #[tokio::test]
    async fn null_column_and_group_by() {
        let ctx = ctx();
        let t = "(VALUES (1, CAST('1.50' AS DECIMAL(4,2))), (1, CAST('2.50' AS DECIMAL(4,2))), \
                 (2, CAST(NULL AS DECIMAL(4,2))), (2, CAST('-0.49' AS DECIMAL(4,2)))) AS t(g, x)";
        let (ty, v) = dec(&ctx, &format!("SELECT round(x) FROM {t} ORDER BY g, x")).await;
        assert_eq!(ty, DataType::Decimal128(3, 0));
        // ORDER BY x ascending, NULLS LAST by default: g=1: 1.50, 2.50; g=2: -0.49, NULL.
        assert_eq!(v, vec![Some(2), Some(3), Some(0), None]);
        let (ty, v) = dec(
            &ctx,
            &format!("SELECT round(sum(x)) FROM {t} GROUP BY g ORDER BY g"),
        )
        .await;
        // Plain DataFusion `sum` is decimal(14,2) here (the engine's is
        // (38,2)); p - s + 1 = 13.
        assert_eq!(ty, DataType::Decimal128(13, 0));
        assert_eq!(v, vec![Some(4), Some(0)]);
        let (_, v) = dec(&ctx, &format!("SELECT round(x, 0) FROM {t} ORDER BY g, x")).await;
        assert_eq!(v, vec![Some(200), Some(300), Some(0), None]);
    }

    #[tokio::test]
    async fn non_decimal_unchanged() {
        let ctx = ctx();
        let b = ctx
            .sql("SELECT round(CAST(2.567 AS DOUBLE), 2), round(CAST(7 AS BIGINT))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let r = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((r.value(0) - 2.57).abs() < 1e-12);
        assert_eq!(b[0].column(1).data_type(), &DataType::Int64);
    }
}
