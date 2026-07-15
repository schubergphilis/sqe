//! Trino-compatible scalar functions that DataFusion does not provide.
//!
//! - Bitwise scalar functions (#346): `bitwise_and`, `bitwise_or`,
//!   `bitwise_xor`, `bitwise_not`, `bitwise_left_shift`, `bitwise_right_shift`.
//!   DataFusion only ships the `_agg` aggregate variants; Trino's scalar forms
//!   are what BI/analytics SQL uses.
//! - `sequence` (#349): Trino's name for DataFusion's inclusive `generate_series`.
//! - `slice` (#349): 1-based sub-array with a length (not an end index).
//! - Higher-order array functions (#354): `filter` / `transform` lambdas, aliased
//!   onto DataFusion 54's `array_filter` / `array_transform`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderUDF, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

/// Register the Trino-compatible scalar functions defined in this module.
pub fn register_scalar_fns(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(BitwiseAnd));
    ctx.register_udf(ScalarUDF::from(BitwiseOr));
    ctx.register_udf(ScalarUDF::from(BitwiseXor));
    ctx.register_udf(ScalarUDF::from(BitwiseNot));
    ctx.register_udf(ScalarUDF::from(BitwiseLeftShift));
    ctx.register_udf(ScalarUDF::from(BitwiseRightShift));
    ctx.register_udf(ScalarUDF::from(Slice));

    // `sequence` is Trino's spelling of DataFusion's inclusive `generate_series`.
    // Matches Trino for the ascending integer form, the 3-arg step form, and the
    // date/timestamp + INTERVAL form. The one divergence is 2-arg *descending*
    // (`sequence(5, 1)`): Trino auto-descends to [5,4,3,2,1] while
    // `generate_series` returns [] unless an explicit negative step is given.
    let sequence = (*datafusion::functions_nested::range::gen_series_udf())
        .clone()
        .with_aliases(["sequence"]);
    ctx.register_udf(sequence);

    register_higher_order_aliases(ctx);
    crate::higher_order::register_match_predicates(ctx);
}

/// Register Trino names for DataFusion 54's higher-order (lambda) array
/// functions.
///
/// DataFusion 54 ships the lambda framework and three higher-order functions:
/// `array_filter`, `array_transform`, and `array_any_match`. Trino spells the
/// first two `filter(array, x -> pred)` and `transform(array, x -> expr)`;
/// argument order, 1-based lambda binding, and NULL/empty-array handling match,
/// so a name alias is all that is needed. `any_match` is already a DataFusion
/// alias for `array_any_match`, so it needs no work here. `all_match` and
/// `none_match` are custom `HigherOrderUDFImpl`s in [`crate::higher_order`]
/// (registered separately from `register_scalar_fns`).
///
/// Not covered (no DataFusion primitive, tracked on #354): `reduce`, Trino's
/// two-lambda fold. It needs a multi-step lambda implementation and is left to
/// a follow-up rather than shimmed into wrong answers.
fn register_higher_order_aliases(ctx: &SessionContext) {
    for func in datafusion::functions_nested::all_default_higher_order_functions() {
        let trino_alias = match func.name() {
            "array_filter" => "filter",
            "array_transform" => "transform",
            _ => continue,
        };
        // Wrap the shipped impl with the Trino alias. `name()` still reports the
        // DataFusion name, so `array_filter` / `array_transform` keep working.
        let aliased =
            HigherOrderUDF::new_from_shared_impl(Arc::clone(func.inner())).with_aliases([trino_alias]);
        ctx.register_higher_order_function(Arc::new(aliased));
    }
}

// ─── bitwise scalar functions (#346) ─────────────────────────────────────────
//
// Trino's `bitwise_and/or/xor/not` are defined on `bigint` and return `bigint`,
// so integer arguments are widened to Int64. The shifts are *logical* (zero
// fill) and, in Trino, preserve the operand's integer width (`integer` -> 32-bit,
// `bigint` -> 64-bit). SQE follows DataFusion, which types every integer literal
// as Int64, so the shifts operate on 64 bits. This matches Trino for `bigint`
// operands; Trino's narrower-than-bigint behaviour is not reachable here because
// the literal never carries a 32-bit type into the function.

/// Logical (zero-fill) left shift; `shift` outside `0..64` yields 0, matching
/// Trino (`bitwise_left_shift(1, 64) = 0`).
fn logical_shl(value: i64, shift: i64) -> i64 {
    if (0..64).contains(&shift) {
        ((value as u64) << (shift as u32)) as i64
    } else {
        0
    }
}

/// Logical (zero-fill) right shift; `shift` outside `0..64` yields 0, matching
/// Trino (`bitwise_right_shift(255, 68) = 0`).
fn logical_shr(value: i64, shift: i64) -> i64 {
    if (0..64).contains(&shift) {
        ((value as u64) >> (shift as u32)) as i64
    } else {
        0
    }
}

fn eval_binary_i64(args: ScalarFunctionArgs, f: fn(i64, i64) -> i64) -> DFResult<ColumnarValue> {
    let rows = args.number_rows;
    let a = args.args[0].to_array(rows)?;
    let b = args.args[1].to_array(rows)?;
    let a = a
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("bitwise: arg 0 not Int64".into()))?;
    let b = b
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("bitwise: arg 1 not Int64".into()))?;
    let out: Int64Array = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| match (x, y) {
            (Some(x), Some(y)) => Some(f(x, y)),
            _ => None,
        })
        .collect();
    Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
}

fn eval_unary_i64(args: ScalarFunctionArgs, f: fn(i64) -> i64) -> DFResult<ColumnarValue> {
    let rows = args.number_rows;
    let a = args.args[0].to_array(rows)?;
    let a = a
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("bitwise: arg 0 not Int64".into()))?;
    let out: Int64Array = a.iter().map(|x| x.map(f)).collect();
    Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
}

macro_rules! bitwise_binary {
    ($struct_name:ident, $sql_name:literal, $f:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct $struct_name;

        impl ScalarUDFImpl for $struct_name {
            fn name(&self) -> &str {
                $sql_name
            }
            fn signature(&self) -> &Signature {
                static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
                    Signature::exact(vec![DataType::Int64, DataType::Int64], Volatility::Immutable)
                });
                &SIG
            }
            fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
                Ok(DataType::Int64)
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                eval_binary_i64(args, $f)
            }
        }
    };
}

bitwise_binary!(BitwiseAnd, "bitwise_and", |a, b| a & b);
bitwise_binary!(BitwiseOr, "bitwise_or", |a, b| a | b);
bitwise_binary!(BitwiseXor, "bitwise_xor", |a, b| a ^ b);
bitwise_binary!(BitwiseLeftShift, "bitwise_left_shift", logical_shl);
bitwise_binary!(BitwiseRightShift, "bitwise_right_shift", logical_shr);

#[derive(Debug, PartialEq, Eq, Hash)]
struct BitwiseNot;

impl ScalarUDFImpl for BitwiseNot {
    fn name(&self) -> &str {
        "bitwise_not"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::exact(vec![DataType::Int64], Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        eval_unary_i64(args, |a| !a)
    }
}

// ─── slice(array, start, length) (#349) ──────────────────────────────────────
//
// Trino `slice(x, start, length)`: 1-based; `start` counts from the end when
// negative; `length` is a count (clamped to the array end). Distinct from
// DataFusion `array_slice(x, begin, end)`, which takes an inclusive end index.

/// Map Trino `(start, length)` against a row of `list_len` elements to a
/// 0-based `(offset, take)` window into the child values.
fn slice_window(list_len: usize, start: i64, length: i64) -> (usize, usize) {
    if length <= 0 {
        return (0, 0);
    }
    let len = list_len as i64;
    // 0-based begin index.
    let begin = if start > 0 {
        start - 1
    } else if start < 0 {
        len + start
    } else {
        // start == 0: Trino has no element 0; treat as an empty slice.
        return (0, 0);
    };
    let begin = begin.clamp(0, len);
    let take = length.min(len - begin).max(0);
    (begin as usize, take as usize)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Slice;

impl ScalarUDFImpl for Slice {
    fn name(&self) -> &str {
        "slice"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(3, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        match args.first() {
            Some(t @ DataType::List(_)) => Ok(t.clone()),
            other => Err(DataFusionError::Plan(format!(
                "slice: first argument must be an array (List), got {other:?}"
            ))),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let list = args.args[0].to_array(rows)?;
        let list = list
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Internal("slice: arg 0 is not a List array".into()))?;
        let starts = args.args[1].to_array(rows)?;
        let starts = starts
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("slice: start must be bigint".into()))?;
        let lengths = args.args[2].to_array(rows)?;
        let lengths = lengths
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("slice: length must be bigint".into()))?;

        let field = match list.data_type() {
            DataType::List(f) => f.clone(),
            other => {
                return Err(DataFusionError::Internal(format!(
                    "slice: unexpected list type {other:?}"
                )))
            }
        };

        let mut pieces: Vec<ArrayRef> = Vec::with_capacity(rows);
        let mut offsets: Vec<i32> = Vec::with_capacity(rows + 1);
        offsets.push(0);
        let mut valid: Vec<bool> = Vec::with_capacity(rows);
        let mut any_null = false;

        for i in 0..rows {
            let row = list.value(i);
            let empty = row.slice(0, 0);
            if list.is_null(i) || starts.is_null(i) || lengths.is_null(i) {
                // Trino: a NULL argument yields a NULL result row.
                valid.push(false);
                any_null = true;
                offsets.push(*offsets.last().unwrap());
                pieces.push(empty);
                continue;
            }
            let (off, take) = slice_window(row.len(), starts.value(i), lengths.value(i));
            pieces.push(row.slice(off, take));
            offsets.push(offsets.last().unwrap() + take as i32);
            valid.push(true);
        }

        let refs: Vec<&dyn Array> = pieces.iter().map(|p| p.as_ref()).collect();
        let values: ArrayRef = if refs.is_empty() {
            arrow::array::new_empty_array(field.data_type())
        } else {
            arrow::compute::concat(&refs)?
        };
        let nulls = if any_null {
            Some(NullBuffer::from(valid))
        } else {
            None
        };
        let result = ListArray::new(field, OffsetBuffer::new(offsets.into()), values, nulls);
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn scalar_i64(sql: &str) -> Option<i64> {
        let ctx = crate::duckdb_test_ctx();
        register_scalar_fns(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0))
        }
    }

    #[tokio::test]
    async fn bitwise_and_or_xor() {
        assert_eq!(scalar_i64("SELECT bitwise_and(5, 3)").await, Some(1));
        assert_eq!(scalar_i64("SELECT bitwise_or(5, 3)").await, Some(7));
        assert_eq!(scalar_i64("SELECT bitwise_xor(5, 3)").await, Some(6));
        assert_eq!(scalar_i64("SELECT bitwise_and(-1, 5)").await, Some(5));
    }

    #[tokio::test]
    async fn bitwise_not_matches_trino() {
        assert_eq!(scalar_i64("SELECT bitwise_not(0)").await, Some(-1));
        assert_eq!(scalar_i64("SELECT bitwise_not(5)").await, Some(-6));
    }

    #[tokio::test]
    async fn bitwise_shifts_are_logical_64bit() {
        assert_eq!(scalar_i64("SELECT bitwise_left_shift(1, 3)").await, Some(8));
        assert_eq!(scalar_i64("SELECT bitwise_right_shift(16, 2)").await, Some(4));
        // bigint operand: logical (zero-fill) right shift, not arithmetic.
        assert_eq!(
            scalar_i64("SELECT bitwise_right_shift(CAST(-8 AS bigint), 1)").await,
            Some(9223372036854775804)
        );
        assert_eq!(
            scalar_i64("SELECT bitwise_left_shift(CAST(-1 AS bigint), 1)").await,
            Some(-2)
        );
        // shift >= width -> 0, matching Trino.
        assert_eq!(scalar_i64("SELECT bitwise_right_shift(255, 68)").await, Some(0));
        assert_eq!(scalar_i64("SELECT bitwise_left_shift(1, 64)").await, Some(0));
    }

    #[tokio::test]
    async fn bitwise_null_propagates() {
        assert_eq!(scalar_i64("SELECT bitwise_and(NULL, 3)").await, None);
    }

    async fn array_i64(sql: &str) -> Vec<i64> {
        let ctx = crate::duckdb_test_ctx();
        register_scalar_fns(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let row = list.value(0);
        let arr = row.as_any().downcast_ref::<Int64Array>().unwrap();
        arr.iter().map(|v| v.unwrap()).collect()
    }

    #[tokio::test]
    async fn sequence_alias_of_generate_series() {
        assert_eq!(array_i64("SELECT sequence(1, 5)").await, vec![1, 2, 3, 4, 5]);
        assert_eq!(array_i64("SELECT sequence(1, 7, 2)").await, vec![1, 3, 5, 7]);
        assert_eq!(array_i64("SELECT sequence(5, 1, -2)").await, vec![5, 3, 1]);
    }

    #[tokio::test]
    async fn slice_matches_trino() {
        assert_eq!(
            array_i64("SELECT slice(make_array(1,2,3,4), 2, 2)").await,
            vec![2, 3]
        );
        // negative start counts from the end
        assert_eq!(
            array_i64("SELECT slice(make_array(1,2,3,4), -2, 2)").await,
            vec![3, 4]
        );
        // length overruns the array -> clamped to the end
        assert_eq!(
            array_i64("SELECT slice(make_array(1,2,3,4), 2, 10)").await,
            vec![2, 3, 4]
        );
        // start past the end -> empty
        assert_eq!(
            array_i64("SELECT slice(make_array(1,2,3,4), 5, 2)").await,
            Vec::<i64>::new()
        );
    }

    /// Run a query returning a single `array<bigint>` cell. `None` means the row
    /// itself is NULL; otherwise the element list, preserving per-element NULLs.
    async fn hof_i64(sql: &str) -> Option<Vec<Option<i64>>> {
        let ctx = crate::duckdb_test_ctx();
        register_scalar_fns(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        if list.is_null(0) {
            return None;
        }
        let row = list.value(0);
        let arr = row.as_any().downcast_ref::<Int64Array>().unwrap();
        Some(arr.iter().collect())
    }

    fn vals(v: &[i64]) -> Option<Vec<Option<i64>>> {
        Some(v.iter().map(|x| Some(*x)).collect())
    }

    #[tokio::test]
    async fn filter_lambda_trino_alias() {
        // Trino: filter(array, x -> pred) keeps matching elements in order.
        assert_eq!(
            hof_i64("SELECT filter(make_array(1,2,3,4), x -> x > 2)").await,
            vals(&[3, 4])
        );
        // The DataFusion name still resolves: the alias did not clobber it.
        assert_eq!(
            hof_i64("SELECT array_filter(make_array(1,2,3,4), x -> x > 2)").await,
            vals(&[3, 4])
        );
    }

    #[tokio::test]
    async fn transform_lambda_trino_alias() {
        assert_eq!(
            hof_i64("SELECT transform(make_array(1,2,3), x -> x * 10)").await,
            vals(&[10, 20, 30])
        );
        assert_eq!(
            hof_i64("SELECT array_transform(make_array(1,2,3), x -> x * 10)").await,
            vals(&[10, 20, 30])
        );
    }

    #[tokio::test]
    async fn filter_lambda_dropping_all_yields_empty() {
        // A predicate that matches nothing -> empty array, matching Trino.
        assert_eq!(
            hof_i64("SELECT filter(make_array(1,2), x -> x > 9)").await,
            vals(&[])
        );
    }

    #[tokio::test]
    async fn higher_order_null_element_handling() {
        // NULL element: transform applies the lambda per element; NULL * 10 is
        // NULL, so the NULL slot survives in place (Trino parity).
        assert_eq!(
            hof_i64("SELECT transform(make_array(1, CAST(NULL AS bigint), 3), x -> x * 10)").await,
            Some(vec![Some(10), None, Some(30)])
        );
        // NULL element under filter: the predicate is NULL (not true), so the
        // NULL element is dropped, matching Trino's SQL three-valued logic.
        assert_eq!(
            hof_i64("SELECT filter(make_array(1, CAST(NULL AS bigint), 3), x -> x > 1)").await,
            vals(&[3])
        );
    }
}
