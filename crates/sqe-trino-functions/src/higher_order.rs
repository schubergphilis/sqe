//! Trino `all_match` / `none_match` higher-order array predicates (#354).
//!
//! DataFusion 54 ships only `array_any_match`. Trino also has `all_match` and
//! `none_match`. Both share `array_any_match`'s exact shape (array + boolean
//! lambda produces a bool, same NULL/empty-array semantics); only the per-row
//! reduction differs. They are implemented here as `HigherOrderUDFImpl` by
//! copying the any_match evaluation and swapping the reducer, so lambda
//! evaluation, NULL handling, and list-slice offset math match DataFusion's own
//! function exactly rather than being re-derived.
//!
//! Registered under the Trino names `all_match` / `none_match`. This needs the
//! DuckDB parse dialect (set on the SQE session) so `x -> pred` parses as a
//! lambda. `reduce` (a two-lambda fold) has no DataFusion counterpart and is
//! deliberately not implemented here.

use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder, Int64Array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{nullif, take, take_arrays};
use arrow::datatypes::{ArrowNativeType, DataType, Field, FieldRef};
use datafusion::common::utils::{
    adjust_offsets_for_slice, list_values, list_values_row_number, take_function_args,
};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};
use datafusion::prelude::SessionContext;

/// Per-range reducer type: given the flat predicate results and the `[start, end)`
/// slice for one row, return the row's result (`None` = SQL NULL).
type RangeReducer = fn(&BooleanArray, usize, usize) -> Option<bool>;

/// `all_match`: true when every element satisfies the predicate. Any element
/// that is false makes the row false; otherwise a NULL predicate result makes
/// the row NULL; an all-true (or empty) range is true. Matches Trino.
fn all_match_for_range(predicate: &BooleanArray, start: usize, end: usize) -> Option<bool> {
    let any_false = (start..end).any(|j| predicate.is_valid(j) && !predicate.value(j));
    if any_false {
        return Some(false);
    }
    let any_null = (start..end).any(|j| predicate.is_null(j));
    if any_null {
        None
    } else {
        Some(true)
    }
}

/// `none_match`: true when no element satisfies the predicate. Any element that
/// is true makes the row false; otherwise a NULL predicate result makes the row
/// NULL; an all-false (or empty) range is true. Matches Trino.
fn none_match_for_range(predicate: &BooleanArray, start: usize, end: usize) -> Option<bool> {
    let any_true = (start..end).any(|j| predicate.is_valid(j) && predicate.value(j));
    if any_true {
        return Some(false);
    }
    let any_null = (start..end).any(|j| predicate.is_null(j));
    if any_null {
        None
    } else {
        Some(true)
    }
}

/// Shared plan-time coercion: the sole value argument must be a list; normalize
/// the list-view / fixed-size variants to plain `List` / `LargeList`.
fn coerce_list_arg(name: &str, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
    let [list] = arg_types else {
        return Err(DataFusionError::Plan(format!(
            "{name} function requires 1 value argument, got {}",
            arg_types.len()
        )));
    };
    let coerced = match list {
        DataType::List(_) | DataType::LargeList(_) => list.clone(),
        DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            DataType::List(Arc::clone(field))
        }
        DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "{name} expected a list as first argument, got {list}"
            )));
        }
    };
    Ok(vec![coerced])
}

/// Shared: bind the lambda's single parameter to the list element type.
fn list_element_lambda_params(
    name: &str,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> DFResult<LambdaParametersProgress> {
    let [list, _] = take_function_args(name, fields)?;
    let ValueOrLambda::Value(list) = list else {
        return Err(DataFusionError::Plan(format!(
            "{name} expects a value as first argument"
        )));
    };
    let field = match list.data_type() {
        DataType::List(field) | DataType::LargeList(field) => field,
        other => {
            return Err(DataFusionError::Plan(format!("expected list, got {other}")));
        }
    };
    Ok(LambdaParametersProgress::Complete(vec![vec![Arc::clone(
        field,
    )]]))
}

/// Shared: the result is a nullable-aware boolean scalar per row.
fn boolean_return_field(name: &str, args: HigherOrderReturnFieldArgs) -> DFResult<Arc<Field>> {
    let [ValueOrLambda::Value(list), _] = take_function_args(name, args.arg_fields)? else {
        return Err(DataFusionError::Plan(format!(
            "{name} expects a value as first argument"
        )));
    };
    Ok(Arc::new(Field::new(
        "",
        DataType::Boolean,
        list.is_nullable(),
    )))
}

/// Shared evaluation: run the predicate lambda over the flattened list values,
/// then reduce each row's element range with `reducer`. This is the
/// `array_any_match` body with the reducer factored out; the null-row bitmap
/// union is kept identical.
fn invoke_match(
    name: &str,
    args: HigherOrderFunctionArgs,
    reducer: RangeReducer,
) -> DFResult<ColumnarValue> {
    let [ValueOrLambda::Value(list), ValueOrLambda::Lambda(lambda)] =
        take_function_args(name, &args.args)?
    else {
        return Err(DataFusionError::Execution(format!(
            "{name} expects a value followed by a lambda"
        )));
    };

    let list_array = list.to_array(args.number_rows)?;

    // Fast path: fully null input (also required for FixedSizeList, which
    // clear_null_values cannot handle when fully null).
    if list_array.null_count() == list_array.len() {
        return Ok(ColumnarValue::Array(new_null_array(
            args.return_type(),
            list_array.len(),
        )));
    }

    let list_values = list_values(&list_array)?;
    let values_param = || Ok(Arc::clone(&list_values));

    let predicate_results = lambda
        .evaluate(&[&values_param], |arrays| {
            let indices = list_values_row_number(&list_array)?;
            Ok(take_arrays(arrays, &indices, None)?)
        })?
        .into_array(list_values.len())?;

    let predicate_bool = predicate_results
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!("{name} predicate must return boolean array"))
        })?;

    let mut values = BooleanBuilder::with_capacity(list_array.len());

    macro_rules! process_list {
        ($list_typed:expr) => {{
            let offsets = adjust_offsets_for_slice($list_typed);
            for i in 0..$list_typed.len() {
                let start = offsets[i].as_usize();
                let end = offsets[i + 1].as_usize();
                values.append_option(reducer(predicate_bool, start, end));
            }
        }};
    }

    match list_array.data_type() {
        DataType::List(_) => process_list!(list_array.as_list::<i32>()),
        DataType::LargeList(_) => process_list!(list_array.as_list::<i64>()),
        other => {
            return Err(DataFusionError::Execution(format!(
                "expected list, got {other}"
            )))
        }
    }

    let (boolean_buffer, predicate_nulls) = values.finish().into_parts();
    // A row is NULL if the input list row was NULL or the predicate poisoned it.
    let nulls = NullBuffer::union(list_array.nulls(), predicate_nulls.as_ref());
    Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
        boolean_buffer,
        nulls,
    ))))
}

/// Build the shared exact signature: one value (the array) plus one lambda.
fn match_signature() -> HigherOrderSignature {
    HigherOrderSignature::exact(
        vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
        Volatility::Immutable,
    )
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayAllMatch {
    signature: HigherOrderSignature,
}

impl HigherOrderUDFImpl for ArrayAllMatch {
    fn name(&self) -> &str {
        "array_all_match"
    }
    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }
    fn coerce_value_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        coerce_list_arg(self.name(), arg_types)
    }
    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> DFResult<LambdaParametersProgress> {
        list_element_lambda_params(self.name(), fields)
    }
    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> DFResult<Arc<Field>> {
        boolean_return_field(self.name(), args)
    }
    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> DFResult<ColumnarValue> {
        invoke_match(self.name(), args, all_match_for_range)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayNoneMatch {
    signature: HigherOrderSignature,
}

impl HigherOrderUDFImpl for ArrayNoneMatch {
    fn name(&self) -> &str {
        "array_none_match"
    }
    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }
    fn coerce_value_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        coerce_list_arg(self.name(), arg_types)
    }
    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> DFResult<LambdaParametersProgress> {
        list_element_lambda_params(self.name(), fields)
    }
    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> DFResult<Arc<Field>> {
        boolean_return_field(self.name(), args)
    }
    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> DFResult<ColumnarValue> {
        invoke_match(self.name(), args, none_match_for_range)
    }
}

// ─── reduce (two-lambda fold) ────────────────────────────────────────────────

/// `reduce(array, initialState, (state, element) -> state, state -> result)`.
///
/// Trino's fold. Unlike the match predicates it is *sequential*: each element's
/// combine result feeds the next. There is no DataFusion primitive to copy.
///
/// The evaluation keeps one accumulator per outer row and advances all rows in
/// lockstep. At element index `k` it gathers the k-th element of every row whose
/// array is still that long, evaluates the combine lambda once over that
/// `num_rows`-wide batch, and scatters the result back only for the active rows
/// (`zip` on an active mask). Because every batch is one-value-per-row, captured
/// outer columns already align, so `spread_captures` is the identity. After the
/// longest array is exhausted, the finish lambda maps each accumulator to the
/// result. A NULL input array yields NULL (Trino semantics).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayReduce {
    signature: HigherOrderSignature,
}

/// Per-row `(start, len)` into the flattened list values, for either offset width.
fn list_row_ranges(list_array: &ArrayRef) -> DFResult<Vec<(usize, usize)>> {
    macro_rules! ranges {
        ($typed:expr) => {{
            let offsets = adjust_offsets_for_slice($typed);
            (0..$typed.len())
                .map(|i| {
                    let start = offsets[i].as_usize();
                    (start, offsets[i + 1].as_usize() - start)
                })
                .collect()
        }};
    }
    Ok(match list_array.data_type() {
        DataType::List(_) => ranges!(list_array.as_list::<i32>()),
        DataType::LargeList(_) => ranges!(list_array.as_list::<i64>()),
        other => {
            return Err(DataFusionError::Execution(format!(
                "reduce expected list, got {other}"
            )))
        }
    })
}

impl HigherOrderUDFImpl for ArrayReduce {
    fn name(&self) -> &str {
        "array_reduce"
    }
    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let [list, init] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "reduce requires 2 value arguments (array, initial state), got {}",
                arg_types.len()
            )));
        };
        let coerced_list = match list {
            DataType::List(_) | DataType::LargeList(_) => list.clone(),
            DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
                DataType::List(Arc::clone(field))
            }
            DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "reduce expected a list as first argument, got {list}"
                )));
            }
        };
        Ok(vec![coerced_list, init.clone()])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> DFResult<LambdaParametersProgress> {
        let [list, init, _, _] = take_function_args(self.name(), fields)?;
        let (ValueOrLambda::Value(list), ValueOrLambda::Value(init)) = (list, init) else {
            return Err(DataFusionError::Plan(
                "reduce expects (array, initial state) as its two value arguments".into(),
            ));
        };
        let element_field = match list.data_type() {
            DataType::List(field) | DataType::LargeList(field) => Arc::clone(field),
            other => {
                return Err(DataFusionError::Plan(format!("expected list, got {other}")));
            }
        };
        let acc_field = Arc::clone(init);
        // combine lambda binds (state, element); finish lambda binds (state).
        Ok(LambdaParametersProgress::Complete(vec![
            vec![Arc::clone(&acc_field), element_field],
            vec![acc_field],
        ]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> DFResult<Arc<Field>> {
        let [_, _, _, finish] = take_function_args(self.name(), args.arg_fields)?;
        let ValueOrLambda::Lambda(finish_field) = finish else {
            return Err(DataFusionError::Plan(
                "reduce expects a finish lambda as its fourth argument".into(),
            ));
        };
        // A NULL input array yields NULL, so the result is always nullable.
        Ok(Arc::new(Field::new(
            "",
            finish_field.data_type().clone(),
            true,
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> DFResult<ColumnarValue> {
        let [ValueOrLambda::Value(list), ValueOrLambda::Value(init), ValueOrLambda::Lambda(combine), ValueOrLambda::Lambda(finish)] =
            take_function_args(self.name(), &args.args)?
        else {
            return Err(DataFusionError::Execution(
                "reduce expects (array, initial state, combine lambda, finish lambda)".into(),
            ));
        };

        let n = args.number_rows;
        let list_array = list.to_array(n)?;
        let values = list_values(&list_array)?;
        let ranges = list_row_ranges(&list_array)?;
        let max_len = ranges.iter().map(|(_, len)| *len).max().unwrap_or(0);

        // Accumulator, one value per row, initialized to the (broadcast) init.
        let mut acc = init.to_array(n)?;

        // Captures are already one-per-row, matching every batch here.
        let identity_spread = |caps: &[ArrayRef]| Ok(caps.to_vec());

        for k in 0..max_len {
            let indices: Int64Array = (0..n)
                .map(|r| {
                    let (start, len) = ranges[r];
                    (k < len).then_some((start + k) as i64)
                })
                .collect();
            let element_k = take(values.as_ref(), &indices, None)?;
            let active: BooleanArray = (0..n)
                .map(|r| Some(k < ranges[r].1 && list_array.is_valid(r)))
                .collect();

            let acc_now = Arc::clone(&acc);
            let acc_cl = || Ok(Arc::clone(&acc_now));
            let el_cl = || Ok(Arc::clone(&element_k));
            let new_acc = combine
                .evaluate(&[&acc_cl, &el_cl], identity_spread)?
                .into_array(n)?;

            // Keep the old accumulator for rows whose array is already exhausted.
            // `&dyn Array` implements `Datum`, so pass it by reference.
            let truthy: &dyn Array = new_acc.as_ref();
            let falsy: &dyn Array = acc.as_ref();
            let next = zip(&active, &truthy, &falsy)?;
            acc = next;
        }

        let acc_now = Arc::clone(&acc);
        let acc_cl = || Ok(Arc::clone(&acc_now));
        let result = finish
            .evaluate(&[&acc_cl], identity_spread)?
            .into_array(n)?;

        // NULL array row produces a NULL result, regardless of the finish lambda.
        let list_is_null: BooleanArray = (0..n).map(|r| Some(list_array.is_null(r))).collect();
        let result = nullif(result.as_ref(), &list_is_null)?;
        Ok(ColumnarValue::Array(result))
    }
}

/// Register the SQE custom higher-order functions (`all_match`, `none_match`,
/// `reduce`) under their Trino names. The primary UDF names (`array_all_match`
/// / `array_none_match` / `array_reduce`) also resolve, mirroring DataFusion's
/// `array_any_match` / `any_match` pairing.
pub fn register_match_predicates(ctx: &SessionContext) {
    let all = HigherOrderUDF::new_from_impl(ArrayAllMatch {
        signature: match_signature(),
    })
    .with_aliases(["all_match"]);
    ctx.register_higher_order_function(Arc::new(all));

    let none = HigherOrderUDF::new_from_impl(ArrayNoneMatch {
        signature: match_signature(),
    })
    .with_aliases(["none_match"]);
    ctx.register_higher_order_function(Arc::new(none));

    // reduce(array, init, combine, finish): two values, two lambdas.
    let reduce = HigherOrderUDF::new_from_impl(ArrayReduce {
        signature: HigherOrderSignature::exact(
            vec![
                ValueOrLambda::Value(()),
                ValueOrLambda::Value(()),
                ValueOrLambda::Lambda(()),
                ValueOrLambda::Lambda(()),
            ],
            Volatility::Immutable,
        ),
    })
    .with_aliases(["reduce"]);
    ctx.register_higher_order_function(Arc::new(reduce));
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BooleanArray;

    async fn one_bool(sql: &str) -> Option<bool> {
        let ctx = crate::duckdb_test_ctx();
        register_match_predicates(&ctx);
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let a = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        if a.is_null(0) {
            None
        } else {
            Some(a.value(0))
        }
    }

    #[tokio::test]
    async fn all_match_matches_trino() {
        assert_eq!(
            one_bool("SELECT all_match(make_array(2, 4, 6), x -> x % 2 = 0)").await,
            Some(true)
        );
        assert_eq!(
            one_bool("SELECT all_match(make_array(2, 3, 4), x -> x % 2 = 0)").await,
            Some(false)
        );
        // Empty array: vacuously true. `array_filter` is a DataFusion built-in
        // (always registered); the Trino `filter` alias is not set up in this
        // module's isolated test context.
        assert_eq!(
            one_bool("SELECT all_match(array_filter(make_array(1), x -> x > 9), x -> x > 0)").await,
            Some(true)
        );
        // No false, but a NULL predicate result: NULL.
        assert_eq!(
            one_bool("SELECT all_match(make_array(2, CAST(NULL AS bigint)), x -> x % 2 = 0)").await,
            None
        );
        // A false element dominates a NULL: false.
        assert_eq!(
            one_bool("SELECT all_match(make_array(3, CAST(NULL AS bigint)), x -> x % 2 = 0)").await,
            Some(false)
        );
    }

    #[tokio::test]
    async fn none_match_matches_trino() {
        assert_eq!(
            one_bool("SELECT none_match(make_array(1, 3, 5), x -> x % 2 = 0)").await,
            Some(true)
        );
        assert_eq!(
            one_bool("SELECT none_match(make_array(1, 2, 3), x -> x % 2 = 0)").await,
            Some(false)
        );
        // A true element dominates a NULL: false.
        assert_eq!(
            one_bool("SELECT none_match(make_array(2, CAST(NULL AS bigint)), x -> x % 2 = 0)")
                .await,
            Some(false)
        );
        // No true, but a NULL predicate result: NULL.
        assert_eq!(
            one_bool("SELECT none_match(make_array(1, CAST(NULL AS bigint)), x -> x % 2 = 0)")
                .await,
            None
        );
    }

    #[tokio::test]
    async fn primary_names_resolve() {
        assert_eq!(
            one_bool("SELECT array_all_match(make_array(2, 4), x -> x % 2 = 0)").await,
            Some(true)
        );
        assert_eq!(
            one_bool("SELECT array_none_match(make_array(1, 3), x -> x % 2 = 0)").await,
            Some(true)
        );
    }

    async fn one_i64(sql: &str) -> Option<i64> {
        let ctx = crate::duckdb_test_ctx();
        register_match_predicates(&ctx);
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let a = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        if a.is_null(0) {
            None
        } else {
            Some(a.value(0))
        }
    }

    #[tokio::test]
    async fn reduce_folds_left() {
        // Sum. finish is identity.
        assert_eq!(
            one_i64(
                "SELECT reduce(make_array(1, 2, 3, 4), CAST(0 AS bigint), (s, x) -> s + x, s -> s)"
            )
            .await,
            Some(10)
        );
        // Sum then double in the finish lambda: (1+2+3) * 2 = 12.
        assert_eq!(
            one_i64(
                "SELECT reduce(make_array(1, 2, 3), CAST(0 AS bigint), (s, x) -> s + x, s -> s * 2)"
            )
            .await,
            Some(12)
        );
    }

    #[tokio::test]
    async fn reduce_empty_array_returns_finish_of_init() {
        // Empty array: no combine steps, so finish is applied to the initial state.
        assert_eq!(
            one_i64(
                "SELECT reduce(array_filter(make_array(1), x -> x > 9), CAST(7 AS bigint), (s, x) -> s + x, s -> s * 10)"
            )
            .await,
            Some(70)
        );
    }

    #[tokio::test]
    async fn reduce_null_array_is_null() {
        assert_eq!(
            one_i64(
                "SELECT reduce(CAST(NULL AS bigint[]), CAST(0 AS bigint), (s, x) -> s + x, s -> s)"
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn reduce_primary_name_resolves() {
        assert_eq!(
            one_i64(
                "SELECT array_reduce(make_array(2, 3), CAST(1 AS bigint), (s, x) -> s * x, s -> s)"
            )
            .await,
            Some(6)
        );
    }
}
