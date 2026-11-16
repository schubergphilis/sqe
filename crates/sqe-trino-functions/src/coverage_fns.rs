//! Trino-compatible functions that DataFusion is missing or mis-resolves (#356).
//!
//! - `count_if(pred)`: aggregate that counts TRUE rows (ignores false + NULL).
//! - `element_at(array|map, k)`: DataFusion binds `element_at` to `map_extract`,
//!   which errors on arrays. This registers a type-dispatching UDF: 1-based
//!   array indexing (negative from the end, out-of-bounds -> NULL) and map
//!   lookup returning the scalar value.
//! - `contains(array, x)`: DataFusion's `contains` is the string function.
//!   This overrides it to dispatch: array membership (three-valued, NULL when
//!   the element is absent but the array holds a NULL) and the original string
//!   `contains` behaviour is preserved.
//! - `cosine_similarity(array, array)`: Trino's vector similarity (#907),
//!   `dot(a, b) / (|a| * |b|)`. DataFusion has only `cosine_distance`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, ListArray, MapArray, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register the functions defined in this module.
pub fn register_coverage_fns(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(CountIf::new()));
    ctx.register_udf(ScalarUDF::from(ElementAt));
    ctx.register_udf(ScalarUDF::from(Contains));
    ctx.register_udf(ScalarUDF::from(CosineSimilarity::default()));
}

// ─── count_if(pred) ──────────────────────────────────────────────────────────

/// `count_if(x)` counts the rows where `x` is TRUE. false and NULL are ignored,
/// matching Trino. `AggregateUDFImpl` requires `Debug + Eq + Hash`; `count_if`
/// carries no distinguishing state, so all instances are equal.
struct CountIf {
    signature: Signature,
}

impl std::fmt::Debug for CountIf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CountIf")
    }
}
impl PartialEq for CountIf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for CountIf {}
impl std::hash::Hash for CountIf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        "count_if".hash(state);
    }
}

impl CountIf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CountIf {
    fn name(&self) -> &str {
        "count_if"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(CountIfAccumulator { count: 0 }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[count]", args.name),
            DataType::Int64,
            false,
        ))])
    }
}

#[derive(Debug)]
struct CountIfAccumulator {
    count: i64,
}

impl Accumulator for CountIfAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = values[0]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| DataFusionError::Plan("count_if: arg is not Boolean".into()))?;
        // Only TRUE increments; false and NULL are skipped.
        self.count += arr.iter().filter(|v| *v == Some(true)).count() as i64;
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let arr = states[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Plan("count_if: state is not Int64".into()))?;
        for v in arr.iter().flatten() {
            self.count += v;
        }
        Ok(())
    }
}

// ─── element_at(array|map, k) ────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct ElementAt;

impl ScalarUDFImpl for ElementAt {
    fn name(&self) -> &str {
        "element_at"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        match args.first() {
            Some(DataType::List(field)) | Some(DataType::LargeList(field)) => {
                Ok(field.data_type().clone())
            }
            Some(DataType::Map(entries, _)) => match entries.data_type() {
                // Map's entries is Struct([key, value]); return the value type.
                DataType::Struct(fields) if fields.len() == 2 => Ok(fields[1].data_type().clone()),
                other => Err(DataFusionError::Plan(format!(
                    "element_at: unexpected map entry type {other:?}"
                ))),
            },
            other => Err(DataFusionError::Plan(format!(
                "element_at: first argument must be an array or map, got {other:?}"
            ))),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let container = args.args[0].to_array(rows)?;
        let keys = args.args[1].to_array(rows)?;

        if let Some(list) = container.as_any().downcast_ref::<ListArray>() {
            let offsets = list.value_offsets();
            let values = list.values();
            let idx = keys.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Plan("element_at: array index must be bigint".into())
            })?;
            let mut take_idx: Vec<Option<i64>> = Vec::with_capacity(rows);
            for i in 0..rows {
                if list.is_null(i) || idx.is_null(i) {
                    take_idx.push(None);
                    continue;
                }
                let start = offsets[i] as i64;
                let len = offsets[i + 1] as i64 - start;
                let n = idx.value(i);
                // Trino element_at is 1-based; negative counts from the end.
                let j = if n > 0 {
                    n - 1
                } else if n < 0 {
                    len + n
                } else {
                    -1 // index 0 does not exist -> NULL
                };
                take_idx.push(if j >= 0 && j < len {
                    Some(start + j)
                } else {
                    None
                });
            }
            let index_array = Int64Array::from(take_idx);
            let result = arrow::compute::take(values.as_ref(), &index_array, None)?;
            return Ok(ColumnarValue::Array(result));
        }

        if let Some(map) = container.as_any().downcast_ref::<MapArray>() {
            let map_keys = map.keys();
            let map_values = map.values();
            let offsets = map.value_offsets();
            let mut take_idx: Vec<Option<i64>> = Vec::with_capacity(rows);
            for i in 0..rows {
                if map.is_null(i) || keys.is_null(i) {
                    take_idx.push(None);
                    continue;
                }
                let wanted = ScalarValue::try_from_array(&keys, i)?;
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                let mut found = None;
                for k in start..end {
                    if ScalarValue::try_from_array(map_keys, k)? == wanted {
                        found = Some(k as i64);
                        break;
                    }
                }
                take_idx.push(found);
            }
            let index_array = Int64Array::from(take_idx);
            let result = arrow::compute::take(map_values.as_ref(), &index_array, None)?;
            return Ok(ColumnarValue::Array(result));
        }

        Err(DataFusionError::Plan(format!(
            "element_at: unsupported container type {:?}",
            container.data_type()
        )))
    }
}

// ─── contains ────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct Contains;

impl ScalarUDFImpl for Contains {
    fn name(&self) -> &str {
        "contains"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let first = args.args[0].to_array(rows)?;

        // Trino `contains(array, element)`: three-valued membership test.
        if let Some(list) = first.as_any().downcast_ref::<ListArray>() {
            // Compare in the list's item type: a 'x' literal is Utf8 while a
            // scanned array<string> holds Utf8View, and ScalarValue equality
            // is type-strict.
            let elems = args.args[1].to_array(rows)?;
            let item = list.value_type();
            let elems = if elems.data_type() != &item
                && arrow::compute::can_cast_types(elems.data_type(), &item)
            {
                arrow::compute::cast(&elems, &item)?
            } else {
                elems
            };
            let mut out: Vec<Option<bool>> = Vec::with_capacity(rows);
            for i in 0..rows {
                if list.is_null(i) || elems.is_null(i) {
                    out.push(None);
                    continue;
                }
                let wanted = ScalarValue::try_from_array(&elems, i)?;
                let row = list.value(i);
                let mut found = false;
                let mut saw_null = false;
                for k in 0..row.len() {
                    let e = ScalarValue::try_from_array(&row, k)?;
                    if e.is_null() {
                        saw_null = true;
                    } else if e == wanted {
                        found = true;
                        break;
                    }
                }
                // Absent but a NULL was present -> cannot confirm absence -> NULL.
                out.push(if found {
                    Some(true)
                } else if saw_null {
                    None
                } else {
                    Some(false)
                });
            }
            return Ok(ColumnarValue::Array(
                Arc::new(BooleanArray::from(out)) as ArrayRef
            ));
        }

        // Preserve DataFusion's string `contains(haystack, needle)`.
        args.args = crate::helpers::utf8_string_args(std::mem::take(&mut args.args))?;
        let first = args.args[0].to_array(rows)?;
        let haystack = first
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "contains: first argument must be an array or string, got {:?}",
                    first.data_type()
                ))
            })?;
        let needle = args.args[1].to_array(rows)?;
        let needle = needle
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("contains: string needle expected".into()))?;
        let out: BooleanArray = haystack
            .iter()
            .zip(needle.iter())
            .map(|(h, n)| match (h, n) {
                (Some(h), Some(n)) => Some(h.contains(n)),
                _ => None,
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    }
}

// ─── cosine_similarity(a, b) ─────────────────────────────────────────────────

/// Trino `cosine_similarity(array(double), array(double))`: 1 for the same
/// direction, 0 for orthogonal, -1 for opposite. Arguments are coerced the
/// way DataFusion's `cosine_distance` coerces them (any numeric list, fixed
/// size lists, `LargeList`), so both functions accept the same columns. The
/// value is computed directly rather than as `1 - cosine_distance`, which
/// would lose precision near 0.
///
/// NULL when either array is NULL, holds a NULL element, or is all zeros
/// (Trino raises on NULL elements). Arrays of different length are an error,
/// as in Trino. Trino's `map(varchar, double)` form is not supported.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
struct CosineSimilarity {
    distance: datafusion::functions_nested::cosine_distance::CosineDistance,
}

impl ScalarUDFImpl for CosineSimilarity {
    fn name(&self) -> &str {
        "cosine_similarity"
    }
    fn signature(&self) -> &Signature {
        self.distance.signature()
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        self.distance.coerce_types(arg_types)
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let [a, b] = &args.args[..] else {
            return Err(DataFusionError::Plan(
                "cosine_similarity takes two arguments".into(),
            ));
        };
        let (a, b) = (a.to_array(rows)?, b.to_array(rows)?);
        let out = match (a.data_type(), b.data_type()) {
            (DataType::List(_), DataType::List(_)) => cosine_similarity_of::<i32>(&a, &b)?,
            (DataType::LargeList(_), DataType::LargeList(_)) => {
                cosine_similarity_of::<i64>(&a, &b)?
            }
            (l, r) => {
                return Err(DataFusionError::Internal(format!(
                    "cosine_similarity: unexpected types after coercion: {l} and {r}"
                )));
            }
        };
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    }
}

/// Row-wise cosine similarity of two `Float64` lists (after coercion).
fn cosine_similarity_of<O: arrow::array::OffsetSizeTrait>(
    a: &ArrayRef,
    b: &ArrayRef,
) -> DFResult<arrow::array::Float64Array> {
    use arrow::array::{AsArray, Float64Array};
    use arrow::datatypes::Float64Type;
    let (a, b) = (a.as_list::<O>(), b.as_list::<O>());
    let mut out = Float64Array::builder(a.len());
    for row in 0..a.len() {
        if a.is_null(row) || b.is_null(row) {
            out.append_null();
            continue;
        }
        let (x, y) = (a.value(row), b.value(row));
        if x.len() != y.len() {
            return Err(DataFusionError::Execution(format!(
                "cosine_similarity: arrays must have the same length, got {} and {}",
                x.len(),
                y.len()
            )));
        }
        if x.null_count() != 0 || y.null_count() != 0 {
            out.append_null();
            continue;
        }
        let x = x.as_primitive::<Float64Type>();
        let y = y.as_primitive::<Float64Type>();
        let (mut dot, mut xx, mut yy) = (0.0, 0.0, 0.0);
        for (p, q) in x.values().iter().zip(y.values()) {
            dot += p * q;
            xx += p * p;
            yy += q * q;
        }
        if xx == 0.0 || yy == 0.0 {
            out.append_null();
        } else {
            out.append_value(dot / (xx.sqrt() * yy.sqrt()));
        }
    }
    Ok(out.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn one_i64(sql: &str) -> Option<i64> {
        let ctx = crate::duckdb_test_ctx();
        register_coverage_fns(&ctx);
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

    async fn one_bool(sql: &str) -> Option<bool> {
        let ctx = crate::duckdb_test_ctx();
        register_coverage_fns(&ctx);
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
    async fn count_if_counts_true_only() {
        // true, false, true, NULL -> 2
        assert_eq!(
            one_i64("SELECT count_if(x) FROM (VALUES (true),(false),(true),(CAST(NULL AS boolean))) t(x)").await,
            Some(2)
        );
        assert_eq!(
            one_i64("SELECT count_if(x > 1) FROM (VALUES (1),(2),(3)) t(x)").await,
            Some(2)
        );
    }

    #[tokio::test]
    async fn element_at_array_matches_trino() {
        assert_eq!(
            one_i64("SELECT element_at(make_array(10,20,30), 2)").await,
            Some(20)
        );
        assert_eq!(
            one_i64("SELECT element_at(make_array(10,20,30), -1)").await,
            Some(30)
        );
        // out of bounds -> NULL
        assert_eq!(
            one_i64("SELECT element_at(make_array(10,20,30), 9)").await,
            None
        );
        assert_eq!(
            one_i64("SELECT element_at(make_array(10,20,30), 0)").await,
            None
        );
    }

    #[tokio::test]
    async fn element_at_map_returns_scalar_value() {
        assert_eq!(
            one_i64("SELECT element_at(MAP(make_array('a','b'), make_array(1,2)), 'b')").await,
            Some(2)
        );
        assert_eq!(
            one_i64("SELECT element_at(MAP(make_array('a'), make_array(1)), 'z')").await,
            None
        );
    }

    #[tokio::test]
    async fn contains_array_is_three_valued() {
        assert_eq!(
            one_bool("SELECT contains(make_array(1,2,3), 2)").await,
            Some(true)
        );
        assert_eq!(
            one_bool("SELECT contains(make_array(1,2,3), 9)").await,
            Some(false)
        );
        // absent + a NULL present -> NULL
        assert_eq!(
            one_bool("SELECT contains(make_array(1, CAST(NULL AS int), 3), 2)").await,
            None
        );
    }

    async fn one_f64(sql: &str) -> Option<f64> {
        let ctx = crate::duckdb_test_ctx();
        register_coverage_fns(&ctx);
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let a = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        (!a.is_null(0)).then(|| a.value(0))
    }

    #[tokio::test]
    async fn cosine_similarity_matches_trino() {
        let cases = [
            ("[1.0, 2.0], [2.0, 4.0]", 1.0),
            ("[1.0, 0.0], [0.0, 1.0]", 0.0),
            ("[1.0, 2.0], [-1.0, -2.0]", -1.0),
            ("[1.0, 2.0], [2.0, 3.0]", 8.0 / (5f64.sqrt() * 13f64.sqrt())),
            // integer and real lists coerce like cosine_distance
            ("[1, 2], [2, 4]", 1.0),
            (
                "arrow_cast([3.0, 4.0], 'FixedSizeList(2, Float32)'), [4.0, 3.0]",
                24.0 / 25.0,
            ),
        ];
        for (args, want) in cases {
            let got = one_f64(&format!("SELECT cosine_similarity({args})"))
                .await
                .unwrap_or_else(|| panic!("{args}: NULL"));
            assert!((got - want).abs() < 1e-12, "{args}: {got} != {want}");
        }
    }

    #[tokio::test]
    async fn cosine_similarity_null_cases_and_length_mismatch() {
        for args in [
            "[0.0, 0.0], [1.0, 2.0]",
            "CAST(NULL AS double[]), [1.0]",
            "[1.0, CAST(NULL AS double)], [1.0, 2.0]",
        ] {
            let got = one_f64(&format!("SELECT cosine_similarity({args})")).await;
            assert_eq!(got, None, "{args}");
        }
        let ctx = crate::duckdb_test_ctx();
        register_coverage_fns(&ctx);
        let err = match ctx.sql("SELECT cosine_similarity([1.0], [1.0, 2.0])").await {
            Ok(df) => df.collect().await.unwrap_err(),
            Err(e) => e,
        };
        assert!(err.to_string().contains("same length"), "{err}");
    }

    #[tokio::test]
    async fn cosine_similarity_is_one_minus_cosine_distance() {
        let got = one_f64(
            "SELECT cosine_similarity(v, w) + cosine_distance(v, w) \
             FROM (VALUES ([0.3, -1.2, 5.0], [2.0, 0.5, 1.5])) t(v, w)",
        )
        .await;
        assert!((got.unwrap() - 1.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn contains_string_still_works() {
        assert_eq!(
            one_bool("SELECT contains('hello world', 'world')").await,
            Some(true)
        );
        assert_eq!(
            one_bool("SELECT contains('hello', 'zzz')").await,
            Some(false)
        );
    }
}
