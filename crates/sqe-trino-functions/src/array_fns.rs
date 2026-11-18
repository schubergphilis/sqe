//! Trino array functions that DataFusion lacks or answers differently.
//!
//! - `array_first(array)` and `array_last(array)`; `array_first(array, pred)`
//!   keeps working (one higher-order function accepts both shapes, because a
//!   plain scalar `array_first` would win name resolution over DataFusion's
//!   lambda form).
//! - `array_remove(array, x)` removes every occurrence (Trino), where
//!   DataFusion's removes the first only.
//! - `zip`, `zip_with`, `array_histogram`, `contains_sequence`, `ngrams`,
//!   `shuffle`.
//!
//! Most of these run row by row over the flattened list values, using
//! `ScalarValue` for equality, so every element type works without a kernel per
//! type. Rows that are NULL give NULL.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, ListArray, StructArray, UInt64Array};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{cast, take, take_arrays};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::utils::{adjust_offsets_for_slice, list_values, take_function_args};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, ValueOrLambda, Volatility,
};
use datafusion::prelude::SessionContext;

// ─── shared helpers ──────────────────────────────────────────────────────────

/// The `item` field every list this module builds uses, so a function's
/// declared return type and the array it returns always agree.
pub(crate) fn item_field(dt: DataType) -> FieldRef {
    Arc::new(Field::new("item", dt, true))
}

/// The element field of any list type.
pub(crate) fn element_field(dt: &DataType, who: &str) -> DFResult<FieldRef> {
    match dt {
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::ListView(f)
        | DataType::LargeListView(f) => Ok(Arc::clone(f)),
        other => Err(DataFusionError::Plan(format!(
            "{who} expects an array, got {other}"
        ))),
    }
}

/// An array argument as a plain `List`, whatever list flavour it arrived as.
fn normalize_list(arr: &ArrayRef, who: &str) -> DFResult<ListArray> {
    if let DataType::List(_) = arr.data_type() {
        return Ok(arr.as_list::<i32>().clone());
    }
    let field = element_field(arr.data_type(), who)?;
    let cast = cast(arr.as_ref(), &DataType::List(field))?;
    Ok(cast.as_list::<i32>().clone())
}

/// A list column split into its flat values and per-row element ranges.
pub(crate) struct ListRows {
    list: ListArray,
    pub values: ArrayRef,
    offsets: OffsetBuffer<i32>,
}

impl ListRows {
    pub(crate) fn new(arr: &ArrayRef, who: &str) -> DFResult<Self> {
        let list = normalize_list(arr, who)?;
        let values = list_values(&list)?;
        let offsets = adjust_offsets_for_slice(&list);
        Ok(Self {
            list,
            values,
            offsets,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.list.len()
    }

    /// `[start, end)` into [`Self::values`], `None` for a NULL row.
    pub(crate) fn range(&self, row: usize) -> Option<(usize, usize)> {
        if self.list.is_null(row) {
            None
        } else {
            Some((self.offsets[row] as usize, self.offsets[row + 1] as usize))
        }
    }

    /// The flat values as scalars, for equality and hashing.
    pub(crate) fn scalars(&self) -> DFResult<Vec<ScalarValue>> {
        (0..self.values.len())
            .map(|i| ScalarValue::try_from_array(self.values.as_ref(), i))
            .collect()
    }
}

/// A list built by picking rows of `flat`: row `r` holds `flat[rows[r][..]]`,
/// or is NULL when `rows[r]` is `None`.
pub(crate) fn list_by_indices(
    field: FieldRef,
    flat: &ArrayRef,
    rows: &[Option<Vec<usize>>],
) -> DFResult<ArrayRef> {
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0i32);
    let mut picked: Vec<u64> = Vec::new();
    let mut valid = Vec::with_capacity(rows.len());
    for row in rows {
        match row {
            Some(idx) => {
                picked.extend(idx.iter().map(|i| *i as u64));
                valid.push(true);
            }
            None => valid.push(false),
        }
        offsets.push(picked.len() as i32);
    }
    let taken = take(flat.as_ref(), &UInt64Array::from(picked), None)?;
    Ok(Arc::new(ListArray::try_new(
        field,
        OffsetBuffer::new(offsets.into()),
        taken,
        Some(NullBuffer::from(valid)),
    )?))
}

/// A `map(key, value)` column from flat entry arrays and per-row entry
/// counts (`None` = NULL map). Shaped like DataFusion's `map()`: `entries`
/// holds non-null `key` and nullable `value`.
pub(crate) fn map_from_parts(
    keys: ArrayRef,
    values: ArrayRef,
    lens: &[Option<usize>],
) -> DFResult<ArrayRef> {
    let mut offsets = Vec::with_capacity(lens.len() + 1);
    offsets.push(0i32);
    let mut total = 0usize;
    for len in lens {
        total += len.unwrap_or(0);
        offsets.push(total as i32);
    }
    let fields = Fields::from(vec![
        Field::new("key", keys.data_type().clone(), false),
        Field::new("value", values.data_type().clone(), true),
    ]);
    let entries = StructArray::try_new(fields, vec![keys, values], None)?;
    let entries_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let nulls = lens
        .iter()
        .any(Option::is_none)
        .then(|| NullBuffer::from(lens.iter().map(Option::is_some).collect::<Vec<_>>()));
    Ok(Arc::new(arrow::array::MapArray::try_new(
        entries_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        nulls,
        false,
    )?))
}

/// A scalar function whose body is a plain function over its argument arrays.
/// Keeps each new Trino function to a return-type rule and one function.
#[derive(Debug)]
pub(crate) struct RowUdf {
    name: &'static str,
    aliases: Vec<String>,
    signature: Signature,
    ret: fn(&[DataType]) -> DFResult<DataType>,
    imp: fn(&[ArrayRef], usize) -> DFResult<ArrayRef>,
}

// Function pointers have no stable identity, so a RowUdf is equal to another by name.
impl PartialEq for RowUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for RowUdf {}
impl std::hash::Hash for RowUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl RowUdf {
    pub(crate) fn udf(
        name: &'static str,
        aliases: &[&str],
        signature: Signature,
        ret: fn(&[DataType]) -> DFResult<DataType>,
        imp: fn(&[ArrayRef], usize) -> DFResult<ArrayRef>,
    ) -> ScalarUDF {
        ScalarUDF::new_from_impl(Self {
            name,
            aliases: aliases.iter().map(|a| (*a).to_string()).collect(),
            signature,
            ret,
            imp,
        })
    }
}

impl ScalarUDFImpl for RowUdf {
    fn name(&self) -> &str {
        self.name
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        (self.ret)(arg_types)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arrays = args
            .args
            .iter()
            .map(|a| a.to_array(args.number_rows))
            .collect::<DFResult<Vec<_>>>()?;
        Ok(ColumnarValue::Array((self.imp)(&arrays, args.number_rows)?))
    }
}

fn arity(name: &str, args: &[DataType], want: usize) -> DFResult<()> {
    if args.len() == want {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} takes {want} arguments, got {}",
            args.len()
        )))
    }
}

/// `Int64` view of an integer argument, with NULL kept.
fn int_arg(arr: &ArrayRef, who: &str) -> DFResult<Int64Array> {
    let cast = cast(arr.as_ref(), &DataType::Int64).map_err(|_| {
        DataFusionError::Plan(format!("{who} expects an integer, got {}", arr.data_type()))
    })?;
    Ok(cast.as_primitive::<arrow::datatypes::Int64Type>().clone())
}

// ─── array_last ──────────────────────────────────────────────────────────────

fn array_last_ret(args: &[DataType]) -> DFResult<DataType> {
    arity("array_last", args, 1)?;
    Ok(element_field(&args[0], "array_last")?.data_type().clone())
}

fn array_last(args: &[ArrayRef], _rows: usize) -> DFResult<ArrayRef> {
    let rows = ListRows::new(&args[0], "array_last")?;
    let idx: UInt64Array = (0..rows.len())
        .map(|i| {
            rows.range(i)
                .and_then(|(s, e)| (e > s).then(|| (e - 1) as u64))
        })
        .collect();
    Ok(take(rows.values.as_ref(), &idx, None)?)
}

// ─── array_first (one-argument form, and the lambda form) ───────────────────

/// `array_first(array)` is the first element (NULL for an empty array);
/// `array_first(array, x -> pred)` is DataFusion's, which this delegates to.
/// One higher-order function owns the name because a scalar one would win name
/// resolution and hide the lambda form.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayFirst {
    signature: HigherOrderSignature,
    aliases: Vec<String>,
    inner: datafusion::functions_nested::array_first::ArrayFirst,
}

impl ArrayFirst {
    fn new() -> Self {
        Self {
            signature: HigherOrderSignature::user_defined(Volatility::Immutable),
            aliases: vec!["list_first".to_string()],
            inner: datafusion::functions_nested::array_first::ArrayFirst::new(),
        }
    }
}

impl HigherOrderUDFImpl for ArrayFirst {
    fn name(&self) -> &str {
        "array_first"
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }
    fn coerce_value_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        self.inner.coerce_value_types(arg_types)
    }
    fn lambda_parameters(
        &self,
        step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> DFResult<LambdaParametersProgress> {
        if fields.len() == 1 {
            return Ok(LambdaParametersProgress::Complete(vec![]));
        }
        self.inner.lambda_parameters(step, fields)
    }
    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> DFResult<FieldRef> {
        if let [ValueOrLambda::Value(list)] = args.arg_fields {
            let element = element_field(list.data_type(), "array_first")?;
            return Ok(Arc::new(
                element.as_ref().clone().with_name("").with_nullable(true),
            ));
        }
        self.inner.return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> DFResult<ColumnarValue> {
        let [ValueOrLambda::Value(list)] = &args.args[..] else {
            return self.inner.invoke_with_args(args);
        };
        let rows = ListRows::new(&list.to_array(args.number_rows)?, "array_first")?;
        let idx: UInt64Array = (0..rows.len())
            .map(|i| rows.range(i).and_then(|(s, e)| (e > s).then_some(s as u64)))
            .collect();
        Ok(ColumnarValue::Array(take(
            rows.values.as_ref(),
            &idx,
            None,
        )?))
    }
}

// ─── array_remove ────────────────────────────────────────────────────────────

/// Trino's `array_remove` removes every occurrence; DataFusion's removes the
/// first. This is `array_remove_all` under the name `array_remove`, holding
/// the `list_remove` alias the built-in held so a session rebuild cannot
/// bring the built-in back.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayRemove {
    inner: Arc<ScalarUDF>,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for ArrayRemove {
    fn name(&self) -> &str {
        "array_remove"
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.inner.return_type(arg_types)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        self.inner.return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }
}

// ─── zip ─────────────────────────────────────────────────────────────────────

/// The anonymous row field type of one `zip` slot.
fn zip_row_type(args: &[DataType]) -> DFResult<DataType> {
    if !(2..=5).contains(&args.len()) {
        return Err(DataFusionError::Plan(format!(
            "zip takes 2 to 5 arrays, got {}",
            args.len()
        )));
    }
    let fields = args
        .iter()
        .map(|a| {
            Ok(Field::new(
                "",
                element_field(a, "zip")?.data_type().clone(),
                true,
            ))
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(DataType::Struct(Fields::from(fields)))
}

fn zip_ret(args: &[DataType]) -> DFResult<DataType> {
    Ok(DataType::List(item_field(zip_row_type(args)?)))
}

/// `zip(a, b, ...)`: an array of rows holding the n-th element of each array;
/// the shorter arrays are padded with NULL. The row fields are unnamed, as
/// Trino's are (`array(row(integer, varchar))`).
fn zip(args: &[ArrayRef], _rows: usize) -> DFResult<ArrayRef> {
    let inputs = args
        .iter()
        .map(|a| ListRows::new(a, "zip"))
        .collect::<DFResult<Vec<_>>>()?;
    let n = inputs[0].len();
    let mut picks: Vec<Vec<Option<u64>>> = vec![Vec::new(); inputs.len()];
    let mut offsets = Vec::with_capacity(n + 1);
    offsets.push(0i32);
    let mut valid = Vec::with_capacity(n);
    let mut total = 0usize;
    for row in 0..n {
        let ranges: Vec<_> = inputs.iter().map(|i| i.range(row)).collect();
        if ranges.iter().any(Option::is_none) {
            valid.push(false);
        } else {
            valid.push(true);
            let ranges: Vec<(usize, usize)> = ranges.into_iter().flatten().collect();
            let longest = ranges.iter().map(|(s, e)| e - s).max().unwrap_or(0);
            for k in 0..longest {
                for (slot, (s, e)) in ranges.iter().enumerate() {
                    picks[slot].push((s + k < *e).then(|| (s + k) as u64));
                }
            }
            total += longest;
        }
        offsets.push(total as i32);
    }
    let columns = inputs
        .iter()
        .zip(picks)
        .map(|(input, pick)| Ok(take(input.values.as_ref(), &UInt64Array::from(pick), None)?))
        .collect::<DFResult<Vec<ArrayRef>>>()?;
    let DataType::Struct(fields) = zip_row_type(
        &args
            .iter()
            .map(|a| a.data_type().clone())
            .collect::<Vec<_>>(),
    )?
    else {
        return Err(DataFusionError::Internal("zip: row type".into()));
    };
    let entries = StructArray::try_new(fields, columns, None)?;
    Ok(Arc::new(ListArray::try_new(
        item_field(entries.data_type().clone()),
        OffsetBuffer::new(offsets.into()),
        Arc::new(entries),
        Some(NullBuffer::from(valid)),
    )?))
}

// ─── zip_with ────────────────────────────────────────────────────────────────

/// `zip_with(a, b, (x, y) -> expr)`: the lambda over the n-th elements of
/// both arrays, the shorter one padded with NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ZipWith {
    signature: HigherOrderSignature,
}

impl HigherOrderUDFImpl for ZipWith {
    fn name(&self) -> &str {
        "zip_with"
    }
    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }
    fn coerce_value_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        arg_types
            .iter()
            .map(|t| match t {
                DataType::List(_) | DataType::LargeList(_) => Ok(t.clone()),
                other => Ok(DataType::List(element_field(other, "zip_with")?)),
            })
            .collect()
    }
    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> DFResult<LambdaParametersProgress> {
        let [ValueOrLambda::Value(a), ValueOrLambda::Value(b), _] = fields else {
            return Err(DataFusionError::Plan(
                "zip_with expects (array, array, lambda)".into(),
            ));
        };
        Ok(LambdaParametersProgress::Complete(vec![vec![
            element_field(a.data_type(), "zip_with")?,
            element_field(b.data_type(), "zip_with")?,
        ]]))
    }
    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> DFResult<FieldRef> {
        let [_, _, ValueOrLambda::Lambda(out)] = args.arg_fields else {
            return Err(DataFusionError::Plan(
                "zip_with expects (array, array, lambda)".into(),
            ));
        };
        Ok(Arc::new(Field::new(
            "",
            DataType::List(item_field(out.data_type().clone())),
            true,
        )))
    }
    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> DFResult<ColumnarValue> {
        let [ValueOrLambda::Value(a), ValueOrLambda::Value(b), ValueOrLambda::Lambda(lambda)] =
            take_function_args(self.name(), &args.args)?
        else {
            return Err(DataFusionError::Execution(
                "zip_with expects (array, array, lambda)".into(),
            ));
        };
        let n = args.number_rows;
        let a = ListRows::new(&a.to_array(n)?, "zip_with")?;
        let b = ListRows::new(&b.to_array(n)?, "zip_with")?;

        let mut pick_a: Vec<Option<u64>> = Vec::new();
        let mut pick_b: Vec<Option<u64>> = Vec::new();
        let mut row_of: Vec<u64> = Vec::new();
        let mut offsets = Vec::with_capacity(n + 1);
        offsets.push(0i32);
        let mut valid = Vec::with_capacity(n);
        for row in 0..n {
            if let (Some((as_, ae)), Some((bs, be))) = (a.range(row), b.range(row)) {
                valid.push(true);
                for k in 0..(ae - as_).max(be - bs) {
                    pick_a.push((as_ + k < ae).then(|| (as_ + k) as u64));
                    pick_b.push((bs + k < be).then(|| (bs + k) as u64));
                    row_of.push(row as u64);
                }
            } else {
                valid.push(false);
            }
            offsets.push(row_of.len() as i32);
        }
        let xs = take(a.values.as_ref(), &UInt64Array::from(pick_a), None)?;
        let ys = take(b.values.as_ref(), &UInt64Array::from(pick_b), None)?;
        let total = row_of.len();
        let row_of = UInt64Array::from(row_of);
        let x_cl = || Ok(Arc::clone(&xs));
        let y_cl = || Ok(Arc::clone(&ys));
        let out = lambda
            .evaluate(&[&x_cl, &y_cl], |caps| {
                Ok(take_arrays(caps, &row_of, None)?)
            })?
            .into_array(total)?;
        Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
            item_field(out.data_type().clone()),
            OffsetBuffer::new(offsets.into()),
            out,
            Some(NullBuffer::from(valid)),
        )?)))
    }
}

// ─── array_histogram ─────────────────────────────────────────────────────────

fn array_histogram_ret(args: &[DataType]) -> DFResult<DataType> {
    arity("array_histogram", args, 1)?;
    let key = element_field(&args[0], "array_histogram")?;
    Ok(crate::map_aggregates::map_type_for(
        key.data_type(),
        &DataType::Int64,
    ))
}

/// `array_histogram(x)`: `map(element, bigint)` counting each distinct
/// element, NULL elements ignored. Keys are in first-seen order.
fn array_histogram(args: &[ArrayRef], _rows: usize) -> DFResult<ArrayRef> {
    let rows = ListRows::new(&args[0], "array_histogram")?;
    let scalars = rows.scalars()?;
    let mut first_seen: Vec<u64> = Vec::new();
    let mut counts: Vec<i64> = Vec::new();
    let mut lens = Vec::with_capacity(rows.len());
    for row in 0..rows.len() {
        let Some((s, e)) = rows.range(row) else {
            lens.push(None);
            continue;
        };
        let mut slot: HashMap<&ScalarValue, usize> = HashMap::new();
        let before = first_seen.len();
        for (i, value) in scalars.iter().enumerate().take(e).skip(s) {
            if value.is_null() {
                continue;
            }
            match slot.get(value) {
                Some(at) => counts[*at] += 1,
                None => {
                    slot.insert(value, counts.len());
                    first_seen.push(i as u64);
                    counts.push(1);
                }
            }
        }
        lens.push(Some(first_seen.len() - before));
    }
    let keys = take(rows.values.as_ref(), &UInt64Array::from(first_seen), None)?;
    map_from_parts(keys, Arc::new(Int64Array::from(counts)), &lens)
}

// ─── contains_sequence ───────────────────────────────────────────────────────

fn contains_sequence_ret(args: &[DataType]) -> DFResult<DataType> {
    arity("contains_sequence", args, 2)?;
    Ok(DataType::Boolean)
}

/// `contains_sequence(x, seq)`: whether `seq` appears in `x` as a run of
/// adjacent elements. An empty `seq` is contained; NULL elements match NULL
/// elements.
fn contains_sequence(args: &[ArrayRef], _rows: usize) -> DFResult<ArrayRef> {
    let haystack = ListRows::new(&args[0], "contains_sequence")?;
    let needle = ListRows::new(&args[1], "contains_sequence")?;
    let needle_values = cast(needle.values.as_ref(), haystack.values.data_type())?;
    let hay = haystack.scalars()?;
    let ndl = (0..needle_values.len())
        .map(|i| ScalarValue::try_from_array(needle_values.as_ref(), i))
        .collect::<DFResult<Vec<_>>>()?;
    let out: arrow::array::BooleanArray = (0..haystack.len())
        .map(|row| {
            let ((hs, he), (ns, ne)) = (haystack.range(row)?, needle.range(row)?);
            let want = &ndl[ns..ne];
            Some(want.is_empty() || hay[hs..he].windows(want.len()).any(|w| w == want))
        })
        .collect();
    Ok(Arc::new(out))
}

// ─── ngrams ──────────────────────────────────────────────────────────────────

fn ngrams_ret(args: &[DataType]) -> DFResult<DataType> {
    arity("ngrams", args, 2)?;
    let element = element_field(&args[0], "ngrams")?;
    Ok(DataType::List(item_field(DataType::List(item_field(
        element.data_type().clone(),
    )))))
}

/// `ngrams(array, n)`: every window of `n` adjacent elements. An array
/// shorter than `n` gives one window, the whole array.
fn ngrams(args: &[ArrayRef], _rows: usize) -> DFResult<ArrayRef> {
    let rows = ListRows::new(&args[0], "ngrams")?;
    let widths = int_arg(&args[1], "ngrams")?;
    let mut windows: Vec<Option<Vec<usize>>> = Vec::new();
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0i32);
    let mut valid = Vec::with_capacity(rows.len());
    for row in 0..rows.len() {
        match (
            rows.range(row),
            widths.is_valid(row).then(|| widths.value(row)),
        ) {
            (Some((s, e)), Some(n)) => {
                if n <= 0 {
                    return Err(DataFusionError::Execution(
                        "N must be greater than zero".into(),
                    ));
                }
                let n = n as usize;
                if n > e - s {
                    windows.push(Some((s..e).collect()));
                } else {
                    for start in s..=(e - n) {
                        windows.push(Some((start..start + n).collect()));
                    }
                }
                valid.push(true);
            }
            _ => valid.push(false),
        }
        offsets.push(windows.len() as i32);
    }
    let element = item_field(rows.values.data_type().clone());
    let inner = list_by_indices(Arc::clone(&element), &rows.values, &windows)?;
    Ok(Arc::new(ListArray::try_new(
        item_field(inner.data_type().clone()),
        OffsetBuffer::new(offsets.into()),
        inner,
        Some(NullBuffer::from(valid)),
    )?))
}

// ─── shuffle ─────────────────────────────────────────────────────────────────

fn shuffle_ret(args: &[DataType]) -> DFResult<DataType> {
    arity("shuffle", args, 1)?;
    Ok(DataType::List(item_field(
        element_field(&args[0], "shuffle")?.data_type().clone(),
    )))
}

/// A random 64-bit seed from the standard library's per-instance hash keys.
fn random_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u8(0);
    hasher.finish()
}

/// splitmix64 step.
fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// `shuffle(array)`: a uniformly random permutation (Fisher-Yates), fresh per
/// row. Volatile.
fn shuffle(args: &[ArrayRef], _rows: usize) -> DFResult<ArrayRef> {
    let rows = ListRows::new(&args[0], "shuffle")?;
    let mut state = random_seed();
    let picks: Vec<Option<Vec<usize>>> = (0..rows.len())
        .map(|row| {
            rows.range(row).map(|(s, e)| {
                let mut idx: Vec<usize> = (s..e).collect();
                for i in (1..idx.len()).rev() {
                    let j = (next_random(&mut state) % (i as u64 + 1)) as usize;
                    idx.swap(i, j);
                }
                idx
            })
        })
        .collect();
    let field = item_field(rows.values.data_type().clone());
    list_by_indices(field, &rows.values, &picks)
}

// ─── registration ────────────────────────────────────────────────────────────

/// Register the functions in this module. Call after DataFusion's built-ins.
pub fn register_array_fns(ctx: &SessionContext) {
    let imm = Volatility::Immutable;
    ctx.register_udf(RowUdf::udf(
        "array_last",
        &["list_last"],
        Signature::any(1, imm),
        array_last_ret,
        array_last,
    ));
    ctx.register_higher_order_function(Arc::new(HigherOrderUDF::new_from_impl(ArrayFirst::new())));
    // Trino semantics: removes every occurrence, not just the first.
    ctx.register_udf(ScalarUDF::new_from_impl(ArrayRemove {
        inner: datafusion::functions_nested::remove::array_remove_all_udf(),
        aliases: vec!["list_remove".to_string()],
    }));
    ctx.register_udf(RowUdf::udf(
        "zip",
        &[],
        Signature::variadic_any(imm),
        zip_ret,
        zip,
    ));
    ctx.register_higher_order_function(Arc::new(HigherOrderUDF::new_from_impl(ZipWith {
        signature: HigherOrderSignature::exact(
            vec![
                ValueOrLambda::Value(()),
                ValueOrLambda::Value(()),
                ValueOrLambda::Lambda(()),
            ],
            imm,
        ),
    })));
    ctx.register_udf(RowUdf::udf(
        "array_histogram",
        &[],
        Signature::any(1, imm),
        array_histogram_ret,
        array_histogram,
    ));
    ctx.register_udf(RowUdf::udf(
        "contains_sequence",
        &[],
        Signature::any(2, imm),
        contains_sequence_ret,
        contains_sequence,
    ));
    ctx.register_udf(RowUdf::udf(
        "ngrams",
        &[],
        Signature::any(2, imm),
        ngrams_ret,
        ngrams,
    ));
    ctx.register_udf(RowUdf::udf(
        "shuffle",
        &[],
        Signature::any(1, Volatility::Volatile),
        shuffle_ret,
        shuffle,
    ));
}

#[cfg(test)]
pub(crate) mod test_support {
    use arrow::array::Array;
    use arrow::util::display::array_value_to_string;
    use sqe_trino_protocol::encode::arrow_to_declared_trino_type;

    /// Run `sql` in a fully registered DuckDB-dialect session; the first
    /// column as one string per row (`NULL` for SQL NULL) and its Trino type.
    pub(crate) async fn query(sql: &str) -> (Vec<String>, String) {
        let ctx = crate::duckdb_test_ctx();
        crate::register_trino_functions(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut values = Vec::new();
        let mut ty = String::new();
        for batch in &batches {
            let col = batch.column(0);
            ty = arrow_to_declared_trino_type(col.data_type());
            for i in 0..col.len() {
                values.push(if col.is_null(i) {
                    "NULL".to_string()
                } else {
                    array_value_to_string(col, i).unwrap()
                });
            }
        }
        (values, ty)
    }

    /// One-row convenience: the value and the Trino type.
    pub(crate) async fn one(sql: &str) -> (String, String) {
        let (values, ty) = query(sql).await;
        assert_eq!(values.len(), 1, "{sql}");
        (values.into_iter().next().unwrap(), ty)
    }

    /// The error text of a failing query (at plan or run time).
    pub(crate) async fn err(sql: &str) -> String {
        let ctx = crate::duckdb_test_ctx();
        crate::register_trino_functions(&ctx);
        match ctx.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => df.collect().await.unwrap_err().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{err, one, query};

    #[tokio::test]
    async fn array_first_and_last() {
        assert_eq!(
            one("SELECT array_first(make_array(5, 6, 7))").await,
            ("5".into(), "bigint".into())
        );
        assert_eq!(
            one("SELECT array_last(make_array(5, 6, 7))").await,
            ("7".into(), "bigint".into())
        );
        // Empty array and NULL array give NULL; the type is the element type.
        assert_eq!(
            one("SELECT array_last(arrow_cast(make_array(), 'List(Int32)'))").await,
            ("NULL".into(), "integer".into())
        );
        assert_eq!(
            one("SELECT array_first(CAST(NULL AS BIGINT[]))").await.0,
            "NULL"
        );
        // The lambda form still works.
        assert_eq!(
            one("SELECT array_first(make_array(1, 2, 3, 4), x -> x > 2)").await,
            ("3".into(), "bigint".into())
        );
        let (rows, _) = query(
            "SELECT array_last(a) FROM (VALUES (make_array(1, 2)), (make_array(3)), (CAST(NULL AS BIGINT[]))) t(a)",
        )
        .await;
        assert_eq!(rows, ["2", "3", "NULL"]);
        let (rows, _) = query(
            "SELECT array_first(a) FROM (VALUES (make_array(1, 2)), (make_array(3)), (CAST(NULL AS BIGINT[]))) t(a)",
        )
        .await;
        assert_eq!(rows, ["1", "3", "NULL"]);
    }

    #[tokio::test]
    async fn array_remove_removes_every_occurrence() {
        assert_eq!(
            one("SELECT array_remove(make_array(1, 2, 1, 3, 1), 1)").await,
            ("[2, 3]".into(), "array(bigint)".into())
        );
        assert_eq!(
            one("SELECT list_remove(make_array(1, 2, 1), 1)").await.0,
            "[2]"
        );
        let (rows, _) = query(
            "SELECT array_remove(a, 1) FROM (VALUES (make_array(1, 1, 2)), (make_array(1)), (CAST(NULL AS BIGINT[]))) t(a)",
        )
        .await;
        assert_eq!(rows, ["[2]", "[]", "NULL"]);
    }

    #[tokio::test]
    async fn zip_pads_and_types_rows_anonymously() {
        assert_eq!(
            one("SELECT zip(make_array(1, 2), make_array('a', 'b'))").await,
            (
                "[{: 1, : a}, {: 2, : b}]".to_string(),
                "array(row(bigint,varchar))".into()
            )
        );
        let (_, ty) = one("SELECT zip(make_array(1), make_array('a'), make_array(true))").await;
        assert_eq!(ty, "array(row(bigint,varchar,boolean))");
        let (rows, _) = query(
            "SELECT cardinality(zip(a, b)) FROM (VALUES (make_array(1, 2, 3), make_array('x')), (make_array(1), make_array('x', 'y')), (CAST(NULL AS BIGINT[]), make_array('x'))) t(a, b)",
        )
        .await;
        assert_eq!(rows, ["3", "2", "NULL"]);
    }

    #[tokio::test]
    async fn zip_with_runs_the_lambda_over_padded_pairs() {
        let (v, ty) = one(
            "SELECT zip_with(make_array(1, 2, 3), make_array(10, 20), (x, y) -> coalesce(x, 0) + coalesce(y, 0))",
        )
        .await;
        assert_eq!((v.as_str(), ty.as_str()), ("[11, 22, 3]", "array(bigint)"));
        // Unequal lengths per row, a captured outer column, and a NULL row.
        let (rows, _) = query(
            "SELECT zip_with(a, b, (x, y) -> coalesce(x, 0) * 100 + coalesce(y, 0) + k) FROM (VALUES (make_array(1, 2), make_array(5), 1000), (make_array(3), make_array(6, 7), 2000), (CAST(NULL AS BIGINT[]), make_array(1), 3000)) t(a, b, k)",
        )
        .await;
        assert_eq!(rows, ["[1105, 1200]", "[2306, 2007]", "NULL"]);
        let (v, ty) = one("SELECT zip_with(make_array(1), make_array('a'), (x, y) -> y)").await;
        assert_eq!((v.as_str(), ty.as_str()), ("[a]", "array(varchar)"));
    }

    #[tokio::test]
    async fn array_histogram_counts_non_null_elements() {
        let (v, ty) = one("SELECT array_histogram(make_array(1, 1, 2, 1, NULL))").await;
        assert_eq!(
            (v.as_str(), ty.as_str()),
            ("{1: 3, 2: 1}", "map(bigint,bigint)")
        );
        let (rows, _) = query(
            "SELECT array_histogram(a) FROM (VALUES (make_array('x', 'y', 'x')), (CAST(NULL AS VARCHAR[]))) t(a)",
        )
        .await;
        assert_eq!(rows, ["{x: 2, y: 1}", "NULL"]);
        let (_, ty) = one("SELECT array_histogram(make_array('x'))").await;
        assert_eq!(ty, "map(varchar,bigint)");
    }

    #[tokio::test]
    async fn contains_sequence_needs_adjacent_elements() {
        assert_eq!(
            one("SELECT contains_sequence(make_array(1, 2, 3, 4), make_array(2, 3))").await,
            ("true".into(), "boolean".into())
        );
        assert_eq!(
            one("SELECT contains_sequence(make_array(1, 2, 3, 4), make_array(2, 4))")
                .await
                .0,
            "false"
        );
        assert_eq!(
            one("SELECT contains_sequence(make_array(1, 2), arrow_cast(make_array(), 'List(Int64)'))").await.0,
            "true"
        );
        let (rows, _) = query(
            "SELECT contains_sequence(a, make_array(2, 3)) FROM (VALUES (make_array(1, 2, 3)), (make_array(3, 2)), (CAST(NULL AS BIGINT[]))) t(a)",
        )
        .await;
        assert_eq!(rows, ["true", "false", "NULL"]);
    }

    #[tokio::test]
    async fn ngrams_slides_a_window() {
        let (v, ty) = one("SELECT ngrams(make_array('a', 'b', 'c', 'd'), 3)").await;
        assert_eq!(
            (v.as_str(), ty.as_str()),
            ("[[a, b, c], [b, c, d]]", "array(array(varchar))")
        );
        assert_eq!(
            one("SELECT ngrams(make_array(1, 2), 5)").await.0,
            "[[1, 2]]"
        );
        let (rows, _) = query(
            "SELECT ngrams(a, 2) FROM (VALUES (make_array(1, 2, 3)), (CAST(NULL AS BIGINT[]))) t(a)",
        )
        .await;
        assert_eq!(rows, ["[[1, 2], [2, 3]]", "NULL"]);
        assert!(err("SELECT ngrams(make_array(1), 0)")
            .await
            .contains("greater than zero"));
    }

    #[tokio::test]
    async fn shuffle_permutes_and_keeps_the_type() {
        let (v, ty) = one("SELECT array_sort(shuffle(make_array(1, 2, 3, 4, 5)))").await;
        assert_eq!(
            (v.as_str(), ty.as_str()),
            ("[1, 2, 3, 4, 5]", "array(bigint)")
        );
        let (rows, _) = query(
            "SELECT cardinality(shuffle(a)) FROM (VALUES (make_array(1, 2, 3)), (CAST(NULL AS BIGINT[]))) t(a)",
        )
        .await;
        assert_eq!(rows, ["3", "NULL"]);
        // A 20-element shuffle is not the identity (odds of 1 in 20! otherwise).
        let (v, _) = one("SELECT shuffle(range(1, 21)) = range(1, 21)").await;
        assert_eq!(v, "false");
    }
}
