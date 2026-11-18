//! Trino map functions that DataFusion lacks or answers differently.
//!
//! - `map_entries(map)` as `array(row(K, V))` with anonymous row fields.
//! - `map_concat(map, ...)`, later maps win.
//! - `map_filter`, `transform_keys`, `transform_values` (lambdas over
//!   `(key, value)`).
//! - `split_to_map`, `split_to_multimap` (varchar to map).
//! - `map_from_entries`, `multimap_from_entries` (array of rows to map).
//!
//! Map keys compare as `ScalarValue`, so every key type works. A NULL map is
//! NULL out; a NULL key is an error, as in Trino.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, ListArray, StringArray, StructArray, UInt64Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{cast, filter, interleave, take, take_arrays};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::utils::take_function_args;
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature, HigherOrderUDF,
    HigherOrderUDFImpl, LambdaParametersProgress, Signature, ValueOrLambda, Volatility,
};
use datafusion::prelude::SessionContext;

use crate::array_fns::{item_field, list_by_indices, map_from_parts, ListRows, RowUdf};
use crate::map_aggregates::map_type_for;

fn exec_err(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(msg.into())
}

/// The key and value fields of a map type.
fn kv_fields(dt: &DataType, who: &str) -> DFResult<(FieldRef, FieldRef)> {
    if let DataType::Map(entries, _) = dt {
        if let DataType::Struct(fields) = entries.data_type() {
            if fields.len() == 2 {
                return Ok((Arc::clone(&fields[0]), Arc::clone(&fields[1])));
            }
        }
    }
    Err(DataFusionError::Plan(format!(
        "{who} expects a map, got {dt}"
    )))
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

/// The entries of the non-NULL rows of a map column, flattened, with the
/// row each entry came from. Entries under NULL rows are left out so a
/// lambda never runs on them.
struct Entries {
    keys: ArrayRef,
    values: ArrayRef,
    /// Entry count per row, `None` for a NULL map.
    lens: Vec<Option<usize>>,
    row_of: Vec<u64>,
}

fn entries_of(arr: &ArrayRef, who: &str) -> DFResult<Entries> {
    let map = arr.as_map_opt().ok_or_else(|| {
        DataFusionError::Plan(format!("{who} expects a map, got {}", arr.data_type()))
    })?;
    let offsets = map.value_offsets();
    let mut picked: Vec<u64> = Vec::new();
    let mut row_of: Vec<u64> = Vec::new();
    let mut lens = Vec::with_capacity(map.len());
    for row in 0..map.len() {
        if map.is_null(row) {
            lens.push(None);
            continue;
        }
        let (s, e) = (offsets[row] as usize, offsets[row + 1] as usize);
        picked.extend((s..e).map(|i| i as u64));
        row_of.extend(std::iter::repeat_n(row as u64, e - s));
        lens.push(Some(e - s));
    }
    let picked = UInt64Array::from(picked);
    Ok(Entries {
        keys: take(map.keys().as_ref(), &picked, None)?,
        values: take(map.values().as_ref(), &picked, None)?,
        lens,
        row_of,
    })
}

fn scalars_of(arr: &ArrayRef) -> DFResult<Vec<ScalarValue>> {
    (0..arr.len())
        .map(|i| ScalarValue::try_from_array(arr.as_ref(), i))
        .collect()
}

// ─── map_entries ─────────────────────────────────────────────────────────────

fn entry_row_type(map: &DataType) -> DFResult<DataType> {
    let (k, v) = kv_fields(map, "map_entries")?;
    Ok(DataType::Struct(Fields::from(vec![
        Field::new("", k.data_type().clone(), true),
        Field::new("", v.data_type().clone(), true),
    ])))
}

fn map_entries_ret(args: &[DataType]) -> DFResult<DataType> {
    arity("map_entries", args, 1)?;
    Ok(DataType::List(item_field(entry_row_type(&args[0])?)))
}

/// `map_entries(map)`: `array(row(K, V))`. Trino's row fields are anonymous;
/// DataFusion's built-in names them `key` and `value`.
fn map_entries(args: &[ArrayRef], _rows: usize) -> DFResult<ArrayRef> {
    let entries = entries_of(&args[0], "map_entries")?;
    let DataType::Struct(fields) = entry_row_type(args[0].data_type())? else {
        return Err(DataFusionError::Internal("map_entries: row type".into()));
    };
    let rows = StructArray::try_new(fields, vec![entries.keys, entries.values], None)?;
    let mut offsets = vec![0i32];
    let mut total = 0usize;
    for len in &entries.lens {
        total += len.unwrap_or(0);
        offsets.push(total as i32);
    }
    let nulls =
        entries.lens.iter().any(Option::is_none).then(|| {
            NullBuffer::from(entries.lens.iter().map(Option::is_some).collect::<Vec<_>>())
        });
    Ok(Arc::new(ListArray::try_new(
        item_field(rows.data_type().clone()),
        OffsetBuffer::new(offsets.into()),
        Arc::new(rows),
        nulls,
    )?))
}

// ─── map_concat ──────────────────────────────────────────────────────────────

fn map_concat_ret(args: &[DataType]) -> DFResult<DataType> {
    let Some(first) = args.first() else {
        return Err(DataFusionError::Plan(
            "map_concat takes at least one map".into(),
        ));
    };
    let (k, v) = kv_fields(first, "map_concat")?;
    for other in &args[1..] {
        let (ok, ov) = kv_fields(other, "map_concat")?;
        if ok.data_type() != k.data_type() || ov.data_type() != v.data_type() {
            return Err(DataFusionError::Plan(format!(
                "map_concat needs maps of one type, got {first} and {other}"
            )));
        }
    }
    Ok(map_type_for(k.data_type(), v.data_type()))
}

/// `map_concat(m1, m2, ...)`: the entries of every map; a key in more than
/// one map takes the value of the last, and keeps the place of its first
/// appearance. Any NULL map gives NULL.
fn map_concat(args: &[ArrayRef], rows: usize) -> DFResult<ArrayRef> {
    let maps = args
        .iter()
        .map(|a| {
            a.as_map_opt()
                .ok_or_else(|| exec_err("map_concat expects maps"))
        })
        .collect::<DFResult<Vec<_>>>()?;
    let keys: Vec<Vec<ScalarValue>> = maps
        .iter()
        .map(|m| scalars_of(m.keys()))
        .collect::<DFResult<_>>()?;
    let mut picks: Vec<(usize, usize)> = Vec::new();
    let mut lens = Vec::with_capacity(rows);
    for row in 0..rows {
        if maps.iter().any(|m| m.is_null(row)) {
            lens.push(None);
            continue;
        }
        let before = picks.len();
        let mut at: HashMap<&ScalarValue, usize> = HashMap::new();
        for (mi, map) in maps.iter().enumerate() {
            let offsets = map.value_offsets();
            #[allow(clippy::needless_range_loop)] // `e` also builds the pick tuple
            for e in offsets[row] as usize..offsets[row + 1] as usize {
                match at.get(&keys[mi][e]) {
                    Some(pos) => picks[*pos] = (mi, e),
                    None => {
                        at.insert(&keys[mi][e], picks.len());
                        picks.push((mi, e));
                    }
                }
            }
        }
        lens.push(Some(picks.len() - before));
    }
    let key_arrays: Vec<&dyn Array> = maps.iter().map(|m| m.keys().as_ref()).collect();
    let value_arrays: Vec<&dyn Array> = maps.iter().map(|m| m.values().as_ref()).collect();
    map_from_parts(
        interleave(&key_arrays, &picks)?,
        interleave(&value_arrays, &picks)?,
        &lens,
    )
}

// ─── map_filter, transform_keys, transform_values ───────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MapLambdaKind {
    Filter,
    TransformKeys,
    TransformValues,
}

/// A lambda over `(key, value)` of every map entry.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MapLambda {
    name: &'static str,
    kind: MapLambdaKind,
    signature: HigherOrderSignature,
}

impl MapLambda {
    fn udf(name: &'static str, kind: MapLambdaKind) -> HigherOrderUDF {
        HigherOrderUDF::new_from_impl(Self {
            name,
            kind,
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
        })
    }
}

impl HigherOrderUDFImpl for MapLambda {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }
    fn coerce_value_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        for t in arg_types {
            kv_fields(t, self.name)?;
        }
        Ok(arg_types.to_vec())
    }
    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> DFResult<LambdaParametersProgress> {
        let [ValueOrLambda::Value(map), _] = fields else {
            return Err(DataFusionError::Plan(format!(
                "{} expects (map, lambda)",
                self.name
            )));
        };
        let (k, v) = kv_fields(map.data_type(), self.name)?;
        Ok(LambdaParametersProgress::Complete(vec![vec![k, v]]))
    }
    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> DFResult<FieldRef> {
        let [ValueOrLambda::Value(map), ValueOrLambda::Lambda(out)] = args.arg_fields else {
            return Err(DataFusionError::Plan(format!(
                "{} expects (map, lambda)",
                self.name
            )));
        };
        let (k, v) = kv_fields(map.data_type(), self.name)?;
        let ty = match self.kind {
            MapLambdaKind::Filter => map_type_for(k.data_type(), v.data_type()),
            MapLambdaKind::TransformKeys => map_type_for(out.data_type(), v.data_type()),
            MapLambdaKind::TransformValues => map_type_for(k.data_type(), out.data_type()),
        };
        Ok(Arc::new(Field::new("", ty, true)))
    }
    fn invoke_with_args(
        &self,
        args: HigherOrderFunctionArgs,
    ) -> DFResult<datafusion::logical_expr::ColumnarValue> {
        let [ValueOrLambda::Value(map), ValueOrLambda::Lambda(lambda)] =
            take_function_args(self.name, &args.args)?
        else {
            return Err(exec_err(format!("{} expects (map, lambda)", self.name)));
        };
        let entries = entries_of(&map.to_array(args.number_rows)?, self.name)?;
        let total = entries.keys.len();
        let row_of = UInt64Array::from(entries.row_of.clone());
        let key_cl = || Ok(Arc::clone(&entries.keys));
        let value_cl = || Ok(Arc::clone(&entries.values));
        let out = lambda
            .evaluate(&[&key_cl, &value_cl], |caps| {
                Ok(take_arrays(caps, &row_of, None)?)
            })?
            .into_array(total)?;
        let result = match self.kind {
            MapLambdaKind::Filter => {
                let pred = out
                    .as_boolean_opt()
                    .ok_or_else(|| exec_err("map_filter: the predicate must be boolean"))?;
                let keep: BooleanArray = (0..total)
                    .map(|i| Some(pred.is_valid(i) && pred.value(i)))
                    .collect();
                let mut at = 0usize;
                let lens: Vec<Option<usize>> = entries
                    .lens
                    .iter()
                    .map(|len| {
                        len.map(|n| {
                            let kept = (at..at + n).filter(|i| keep.value(*i)).count();
                            at += n;
                            kept
                        })
                    })
                    .collect();
                map_from_parts(
                    filter(entries.keys.as_ref(), &keep)?,
                    filter(entries.values.as_ref(), &keep)?,
                    &lens,
                )?
            }
            MapLambdaKind::TransformValues => {
                map_from_parts(Arc::clone(&entries.keys), out, &entries.lens)?
            }
            MapLambdaKind::TransformKeys => {
                check_keys(&out, &entries.lens)?;
                map_from_parts(out, Arc::clone(&entries.values), &entries.lens)?
            }
        };
        Ok(datafusion::logical_expr::ColumnarValue::Array(result))
    }
}

/// Error on a NULL key, or on a repeated key within one row's `lens` range.
fn check_keys(keys: &ArrayRef, lens: &[Option<usize>]) -> DFResult<()> {
    if keys.null_count() > 0 {
        return Err(exec_err("map key cannot be null"));
    }
    let scalars = scalars_of(keys)?;
    let mut at = 0usize;
    for n in lens.iter().flatten() {
        let mut seen: HashMap<&ScalarValue, ()> = HashMap::new();
        for k in &scalars[at..at + n] {
            if seen.insert(k, ()).is_some() {
                return Err(exec_err(format!(
                    "Duplicate map keys ({k}) are not allowed"
                )));
            }
        }
        at += n;
    }
    Ok(())
}

// ─── split_to_map, split_to_multimap ─────────────────────────────────────────

fn varchar_arg(arr: &ArrayRef, who: &str) -> DFResult<StringArray> {
    let cast = cast(arr.as_ref(), &DataType::Utf8).map_err(|_| {
        DataFusionError::Plan(format!("{who} expects varchar, got {}", arr.data_type()))
    })?;
    Ok(cast.as_string::<i32>().clone())
}

/// Split every non-NULL row of `args` into `(key, value)` pairs. A NULL
/// argument gives a NULL row.
#[allow(clippy::type_complexity)]
fn split_pairs(args: &[ArrayRef], who: &str) -> DFResult<Vec<Option<Vec<(String, String)>>>> {
    let text = varchar_arg(&args[0], who)?;
    let entry_delim = varchar_arg(&args[1], who)?;
    let kv_delim = varchar_arg(&args[2], who)?;
    (0..text.len())
        .map(|row| {
            if text.is_null(row) || entry_delim.is_null(row) || kv_delim.is_null(row) {
                return Ok(None);
            }
            let (entry_d, kv_d) = (entry_delim.value(row), kv_delim.value(row));
            if entry_d.is_empty() {
                return Err(exec_err("entryDelimiter is empty"));
            }
            if kv_d.is_empty() {
                return Err(exec_err("keyValueDelimiter is empty"));
            }
            if entry_d == kv_d {
                return Err(exec_err(
                    "entryDelimiter and keyValueDelimiter must not be the same",
                ));
            }
            let s = text.value(row);
            if s.is_empty() {
                return Ok(Some(Vec::new()));
            }
            s.split(entry_d)
                .map(|entry| {
                    let mut parts = entry.split(kv_d);
                    match (parts.next(), parts.next(), parts.next()) {
                        (Some(k), Some(v), None) => Ok((k.to_string(), v.to_string())),
                        _ => Err(exec_err(format!(
                            "Key-value delimiter must appear exactly once in each entry. Bad input: '{entry}'"
                        ))),
                    }
                })
                .collect::<DFResult<Vec<_>>>()
                .map(Some)
        })
        .collect()
}

fn split_to_map_ret(args: &[DataType]) -> DFResult<DataType> {
    arity("split_to_map", args, 3)?;
    Ok(map_type_for(&DataType::Utf8, &DataType::Utf8))
}

/// `split_to_map(s, entryDelim, kvDelim)`: `map(varchar, varchar)`. A key
/// that appears twice is an error.
fn split_to_map(args: &[ArrayRef], _rows: usize) -> DFResult<ArrayRef> {
    let rows = split_pairs(args, "split_to_map")?;
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut lens = Vec::with_capacity(rows.len());
    for row in &rows {
        let Some(pairs) = row else {
            lens.push(None);
            continue;
        };
        let mut seen: HashMap<&str, ()> = HashMap::new();
        for (k, v) in pairs {
            if seen.insert(k.as_str(), ()).is_some() {
                return Err(exec_err(format!("Duplicate keys ({k}) are not allowed")));
            }
            keys.push(k.as_str());
            values.push(v.as_str());
        }
        lens.push(Some(pairs.len()));
    }
    map_from_parts(
        Arc::new(StringArray::from(keys)),
        Arc::new(StringArray::from(values)),
        &lens,
    )
}

fn split_to_multimap_ret(args: &[DataType]) -> DFResult<DataType> {
    arity("split_to_multimap", args, 3)?;
    Ok(map_type_for(
        &DataType::Utf8,
        &DataType::List(item_field(DataType::Utf8)),
    ))
}

/// `split_to_multimap(s, entryDelim, kvDelim)`: `map(varchar, array(varchar))`,
/// the values of a repeated key kept in the order they appear.
fn split_to_multimap(args: &[ArrayRef], _rows: usize) -> DFResult<ArrayRef> {
    let rows = split_pairs(args, "split_to_multimap")?;
    let mut keys: Vec<&str> = Vec::new();
    let mut all_values: Vec<&str> = Vec::new();
    let mut groups: Vec<Option<Vec<usize>>> = Vec::new();
    let mut lens = Vec::with_capacity(rows.len());
    for row in &rows {
        let Some(pairs) = row else {
            lens.push(None);
            continue;
        };
        let mut at: HashMap<&str, usize> = HashMap::new();
        let before = keys.len();
        for (k, v) in pairs {
            let slot = *at.entry(k.as_str()).or_insert_with(|| {
                keys.push(k.as_str());
                groups.push(Some(Vec::new()));
                groups.len() - 1
            });
            if let Some(g) = groups[slot].as_mut() {
                g.push(all_values.len());
            }
            all_values.push(v.as_str());
        }
        lens.push(Some(keys.len() - before));
    }
    let flat: ArrayRef = Arc::new(StringArray::from(all_values));
    let values = list_by_indices(item_field(DataType::Utf8), &flat, &groups)?;
    map_from_parts(Arc::new(StringArray::from(keys)), values, &lens)
}

// ─── map_from_entries, multimap_from_entries ────────────────────────────────

/// The key and value types of an `array(row(K, V))`.
fn entry_types(arr: &DataType, who: &str) -> DFResult<(DataType, DataType)> {
    let element = crate::array_fns::element_field(arr, who)?;
    match element.data_type() {
        DataType::Struct(f) if f.len() == 2 => {
            Ok((f[0].data_type().clone(), f[1].data_type().clone()))
        }
        other => Err(DataFusionError::Plan(format!(
            "{who} expects array(row(K, V)), got array({other})"
        ))),
    }
}

fn map_from_entries_ret(args: &[DataType]) -> DFResult<DataType> {
    arity("map_from_entries", args, 1)?;
    let (k, v) = entry_types(&args[0], "map_from_entries")?;
    Ok(map_type_for(&k, &v))
}

fn multimap_from_entries_ret(args: &[DataType]) -> DFResult<DataType> {
    arity("multimap_from_entries", args, 1)?;
    let (k, v) = entry_types(&args[0], "multimap_from_entries")?;
    Ok(map_type_for(&k, &DataType::List(item_field(v))))
}

/// The key and value columns of the entry rows, after checking that no
/// entry and no key is NULL.
fn entry_columns(rows: &ListRows, who: &str) -> DFResult<(ArrayRef, ArrayRef)> {
    let entries = rows
        .values
        .as_struct_opt()
        .ok_or_else(|| DataFusionError::Plan(format!("{who} expects an array of rows")))?;
    // Only the entries of non-NULL rows matter, a NULL row's may be anything.
    for row in 0..rows.len() {
        if let Some((s, e)) = rows.range(row) {
            for i in s..e {
                if entries.is_null(i) {
                    return Err(exec_err("map entry cannot be null"));
                }
                if entries.column(0).is_null(i) {
                    return Err(exec_err("map key cannot be null"));
                }
            }
        }
    }
    Ok((Arc::clone(entries.column(0)), Arc::clone(entries.column(1))))
}

/// `map_from_entries(array(row(K, V)))`: a repeated key is an error.
fn map_from_entries(args: &[ArrayRef], _rows: usize) -> DFResult<ArrayRef> {
    let rows = ListRows::new(&args[0], "map_from_entries")?;
    let (keys, values) = entry_columns(&rows, "map_from_entries")?;
    let key_scalars = scalars_of(&keys)?;
    let mut picked: Vec<u64> = Vec::new();
    let mut lens = Vec::with_capacity(rows.len());
    for row in 0..rows.len() {
        let Some((s, e)) = rows.range(row) else {
            lens.push(None);
            continue;
        };
        let mut seen: HashMap<&ScalarValue, ()> = HashMap::new();
        #[allow(clippy::needless_range_loop)] // `i` is the position in the key array
        for i in s..e {
            if seen.insert(&key_scalars[i], ()).is_some() {
                return Err(exec_err(format!(
                    "Duplicate map keys ({}) are not allowed",
                    key_scalars[i]
                )));
            }
            picked.push(i as u64);
        }
        lens.push(Some(e - s));
    }
    let picked = UInt64Array::from(picked);
    map_from_parts(
        take(keys.as_ref(), &picked, None)?,
        take(values.as_ref(), &picked, None)?,
        &lens,
    )
}

/// `multimap_from_entries(array(row(K, V)))`: `map(K, array(V))`, the values
/// of a repeated key in the order they appear.
fn multimap_from_entries(args: &[ArrayRef], _rows: usize) -> DFResult<ArrayRef> {
    let rows = ListRows::new(&args[0], "multimap_from_entries")?;
    let (keys, values) = entry_columns(&rows, "multimap_from_entries")?;
    let key_scalars = scalars_of(&keys)?;
    let mut first: Vec<u64> = Vec::new();
    let mut groups: Vec<Option<Vec<usize>>> = Vec::new();
    let mut lens = Vec::with_capacity(rows.len());
    for row in 0..rows.len() {
        let Some((s, e)) = rows.range(row) else {
            lens.push(None);
            continue;
        };
        let before = first.len();
        let mut at: HashMap<&ScalarValue, usize> = HashMap::new();
        #[allow(clippy::needless_range_loop)] // `i` is the position pushed to `first`
        for i in s..e {
            let slot = *at.entry(&key_scalars[i]).or_insert_with(|| {
                first.push(i as u64);
                groups.push(Some(Vec::new()));
                groups.len() - 1
            });
            if let Some(g) = groups[slot].as_mut() {
                g.push(i);
            }
        }
        lens.push(Some(first.len() - before));
    }
    let value_lists = list_by_indices(item_field(values.data_type().clone()), &values, &groups)?;
    map_from_parts(
        take(keys.as_ref(), &UInt64Array::from(first), None)?,
        value_lists,
        &lens,
    )
}

// ─── registration ────────────────────────────────────────────────────────────

/// Register the functions in this module. Call after DataFusion's built-ins.
pub fn register_map_fns(ctx: &SessionContext) {
    let imm = Volatility::Immutable;
    // Trino semantics: anonymous row fields, where the built-in names them.
    ctx.register_udf(RowUdf::udf(
        "map_entries",
        &[],
        Signature::any(1, imm),
        map_entries_ret,
        map_entries,
    ));
    ctx.register_udf(RowUdf::udf(
        "map_concat",
        &[],
        Signature::variadic_any(imm),
        map_concat_ret,
        map_concat,
    ));
    for (name, kind) in [
        ("map_filter", MapLambdaKind::Filter),
        ("transform_keys", MapLambdaKind::TransformKeys),
        ("transform_values", MapLambdaKind::TransformValues),
    ] {
        ctx.register_higher_order_function(Arc::new(MapLambda::udf(name, kind)));
    }
    ctx.register_udf(RowUdf::udf(
        "split_to_map",
        &[],
        Signature::any(3, imm),
        split_to_map_ret,
        split_to_map,
    ));
    ctx.register_udf(RowUdf::udf(
        "split_to_multimap",
        &[],
        Signature::any(3, imm),
        split_to_multimap_ret,
        split_to_multimap,
    ));
    ctx.register_udf(RowUdf::udf(
        "map_from_entries",
        &[],
        Signature::any(1, imm),
        map_from_entries_ret,
        map_from_entries,
    ));
    ctx.register_udf(RowUdf::udf(
        "multimap_from_entries",
        &[],
        Signature::any(1, imm),
        multimap_from_entries_ret,
        multimap_from_entries,
    ));
}

#[cfg(test)]
mod tests {
    use crate::array_fns::test_support::{err, one, query};

    const MAP_AB: &str = "map(make_array('a', 'b'), make_array(1, 2))";

    #[tokio::test]
    async fn map_entries_has_anonymous_row_fields() {
        let (v, ty) = one(&format!("SELECT map_entries({MAP_AB})")).await;
        assert_eq!(
            (v.as_str(), ty.as_str()),
            ("[{: a, : 1}, {: b, : 2}]", "array(row(varchar,bigint))")
        );
        let (rows, _) = query(
            "SELECT cardinality(map_entries(CASE WHEN i = 1 THEN map(make_array('a'), make_array(1)) END)) FROM (VALUES (1), (2)) t(i)",
        )
        .await;
        assert_eq!(rows, ["1", "NULL"]);
    }

    #[tokio::test]
    async fn map_concat_later_maps_win() {
        let (v, ty) = one(
            "SELECT map_concat(map(make_array('a', 'b'), make_array(1, 2)), map(make_array('b', 'c'), make_array(20, 30)))",
        )
        .await;
        assert_eq!(
            (v.as_str(), ty.as_str()),
            ("{a: 1, b: 20, c: 30}", "map(varchar,bigint)")
        );
        let (rows, _) = query(
            "SELECT map_concat(CASE WHEN i = 1 THEN map(make_array('a'), make_array(1)) END, map(make_array('z'), make_array(9))) FROM (VALUES (1), (2)) t(i)",
        )
        .await;
        assert_eq!(rows, ["{a: 1, z: 9}", "NULL"]);
    }

    #[tokio::test]
    async fn map_filter_and_transforms() {
        let (v, ty) = one(&format!("SELECT map_filter({MAP_AB}, (k, v) -> v > 1)")).await;
        assert_eq!((v.as_str(), ty.as_str()), ("{b: 2}", "map(varchar,bigint)"));
        let (v, ty) = one(&format!(
            "SELECT transform_values({MAP_AB}, (k, v) -> v * 10)"
        ))
        .await;
        assert_eq!(
            (v.as_str(), ty.as_str()),
            ("{a: 10, b: 20}", "map(varchar,bigint)")
        );
        let (v, ty) = one(&format!(
            "SELECT transform_keys({MAP_AB}, (k, v) -> upper(k))"
        ))
        .await;
        assert_eq!(
            (v.as_str(), ty.as_str()),
            ("{A: 1, B: 2}", "map(varchar,bigint)")
        );
        // The value type follows the lambda.
        let (_, ty) = one(&format!("SELECT transform_values({MAP_AB}, (k, v) -> k)")).await;
        assert_eq!(ty, "map(varchar,varchar)");
        // A captured column, unequal map sizes, and a NULL row.
        let (rows, _) = query(
            "SELECT map_filter(CASE i WHEN 1 THEN map(make_array('a', 'b'), make_array(1, 5)) WHEN 2 THEN map(make_array('c'), make_array(3)) END, (k, v) -> v > n) FROM (VALUES (1, 2), (2, 10), (3, 0)) t(i, n)",
        )
        .await;
        assert_eq!(rows, ["{b: 5}", "{}", "NULL"]);
        let (rows, _) = query(
            "SELECT transform_values(CASE WHEN i = 1 THEN map(make_array('a', 'b'), make_array(1, 5)) END, (k, v) -> v + n) FROM (VALUES (1, 100), (2, 0)) t(i, n)",
        )
        .await;
        assert_eq!(rows, ["{a: 101, b: 105}", "NULL"]);
        assert!(
            err(&format!("SELECT transform_keys({MAP_AB}, (k, v) -> 'x')"))
                .await
                .contains("Duplicate map keys")
        );
    }

    #[tokio::test]
    async fn split_to_map_parses_and_rejects_duplicates() {
        let (v, ty) = one("SELECT split_to_map('a:1,b:2', ',', ':')").await;
        assert_eq!(
            (v.as_str(), ty.as_str()),
            ("{a: 1, b: 2}", "map(varchar,varchar)")
        );
        assert_eq!(one("SELECT split_to_map('', ',', ':')").await.0, "{}");
        assert!(err("SELECT split_to_map('a:1,a:2', ',', ':')")
            .await
            .contains("Duplicate keys (a)"));
        assert!(err("SELECT split_to_map('a', ',', ':')")
            .await
            .contains("exactly once"));
        let (rows, _) = query(
            "SELECT split_to_map(s, ',', '=') FROM (VALUES ('x=1'), (CAST(NULL AS VARCHAR))) t(s)",
        )
        .await;
        assert_eq!(rows, ["{x: 1}", "NULL"]);
    }

    #[tokio::test]
    async fn split_to_multimap_groups_values() {
        let (v, ty) = one("SELECT split_to_multimap('a:1,b:2,a:3', ',', ':')").await;
        assert_eq!(
            (v.as_str(), ty.as_str()),
            ("{a: [1, 3], b: [2]}", "map(varchar,array(varchar))")
        );
        let (rows, _) = query(
            "SELECT split_to_multimap(s, ',', ':') FROM (VALUES ('k:v'), (CAST(NULL AS VARCHAR))) t(s)",
        )
        .await;
        assert_eq!(rows, ["{k: [v]}", "NULL"]);
    }

    #[tokio::test]
    async fn from_entries_builds_maps() {
        let entries = "make_array(row(1, 'a'), row(2, 'b'))";
        let (v, ty) = one(&format!("SELECT map_from_entries({entries})")).await;
        assert_eq!(
            (v.as_str(), ty.as_str()),
            ("{1: a, 2: b}", "map(bigint,varchar)")
        );
        let (v, ty) =
            one("SELECT multimap_from_entries(make_array(row(1, 'a'), row(1, 'b'), row(2, 'c')))")
                .await;
        assert_eq!(
            (v.as_str(), ty.as_str()),
            ("{1: [a, b], 2: [c]}", "map(bigint,array(varchar))")
        );
        assert!(
            err("SELECT map_from_entries(make_array(row(1, 'a'), row(1, 'b')))")
                .await
                .contains("Duplicate map keys (1)")
        );
        assert!(
            err("SELECT map_from_entries(make_array(row(CAST(NULL AS BIGINT), 'a')))")
                .await
                .contains("map key cannot be null")
        );
        let (rows, _) = query(
            "SELECT map_from_entries(CASE WHEN i = 1 THEN make_array(row(1, 'x')) END) FROM (VALUES (1), (2)) t(i)",
        )
        .await;
        assert_eq!(rows, ["{1: x}", "NULL"]);
    }
}
