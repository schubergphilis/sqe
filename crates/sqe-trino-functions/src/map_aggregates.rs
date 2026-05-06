//! Trino Map-producing aggregates: `map_agg`, `multimap_agg`, `map_union`.
//!
//! Three UDAFs that complete the Map-aggregate family alongside the
//! `histogram(x)` UDAF in the sibling `histogram` module.
//!
//! - `map_agg(k, v)` returns `MAP<typeof(k), typeof(v)>`. Aggregates
//!   `(k, v)` pairs into a single Map. Per Trino's spec, behaviour on
//!   duplicate keys is implementation-defined; we keep the last value
//!   seen (matches DuckDB and Snowflake behaviour). NULL keys are
//!   skipped; NULL values are kept.
//!
//! - `multimap_agg(k, v)` returns `MAP<typeof(k), ARRAY<typeof(v)>>`.
//!   Groups all values per distinct key into an Array. Order within
//!   each array follows the row order Trino observes (we preserve
//!   insertion order). NULL keys are skipped; NULL values land in the
//!   array.
//!
//! - `map_union(m)` takes a `MAP<K, V>` column and returns
//!   `MAP<K, V>` containing the union of every input map's entries.
//!   Per Trino's spec, behaviour on duplicate keys is again
//!   implementation-defined; we keep the last value seen.
//!
//! Type genericity follows the same pattern as `histogram`: store
//! entries as `Vec<(ScalarValue, ScalarValue)>` (or
//! `Vec<(ScalarValue, Vec<ScalarValue>)>` for multimap) and
//! materialize typed keys / values arrays at finalization via
//! `ScalarValue::iter_to_array`. Multi-phase aggregation state is a
//! `List<Struct{key, value}>` per partial accumulator.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ListArray, MapArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

// ─── Shared helpers ────────────────────────────────────────────────────────

/// Build the `MAP<K, V>` DataType for the given key + value type pair.
fn map_type_for(key_type: &DataType, value_type: &DataType) -> DataType {
    let entries_struct = DataType::Struct(Fields::from(vec![
        Field::new("key", key_type.clone(), false),
        Field::new("value", value_type.clone(), true),
    ]));
    DataType::Map(
        Arc::new(Field::new("entries", entries_struct, false)),
        // sorted = false: Trino does not require entry ordering.
        false,
    )
}

/// Build the state-field shape `List<Struct{key, value}>` used by
/// `map_agg` and `map_union`. `value_type` is the leaf value type for
/// these flat aggregates.
fn flat_state_field(name: String, key_type: DataType, value_type: DataType) -> FieldRef {
    let entries_struct = DataType::Struct(Fields::from(vec![
        Field::new("key", key_type, false),
        Field::new("value", value_type, true),
    ]));
    Arc::new(Field::new(
        name,
        DataType::List(Arc::new(Field::new("item", entries_struct, true))),
        true,
    ))
}

/// Materialize a single-row `MapArray` from typed keys + values arrays.
/// Caller is responsible for ensuring keys.len() == values.len().
fn build_map_array(
    keys: ArrayRef,
    values: ArrayRef,
    key_type: &DataType,
    value_type: &DataType,
) -> DFResult<MapArray> {
    if keys.len() != values.len() {
        return Err(DataFusionError::Internal(format!(
            "build_map_array: keys.len()={} values.len()={}",
            keys.len(),
            values.len()
        )));
    }
    let n_entries = keys.len();
    let entries_struct = StructArray::new(
        Fields::from(vec![
            Field::new("key", key_type.clone(), false),
            Field::new("value", value_type.clone(), true),
        ]),
        vec![keys, values],
        None,
    );
    let entries_field = Arc::new(Field::new(
        "entries",
        entries_struct.data_type().clone(),
        false,
    ));
    let offsets = OffsetBuffer::new(vec![0i32, n_entries as i32].into());
    Ok(MapArray::new(
        entries_field,
        offsets,
        entries_struct,
        None,
        false,
    ))
}

/// Materialize keys / values arrays from a Vec<(ScalarValue, ScalarValue)>.
/// Empty input produces zero-row arrays of the right types.
fn materialize_kv_arrays(
    entries: &[(ScalarValue, ScalarValue)],
    key_type: &DataType,
    value_type: &DataType,
) -> DFResult<(ArrayRef, ArrayRef)> {
    let keys: ArrayRef = if entries.is_empty() {
        arrow::array::new_empty_array(key_type)
    } else {
        ScalarValue::iter_to_array(entries.iter().map(|(k, _)| k.clone())).map_err(
            |e| {
                DataFusionError::Execution(format!(
                    "map aggregate: failed to materialize keys: {e}"
                ))
            },
        )?
    };
    let values: ArrayRef = if entries.is_empty() {
        arrow::array::new_empty_array(value_type)
    } else {
        ScalarValue::iter_to_array(entries.iter().map(|(_, v)| v.clone())).map_err(
            |e| {
                DataFusionError::Execution(format!(
                    "map aggregate: failed to materialize values: {e}"
                ))
            },
        )?
    };
    Ok((keys, values))
}

// ─── map_agg(k, v) ─────────────────────────────────────────────────────────

/// `map_agg(k, v)` aggregate. Returns `MAP<typeof(k), typeof(v)>`.
#[derive(Debug)]
pub(crate) struct MapAgg {
    signature: Signature,
}

impl MapAgg {
    pub(crate) fn udaf() -> AggregateUDF {
        AggregateUDF::from(Self {
            signature: Signature::any(2, Volatility::Immutable),
        })
    }
}

impl PartialEq for MapAgg {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for MapAgg {}
impl std::hash::Hash for MapAgg {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

impl AggregateUDFImpl for MapAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "map_agg"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        if args.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "map_agg(k, v) takes exactly 2 arguments, got {}",
                args.len()
            )));
        }
        Ok(map_type_for(&args[0], &args[1]))
    }
    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let key_type = acc_args.expr_fields[0].data_type().clone();
        let value_type = acc_args.expr_fields[1].data_type().clone();
        Ok(Box::new(MapAggAccumulator {
            entries: Vec::new(),
            key_type,
            value_type,
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![flat_state_field(
            format_state_name(args.name, "entries"),
            args.input_fields[0].data_type().clone(),
            args.input_fields[1].data_type().clone(),
        )])
    }
}

#[derive(Debug)]
struct MapAggAccumulator {
    entries: Vec<(ScalarValue, ScalarValue)>,
    key_type: DataType,
    value_type: DataType,
}

impl MapAggAccumulator {
    /// Insert `(key, value)`, replacing any existing entry with the same
    /// key. Trino's spec leaves duplicate-key behaviour up to the
    /// implementation; "last wins" matches DuckDB and Snowflake.
    fn put(&mut self, key: ScalarValue, value: ScalarValue) {
        if key.is_null() {
            return;
        }
        for entry in &mut self.entries {
            if entry.0 == key {
                entry.1 = value;
                return;
            }
        }
        self.entries.push((key, value));
    }
}

impl Accumulator for MapAggAccumulator {
    fn update_batch(&mut self, args: &[ArrayRef]) -> DFResult<()> {
        if args.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "map_agg::update_batch expected 2 arrays, got {}",
                args.len()
            )));
        }
        let keys = &args[0];
        let values = &args[1];
        for i in 0..keys.len() {
            if keys.is_null(i) {
                continue;
            }
            let key = ScalarValue::try_from_array(keys, i)?;
            let value = ScalarValue::try_from_array(values, i)?;
            self.put(key, value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let (keys, values) =
            materialize_kv_arrays(&self.entries, &self.key_type, &self.value_type)?;
        let map = build_map_array(keys, values, &self.key_type, &self.value_type)?;
        Ok(ScalarValue::Map(Arc::new(map)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .entries
                .iter()
                .map(|(k, v)| k.size() + v.size())
                .sum::<usize>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let (keys, values) =
            materialize_kv_arrays(&self.entries, &self.key_type, &self.value_type)?;
        let entries_struct = StructArray::new(
            Fields::from(vec![
                Field::new("key", self.key_type.clone(), false),
                Field::new("value", self.value_type.clone(), true),
            ]),
            vec![keys, values],
            None,
        );
        let item_field = Arc::new(Field::new(
            "item",
            entries_struct.data_type().clone(),
            true,
        ));
        let offsets = OffsetBuffer::new(vec![0i32, self.entries.len() as i32].into());
        let list = ListArray::new(item_field, offsets, Arc::new(entries_struct), None);
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        merge_flat_state(states, |key, value| self.put(key, value))
    }
}

// ─── multimap_agg(k, v) ────────────────────────────────────────────────────

/// `multimap_agg(k, v)` aggregate. Returns `MAP<typeof(k), ARRAY<typeof(v)>>`.
#[derive(Debug)]
pub(crate) struct MultimapAgg {
    signature: Signature,
}

impl MultimapAgg {
    pub(crate) fn udaf() -> AggregateUDF {
        AggregateUDF::from(Self {
            signature: Signature::any(2, Volatility::Immutable),
        })
    }
}

impl PartialEq for MultimapAgg {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for MultimapAgg {}
impl std::hash::Hash for MultimapAgg {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

impl AggregateUDFImpl for MultimapAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "multimap_agg"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        if args.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "multimap_agg(k, v) takes exactly 2 arguments, got {}",
                args.len()
            )));
        }
        let value_array_type =
            DataType::List(Arc::new(Field::new("item", args[1].clone(), true)));
        Ok(map_type_for(&args[0], &value_array_type))
    }
    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let key_type = acc_args.expr_fields[0].data_type().clone();
        let value_type = acc_args.expr_fields[1].data_type().clone();
        Ok(Box::new(MultimapAggAccumulator {
            entries: Vec::new(),
            key_type,
            value_type,
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        // State stores raw (key, single-value) pairs even though the output
        // groups values into arrays per key. Multi-phase merge appends the
        // partial's entries one (k, v) at a time, which is correct for
        // multimap_agg's order-preserving semantics.
        Ok(vec![flat_state_field(
            format_state_name(args.name, "entries"),
            args.input_fields[0].data_type().clone(),
            args.input_fields[1].data_type().clone(),
        )])
    }
}

#[derive(Debug)]
struct MultimapAggAccumulator {
    entries: Vec<(ScalarValue, Vec<ScalarValue>)>,
    key_type: DataType,
    value_type: DataType,
}

impl MultimapAggAccumulator {
    /// Append `value` to the slot for `key`, creating the slot if needed.
    /// NULL keys are skipped; NULL values are kept.
    fn append(&mut self, key: ScalarValue, value: ScalarValue) {
        if key.is_null() {
            return;
        }
        for entry in &mut self.entries {
            if entry.0 == key {
                entry.1.push(value);
                return;
            }
        }
        self.entries.push((key, vec![value]));
    }
}

impl Accumulator for MultimapAggAccumulator {
    fn update_batch(&mut self, args: &[ArrayRef]) -> DFResult<()> {
        if args.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "multimap_agg::update_batch expected 2 arrays, got {}",
                args.len()
            )));
        }
        let keys = &args[0];
        let values = &args[1];
        for i in 0..keys.len() {
            if keys.is_null(i) {
                continue;
            }
            let key = ScalarValue::try_from_array(keys, i)?;
            let value = ScalarValue::try_from_array(values, i)?;
            self.append(key, value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        // Build keys (one per distinct key) and values (one List<V> per key).
        let n_keys = self.entries.len();
        let item_field = Arc::new(Field::new("item", self.value_type.clone(), true));
        let value_array_type = DataType::List(Arc::clone(&item_field));

        let keys: ArrayRef = if n_keys == 0 {
            arrow::array::new_empty_array(&self.key_type)
        } else {
            ScalarValue::iter_to_array(self.entries.iter().map(|(k, _)| k.clone()))
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "multimap_agg: keys materialize failed: {e}"
                    ))
                })?
        };

        // Flatten all per-key Vec<V> into a single contiguous values array,
        // and build offsets so the inner ListArray can slice the right
        // run for each key.
        let mut flat_values: Vec<ScalarValue> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(n_keys + 1);
        offsets.push(0);
        for (_, v_vec) in &self.entries {
            flat_values.extend(v_vec.iter().cloned());
            offsets.push(flat_values.len() as i32);
        }
        let inner_values_arr: ArrayRef = if flat_values.is_empty() {
            arrow::array::new_empty_array(&self.value_type)
        } else {
            ScalarValue::iter_to_array(flat_values).map_err(|e| {
                DataFusionError::Execution(format!(
                    "multimap_agg: values materialize failed: {e}"
                ))
            })?
        };
        let values_list: ArrayRef = Arc::new(ListArray::new(
            item_field,
            OffsetBuffer::new(offsets.into()),
            inner_values_arr,
            None,
        ));

        let map = build_map_array(keys, values_list, &self.key_type, &value_array_type)?;
        Ok(ScalarValue::Map(Arc::new(map)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .entries
                .iter()
                .map(|(k, vs)| {
                    k.size() + vs.iter().map(ScalarValue::size).sum::<usize>()
                })
                .sum::<usize>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        // Flatten the (key, Vec<value>) entries back into raw (k, v) pairs
        // so the state shape stays a flat List<Struct{k, v}> (matches the
        // shape declared in `state_fields`). The merge step reconstructs
        // the per-key Vec<V> by walking these in order.
        let mut flat_keys: Vec<ScalarValue> = Vec::new();
        let mut flat_values: Vec<ScalarValue> = Vec::new();
        for (k, vs) in &self.entries {
            for v in vs {
                flat_keys.push(k.clone());
                flat_values.push(v.clone());
            }
        }
        let n = flat_keys.len();

        let keys_arr: ArrayRef = if flat_keys.is_empty() {
            arrow::array::new_empty_array(&self.key_type)
        } else {
            ScalarValue::iter_to_array(flat_keys).map_err(|e| {
                DataFusionError::Execution(format!(
                    "multimap_agg::state keys materialize failed: {e}"
                ))
            })?
        };
        let values_arr: ArrayRef = if flat_values.is_empty() {
            arrow::array::new_empty_array(&self.value_type)
        } else {
            ScalarValue::iter_to_array(flat_values).map_err(|e| {
                DataFusionError::Execution(format!(
                    "multimap_agg::state values materialize failed: {e}"
                ))
            })?
        };

        let entries_struct = StructArray::new(
            Fields::from(vec![
                Field::new("key", self.key_type.clone(), false),
                Field::new("value", self.value_type.clone(), true),
            ]),
            vec![keys_arr, values_arr],
            None,
        );
        let item_field = Arc::new(Field::new(
            "item",
            entries_struct.data_type().clone(),
            true,
        ));
        let offsets = OffsetBuffer::new(vec![0i32, n as i32].into());
        let list = ListArray::new(item_field, offsets, Arc::new(entries_struct), None);
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        merge_flat_state(states, |key, value| self.append(key, value))
    }
}

// ─── map_union(map) ────────────────────────────────────────────────────────

/// `map_union(m)` aggregate. Returns `MAP<K, V>` containing every entry
/// from every input map. Duplicate keys keep the last value seen.
#[derive(Debug)]
pub(crate) struct MapUnion {
    signature: Signature,
}

impl MapUnion {
    pub(crate) fn udaf() -> AggregateUDF {
        AggregateUDF::from(Self {
            signature: Signature::any(1, Volatility::Immutable),
        })
    }
}

impl PartialEq for MapUnion {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for MapUnion {}
impl std::hash::Hash for MapUnion {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

impl AggregateUDFImpl for MapUnion {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "map_union"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        if args.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "map_union(m) takes exactly 1 argument, got {}",
                args.len()
            )));
        }
        // Same Map type as the input.
        Ok(args[0].clone())
    }
    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let map_type = acc_args.expr_fields[0].data_type().clone();
        let (key_type, value_type) = decompose_map_type(&map_type)?;
        Ok(Box::new(MapUnionAccumulator {
            entries: Vec::new(),
            key_type,
            value_type,
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        let (key_type, value_type) =
            decompose_map_type(args.input_fields[0].data_type())?;
        Ok(vec![flat_state_field(
            format_state_name(args.name, "entries"),
            key_type,
            value_type,
        )])
    }
}

#[derive(Debug)]
struct MapUnionAccumulator {
    entries: Vec<(ScalarValue, ScalarValue)>,
    key_type: DataType,
    value_type: DataType,
}

impl MapUnionAccumulator {
    fn put(&mut self, key: ScalarValue, value: ScalarValue) {
        if key.is_null() {
            return;
        }
        for entry in &mut self.entries {
            if entry.0 == key {
                entry.1 = value;
                return;
            }
        }
        self.entries.push((key, value));
    }
}

impl Accumulator for MapUnionAccumulator {
    fn update_batch(&mut self, args: &[ArrayRef]) -> DFResult<()> {
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "map_union::update_batch expected 1 array, got {}",
                args.len()
            )));
        }
        let map_array = args[0]
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "map_union: input must be a MAP, got {:?}",
                    args[0].data_type()
                ))
            })?;
        for i in 0..map_array.len() {
            if map_array.is_null(i) {
                continue;
            }
            let entries = map_array.value(i);
            let s = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "map_union: map entries are not a StructArray".into(),
                    )
                })?;
            let keys = s.column(0);
            let values = s.column(1);
            for j in 0..keys.len() {
                if keys.is_null(j) {
                    continue;
                }
                let key = ScalarValue::try_from_array(keys, j)?;
                let value = ScalarValue::try_from_array(values, j)?;
                self.put(key, value);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let (keys, values) =
            materialize_kv_arrays(&self.entries, &self.key_type, &self.value_type)?;
        let map = build_map_array(keys, values, &self.key_type, &self.value_type)?;
        Ok(ScalarValue::Map(Arc::new(map)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .entries
                .iter()
                .map(|(k, v)| k.size() + v.size())
                .sum::<usize>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        // Same flat List<Struct{k, v}> shape as map_agg.
        let (keys, values) =
            materialize_kv_arrays(&self.entries, &self.key_type, &self.value_type)?;
        let entries_struct = StructArray::new(
            Fields::from(vec![
                Field::new("key", self.key_type.clone(), false),
                Field::new("value", self.value_type.clone(), true),
            ]),
            vec![keys, values],
            None,
        );
        let item_field = Arc::new(Field::new(
            "item",
            entries_struct.data_type().clone(),
            true,
        ));
        let offsets = OffsetBuffer::new(vec![0i32, self.entries.len() as i32].into());
        let list = ListArray::new(item_field, offsets, Arc::new(entries_struct), None);
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        merge_flat_state(states, |key, value| self.put(key, value))
    }
}

/// Pull `(key_type, value_type)` out of a `DataType::Map`. Returns a Plan
/// error if the input is not a Map.
fn decompose_map_type(map_type: &DataType) -> DFResult<(DataType, DataType)> {
    let DataType::Map(entries_field, _) = map_type else {
        return Err(DataFusionError::Plan(format!(
            "map_union: expected MAP input, got {map_type:?}"
        )));
    };
    let DataType::Struct(fields) = entries_field.data_type() else {
        return Err(DataFusionError::Internal(format!(
            "map_union: MAP entries field is not a Struct: {:?}",
            entries_field.data_type()
        )));
    };
    if fields.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "map_union: MAP entries struct must have 2 fields (key, value), got {}",
            fields.len()
        )));
    }
    Ok((fields[0].data_type().clone(), fields[1].data_type().clone()))
}

/// Walk the merge-state ListArray of `Struct{key, value}` and call `f`
/// on every (key, value) pair. Used by `map_agg`, `multimap_agg`, and
/// `map_union`'s `merge_batch` since their state shape is identical.
fn merge_flat_state(
    states: &[ArrayRef],
    mut f: impl FnMut(ScalarValue, ScalarValue),
) -> DFResult<()> {
    if states.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "map aggregate merge_batch expected 1 state array, got {}",
            states.len()
        )));
    }
    let list = states[0]
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "map aggregate merge_batch state[0] is not a ListArray".into(),
            )
        })?;
    for i in 0..list.len() {
        if list.is_null(i) {
            continue;
        }
        let entries = list.value(i);
        let s = entries.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
            DataFusionError::Internal(
                "map aggregate merge_batch list element is not a StructArray".into(),
            )
        })?;
        let keys = s.column(0);
        let values = s.column(1);
        for j in 0..keys.len() {
            if keys.is_null(j) {
                continue;
            }
            let key = ScalarValue::try_from_array(keys, j)?;
            let value = ScalarValue::try_from_array(values, j)?;
            f(key, value);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    fn map_agg_acc(k: DataType, v: DataType) -> MapAggAccumulator {
        MapAggAccumulator {
            entries: Vec::new(),
            key_type: k,
            value_type: v,
        }
    }

    fn multimap_agg_acc(k: DataType, v: DataType) -> MultimapAggAccumulator {
        MultimapAggAccumulator {
            entries: Vec::new(),
            key_type: k,
            value_type: v,
        }
    }

    #[test]
    fn map_agg_last_wins_on_duplicate_key() {
        let mut acc = map_agg_acc(DataType::Utf8, DataType::Int64);
        let k: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a"]));
        let v: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 99]));
        acc.update_batch(&[k, v]).unwrap();
        // Two distinct keys; "a" should hold 99 (last write), "b" -> 2.
        let lookup = |key: &str| -> i64 {
            acc.entries
                .iter()
                .find(|(k, _)| matches!(k, ScalarValue::Utf8(Some(s)) if s == key))
                .map(|(_, v)| match v {
                    ScalarValue::Int64(Some(n)) => *n,
                    _ => panic!("expected Int64 value"),
                })
                .unwrap_or(-1)
        };
        assert_eq!(lookup("a"), 99);
        assert_eq!(lookup("b"), 2);
        assert_eq!(acc.entries.len(), 2);
    }

    #[test]
    fn map_agg_skips_null_keys() {
        let mut acc = map_agg_acc(DataType::Utf8, DataType::Int64);
        let k: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("b")]));
        let v: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        acc.update_batch(&[k, v]).unwrap();
        assert_eq!(acc.entries.len(), 2, "null key should be skipped");
    }

    #[test]
    fn multimap_agg_groups_values_by_key() {
        let mut acc = multimap_agg_acc(DataType::Utf8, DataType::Int64);
        let k: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a", "a"]));
        let v: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
        acc.update_batch(&[k, v]).unwrap();
        // a -> [1, 3, 4], b -> [2]
        let lookup = |key: &str| -> Vec<i64> {
            acc.entries
                .iter()
                .find(|(k, _)| matches!(k, ScalarValue::Utf8(Some(s)) if s == key))
                .map(|(_, vs)| {
                    vs.iter()
                        .map(|v| match v {
                            ScalarValue::Int64(Some(n)) => *n,
                            _ => panic!("expected Int64"),
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        assert_eq!(lookup("a"), vec![1, 3, 4]);
        assert_eq!(lookup("b"), vec![2]);
    }

    #[test]
    fn empty_group_evaluates_to_empty_map_for_all_three() {
        let mut a = map_agg_acc(DataType::Utf8, DataType::Int64);
        match a.evaluate().unwrap() {
            ScalarValue::Map(m) => assert_eq!(m.value(0).len(), 0),
            other => panic!("map_agg empty group: {other:?}"),
        }
        let mut b = multimap_agg_acc(DataType::Utf8, DataType::Int64);
        match b.evaluate().unwrap() {
            ScalarValue::Map(m) => assert_eq!(m.value(0).len(), 0),
            other => panic!("multimap_agg empty group: {other:?}"),
        }
    }

    #[test]
    fn map_agg_merge_combines_partials_last_wins() {
        // Two partials; the second's "a" should win because its row
        // walks the merge_batch later.
        let mut p1 = map_agg_acc(DataType::Utf8, DataType::Int64);
        p1.update_batch(&[
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        ])
        .unwrap();
        let s1 = p1.state().unwrap();

        let mut p2 = map_agg_acc(DataType::Utf8, DataType::Int64);
        p2.update_batch(&[
            Arc::new(StringArray::from(vec!["a", "c"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![99, 3])) as ArrayRef,
        ])
        .unwrap();
        let s2 = p2.state().unwrap();

        let list1 = match &s1[0] {
            ScalarValue::List(l) => Arc::clone(l),
            other => panic!("expected List, got {other:?}"),
        };
        let list2 = match &s2[0] {
            ScalarValue::List(l) => Arc::clone(l),
            other => panic!("expected List, got {other:?}"),
        };
        let combined = arrow::compute::concat(&[list1.as_ref(), list2.as_ref()]).unwrap();

        let mut final_acc = map_agg_acc(DataType::Utf8, DataType::Int64);
        final_acc.merge_batch(&[combined]).unwrap();

        let lookup = |key: &str| -> i64 {
            final_acc
                .entries
                .iter()
                .find(|(k, _)| matches!(k, ScalarValue::Utf8(Some(s)) if s == key))
                .map(|(_, v)| match v {
                    ScalarValue::Int64(Some(n)) => *n,
                    _ => panic!("expected Int64"),
                })
                .unwrap_or(-1)
        };
        assert_eq!(lookup("a"), 99, "second partial's 'a' wins");
        assert_eq!(lookup("b"), 2);
        assert_eq!(lookup("c"), 3);
    }
}
