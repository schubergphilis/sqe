//! Trino `histogram(x)` aggregate UDAF.
//!
//! `histogram(x)` returns a `MAP<T, BIGINT>` with one entry per distinct
//! value of `x`, where the value is the count of occurrences. NULL inputs
//! are skipped per Trino spec.
//!
//! Implementation notes:
//!
//! - The accumulator stores `Vec<(ScalarValue, i64)>` with linear lookup.
//!   For groups with thousands of distinct keys, switching to a HashMap
//!   would be faster, but `ScalarValue::Hash` is implemented only for
//!   primitive types and most callers have small-cardinality columns.
//!   The linear path keeps the code type-flexible.
//! - Multi-phase aggregation is supported via `state()` / `merge_batch()`.
//!   State shape: a single `List<Struct{key: K_TYPE, count: Int64}>`
//!   ScalarValue per partial accumulator. `merge_batch` consumes a List
//!   array of these structs and merges by key.
//! - Output is a `MapArray` with `keys: K_TYPE` and `values: Int64`,
//!   matching Trino's `MAP<T, BIGINT>` return type.
//!
//! Type genericity: the key column can be any DataType. The accumulator
//! captures the runtime key type from `AccumulatorArgs`, then uses it to
//! materialize typed key arrays at finalization.
//!
//! `map_agg(k, v)`, `multimap_agg(k, v)`, and `map_union(map)` follow the
//! same accumulator pattern but with different state/output shapes; they
//! ship in a follow-up MR once the histogram path is exercised.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int64Array, Int64Builder, MapArray, StructArray,
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

/// `histogram(x)` aggregate. Returns `MAP<typeof(x), BIGINT>` with the
/// count per distinct value.
#[derive(Debug)]
pub(crate) struct Histogram {
    signature: Signature,
}

impl Histogram {
    pub(crate) fn udaf() -> AggregateUDF {
        AggregateUDF::from(Self {
            signature: Signature::any(1, Volatility::Immutable),
        })
    }

    /// Build the `MAP<K, Int64>` DataType for the given key type.
    fn map_type_for_key(key_type: &DataType) -> DataType {
        let entries_struct = DataType::Struct(Fields::from(vec![
            Field::new("key", key_type.clone(), false),
            Field::new("value", DataType::Int64, true),
        ]));
        DataType::Map(
            Arc::new(Field::new("entries", entries_struct, false)),
            // sorted = false: Trino does not guarantee histogram entry order
            false,
        )
    }
}

impl PartialEq for Histogram {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for Histogram {}

impl std::hash::Hash for Histogram {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

impl AggregateUDFImpl for Histogram {

    fn name(&self) -> &str {
        "histogram"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        if args.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "histogram(x) takes exactly 1 argument, got {}",
                args.len()
            )));
        }
        Ok(Self::map_type_for_key(&args[0]))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let key_type = acc_args.expr_fields[0].data_type().clone();
        Ok(Box::new(HistogramAccumulator::try_new(key_type)?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        // State for multi-phase aggregation: a single List<Struct{key, count}>
        // per accumulator. The list groups all (key, count) entries this
        // partial has accumulated. merge_batch unflattens by walking the
        // outer list and folding each entry into the new accumulator.
        let key_type = args.input_fields[0].data_type().clone();
        let entries_struct = DataType::Struct(Fields::from(vec![
            Field::new("key", key_type, false),
            Field::new("count", DataType::Int64, false),
        ]));
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "entries"),
            DataType::List(Arc::new(Field::new("item", entries_struct, true))),
            true,
        ))])
    }
}

/// Accumulator state: linear vector of (key, count). The vector lookup
/// is O(N) per update, where N is the number of distinct keys seen so far.
/// For groups with many distinct keys this is suboptimal; switching to a
/// HashMap requires `Hash` on every supported key type, which is more
/// invasive than the lookup overhead is worth at the cardinalities that
/// `histogram()` is actually used for in dbt / BI workloads.
#[derive(Debug)]
struct HistogramAccumulator {
    entries: Vec<(ScalarValue, i64)>,
    key_type: DataType,
}

impl HistogramAccumulator {
    fn try_new(key_type: DataType) -> DFResult<Self> {
        Ok(Self {
            entries: Vec::new(),
            key_type,
        })
    }

    /// Add `count` occurrences of `key` to the accumulator. NULL keys
    /// are skipped per Trino spec.
    fn bump(&mut self, key: ScalarValue, count: i64) {
        if key.is_null() || count <= 0 {
            return;
        }
        for entry in &mut self.entries {
            if entry.0 == key {
                entry.1 += count;
                return;
            }
        }
        self.entries.push((key, count));
    }
}

impl Accumulator for HistogramAccumulator {
    fn update_batch(&mut self, args: &[ArrayRef]) -> DFResult<()> {
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "histogram::update_batch expected 1 array, got {}",
                args.len()
            )));
        }
        let arr = &args[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let key = ScalarValue::try_from_array(arr, i)?;
            self.bump(key, 1);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        // Materialize a single-element MapArray containing every entry.
        let n_entries = self.entries.len();

        // Build the typed keys array via ScalarValue::iter_to_array. This
        // dispatches over the key DataType so we don't have to write a
        // type-specific builder per primitive.
        let key_scalars: Vec<ScalarValue> = if n_entries == 0 {
            // iter_to_array errors on empty input; fall through to a
            // single-element empty Map below by producing a zero-row keys
            // array of the right type.
            Vec::new()
        } else {
            self.entries.iter().map(|(k, _)| k.clone()).collect()
        };

        let keys_arr: ArrayRef = if key_scalars.is_empty() {
            // Empty group: produce a zero-row array of the key type.
            arrow::array::new_empty_array(&self.key_type)
        } else {
            ScalarValue::iter_to_array(key_scalars).map_err(|e| {
                DataFusionError::Execution(format!(
                    "histogram: failed to materialize keys array: {e}"
                ))
            })?
        };

        // Counts: always Int64.
        let mut count_builder = Int64Builder::with_capacity(n_entries);
        for (_, c) in &self.entries {
            count_builder.append_value(*c);
        }
        let counts_arr: ArrayRef = Arc::new(count_builder.finish());

        // Wrap (keys, counts) in a StructArray with the right field names.
        let key_field = Arc::new(Field::new("key", self.key_type.clone(), false));
        let value_field = Arc::new(Field::new("value", DataType::Int64, true));
        let entries_struct = StructArray::new(
            Fields::from(vec![key_field.as_ref().clone(), value_field.as_ref().clone()]),
            vec![keys_arr, counts_arr],
            None,
        );

        // MapArray with a single row containing all entries.
        let offsets = OffsetBuffer::new(vec![0i32, n_entries as i32].into());
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                key_field.as_ref().clone(),
                value_field.as_ref().clone(),
            ])),
            false,
        ));
        let map_array = MapArray::new(entries_field, offsets, entries_struct, None, false);

        Ok(ScalarValue::Map(Arc::new(map_array)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .entries
                .iter()
                .map(|(k, _)| k.size() + std::mem::size_of::<i64>())
                .sum::<usize>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        // Materialize state as a single List<Struct{key, count}> ScalarValue.
        // Empty groups produce a zero-row list of the right type.
        let n_entries = self.entries.len();

        let key_scalars: Vec<ScalarValue> =
            self.entries.iter().map(|(k, _)| k.clone()).collect();
        let keys_arr: ArrayRef = if key_scalars.is_empty() {
            arrow::array::new_empty_array(&self.key_type)
        } else {
            ScalarValue::iter_to_array(key_scalars).map_err(|e| {
                DataFusionError::Execution(format!(
                    "histogram: failed to materialize state keys: {e}"
                ))
            })?
        };

        let mut count_builder = Int64Builder::with_capacity(n_entries);
        for (_, c) in &self.entries {
            count_builder.append_value(*c);
        }
        let counts_arr: ArrayRef = Arc::new(count_builder.finish());

        let entries_struct = StructArray::new(
            Fields::from(vec![
                Field::new("key", self.key_type.clone(), false),
                Field::new("count", DataType::Int64, false),
            ]),
            vec![keys_arr, counts_arr],
            None,
        );

        // Wrap in a single-element ListArray.
        let item_field = Arc::new(Field::new(
            "item",
            entries_struct.data_type().clone(),
            true,
        ));
        let offsets = OffsetBuffer::new(vec![0i32, n_entries as i32].into());
        let list_array = arrow::array::ListArray::new(
            item_field,
            offsets,
            Arc::new(entries_struct),
            None,
        );

        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "histogram::merge_batch expected 1 state array, got {}",
                states.len()
            )));
        }
        // states[0] is a ListArray. Each row of the list is a StructArray
        // with (key, count) fields. Walk each row's struct entries and
        // fold into self.
        let list = states[0]
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "histogram::merge_batch state[0] is not a ListArray".into(),
                )
            })?;
        for i in 0..list.len() {
            if list.is_null(i) {
                continue;
            }
            let entries = list.value(i);
            let s = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "histogram::merge_batch list element is not a StructArray".into(),
                    )
                })?;
            let keys = s.column(0);
            let counts = s
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "histogram::merge_batch struct value column is not Int64".into(),
                    )
                })?;
            for j in 0..keys.len() {
                if keys.is_null(j) {
                    continue;
                }
                let key = ScalarValue::try_from_array(keys, j)?;
                let count = counts.value(j);
                self.bump(key, count);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    fn make_acc(key_type: DataType) -> HistogramAccumulator {
        HistogramAccumulator::try_new(key_type).unwrap()
    }

    #[test]
    fn counts_distinct_string_values() {
        let mut acc = make_acc(DataType::Utf8);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![
            "a", "b", "a", "c", "a", "b",
        ]));
        acc.update_batch(&[arr]).unwrap();
        // Internal state: a→3, b→2, c→1.
        let lookup = |key: &str| -> i64 {
            acc.entries
                .iter()
                .find(|(k, _)| matches!(k, ScalarValue::Utf8(Some(s)) if s == key))
                .map(|(_, c)| *c)
                .unwrap_or(0)
        };
        assert_eq!(lookup("a"), 3);
        assert_eq!(lookup("b"), 2);
        assert_eq!(lookup("c"), 1);
    }

    #[test]
    fn nulls_are_skipped() {
        let mut acc = make_acc(DataType::Utf8);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("a"),
            None,
        ]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.entries.len(), 1, "only 'a' should be counted");
        assert_eq!(acc.entries[0].1, 2);
    }

    #[test]
    fn empty_group_evaluates_to_empty_map() {
        let mut acc = make_acc(DataType::Utf8);
        let result = acc.evaluate().unwrap();
        match result {
            ScalarValue::Map(map) => {
                assert_eq!(map.len(), 1, "single Map row");
                assert_eq!(map.value(0).len(), 0, "zero entries in the map");
            }
            other => panic!("expected ScalarValue::Map, got {other:?}"),
        }
    }

    #[test]
    fn integer_keys_work() {
        let mut acc = make_acc(DataType::Int64);
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 1, 3, 1, 2]));
        acc.update_batch(&[arr]).unwrap();
        // Three distinct keys: 1→3, 2→2, 3→1.
        assert_eq!(acc.entries.len(), 3);
        let lookup = |key: i64| -> i64 {
            acc.entries
                .iter()
                .find(|(k, _)| matches!(k, ScalarValue::Int64(Some(v)) if *v == key))
                .map(|(_, c)| *c)
                .unwrap_or(0)
        };
        assert_eq!(lookup(1), 3);
        assert_eq!(lookup(2), 2);
        assert_eq!(lookup(3), 1);
    }

    #[test]
    fn merge_combines_partials() {
        // Build two partial accumulators and merge their states.
        let mut p1 = make_acc(DataType::Utf8);
        p1.update_batch(&[Arc::new(StringArray::from(vec!["a", "a", "b"])) as ArrayRef])
            .unwrap();
        let s1 = p1.state().unwrap();

        let mut p2 = make_acc(DataType::Utf8);
        p2.update_batch(&[Arc::new(StringArray::from(vec!["b", "c", "c"])) as ArrayRef])
            .unwrap();
        let s2 = p2.state().unwrap();

        // Materialize each state's List<Struct> as an ArrayRef, then
        // concatenate so merge_batch sees both partials in one call.
        let list1 = match &s1[0] {
            ScalarValue::List(l) => Arc::clone(l),
            other => panic!("expected List state, got {other:?}"),
        };
        let list2 = match &s2[0] {
            ScalarValue::List(l) => Arc::clone(l),
            other => panic!("expected List state, got {other:?}"),
        };
        let combined =
            arrow::compute::concat(&[list1.as_ref(), list2.as_ref()]).unwrap();

        let mut final_acc = make_acc(DataType::Utf8);
        final_acc.merge_batch(&[combined]).unwrap();

        // Combined: a→2, b→2, c→2.
        let lookup = |key: &str| -> i64 {
            final_acc
                .entries
                .iter()
                .find(|(k, _)| matches!(k, ScalarValue::Utf8(Some(s)) if s == key))
                .map(|(_, c)| *c)
                .unwrap_or(0)
        };
        assert_eq!(lookup("a"), 2);
        assert_eq!(lookup("b"), 2, "b appeared in both partials");
        assert_eq!(lookup("c"), 2);
    }
}
