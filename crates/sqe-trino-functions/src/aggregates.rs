//! Trino-compatible aggregate UDAFs.
//!
//! Currently:
//!
//! - `max_by(x, y)` / `arg_max(x, y)` — returns x for the row where y is
//!   maximum across the group. Trino spec.
//! - `min_by(x, y)` / `arg_min(x, y)` — returns x for the row where y is
//!   minimum across the group. Trino spec.
//!
//! Both share a single `ArgExtremumAccumulator` that holds the best (x, y)
//! pair seen so far. The accumulator is type-flexible: x can be any
//! `DataType`, y must be a type whose `ScalarValue::partial_cmp` returns
//! a usable ordering (numeric, string, date, timestamp, etc.).
//!
//! State for partial aggregation is `(best_x, best_y)`. Multi-phase
//! aggregation works because `merge_batch` processes each partial's
//! `(best_x, best_y)` exactly the way `update_batch` processes each row's
//! `(x, y)` — pick the larger / smaller y, carry the matching x.
//!
//! Earlier SQE versions registered scalar stubs for `max_by` and `min_by`
//! that returned the first argument. The stubs prevented parse errors
//! but produced wrong results in any aggregation context. This module
//! replaces them.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

/// Direction of the arg-extremum aggregate. `Max` keeps the largest y;
/// `Min` keeps the smallest y. Both store (best_x, best_y) and return x.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Direction {
    Max,
    Min,
}

impl Direction {
    fn primary_name(self) -> &'static str {
        match self {
            Self::Max => "max_by",
            Self::Min => "min_by",
        }
    }

    /// Aliases registered alongside the primary name. `arg_max` / `arg_min`
    /// are the names DuckDB and ClickHouse use for the same semantics.
    fn aliases(self) -> Vec<String> {
        match self {
            Self::Max => vec!["arg_max".to_string()],
            Self::Min => vec!["arg_min".to_string()],
        }
    }
}

/// A `max_by` or `min_by` aggregate. Single struct, two registered names
/// (one per `Direction`).
///
/// `AggregateUDFImpl` requires `Debug + DynEq + DynHash`, which in turn
/// require `Eq + Hash`. We compare by direction only: two `ArgExtremum`
/// instances with the same direction are semantically identical (both
/// hold the same fixed-shape signature and the alias list is derived
/// from direction).
#[derive(Debug)]
pub(crate) struct ArgExtremum {
    direction: Direction,
    aliases: Vec<String>,
    signature: Signature,
}

impl PartialEq for ArgExtremum {
    fn eq(&self, other: &Self) -> bool {
        self.direction == other.direction
    }
}

impl Eq for ArgExtremum {}

impl std::hash::Hash for ArgExtremum {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.direction.hash(state);
    }
}

impl ArgExtremum {
    fn new(direction: Direction) -> Self {
        Self {
            direction,
            aliases: direction.aliases(),
            // Two arguments, any types. Trino's signature is also wide:
            // x can be any column type, y must be Comparable. We catch
            // unorderable y at runtime in the accumulator rather than
            // restricting the signature.
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    pub(crate) fn max_by_udaf() -> AggregateUDF {
        AggregateUDF::from(Self::new(Direction::Max))
    }

    pub(crate) fn min_by_udaf() -> AggregateUDF {
        AggregateUDF::from(Self::new(Direction::Min))
    }
}

impl AggregateUDFImpl for ArgExtremum {

    fn name(&self) -> &str {
        self.direction.primary_name()
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        // Returns the type of x (the first argument).
        if args.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "{}(x, y) takes exactly 2 arguments, got {}",
                self.direction.primary_name(),
                args.len()
            )));
        }
        Ok(args[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let x_type = acc_args.expr_fields[0].data_type().clone();
        let y_type = acc_args.expr_fields[1].data_type().clone();
        Ok(Box::new(ArgExtremumAccumulator::try_new(
            self.direction,
            x_type,
            y_type,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        let x_type = args.input_fields[0].data_type().clone();
        let y_type = args.input_fields[1].data_type().clone();
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "best_x"),
                x_type,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "best_y"),
                y_type,
                true,
            )),
        ])
    }
}

/// State + update logic shared by `max_by` and `min_by`.
#[derive(Debug)]
struct ArgExtremumAccumulator {
    direction: Direction,
    /// `Some(x)` once any non-null y has been observed. The matching y
    /// is parked in `best_y`. We keep these as `ScalarValue` so any
    /// column type round-trips through serialization unchanged.
    best_x: Option<ScalarValue>,
    best_y: Option<ScalarValue>,
    /// Cached types so `evaluate()` and `state()` produce typed nulls
    /// when the group had no non-null y.
    x_type: DataType,
    y_type: DataType,
}

impl ArgExtremumAccumulator {
    fn try_new(
        direction: Direction,
        x_type: DataType,
        y_type: DataType,
    ) -> DFResult<Self> {
        Ok(Self {
            direction,
            best_x: None,
            best_y: None,
            x_type,
            y_type,
        })
    }

    /// Decide whether `y` is a new winner against the parked `best_y`.
    /// `None` for `best_y` means we have not seen any non-null y yet.
    fn is_better(&self, y: &ScalarValue) -> bool {
        let Some(current) = &self.best_y else {
            return true;
        };
        let cmp = y.partial_cmp(current);
        matches!(
            (self.direction, cmp),
            (Direction::Max, Some(Ordering::Greater))
                | (Direction::Min, Some(Ordering::Less))
        )
    }

    /// Walk a (x_array, y_array) pair and update the parked best.
    /// Used both by `update_batch` (raw row inputs) and `merge_batch`
    /// (partial-state inputs). They are structurally identical: each
    /// row of the merge input is itself a (best_x, best_y) pair from a
    /// partial accumulator.
    fn update_from_pair(&mut self, x_arr: &ArrayRef, y_arr: &ArrayRef) -> DFResult<()> {
        let n = x_arr.len();
        for i in 0..n {
            let y = ScalarValue::try_from_array(y_arr, i)?;
            if y.is_null() {
                continue;
            }
            if self.is_better(&y) {
                self.best_x = Some(ScalarValue::try_from_array(x_arr, i)?);
                self.best_y = Some(y);
            }
        }
        Ok(())
    }
}

impl Accumulator for ArgExtremumAccumulator {
    fn update_batch(&mut self, args: &[ArrayRef]) -> DFResult<()> {
        if args.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "ArgExtremumAccumulator::update_batch expected 2 arrays, got {}",
                args.len()
            )));
        }
        self.update_from_pair(&args[0], &args[1])
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        match &self.best_x {
            Some(v) => Ok(v.clone()),
            None => ScalarValue::try_from(&self.x_type),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.best_x.as_ref().map(ScalarValue::size).unwrap_or(0)
            + self.best_y.as_ref().map(ScalarValue::size).unwrap_or(0)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let x = match &self.best_x {
            Some(v) => v.clone(),
            None => ScalarValue::try_from(&self.x_type)?,
        };
        let y = match &self.best_y {
            Some(v) => v.clone(),
            None => ScalarValue::try_from(&self.y_type)?,
        };
        Ok(vec![x, y])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "ArgExtremumAccumulator::merge_batch expected 2 state arrays, got {}",
                states.len()
            )));
        }
        self.update_from_pair(&states[0], &states[1])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};

    fn make_acc(direction: Direction) -> ArgExtremumAccumulator {
        ArgExtremumAccumulator::try_new(direction, DataType::Utf8, DataType::Float64).unwrap()
    }

    #[test]
    fn max_by_picks_x_at_max_y() {
        let mut acc = make_acc(Direction::Max);
        let x: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let y: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 3.0, 2.0]));
        acc.update_batch(&[x, y]).unwrap();
        let result = acc.evaluate().unwrap();
        assert_eq!(result, ScalarValue::Utf8(Some("b".to_string())));
    }

    #[test]
    fn min_by_picks_x_at_min_y() {
        let mut acc = make_acc(Direction::Min);
        let x: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let y: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 3.0, 2.0]));
        acc.update_batch(&[x, y]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Utf8(Some("a".to_string())));
    }

    #[test]
    fn null_ys_are_skipped() {
        let mut acc = make_acc(Direction::Max);
        let x: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]));
        let y: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0), None, Some(2.0)]));
        acc.update_batch(&[x, y]).unwrap();
        // The NULL y at index 1 should be skipped; max y is 2.0 -> x = "c".
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Utf8(Some("c".to_string())));
    }

    #[test]
    fn empty_group_returns_typed_null() {
        let mut acc = make_acc(Direction::Max);
        let x: ArrayRef = Arc::new(StringArray::from(vec![Option::<&str>::None]));
        let y: ArrayRef = Arc::new(Float64Array::from(vec![Option::<f64>::None]));
        acc.update_batch(&[x, y]).unwrap();
        // All NULL y -> nothing observed -> typed NULL of x's type.
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Utf8(None));
    }

    #[test]
    fn merge_batch_combines_partials() {
        // Simulate two partial accumulators producing partial states,
        // then merge their states into a final accumulator. Multi-phase
        // aggregation must produce the same answer as a single-phase walk.
        let mut p1 = make_acc(Direction::Max);
        p1.update_batch(&[
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 5.0])) as ArrayRef,
        ])
        .unwrap();
        let s1 = p1.state().unwrap();

        let mut p2 = make_acc(Direction::Max);
        p2.update_batch(&[
            Arc::new(StringArray::from(vec!["c", "d"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![3.0, 4.0])) as ArrayRef,
        ])
        .unwrap();
        let s2 = p2.state().unwrap();

        // Build a single state-array pair from both partials.
        let merged_x: ArrayRef = Arc::new(StringArray::from(vec![
            s1[0].to_string(),
            s2[0].to_string(),
        ]));
        let merged_y: ArrayRef = Arc::new(Float64Array::from(vec![
            match &s1[1] {
                ScalarValue::Float64(Some(v)) => *v,
                _ => panic!("expected Float64 in state"),
            },
            match &s2[1] {
                ScalarValue::Float64(Some(v)) => *v,
                _ => panic!("expected Float64 in state"),
            },
        ]));

        let mut final_acc = make_acc(Direction::Max);
        final_acc.merge_batch(&[merged_x, merged_y]).unwrap();
        // Best y across both partials is 5.0 -> x = "b".
        assert_eq!(final_acc.evaluate().unwrap(), ScalarValue::Utf8(Some("b".to_string())));
    }

    #[test]
    fn integer_value_type_works() {
        // Integer y is the common dbt case (max_by(name, last_seen_at)).
        let mut acc = ArgExtremumAccumulator::try_new(
            Direction::Max,
            DataType::Utf8,
            DataType::Int64,
        )
        .unwrap();
        let x: ArrayRef = Arc::new(StringArray::from(vec!["alice", "bob", "carol"]));
        let y: ArrayRef = Arc::new(Int64Array::from(vec![100, 300, 200]));
        acc.update_batch(&[x, y]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Utf8(Some("bob".to_string()))
        );
    }
}
