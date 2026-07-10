//! Trino-compatible higher-moment aggregates: `skewness` and `kurtosis`.
//!
//! Neither is a DataFusion built-in. Both are functions of the same online
//! central moments (count, m1=mean, m2, m3, m4), so a single
//! `CentralMomentsAccumulator` backs both -- the `Moment` kind only changes
//! the final `evaluate` formula. Same shared-struct pattern as `max_by` /
//! `min_by` in [`crate::aggregates`].
//!
//! The update and merge arithmetic mirrors Trino's
//! `io.trino.operator.aggregation.state.CentralMomentsState` byte-for-byte:
//!
//! - `update(x)` is Terriberry's online single-pass moment update.
//! - `merge(a, b)` is the Pebay parallel-combine, required for multi-phase /
//!   distributed aggregation (each `merge_batch` row is another partial state).
//!
//! Final formulas, matching Trino's `CentralMomentsAggregation`:
//!
//! - `skewness` (n >= 3): `sqrt(n) * m3 / m2^1.5` (population skewness g1)
//! - `kurtosis` (n >= 4):
//!   `((n-1)*n*(n+1))/((n-2)*(n-3)) * m4/m2^2 - 3*(n-1)^2/((n-2)*(n-3))`
//!   (sample excess kurtosis G2)
//!
//! Trino has no `m2 == 0` guard, so constant input yields NaN/inf. We
//! replicate that exactly rather than "fixing" it, to stay bug-for-bug
//! compatible; only the `n < 3` / `n < 4` guards produce NULL.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

/// Which higher moment the aggregate reports. Both share one accumulator;
/// only the final formula and the minimum-count guard differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Moment {
    Skewness,
    Kurtosis,
}

impl Moment {
    fn name(self) -> &'static str {
        match self {
            Self::Skewness => "skewness",
            Self::Kurtosis => "kurtosis",
        }
    }
}

/// A `skewness` or `kurtosis` aggregate. Single struct, two registered names
/// (one per `Moment`). `Eq`/`Hash` compare by moment only: two instances with
/// the same moment are semantically identical.
#[derive(Debug)]
pub(crate) struct CentralMoment {
    moment: Moment,
    signature: Signature,
}

impl PartialEq for CentralMoment {
    fn eq(&self, other: &Self) -> bool {
        self.moment == other.moment
    }
}

impl Eq for CentralMoment {}

impl std::hash::Hash for CentralMoment {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.moment.hash(state);
    }
}

impl CentralMoment {
    fn new(moment: Moment) -> Self {
        Self {
            moment,
            // Trino accepts any numeric argument; DataFusion's coercion layer
            // casts the input to Float64 before it reaches the accumulator,
            // matching how the built-in variance/stddev aggregates declare
            // their signature.
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }

    pub(crate) fn skewness_udaf() -> AggregateUDF {
        AggregateUDF::from(Self::new(Moment::Skewness))
    }

    pub(crate) fn kurtosis_udaf() -> AggregateUDF {
        AggregateUDF::from(Self::new(Moment::Kurtosis))
    }
}

impl AggregateUDFImpl for CentralMoment {
    fn name(&self) -> &str {
        self.moment.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(CentralMomentsAccumulator::new(self.moment)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "count"),
                DataType::UInt64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "m1"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "m2"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "m3"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "m4"),
                DataType::Float64,
                true,
            )),
        ])
    }
}

/// Online central-moments accumulator shared by `skewness` and `kurtosis`.
#[derive(Debug)]
struct CentralMomentsAccumulator {
    moment: Moment,
    count: u64,
    m1: f64,
    m2: f64,
    m3: f64,
    m4: f64,
}

impl CentralMomentsAccumulator {
    fn new(moment: Moment) -> Self {
        Self {
            moment,
            count: 0,
            m1: 0.0,
            m2: 0.0,
            m3: 0.0,
            m4: 0.0,
        }
    }

    /// Add one observation. Terriberry's single-pass update; every new moment
    /// is computed from the pre-update `m2`/`m3`/`m4`, mirroring Trino's
    /// `CentralMomentsState.update(double)`.
    fn update_one(&mut self, value: f64) {
        let n1 = self.count as f64;
        let n = n1 + 1.0;
        let (m1, m2, m3, m4) = (self.m1, self.m2, self.m3, self.m4);
        let delta = value - m1;
        let delta_n = delta / n;
        let delta_n2 = delta_n * delta_n;
        let dm2 = delta * delta_n * n1;

        self.count += 1;
        self.m1 = m1 + delta_n;
        self.m2 = m2 + dm2;
        self.m3 = m3 + dm2 * delta_n * (n - 2.0) - 3.0 * delta_n * m2;
        self.m4 = m4 + dm2 * delta_n2 * (n * n - 3.0 * n + 3.0) + 6.0 * delta_n2 * m2
            - 4.0 * delta_n * m3;
    }

    /// Merge another (partial) moment state into this one. Pebay parallel
    /// combine, mirroring Trino's `CentralMomentsState.merge(...)`. A zero-count
    /// operand is a no-op (matches Trino, and keeps merging into an empty
    /// accumulator correct).
    fn merge_one(&mut self, count_b: u64, m1b: f64, m2b: f64, m3b: f64, m4b: f64) {
        if count_b == 0 {
            return;
        }
        let na = self.count as f64;
        let nb = count_b as f64;
        let (m1a, m2a, m3a, m4a) = (self.m1, self.m2, self.m3, self.m4);
        let n = na + nb;
        let delta = m1b - m1a;
        let delta2 = delta * delta;
        let delta3 = delta * delta2;
        let delta4 = delta2 * delta2;

        self.count += count_b;
        self.m1 = (na * m1a + nb * m1b) / n;
        self.m2 = m2a + m2b + delta2 * na * nb / n;
        self.m3 = m3a + m3b + delta3 * na * nb * (na - nb) / (n * n)
            + 3.0 * delta * (na * m2b - nb * m2a) / n;
        self.m4 = m4a
            + m4b
            + delta4 * na * nb * (na * na - na * nb + nb * nb) / (n * n * n)
            + 6.0 * delta2 * (na * na * m2b + nb * nb * m2a) / (n * n)
            + 4.0 * delta * (na * m3b - nb * m3a) / n;
    }
}

impl Accumulator for CentralMomentsAccumulator {
    fn update_batch(&mut self, args: &[ArrayRef]) -> DFResult<()> {
        let arr = args[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "central-moments accumulator expected a Float64 input".to_string(),
                )
            })?;
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                self.update_one(arr.value(i));
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let counts = states[0]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Internal("count state must be UInt64".to_string()))?;
        let float_state = |idx: usize| -> DFResult<&Float64Array> {
            states[idx].as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                DataFusionError::Internal(format!("moment state {idx} must be Float64"))
            })
        };
        let (m1, m2, m3, m4) = (float_state(1)?, float_state(2)?, float_state(3)?, float_state(4)?);

        for i in 0..counts.len() {
            if counts.is_valid(i) {
                self.merge_one(counts.value(i), m1.value(i), m2.value(i), m3.value(i), m4.value(i));
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::UInt64(Some(self.count)),
            ScalarValue::Float64(Some(self.m1)),
            ScalarValue::Float64(Some(self.m2)),
            ScalarValue::Float64(Some(self.m3)),
            ScalarValue::Float64(Some(self.m4)),
        ])
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let n = self.count as f64;
        let result = match self.moment {
            // Population skewness g1. Trino guards only on n < 3 (no m2 == 0
            // guard), so constant input yields NaN -- matched intentionally.
            Moment::Skewness => {
                if self.count < 3 {
                    None
                } else {
                    Some(n.sqrt() * self.m3 / self.m2.powf(1.5))
                }
            }
            // Sample excess kurtosis G2. Trino guards only on n < 4.
            Moment::Kurtosis => {
                if self.count < 4 {
                    None
                } else {
                    let m2 = self.m2;
                    let val = ((n - 1.0) * n * (n + 1.0)) / ((n - 2.0) * (n - 3.0)) * self.m4
                        / (m2 * m2)
                        - 3.0 * ((n - 1.0) * (n - 1.0)) / ((n - 2.0) * (n - 3.0));
                    Some(val)
                }
            }
        };
        Ok(ScalarValue::Float64(result))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f64_array(vals: &[f64]) -> ArrayRef {
        Arc::new(Float64Array::from(vals.to_vec()))
    }

    fn run(moment: Moment, vals: &[f64]) -> Option<f64> {
        let mut acc = CentralMomentsAccumulator::new(moment);
        acc.update_batch(&[f64_array(vals)]).unwrap();
        match acc.evaluate().unwrap() {
            ScalarValue::Float64(v) => v,
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn skewness_matches_trino_formula() {
        // [1,2,3,4,10]: mean 4, m2=50, m3=180 -> sqrt(5)*180/50^1.5.
        let got = run(Moment::Skewness, &[1.0, 2.0, 3.0, 4.0, 10.0]).unwrap();
        assert!((got - 1.138_419_957_660_617).abs() < 1e-8, "skewness got {got}");
    }

    #[test]
    fn skewness_of_symmetric_data_is_zero() {
        let got = run(Moment::Skewness, &[1.0, 2.0, 3.0, 4.0, 5.0]).unwrap();
        assert!(got.abs() < 1e-12, "symmetric skewness should be 0, got {got}");
    }

    #[test]
    fn kurtosis_matches_trino_formula() {
        // [1,2,3,4,10]: m2=50, m4=1394, n=5 -> 20*1394/2500 - 8 = 3.152.
        let got = run(Moment::Kurtosis, &[1.0, 2.0, 3.0, 4.0, 10.0]).unwrap();
        assert!((got - 3.152).abs() < 1e-9, "kurtosis got {got}");
    }

    #[test]
    fn skewness_needs_three_values() {
        assert_eq!(run(Moment::Skewness, &[1.0, 2.0]), None);
        assert!(run(Moment::Skewness, &[1.0, 2.0, 4.0]).is_some());
    }

    #[test]
    fn kurtosis_needs_four_values() {
        assert_eq!(run(Moment::Kurtosis, &[1.0, 2.0, 3.0]), None);
        assert!(run(Moment::Kurtosis, &[1.0, 2.0, 3.0, 5.0]).is_some());
    }

    #[test]
    fn nulls_are_skipped() {
        let mut acc = CentralMomentsAccumulator::new(Moment::Skewness);
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(2.0),
            Some(3.0),
            None,
            Some(4.0),
            Some(10.0),
        ]));
        acc.update_batch(&[arr]).unwrap();
        // Same non-null values as the [1,2,3,4,10] case.
        match acc.evaluate().unwrap() {
            ScalarValue::Float64(Some(v)) => {
                assert!((v - 1.138_419_957_660_617).abs() < 1e-8, "got {v}")
            }
            other => panic!("expected value, got {other:?}"),
        }
    }

    #[test]
    fn merge_matches_single_batch() {
        // Split [1,2,3,4,10,7,6] across two partials, merge, and compare to a
        // single-batch accumulator over the same values. Exercises merge_one
        // for both skewness and kurtosis.
        let all = [1.0, 2.0, 3.0, 4.0, 10.0, 7.0, 6.0];
        for moment in [Moment::Skewness, Moment::Kurtosis] {
            let mut p1 = CentralMomentsAccumulator::new(moment);
            p1.update_batch(&[f64_array(&all[..3])]).unwrap();
            let s1 = p1.state().unwrap();

            let mut p2 = CentralMomentsAccumulator::new(moment);
            p2.update_batch(&[f64_array(&all[3..])]).unwrap();
            let s2 = p2.state().unwrap();

            let state_arrays = |s: &[ScalarValue]| -> Vec<ArrayRef> {
                vec![
                    ScalarValue::iter_to_array(vec![s[0].clone()]).unwrap(),
                    ScalarValue::iter_to_array(vec![s[1].clone()]).unwrap(),
                    ScalarValue::iter_to_array(vec![s[2].clone()]).unwrap(),
                    ScalarValue::iter_to_array(vec![s[3].clone()]).unwrap(),
                    ScalarValue::iter_to_array(vec![s[4].clone()]).unwrap(),
                ]
            };

            let mut merged = CentralMomentsAccumulator::new(moment);
            merged.merge_batch(&state_arrays(&s1)).unwrap();
            merged.merge_batch(&state_arrays(&s2)).unwrap();
            let merged_val = match merged.evaluate().unwrap() {
                ScalarValue::Float64(Some(v)) => v,
                other => panic!("expected value, got {other:?}"),
            };

            let single = run(moment, &all).unwrap();
            assert!(
                (merged_val - single).abs() < 1e-9,
                "{:?}: merge {merged_val} != single {single}",
                moment
            );
        }
    }
}
