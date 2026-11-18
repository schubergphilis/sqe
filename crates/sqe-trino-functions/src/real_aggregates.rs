//! Trino's `real`-in, `real`-out statistical aggregates.
//!
//! Trino 484 declares `corr`, `covar_pop`, `covar_samp`, `regr_slope` and
//! `regr_intercept` for `(real, real)` as well as `(double, double)`, and the
//! `real` form returns `real`. DataFusion's versions take `Float64` only, so a
//! `REAL` column is widened and the result is `double`.
//!
//! Each [`RealStatAgg`] wraps DataFusion's own aggregate: the arithmetic runs
//! in double precision, as it does in Trino, and the final value is narrowed
//! to `Float32` when both inputs are `Float32`. State, partial/final
//! aggregation, `GROUP BY` and window frames are DataFusion's own. Any other
//! input type gets the `double` result it had before.
//!
//! These shadow the DataFusion built-ins (Trino result type). Nothing
//! re-aggregates them by name: `SingleDistinctCountCompanionRule` only admits
//! `sum`/`min`/`max`/`count`, and so does DataFusion's
//! `single_distinct_to_groupby`.
//!
//! Trino has no `regr_count`, `regr_r2`, `regr_avgx`, `regr_avgy`,
//! `regr_sxx`, `regr_syy` or `regr_sxy`, so those keep DataFusion's types.

use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{plan_err, Result, ScalarValue};
use datafusion::functions_aggregate::correlation::Correlation;
use datafusion::functions_aggregate::covariance::{CovariancePopulation, CovarianceSample};
use datafusion::functions_aggregate::regr::{Regr, RegrType};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator,
    ReversedUDAF, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register the five wrapped aggregates, replacing DataFusion's.
pub fn register_real_aggregates(ctx: &SessionContext) {
    for inner in [
        AggregateUDF::new_from_impl(Correlation::new()),
        AggregateUDF::new_from_impl(CovariancePopulation::new()),
        AggregateUDF::new_from_impl(CovarianceSample::new()),
        AggregateUDF::new_from_impl(Regr::new(RegrType::Slope, "regr_slope")),
        AggregateUDF::new_from_impl(Regr::new(RegrType::Intercept, "regr_intercept")),
    ] {
        ctx.register_udaf(AggregateUDF::new_from_impl(RealStatAgg::new(inner)));
    }
}

/// A two-argument DataFusion statistical aggregate with Trino's `real`
/// overload.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RealStatAgg {
    inner: AggregateUDF,
    signature: Signature,
}

impl RealStatAgg {
    fn new(inner: AggregateUDF) -> Self {
        Self {
            inner,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn is_real_pair(arg_types: &[DataType]) -> bool {
    matches!(arg_types, [DataType::Float32, DataType::Float32])
}

fn to_double(values: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
    values
        .iter()
        .map(|v| Ok(cast(v, &DataType::Float64)?))
        .collect()
}

impl AggregateUDFImpl for RealStatAgg {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// `(real, real)` stays real; every other numeric pair is computed as
    /// double.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!("{} takes 2 arguments, got {}", self.name(), arg_types.len());
        }
        if is_real_pair(arg_types) {
            return Ok(vec![DataType::Float32; 2]);
        }
        for t in arg_types {
            let numeric = t.is_numeric() || matches!(t, DataType::Null);
            if !numeric {
                return plan_err!("{} does not accept {t}", self.name());
            }
        }
        Ok(vec![DataType::Float64; 2])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(if is_real_pair(arg_types) {
            DataType::Float32
        } else {
            DataType::Float64
        })
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let real = args.return_type() == &DataType::Float32;
        let inner = self.inner.accumulator(args)?;
        Ok(Box::new(RealAccumulator { inner, real }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let real = args.return_type() == &DataType::Float32;
        let inner = self.inner.create_groups_accumulator(args)?;
        Ok(Box::new(RealGroups { inner, real }))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_udf()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

#[derive(Debug)]
struct RealAccumulator {
    inner: Box<dyn Accumulator>,
    real: bool,
}

impl Accumulator for RealAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(&to_double(values)?)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let v = self.inner.evaluate()?;
        Ok(match v {
            ScalarValue::Float64(x) if self.real => ScalarValue::Float32(x.map(|x| x as f32)),
            other => other,
        })
    }

    fn size(&self) -> usize {
        self.inner.size() + size_of::<bool>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(&to_double(values)?)
    }

    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
}

struct RealGroups {
    inner: Box<dyn GroupsAccumulator>,
    real: bool,
}

impl GroupsAccumulator for RealGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner.update_batch(
            &to_double(values)?,
            group_indices,
            opt_filter,
            total_num_groups,
        )
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let out = self.inner.evaluate(emit_to)?;
        if self.real {
            Ok(cast(&out, &DataType::Float32)?)
        } else {
            Ok(out)
        }
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .merge_batch(values, group_indices, total_num_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(&to_double(values)?, opt_filter)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float32Array, Float64Array};
    use arrow::datatypes::DataType;
    use datafusion::prelude::SessionContext;

    use super::register_real_aggregates;

    fn ctx() -> SessionContext {
        let ctx = crate::duckdb_test_ctx();
        register_real_aggregates(&ctx);
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> (DataType, Option<f64>) {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        let v = match col.data_type() {
            DataType::Float32 => {
                let a = col.as_any().downcast_ref::<Float32Array>().unwrap();
                (!a.is_null(0)).then(|| f64::from(a.value(0)))
            }
            DataType::Float64 => {
                let a = col.as_any().downcast_ref::<Float64Array>().unwrap();
                (!a.is_null(0)).then(|| a.value(0))
            }
            t => panic!("unexpected type {t}"),
        };
        (col.data_type().clone(), v)
    }

    const REAL_T: &str = "(VALUES (CAST(1 AS REAL), CAST(2 AS REAL)), \
        (CAST(2 AS REAL), CAST(4 AS REAL)), (CAST(3 AS REAL), CAST(7 AS REAL)), \
        (CAST(NULL AS REAL), CAST(9 AS REAL))) AS t(x, y)";
    const DBL_T: &str = "(VALUES (CAST(1 AS DOUBLE), CAST(2 AS DOUBLE)), \
        (CAST(2 AS DOUBLE), CAST(4 AS DOUBLE)), (CAST(3 AS DOUBLE), CAST(7 AS DOUBLE)), \
        (CAST(NULL AS DOUBLE), CAST(9 AS DOUBLE))) AS t(x, y)";

    #[tokio::test]
    async fn real_inputs_return_real() {
        let ctx = ctx();
        // x = 1,2,3  y = 2,4,7: cov_pop = 5/3, cov_samp = 2.5,
        // slope = 2.5, intercept = 13/3 - 2.5*2 = -2/3.
        for (f, expect) in [
            ("covar_pop", 5.0 / 3.0),
            ("covar_samp", 2.5),
            ("regr_slope", 2.5),
            ("regr_intercept", -2.0 / 3.0),
            ("corr", 0.9933992677987828),
        ] {
            // regr_* and corr take (y, x); the order does not matter for the
            // symmetric ones, so swap to (y, x) only for the regressions.
            let args = if f.starts_with("regr") {
                "y, x"
            } else {
                "x, y"
            };
            let (t, v) = one(&ctx, &format!("SELECT {f}({args}) FROM {REAL_T}")).await;
            assert_eq!(t, DataType::Float32, "{f}");
            let v = v.unwrap();
            assert!((v - expect).abs() < 1e-5, "{f}: {v} vs {expect}");
        }
    }

    #[tokio::test]
    async fn double_inputs_stay_double() {
        let ctx = ctx();
        for f in [
            "corr",
            "covar_pop",
            "covar_samp",
            "regr_slope",
            "regr_intercept",
        ] {
            let (t, v) = one(&ctx, &format!("SELECT {f}(y, x) FROM {DBL_T}")).await;
            assert_eq!(t, DataType::Float64, "{f}");
            assert!(v.is_some());
        }
        // Mixed real/double and integers compute as double.
        let (t, _) = one(
            &ctx,
            "SELECT corr(CAST(x AS REAL), CAST(y AS DOUBLE)) FROM (VALUES (1, 2), (2, 5), (3, 7)) AS t(x, y)",
        )
        .await;
        assert_eq!(t, DataType::Float64);
        let (t, v) = one(
            &ctx,
            "SELECT covar_pop(x, y) FROM (VALUES (1, 2), (2, 4), (3, 7)) AS t(x, y)",
        )
        .await;
        assert_eq!(t, DataType::Float64);
        assert!((v.unwrap() - 5.0 / 3.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn null_and_empty_give_null_of_real() {
        let ctx = ctx();
        let (t, v) = one(
            &ctx,
            "SELECT corr(x, y) FROM (VALUES (CAST(NULL AS REAL), CAST(1 AS REAL))) AS t(x, y)",
        )
        .await;
        assert_eq!(t, DataType::Float32);
        assert_eq!(v, None);
        let (t, v) = one(
            &ctx,
            "SELECT regr_slope(y, x) FROM (VALUES (CAST(1 AS REAL), CAST(1 AS REAL))) AS t(x, y) WHERE x > 5",
        )
        .await;
        assert_eq!(t, DataType::Float32);
        assert_eq!(v, None);
    }

    #[tokio::test]
    async fn group_by_returns_real_per_group() {
        let ctx = ctx();
        let batches = ctx
            .sql(
                "SELECT g, covar_samp(x, y) AS c FROM (VALUES \
                 (1, CAST(1 AS REAL), CAST(2 AS REAL)), (1, CAST(2 AS REAL), CAST(4 AS REAL)), \
                 (1, CAST(3 AS REAL), CAST(7 AS REAL)), \
                 (2, CAST(1 AS REAL), CAST(1 AS REAL)), (2, CAST(2 AS REAL), CAST(3 AS REAL))) \
                 AS t(g, x, y) GROUP BY g ORDER BY g",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &batches[0];
        assert_eq!(batch.column(1).data_type(), &DataType::Float32);
        let c = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert!((c.value(0) - 2.5).abs() < 1e-6);
        assert!((c.value(1) - 1.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn window_use_keeps_working() {
        let ctx = ctx();
        let batches = ctx
            .sql(
                "SELECT corr(x, y) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
                 FROM (VALUES (CAST(1 AS REAL), CAST(2 AS REAL)), (CAST(2 AS REAL), CAST(4 AS REAL)), \
                 (CAST(3 AS REAL), CAST(7 AS REAL))) AS t(x, y) ORDER BY x",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        assert_eq!(col.data_type(), &DataType::Float32);
        let a = col.as_any().downcast_ref::<Float32Array>().unwrap();
        assert!(a.is_null(0));
        assert!((a.value(1) - 1.0).abs() < 1e-6);
        assert!((a.value(2) - 0.993399).abs() < 1e-4);
    }
}
