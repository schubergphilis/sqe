//! Trino's `real`-in, `real`-out aggregates: `avg`, `sum` and the
//! two-argument statistical ones.
//!
//! `avg(real)` and `sum(real)` return `real` in Trino; DataFusion widens both
//! to `double`. `min` and `max` already keep `real`; `stddev*` and
//! `variance`/`var_*` are `double` in Trino for every input, as they are here.
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
//! These shadow the DataFusion built-ins (Trino result type).
//! `SingleDistinctCountCompanionRule` and DataFusion's
//! `single_distinct_to_groupby` re-apply `sum` over its own partials, so a
//! `sum(real)` next to a `count(DISTINCT ...)` rounds each partial to `real`
//! before the outer sum; the type is still `real`.
//!
//! Trino has no `regr_count`, `regr_r2`, `regr_avgx`, `regr_avgy`,
//! `regr_sxx`, `regr_syy` or `regr_sxy`, so those keep DataFusion's types.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{plan_err, Result, ScalarValue};
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::correlation::Correlation;
use datafusion::functions_aggregate::covariance::{CovariancePopulation, CovarianceSample};
use datafusion::functions_aggregate::regr::{Regr, RegrType};
use datafusion::functions_aggregate::sum::Sum;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator,
    ReversedUDAF, SetMonotonicity, Signature, Volatility,
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
    // `avg` and `sum` wrap whichever version the context already has (the
    // engine's decimal ones), so decimal results keep Trino's type.
    let state = ctx.state();
    let funcs = state.aggregate_functions();
    let avg = funcs.get("avg").map_or_else(
        || AggregateUDF::new_from_impl(Avg::new()),
        |a| (**a).clone(),
    );
    let sum = funcs.get("sum").map_or_else(
        || AggregateUDF::new_from_impl(Sum::new()),
        |a| (**a).clone(),
    );
    for inner in [avg, sum] {
        ctx.register_udaf(AggregateUDF::new_from_impl(RealStatAgg::new(inner)));
    }
}

/// A DataFusion aggregate (two-argument statistical, or `avg` / `sum`) with
/// Trino's `real` overload.
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

    /// `avg` and `sum` take one argument and keep the wrapped aggregate's
    /// types for everything but `real`.
    fn is_unary(&self) -> bool {
        matches!(self.inner.name(), "avg" | "sum")
    }
}

/// `args` with its `real` input and result declared as `double`.
fn with_wide<T>(args: AccumulatorArgs, f: impl FnOnce(AccumulatorArgs) -> Result<T>) -> Result<T> {
    let fields = widen_fields(args.expr_fields);
    let return_field = Arc::new(
        args.return_field
            .as_ref()
            .clone()
            .with_data_type(DataType::Float64),
    );
    f(AccumulatorArgs {
        return_field,
        expr_fields: &fields,
        ..args
    })
}

/// Every argument is `real` (one for `avg`, two for the statistical ones).
fn is_real(arg_types: &[DataType]) -> bool {
    !arg_types.is_empty() && arg_types.iter().all(|t| t == &DataType::Float32)
}

/// The same call with `real` inputs and result declared as `double`: the
/// wrapped DataFusion aggregate then runs in double precision.
fn widen_fields(fields: &[FieldRef]) -> Vec<FieldRef> {
    fields
        .iter()
        .map(|f| Arc::new(f.as_ref().clone().with_data_type(DataType::Float64)))
        .collect()
}

/// Widen `real` inputs to double for the wrapped aggregate; every other
/// input is already what the wrapped aggregate takes and passes unchanged.
fn to_double(real: bool, values: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
    values
        .iter()
        .map(|v| {
            if real {
                Ok(cast(v, &DataType::Float64)?)
            } else {
                Ok(Arc::clone(v))
            }
        })
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
        if self.is_unary() {
            // `avg(real)` / `sum(real)` stay real; every other input follows
            // the wrapped aggregate's own signature.
            if let [DataType::Float32] = arg_types {
                return Ok(vec![DataType::Float32]);
            }
            let fields: Vec<FieldRef> = arg_types
                .iter()
                .map(|t| Arc::new(Field::new("x", t.clone(), true)))
                .collect();
            return Ok(fields_with_udf(&fields, &self.inner)?
                .iter()
                .map(|f| f.data_type().clone())
                .collect());
        }
        if arg_types.len() != 2 {
            return plan_err!("{} takes 2 arguments, got {}", self.name(), arg_types.len());
        }
        if is_real(arg_types) {
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
        if is_real(arg_types) {
            Ok(DataType::Float32)
        } else if self.is_unary() {
            self.inner.return_type(arg_types)
        } else {
            Ok(DataType::Float64)
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let real = args.return_type() == &DataType::Float32;
        if !real {
            let inner = self.inner.accumulator(args)?;
            return Ok(Box::new(RealAccumulator { inner, real }));
        }
        let fields = widen_fields(args.expr_fields);
        let wide = AccumulatorArgs {
            return_field: Arc::new(
                args.return_field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Float64),
            ),
            expr_fields: &fields,
            ..args
        };
        let inner = self.inner.accumulator(wide)?;
        Ok(Box::new(RealAccumulator { inner, real }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.return_type() != &DataType::Float32 {
            return self.inner.state_fields(args);
        }
        let fields = widen_fields(args.input_fields);
        self.inner.state_fields(StateFieldsArgs {
            input_fields: &fields,
            return_field: Arc::new(
                args.return_field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Float64),
            ),
            ..args
        })
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        if args.return_type() != &DataType::Float32 {
            return self.inner.groups_accumulator_supported(args);
        }
        let fields = widen_fields(args.expr_fields);
        let wide = AccumulatorArgs {
            return_field: Arc::new(
                args.return_field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Float64),
            ),
            expr_fields: &fields,
            ..args
        };
        self.inner.groups_accumulator_supported(wide)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let real = args.return_type() == &DataType::Float32;
        if !real {
            let inner = self.inner.create_groups_accumulator(args)?;
            return Ok(Box::new(RealGroups { inner, real }));
        }
        let fields = widen_fields(args.expr_fields);
        let wide = AccumulatorArgs {
            return_field: Arc::new(
                args.return_field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Float64),
            ),
            expr_fields: &fields,
            ..args
        };
        let inner = self.inner.create_groups_accumulator(wide)?;
        Ok(Box::new(RealGroups { inner, real }))
    }

    /// Window frames that slide need a retracting accumulator; DataFusion's
    /// plain `sum` one cannot retract.
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let real = args.return_type() == &DataType::Float32;
        let inner = if real {
            with_wide(args, |a| self.inner.create_sliding_accumulator(a))?
        } else {
            self.inner.create_sliding_accumulator(args)?
        };
        Ok(Box::new(RealAccumulator { inner, real }))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.inner().order_sensitivity()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.inner().set_monotonicity(data_type)
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
        self.inner.update_batch(&to_double(self.real, values)?)
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
        self.inner.retract_batch(&to_double(self.real, values)?)
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
            &to_double(self.real, values)?,
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
        self.inner
            .convert_to_state(&to_double(self.real, values)?, opt_filter)
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

    #[tokio::test]
    async fn avg_of_real_is_real() {
        let ctx = ctx();
        let (t, v) = one(
            &ctx,
            "SELECT avg(x) FROM (VALUES (CAST(1 AS REAL)), (CAST(2 AS REAL)), (CAST(4 AS REAL)), (CAST(NULL AS REAL))) AS t(x)",
        )
        .await;
        assert_eq!(t, DataType::Float32);
        assert!((v.unwrap() - 7.0 / 3.0).abs() < 1e-6);
        // DISTINCT, GROUP BY and a window frame take the same path.
        let (t, v) = one(
            &ctx,
            "SELECT avg(DISTINCT x) FROM (VALUES (CAST(1 AS REAL)), (CAST(1 AS REAL)), (CAST(3 AS REAL))) AS t(x)",
        )
        .await;
        assert_eq!(t, DataType::Float32);
        assert_eq!(v, Some(2.0));
        let batches = ctx
            .sql("SELECT g, avg(x) FROM (VALUES (1, CAST(1 AS REAL)), (1, CAST(2 AS REAL)), (2, CAST(5 AS REAL))) AS t(g, x) GROUP BY g ORDER BY g")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!((col.value(0), col.value(1)), (1.5, 5.0));
        let (t, v) = one(
            &ctx,
            "SELECT avg(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (CAST(1 AS REAL)), (CAST(3 AS REAL))) AS t(x) ORDER BY x DESC",
        )
        .await;
        assert_eq!(t, DataType::Float32);
        assert_eq!(v, Some(2.0));
        // No rows, or only NULLs, is a NULL real.
        let (t, v) = one(
            &ctx,
            "SELECT avg(x) FROM (VALUES (CAST(NULL AS REAL))) AS t(x)",
        )
        .await;
        assert_eq!((t, v), (DataType::Float32, None));
    }

    #[tokio::test]
    async fn avg_of_other_types_is_unchanged() {
        let ctx = ctx();
        let (t, v) = one(&ctx, "SELECT avg(x) FROM (VALUES (1), (2)) AS t(x)").await;
        assert_eq!((t, v), (DataType::Float64, Some(1.5)));
        let (t, _) = one(
            &ctx,
            "SELECT avg(CAST(x AS DOUBLE)) FROM (VALUES (1), (2)) AS t(x)",
        )
        .await;
        assert_eq!(t, DataType::Float64);
        let df = ctx
            .sql("SELECT avg(CAST(x AS DECIMAL(10, 2))) FROM (VALUES (1), (2)) AS t(x)")
            .await
            .unwrap();
        let t = df.schema().field(0).data_type().clone();
        assert!(matches!(t, DataType::Decimal128(_, 6)), "{t}");
    }

    /// Trino: `sum(real)` is `real`, accumulated in double precision.
    #[tokio::test]
    async fn sum_of_real_is_real() {
        let ctx = ctx();
        let (t, v) = one(
            &ctx,
            "SELECT sum(x) FROM (VALUES (CAST(1.5 AS REAL)), (CAST(2 AS REAL)), (CAST(NULL AS REAL))) AS t(x)",
        )
        .await;
        assert_eq!((t, v), (DataType::Float32, Some(3.5)));
        let (t, v) = one(
            &ctx,
            "SELECT sum(DISTINCT x) FROM (VALUES (CAST(1 AS REAL)), (CAST(1 AS REAL)), (CAST(3 AS REAL))) AS t(x)",
        )
        .await;
        assert_eq!((t, v), (DataType::Float32, Some(4.0)));
        // A sliding frame needs the retracting accumulator: rows 5, 3, 1
        // give 8, 4, 1.
        let batches = ctx
            .sql("SELECT sum(x) OVER (ORDER BY x DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM (VALUES (CAST(1 AS REAL)), (CAST(3 AS REAL)), (CAST(5 AS REAL))) AS t(x) ORDER BY x DESC")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let got: Vec<f32> = batches
            .iter()
            .flat_map(|b| {
                let col = b.column(0).as_any().downcast_ref::<Float32Array>().unwrap();
                col.values().to_vec()
            })
            .collect();
        assert_eq!(got, vec![8.0, 4.0, 1.0]);
        let (t, v) = one(
            &ctx,
            "SELECT sum(x) FROM (VALUES (CAST(NULL AS REAL))) AS t(x)",
        )
        .await;
        assert_eq!((t, v), (DataType::Float32, None));
    }

    #[tokio::test]
    async fn sum_of_other_types_is_unchanged() {
        let ctx = ctx();
        for (sql, want) in [
            (
                "SELECT sum(x) FROM (VALUES (1), (2)) AS t(x)",
                DataType::Int64,
            ),
            (
                "SELECT sum(CAST(x AS DOUBLE)) FROM (VALUES (1), (2)) AS t(x)",
                DataType::Float64,
            ),
            (
                "SELECT sum(CAST(x AS DECIMAL(10, 2))) FROM (VALUES (1), (2)) AS t(x)",
                DataType::Decimal128(20, 2),
            ),
        ] {
            let df = ctx.sql(sql).await.unwrap();
            assert_eq!(df.schema().field(0).data_type(), &want, "{sql}");
        }
    }

    /// Trino: `sum`, `min`, `max` keep `real`; `stddev`, `variance` and their
    /// `_pop` / `_samp` forms are `double` whatever the input.
    #[tokio::test]
    async fn other_real_aggregates_already_match_trino() {
        let ctx = ctx();
        for f in ["min", "max"] {
            let (t, _) = one(
                &ctx,
                &format!(
                    "SELECT {f}(x) FROM (VALUES (CAST(1 AS REAL)), (CAST(2 AS REAL))) AS t(x)"
                ),
            )
            .await;
            assert_eq!(t, DataType::Float32, "{f}");
        }
        for f in ["stddev", "stddev_pop", "stddev_samp", "var_pop", "var_samp"] {
            let (t, _) = one(
                &ctx,
                &format!(
                    "SELECT {f}(x) FROM (VALUES (CAST(1 AS REAL)), (CAST(2 AS REAL))) AS t(x)"
                ),
            )
            .await;
            assert_eq!(t, DataType::Float64, "{f}");
        }
    }
}
