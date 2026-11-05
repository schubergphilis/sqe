//! Decimal results round half away from zero, as in Trino, where DataFusion
//! truncates: `2.00 / 3.00` is 0.666667 (not 0.666666), `CAST(7.5 AS INT)`
//! is 8. `avg(decimal(p,s))` has Trino's type `decimal(p,s)`, not
//! DataFusion's `decimal(p+4,s+4)`: `avg` of 0.01, 0.01, 0.00 is 0.01.
//!
//! Three pieces, all registered by `engine_context`:
//!
//! - [`RescaleUdf`] (`__sqe_rescale(x, '<type>', safe)`): arrow's decimal
//!   cast, which already rounds half away from zero, behind a function the
//!   optimizer cannot see through. A plain `CAST` to a smaller scale gets
//!   stripped from a comparison by DataFusion's `unwrap_cast_in_comparison`:
//!   `CAST(x AS DECIMAL(10,2)) = 1.23` becomes `x = 1.230` and misses every
//!   `x` that only rounds to 1.23.
//! - [`DecimalRoundingRule`], an analyzer rule after type coercion: a decimal
//!   division gets one guard digit on its dividend and is rounded back to
//!   its planned type; a cast to a smaller decimal scale, or from a decimal
//!   to an integer, rounds through [`RescaleUdf`].
//! - [`RoundedAvg`]: DataFusion's `avg` with Trino's decimal result type,
//!   whose final division rounds. Count, sum and state are DataFusion's own;
//!   only the result type and the last step differ.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Decimal256Array};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{
    i256, DataType, Field, FieldRef, UInt64Type, DECIMAL256_MAX_PRECISION,
};
use datafusion::arrow::util::display::FormatOptions;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{exec_err, internal_err, plan_err, DFSchema, ScalarValue};
use datafusion::error::Result;
use datafusion::functions_aggregate::average::Avg;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, BinaryExpr, Cast, ColumnarValue, DmlStatement,
    Documentation, EmitTo, Expr, ExprSchemable, GroupsAccumulator, LogicalPlan, Operator,
    ReturnFieldArgs, ReversedUDAF, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TryCast, Volatility, WriteOp,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

/// Name of [`RescaleUdf`].
pub const RESCALE: &str = "__sqe_rescale";

/// Register [`RescaleUdf`] and [`RoundedAvg`] (over DataFusion's `avg` and
/// `mean`) on `ctx`. A worker only needs the UDF, to decode a pushed
/// predicate that holds one: [`register_rescale`].
pub fn register_decimal_rounding(ctx: &SessionContext) {
    register_rescale(ctx);
    ctx.register_udaf(AggregateUDF::new_from_impl(RoundedAvg::new()));
}

/// Register [`RescaleUdf`] on `ctx`.
pub fn register_rescale(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(RescaleUdf::new()));
}

/// `__sqe_rescale(x, '<arrow type>', safe)`: cast `x` to the type named by
/// the string literal, rounding half away from zero when the scale drops.
/// `safe` is `true` for `TRY_CAST` (an overflow gives NULL, not an error).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RescaleUdf {
    signature: Signature,
}

impl RescaleUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }

    /// `__sqe_rescale(expr, target, safe)`.
    fn call(expr: Expr, target: &DataType, safe: bool) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(Self::new())),
            vec![
                expr,
                Expr::Literal(ScalarValue::Utf8(Some(target.to_string())), None),
                Expr::Literal(ScalarValue::Boolean(Some(safe)), None),
            ],
        ))
    }
}

impl Default for RescaleUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for RescaleUdf {
    fn name(&self) -> &str {
        RESCALE
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("{RESCALE} reports its type through return_field_from_args")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let target = match args.scalar_arguments.get(1) {
            Some(Some(ScalarValue::Utf8(Some(name)))) => name.parse::<DataType>()?,
            _ => return plan_err!("{RESCALE} takes the target type as a string literal"),
        };
        let safe = match args.scalar_arguments.get(2) {
            Some(Some(ScalarValue::Boolean(Some(safe)))) => *safe,
            _ => return plan_err!("{RESCALE} takes `safe` as a boolean literal"),
        };
        let nullable = safe || args.arg_fields.first().is_none_or(|f| f.is_nullable());
        Ok(Arc::new(Field::new(RESCALE, target, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let safe = matches!(
            args.args.get(2),
            Some(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
        );
        let options = CastOptions {
            safe,
            format_options: FormatOptions::default(),
        };
        args.args[0].cast_to(args.return_field.data_type(), Some(&options))
    }
}

/// Rounds decimal division and decimal casts half away from zero. Runs after
/// type coercion, so every operand type is final.
#[derive(Debug, Default)]
pub struct DecimalRoundingRule;

impl AnalyzerRule for DecimalRoundingRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(round_plan).data()
    }

    fn name(&self) -> &str {
        "sqe_decimal_rounding"
    }
}

/// Rewrite one node's expressions against the schema its expressions read,
/// the same schema DataFusion's type coercion resolves them in.
fn round_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = merge_schema(&plan.inputs());
    if let LogicalPlan::TableScan(scan) = &plan {
        let source =
            DFSchema::try_from_qualified_schema(scan.table_name.clone(), &scan.source.schema())?;
        schema.merge(&source);
    }
    if let LogicalPlan::Dml(DmlStatement {
        op: WriteOp::MergeInto(_),
        table_name,
        target,
        ..
    }) = &plan
    {
        let target = DFSchema::try_from_qualified_schema(table_name.clone(), &target.schema())?;
        schema.merge(&target);
    }

    let names = NamePreserver::new(&plan);
    let out = plan.map_expressions(|expr| {
        let name = names.save(&expr);
        Ok(round_expr(expr, &schema)?.update_data(|e| name.restore(e)))
    })?;
    if out.transformed {
        out.map_data(|plan| plan.recompute_schema())
    } else {
        Ok(out)
    }
}

/// Bottom up, skipping what an earlier pass already rounded: a view body is
/// analyzed on its own and again inside the statement that reads it, and a
/// second guard digit would round twice.
fn round_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    expr.transform_down_up(
        |e| {
            let done = matches!(&e, Expr::ScalarFunction(f) if f.func.name() == RESCALE);
            Ok(if done {
                Transformed::new(e, false, TreeNodeRecursion::Jump)
            } else {
                Transformed::no(e)
            })
        },
        |e| round_node(e, schema),
    )
}

fn round_node(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let rounded = match &expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Divide,
            right,
        }) => round_division(&expr, left, right, schema),
        Expr::Cast(Cast { expr: inner, field }) if field.metadata().is_empty() => {
            round_cast(inner, field.data_type(), false, schema)
        }
        Expr::TryCast(TryCast { expr: inner, field }) if field.metadata().is_empty() => {
            round_cast(inner, field.data_type(), true, schema)
        }
        _ => None,
    };
    Ok(match rounded {
        Some(e) => Transformed::yes(e),
        None => Transformed::no(expr),
    })
}

/// `a / b` over decimals as `__sqe_rescale(CAST(a AS <one more digit>) / b,
/// '<planned type>')`: arrow divides with truncation, so one extra digit,
/// rounded away, gives the half-up result at the planned scale. `None` when
/// the operands are not two decimals of one width, or no guard digit fits.
fn round_division(div: &Expr, left: &Expr, right: &Expr, schema: &DFSchema) -> Option<Expr> {
    let planned = div.get_type(schema).ok()?;
    let planned_scale = decimal_scale(&planned)?;
    let (lt, rt) = (left.get_type(schema).ok()?, right.get_type(schema).ok()?);
    let (guard_left, guard_right) = match (&lt, &rt) {
        (DataType::Decimal128(p1, s1), DataType::Decimal128(_, s2))
            if *s1 >= 0 && *s2 >= 0 && i16::from(*p1) + i16::from(*s2) + 5 <= 38 =>
        {
            (DataType::Decimal128(p1 + 1, s1 + 1), rt.clone())
        }
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2))
        | (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2))
            if *s1 >= 0 && *s2 >= 0 && *p1 < DECIMAL256_MAX_PRECISION =>
        {
            (
                DataType::Decimal256(p1 + 1, s1 + 1),
                DataType::Decimal256(*p2, *s2),
            )
        }
        _ => return None,
    };
    let cast = |e: &Expr, to: &DataType, from: &DataType| {
        if to == from {
            e.clone()
        } else {
            Expr::Cast(Cast::new(Box::new(e.clone()), to.clone()))
        }
    };
    let guarded = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(cast(left, &guard_left, &lt)),
        Operator::Divide,
        Box::new(cast(right, &guard_right, &rt)),
    ));
    // Only worth it when the guarded quotient carries a digit past the
    // planned scale (not so at the maximum scale).
    let guarded_scale = decimal_scale(&guarded.get_type(schema).ok()?)?;
    (guarded_scale > planned_scale).then(|| RescaleUdf::call(guarded, &planned, false))
}

/// A cast from a decimal to a smaller decimal scale, or to an integer,
/// through [`RescaleUdf`].
fn round_cast(inner: &Expr, target: &DataType, safe: bool, schema: &DFSchema) -> Option<Expr> {
    let source = inner.get_type(schema).ok()?;
    let (precision, scale) = decimal_parts(&source)?;
    if scale <= 0 {
        return None;
    }
    if let Some(target_scale) = decimal_scale(target) {
        return (target_scale < scale).then(|| RescaleUdf::call(inner.clone(), target, safe));
    }
    if !target.is_integer() {
        return None;
    }
    // Round to scale 0 first: 9.5 needs one more integer digit as 10.
    let digits = precision.saturating_sub(scale as u8).saturating_add(1);
    let whole = match source {
        DataType::Decimal32(..) => DataType::Decimal32(digits, 0),
        DataType::Decimal64(..) => DataType::Decimal64(digits, 0),
        DataType::Decimal128(..) => DataType::Decimal128(digits, 0),
        _ => DataType::Decimal256(digits, 0),
    };
    let rounded = Box::new(RescaleUdf::call(inner.clone(), &whole, safe));
    Some(if safe {
        Expr::TryCast(TryCast::new(rounded, target.clone()))
    } else {
        Expr::Cast(Cast::new(rounded, target.clone()))
    })
}

fn decimal_parts(t: &DataType) -> Option<(u8, i8)> {
    match t {
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => Some((*p, *s)),
        _ => None,
    }
}

fn decimal_scale(t: &DataType) -> Option<i8> {
    decimal_parts(t).map(|(_, s)| s)
}

/// DataFusion's `avg` (and `mean`), returning the input decimal type as
/// Trino does and rounding half away from zero. The wrapped accumulators
/// keep DataFusion's state, so partial and final aggregates, windows and
/// spilled states are unchanged; only the final `sum / count` is computed
/// here.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RoundedAvg {
    inner: Avg,
}

impl RoundedAvg {
    pub fn new() -> Self {
        Self { inner: Avg::new() }
    }
}

impl Default for RoundedAvg {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for RoundedAvg {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    /// Trino's type: `avg(decimal(p,s))` is `decimal(p,s)`, where DataFusion
    /// widens to `decimal(p+4,s+4)`. The mean of values that fit `(p,s)`
    /// fits too.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [t @ (DataType::Decimal128(..) | DataType::Decimal256(..))] => Ok(t.clone()),
            _ => self.inner.return_type(arg_types),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let ret = args.return_type().clone();
        let distinct = args.is_distinct;
        let inner = self.inner.accumulator(args)?;
        Ok(if decimal_parts(&ret).is_some() {
            Box::new(RoundedAvgAccumulator {
                inner,
                ret,
                distinct,
            })
        } else {
            inner
        })
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
        let ret = args.return_type().clone();
        let inner = self.inner.create_groups_accumulator(args)?;
        Ok(if decimal_parts(&ret).is_some() {
            Box::new(RoundedAvgGroups { inner, ret })
        } else {
            inner
        })
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_expr()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

/// `avg` over one group (no `GROUP BY`, `DISTINCT`, windows).
#[derive(Debug)]
struct RoundedAvgAccumulator {
    inner: Box<dyn Accumulator>,
    ret: DataType,
    distinct: bool,
}

impl Accumulator for RoundedAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let state = self.inner.state()?;
        let (count, sum) = if self.distinct {
            // One list of the distinct values.
            let values = match state.first() {
                Some(ScalarValue::List(list)) if !list.is_null(0) => list.value(0),
                _ => return ScalarValue::try_from(&self.ret),
            };
            distinct_count_and_sum(&values)?
        } else {
            let [count, sum] = state.as_slice() else {
                return internal_err!("avg state has {} fields, expected 2", state.len());
            };
            (count.to_array()?, sum.to_array()?)
        };
        let avg = rounded_avg(&count, &sum, &self.ret)?;
        ScalarValue::try_from_array(&avg, 0)
    }

    fn size(&self) -> usize {
        self.inner.size() + size_of::<Self>() - size_of::<Box<dyn Accumulator>>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(values)
    }

    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
}

/// `avg` with `GROUP BY`. `state` hands over the same counts and sums
/// `evaluate` would have divided, and frees them the same way.
struct RoundedAvgGroups {
    inner: Box<dyn GroupsAccumulator>,
    ret: DataType,
}

impl GroupsAccumulator for RoundedAvgGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&datafusion::arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let state = self.inner.state(emit_to)?;
        let [count, sum] = state.as_slice() else {
            return internal_err!("avg state has {} fields, expected 2", state.len());
        };
        rounded_avg(count, sum, &self.ret)
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
        opt_filter: Option<&datafusion::arrow::array::BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(values, opt_filter)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

/// The count and sum of `AVG(DISTINCT)`'s value list, as one-row arrays.
fn distinct_count_and_sum(values: &ArrayRef) -> Result<(ArrayRef, ArrayRef)> {
    let scale = decimal_scale(values.data_type()).unwrap_or(0);
    let wide = to_i256(values, scale)?;
    let mut sum = i256::ZERO;
    let mut count = 0u64;
    for v in wide.iter().flatten() {
        let Some(next) = sum.checked_add(v) else {
            return exec_err!("Arithmetic overflow in avg");
        };
        sum = next;
        count += 1;
    }
    let count: ArrayRef = Arc::new(datafusion::arrow::array::UInt64Array::from(vec![count]));
    let sum: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(sum)])
            .with_precision_and_scale(DECIMAL256_MAX_PRECISION, scale)?,
    );
    Ok((count, sum))
}

/// `sum` as `Decimal256(76, scale)`; widening, never lossy.
fn to_i256(sum: &ArrayRef, scale: i8) -> Result<Decimal256Array> {
    let wide = cast_with_options(
        sum,
        &DataType::Decimal256(DECIMAL256_MAX_PRECISION, scale),
        &CastOptions::default(),
    )?;
    Ok(wide
        .as_primitive::<datafusion::arrow::datatypes::Decimal256Type>()
        .clone())
}

/// `sum / count` per row at the scale of `ret`, rounded half away from zero,
/// as `ret`. NULL where the count is 0 or the sum is NULL.
fn rounded_avg(count: &ArrayRef, sum: &ArrayRef, ret: &DataType) -> Result<ArrayRef> {
    let Some(sum_scale) = decimal_scale(sum.data_type()) else {
        return internal_err!("avg sum is {}, expected a decimal", sum.data_type());
    };
    let Some((_, ret_scale)) = decimal_parts(ret) else {
        return internal_err!("avg result is {ret}, expected a decimal");
    };
    let Some(shift) = u32::try_from(i32::from(ret_scale) - i32::from(sum_scale))
        .ok()
        .and_then(|k| i256::from_i128(10).checked_pow(k))
    else {
        return internal_err!("avg result scale {ret_scale} below sum scale {sum_scale}");
    };
    let counts = count.as_primitive::<UInt64Type>();
    let sums = to_i256(sum, sum_scale)?;
    let mut out = Vec::with_capacity(sums.len());
    for i in 0..sums.len() {
        if counts.is_null(i) || counts.value(i) == 0 || sums.is_null(i) {
            out.push(None);
            continue;
        }
        let n = i256::from_i128(i128::from(counts.value(i)));
        match div_round_half_away(sums.value(i), n, shift) {
            Some(v) => out.push(Some(v)),
            None => return exec_err!("Arithmetic overflow in avg"),
        }
    }
    let wide: ArrayRef = Arc::new(
        Decimal256Array::from(out).with_precision_and_scale(DECIMAL256_MAX_PRECISION, ret_scale)?,
    );
    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    Ok(cast_with_options(&wide, ret, &options)?)
}

/// `sum * shift / n`, rounded half away from zero. Divides before it
/// multiplies, so only a result that does not fit overflows.
fn div_round_half_away(sum: i256, n: i256, shift: i256) -> Option<i256> {
    let whole = sum.checked_div(n)?;
    let rest = sum.checked_rem(n)?.checked_mul(shift)?;
    let mut frac = rest.checked_div(n)?;
    let left = rest.checked_rem(n)?;
    if left.wrapping_abs().checked_mul(i256::from_i128(2))? >= n {
        frac = frac.checked_add(rest.signum())?;
    }
    whole.checked_mul(shift)?.checked_add(frac)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Decimal128Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::config::ConfigOptions;
    use datafusion::datasource::MemTable;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::prelude::SessionContext;

    use super::{DecimalRoundingRule, RESCALE};
    use crate::session_tuning::{engine_context, engine_session_config};

    /// `t(g, m DECIMAL(7,2), x DECIMAL(10,3))` over four partitions, so a
    /// grouped `avg` runs partial, repartition and final.
    fn table() -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("m", DataType::Decimal128(7, 2), true),
            Field::new("x", DataType::Decimal128(10, 3), true),
        ]));
        // avg, sum/3: g=1: 0.01 0.01 0.00 -> 0.01, 0.006667;
        // g=2: 0.02 0.01 -0.01 -> 0.01, 0.006667;
        // g=3: -0.01 -0.01 0.00 -> -0.01, -0.006667.
        let rows: [(i64, i128, i128); 9] = [
            (1, 1, 1234),
            (1, 1, 1226),
            (1, 0, 1225),
            (2, 2, 1236),
            (2, 1, 1224),
            (2, -1, -1225),
            (3, -1, 7500),
            (3, -1, -7500),
            (3, 0, 9500),
        ];
        let partitions = rows
            .chunks(3)
            .map(|chunk| {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(
                            chunk.iter().map(|r| r.0).collect::<Vec<_>>(),
                        )),
                        Arc::new(
                            Decimal128Array::from(chunk.iter().map(|r| r.1).collect::<Vec<_>>())
                                .with_precision_and_scale(7, 2)
                                .unwrap(),
                        ),
                        Arc::new(
                            Decimal128Array::from(chunk.iter().map(|r| r.2).collect::<Vec<_>>())
                                .with_precision_and_scale(10, 3)
                                .unwrap(),
                        ),
                    ],
                )
                .unwrap();
                vec![batch]
            })
            .collect();
        Arc::new(MemTable::try_new(schema, partitions).unwrap())
    }

    fn ctx() -> SessionContext {
        let ctx = engine_context(
            engine_session_config().with_target_partitions(4),
            Arc::new(RuntimeEnv::default()),
        );
        ctx.register_table("t", table()).unwrap();
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }

    /// The one value of a one-row, one-column result.
    async fn value(ctx: &SessionContext, sql: &str) -> String {
        let out = rows(ctx, sql).await;
        let line = out.lines().nth(3).unwrap_or_default();
        line.trim_matches(|c| c == '|' || c == ' ').to_string()
    }

    #[tokio::test]
    async fn decimal_division_rounds_half_away_from_zero() {
        let ctx = ctx();
        assert_eq!(value(&ctx, "SELECT 2.00 / 3.00").await, "0.666667");
        assert_eq!(value(&ctx, "SELECT -2.00 / 3.00").await, "-0.666667");
        assert_eq!(value(&ctx, "SELECT 1.00 / 8.00").await, "0.125000");
        assert_eq!(value(&ctx, "SELECT 1.00 / 3.00").await, "0.333333");
        // Over a column, not folded at planning.
        let out = rows(
            &ctx,
            "SELECT g, sum(m) / 3.00 AS q FROM t GROUP BY g ORDER BY g",
        )
        .await;
        assert!(out.contains("| 1 | 0.006667  |"), "\n{out}");
        assert!(out.contains("| 3 | -0.006667 |"), "\n{out}");
    }

    /// p1 + s2 + 5 > 38: the guard digit goes through Decimal256 and the
    /// result keeps its Decimal128 type.
    #[tokio::test]
    async fn wide_decimal_division_rounds_through_decimal256() {
        let ctx = ctx();
        let sql = "SELECT CAST(2 AS DECIMAL(38,10)) / CAST(3 AS DECIMAL(38,2)) AS q";
        assert_eq!(value(&ctx, sql).await, "0.66666666666667");
        let df = ctx.sql(sql).await.unwrap();
        assert_eq!(
            df.schema().field(0).data_type(),
            &DataType::Decimal128(38, 14)
        );
    }

    #[tokio::test]
    async fn avg_of_decimals_rounds_half_away_from_zero() {
        // Trino's type: avg(decimal(7,2)) is decimal(7,2), so 0.00666...
        // rounds to 0.01 (truncation gave 0.00).
        let ctx = ctx();
        let grouped = rows(&ctx, "SELECT g, avg(m) AS a FROM t GROUP BY g ORDER BY g").await;
        for line in ["| 1 | 0.01  |", "| 2 | 0.01  |", "| 3 | -0.01 |"] {
            assert!(grouped.contains(line), "{line}\n{grouped}");
        }
        assert_eq!(
            value(&ctx, "SELECT avg(m) FROM t WHERE g = 1").await,
            "0.01"
        );
        assert_eq!(
            value(&ctx, "SELECT mean(m) FROM t WHERE g = 3").await,
            "-0.01"
        );
        assert_eq!(
            value(&ctx, "SELECT avg(DISTINCT m) FROM t WHERE g = 2").await,
            "0.01"
        );
        let window = rows(
            &ctx,
            "SELECT DISTINCT g, avg(m) OVER (PARTITION BY g) AS a FROM t ORDER BY g",
        )
        .await;
        assert!(window.contains("| 1 | 0.01  |"), "\n{window}");
        assert!(window.contains("| 3 | -0.01 |"), "\n{window}");
        // Sliding frame: retract keeps the same state. At 1.226 the frame
        // holds 0.00 and 0.01, an exact half: 0.005 rounds away to 0.01.
        let sliding = rows(
            &ctx,
            "SELECT x, avg(m) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a \
             FROM t WHERE g = 1 ORDER BY x",
        )
        .await;
        assert!(sliding.contains("| 1.225 | 0.00 |"), "\n{sliding}");
        assert!(sliding.contains("| 1.226 | 0.01 |"), "\n{sliding}");
        assert!(sliding.contains("| 1.234 | 0.01 |"), "\n{sliding}");
        // -0.005 rounds away from zero too.
        assert_eq!(
            value(&ctx, "SELECT avg(m) FROM t WHERE g = 3 AND x > 0").await,
            "-0.01"
        );
        let df = ctx.sql("SELECT avg(m) FROM t").await.unwrap();
        assert_eq!(
            df.schema().field(0).data_type(),
            &DataType::Decimal128(7, 2)
        );
        assert_eq!(df.schema().field(0).name(), "avg(t.m)");
        // Integers still average to a double, as in Trino.
        let df = ctx.sql("SELECT avg(g) FROM t").await.unwrap();
        assert_eq!(df.schema().field(0).data_type(), &DataType::Float64);
    }

    #[tokio::test]
    async fn decimal_to_integer_cast_rounds_half_away_from_zero() {
        let ctx = ctx();
        let out = rows(
            &ctx,
            "SELECT x, CAST(x AS INT) AS i, TRY_CAST(x AS BIGINT) AS j FROM t WHERE g = 3 ORDER BY x",
        )
        .await;
        for line in [
            "| -7.500 | -8 | -8 |",
            "| 7.500  | 8  | 8  |",
            "| 9.500  | 10 | 10 |",
        ] {
            assert!(out.contains(line), "{line}\n{out}");
        }
        assert_eq!(value(&ctx, "SELECT CAST(7.4 AS INT)").await, "7");
    }

    /// DataFusion's `unwrap_cast_in_comparison` turned this into `x = 1.230`
    /// and returned no row.
    #[tokio::test]
    async fn scale_reducing_cast_in_a_filter_compares_the_rounded_value() {
        let ctx = ctx();
        let out = rows(
            &ctx,
            "SELECT x FROM t WHERE CAST(x AS DECIMAL(10,2)) = 1.23 ORDER BY x",
        )
        .await;
        let got: Vec<&str> = out
            .lines()
            .skip(3)
            .filter(|l| l.starts_with("| "))
            .collect();
        assert_eq!(got, ["| 1.225 |", "| 1.226 |", "| 1.234 |"], "\n{out}");
        let negative = value(
            &ctx,
            "SELECT count(*) FROM t WHERE CAST(x AS DECIMAL(10,2)) = -1.23",
        )
        .await;
        assert_eq!(negative, "1");
        let plan = rows(
            &ctx,
            "EXPLAIN SELECT x FROM t WHERE CAST(x AS DECIMAL(10,2)) = 1.23",
        )
        .await;
        assert!(plan.contains(RESCALE), "\n{plan}");
    }

    /// Column names and types are DataFusion's, so a client sees the same
    /// schema with or without the rewrite. The one exception, the type of
    /// `avg(decimal)`, is asserted in `avg_of_decimals_rounds_half_away_from_zero`.
    #[tokio::test]
    async fn rewrite_keeps_column_names_and_types() {
        let ctx = ctx();
        let plain = SessionContext::new_with_config(engine_session_config());
        plain.register_table("t", table()).unwrap();
        for sql in [
            "SELECT m / 3.00, CAST(x AS DECIMAL(10,2)) AS c, CAST(x AS INT) AS i FROM t",
            "SELECT g, avg(g), sum(x) / count(x) FROM t GROUP BY g",
            "SELECT CAST(x AS DECIMAL(10,1)) AS c, count(*) FROM t GROUP BY CAST(x AS DECIMAL(10,1))",
        ] {
            let ours = ctx.sql(sql).await.unwrap();
            let theirs = plain.sql(sql).await.unwrap();
            assert_eq!(ours.schema().as_arrow(), theirs.schema().as_arrow(), "{sql}");
            ours.collect().await.unwrap();
        }
    }

    /// A view body is analyzed alone and again inside the statement that
    /// reads it; a second pass must not add a second guard digit.
    #[tokio::test]
    async fn rule_is_idempotent() {
        let ctx = ctx();
        let sql = "SELECT g, sum(m) / 3.00 AS q, CAST(x AS INT) AS i FROM t \
                   WHERE CAST(x AS DECIMAL(10,2)) > 1.00 GROUP BY g, x";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let options = ConfigOptions::default();
        let coerced = TypeCoercion::new().analyze(plan, &options).unwrap();
        let once = DecimalRoundingRule.analyze(coerced, &options).unwrap();
        let twice = DecimalRoundingRule.analyze(once.clone(), &options).unwrap();
        assert!(
            once.display_indent().to_string().contains(RESCALE),
            "{once}"
        );
        assert_eq!(
            once.display_indent().to_string(),
            twice.display_indent().to_string()
        );
    }
}
