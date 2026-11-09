//! Decimal arithmetic with Trino's result types. DataFusion takes them from
//! arrow: `a / b` has scale `s1 + 4`, `a * b` precision `p1 + p2 + 1`, an
//! integer operand is `decimal(20,0)` and `sum(decimal(p,s))` is
//! `decimal(p+10,s)`. Trino (like Spark) gives a division at least six
//! decimals, lets a result that needs more than 38 digits give up scale
//! (down to six) before integer digits, types an integer as
//! `decimal(10,0)` and a bigint as `decimal(19,0)`, and sums into
//! `decimal(38,s)`. The scale a chain of operations ends at decides its
//! digits: TPC-DS q58 is `10.912200` in Trino and `10.91216667` here
//! without these rules.
//!
//! - [`DecimalArithmeticRule`], an analyzer rule that runs before type
//!   coercion, replaces `a + b`, `a - b`, `a * b` and `a / b` over decimals
//!   (or a decimal and an integer) by [`DecimalArithUdf`] with Trino's
//!   type. Before coercion, so DataFusion coerces every parent (a
//!   comparison, a `CASE`, a `UNION`) against the new type.
//! - [`DecimalArithUdf`] (`__sqe_decimal_arith(a, b, '<op>', '<type>')`)
//!   computes the exact result (a division to one guard digit), in
//!   Decimal256 when 38 digits do not hold it, and rounds half away from
//!   zero to the type, as Trino does.
//! - [`TrinoSum`]: DataFusion's `sum` returning `decimal(38,s)`.
//!
//! `%` already has Trino's type in arrow and is left alone.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::kernels::numeric::{add, div, mul, sub};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION,
};
use datafusion::arrow::util::display::FormatOptions;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{exec_err, internal_err, plan_err, DFSchema, ScalarValue};
use datafusion::error::Result;
use datafusion::functions_aggregate::sum::Sum;
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion::logical_expr::utils::{merge_schema, AggregateOrderSensitivity};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, BinaryExpr, ColumnarValue, DmlStatement, Documentation, Expr,
    ExprSchemable, GroupsAccumulator, LogicalPlan, Operator, ReturnFieldArgs, ReversedUDAF,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, SetMonotonicity, Signature, StatisticsArgs,
    Volatility, WriteOp,
};
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::optimizer::AnalyzerRule;

/// Name of [`DecimalArithUdf`].
pub const DECIMAL_ARITH: &str = "__sqe_decimal_arith";

/// Replaces decimal `+ - * /` by [`DecimalArithUdf`] with Trino's result
/// type. Runs before DataFusion's type coercion.
#[derive(Debug, Default)]
pub struct DecimalArithmeticRule;

impl AnalyzerRule for DecimalArithmeticRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| rewrite_plan_exprs(plan, type_node))
            .data()
    }

    fn name(&self) -> &str {
        "sqe_decimal_arithmetic"
    }
}

/// Rewrite one node's expressions, bottom up with `node`, against the
/// schema its expressions read. Keeps each expression's name.
pub(super) fn rewrite_plan_exprs(
    plan: LogicalPlan,
    node: fn(Expr, &DFSchema) -> Result<Transformed<Expr>>,
) -> Result<Transformed<LogicalPlan>> {
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
        let typed = expr.transform_up(|e| node(e, &schema))?;
        Ok(typed.update_data(|e| name.restore(e)))
    })?;
    if out.transformed {
        out.map_data(|plan| plan.recompute_schema())
    } else {
        Ok(out)
    }
}

fn type_node(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if !matches!(
        op,
        Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide
    ) {
        return Ok(Transformed::no(expr));
    }
    let (Some(lt), Some(rt)) = (coerced_type(left, schema), coerced_type(right, schema)) else {
        return Ok(Transformed::no(expr));
    };
    let (Some(l), Some(r)) = (trino_decimal(left, &lt), trino_decimal(right, &rt)) else {
        return Ok(Transformed::no(expr));
    };
    // Two integers stay integer arithmetic.
    if !is_decimal(&lt) && !is_decimal(&rt) {
        return Ok(Transformed::no(expr));
    }
    let Some(target) = trino_result_type(*op, l, r) else {
        return Ok(Transformed::no(expr));
    };
    // A 38-digit result can overflow its precision, which arrow's kernels
    // do not check (they check the i128 range): Trino raises an error.
    let native = BinaryTypeCoercer::new(&lt, op, &rt).get_result_type().ok();
    let full = matches!(target, DataType::Decimal128(DECIMAL128_MAX_PRECISION, _));
    if native.as_ref() == Some(&target) && !full {
        return Ok(Transformed::no(expr));
    }
    Ok(Transformed::yes(DecimalArithUdf::call(
        left.as_ref().clone(),
        right.as_ref().clone(),
        *op,
        &target,
    )))
}

/// The type `expr` has once DataFusion coerces it: before coercion a
/// `CASE` or `coalesce` over an integer and a decimal reports its first
/// branch's type.
pub(super) fn coerced_type(expr: &Expr, schema: &DFSchema) -> Option<DataType> {
    let coerced = expr
        .clone()
        .rewrite(&mut TypeCoercionRewriter::new(schema))
        .ok()?
        .data;
    coerced.get_type(schema).ok()
}

fn is_decimal(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..)
    )
}

/// `(precision, scale)` of an operand as Trino types it: a decimal as is
/// (none wider than 38 digits), an integer as `decimal(n,0)` with `n` its
/// type's digits. An integer literal is Trino's `integer` when it fits one,
/// `decimal(10,0)`, though DataFusion plans every integer literal as Int64.
fn trino_decimal(expr: &Expr, t: &DataType) -> Option<(u8, i8)> {
    Some(match t {
        DataType::Decimal32(p, s) | DataType::Decimal64(p, s) | DataType::Decimal128(p, s) => {
            (*p, *s)
        }
        DataType::Int8 | DataType::UInt8 => (3, 0),
        DataType::Int16 | DataType::UInt16 => (5, 0),
        DataType::Int32 | DataType::UInt32 => (10, 0),
        DataType::Int64 if fits_integer(expr) => (10, 0),
        DataType::Int64 => (19, 0),
        DataType::UInt64 => (20, 0),
        _ => return None,
    })
}

/// An Int64 literal (or its negation) in Trino's `integer` range.
fn fits_integer(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => i32::try_from(*v).is_ok(),
        Expr::Negative(inner) => fits_integer(inner),
        _ => false,
    }
}

/// Trino's result type for `l op r` (`decimal(p,s)` operands).
///
/// - `+ -`: scale `max(s1,s2)`, precision `max(p1-s1,p2-s2) + scale + 1`;
///   past 38 digits the scale gives way to the integer digits.
/// - `*`: `p1 + p2`, `s1 + s2`; `/`: scale `max(6, s1 + p2 + 1)`, precision
///   `p1 - s1 + s2 + scale`. Past 38 digits both keep the integer digits
///   and cut the scale, but not below `min(scale, 6)`.
pub(crate) fn trino_result_type(op: Operator, l: (u8, i8), r: (u8, i8)) -> Option<DataType> {
    let (p1, s1) = (i32::from(l.0), i32::from(l.1));
    let (p2, s2) = (i32::from(r.0), i32::from(r.1));
    if s1 < 0 || s2 < 0 {
        return None;
    }
    let max = i32::from(DECIMAL128_MAX_PRECISION);
    let (p, s) = match op {
        Operator::Plus | Operator::Minus => {
            let scale = s1.max(s2);
            let integer = (p1 - s1).max(p2 - s2);
            if integer + scale < max {
                (integer + scale + 1, scale)
            } else {
                (max, scale.min(max - integer).max(0))
            }
        }
        Operator::Multiply => reduce_scale(p1 + p2, s1 + s2),
        Operator::Divide => {
            let scale = 6.max(s1 + p2 + 1);
            reduce_scale(p1 - s1 + s2 + scale, scale)
        }
        _ => return None,
    };
    Some(DataType::Decimal128(
        u8::try_from(p).ok()?,
        i8::try_from(s).ok()?,
    ))
}

fn reduce_scale(precision: i32, scale: i32) -> (i32, i32) {
    let max = i32::from(DECIMAL128_MAX_PRECISION);
    if precision <= max {
        return (precision, scale);
    }
    let integer = precision - scale;
    (max, (max - integer).max(scale.min(6)))
}

/// `__sqe_decimal_arith(a, b, '<op>', '<arrow type>')`: `a op b` rounded
/// half away from zero to the type named by the last argument.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecimalArithUdf {
    signature: Signature,
}

impl DecimalArithUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }

    fn call(left: Expr, right: Expr, op: Operator, target: &DataType) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(Self::new())),
            vec![
                left,
                right,
                Expr::Literal(ScalarValue::Utf8(Some(op.to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some(target.to_string())), None),
            ],
        ))
    }
}

impl Default for DecimalArithUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for DecimalArithUdf {
    fn name(&self) -> &str {
        DECIMAL_ARITH
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("{DECIMAL_ARITH} reports its type through return_field_from_args")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let target = match args.scalar_arguments.get(3) {
            Some(Some(ScalarValue::Utf8(Some(name)))) => name.parse::<DataType>()?,
            _ => return plan_err!("{DECIMAL_ARITH} takes the result type as a string literal"),
        };
        let nullable = args.arg_fields.iter().take(2).any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(DECIMAL_ARITH, target, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let op = match args.args.get(2) {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(op)))) => op.as_str(),
            _ => return exec_err!("{DECIMAL_ARITH} takes the operator as a string literal"),
        };
        let scalar = args
            .args
            .iter()
            .take(2)
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args[..2])?;
        let out = arith(op, &arrays[0], &arrays[1], args.return_field.data_type())?;
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
        } else {
            ColumnarValue::Array(out)
        })
    }
}

/// Which decimal width an intermediate result needs.
#[derive(Clone, Copy, PartialEq)]
enum Width {
    Narrow,
    Wide,
}

impl Width {
    fn of(digits: i32) -> Option<Self> {
        if digits <= i32::from(DECIMAL128_MAX_PRECISION) {
            Some(Width::Narrow)
        } else if digits <= i32::from(DECIMAL256_MAX_PRECISION) {
            Some(Width::Wide)
        } else {
            None
        }
    }

    fn decimal(self, precision: i32, scale: i32) -> DataType {
        let max = match self {
            Width::Narrow => DECIMAL128_MAX_PRECISION,
            Width::Wide => DECIMAL256_MAX_PRECISION,
        };
        let p = u8::try_from(precision).unwrap_or(max).min(max);
        let s = i8::try_from(scale).unwrap_or(i8::MAX);
        match self {
            Width::Narrow => DataType::Decimal128(p, s),
            Width::Wide => DataType::Decimal256(p, s),
        }
    }
}

fn exact_cast(array: &ArrayRef, to: &DataType) -> Result<ArrayRef> {
    if array.data_type() == to {
        return Ok(Arc::clone(array));
    }
    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    Ok(cast_with_options(array, to, &options)?)
}

/// `(precision, scale)` of an evaluated operand; integers as
/// `decimal(20,0)`, which holds any of them exactly.
fn operand(array: &ArrayRef) -> Result<(ArrayRef, i32, i32)> {
    match array.data_type() {
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => Ok((Arc::clone(array), i32::from(*p), i32::from(*s))),
        t if t.is_integer() => Ok((exact_cast(array, &DataType::Decimal128(20, 0))?, 20, 0)),
        t => exec_err!("{DECIMAL_ARITH} takes decimal or integer operands, got {t}"),
    }
}

/// `l op r`, exact (a quotient truncated one digit past the target scale),
/// then cast to `target`, which rounds half away from zero.
fn arith(op: &str, l: &ArrayRef, r: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    let (l, p1, s1) = operand(l)?;
    let (r, p2, s2) = operand(r)?;
    let Some(target_scale) = decimal_scale(target) else {
        return exec_err!("{DECIMAL_ARITH} returns a decimal, not {target}");
    };
    let overflow = || {
        exec_err!(
            "{DECIMAL_ARITH}: decimal({p1},{s1}) {op} decimal({p2},{s2}) needs more than 76 digits"
        )
    };
    let out = match op {
        "+" | "-" => {
            let scale = s1.max(s2);
            let digits = (p1 - s1).max(p2 - s2) + scale + 1;
            // An unrounded sum outside 38 digits overflows its type anyway.
            let width = if scale == target_scale {
                Width::Narrow
            } else {
                match Width::of(digits) {
                    Some(w) => w,
                    None => return overflow(),
                }
            };
            let to = width.decimal(digits, scale);
            let (l, r) = (exact_cast(&l, &to)?, exact_cast(&r, &to)?);
            if op == "+" {
                add(&l, &r)?
            } else {
                sub(&l, &r)?
            }
        }
        "*" => {
            // An unrounded product outside 38 digits overflows its type.
            let width = if s1 + s2 == target_scale {
                Width::Narrow
            } else {
                match Width::of(p1 + p2) {
                    Some(w) => w,
                    None => return overflow(),
                }
            };
            let (l, r) = (
                exact_cast(&l, &width.decimal(p1, s1))?,
                exact_cast(&r, &width.decimal(p2, s2))?,
            );
            mul(&l, &r)?
        }
        "/" => {
            // arrow divides to scale s1 + 4, truncating: widen the dividend
            // so that is at least one digit past the target scale.
            let quotient_scale = (target_scale + 1).max(s1 + 4);
            let dividend_scale = quotient_scale - 4;
            let dividend_precision = p1 + dividend_scale - s1;
            // arrow scales the dividend by 10^(4 + s2) before dividing.
            let Some(width) = Width::of(dividend_precision + 4 + s2) else {
                return overflow();
            };
            let (l, r) = (
                exact_cast(&l, &width.decimal(dividend_precision, dividend_scale))?,
                exact_cast(&r, &width.decimal(p2, s2))?,
            );
            div(&l, &r)?
        }
        _ => return exec_err!("{DECIMAL_ARITH}: unknown operator {op}"),
    };
    let out = exact_cast(&out, target)?;
    // A cast to the type the kernel already returned checks nothing; arrow's
    // kernels only check the i128 range, not the precision.
    if let DataType::Decimal128(p, _) = target {
        if out
            .as_primitive::<Decimal128Type>()
            .validate_decimal_precision(*p)
            .is_err()
        {
            return exec_err!("Decimal overflow");
        }
    }
    Ok(out)
}

fn decimal_scale(t: &DataType) -> Option<i32> {
    match t {
        DataType::Decimal32(_, s)
        | DataType::Decimal64(_, s)
        | DataType::Decimal128(_, s)
        | DataType::Decimal256(_, s) => Some(i32::from(*s)),
        _ => None,
    }
}

/// DataFusion's `sum`, with Trino's `decimal(38,s)` for a `decimal(p,s)`
/// input instead of `decimal(p+10,s)`. The accumulators take their type
/// from the return type, so state, partial and final aggregation follow.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TrinoSum {
    inner: Sum,
}

impl TrinoSum {
    pub fn new() -> Self {
        Self { inner: Sum::new() }
    }
}

impl Default for TrinoSum {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for TrinoSum {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(DataType::Decimal128(_, s)) => {
                Ok(DataType::Decimal128(DECIMAL128_MAX_PRECISION, *s))
            }
            _ => self.inner.return_type(arg_types),
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(args)
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
        self.inner.create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.create_sliding_accumulator(args)
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_expr()
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.set_monotonicity(data_type)
    }

    fn simplify_expr_op_literal(
        &self,
        agg_function: &AggregateFunction,
        arg: &Expr,
        op: Operator,
        lit: &Expr,
        arg_is_left: bool,
    ) -> Result<Option<Expr>> {
        self.inner
            .simplify_expr_op_literal(agg_function, arg, op, lit, arg_is_left)
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.inner.value_from_stats(statistics_args)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::logical_expr::Operator;
    use datafusion::prelude::SessionContext;

    use super::trino_result_type;
    use crate::session_tuning::{engine_context, engine_session_config};

    fn ctx() -> SessionContext {
        engine_context(engine_session_config(), Arc::new(RuntimeEnv::default()))
    }

    /// Type and value of a one-row, one-column result.
    async fn typed(ctx: &SessionContext, sql: &str) -> (DataType, String) {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let ty = batches[0].schema().field(0).data_type().clone();
        let out = pretty_format_batches(&batches).unwrap().to_string();
        let line = out.lines().nth(3).unwrap_or_default();
        (ty, line.trim_matches(|c| c == '|' || c == ' ').to_string())
    }

    /// Types and values from Trino 481 for the same expressions.
    #[tokio::test]
    async fn values_match_trino() {
        let ctx = ctx();
        let d = DataType::Decimal128;
        for (sql, ty, value) in [
            // TPC-DS q61's ratio.
            (
                "SELECT CAST(228296.12 AS DECIMAL(15,4)) / CAST(262862.45 AS DECIMAL(15,4)) * 100",
                d(38, 13),
                "86.8500312615971",
            ),
            (
                "SELECT CAST(2 AS DECIMAL(7,2)) / CAST(3 AS DECIMAL(7,2))",
                d(17, 10),
                "0.6666666667",
            ),
            (
                "SELECT CAST(7 AS DECIMAL(7,2)) / 3",
                d(18, 13),
                "2.3333333333333",
            ),
            (
                "SELECT CAST(-2 AS DECIMAL(3,2)) / 3",
                d(14, 13),
                "-0.6666666666667",
            ),
            (
                "SELECT CAST(1.25 AS DECIMAL(38,2)) + CAST(1.005 AS DECIMAL(38,10))",
                d(38, 2),
                "2.26",
            ),
            (
                "SELECT CAST(1.5 AS DECIMAL(15,4)) * CAST(2.25 AS DECIMAL(7,2))",
                d(22, 6),
                "3.375000",
            ),
            (
                "SELECT CAST(-0.0150000075 AS DECIMAL(35,20)) * 100",
                d(38, 13),
                "-1.5000007500000",
            ),
            (
                "SELECT CAST(2 AS DECIMAL(38,2)) / 3.0",
                d(38, 6),
                "0.666667",
            ),
        ] {
            assert_eq!(typed(&ctx, sql).await, (ty, value.to_string()), "{sql}");
        }
    }

    /// `sum(decimal(p,s))` is `decimal(38,s)` and a comparison coerces
    /// against the new type (TPC-DS q75's `< 0.9`).
    #[tokio::test]
    async fn sum_and_comparisons_take_the_new_types() {
        let ctx = ctx();
        let (ty, v) = typed(
            &ctx,
            "SELECT sum(x) FROM (VALUES (CAST(1.5 AS DECIMAL(7,2))), (2.25)) v(x)",
        )
        .await;
        assert_eq!((ty, v.as_str()), (DataType::Decimal128(38, 2), "3.75"));
        let (_, v) = typed(
            &ctx,
            "SELECT CAST(899 AS DECIMAL(17,2)) / CAST(1000 AS DECIMAL(17,2)) < 0.9",
        )
        .await;
        assert_eq!(v, "true");
        let (ty, v) = typed(
            &ctx,
            "SELECT CASE WHEN true THEN CAST(1 AS DECIMAL(7,2)) ELSE 2 END / CAST(3 AS DECIMAL(7,2))",
        )
        .await;
        // DataFusion's CASE type over decimal(7,2) and a bigint literal is
        // decimal(22,2) (Trino's is decimal(12,2)), so the precision is wider
        // than Trino's (22,10); the scale and the value match.
        assert_eq!(
            (ty, v.as_str()),
            (DataType::Decimal128(32, 10), "0.3333333333")
        );
    }

    /// A view body is analyzed alone and again inside the statement that
    /// reads it; the second pass changes nothing.
    #[tokio::test]
    async fn rule_is_idempotent() {
        let ctx = ctx();
        let state = ctx.state();
        for sql in [
            "SELECT CAST(1 AS DECIMAL(7,2)) * 2 + CAST(3 AS DECIMAL(15,4)) / 7 - 1",
            "SELECT sum(x) / count(x), sum(x) * 100 FROM (VALUES (CAST(1.5 AS DECIMAL(7,2)))) v(x)",
            "SELECT CAST(1 AS DECIMAL(38,2)) + CAST(2 AS DECIMAL(38,2))",
        ] {
            let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
            let analyze = |p| {
                state
                    .analyzer()
                    .execute_and_check(p, state.config_options(), |_, _| {})
                    .unwrap()
            };
            let once = analyze(plan);
            let twice = analyze(once.clone());
            assert_eq!(once, twice, "{sql}");
        }
    }

    #[tokio::test]
    async fn overflow_and_division_by_zero_are_errors() {
        let ctx = ctx();
        for sql in [
            "SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0))",
            "SELECT CAST(1 AS DECIMAL(7,2)) / CAST(0 AS DECIMAL(7,2))",
        ] {
            let out = ctx.sql(sql).await.unwrap().collect().await;
            assert!(out.is_err(), "{sql}");
        }
    }

    /// Every row was checked against Trino 481 with `typeof`.
    #[test]
    fn result_types_match_trino() {
        use Operator::{Divide as D, Minus as S, Multiply as M, Plus as A};
        type Decimal = (u8, i8);
        let cases: [(Operator, Decimal, Decimal, Decimal); 23] = [
            (A, (15, 4), (7, 2), (16, 4)),
            (A, (38, 2), (38, 10), (38, 2)),
            (A, (38, 2), (30, 10), (38, 2)),
            (A, (20, 2), (30, 10), (31, 10)),
            (A, (38, 6), (38, 10), (38, 6)),
            (A, (36, 2), (38, 10), (38, 4)),
            (A, (37, 2), (37, 2), (38, 2)),
            (A, (38, 2), (38, 2), (38, 2)),
            (A, (38, 20), (38, 25), (38, 20)),
            (A, (38, 2), (10, 0), (38, 2)),
            (S, (15, 4), (7, 2), (16, 4)),
            (M, (15, 4), (7, 2), (22, 6)),
            (M, (30, 10), (20, 10), (38, 8)),
            (M, (7, 2), (10, 0), (17, 2)),
            (M, (38, 0), (38, 0), (38, 0)),
            (M, (35, 20), (10, 0), (38, 13)),
            (M, (35, 20), (19, 0), (38, 6)),
            (M, (35, 20), (3, 0), (38, 20)),
            (M, (38, 20), (10, 0), (38, 10)),
            (D, (15, 4), (7, 2), (25, 12)),
            (D, (7, 2), (15, 4), (27, 18)),
            (D, (38, 2), (2, 1), (38, 6)),
            (D, (35, 20), (19, 0), (38, 23)),
        ];
        for (op, l, r, (p, s)) in cases {
            assert_eq!(
                trino_result_type(op, l, r),
                Some(DataType::Decimal128(p, s)),
                "decimal{l:?} {op} decimal{r:?}"
            );
        }
        // More Trino rows: (7,2)/3 -> (18,13), (7,2)/(7,2) -> (17,10),
        // (15,4)/(15,4) -> (35,20), (38,2)/(38,2) -> (38,6).
        for (l, r, p, s) in [
            ((7, 2), (10, 0), 18, 13),
            ((7, 2), (7, 2), 17, 10),
            ((15, 4), (15, 4), 35, 20),
            ((38, 2), (38, 2), 38, 6),
        ] {
            assert_eq!(trino_result_type(D, l, r), Some(DataType::Decimal128(p, s)));
        }
    }
}
