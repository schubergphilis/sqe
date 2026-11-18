//! An integer literal is `integer` when it fits, as in Trino. DataFusion
//! types every integer literal as `bigint`, so `abs(-5)`, `CASE .. THEN 1`
//! and `CAST('42' AS integer) + 1` came back `bigint` where Trino says
//! `integer`, and `CREATE TABLE AS SELECT 1` wrote a `long` column.
//!
//! [`IntegerLiteralRule`] runs before type coercion and narrows every
//! `Int64` literal in `[-2147483647, 2147483647]` to `Int32`; coercion then
//! widens it again wherever it meets a `bigint`. Arithmetic between two
//! `integer` operands stays `integer`. DataFusion's `+ - *` wrap on
//! overflow, which a narrow literal would make reachable (`int_col + 1` was
//! `bigint`), so [`CheckedIntegerArithmeticRule`] runs after coercion and
//! sends `tinyint`, `smallint` and `integer` `+ - *` through arrow's checked
//! kernels: `2147483647 + 1` fails with `integer addition overflow`, as in
//! Trino. `bigint` arithmetic still wraps.
//!
//! Untouched:
//!
//! - `LIMIT` / `OFFSET`: DataFusion reads them only as `Int64` literals.
//! - Ordinals (`ORDER BY 1`, `GROUP BY 1`) and type parameters: resolved by
//!   the SQL planner before any analyzer rule runs.
//! - Extension nodes: their expressions are not ours to retype.
//! - A `VALUES` column narrows only when every row in it is such a literal
//!   (or NULL), so the column keeps one type.
//! - A plan holding `WITH RECURSIVE`: the work table takes its schema from
//!   the planned (`bigint`) anchor term, before any analyzer rule runs, so a
//!   narrowed anchor no longer matches it. Literals there stay `bigint`.
//!
//! `-2147483648` stays `bigint`: Trino reads the literal `2147483648` as
//! `bigint` before negating it.

use std::sync::Arc;

use datafusion::arrow::compute::kernels::numeric::{add, mul, sub};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{exec_datafusion_err, DFSchema, DataFusionError, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Union, Values, Volatility,
};
use datafusion::optimizer::analyzer::type_coercion::coerce_union_schema;
use datafusion::optimizer::AnalyzerRule;

use super::decimal_arithmetic::rewrite_plan_exprs;

/// Types integer literals that fit 32 bits as `integer`. Runs before
/// DataFusion's type coercion and SQE's decimal rules.
#[derive(Debug, Default)]
pub struct IntegerLiteralRule;

impl AnalyzerRule for IntegerLiteralRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut recursive = false;
        plan.apply_with_subqueries(|plan| {
            recursive = matches!(plan, LogicalPlan::RecursiveQuery(_));
            Ok(if recursive {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        if recursive {
            return Ok(plan);
        }
        let narrowed = plan.transform_up_with_subqueries(|plan| match plan {
            LogicalPlan::Limit(_) | LogicalPlan::Extension(_) => Ok(Transformed::no(plan)),
            LogicalPlan::Values(values) => narrow_values(values),
            LogicalPlan::Union(union) => retype_union(union),
            plan => rewrite_plan_exprs(plan, narrow_node),
        })?;
        if narrowed.transformed && has_lambda(&narrowed.data)? {
            refresh_lambda_variables(narrowed.data)
        } else {
            Ok(narrowed.data)
        }
    }

    fn name(&self) -> &str {
        "sqe_integer_literals"
    }
}

fn has_lambda(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            found = expr.exists(|e| Ok(matches!(e, Expr::Lambda(_))))?;
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
    })?;
    Ok(found)
}

/// A lambda variable carries the field it was resolved to when the SQL was
/// planned, so `x` in `array_filter(make_array(1, 2), x -> x > 1)` stays
/// `bigint` after the array became `integer`, and physical planning refuses
/// the mismatch. Bottom up, each node takes its inputs' new schema, its
/// lambda variables are resolved again against it, and its own schema is
/// recomputed. A lambda over a narrowed column of a subquery needs the
/// nodes in between recomputed too, so every node is visited.
fn refresh_lambda_variables(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|plan| {
        let inputs = merge_schema(&plan.inputs());
        let plan = plan
            .map_expressions(|expr| expr.resolve_lambda_variables(&inputs))?
            .data;
        Ok(Transformed::yes(plan.recompute_schema()?))
    })
    .data()
}

fn narrow_node(expr: Expr, _schema: &DFSchema) -> Result<Transformed<Expr>> {
    Ok(match expr {
        Expr::Literal(ScalarValue::Int64(Some(n)), meta) if fits(n) => {
            Transformed::yes(Expr::Literal(ScalarValue::Int32(Some(n as i32)), meta))
        }
        e => Transformed::no(e),
    })
}

fn fits(n: i64) -> bool {
    (-(i32::MAX as i64)..=i32::MAX as i64).contains(&n)
}

/// Narrows the `bigint` columns of a `VALUES` list whose every row is a
/// literal that fits (or a NULL of that column).
fn narrow_values(values: Values) -> Result<Transformed<LogicalPlan>> {
    let Values { schema, mut values } = values;
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut transformed = false;
    for (j, field) in fields.iter_mut().enumerate() {
        if field.data_type() != &DataType::Int64 {
            continue;
        }
        let narrowable = values.iter().all(|row| match &row[j] {
            Expr::Literal(ScalarValue::Int64(None), _) => true,
            Expr::Literal(ScalarValue::Int64(Some(n)), _) => fits(*n),
            _ => false,
        });
        if !narrowable {
            continue;
        }
        for row in values.iter_mut() {
            if let Expr::Literal(ScalarValue::Int64(v), meta) = &row[j] {
                row[j] = Expr::Literal(ScalarValue::Int32(v.map(|n| n as i32)), meta.clone());
            }
        }
        *field = field.clone().with_data_type(DataType::Int32);
        transformed = true;
    }
    if !transformed {
        return Ok(Transformed::no(LogicalPlan::Values(Values {
            schema,
            values,
        })));
    }
    let qualified = fields
        .into_iter()
        .zip(schema.iter().map(|(q, _)| q.cloned()))
        .map(|(f, q)| (q, Arc::new(f)))
        .collect::<Vec<_>>();
    let schema = DFSchema::new_with_metadata(qualified, schema.metadata().clone())?;
    Ok(Transformed::yes(LogicalPlan::Values(Values {
        schema: Arc::new(schema),
        values,
    })))
}

/// Gives a `UNION` the common type of its inputs once their literals are
/// narrowed. Type coercion starts from the union's own schema, which the
/// SQL planner typed from the `bigint` literals, so without this
/// `SELECT 1 UNION ALL SELECT 2` stayed `bigint`. Names and qualifiers stay
/// the union's.
fn retype_union(union: Union) -> Result<Transformed<LogicalPlan>> {
    let common = coerce_union_schema(&union.inputs)?;
    let same = union
        .schema
        .fields()
        .iter()
        .zip(common.fields())
        .all(|(f, c)| f.data_type() == c.data_type());
    if same {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    }
    let fields = union
        .schema
        .iter()
        .zip(common.fields())
        .map(|((q, f), c)| {
            let field = f.as_ref().clone().with_data_type(c.data_type().clone());
            (q.cloned(), Arc::new(field))
        })
        .collect::<Vec<_>>();
    let schema = DFSchema::new_with_metadata(fields, union.schema.metadata().clone())?;
    Ok(Transformed::yes(LogicalPlan::Union(Union {
        inputs: union.inputs,
        schema: Arc::new(schema),
    })))
}

/// Sends `+ - *` over two `tinyint`, `smallint` or `integer` operands
/// through [`CheckedIntArith`], which errors on overflow where DataFusion's
/// operators wrap. Runs after type coercion, where both operands have the
/// result's type.
#[derive(Debug, Default)]
pub struct CheckedIntegerArithmeticRule;

impl AnalyzerRule for CheckedIntegerArithmeticRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| rewrite_plan_exprs(plan, check_node))
            .data()
    }

    fn name(&self) -> &str {
        "sqe_checked_integer_arithmetic"
    }
}

fn check_node(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if !matches!(op, Operator::Plus | Operator::Minus | Operator::Multiply) {
        return Ok(Transformed::no(expr));
    }
    let ty = left.get_type(schema)?;
    if !matches!(ty, DataType::Int8 | DataType::Int16 | DataType::Int32)
        || right.get_type(schema)? != ty
    {
        return Ok(Transformed::no(expr));
    }
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        unreachable!("matched above");
    };
    Ok(Transformed::yes(CheckedIntArith::call(*left, op, *right)))
}

/// `__sqe_checked_add(a, b)` (and `_subtract`, `_multiply`): `a op b` in
/// the operands' integer type, an error on overflow.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CheckedIntArith {
    op: Operator,
    signature: Signature,
}

impl CheckedIntArith {
    fn call(left: Expr, op: Operator, right: Expr) -> Expr {
        let udf = Self {
            op,
            signature: Signature::any(2, Volatility::Immutable),
        };
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(udf)),
            vec![left, right],
        ))
    }

    fn overflow(&self, ty: &DataType, e: ArrowError) -> DataFusionError {
        let ArrowError::ArithmeticOverflow(detail) = e else {
            return e.into();
        };
        let ty = match ty {
            DataType::Int8 => "tinyint",
            DataType::Int16 => "smallint",
            _ => "integer",
        };
        let detail = detail
            .strip_prefix("Overflow happened on: ")
            .unwrap_or(&detail);
        exec_datafusion_err!("{ty} {} overflow: {detail}", self.word())
    }

    fn word(&self) -> &'static str {
        match self.op {
            Operator::Plus => "addition",
            Operator::Minus => "subtraction",
            _ => "multiplication",
        }
    }
}

impl ScalarUDFImpl for CheckedIntArith {
    fn name(&self) -> &str {
        match self.op {
            Operator::Plus => "__sqe_checked_add",
            Operator::Minus => "__sqe_checked_subtract",
            _ => "__sqe_checked_multiply",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        let ty = args.arg_fields[0].data_type().clone();
        Ok(Arc::new(Field::new(self.name(), ty, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (l, r) = (&arrays[0], &arrays[1]);
        let out = match self.op {
            Operator::Plus => add(l, r),
            Operator::Minus => sub(l, r),
            _ => mul(l, r),
        }
        .map_err(|e| self.overflow(l.data_type(), e))?;
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
        } else {
            ColumnarValue::Array(out)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::SessionContext;

    use crate::session_tuning::{engine_context, engine_session_config};

    fn ctx() -> SessionContext {
        engine_context(engine_session_config(), Arc::new(RuntimeEnv::default()))
    }

    /// The executed type of the first column, and its first value.
    async fn first(ctx: &SessionContext, sql: &str) -> (DataType, String) {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let col = batches[0].column(0);
        let v = datafusion::arrow::util::display::array_value_to_string(col, 0).unwrap();
        (col.data_type().clone(), v)
    }

    #[tokio::test]
    async fn literals_that_fit_are_integer() {
        let ctx = ctx();
        for (sql, ty, v) in [
            ("SELECT 7", DataType::Int32, "7"),
            ("SELECT -5", DataType::Int32, "-5"),
            ("SELECT abs(-5)", DataType::Int32, "5"),
            (
                "SELECT CASE WHEN true THEN 1 ELSE 2 END",
                DataType::Int32,
                "1",
            ),
            ("SELECT CAST('42' AS INTEGER) + 1", DataType::Int32, "43"),
            ("SELECT 2147483647", DataType::Int32, "2147483647"),
            ("SELECT 2147483648", DataType::Int64, "2147483648"),
            ("SELECT -2147483648", DataType::Int64, "-2147483648"),
            ("SELECT CAST(1 AS BIGINT)", DataType::Int64, "1"),
            ("SELECT 1 + CAST(1 AS BIGINT)", DataType::Int64, "2"),
            (
                "SELECT x FROM (VALUES (1), (2)) AS t(x)",
                DataType::Int32,
                "1",
            ),
            (
                "SELECT x FROM (VALUES (1), (NULL)) AS t(x)",
                DataType::Int32,
                "1",
            ),
            (
                "SELECT x FROM (VALUES (1), (5000000000)) AS t(x)",
                DataType::Int64,
                "1",
            ),
        ] {
            assert_eq!(first(&ctx, sql).await, (ty, v.to_string()), "{sql}");
        }
    }

    async fn error(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Ok(df) => df.collect().await.map(|_| ()),
            Err(e) => Err(e),
        }
        .expect_err(sql)
        .to_string()
    }

    #[tokio::test]
    async fn integer_arithmetic_overflows_as_in_trino() {
        let ctx = ctx();
        for (sql, msg) in [
            (
                "SELECT 2147483647 + 1",
                "integer addition overflow: 2147483647 + 1",
            ),
            ("SELECT -2147483647 - 2", "integer subtraction overflow"),
            ("SELECT 65536 * 65536", "integer multiplication overflow"),
            (
                "SELECT x + 1 FROM (VALUES (2147483647)) AS t(x)",
                "integer addition overflow",
            ),
            (
                "SELECT CAST(32767 AS SMALLINT) + CAST(1 AS SMALLINT)",
                "smallint addition overflow",
            ),
        ] {
            let err = error(&ctx, sql).await;
            assert!(err.contains(msg), "{sql}: {err}");
        }
        for (sql, ty, v) in [
            ("SELECT 2147483646 + 1", DataType::Int32, "2147483647"),
            (
                "SELECT x * 2 FROM (VALUES (3), (NULL)) AS t(x)",
                DataType::Int32,
                "6",
            ),
            (
                "SELECT 2147483647 + CAST(1 AS BIGINT)",
                DataType::Int64,
                "2147483648",
            ),
            (
                "SELECT min(x) FROM (SELECT 1 AS x UNION ALL SELECT 2) t",
                DataType::Int32,
                "1",
            ),
            (
                "SELECT min(x) FROM (SELECT 1 AS x UNION ALL SELECT 5000000000) t",
                DataType::Int64,
                "1",
            ),
        ] {
            assert_eq!(first(&ctx, sql).await, (ty, v.to_string()), "{sql}");
        }
    }

    /// A recursive CTE keeps DataFusion's `bigint` literals: its work table
    /// is typed before any analyzer rule runs.
    #[tokio::test]
    async fn recursive_queries_keep_bigint_literals() {
        let ctx = ctx();
        let sql = "WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM w WHERE n < 3) \
                   SELECT n FROM w ORDER BY n";
        assert_eq!(first(&ctx, sql).await, (DataType::Int64, "1".to_string()));
    }

    #[tokio::test]
    async fn lambdas_follow_their_narrowed_arguments() {
        let ctx = ctx();
        for (sql, v) in [
            (
                "SELECT cardinality(array_filter(make_array(1, 2, 3), x -> x > 1))",
                "2",
            ),
            (
                "SELECT array_transform(make_array(1, 2), x -> x + 1)",
                "[2, 3]",
            ),
            (
                "SELECT array_transform(a, x -> x * 2) FROM (SELECT make_array(1, 2) AS a) t",
                "[2, 4]",
            ),
        ] {
            assert_eq!(first(&ctx, sql).await.1, v, "{sql}");
        }
    }

    #[tokio::test]
    async fn limit_offset_and_ordinals_still_work() {
        let ctx = ctx();
        let (ty, v) = first(
            &ctx,
            "SELECT x FROM (VALUES (3), (1), (2)) AS t(x) ORDER BY 1 LIMIT 1 OFFSET 1",
        )
        .await;
        assert_eq!((ty, v.as_str()), (DataType::Int32, "2"));
    }
}
