//! A floating-point value meets a decimal in floating point, as in Trino.
//! DataFusion prefers the decimal: it casts a double to `decimal(30,15)`,
//! so `double_col > 9.0` (a decimal literal under `parse_float_as_decimal`)
//! fails on a NaN row (`Overflowing on NaN`) or on a value past 1e15, and
//! the cast column keeps the filter from pruning files and row groups.
//! `COALESCE`, `CASE` and `UNION` resolve their type the same way and fail
//! on the same rows.
//!
//! [`FloatOverDecimalRule`] runs before type coercion and casts the decimal
//! operands to the float type in:
//!
//! - comparisons: `=, <>, <, <=, >, >=, IS [NOT] DISTINCT FROM`, `BETWEEN`,
//!   `IN (..)`, `IN (SELECT ..)` and the `CASE x WHEN ..` operand,
//! - the results of `CASE` and of `coalesce`, `nvl`, `ifnull`, `nullif`,
//!   `nvl2`, `greatest`, `least` and `make_array` (`[a, b]`),
//! - the columns of a `UNION` and of a `VALUES` list.
//!
//! Arithmetic already takes the float type in DataFusion.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult};
use datafusion::common::{Column, DFSchema, DFSchemaRef};
use datafusion::error::Result;
use datafusion::logical_expr::expr::{Case, InList, InSubquery, ScalarFunction};
use datafusion::logical_expr::expr_schema::cast_subquery;
use datafusion::logical_expr::type_coercion::binary::type_union_resolution;
use datafusion::logical_expr::{
    cast, Between, BinaryExpr, Expr, LogicalPlan, Operator, Projection, Union, Values,
};
use datafusion::optimizer::AnalyzerRule;

use super::decimal_arithmetic::{coerced_type, rewrite_plan_exprs};

/// Casts the decimal operands that meet a float to that float type. Runs
/// before DataFusion's type coercion.
#[derive(Debug, Default)]
pub struct FloatOverDecimalRule;

impl AnalyzerRule for FloatOverDecimalRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            rewrite_plan_exprs(plan, float_node)?
                .transform_data(union_as_float)?
                .transform_data(values_as_float)
        })
        .data()
    }

    fn name(&self) -> &str {
        "sqe_float_over_decimal"
    }
}

/// Functions whose arguments resolve to one common type.
const UNIFYING_FUNCTIONS: &[&str] = &[
    "coalesce",
    "nvl",
    "ifnull",
    "nullif",
    "greatest",
    "least",
    "make_array",
];

fn float_node(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    Ok(match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
            as_float(vec![*left, *right], schema).update_data(|operands| {
                let [left, right] = <[Expr; 2]>::try_from(operands).expect("two operands");
                Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
            })
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => as_float(vec![*expr, *low, *high], schema).update_data(|operands| {
            let [expr, low, high] = <[Expr; 3]>::try_from(operands).expect("three operands");
            Expr::Between(Between::new(
                Box::new(expr),
                negated,
                Box::new(low),
                Box::new(high),
            ))
        }),
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let mut operands = vec![*expr];
            operands.extend(list);
            as_float(operands, schema).update_data(|mut operands| {
                let expr = operands.remove(0);
                Expr::InList(InList::new(Box::new(expr), operands, negated))
            })
        }
        Expr::InSubquery(InSubquery {
            expr,
            subquery,
            negated,
        }) => {
            let compared = coerced_type(&expr, schema)
                .map(|t| (t, subquery.subquery.schema().field(0).data_type().clone()));
            match compared {
                Some((left, right))
                    if is_decimal(&left) != is_decimal(&right)
                        && widest_float([&left, &right].into_iter()).is_some() =>
                {
                    let float = widest_float([&left, &right].into_iter()).expect("a float");
                    let (expr, subquery) = if is_decimal(&left) {
                        (cast(*expr, float), subquery)
                    } else {
                        (*expr, cast_subquery(subquery, &float)?)
                    };
                    Transformed::yes(Expr::InSubquery(InSubquery::new(
                        Box::new(expr),
                        subquery,
                        negated,
                    )))
                }
                _ => Transformed::no(Expr::InSubquery(InSubquery::new(expr, subquery, negated))),
            }
        }
        Expr::Case(case) => case_as_float(case, schema),
        Expr::ScalarFunction(ScalarFunction { func, args })
            if UNIFYING_FUNCTIONS.contains(&func.name()) =>
        {
            as_float(args, schema)
                .update_data(|args| Expr::ScalarFunction(ScalarFunction::new_udf(func, args)))
        }
        // `nvl2(x, a, b)`: `x` is only tested for NULL.
        Expr::ScalarFunction(ScalarFunction { func, mut args })
            if func.name() == "nvl2" && !args.is_empty() =>
        {
            let tested = args.remove(0);
            as_float(args, schema).update_data(|mut args| {
                args.insert(0, tested);
                Expr::ScalarFunction(ScalarFunction::new_udf(func, args))
            })
        }
        expr => Transformed::no(expr),
    })
}

/// `CASE [x] WHEN w THEN t .. ELSE e END`: `x` and the `w`s are compared,
/// the `t`s and `e` are the result.
fn case_as_float(case: Case, schema: &DFSchema) -> Transformed<Expr> {
    let Case {
        expr,
        when_then_expr,
        else_expr,
    } = case;
    let (whens, thens): (Vec<Expr>, Vec<Expr>) =
        when_then_expr.into_iter().map(|(w, t)| (*w, *t)).unzip();
    let n = whens.len();

    let mut results = thens;
    results.extend(else_expr.map(|e| *e));
    let results = as_float(results, schema);
    let mut transformed = results.transformed;
    let mut results = results.data;
    let else_expr = (results.len() > n).then(|| Box::new(results.pop().expect("else")));

    let (expr, whens) = match expr {
        Some(x) => {
            let mut compared = vec![*x];
            compared.extend(whens);
            let compared = as_float(compared, schema);
            transformed |= compared.transformed;
            let mut compared = compared.data;
            let x = compared.remove(0);
            (Some(Box::new(x)), compared)
        }
        None => (None, whens),
    };

    let case = Expr::Case(Case {
        expr,
        when_then_expr: whens
            .into_iter()
            .zip(results)
            .map(|(w, t)| (Box::new(w), Box::new(t)))
            .collect(),
        else_expr,
    });
    if transformed {
        Transformed::yes(case)
    } else {
        Transformed::no(case)
    }
}

/// A `UNION` whose column holds a float in one input and a decimal in
/// another reads the decimal inputs through a projection that casts them.
fn union_as_float(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Union(union) = plan else {
        return Ok(Transformed::no(plan));
    };
    let width = union.schema.fields().len();
    let targets: Vec<Option<DataType>> = (0..width)
        .map(|i| {
            let types: Vec<&DataType> = union
                .inputs
                .iter()
                .filter_map(|input| input.schema().fields().get(i).map(|f| f.data_type()))
                .collect();
            widest_float(types.iter().copied()).filter(|_| types.iter().any(|t| is_decimal(t)))
        })
        .collect();
    if targets.iter().all(Option::is_none) {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    }
    let inputs = union
        .inputs
        .into_iter()
        .map(|input| {
            let schema = input.schema();
            let needs_cast = |i: usize| {
                targets[i].is_some()
                    && schema
                        .fields()
                        .get(i)
                        .is_some_and(|f| is_decimal(f.data_type()))
            };
            if !(0..schema.fields().len()).any(needs_cast) {
                return Ok(input);
            }
            let exprs = schema
                .iter()
                .enumerate()
                .map(|(i, (qualifier, field))| {
                    let column = Expr::Column(Column::from((qualifier, field)));
                    match &targets[i] {
                        Some(float) if needs_cast(i) => {
                            cast(column, float.clone()).alias(field.name())
                        }
                        _ => column,
                    }
                })
                .collect();
            Ok(Arc::new(LogicalPlan::Projection(Projection::try_new(
                exprs,
                Arc::clone(&input),
            )?)))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Transformed::yes(LogicalPlan::Union(
        Union::try_new_with_loose_types(inputs)?,
    )))
}

/// A `VALUES` column whose rows mix a float and a decimal. The SQL planner
/// fixes the column type and casts every row to it before any analyzer
/// rule runs, so this undoes that: a row's cast to the column type is
/// stripped, the planner's inference is replayed on the rows as written,
/// and only when it lands on the column's type again (the type was
/// inferred, not taken from an `INSERT` target) is the column retyped to
/// the float.
///
/// The planner's cast and a user's `CAST` build the same expression, so a
/// row the user cast to exactly the inferred decimal (`CAST(d AS
/// DECIMAL(30,15))` next to `1.5` for a double `d`) is read as the double
/// it was cast from. A cast to any other decimal type is kept.
fn values_as_float(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Values(Values { schema, mut values }) = plan else {
        return Ok(Transformed::no(plan));
    };
    let empty = DFSchema::empty();
    let mut fields: Vec<_> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut transformed = false;
    for (j, field) in fields.iter_mut().enumerate() {
        let column = field.data_type().clone();
        if !is_decimal(&column) {
            continue;
        }
        let written: Vec<&Expr> = values
            .iter()
            .map(|row| as_written(&row[j], &column))
            .collect();
        let Some(types) = written
            .iter()
            .map(|e| coerced_type(e, &empty))
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        let Some(float) = widest_float(types.iter()) else {
            continue;
        };
        // `LogicalPlanBuilder::infer_data`: a pairwise fold that skips NULL.
        let inferred = types
            .iter()
            .filter(|t| **t != DataType::Null)
            .try_fold(None::<DataType>, |common, t| match common {
                None => Some(Some(t.clone())),
                Some(c) => type_union_resolution(&[c, t.clone()]).map(Some),
            })
            .flatten();
        if inferred.as_ref() != Some(&column) {
            continue;
        }
        for (row, t) in values.iter_mut().zip(&types) {
            let e = as_written(&row[j], &column).clone();
            row[j] = if *t == float {
                e
            } else {
                cast(e, float.clone())
            };
        }
        *field = field.clone().with_data_type(float);
        transformed = true;
    }
    if !transformed {
        return Ok(Transformed::no(LogicalPlan::Values(Values {
            schema,
            values,
        })));
    }
    let qualified = schema
        .iter()
        .zip(fields)
        .map(|((q, _), f)| (q.cloned(), Arc::new(f)))
        .collect();
    let schema = DFSchemaRef::new(DFSchema::new_with_metadata(
        qualified,
        schema.metadata().clone(),
    )?);
    Ok(Transformed::yes(LogicalPlan::Values(Values {
        schema,
        values,
    })))
}

/// A `VALUES` row as written: without the planner's cast to the column
/// type. A typed NULL literal the planner put in place of `NULL` is kept;
/// its decimal type does not count against the float.
fn as_written<'a>(e: &'a Expr, column: &DataType) -> &'a Expr {
    match e {
        Expr::Cast(c) if c.field.data_type() == column => &c.expr,
        e => e,
    }
}

fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

/// The operands, with every decimal cast to the widest float among them
/// when there is one. Unchanged when an operand's type is unknown.
fn as_float(operands: Vec<Expr>, schema: &DFSchema) -> Transformed<Vec<Expr>> {
    let types: Option<Vec<DataType>> = operands.iter().map(|e| coerced_type(e, schema)).collect();
    let Some(types) = types else {
        return Transformed::no(operands);
    };
    let Some(float) = widest_float(types.iter()) else {
        return Transformed::no(operands);
    };
    if !types.iter().any(is_decimal) {
        return Transformed::no(operands);
    }
    let operands = operands
        .into_iter()
        .zip(&types)
        .map(|(e, t)| {
            if is_decimal(t) {
                cast(e, float.clone())
            } else {
                e
            }
        })
        .collect();
    Transformed::yes(operands)
}

fn widest_float<'a>(types: impl Iterator<Item = &'a DataType>) -> Option<DataType> {
    types.fold(None, |widest, t| match (widest, t) {
        (_, DataType::Float64) | (Some(DataType::Float64), _) => Some(DataType::Float64),
        (_, DataType::Float32) | (Some(DataType::Float32), _) => Some(DataType::Float32),
        (_, DataType::Float16) | (Some(DataType::Float16), _) => Some(DataType::Float16),
        (widest, _) => widest,
    })
}

fn is_decimal(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::SessionContext;

    use crate::session_tuning::{engine_context, engine_session_config};

    fn ctx() -> SessionContext {
        engine_context(engine_session_config(), Arc::new(RuntimeEnv::default()))
    }

    /// The first column of every row, comma-separated, and the type of the
    /// first column as executed (`df.schema()` is the type before coercion).
    async fn rows(ctx: &SessionContext, sql: &str) -> (String, String) {
        let df = ctx.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let batches = df.collect().await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let ty = batches
            .first()
            .map(|b| b.schema().field(0).data_type().to_string())
            .unwrap_or_default();
        let out = pretty_format_batches(&batches).unwrap().to_string();
        let got: Vec<String> = out
            .lines()
            .skip(3)
            .filter(|l| l.starts_with('|'))
            .map(|l| l.trim_matches(|c| c == '|' || c == ' ').to_string())
            .collect();
        (got.join(","), ty)
    }

    async fn nan_table() -> SessionContext {
        let ctx = ctx();
        ctx.sql(
            "CREATE TABLE t AS SELECT * FROM (VALUES \
             (1, CAST(1.5 AS DOUBLE)), (2, CAST('NaN' AS DOUBLE)), (3, CAST(1e20 AS DOUBLE)), \
             (4, CAST(-0.5 AS DOUBLE))) AS v(id, d)",
        )
        .await
        .unwrap();
        ctx
    }

    /// Answers as in Trino: a NaN or a double past `decimal(30,15)` compares
    /// with a decimal literal instead of failing the cast.
    #[tokio::test]
    async fn doubles_compare_with_decimals_in_floating_point() {
        let ctx = nan_table().await;
        for (sql, want) in [
            // NaN sorts above every number, as in Trino.
            ("SELECT id FROM t WHERE d > 9.0 ORDER BY id", "2,3"),
            ("SELECT id FROM t WHERE d = 1.5 ORDER BY id", "1"),
            ("SELECT id FROM t WHERE 1.5 <> d ORDER BY id", "2,3,4"),
            (
                "SELECT id FROM t WHERE d BETWEEN -1.0 AND 2.0 ORDER BY id",
                "1,4",
            ),
            (
                "SELECT id FROM t WHERE d NOT BETWEEN -1.0 AND 2.0 ORDER BY id",
                "2,3",
            ),
            ("SELECT id FROM t WHERE d IN (1.5, -0.5) ORDER BY id", "1,4"),
            (
                "SELECT id FROM t WHERE d IS DISTINCT FROM 1.5 ORDER BY id",
                "2,3,4",
            ),
            (
                "SELECT id FROM t WHERE CASE d WHEN 1.5 THEN true END ORDER BY id",
                "1",
            ),
            ("SELECT id FROM t WHERE d IN (SELECT 1.5) ORDER BY id", "1"),
            (
                "SELECT id FROM t WHERE 1.5 NOT IN (SELECT d FROM t) ORDER BY id",
                "",
            ),
            // A REAL compares as REAL.
            (
                "SELECT id FROM t WHERE CAST(d AS REAL) >= 1.5 ORDER BY id",
                "1,2,3",
            ),
        ] {
            assert_eq!(rows(&ctx, sql).await.0, want, "{sql}");
        }
    }

    /// `CASE`, `COALESCE`-like functions and `UNION` take the float type,
    /// as in Trino, so a NaN passes through.
    #[tokio::test]
    async fn doubles_meet_decimals_as_doubles() {
        let ctx = nan_table().await;
        for (sql, want) in [
            ("SELECT COALESCE(d, 0.5) FROM t WHERE id = 2", "NaN"),
            ("SELECT nvl(d, 0.5) FROM t WHERE id = 2", "NaN"),
            ("SELECT nullif(d, 0.5) FROM t WHERE id = 2", "NaN"),
            ("SELECT nvl2(id, d, 0.5) FROM t WHERE id = 2", "NaN"),
            ("SELECT greatest(d, 0.5) FROM t WHERE id = 2", "NaN"),
            ("SELECT least(d, 0.5) FROM t WHERE id = 3", "0.5"),
            (
                "SELECT CASE WHEN id = 2 THEN d ELSE 0.5 END FROM t WHERE id = 2",
                "NaN",
            ),
            (
                "SELECT CASE WHEN id = 2 THEN 0.5 ELSE d END FROM t WHERE id = 3",
                "1e20",
            ),
            (
                "SELECT d FROM t WHERE id = 2 UNION ALL SELECT 0.5 ORDER BY 1",
                "0.5,NaN",
            ),
            (
                "SELECT 0.5 UNION ALL SELECT d FROM t WHERE id = 2 ORDER BY 1",
                "0.5,NaN",
            ),
        ] {
            let (got, ty) = rows(&ctx, sql).await;
            assert_eq!((got.as_str(), ty.as_str()), (want, "Float64"), "{sql}");
        }
        for (sql, want) in [
            (
                "SELECT c FROM (VALUES (CAST('NaN' AS DOUBLE)), (1.5)) v(c) ORDER BY 1",
                "1.5,NaN",
            ),
            (
                "SELECT c FROM (VALUES (1.5), (NULL), (CAST('NaN' AS DOUBLE))) v(c) \
                 WHERE c IS NOT NULL ORDER BY 1",
                "1.5,NaN",
            ),
            ("VALUES (1.5), (CAST('NaN' AS DOUBLE))", "1.5,NaN"),
            (
                "SELECT d FROM t WHERE id = 2 UNION ALL VALUES (0.5) ORDER BY 1",
                "0.5,NaN",
            ),
        ] {
            let (got, ty) = rows(&ctx, sql).await;
            assert_eq!((got.as_str(), ty.as_str()), (want, "Float64"), "{sql}");
        }
        let (got, ty) = rows(&ctx, "SELECT [d, 0.5] FROM t WHERE id = 2").await;
        assert_eq!(got, "[NaN, 0.5]");
        assert!(ty.contains("Float64"), "{ty}");
    }

    /// A `VALUES` column keeps the type the rows or an `INSERT` target give
    /// it when no float meets a decimal by inference.
    #[tokio::test]
    async fn values_keep_decimals_that_were_not_inferred() {
        let ctx = ctx();
        for (sql, want, ty) in [
            ("VALUES (1.5), (2.25)", "1.50,2.25", "Decimal128(3, 2)"),
            (
                "SELECT c FROM (VALUES (CAST(CAST(1.5 AS DOUBLE) AS DECIMAL(38,10))), (2.5)) v(c)",
                "1.5000000000,2.5000000000",
                "Decimal128(38, 10)",
            ),
        ] {
            let (got, got_ty) = rows(&ctx, sql).await;
            assert_eq!((got.as_str(), got_ty.as_str()), (want, ty), "{sql}");
        }
        ctx.sql("CREATE TABLE x (c DECIMAL(10,2))").await.unwrap();
        ctx.sql("INSERT INTO x VALUES (CAST(1.5 AS DOUBLE)), (2.5)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let (got, ty) = rows(&ctx, "SELECT c FROM x ORDER BY c").await;
        assert_eq!(
            (got.as_str(), ty.as_str()),
            ("1.50,2.50", "Decimal128(10, 2)")
        );
    }

    /// The decimal side is cast, so the column stays bare and the filter
    /// stays one a scan can prune on.
    #[tokio::test]
    async fn the_float_column_stays_uncast() {
        let ctx = ctx();
        ctx.sql("CREATE TABLE t (d DOUBLE, x DECIMAL(10,2))")
            .await
            .unwrap();
        let plan = |sql: &'static str| {
            let ctx = ctx.clone();
            async move {
                let df = ctx.sql(sql).await.unwrap();
                format!("{}", df.into_optimized_plan().unwrap().display_indent())
            }
        };
        let p = plan("SELECT * FROM t WHERE d > 9.0").await;
        assert!(p.contains("t.d > Float64(9)"), "{p}");
        // A decimal column against a double is cast to double too.
        let p = plan("SELECT * FROM t WHERE x < d").await;
        assert!(p.contains("CAST(t.x AS Float64) < t.d"), "{p}");
        // Two decimals and a decimal against an integer are untouched.
        let p = plan("SELECT * FROM t WHERE x > 9.0").await;
        assert!(!p.contains("Float64"), "{p}");
        let p = plan("SELECT * FROM t WHERE x > 9").await;
        assert!(!p.contains("Float64"), "{p}");
        let p = plan("SELECT COALESCE(x, 9.0) FROM t").await;
        assert!(!p.contains("Float64"), "{p}");
    }
}
