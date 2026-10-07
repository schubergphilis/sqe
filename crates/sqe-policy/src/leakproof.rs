//! Which caller expressions are safe to evaluate on rows a row filter hides.
//!
//! The optimizer merges a caller's predicate with the policy row filter into
//! one conjunction, may reorder it, and AND evaluates its right side over the
//! whole batch unless few rows pass. A predicate that can fail (division by
//! zero, an overflowing or unparsable cast) then runs on hidden rows, and the
//! error reveals their values. A predicate that cannot fail only answers
//! true/false/NULL, which the row filter discards, so it may run anywhere.
//! This is PostgreSQL's "leakproof" rule for security-barrier quals.
//!
//! The classification is an allowlist: column references, literals, boolean
//! logic, comparisons, `IN`, `BETWEEN`, `LIKE` with a constant pattern, and
//! `CASE`/`CAST` built from these, where every implicit coercion of a value
//! read from a row is a lossless widening. Everything else (arithmetic,
//! functions, subqueries, a cast that can fail) counts as fallible. A
//! sub-expression that reads no row is data-independent and never counts.

use arrow_schema::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::DFSchema;
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator};

/// True when some expression in `plan` that the optimizer can move below a
/// row filter is fallible. Covers filters, join conditions, predicates
/// already pushed into a scan, and the projection, group-by and window
/// partition expressions a filter can be substituted through. Aggregate
/// arguments and sort keys only ever run above the filter, so they are
/// skipped.
pub(crate) fn plan_has_fallible_predicate(plan: &LogicalPlan) -> bool {
    let mut found = false;
    // The closure never fails; a traversal error is treated as fallible.
    let walked = plan.apply_with_subqueries(|node| {
        found = node_is_fallible(node);
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found || walked.is_err()
}

fn node_is_fallible(node: &LogicalPlan) -> bool {
    match node {
        LogicalPlan::Filter(f) => is_fallible(&f.predicate, f.input.schema()),
        LogicalPlan::Join(j) => {
            let Ok(both) = j.left.schema().join(j.right.schema()) else {
                return true;
            };
            j.on.iter().any(|(l, r)| {
                is_fallible(l, j.left.schema())
                    || is_fallible(r, j.right.schema())
                    || !coerces_losslessly(&[l, r], &both)
            }) || j.filter.as_ref().is_some_and(|f| is_fallible(f, &both))
        }
        LogicalPlan::TableScan(scan) => {
            !scan.filters.is_empty() && {
                let Ok(schema) = DFSchema::try_from_qualified_schema(
                    scan.table_name.clone(),
                    &scan.source.schema(),
                ) else {
                    return true;
                };
                scan.filters.iter().any(|f| is_fallible(f, &schema))
            }
        }
        LogicalPlan::Projection(p) => p.expr.iter().any(|e| is_fallible(e, p.input.schema())),
        LogicalPlan::Aggregate(a) => a.group_expr.iter().any(|e| is_fallible(e, a.input.schema())),
        LogicalPlan::Window(w) => w.window_expr.iter().any(|e| {
            match unalias(e) {
                Expr::WindowFunction(wf) => {
                    wf.params.partition_by.iter().any(|p| is_fallible(p, w.input.schema()))
                }
                other => is_fallible(other, w.input.schema()),
            }
        }),
        _ => false,
    }
}

fn unalias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => unalias(&a.expr),
        _ => e,
    }
}

/// True when evaluating `expr` against rows of `schema` can raise an error
/// that depends on a row's values.
pub(crate) fn is_fallible(expr: &Expr, schema: &DFSchema) -> bool {
    if !reads_rows(expr) {
        return false;
    }
    let f = |e: &Expr| is_fallible(e, schema);
    match expr {
        Expr::Column(_) | Expr::OuterReferenceColumn(..) => false,
        Expr::Alias(a) => f(&a.expr),
        Expr::Not(e)
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsUnknown(e)
        | Expr::IsNotTrue(e)
        | Expr::IsNotFalse(e)
        | Expr::IsNotUnknown(e) => f(e),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And | Operator::Or => f(left) || f(right),
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom => {
                f(left) || f(right) || !coerces_losslessly(&[left, right], schema)
            }
            _ => true,
        },
        Expr::InList(i) => {
            f(&i.expr)
                || i.list.iter().any(f)
                || !coerces_losslessly(
                    &std::iter::once(i.expr.as_ref()).chain(&i.list).collect::<Vec<_>>(),
                    schema,
                )
        }
        Expr::Between(b) => {
            f(&b.expr)
                || f(&b.low)
                || f(&b.high)
                || !coerces_losslessly(&[&b.expr, &b.low, &b.high], schema)
        }
        // A constant pattern can only fail independently of the data.
        Expr::Like(l) => {
            f(&l.expr)
                || reads_rows(&l.pattern)
                || !matches!(type_of(&l.expr, schema), Some(t) if is_string(&t))
        }
        Expr::Case(c) => {
            let whens = c.when_then_expr.iter().map(|(w, _)| w.as_ref());
            let values: Vec<&Expr> = c
                .when_then_expr
                .iter()
                .map(|(_, t)| t.as_ref())
                .chain(c.else_expr.as_deref())
                .collect();
            let operand_fallible = match &c.expr {
                Some(base) => {
                    let operands: Vec<&Expr> = std::iter::once(base.as_ref()).chain(whens.clone()).collect();
                    operands.iter().any(|e| f(e)) || !coerces_losslessly(&operands, schema)
                }
                None => whens.clone().any(f),
            };
            operand_fallible || values.iter().any(|e| f(e)) || !coerces_losslessly(&values, schema)
        }
        Expr::TryCast(c) => f(&c.expr),
        Expr::Cast(c) => {
            f(&c.expr)
                || !matches!(type_of(&c.expr, schema), Some(from) if widens(&from, c.field.data_type()))
        }
        Expr::GroupingSet(g) => g.distinct_expr().into_iter().any(f),
        _ => true,
    }
}

/// Whether `expr` reads any value of a row (directly or through a subquery).
fn reads_rows(expr: &Expr) -> bool {
    expr.exists(|e| {
        Ok(matches!(
            e,
            Expr::Column(_)
                | Expr::OuterReferenceColumn(..)
                | Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
        ))
    })
    .unwrap_or(true)
}

fn type_of(expr: &Expr, schema: &DFSchema) -> Option<DataType> {
    expr.get_type(schema).ok()
}

/// True when type coercion brings `exprs` to a common type without a cast
/// that can fail on a value read from a row. A constant may be cast
/// arbitrarily: its failure does not depend on the data.
fn coerces_losslessly(exprs: &[&Expr], schema: &DFSchema) -> bool {
    let Some(types) = exprs.iter().map(|e| type_of(e, schema)).collect::<Option<Vec<_>>>() else {
        return false;
    };
    let Some(common) = types
        .iter()
        .skip(1)
        .try_fold(types[0].clone(), |acc, t| comparison_coercion(&acc, t))
    else {
        return false;
    };
    exprs
        .iter()
        .zip(&types)
        .all(|(e, t)| !reads_rows(e) || widens(t, &common))
}

fn is_string(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

/// Integer digits a value of an integer type can need.
fn integer_digits(t: &DataType) -> Option<i16> {
    Some(match t {
        DataType::Int8 | DataType::UInt8 => 3,
        DataType::Int16 | DataType::UInt16 => 5,
        DataType::Int32 | DataType::UInt32 => 10,
        DataType::Int64 => 19,
        DataType::UInt64 => 20,
        _ => return None,
    })
}

fn signed_width(t: &DataType) -> Option<u8> {
    Some(match t {
        DataType::Int8 => 8,
        DataType::Int16 => 16,
        DataType::Int32 => 32,
        DataType::Int64 => 64,
        _ => return None,
    })
}

fn unsigned_width(t: &DataType) -> Option<u8> {
    Some(match t {
        DataType::UInt8 => 8,
        DataType::UInt16 => 16,
        DataType::UInt32 => 32,
        DataType::UInt64 => 64,
        _ => return None,
    })
}

/// True when every value of `from` casts to `to` without error. Lossy but
/// infallible conversions (an integer to a float) count.
fn widens(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    if from == to || (is_string(from) && is_string(to)) {
        return true;
    }
    if let (Some(a), Some(b)) = (signed_width(from), signed_width(to)) {
        return a <= b;
    }
    if let (Some(a), Some(b)) = (unsigned_width(from), unsigned_width(to)) {
        return a <= b;
    }
    if let (Some(a), Some(b)) = (unsigned_width(from), signed_width(to)) {
        return a < b;
    }
    match (from, to) {
        (_, Float64) if integer_digits(from).is_some() => true,
        (Float16 | Float32, Float32 | Float64) => true,
        (Decimal128(..) | Decimal256(..), Float64) => true,
        (Decimal128(p, s) | Decimal256(p, s), Decimal128(p2, s2) | Decimal256(p2, s2)) => {
            s2 >= s && (*p2 as i16 - *s2 as i16) >= (*p as i16 - *s as i16)
        }
        (_, Decimal128(p, s) | Decimal256(p, s)) => {
            integer_digits(from).is_some_and(|d| *p as i16 - *s as i16 >= d)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use datafusion::logical_expr::{cast, col, lit, try_cast};

    fn schema() -> DFSchema {
        DFSchema::try_from(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("name", DataType::Utf8, true),
            Field::new("day", DataType::Date32, true),
        ]))
        .unwrap()
    }

    #[test]
    fn plain_comparisons_are_leakproof() {
        let s = schema();
        for e in [
            col("id").eq(lit(5i64)),
            col("amount").gt(lit(0.05f64)),
            col("name").like(lit("A%")),
            col("day").gt_eq(lit("1994-01-01")),
            col("id").in_list(vec![lit(1), lit(2)], false),
            col("id").between(lit(1), lit(10)).and(col("name").is_not_null()),
            try_cast(col("name"), DataType::Int64).eq(lit(1i64)),
            cast(col("id"), DataType::Int64).eq(lit(1i64)),
        ] {
            assert!(!is_fallible(&e, &s), "{e} should be leakproof");
        }
    }

    #[test]
    fn arithmetic_failing_casts_and_functions_are_fallible() {
        let s = schema();
        for e in [
            (lit(1) / (col("id") - lit(2))).eq(lit(1)),
            cast(col("name"), DataType::Int64).eq(lit(1i64)),
            // Coercion casts the string column, which can fail per row.
            col("name").eq(lit(5i64)),
            col("name").eq(col("id")),
            datafusion::functions::expr_fn::abs(col("id")).eq(lit(1)),
        ] {
            assert!(is_fallible(&e, &s), "{e} should be fallible");
        }
    }

    #[test]
    fn constant_sub_expressions_never_count() {
        let s = schema();
        assert!(!is_fallible(&col("id").eq(lit(1) / lit(0)), &s));
    }
}
