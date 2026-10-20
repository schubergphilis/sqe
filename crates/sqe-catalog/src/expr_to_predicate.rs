//! Converts DataFusion `Expr` filter expressions to iceberg-rust `Predicate`
//! values for predicate pushdown into Iceberg table scans.
//!
//! Supported expressions:
//! - Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
//! - `IS NULL` / `IS NOT NULL`
//! - `AND` / `OR` / `NOT`
//! - `IN` list / `NOT IN` list
//! - `LIKE` prefix patterns (e.g., `col LIKE 'prefix%'`) → `STARTS WITH`
//! - Boolean columns used as bare predicates (e.g., `WHERE is_active`)
//! - `CAST` expressions (passed through, except date casts)

use datafusion::arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::logical_expr::{Expr, Like, Operator};
use datafusion::scalar::ScalarValue;
use iceberg::expr::{BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression};
use iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType};

/// Intermediate result of converting a DataFusion expression node.
///
/// A DataFusion `Expr` can represent a full predicate (`a > 10`), a bare
/// column reference, or a scalar literal.  We need to distinguish these
/// cases so that binary operators like `=` can combine a column with a
/// literal into a `Predicate::Binary`.
enum TransformedResult {
    Predicate(Predicate),
    /// The column and, when the scan schema is known, its type.
    Column(Reference, Option<DataType>),
    Literal(Datum),
    NotTransformed,
}

enum OpTransformedResult {
    Operator(PredicateOperator),
    And,
    Or,
    NotTransformed,
}

/// Converts a slice of DataFusion filter expressions into a single iceberg
/// [`Predicate`], combining them with `AND`.
///
/// Returns `None` if none of the filters could be converted.
pub fn convert_filters_to_predicate(filters: &[Expr]) -> Option<Predicate> {
    convert_filters_to_predicate_for(filters, None)
}

/// [`convert_filters_to_predicate`] against the scan schema. A timestamp
/// literal is then converted to its column's unit and zone: a cast like
/// `CAST(ts AS TIMESTAMP(9)) = <ns literal>` reaches here as `ts = <ns>`,
/// and Iceberg binds a literal to a column by reinterpreting the number.
pub fn convert_filters_to_predicate_for(
    filters: &[Expr],
    schema: Option<&Schema>,
) -> Option<Predicate> {
    filters
        .iter()
        .filter_map(|f| convert_filter_to_predicate(f, schema))
        .reduce(Predicate::and)
}

fn convert_filter_to_predicate(expr: &Expr, schema: Option<&Schema>) -> Option<Predicate> {
    match to_iceberg_predicate(expr, schema) {
        TransformedResult::Predicate(predicate) => Some(predicate),
        TransformedResult::Column(column, _) => {
            // A bare column in a filter context represents a boolean column check.
            // Convert it to: column = true
            Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::Eq,
                column,
                Datum::bool(true),
            )))
        }
        TransformedResult::Literal(_) | TransformedResult::NotTransformed => None,
    }
}

fn to_iceberg_predicate(expr: &Expr, schema: Option<&Schema>) -> TransformedResult {
    match expr {
        Expr::BinaryExpr(binary) => {
            let left = to_iceberg_predicate(&binary.left, schema);
            let right = to_iceberg_predicate(&binary.right, schema);
            let op = to_iceberg_operation(binary.op);
            match op {
                OpTransformedResult::Operator(op) => to_iceberg_binary_predicate(left, right, op),
                OpTransformedResult::And => to_iceberg_and_predicate(left, right),
                OpTransformedResult::Or => to_iceberg_or_predicate(left, right),
                OpTransformedResult::NotTransformed => TransformedResult::NotTransformed,
            }
        }
        Expr::Not(exp) => {
            let inner = to_iceberg_predicate(exp, schema);
            match inner {
                TransformedResult::Predicate(p) => TransformedResult::Predicate(!p),
                TransformedResult::Column(column, _) => {
                    // NOT of a bare boolean column: NOT col => col = false
                    TransformedResult::Predicate(Predicate::Binary(BinaryExpression::new(
                        PredicateOperator::Eq,
                        column,
                        Datum::bool(false),
                    )))
                }
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::Column(column) => {
            let data_type = schema
                .and_then(|s| s.field_with_name(column.name()).ok())
                .map(|f| f.data_type().clone());
            TransformedResult::Column(Reference::new(column.name()), data_type)
        }
        Expr::Literal(literal, _) => match scalar_value_to_datum(literal) {
            Some(data) => TransformedResult::Literal(data),
            None => TransformedResult::NotTransformed,
        },
        Expr::InList(inlist) => {
            let mut datums = vec![];
            for item in &inlist.list {
                match to_iceberg_predicate(item, schema) {
                    TransformedResult::Literal(l) => datums.push(l),
                    _ => return TransformedResult::NotTransformed,
                }
            }
            match to_iceberg_predicate(&inlist.expr, schema) {
                TransformedResult::Column(r, data_type) => {
                    let Some(datums) = datums
                        .into_iter()
                        .map(|d| datum_for_column(d, data_type.as_ref()))
                        .collect::<Option<Vec<_>>>()
                    else {
                        return TransformedResult::NotTransformed;
                    };
                    if inlist.negated {
                        TransformedResult::Predicate(r.is_not_in(datums))
                    } else {
                        TransformedResult::Predicate(r.is_in(datums))
                    }
                }
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::IsNull(inner) => match to_iceberg_predicate(inner, schema) {
            TransformedResult::Column(r, _) => TransformedResult::Predicate(Predicate::Unary(
                UnaryExpression::new(PredicateOperator::IsNull, r),
            )),
            _ => TransformedResult::NotTransformed,
        },
        Expr::IsNotNull(inner) => match to_iceberg_predicate(inner, schema) {
            TransformedResult::Column(r, _) => TransformedResult::Predicate(Predicate::Unary(
                UnaryExpression::new(PredicateOperator::NotNull, r),
            )),
            _ => TransformedResult::NotTransformed,
        },
        // `CAST(col AS t) op lit` is pushed as `col op lit`, which prunes
        // correctly only when the cast keeps every value and its order:
        // `CAST(f AS INT) = 3` holds for f = 3.4, yet `f = 3` prunes a file
        // whose f runs 3.2..3.6.
        Expr::Cast(c) => match to_iceberg_predicate(&c.expr, schema) {
            TransformedResult::Column(r, Some(from))
                if cast_is_exact(&from, c.field.data_type()) =>
            {
                TransformedResult::Column(r, Some(from))
            }
            _ => TransformedResult::NotTransformed,
        },
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            // Iceberg's StartsWith is case-sensitive; ILIKE and escape chars
            // are not supported for pushdown.
            if escape_char.is_some() || *case_insensitive {
                return TransformedResult::NotTransformed;
            }

            let pattern_str = match to_iceberg_predicate(pattern, schema) {
                TransformedResult::Literal(d) => match d.literal() {
                    PrimitiveLiteral::String(s) => s.clone(),
                    _ => return TransformedResult::NotTransformed,
                },
                _ => return TransformedResult::NotTransformed,
            };

            // Only simple prefix patterns: ends with '%' and no other wildcards.
            if pattern_str.ends_with('%')
                && !pattern_str[..pattern_str.len() - 1].contains(['%', '_'])
            {
                let prefix = pattern_str[..pattern_str.len() - 1].to_string();
                let column = match to_iceberg_predicate(expr, schema) {
                    TransformedResult::Column(r, _) => r,
                    _ => return TransformedResult::NotTransformed,
                };
                let predicate = if *negated {
                    column.not_starts_with(Datum::string(prefix))
                } else {
                    column.starts_with(Datum::string(prefix))
                };
                TransformedResult::Predicate(predicate)
            } else {
                TransformedResult::NotTransformed
            }
        }
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_operation(op: Operator) -> OpTransformedResult {
    match op {
        Operator::Eq => OpTransformedResult::Operator(PredicateOperator::Eq),
        Operator::NotEq => OpTransformedResult::Operator(PredicateOperator::NotEq),
        Operator::Lt => OpTransformedResult::Operator(PredicateOperator::LessThan),
        Operator::LtEq => OpTransformedResult::Operator(PredicateOperator::LessThanOrEq),
        Operator::Gt => OpTransformedResult::Operator(PredicateOperator::GreaterThan),
        Operator::GtEq => OpTransformedResult::Operator(PredicateOperator::GreaterThanOrEq),
        Operator::And => OpTransformedResult::And,
        Operator::Or => OpTransformedResult::Or,
        _ => OpTransformedResult::NotTransformed,
    }
}

/// Whether a cast from `from` to `to` maps every value to a distinct value
/// in the same order, with a literal of `to` binding exactly to a `from`
/// column. Float widening is left out: Iceberg rounds a double literal to
/// f32 when it binds it to a float column. A zone change is safe only
/// between UTC spellings, because Arrow reads a timestamp without a zone
/// as local time in the target zone.
fn cast_is_exact(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    fn utc(tz: &Option<std::sync::Arc<str>>) -> bool {
        matches!(tz.as_deref(), None | Some("+00:00" | "UTC" | "Z"))
    }
    match (from, to) {
        _ if from == to => true,
        (Int8, Int16 | Int32 | Int64) | (Int16, Int32 | Int64) | (Int32, Int64) => true,
        (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View) => true,
        (Timestamp(fu, ftz), Timestamp(tu, ttz)) => {
            let unit_ok = fu == tu || (*fu == TimeUnit::Microsecond && *tu == TimeUnit::Nanosecond);
            unit_ok && (ftz == ttz || (utc(ftz) && utc(ttz)))
        }
        _ => false,
    }
}

/// For AND, if only one side converts, we still push that side down.
/// SAFETY: Dropping one side of an AND is correct only because
/// supports_filters_pushdown returns Inexact, which forces DataFusion
/// to re-evaluate the full filter post-scan. If this ever changes to
/// Exact, partial pushdown would silently drop filter conditions.
fn to_iceberg_and_predicate(
    left: TransformedResult,
    right: TransformedResult,
) -> TransformedResult {
    match (left, right) {
        (TransformedResult::Predicate(l), TransformedResult::Predicate(r)) => {
            TransformedResult::Predicate(l.and(r))
        }
        (TransformedResult::Predicate(l), _) => TransformedResult::Predicate(l),
        (_, TransformedResult::Predicate(r)) => TransformedResult::Predicate(r),
        _ => TransformedResult::NotTransformed,
    }
}

/// For OR, *both* sides must convert — otherwise the result would be too broad.
fn to_iceberg_or_predicate(left: TransformedResult, right: TransformedResult) -> TransformedResult {
    match (left, right) {
        (TransformedResult::Predicate(l), TransformedResult::Predicate(r)) => {
            TransformedResult::Predicate(l.or(r))
        }
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_binary_predicate(
    left: TransformedResult,
    right: TransformedResult,
    op: PredicateOperator,
) -> TransformedResult {
    let (r, d, final_op) = match (left, right) {
        (TransformedResult::Column(r, t), TransformedResult::Literal(d)) => {
            (r, datum_for_column(d, t.as_ref()), op)
        }
        (TransformedResult::Literal(d), TransformedResult::Column(r, t)) => (
            r,
            datum_for_column(d, t.as_ref()),
            reverse_predicate_operator(op),
        ),
        _ => return TransformedResult::NotTransformed,
    };
    let Some(d) = d else {
        return TransformedResult::NotTransformed;
    };
    TransformedResult::Predicate(Predicate::Binary(BinaryExpression::new(final_op, r, d)))
}

fn reverse_predicate_operator(op: PredicateOperator) -> PredicateOperator {
    match op {
        PredicateOperator::Eq => PredicateOperator::Eq,
        PredicateOperator::NotEq => PredicateOperator::NotEq,
        PredicateOperator::GreaterThan => PredicateOperator::LessThan,
        PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
        PredicateOperator::LessThan => PredicateOperator::GreaterThan,
        PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
        _ => unreachable!("reverse_predicate_operator called with {}", op),
    }
}

const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

/// Convert a DataFusion [`ScalarValue`] to an iceberg [`Datum`].
fn scalar_value_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Boolean(Some(v)) => Some(Datum::bool(*v)),
        ScalarValue::Int8(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int16(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int32(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int64(Some(v)) => Some(Datum::long(*v)),
        ScalarValue::Float32(Some(v)) => Some(Datum::double(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(Datum::double(*v)),
        ScalarValue::Utf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::LargeUtf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::Binary(Some(v)) => Some(Datum::binary(v.clone())),
        ScalarValue::LargeBinary(Some(v)) => Some(Datum::binary(v.clone())),
        ScalarValue::Date32(Some(v)) => Some(Datum::date(*v)),
        ScalarValue::Date64(Some(v)) => Some(Datum::date((*v / MILLIS_PER_DAY) as i32)),
        ScalarValue::TimestampMicrosecond(Some(v), None) => Some(Datum::timestamp_micros(*v)),
        ScalarValue::TimestampMicrosecond(Some(v), Some(_)) => Some(Datum::timestamptz_micros(*v)),
        ScalarValue::TimestampNanosecond(Some(v), None) => Some(Datum::timestamp_nanos(*v)),
        ScalarValue::TimestampNanosecond(Some(v), Some(_)) => Some(Datum::timestamptz_nanos(*v)),
        _ => None,
    }
}

/// `datum` in the unit and zone of the column it is compared with, or
/// `None` when that can't be done exactly (a nanosecond literal with a
/// sub-microsecond part against a microsecond column, or a nanosecond
/// literal against a column of unknown type). Iceberg binds a timestamp
/// literal by reinterpreting the number, so a unit mismatch would prune
/// with a value off by 1000x. Not pushing a conjunct down is safe: the
/// pushdown is `Inexact` and DataFusion still applies the filter.
fn datum_for_column(datum: Datum, column_type: Option<&DataType>) -> Option<Datum> {
    let nanos = match datum.data_type() {
        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => false,
        PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs => true,
        _ => return Some(datum),
    };
    let PrimitiveLiteral::Long(v) = *datum.literal() else {
        return Some(datum);
    };
    match column_type {
        Some(DataType::Timestamp(TimeUnit::Microsecond, tz)) => {
            let micros = if nanos {
                (v % 1_000 == 0).then_some(v / 1_000)?
            } else {
                v
            };
            Some(match tz {
                Some(_) => Datum::timestamptz_micros(micros),
                None => Datum::timestamp_micros(micros),
            })
        }
        Some(DataType::Timestamp(TimeUnit::Nanosecond, tz)) => {
            let nanos = if nanos { v } else { v.checked_mul(1_000)? };
            Some(match tz {
                Some(_) => Datum::timestamptz_nanos(nanos),
                None => Datum::timestamp_nanos(nanos),
            })
        }
        _ if nanos => None,
        _ => Some(datum),
    }
}

/// Returns `true` if the filter expression is a type we can push down.
///
/// Used by `SqeTableProvider::supports_filters_pushdown()` to tell DataFusion
/// which filters the table provider will handle.
pub fn is_filter_pushdown_supported(expr: &Expr, schema: Option<&Schema>) -> bool {
    convert_filter_to_predicate(expr, schema).is_some()
}

#[cfg(test)]
mod tests {
    use datafusion::common::Column;
    use datafusion::logical_expr::{BinaryExpr, Expr};
    use datafusion::scalar::ScalarValue;
    use iceberg::expr::{Predicate, Reference};
    use iceberg::spec::Datum;

    use super::*;

    // ─── Helper ────────────────────────────────────────────────────

    fn col(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    fn lit_i64(v: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), None)
    }

    fn lit_str(v: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(v.to_string())), None)
    }

    fn lit_bool(v: bool) -> Expr {
        Expr::Literal(ScalarValue::Boolean(Some(v)), None)
    }

    fn lit_f64(v: f64) -> Expr {
        Expr::Literal(ScalarValue::Float64(Some(v)), None)
    }

    fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    // ─── Comparison operators ──────────────────────────────────────

    #[test]
    fn test_eq() {
        let expr = binary(col("a"), Operator::Eq, lit_i64(42));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, Reference::new("a").equal_to(Datum::long(42)));
    }

    #[test]
    fn test_neq() {
        let expr = binary(col("a"), Operator::NotEq, lit_i64(7));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, Reference::new("a").not_equal_to(Datum::long(7)));
    }

    #[test]
    fn test_lt() {
        let expr = binary(col("a"), Operator::Lt, lit_i64(10));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, Reference::new("a").less_than(Datum::long(10)));
    }

    #[test]
    fn test_lteq() {
        let expr = binary(col("a"), Operator::LtEq, lit_i64(10));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(
            pred,
            Reference::new("a").less_than_or_equal_to(Datum::long(10))
        );
    }

    #[test]
    fn test_gt() {
        let expr = binary(col("a"), Operator::Gt, lit_i64(10));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, Reference::new("a").greater_than(Datum::long(10)));
    }

    #[test]
    fn test_gteq() {
        let expr = binary(col("a"), Operator::GtEq, lit_i64(10));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(
            pred,
            Reference::new("a").greater_than_or_equal_to(Datum::long(10))
        );
    }

    // ─── Reversed operand (literal on left) ────────────────────────

    #[test]
    fn test_reversed_operand() {
        // 5 < col  =>  col > 5
        let expr = binary(lit_i64(5), Operator::Lt, col("x"));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, Reference::new("x").greater_than(Datum::long(5)));
    }

    // ─── IS NULL / IS NOT NULL ─────────────────────────────────────

    #[test]
    fn test_is_null() {
        let expr = Expr::IsNull(Box::new(col("a")));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, Reference::new("a").is_null());
    }

    #[test]
    fn test_is_not_null() {
        let expr = Expr::IsNotNull(Box::new(col("a")));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, Reference::new("a").is_not_null());
    }

    // ─── AND / OR / NOT ────────────────────────────────────────────

    #[test]
    fn test_and() {
        let left = binary(col("a"), Operator::Gt, lit_i64(1));
        let right = binary(col("b"), Operator::Eq, lit_str("x"));
        let expr = binary(left, Operator::And, right);
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        let expected = Predicate::and(
            Reference::new("a").greater_than(Datum::long(1)),
            Reference::new("b").equal_to(Datum::string("x")),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn test_or() {
        let left = binary(col("a"), Operator::Gt, lit_i64(1));
        let right = binary(col("b"), Operator::Lt, lit_i64(5));
        let expr = binary(left, Operator::Or, right);
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        let expected = Predicate::or(
            Reference::new("a").greater_than(Datum::long(1)),
            Reference::new("b").less_than(Datum::long(5)),
        );
        assert_eq!(pred, expected);
    }

    #[test]
    fn test_not() {
        let inner = binary(col("a"), Operator::Eq, lit_i64(1));
        let expr = Expr::Not(Box::new(inner));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, !Reference::new("a").equal_to(Datum::long(1)));
    }

    // ─── IN / NOT IN ───────────────────────────────────────────────

    #[test]
    fn test_in_list() {
        let expr = Expr::InList(datafusion::logical_expr::expr::InList {
            expr: Box::new(col("a")),
            list: vec![lit_i64(1), lit_i64(2), lit_i64(3)],
            negated: false,
        });
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(
            pred,
            Reference::new("a").is_in([Datum::long(1), Datum::long(2), Datum::long(3)])
        );
    }

    #[test]
    fn test_not_in_list() {
        let expr = Expr::InList(datafusion::logical_expr::expr::InList {
            expr: Box::new(col("a")),
            list: vec![lit_i64(1), lit_i64(2)],
            negated: true,
        });
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(
            pred,
            Reference::new("a").is_not_in([Datum::long(1), Datum::long(2)])
        );
    }

    // ─── LIKE (prefix pattern) ─────────────────────────────────────

    #[test]
    fn test_like_prefix() {
        let expr = Expr::Like(Like {
            negated: false,
            expr: Box::new(col("name")),
            pattern: Box::new(lit_str("foo%")),
            escape_char: None,
            case_insensitive: false,
        });
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(
            pred,
            Reference::new("name").starts_with(Datum::string("foo"))
        );
    }

    #[test]
    fn test_not_like_prefix() {
        let expr = Expr::Like(Like {
            negated: true,
            expr: Box::new(col("name")),
            pattern: Box::new(lit_str("bar%")),
            escape_char: None,
            case_insensitive: false,
        });
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(
            pred,
            Reference::new("name").not_starts_with(Datum::string("bar"))
        );
    }

    #[test]
    fn test_like_complex_pattern_not_pushed_down() {
        let expr = Expr::Like(Like {
            negated: false,
            expr: Box::new(col("name")),
            pattern: Box::new(lit_str("fo%o")),
            escape_char: None,
            case_insensitive: false,
        });
        assert!(convert_filters_to_predicate(&[expr]).is_none());
    }

    #[test]
    fn test_ilike_not_pushed_down() {
        let expr = Expr::Like(Like {
            negated: false,
            expr: Box::new(col("name")),
            pattern: Box::new(lit_str("foo%")),
            escape_char: None,
            case_insensitive: true,
        });
        assert!(convert_filters_to_predicate(&[expr]).is_none());
    }

    // ─── Multiple filters combined with AND ────────────────────────

    #[test]
    fn test_multiple_filters() {
        let f1 = binary(col("a"), Operator::Gt, lit_i64(5));
        let f2 = binary(col("b"), Operator::Eq, lit_str("hello"));
        let pred = convert_filters_to_predicate(&[f1, f2]).unwrap();
        let expected = Predicate::and(
            Reference::new("a").greater_than(Datum::long(5)),
            Reference::new("b").equal_to(Datum::string("hello")),
        );
        assert_eq!(pred, expected);
    }

    // ─── Unsupported expressions ───────────────────────────────────

    #[test]
    fn test_unsupported_returns_none() {
        // Addition is not a pushdown-able expression
        let expr = binary(col("a"), Operator::Plus, lit_i64(1));
        assert!(convert_filters_to_predicate(&[expr]).is_none());
    }

    #[test]
    fn test_empty_filters() {
        assert!(convert_filters_to_predicate(&[]).is_none());
    }

    // ─── Scalar value conversions ──────────────────────────────────

    #[test]
    fn test_boolean_literal() {
        let expr = binary(col("flag"), Operator::Eq, lit_bool(true));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, Reference::new("flag").equal_to(Datum::bool(true)));
    }

    #[test]
    fn test_float_literal() {
        let expr = binary(col("price"), Operator::Lt, lit_f64(9.99));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, Reference::new("price").less_than(Datum::double(9.99)));
    }

    #[test]
    fn test_date32_literal() {
        let expr = binary(
            col("d"),
            Operator::Eq,
            Expr::Literal(ScalarValue::Date32(Some(19000)), None),
        );
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, Reference::new("d").equal_to(Datum::date(19000)));
    }

    #[test]
    fn test_timestamp_micros_literal() {
        let ts = 1672876800000000i64;
        let expr = binary(
            col("ts"),
            Operator::GtEq,
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(ts), None), None),
        );
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(
            pred,
            Reference::new("ts").greater_than_or_equal_to(Datum::timestamp_micros(ts))
        );
    }

    // ─── Bare boolean column ───────────────────────────────────────

    #[test]
    fn test_bare_boolean_column() {
        let pred = convert_filters_to_predicate(&[col("active")]).unwrap();
        assert_eq!(
            pred,
            Predicate::Binary(BinaryExpression::new(
                PredicateOperator::Eq,
                Reference::new("active"),
                Datum::bool(true),
            ))
        );
    }

    #[test]
    fn test_not_bare_boolean_column() {
        let expr = Expr::Not(Box::new(col("active")));
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(
            pred,
            Predicate::Binary(BinaryExpression::new(
                PredicateOperator::Eq,
                Reference::new("active"),
                Datum::bool(false),
            ))
        );
    }

    // ─── is_filter_pushdown_supported ──────────────────────────────

    #[test]
    fn test_pushdown_supported() {
        let expr = binary(col("a"), Operator::Eq, lit_i64(1));
        assert!(is_filter_pushdown_supported(&expr, None));
    }

    #[test]
    fn test_pushdown_unsupported() {
        let expr = binary(col("a"), Operator::Plus, lit_i64(1));
        assert!(!is_filter_pushdown_supported(&expr, None));
    }

    #[test]
    fn timestamp_literal_takes_the_column_unit_and_zone() {
        use datafusion::arrow::datatypes::Field;
        let tz = Some("+00:00".into());
        let schema = Schema::new(vec![
            Field::new("us", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new(
                "ns",
                DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
                true,
            ),
        ]);
        let pushed = |name: &str, v: ScalarValue| {
            let expr = binary(col(name), Operator::Eq, Expr::Literal(v, None));
            convert_filters_to_predicate_for(&[expr], Some(&schema))
        };
        // A ns literal on a µs column (a cast that was passed through)
        // is scaled to µs, or not pushed when it has a sub-µs part.
        let p = pushed("us", ScalarValue::TimestampNanosecond(Some(5_000), None));
        assert_eq!(
            p.unwrap(),
            Reference::new("us").equal_to(Datum::timestamp_micros(5))
        );
        assert!(pushed("us", ScalarValue::TimestampNanosecond(Some(5_001), None)).is_none());
        // A zoned ns column gets a zoned ns datum, whatever the literal's zone.
        let p = pushed("ns", ScalarValue::TimestampNanosecond(Some(7), None));
        assert_eq!(
            p.unwrap(),
            Reference::new("ns").equal_to(Datum::timestamptz_nanos(7))
        );
        let p = pushed("ns", ScalarValue::TimestampMicrosecond(Some(7), tz));
        assert_eq!(
            p.unwrap(),
            Reference::new("ns").equal_to(Datum::timestamptz_nanos(7_000))
        );
        // Only an exact cast is looked through.
        let f_schema = Schema::new(vec![
            Field::new("f", DataType::Float32, true),
            Field::new("i", DataType::Int32, true),
        ]);
        let cast_eq = |name: &str, to: DataType, v: ScalarValue| {
            let cast = Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(col(name)), to));
            let expr = binary(cast, Operator::Eq, Expr::Literal(v, None));
            convert_filters_to_predicate_for(&[expr], Some(&f_schema))
        };
        assert!(cast_eq("f", DataType::Int32, ScalarValue::Int32(Some(3))).is_none());
        assert!(cast_eq("f", DataType::Float64, ScalarValue::Float64(Some(0.1))).is_none());
        let p = cast_eq("i", DataType::Int64, ScalarValue::Int64(Some(3)));
        assert_eq!(p.unwrap(), Reference::new("i").equal_to(Datum::long(3)));
        // Without a column type a ns literal is not pushed.
        let expr = binary(
            col("x"),
            Operator::Eq,
            Expr::Literal(ScalarValue::TimestampNanosecond(Some(7), None), None),
        );
        assert!(!is_filter_pushdown_supported(&expr, None));
    }

    // ─── AND with partial pushdown ─────────────────────────────────

    #[test]
    fn test_and_partial_pushdown() {
        // (a > 1) AND (unsupported) — only the first half survives
        let supported = binary(col("a"), Operator::Gt, lit_i64(1));
        let unsupported = binary(col("a"), Operator::Plus, lit_i64(1));
        let expr = binary(supported, Operator::And, unsupported);
        let pred = convert_filters_to_predicate(&[expr]).unwrap();
        assert_eq!(pred, Reference::new("a").greater_than(Datum::long(1)));
    }

    #[test]
    fn test_or_partial_pushdown_returns_none() {
        // (a > 1) OR (unsupported) — cannot push down
        let supported = binary(col("a"), Operator::Gt, lit_i64(1));
        let unsupported = binary(col("a"), Operator::Plus, lit_i64(1));
        let expr = binary(supported, Operator::Or, unsupported);
        assert!(convert_filters_to_predicate(&[expr]).is_none());
    }
}
