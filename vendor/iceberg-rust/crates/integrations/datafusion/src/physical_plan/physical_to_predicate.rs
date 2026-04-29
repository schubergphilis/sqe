// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Convert DataFusion **physical** expressions into iceberg
// [`Predicate`]s for the runtime/dynamic filter pushdown path.
//
// `expr_to_predicate` handles the *logical* `Expr` tree from
// `supports_filters_pushdown`. That covers static filters known at
// planning time. This module handles the smaller subset of physical
// expressions that show up at runtime — specifically, what
// `HashJoinExec`'s `enable_dynamic_filter_pushdown` produces once a
// build side completes:
//
//   - `DynamicFilterPhysicalExpr`: wrapper whose inner expression
//     starts as `lit(true)` and is replaced when the build is sealed.
//   - `BinaryExpr(col >= literal AND col <= literal)`: min/max bounds
//     emitted by `shared_bounds.rs`.
//   - `InListExpr(col, [literals])`: membership filter for join keys.
//   - `BinaryExpr` of the above ANDed together.
//
// Anything outside this vocabulary returns `None`, and the reader
// falls back to its static predicate.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, InListExpr, Literal,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;

/// Convert a slice of runtime [`PhysicalExpr`]s into a single iceberg
/// [`Predicate`] (ANDed together). Returns `None` if no expression in
/// the slice is currently translatable — for example because every
/// `DynamicFilterPhysicalExpr` is still at its initial `lit(true)`
/// placeholder.
pub fn convert_physical_filters_to_predicate(
    filters: &[Arc<dyn PhysicalExpr>],
) -> Option<Predicate> {
    filters
        .iter()
        .filter_map(|f| convert_physical(f))
        .reduce(Predicate::and)
}

fn convert_physical(expr: &Arc<dyn PhysicalExpr>) -> Option<Predicate> {
    let any = expr.as_any();

    // 1. DynamicFilterPhysicalExpr: unwrap to the current inner.
    //    If the inner is still `lit(true)` we cannot produce anything
    //    useful yet — the build side has not completed.
    if let Some(dynamic) = any.downcast_ref::<DynamicFilterPhysicalExpr>() {
        let inner = dynamic.current().ok()?;
        if is_literal_true(&inner) {
            return None;
        }
        return convert_physical(&inner);
    }

    // 2. BinaryExpr: AND combines two predicates; comparisons against a
    //    literal map to iceberg::Predicate::Binary.
    if let Some(binary) = any.downcast_ref::<BinaryExpr>() {
        return convert_binary(binary);
    }

    // 3. InListExpr: IN-list of literals → Predicate::Set
    if let Some(in_list) = any.downcast_ref::<InListExpr>() {
        return convert_in_list(in_list);
    }

    None
}

fn convert_binary(binary: &BinaryExpr) -> Option<Predicate> {
    use datafusion::logical_expr::Operator;

    match binary.op() {
        Operator::And => {
            // Best-effort: produce whichever side(s) translate.
            let left = convert_physical(binary.left());
            let right = convert_physical(binary.right());
            match (left, right) {
                (Some(l), Some(r)) => Some(l.and(r)),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        Operator::Or => {
            // Predicate::or requires both sides to translate; OR of
            // unknown loses correctness if we drop a branch.
            let left = convert_physical(binary.left())?;
            let right = convert_physical(binary.right())?;
            Some(left.or(right))
        }
        op @ (Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq) => convert_comparison(binary.left(), *op, binary.right()),
        _ => None,
    }
}

fn convert_comparison(
    left: &Arc<dyn PhysicalExpr>,
    op: datafusion::logical_expr::Operator,
    right: &Arc<dyn PhysicalExpr>,
) -> Option<Predicate> {
    use datafusion::logical_expr::Operator;

    // Normalize so the column is on the left.
    let (col_name, literal, op) = match (extract_column(left), extract_literal(right)) {
        (Some(col), Some(lit)) => (col, lit, op),
        _ => match (extract_column(right), extract_literal(left)) {
            (Some(col), Some(lit)) => (col, lit, flip_op(op)),
            _ => return None,
        },
    };

    let datum = scalar_to_datum(&literal)?;
    let reference = Reference::new(col_name);
    Some(match op {
        Operator::Eq => reference.equal_to(datum),
        Operator::NotEq => reference.not_equal_to(datum),
        Operator::Lt => reference.less_than(datum),
        Operator::LtEq => reference.less_than_or_equal_to(datum),
        Operator::Gt => reference.greater_than(datum),
        Operator::GtEq => reference.greater_than_or_equal_to(datum),
        _ => return None,
    })
}

fn convert_in_list(in_list: &InListExpr) -> Option<Predicate> {
    if in_list.negated() {
        // not_in is supported by iceberg but we keep this conservative
        // for v1 — extend later when needed.
        return None;
    }
    let col_name = extract_column(in_list.expr())?;
    let mut datums = Vec::with_capacity(in_list.list().len());
    for item in in_list.list() {
        let lit = extract_literal(item)?;
        datums.push(scalar_to_datum(&lit)?);
    }
    if datums.is_empty() {
        return None;
    }
    Some(Reference::new(col_name).is_in(datums))
}

fn extract_column(expr: &Arc<dyn PhysicalExpr>) -> Option<String> {
    expr.as_any()
        .downcast_ref::<Column>()
        .map(|c| c.name().to_string())
}

fn extract_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    expr.as_any()
        .downcast_ref::<Literal>()
        .map(|l| l.value().clone())
}

fn is_literal_true(expr: &Arc<dyn PhysicalExpr>) -> bool {
    matches!(
        extract_literal(expr),
        Some(ScalarValue::Boolean(Some(true)))
    )
}

fn flip_op(op: datafusion::logical_expr::Operator) -> datafusion::logical_expr::Operator {
    use datafusion::logical_expr::Operator;
    match op {
        Operator::Eq => Operator::Eq,
        Operator::NotEq => Operator::NotEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

/// Convert a DataFusion [`ScalarValue`] into an iceberg [`Datum`].
/// Only the primitive types relevant to TPC-H/SSB join keys are
/// supported here; unhandled types return `None` so the caller skips
/// the predicate.
fn scalar_to_datum(scalar: &ScalarValue) -> Option<Datum> {
    match scalar {
        ScalarValue::Boolean(Some(v)) => Some(Datum::bool(*v)),
        ScalarValue::Int32(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int64(Some(v)) => Some(Datum::long(*v)),
        ScalarValue::Float32(Some(v)) => Some(Datum::float(*v)),
        ScalarValue::Float64(Some(v)) => Some(Datum::double(*v)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            Some(Datum::string(s.clone()))
        }
        ScalarValue::Date32(Some(d)) => Some(Datum::date(*d)),
        // Decimal values are intentionally not handled in v1 of the
        // converter. iceberg's `Datum::decimal` expects a `fastnum`
        // decimal that carries explicit precision and scale; the
        // ScalarValue precision/scale would need to round-trip through
        // that. Hash join runtime filters at SF1/SF10 are on integer
        // keys (orderkey, partkey, suppkey) so this gap is harmless
        // for the bench. Returning `None` here makes the converter
        // fall through to the per-batch evaluator for any decimal
        // runtime filter, preserving correctness.
        _ => None,
    }
}

/// Suppress an unused import warning when feature flags trim the
/// coverage of `scalar_to_datum`'s match arms.
#[allow(dead_code)]
fn _ensure_data_type_import_used(_dt: DataType) {}
