// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Correlation detection and #306's uncorrelated-subquery hoist — see
//! `super`'s module doc.

use std::collections::HashMap;

use super::from_clause::resolve_subquery_schema;
use super::scalar::{compile_scalar_subquery, materialize_in_subquery_index};
use super::{select_id, HoistedSubquery};
use crate::codegen::select::CodegenError;
use crate::codegen::{Emitter, RegAlloc, Scope};
use crate::parser::ast::{BinaryOp, Expr, ExprKind, FunctionArgs, ResultColumn, Select};
use crate::schema::TableSchema;

/// Whether `subselect` is correlated — references a column that isn't
/// one of its own `own_schema`'s columns (#306). `own_schema: None`
/// (a `FROM`-less subquery) is always treated as correlated: a
/// `FROM`-less subquery can only reference the enclosing scope, and
/// hoisting has nothing to gain there anyway. Walks `subselect`'s own
/// `WHERE` clause and projected result-column expressions; a nested
/// subquery-bearing expression found anywhere in that walk is
/// conservatively treated as correlated too, rather than reasoning
/// through another scope level. Correlated=true is always the *safe*
/// answer when this check is unsure — it only ever suppresses hoisting,
/// never wrongly allows it.
pub(crate) fn subquery_is_correlated(subselect: &Select, own_schema: Option<&TableSchema>) -> bool {
    let Some(schema) = own_schema else {
        return true;
    };
    // A column reference qualified by a table name (`other.a_id`) only
    // counts as "this subquery's own" when the qualifier names the
    // subquery's own table or its alias — matching `schema.name` alone
    // would wrongly call `WHERE other.a_id = t.id` uncorrelated whenever
    // the *outer* table happens to share a column name with the
    // subquery's own table (`t.id`/`other.id` in #306's regression
    // fixture), since a bare name-only check can't tell `t.id` isn't
    // `other`'s own `id`.
    let own_qualifiers: Vec<&str> = std::iter::once(schema.name.as_str())
        .chain(
            subselect
                .from
                .as_ref()
                .and_then(|f| f.first.alias.as_deref()),
        )
        .collect();
    let mut correlated = false;
    if let Some(where_expr) = &subselect.where_clause {
        walk_expr_for_correlation(where_expr, schema, &own_qualifiers, &mut correlated);
    }
    for col in &subselect.columns {
        match col {
            ResultColumn::Expr { expr, .. } => {
                walk_expr_for_correlation(expr, schema, &own_qualifiers, &mut correlated);
            }
            // `*`/`table.*` project only the subquery's own columns —
            // never a reference to the enclosing scope.
            ResultColumn::Star | ResultColumn::TableStar { .. } => {}
        }
        if correlated {
            break;
        }
    }
    correlated
}

fn walk_expr_for_correlation(
    expr: &Expr,
    schema: &TableSchema,
    own_qualifiers: &[&str],
    correlated: &mut bool,
) {
    if *correlated {
        return;
    }
    match &expr.kind {
        ExprKind::Column { table, name, .. } => {
            let qualifier_ok = match table {
                Some(t) => own_qualifiers.iter().any(|q| q.eq_ignore_ascii_case(t)),
                None => true,
            };
            if !qualifier_ok || !schema.columns.iter().any(|c| c.eq_ignore_ascii_case(name)) {
                *correlated = true;
            }
        }
        ExprKind::Literal(_) | ExprKind::Param(_) => {}
        ExprKind::FunctionCall { args, .. } => {
            if let FunctionArgs::List(list) = args {
                for a in list {
                    walk_expr_for_correlation(a, schema, own_qualifiers, correlated);
                }
            }
        }
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. }
        | ExprKind::Collate { expr: e, .. }
        | ExprKind::Paren(e) => walk_expr_for_correlation(e, schema, own_qualifiers, correlated),
        ExprKind::Binary { lhs, rhs, .. } | ExprKind::Is { lhs, rhs, .. } => {
            walk_expr_for_correlation(lhs, schema, own_qualifiers, correlated);
            walk_expr_for_correlation(rhs, schema, own_qualifiers, correlated);
        }
        ExprKind::Between {
            expr: e, lo, hi, ..
        } => {
            walk_expr_for_correlation(e, schema, own_qualifiers, correlated);
            walk_expr_for_correlation(lo, schema, own_qualifiers, correlated);
            walk_expr_for_correlation(hi, schema, own_qualifiers, correlated);
        }
        ExprKind::In { expr: e, list, .. } => {
            walk_expr_for_correlation(e, schema, own_qualifiers, correlated);
            for item in list {
                walk_expr_for_correlation(item, schema, own_qualifiers, correlated);
            }
        }
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            ..
        } => {
            walk_expr_for_correlation(e, schema, own_qualifiers, correlated);
            walk_expr_for_correlation(pattern, schema, own_qualifiers, correlated);
            if let Some(esc) = escape {
                walk_expr_for_correlation(esc, schema, own_qualifiers, correlated);
            }
        }
        ExprKind::Case {
            operand,
            whens,
            else_,
        } => {
            if let Some(o) = operand {
                walk_expr_for_correlation(o, schema, own_qualifiers, correlated);
            }
            for (w, t) in whens {
                walk_expr_for_correlation(w, schema, own_qualifiers, correlated);
                walk_expr_for_correlation(t, schema, own_qualifiers, correlated);
            }
            if let Some(e) = else_ {
                walk_expr_for_correlation(e, schema, own_qualifiers, correlated);
            }
        }
        // Nested subquery-bearing expressions: conservatively correlated
        // rather than reasoning through another scope level (out of
        // scope for #306's hoist pass).
        ExprKind::Subquery(_)
        | ExprKind::Exists { .. }
        | ExprKind::InSubquery { .. }
        | ExprKind::InSubqueryMulti { .. } => {
            *correlated = true;
        }
    }
}

pub(super) fn is_comparison_op(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    )
}

/// Whether `subquery` is a candidate for #306's hoist: it has a
/// (non-joined, single-table) `FROM` this pass can resolve, and it's
/// not correlated against the enclosing query.
fn subquery_hoistable(subquery: &Select, outer_scope: &Scope) -> bool {
    let Ok(resolved) = resolve_subquery_schema(subquery, &outer_scope.catalog) else {
        return false;
    };
    let Some(schema) = resolved else {
        // FROM-less: nothing to gain from hoisting (no per-row scan
        // cost to eliminate), and `subquery_is_correlated` already
        // treats it as correlated anyway.
        return false;
    };
    !subquery_is_correlated(subquery, Some(&schema))
}

/// The top-level `AND`-conjuncts of `expr` — splits only `AND` (and
/// `Paren`-wrapped `AND`), leaving any `OR`/`NOT`/other nesting as a
/// single opaque conjunct. Used by
/// [`hoist_uncorrelated_where_subqueries`] to find a WHERE clause's
/// directly-AND-joined subquery conjuncts without having to reason
/// about deeper nesting.
pub(super) fn top_level_and_conjuncts(expr: &Expr) -> Vec<&Expr> {
    match &expr.kind {
        ExprKind::Binary {
            op: BinaryOp::And,
            lhs,
            rhs,
        } => {
            let mut out = top_level_and_conjuncts(lhs);
            out.extend(top_level_and_conjuncts(rhs));
            out
        }
        ExprKind::Paren(inner) => top_level_and_conjuncts(inner),
        _ => vec![expr],
    }
}

/// Recognizes and materializes one hoistable conjunct: a top-level
/// `expr IN (SELECT ...)`, or a top-level comparison with a scalar
/// subquery operand — the two shapes #306's reproduction actually hits.
/// Returns the subquery's own [`select_id`] (for the caller to key the
/// returned map) plus what got precomputed, or `None` if `conjunct`
/// doesn't match either shape or its subquery turns out to be
/// correlated (in which case nothing is emitted and the caller's
/// existing per-row path handles it unchanged). Returns the `usize` key
/// rather than the `&Select` itself — `conjunct`/`outer_scope` are two
/// independent borrowed parameters, so the qualified subset's ban on
/// explicit lifetimes has no elidable shape that could tie an `&Select`
/// return to `conjunct`'s borrow alone.
fn try_hoist_conjunct(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    outer_scope: &Scope,
    conjunct: &Expr,
) -> Result<Option<(usize, HoistedSubquery)>, CodegenError> {
    match &conjunct.kind {
        ExprKind::InSubquery { subquery, .. } if subquery_hoistable(subquery, outer_scope) => {
            let eph_cursor = materialize_in_subquery_index(em, reg, outer_scope, subquery)?;
            Ok(Some((
                select_id(subquery),
                HoistedSubquery::In { eph_cursor },
            )))
        }
        ExprKind::Binary { op, lhs, rhs } if is_comparison_op(*op) => {
            for side in [lhs.as_ref(), rhs.as_ref()] {
                if let ExprKind::Subquery(subquery) = &side.kind {
                    if subquery_hoistable(subquery, outer_scope) {
                        let dest = compile_scalar_subquery(em, reg, outer_scope, subquery)?;
                        return Ok(Some((
                            select_id(subquery),
                            HoistedSubquery::Scalar { reg: dest },
                        )));
                    }
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Hoists every uncorrelated `IN`/scalar subquery found as a top-level
/// `WHERE`-clause conjunct (#306) out of the enclosing single-table
/// scan's per-row loop: each is materialized exactly once here, *before*
/// the scan's `Rewind`, instead of being re-materialized on every outer
/// row by `compile_in_subquery`/`compile_scalar_subquery`'s normal
/// per-occurrence codegen. Returns a map (from [`select_id`] to what got
/// precomputed) meant to be attached to the scan's own [`Scope`] via
/// [`Scope::with_hoisted`] — `compile_cond`/`compile_value`'s
/// `InSubquery`/`Subquery` dispatch then read the cheap precomputed
/// cursor/register per row instead of rebuilding it.
///
/// Deliberately narrow, matching the issue's own reproduction: only a
/// conjunct that is *exactly* `expr IN (SELECT ...)` or a top-level
/// comparison with a scalar-subquery operand is recognized (see
/// [`try_hoist_conjunct`]); `OR`, `NOT`, deeper nesting, multi-column
/// `IN`, and any correlated subquery are left completely untouched and
/// fall through to the existing, unmodified per-row path. Also
/// deliberately scoped to the single-table scan path
/// (`compile_direct_scan`/`compile_sorted_scan`) — the joined-query path
/// (`compile_join_level`/`emit_join_final_row`) is a follow-up this pass
/// does not attempt, so a joined query's WHERE-clause subquery keeps
/// re-evaluating per candidate row exactly as before.
pub(crate) fn hoist_uncorrelated_where_subqueries(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    outer_scope: &Scope,
    where_clause: &Expr,
) -> Result<HashMap<usize, HoistedSubquery>, CodegenError> {
    let mut out = HashMap::new();
    for conjunct in top_level_and_conjuncts(where_clause) {
        if let Some((key, hoisted)) = try_hoist_conjunct(em, reg, outer_scope, conjunct)? {
            out.insert(key, hoisted);
        }
    }
    Ok(out)
}
