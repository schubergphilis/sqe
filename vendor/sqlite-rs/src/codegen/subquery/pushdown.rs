// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Predicate push-down into `FROM`-subqueries and views (#532) — see
//! `super`'s module doc.
//!
//! Runs once, right after [`super::expand_views`]/[`super::expand_with_clause`]
//! have rewritten every view/CTE reference into a `TableRefKind::Subquery`,
//! and before any scan/join codegen or catalog resolution happens. Splits
//! the enclosing `SELECT`'s own `WHERE` into top-level `AND`-conjuncts
//! (the same split [`super::hoist_uncorrelated_where_subqueries`] uses) and
//! moves a conjunct into a `FROM`-subquery's own `WHERE` when doing so is
//! provably safe:
//!
//! - the subquery is a plain single-table `SELECT` (no `JOIN` of its own,
//!   no `DISTINCT`/aggregate/`GROUP BY`/`HAVING`/`LIMIT`, not a compound
//!   `UNION`) — anything else could change which rows survive a filter
//!   applied *before* that operation instead of after;
//! - the subquery's projection is either a bare `SELECT *` (any column
//!   name passes through unchanged) or a list of plain column references,
//!   optionally aliased (`SELECT a, b AS c FROM t` — each output name is
//!   exactly that one column, alias or not) — a *computed* result column
//!   has no single underlying column a predicate on it could be rewritten
//!   against, so a conjunct touching one is left alone;
//! - every column the conjunct references resolves unambiguously to that
//!   one subquery (qualified with its alias, or unqualified when the
//!   enclosing `FROM` has no `JOIN` at all to be ambiguous with).
//!
//! A conjunct containing its own subquery expression (`Subquery`/`Exists`/
//! `InSubquery`/`InSubqueryMulti`) is never pushed — reasoning about a
//! second scope nested inside the moved predicate is out of scope here,
//! matching this pass's conservative default of leaving anything
//! unrecognized exactly where it was. Pushing is applied recursively, so
//! a predicate can chain through nested views/CTEs.

use super::correlation::top_level_and_conjuncts;
use crate::codegen::select::select_has_aggregate;
use crate::parser::ast::{
    BinaryOp, Expr, ExprKind, FunctionArgs, ResultColumn, Select, TableRef, TableRefKind,
};

/// How a `FROM`-subquery's projection maps an output column name back to
/// the underlying expression a pushed predicate should reference instead.
/// Shared with [`super::flatten`], which needs the identical projection-
/// shape eligibility check when rewriting the *rest* of the enclosing
/// query's references to a flattened subquery's alias.
pub(super) enum ColumnMap {
    /// `SELECT * FROM t` — any bare name passes through unchanged.
    Wildcard,
    /// `SELECT a, b AS c, ... FROM t` — each output name (its alias, or
    /// its own name when unaliased) maps to exactly that column; a
    /// computed expression is never in this list.
    Explicit(Vec<(String, Expr)>),
}

/// Recursively pushes safely-movable outer `WHERE` conjuncts into
/// `select`'s own `FROM`-subqueries/views (#532), then descends into
/// whatever subqueries remain so a pushed predicate keeps chaining
/// through nested views/CTEs.
pub fn push_down_where_predicates(select: &mut Select) {
    let require_qualified = select
        .from
        .as_ref()
        .is_none_or(|from| !from.joins.is_empty());

    if let Some(where_expr) = select.where_clause.clone() {
        let mut remaining = Vec::new();
        let mut any_pushed = false;
        for conjunct in top_level_and_conjuncts(&where_expr) {
            if push_conjunct_into_subqueries(select, conjunct, require_qualified) {
                any_pushed = true;
            } else {
                remaining.push(conjunct.clone());
            }
        }
        if any_pushed {
            select.where_clause = rebuild_conjunction(remaining);
        }
    }

    recurse_into_from_subqueries(select);
}

fn push_conjunct_into_subqueries(
    select: &mut Select,
    conjunct: &Expr,
    require_qualified: bool,
) -> bool {
    let Some(from) = &mut select.from else {
        return false;
    };
    if try_push_into_table_ref(&mut from.first, conjunct, require_qualified) {
        return true;
    }
    from.joins
        .iter_mut()
        .any(|join| try_push_into_table_ref(&mut join.table, conjunct, require_qualified))
}

fn try_push_into_table_ref(
    table_ref: &mut TableRef,
    conjunct: &Expr,
    require_qualified: bool,
) -> bool {
    let TableRefKind::Subquery(inner) = &mut table_ref.kind else {
        return false;
    };
    if !subquery_pushdown_safe(inner) {
        return false;
    }
    let Some(column_map) = subquery_column_map(inner) else {
        return false;
    };
    // A subquery in FROM always carries a mandatory alias (enforced by
    // the parser), so this is never actually `None`.
    let Some(alias) = table_ref.alias.as_deref() else {
        return false;
    };

    let mut candidate = conjunct.clone();
    if !rewrite_for_pushdown(&mut candidate, alias, &column_map, require_qualified) {
        return false;
    }

    inner.where_clause = Some(and_exprs(inner.where_clause.take(), candidate));
    true
}

/// Whether `inner`'s own shape rules out moving a filter earlier: a
/// `JOIN` of its own, `DISTINCT`, an aggregate/`GROUP BY`/`HAVING`, a
/// `LIMIT`, or a compound (`UNION`) body would all change which rows a
/// pre-filter leaves behind versus filtering the materialized result.
fn subquery_pushdown_safe(inner: &Select) -> bool {
    if inner.distinct.is_some()
        || inner.having.is_some()
        || !inner.group_by.is_empty()
        || inner.limit.is_some()
        || !inner.compound.is_empty()
        || select_has_aggregate(inner)
    {
        return false;
    }
    inner
        .from
        .as_ref()
        .is_some_and(|from| from.joins.is_empty())
}

pub(super) fn subquery_column_map(inner: &Select) -> Option<ColumnMap> {
    if let [ResultColumn::Star] = inner.columns.as_slice() {
        return Some(ColumnMap::Wildcard);
    }
    let mut out = Vec::with_capacity(inner.columns.len());
    for col in &inner.columns {
        let ResultColumn::Expr { expr, alias } = col else {
            return None;
        };
        let ExprKind::Column {
            catalog: None,
            name,
            ..
        } = &expr.kind
        else {
            return None;
        };
        let output_name = alias.clone().unwrap_or_else(|| name.clone());
        out.push((output_name, expr.clone()));
    }
    Some(ColumnMap::Explicit(out))
}

/// Rewrites `expr` in place so every column reference it makes against
/// `alias` becomes the equivalent reference inside the subquery's own
/// scope, per `column_map`. Returns `false` (leaving `expr` partially,
/// harmlessly mutated — the caller discards it on failure) as soon as any
/// column can't be proven to belong solely to `alias` and be identity-
/// mapped, or a nested subquery expression is found.
fn rewrite_for_pushdown(
    expr: &mut Expr,
    alias: &str,
    column_map: &ColumnMap,
    require_qualified: bool,
) -> bool {
    match &mut expr.kind {
        ExprKind::Literal(_) | ExprKind::Param(_) => true,
        ExprKind::Column {
            table,
            catalog,
            name,
        } => {
            if catalog.is_some() {
                return false;
            }
            let qualifies = match table {
                Some(t) => t.eq_ignore_ascii_case(alias),
                None => !require_qualified,
            };
            if !qualifies {
                return false;
            }
            match column_map {
                ColumnMap::Wildcard => {
                    *table = None;
                    true
                }
                ColumnMap::Explicit(cols) => {
                    let Some((_, underlying)) =
                        cols.iter().find(|(n, _)| n.eq_ignore_ascii_case(name))
                    else {
                        return false;
                    };
                    *expr = underlying.clone();
                    true
                }
            }
        }
        ExprKind::FunctionCall { args, .. } => match args {
            FunctionArgs::Star => true,
            FunctionArgs::List(list) => list
                .iter_mut()
                .all(|e| rewrite_for_pushdown(e, alias, column_map, require_qualified)),
        },
        ExprKind::Unary { expr: inner, .. } => {
            rewrite_for_pushdown(inner, alias, column_map, require_qualified)
        }
        ExprKind::Binary { lhs, rhs, .. } => {
            rewrite_for_pushdown(lhs, alias, column_map, require_qualified)
                && rewrite_for_pushdown(rhs, alias, column_map, require_qualified)
        }
        ExprKind::Is { lhs, rhs, .. } => {
            rewrite_for_pushdown(lhs, alias, column_map, require_qualified)
                && rewrite_for_pushdown(rhs, alias, column_map, require_qualified)
        }
        ExprKind::IsNull { expr: inner, .. } => {
            rewrite_for_pushdown(inner, alias, column_map, require_qualified)
        }
        ExprKind::Between {
            expr: e, lo, hi, ..
        } => {
            rewrite_for_pushdown(e, alias, column_map, require_qualified)
                && rewrite_for_pushdown(lo, alias, column_map, require_qualified)
                && rewrite_for_pushdown(hi, alias, column_map, require_qualified)
        }
        ExprKind::In { expr: e, list, .. } => {
            rewrite_for_pushdown(e, alias, column_map, require_qualified)
                && list
                    .iter_mut()
                    .all(|item| rewrite_for_pushdown(item, alias, column_map, require_qualified))
        }
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            ..
        } => {
            rewrite_for_pushdown(e, alias, column_map, require_qualified)
                && rewrite_for_pushdown(pattern, alias, column_map, require_qualified)
                && match escape {
                    Some(esc) => rewrite_for_pushdown(esc, alias, column_map, require_qualified),
                    None => true,
                }
        }
        ExprKind::Case {
            operand,
            whens,
            else_,
        } => {
            let operand_ok = match operand {
                Some(o) => rewrite_for_pushdown(o, alias, column_map, require_qualified),
                None => true,
            };
            let whens_ok = whens.iter_mut().all(|(w, t)| {
                rewrite_for_pushdown(w, alias, column_map, require_qualified)
                    && rewrite_for_pushdown(t, alias, column_map, require_qualified)
            });
            let else_ok = match else_ {
                Some(e) => rewrite_for_pushdown(e, alias, column_map, require_qualified),
                None => true,
            };
            operand_ok && whens_ok && else_ok
        }
        ExprKind::Cast { expr: e, .. } => {
            rewrite_for_pushdown(e, alias, column_map, require_qualified)
        }
        ExprKind::Collate { expr: e, .. } => {
            rewrite_for_pushdown(e, alias, column_map, require_qualified)
        }
        ExprKind::Paren(inner) => rewrite_for_pushdown(inner, alias, column_map, require_qualified),
        ExprKind::Subquery(_)
        | ExprKind::Exists { .. }
        | ExprKind::InSubquery { .. }
        | ExprKind::InSubqueryMulti { .. } => false,
    }
}

fn and_exprs(existing: Option<Expr>, addition: Expr) -> Expr {
    match existing {
        Some(e) => Expr {
            span: e.span,
            kind: ExprKind::Binary {
                op: BinaryOp::And,
                lhs: Box::new(e),
                rhs: Box::new(addition),
            },
        },
        None => addition,
    }
}

fn rebuild_conjunction(exprs: Vec<Expr>) -> Option<Expr> {
    let mut iter = exprs.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, next| Expr {
        span: acc.span,
        kind: ExprKind::Binary {
            op: BinaryOp::And,
            lhs: Box::new(acc),
            rhs: Box::new(next),
        },
    }))
}

fn recurse_into_from_subqueries(select: &mut Select) {
    let Some(from) = &mut select.from else {
        return;
    };
    if let TableRefKind::Subquery(inner) = &mut from.first.kind {
        push_down_where_predicates(inner);
    }
    for join in &mut from.joins {
        if let TableRefKind::Subquery(inner) = &mut join.table.kind {
            push_down_where_predicates(inner);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::parser::error::{parse_select, ParseOutcome};

    fn parse(sql: &str) -> Select {
        match parse_select(sql) {
            ParseOutcome::Accepted(select) => *select,
            other => panic!("expected Accepted, got {other:?}"),
        }
    }

    fn pushed(sql: &str) -> String {
        let mut select = parse(sql);
        push_down_where_predicates(&mut select);
        select.to_string()
    }

    #[test]
    fn pushes_predicate_through_wildcard_projection() {
        let out = pushed("SELECT * FROM (SELECT a, b FROM t) AS sub WHERE sub.a = 1");
        assert!(out.contains("WHERE a = 1"), "{out}");
    }

    #[test]
    fn pushes_predicate_and_rewrites_aliased_column() {
        let out = pushed("SELECT * FROM (SELECT a, b AS c FROM t) AS sub WHERE sub.c = 1");
        assert!(out.contains("WHERE b = 1"), "{out}");
    }

    #[test]
    fn does_not_push_when_column_unknown_in_explicit_map() {
        let out = pushed("SELECT * FROM (SELECT a FROM t) AS sub WHERE sub.zzz = 1");
        assert!(out.contains("zzz"), "{out}");
    }

    #[test]
    fn does_not_push_when_subquery_has_group_by() {
        let out = pushed(
            "SELECT * FROM (SELECT a, count(*) AS c FROM t GROUP BY a) AS sub WHERE sub.a = 1",
        );
        assert!(out.contains("sub.a = 1"), "{out}");
    }

    #[test]
    fn does_not_push_when_subquery_has_distinct() {
        let out = pushed("SELECT * FROM (SELECT DISTINCT a FROM t) AS sub WHERE sub.a = 1");
        assert!(out.contains("sub.a = 1"), "{out}");
    }

    #[test]
    fn does_not_push_when_subquery_has_limit() {
        let out = pushed("SELECT * FROM (SELECT a FROM t LIMIT 5) AS sub WHERE sub.a = 1");
        assert!(out.contains("sub.a = 1"), "{out}");
    }

    #[test]
    fn does_not_push_when_subquery_is_compound() {
        let out =
            pushed("SELECT * FROM (SELECT a FROM t UNION SELECT a FROM t2) AS sub WHERE sub.a = 1");
        assert!(out.contains("sub.a = 1"), "{out}");
    }

    #[test]
    fn does_not_push_when_subquery_has_own_join() {
        let out = pushed(
            "SELECT * FROM (SELECT t.a FROM t JOIN t2 ON t.a = t2.a) AS sub WHERE sub.a = 1",
        );
        assert!(out.contains("sub.a = 1"), "{out}");
    }

    #[test]
    fn does_not_push_computed_projection() {
        let out = pushed("SELECT * FROM (SELECT a + 1 AS c FROM t) AS sub WHERE sub.c = 1");
        assert!(out.contains("sub.c = 1"), "{out}");
    }

    #[test]
    fn does_not_push_catalog_qualified_column() {
        let out = pushed("SELECT * FROM (SELECT a FROM t) AS sub WHERE main.sub.a = 1");
        assert!(out.contains("main.sub.a = 1"), "{out}");
    }

    #[test]
    fn requires_qualification_when_join_present() {
        let out = pushed("SELECT * FROM (SELECT a FROM t) AS sub JOIN t2 ON t2.x = 1 WHERE a = 1");
        assert!(out.contains("WHERE a = 1"), "{out}");
    }

    #[test]
    fn allows_unqualified_column_without_join() {
        let out = pushed("SELECT * FROM (SELECT a FROM t) AS sub WHERE a = 1");
        assert!(out.contains("(SELECT a FROM t WHERE a = 1)"), "{out}");
    }

    #[test]
    fn does_not_push_nested_subquery_expression() {
        let out =
            pushed("SELECT * FROM (SELECT a FROM t) AS sub WHERE sub.a IN (SELECT x FROM t2)");
        assert!(out.contains("sub.a IN"), "{out}");
    }

    #[test]
    fn pushes_into_second_join_table() {
        let out = pushed(
            "SELECT * FROM t0 JOIN (SELECT a FROM t) AS sub ON t0.x = sub.a WHERE sub.a = 1",
        );
        assert!(out.contains("(SELECT a FROM t WHERE a = 1)"), "{out}");
    }

    #[test]
    fn splits_conjuncts_pushing_only_the_movable_one() {
        let out = pushed("SELECT * FROM (SELECT a FROM t) AS sub WHERE sub.a = 1 AND sub.zzz = 1");
        assert!(out.contains("t WHERE a = 1"), "{out}");
        assert!(out.contains("sub.zzz = 1"), "{out}");
    }

    #[test]
    fn rewrite_for_pushdown_covers_every_expr_kind() {
        let out = pushed(
            "SELECT * FROM (SELECT a FROM t) AS sub WHERE \
             CASE WHEN sub.a BETWEEN 1 AND 10 THEN sub.a IN (1, 2) ELSE sub.a LIKE 'x' END \
               AND CAST(sub.a AS INTEGER) IS NULL \
               AND (-sub.a) IS NOT 1 \
               AND (sub.a) COLLATE NOCASE = 1 \
               AND foo(sub.a)",
        );
        assert!(out.contains("FROM t WHERE"), "{out}");
        assert!(!out.contains("sub.a"), "{out}");
    }

    #[test]
    fn does_not_push_like_escape_referencing_unmapped_column() {
        let out =
            pushed("SELECT * FROM (SELECT a FROM t) AS sub WHERE sub.a LIKE 'x' ESCAPE sub.zzz");
        assert!(out.contains("zzz"), "{out}");
    }

    #[test]
    fn does_not_push_exists_or_scalar_subquery_conjunct() {
        let out = pushed(
            "SELECT * FROM (SELECT a FROM t) AS sub WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.x = sub.a)",
        );
        assert!(out.contains("EXISTS"), "{out}");

        let out = pushed("SELECT * FROM (SELECT a FROM t) AS sub WHERE (SELECT x FROM t2) = sub.a");
        assert!(out.contains("sub.a"), "{out}");
    }

    #[test]
    fn does_not_push_in_subquery_multi_conjunct() {
        let out = pushed(
            "SELECT * FROM (SELECT a FROM t) AS sub WHERE (sub.a, 1) IN (SELECT x, y FROM t2)",
        );
        assert!(out.contains("sub.a"), "{out}");
    }

    #[test]
    fn recurses_into_nested_subqueries() {
        let out = pushed(
            "SELECT * FROM (SELECT * FROM (SELECT a FROM t) AS inner1) AS outer1 WHERE outer1.a = 1",
        );
        assert!(out.contains("(SELECT a FROM t WHERE a = 1)"), "{out}");
    }
}
