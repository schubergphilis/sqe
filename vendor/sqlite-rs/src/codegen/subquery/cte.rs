// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Non-recursive `WITH`-clause materialization (#376). Rather than
//! teaching codegen a second table-materialization path, this rewrites
//! a `WITH` clause away *before* codegen ever sees it: every `FROM`/
//! `JOIN` table reference (in the main query, and in a later CTE's own
//! body) that names an earlier-or-current CTE becomes a
//! `TableRefKind::Subquery` wrapping that CTE's `query` — exactly the
//! shape #257's `FROM`-subquery-in-derived-table machinery
//! (`materialize_from_subquery`) already materializes into an
//! ephemeral table and scans like any other table. A CTE name shadows
//! a same-named real table for the scope of the one `SELECT` that
//! declared it, without touching the catalog: the substitution is
//! purely a local AST rewrite, so it can never leak into a sibling
//! statement.
//!
//! `WITH RECURSIVE` is rejected by the parser already (#375) — nothing
//! here needs to guard against it.

use std::borrow::Cow;

use crate::parser::ast::{CommonTableExpr, ResultColumn, Select, TableRef, TableRefKind};

/// Rewrites away `select.with_clause`, if any — see the module doc.
/// A `Select` with no `WITH` clause is returned as `Cow::Borrowed`
/// rather than a full AST clone — the common case for a query with no
/// `WITH` clause at all (#590 item 6).
pub fn expand_with_clause(select: &Select) -> Cow<'_, Select> {
    let Some(with) = &select.with_clause else {
        return Cow::Borrowed(select);
    };

    // Each CTE is resolved in declaration order, against every CTE
    // declared before it (SQLite's non-recursive `WITH` visibility
    // rule) — `resolved` accumulates the already-rewritten definitions
    // so a later CTE (or the main query) referencing an earlier one
    // picks up its fully-substituted body.
    let mut resolved: Vec<CommonTableExpr> = Vec::with_capacity(with.ctes.len());
    for cte in &with.ctes {
        let mut query = (*cte.query).clone();
        substitute_cte_refs(&mut query, &resolved);
        if let Some(columns) = &cte.columns {
            apply_column_aliases(&mut query, columns);
        }
        resolved.push(CommonTableExpr {
            name: cte.name.clone(),
            columns: cte.columns.clone(),
            query: Box::new(query),
            span: cte.span,
        });
    }

    let mut out = select.clone();
    out.with_clause = None;
    substitute_cte_refs(&mut out, &resolved);
    Cow::Owned(out)
}

/// Substitutes every `FROM`/`JOIN` table reference in `select`'s own
/// main `FROM` clause and each `UNION ALL` compound arm's `FROM` clause
/// that names one of `ctes` with a `TableRefKind::Subquery` wrapping
/// that CTE's query. Does not recurse into subquery *expressions*
/// (scalar/`IN`/`EXISTS`) — a CTE is only visible in `FROM`/`JOIN`
/// position in this pass, matching how #257's subquery-in-FROM support
/// is itself scoped.
fn substitute_cte_refs(select: &mut Select, ctes: &[CommonTableExpr]) {
    if let Some(from) = &mut select.from {
        substitute_table_ref(&mut from.first, ctes);
        for join in &mut from.joins {
            substitute_table_ref(&mut join.table, ctes);
        }
    }
    for arm in &mut select.compound {
        if let Some(from) = &mut arm.from {
            substitute_table_ref(&mut from.first, ctes);
            for join in &mut from.joins {
                substitute_table_ref(&mut join.table, ctes);
            }
        }
    }
}

fn substitute_table_ref(table_ref: &mut TableRef, ctes: &[CommonTableExpr]) {
    match &mut table_ref.kind {
        TableRefKind::Name(name) => {
            let Some(cte) = ctes.iter().find(|c| c.name.eq_ignore_ascii_case(name)) else {
                return;
            };
            // A subquery-in-FROM's alias is mandatory to the rest of
            // codegen (`resolve_from_table_schema`'s doc comment) —
            // default it to the CTE's own name when the reference
            // didn't supply one itself (the common `FROM cte_name`
            // case, as opposed to `FROM cte_name AS c`).
            let alias = table_ref.alias.clone().or_else(|| Some(cte.name.clone()));
            table_ref.kind = TableRefKind::Subquery(cte.query.clone());
            table_ref.alias = alias;
        }
        // An inline derived table (`FROM (SELECT ... FROM cte_name) sub`)
        // can itself reference an earlier-declared CTE in its own FROM —
        // recurse into it the same way view expansion does, so a CTE
        // isn't only resolvable directly under a query's top-level FROM.
        TableRefKind::Subquery(inner) => substitute_cte_refs(inner, ctes),
    }
}

/// Renames a CTE's own result columns to its explicit `(col, ...)` list
/// by giving each result column an explicit alias — the synthetic
/// schema #257's `subquery_output_columns` builds for a materialized
/// `FROM`-subquery already prefers an explicit alias over any name it
/// would otherwise derive, so this is the only hook needed to honor
/// `WITH cte(a, b) AS (...)`. Only a same-length, all-`Expr` (no `*`/
/// `table.*`) result-column list can be renamed positionally; anything
/// else is left alone (the CTE still compiles, just exposed under its
/// query's own natural column names instead of the declared list).
pub(super) fn apply_column_aliases(query: &mut Select, columns: &[String]) {
    if query.columns.len() != columns.len() {
        return;
    }
    if query
        .columns
        .iter()
        .any(|c| !matches!(c, ResultColumn::Expr { .. }))
    {
        return;
    }
    for (col, name) in query.columns.iter_mut().zip(columns) {
        if let ResultColumn::Expr { alias, .. } = col {
            *alias = Some(name.clone());
        }
    }
}
