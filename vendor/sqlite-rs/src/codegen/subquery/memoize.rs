// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! #314's per-probe-value memoization cache for correlated scalar
//! subqueries — see `super`'s module doc.

use std::collections::HashMap;

use super::correlation::{is_comparison_op, subquery_is_correlated, top_level_and_conjuncts};
use super::from_clause::resolve_subquery_schema;
use super::scalar::compile_scalar_subquery;
use super::{select_id, MemoizedSubquery};
use crate::codegen::select::CodegenError;
use crate::codegen::{Emitter, RegAlloc, Scope};
use crate::parser::ast::{Expr, ExprKind, FunctionArgs, ResultColumn, Select};
use crate::schema::TableSchema;
use crate::vdbe::{Instruction, Opcode, P4};

/// Walks `expr` collecting the single outer column `subselect` (whose
/// own schema is `own_schema`) is correlated against, mirroring
/// `correlation::walk_expr_for_correlation`'s traversal shape but
/// gathering an identifier instead of a bare bool. Sets `*ambiguous`
/// (and gives up) on: a reference that resolves to neither
/// `own_schema` nor `outer_schema` (out of this pass's
/// single-table-correlation scope entirely — e.g. a deeper
/// `outer.outer` reference), a *second* distinct outer column (the
/// memoization cache below only has room for one probe value), or any
/// nested subquery-bearing expression (conservatively out of scope,
/// same as the #306 correlation check).
#[allow(clippy::too_many_arguments)]
fn collect_correlated_column(
    expr: &Expr,
    own_schema: &TableSchema,
    own_qualifiers: &[&str],
    outer_schema: &TableSchema,
    found: &mut Option<String>,
    ambiguous: &mut bool,
) {
    if *ambiguous {
        return;
    }
    match &expr.kind {
        ExprKind::Column { table, name, .. } => {
            let qualifier_ok = match table {
                Some(t) => own_qualifiers.iter().any(|q| q.eq_ignore_ascii_case(t)),
                None => true,
            };
            let is_own = qualifier_ok
                && own_schema
                    .columns
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(name));
            if is_own {
                return;
            }
            if !outer_schema
                .columns
                .iter()
                .any(|c| c.eq_ignore_ascii_case(name))
            {
                *ambiguous = true;
                return;
            }
            match found {
                Some(existing) if existing.eq_ignore_ascii_case(name) => {}
                Some(_) => *ambiguous = true,
                None => *found = Some(name.clone()),
            }
        }
        ExprKind::Literal(_) | ExprKind::Param(_) => {}
        ExprKind::FunctionCall { args, .. } => {
            if let FunctionArgs::List(list) = args {
                for a in list {
                    collect_correlated_column(
                        a,
                        own_schema,
                        own_qualifiers,
                        outer_schema,
                        found,
                        ambiguous,
                    );
                }
            }
        }
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. }
        | ExprKind::Collate { expr: e, .. }
        | ExprKind::Paren(e) => collect_correlated_column(
            e,
            own_schema,
            own_qualifiers,
            outer_schema,
            found,
            ambiguous,
        ),
        ExprKind::Binary { lhs, rhs, .. } | ExprKind::Is { lhs, rhs, .. } => {
            collect_correlated_column(
                lhs,
                own_schema,
                own_qualifiers,
                outer_schema,
                found,
                ambiguous,
            );
            collect_correlated_column(
                rhs,
                own_schema,
                own_qualifiers,
                outer_schema,
                found,
                ambiguous,
            );
        }
        ExprKind::Between {
            expr: e, lo, hi, ..
        } => {
            collect_correlated_column(
                e,
                own_schema,
                own_qualifiers,
                outer_schema,
                found,
                ambiguous,
            );
            collect_correlated_column(
                lo,
                own_schema,
                own_qualifiers,
                outer_schema,
                found,
                ambiguous,
            );
            collect_correlated_column(
                hi,
                own_schema,
                own_qualifiers,
                outer_schema,
                found,
                ambiguous,
            );
        }
        ExprKind::In { expr: e, list, .. } => {
            collect_correlated_column(
                e,
                own_schema,
                own_qualifiers,
                outer_schema,
                found,
                ambiguous,
            );
            for item in list {
                collect_correlated_column(
                    item,
                    own_schema,
                    own_qualifiers,
                    outer_schema,
                    found,
                    ambiguous,
                );
            }
        }
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            ..
        } => {
            collect_correlated_column(
                e,
                own_schema,
                own_qualifiers,
                outer_schema,
                found,
                ambiguous,
            );
            collect_correlated_column(
                pattern,
                own_schema,
                own_qualifiers,
                outer_schema,
                found,
                ambiguous,
            );
            if let Some(esc) = escape {
                collect_correlated_column(
                    esc,
                    own_schema,
                    own_qualifiers,
                    outer_schema,
                    found,
                    ambiguous,
                );
            }
        }
        ExprKind::Case {
            operand,
            whens,
            else_,
        } => {
            if let Some(o) = operand {
                collect_correlated_column(
                    o,
                    own_schema,
                    own_qualifiers,
                    outer_schema,
                    found,
                    ambiguous,
                );
            }
            for (w, t) in whens {
                collect_correlated_column(
                    w,
                    own_schema,
                    own_qualifiers,
                    outer_schema,
                    found,
                    ambiguous,
                );
                collect_correlated_column(
                    t,
                    own_schema,
                    own_qualifiers,
                    outer_schema,
                    found,
                    ambiguous,
                );
            }
            if let Some(e) = else_ {
                collect_correlated_column(
                    e,
                    own_schema,
                    own_qualifiers,
                    outer_schema,
                    found,
                    ambiguous,
                );
            }
        }
        ExprKind::Subquery(_)
        | ExprKind::Exists { .. }
        | ExprKind::InSubquery { .. }
        | ExprKind::InSubqueryMulti { .. } => {
            *ambiguous = true;
        }
    }
}

/// Whether `subselect` (whose own schema is `own_schema`) is correlated
/// against exactly one column of `outer_schema` — the shape #314's
/// memoization cache needs a single probe value for. `None` if zero
/// columns, more than one distinct column, or anything
/// [`collect_correlated_column`] can't reason about.
fn single_correlated_outer_column(
    subselect: &Select,
    own_schema: &TableSchema,
    outer_schema: &TableSchema,
) -> Option<String> {
    let own_qualifiers: Vec<&str> = std::iter::once(own_schema.name.as_str())
        .chain(
            subselect
                .from
                .as_ref()
                .and_then(|f| f.first.alias.as_deref()),
        )
        .collect();
    let mut found = None;
    let mut ambiguous = false;
    if let Some(where_expr) = &subselect.where_clause {
        collect_correlated_column(
            where_expr,
            own_schema,
            &own_qualifiers,
            outer_schema,
            &mut found,
            &mut ambiguous,
        );
    }
    for col in &subselect.columns {
        if let ResultColumn::Expr { expr, .. } = col {
            collect_correlated_column(
                expr,
                own_schema,
                &own_qualifiers,
                outer_schema,
                &mut found,
                &mut ambiguous,
            );
        }
    }
    if ambiguous {
        None
    } else {
        found
    }
}

/// Whether `subquery` is a candidate for #314's memoization cache: it
/// has a (non-joined, single-table) `FROM` this pass can resolve, it
/// *is* correlated (#306's hoist already handles the uncorrelated
/// case), and it's correlated against exactly one column of
/// `outer_schema`.
fn subquery_memoizable(
    subquery: &Select,
    outer_schema: &TableSchema,
    outer_scope: &Scope,
) -> Option<String> {
    let resolved = resolve_subquery_schema(subquery, &outer_scope.catalog).ok()??;
    if !subquery_is_correlated(subquery, Some(&resolved)) {
        return None;
    }
    single_correlated_outer_column(subquery, &resolved, outer_schema)
}

/// Recognizes one memoizable conjunct: a top-level comparison with a
/// correlated (single-outer-column) scalar-subquery operand. Emits the
/// subquery's cache index (`OpenEphemeral`, index mode, empty at this
/// point — populated lazily, per distinct probe value, by
/// [`compile_memoized_scalar_subquery`]) and returns its
/// [`select_id`] plus the [`MemoizedSubquery`] handle, or `None` if
/// `conjunct` doesn't match this shape.
fn try_memoize_conjunct(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    outer_scope: &Scope,
    outer_schema: &TableSchema,
    conjunct: &Expr,
) -> Option<(usize, MemoizedSubquery)> {
    let ExprKind::Binary { op, lhs, rhs } = &conjunct.kind else {
        return None;
    };
    if !is_comparison_op(*op) {
        return None;
    }
    for side in [lhs.as_ref(), rhs.as_ref()] {
        if let ExprKind::Subquery(subquery) = &side.kind {
            if let Some(probe_column) = subquery_memoizable(subquery, outer_schema, outer_scope) {
                let cache_cursor = reg.alloc_cursor();
                em.emit(Instruction {
                    opcode: Opcode::OpenEphemeral,
                    p1: cache_cursor,
                    p2: 0,
                    p3: 0,
                    p4: P4::None,
                    p5: 0,
                });
                return Some((
                    select_id(subquery),
                    MemoizedSubquery {
                        cache_cursor,
                        probe_column,
                    },
                ));
            }
        }
    }
    None
}

/// Sets up #314's per-probe-value memoization cache for every
/// correlated, single-outer-column scalar subquery found as a top-level
/// `WHERE`-clause conjunct, mirroring
/// `correlation::hoist_uncorrelated_where_subqueries`'s structure and
/// scope boundary (single-table `WHERE`-clause scans only; a joined
/// query's `WHERE` clause is not attempted). Returns a map meant to be
/// attached to the scan's own [`Scope`] via [`Scope::with_memoized`] —
/// [`crate::codegen::expr::compile_value`]'s `Subquery` dispatch then
/// routes through [`compile_memoized_scalar_subquery`] instead of
/// unconditionally re-running the subquery every row.
///
/// Deliberately scalar-only: an `IN (SELECT ...)` subquery's per-probe-
/// value "result" is a whole membership set, not a single value, which
/// would need a cache of ephemeral indexes rather than a cache of
/// scalars — a larger follow-up left for a future ticket rather than
/// folded into this one's narrower scope.
pub(crate) fn memoize_correlated_where_subqueries(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    outer_scope: &Scope,
    outer_schema: &TableSchema,
    where_clause: &Expr,
) -> HashMap<usize, MemoizedSubquery> {
    let mut out = HashMap::new();
    for conjunct in top_level_and_conjuncts(where_clause) {
        if let Some((key, memo)) =
            try_memoize_conjunct(em, reg, outer_scope, outer_schema, conjunct)
        {
            out.insert(key, memo);
        }
    }
    out
}

/// Compiles a memoized correlated scalar subquery (#314, index-mode
/// cache per #494): reads the current outer row's `memo.probe_column`
/// value, probes `memo.cache_cursor` — an ephemeral index keyed on the
/// probe value alone (`Found`, O(log n) via the ephemeral cursor's
/// `BTreeMap`, not a per-row linear scan) — and on a hit reads the
/// cached result straight back out (`Column`), skipping
/// [`compile_scalar_subquery`]'s whole inner scan entirely. On a miss
/// (including every NULL probe value, which never caches — SQL's
/// `NULL = NULL` is unknown, not true), runs the subquery normally and,
/// for a non-NULL probe, inserts `(probe, result)` into the cache
/// (`IdxInsert`, keyed on just the probe register via `P4`, with the
/// result register as `P5`'s extra payload — see `idx_insert`'s doc) for
/// the next outer row with the same value to hit.
///
/// No entry-count cap: an ephemeral cursor's own `MAX_EPHEMERAL_ROWS`
/// ceiling (`src/vdbe/cursor.rs`) is the only limit, since a lookup's
/// cost no longer grows with the cache's size the way the old
/// linear-scan table cache's did.
///
/// The cache-hit comparison is a plain, uncollated `Eq` (byte-exact
/// record encoding) — never a false positive (an actual SQL-distinct
/// pair of values is never judged equal by a *stricter* byte-exact
/// comparison), so correctness is preserved even for a `NOCASE`-collated
/// probe column; the only cost is a few avoidable cache misses in that
/// case.
pub(crate) fn compile_memoized_scalar_subquery(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    outer_scope: &Scope,
    subselect: &Select,
    memo: &MemoizedSubquery,
) -> Result<i32, CodegenError> {
    let binding = outer_scope
        .tables
        .first()
        .ok_or_else(|| CodegenError::Unsupported {
            reason: "memoized correlated subquery has no outer table binding".to_string(),
        })?;
    let col_idx = crate::codegen::expr::column_index(&binding.schema, &memo.probe_column)
        .ok_or_else(|| CodegenError::Unsupported {
            reason: format!(
                "memoized correlated subquery's probe column {:?} not found on the outer table",
                memo.probe_column
            ),
        })?;

    let dest = reg.alloc();
    let probe_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::Column,
        binding.cursor,
        i32::try_from(col_idx).unwrap_or(0),
        probe_reg,
    ));

    let end_label = em.new_label();
    let null_probe_label = em.new_label();
    let miss_label = em.new_label();
    let hit_label = em.new_label();

    let null_addr = em.emit(Instruction::new(Opcode::IsNull, probe_reg, 0, 0));
    em.patch_p2(null_addr, null_probe_label);

    let found_addr = em.emit(Instruction::with_p4(
        Opcode::Found,
        memo.cache_cursor,
        0,
        probe_reg,
        P4::Int(1),
    ));
    em.patch_p2(found_addr, hit_label);
    em.goto(miss_label);

    em.place(hit_label);
    em.emit(Instruction::new(Opcode::Column, memo.cache_cursor, 1, dest));
    em.goto(end_label);

    em.place(miss_label);
    let fresh = compile_scalar_subquery(em, reg, outer_scope, subselect)?;
    em.emit(Instruction::new(Opcode::Copy, fresh, dest, 0));
    let key_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Copy, probe_reg, key_reg, 0));
    let val_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Copy, dest, val_reg, 0));
    em.emit(Instruction {
        opcode: Opcode::IdxInsert,
        p1: memo.cache_cursor,
        p2: key_reg,
        p3: 0,
        p4: P4::Int(1),
        p5: 1,
    });
    em.goto(end_label);

    em.place(null_probe_label);
    let fresh_null = compile_scalar_subquery(em, reg, outer_scope, subselect)?;
    em.emit(Instruction::new(Opcode::Copy, fresh_null, dest, 0));

    em.place(end_label);
    Ok(dest)
}
