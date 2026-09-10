// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::joins::LevelPlan;
use super::limit_scan::{
    compile_limit_setup, emit_limit_guard, emit_offset_guard, is_rowid_reference,
    top_level_equality_operands,
};
use super::order_by::strip_collate;
use super::*;
use crate::planner::{estimate_index_cost, estimate_scan_cost};
/// The access strategy #243's join-level planner picked for a table
/// binding, in place of an unconditional `Rewind`/`Next` full scan --
/// see [`choose_join_access`].
pub(in crate::codegen) enum JoinAccess {
    /// The `ON` equality's other side is a rowid reference (the
    /// `rowid`/`_rowid_`/`oid` keywords, or the table's `INTEGER PRIMARY
    /// KEY` alias column): a `SeekRowid` point lookup, generalizing
    /// [`try_compile_rowid_seek`] to a join's inner table.
    Rowid(Expr),
    /// The `ON` equality's other side is a column with a single-column
    /// `UNIQUE` index: a `SeekIndexEq` + `IdxRowid` + `SeekRowid` point
    /// lookup (#243).
    UniqueIndex { index: IndexSchema, operand: Expr },
}

/// Whether `table`/`name` (a `Column` expression's qualifier and column
/// name) names a column of `binding`'s own schema -- used by
/// [`choose_join_access`] to tell which side of a join's `=` belongs to
/// the table currently being brought into the loop.
pub(super) fn column_belongs_to_binding(
    binding: &TableBinding,
    table: Option<&str>,
    name: &str,
) -> bool {
    if let Some(table) = table {
        if !binding.matches_qualifier(table) {
            return false;
        }
    }
    column_index(&binding.schema, name).is_some()
}

/// Whether every `Column` reference inside `expr` resolves only to one
/// of `prior_bindings` (already-positioned tables earlier in the join
/// order) -- the safety condition for using `expr` as a join-level index
/// seek's probe value, since it compiles against cursors that must
/// already hold a row. Deliberately narrow: only a bare/qualified
/// column reference or a literal/parameter are recognized; anything
/// else (a sub-expression, a function call) is rejected rather than
/// risk treating a reference to the *current* or a *not-yet-bound*
/// table as safe.
pub(super) fn expr_is_safe_join_probe(expr: &Expr, prior_bindings: &[TableBinding]) -> bool {
    match &expr.kind {
        ExprKind::Literal(_) | ExprKind::Param(_) => true,
        ExprKind::Column { table, name, .. } => prior_bindings
            .iter()
            .any(|b| column_belongs_to_binding(b, table.as_deref(), name)),
        _ => false,
    }
}

/// Picks an index/rowid seek for join level `level` out of `on_expr` (a
/// single top-level `binding.column = <safe probe>` equality -- anything
/// else, including `AND`-compound `ON` clauses, falls back to the
/// ordinary `Rewind`/`Next` scan per #243's bounded scope, matching
/// [`try_compile_rowid_seek`]'s own narrowness), or `None` to keep the
/// full scan. Only a `UNIQUE` single-column index (or the rowid) is
/// considered -- a non-unique index could match more than one row, which
/// this seek-once codegen shape can't express (see the module doc's
/// LEFT JOIN "matched" flag: it assumes at most one inner-side match).
///
/// #461/spec 011/Req 4: a structurally-available `UniqueIndex` pick is
/// additionally vetoed back to `None` (full scan) when `binding.stats`
/// (`ANALYZE` data, #461) says that index's `avg_eq` is no cheaper than
/// a full scan -- e.g. a `UNIQUE` index over mostly-NULL/skewed data.
/// `binding.stats` is empty whenever no caller has threaded real
/// `planner::load_stats` output through this codegen path (including
/// every call before #461), and an empty `Stats`' cost estimates are
/// both `u64::MAX` for scan and index alike, so the veto condition
/// (`index cost > scan cost`) is never true and the structural pick
/// survives unchanged -- this is what keeps every pre-#461 caller (and
/// any caller that hasn't wired live stats through yet) byte-for-byte
/// unaffected. A `Rowid` pick is never vetoed: a rowid seek is O(1)
/// regardless of statistics, matching real SQLite's own unconditional
/// preference for it.
pub(in crate::codegen) fn choose_join_access(
    binding: &TableBinding,
    on_expr: &Expr,
    prior_bindings: &[TableBinding],
) -> Option<JoinAccess> {
    let (lhs, rhs) = top_level_equality_operands(on_expr)?;
    let (this_side, other_side) = if matches!(&lhs.kind, ExprKind::Column { table, name, .. } if column_belongs_to_binding(binding, table.as_deref(), name))
    {
        (lhs, rhs)
    } else if matches!(&rhs.kind, ExprKind::Column { table, name, .. } if column_belongs_to_binding(binding, table.as_deref(), name))
    {
        (rhs, lhs)
    } else {
        return None;
    };
    if !expr_is_safe_join_probe(other_side, prior_bindings) {
        return None;
    }
    if is_rowid_reference(&binding.schema, this_side) {
        return Some(JoinAccess::Rowid(other_side.clone()));
    }
    let ExprKind::Column { name, .. } = &this_side.kind else {
        return None;
    };
    let index = binding.schema.indexes.iter().find(|idx| {
        idx.unique
            && idx.columns.len() == 1
            && idx
                .columns
                .first()
                .is_some_and(|c| c.name.eq_ignore_ascii_case(name))
    })?;
    let index_cost = estimate_index_cost(&index.name, &binding.stats);
    let scan_cost = estimate_scan_cost(&binding.stats);
    if index_cost.estimated_rows > scan_cost.estimated_rows {
        return None;
    }
    Some(JoinAccess::UniqueIndex {
        index: index.clone(),
        operand: other_side.clone(),
    })
}

/// #545: a transient automatic index for a join level whose nested-loop
/// scan [`choose_join_access`] couldn't turn into a seek against a real
/// index — see [`choose_auto_index_probe`].
pub(in crate::codegen) struct AutoIndexProbe {
    /// The expression to seek the ephemeral index with, each time this
    /// level's join loop would otherwise start (`prior_bindings` only —
    /// see [`expr_is_safe_join_probe`]).
    pub(in crate::codegen) probe: Expr,
    /// `binding`'s own column the ephemeral index is built from (one
    /// `IdxInsert` per row, in a one-time pre-pass over `binding`'s
    /// cursor, keyed on this column plus the row's rowid).
    pub(in crate::codegen) key_column: usize,
}

/// Picks a transient automatic-index build for join level `binding`
/// (#545, sqlite.org/optoverview.html#autoindex) once
/// [`choose_join_access`] has already ruled out a structural seek
/// against a real index — same top-level-equality shape (a single
/// `binding.column = <safe probe>` `ON` clause) as that function,
/// additionally gated on
/// [`crate::planner::is_automatic_index_worthwhile`] judging `binding`'s
/// `ANALYZE` stats large enough that building the transient index once
/// beats repeatedly nested-loop-scanning the table — no stats at all,
/// or too few rows, and this returns `None` (same "stats-free database
/// is byte-for-byte unaffected" guarantee [`choose_join_access`] makes).
pub(in crate::codegen) fn choose_auto_index_probe(
    binding: &TableBinding,
    on_expr: &Expr,
    prior_bindings: &[TableBinding],
) -> Option<AutoIndexProbe> {
    if !crate::planner::is_automatic_index_worthwhile(&binding.stats) {
        return None;
    }
    let (lhs, rhs) = top_level_equality_operands(on_expr)?;
    let (this_side, other_side) = if matches!(&lhs.kind, ExprKind::Column { table, name, .. } if column_belongs_to_binding(binding, table.as_deref(), name))
    {
        (lhs, rhs)
    } else if matches!(&rhs.kind, ExprKind::Column { table, name, .. } if column_belongs_to_binding(binding, table.as_deref(), name))
    {
        (rhs, lhs)
    } else {
        return None;
    };
    if !expr_is_safe_join_probe(other_side, prior_bindings) {
        return None;
    }
    let ExprKind::Column { name, .. } = &this_side.kind else {
        return None;
    };
    let key_column = column_index(&binding.schema, name)?;
    Some(AutoIndexProbe {
        probe: other_side.clone(),
        key_column,
    })
}

/// Projects `select`'s result columns against `scope` (a join-aware
/// counterpart to `emit_row_via_sink`/`compile_row_values`: `*`/
/// `table.*` expand across every binding in `scope`, in FROM order,
/// rather than a single schema's columns) into a contiguous register
/// run, then hands `(first, count)` to `sink`.
pub(super) fn emit_join_row<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    scope: &Scope,
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    let mut regs = Vec::new();
    for col in &select.columns {
        match col {
            ResultColumn::Star => {
                for (i, binding) in scope.tables.iter().enumerate() {
                    let suppressed = scope.dedup_star.get(i);
                    for idx in 0..binding.schema.columns.len() {
                        let Some(name) = binding.schema.columns.get(idx) else {
                            continue;
                        };
                        if suppressed.is_some_and(|s| s.contains(&name.to_ascii_lowercase())) {
                            continue;
                        }
                        regs.push(emit_join_column(em, reg, binding, idx)?);
                    }
                }
            }
            ResultColumn::TableStar { table } => {
                let binding = scope
                    .tables
                    .iter()
                    .find(|b| b.matches_qualifier(table))
                    .ok_or_else(|| CodegenError::UnknownColumn {
                        name: format!("{table}.*"),
                    })?;
                for idx in 0..binding.schema.columns.len() {
                    regs.push(emit_join_column(em, reg, binding, idx)?);
                }
            }
            ResultColumn::Expr { expr, .. } => {
                regs.push(compile_value(em, reg, scope, expr)?);
            }
        }
    }
    let Some(&first) = regs.first() else {
        let r = reg.alloc();
        return sink(em, reg, r, 0);
    };
    for (i, r) in regs.iter().enumerate() {
        let want = first.saturating_add(i32::try_from(i).unwrap_or(i32::MAX));
        if *r != want {
            return Err(CodegenError::Unsupported {
                reason: "result columns must land in contiguous registers for MakeRecord/\
                         ResultRow (a function call or other multi-register expression mixed \
                         with other columns is not yet supported)"
                    .to_string(),
            });
        }
    }
    sink(em, reg, first, i32::try_from(regs.len()).unwrap_or(0))
}

/// Reads one `*`/`table.*`-expanded column of a joined table: NULL
/// when that binding is null-extended (LEFT JOIN's no-match branch),
/// otherwise the same `emit_column_read` every other column read in
/// this crate goes through (rowid-alias-aware, etc.).
pub(super) fn emit_join_column(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    binding: &TableBinding,
    idx: usize,
) -> Result<i32, CodegenError> {
    let r = reg.alloc();
    if binding.forced_null {
        em.emit(Instruction::new(Opcode::Null, 0, r, 0));
    } else {
        emit_column_read(em, &binding.schema, binding.cursor, idx, r)?;
    }
    Ok(r)
}

/// The absolute column offset of `scope.tables[binding_idx]`'s own
/// column block within the flat, all-tables-concatenated row
/// [`compile_joined_sorted_scan`]'s pass 1 buffers into the sorter (every
/// binding's *full* schema column set, in `scope.tables` order, `*`-dedup
/// notwithstanding — the sorter's row is the ORDER BY plan's raw
/// material, not the final projection).
pub(super) fn joined_column_offset(scope: &Scope, binding_idx: usize) -> usize {
    scope
        .tables
        .get(..binding_idx)
        .unwrap_or(&[])
        .iter()
        .map(|b| b.schema.columns.len())
        .sum()
}

/// Resolves `table`/`name` (a bare, possibly-qualified column reference)
/// to `(binding_idx, local_idx)` against `scope.tables` — delegates the
/// qualifier-match/ambiguity rule itself to
/// [`Scope::resolve_own_binding_index`] (the same logic
/// [`Scope::resolve`] uses), only adding the binding-local column index
/// [`Scope::resolve`] doesn't need (its `cursor`-based callers read
/// through a per-binding cursor instead of an absolute offset).
pub(super) fn resolve_scope_column(
    scope: &Scope,
    table: Option<&str>,
    name: &str,
) -> Result<(usize, usize), CodegenError> {
    let i = scope.resolve_own_binding_index(table, name)?;
    let idx = scope
        .tables
        .get(i)
        .and_then(|b| column_index(&b.schema, name))
        .unwrap_or(0);
    Ok((i, idx))
}

/// Where a joined `ORDER BY` term's sort key comes from: a raw column
/// already present in the flat, all-bindings-concatenated row
/// [`compile_joined_sorted_scan`]'s pass 1 buffers (an absolute offset,
/// per [`joined_column_offset`]), or a genuine expression evaluated
/// against the *live* join scope during pass 1 and appended as a
/// trailing sort-only field — mirrors [`OrderByTarget`] for the
/// single-table case.
#[derive(Debug, Clone)]
pub(super) enum JoinOrderTarget {
    Offset(usize),
    Expr(Expr),
}

pub(super) struct JoinOrderPlan {
    target: JoinOrderTarget,
    descending: bool,
    collation: Collation,
    nulls_first: bool,
}

impl JoinOrderPlan {
    /// #333's `GROUP BY`+JOIN pass 1 needs to build a `JoinOrderPlan`
    /// list from scratch (a `GROUP BY` key sorted ascending, not an
    /// actual `ORDER BY` clause) to drive the shared
    /// [`compile_join_level_for_sort`] traversal — this constructor is
    /// that module's only way in, since the fields above stay private
    /// to keep [`resolve_join_order_by`] the sole source of truth for
    /// an actual `ORDER BY` clause's own plans.
    pub(super) fn ascending_offset(offset: usize, collation: Collation) -> Self {
        JoinOrderPlan {
            target: JoinOrderTarget::Offset(offset),
            descending: false,
            collation,
            nulls_first: true,
        }
    }
}

/// [`resolve_order_by`]'s joined counterpart: resolves each `ORDER BY`
/// term against the full-join `scope` instead of a single schema. Only
/// a bare (optionally table-qualified) column or a result-column alias
/// resolves to [`JoinOrderTarget::Offset`]; anything else (including a
/// `SELECT *`-relative ordinal — not supported for the joined case,
/// narrower than the single-table path) becomes a computed
/// [`JoinOrderTarget::Expr`], evaluated once per candidate row during
/// pass 1.
pub(super) fn resolve_join_order_by(
    select: &Select,
    scope: &Scope,
) -> Result<Vec<JoinOrderPlan>, CodegenError> {
    let mut plans = Vec::with_capacity(select.order_by.len());
    for term in &select.order_by {
        let base_expr = strip_collate(&term.expr);
        let target = resolve_join_order_by_target(base_expr, select, scope)?;
        let descending = term.desc.unwrap_or(false);
        let nulls_first = term
            .nulls_last
            .map_or(!descending, |nulls_last| !nulls_last);
        plans.push(JoinOrderPlan {
            target,
            descending,
            collation: collation_of(&term.expr)
                .or_else(|| expr_collation(scope, base_expr))
                .unwrap_or(Collation::Binary),
            nulls_first,
        });
    }
    Ok(plans)
}

pub(super) fn resolve_join_order_by_target(
    expr: &Expr,
    select: &Select,
    scope: &Scope,
) -> Result<JoinOrderTarget, CodegenError> {
    match &expr.kind {
        ExprKind::Column { table, name, .. } => {
            // Result-column aliases take precedence over table columns,
            // same as the single-table path, but only for an
            // unqualified reference.
            if table.is_none() {
                if let Some(ResultColumn::Expr {
                    expr: aliased_expr, ..
                }) = select
                    .columns
                    .iter()
                    .find(|c| matches!(c, ResultColumn::Expr { alias: Some(a), .. } if a == name))
                {
                    return resolve_join_order_by_target(aliased_expr, select, scope);
                }
            }
            let (binding_idx, local_idx) = resolve_scope_column(scope, table.as_deref(), name)?;
            Ok(JoinOrderTarget::Offset(
                joined_column_offset(scope, binding_idx).saturating_add(local_idx),
            ))
        }
        _ => Ok(JoinOrderTarget::Expr(expr.clone())),
    }
}

/// Reads every column of every `scope.tables` binding (in order,
/// `*`-dedup notwithstanding) into a contiguous register run — the flat
/// row [`compile_joined_sorted_scan`]'s pass 1 buffers into the sorter.
/// Returns `None` (no registers allocated) only when `scope` has no
/// tables at all — not reachable from a real `FROM` clause, but kept
/// total rather than panicking.
pub(super) fn emit_full_joined_row(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    scope: &Scope,
) -> Result<Option<i32>, CodegenError> {
    let mut first: Option<i32> = None;
    for binding in &scope.tables {
        for idx in 0..binding.schema.columns.len() {
            let r = emit_join_column(em, reg, binding, idx)?;
            if first.is_none() {
                first = Some(r);
            }
        }
    }
    Ok(first)
}

/// Reconstructs `select`'s result-column projection from the flat,
/// all-bindings-concatenated pseudo record `compile_joined_sorted_scan`'s
/// pass 2 re-opens after sorting — the joined counterpart to
/// `emit_row_via_sink`/`compile_row_values`'s pseudo-cursor mode.
/// Restricted to `*`/`table.*`/bare-column result columns: a computed
/// expression can't be safely re-evaluated against the flat pseudo
/// record (a bare `Column` maps to one absolute offset, but an arbitrary
/// expression would need every column reference inside it individually
/// retargeted — no small task within this ticket's scope), so it
/// reports a clean `Unsupported` error instead of silently mis-projecting.
pub(super) fn emit_joined_pseudo_projection(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    scope: &Scope,
    pseudo_cursor: i32,
) -> Result<(i32, usize), CodegenError> {
    let mut regs = Vec::new();
    let read_offset = |em: &mut Emitter, reg: &mut RegAlloc, abs: usize| -> i32 {
        let r = reg.alloc();
        em.emit(Instruction::new(
            Opcode::Column,
            pseudo_cursor,
            i32::try_from(abs).unwrap_or(0),
            r,
        ));
        r
    };
    for col in &select.columns {
        match col {
            ResultColumn::Star => {
                for (i, binding) in scope.tables.iter().enumerate() {
                    let suppressed = scope.dedup_star.get(i);
                    let base = joined_column_offset(scope, i);
                    for idx in 0..binding.schema.columns.len() {
                        let Some(name) = binding.schema.columns.get(idx) else {
                            continue;
                        };
                        if suppressed.is_some_and(|s| s.contains(&name.to_ascii_lowercase())) {
                            continue;
                        }
                        regs.push(read_offset(em, reg, base.saturating_add(idx)));
                    }
                }
            }
            ResultColumn::TableStar { table } => {
                let i = scope
                    .tables
                    .iter()
                    .position(|b| b.matches_qualifier(table))
                    .ok_or_else(|| CodegenError::UnknownColumn {
                        name: format!("{table}.*"),
                    })?;
                let base = joined_column_offset(scope, i);
                let count = scope
                    .tables
                    .get(i)
                    .map(|b| b.schema.columns.len())
                    .unwrap_or(0);
                for idx in 0..count {
                    regs.push(read_offset(em, reg, base.saturating_add(idx)));
                }
            }
            ResultColumn::Expr {
                expr:
                    Expr {
                        kind: ExprKind::Column { table, name, .. },
                        ..
                    },
                ..
            } => {
                let (binding_idx, local_idx) = resolve_scope_column(scope, table.as_deref(), name)?;
                let abs = joined_column_offset(scope, binding_idx).saturating_add(local_idx);
                regs.push(read_offset(em, reg, abs));
            }
            ResultColumn::Expr { .. } => {
                return Err(CodegenError::Unsupported {
                    reason: "ORDER BY combined with a JOIN only supports `*`/`table.*`/bare \
                             column result columns today — a computed expression in the SELECT \
                             list can't yet be re-projected from the sorted output"
                        .to_string(),
                });
            }
        }
    }
    let Some(&first) = regs.first() else {
        return Ok((reg.alloc(), 0));
    };
    for (i, r) in regs.iter().enumerate() {
        let want = first.saturating_add(i32::try_from(i).unwrap_or(i32::MAX));
        if *r != want {
            return Err(CodegenError::Unsupported {
                reason: "result columns must land in contiguous registers for MakeRecord/\
                         ResultRow (a function call or other multi-register expression mixed \
                         with other columns is not yet supported)"
                    .to_string(),
            });
        }
    }
    Ok((first, regs.len()))
}

/// #250: `ORDER BY` combined with a JOIN — the joined counterpart to
/// [`compile_sorted_scan`]. Pass 1 drives the same nested-loop join as
/// the unsorted path (via [`compile_join_level_for_sort`], a variant of
/// [`compile_join_level`] whose innermost emission buffers the full
/// joined row — every binding's every column, plus a trailing register
/// per computed `ORDER BY` expression — into the sorter instead of
/// emitting `ResultRow`), `WHERE`-filtered but pre-LIMIT (LIMIT applies
/// to the sorted output). Pass 2 walks the sorted buffer via an
/// `OpenPseudo` cursor over the flat record and re-projects `select`'s
/// result columns from it (see [`emit_joined_pseudo_projection`]'s
/// scope restriction), applying LIMIT/OFFSET exactly as the single-table
/// sorted path does. `DISTINCT` combined with `ORDER BY` on a joined
/// `SELECT` is rejected by the caller before this function is ever
/// reached.
#[allow(clippy::too_many_arguments)]
pub(super) fn compile_joined_sorted_scan<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    exec_bindings: &[TableBinding],
    orig_bindings: &[TableBinding],
    pos_of: &[usize],
    levels: &[LevelPlan],
    dedup_star: &[std::collections::HashSet<String>],
    catalog: &[TableSchema],
    full_scope: &Scope,
    order_by_plans: &[JoinOrderPlan],
    sort_cursor: i32,
    pseudo_cursor: i32,
    end_label: Label,
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    let sorter_open_addr = em.emit(Instruction::with_p4(
        Opcode::SorterOpen,
        sort_cursor,
        0,
        0,
        P4::None,
    ));

    let mut null_mask = vec![false; exec_bindings.len()];
    let mut matched_regs: Vec<Option<i32>> = vec![None; exec_bindings.len()];
    compile_join_level_for_sort(
        em,
        reg,
        select,
        exec_bindings,
        orig_bindings,
        pos_of,
        levels,
        dedup_star,
        &mut null_mask,
        &mut matched_regs,
        0,
        catalog,
        order_by_plans,
        sort_cursor,
        sorter_open_addr,
    )?;

    let sort_addr = em.emit(Instruction::new(Opcode::SorterSort, sort_cursor, 0, 0));
    em.patch_p2(sort_addr, end_label);

    let limit = compile_limit_setup(em, reg, full_scope, select)?;

    let sorted_loop = em.new_label();
    em.place(sorted_loop);
    let sorter_data_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::SorterData,
        sort_cursor,
        sorter_data_reg,
        0,
    ));
    em.emit(Instruction::new(
        Opcode::OpenPseudo,
        pseudo_cursor,
        sorter_data_reg,
        0,
    ));

    let row_skip = em.new_label();
    // The pseudo scope's own bindings' cursor numbers don't matter for
    // `emit_joined_pseudo_projection` (it always reads `pseudo_cursor`
    // directly at an absolute offset) — `full_scope` is passed through
    // purely for its `tables`/`dedup_star` structure.
    if let Some(limit) = &limit {
        emit_offset_guard(em, limit, row_skip);
    }
    if let Some(limit) = &limit {
        emit_limit_guard(em, limit, end_label);
    }
    let (first, count) = emit_joined_pseudo_projection(em, reg, select, full_scope, pseudo_cursor)?;
    sink(em, reg, first, i32::try_from(count).unwrap_or(0))?;

    em.place(row_skip);
    let sorted_next = em.emit(Instruction::new(Opcode::SorterNext, sort_cursor, 0, 0));
    em.patch_p2(sorted_next, sorted_loop);
    Ok(())
}

/// Builds the [`SortKeyColumn`] list for one buffered joined row: a
/// bare-column `ORDER BY` term reads its offset straight from
/// `plan.target`, while a genuine expression is computed into its own
/// trailing register (relative to `first`, the row's leading register)
/// via [`compile_value`]. Shared by [`compile_join_level_for_sort`] (the
/// ordinary join tree's `ORDER BY` pass) and #288's
/// `compile_full_join_two_table` sorter-buffering emission points, so
/// both stay byte-for-byte identical in how a sort key is derived.
pub(super) fn compile_join_order_by_sort_keys(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    scope: &Scope,
    order_by_plans: &[JoinOrderPlan],
    first: i32,
) -> Result<Vec<SortKeyColumn>, CodegenError> {
    let mut sort_keys = Vec::with_capacity(order_by_plans.len());
    for plan in order_by_plans {
        let index = match &plan.target {
            JoinOrderTarget::Offset(off) => *off,
            JoinOrderTarget::Expr(expr) => {
                let r = compile_value(em, reg, scope, expr)?;
                usize::try_from(r.saturating_sub(first)).unwrap_or(0)
            }
        };
        sort_keys.push(SortKeyColumn {
            index,
            descending: plan.descending,
            collation: plan.collation,
            nulls_first: plan.nulls_first,
        });
    }
    Ok(sort_keys)
}

/// [`compile_join_level`]'s variant for the `ORDER BY`+JOIN sorted path
/// (#250): shares the exact same traversal — including the #243
/// single-check-access seek optimization, which this variant used to miss
/// entirely back when it was a hand-forked copy of `compile_join_level`
/// (see [`super::joins::compile_join_level_traverse`]) — but the innermost
/// level buffers the full joined row plus `ORDER BY` sort keys into
/// `sort_cursor` (see [`emit_full_joined_row`]) instead of applying `LIMIT`
/// and projecting `select.columns` via [`emit_join_row`]. Only `WHERE`
/// still applies at this innermost point — `LIMIT`/`DISTINCT` apply to the
/// sorted output in [`compile_joined_sorted_scan`]'s pass 2 instead.
#[allow(clippy::too_many_arguments)]
pub(super) fn compile_join_level_for_sort(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    exec_bindings: &[TableBinding],
    orig_bindings: &[TableBinding],
    pos_of: &[usize],
    levels: &[LevelPlan],
    dedup_star: &[std::collections::HashSet<String>],
    null_mask: &mut Vec<bool>,
    matched_regs: &mut Vec<Option<i32>>,
    level: usize,
    catalog: &[TableSchema],
    order_by_plans: &[JoinOrderPlan],
    sort_cursor: i32,
    sorter_open_addr: usize,
) -> Result<(), CodegenError> {
    super::joins::compile_join_level_traverse(
        em,
        reg,
        exec_bindings,
        orig_bindings,
        pos_of,
        levels,
        dedup_star,
        null_mask,
        matched_regs,
        level,
        catalog,
        &mut |em, reg, scope| {
            let row_skip = em.new_label();
            if let Some(where_expr) = &select.where_clause {
                compile_cond(
                    em,
                    reg,
                    scope,
                    where_expr,
                    CondTargets::null_is_false(Target::Fallthrough, Target::Jump(row_skip)),
                )?;
            }
            let Some(first) = emit_full_joined_row(em, reg, scope)? else {
                em.place(row_skip);
                return Ok(());
            };
            let sort_keys = compile_join_order_by_sort_keys(em, reg, scope, order_by_plans, first)?;
            em.patch_p4(sorter_open_addr, P4::SortKey(sort_keys));

            let count = usize::try_from(reg.peek().saturating_sub(first)).unwrap_or(0);
            let record_reg = reg.alloc();
            em.emit(Instruction::new(
                Opcode::MakeRecord,
                first,
                i32::try_from(count).unwrap_or(0),
                record_reg,
            ));
            em.emit(Instruction::new(
                Opcode::SorterInsert,
                sort_cursor,
                record_reg,
                0,
            ));
            em.place(row_skip);
            Ok(())
        },
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::parser::ast::BinaryOp;
    use crate::parser::tokenizer::Span;
    use crate::planner::Stats;
    use crate::schema::{IndexSchema, IndexedColumn};

    fn span() -> Span {
        Span {
            line: 0,
            column: 0,
            offset: 0,
            len: 0,
        }
    }

    fn col(table: Option<&str>, name: &str) -> Expr {
        Expr {
            kind: ExprKind::Column {
                table: table.map(str::to_string),
                catalog: None,
                name: name.to_string(),
            },
            span: span(),
        }
    }

    fn lit_int(n: i64) -> Expr {
        Expr {
            kind: ExprKind::Literal(crate::parser::ast::Literal::Integer(n)),
            span: span(),
        }
    }

    fn eq(lhs: Expr, rhs: Expr) -> Expr {
        Expr {
            kind: ExprKind::Binary {
                op: BinaryOp::Eq,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            },
            span: span(),
        }
    }

    fn schema(name: &str, columns: &[&str], indexes: Vec<IndexSchema>) -> TableSchema {
        TableSchema {
            unresolved_autoindex: false,
            name: name.to_string(),
            root_page: 0,
            columns: columns.iter().map(|c| (*c).to_string()).collect(),
            column_types: vec![String::new(); columns.len()],
            column_collations: vec![],
            without_rowid: false,
            strict: false,
            is_virtual: false,
            sql: String::new(),
            indexes,
            rowid_alias: None,
        }
    }

    fn binding(schema: TableSchema, alias: Option<&str>) -> TableBinding {
        TableBinding {
            alias: alias.map(str::to_string),
            name: schema.name.clone(),
            schema,
            cursor: 0,
            forced_null: false,
            stats: Stats::default(),
        }
    }

    fn unique_index(name: &str, col: &str) -> IndexSchema {
        IndexSchema {
            name: name.to_string(),
            unique: true,
            columns: vec![IndexedColumn {
                name: col.to_string(),
                desc: false,
                collation: crate::vdbe::Collation::Binary,
            }],
            root_page: 0,
        }
    }

    #[test]
    fn column_belongs_to_binding_qualifier_mismatch() {
        let b = binding(schema("t", &["a"], vec![]), Some("alias"));
        assert!(!column_belongs_to_binding(&b, Some("other"), "a"));
    }

    #[test]
    fn column_belongs_to_binding_no_qualifier_checks_schema() {
        let b = binding(schema("t", &["a"], vec![]), None);
        assert!(column_belongs_to_binding(&b, None, "a"));
        assert!(!column_belongs_to_binding(&b, None, "missing"));
    }

    #[test]
    fn expr_is_safe_join_probe_literal_and_param() {
        let priors: Vec<TableBinding> = vec![];
        assert!(expr_is_safe_join_probe(&lit_int(1), &priors));
        let param = Expr {
            kind: ExprKind::Param(crate::parser::ast::ParamKind::Numbered(1)),
            span: span(),
        };
        assert!(expr_is_safe_join_probe(&param, &priors));
    }

    #[test]
    fn expr_is_safe_join_probe_rejects_unknown_column() {
        let priors = vec![binding(schema("t0", &["id"], vec![]), None)];
        assert!(!expr_is_safe_join_probe(
            &col(Some("t0"), "missing"),
            &priors
        ));
    }

    #[test]
    fn expr_is_safe_join_probe_rejects_other_expr_kinds() {
        let priors: Vec<TableBinding> = vec![];
        // A binary expression is neither Literal/Param/Column.
        let expr = eq(lit_int(1), lit_int(2));
        assert!(!expr_is_safe_join_probe(&expr, &priors));
    }

    #[test]
    fn choose_join_access_rowid_seek() {
        let t0 = binding(schema("t0", &["id"], vec![]), None);
        let t1 = schema("t1", &["rowid", "name"], vec![]);
        let t1b = binding(t1, None);
        let on_expr = eq(col(Some("t1"), "rowid"), col(Some("t0"), "id"));
        let result = choose_join_access(&t1b, &on_expr, &[t0]);
        assert!(matches!(result, Some(JoinAccess::Rowid(_))));
    }

    #[test]
    fn choose_join_access_none_when_not_equality() {
        let t0 = binding(schema("t0", &["id"], vec![]), None);
        let t1b = binding(schema("t1", &["id"], vec![]), None);
        let on_expr = col(Some("t1"), "id");
        assert!(choose_join_access(&t1b, &on_expr, &[t0]).is_none());
    }

    #[test]
    fn choose_join_access_none_when_neither_side_belongs() {
        let t0 = binding(schema("t0", &["id"], vec![]), None);
        let t1b = binding(schema("t1", &["id"], vec![]), None);
        let on_expr = eq(col(Some("other"), "x"), col(Some("t0"), "id"));
        assert!(choose_join_access(&t1b, &on_expr, &[t0]).is_none());
    }

    #[test]
    fn choose_join_access_none_when_probe_unsafe() {
        let t0 = binding(schema("t0", &["id"], vec![]), None);
        let t1b = binding(schema("t1", &["id"], vec![]), None);
        // Other side references t1 itself (not-yet-bound), unsafe.
        let on_expr = eq(col(Some("t1"), "id"), col(Some("t1"), "id"));
        assert!(choose_join_access(&t1b, &on_expr, &[t0]).is_none());
    }

    #[test]
    fn choose_join_access_none_when_no_matching_index() {
        let t0 = binding(schema("t0", &["id"], vec![]), None);
        let t1b = binding(schema("t1", &["other"], vec![]), None);
        let on_expr = eq(col(Some("t1"), "other"), col(Some("t0"), "id"));
        assert!(choose_join_access(&t1b, &on_expr, &[t0]).is_none());
    }

    #[test]
    fn choose_join_access_unique_index_pick() {
        let t0 = binding(schema("t0", &["id"], vec![]), None);
        let idx = unique_index("idx_key", "key");
        let t1b = binding(schema("t1", &["id", "key"], vec![idx]), None);
        let on_expr = eq(col(Some("t1"), "key"), col(Some("t0"), "id"));
        let result = choose_join_access(&t1b, &on_expr, &[t0]);
        assert!(matches!(result, Some(JoinAccess::UniqueIndex { .. })));
    }

    #[test]
    fn choose_join_access_vetoed_by_expensive_index_stats() {
        let t0 = binding(schema("t0", &["id"], vec![]), None);
        let idx = unique_index("idx_key", "key");
        let mut t1_schema = schema("t1", &["id", "key"], vec![idx]);
        t1_schema.name = "t1".to_string();
        let stats = Stats::from_stat1_rows(vec![
            (None, "10".to_string()),
            (Some("idx_key".to_string()), "1000 1000".to_string()),
        ]);
        let mut t1b = binding(t1_schema, None);
        t1b.stats = stats;
        let on_expr = eq(col(Some("t1"), "key"), col(Some("t0"), "id"));
        assert!(choose_join_access(&t1b, &on_expr, &[t0]).is_none());
    }

    #[test]
    fn choose_join_access_rhs_belongs_to_binding() {
        let t0 = binding(schema("t0", &["id"], vec![]), None);
        let t1b = binding(schema("t1", &["rowid"], vec![]), None);
        // lhs belongs to prior t0, rhs belongs to t1 -> swapped branch.
        let on_expr = eq(col(Some("t0"), "id"), col(Some("t1"), "rowid"));
        let result = choose_join_access(&t1b, &on_expr, &[t0]);
        assert!(matches!(result, Some(JoinAccess::Rowid(_))));
    }

    #[test]
    fn choose_auto_index_probe_requires_worthwhile_stats() {
        let t0 = binding(schema("t0", &["id"], vec![]), None);
        let t1b = binding(schema("t1", &["key"], vec![]), None);
        let on_expr = eq(col(Some("t1"), "key"), col(Some("t0"), "id"));
        // No stats at all -> not worthwhile.
        assert!(choose_auto_index_probe(&t1b, &on_expr, &[t0]).is_none());
    }

    #[test]
    fn choose_auto_index_probe_picks_when_worthwhile() {
        let t0 = binding(schema("t0", &["id"], vec![]), None);
        let stats = Stats::from_stat1_rows(vec![(None, "10000".to_string())]);
        let mut t1b = binding(schema("t1", &["id", "key"], vec![]), None);
        t1b.stats = stats;
        let on_expr = eq(col(Some("t1"), "key"), col(Some("t0"), "id"));
        let probe = choose_auto_index_probe(&t1b, &on_expr, &[t0]);
        assert!(probe.is_some());
        assert_eq!(probe.unwrap().key_column, 1);
    }

    #[test]
    fn choose_auto_index_probe_none_when_equality_fails() {
        let stats = Stats::from_stat1_rows(vec![(None, "10000".to_string())]);
        let mut t1b = binding(schema("t1", &["id"], vec![]), None);
        t1b.stats = stats;
        let on_expr = col(Some("t1"), "id");
        assert!(choose_auto_index_probe(&t1b, &on_expr, &[]).is_none());
    }

    #[test]
    fn choose_auto_index_probe_none_when_neither_side_belongs() {
        let stats = Stats::from_stat1_rows(vec![(None, "10000".to_string())]);
        let mut t1b = binding(schema("t1", &["id"], vec![]), None);
        t1b.stats = stats;
        let on_expr = eq(col(Some("other"), "x"), col(Some("other2"), "y"));
        assert!(choose_auto_index_probe(&t1b, &on_expr, &[]).is_none());
    }

    #[test]
    fn choose_auto_index_probe_none_when_probe_unsafe() {
        let stats = Stats::from_stat1_rows(vec![(None, "10000".to_string())]);
        let mut t1b = binding(schema("t1", &["id"], vec![]), None);
        t1b.stats = stats;
        let on_expr = eq(col(Some("t1"), "id"), col(Some("t1"), "id"));
        assert!(choose_auto_index_probe(&t1b, &on_expr, &[]).is_none());
    }

    #[test]
    fn joined_column_offset_sums_prior_bindings() {
        let scope = Scope {
            tables: vec![
                binding(schema("t0", &["a", "b"], vec![]), None),
                binding(schema("t1", &["c"], vec![]), None),
            ],
            ..Scope::default()
        };
        assert_eq!(joined_column_offset(&scope, 0), 0);
        assert_eq!(joined_column_offset(&scope, 1), 2);
        assert_eq!(joined_column_offset(&scope, 2), 3);
    }

    #[test]
    fn resolve_scope_column_finds_binding_and_local_index() {
        let scope = Scope {
            tables: vec![
                binding(schema("t0", &["a", "b"], vec![]), None),
                binding(schema("t1", &["c"], vec![]), None),
            ],
            ..Scope::default()
        };
        let (binding_idx, local_idx) = resolve_scope_column(&scope, Some("t1"), "c").unwrap();
        assert_eq!(binding_idx, 1);
        assert_eq!(local_idx, 0);
    }

    #[test]
    fn resolve_scope_column_unknown_errors() {
        let scope = Scope {
            tables: vec![binding(schema("t0", &["a"], vec![]), None)],
            ..Scope::default()
        };
        assert!(resolve_scope_column(&scope, None, "missing").is_err());
    }

    #[test]
    fn join_order_plan_ascending_offset_defaults() {
        let plan = JoinOrderPlan::ascending_offset(3, Collation::Binary);
        assert!(matches!(plan.target, JoinOrderTarget::Offset(3)));
        assert!(!plan.descending);
        assert!(plan.nulls_first);
    }

    fn empty_select(columns: Vec<ResultColumn>) -> Select {
        Select {
            with_clause: None,
            distinct: None,
            columns,
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            compound: vec![],
            order_by: vec![],
            limit: None,
            span: span(),
        }
    }

    #[test]
    fn resolve_join_order_by_target_prefers_alias() {
        let select = empty_select(vec![ResultColumn::Expr {
            expr: col(Some("t0"), "a"),
            alias: Some("aliased".to_string()),
        }]);
        let scope = Scope {
            tables: vec![binding(schema("t0", &["a"], vec![]), None)],
            ..Scope::default()
        };
        let target = resolve_join_order_by_target(&col(None, "aliased"), &select, &scope).unwrap();
        assert!(matches!(target, JoinOrderTarget::Offset(0)));
    }

    #[test]
    fn resolve_join_order_by_target_non_column_becomes_expr() {
        let select = empty_select(vec![]);
        let scope = Scope::default();
        let expr = lit_int(42);
        let target = resolve_join_order_by_target(&expr, &select, &scope).unwrap();
        assert!(matches!(target, JoinOrderTarget::Expr(_)));
    }
}
