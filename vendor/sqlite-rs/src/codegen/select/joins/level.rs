// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::super::join_access::{
    choose_auto_index_probe, choose_join_access, emit_join_row, JoinAccess,
};
use super::super::limit_scan::{emit_limit_guard, emit_offset_guard, LimitState};
use super::super::*;
use super::{join_scope, synthesize_equality_constraint};
use crate::codegen::index_maintenance::valid_index_root_page;
use crate::parser::ast::Join;

/// Resolves one `Join`'s constraint into an `ON`-equivalent `Expr` (or
/// `None` for an unconditional `CROSS`/no-shared-column `NATURAL` join):
/// an explicit `ON` clause is used as-is, `USING (...)`/`NATURAL` both
/// route through [`synthesize_equality_constraint`] (`NATURAL` first
/// computing its own shared-column-name list), and either populates
/// `dedup_star[right_idx]` with the columns `SELECT *` must dedup.
/// Shared by [`compile_select_joined_scan`]'s N-way join-level loop and
/// [`compile_full_join_two_table`]'s dedicated two-table path — both used
/// to hand-fork this same match/dedup-bookkeeping block.
pub(in crate::codegen::select) fn resolve_join_constraint(
    join: &Join,
    left: &[TableBinding],
    right: &TableBinding,
    right_idx: usize,
    dedup_star: &mut [std::collections::HashSet<String>],
) -> Result<Option<Expr>, CodegenError> {
    match &join.constraint {
        Some(JoinConstraint::On(e)) => Ok(Some(e.clone())),
        Some(JoinConstraint::Using(cols)) => {
            let (expr, shared) = synthesize_equality_constraint(left, right, &cols[..], true)?;
            if let Some(slot) = dedup_star.get_mut(right_idx) {
                slot.extend(shared);
            }
            Ok(expr)
        }
        None if join.natural => {
            let shared_names: Vec<String> = right
                .schema
                .columns
                .iter()
                .filter(|name| {
                    left.iter().any(|b| {
                        b.schema
                            .columns
                            .iter()
                            .any(|c| c.eq_ignore_ascii_case(name))
                    })
                })
                .cloned()
                .collect();
            if shared_names.is_empty() {
                Ok(None)
            } else {
                let (expr, shared) =
                    synthesize_equality_constraint(left, right, &shared_names, false)?;
                if let Some(slot) = dedup_star.get_mut(right_idx) {
                    slot.extend(shared);
                }
                Ok(expr)
            }
        }
        None => Ok(None),
    }
}

/// One constraint checked while iterating `exec_bindings[check_level]`'s
/// own loop (see [`compile_join_level`]): `constraint` gates whether
/// recursion continues to the next level (`None` means unconditional —
/// a `CROSS`/`NATURAL`-with-no-shared-columns join), and if
/// `sets_matched` is `Some(outer_level)`, passing it also marks
/// `outer_level`'s "matched" register. For a classic `LEFT JOIN`,
/// `outer_level == check_level` (the table's own loop both checks its
/// `ON` condition and owns the matched flag, exactly #237's original
/// shape). For `RIGHT JOIN` reordered into an equivalent `LEFT JOIN`
/// (see [`compile_select_joined`]), `outer_level` is the RIGHT-joined
/// table's own (shallower) level, while `check_level` is the deepest
/// level of the chain it was joined against — the constraint can only
/// be evaluated once every table it references is bound.
#[derive(Debug, Clone)]
pub(in crate::codegen::select) struct LevelCheck {
    pub(super) constraint: Option<Expr>,
    pub(super) sets_matched: Option<usize>,
}

/// The full plan for one execution level: zero or more [`LevelCheck`]s
/// run inside its own `Rewind`/`Next` loop, and — if `null_span` is
/// `Some((start, end))` — this level owns an outer-join "matched"
/// register, tested once its own loop exhausts. If nothing matched,
/// every level in `start..=end` (inclusive, always this level or
/// deeper) gets `null_mask` forced on and recursion jumps directly to
/// `end + 1`, skipping those levels' own loops entirely — there is
/// nothing to iterate for a synthesized outer-join row. A classic
/// `LEFT JOIN` has `null_span == Some((level, level))` (only itself);
/// `RIGHT JOIN`'s reordering produces `null_span == Some((outer_level +
/// 1, check_level))`, spanning every level of the chain it was joined
/// against.
#[derive(Debug, Clone, Default)]
pub(in crate::codegen::select) struct LevelPlan {
    pub(super) checks: Vec<LevelCheck>,
    pub(super) null_span: Option<(usize, usize)>,
}

/// Recursively emits the nested-loop join, one table per recursion
/// level. `exec_bindings` is in *execution* order (level `i` opens
/// `exec_bindings[i]`'s cursor); `orig_bindings`/`pos_of` are the
/// original FROM-clause-order bindings and the original-index ->
/// execution-level map, used only to build a [`Scope`] in FROM order
/// (see [`join_scope`]) — `SELECT *` expansion and column-ambiguity
/// resolution must not depend on RIGHT JOIN's internal reordering.
/// `level == exec_bindings.len()` is the innermost point — every
/// table's cursor is positioned on a candidate combination, so this is
/// where `WHERE`, `LIMIT`/`OFFSET`, and the result-column projection
/// all compile, via [`emit_join_final_row`].
///
/// A level with `levels[level].null_span == Some((start, end))` wraps
/// its own `Rewind`/`Next` loop with a `matched` flag register:
/// cleared before the loop, set to 1 by any [`LevelCheck`] (at this
/// level or a deeper `check_level`) whose `sets_matched` names this
/// level, and tested with `IfNot` right after the loop exits — if it's
/// still 0, the join recurses exactly once more (jumping straight to
/// `end + 1`) with `null_mask` set for every level in `start..=end`,
/// which (per [`join_scope`]) makes every reference to those tables'
/// columns compile to a NULL literal instead of a real `Column`/
/// `Rowid` read, so a non-matching row still contributes exactly one
/// null-extended output row.
#[allow(clippy::too_many_arguments)]
pub(in crate::codegen::select) fn compile_join_level<F>(
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
    end_label: Label,
    limit: Option<&LimitState>,
    distinct_cursor: Option<i32>,
    catalog: &[TableSchema],
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    compile_join_level_traverse(
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
            emit_join_final_row(
                em,
                reg,
                select,
                scope,
                end_label,
                limit,
                distinct_cursor,
                sink,
            )
        },
    )
}

/// Shared nested-loop/outer-join traversal behind both [`compile_join_level`]
/// (the unsorted path) and [`super::join_access::compile_join_level_for_sort`]
/// (#250's `ORDER BY`+JOIN sorted path) — every level's `Rewind`/`Next` loop,
/// `ON`-condition checks, `#243` single-check-access seek optimization, and
/// `LEFT`/`RIGHT` "matched"-register/null-extension bookkeeping lives here
/// exactly once, so the sorted path can no longer silently miss the seek
/// optimization the unsorted path gets (the bug this extraction fixes: the
/// two paths used to be hand-forked copies, and only one of them ever grew
/// the #243 seek). `leaf` is invoked once per candidate combination (i.e.
/// once `level == exec_bindings.len()`) with the fully-resolved [`Scope`];
/// each caller supplies its own innermost emission (direct row emit for the
/// unsorted path, sort-buffer write for the sorted path) via `leaf`.
#[allow(clippy::too_many_arguments)]
pub(in crate::codegen::select) fn compile_join_level_traverse<L>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    exec_bindings: &[TableBinding],
    orig_bindings: &[TableBinding],
    pos_of: &[usize],
    levels: &[LevelPlan],
    dedup_star: &[std::collections::HashSet<String>],
    null_mask: &mut Vec<bool>,
    matched_regs: &mut Vec<Option<i32>>,
    level: usize,
    catalog: &[TableSchema],
    leaf: &mut L,
) -> Result<(), CodegenError>
where
    L: FnMut(&mut Emitter, &mut RegAlloc, &Scope) -> Result<(), CodegenError>,
{
    if level == exec_bindings.len() {
        let scope = join_scope(orig_bindings, null_mask, pos_of, catalog, dedup_star);
        return leaf(em, reg, &scope);
    }

    let Some(binding) = exec_bindings.get(level) else {
        return Err(CodegenError::Unsupported {
            reason: "join level out of range".to_string(),
        });
    };
    let cursor = binding.cursor;
    let plan = levels.get(level).cloned().unwrap_or_default();

    if plan.null_span.is_some() {
        let matched = reg.alloc();
        em.emit(Instruction::new(Opcode::Integer, 0, matched, 0));
        if let Some(slot) = matched_regs.get_mut(level) {
            *slot = Some(matched);
        }
    }

    // #243's SeekRowid/SeekIndexEq point-lookup optimization, adapted to
    // this reordering-aware plan: only tried when the level has exactly
    // one check (the common case — a RIGHT JOIN group's deepest level
    // combining an intra-group condition with the outer join's own
    // condition has two, and always falls back to the full scan below;
    // see [`choose_join_access`]'s own narrowness for the rest of the
    // conditions this applies under).
    let single_check_access = match plan.checks.as_slice() {
        [check] => check.constraint.as_ref().and_then(|constraint| {
            let prior = exec_bindings.get(..level)?;
            choose_join_access(binding, constraint, prior)
        }),
        _ => None,
    };

    // #545: when no structural seek exists for this level's single
    // check, building a transient automatic index over `binding`'s join
    // column can turn this level's scan into a seek, same as a real
    // index would — see [`choose_auto_index_probe`]'s gating (`ANALYZE`
    // stats required).
    let auto_index_probe = if single_check_access.is_none() {
        match plan.checks.as_slice() {
            [check] => check.constraint.as_ref().and_then(|constraint| {
                let prior = exec_bindings.get(..level)?;
                choose_auto_index_probe(binding, constraint, prior)
            }),
            _ => None,
        }
    } else {
        None
    };

    match single_check_access {
        Some(access) => {
            let scope = join_scope(orig_bindings, null_mask, pos_of, catalog, dedup_star);
            let miss = em.new_label();
            match access {
                JoinAccess::Rowid(operand) => {
                    let value_reg = compile_value(em, reg, &scope, &operand)?;
                    let seek_addr =
                        em.emit(Instruction::new(Opcode::SeekRowid, cursor, 0, value_reg));
                    em.patch_p2(seek_addr, miss);
                }
                JoinAccess::UniqueIndex { index, operand } => {
                    let value_reg = compile_value(em, reg, &scope, &operand)?;
                    let index_cursor = i32::try_from(exec_bindings.len().saturating_add(level))
                        .unwrap_or(i32::MAX);
                    let root_page = valid_index_root_page(&index)?;
                    let mut open_instr =
                        Instruction::new(Opcode::OpenRead, index_cursor, root_page, 0);
                    open_instr.p5 = 1;
                    em.emit(open_instr);
                    let leading_collation = index
                        .columns
                        .first()
                        .map_or(Collation::Binary, |c| c.collation);
                    let seek_instr = Instruction::with_p4(
                        Opcode::SeekIndexEq,
                        index_cursor,
                        0,
                        value_reg,
                        P4::SeekKey(vec![leading_collation]),
                    );
                    let seek_addr = em.emit(seek_instr);
                    em.patch_p2(seek_addr, miss);
                    let rowid_reg = reg.alloc();
                    em.emit(Instruction::new(
                        Opcode::IdxRowid,
                        index_cursor,
                        rowid_reg,
                        0,
                    ));
                    let table_seek_addr =
                        em.emit(Instruction::new(Opcode::SeekRowid, cursor, 0, rowid_reg));
                    em.patch_p2(table_seek_addr, miss);
                }
            }
            if let Some(outer_level) = plan.checks.first().and_then(|c| c.sets_matched) {
                let target = matched_regs
                    .get(outer_level)
                    .copied()
                    .flatten()
                    .ok_or_else(|| CodegenError::Unsupported {
                        reason: "join level plan referenced an unallocated matched register"
                            .to_string(),
                    })?;
                em.emit(Instruction::new(Opcode::Integer, 1, target, 0));
            }
            let next_level = level.saturating_add(1);
            compile_join_level_traverse(
                em,
                reg,
                exec_bindings,
                orig_bindings,
                pos_of,
                levels,
                dedup_star,
                null_mask,
                matched_regs,
                next_level,
                catalog,
                leaf,
            )?;
            em.place(miss);
        }
        None if auto_index_probe.is_some() => {
            // #545: build a transient automatic index over `binding`'s
            // join column (a one-time `Once`-guarded pre-pass), then
            // probe it instead of a `Rewind`/`Next` full scan. Unlike a real
            // index's `SeekIndexEq` + recheck-then-`IdxNext` shape
            // (#450), the `AutoIndexSeek`/`AutoIndexRowid`/
            // `AutoIndexNext` primitives are an exact-key multi-map, so
            // there's no leading-column recheck loop needed — every
            // rowid `AutoIndexNext` yields already shares the seeked
            // key by construction.
            //
            // The match arm's own guard already established
            // `auto_index_probe.is_some()`; a nested `match` (rather
            // than an `unreachable!` else-branch) keeps that fact
            // encoded in the type system instead of a runtime panic
            // path, which this crate's macro-vocabulary gate (`make
            // mvl-limit`) doesn't allow outside its curated allowlist.
            let probe = match &auto_index_probe {
                Some(probe) => probe,
                None => {
                    return Err(CodegenError::Unsupported {
                        reason: "unreachable: guarded by the match arm's own is_some() condition"
                            .to_string(),
                    })
                }
            };
            let rewind_end = em.new_label();
            let eph_cursor = reg.alloc_cursor();

            let once_addr = em.emit(Instruction::new(Opcode::Once, 0, 0, 0));
            let after_build = em.new_label();
            let mut open_instr = Instruction::new(Opcode::OpenEphemeral, eph_cursor, 0, 0);
            open_instr.p5 = 2;
            em.emit(open_instr);
            let build_end = em.new_label();
            let build_rewind = em.emit(Instruction::new(Opcode::Rewind, cursor, 0, 0));
            em.patch_p2(build_rewind, build_end);
            let build_loop = em.new_label();
            em.place(build_loop);
            let key_reg = reg.alloc();
            emit_column_read(em, &binding.schema, cursor, probe.key_column, key_reg)?;
            let build_rowid_reg = reg.alloc();
            em.emit(Instruction::new(Opcode::Rowid, cursor, build_rowid_reg, 0));
            let leading_collation = binding
                .schema
                .column_collations
                .get(probe.key_column)
                .copied()
                .unwrap_or(Collation::Binary);
            em.emit(Instruction::with_p4(
                Opcode::AutoIndexInsert,
                eph_cursor,
                key_reg,
                build_rowid_reg,
                P4::SeekKey(vec![leading_collation]),
            ));
            let build_next = em.emit(Instruction::new(Opcode::Next, cursor, 0, 0));
            em.patch_p2(build_next, build_loop);
            em.place(build_end);
            em.place(after_build);
            em.patch_p2(once_addr, after_build);

            let scope = join_scope(orig_bindings, null_mask, pos_of, catalog, dedup_star);
            let probe_reg = compile_value(em, reg, &scope, &probe.probe)?;
            let seek_addr = em.emit(Instruction::with_p4(
                Opcode::AutoIndexSeek,
                eph_cursor,
                0,
                probe_reg,
                P4::SeekKey(vec![leading_collation]),
            ));
            em.patch_p2(seek_addr, rewind_end);

            let loop_start = em.new_label();
            em.place(loop_start);

            let skip = em.new_label();
            let idx_rowid_reg = reg.alloc();
            em.emit(Instruction::new(
                Opcode::AutoIndexRowid,
                eph_cursor,
                idx_rowid_reg,
                0,
            ));
            let table_seek_addr = em.emit(Instruction::new(
                Opcode::SeekRowid,
                cursor,
                0,
                idx_rowid_reg,
            ));
            em.patch_p2(table_seek_addr, skip);

            for check in &plan.checks {
                if let Some(constraint) = &check.constraint {
                    let scope = join_scope(orig_bindings, null_mask, pos_of, catalog, dedup_star);
                    compile_cond(
                        em,
                        reg,
                        &scope,
                        constraint,
                        CondTargets::null_is_false(Target::Fallthrough, Target::Jump(skip)),
                    )?;
                }
                if let Some(outer_level) = check.sets_matched {
                    let target = matched_regs
                        .get(outer_level)
                        .copied()
                        .flatten()
                        .ok_or_else(|| CodegenError::Unsupported {
                            reason: "join level plan referenced an unallocated matched register"
                                .to_string(),
                        })?;
                    em.emit(Instruction::new(Opcode::Integer, 1, target, 0));
                }
            }
            let next_level = level.saturating_add(1);
            compile_join_level_traverse(
                em,
                reg,
                exec_bindings,
                orig_bindings,
                pos_of,
                levels,
                dedup_star,
                null_mask,
                matched_regs,
                next_level,
                catalog,
                leaf,
            )?;

            em.place(skip);
            let next_addr = em.emit(Instruction::new(Opcode::AutoIndexNext, eph_cursor, 0, 0));
            em.patch_p2(next_addr, loop_start);
            em.place(rewind_end);
        }
        None => {
            let rewind_end = em.new_label();

            let rewind_addr = em.emit(Instruction::new(Opcode::Rewind, cursor, 0, 0));
            em.patch_p2(rewind_addr, rewind_end);
            let loop_start = em.new_label();
            em.place(loop_start);

            let skip = em.new_label();
            for check in &plan.checks {
                if let Some(constraint) = &check.constraint {
                    let scope = join_scope(orig_bindings, null_mask, pos_of, catalog, dedup_star);
                    compile_cond(
                        em,
                        reg,
                        &scope,
                        constraint,
                        CondTargets::null_is_false(Target::Fallthrough, Target::Jump(skip)),
                    )?;
                }
                if let Some(outer_level) = check.sets_matched {
                    let target = matched_regs
                        .get(outer_level)
                        .copied()
                        .flatten()
                        .ok_or_else(|| CodegenError::Unsupported {
                            reason: "join level plan referenced an unallocated matched register"
                                .to_string(),
                        })?;
                    em.emit(Instruction::new(Opcode::Integer, 1, target, 0));
                }
            }
            let next_level = level.saturating_add(1);
            compile_join_level_traverse(
                em,
                reg,
                exec_bindings,
                orig_bindings,
                pos_of,
                levels,
                dedup_star,
                null_mask,
                matched_regs,
                next_level,
                catalog,
                leaf,
            )?;
            em.place(skip);
            let next_addr = em.emit(Instruction::new(Opcode::Next, cursor, 0, 0));
            em.patch_p2(next_addr, loop_start);
            em.place(rewind_end);
        }
    }

    if let Some((start, end)) = plan.null_span {
        let matched = matched_regs.get(level).copied().flatten().ok_or_else(|| {
            CodegenError::Unsupported {
                reason: "join level plan missing matched register for outer join".to_string(),
            }
        })?;
        // `matched` is still 0 iff nothing satisfied this outer join —
        // emit exactly one null-extended row for `start..=end` in that
        // case, then continue from `end + 1` (skipping those levels'
        // own loops entirely — there's nothing to iterate).
        let do_null = em.new_label();
        let after_null = em.new_label();
        let addr = em.emit(Instruction::new(Opcode::IfNot, matched, 0, 0));
        em.patch_p2(addr, do_null);
        em.goto(after_null);

        em.place(do_null);
        for lv in start..=end {
            if let Some(slot) = null_mask.get_mut(lv) {
                *slot = true;
            }
        }
        compile_join_level_traverse(
            em,
            reg,
            exec_bindings,
            orig_bindings,
            pos_of,
            levels,
            dedup_star,
            null_mask,
            matched_regs,
            end.saturating_add(1),
            catalog,
            leaf,
        )?;
        for lv in start..=end {
            if let Some(slot) = null_mask.get_mut(lv) {
                *slot = false;
            }
        }
        em.place(after_null);
    }
    Ok(())
}

/// Applies `WHERE`, `LIMIT`/`OFFSET`, and the result-column projection
/// to one candidate join row (`scope` already reflects every table's
/// forced-null state for this branch) — factored out of
/// [`compile_join_level`]'s innermost level so [`compile_full_join_two_table`]
/// can reuse the exact same sequencing for its own three emission
/// points (matched, left-nulled, right-unmatched).
#[allow(clippy::too_many_arguments)]
pub(in crate::codegen::select) fn emit_join_final_row<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    scope: &Scope,
    end_label: Label,
    limit: Option<&LimitState>,
    distinct_cursor: Option<i32>,
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
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
    if let Some(distinct_cursor) = distinct_cursor {
        emit_join_distinct_guard(em, reg, select, scope, distinct_cursor, row_skip)?;
    }
    if let Some(limit) = limit {
        emit_offset_guard(em, limit, row_skip);
    }
    if let Some(limit) = limit {
        emit_limit_guard(em, limit, end_label);
    }
    emit_join_row(em, reg, select, scope, sink)?;
    em.place(row_skip);
    Ok(())
}

/// #250: `DISTINCT` combined with a JOIN. Same ephemeral-index dedup
/// mechanism as the single-table [`emit_distinct_guard`] — `Found`
/// against `distinct_cursor` skips an already-seen row, `IdxInsert`
/// records a new one — but keyed by `select`'s result columns projected
/// against the joined `scope` via [`emit_join_row`] rather than a single
/// schema. The projection is computed twice (once here to test/record
/// it, once more via the ordinary [`emit_join_row`] call right after in
/// [`emit_join_final_row`]) rather than threading the registers through
/// — both computations are side-effect-free column reads/literal
/// expressions, so the only cost is a handful of extra bump-allocated
/// registers, which this compiler already treats as cheap.
pub(in crate::codegen::select) fn emit_join_distinct_guard(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    scope: &Scope,
    distinct_cursor: i32,
    skip_label: Label,
) -> Result<(), CodegenError> {
    let mut captured: Option<(i32, i32)> = None;
    emit_join_row(em, reg, select, scope, &mut |_em, _reg, first, count| {
        captured = Some((first, count));
        Ok(())
    })?;
    let Some((first, count)) = captured else {
        return Ok(());
    };
    let addr = em.emit(Instruction::with_p4(
        Opcode::Found,
        distinct_cursor,
        0,
        first,
        P4::Int(i64::from(count)),
    ));
    em.patch_p2(addr, skip_label);
    em.emit(Instruction::with_p4(
        Opcode::IdxInsert,
        distinct_cursor,
        first,
        0,
        P4::Int(i64::from(count)),
    ));
    Ok(())
}
