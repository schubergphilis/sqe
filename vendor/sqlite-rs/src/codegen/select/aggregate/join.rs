// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::super::join_access::{
    compile_join_level_for_sort, joined_column_offset, resolve_scope_column, JoinOrderPlan,
};
use super::super::joins::LevelPlan;
use super::super::limit_scan::{
    compile_limit_setup, emit_limit_guard, emit_offset_guard, LimitState,
};
use super::super::order_by::strip_collate;
use super::super::*;
use super::accum::{collect_aggregates, AggSlot};

/// [`super::compile_grouped_scan`]'s joined counterpart (#333):
/// `GROUP BY`/an implicit whole-table aggregate combined with a JOIN.
/// Shares the same sort-then-group shape, generalized from a single
/// `TableSchema`/cursor to a joined [`Scope`] the same way #250
/// generalized `ORDER BY`/`DISTINCT` (see
/// [`super::super::join_access::compile_joined_sorted_scan`]): pass 1
/// reuses [`compile_join_level_for_sort`] to buffer every WHERE-matching
/// joined row (every binding's every column, flat and concatenated,
/// exactly as [`super::super::join_access::emit_full_joined_row`]
/// already does for the `ORDER BY` case) into the sorter, keyed by the
/// `GROUP BY` columns; pass 2 walks the sorted buffer through a shared
/// flat pseudo cursor, detecting group boundaries and accumulating via
/// `AggStep`/`AggFinal` by absolute column offset instead of a
/// `TableSchema`-relative index.
///
/// Bounded MVP scope (documented rather than silently wrong, matching
/// this module's existing joined-`ORDER BY` restrictions):
/// - Every `GROUP BY` term must be a bare (optionally table-qualified)
///   column — a computed `GROUP BY` expression combined with a JOIN is
///   rejected.
/// - Every aggregate call's argument must likewise be a bare column (or
///   absent, for `count(*)`).
/// - Every result column must be `*`/`table.*`/a bare column/exactly one
///   whole aggregate call — an aggregate nested inside a larger
///   expression (`count(*) + 1`) is rejected.
/// - `HAVING` combined with a JOIN is rejected outright.
///
/// None of these restrictions apply to the single-table path — only to
/// a `GROUP BY`/aggregate combined with a JOIN.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) fn compile_joined_grouped_scan<F>(
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
    sort_cursor: i32,
    pseudo_cursor: i32,
    flush_cursor: i32,
    // #502: `Some((order_sort_cursor, order_pseudo_cursor))` when
    // `select.order_by` is non-empty — a second sorter pass that
    // re-orders every finalized group row (raw columns + finalized
    // aggregates) before it reaches `sink`, instead of `sink` being
    // called directly as each group is flushed.
    order_cursors: Option<(i32, i32)>,
    end_label: Label,
    implicit_group: bool,
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    if select.having.is_some() {
        return Err(CodegenError::Unsupported {
            reason: "HAVING combined with a JOIN is not yet supported".to_string(),
        });
    }

    let group_offsets: Vec<usize> = select
        .group_by
        .iter()
        .map(|expr| joined_bare_column_offset(full_scope, expr))
        .collect::<Result<_, _>>()?;

    let aggs = collect_aggregates(select)?;
    for (_, _, arg, _) in &aggs {
        if let Some(expr) = arg {
            joined_bare_column_offset(full_scope, expr)?;
        }
    }
    // DISTINCT-aggregate ephemeral dedup cursors start right after the
    // highest cursor number this scan already uses — `flush_cursor`
    // normally, or past the two extra `ORDER BY` sorter cursors
    // (#502) when `order_cursors` is `Some`.
    let eph_base = match order_cursors {
        Some((_, order_pseudo_cursor)) => order_pseudo_cursor.saturating_add(1),
        None => flush_cursor.saturating_add(1),
    };
    let agg_slots: Vec<AggSlot> = aggs
        .into_iter()
        .enumerate()
        .map(|(slot, (call, name, arg, distinct))| AggSlot {
            call,
            name,
            arg,
            slot: i32::try_from(slot).unwrap_or(0),
            eph_cursor: distinct.then(|| eph_base.saturating_add(i32::try_from(slot).unwrap_or(0))),
        })
        .collect();
    validate_joined_group_projection(select, &agg_slots)?;

    let total_width = joined_column_offset(full_scope, full_scope.tables.len());

    // #502: a trailing `ORDER BY` sorts the *finalized* group rows —
    // `total_width` raw joined columns followed by `agg_slots.len()`
    // finalized aggregate values, exactly what `flush_joined_group`
    // already assembles into `dests` for every group. Resolved and
    // opened up front so `SorterInsert` calls inside the boundary/tail
    // flushes below have a P4 sort-key layout already in place.
    if let Some((order_sort_cursor, _)) = order_cursors {
        let order_sort_keys: Vec<SortKeyColumn> = select
            .order_by
            .iter()
            .map(|term| {
                let offset = resolve_group_order_target(
                    select,
                    full_scope,
                    &agg_slots,
                    total_width,
                    &term.expr,
                )?;
                let descending = term.desc.unwrap_or(false);
                let nulls_first = term
                    .nulls_last
                    .map_or(!descending, |nulls_last| !nulls_last);
                Ok(SortKeyColumn {
                    index: offset,
                    descending,
                    collation: collation_of(&term.expr)
                        .or_else(|| expr_collation(full_scope, &term.expr))
                        .unwrap_or(Collation::Binary),
                    nulls_first,
                })
            })
            .collect::<Result<_, CodegenError>>()?;
        em.emit(Instruction::with_p4(
            Opcode::SorterOpen,
            order_sort_cursor,
            0,
            0,
            P4::SortKey(order_sort_keys),
        ));
    }

    let order_by_plans: Vec<JoinOrderPlan> = select
        .group_by
        .iter()
        .zip(&group_offsets)
        .map(|(expr, &offset)| {
            JoinOrderPlan::ascending_offset(
                offset,
                collation_of(expr)
                    .or_else(|| expr_collation(full_scope, expr))
                    .unwrap_or(Collation::Binary),
            )
        })
        .collect();

    // Pass 1: buffer every WHERE-matching joined row, sorted by the
    // GROUP BY key — reuses the exact traversal #250's joined `ORDER
    // BY` pass 1 uses, just keyed differently.
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
        &order_by_plans,
        sort_cursor,
        sorter_open_addr,
    )?;

    let sort_addr = em.emit(Instruction::new(Opcode::SorterSort, sort_cursor, 0, 0));
    let empty_sorter_target = if implicit_group {
        em.new_label()
    } else {
        end_label
    };
    em.patch_p2(sort_addr, empty_sorter_target);

    let limit = compile_limit_setup(em, reg, full_scope, select)?;

    let zero_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Integer, 0, zero_reg, 0));
    let have_group_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Integer, 0, have_group_reg, 0));

    let prev_key_regs: Vec<i32> = group_offsets.iter().map(|_| reg.alloc()).collect();
    let snapshot_regs: Vec<i32> = (0..total_width).map(|_| reg.alloc()).collect();
    for &r in &snapshot_regs {
        em.emit(Instruction::new(Opcode::Null, 0, r, 0));
    }

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

    let cur_key_regs: Vec<i32> = group_offsets
        .iter()
        .map(|&offset| {
            let r = reg.alloc();
            em.emit(Instruction::new(
                Opcode::Column,
                pseudo_cursor,
                i32::try_from(offset).unwrap_or(0),
                r,
            ));
            r
        })
        .collect();

    let group_key_p4s: Vec<P4> = select
        .group_by
        .iter()
        .map(|expr| {
            let collation = collation_of(expr)
                .or_else(|| expr_collation(full_scope, expr))
                .unwrap_or(Collation::Binary);
            let affinity = comparison_affinity(expr_affinity(full_scope, expr), None);
            p4_coll_seq(collation, affinity)
        })
        .collect();

    let boundary_label = em.new_label();
    let not_boundary_label = em.new_label();
    let first_row_check = em.emit(Instruction::new(Opcode::Eq, have_group_reg, 0, zero_reg));
    em.patch_p2(first_row_check, boundary_label);
    for ((&cur, &prev), p4) in cur_key_regs.iter().zip(&prev_key_regs).zip(&group_key_p4s) {
        let a_null = em.new_label();
        let same_col = em.new_label();
        let a_null_addr = em.emit(Instruction::new(Opcode::IsNull, cur, 0, 0));
        em.patch_p2(a_null_addr, a_null);
        let b_null_addr = em.emit(Instruction::new(Opcode::IsNull, prev, 0, 0));
        em.patch_p2(b_null_addr, boundary_label);
        let eq_addr = em.emit(Instruction::with_p4(Opcode::Eq, cur, 0, prev, p4.clone()));
        em.patch_p2(eq_addr, same_col);
        let goto_boundary = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
        em.patch_p2(goto_boundary, boundary_label);
        em.place(a_null);
        let b_not_null_addr = em.emit(Instruction::new(Opcode::NotNull, prev, 0, 0));
        em.patch_p2(b_not_null_addr, boundary_label);
        em.place(same_col);
    }
    let goto_not_boundary = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
    em.patch_p2(goto_not_boundary, not_boundary_label);

    em.place(boundary_label);
    let skip_flush = em.new_label();
    let flush_check = em.emit(Instruction::new(Opcode::Eq, have_group_reg, 0, zero_reg));
    em.patch_p2(flush_check, skip_flush);
    flush_joined_group(
        em,
        reg,
        select,
        full_scope,
        dedup_star,
        total_width,
        flush_cursor,
        &snapshot_regs,
        &agg_slots,
        limit.as_ref(),
        end_label,
        order_cursors.map(|(order_sort_cursor, _)| order_sort_cursor),
        sink,
    )?;
    em.place(skip_flush);
    for (&cur, &prev) in cur_key_regs.iter().zip(&prev_key_regs) {
        em.emit(Instruction::new(Opcode::Copy, cur, prev, 0));
    }
    em.emit(Instruction::new(Opcode::Integer, 1, have_group_reg, 0));
    for agg in &agg_slots {
        emit_joined_agg_step(em, reg, full_scope, pseudo_cursor, agg, true)?;
    }
    let after_accumulate = em.new_label();
    let goto_after_accumulate = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
    em.patch_p2(goto_after_accumulate, after_accumulate);

    em.place(not_boundary_label);
    for agg in &agg_slots {
        emit_joined_agg_step(em, reg, full_scope, pseudo_cursor, agg, false)?;
    }

    em.place(after_accumulate);
    for (idx, &dest) in snapshot_regs.iter().enumerate() {
        em.emit(Instruction::new(
            Opcode::Column,
            pseudo_cursor,
            i32::try_from(idx).unwrap_or(0),
            dest,
        ));
    }

    let sorted_next = em.emit(Instruction::new(Opcode::SorterNext, sort_cursor, 0, 0));
    em.patch_p2(sorted_next, sorted_loop);

    if implicit_group {
        em.place(empty_sorter_target);
    }
    let skip_tail_flush = em.new_label();
    if !implicit_group {
        let tail_check = em.emit(Instruction::new(Opcode::Eq, have_group_reg, 0, zero_reg));
        em.patch_p2(tail_check, skip_tail_flush);
    }
    flush_joined_group(
        em,
        reg,
        select,
        full_scope,
        dedup_star,
        total_width,
        flush_cursor,
        &snapshot_regs,
        &agg_slots,
        limit.as_ref(),
        end_label,
        order_cursors.map(|(order_sort_cursor, _)| order_sort_cursor),
        sink,
    )?;
    em.place(skip_tail_flush);

    // Pass 3 (#502): every finalized group row is now buffered in
    // `order_sort_cursor`, keyed by the `ORDER BY` targets resolved
    // above. Drain it sorted, applying `LIMIT`/`OFFSET` here — not at
    // group-flush time — since the final order (and therefore which
    // rows a `LIMIT` keeps) isn't known until this sort completes.
    // Mirrors `compile_joined_sorted_scan`'s plain-`ORDER BY` pass 2.
    if let Some((order_sort_cursor, order_pseudo_cursor)) = order_cursors {
        let order_sort_addr = em.emit(Instruction::new(
            Opcode::SorterSort,
            order_sort_cursor,
            0,
            0,
        ));
        em.patch_p2(order_sort_addr, end_label);

        let order_loop = em.new_label();
        em.place(order_loop);
        let order_data_reg = reg.alloc();
        em.emit(Instruction::new(
            Opcode::SorterData,
            order_sort_cursor,
            order_data_reg,
            0,
        ));
        em.emit(Instruction::new(
            Opcode::OpenPseudo,
            order_pseudo_cursor,
            order_data_reg,
            0,
        ));

        let order_row_skip = em.new_label();
        if let Some(limit) = &limit {
            emit_offset_guard(em, limit, order_row_skip);
        }
        if let Some(limit) = &limit {
            emit_limit_guard(em, limit, end_label);
        }
        let (first, count) = project_grouped_result_columns(
            em,
            reg,
            select,
            full_scope,
            dedup_star,
            total_width,
            &agg_slots,
            order_pseudo_cursor,
        )?;
        sink(em, reg, first, i32::try_from(count).unwrap_or(0))?;

        em.place(order_row_skip);
        let order_next = em.emit(Instruction::new(
            Opcode::SorterNext,
            order_sort_cursor,
            0,
            0,
        ));
        em.patch_p2(order_next, order_loop);
    }
    Ok(())
}

/// Resolves `expr` (after stripping any `COLLATE` wrapper) to its
/// absolute flat-row offset — the joined counterpart of a `GROUP
/// BY`/aggregate-argument column reference — or `Unsupported` when
/// `expr` isn't a bare (optionally qualified) column, per this module's
/// documented MVP restriction.
fn joined_bare_column_offset(full_scope: &Scope, expr: &Expr) -> Result<usize, CodegenError> {
    let stripped = strip_collate(expr);
    let ExprKind::Column { table, name, .. } = &stripped.kind else {
        return Err(CodegenError::Unsupported {
            reason: "GROUP BY/aggregate arguments combined with a JOIN only support a bare \
                     column today — a computed expression is not yet supported"
                .to_string(),
        });
    };
    let (binding_idx, local_idx) = resolve_scope_column(full_scope, table.as_deref(), name)?;
    Ok(joined_column_offset(full_scope, binding_idx).saturating_add(local_idx))
}

/// [`super::accum::emit_agg_step`]'s joined counterpart: reads
/// `agg.arg`'s value straight off the flat pseudo cursor at its
/// resolved absolute offset (per [`joined_bare_column_offset`]'s
/// bare-column restriction) instead of compiling a general expression
/// against a `Scope`.
fn emit_joined_agg_step(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    full_scope: &Scope,
    pseudo_cursor: i32,
    agg: &AggSlot,
    reset: bool,
) -> Result<(), CodegenError> {
    let (arg_reg, arity, collation) = match &agg.arg {
        Some(expr) => {
            let offset = joined_bare_column_offset(full_scope, expr)?;
            let r = reg.alloc();
            em.emit(Instruction::new(
                Opcode::Column,
                pseudo_cursor,
                i32::try_from(offset).unwrap_or(0),
                r,
            ));
            (
                Some(r),
                1usize,
                collation_of(expr)
                    .or_else(|| expr_collation(full_scope, expr))
                    .unwrap_or(Collation::Binary),
            )
        }
        None => (None, 0usize, Collation::Binary),
    };
    let p2 = arg_reg.unwrap_or(0);
    let mut instr = Instruction::with_p4(
        Opcode::AggStep,
        agg.slot,
        p2,
        0,
        P4::AggFunc {
            name: agg.name.clone(),
            arity,
            collation,
        },
    );
    if reset {
        instr.p5 = 1;
    }
    em.emit(instr);
    Ok(())
}

fn strip_paren(expr: &Expr) -> &Expr {
    match &expr.kind {
        ExprKind::Paren(inner) => strip_paren(inner),
        _ => expr,
    }
}

/// Validates that every result column is `*`/`table.*`/a bare column/
/// exactly one whole aggregate call — this module's documented MVP
/// restriction on what can be re-projected out of the flat, joined
/// sorted buffer.
fn validate_joined_group_projection(
    select: &Select,
    agg_slots: &[AggSlot],
) -> Result<(), CodegenError> {
    for col in &select.columns {
        if let ResultColumn::Expr { expr, .. } = col {
            let stripped = strip_paren(expr);
            if agg_slots.iter().any(|slot| slot.call == *stripped) {
                continue;
            }
            if matches!(stripped.kind, ExprKind::Column { .. }) {
                continue;
            }
            return Err(CodegenError::Unsupported {
                reason: "GROUP BY/aggregate combined with a JOIN only supports `*`/`table.*`/\
                         bare column/whole aggregate-call result columns today — an aggregate \
                         nested inside a larger expression is not yet supported"
                    .to_string(),
            });
        }
    }
    Ok(())
}

/// [`super::accum::flush_group`]'s joined counterpart: finalizes one
/// group's row — `snapshot_regs` (the group's last-seen flat joined
/// row) plus each aggregate's finalized value — into a fresh
/// `total_width + agg_slots.len()`-wide record.
///
/// When `order_sort_cursor` is `None` (no `ORDER BY`), that record is
/// reprojected into `select`'s result columns and handed to `sink`
/// directly, applying `limit`/`end_label` per group exactly as before
/// #502. When `order_sort_cursor` is `Some` (#502), `limit` is ignored
/// here — the record is inserted as-is into that second sorter
/// instead, and [`compile_joined_grouped_scan`]'s pass 3 reprojects and
/// applies `LIMIT`/`OFFSET` once the final `ORDER BY` order is known,
/// same as [`compile_joined_sorted_scan`]'s plain-`ORDER BY` pass 2.
#[allow(clippy::too_many_arguments)]
fn flush_joined_group<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    full_scope: &Scope,
    dedup_star: &[std::collections::HashSet<String>],
    total_width: usize,
    flush_cursor: i32,
    snapshot_regs: &[i32],
    agg_slots: &[AggSlot],
    limit: Option<&LimitState>,
    end_label: Label,
    order_sort_cursor: Option<i32>,
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    let synthetic_count = total_width.saturating_add(agg_slots.len());
    let dests: Vec<i32> = (0..synthetic_count).map(|_| reg.alloc()).collect();
    let synthetic_first = dests.first().copied().unwrap_or_else(|| reg.alloc());
    for (&snap, &dest) in snapshot_regs.iter().zip(&dests) {
        em.emit(Instruction::new(Opcode::Copy, snap, dest, 0));
    }
    let agg_dests = dests.get(total_width..).unwrap_or(&[]);
    for (agg, &dest) in agg_slots.iter().zip(agg_dests) {
        let arity = usize::from(agg.arg.is_some());
        em.emit(Instruction::with_p4(
            Opcode::AggFinal,
            agg.slot,
            0,
            dest,
            P4::Str(format!("{}({arity})", agg.name)),
        ));
    }
    let record_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::MakeRecord,
        synthetic_first,
        i32::try_from(synthetic_count).unwrap_or(0),
        record_reg,
    ));

    if let Some(order_sort_cursor) = order_sort_cursor {
        em.emit(Instruction::new(
            Opcode::SorterInsert,
            order_sort_cursor,
            record_reg,
            0,
        ));
        return Ok(());
    }

    em.emit(Instruction::new(
        Opcode::OpenPseudo,
        flush_cursor,
        record_reg,
        0,
    ));

    let skip_label = em.new_label();
    if let Some(limit) = limit {
        emit_offset_guard(em, limit, skip_label);
    }
    if let Some(limit) = limit {
        emit_limit_guard(em, limit, end_label);
    }

    let (first, count) = project_grouped_result_columns(
        em,
        reg,
        select,
        full_scope,
        dedup_star,
        total_width,
        agg_slots,
        flush_cursor,
    )?;
    sink(em, reg, first, i32::try_from(count).unwrap_or(0))?;
    em.place(skip_label);
    Ok(())
}

/// Reprojects `select`'s result columns from `cursor` — a pseudo
/// cursor over a `total_width + agg_slots.len()`-wide record (raw
/// joined columns followed by finalized aggregate values, the same
/// layout [`flush_joined_group`] assembles) — per
/// [`validate_joined_group_projection`]'s `*`/`table.*`/bare-column/
/// whole-aggregate-call restriction. Shared by `flush_joined_group`'s
/// no-`ORDER BY` path and [`compile_joined_grouped_scan`]'s pass 3
/// (#502), since both read back the identical row shape.
#[allow(clippy::too_many_arguments)]
fn project_grouped_result_columns(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    full_scope: &Scope,
    dedup_star: &[std::collections::HashSet<String>],
    total_width: usize,
    agg_slots: &[AggSlot],
    cursor: i32,
) -> Result<(i32, usize), CodegenError> {
    let mut regs = Vec::new();
    let read_offset = |em: &mut Emitter, reg: &mut RegAlloc, abs: usize| -> i32 {
        let r = reg.alloc();
        em.emit(Instruction::new(
            Opcode::Column,
            cursor,
            i32::try_from(abs).unwrap_or(0),
            r,
        ));
        r
    };
    for col in &select.columns {
        match col {
            ResultColumn::Star => {
                for (i, binding) in full_scope.tables.iter().enumerate() {
                    let suppressed = dedup_star.get(i);
                    let base = joined_column_offset(full_scope, i);
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
                let i = full_scope
                    .tables
                    .iter()
                    .position(|b| b.matches_qualifier(table))
                    .ok_or_else(|| CodegenError::UnknownColumn {
                        name: format!("{table}.*"),
                    })?;
                let base = joined_column_offset(full_scope, i);
                let count = full_scope
                    .tables
                    .get(i)
                    .map(|b| b.schema.columns.len())
                    .unwrap_or(0);
                for idx in 0..count {
                    regs.push(read_offset(em, reg, base.saturating_add(idx)));
                }
            }
            ResultColumn::Expr { expr, .. } => {
                let stripped = strip_paren(expr);
                if let Some(pos) = agg_slots.iter().position(|slot| slot.call == *stripped) {
                    regs.push(read_offset(em, reg, total_width.saturating_add(pos)));
                    continue;
                }
                let ExprKind::Column { table, name, .. } = &stripped.kind else {
                    return Err(CodegenError::Unsupported {
                        reason: "GROUP BY/aggregate combined with a JOIN only supports `*`/\
                                 `table.*`/bare column/whole aggregate-call result columns today"
                            .to_string(),
                    });
                };
                let (binding_idx, local_idx) =
                    resolve_scope_column(full_scope, table.as_deref(), name)?;
                let abs = joined_column_offset(full_scope, binding_idx).saturating_add(local_idx);
                regs.push(read_offset(em, reg, abs));
            }
        }
    }
    let Some(&first) = regs.first() else {
        let r = reg.alloc();
        return Ok((r, 0));
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

/// Structural (span-independent) match between an `ORDER BY` term and
/// one already-collected `agg_slots` entry: same function name, same
/// `DISTINCT`-ness, and — since every aggregate argument is restricted
/// to a bare column (or absent, for `count(*)`) — the same resolved
/// column offset. See [`resolve_group_order_target`]'s doc for why
/// plain `Expr` equality doesn't work here.
fn matches_agg_slot(
    full_scope: &Scope,
    stripped: &Expr,
    slot: &AggSlot,
) -> Result<bool, CodegenError> {
    let ExprKind::FunctionCall {
        name,
        distinct,
        args,
    } = &stripped.kind
    else {
        return Ok(false);
    };
    if !name.eq_ignore_ascii_case(&slot.name) || *distinct != slot.eph_cursor.is_some() {
        return Ok(false);
    }
    match (args, &slot.arg) {
        (FunctionArgs::Star, None) => Ok(true),
        (FunctionArgs::List(list), None) => Ok(list.is_empty()),
        (FunctionArgs::List(list), Some(slot_arg)) => {
            let [arg_expr] = list.as_slice() else {
                return Ok(false);
            };
            let a = joined_bare_column_offset(full_scope, arg_expr)?;
            let b = joined_bare_column_offset(full_scope, slot_arg)?;
            Ok(a == b)
        }
        (FunctionArgs::Star, Some(_)) => Ok(false),
    }
}

/// Resolves one `ORDER BY` term to its absolute offset within a
/// finalized group row (`total_width` raw joined columns followed by
/// `agg_slots.len()` finalized aggregate values) — #502's counterpart
/// to [`joined_bare_column_offset`] for the post-`GROUP BY` sort key.
/// An ordinal or alias recurses into the referenced/aliased result
/// column's own expression; per
/// [`validate_joined_group_projection`]'s restriction, every legal
/// result column expression is itself either a bare column or a whole
/// aggregate call, so this always bottoms out in one of those two
/// cases (or a clean `Unsupported` for anything else, e.g. an `ORDER
/// BY` expression that isn't a bare column or a bare aggregate call).
fn resolve_group_order_target(
    select: &Select,
    full_scope: &Scope,
    agg_slots: &[AggSlot],
    total_width: usize,
    expr: &Expr,
) -> Result<usize, CodegenError> {
    let stripped = strip_paren(strip_collate(expr));
    // Unlike `validate_joined_group_projection`/`project_grouped_result_
    // columns` (which only ever compare a `select.columns` expr against
    // `agg_slots` built from that very same list, so plain `Expr`
    // equality — spans included — always matches by construction), an
    // `ORDER BY` term is a separately parsed `Expr` even when it's the
    // exact same aggregate call textually (different `Span`), so it
    // needs a structural match instead of `==`.
    for (pos, slot) in agg_slots.iter().enumerate() {
        if matches_agg_slot(full_scope, stripped, slot)? {
            return Ok(total_width.saturating_add(pos));
        }
    }
    if let ExprKind::Literal(Literal::Integer(n)) = &stripped.kind {
        let ordinal = usize::try_from(*n)
            .ok()
            .and_then(|n| n.checked_sub(1))
            .and_then(|idx| select.columns.get(idx));
        let Some(ResultColumn::Expr { expr: target, .. }) = ordinal else {
            return Err(CodegenError::Unsupported {
                reason: format!(
                    "ORDER BY position {n} is out of range for a {}-column result set",
                    select.columns.len()
                ),
            });
        };
        return resolve_group_order_target(select, full_scope, agg_slots, total_width, target);
    }
    if let ExprKind::Column {
        table: None, name, ..
    } = &stripped.kind
    {
        if let Some(ResultColumn::Expr {
            expr: aliased_expr, ..
        }) = select
            .columns
            .iter()
            .find(|c| matches!(c, ResultColumn::Expr { alias: Some(a), .. } if a == name))
        {
            return resolve_group_order_target(
                select,
                full_scope,
                agg_slots,
                total_width,
                aliased_expr,
            );
        }
    }
    joined_bare_column_offset(full_scope, stripped)
}
