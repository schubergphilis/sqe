// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::limit_scan::{compile_limit_setup, emit_limit_guard, emit_offset_guard, LimitState};
use super::order_by::{OrderByPlan, OrderByTarget};
use super::projection::{compile_row_values, emit_row_via_sink, ResultColumnPlan};
use super::*;
use crate::codegen::index_maintenance::valid_index_root_page;

/// Finds a single index on `schema` whose declared column order is a
/// prefix match (case-insensitively, column-for-column) for `plans` — the
/// resolved `ORDER BY` term list — either forward (the index's own
/// ascending/descending declaration for each column already matches the
/// requested direction) or backward (every column's requested direction
/// is the exact reverse of the index's declaration). Returns the index
/// plus whether walking it needs to go forward (ascending b-tree order)
/// or backward (descending) to produce `plans`' requested order.
///
/// Deliberately narrow (#296 MVP): only a bare column target (no
/// computed `ORDER BY` expression), `BINARY` collation only (matching
/// `src/btree/index.rs`'s own BINARY-only key comparison — a `NOCASE`/
/// other-collation `ORDER BY` can't be satisfied by walking the index's
/// raw byte order), and a `NULLS FIRST`/`NULLS LAST` clause that agrees
/// with the direction's default (an explicit clause overriding that
/// default has no way to be expressed by a plain b-tree walk). A mixed
/// per-column direction across `plans` that doesn't uniformly match (or
/// uniformly reverse) the index's own per-column directions also falls
/// through to `None` — the sorter path handles it instead.
pub(super) fn index_matches_ordering(
    schema: &TableSchema,
    index: &IndexSchema,
    plans: &[OrderByPlan],
) -> Option<bool> {
    if index.columns.len() < plans.len() {
        return None;
    }
    match index_ordering_prefix(schema, index, plans) {
        Some((len, forward)) if len == plans.len() => Some(forward),
        _ => None,
    }
}

/// Graded version of [`index_matches_ordering`] (#574): rather than
/// all-or-nothing, returns the length of the leading run of `plans` that
/// `index`'s declared column order satisfies (forward or backward,
/// uniformly across the run) plus that run's direction. A full match is
/// just the case where the returned length equals `plans.len()` — that's
/// exactly what `index_matches_ordering` checks for. `None` when not even
/// the first `ORDER BY` term is satisfied.
pub(super) fn index_ordering_prefix(
    schema: &TableSchema,
    index: &IndexSchema,
    plans: &[OrderByPlan],
) -> Option<(usize, bool)> {
    let mut forward: Option<bool> = None;
    let mut prefix_len = 0usize;
    for (i, plan) in plans.iter().enumerate() {
        let Some(index_col) = index.columns.get(i) else {
            break;
        };
        if plan.collation != Collation::Binary {
            break;
        }
        if plan.nulls_first == plan.descending {
            // An explicit NULLS clause overriding the direction's
            // default can't be expressed by a raw b-tree walk.
            break;
        }
        let OrderByTarget::Column(col_idx) = &plan.target else {
            break;
        };
        let Some(col_name) = schema.columns.get(*col_idx) else {
            break;
        };
        if !index_col.name.eq_ignore_ascii_case(col_name) {
            break;
        }
        let this_forward = plan.descending == index_col.desc;
        match forward {
            None => forward = Some(this_forward),
            Some(f) if f == this_forward => {}
            _ => break,
        }
        prefix_len = i.saturating_add(1);
    }
    forward.filter(|_| prefix_len > 0).map(|f| (prefix_len, f))
}

/// Finds the index whose declared column order satisfies the *longest*
/// strict prefix of `plans` — strictly shorter than `plans.len()`, since a
/// full match is [`find_ordering_index`]'s job, not this one. Used by
/// [`try_compile_partial_sorted_index_scan`] (#574) to walk that index for
/// the already-satisfied prefix and only sort the remaining suffix,
/// per prefix-group, instead of sorting the whole result set.
pub(super) fn find_partial_ordering_index(
    schema: &TableSchema,
    plans: &[OrderByPlan],
) -> Option<(usize, usize, bool)> {
    if plans.len() < 2 {
        // A single-term ORDER BY has no suffix left once its one term is
        // satisfied — that's a full match, not a partial one.
        return None;
    }
    schema
        .indexes
        .iter()
        .enumerate()
        .filter_map(|(i, index)| {
            let (len, forward) = index_ordering_prefix(schema, index, plans)?;
            (len < plans.len()).then_some((i, len, forward))
        })
        .max_by_key(|&(_, len, _)| len)
}

pub(super) fn find_ordering_index(
    schema: &TableSchema,
    plans: &[OrderByPlan],
) -> Option<(usize, bool)> {
    if plans.is_empty() {
        return None;
    }
    schema.indexes.iter().enumerate().find_map(|(i, index)| {
        index_matches_ordering(schema, index, plans).map(|forward| (i, forward))
    })
}

/// Compiles `SELECT ... ORDER BY <indexed col(s)> [DESC] LIMIT n [OFFSET
/// m]` (#296) as a direct index b-tree walk — `IdxRewind`/`IdxNext`
/// (forward) or `IdxLast`/`IdxPrev` (backward), `IdxRowid` +
/// `SeekRowid` to fetch the full row, LIMIT/OFFSET as an early-exit
/// guard during the walk — in place of `compile_sorted_scan`'s
/// `Rewind`/`Next` + sorter pipeline. No buffering, no sort, ever.
///
/// Returns `Ok(true)` when this fast path was taken; `Ok(false)` leaves
/// `em`/`reg` untouched so the caller falls back to
/// [`super::limit_scan::compile_sorted_scan`]. MVP guardrail (matching
/// the issue's bounded scope): only taken when there's no `WHERE` clause
/// this index couldn't also serve — since this MVP does no cardinality
/// estimation, "no WHERE at all" is the conservative stand-in for that
/// — and no `DISTINCT`/`WITHOUT ROWID` table (this path leans on
/// `SeekRowid`, which needs an ordinary rowid table).
#[allow(clippy::too_many_arguments)]
pub(super) fn try_compile_index_ordered_scan<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    order_by_plans: &[OrderByPlan],
    cursors: ScanCursors,
    end_label: Label,
    catalog: &[TableSchema],
    sink: &mut F,
) -> Result<bool, CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    if select.where_clause.is_some() {
        return Ok(false);
    }
    if matches!(select.distinct, Some(Distinctness::Distinct)) {
        return Ok(false);
    }
    if schema.without_rowid {
        return Ok(false);
    }
    let Some((index_idx, forward)) = find_ordering_index(schema, order_by_plans) else {
        return Ok(false);
    };
    let Some(index) = schema.indexes.get(index_idx) else {
        return Ok(false);
    };

    // No dedicated cursor slot exists for this path's index cursor —
    // reuse the sort cursor number, since `compile_sorted_scan`'s
    // `SorterOpen`/`SorterInsert` never run on this branch.
    let index_cursor = cursors.sort;
    let root_page = valid_index_root_page(index)?;
    let mut open_instr = Instruction::new(Opcode::OpenRead, index_cursor, root_page, 0);
    open_instr.p5 = 1;
    em.emit(open_instr);

    let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    let limit = compile_limit_setup(em, reg, &scope, select)?;

    let (rewind_op, next_op) = if forward {
        (Opcode::IdxRewind, Opcode::IdxNext)
    } else {
        (Opcode::IdxLast, Opcode::IdxPrev)
    };
    let rewind_addr = em.emit(Instruction::new(rewind_op, index_cursor, 0, 0));
    em.patch_p2(rewind_addr, end_label);
    let loop_start = em.new_label();
    em.place(loop_start);

    let row_skip = em.new_label();
    let rowid_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::IdxRowid,
        index_cursor,
        rowid_reg,
        0,
    ));
    let table_seek_addr = em.emit(Instruction::new(
        Opcode::SeekRowid,
        cursors.table,
        0,
        rowid_reg,
    ));
    em.patch_p2(table_seek_addr, row_skip);

    if let Some(limit) = &limit {
        emit_offset_guard(em, limit, row_skip);
    }
    if let Some(limit) = &limit {
        emit_limit_guard(em, limit, end_label);
    }
    emit_row_via_sink(em, reg, select, schema, cursors.table, false, catalog, sink)?;

    em.place(row_skip);
    let next_addr = em.emit(Instruction::new(next_op, index_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);
    Ok(true)
}

/// Drains the current group's sorter (#574): sorts it, walks it emitting
/// every row through `sink` under the (global, cross-group) LIMIT/OFFSET
/// guards, then falls through once exhausted. A no-op-shaped empty
/// group (never actually produced by
/// [`try_compile_partial_sorted_index_scan`], which only calls this once
/// a group has at least one buffered row) still degrades safely: jumps
/// straight past the drain loop rather than to `end_label` — an empty
/// *group* is not the same thing as an empty *result set*, which is
/// instead handled by the outer index-emptiness check before any group
/// ever starts.
#[allow(clippy::too_many_arguments)]
fn drain_sorted_group<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    cursors: ScanCursors,
    limit: &Option<LimitState>,
    end_label: Label,
    catalog: &[TableSchema],
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    let empty_label = em.new_label();
    let sort_addr = em.emit(Instruction::new(Opcode::SorterSort, cursors.sort, 0, 0));
    em.patch_p2(sort_addr, empty_label);

    let sorted_loop = em.new_label();
    em.place(sorted_loop);
    let sorter_data_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::SorterData,
        cursors.sort,
        sorter_data_reg,
        0,
    ));
    em.emit(Instruction::new(
        Opcode::OpenPseudo,
        cursors.pseudo,
        sorter_data_reg,
        0,
    ));

    let row_skip = em.new_label();
    if let Some(limit) = limit {
        emit_offset_guard(em, limit, row_skip);
    }
    if let Some(limit) = limit {
        emit_limit_guard(em, limit, end_label);
    }
    emit_row_via_sink(em, reg, select, schema, cursors.pseudo, true, catalog, sink)?;

    em.place(row_skip);
    let sorted_next = em.emit(Instruction::new(Opcode::SorterNext, cursors.sort, 0, 0));
    em.patch_p2(sorted_next, sorted_loop);
    em.place(empty_label);
    Ok(())
}

/// Compiles `SELECT ... ORDER BY <cols> [LIMIT n [OFFSET m]]` (#574) when
/// an index provides a strict, non-empty *prefix* of the requested order
/// — narrower than [`try_compile_index_ordered_scan`]'s all-of-it match.
/// Walks that index directly (`IdxRewind`/`IdxNext` or `IdxLast`/
/// `IdxPrev`, same as that function) and only sorts the unsatisfied
/// `ORDER BY` suffix, re-opening a fresh sorter every time the prefix
/// columns' value changes (a "group boundary", detected the same
/// chained `IsNull`/`Eq` way [`super::aggregate::compile_grouped_scan`]
/// detects a `GROUP BY` boundary) instead of sorting the whole result
/// set in one pass.
///
/// Returns `Ok(true)` when this path was taken; `Ok(false)` leaves
/// `em`/`reg` untouched so the caller falls back to
/// [`super::limit_scan::compile_sorted_scan`]. Same MVP guardrails as
/// `try_compile_index_ordered_scan`: no `WHERE` (no cardinality
/// estimation here to judge whether this index still wins under a
/// filter), no `DISTINCT`, and an ordinary rowid table (`SeekRowid`
/// needs one). Unlike `try_compile_index_ordered_scan`, this path's own
/// sorter genuinely runs, so the index cursor reuses the (otherwise
/// unused, since `DISTINCT` is excluded above) `cursors.distinct` slot
/// instead of `cursors.sort`.
#[allow(clippy::too_many_arguments)]
pub(super) fn try_compile_partial_sorted_index_scan<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    order_by_plans: &[OrderByPlan],
    cursors: ScanCursors,
    end_label: Label,
    catalog: &[TableSchema],
    sink: &mut F,
) -> Result<bool, CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    if select.where_clause.is_some() {
        return Ok(false);
    }
    if matches!(select.distinct, Some(Distinctness::Distinct)) {
        return Ok(false);
    }
    if schema.without_rowid {
        return Ok(false);
    }
    let Some((index_idx, prefix_len, forward)) =
        find_partial_ordering_index(schema, order_by_plans)
    else {
        return Ok(false);
    };
    let Some(index) = schema.indexes.get(index_idx) else {
        return Ok(false);
    };
    let Some(suffix_plans) = order_by_plans.get(prefix_len..) else {
        return Ok(false);
    };
    let Some(prefix_columns) = index.columns.get(..prefix_len) else {
        return Ok(false);
    };
    let prefix_p4s: Vec<P4> = prefix_columns
        .iter()
        .map(|c| p4_coll_seq(c.collation, Affinity::Blob))
        .collect();

    let index_cursor = cursors.distinct;
    let root_page = valid_index_root_page(index)?;
    let mut open_instr = Instruction::new(Opcode::OpenRead, index_cursor, root_page, 0);
    open_instr.p5 = 1;
    em.emit(open_instr);

    let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    let limit = compile_limit_setup(em, reg, &scope, select)?;

    let have_group_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Integer, 0, have_group_reg, 0));
    let zero_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Integer, 0, zero_reg, 0));
    let prev_key_regs: Vec<i32> = (0..prefix_len).map(|_| reg.alloc()).collect();

    let (rewind_op, next_op) = if forward {
        (Opcode::IdxRewind, Opcode::IdxNext)
    } else {
        (Opcode::IdxLast, Opcode::IdxPrev)
    };
    let rewind_addr = em.emit(Instruction::new(rewind_op, index_cursor, 0, 0));
    em.patch_p2(rewind_addr, end_label);
    let loop_start = em.new_label();
    em.place(loop_start);

    // This row's prefix-column values, read straight off the index
    // entry — cheap, and needed for the boundary check below whether or
    // not the table row itself turns out to be readable.
    let cur_key_regs: Vec<i32> = (0..prefix_len)
        .map(|i| {
            let r = reg.alloc();
            em.emit(Instruction::new(
                Opcode::Column,
                index_cursor,
                i32::try_from(i).unwrap_or(0),
                r,
            ));
            r
        })
        .collect();

    let row_skip = em.new_label();
    let rowid_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::IdxRowid,
        index_cursor,
        rowid_reg,
        0,
    ));
    let table_seek_addr = em.emit(Instruction::new(
        Opcode::SeekRowid,
        cursors.table,
        0,
        rowid_reg,
    ));
    em.patch_p2(table_seek_addr, row_skip);

    // Buffer this row's full column tuple, plus a trailing register per
    // suffix `ORDER BY` term, into whichever sorter is current when we
    // reach `not_boundary_label` below — mirroring `compile_sorted_scan`'s
    // pass 1, just with a narrower (suffix-only) sort key.
    let (first, _count) = compile_row_values(
        em,
        reg,
        schema,
        &schema
            .columns
            .iter()
            .map(|c| ResultColumnPlan::Column(c.clone()))
            .collect::<Vec<_>>(),
        cursors.table,
        false,
        catalog,
    )?;
    let mut sort_keys = Vec::with_capacity(suffix_plans.len());
    for plan in suffix_plans {
        let key_index = match &plan.target {
            OrderByTarget::Column(idx) => *idx,
            OrderByTarget::Expr(expr) => {
                let r = compile_value(em, reg, &scope, expr)?;
                usize::try_from(r.saturating_sub(first)).unwrap_or(0)
            }
        };
        sort_keys.push(SortKeyColumn {
            index: key_index,
            descending: plan.descending,
            collation: plan.collation,
            nulls_first: plan.nulls_first,
        });
    }
    let record_count = usize::try_from(reg.peek().saturating_sub(first)).unwrap_or(0);
    let record_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::MakeRecord,
        first,
        i32::try_from(record_count).unwrap_or(0),
        record_reg,
    ));

    let boundary_label = em.new_label();
    let merge_label = em.new_label();
    let first_row_check = em.emit(Instruction::new(Opcode::Eq, have_group_reg, 0, zero_reg));
    em.patch_p2(first_row_check, boundary_label);
    for ((&cur, &prev), p4) in cur_key_regs.iter().zip(&prev_key_regs).zip(&prefix_p4s) {
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
    let goto_merge = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
    em.patch_p2(goto_merge, merge_label);

    em.place(boundary_label);
    let skip_flush = em.new_label();
    let flush_check = em.emit(Instruction::new(Opcode::Eq, have_group_reg, 0, zero_reg));
    em.patch_p2(flush_check, skip_flush);
    drain_sorted_group(
        em, reg, select, schema, cursors, &limit, end_label, catalog, sink,
    )?;
    em.place(skip_flush);
    em.emit(Instruction::with_p4(
        Opcode::SorterOpen,
        cursors.sort,
        0,
        0,
        P4::SortKey(sort_keys),
    ));
    for (&cur, &prev) in cur_key_regs.iter().zip(&prev_key_regs) {
        em.emit(Instruction::new(Opcode::Copy, cur, prev, 0));
    }
    em.emit(Instruction::new(Opcode::Integer, 1, have_group_reg, 0));

    em.place(merge_label);
    em.emit(Instruction::new(
        Opcode::SorterInsert,
        cursors.sort,
        record_reg,
        0,
    ));

    em.place(row_skip);
    let next_addr = em.emit(Instruction::new(next_op, index_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);

    // Tail flush: the very last group never sees another row to trigger
    // `boundary_label`'s mid-loop flush.
    let skip_final_flush = em.new_label();
    let final_check = em.emit(Instruction::new(Opcode::Eq, have_group_reg, 0, zero_reg));
    em.patch_p2(final_check, skip_final_flush);
    drain_sorted_group(
        em, reg, select, schema, cursors, &limit, end_label, catalog, sink,
    )?;
    em.place(skip_final_flush);

    Ok(true)
}
