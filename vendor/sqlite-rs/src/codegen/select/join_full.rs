// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::eqp::table_binding_name;
use super::join_access::{
    compile_join_order_by_sort_keys, emit_full_joined_row, emit_joined_pseudo_projection,
    resolve_join_order_by, JoinOrderPlan,
};
use super::joins::{emit_join_final_row, resolve_join_constraint};
use super::limit_scan::{compile_limit_setup, emit_limit_guard, emit_offset_guard};
use super::*;
use crate::codegen::index_maintenance::valid_table_root_page;
/// #250: `A FULL JOIN B ON cond` (or `USING (...)`/`NATURAL`),
/// restricted to the two-table case — `compile_select_joined` only
/// calls this when `FULL` is the sole join in the `FROM` clause; any
/// other shape (a `FULL JOIN` combined with another join) is rejected
/// there with a clean `Unsupported` error instead.
///
/// `A FULL JOIN B ON cond` is exactly `(A LEFT JOIN B ON cond)` rows,
/// plus any row of `B` matched by no row of `A` at all (null-extended
/// on `A`'s side instead). This is *not* simply `A LEFT JOIN B` unioned
/// with `B LEFT JOIN A` — that would double-count every matched pair —
/// so pass 1 runs the ordinary two-table LEFT JOIN nested loop
/// (mirroring the shape [`compile_join_level`] emits for a plain LEFT
/// JOIN), additionally recording every matched `B` rowid into an
/// ephemeral index the moment `cond` passes (mirroring
/// `emit_distinct_guard`'s `OpenEphemeral`/`Found`/`IdxInsert` dedup
/// mechanism, keyed by `B`'s rowid instead of a result-row tuple); pass
/// 2 then re-scans `B` and emits one `A`-nulled row for every `B`
/// rowid pass 1 never recorded. `WHERE`/`LIMIT`/`OFFSET` apply
/// identically at all three emission points via
/// [`emit_join_final_row`].
///
/// #288: `ORDER BY` and `DISTINCT` are each independently supported on
/// top of this two-pass shape now. `DISTINCT` (no `ORDER BY`) just
/// threads a `distinct_cursor` through the same three
/// [`emit_join_final_row`] call sites — identical to how the ordinary
/// join tree's `DISTINCT` support works, since that function already
/// accepts one. `ORDER BY` instead routes all three emission points
/// through [`emit_full_join_sort_row`], which buffers the full joined
/// row (every column of both tables, `WHERE`-filtered but pre-
/// `LIMIT`/`DISTINCT`) into a sorter cursor — mirroring
/// [`super::join_access::compile_joined_sorted_scan`]'s pass 1/pass 2
/// split for the ordinary join tree. Once both FULL JOIN passes finish,
/// a fourth pass drains the sorter and re-projects `select`'s result
/// columns from the sorted, flat pseudo-record, applying `LIMIT`/
/// `OFFSET` there (post-sort, matching SQLite's own pipeline order).
/// `ORDER BY` and `DISTINCT` combined stay rejected — `compile_select_joined`
/// turns that away before ever reaching this function.
pub(super) fn compile_full_join_two_table(
    select: &Select,
    schemas: &[TableSchema],
    from: &FromClause,
    stats_by_table: &std::collections::HashMap<String, crate::planner::Stats>,
) -> Result<Program, CodegenError> {
    let table_refs: Vec<&TableRef> = std::iter::once(&from.first)
        .chain(from.joins.iter().map(|j| &j.table))
        .collect();

    let mut em = Emitter::new();
    let mut reg = RegAlloc::new();
    let init_addr = em.emit(Instruction::new(Opcode::Init, 0, 0, 0));
    let body_start = em.new_label();
    em.place(body_start);
    em.patch_p2(init_addr, body_start);

    let mut bindings = Vec::with_capacity(2);
    for (i, (table_ref, schema)) in table_refs.iter().zip(schemas.iter()).enumerate() {
        let cursor = i32::try_from(i).unwrap_or(0);
        let root_page = valid_table_root_page(schema)?;
        em.emit(Instruction::new(Opcode::OpenRead, cursor, root_page, 0));
        bindings.push(TableBinding {
            alias: table_ref.alias.clone(),
            name: table_binding_name(table_ref),
            schema: schema.clone(),
            cursor,
            forced_null: false,
            stats: stats_by_table
                .get(&schema.name)
                .cloned()
                .unwrap_or_default(),
        });
    }
    let Some(join) = from.joins.first() else {
        return Err(CodegenError::Unsupported {
            reason: "FULL JOIN codegen only supports a single two-table FULL JOIN today"
                .to_string(),
        });
    };
    let out_of_range = || CodegenError::Unsupported {
        reason: "FULL JOIN codegen only supports a single two-table FULL JOIN today".to_string(),
    };
    let binding_a = bindings.first().cloned().ok_or_else(out_of_range)?;
    let binding_b = bindings.get(1).cloned().ok_or_else(out_of_range)?;

    let mut dedup_star: Vec<std::collections::HashSet<String>> =
        vec![std::collections::HashSet::new(); 2];
    let left = std::slice::from_ref(&binding_a);
    let constraint = resolve_join_constraint(join, left, &binding_b, 1, &mut dedup_star)?;

    let full_scope = Scope {
        tables: bindings.clone(),
        catalog: schemas.to_vec(),
        outer: None,
        dedup_star: dedup_star.clone(),
        ..Scope::default()
    };

    // #288: `ORDER BY` and `DISTINCT` combined stay rejected — the
    // dispatch in `compile_select_joined` already turns that away
    // before ever reaching this function, but the check is repeated
    // here defensively since nothing else guarantees that's the only
    // path in.
    let has_order_by = !select.order_by.is_empty();
    let has_distinct = matches!(select.distinct, Some(Distinctness::Distinct));
    if has_order_by && has_distinct {
        return Err(CodegenError::Unsupported {
            reason: "DISTINCT combined with ORDER BY and a FULL JOIN is not yet supported"
                .to_string(),
        });
    }
    let order_by_plans = if has_order_by {
        resolve_join_order_by(select, &full_scope)?
    } else {
        Vec::new()
    };

    let limit = compile_limit_setup(&mut em, &mut reg, &full_scope, select)?;

    // Ephemeral index tracking every `B` rowid matched during pass 1 —
    // same mechanism `emit_distinct_guard` uses for DISTINCT, keyed by
    // `B`'s rowid instead of a result-row tuple.
    let eph_cursor: i32 = 2;
    em.emit(Instruction::new(Opcode::OpenEphemeral, eph_cursor, 0, 0));

    // `distinct_cursor` (DISTINCT, no ORDER BY) and `sort_cursor`/
    // `pseudo_cursor` (ORDER BY) never coexist — rejected above — so
    // cursor 3 (and 4) are safely reused across either shape.
    let distinct_cursor = (has_distinct && !has_order_by).then_some(3);
    if let Some(dc) = distinct_cursor {
        em.emit(Instruction::new(Opcode::OpenEphemeral, dc, 0, 0));
    }
    let sort_cursor = has_order_by.then_some(3);
    let pseudo_cursor = has_order_by.then_some(4);
    let sorter_open_addr =
        sort_cursor.map(|sc| em.emit(Instruction::with_p4(Opcode::SorterOpen, sc, 0, 0, P4::None)));
    // The sort-key descriptor is identical across all three emission
    // points (same buffered-row layout regardless of which table's
    // half is null-extended), so the `SorterOpen`'s placeholder P4 is
    // patched only once, on whichever emission point buffers a row
    // first.
    let mut sort_key_patched = false;

    let end_label = em.new_label();
    let mut sink = |em: &mut Emitter, _reg: &mut RegAlloc, first: i32, count: i32| {
        em.emit(Instruction::new(Opcode::ResultRow, first, count, 0));
        Ok(())
    };

    let a_cursor = binding_a.cursor;
    let b_cursor = binding_b.cursor;
    let matched = reg.alloc();

    // Pass 1: `A LEFT JOIN B ON cond`, instrumented to record every
    // matched `B` rowid.
    let a_rewind_end = em.new_label();
    let a_rewind = em.emit(Instruction::new(Opcode::Rewind, a_cursor, 0, 0));
    em.patch_p2(a_rewind, a_rewind_end);
    let a_loop = em.new_label();
    em.place(a_loop);

    em.emit(Instruction::new(Opcode::Integer, 0, matched, 0));

    let b_rewind_end = em.new_label();
    let b_rewind = em.emit(Instruction::new(Opcode::Rewind, b_cursor, 0, 0));
    em.patch_p2(b_rewind, b_rewind_end);
    let b_loop = em.new_label();
    em.place(b_loop);

    let b_skip = em.new_label();
    let match_scope = Scope {
        tables: bindings.clone(),
        catalog: schemas.to_vec(),
        outer: None,
        dedup_star: dedup_star.clone(),
        ..Scope::default()
    };
    if let Some(c) = &constraint {
        compile_cond(
            &mut em,
            &mut reg,
            &match_scope,
            c,
            CondTargets::null_is_false(Target::Fallthrough, Target::Jump(b_skip)),
        )?;
    }
    em.emit(Instruction::new(Opcode::Integer, 1, matched, 0));
    let rowid_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Rowid, b_cursor, rowid_reg, 0));
    em.emit(Instruction::with_p4(
        Opcode::IdxInsert,
        eph_cursor,
        rowid_reg,
        0,
        P4::Int(1),
    ));
    if has_order_by {
        emit_full_join_sort_row(
            &mut em,
            &mut reg,
            select,
            &match_scope,
            sort_cursor.unwrap_or(0),
            sorter_open_addr.unwrap_or(0),
            &order_by_plans,
            &mut sort_key_patched,
        )?;
    } else {
        emit_join_final_row(
            &mut em,
            &mut reg,
            select,
            &match_scope,
            end_label,
            limit.as_ref(),
            distinct_cursor,
            &mut sink,
        )?;
    }
    em.place(b_skip);
    let b_next = em.emit(Instruction::new(Opcode::Next, b_cursor, 0, 0));
    em.patch_p2(b_next, b_loop);
    em.place(b_rewind_end);

    let do_null = em.new_label();
    let after_null = em.new_label();
    let addr = em.emit(Instruction::new(Opcode::IfNot, matched, 0, 0));
    em.patch_p2(addr, do_null);
    em.goto(after_null);
    em.place(do_null);
    let mut b_null_bindings = bindings.clone();
    if let Some(b) = b_null_bindings.get_mut(1) {
        b.forced_null = true;
    }
    let b_null_scope = Scope {
        tables: b_null_bindings,
        catalog: schemas.to_vec(),
        outer: None,
        dedup_star: dedup_star.clone(),
        ..Scope::default()
    };
    if has_order_by {
        emit_full_join_sort_row(
            &mut em,
            &mut reg,
            select,
            &b_null_scope,
            sort_cursor.unwrap_or(0),
            sorter_open_addr.unwrap_or(0),
            &order_by_plans,
            &mut sort_key_patched,
        )?;
    } else {
        emit_join_final_row(
            &mut em,
            &mut reg,
            select,
            &b_null_scope,
            end_label,
            limit.as_ref(),
            distinct_cursor,
            &mut sink,
        )?;
    }
    em.place(after_null);

    let a_next = em.emit(Instruction::new(Opcode::Next, a_cursor, 0, 0));
    em.patch_p2(a_next, a_loop);
    em.place(a_rewind_end);

    // Pass 2: one `A`-nulled row for every `B` rowid pass 1 never
    // recorded.
    let b2_rewind_end = em.new_label();
    let b2_rewind = em.emit(Instruction::new(Opcode::Rewind, b_cursor, 0, 0));
    em.patch_p2(b2_rewind, b2_rewind_end);
    let b2_loop = em.new_label();
    em.place(b2_loop);
    let b2_skip = em.new_label();
    let rowid2_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Rowid, b_cursor, rowid2_reg, 0));
    let found_addr = em.emit(Instruction::with_p4(
        Opcode::Found,
        eph_cursor,
        0,
        rowid2_reg,
        P4::Int(1),
    ));
    em.patch_p2(found_addr, b2_skip);
    let mut a_null_bindings = bindings.clone();
    if let Some(a) = a_null_bindings.get_mut(0) {
        a.forced_null = true;
    }
    let a_null_scope = Scope {
        tables: a_null_bindings,
        catalog: schemas.to_vec(),
        outer: None,
        dedup_star: dedup_star.clone(),
        ..Scope::default()
    };
    if has_order_by {
        emit_full_join_sort_row(
            &mut em,
            &mut reg,
            select,
            &a_null_scope,
            sort_cursor.unwrap_or(0),
            sorter_open_addr.unwrap_or(0),
            &order_by_plans,
            &mut sort_key_patched,
        )?;
    } else {
        emit_join_final_row(
            &mut em,
            &mut reg,
            select,
            &a_null_scope,
            end_label,
            limit.as_ref(),
            distinct_cursor,
            &mut sink,
        )?;
    }
    em.place(b2_skip);
    let b2_next = em.emit(Instruction::new(Opcode::Next, b_cursor, 0, 0));
    em.patch_p2(b2_next, b2_loop);
    em.place(b2_rewind_end);

    // #288: once both passes have finished buffering every candidate
    // row into the sorter, drain it in sorted order and re-project
    // `select`'s result columns from the flat pseudo-record — applying
    // `LIMIT`/`OFFSET` here (post-sort), mirroring
    // `compile_joined_sorted_scan`'s own pass 2 for the ordinary join
    // tree.
    if has_order_by {
        let sort_cursor = sort_cursor.unwrap_or(0);
        let pseudo_cursor = pseudo_cursor.unwrap_or(0);
        let sort_addr = em.emit(Instruction::new(Opcode::SorterSort, sort_cursor, 0, 0));
        em.patch_p2(sort_addr, end_label);

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
        if let Some(limit) = &limit {
            emit_offset_guard(&mut em, limit, row_skip);
        }
        if let Some(limit) = &limit {
            emit_limit_guard(&mut em, limit, end_label);
        }
        let (first, count) =
            emit_joined_pseudo_projection(&mut em, &mut reg, select, &full_scope, pseudo_cursor)?;
        sink(&mut em, &mut reg, first, i32::try_from(count).unwrap_or(0))?;

        em.place(row_skip);
        let sorted_next = em.emit(Instruction::new(Opcode::SorterNext, sort_cursor, 0, 0));
        em.patch_p2(sorted_next, sorted_loop);
    }

    em.place(end_label);
    em.emit(Instruction::new(Opcode::Halt, 0, 0, 0));
    Ok(em.finish())
}

/// #288: `ORDER BY` combined with a `FULL JOIN` — buffers one candidate
/// row (`WHERE`-filtered, pre-`LIMIT`/`DISTINCT`) into `sort_cursor`
/// instead of emitting it directly, mirroring
/// [`super::join_access::compile_join_level_for_sort`]'s innermost
/// emission for the ordinary join tree. Called at all three of
/// [`compile_full_join_two_table`]'s emission points (matched,
/// left-nulled, right-unmatched) — `scope` already reflects each
/// branch's forced-null bindings, exactly like
/// [`emit_join_final_row`]'s own three call sites. The `SorterOpen`'s
/// sort-key descriptor is identical across all three (same buffered-row
/// layout regardless of which side is null-extended), so it's patched
/// only once via `patched`.
#[allow(clippy::too_many_arguments)]
fn emit_full_join_sort_row(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    scope: &Scope,
    sort_cursor: i32,
    sorter_open_addr: usize,
    order_by_plans: &[JoinOrderPlan],
    patched: &mut bool,
) -> Result<(), CodegenError> {
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
    if !*patched {
        em.patch_p4(sorter_open_addr, P4::SortKey(sort_keys));
        *patched = true;
    }
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
}
