// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
mod accum;
mod hash;
mod join;

use super::limit_scan::compile_limit_setup;
use super::order_by::{order_by_target_for_expr, OrderByPlan, OrderByTarget};
use super::*;
use crate::codegen::index_maintenance::{valid_index_root_page, valid_table_root_page};

pub(crate) use accum::select_has_aggregate;
use accum::FLUSH_CURSOR;
pub(super) use accum::{
    collect_aggregates, emit_agg_step, flush_group, read_pseudo_column, read_row_columns_into,
    AggSlot,
};
// #631 spike: no longer wired into GROUP BY dispatch (see entry.rs),
// kept for possible reuse; `allow` avoids `-D warnings` failing lint.
#[allow(unused_imports)]
pub(in crate::codegen::select) use hash::try_compile_hash_grouped_scan;
pub(crate) use join::compile_joined_grouped_scan;

/// Emits a fast `COUNT(*)` (#444, #543): either a bare `SELECT
/// count(*) FROM t` (no `WHERE`), counted by `Opcode::Count` — summing
/// leaf-page cell counts of the table's own b-tree without opening a
/// cursor or decoding any row (#543; works even without an index) — or
/// `SELECT count(*) FROM t WHERE indexed_col = <literal/param>` against
/// a `UNIQUE` index's leading column (`SeekIndexEq`, a single-entry
/// probe — the count is trivially 0 or 1). Either way, the table cursor
/// is never opened at all.
///
/// Returns `Ok(true)` when this fast path was taken (result row already
/// emitted via `sink`); `Ok(false)` leaves `em`/`reg` untouched.
/// Deliberately narrow: `GROUP BY`/`HAVING`/`DISTINCT`/`ORDER BY`/
/// `LIMIT` all fall back to [`compile_grouped_scan`], as does any
/// non-equality or multi-column `WHERE`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn try_compile_index_only_count<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    cursors: ScanCursors,
    catalog: &[TableSchema],
    sink: &mut F,
) -> Result<bool, CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    if select.having.is_some() || select.limit.is_some() || !select.order_by.is_empty() {
        return Ok(false);
    }
    let [ResultColumn::Expr { expr, .. }] = select.columns.as_slice() else {
        return Ok(false);
    };
    let ExprKind::FunctionCall {
        name,
        args,
        distinct,
    } = &expr.kind
    else {
        return Ok(false);
    };
    if *distinct || !name.eq_ignore_ascii_case("count") || !matches!(args, FunctionArgs::Star) {
        return Ok(false);
    }

    let index_cursor = cursors.sort;
    let count_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Integer, 0, count_reg, 0));

    match &select.where_clause {
        None => {
            let root_page = valid_table_root_page(schema)?;
            em.emit(Instruction::new(Opcode::Count, root_page, count_reg, 0));
        }
        Some(where_expr) => {
            let Some((lhs, rhs)) = super::limit_scan::top_level_equality_operands(where_expr)
            else {
                return Ok(false);
            };
            fn where_col(expr: &Expr) -> Option<&str> {
                match &expr.kind {
                    ExprKind::Column { name, .. } => Some(name.as_str()),
                    _ => None,
                }
            }
            let (where_col_name, operand) = match (where_col(lhs), where_col(rhs)) {
                (Some(name), _) => (name, rhs),
                (_, Some(name)) => (name, lhs),
                _ => return Ok(false),
            };
            let is_supported_operand = matches!(
                &operand.kind,
                ExprKind::Literal(Literal::Integer(_))
                    | ExprKind::Param(ParamKind::Anonymous | ParamKind::Numbered(_))
            );
            if !is_supported_operand {
                return Ok(false);
            }
            // Uniqueness is no longer required (#450): a non-`UNIQUE`
            // index's leading-column match may have duplicate-key
            // siblings, walked below via `IdxNext` + a leading-column
            // recheck instead of assuming a single hit means count 1.
            let Some(index) = schema.indexes.iter().find(|idx| {
                idx.columns
                    .first()
                    .is_some_and(|c| c.name.eq_ignore_ascii_case(where_col_name))
            }) else {
                return Ok(false);
            };
            let root_page = valid_index_root_page(index)?;
            let mut open_instr = Instruction::new(Opcode::OpenRead, index_cursor, root_page, 0);
            open_instr.p5 = 1;
            em.emit(open_instr);

            let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
            let value_reg = compile_value(em, reg, &scope, operand)?;
            let leading_collation = index
                .columns
                .first()
                .map_or(Collation::Binary, |c| c.collation);
            let miss_label = em.new_label();
            let seek_addr = em.emit(Instruction::with_p4(
                Opcode::SeekIndexEq,
                index_cursor,
                0,
                value_reg,
                P4::SeekKey(vec![leading_collation]),
            ));
            em.patch_p2(seek_addr, miss_label);

            let loop_start = em.new_label();
            em.place(loop_start);
            let one_reg = reg.alloc();
            em.emit(Instruction::new(Opcode::Integer, 1, one_reg, 0));
            em.emit(Instruction::new(Opcode::Add, one_reg, count_reg, count_reg));

            // A `UNIQUE` index's single match falls straight through
            // here on the very first `IdxNext` (nothing shares its
            // key); a non-`UNIQUE` index's duplicate-key siblings loop
            // back to `loop_start`, incrementing once per match.
            let next_addr = em.emit(Instruction::new(Opcode::IdxNext, index_cursor, 0, 0));
            let recheck = em.new_label();
            em.patch_p2(next_addr, recheck);
            let exhausted = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
            em.patch_p2(exhausted, miss_label);

            em.place(recheck);
            let leading = reg.alloc();
            em.emit(Instruction::new(Opcode::Column, index_cursor, 0, leading));
            // The leading index column's declared `COLLATE` (#500),
            // matching `SeekIndexEq`'s own probe comparison just above.
            let eq_addr = em.emit(Instruction::with_p4(
                Opcode::Eq,
                leading,
                0,
                value_reg,
                p4_coll_seq(leading_collation, Affinity::Blob),
            ));
            em.patch_p2(eq_addr, loop_start);

            em.place(miss_label);
        }
    }

    sink(em, reg, count_reg, 1)?;
    Ok(true)
}

/// Emits an index-only `SUM(col)`/`AVG(col)` (#544): when `col` is the
/// leading column of some index on `schema` and there's no `WHERE`
/// clause restricting which rows are summed, walks that index's
/// b-tree end to end (`IdxRewind`/`IdxNext`), reading each entry's
/// leading column straight off the index cursor and folding it into
/// the ordinary `AggStep` accumulator (`crate::vdbe::aggregate`) —
/// the table cursor is never opened at all.
///
/// Returns `Ok(true)` when this fast path was taken (result row
/// already emitted via `sink`); `Ok(false)` leaves `em`/`reg`
/// untouched. Deliberately narrow: any `WHERE`/`GROUP BY`/`HAVING`/
/// `DISTINCT`/`ORDER BY`/`LIMIT`, a non-bare-column argument, or a
/// table with no matching index all fall back to
/// [`compile_grouped_scan`].
pub(crate) fn try_compile_index_only_sum<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    cursors: ScanCursors,
    sink: &mut F,
) -> Result<bool, CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    if select.where_clause.is_some()
        || select.having.is_some()
        || select.limit.is_some()
        || !select.order_by.is_empty()
        || !select.group_by.is_empty()
    {
        return Ok(false);
    }
    let [ResultColumn::Expr { expr, .. }] = select.columns.as_slice() else {
        return Ok(false);
    };
    let ExprKind::FunctionCall {
        name,
        args,
        distinct,
    } = &expr.kind
    else {
        return Ok(false);
    };
    if *distinct || !(name.eq_ignore_ascii_case("sum") || name.eq_ignore_ascii_case("avg")) {
        return Ok(false);
    }
    let FunctionArgs::List(list) = args else {
        return Ok(false);
    };
    let [arg] = list.as_slice() else {
        return Ok(false);
    };
    let ExprKind::Column { name: col_name, .. } = &arg.kind else {
        return Ok(false);
    };
    let Some(index) = schema.indexes.iter().find(|idx| {
        idx.columns
            .first()
            .is_some_and(|c| c.name.eq_ignore_ascii_case(col_name))
    }) else {
        return Ok(false);
    };

    let index_cursor = cursors.sort;
    let root_page = valid_index_root_page(index)?;
    let mut open_instr = Instruction::new(Opcode::OpenRead, index_cursor, root_page, 0);
    open_instr.p5 = 1;
    em.emit(open_instr);

    let agg_name = name.to_ascii_lowercase();
    let leading_collation = index
        .columns
        .first()
        .map_or(Collation::Binary, |c| c.collation);

    let done_label = em.new_label();
    let rewind_addr = em.emit(Instruction::new(Opcode::IdxRewind, index_cursor, 0, 0));
    em.patch_p2(rewind_addr, done_label);
    let loop_start = em.new_label();
    em.place(loop_start);

    let value_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Column, index_cursor, 0, value_reg));
    em.emit(Instruction::with_p4(
        Opcode::AggStep,
        0,
        value_reg,
        0,
        P4::AggFunc {
            name: agg_name.clone(),
            arity: 1,
            collation: leading_collation,
        },
    ));

    let next_addr = em.emit(Instruction::new(Opcode::IdxNext, index_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);
    em.place(done_label);

    let final_reg = reg.alloc();
    em.emit(Instruction::with_p4(
        Opcode::AggFinal,
        0,
        0,
        final_reg,
        P4::Str(format!("{agg_name}(1)")),
    ));

    sink(em, reg, final_reg, 1)?;
    Ok(true)
}

/// Emits a `Sorter`-free implicit-whole-table-group aggregate (#633):
/// when there's no `GROUP BY` key at all (#287's implicit whole-table
/// group) and no aggregate call uses `DISTINCT`, every WHERE-matching
/// row belongs to the same single group, so there is nothing to sort
/// by — a single linear `Rewind`/`Next` scan calling `AggStep` inline
/// produces the identical result [`compile_grouped_scan`]'s
/// buffer-then-flush machinery does, without ever opening a `Sorter`
/// (no `MakeRecord`/`SorterInsert`/`SorterSort`/`SorterData` round
/// trip per row).
///
/// Reuses [`flush_group`]/[`AggSlot`]/`compile_limit_setup` so
/// `HAVING`, `LIMIT`/`OFFSET`, and #287's "a zero-row table still
/// flushes one row" behavior (`count(*) = 0`, other aggregates `NULL`)
/// all come from the same code the `Sorter`-backed path uses.
///
/// Returns `Ok(true)` when this fast path was taken (result row
/// already emitted via `sink`); `Ok(false)` leaves `em`/`reg`
/// untouched — a `DISTINCT` aggregate falls back to
/// [`compile_grouped_scan`]. Only called from the implicit-group
/// (`select.group_by.is_empty()`) branch; an explicit `GROUP BY`
/// always has `compile_grouped_scan`'s sort-then-group semantics.
#[allow(clippy::too_many_arguments)]
pub(super) fn try_compile_direct_agg_scan<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    cursors: ScanCursors,
    end_label: Label,
    catalog: &[TableSchema],
    sink: &mut F,
) -> Result<bool, CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    let aggs = collect_aggregates(select)?;
    if aggs.iter().any(|(_, _, _, distinct)| *distinct) {
        return Ok(false);
    }

    let table_scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    // #322: hoist any uncorrelated WHERE-clause subquery once, up
    // front — see `compile_grouped_scan`'s identical comment.
    let hoisted = match &select.where_clause {
        Some(where_expr) => crate::codegen::subquery::hoist_uncorrelated_where_subqueries(
            em,
            reg,
            &table_scope,
            where_expr,
        )?,
        None => std::collections::HashMap::new(),
    };
    let table_scope = table_scope.with_hoisted(std::rc::Rc::new(hoisted));

    let limit = compile_limit_setup(em, reg, &table_scope, select)?;

    let zero_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Integer, 0, zero_reg, 0));
    let have_group_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Integer, 0, have_group_reg, 0));

    let snapshot_regs: Vec<i32> = schema.columns.iter().map(|_| reg.alloc()).collect();
    // NULL-initialized up front so a zero-row (or zero-match) table's
    // tail flush reads NULL for any plain column — see
    // `compile_grouped_scan`'s identical comment.
    for &r in &snapshot_regs {
        em.emit(Instruction::new(Opcode::Null, 0, r, 0));
    }

    let agg_slots: Vec<AggSlot> = aggs
        .into_iter()
        .enumerate()
        .map(|(slot, (call, name, arg, _distinct))| AggSlot {
            call,
            name,
            arg,
            slot: i32::try_from(slot).unwrap_or(0),
            eph_cursor: None,
        })
        .collect();

    let scan_rewind = em.emit(Instruction::new(Opcode::Rewind, cursors.table, 0, 0));
    let tail_label = em.new_label();
    em.patch_p2(scan_rewind, tail_label);
    let scan_loop = em.new_label();
    em.place(scan_loop);

    let scan_skip = em.new_label();
    if let Some(where_expr) = &select.where_clause {
        compile_cond(
            em,
            reg,
            &table_scope,
            where_expr,
            CondTargets::null_is_false(Target::Fallthrough, Target::Jump(scan_skip)),
        )?;
    }

    let boundary_label = em.new_label();
    let not_boundary_label = em.new_label();
    let first_row_check = em.emit(Instruction::new(Opcode::Eq, have_group_reg, 0, zero_reg));
    em.patch_p2(first_row_check, boundary_label);
    let goto_not_boundary = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
    em.patch_p2(goto_not_boundary, not_boundary_label);

    em.place(boundary_label);
    em.emit(Instruction::new(Opcode::Integer, 1, have_group_reg, 0));
    // This table's first matching row: fold with `reset: true` so a
    // freshly-numbered slot starts a fresh accumulator — see
    // `compile_grouped_scan`'s identical comment.
    for agg in &agg_slots {
        emit_agg_step(em, reg, &table_scope, agg, true)?;
    }
    // The single implicit group's "arbitrary row" for any plain
    // (non-aggregate) result/`HAVING` column is its first matching
    // row, matching `compile_grouped_scan`'s choice — snapshotted
    // straight off the real table cursor (not `read_row_columns_into`,
    // which is only safe against the pass-2 pseudo cursor's
    // already-materialized record; a rowid-alias column here still
    // needs `Opcode::Rowid` against the live table cursor).
    for (idx, &r) in snapshot_regs.iter().enumerate() {
        emit_column_read(em, schema, cursors.table, idx, r)?;
    }
    let after_accumulate = em.new_label();
    let goto_after_accumulate = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
    em.patch_p2(goto_after_accumulate, after_accumulate);

    em.place(not_boundary_label);
    for agg in &agg_slots {
        emit_agg_step(em, reg, &table_scope, agg, false)?;
    }

    em.place(after_accumulate);
    em.place(scan_skip);
    let scan_next = em.emit(Instruction::new(Opcode::Next, cursors.table, 0, 0));
    em.patch_p2(scan_next, scan_loop);

    // Tail flush: always exactly one row (#287), whether or not any
    // row ever matched — `have_group_reg`/`snapshot_regs`' NULL
    // initialization and `AggFinal`'s never-stepped-slot handling
    // produce `count(*) = 0`/other aggregates `NULL` when it didn't.
    em.place(tail_label);
    flush_group(
        em,
        reg,
        select,
        schema,
        catalog,
        &snapshot_regs,
        &agg_slots,
        limit.as_ref(),
        end_label,
        sink,
    )?;
    Ok(true)
}

/// #506: which of `schema`'s columns [`compile_grouped_scan`]'s pass 1
/// actually needs to serialize into the sort record — the `GROUP BY`
/// key, every aggregate argument, and every plain column `select.columns`/
/// `select.having` reads (via the pass-2 pseudo cursor's "arbitrary row"
/// snapshot, see `read_row_columns_into`). A schema column that's part
/// of neither is dead weight through the sort pipeline: read off the
/// real cursor, `MakeRecord`-encoded, `SorterInsert`-decoded, and
/// `read_row_columns_into`-decoded again, without ever being asked for.
///
/// Conservative by construction: any `*`/`table.*` result column, or a
/// column reference this walk can't fully account for (namely, a
/// subquery-bearing expression — mirroring
/// `correlation::subquery_is_correlated`'s same "correlated=true is
/// always the safe default" stance), returns every column rather than
/// guessing wrong. Returning "every column" here is exactly
/// [`compile_grouped_scan`]'s pre-#506 behavior, so this never makes a
/// query behave differently from before — only cheaper.
fn columns_needed_for_projection(
    select: &Select,
    schema: &TableSchema,
) -> std::collections::HashSet<usize> {
    let all_columns = || (0..schema.columns.len()).collect();

    let mut names = std::collections::HashSet::new();
    let mut bail = false;
    for expr in &select.group_by {
        walk_expr_for_column_refs(expr, &mut names, &mut bail);
    }
    for col in &select.columns {
        match col {
            ResultColumn::Expr { expr, .. } => {
                walk_expr_for_column_refs(expr, &mut names, &mut bail)
            }
            ResultColumn::Star | ResultColumn::TableStar { .. } => bail = true,
        }
    }
    if let Some(having) = &select.having {
        walk_expr_for_column_refs(having, &mut names, &mut bail);
    }
    if bail {
        return all_columns();
    }

    let mut indices = std::collections::HashSet::with_capacity(names.len());
    for name in &names {
        match schema
            .columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(name))
        {
            Some(idx) => {
                indices.insert(idx);
            }
            // Should always resolve at this point in codegen — an
            // unknown column would already have failed earlier
            // validation. Fall back to "every column" rather than
            // silently dropping one we can't place, on the off chance
            // this is reached some other way.
            None => return all_columns(),
        }
    }
    indices
}

/// #506's `needed` set, ordered ascending by original `schema` column
/// index — this order becomes the sort record's own column layout, so
/// every pass-2 index-based pseudo-cursor read needs translating
/// through the position each original index lands at here.
fn ordered_needed_columns(
    needed: &std::collections::HashSet<usize>,
    schema: &TableSchema,
) -> Vec<usize> {
    (0..schema.columns.len())
        .filter(|i| needed.contains(i))
        .collect()
}

/// The reverse of [`ordered_needed_columns`]: `result[orig_idx]` is the
/// compacted record's position for that original `schema` column index,
/// or `None` if it was pruned away entirely. Sized to `schema.columns.len()`
/// so every lookup is a plain (safe, bounds-checked) `.get()`.
fn compact_index_map(needed_order: &[usize], schema_len: usize) -> Vec<Option<usize>> {
    let mut map = vec![None; schema_len];
    for (pos, &orig) in needed_order.iter().enumerate() {
        if let Some(slot) = map.get_mut(orig) {
            *slot = Some(pos);
        }
    }
    map
}

/// #665: a synthetic `TableSchema` covering only `needed_order`'s
/// columns, in that order — so [`Scope::single`]'s ordinary name-based
/// column resolution (`crate::codegen::expr::column_index`) automatically
/// maps a column name to its *compacted* position, with no per-call-site
/// index translation needed for any expression compiled through it
/// (`compile_value`/`compile_cond` against the pass-2 pseudo cursor).
/// Only the handful of call sites that address a column by raw original
/// `schema` index (not by name) still need [`compact_index_map`]
/// directly — see [`compile_grouped_scan`]'s pass 2.
fn compact_schema(schema: &TableSchema, needed_order: &[usize]) -> TableSchema {
    TableSchema {
        unresolved_autoindex: false,
        name: schema.name.clone(),
        root_page: 0,
        columns: needed_order
            .iter()
            .map(|&i| schema.columns.get(i).cloned().unwrap_or_default())
            .collect(),
        without_rowid: schema.without_rowid,
        strict: false,
        column_types: needed_order
            .iter()
            .map(|&i| schema.column_types.get(i).cloned().unwrap_or_default())
            .collect(),
        column_collations: needed_order
            .iter()
            .map(|&i| {
                schema
                    .column_collations
                    .get(i)
                    .copied()
                    .unwrap_or(Collation::Binary)
            })
            .collect(),
        is_virtual: false,
        sql: String::new(),
        indexes: Vec::new(),
        rowid_alias: schema
            .rowid_alias
            .and_then(|orig| needed_order.iter().position(|&i| i == orig)),
    }
}

/// #665: pass 1's `MakeRecord` source registers — a real `Column`/
/// `Rowid` read for exactly `needed_order`'s columns, in that order, and
/// nothing at all for any other column (no register, no placeholder).
/// This is the actual sort-record width reduction #506 stopped short of
/// (see that ticket's doc on [`compact_schema`]/[`compact_index_map`]
/// for why it's safe now): pass 2 resolves every column reference
/// against a [`compact_schema`]-backed scope/index map instead of
/// assuming the record still mirrors `schema`'s full column layout.
/// Returns the first allocated register (mirrors `compile_row_values`'s
/// return), or a fresh unused register if `needed_order` is empty.
fn compile_row_values_compact(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    schema: &TableSchema,
    needed_order: &[usize],
    cursor: i32,
) -> Result<i32, CodegenError> {
    let mut first = None;
    for &idx in needed_order {
        let r = reg.alloc();
        first.get_or_insert(r);
        emit_column_read(em, schema, cursor, idx, r)?;
    }
    Ok(first.unwrap_or_else(|| reg.alloc()))
}

/// Same exhaustive `ExprKind` traversal shape as
/// `subquery::correlation::walk_expr_for_correlation` — see that
/// function's doc for why a subquery-bearing expression bails
/// conservatively rather than being reasoned through.
fn walk_expr_for_column_refs(
    expr: &Expr,
    names: &mut std::collections::HashSet<String>,
    bail: &mut bool,
) {
    if *bail {
        return;
    }
    match &expr.kind {
        ExprKind::Column { name, .. } => {
            names.insert(name.to_ascii_lowercase());
        }
        ExprKind::Literal(_) | ExprKind::Param(_) => {}
        ExprKind::FunctionCall { args, .. } => {
            if let FunctionArgs::List(list) = args {
                for a in list {
                    walk_expr_for_column_refs(a, names, bail);
                }
            }
        }
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. }
        | ExprKind::Collate { expr: e, .. }
        | ExprKind::Paren(e) => walk_expr_for_column_refs(e, names, bail),
        ExprKind::Binary { lhs, rhs, .. } | ExprKind::Is { lhs, rhs, .. } => {
            walk_expr_for_column_refs(lhs, names, bail);
            walk_expr_for_column_refs(rhs, names, bail);
        }
        ExprKind::Between {
            expr: e, lo, hi, ..
        } => {
            walk_expr_for_column_refs(e, names, bail);
            walk_expr_for_column_refs(lo, names, bail);
            walk_expr_for_column_refs(hi, names, bail);
        }
        ExprKind::In { expr: e, list, .. } => {
            walk_expr_for_column_refs(e, names, bail);
            for item in list {
                walk_expr_for_column_refs(item, names, bail);
            }
        }
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            ..
        } => {
            walk_expr_for_column_refs(e, names, bail);
            walk_expr_for_column_refs(pattern, names, bail);
            if let Some(esc) = escape {
                walk_expr_for_column_refs(esc, names, bail);
            }
        }
        ExprKind::Case {
            operand,
            whens,
            else_,
        } => {
            if let Some(o) = operand {
                walk_expr_for_column_refs(o, names, bail);
            }
            for (w, t) in whens {
                walk_expr_for_column_refs(w, names, bail);
                walk_expr_for_column_refs(t, names, bail);
            }
            if let Some(e) = else_ {
                walk_expr_for_column_refs(e, names, bail);
            }
        }
        ExprKind::Subquery(_)
        | ExprKind::Exists { .. }
        | ExprKind::InSubquery { .. }
        | ExprKind::InSubqueryMulti { .. } => {
            *bail = true;
        }
    }
}

/// #239: `GROUP BY` / `HAVING`. Strategy mirrors real SQLite's
/// sort-then-group `select.c` shape rather than a hash table, since the
/// `Sorter*` opcode family this compiler already has for `ORDER BY`
/// (see [`compile_sorted_scan`]) does the heavy lifting for free: pass 1
/// sorts every WHERE-matching row by its GROUP BY key, pass 2 walks the
/// sorted stream detecting key changes as group boundaries, accumulating
/// one register (or two, for `avg`) per aggregate call, and flushing a
/// finalized output row through `sink` at each boundary (and once more
/// after the loop, for the final group).
///
/// Known simplifications (documented rather than silently wrong):
/// - `GROUP BY`/`HAVING` combined with `ORDER BY` or `DISTINCT` on the
///   same `SELECT` are rejected outright (see the caller) rather than
///   composed.
/// - Only `count`/`sum`/`avg`/`min`/`max` are supported aggregates;
///   `group_concat`/`string_agg`/`total` are rejected.
/// - Aggregate-call detection only descends through `Paren`/`Collate`/
///   `Unary`/`Binary` wrappers — an aggregate nested inside `CASE`/
///   `BETWEEN`/`IN`/`LIKE` is not found, and compiling it falls through
///   to `compile_value`'s ordinary aggregate-rejection error.
/// - A `GROUP BY`/aggregate-argument expression that itself reads the
///   table's `INTEGER PRIMARY KEY` rowid-alias column mid-expression
///   (not as a bare column) reads the wrong value against the pass-2
///   pseudo cursor — narrow enough (grouping/aggregating by a *bare*
///   rowid-alias column is handled correctly; only a compound
///   expression referencing it is affected) not to block this ticket.
///
/// `select.group_by.is_empty()` is also the entry point for #287's
/// implicit whole-table group: with no `GROUP BY` key at all, every
/// WHERE-matching row belongs to one synthetic group (`group_targets`
/// is empty, so the boundary check below only ever fires on the very
/// first row). The one place that still needs to differ from an
/// explicit `GROUP BY ()`-shaped zero-row result is the *tail* flush:
/// an explicit `GROUP BY` over zero matching rows correctly produces
/// zero groups, but a whole-table aggregate with zero matching rows
/// still produces exactly one row (`count(*) = 0`, other aggregates
/// `NULL`) — `implicit_group` selects that behavior.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) fn compile_grouped_scan<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    cursors: ScanCursors,
    end_label: Label,
    catalog: &[TableSchema],
    implicit_group: bool,
    outer_scope: Option<&Scope>,
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    let mut table_scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    if let Some(outer) = outer_scope {
        table_scope = table_scope.with_outer(outer.clone());
    }
    // #322: hoist any uncorrelated WHERE-clause IN/scalar (including
    // aggregate, #304) subquery out of pass 1's scan loop below,
    // materializing it exactly once here rather than once per
    // WHERE-matching row — #306 already did this for
    // `compile_direct_scan`/`compile_sorted_scan`, but never for this
    // aggregate/GROUP BY scan, so a WHERE-clause aggregate subquery over
    // the same table (e.g. `WHERE x > (SELECT avg(x) FROM t)`) was
    // re-scanning the whole table once per row here — O(n^2), severe
    // enough to hit the VDBE step guard rail on a real-sized table.
    let hoisted = match &select.where_clause {
        Some(where_expr) => crate::codegen::subquery::hoist_uncorrelated_where_subqueries(
            em,
            reg,
            &table_scope,
            where_expr,
        )?,
        None => std::collections::HashMap::new(),
    };
    let table_scope = table_scope.with_hoisted(std::rc::Rc::new(hoisted));
    let group_targets: Vec<OrderByTarget> = select
        .group_by
        .iter()
        .map(|expr| order_by_target_for_expr(expr, schema))
        .collect::<Result<_, _>>()?;
    let needed_columns = columns_needed_for_projection(select, schema);
    // #665: `needed_order`/`compact_of` are the sort record's actual
    // (narrowed) column layout and the original-index -> that-layout
    // translation table; `pseudo_scope` resolves by name against
    // `compact_schema` (so `compile_value`/`emit_agg_step`'s GROUP BY
    // and aggregate-argument expressions need no translation at all —
    // only the couple of by-index reads below do.
    let needed_order = ordered_needed_columns(&needed_columns, schema);
    let compact_of = compact_index_map(&needed_order, schema.columns.len());
    let pseudo_schema = compact_schema(schema, &needed_order);
    let mut pseudo_scope =
        Scope::single(&pseudo_schema, cursors.pseudo).with_catalog(catalog.to_vec());
    if let Some(outer) = outer_scope {
        pseudo_scope = pseudo_scope.with_outer(outer.clone());
    }

    // Pass 1: buffer every WHERE-matching row's needed column values
    // (#665: only the columns `needed_order` names, not the full row),
    // plus a trailing register per computed (non-bare-column) GROUP BY
    // expression, sorted by the GROUP BY key — identical in shape to
    // `compile_sorted_scan`'s ORDER BY pass 1.
    let sorter_open_addr = em.emit(Instruction::with_p4(
        Opcode::SorterOpen,
        cursors.sort,
        0,
        0,
        P4::None,
    ));

    let scan_rewind = em.emit(Instruction::new(Opcode::Rewind, cursors.table, 0, 0));
    let sort_step = em.new_label();
    em.patch_p2(scan_rewind, sort_step);
    let scan_loop = em.new_label();
    em.place(scan_loop);

    let scan_skip = em.new_label();
    if let Some(where_expr) = &select.where_clause {
        compile_cond(
            em,
            reg,
            &table_scope,
            where_expr,
            CondTargets::null_is_false(Target::Fallthrough, Target::Jump(scan_skip)),
        )?;
    }
    let first = compile_row_values_compact(em, reg, schema, &needed_order, cursors.table)?;

    let mut sort_keys = Vec::with_capacity(group_targets.len());
    for (expr, target) in select.group_by.iter().zip(&group_targets) {
        let index = match target {
            // Always resolves: `needed_columns` includes every column
            // `select.group_by` references (`columns_needed_for_projection`),
            // so `idx` always has a compacted position.
            OrderByTarget::Column(idx) => compact_of.get(*idx).copied().flatten().unwrap_or(0),
            OrderByTarget::Expr(e) => {
                let r = compile_value(em, reg, &table_scope, e)?;
                usize::try_from(r.saturating_sub(first)).unwrap_or(0)
            }
        };
        sort_keys.push(SortKeyColumn {
            index,
            descending: false,
            collation: collation_of(expr)
                .or_else(|| expr_collation(&table_scope, expr))
                .unwrap_or(Collation::Binary),
            nulls_first: true,
        });
    }
    em.patch_p4(sorter_open_addr, P4::SortKey(sort_keys));

    let count = usize::try_from(reg.peek().saturating_sub(first)).unwrap_or(0);
    let record_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::MakeRecord,
        first,
        i32::try_from(count).unwrap_or(0),
        record_reg,
    ));
    // `first..first+count` (this row's `MakeRecord` source run) is still
    // live here — `record_reg` was allocated after it — so `SorterInsert`
    // can read its own sort key's values straight out of those registers
    // instead of decoding them back out of the blob it's also handed;
    // see `crate::vdbe::sorter`'s module doc for the `p3`/`p5` contract.
    let mut sorter_insert = Instruction::new(Opcode::SorterInsert, cursors.sort, record_reg, first);
    sorter_insert.p5 = 1;
    em.emit(sorter_insert);

    em.place(scan_skip);
    let scan_next = em.emit(Instruction::new(Opcode::Next, cursors.table, 0, 0));
    em.patch_p2(scan_next, scan_loop);

    // Pass 2: walk the sorted buffer, grouping and aggregating.
    em.place(sort_step);
    let sort_addr = em.emit(Instruction::new(Opcode::SorterSort, cursors.sort, 0, 0));
    // `SorterSort` jumps straight past pass 2 when the sorter is empty
    // (a WHERE-matching-zero-rows table). An explicit `GROUP BY` in
    // that case has zero groups, so `end_label` is correct as-is; the
    // implicit whole-table group (#287) still needs its one all-NULL
    // (count(*) = 0) row, so it jumps to `tail_flush_label` below
    // instead — the same unconditional flush the normal end-of-loop
    // path falls through to.
    let empty_sorter_target = if implicit_group {
        em.new_label()
    } else {
        end_label
    };
    em.patch_p2(sort_addr, empty_sorter_target);

    let limit = compile_limit_setup(em, reg, &table_scope, select)?;

    let aggs = collect_aggregates(select)?;
    let zero_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Integer, 0, zero_reg, 0));
    let have_group_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Integer, 0, have_group_reg, 0));

    let prev_key_regs: Vec<i32> = group_targets.iter().map(|_| reg.alloc()).collect();
    let snapshot_regs: Vec<i32> = schema.columns.iter().map(|_| reg.alloc()).collect();
    // Initialized to NULL up front so the implicit whole-table group's
    // tail flush over a zero-row table (no row ever reaches
    // `read_row_columns_into` to overwrite these) reads NULL for any
    // plain (non-aggregate) column reference, matching SQLite's
    // "arbitrary row" semantics degrading to NULL when there is none.
    // Harmless for the explicit-`GROUP BY` case too: every real group
    // always has at least one row, which unconditionally overwrites
    // these before its flush.
    for &r in &snapshot_regs {
        em.emit(Instruction::new(Opcode::Null, 0, r, 0));
    }
    // Aggregate-context slots (`Vm::agg_contexts`) are a disjoint table
    // from the register file, addressed by their own small integer
    // space — a bare 0-based counter here, not `reg.alloc()`.
    let agg_slots: Vec<AggSlot> = aggs
        .into_iter()
        .enumerate()
        .map(|(slot, (call, name, arg, distinct))| {
            let slot = i32::try_from(slot).unwrap_or(0);
            let eph_cursor = distinct.then(|| FLUSH_CURSOR.saturating_add(1).saturating_add(slot));
            AggSlot {
                call,
                name,
                arg,
                slot,
                eph_cursor,
            }
        })
        .collect();

    // `OpenPseudo` only records `cursors.pseudo -> sorter_data_reg` (the
    // register index, not a snapshot of its value, per
    // `CursorSlot::Pseudo`) — so it only needs to run once, before the
    // loop, not on every row. `SorterData` still runs per-row to refresh
    // the register's contents.
    let sorter_data_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::OpenPseudo,
        cursors.pseudo,
        sorter_data_reg,
        0,
    ));
    let sorted_loop = em.new_label();
    em.place(sorted_loop);
    em.emit(Instruction::new(
        Opcode::SorterData,
        cursors.sort,
        sorter_data_reg,
        0,
    ));

    // Compute this row's GROUP BY key into fresh registers.
    let cur_key_regs: Vec<i32> = group_targets
        .iter()
        .zip(&select.group_by)
        .map(|(target, expr)| match target {
            OrderByTarget::Column(idx) => {
                let r = reg.alloc();
                // Always resolves — see the identical comment on
                // pass 1's `sort_keys` loop above.
                let compact_idx = compact_of.get(*idx).copied().flatten().unwrap_or(0);
                read_pseudo_column(em, &pseudo_schema, cursors.pseudo, compact_idx, r)?;
                Ok(r)
            }
            OrderByTarget::Expr(_) => compile_value(em, reg, &pseudo_scope, expr),
        })
        .collect::<Result<_, CodegenError>>()?;

    let group_key_p4s: Vec<P4> = select
        .group_by
        .iter()
        .map(|expr| {
            let collation = collation_of(expr)
                .or_else(|| expr_collation(&table_scope, expr))
                .unwrap_or(Collation::Binary);
            let affinity = comparison_affinity(expr_affinity(&table_scope, expr), None);
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
    flush_group(
        em,
        reg,
        select,
        schema,
        catalog,
        &snapshot_regs,
        &agg_slots,
        limit.as_ref(),
        end_label,
        sink,
    )?;
    em.place(skip_flush);
    for (&cur, &prev) in cur_key_regs.iter().zip(&prev_key_regs) {
        em.emit(Instruction::new(Opcode::Copy, cur, prev, 0));
    }
    em.emit(Instruction::new(Opcode::Integer, 1, have_group_reg, 0));
    // This is a *new* group's first row: fold it in with `reset: true`
    // so a slot number reused from the previous group starts a fresh
    // accumulator rather than continuing the old one — then skip the
    // plain (non-reset) fold below, which is only for a group's
    // second-and-later rows.
    for agg in &agg_slots {
        emit_agg_step(em, reg, &pseudo_scope, agg, true)?;
    }
    // A plain (non-aggregate) result/`HAVING` column has no aggregate
    // to fold, so it takes on a single "arbitrary row" from the group
    // instead — and the real oracle's own sort-then-group strategy
    // (`select.c`) observably picks the *first* row of the group for
    // that arbitrary choice, not the last. So the snapshot happens
    // exactly once, here, on the group's first (boundary) row; a
    // group's second-and-later rows (`not_boundary_label` below) only
    // fold their aggregates and never touch `snapshot_regs` again.
    //
    // #665: `snapshot_regs` is still one register per *original*
    // `schema` column (unchanged — `flush_group`'s own synthetic schema
    // still zips against it 1:1), but the sort record itself now only
    // has `needed_order`'s columns, so only those get a real read; the
    // rest keep the NULL they were initialized to above (exactly the
    // value they'd have decoded to anyway, since `needed_columns`
    // already covers every column any compiled expression touches).
    for (orig_idx, &dest) in snapshot_regs.iter().enumerate() {
        if let Some(compact_idx) = compact_of.get(orig_idx).copied().flatten() {
            read_pseudo_column(em, &pseudo_schema, cursors.pseudo, compact_idx, dest)?;
        }
    }
    let after_accumulate = em.new_label();
    let goto_after_accumulate = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
    em.patch_p2(goto_after_accumulate, after_accumulate);

    em.place(not_boundary_label);
    for agg in &agg_slots {
        emit_agg_step(em, reg, &pseudo_scope, agg, false)?;
    }

    em.place(after_accumulate);
    let sorted_next = em.emit(Instruction::new(Opcode::SorterNext, cursors.sort, 0, 0));
    em.patch_p2(sorted_next, sorted_loop);

    // Tail flush: the very last group never sees another row to trigger
    // `boundary_label`'s mid-loop flush. An explicit `GROUP BY` over
    // zero matching rows correctly produces zero groups (skip the
    // flush when `have_group_reg` never went high); the implicit
    // whole-table group (#287) always flushes exactly one row, even
    // over an empty table — `count(*)` finalizes to 0, other
    // aggregates to NULL, via `snapshot_regs`'s NULL initialization
    // above and `AggFinal`'s never-stepped-slot handling.
    if implicit_group {
        em.place(empty_sorter_target);
    }
    let skip_tail_flush = em.new_label();
    if !implicit_group {
        let tail_check = em.emit(Instruction::new(Opcode::Eq, have_group_reg, 0, zero_reg));
        em.patch_p2(tail_check, skip_tail_flush);
    }
    flush_group(
        em,
        reg,
        select,
        schema,
        catalog,
        &snapshot_regs,
        &agg_slots,
        limit.as_ref(),
        end_label,
        sink,
    )?;
    em.place(skip_tail_flush);
    Ok(())
}

/// Compiles an explicit `GROUP BY <indexed col(s)>` (#310) as a direct
/// index b-tree walk feeding [`compile_grouped_scan`]'s pass-2
/// boundary-detection/accumulate/flush logic directly, in place of pass
/// 1's `SorterOpen`/full-table-buffer/`SorterSort` — mirroring #296's
/// [`super::index_scan::try_compile_index_ordered_scan`] MVP, but for
/// `GROUP BY` instead of `ORDER BY`. Since the index already produces
/// rows in group-key order, there is nothing to sort: each row is
/// fetched straight off `cursors.table` (via `IdxRowid` + `SeekRowid`,
/// same as the `ORDER BY` fast path) and read directly through
/// `table_scope`, with no pseudo cursor, no `MakeRecord`, and no
/// sorter at all — `cursors.table` plays the role `cursors.pseudo`
/// plays in [`compile_grouped_scan`]'s pass 2, since both are simply
/// "the cursor positioned on the current row" as far as
/// `read_row_columns_into`/[`emit_agg_step`] are concerned.
///
/// Returns `Ok(true)` when this fast path was taken; `Ok(false)` leaves
/// `em`/`reg` untouched so the caller falls back to
/// [`compile_grouped_scan`]. MVP guardrail (matching #296's own): only
/// taken with no `WHERE` clause (no cardinality estimation to judge an
/// index scan against a filtered table scan), an ordinary rowid table,
/// and every `GROUP BY` term a bare column (a computed `GROUP BY`
/// expression has no corresponding index column to match against).
/// Whether `select`'s `GROUP BY` can walk an existing index in key order
/// instead of sorting (`(index_position, forward)` if so) — the shared
/// eligibility check `try_compile_index_ordered_group_by` gates on before
/// compiling, factored out so [`super::eqp::explain_query_plan`] can ask
/// the identical question (whether the compiled program needs a
/// `Sorter`, i.e. a temp b-tree, for this `GROUP BY`) without
/// duplicating — and risking drifting from — this eligibility logic.
pub(super) fn group_by_index_ordering(
    select: &Select,
    schema: &TableSchema,
    implicit_group: bool,
) -> Result<Option<(usize, bool)>, CodegenError> {
    if implicit_group || select.group_by.is_empty() {
        // No GROUP BY key at all: nothing for an index to order by,
        // and #287's implicit whole-table group has exactly one group
        // regardless — sorting a single group is already free.
        return Ok(None);
    }
    if select.where_clause.is_some() || schema.without_rowid {
        return Ok(None);
    }
    let group_targets: Vec<OrderByTarget> = select
        .group_by
        .iter()
        .map(|expr| order_by_target_for_expr(expr, schema))
        .collect::<Result<_, _>>()?;
    // Every target must be a bare column — a computed GROUP BY
    // expression has no corresponding index column to match against.
    // Collected into `group_col_indices` up front (rather than
    // re-matching `OrderByTarget::Column` inside the per-row loop
    // below) so that loop has no "already-rejected, can't happen"
    // branch to justify with an `unreachable!` the qualified-subset
    // gate (`make check-mvl-limit`) doesn't allow.
    let Some(_group_col_indices): Option<Vec<usize>> = group_targets
        .iter()
        .map(|t| match t {
            OrderByTarget::Column(idx) => Some(*idx),
            OrderByTarget::Expr(_) => None,
        })
        .collect()
    else {
        return Ok(None);
    };
    let plans: Vec<OrderByPlan> = select
        .group_by
        .iter()
        .zip(&group_targets)
        .map(|(expr, target)| OrderByPlan {
            target: target.clone(),
            descending: false,
            collation: collation_of(expr).unwrap_or_else(|| match target {
                OrderByTarget::Column(idx) => schema
                    .column_collations
                    .get(*idx)
                    .copied()
                    .unwrap_or(Collation::Binary),
                OrderByTarget::Expr(_) => Collation::Binary,
            }),
            nulls_first: true,
        })
        .collect();
    Ok(super::index_scan::find_ordering_index(schema, &plans))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn try_compile_index_ordered_group_by<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    cursors: ScanCursors,
    end_label: Label,
    catalog: &[TableSchema],
    implicit_group: bool,
    sink: &mut F,
) -> Result<bool, CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    let Some((index_idx, forward)) = group_by_index_ordering(select, schema, implicit_group)?
    else {
        return Ok(false);
    };
    let group_targets: Vec<OrderByTarget> = select
        .group_by
        .iter()
        .map(|expr| order_by_target_for_expr(expr, schema))
        .collect::<Result<_, _>>()?;
    // `group_by_index_ordering` already confirmed every target is a
    // bare column (that's what makes it index-orderable) — re-derived
    // here since it doesn't return the indices themselves.
    let Some(group_col_indices): Option<Vec<usize>> = group_targets
        .iter()
        .map(|t| match t {
            OrderByTarget::Column(idx) => Some(*idx),
            OrderByTarget::Expr(_) => None,
        })
        .collect()
    else {
        return Ok(false);
    };
    let Some(index) = schema.indexes.get(index_idx) else {
        return Ok(false);
    };

    let table_scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());

    // No dedicated cursor slot exists for this path's index cursor —
    // reuse the sort cursor number, since `SorterOpen`/`SorterInsert`
    // never run on this branch (matching #296's own convention).
    let index_cursor = cursors.sort;
    let root_page = valid_index_root_page(index)?;
    let mut open_instr = Instruction::new(Opcode::OpenRead, index_cursor, root_page, 0);
    open_instr.p5 = 1;
    em.emit(open_instr);

    let limit = compile_limit_setup(em, reg, &table_scope, select)?;

    let aggs = collect_aggregates(select)?;
    let zero_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Integer, 0, zero_reg, 0));
    let have_group_reg = reg.alloc();
    em.emit(Instruction::new(Opcode::Integer, 0, have_group_reg, 0));

    let prev_key_regs: Vec<i32> = group_targets.iter().map(|_| reg.alloc()).collect();
    let snapshot_regs: Vec<i32> = schema.columns.iter().map(|_| reg.alloc()).collect();
    for &r in &snapshot_regs {
        em.emit(Instruction::new(Opcode::Null, 0, r, 0));
    }
    let agg_slots: Vec<AggSlot> = aggs
        .into_iter()
        .enumerate()
        .map(|(slot, (call, name, arg, distinct))| {
            let slot = i32::try_from(slot).unwrap_or(0);
            let eph_cursor = distinct.then(|| FLUSH_CURSOR.saturating_add(1).saturating_add(slot));
            AggSlot {
                call,
                name,
                arg,
                slot,
                eph_cursor,
            }
        })
        .collect();

    let (rewind_op, next_op) = if forward {
        (Opcode::IdxRewind, Opcode::IdxNext)
    } else {
        (Opcode::IdxLast, Opcode::IdxPrev)
    };
    let empty_index_target = end_label;
    let rewind_addr = em.emit(Instruction::new(rewind_op, index_cursor, 0, 0));
    em.patch_p2(rewind_addr, empty_index_target);

    let indexed_loop = em.new_label();
    em.place(indexed_loop);
    let rowid_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::IdxRowid,
        index_cursor,
        rowid_reg,
        0,
    ));
    let row_skip = em.new_label();
    let table_seek_addr = em.emit(Instruction::new(
        Opcode::SeekRowid,
        cursors.table,
        0,
        rowid_reg,
    ));
    em.patch_p2(table_seek_addr, row_skip);

    // Compute this row's GROUP BY key straight off the table cursor —
    // `group_col_indices` (checked above) means this is always a plain
    // column read, never `compile_value`.
    let cur_key_regs: Vec<i32> = group_col_indices
        .iter()
        .map(|&idx| {
            let r = reg.alloc();
            read_pseudo_column(em, schema, cursors.table, idx, r)?;
            Ok(r)
        })
        .collect::<Result<_, CodegenError>>()?;

    let group_key_p4s: Vec<P4> = select
        .group_by
        .iter()
        .map(|expr| {
            let collation = collation_of(expr)
                .or_else(|| expr_collation(&table_scope, expr))
                .unwrap_or(Collation::Binary);
            let affinity = comparison_affinity(expr_affinity(&table_scope, expr), None);
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
    flush_group(
        em,
        reg,
        select,
        schema,
        catalog,
        &snapshot_regs,
        &agg_slots,
        limit.as_ref(),
        end_label,
        sink,
    )?;
    em.place(skip_flush);
    for (&cur, &prev) in cur_key_regs.iter().zip(&prev_key_regs) {
        em.emit(Instruction::new(Opcode::Copy, cur, prev, 0));
    }
    em.emit(Instruction::new(Opcode::Integer, 1, have_group_reg, 0));
    for agg in &agg_slots {
        emit_agg_step(em, reg, &table_scope, agg, true)?;
    }
    let after_accumulate = em.new_label();
    let goto_after_accumulate = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
    em.patch_p2(goto_after_accumulate, after_accumulate);

    em.place(not_boundary_label);
    for agg in &agg_slots {
        emit_agg_step(em, reg, &table_scope, agg, false)?;
    }

    em.place(after_accumulate);
    read_row_columns_into(em, schema, cursors.table, &snapshot_regs)?;

    em.place(row_skip);
    let idx_next_addr = em.emit(Instruction::new(next_op, index_cursor, 0, 0));
    em.patch_p2(idx_next_addr, indexed_loop);

    // Tail flush: the very last group never sees another row to
    // trigger `boundary_label`'s mid-loop flush. Zero matching rows
    // (an empty table) means `empty_index_target == end_label` was
    // taken above, so this tail flush is only reached having seen at
    // least one row — `have_group_reg` is unconditionally set by then,
    // matching the explicit-`GROUP BY` (non-implicit) case in
    // `compile_grouped_scan`.
    flush_group(
        em,
        reg,
        select,
        schema,
        catalog,
        &snapshot_regs,
        &agg_slots,
        limit.as_ref(),
        end_label,
        sink,
    )?;
    Ok(true)
}
