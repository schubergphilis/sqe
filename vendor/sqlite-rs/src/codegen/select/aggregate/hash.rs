// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Hash-based `GROUP BY` codegen (#570) — the O(n) alternative to
//! [`super::compile_grouped_scan`]'s sort-then-group strategy, sharing
//! that function's pass 1 verbatim in shape and its per-group flush
//! (`flush_group`) verbatim in code.

use super::super::limit_scan::compile_limit_setup;
use super::super::order_by::{order_by_target_for_expr, OrderByTarget};
use super::super::*;
use super::{
    collect_aggregates, columns_needed_for_projection, flush_group, read_row_columns_into, AggSlot,
};

/// This spike's own pre-#665 copy of what was `compile_grouped_scan`'s
/// `compile_row_values_pruned`: one register per `schema` column in
/// declared order (a real read for a column in `needed`, a cheap `Null`
/// placeholder otherwise), so `group_keys`/`group_key`'s below — which
/// still index by raw original `schema` column index, matching this
/// file's "identical record layout" doc comment — stay correct. Kept
/// deliberately un-migrated to #665's true-narrowing scheme: this
/// function is unwired from GROUP BY dispatch (#631) and only kept for
/// possible reuse, so it isn't worth threading the same
/// `compact_schema`/`compact_index_map` translation through a dead path.
fn compile_row_values_pruned(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    schema: &TableSchema,
    needed: &std::collections::HashSet<usize>,
    cursor: i32,
) -> Result<i32, CodegenError> {
    let mut first = None;
    for idx in 0..schema.columns.len() {
        let r = reg.alloc();
        first.get_or_insert(r);
        if needed.contains(&idx) {
            emit_column_read(em, schema, cursor, idx, r)?;
        } else {
            em.emit(Instruction::new(Opcode::Null, 0, r, 0));
        }
    }
    Ok(first.unwrap_or_else(|| reg.alloc()))
}

/// Compiles an explicit `GROUP BY` as a single-pass hash aggregation
/// (#570): each WHERE-matching row is folded straight into its group's
/// accumulators at scan time (`HashAggFind` + one `HashAggStep` per
/// aggregate call), and the groups are then walked once
/// (`HashAggRewind`/`HashAggData`/`HashAggNext`) and flushed through the
/// very same [`flush_group`] the sort strategy uses.
///
/// The win over [`super::compile_grouped_scan`] is asymptotic, not
/// constant-factor: the sort strategy buffers all `n` rows and sorts
/// them, O(n log n), purely so a group's rows end up adjacent; folding
/// into a hash table needs no adjacency at all, so the build is O(n) and
/// only the `K` groups (not the `n` rows) are ever ordered — see
/// `crate::vdbe::hash_agg`'s module doc for why they are ordered at all.
/// Memory is strictly better too: one retained row per *group* instead
/// of one buffered row per *row*.
///
/// Returns `Ok(true)` when this path was taken; `Ok(false)` leaves
/// `em`/`reg` untouched so the caller falls back to
/// [`super::compile_grouped_scan`], which stays the always-correct
/// general path (spec 001's Tier 3 "simplifiable, not droppable").
/// Deliberate narrowings, each because the sort strategy has something
/// this one structurally does not:
/// - No explicit `GROUP BY` key (#287's implicit whole-table group):
///   one group, so there is nothing to hash and nothing to save.
/// - A `DISTINCT` aggregate (`count(DISTINCT x)`): `emit_agg_step`
///   dedups through a per-slot ephemeral index reopened on each group's
///   *boundary* row, which only exists when a group's rows are
///   adjacent. Interleaved rows would need one dedup set per live
///   group; until that exists, these fall back to the sorter.
#[allow(clippy::too_many_arguments, clippy::too_many_lines, dead_code, unused)]
pub(in crate::codegen::select) fn try_compile_hash_grouped_scan<F>(
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
    if select.group_by.is_empty() {
        return Ok(false);
    }
    let aggs = collect_aggregates(select)?;
    if aggs.iter().any(|(_, _, _, distinct)| *distinct) {
        return Ok(false);
    }

    let table_scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    // Same #322 hoist as the sort strategy: an uncorrelated WHERE-clause
    // subquery is materialized once here rather than once per scanned
    // row.
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

    // Aggregate-context slots are a disjoint table from the register
    // file, addressed by their own small integer space — a bare 0-based
    // counter, not `reg.alloc()`. `eph_cursor` is always `None` here:
    // `DISTINCT` aggregates were rejected above.
    let agg_slots: Vec<AggSlot> = aggs
        .into_iter()
        .enumerate()
        .map(|(slot, (call, name, arg, _))| AggSlot {
            call,
            name,
            arg,
            slot: i32::try_from(slot).unwrap_or(0),
            eph_cursor: None,
        })
        .collect();

    // The hash table reuses the sort cursor's number: `SorterOpen` never
    // runs on this branch, the same convention
    // `try_compile_index_ordered_group_by` uses for its index cursor.
    let hash_cursor = cursors.sort;
    let open_addr = em.emit(Instruction::with_p4(
        Opcode::HashAggOpen,
        hash_cursor,
        0,
        0,
        P4::None,
    ));

    // The one and only pass over the table: filter, project, fold.
    let scan_rewind = em.emit(Instruction::new(Opcode::Rewind, cursors.table, 0, 0));
    let scan_done = em.new_label();
    em.patch_p2(scan_rewind, scan_done);
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
    let first = compile_row_values_pruned(em, reg, schema, &needed_columns, cursors.table)?;

    // Identical record layout to the sort strategy's pass 1: every
    // schema column in declared order, then one trailing register per
    // computed (non-bare-column) GROUP BY expression. Keeping the layout
    // identical is what lets the group row be read back through an
    // ordinary `OpenPseudo` cursor below.
    let mut group_keys = Vec::with_capacity(group_targets.len());
    for (expr, target) in select.group_by.iter().zip(&group_targets) {
        let index = match target {
            OrderByTarget::Column(idx) => *idx,
            OrderByTarget::Expr(e) => {
                let r = compile_value(em, reg, &table_scope, e)?;
                usize::try_from(r.saturating_sub(first)).unwrap_or(0)
            }
        };
        // The same collation and comparison affinity the sort strategy
        // puts on its group-boundary `Eq` — see `crate::vdbe::hash_agg`
        // for why hash equality has to agree with that comparison
        // exactly.
        let collation = collation_of(expr)
            .or_else(|| expr_collation(&table_scope, expr))
            .unwrap_or(Collation::Binary);
        let affinity = comparison_affinity(expr_affinity(&table_scope, expr), None);
        group_keys.push(crate::vdbe::GroupKeyColumn {
            index,
            collation,
            affinity: affinity.to_p4_byte(),
        });
    }
    em.patch_p4(open_addr, P4::GroupKey(group_keys));

    let count = usize::try_from(reg.peek().saturating_sub(first)).unwrap_or(0);
    let record_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::MakeRecord,
        first,
        i32::try_from(count).unwrap_or(0),
        record_reg,
    ));
    // `P3` names the record's own source-register run, so `HashAggFind`
    // reads the group key straight out of registers instead of decoding
    // `record_reg`'s blob back again — the `P4::GroupKey` column indices
    // index both identically, by construction.
    em.emit(Instruction::new(
        Opcode::HashAggFind,
        hash_cursor,
        record_reg,
        first,
    ));
    for agg in &agg_slots {
        emit_hash_agg_step(em, reg, &table_scope, hash_cursor, agg)?;
    }

    em.place(scan_skip);
    let scan_next = em.emit(Instruction::new(Opcode::Next, cursors.table, 0, 0));
    em.patch_p2(scan_next, scan_loop);

    // Walk the groups. `compile_limit_setup` has to run outside the loop
    // (it initializes the counter registers `flush_group`'s guards
    // decrement), exactly as it does in the sort strategy.
    em.place(scan_done);
    let limit = compile_limit_setup(em, reg, &table_scope, select)?;
    let snapshot_regs: Vec<i32> = schema.columns.iter().map(|_| reg.alloc()).collect();

    // `OpenPseudo` records only `cursors.pseudo -> group_row_reg` (the
    // register index, not its value), so it runs once, before the loop;
    // `HashAggData` refreshes the register's contents per group.
    let group_row_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::OpenPseudo,
        cursors.pseudo,
        group_row_reg,
        0,
    ));
    // An explicit `GROUP BY` over zero matching rows produces zero
    // groups, so an empty table jumps straight past every flush — the
    // implicit whole-table group's "still emit one row" case never
    // reaches this path (rejected above).
    let rewind_addr = em.emit(Instruction::new(Opcode::HashAggRewind, hash_cursor, 0, 0));
    em.patch_p2(rewind_addr, end_label);

    let group_loop = em.new_label();
    em.place(group_loop);
    em.emit(Instruction::new(
        Opcode::HashAggData,
        hash_cursor,
        group_row_reg,
        0,
    ));
    read_row_columns_into(em, schema, cursors.pseudo, &snapshot_regs)?;
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
    let group_next = em.emit(Instruction::new(Opcode::HashAggNext, hash_cursor, 0, 0));
    em.patch_p2(group_next, group_loop);
    Ok(true)
}

/// Emits one `HashAggStep` for `agg`'s slot — the hash-table
/// counterpart of [`super::emit_agg_step`], compiling the same argument
/// expression against the same scope and handing the same `P4::AggFunc`
/// descriptor to the same `crate::vdbe::aggregate::step` kernel. The
/// only differences are the target (the located group's accumulators
/// rather than the VM-wide context slot) and the absence of a `reset`
/// flag: a hash table gives each group its own accumulator by
/// construction, so there is no reused slot to discard.
fn emit_hash_agg_step(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    scope: &Scope,
    hash_cursor: i32,
    agg: &AggSlot,
) -> Result<(), CodegenError> {
    let (arg_reg, arity, collation) = match &agg.arg {
        Some(expr) => {
            let collation = collation_of(expr)
                .or_else(|| expr_collation(scope, expr))
                .unwrap_or(Collation::Binary);
            (
                Some(compile_value(em, reg, scope, expr)?),
                1usize,
                collation,
            )
        }
        None => (None, 0usize, Collation::Binary),
    };
    em.emit(Instruction::with_p4(
        Opcode::HashAggStep,
        agg.slot,
        arg_reg.unwrap_or(0),
        hash_cursor,
        P4::AggFunc {
            name: agg.name.clone(),
            arity,
            collation,
        },
    ));
    Ok(())
}
