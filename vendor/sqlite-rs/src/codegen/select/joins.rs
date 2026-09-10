// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
mod level;

use super::aggregate::{compile_joined_grouped_scan, select_has_aggregate};
use super::eqp::table_binding_name;
use super::join_access::{compile_joined_sorted_scan, resolve_join_order_by};
use super::join_full::compile_full_join_two_table;
use super::limit_scan::compile_limit_setup;
use super::order_by::SYNTHETIC_SPAN;
use super::*;
use crate::codegen::index_maintenance::valid_table_root_page;

pub(super) use level::{
    compile_join_level, compile_join_level_traverse, emit_join_final_row, resolve_join_constraint,
    LevelCheck, LevelPlan,
};
/// Compiles a joined `select` (#237: `INNER`/plain `JOIN`, `LEFT
/// [OUTER] JOIN`, `CROSS JOIN`) against `schemas` — one schema per
/// table in `select.from`'s order: the first table, then each
/// `Join::table` in `select.from.joins`'s order. A classic
/// nested-loop join: `OpenRead` every cursor up front, then
/// outer-to-inner `Rewind`/`Next` (the first table outermost),
/// testing each join's `ON` condition right after entering its own
/// loop. `LEFT JOIN` additionally tracks a per-outer-row "matched"
/// flag register and, when no inner row satisfied `ON`, emits exactly
/// one row with that table's (and anything joined off of it)
/// columns forced to NULL — see [`compile_join_level`].
///
/// TODO(#237 follow-up): `DISTINCT` combined with a JOIN is rejected
/// outright (`Unsupported`) rather than silently mis-compiled — the
/// ephemeral-index DISTINCT guard is hard-wired to a single
/// `TableSchema`, and generalizing it to a multi-table `Scope` was out
/// of this ticket's bounded scope. `WHERE`/`ORDER BY`/`LIMIT`/`OFFSET`/
/// projections (including `*`/`table.*`) all work across the join,
/// including combined with `GROUP BY`/aggregates (#502).
///
/// `full_catalog` (#257) is only consulted when a `FROM` slot is a
/// subquery — to resolve *its own* `FROM` table(s), which need not be
/// among `schemas` (the outer query's own joined tables). It's
/// deliberately separate from the `Scope::catalog` a scalar/`IN`/
/// `EXISTS` subquery *expression* inside this JOIN's `WHERE` resolves
/// against (still just `schemas`, unchanged, per the existing scope
/// limitation this function's own doc doesn't relitigate here).
/// `compile_full_join_two_table`'s dedicated path does not accept a
/// subquery-in-FROM (not yet supported there).
pub fn compile_select_joined(
    select: &Select,
    schemas: &[TableSchema],
    full_catalog: &[TableSchema],
    stats_by_table: &std::collections::HashMap<String, crate::planner::Stats>,
) -> Result<Program, CodegenError> {
    let Some(from) = &select.from else {
        return Err(CodegenError::NoFromClause);
    };
    let table_count = from.joins.len().saturating_add(1);
    if schemas.len() != table_count {
        return Err(CodegenError::Unsupported {
            reason: format!(
                "compile_select_joined needs one schema per FROM table ({table_count} tables, \
                 {} schemas given)",
                schemas.len()
            ),
        });
    }
    if !select.compound.is_empty() {
        return Err(CodegenError::Unsupported {
            reason: "UNION ALL with a JOIN in one of its arms is not yet supported".to_string(),
        });
    }

    // #250's codegen half: `FULL JOIN` gets its own dedicated two-table
    // emitter (see `compile_full_join_two_table`'s doc comment) rather
    // than participating in the `RIGHT`-reordering scheme below — it's
    // only supported as the sole join in the `FROM` clause today. #288
    // extended that emitter's pass-1/pass-2 shape to also support
    // `ORDER BY` (via its own sorter pass) and `DISTINCT` (via the same
    // ephemeral-index guard the rest of this module uses); only the two
    // combined together stays rejected, same as the ordinary join tree
    // below.
    if from.joins.len() == 1 && from.joins.first().is_some_and(|j| j.op == JoinOp::Full) {
        // #288: `ORDER BY` and `DISTINCT` are each independently
        // supported combined with a `FULL JOIN` now (see
        // `compile_full_join_two_table`'s doc comment) — only their
        // *combination* stays rejected, mirroring the same restriction
        // the ordinary join tree enforces just below in
        // `compile_select_joined_scan`.
        if !select.order_by.is_empty() && matches!(select.distinct, Some(Distinctness::Distinct)) {
            return Err(CodegenError::Unsupported {
                reason: "DISTINCT combined with ORDER BY and a FULL JOIN is not yet supported"
                    .to_string(),
            });
        }
        return compile_full_join_two_table(select, schemas, from, stats_by_table);
    }
    if from.joins.iter().any(|j| j.op == JoinOp::Full) {
        return Err(CodegenError::Unsupported {
            reason: "FULL JOIN codegen only supports a single two-table FULL JOIN today \
                     (`SELECT ... FROM a FULL JOIN b ON ...`) — a FULL JOIN combined with \
                     any other join in the same FROM clause is not yet supported"
                .to_string(),
        });
    }
    // RIGHT JOIN is implemented by reordering the join chain into an
    // equivalent LEFT JOIN (`A RIGHT JOIN B` == `B LEFT JOIN A`,
    // generalized to an N-way chain — see the `working_order`/`pos_of`
    // construction below and `LevelPlan`'s doc comment). Only one
    // `RIGHT JOIN` per `FROM` clause is supported: a second one would,
    // in the general case, share its deepest check level with the
    // first (see the design notes accompanying this ticket), which
    // this compiler doesn't attempt to disambiguate — rejected here
    // with a clean error rather than risking a silently wrong plan.
    let right_count = from.joins.iter().filter(|j| j.op == JoinOp::Right).count();
    if right_count > 1 {
        return Err(CodegenError::Unsupported {
            reason: "RIGHT JOIN codegen only supports a single RIGHT JOIN per FROM clause \
                     today — a chain with more than one RIGHT JOIN is not yet supported"
                .to_string(),
        });
    }

    let mut em = Emitter::new();
    let mut reg = RegAlloc::new();

    let init_addr = em.emit(Instruction::new(Opcode::Init, 0, 0, 0));
    let body_start = em.new_label();
    em.place(body_start);
    em.patch_p2(init_addr, body_start);

    let end_label = em.new_label();
    let mut sink = |em: &mut Emitter, _reg: &mut RegAlloc, first: i32, count: i32| {
        em.emit(Instruction::new(Opcode::ResultRow, first, count, 0));
        Ok(())
    };
    compile_select_joined_scan(
        &mut em,
        &mut reg,
        select,
        schemas,
        full_catalog,
        0,
        end_label,
        stats_by_table,
        &mut sink,
    )?;

    em.place(end_label);
    em.emit(Instruction::new(Opcode::Halt, 0, 0, 0));

    Ok(em.finish())
}

/// The scan/filter/project core of [`compile_select_joined`] (INNER/LEFT/
/// CROSS/NATURAL/USING/RIGHT — `FULL JOIN` stays on its own dedicated
/// [`compile_full_join_two_table`] path), minus the `Init`/`Halt`
/// bracketing and with every cursor number offset by `cursor_base` —
/// factored out, mirroring [`compile_select_scan`]'s relationship to
/// [`compile_select`], so #250's `INSERT ... SELECT` codegen can drive
/// the same joined nested-loop scan with its own cursor numbers already
/// claimed by the target table/its indexes, substituting its own row
/// sink in place of `ResultRow`.
///
/// `ORDER BY` and `DISTINCT` are both supported here now (#250's last
/// piece): `ORDER BY` routes through a dedicated sort pass1/pass2 (see
/// [`compile_join_level_for_sort`]) whose pass-2 result-column
/// reconstruction is restricted to `*`/`table.*`/bare-column result
/// columns (a computed expression in the `SELECT` list combined with a
/// joined `ORDER BY` returns a clean `Unsupported` error rather than
/// silently mis-projecting); `DISTINCT` (without `ORDER BY`) instead
/// hooks directly into the ordinary nested-loop scan's final-row
/// emission via an ephemeral-index guard, since it never needs the
/// sorter at all. The two combined on a JOIN are rejected outright — see
/// the caller.
#[allow(clippy::too_many_arguments)]
pub(crate) fn compile_select_joined_scan<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schemas: &[TableSchema],
    full_catalog: &[TableSchema],
    cursor_base: i32,
    end_label: Label,
    stats_by_table: &std::collections::HashMap<String, crate::planner::Stats>,
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    let Some(from) = &select.from else {
        return Err(CodegenError::NoFromClause);
    };
    if !select.order_by.is_empty() && matches!(select.distinct, Some(Distinctness::Distinct)) {
        return Err(CodegenError::Unsupported {
            reason: "DISTINCT combined with ORDER BY and a JOIN is not yet supported".to_string(),
        });
    }
    // `FULL JOIN` has its own dedicated two-table emitter
    // (`compile_full_join_two_table`), reached only through
    // `compile_select_joined`'s own dispatch above this function — a
    // caller reaching `compile_select_joined_scan` directly (#250's
    // `INSERT ... SELECT`, see `insert.rs::compile_insert`) never goes
    // through that dispatch, so a `FULL JOIN` in `select.from.joins`
    // here would otherwise silently be treated like an ordinary
    // inner/left join by the nested-loop machinery below. Rejected
    // explicitly instead.
    if from.joins.iter().any(|j| j.op == JoinOp::Full) {
        return Err(CodegenError::Unsupported {
            reason: "FULL JOIN combined with INSERT ... SELECT is not yet supported".to_string(),
        });
    }
    let right_count = from.joins.iter().filter(|j| j.op == JoinOp::Right).count();
    if right_count > 1 {
        return Err(CodegenError::Unsupported {
            reason: "RIGHT JOIN codegen only supports a single RIGHT JOIN per FROM clause \
                     today — a chain with more than one RIGHT JOIN is not yet supported"
                .to_string(),
        });
    }

    let table_refs: Vec<&TableRef> = std::iter::once(&from.first)
        .chain(from.joins.iter().map(|j| &j.table))
        .collect();
    let n = schemas.len();
    // `bindings` stays in original FROM-clause order throughout — its
    // `cursor` field is filled in below once the execution order
    // (`working_order`) is known, and it (not the reordered execution
    // list) is what every `Scope` gets built from, so `SELECT *`
    // expansion order and column-ambiguity resolution are unaffected
    // by RIGHT JOIN's internal reordering.
    let mut bindings = Vec::with_capacity(n);
    for (table_ref, schema) in table_refs.iter().zip(schemas.iter()) {
        bindings.push(TableBinding {
            alias: table_ref.alias.clone(),
            name: table_binding_name(table_ref),
            schema: schema.clone(),
            cursor: 0,
            forced_null: false,
            stats: stats_by_table
                .get(&schema.name)
                .cloned()
                .unwrap_or_default(),
        });
    }

    // `dedup_star[i]` names the columns (lowercased) that a plain `*`
    // expansion must skip for `bindings[i]` — populated below for the
    // *right*-hand side of each NATURAL/USING join (#250's codegen
    // half), since SQLite keeps only the left-most occurrence of a
    // naturally-/USING-joined column in `SELECT *` output. Indexed by
    // *original* FROM-clause position, same as `bindings`.
    let mut dedup_star: Vec<std::collections::HashSet<String>> =
        vec![std::collections::HashSet::new(); n];
    let mut constraints: Vec<Option<Expr>> = Vec::with_capacity(from.joins.len());
    for (i, join) in from.joins.iter().enumerate() {
        let right_idx = i.checked_add(1).ok_or_else(|| CodegenError::Unsupported {
            reason: "too many joined tables".to_string(),
        })?;
        let left = bindings
            .get(0..right_idx)
            .ok_or_else(|| CodegenError::Unsupported {
                reason: "join level out of range".to_string(),
            })?;
        let right = bindings
            .get(right_idx)
            .ok_or_else(|| CodegenError::Unsupported {
                reason: "join level out of range".to_string(),
            })?;
        let constraint = resolve_join_constraint(join, left, right, right_idx, &mut dedup_star)?;
        constraints.push(constraint);
    }

    // #470/#462 (spec 011): a chain made entirely of `INNER`/`CROSS`
    // joins (already-resolved `NATURAL`/`USING` included) is safe to
    // execute in any order — reorder it by the #461 cost model's
    // estimated row counts (smallest table outermost) rather than
    // always compiling FROM-clause order. `LEFT`/`RIGHT`/`FULL` chains
    // (and `right_step` below) keep their original order unconditionally,
    // since reordering either would change the result set.
    let reorder_plan = super::join_order::is_reorderable_inner_chain(from).then(|| {
        let seekable = super::join_order::seekable_tables(schemas, &constraints);
        let costs = super::join_order::scan_costs(schemas, stats_by_table, &seekable);
        super::join_order::plan_join_order(&costs)
    });

    // Determine execution order: `working_order[exec_pos]` is the
    // original FROM-clause index executed at that recursion level.
    // Every `Inner`/`Left`/`Cross` join (including already-resolved
    // NATURAL/USING) simply appends its table to the end, exactly like
    // #237. A `Right` join instead *prepends* its table to the front —
    // `A RIGHT JOIN B` becomes `B`'s cursor loop outermost, with the
    // entire prior chain (everything already in `working_order`)
    // nested beneath it as the side that gets null-extended on a miss,
    // i.e. exactly `B LEFT JOIN A`. `right_count <= 1` is enforced
    // above, so at most one such prepend ever happens.
    struct NormalStep {
        table: usize,
        is_left: bool,
        join_index: usize,
    }
    struct RightStep {
        new_table: usize,
        deep_orig: usize,
        join_index: usize,
    }
    let mut working_order: Vec<usize> = vec![0];
    let mut normal_steps: Vec<NormalStep> = Vec::with_capacity(from.joins.len());
    let mut right_step: Option<RightStep> = None;
    for (j, join) in from.joins.iter().enumerate() {
        let new_table = j.saturating_add(1);
        if join.op == JoinOp::Right {
            let deep_orig = *working_order.last().unwrap_or(&0);
            right_step = Some(RightStep {
                new_table,
                deep_orig,
                join_index: j,
            });
            working_order = std::iter::once(new_table)
                .chain(working_order.iter().copied())
                .collect();
        } else {
            normal_steps.push(NormalStep {
                table: new_table,
                is_left: join.op == JoinOp::Left,
                join_index: j,
            });
            if reorder_plan.is_none() {
                working_order.push(new_table);
            }
        }
    }
    if let Some(order) = &reorder_plan {
        working_order = order.clone();
    }

    // `pos_of[original_index]` is the execution-order recursion level
    // that original table ends up at.
    let mut pos_of = vec![0usize; n];
    for (pos, &orig) in working_order.iter().enumerate() {
        if let Some(slot) = pos_of.get_mut(orig) {
            *slot = pos;
        }
    }

    // Cursor numbers follow execution order (simplest: a table's
    // cursor number is just its recursion level), and every cursor is
    // opened exactly once, in that same order — `OpenRead` for a real
    // table, or (#257) materialized into an ephemeral table when this
    // FROM slot is a subquery.
    for (pos, &orig) in working_order.iter().enumerate() {
        let cursor = cursor_base.saturating_add(i32::try_from(pos).unwrap_or(0));
        if let Some(binding) = bindings.get_mut(orig) {
            binding.cursor = cursor;
        }
        match table_refs.get(orig).map(|t| &t.kind) {
            Some(TableRefKind::Subquery(subquery)) => {
                crate::codegen::subquery::materialize_from_subquery(
                    em,
                    reg,
                    subquery,
                    full_catalog,
                    cursor,
                )?;
            }
            _ => {
                let Some(binding) = bindings.get(orig) else {
                    return Err(CodegenError::Unsupported {
                        reason: "join table binding out of range".to_string(),
                    });
                };
                let root_page = valid_table_root_page(&binding.schema)?;
                em.emit(Instruction::new(Opcode::OpenRead, cursor, root_page, 0));
            }
        }
    }

    let exec_bindings: Vec<TableBinding> = working_order
        .iter()
        .filter_map(|&orig| bindings.get(orig).cloned())
        .collect();

    // Per-execution-level plan: `levels[level]` describes what to check
    // while iterating `exec_bindings[level]`'s own loop, and whether
    // this level owns an outer-join "matched" register.
    let mut levels: Vec<LevelPlan> = vec![LevelPlan::default(); n];
    for step in &normal_steps {
        let constraint = constraints.get(step.join_index).cloned().flatten();
        // Reordered chains can't rely on a join's constraint being
        // checkable at its own (originally-adjacent) table's level
        // anymore — place it at the first execution level where every
        // table the constraint actually reads (plus the joined table
        // itself) is already bound, per
        // `join_order::referenced_binding_indices`'s doc comment.
        // Unreordered chains keep the original, narrower placement
        // unchanged.
        let pos = match &reorder_plan {
            Some(_) => constraint
                .as_ref()
                .map(|c| super::join_order::referenced_binding_indices(c, &bindings))
                .unwrap_or_default()
                .into_iter()
                .chain(std::iter::once(step.table))
                .filter_map(|i| pos_of.get(i).copied())
                .max()
                .unwrap_or(0),
            None => pos_of.get(step.table).copied().unwrap_or(0),
        };
        if let Some(plan) = levels.get_mut(pos) {
            plan.checks.push(LevelCheck {
                constraint,
                sets_matched: if step.is_left { Some(pos) } else { None },
            });
            if step.is_left {
                plan.null_span = Some((pos, pos));
            }
        }
    }
    if let Some(rs) = &right_step {
        // `rs.new_table`'s own execution level (`outer_pos`) needs no
        // special handling at all — it's a plain unconditional scan,
        // exactly as if it were `from.first` (nothing shallower depends
        // on it). The outer-join bookkeeping (matched register,
        // null-extension) belongs to its *immediate child* level
        // (`outer_pos + 1`) instead — reset before that level's own
        // `Rewind`, checked after its own loop exhausts, precisely
        // mirroring a classic `LEFT JOIN`'s placement (whose "matched"
        // owner is likewise the LEFT-joined table's own level, nested
        // inside its parent's loop for the right per-row cadence) —
        // only here `check_pos` (where the constraint actually gets
        // evaluated) may be deeper than the owning level whenever the
        // pre-existing chain being RIGHT-joined against has more than
        // one table.
        let outer_pos = pos_of.get(rs.new_table).copied().unwrap_or(0);
        let check_pos = pos_of.get(rs.deep_orig).copied().unwrap_or(0);
        let owner_pos = outer_pos.saturating_add(1);
        let constraint = constraints.get(rs.join_index).cloned().flatten();
        if let Some(plan) = levels.get_mut(check_pos) {
            plan.checks.push(LevelCheck {
                constraint,
                sets_matched: Some(owner_pos),
            });
        }
        if let Some(plan) = levels.get_mut(owner_pos) {
            plan.null_span = Some((owner_pos, check_pos));
        }
    }

    let full_scope = Scope {
        tables: bindings.clone(),
        catalog: schemas.to_vec(),
        outer: None,
        dedup_star: dedup_star.clone(),
        ..Scope::default()
    };
    let table_cursor_count = i32::try_from(n).unwrap_or(0);

    if !select.group_by.is_empty() || select_has_aggregate(select) {
        if select.distinct.is_some() {
            return Err(CodegenError::Unsupported {
                reason: "GROUP BY/aggregate combined with DISTINCT and a JOIN is not yet \
                         supported"
                    .to_string(),
            });
        }
        let implicit_group = select.group_by.is_empty();
        let sort_cursor = cursor_base.saturating_add(table_cursor_count);
        let pseudo_cursor = sort_cursor.saturating_add(1);
        let flush_cursor = pseudo_cursor.saturating_add(1);
        // #502: a trailing `ORDER BY` re-sort needs its own sorter +
        // pseudo cursor, allocated right after `flush_cursor` so
        // `eph_base` (DISTINCT-aggregate dedup cursors, computed inside
        // `compile_joined_grouped_scan`) shifts up by two to avoid
        // colliding with them.
        let order_cursors = if select.order_by.is_empty() {
            None
        } else {
            Some((
                flush_cursor.saturating_add(1),
                flush_cursor.saturating_add(2),
            ))
        };
        return compile_joined_grouped_scan(
            em,
            reg,
            select,
            &exec_bindings,
            &bindings,
            &pos_of,
            &levels,
            &dedup_star,
            schemas,
            &full_scope,
            sort_cursor,
            pseudo_cursor,
            flush_cursor,
            order_cursors,
            end_label,
            implicit_group,
            sink,
        );
    }

    if !select.order_by.is_empty() {
        let order_by_plans = resolve_join_order_by(select, &full_scope)?;
        let sort_cursor = cursor_base.saturating_add(table_cursor_count);
        let pseudo_cursor = sort_cursor.saturating_add(1);
        return compile_joined_sorted_scan(
            em,
            reg,
            select,
            &exec_bindings,
            &bindings,
            &pos_of,
            &levels,
            &dedup_star,
            schemas,
            &full_scope,
            &order_by_plans,
            sort_cursor,
            pseudo_cursor,
            end_label,
            sink,
        );
    }

    let limit = compile_limit_setup(em, reg, &full_scope, select)?;
    let distinct_cursor = matches!(select.distinct, Some(Distinctness::Distinct)).then(|| {
        let cursor = cursor_base.saturating_add(table_cursor_count);
        em.emit(Instruction::new(Opcode::OpenEphemeral, cursor, 0, 0));
        cursor
    });

    let mut null_mask = vec![false; n];
    let mut matched_regs: Vec<Option<i32>> = vec![None; n];
    compile_join_level(
        em,
        reg,
        select,
        &exec_bindings,
        &bindings,
        &pos_of,
        &levels,
        &dedup_star,
        &mut null_mask,
        &mut matched_regs,
        0,
        end_label,
        limit.as_ref(),
        distinct_cursor,
        schemas,
        sink,
    )
}

/// Builds the [`Scope`] a join-tree node sees at compile time. `bindings`
/// is always in *original* FROM-clause order (so `SELECT *` expansion
/// order and column-ambiguity resolution never depend on RIGHT JOIN's
/// internal execution reordering — see [`compile_select_joined`]);
/// `null_mask` is indexed by *execution* level instead, so `pos_of`
/// (original index -> execution level) translates between the two:
/// binding `orig` is forced null when `null_mask[pos_of[orig]]` is set
/// (an outer join's no-match branch, see [`compile_join_level`]) — the
/// shared `bindings` vec itself is never mutated.
pub(super) fn join_scope(
    bindings: &[TableBinding],
    null_mask: &[bool],
    pos_of: &[usize],
    catalog: &[TableSchema],
    dedup_star: &[std::collections::HashSet<String>],
) -> Scope {
    Scope {
        tables: bindings
            .iter()
            .enumerate()
            .map(|(orig, b)| {
                let forced_null = pos_of
                    .get(orig)
                    .and_then(|&pos| null_mask.get(pos))
                    .copied()
                    .unwrap_or(false)
                    || b.forced_null;
                TableBinding {
                    alias: b.alias.clone(),
                    name: b.name.clone(),
                    schema: b.schema.clone(),
                    cursor: b.cursor,
                    forced_null,
                    stats: b.stats.clone(),
                }
            })
            .collect(),
        catalog: catalog.to_vec(),
        outer: None,
        dedup_star: dedup_star.to_vec(),
        ..Scope::default()
    }
}

/// Builds the qualified-column `Expr` used to reference `binding`'s
/// `name` column when synthesizing a NATURAL/USING join's equality
/// constraint — qualified (rather than a bare unqualified `Column`) so
/// resolution never has to fall back to [`Scope::resolve`]'s
/// unqualified-ambiguity rule, which would incorrectly reject a column
/// name shared by more than one already-joined left-side table.
pub(super) fn qualified_column_expr(binding: &TableBinding, name: &str) -> Expr {
    Expr {
        kind: ExprKind::Column {
            table: Some(
                binding
                    .alias
                    .clone()
                    .unwrap_or_else(|| binding.name.clone()),
            ),
            catalog: None,
            name: name.to_string(),
        },
        span: SYNTHETIC_SPAN,
    }
}

/// Synthesizes the `ON`-equivalent equality constraint for a NATURAL
/// or `USING (...)` join: for each name in `cols`, finds a left-side
/// binding (searched in `left`, i.e. `bindings[0..=i]`, first match
/// wins — this is the "simplest defensible interpretation" for 3+-way
/// chains noted in #250's follow-up plan, since a qualified reference
/// to that one binding's column sidesteps the unqualified-ambiguity
/// question entirely) and requires `right` (`bindings[i + 1]`) to have
/// a same-named column, ANDing `left.col = right.col` together across
/// every name. Returns the synthesized `Expr` (`None` only if `cols`
/// is empty) plus the exact schema-cased column names used, so the
/// caller can also populate `dedup_star` for `SELECT *`
/// de-duplication.
pub(super) fn synthesize_equality_constraint(
    left: &[TableBinding],
    right: &TableBinding,
    cols: &[String],
    require_left_match: bool,
) -> Result<(Option<Expr>, Vec<String>), CodegenError> {
    let mut acc: Option<Expr> = None;
    let mut shared = Vec::with_capacity(cols.len());
    for name in cols {
        let left_binding = left.iter().find(|b| {
            b.schema
                .columns
                .iter()
                .any(|c| c.eq_ignore_ascii_case(name))
        });
        let Some(left_binding) = left_binding else {
            if require_left_match {
                return Err(CodegenError::UnknownColumn { name: name.clone() });
            }
            continue;
        };
        let right_idx = column_index(&right.schema, name)
            .ok_or_else(|| CodegenError::UnknownColumn { name: name.clone() })?;
        let right_name = right
            .schema
            .columns
            .get(right_idx)
            .cloned()
            .ok_or_else(|| CodegenError::UnknownColumn { name: name.clone() })?;
        let eq = Expr {
            kind: ExprKind::Binary {
                op: BinaryOp::Eq,
                lhs: Box::new(qualified_column_expr(left_binding, name)),
                rhs: Box::new(qualified_column_expr(right, &right_name)),
            },
            span: SYNTHETIC_SPAN,
        };
        acc = Some(match acc {
            Some(prev) => Expr {
                kind: ExprKind::Binary {
                    op: BinaryOp::And,
                    lhs: Box::new(prev),
                    rhs: Box::new(eq),
                },
                span: SYNTHETIC_SPAN,
            },
            None => eq,
        });
        shared.push(name.to_ascii_lowercase());
    }
    Ok((acc, shared))
}
