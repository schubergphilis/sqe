// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::order_by::{OrderByPlan, OrderByTarget};
use super::projection::{
    compile_row_values, emit_distinct_guard, emit_row_via_sink, ResultColumnPlan,
};
use super::*;
use crate::planner::{is_skip_scan_worthwhile, Stats};
use std::collections::HashMap;
/// LIMIT/OFFSET counters, set up once before the scan loop starts.
pub(super) struct LimitState {
    offset_reg: Option<i32>,
    limit_reg: Option<i32>,
}

pub(super) fn compile_limit_setup(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    scope: &Scope,
    select: &Select,
) -> Result<Option<LimitState>, CodegenError> {
    let Some(limit) = &select.limit else {
        return Ok(None);
    };
    let limit_reg = compile_value(em, reg, scope, &limit.limit)?;
    let offset_reg = match &limit.offset {
        Some(offset_expr) => Some(compile_value(em, reg, scope, offset_expr)?),
        None => None,
    };
    Ok(Some(LimitState {
        offset_reg,
        limit_reg: Some(limit_reg),
    }))
}

/// Emits the OFFSET skip-guard (jumping to `row_skip` while
/// `offset_reg` still has rows to skip) — call once per scanned row,
/// before deciding whether to emit it.
pub(super) fn emit_offset_guard(em: &mut Emitter, limit: &LimitState, row_skip: Label) {
    if let Some(offset_reg) = limit.offset_reg {
        let addr = em.emit(Instruction::new(Opcode::IfPos, offset_reg, 0, 1));
        em.patch_p2(addr, row_skip);
    }
}

/// Emits the LIMIT stop-guard: call once per row *before* emitting it
/// (mirroring `emit_offset_guard`'s check-before-act shape, not
/// `emit_row_via_sink`'s old post-emit position). `IfNotZero` decrements
/// `limit_reg` only while it's positive and jumps whenever it's
/// nonzero — a negative `LIMIT` (SQLite's "no limit" convention) never
/// reaches zero and always falls into that jump, staying unbounded —
/// then a `Goto` reached only when `limit_reg` has hit exactly zero
/// stops the scan before this row is ever emitted.
///
/// This ordering matters for `LIMIT 0`: the old post-emit
/// `DecrJumpZero` never got a chance to run before the *first* row was
/// already emitted, so `LIMIT 0` emitted every row instead of none
/// (#129's benchmarking incidentally caught this pre-existing bug).
pub(super) fn emit_limit_guard(em: &mut Emitter, limit: &LimitState, end_label: Label) {
    if let Some(limit_reg) = limit.limit_reg {
        let has_budget_addr = em.emit(Instruction::new(Opcode::IfNotZero, limit_reg, 0, 0));
        let stop_addr = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
        em.patch_p2(stop_addr, end_label);
        let continue_label = em.new_label();
        em.patch_p2(has_budget_addr, continue_label);
        em.place(continue_label);
    }
}

/// The two sides of a top-level `=` expression, or `None` for any other
/// shape. Used by [`try_compile_rowid_seek`] to recognize `WHERE rowid =
/// <int literal>` / `WHERE rowid = ?` (#137).
///
/// Single input reference, so lifetime elision ties both tuple elements
/// to it without an explicit `<'a>` annotation — the qualified subset
/// (`make check-mvl-limit`) forbids explicit lifetimes, and a helper taking
/// both `schema` and `expr` by reference while returning a borrow of
/// `expr` alone would need one. The caller also needs `schema` (to pick
/// the non-rowid side via [`is_rowid_reference`]), so that step happens
/// in [`try_compile_rowid_seek`] itself, which already holds both.
pub(crate) fn top_level_equality_operands(expr: &Expr) -> Option<(&Expr, &Expr)> {
    let ExprKind::Binary {
        op: BinaryOp::Eq,
        lhs,
        rhs,
    } = &expr.kind
    else {
        return None;
    };
    Some((lhs, rhs))
}

pub(crate) fn is_rowid_reference(schema: &TableSchema, expr: &Expr) -> bool {
    let ExprKind::Column { name, .. } = &expr.kind else {
        return false;
    };
    is_rowid_reference_name(schema, name)
}

fn is_rowid_reference_name(schema: &TableSchema, name: &str) -> bool {
    if name.eq_ignore_ascii_case("rowid")
        || name.eq_ignore_ascii_case("_rowid_")
        || name.eq_ignore_ascii_case("oid")
    {
        return true;
    }
    schema
        .rowid_alias
        .and_then(|idx| schema.columns.get(idx))
        .is_some_and(|col| col.eq_ignore_ascii_case(name))
}

/// An operand `top_level_equality_operands`/[`propagate_constants`] will
/// hand to a seek/probe fast path — an integer literal or a bind
/// parameter. Anything else (a string literal needing numeric-affinity
/// coercion, a sub-expression, a named parameter) is left for the
/// ordinary scan rather than risk miscompiling a case these paths
/// weren't built to handle.
fn is_supported_seek_operand(expr: &Expr) -> bool {
    matches!(
        &expr.kind,
        ExprKind::Literal(Literal::Integer(_))
            | ExprKind::Param(ParamKind::Anonymous | ParamKind::Numbered(_))
    )
}

/// Flattens nested top-level `AND` conjuncts of `expr` into their leaf
/// conjuncts (in left-to-right order) — `a AND b AND c` yields `[a, b,
/// c]`; anything that isn't a top-level `AND` (including a single bare
/// condition) yields `[expr]` itself.
fn flatten_and_conjuncts(expr: &Expr) -> Vec<&Expr> {
    match &expr.kind {
        ExprKind::Binary {
            op: BinaryOp::And,
            lhs,
            rhs,
        } => {
            let mut conjuncts = flatten_and_conjuncts(lhs);
            conjuncts.extend(flatten_and_conjuncts(rhs));
            conjuncts
        }
        _ => vec![expr],
    }
}

/// Constant propagation (#605): resolves every column reachable — by a
/// chain of top-level `AND`-conjoined equalities in `where_expr` — to a
/// literal/bind-parameter constant, keyed by lowercased column name. For
/// `a = b AND b = 5`, both `a` and `b` resolve to the literal `5`, so a
/// fast path probing on `a` can use it exactly as if the query had
/// written `a = 5` directly. Only pure `column = column` and `column =
/// <supported operand>` top-level equalities feed the propagation —
/// mixed operators (comparisons, `OR`, function calls) are left alone,
/// so this never changes the WHERE clause's own semantics, only what a
/// fast path is allowed to assume about it.
pub(crate) fn propagate_constants(where_expr: &Expr) -> HashMap<String, Expr> {
    let mut direct: HashMap<String, Expr> = HashMap::new();
    let mut column_links: Vec<(String, String)> = Vec::new();
    for conjunct in flatten_and_conjuncts(where_expr) {
        let Some((lhs, rhs)) = top_level_equality_operands(conjunct) else {
            continue;
        };
        match (where_col(lhs), where_col(rhs)) {
            (Some(name), None) if is_supported_seek_operand(rhs) => {
                direct.insert(name.to_ascii_lowercase(), rhs.clone());
            }
            (None, Some(name)) if is_supported_seek_operand(lhs) => {
                direct.insert(name.to_ascii_lowercase(), lhs.clone());
            }
            (Some(a), Some(b)) => {
                column_links.push((a.to_ascii_lowercase(), b.to_ascii_lowercase()));
            }
            _ => {}
        }
    }
    // Fixed-point propagation across `column = column` links: each pass
    // may resolve a new column from one already known, so keep going
    // until a full pass resolves nothing new. Conjunct counts in
    // practice are small, so the naive O(n^2) worst case never matters.
    loop {
        let mut changed = false;
        for (a, b) in &column_links {
            if let Some(value) = direct.get(a).cloned() {
                if direct.insert(b.clone(), value).is_none() {
                    changed = true;
                }
            }
            if let Some(value) = direct.get(b).cloned() {
                if direct.insert(a.clone(), value).is_none() {
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    direct
}

/// Flattens nested top-level `OR` conjuncts of `expr` into their leaf
/// disjuncts (in left-to-right order) — mirrors [`flatten_and_conjuncts`]
/// for `OR` instead of `AND`.
fn flatten_or_disjuncts(expr: &Expr) -> Vec<&Expr> {
    match &expr.kind {
        ExprKind::Binary {
            op: BinaryOp::Or,
            lhs,
            rhs,
        } => {
            let mut disjuncts = flatten_or_disjuncts(lhs);
            disjuncts.extend(flatten_or_disjuncts(rhs));
            disjuncts
        }
        _ => vec![expr],
    }
}

/// OR-to-IN conversion (#605): when `where_expr` is a top-level `OR`
/// chain of at least two equalities, all against the same column
/// (recognized via `column_matches`) and each against a supported
/// literal/bind-parameter operand, returns that operand list — letting a
/// seek-based fast path probe once per value instead of falling back to
/// a full scan. `x = 1 OR x = 2 OR x = 3` yields `[1, 2, 3]`; any
/// disjunct that isn't a pure equality against the same column (a
/// different column, a compound sub-condition, an unsupported operand)
/// disqualifies the whole chain, since a fast path can only skip rows
/// the WHERE clause would exclude anyway — it must never accept a
/// disjunct it can't also enforce.
fn or_chain_equality_operands<F>(expr: &Expr, column_matches: F) -> Option<Vec<&Expr>>
where
    F: Fn(&Expr) -> bool,
{
    let disjuncts = flatten_or_disjuncts(expr);
    if disjuncts.len() < 2 {
        return None;
    }
    let mut operands = Vec::with_capacity(disjuncts.len());
    for disjunct in disjuncts {
        let (lhs, rhs) = top_level_equality_operands(disjunct)?;
        let operand = if column_matches(lhs) {
            rhs
        } else if column_matches(rhs) {
            lhs
        } else {
            return None;
        };
        if !is_supported_seek_operand(operand) {
            return None;
        }
        operands.push(operand);
    }
    Some(operands)
}

/// Looks up the rowid's resolved constant (per [`propagate_constants`])
/// among its recognized aliases (`rowid`/`_rowid_`/`oid`, or the table's
/// `INTEGER PRIMARY KEY` alias column), in that fixed order — so lookup
/// is deterministic even if more than one alias somehow resolved.
fn resolve_rowid_constant(schema: &TableSchema, constants: &HashMap<String, Expr>) -> Option<Expr> {
    for name in ["rowid", "_rowid_", "oid"] {
        if let Some(value) = constants.get(name) {
            return Some(value.clone());
        }
    }
    let alias = schema.rowid_alias.and_then(|idx| schema.columns.get(idx))?;
    constants.get(&alias.to_ascii_lowercase()).cloned()
}

/// Emits `Integer`/`Variable` + `SeekRowid` in place of the
/// `Rewind`/`Next` scan loop when `select`'s `WHERE` clause is a single
/// top-level equality between a rowid reference (the `rowid`/`_rowid_`/
/// `oid` keywords, or the table's actual `INTEGER PRIMARY KEY` alias
/// column) and an integer literal or bind parameter — O(log n) point
/// lookup instead of O(n) full scan (#137). Returns `Ok(true)` when the
/// fast path was taken; `Ok(false)` leaves `em`/`reg` untouched so the
/// caller falls back to the ordinary scan. Deliberately narrow —
/// secondary-index columns, ranges, and compound conditions (`AND`/`OR`)
/// all fall through to the ordinary scan and stay in V4 per the issue's
/// bounded scope.
#[allow(clippy::too_many_arguments)]
pub(super) fn try_compile_rowid_seek<F>(
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
    if matches!(select.distinct, Some(Distinctness::Distinct)) {
        // A single-row result is already distinct — but keeping this
        // path free of the ephemeral-index bookkeeping means it can
        // stay a straight-line seek. Not worth special-casing; DISTINCT
        // falls back to the ordinary scan.
        return Ok(false);
    }
    let Some(where_expr) = &select.where_clause else {
        return Ok(false);
    };
    let direct_operand = top_level_equality_operands(where_expr).and_then(|(lhs, rhs)| {
        if is_rowid_reference(schema, lhs) {
            Some(rhs)
        } else if is_rowid_reference(schema, rhs) {
            Some(lhs)
        } else {
            None
        }
    });
    // #605: falls back to constant propagation through an `AND`
    // conjunction (`rowid = a AND a = 5`), then to OR-to-IN conversion
    // (`rowid = 1 OR rowid = 2`), when the rowid isn't equated to a
    // supported operand directly.
    let operands: Vec<Expr> = match direct_operand {
        Some(operand) if is_supported_seek_operand(operand) => vec![operand.clone()],
        _ => {
            let constants = propagate_constants(where_expr);
            match resolve_rowid_constant(schema, &constants) {
                Some(value) => vec![value],
                None => {
                    match or_chain_equality_operands(where_expr, |e| is_rowid_reference(schema, e))
                    {
                        Some(or_operands) => or_operands.into_iter().cloned().collect(),
                        None => return Ok(false),
                    }
                }
            }
        }
    };

    let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    let limit = compile_limit_setup(em, reg, &scope, select)?;
    let mut operands = operands.into_iter().peekable();
    while let Some(operand) = operands.next() {
        let value_reg = compile_value(em, reg, &scope, &operand)?;
        let seek_addr = em.emit(Instruction::new(
            Opcode::SeekRowid,
            cursors.table,
            0,
            value_reg,
        ));
        let not_found = if operands.peek().is_some() {
            em.new_label()
        } else {
            end_label
        };
        em.patch_p2(seek_addr, not_found);

        let row_skip = em.new_label();
        if let Some(limit) = &limit {
            emit_offset_guard(em, limit, row_skip);
        }
        if let Some(limit) = &limit {
            emit_limit_guard(em, limit, end_label);
        }
        emit_row_via_sink(em, reg, select, schema, cursors.table, false, catalog, sink)?;
        em.place(row_skip);
        if not_found != end_label {
            em.place(not_found);
        }
    }
    Ok(true)
}

/// Resolves every `select`'s result column to a bare, unqualified
/// column *name* — `*`/`schema`-qualified `table.*` expand to `schema`'s
/// own column list (#535: this is a single-table scan, so `*` means
/// exactly those columns); `None` if a `table.*` names some other table,
/// or any result column is a non-bare-column expression. Used by
/// [`try_compile_covering_index_scan`] to check "does this SELECT only
/// ever need columns an index already carries" without pulling in the
/// general projection machinery, which doesn't (yet) know how to read a
/// computed expression off an index record.
fn bare_result_column_names(select: &Select, schema: &TableSchema) -> Option<Vec<String>> {
    let mut out = Vec::with_capacity(select.columns.len());
    for col in &select.columns {
        match col {
            ResultColumn::Star => out.extend(schema.columns.iter().cloned()),
            ResultColumn::TableStar { table } if table.eq_ignore_ascii_case(&schema.name) => {
                out.extend(schema.columns.iter().cloned());
            }
            ResultColumn::Expr {
                expr:
                    Expr {
                        kind: ExprKind::Column { name, .. },
                        ..
                    },
                ..
            } => out.push(name.clone()),
            _ => return None,
        }
    }
    Some(out)
}

fn where_col(expr: &Expr) -> Option<&str> {
    match &expr.kind {
        ExprKind::Column { name, .. } => Some(name.as_str()),
        _ => None,
    }
}

/// A [`find_covering_index`] match: which of `schema.indexes` was
/// matched (by position, not reference — see that function's doc for
/// why) plus a clone of the probe operand expression(s) — more than one
/// when an OR-chain of equalities (#605) resolved to the same leading
/// column.
pub(super) struct CoveringIndexMatch {
    pub(super) index_position: usize,
    pub(super) operands: Vec<Expr>,
}

/// Finds an index of `schema` usable as a covering-index scan (#444,
/// non-`UNIQUE` indexes per #450) for `select`: `select.where_clause`
/// must be a single
/// top-level equality between the index's leading column and a
/// literal/bind-parameter operand, and every `SELECT`-list column (bare
/// columns only — see [`bare_result_column_names`]) must itself be
/// carried by that index. Returns the matched index's position in
/// `schema.indexes` plus the probe operand expression (owned, not
/// borrowed: a function taking two independent `&` parameters can't
/// return a value borrowing from either one without a named lifetime
/// tying them together, which the qualified subset forbids — see
/// `tools/mvl-limit`). Shared by [`try_compile_covering_index_scan`]
/// (which actually emits the scan) and `eqp.rs` (which only reports it),
/// so the two can never drift apart.
pub(super) fn find_covering_index(
    schema: &TableSchema,
    select: &Select,
) -> Option<CoveringIndexMatch> {
    if matches!(select.distinct, Some(Distinctness::Distinct)) {
        return None;
    }
    let where_expr = select.where_clause.as_ref()?;
    // #605: constant propagation lets an equality against a leading
    // index column resolve through an `AND`-conjoined chain
    // (`idx_col = a AND a = 5`), not just a direct literal/param operand.
    let constants = propagate_constants(where_expr);
    // A non-`UNIQUE` index's leading column can match more than one row
    // (#450) — `try_compile_covering_index_scan` walks every duplicate
    // via `IdxNext` after the initial `SeekIndexEq`, so uniqueness is no
    // longer a precondition here, just the leading-column match.
    let direct = schema
        .indexes
        .iter()
        .enumerate()
        .find_map(|(index_position, idx)| {
            let leading = idx.columns.first()?;
            let operand = constants.get(&leading.name.to_ascii_lowercase())?;
            Some((index_position, vec![operand.clone()]))
        });
    // #605: OR-to-IN conversion — an OR-chain of equalities all against
    // the same leading column probes once per value instead of falling
    // back to a full scan.
    let (index_position, operands) = match direct {
        Some(found) => found,
        None => schema
            .indexes
            .iter()
            .enumerate()
            .find_map(|(index_position, idx)| {
                let leading = idx.columns.first()?;
                let matches =
                    |e: &Expr| where_col(e).is_some_and(|n| n.eq_ignore_ascii_case(&leading.name));
                let operands = or_chain_equality_operands(where_expr, matches)?;
                Some((index_position, operands.into_iter().cloned().collect()))
            })?,
    };
    let index = schema.indexes.get(index_position)?;
    let result_names = bare_result_column_names(select, schema)?;
    // #535: the table's own `INTEGER PRIMARY KEY` alias column (when it
    // has one) is the rowid — every index leaf entry already carries it,
    // so it's covered by *any* index on this table, not just one that
    // happens to declare it as a column.
    let rowid_col = schema.rowid_alias.and_then(|idx| schema.columns.get(idx));
    let covers = |name: &str| {
        index
            .columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(name))
            || rowid_col.is_some_and(|rc| rc.eq_ignore_ascii_case(name))
    };
    if !result_names.iter().all(|n| covers(n)) {
        return None;
    }
    Some(CoveringIndexMatch {
        index_position,
        operands,
    })
}

/// Emits an index-only ("covering index") scan (#444) in place of
/// `SeekRowid` + full row decode, when [`find_covering_index`] finds an
/// index that carries every column this `SELECT` needs — `SeekIndexEq`
/// (the point probe) + `Column` reads straight out of the matched index
/// entry, never touching the table cursor at all. When the index isn't
/// `UNIQUE` (#450), the initial match may have duplicate-key siblings:
/// an `IdxNext` + leading-column-still-equal recheck loop walks and
/// emits each one, falling out (to `end_label`) the first time the
/// leading column no longer matches the probe (or the index is
/// exhausted) — a `UNIQUE` index's single match still falls out the same
/// way on its very first `IdxNext`, so this subsumes #444's original
/// single-probe behavior rather than branching around it.
///
/// Returns `Ok(true)` when this fast path was taken; `Ok(false)` leaves
/// `em`/`reg` untouched so the caller falls back to the ordinary scan
/// (or [`try_compile_rowid_seek`]).
#[allow(clippy::too_many_arguments)]
pub(super) fn try_compile_covering_index_scan<F>(
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
    let Some(CoveringIndexMatch {
        index_position,
        operands,
    }) = find_covering_index(schema, select)
    else {
        return Ok(false);
    };
    let Some(index) = schema.indexes.get(index_position) else {
        return Ok(false);
    };
    // Re-derived rather than threaded through `find_covering_index`'s
    // return value: `bare_result_column_names` is infallible once
    // `find_covering_index` has already returned `Some`.
    let result_names = bare_result_column_names(select, schema).unwrap_or_default();

    let index_cursor = cursors.sort;
    let root_page = crate::codegen::index_maintenance::valid_index_root_page(index)?;
    let mut open_instr = Instruction::new(Opcode::OpenRead, index_cursor, root_page, 0);
    open_instr.p5 = 1;
    em.emit(open_instr);

    let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    let limit = compile_limit_setup(em, reg, &scope, select)?;
    let leading_collation = index
        .columns
        .first()
        .map_or(Collation::Binary, |c| c.collation);

    // #605: one probe per resolved operand — a single value in the
    // common case, or one per value of an OR-to-IN-converted chain.
    // `not_found` is where a mismatch/exhaustion for *this* operand
    // continues: the next operand's fresh `SeekIndexEq`, or `end_label`
    // once there are no more.
    let mut operands = operands.into_iter().peekable();
    while let Some(operand) = operands.next() {
        let not_found = if operands.peek().is_some() {
            em.new_label()
        } else {
            end_label
        };

        let value_reg = compile_value(em, reg, &scope, &operand)?;
        let seek_addr = em.emit(Instruction::with_p4(
            Opcode::SeekIndexEq,
            index_cursor,
            0,
            value_reg,
            P4::SeekKey(vec![leading_collation]),
        ));
        em.patch_p2(seek_addr, not_found);

        let loop_start = em.new_label();
        em.place(loop_start);

        let row_skip = em.new_label();
        if let Some(limit) = &limit {
            emit_offset_guard(em, limit, row_skip);
        }
        if let Some(limit) = &limit {
            emit_limit_guard(em, limit, end_label);
        }

        let mut regs = Vec::with_capacity(result_names.len());
        for name in &result_names {
            let r = reg.alloc();
            match index
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
            {
                Some(col_idx) => em.emit(Instruction::new(
                    Opcode::Column,
                    index_cursor,
                    i32::try_from(col_idx).unwrap_or(0),
                    r,
                )),
                // #535: not one of the index's own columns — must be the
                // rowid-alias column `covers()` (in `find_covering_index`)
                // let through, retrievable from the index leaf's own rowid
                // rather than a declared column.
                None => em.emit(Instruction::new(Opcode::IdxRowid, index_cursor, r, 0)),
            };
            regs.push(r);
        }
        let (first, count) = match regs.first() {
            Some(&first) => (first, i32::try_from(regs.len()).unwrap_or(0)),
            None => (reg.alloc(), 0),
        };
        sink(em, reg, first, count)?;

        // A `UNIQUE` index's single match falls straight out here on the
        // very first `IdxNext` (nothing shares its key); a non-`UNIQUE`
        // index's duplicate-key siblings loop back to `loop_start` for as
        // long as the leading column keeps matching the probe (#450).
        // Exhausting the index entirely (`IdxNext` failing) is handled
        // the same as a leading-column mismatch (`not_found`): either
        // way, this operand's matches are done, but another OR-chain
        // operand may still have its own fresh seek to try.
        em.place(row_skip);
        let next_addr = em.emit(Instruction::new(Opcode::IdxNext, index_cursor, 0, 0));
        let recheck = em.new_label();
        em.patch_p2(next_addr, recheck);
        let exhausted = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
        em.patch_p2(exhausted, not_found);

        em.place(recheck);
        let leading = reg.alloc();
        em.emit(Instruction::new(Opcode::Column, index_cursor, 0, leading));
        // Reuses `leading_collation` (#500) — the same declared `COLLATE`
        // `SeekIndexEq`'s own probe comparison just above already used, so
        // the recheck never disagrees with the seek that feeds it.
        let eq_addr = em.emit(Instruction::with_p4(
            Opcode::Eq,
            leading,
            0,
            value_reg,
            p4_coll_seq(leading_collation, Affinity::Blob),
        ));
        em.patch_p2(eq_addr, loop_start);
        // Eq's fallthrough (leading column no longer matches this
        // operand's value) needs an explicit jump to `not_found` — unlike
        // the single-operand original, more code may follow in program
        // order (the next operand's own seek), so this can no longer
        // rely on physically falling into `end_label`.
        let mismatch = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
        em.patch_p2(mismatch, not_found);

        if not_found != end_label {
            em.place(not_found);
        }
    }

    Ok(true)
}

/// A [`find_skip_scan_index`] match: which of `schema.indexes` was
/// matched, the matched column's position *within that index*
/// (guaranteed `> 0` — position `0` is the leading column, already
/// handled by [`find_covering_index`]/[`try_compile_rowid_seek`]), and
/// a clone of the probe operand expression.
pub(super) struct SkipScanMatch {
    pub(super) index_position: usize,
    pub(super) column_position: usize,
    pub(super) operand: Expr,
}

/// Finds an index usable for a skip-scan (#485) over `select`:
/// `select.where_clause` must be a single top-level equality between a
/// *non-leading* column of one of `schema.indexes` and a literal/bind-
/// parameter operand, and [`is_skip_scan_worthwhile`] must judge the
/// index's leading column low-cardinality enough (per `stats`) for
/// walking the whole index to beat a full table scan. Deliberately
/// narrow, mirroring [`find_covering_index`]'s scope: an integer
/// literal or bind-parameter operand only, no `DISTINCT`, no `WITHOUT
/// ROWID` table (this path leans on `SeekRowid` to fetch the full row
/// once the index entry matches, same as
/// [`super::index_scan::try_compile_index_ordered_scan`]).
pub(super) fn find_skip_scan_index(
    schema: &TableSchema,
    select: &Select,
    stats: &Stats,
) -> Option<SkipScanMatch> {
    if matches!(select.distinct, Some(Distinctness::Distinct)) {
        return None;
    }
    if schema.without_rowid {
        return None;
    }
    let where_expr = select.where_clause.as_ref()?;
    // #605: same constant-propagation fallback as `find_covering_index`.
    let constants = propagate_constants(where_expr);
    schema
        .indexes
        .iter()
        .enumerate()
        .find_map(|(index_position, index)| {
            let column_position = index
                .columns
                .iter()
                .position(|c| constants.contains_key(&c.name.to_ascii_lowercase()))?;
            if column_position == 0 {
                // The leading column is a covering-index/rowid-seek match,
                // not a skip-scan one.
                return None;
            }
            if !is_skip_scan_worthwhile(&index.name, stats) {
                return None;
            }
            let matched_name = &index.columns.get(column_position)?.name;
            let operand = constants.get(&matched_name.to_ascii_lowercase())?.clone();
            Some(SkipScanMatch {
                index_position,
                column_position,
                operand,
            })
        })
}

/// Compiles a skip-scan (#485): a query filtering on a non-leading
/// column of a composite index, where [`find_skip_scan_index`] judges
/// the leading column's cardinality low enough that walking the whole
/// index (`IdxRewind`/`IdxNext`) — checking the matched column on each
/// narrower index entry, then `IdxRowid` + `SeekRowid` to fetch the
/// full row only for a match — beats a full `Rewind`/`Next` table scan
/// that decodes every column of every row. Unlike real SQLite's
/// skip-scan (a genuine per-distinct-leading-value binary seek),
/// `IndexCursor::seek` in this codebase is a documented Tier 0 linear
/// scan (`src/btree/index.rs`), so this walks every index entry rather
/// than truly skipping past a large group once it stops matching — the
/// win here comes from decoding narrower index rows and only touching
/// the table for genuine matches, not from sub-linear seeking.
///
/// Returns `Ok(true)` when this fast path was taken; `Ok(false)` leaves
/// `em`/`reg` untouched so the caller falls back to the ordinary scan.
#[allow(clippy::too_many_arguments)]
pub(super) fn try_compile_skip_scan_index<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    cursors: ScanCursors,
    end_label: Label,
    catalog: &[TableSchema],
    stats: &Stats,
    sink: &mut F,
) -> Result<bool, CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    let Some(SkipScanMatch {
        index_position,
        column_position,
        operand,
    }) = find_skip_scan_index(schema, select, stats)
    else {
        return Ok(false);
    };
    let Some(index) = schema.indexes.get(index_position) else {
        return Ok(false);
    };

    // No dedicated cursor slot exists for this path's index cursor —
    // reuse the sort cursor number, mirroring
    // `try_compile_index_ordered_scan`/`try_compile_covering_index_scan`
    // (neither of `SorterOpen`/`SorterInsert` ever runs on this branch).
    let index_cursor = cursors.sort;
    let root_page = crate::codegen::index_maintenance::valid_index_root_page(index)?;
    let mut open_instr = Instruction::new(Opcode::OpenRead, index_cursor, root_page, 0);
    open_instr.p5 = 1;
    em.emit(open_instr);

    let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    let limit = compile_limit_setup(em, reg, &scope, select)?;
    let probe_reg = compile_value(em, reg, &scope, &operand)?;

    let rewind_addr = em.emit(Instruction::new(Opcode::IdxRewind, index_cursor, 0, 0));
    em.patch_p2(rewind_addr, end_label);
    let loop_start = em.new_label();
    em.place(loop_start);

    let row_skip = em.new_label();
    let col_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::Column,
        index_cursor,
        i32::try_from(column_position).unwrap_or(0),
        col_reg,
    ));
    let eq_addr = em.emit(Instruction::new(Opcode::Eq, col_reg, 0, probe_reg));
    let matched = em.new_label();
    em.patch_p2(eq_addr, matched);
    let mismatch_addr = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
    em.patch_p2(mismatch_addr, row_skip);

    em.place(matched);
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
    let next_addr = em.emit(Instruction::new(Opcode::IdxNext, index_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compile_direct_scan<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    cursors: ScanCursors,
    end_label: Label,
    catalog: &[TableSchema],
    stats: &Stats,
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    if try_compile_rowid_seek(em, reg, select, schema, cursors, end_label, catalog, sink)? {
        return Ok(());
    }
    if try_compile_covering_index_scan(em, reg, select, schema, cursors, end_label, catalog, sink)?
    {
        return Ok(());
    }
    if super::range_scan::try_compile_between_seek(
        em, reg, select, schema, cursors, end_label, catalog, sink,
    )? {
        return Ok(());
    }
    if super::range_scan::try_compile_like_prefix_seek(
        em, reg, select, schema, cursors, end_label, catalog, sink,
    )? {
        return Ok(());
    }
    if super::range_scan::try_compile_in_list_seek(
        em, reg, select, schema, cursors, end_label, catalog, sink,
    )? {
        return Ok(());
    }
    if super::range_scan::try_compile_forward_comparison_seek(
        em, reg, select, schema, cursors, end_label, catalog, sink,
    )? {
        return Ok(());
    }
    if try_compile_skip_scan_index(
        em, reg, select, schema, cursors, end_label, catalog, stats, sink,
    )? {
        return Ok(());
    }
    if matches!(select.distinct, Some(Distinctness::Distinct)) {
        em.emit(Instruction::new(
            Opcode::OpenEphemeral,
            cursors.distinct,
            0,
            0,
        ));
    }
    let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    // #306: hoist any uncorrelated WHERE-clause IN/scalar subquery out
    // of the scan loop below, materializing it exactly once here rather
    // than on every outer row.
    let hoisted = match &select.where_clause {
        Some(where_expr) => crate::codegen::subquery::hoist_uncorrelated_where_subqueries(
            em, reg, &scope, where_expr,
        )?,
        None => std::collections::HashMap::new(),
    };
    let scope = scope.with_hoisted(std::rc::Rc::new(hoisted));
    // #314: memoize any correlated, single-outer-column WHERE-clause
    // scalar subquery against the scan's per-row correlated value,
    // instead of re-running it on every outer row.
    let memoized = match &select.where_clause {
        Some(where_expr) => crate::codegen::subquery::memoize_correlated_where_subqueries(
            em, reg, &scope, schema, where_expr,
        ),
        None => std::collections::HashMap::new(),
    };
    let scope = scope.with_memoized(std::rc::Rc::new(memoized));
    let limit = compile_limit_setup(em, reg, &scope, select)?;

    let rewind_addr = em.emit(Instruction::new(Opcode::Rewind, cursors.table, 0, 0));
    em.patch_p2(rewind_addr, end_label);
    let loop_start = em.new_label();
    em.place(loop_start);

    let row_skip = em.new_label();
    if let Some(where_expr) = &select.where_clause {
        compile_cond(
            em,
            reg,
            &scope,
            where_expr,
            // `WHERE` is the boundary where SQL's three-valued logic
            // collapses to two: a predicate whose truth is unknown
            // excludes the row exactly like a false one.
            CondTargets::null_is_false(Target::Fallthrough, Target::Jump(row_skip)),
        )?;
    }
    emit_distinct_guard(
        em,
        reg,
        select,
        schema,
        cursors.table,
        false,
        cursors.distinct,
        row_skip,
        catalog,
    )?;
    if let Some(limit) = &limit {
        emit_offset_guard(em, limit, row_skip);
    }
    if let Some(limit) = &limit {
        emit_limit_guard(em, limit, end_label);
    }
    emit_row_via_sink(em, reg, select, schema, cursors.table, false, catalog, sink)?;

    em.place(row_skip);
    let next_addr = em.emit(Instruction::new(Opcode::Next, cursors.table, 0, 0));
    em.patch_p2(next_addr, loop_start);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compile_sorted_scan<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    order_by_plans: &[OrderByPlan],
    cursors: ScanCursors,
    end_label: Label,
    catalog: &[TableSchema],
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    if matches!(select.distinct, Some(Distinctness::Distinct)) {
        em.emit(Instruction::new(
            Opcode::OpenEphemeral,
            cursors.distinct,
            0,
            0,
        ));
    }

    let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    // #306: same hoist as `compile_direct_scan` — see its comment.
    let hoisted = match &select.where_clause {
        Some(where_expr) => crate::codegen::subquery::hoist_uncorrelated_where_subqueries(
            em, reg, &scope, where_expr,
        )?,
        None => std::collections::HashMap::new(),
    };
    let scope = scope.with_hoisted(std::rc::Rc::new(hoisted));
    // #314: same memoization as `compile_direct_scan` — see its comment.
    let memoized = match &select.where_clause {
        Some(where_expr) => crate::codegen::subquery::memoize_correlated_where_subqueries(
            em, reg, &scope, schema, where_expr,
        ),
        None => std::collections::HashMap::new(),
    };
    let scope = scope.with_memoized(std::rc::Rc::new(memoized));

    // LIMIT/OFFSET are set up here, before the sorter opens, rather than
    // just before pass 2 — so a combined bound register is ready in time
    // to cap the sorter's buffer to the top-K rows it actually needs
    // (#129: an unbounded sort was the dominant cost of `ORDER BY ...
    // LIMIT N` on large tables). Reuses `OffsetLimit` (already
    // implemented for #89, previously unused by codegen — see this
    // module's doc comment) for its `-1`-means-unbounded convention
    // (`LIMIT -1`/no `LIMIT`), so the sorter falls back to its old
    // unbounded behavior automatically whenever the bound can't be
    // known to be safe. Bounding is skipped for `DISTINCT`: it dedupes
    // *after* the sort, so a bound applied before dedup could evict a
    // row DISTINCT would have deduped away, undercounting the result.
    let limit = compile_limit_setup(em, reg, &scope, select)?;
    let bound_reg = if matches!(select.distinct, Some(Distinctness::Distinct)) {
        None
    } else {
        limit.as_ref().map(|limit_state| {
            let offset_reg = limit_state.offset_reg.unwrap_or_else(|| {
                let zero = reg.alloc();
                em.emit(Instruction::new(Opcode::Integer, 0, zero, 0));
                zero
            });
            let combined = reg.alloc();
            em.emit(Instruction::new(
                Opcode::OffsetLimit,
                limit_state.limit_reg.unwrap_or(0),
                combined,
                offset_reg,
            ));
            combined
        })
    };

    // The sort-key descriptor (which register each term reads) isn't
    // known until pass 1 below actually allocates the computed-expression
    // registers, so `SorterOpen` is emitted with a placeholder P4 and
    // patched once that layout is known — it must still precede the scan
    // loop in program order.
    let mut sorter_open = Instruction::with_p4(Opcode::SorterOpen, cursors.sort, 0, 0, P4::None);
    if let Some(bound_reg) = bound_reg {
        sorter_open.p2 = bound_reg;
        sorter_open.p5 = 1;
    }
    let sorter_open_addr = em.emit(sorter_open);

    // Pass 1: buffer every matching row's full column tuple — plus a
    // trailing register per computed ORDER BY expression — into the
    // sorter, WHERE-filtered but pre-DISTINCT/LIMIT (those apply on
    // the sorted output, matching SQLite's own ORDER BY pipeline
    // shape). The trailing expression registers are never read back by
    // `sink` (it only ever projects `select.columns`), so they exist
    // purely as sort keys.
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
            &scope,
            where_expr,
            // `WHERE` is the boundary where SQL's three-valued logic
            // collapses to two: a predicate whose truth is unknown
            // excludes the row exactly like a false one.
            CondTargets::null_is_false(Target::Fallthrough, Target::Jump(scan_skip)),
        )?;
    }
    let (first, _schema_count) = compile_row_values(
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

    // Compute every genuine-expression sort key into its own register,
    // appended after the schema-column block. A key's final register
    // need not be the highest one its expression allocates (e.g. `CASE`
    // allocates its destination before its branches), so the record's
    // span is widened to `reg`'s post-compile watermark rather than
    // trusting the last returned register — any intervening temporary
    // just becomes an unread extra field.
    let mut sort_keys = Vec::with_capacity(order_by_plans.len());
    for plan in order_by_plans {
        let index = match &plan.target {
            OrderByTarget::Column(idx) => *idx,
            OrderByTarget::Expr(expr) => {
                let r = compile_value(em, reg, &scope, expr)?;
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
        cursors.sort,
        record_reg,
        0,
    ));

    em.place(scan_skip);
    let scan_next = em.emit(Instruction::new(Opcode::Next, cursors.table, 0, 0));
    em.patch_p2(scan_next, scan_loop);

    // Pass 2: iterate the sorted buffer, re-deriving the schema's full
    // column tuple from each sorted record via an `OpenPseudo` cursor,
    // then apply DISTINCT/LIMIT/OFFSET and emit result columns exactly
    // as the direct-scan path does, reading from `cursors.pseudo`
    // instead of `cursors.table`.
    em.place(sort_step);
    let sort_addr = em.emit(Instruction::new(Opcode::SorterSort, cursors.sort, 0, 0));
    em.patch_p2(sort_addr, end_label);

    let sorted_loop = em.new_label();
    em.place(sorted_loop);
    let sorter_data_reg = reg.alloc();
    em.emit(Instruction::new(
        Opcode::SorterData,
        cursors.sort,
        sorter_data_reg,
        0,
    ));
    // Re-opened every iteration rather than opened once before the loop
    // with `sorter_data_reg` merely updated: `cursor.rs`'s pseudo-cursor
    // is a cheap, idempotent register-pointer rebind (no allocation or
    // I/O), and this mirrors SQLite's own per-row `OpenPseudo` re-open
    // when the underlying data register changes each iteration.
    em.emit(Instruction::new(
        Opcode::OpenPseudo,
        cursors.pseudo,
        sorter_data_reg,
        0,
    ));

    let row_skip = em.new_label();
    emit_distinct_guard(
        em,
        reg,
        select,
        schema,
        cursors.pseudo,
        true,
        cursors.distinct,
        row_skip,
        catalog,
    )?;
    if let Some(limit) = &limit {
        emit_offset_guard(em, limit, row_skip);
    }
    if let Some(limit) = &limit {
        emit_limit_guard(em, limit, end_label);
    }
    emit_row_via_sink(em, reg, select, schema, cursors.pseudo, true, catalog, sink)?;

    em.place(row_skip);
    let sorted_next = em.emit(Instruction::new(Opcode::SorterNext, cursors.sort, 0, 0));
    em.patch_p2(sorted_next, sorted_loop);
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOp, Literal};
    use crate::parser::tokenizer::Span;

    fn span() -> Span {
        Span {
            line: 0,
            column: 0,
            offset: 0,
            len: 0,
        }
    }

    fn col(name: &str) -> Expr {
        Expr {
            kind: ExprKind::Column {
                table: None,
                catalog: None,
                name: name.to_string(),
            },
            span: span(),
        }
    }

    fn lit_int(n: i64) -> Expr {
        Expr {
            kind: ExprKind::Literal(Literal::Integer(n)),
            span: span(),
        }
    }

    fn binary(op: BinaryOp, lhs: Expr, rhs: Expr) -> Expr {
        Expr {
            kind: ExprKind::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            },
            span: span(),
        }
    }

    fn eq(lhs: Expr, rhs: Expr) -> Expr {
        binary(BinaryOp::Eq, lhs, rhs)
    }

    fn and(lhs: Expr, rhs: Expr) -> Expr {
        binary(BinaryOp::And, lhs, rhs)
    }

    fn resolved(constants: &HashMap<String, Expr>, name: &str) -> Option<i64> {
        match constants.get(name)?.kind {
            ExprKind::Literal(Literal::Integer(n)) => Some(n),
            _ => None,
        }
    }

    /// #605: `a = b AND b = 5` propagates the literal to both `a` and `b`.
    #[test]
    fn propagate_constants_resolves_direct_chain() {
        let where_expr = and(eq(col("a"), col("b")), eq(col("b"), lit_int(5)));
        let constants = propagate_constants(&where_expr);
        assert_eq!(resolved(&constants, "a"), Some(5));
        assert_eq!(resolved(&constants, "b"), Some(5));
    }

    /// #605: propagation follows a multi-hop chain (`a = b AND b = c AND
    /// c = 5`), not just a single indirection.
    #[test]
    fn propagate_constants_resolves_transitive_chain() {
        let where_expr = and(
            and(eq(col("a"), col("b")), eq(col("b"), col("c"))),
            eq(col("c"), lit_int(7)),
        );
        let constants = propagate_constants(&where_expr);
        assert_eq!(resolved(&constants, "a"), Some(7));
        assert_eq!(resolved(&constants, "b"), Some(7));
        assert_eq!(resolved(&constants, "c"), Some(7));
    }

    /// A single top-level equality (no `AND`) still resolves — the
    /// pre-existing, non-compound shape must keep working exactly as it
    /// did before propagation existed.
    #[test]
    fn propagate_constants_resolves_single_equality() {
        let where_expr = eq(col("a"), lit_int(1));
        let constants = propagate_constants(&where_expr);
        assert_eq!(resolved(&constants, "a"), Some(1));
    }

    /// An `OR`-joined condition must not propagate: `a = b OR b = 5`
    /// does not imply `a = 5`, so neither should end up in the map.
    #[test]
    fn propagate_constants_ignores_or() {
        let where_expr = binary(
            BinaryOp::Or,
            eq(col("a"), col("b")),
            eq(col("b"), lit_int(5)),
        );
        let constants = propagate_constants(&where_expr);
        assert!(constants.is_empty());
    }

    /// A non-equality conjunct (`a < 5`) must not feed the map, even
    /// alongside an otherwise-propagating equality chain.
    #[test]
    fn propagate_constants_ignores_non_equality_conjuncts() {
        let where_expr = and(
            eq(col("a"), col("b")),
            binary(BinaryOp::Lt, col("b"), lit_int(5)),
        );
        let constants = propagate_constants(&where_expr);
        assert!(constants.is_empty());
    }

    fn or(lhs: Expr, rhs: Expr) -> Expr {
        binary(BinaryOp::Or, lhs, rhs)
    }

    fn resolved_ints(operands: Vec<&Expr>) -> Vec<i64> {
        operands
            .into_iter()
            .map(|e| match e.kind {
                ExprKind::Literal(Literal::Integer(n)) => n,
                _ => panic!("expected integer literal, got {e:?}"),
            })
            .collect()
    }

    /// #605: `x = 1 OR x = 2 OR x = 3` converts to the operand list
    /// `[1, 2, 3]` for a fast path to probe once per value.
    #[test]
    fn or_chain_equality_operands_collects_same_column_chain() {
        let where_expr = or(
            or(eq(col("x"), lit_int(1)), eq(col("x"), lit_int(2))),
            eq(col("x"), lit_int(3)),
        );
        let operands =
            or_chain_equality_operands(&where_expr, |e| where_col(e) == Some("x")).unwrap();
        assert_eq!(resolved_ints(operands), vec![1, 2, 3]);
    }

    /// A disjunct against a *different* column disqualifies the whole
    /// chain — a fast path that only probes `x` can't also enforce `y`.
    #[test]
    fn or_chain_equality_operands_rejects_mixed_columns() {
        let where_expr = or(eq(col("x"), lit_int(1)), eq(col("y"), lit_int(2)));
        assert!(or_chain_equality_operands(&where_expr, |e| where_col(e) == Some("x")).is_none());
    }

    /// A single equality (no `OR` at all) is not an OR-chain — that
    /// shape is handled by the direct-equality path instead.
    #[test]
    fn or_chain_equality_operands_rejects_non_chain() {
        let where_expr = eq(col("x"), lit_int(1));
        assert!(or_chain_equality_operands(&where_expr, |e| where_col(e) == Some("x")).is_none());
    }

    /// A non-equality disjunct (`x < 5`) disqualifies the whole chain —
    /// a fast path built only for point probes can't also enforce it.
    #[test]
    fn or_chain_equality_operands_rejects_non_equality_disjunct() {
        let where_expr = or(
            eq(col("x"), lit_int(1)),
            binary(BinaryOp::Lt, col("x"), lit_int(5)),
        );
        assert!(or_chain_equality_operands(&where_expr, |e| where_col(e) == Some("x")).is_none());
    }
}
