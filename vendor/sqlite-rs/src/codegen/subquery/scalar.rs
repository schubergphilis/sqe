// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Scalar/`EXISTS`/`IN` subquery-expression compilation — see
//! `super`'s module doc.

use super::from_clause::resolve_subquery_schema;
use super::{select_id, HoistedSubquery};
use crate::codegen::expr::{compile_cond, compile_value};
use crate::codegen::index_maintenance::{valid_index_root_page, valid_table_root_page};
use crate::codegen::select::join_access::{choose_join_access, JoinAccess};
use crate::codegen::select::{
    compile_grouped_scan, select_has_aggregate, try_compile_index_only_count,
    try_compile_index_only_sum, CodegenError, ScanCursors,
};
use crate::codegen::{CondTargets, Emitter, NullTarget, RegAlloc, Scope, Target};
use crate::parser::ast::{Expr, ResultColumn, Select};
use crate::vdbe::{Collation, Instruction, Opcode, P4};

/// A subquery's single projected result-column expression — scalar
/// subqueries and single-column `IN (SELECT ...)` both need exactly one
/// (`SELECT *`/`table.*`/more than one column is `Unsupported`); see
/// [`multi_result_exprs`] for the multi-column `IN` counterpart.
fn single_result_expr(subselect: &Select) -> Result<&Expr, CodegenError> {
    match subselect.columns.as_slice() {
        [ResultColumn::Expr { expr, .. }] => Ok(expr),
        _ => Err(CodegenError::Unsupported {
            reason: "a scalar/IN subquery must project exactly one expression column".to_string(),
        }),
    }
}

/// A subquery's N projected result-column expressions for
/// multi-column `IN` (#251) — `SELECT *`/`table.*` isn't supported
/// here (arity must be known statically from the expression list).
fn multi_result_exprs(subselect: &Select) -> Result<Vec<&Expr>, CodegenError> {
    subselect
        .columns
        .iter()
        .map(|c| match c {
            ResultColumn::Expr { expr, .. } => Ok(expr),
            _ => Err(CodegenError::Unsupported {
                reason: "a multi-column IN subquery's result columns must be plain expressions \
                         (no * / table.*)"
                    .to_string(),
            }),
        })
        .collect()
}

/// Compiles each of `exprs` into a value register, requiring the
/// results land in a contiguous range (mirrors `select.rs`'s
/// `MakeRecord` contiguity check) — returns `(first register, count)`.
fn compile_contiguous(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    scope: &Scope,
    exprs: impl IntoIterator<Item = impl std::borrow::Borrow<Expr>>,
    what: &str,
) -> Result<(i32, i32), CodegenError> {
    let mut regs = Vec::new();
    for e in exprs {
        regs.push(compile_value(em, reg, scope, e.borrow())?);
    }
    let Some(&first) = regs.first() else {
        return Err(CodegenError::Unsupported {
            reason: format!("{what} must not be empty"),
        });
    };
    for (i, r) in regs.iter().enumerate() {
        let want = first.saturating_add(i32::try_from(i).unwrap_or(i32::MAX));
        if *r != want {
            return Err(CodegenError::Unsupported {
                reason: format!("{what} must land in contiguous registers"),
            });
        }
    }
    Ok((first, i32::try_from(regs.len()).unwrap_or(0)))
}

/// Compiles a scalar subquery `(SELECT ...)` (#238) into a fresh
/// register: NULL if the subquery yields zero rows, otherwise its
/// first result column's value from the *first* row returned (matching
/// SQLite: more than one row silently takes the first rather than
/// erroring).
pub(crate) fn compile_scalar_subquery(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    outer_scope: &Scope,
    subselect: &Select,
) -> Result<i32, CodegenError> {
    if !subselect.order_by.is_empty() || subselect.limit.is_some() {
        return Err(CodegenError::Unsupported {
            reason: "ORDER BY/LIMIT in a scalar subquery is not yet supported".to_string(),
        });
    }
    let dest = reg.alloc();
    em.emit(Instruction::new(Opcode::Null, 0, dest, 0));

    let catalog = outer_scope.catalog.clone();
    let resolved = resolve_subquery_schema(subselect, &catalog)?;
    let Some(schema) = resolved else {
        // No FROM: a single computed expression, evaluated exactly
        // once (no rows to iterate).
        if subselect.where_clause.is_some() {
            return Err(CodegenError::Unsupported {
                reason: "a FROM-less scalar subquery cannot have a WHERE clause".to_string(),
            });
        }
        let col_expr = single_result_expr(subselect)?;
        let empty_scope = Scope::default()
            .with_catalog(catalog)
            .with_outer(outer_scope.clone());
        let v = compile_value(em, reg, &empty_scope, col_expr)?;
        em.emit(Instruction::new(Opcode::Copy, v, dest, 0));
        return Ok(dest);
    };

    let sub_cursor = reg.alloc_cursor();

    let root_page = valid_table_root_page(&schema)?;
    em.emit(Instruction::new(Opcode::OpenRead, sub_cursor, root_page, 0));

    if select_has_aggregate(subselect) {
        // #304: the subquery's projected expression contains an
        // aggregate call (e.g. `(SELECT max(x) FROM t ...)`) — route
        // through the same implicit-whole-table-group machinery #287
        // built for a top-level `GROUP BY`-less aggregate query, via
        // its `sink` callback, instead of `compile_value`'s plain
        // (aggregate-rejecting) expression path. `compile_grouped_scan`
        // always emits exactly one finalized group's registers, so the
        // sink just copies the first of them into `dest` — no loop/
        // `Rewind`/`Next`/`WHERE`-skip bookkeeping needed here, that's
        // all internal to `compile_grouped_scan` now.
        let cursors = ScanCursors {
            table: sub_cursor,
            sort: reg.alloc_cursor(),
            pseudo: reg.alloc_cursor(),
            distinct: reg.alloc_cursor(),
        };
        let mut sink = |em: &mut Emitter, _reg: &mut RegAlloc, first: i32, _count: i32| {
            em.emit(Instruction::new(Opcode::Copy, first, dest, 0));
            Ok(())
        };
        // #634: try the same index-only fast paths top-level aggregate
        // queries get (`entry.rs`'s dispatch order) before falling back
        // to `compile_grouped_scan`'s buffer-then-flush machinery — a
        // subquery's projected aggregate is otherwise never given the
        // chance at an index-only scan.
        if try_compile_index_only_count(em, reg, subselect, &schema, cursors, &catalog, &mut sink)?
        {
            return Ok(dest);
        }
        if try_compile_index_only_sum(em, reg, subselect, &schema, cursors, &mut sink)? {
            return Ok(dest);
        }
        let end_label = em.new_label();
        compile_grouped_scan(
            em,
            reg,
            subselect,
            &schema,
            cursors,
            end_label,
            &catalog,
            true,
            Some(outer_scope),
            &mut sink,
        )?;
        em.place(end_label);
        return Ok(dest);
    }

    let col_expr = single_result_expr(subselect)?;
    let sub_scope = Scope::single(&schema, sub_cursor)
        .with_catalog(catalog)
        .with_outer(outer_scope.clone());

    let end_label = em.new_label();

    // #434: a `WHERE` clause that's a single equality between this
    // subquery's own table and a safe outer-query probe (the
    // correlated case) compiles to a `SeekRowid`/`SeekIndexEq` point
    // lookup — the same #243 join-level access strategy
    // (`join_access::choose_join_access`) a `JOIN ... ON` condition
    // gets — instead of an unconditional `Rewind`/`Next` scan. This is
    // the actual technique the sqlite3 oracle uses for this exact
    // query shape (confirmed via `EXPLAIN`: `cat = t.y` compiles to a
    // single `SeekRowid`, never a table scan, and the subquery is
    // simply re-run per outer row — no caching at all), and it makes
    // #314's memoization cache unnecessary for this shape (that cache
    // still helps a correlated subquery whose `WHERE` isn't a seekable
    // equality).
    let seek_access = subselect.where_clause.as_ref().and_then(|where_expr| {
        let sub_binding = sub_scope.tables.first()?;
        choose_join_access(sub_binding, where_expr, &outer_scope.tables)
    });

    if let Some(access) = seek_access {
        let value_reg = match &access {
            JoinAccess::Rowid(operand) | JoinAccess::UniqueIndex { operand, .. } => {
                compile_value(em, reg, outer_scope, operand)?
            }
        };
        // A NULL probe value can never equal anything (SQL's `NULL =
        // x` is unknown, not true) — `SeekRowid`/`SeekIndexEq` require
        // an actual key, so this must be checked explicitly rather
        // than let it reach either opcode as a malformed target.
        let null_addr = em.emit(Instruction::new(Opcode::IsNull, value_reg, 0, 0));
        em.patch_p2(null_addr, end_label);
        match access {
            JoinAccess::Rowid(_) => {
                let seek_addr = em.emit(Instruction::new(
                    Opcode::SeekRowid,
                    sub_cursor,
                    0,
                    value_reg,
                ));
                em.patch_p2(seek_addr, end_label);
            }
            JoinAccess::UniqueIndex { index, .. } => {
                let index_cursor = reg.alloc_cursor();
                let root_page = valid_index_root_page(&index)?;
                let mut open_instr = Instruction::new(Opcode::OpenRead, index_cursor, root_page, 0);
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
                em.patch_p2(seek_addr, end_label);
                let rowid_reg = reg.alloc();
                em.emit(Instruction::new(
                    Opcode::IdxRowid,
                    index_cursor,
                    rowid_reg,
                    0,
                ));
                let table_seek_addr = em.emit(Instruction::new(
                    Opcode::SeekRowid,
                    sub_cursor,
                    0,
                    rowid_reg,
                ));
                em.patch_p2(table_seek_addr, end_label);
            }
        }
        let v = compile_value(em, reg, &sub_scope, col_expr)?;
        em.emit(Instruction::new(Opcode::Copy, v, dest, 0));
        em.goto(end_label);
        em.place(end_label);
        return Ok(dest);
    }

    let rewind_addr = em.emit(Instruction::new(Opcode::Rewind, sub_cursor, 0, 0));
    em.patch_p2(rewind_addr, end_label);
    let loop_start = em.new_label();
    em.place(loop_start);

    let skip = em.new_label();
    if let Some(where_expr) = &subselect.where_clause {
        compile_cond(
            em,
            reg,
            &sub_scope,
            where_expr,
            CondTargets::null_is_false(Target::Fallthrough, Target::Jump(skip)),
        )?;
    }
    let v = compile_value(em, reg, &sub_scope, col_expr)?;
    em.emit(Instruction::new(Opcode::Copy, v, dest, 0));
    em.goto(end_label);

    em.place(skip);
    let next_addr = em.emit(Instruction::new(Opcode::Next, sub_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);
    em.place(end_label);
    Ok(dest)
}

/// Compiles `EXISTS (SELECT ...)`/`NOT EXISTS (SELECT ...)` (#238) as a
/// jump: runs the subquery's scan and jumps to the true continuation as
/// soon as one row satisfies its `WHERE` clause (or immediately, if it
/// has none), without materializing anything — cheaper than the
/// scalar/`IN` forms since `EXISTS` never needs a row's actual values.
/// `EXISTS` is always definitely true or false (never SQL's unknown),
/// so `targets.on_null` is not consulted.
///
/// #580: when the `WHERE` clause is a single correlated equality
/// against a rowid or unique index (the same shape #434 detects for
/// scalar subqueries via `choose_join_access`), this compiles to a
/// `SeekRowid`/`SeekIndexEq` point lookup instead of an unconditional
/// `Rewind`/`Next` scan — no scan loop at all, not merely an early exit
/// from one.
pub(crate) fn compile_exists(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    outer_scope: &Scope,
    subselect: &Select,
    negated: bool,
    targets: CondTargets,
) -> Result<(), CodegenError> {
    let catalog = outer_scope.catalog.clone();
    let resolved = resolve_subquery_schema(subselect, &catalog)?;
    let Some(schema) = resolved else {
        return Err(CodegenError::Unsupported {
            reason: "EXISTS (SELECT ...) requires a FROM clause".to_string(),
        });
    };
    let sub_cursor = reg.alloc_cursor();
    let sub_scope = Scope::single(&schema, sub_cursor)
        .with_catalog(catalog)
        .with_outer(outer_scope.clone());

    let (exists_true, exists_false) = if negated {
        (targets.on_false, targets.on_true)
    } else {
        (targets.on_true, targets.on_false)
    };
    let (t_label, t_is_new) = crate::codegen::expr::ensure_label(em, exists_true);

    let seek_access = subselect.where_clause.as_ref().and_then(|where_expr| {
        let sub_binding = sub_scope.tables.first()?;
        choose_join_access(sub_binding, where_expr, &outer_scope.tables)
    });

    if let Some(access) = seek_access {
        let not_found = em.new_label();
        let value_reg = match &access {
            JoinAccess::Rowid(operand) | JoinAccess::UniqueIndex { operand, .. } => {
                compile_value(em, reg, outer_scope, operand)?
            }
        };
        // A NULL probe value can never equal anything (SQL's `NULL =
        // x` is unknown, not true) — `SeekRowid`/`SeekIndexEq` require
        // an actual key, so this must be checked explicitly rather
        // than let it reach either opcode as a malformed target.
        let null_addr = em.emit(Instruction::new(Opcode::IsNull, value_reg, 0, 0));
        em.patch_p2(null_addr, not_found);
        let root_page = valid_table_root_page(&schema)?;
        em.emit(Instruction::new(Opcode::OpenRead, sub_cursor, root_page, 0));
        match access {
            JoinAccess::Rowid(_) => {
                let seek_addr = em.emit(Instruction::new(
                    Opcode::SeekRowid,
                    sub_cursor,
                    0,
                    value_reg,
                ));
                em.patch_p2(seek_addr, not_found);
            }
            JoinAccess::UniqueIndex { index, .. } => {
                let index_cursor = reg.alloc_cursor();
                let root_page = valid_index_root_page(&index)?;
                let mut open_instr = Instruction::new(Opcode::OpenRead, index_cursor, root_page, 0);
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
                em.patch_p2(seek_addr, not_found);
            }
        }
        em.goto(t_label);
        em.place(not_found);
        if let Target::Jump(fl) = exists_false {
            em.goto(fl);
        }
        if t_is_new {
            em.place(t_label);
        }
        return Ok(());
    }

    let root_page = valid_table_root_page(&schema)?;
    em.emit(Instruction::new(Opcode::OpenRead, sub_cursor, root_page, 0));
    let not_found = em.new_label();
    let rewind_addr = em.emit(Instruction::new(Opcode::Rewind, sub_cursor, 0, 0));
    em.patch_p2(rewind_addr, not_found);
    let loop_start = em.new_label();
    em.place(loop_start);

    let skip = em.new_label();
    if let Some(where_expr) = &subselect.where_clause {
        compile_cond(
            em,
            reg,
            &sub_scope,
            where_expr,
            CondTargets::null_is_false(Target::Fallthrough, Target::Jump(skip)),
        )?;
    }
    em.goto(t_label);
    em.place(skip);
    let next_addr = em.emit(Instruction::new(Opcode::Next, sub_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);
    em.place(not_found);

    if let Target::Jump(fl) = exists_false {
        em.goto(fl);
    }
    if t_is_new {
        em.place(t_label);
    }
    Ok(())
}

/// Compiles `expr IN (SELECT ...)`/`expr NOT IN (SELECT ...)` (#238):
/// materializes the subquery's single result column into a fresh
/// ephemeral index (the same `OpenEphemeral`/`IdxInsert`/`Found`
/// machinery `DISTINCT` uses), then tests `expr`'s value for membership.
/// Known simplification: a NULL `expr` always routes to the unknown
/// (`on_null`) continuation, rather than SQLite's more precise rule
/// that `NULL IN (<empty subquery result>)` is definitely false — this
/// matches the literal-list `IN` form's own documented NULL-handling
/// shape in this compiler.
///
/// A strict N=1 case of [`compile_in_subquery_multi`] — this is a thin
/// wrapper over it with a one-element LHS tuple, so both forms share the
/// exact same ephemeral-index/`Found` codegen.
///
/// #306: if this subquery was hoisted (materialized once, before the
/// enclosing scan's `Rewind`, because it's uncorrelated — see
/// `correlation::hoist_uncorrelated_where_subqueries`), its ephemeral
/// index is already built; reuse the cached cursor instead of
/// delegating to `compile_in_subquery_multi`'s normal per-occurrence
/// materialization.
pub(crate) fn compile_in_subquery(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    outer_scope: &Scope,
    lhs: &Expr,
    subselect: &Select,
    negated: bool,
    targets: CondTargets,
) -> Result<(), CodegenError> {
    let Some(HoistedSubquery::In { eph_cursor }) =
        outer_scope.hoisted.get(&select_id(subselect)).copied()
    else {
        return compile_in_subquery_multi(
            em,
            reg,
            outer_scope,
            std::slice::from_ref(lhs),
            subselect,
            negated,
            targets,
        );
    };

    let l = compile_value(em, reg, outer_scope, lhs)?;

    let (true_label, true_is_new) = crate::codegen::expr::ensure_label(em, targets.on_true);
    let (false_label, false_is_new) = crate::codegen::expr::ensure_label(em, targets.on_false);
    let (found_label, notfound_label) = if negated {
        (false_label, true_label)
    } else {
        (true_label, false_label)
    };
    let null_label = match targets.on_null {
        NullTarget::True => true_label,
        NullTarget::False => false_label,
    };

    let null_addr = em.emit(Instruction::new(Opcode::IsNull, l, 0, 0));
    em.patch_p2(null_addr, null_label);
    let found_addr = em.emit(Instruction::with_p4(
        Opcode::Found,
        eph_cursor,
        0,
        l,
        P4::Int(1),
    ));
    em.patch_p2(found_addr, found_label);
    em.goto(notfound_label);

    if false_is_new {
        em.place(false_label);
    }
    if true_is_new {
        em.place(true_label);
    }
    Ok(())
}

/// Materializes a single-column `IN`-subquery's result column into a
/// fresh ephemeral membership index, returning the cursor. Used by
/// `correlation::try_hoist_conjunct` to materialize a hoisted,
/// uncorrelated `IN`-subquery exactly once, before the enclosing scan's
/// `Rewind` (#306), instead of [`compile_in_subquery_multi`]'s normal
/// per-occurrence materialization.
pub(super) fn materialize_in_subquery_index(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    outer_scope: &Scope,
    subselect: &Select,
) -> Result<i32, CodegenError> {
    let catalog = outer_scope.catalog.clone();
    let resolved = resolve_subquery_schema(subselect, &catalog)?;
    let Some(schema) = resolved else {
        return Err(CodegenError::Unsupported {
            reason: "IN (SELECT ...) requires a FROM clause".to_string(),
        });
    };
    let col_expr = single_result_expr(subselect)?;
    let sub_cursor = reg.alloc_cursor();
    let sub_scope = Scope::single(&schema, sub_cursor)
        .with_catalog(catalog)
        .with_outer(outer_scope.clone());

    let eph_cursor = reg.alloc_cursor();
    em.emit(Instruction::new(Opcode::OpenEphemeral, eph_cursor, 0, 0));

    let root_page = valid_table_root_page(&schema)?;
    em.emit(Instruction::new(Opcode::OpenRead, sub_cursor, root_page, 0));
    let scan_end = em.new_label();
    let rewind_addr = em.emit(Instruction::new(Opcode::Rewind, sub_cursor, 0, 0));
    em.patch_p2(rewind_addr, scan_end);
    let loop_start = em.new_label();
    em.place(loop_start);

    let skip = em.new_label();
    if let Some(where_expr) = &subselect.where_clause {
        compile_cond(
            em,
            reg,
            &sub_scope,
            where_expr,
            CondTargets::null_is_false(Target::Fallthrough, Target::Jump(skip)),
        )?;
    }
    let v = compile_value(em, reg, &sub_scope, col_expr)?;
    em.emit(Instruction::with_p4(
        Opcode::IdxInsert,
        eph_cursor,
        v,
        0,
        P4::Int(1),
    ));
    em.place(skip);
    let next_addr = em.emit(Instruction::new(Opcode::Next, sub_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);
    em.place(scan_end);
    Ok(eph_cursor)
}

/// Compiles `(a, b, ...) IN (SELECT ...)`/`... NOT IN (SELECT ...)`
/// (#251): the multi-column generalization of [`compile_in_subquery`].
/// Materializes the subquery's N projected columns into a fresh
/// ephemeral index keyed on all N (`Found`/`IdxInsert`'s `P4::Int`
/// key-column-count, already N-column-capable — see
/// `vdbe/cursor.rs::found`/`idx_insert`), then tests the LHS tuple's N
/// values for membership the same way. Requires the LHS tuple and the
/// subquery's projection to compile into contiguous register ranges
/// (`compile_contiguous`) and to have matching arity. NULL handling
/// mirrors [`compile_in_subquery`]: any NULL component in the LHS tuple
/// routes to the unknown (`on_null`) continuation.
pub(crate) fn compile_in_subquery_multi(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    outer_scope: &Scope,
    lhs_exprs: &[Expr],
    subselect: &Select,
    negated: bool,
    targets: CondTargets,
) -> Result<(), CodegenError> {
    let catalog = outer_scope.catalog.clone();
    let resolved = resolve_subquery_schema(subselect, &catalog)?;
    let Some(schema) = resolved else {
        return Err(CodegenError::Unsupported {
            reason: "IN (SELECT ...) requires a FROM clause".to_string(),
        });
    };
    let col_exprs = multi_result_exprs(subselect)?;
    if col_exprs.len() != lhs_exprs.len() {
        return Err(CodegenError::Unsupported {
            reason: format!(
                "multi-column IN: left-hand tuple has {} column(s) but the subquery projects {}",
                lhs_exprs.len(),
                col_exprs.len()
            ),
        });
    }
    let sub_cursor = reg.alloc_cursor();
    let sub_scope = Scope::single(&schema, sub_cursor)
        .with_catalog(catalog)
        .with_outer(outer_scope.clone());

    let (l_first, l_count) = compile_contiguous(
        em,
        reg,
        outer_scope,
        lhs_exprs.iter(),
        "multi-column IN's left-hand tuple",
    )?;

    let eph_cursor = reg.alloc_cursor();
    em.emit(Instruction::new(Opcode::OpenEphemeral, eph_cursor, 0, 0));

    let root_page = valid_table_root_page(&schema)?;
    em.emit(Instruction::new(Opcode::OpenRead, sub_cursor, root_page, 0));
    let scan_end = em.new_label();
    let rewind_addr = em.emit(Instruction::new(Opcode::Rewind, sub_cursor, 0, 0));
    em.patch_p2(rewind_addr, scan_end);
    let loop_start = em.new_label();
    em.place(loop_start);

    let skip = em.new_label();
    if let Some(where_expr) = &subselect.where_clause {
        compile_cond(
            em,
            reg,
            &sub_scope,
            where_expr,
            CondTargets::null_is_false(Target::Fallthrough, Target::Jump(skip)),
        )?;
    }
    let (v_first, v_count) = compile_contiguous(
        em,
        reg,
        &sub_scope,
        col_exprs.iter().copied(),
        "multi-column IN's subquery projection",
    )?;
    em.emit(Instruction::with_p4(
        Opcode::IdxInsert,
        eph_cursor,
        v_first,
        0,
        P4::Int(v_count.into()),
    ));
    em.place(skip);
    let next_addr = em.emit(Instruction::new(Opcode::Next, sub_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);
    em.place(scan_end);

    let (true_label, true_is_new) = crate::codegen::expr::ensure_label(em, targets.on_true);
    let (false_label, false_is_new) = crate::codegen::expr::ensure_label(em, targets.on_false);
    let (found_label, notfound_label) = if negated {
        (false_label, true_label)
    } else {
        (true_label, false_label)
    };
    let null_label = match targets.on_null {
        NullTarget::True => true_label,
        NullTarget::False => false_label,
    };

    for i in 0..l_count {
        let r = l_first.saturating_add(i);
        let null_addr = em.emit(Instruction::new(Opcode::IsNull, r, 0, 0));
        em.patch_p2(null_addr, null_label);
    }
    let found_addr = em.emit(Instruction::with_p4(
        Opcode::Found,
        eph_cursor,
        0,
        l_first,
        P4::Int(l_count.into()),
    ));
    em.patch_p2(found_addr, found_label);
    em.goto(notfound_label);

    if false_is_new {
        em.place(false_label);
    }
    if true_is_new {
        em.place(true_label);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::codegen::select::compile_select_with_catalog;
    use crate::parser::{parse_select, ParseOutcome};
    use crate::schema::{IndexSchema, IndexedColumn, TableSchema};

    fn table(name: &str, root_page: u32, columns: &[&str], sql: &str) -> TableSchema {
        TableSchema {
            unresolved_autoindex: false,
            name: name.to_string(),
            root_page,
            columns: columns.iter().map(|c| c.to_string()).collect(),
            without_rowid: false,
            strict: false,
            column_types: vec![String::new(); columns.len()],
            column_collations: vec![],
            is_virtual: false,
            sql: sql.to_string(),
            indexes: vec![],
            rowid_alias: None,
        }
        .with_computed_rowid_alias()
    }

    fn select(sql: &str) -> Select {
        match parse_select(sql) {
            ParseOutcome::Accepted(s) => *s,
            other => panic!("failed to parse {sql:?}: {other:?}"),
        }
    }

    fn t() -> TableSchema {
        table("t", 2, &["x"], "CREATE TABLE t(x)")
    }

    fn s_rowid() -> TableSchema {
        table(
            "s",
            3,
            &["id", "v"],
            "CREATE TABLE s(id INTEGER PRIMARY KEY, v)",
        )
    }

    fn s2_unique() -> TableSchema {
        let mut s2 = table("s2", 4, &["k", "v"], "CREATE TABLE s2(k, v)");
        s2.indexes.push(IndexSchema {
            name: "idx_k".to_string(),
            unique: true,
            columns: vec![IndexedColumn {
                name: "k".to_string(),
                desc: false,
                collation: Collation::Binary,
            }],
            root_page: 5,
        });
        s2
    }

    fn compile(sql: &str, catalog: &[TableSchema]) -> Result<crate::vdbe::Program, CodegenError> {
        let sel = select(sql);
        compile_select_with_catalog(&sel, &t(), catalog)
    }

    fn opcodes(program: &crate::vdbe::Program) -> Vec<Opcode> {
        program.instructions.iter().map(|i| i.opcode).collect()
    }

    #[test]
    fn scalar_subquery_plain_scan_with_where() {
        let catalog = [t(), s_rowid()];
        let program = compile("SELECT (SELECT v FROM s WHERE v > 0) FROM t", &catalog).unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::Rewind));
        assert!(ops.contains(&Opcode::Next));
    }

    #[test]
    fn scalar_subquery_correlated_rowid_seek() {
        let catalog = [t(), s_rowid()];
        let program =
            compile("SELECT (SELECT v FROM s WHERE s.id = t.x) FROM t", &catalog).unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::SeekRowid));
    }

    #[test]
    fn scalar_subquery_correlated_unique_index_seek() {
        let catalog = [t(), s2_unique()];
        let program = compile(
            "SELECT (SELECT v FROM s2 WHERE s2.k = t.x) FROM t",
            &catalog,
        )
        .unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::SeekIndexEq));
        assert!(ops.contains(&Opcode::IdxRowid));
        assert!(ops.contains(&Opcode::SeekRowid));
    }

    #[test]
    fn scalar_subquery_with_aggregate() {
        let catalog = [t(), s_rowid()];
        let program = compile("SELECT (SELECT max(v) FROM s) FROM t", &catalog).unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::AggStep));
        assert!(ops.contains(&Opcode::AggFinal));
    }

    fn s_indexed_v() -> TableSchema {
        let mut s = s_rowid();
        s.indexes.push(IndexSchema {
            name: "idx_v".to_string(),
            unique: false,
            columns: vec![IndexedColumn {
                name: "v".to_string(),
                desc: false,
                collation: Collation::Binary,
            }],
            root_page: 6,
        });
        s
    }

    #[test]
    fn scalar_subquery_with_aggregate_index_only_sum() {
        let catalog = [t(), s_indexed_v()];
        let program = compile("SELECT (SELECT sum(v) FROM s) FROM t", &catalog).unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::IdxRewind));
        assert!(ops.contains(&Opcode::IdxNext));
    }

    #[test]
    fn scalar_subquery_with_aggregate_index_only_avg() {
        let catalog = [t(), s_indexed_v()];
        let program = compile("SELECT (SELECT avg(v) FROM s) FROM t", &catalog).unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::IdxRewind));
        assert!(ops.contains(&Opcode::IdxNext));
    }

    #[test]
    fn scalar_subquery_with_aggregate_index_only_count_star() {
        let catalog = [t(), s_rowid()];
        let program = compile("SELECT (SELECT count(*) FROM s) FROM t", &catalog).unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::Count));
    }

    #[test]
    fn scalar_subquery_from_less_computed_expression() {
        let catalog = [t()];
        let program = compile("SELECT (SELECT 1 + 1)", &catalog).unwrap();
        assert!(opcodes(&program).contains(&Opcode::Copy));
    }

    #[test]
    fn scalar_subquery_star_projection_is_unsupported() {
        let catalog = [t(), s_rowid()];
        let err = compile("SELECT (SELECT * FROM s) FROM t", &catalog).unwrap_err();
        match err {
            CodegenError::Unsupported { reason } => {
                assert!(reason.contains("exactly one expression column"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn exists_plain_scan() {
        let catalog = [t(), s_rowid()];
        let program = compile("SELECT x FROM t WHERE EXISTS (SELECT 1 FROM s)", &catalog).unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::Rewind));
    }

    #[test]
    fn not_exists_plain_scan_with_where() {
        let catalog = [t(), s_rowid()];
        let program = compile(
            "SELECT x FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE v > 0)",
            &catalog,
        )
        .unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::Rewind));
        assert!(ops.contains(&Opcode::Next));
    }

    #[test]
    fn exists_correlated_rowid_seek() {
        let catalog = [t(), s_rowid()];
        let program = compile(
            "SELECT x FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.x)",
            &catalog,
        )
        .unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::SeekRowid));
    }

    #[test]
    fn not_exists_correlated_unique_index_seek() {
        let catalog = [t(), s2_unique()];
        let program = compile(
            "SELECT x FROM t WHERE NOT EXISTS (SELECT 1 FROM s2 WHERE s2.k = t.x)",
            &catalog,
        )
        .unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::SeekIndexEq));
    }

    #[test]
    fn exists_from_less_is_unsupported() {
        let catalog = [t()];
        let err = compile("SELECT x FROM t WHERE EXISTS (SELECT 1)", &catalog).unwrap_err();
        match err {
            CodegenError::Unsupported { reason } => {
                assert!(reason.contains("EXISTS (SELECT ...) requires a FROM clause"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn in_subquery_hoisted_uncorrelated() {
        let catalog = [t(), s_rowid()];
        let program = compile("SELECT x FROM t WHERE x IN (SELECT v FROM s)", &catalog).unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::Found));
        assert!(ops.contains(&Opcode::OpenEphemeral));
    }

    #[test]
    fn in_subquery_not_hoisted_when_not_a_bare_conjunct() {
        let catalog = [t(), s_rowid()];
        let program = compile(
            "SELECT x FROM t WHERE (x IN (SELECT v FROM s)) OR (x IS NULL)",
            &catalog,
        )
        .unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::Found));
        assert!(ops.contains(&Opcode::Rewind));
    }

    #[test]
    fn multi_column_in_subquery() {
        let catalog = [t(), s_rowid()];
        let program = compile(
            "SELECT x FROM t WHERE (x, x) IN (SELECT id, v FROM s)",
            &catalog,
        )
        .unwrap();
        let ops = opcodes(&program);
        assert!(ops.contains(&Opcode::IdxInsert));
        assert!(ops.contains(&Opcode::Found));
    }

    #[test]
    fn multi_column_in_subquery_arity_mismatch_is_unsupported() {
        let catalog = [t(), s_rowid()];
        let err = compile(
            "SELECT x FROM t WHERE (x, x) IN (SELECT id FROM s)",
            &catalog,
        )
        .unwrap_err();
        match err {
            CodegenError::Unsupported { reason } => {
                assert!(reason.contains("left-hand tuple has 2 column(s)"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn multi_column_in_subquery_star_is_unsupported() {
        let catalog = [t(), s_rowid()];
        let err = compile(
            "SELECT x FROM t WHERE (x, x) IN (SELECT * FROM s)",
            &catalog,
        )
        .unwrap_err();
        match err {
            CodegenError::Unsupported { reason } => {
                assert!(reason.contains("no * / table.*"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
}
