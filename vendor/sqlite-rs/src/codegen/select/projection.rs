// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::*;
pub(super) enum ResultColumnPlan {
    Column(String),
    Expr(Expr),
}

pub(super) fn result_columns(select: &Select, schema: &TableSchema) -> Vec<ResultColumnPlan> {
    let mut out = Vec::new();
    for col in &select.columns {
        match col {
            ResultColumn::Star | ResultColumn::TableStar { .. } => {
                for name in &schema.columns {
                    out.push(ResultColumnPlan::Column(name.clone()));
                }
            }
            ResultColumn::Expr { expr, .. } => out.push(ResultColumnPlan::Expr(expr.clone())),
        }
    }
    out
}

/// A result column's collation for DISTINCT/UNION dedup purposes (#518):
/// an explicit `x COLLATE name` wins, else a bare column falls back to
/// its schema-declared `COLLATE` (default [`Collation::Binary`]) —
/// mirrors [`order_by::resolve_order_by`]'s identical fallback for
/// ORDER BY keys.
fn result_column_collations(schema: &TableSchema, cols: &[ResultColumnPlan]) -> Vec<Collation> {
    cols.iter()
        .map(|col| match col {
            ResultColumnPlan::Column(name) => schema
                .columns
                .iter()
                .position(|c| c == name)
                .and_then(|idx| schema.column_collations.get(idx).copied())
                .unwrap_or(Collation::Binary),
            ResultColumnPlan::Expr(expr) => {
                collation_of(expr).unwrap_or_else(|| match &order_by::strip_collate(expr).kind {
                    ExprKind::Column { name, .. } => schema
                        .columns
                        .iter()
                        .position(|c| c == name)
                        .and_then(|idx| schema.column_collations.get(idx).copied())
                        .unwrap_or(Collation::Binary),
                    _ => Collation::Binary,
                })
            }
        })
        .collect()
}

/// Compiles each result column into a contiguous register range,
/// returning `(first, count)`. Each column is first compiled into
/// whatever register the bump allocator hands out next (a compound
/// expression, e.g. CASE or a function call, may itself allocate
/// temporaries before settling on its final result register), then
/// `Opcode::Copy`'d into a freshly reserved contiguous run — mirroring
/// the aggregate/snapshot dest-block pattern below (#141).
pub(super) fn compile_row_values(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    schema: &TableSchema,
    cols: &[ResultColumnPlan],
    cursor: i32,
    pseudo: bool,
    catalog: &[TableSchema],
) -> Result<(i32, usize), CodegenError> {
    let mut regs = Vec::with_capacity(cols.len());
    for col in cols {
        let r = match col {
            ResultColumnPlan::Column(name) => {
                let idx =
                    column_index(schema, name).ok_or_else(|| CodegenError::UnknownColumn {
                        name: (*name).to_string(),
                    })?;
                let r = reg.alloc();
                if pseudo && schema.rowid_alias == Some(idx) {
                    // `cursor` is a post-`ORDER BY` `OpenPseudo` re-read
                    // of an already-materialized record (see
                    // `compile_sorted_scan`'s pass 1), not a live table
                    // cursor — there is no rowid to fetch via
                    // `Opcode::Rowid` (it isn't a table cursor at all).
                    // Pass 1 built this record via `emit_column_read`
                    // against the *real* cursor, which already resolved
                    // the rowid alias into an ordinary field at this
                    // same position — so a plain `Column` read recovers
                    // it here.
                    em.emit(Instruction::new(
                        Opcode::Column,
                        cursor,
                        i32::try_from(idx).map_err(|_| CodegenError::Unsupported {
                            reason: format!("column index {idx} does not fit in a P2 operand"),
                        })?,
                        r,
                    ));
                } else {
                    // Must go through `emit_column_read`, not a bare
                    // `Column`: this is the `*` / `tbl.*` expansion path, and
                    // an `INTEGER PRIMARY KEY` column is a NULL placeholder
                    // in the record. Emitting `Column` here is why
                    // `SELECT * FROM t` answered NULL for the rowid alias
                    // while `SELECT id FROM t` (which routes through
                    // `compile_value`) answered correctly.
                    emit_column_read(em, schema, cursor, idx, r)?;
                }
                r
            }
            ResultColumnPlan::Expr(expr) => {
                // A bare `name`/`tbl.name` reference — e.g. plain
                // `SELECT id FROM t ORDER BY id` — compiles as an `Expr`
                // here, not the `Column` variant above (that one is
                // reserved for `*`/`tbl.*` expansion), so it needs the
                // same pseudo-cursor rowid-alias special case: `Rowid`
                // only works against a real table cursor, and `cursor`
                // here may be the post-`ORDER BY` pseudo cursor instead.
                // A compound expression that merely *references* the
                // rowid alias (`id + 1`) isn't covered by this — falls
                // through to `compile_value`, matching this crate's
                // existing register-reuse limitations for compound
                // result-column expressions.
                if let ExprKind::Column {
                    name,
                    table: None,
                    catalog: None,
                } = &expr.kind
                {
                    let pseudo_rowid_idx = pseudo
                        .then(|| column_index(schema, name))
                        .flatten()
                        .filter(|idx| schema.rowid_alias == Some(*idx));
                    if let Some(idx) = pseudo_rowid_idx {
                        let r = reg.alloc();
                        em.emit(Instruction::new(
                            Opcode::Column,
                            cursor,
                            i32::try_from(idx).map_err(|_| CodegenError::Unsupported {
                                reason: format!("column index {idx} does not fit in a P2 operand"),
                            })?,
                            r,
                        ));
                        r
                    } else {
                        compile_value(
                            em,
                            reg,
                            &Scope::single(schema, cursor).with_catalog(catalog.to_vec()),
                            expr,
                        )?
                    }
                } else {
                    compile_value(
                        em,
                        reg,
                        &Scope::single(schema, cursor).with_catalog(catalog.to_vec()),
                        expr,
                    )?
                }
            }
        };
        regs.push(r);
    }
    if cols.is_empty() {
        return Ok((reg.alloc(), 0));
    }
    let Some(&first) = regs.first() else {
        return Ok((reg.alloc(), 0));
    };
    let already_contiguous = regs
        .iter()
        .enumerate()
        .all(|(i, r)| *r == first.saturating_add(i32::try_from(i).unwrap_or(i32::MAX)));
    if already_contiguous {
        return Ok((first, cols.len()));
    }
    // Not naturally contiguous (a compound expression allocated
    // temporaries before its own result register) — reserve a fresh
    // contiguous run *after* every column has already been compiled
    // (so nothing else bump-allocates in between), then copy each
    // computed value into place. Same dest-block pattern as the
    // aggregate/snapshot record above (#141).
    let dests: Vec<i32> = (0..regs.len()).map(|_| reg.alloc()).collect();
    let Some(&dest_first) = dests.first() else {
        return Ok((reg.alloc(), 0));
    };
    for (&r, &dest) in regs.iter().zip(&dests) {
        em.emit(Instruction::new(Opcode::Copy, r, dest, 0));
    }
    Ok((dest_first, cols.len()))
}

/// Computes each result column into a contiguous register run, then
/// hands `(first, count)` to `sink` — in place of always emitting
/// `ResultRow`, so this same call site works for `compile_select`
/// (whose sink emits `ResultRow`) and #208's `INSERT ... SELECT` (whose
/// sink feeds the row into `insert.rs`'s per-row write path).
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_row_via_sink<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    cursor: i32,
    pseudo: bool,
    catalog: &[TableSchema],
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    let cols = result_columns(select, schema);
    let (first, count) = compile_row_values(em, reg, schema, &cols, cursor, pseudo, catalog)?;
    sink(em, reg, first, i32::try_from(count).unwrap_or(0))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_distinct_guard(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    cursor: i32,
    pseudo: bool,
    distinct_cursor: i32,
    skip_label: Label,
    catalog: &[TableSchema],
) -> Result<(), CodegenError> {
    if !matches!(select.distinct, Some(Distinctness::Distinct)) {
        return Ok(());
    }
    let cols = result_columns(select, schema);
    let collations = result_column_collations(schema, &cols);
    let (first, _count) = compile_row_values(em, reg, schema, &cols, cursor, pseudo, catalog)?;
    emit_dedup_check(em, distinct_cursor, first, collations, skip_label);
    Ok(())
}

/// The `Found`/`IdxInsert` ephemeral-index dedup check, at the
/// register level: given a row already computed into `first..first+count`,
/// skip to `skip_label` if `dedup_cursor` has seen it before, else record
/// it. Shared by `emit_distinct_guard` (`SELECT DISTINCT`, against
/// `select.distinct`) and plain `UNION`'s compound-select dedup (#378),
/// which drives the same check across every arm's own result
/// columns/schema, sharing one cursor opened once for the whole
/// compound statement rather than once per arm.
pub(super) fn emit_dedup_check(
    em: &mut Emitter,
    dedup_cursor: i32,
    first: i32,
    collations: Vec<Collation>,
    skip_label: Label,
) {
    let addr = em.emit(Instruction::with_p4(
        Opcode::Found,
        dedup_cursor,
        0,
        first,
        P4::SeekKey(collations.clone()),
    ));
    em.patch_p2(addr, skip_label);
    em.emit(Instruction::with_p4(
        Opcode::IdxInsert,
        dedup_cursor,
        first,
        0,
        P4::SeekKey(collations),
    ));
}
