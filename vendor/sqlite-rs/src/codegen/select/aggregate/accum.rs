// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::super::limit_scan::{emit_limit_guard, emit_offset_guard, LimitState};
use super::super::projection::{compile_row_values, result_columns};
use super::super::*;

/// One aggregate call's `AggStep`/`AggFinal` binding (#263): `name`
/// selects the accumulator kind in `crate::vdbe::aggregate`, `arg` is
/// its single argument expression (`None` only for `count(*)`), and
/// `slot` is this call's aggregate-context slot number — the `AggStep`/
/// `AggFinal` analogue of the old `AggSlot`'s `primary` register, but
/// addressing `Vm::agg_contexts` (a disjoint table from the register
/// file) instead.
pub(in crate::codegen::select) struct AggSlot {
    pub(in crate::codegen::select) call: Expr,
    pub(in crate::codegen::select) name: String,
    pub(in crate::codegen::select) arg: Option<Expr>,
    pub(in crate::codegen::select) slot: i32,
    /// `count(DISTINCT x)`/`sum(DISTINCT x)`/`avg(DISTINCT x)`: this
    /// slot's `AggStep` is guarded by an ephemeral dedup cursor (see
    /// [`super::emit_agg_step`]) numbered `eph_cursor`, `None` for a
    /// plain (non-`DISTINCT`) aggregate.
    pub(in crate::codegen::select) eph_cursor: Option<i32>,
}

/// Recognizes `expr` as an aggregate call this compiler can accumulate,
/// or reports why not. Only called on expressions [`find_aggregates`]
/// already identified as `is_aggregate_call`, so the "not an aggregate
/// at all" case can't happen here. Only `count`/`sum`/`avg`/`min`/`max`
/// have a `crate::vdbe::aggregate::AggState` accumulator today — same
/// set the old register-arithmetic scheme supported.
pub(in crate::codegen::select) fn classify_aggregate(
    expr: &Expr,
) -> Result<(String, Option<Expr>, bool), CodegenError> {
    let ExprKind::FunctionCall {
        name,
        args,
        distinct,
    } = &expr.kind
    else {
        return Err(CodegenError::Unsupported {
            reason: "classify_aggregate called on a non-call expression".to_string(),
        });
    };
    let arg = match args {
        FunctionArgs::Star => None,
        FunctionArgs::List(list) if list.len() <= 1 => list.first().cloned(),
        FunctionArgs::List(_) => {
            return Err(CodegenError::Unsupported {
                reason: format!(
                    "aggregate function {} with more than one argument is not yet supported",
                    name.to_ascii_lowercase()
                ),
            })
        }
    };
    let name = name.to_ascii_lowercase();
    match name.as_str() {
        "count" => {}
        "sum" | "avg" | "min" | "max" if arg.is_some() => {}
        "sum" | "avg" | "min" | "max" => {
            return Err(CodegenError::Unsupported {
                reason: format!("{name}() requires a single argument"),
            })
        }
        other => {
            return Err(CodegenError::Unsupported {
                reason: format!("aggregate function {other} not yet supported in GROUP BY"),
            })
        }
    }
    // `count(*)` has no argument to dedup against — `DISTINCT` is a
    // parser-accepted no-op there, same as SQLite's own `count(DISTINCT *)`
    // rejection is not modeled here since `*` never reaches this branch
    // with `arg = None` from anything but `count`.
    let distinct = *distinct && arg.is_some();
    Ok((name, arg, distinct))
}

/// One collected aggregate call: the call expression itself, its
/// lowercase name, its (at most one) argument, and whether it was
/// written with `DISTINCT`.
pub(in crate::codegen::select) type CollectedAggregate = (Expr, String, Option<Expr>, bool);

/// Finds every aggregate-call sub-expression reachable from `select`'s
/// result columns and `HAVING` clause through `Paren`/`Collate`/`Unary`/
/// `Binary` wrappers (see [`compile_grouped_scan`]'s doc comment for the
/// bound), deduplicated by AST equality so `HAVING count(*) > 1` sharing
/// a call with a `count(*)` result column accumulates into one slot.
pub(in crate::codegen::select) fn collect_aggregates(
    select: &Select,
) -> Result<Vec<CollectedAggregate>, CodegenError> {
    let mut found: Vec<Expr> = Vec::new();
    for col in &select.columns {
        if let ResultColumn::Expr { expr, .. } = col {
            find_aggregates(expr, &mut found);
        }
    }
    if let Some(having) = &select.having {
        find_aggregates(having, &mut found);
    }
    found
        .into_iter()
        .map(|call| {
            let (name, arg, distinct) = classify_aggregate(&call)?;
            Ok((call, name, arg, distinct))
        })
        .collect()
}

/// Whether `select` has any aggregate call in its result columns or
/// `HAVING` clause — the #287 trigger for compiling an implicit
/// whole-table group when `select.group_by.is_empty()`, distinguishing
/// `SELECT count(*) FROM t;` (implicit group) from an ordinary
/// aggregate-free `SELECT` (plain scan).
pub(crate) fn select_has_aggregate(select: &Select) -> bool {
    let mut found = Vec::new();
    for col in &select.columns {
        if let ResultColumn::Expr { expr, .. } = col {
            find_aggregates(expr, &mut found);
            if !found.is_empty() {
                return true;
            }
        }
    }
    if let Some(having) = &select.having {
        find_aggregates(having, &mut found);
    }
    !found.is_empty()
}

pub(in crate::codegen::select) fn find_aggregates(expr: &Expr, out: &mut Vec<Expr>) {
    if let ExprKind::FunctionCall { name, args, .. } = &expr.kind {
        if is_aggregate_call(name, args) {
            if !out.contains(expr) {
                out.push(expr.clone());
            }
            return;
        }
    }
    match &expr.kind {
        ExprKind::Paren(inner) | ExprKind::Collate { expr: inner, .. } => {
            find_aggregates(inner, out);
        }
        ExprKind::Unary { expr: inner, .. } => find_aggregates(inner, out),
        ExprKind::Binary { lhs, rhs, .. } => {
            find_aggregates(lhs, out);
            find_aggregates(rhs, out);
        }
        _ => {}
    }
}

/// Rewrites every aggregate-call sub-expression matching one of
/// `agg_slots` into a `Column` reference to that slot's synthetic
/// output-record field (see [`flush_group`]), so the rewritten
/// expression can compile against the flush-time synthetic
/// schema/record via the ordinary (aggregate-unaware) `compile_value`/
/// `compile_cond` machinery.
pub(in crate::codegen::select) fn substitute_aggregates(
    expr: &Expr,
    agg_slots: &[AggSlot],
    synthetic_names: &[String],
) -> Expr {
    if let Some(pos) = agg_slots.iter().position(|slot| slot.call == *expr) {
        return Expr {
            kind: ExprKind::Column {
                table: None,
                catalog: None,
                name: synthetic_names.get(pos).cloned().unwrap_or_default(),
            },
            span: expr.span,
        };
    }
    let kind = match &expr.kind {
        ExprKind::Paren(inner) => ExprKind::Paren(Box::new(substitute_aggregates(
            inner,
            agg_slots,
            synthetic_names,
        ))),
        ExprKind::Collate {
            expr: inner,
            collation,
        } => ExprKind::Collate {
            expr: Box::new(substitute_aggregates(inner, agg_slots, synthetic_names)),
            collation: collation.clone(),
        },
        ExprKind::Unary { op, expr: inner } => ExprKind::Unary {
            op: *op,
            expr: Box::new(substitute_aggregates(inner, agg_slots, synthetic_names)),
        },
        ExprKind::Binary { op, lhs, rhs } => ExprKind::Binary {
            op: *op,
            lhs: Box::new(substitute_aggregates(lhs, agg_slots, synthetic_names)),
            rhs: Box::new(substitute_aggregates(rhs, agg_slots, synthetic_names)),
        },
        other => other.clone(),
    };
    Expr {
        kind,
        span: expr.span,
    }
}

/// Pseudo-cursor-safe single-column read: like `emit_column_read`, but
/// aware that `cursor` re-reads an already-materialized record (so the
/// rowid-alias column is an ordinary field within it, not something
/// `Opcode::Rowid` can fetch) — see `compile_row_values`'s identical
/// special case for why.
pub(in crate::codegen::select) fn read_pseudo_column(
    em: &mut Emitter,
    schema: &TableSchema,
    cursor: i32,
    idx: usize,
    dest: i32,
) -> Result<(), CodegenError> {
    if schema.rowid_alias == Some(idx) {
        em.emit(Instruction::new(
            Opcode::Column,
            cursor,
            i32::try_from(idx).map_err(|_| CodegenError::Unsupported {
                reason: format!("column index {idx} does not fit in a P2 operand"),
            })?,
            dest,
        ));
        return Ok(());
    }
    emit_column_read(em, schema, cursor, idx, dest)
}

/// Reads every one of `schema`'s columns from the pass-2 pseudo cursor
/// into the given (already-allocated, persistent) destination
/// registers — the per-row snapshot `compile_grouped_scan` keeps so a
/// plain (non-aggregate) result/`HAVING` column reads the group's last
/// row, matching SQLite's own "arbitrary row" semantics for a
/// non-grouped-by column.
pub(in crate::codegen::select) fn read_row_columns_into(
    em: &mut Emitter,
    schema: &TableSchema,
    cursor: i32,
    dest: &[i32],
) -> Result<(), CodegenError> {
    for (idx, &r) in dest.iter().enumerate() {
        read_pseudo_column(em, schema, cursor, idx, r)?;
    }
    Ok(())
}

/// Emits one `AggStep` for `agg`'s slot (#263): compiles `agg.arg` (if
/// any) into a fresh register and folds it via `Opcode::AggStep`,
/// exactly the shape `crate::vdbe::exec::agg_step` expects — a
/// contiguous argument-register run starting at `P2`, arity/name via
/// `P4::AggFunc`. `reset` sets `P5`, which discards this slot's prior
/// state before folding (`Vm`'s "start a fresh accumulator" behavior)
/// — the group-boundary row for a reused slot number passes `true`;
/// every other row in the same group passes `false`.
///
/// `min`/`max` compare under `agg.arg`'s collation: an explicit
/// `COLLATE` wrapper wins, else the argument's own schema-declared
/// collation (#500), same resolution `expr_collation` gives the scalar
/// comparison path. Unlike that scalar path, this
/// does not also apply a comparison *affinity* first:
/// `crate::vdbe::aggregate::step`'s `compare` call has no affinity
/// parameter to feed one to, a pre-existing gap in the `AggStep`/
/// `AggFinal` opcode contract (not introduced by this ticket, and not
/// regressed from the old register-arithmetic scheme, which also had
/// no affinity handling on its `Lt`/`Gt` compares before #265's
/// collation-only fix).
pub(in crate::codegen::select) fn emit_agg_step(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    scope: &Scope,
    agg: &AggSlot,
    reset: bool,
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

    // `count(DISTINCT x)`/`sum(DISTINCT x)`/`avg(DISTINCT x)`: guard the
    // fold with the same `OpenEphemeral`/`Found`/`IdxInsert` dedup shape
    // `emit_distinct_guard` uses for a top-level `SELECT DISTINCT` — one
    // ephemeral index per slot, reset (reopened, which discards its prior
    // contents) on this slot's group-boundary row so each group gets its
    // own DISTINCT set.
    let Some(eph_cursor) = agg.eph_cursor else {
        em.emit(instr);
        return Ok(());
    };
    if reset {
        em.emit(Instruction::new(Opcode::OpenEphemeral, eph_cursor, 0, 0));
    }
    let skip_step = em.new_label();
    let found_addr = em.emit(Instruction::with_p4(
        Opcode::Found,
        eph_cursor,
        0,
        p2,
        P4::Int(1),
    ));
    em.patch_p2(found_addr, skip_step);
    em.emit(Instruction::with_p4(
        Opcode::IdxInsert,
        eph_cursor,
        p2,
        0,
        P4::Int(1),
    ));
    em.emit(instr);
    em.place(skip_step);
    Ok(())
}

/// Finalizes and emits one grouped output row via `sink`, applying
/// `HAVING`/`LIMIT`/`OFFSET` exactly as the ungrouped scans do. Builds a
/// synthetic record — the group's snapshot column values (from the last
/// row seen) followed by each aggregate's finalized value — and opens a
/// fresh pseudo cursor over it, so `select.columns`/`having` (with
/// aggregate calls rewritten to reference the synthetic record's
/// trailing fields via [`substitute_aggregates`]) compile through the
/// ordinary `compile_row_values`/`compile_cond` machinery unchanged.
#[allow(clippy::too_many_arguments)]
pub(in crate::codegen::select) fn flush_group<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    catalog: &[TableSchema],
    snapshot_regs: &[i32],
    agg_slots: &[AggSlot],
    limit: Option<&LimitState>,
    end_label: Label,
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
    let synthetic_names: Vec<String> = (0..agg_slots.len()).map(|i| format!("__agg{i}")).collect();

    let mut synthetic_columns = schema.columns.clone();
    synthetic_columns.extend(synthetic_names.iter().cloned());
    let mut synthetic_types = schema.column_types.clone();
    synthetic_types.extend(synthetic_names.iter().map(|_| String::new()));
    let synthetic_schema = TableSchema {
        unresolved_autoindex: false,
        name: schema.name.clone(),
        root_page: 0,
        columns: synthetic_columns,
        without_rowid: schema.without_rowid,
        strict: false,
        column_types: synthetic_types,
        column_collations: vec![],
        is_virtual: false,
        sql: String::new(),
        indexes: Vec::new(),
        rowid_alias: None,
    };

    // Allocate one fresh, contiguous register per snapshot/aggregate
    // field up front — `reg.alloc()` bump-allocates sequentially, so as
    // long as nothing else allocates in between, `dests` is guaranteed
    // contiguous for `MakeRecord`.
    let synthetic_count = snapshot_regs.len().saturating_add(agg_slots.len());
    let dests: Vec<i32> = (0..synthetic_count).map(|_| reg.alloc()).collect();
    let synthetic_first = dests.first().copied().unwrap_or_else(|| reg.alloc());
    for (&snap, &dest) in snapshot_regs.iter().zip(&dests) {
        em.emit(Instruction::new(Opcode::Copy, snap, dest, 0));
    }
    let agg_dests = dests.get(snapshot_regs.len()..).unwrap_or(&[]);
    for (agg, &dest) in agg_slots.iter().zip(agg_dests) {
        // `avg()`'s sum/count division now happens inside
        // `crate::vdbe::aggregate::finalize` — `AggFinal` just reads
        // the slot's already-finalized value straight into `dest`.
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
    let flush_cursor = FLUSH_CURSOR;
    em.emit(Instruction::new(
        Opcode::OpenPseudo,
        flush_cursor,
        record_reg,
        0,
    ));

    let flush_scope = Scope::single(&synthetic_schema, flush_cursor).with_catalog(catalog.to_vec());
    let skip_label = em.new_label();
    if let Some(having) = &select.having {
        let rewritten = substitute_aggregates(having, agg_slots, &synthetic_names);
        compile_cond(
            em,
            reg,
            &flush_scope,
            &rewritten,
            CondTargets::null_is_false(Target::Fallthrough, Target::Jump(skip_label)),
        )?;
    }
    if let Some(limit) = limit {
        emit_offset_guard(em, limit, skip_label);
    }
    if let Some(limit) = limit {
        emit_limit_guard(em, limit, end_label);
    }

    let rewritten_columns: Vec<ResultColumn> = select
        .columns
        .iter()
        .map(|col| match col {
            ResultColumn::Expr { expr, alias } => ResultColumn::Expr {
                expr: substitute_aggregates(expr, agg_slots, &synthetic_names),
                alias: alias.clone(),
            },
            other => other.clone(),
        })
        .collect();
    let throwaway = Select {
        with_clause: None,
        distinct: None,
        columns: rewritten_columns,
        from: None,
        where_clause: None,
        group_by: Vec::new(),
        having: None,
        compound: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        span: select.span,
    };
    let cols = result_columns(&throwaway, &synthetic_schema);
    let (proj_first, proj_count) = compile_row_values(
        em,
        reg,
        &synthetic_schema,
        &cols,
        flush_cursor,
        true,
        catalog,
    )?;
    sink(em, reg, proj_first, i32::try_from(proj_count).unwrap_or(0))?;
    em.place(skip_label);
    Ok(())
}

/// A cursor number for `flush_group`'s synthetic per-group record —
/// distinct from [`ScanCursors`]'s four numbers (0-3), which stay live
/// across every `flush_group` call within the same grouped scan.
pub(super) const FLUSH_CURSOR: i32 = 4;
