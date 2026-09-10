// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Real-index range-seek fast paths (#606, ADR-0034): `col BETWEEN lo AND
//! hi`, `col LIKE 'prefix%'`/`col GLOB 'prefix*'`, and `col IN (v1, ...)`
//! against a single-column-indexed `WHERE` clause each get an
//! index-b-tree seek in place of a full `Rewind`/`Next` scan +
//! per-row filter. Each fast path is narrowly pattern-matched, mirroring
//! `limit_scan.rs`'s `try_compile_rowid_seek`/
//! `try_compile_covering_index_scan` — anything outside the recognized
//! shape returns `Ok(false)` and leaves `em`/`reg` untouched, so the
//! caller falls back to the ordinary scan and the existing `BETWEEN`/
//! `IN`/`LIKE` filter lowering in `src/codegen/expr/cond.rs` is used
//! unchanged.
//!
//! `BETWEEN`/`IN` walk `[lo, hi]`/`{v1, v2, ...}` using the new
//! `SeekIndexGE`/`IdxCompareGT` opcodes (see ADR-0034): `SeekIndexGE`
//! seeks to the range floor, then an `IdxNext` loop guarded by
//! `IdxCompareGT` (checked at the top of each iteration) stops once the
//! walk passes the upper bound.
//!
//! `LIKE 'prefix%'` reuses the same `SeekIndexGE`/`IdxCompareGT` pair
//! rather than a dedicated prefix-compare opcode: the seek floor is the
//! prefix itself, and the upper bound is `prefix` with the maximum
//! Unicode scalar value (`char::MAX`, U+10FFFF) appended. Any string
//! that starts with `prefix` followed by further characters sorts
//! strictly below `prefix + char::MAX` (its first extra character can
//! only be `char::MAX` itself in the same, practically nonexistent,
//! edge case that would also confuse SQLite's own byte-increment
//! upper-bound trick), and `prefix` itself (no suffix, matching `LIKE
//! 'prefix%'` per its trailing `%` matching zero characters) sorts
//! below `prefix + char::MAX` as a strict prefix always sorts before a
//! longer string sharing that prefix. This keeps the upper-bound
//! representation a plain `Value::Text` usable with the same
//! `IdxCompareGT` opcode BETWEEN already needs, rather than a second
//! bespoke byte-prefix-compare primitive.
use super::limit_scan::{compile_limit_setup, emit_limit_guard, emit_offset_guard};
use super::projection::emit_row_via_sink;
use super::*;
use crate::parser::ast::BinaryOp;
use crate::parser::tokenizer::Span;
use crate::vdbe::{affinity_of, Affinity};

fn dummy_span() -> Span {
    Span {
        line: 0,
        column: 0,
        offset: 0,
        len: 0,
    }
}

fn literal_expr(lit: Literal) -> Expr {
    Expr {
        kind: ExprKind::Literal(lit),
        span: dummy_span(),
    }
}

pub(super) fn where_col(expr: &Expr) -> Option<&str> {
    match &expr.kind {
        ExprKind::Column { name, .. } => Some(name.as_str()),
        _ => None,
    }
}

/// Same operand restriction as `limit_scan.rs`'s fast paths: a literal
/// (int/float/string — string included here since `BETWEEN`/`IN` over a
/// text column is common, unlike the rowid-seek path's integer-only
/// scope) or a bind parameter. Anything else (a sub-expression, a named
/// parameter) falls back to the ordinary scan.
///
/// Does NOT by itself guarantee the literal is safe to seek with — a
/// seek builds a raw probe key compared byte-for-byte against what's
/// already stored in the index (itself built with the indexed column's
/// declared affinity applied at `INSERT` time), unlike the ordinary
/// filter path's `Eq`/`Ge`/`Le` opcodes, which apply *comparison*
/// affinity dynamically at compare time from both operands' types. A
/// literal whose storage class doesn't already match the column's
/// affinity (e.g. the string `'10'` against an `INTEGER`-affinity
/// column) would silently seek to the wrong place — SQLite's own
/// affinity-coercion rules (well-formed numeric text only) aren't
/// reproduced here. [`operand_matches_column_affinity`] is the
/// additional check every fast path in this file layers on top of this
/// one before trusting an operand.
pub(super) fn is_supported_operand(expr: &Expr) -> bool {
    matches!(
        &expr.kind,
        ExprKind::Literal(Literal::Integer(_) | Literal::Float(_) | Literal::Str(_))
            | ExprKind::Param(ParamKind::Anonymous | ParamKind::Numbered(_))
    )
}

/// Whether `operand`'s literal storage class already matches
/// `column_affinity` closely enough that building a raw seek probe from
/// it — with no affinity coercion applied — compares correctly against
/// what's actually stored in the index. A bind parameter is passed
/// through uncheckable at compile time (same accepted risk every other
/// equality fast path in `limit_scan.rs` already takes for `Param`
/// operands); only a `Literal` operand is actually constrained here.
/// See [`is_supported_operand`]'s doc for why this check exists.
pub(super) fn operand_matches_column_affinity(expr: &Expr, column_affinity: Affinity) -> bool {
    match &expr.kind {
        ExprKind::Literal(Literal::Integer(_) | Literal::Float(_)) => matches!(
            column_affinity,
            Affinity::Integer | Affinity::Real | Affinity::Numeric
        ),
        ExprKind::Literal(Literal::Str(_)) => matches!(column_affinity, Affinity::Text),
        ExprKind::Param(_) => true,
        _ => false,
    }
}

/// [`affinity_of`] for `col_name`'s declared type in `schema`, defaulting
/// to [`Affinity::Blob`] (SQLite's own default for a column with no
/// declared type) if the name can't be resolved — callers only reach
/// this after [`find_leading_index`] already confirmed the column
/// exists, so an unresolved name here would be a schema/index
/// inconsistency, not a normal fallback path; defaulting to `Blob`
/// (never matches a `Literal` operand in
/// [`operand_matches_column_affinity`]) just means such an
/// inconsistency falls back to the ordinary scan rather than panicking.
fn column_affinity(schema: &TableSchema, col_name: &str) -> Affinity {
    schema
        .columns
        .iter()
        .position(|c| c.eq_ignore_ascii_case(col_name))
        .and_then(|i| schema.column_types.get(i))
        .map_or(Affinity::Blob, |ty| affinity_of(ty))
}

/// Finds the position (into `schema.indexes`) of an index whose
/// *leading* column matches `col_name` — mirrors
/// `limit_scan.rs::find_covering_index`'s index lookup. Only the
/// leading column of the match is ever probed/compared by the opcodes
/// these fast paths emit, so a multi-column index still works (just as
/// a leading-column-only lookup). Returns a position rather than a
/// borrowed `&IndexSchema` (an explicit lifetime parameter would be
/// needed to return one, past this codebase's qualified-language-subset
/// limit, `tools/mvl-limit`) — every caller already re-fetches via
/// `schema.indexes.get(position)` right after.
pub(super) fn find_leading_index(schema: &TableSchema, col_name: &str) -> Option<usize> {
    schema.indexes.iter().position(|idx| {
        idx.columns
            .first()
            .is_some_and(|c| c.name.eq_ignore_ascii_case(col_name))
    })
}

/// Opens `index` on `cursors.sort` (reused across every fast path in
/// this file, mirroring `limit_scan.rs`/`index_scan.rs`'s own reuse of
/// that cursor slot — none of these paths ever run alongside a sort).
fn open_index_cursor(
    em: &mut Emitter,
    index: &IndexSchema,
    index_cursor: i32,
) -> Result<(), CodegenError> {
    let root_page = crate::codegen::index_maintenance::valid_index_root_page(index)?;
    let mut open_instr = Instruction::new(Opcode::OpenRead, index_cursor, root_page, 0);
    open_instr.p5 = 1;
    em.emit(open_instr);
    Ok(())
}

/// Every select-list column, when it's a bare unqualified column
/// reference (`ResultColumn::Expr` wrapping `ExprKind::Column{table:
/// None, catalog: None, ..}`) — the shape every fast path in this file
/// is profiled against (`SELECT id, n, x, ... FROM t WHERE ...`), and
/// the only shape [`emit_matched_row`]'s indexed-column substitution
/// (#664) knows how to recognize. `None` for `*`/`tbl.*`, a qualified
/// reference, or any computed expression — [`emit_matched_row`] falls
/// back to the ordinary full-row read via [`emit_row_via_sink`] for
/// those, exactly as before this optimization existed.
fn bare_column_names(select: &Select) -> Option<Vec<&str>> {
    select
        .columns
        .iter()
        .map(|col| match col {
            ResultColumn::Expr {
                expr:
                    Expr {
                        kind:
                            ExprKind::Column {
                                table: None,
                                catalog: None,
                                name,
                            },
                        ..
                    },
                ..
            } => Some(name.as_str()),
            _ => None,
        })
        .collect()
}

/// Reads `idx`'s value from `index_cursor`'s own leading key column
/// (position 0) into `dest`, instead of [`emit_column_read`]'s ordinary
/// read off the table cursor (#664) — every fast path in this file
/// positions its index cursor on an entry whose key already carries the
/// one column the seek matched against, so re-reading that same column
/// a moment later off the table row `IdxRowid`+`SeekRowid` just fetched
/// is a wholly redundant round trip (the table lookup itself still
/// stands: every *other* selected column still only lives in the table
/// row). Mirrors `emit_column_read`'s REAL-affinity fixup (#143) since
/// this is still logically that same schema column, just sourced from a
/// different cursor; the rowid-alias case never arises here — a
/// rowid-alias column is never itself indexed as an ordinary index
/// column, so [`emit_matched_row`] never requests it.
fn emit_indexed_column_read(
    em: &mut Emitter,
    schema: &TableSchema,
    index_cursor: i32,
    idx: usize,
    dest: i32,
) {
    em.emit(Instruction::new(Opcode::Column, index_cursor, 0, dest));
    if schema
        .column_types
        .get(idx)
        .is_some_and(|t| affinity_of(t) == Affinity::Real)
    {
        em.emit(Instruction::new(Opcode::RealAffinity, dest, 0, 0));
    }
}

/// Emits the shared "fetch the full row and hand it to `sink`" tail
/// every fast path in this file uses once the index cursor is
/// positioned on a matching entry: `IdxRowid` + `SeekRowid` (jumping to
/// `row_skip` if the table row is somehow missing), then LIMIT/OFFSET
/// guards, then the row projection. `indexed_col_name` is the column
/// this fast path's seek matched against (already sitting in
/// `index_cursor`'s key) — when every select-list column is a bare
/// reference ([`bare_column_names`]), that one column is read straight
/// from the index cursor instead of the table row (#664); any other
/// select-list shape falls back to [`emit_row_via_sink`]'s ordinary
/// full-row read, unchanged from before this optimization existed.
#[allow(clippy::too_many_arguments)]
fn emit_matched_row<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    select: &Select,
    schema: &TableSchema,
    cursors: ScanCursors,
    index_cursor: i32,
    indexed_col_name: &str,
    limit: &Option<super::limit_scan::LimitState>,
    row_skip: Label,
    end_label: Label,
    catalog: &[TableSchema],
    sink: &mut F,
) -> Result<(), CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, i32) -> Result<(), CodegenError>,
{
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

    if let Some(limit) = limit {
        emit_offset_guard(em, limit, row_skip);
    }
    if let Some(limit) = limit {
        emit_limit_guard(em, limit, end_label);
    }

    if let Some(names) = bare_column_names(select) {
        let mut first = None;
        for name in &names {
            let idx = column_index(schema, name).ok_or_else(|| CodegenError::UnknownColumn {
                name: (*name).to_string(),
            })?;
            let r = reg.alloc();
            if name.eq_ignore_ascii_case(indexed_col_name) && schema.rowid_alias != Some(idx) {
                emit_indexed_column_read(em, schema, index_cursor, idx, r);
            } else {
                emit_column_read(em, schema, cursors.table, idx, r)?;
            }
            first.get_or_insert(r);
        }
        let first = first.unwrap_or_else(|| reg.alloc());
        return sink(em, reg, first, i32::try_from(names.len()).unwrap_or(0));
    }
    emit_row_via_sink(em, reg, select, schema, cursors.table, false, catalog, sink)
}

/// Compiles `WHERE col BETWEEN lo AND hi` (`col` a plain column with a
/// matching index, `lo`/`hi` literals or bind parameters) as a
/// `SeekIndexGE(lo)` + `IdxCompareGT(hi)`-guarded `IdxNext` walk, in
/// place of the ordinary `Rewind`/`Next` scan + `compile_cond`'s
/// `Ge`/`Le` filter (`src/codegen/expr/cond.rs`, untouched, still used
/// for every other shape). Returns `Ok(false)` — `em`/`reg` untouched —
/// for `NOT BETWEEN`, a non-column `expr`, an unindexed column, an
/// unsupported operand, `DISTINCT`, or any WHERE clause that isn't a
/// single top-level `BETWEEN`.
#[allow(clippy::too_many_arguments)]
pub(super) fn try_compile_between_seek<F>(
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
        return Ok(false);
    }
    let Some(where_expr) = &select.where_clause else {
        return Ok(false);
    };
    let ExprKind::Between {
        expr,
        lo,
        hi,
        negated: false,
    } = &where_expr.kind
    else {
        return Ok(false);
    };
    let Some(col_name) = where_col(expr) else {
        return Ok(false);
    };
    if !is_supported_operand(lo) || !is_supported_operand(hi) {
        return Ok(false);
    }
    let Some(index_position) = find_leading_index(schema, col_name) else {
        return Ok(false);
    };
    let Some(index) = schema.indexes.get(index_position) else {
        return Ok(false);
    };
    let affinity = column_affinity(schema, col_name);
    if !operand_matches_column_affinity(lo, affinity)
        || !operand_matches_column_affinity(hi, affinity)
    {
        return Ok(false);
    }
    let leading_collation = index
        .columns
        .first()
        .map_or(Collation::Binary, |c| c.collation);

    let index_cursor = cursors.sort;
    open_index_cursor(em, index, index_cursor)?;

    let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    let limit = compile_limit_setup(em, reg, &scope, select)?;
    let lo_reg = compile_value(em, reg, &scope, lo)?;
    let hi_reg = compile_value(em, reg, &scope, hi)?;

    let seek_addr = em.emit(Instruction::with_p4(
        Opcode::SeekIndexGE,
        index_cursor,
        0,
        lo_reg,
        P4::SeekKey(vec![leading_collation]),
    ));
    em.patch_p2(seek_addr, end_label);

    let loop_start = em.new_label();
    em.place(loop_start);

    let stop_addr = em.emit(Instruction::with_p4(
        Opcode::IdxCompareGT,
        index_cursor,
        0,
        hi_reg,
        P4::SeekKey(vec![leading_collation]),
    ));
    em.patch_p2(stop_addr, end_label);

    let row_skip = em.new_label();
    emit_matched_row(
        em,
        reg,
        select,
        schema,
        cursors,
        index_cursor,
        col_name,
        &limit,
        row_skip,
        end_label,
        catalog,
        sink,
    )?;

    em.place(row_skip);
    let next_addr = em.emit(Instruction::new(Opcode::IdxNext, index_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);
    Ok(true)
}

/// #666: `UPDATE`/`DELETE`'s row-scan equivalent of
/// [`try_compile_forward_comparison_seek`]/[`try_compile_between_seek`],
/// stripped of `SELECT`-only concerns (`DISTINCT`, LIMIT/OFFSET,
/// projection) — recognizes the same two WHERE shapes (`col >/>=/</<=
/// lit` and `col BETWEEN lo AND hi` against a leading-indexed column)
/// and emits the same `SeekIndexGE`/`IdxCompareGT`/`IdxNext` walk, but
/// calls `sink` once per matching *index* entry instead of fetching the
/// full table row itself — `sink` is responsible for turning that into
/// a table-row action (typically `IdxRowid` + `SeekRowid` onto
/// `row_skip` on a miss, then the caller's own per-row body). Returns
/// `Ok(false)` — `em`/`reg` untouched — for any unrecognized shape,
/// exactly like its `SELECT` counterparts.
#[allow(clippy::too_many_arguments)]
pub(crate) fn try_compile_range_row_seek<F>(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    where_expr: &Expr,
    schema: &TableSchema,
    scope: &Scope,
    index_cursor: i32,
    end_label: Label,
    sink: &mut F,
) -> Result<bool, CodegenError>
where
    F: FnMut(&mut Emitter, &mut RegAlloc, i32, Label) -> Result<(), CodegenError>,
{
    if let ExprKind::Between {
        expr,
        lo,
        hi,
        negated: false,
    } = &where_expr.kind
    {
        let Some(col_name) = where_col(expr) else {
            return Ok(false);
        };
        if !is_supported_operand(lo) || !is_supported_operand(hi) {
            return Ok(false);
        }
        let Some(index_position) = find_leading_index(schema, col_name) else {
            return Ok(false);
        };
        let Some(index) = schema.indexes.get(index_position) else {
            return Ok(false);
        };
        let affinity = column_affinity(schema, col_name);
        if !operand_matches_column_affinity(lo, affinity)
            || !operand_matches_column_affinity(hi, affinity)
        {
            return Ok(false);
        }
        let leading_collation = index
            .columns
            .first()
            .map_or(Collation::Binary, |c| c.collation);

        open_index_cursor(em, index, index_cursor)?;
        let lo_reg = compile_value(em, reg, scope, lo)?;
        let hi_reg = compile_value(em, reg, scope, hi)?;

        let seek_addr = em.emit(Instruction::with_p4(
            Opcode::SeekIndexGE,
            index_cursor,
            0,
            lo_reg,
            P4::SeekKey(vec![leading_collation]),
        ));
        em.patch_p2(seek_addr, end_label);

        let loop_start = em.new_label();
        em.place(loop_start);

        let stop_addr = em.emit(Instruction::with_p4(
            Opcode::IdxCompareGT,
            index_cursor,
            0,
            hi_reg,
            P4::SeekKey(vec![leading_collation]),
        ));
        em.patch_p2(stop_addr, end_label);

        let row_skip = em.new_label();
        sink(em, reg, index_cursor, row_skip)?;
        em.place(row_skip);
        let next_addr = em.emit(Instruction::new(Opcode::IdxNext, index_cursor, 0, 0));
        em.patch_p2(next_addr, loop_start);
        return Ok(true);
    }

    let Some((col_name, operand, inclusive)) = as_forward_comparison(where_expr) else {
        return Ok(false);
    };
    if !is_supported_operand(operand) {
        return Ok(false);
    }
    let Some(index_position) = find_leading_index(schema, col_name) else {
        return Ok(false);
    };
    let Some(index) = schema.indexes.get(index_position) else {
        return Ok(false);
    };
    let affinity = column_affinity(schema, col_name);
    if !operand_matches_column_affinity(operand, affinity) {
        return Ok(false);
    }
    let leading_collation = index
        .columns
        .first()
        .map_or(Collation::Binary, |c| c.collation);

    open_index_cursor(em, index, index_cursor)?;
    let bound_reg = compile_value(em, reg, scope, operand)?;

    let seek_addr = em.emit(Instruction::with_p4(
        Opcode::SeekIndexGE,
        index_cursor,
        0,
        bound_reg,
        P4::SeekKey(vec![leading_collation]),
    ));
    em.patch_p2(seek_addr, end_label);

    if !inclusive {
        let skip_start = em.new_label();
        em.place(skip_start);
        let past_bound = em.new_label();
        let gt_addr = em.emit(Instruction::with_p4(
            Opcode::IdxCompareGT,
            index_cursor,
            0,
            bound_reg,
            P4::SeekKey(vec![leading_collation]),
        ));
        em.patch_p2(gt_addr, past_bound);
        let skip_next_addr = em.emit(Instruction::new(Opcode::IdxNext, index_cursor, 0, 0));
        em.patch_p2(skip_next_addr, skip_start);
        let exhausted_addr = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
        em.patch_p2(exhausted_addr, end_label);
        em.place(past_bound);
    }

    let loop_start = em.new_label();
    em.place(loop_start);
    let row_skip = em.new_label();
    sink(em, reg, index_cursor, row_skip)?;
    em.place(row_skip);
    let next_addr = em.emit(Instruction::new(Opcode::IdxNext, index_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);
    Ok(true)
}

/// The maximum Unicode scalar value, `char::MAX` (U+10FFFF) — see this
/// module's doc comment for why appending it to a literal prefix gives a
/// safe strict upper bound for `LIKE 'prefix%'`/`GLOB 'prefix*'`.
fn prefix_upper_bound(prefix: &str) -> String {
    let mut s = String::with_capacity(prefix.len().saturating_add(4));
    s.push_str(prefix);
    s.push(char::MAX);
    s
}

/// Extracts the literal, non-wildcard prefix of a `LIKE`/`GLOB` pattern
/// string, requiring exactly one trailing wildcard (`%`/`*`) and nothing
/// else wildcard-ish anywhere — see this module's doc and #606's bail-out
/// list. Returns `None` for any pattern shape outside that.
pub(super) fn like_literal_prefix(pattern: &str, glob: bool) -> Option<String> {
    let wildcard = if glob { '*' } else { '%' };
    let single = if glob { '?' } else { '_' };
    if pattern.is_empty() {
        return None;
    }
    let prefix = pattern.strip_suffix(wildcard)?;
    if prefix.is_empty() {
        // Empty prefix (leading wildcard, or the pattern is just "%")
        // matches everything — no seek floor to compute.
        return None;
    }
    if prefix.contains(wildcard) || prefix.contains(single) {
        return None;
    }
    if !glob && prefix.contains('\\') {
        // Conservative: an ESCAPE clause changes how backslashes (or
        // whatever escape char is named) are interpreted — bail rather
        // than risk misreading an escaped wildcard as literal.
        return None;
    }
    if prefix.ends_with('\u{10FFFF}') {
        // The one prefix value `prefix_upper_bound` can't safely
        // represent an exclusive-enough bound for — bail per #606.
        return None;
    }
    Some(prefix.to_string())
}

/// Compiles `WHERE col LIKE 'prefix%'` / `WHERE col GLOB 'prefix*'`
/// (`col` a plain column with a matching index, pattern a string
/// literal with exactly one non-empty literal prefix followed by a
/// single trailing wildcard) as a `SeekIndexGE`/`IdxCompareGT` range
/// walk — see this module's doc comment for the upper-bound
/// construction. Returns `Ok(false)` for `NOT LIKE`/`NOT GLOB`, an
/// `ESCAPE` clause, a non-literal pattern, any other pattern shape (see
/// [`like_literal_prefix`]), an unindexed column, `DISTINCT`, or any
/// `WHERE` clause that isn't a single top-level `LIKE`/`GLOB` — falling
/// back to the ordinary scan and the existing `like()`/`glob()`
/// function-call filter (`src/codegen/expr/cond.rs`,
/// `src/codegen/expr/value.rs`, untouched).
#[allow(clippy::too_many_arguments)]
pub(super) fn try_compile_like_prefix_seek<F>(
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
        return Ok(false);
    }
    let Some(where_expr) = &select.where_clause else {
        return Ok(false);
    };
    let ExprKind::Like {
        expr,
        pattern,
        glob,
        negated: false,
        escape: None,
    } = &where_expr.kind
    else {
        return Ok(false);
    };
    let Some(col_name) = where_col(expr) else {
        return Ok(false);
    };
    let ExprKind::Literal(Literal::Str(pattern_str)) = &pattern.kind else {
        return Ok(false);
    };
    let Some(prefix) = like_literal_prefix(pattern_str, *glob) else {
        return Ok(false);
    };
    let Some(index_position) = find_leading_index(schema, col_name) else {
        return Ok(false);
    };
    let Some(index) = schema.indexes.get(index_position) else {
        return Ok(false);
    };
    // A prefix seek only compares correctly against what's actually
    // stored in the index if the column keeps text as text — a
    // numeric-affinity column's index entries are the coerced numbers,
    // not the original text, so a text probe would sort into the wrong
    // place (see `is_supported_operand`'s doc for the general hazard).
    if column_affinity(schema, col_name) != Affinity::Text {
        return Ok(false);
    }
    let leading_collation = index
        .columns
        .first()
        .map_or(Collation::Binary, |c| c.collation);

    let index_cursor = cursors.sort;
    open_index_cursor(em, index, index_cursor)?;

    let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    let limit = compile_limit_setup(em, reg, &scope, select)?;
    let lo_reg = compile_value(em, reg, &scope, &literal_expr(Literal::Str(prefix.clone())))?;
    let hi_reg = compile_value(
        em,
        reg,
        &scope,
        &literal_expr(Literal::Str(prefix_upper_bound(&prefix))),
    )?;

    let seek_addr = em.emit(Instruction::with_p4(
        Opcode::SeekIndexGE,
        index_cursor,
        0,
        lo_reg,
        P4::SeekKey(vec![leading_collation]),
    ));
    em.patch_p2(seek_addr, end_label);

    let loop_start = em.new_label();
    em.place(loop_start);

    let stop_addr = em.emit(Instruction::with_p4(
        Opcode::IdxCompareGT,
        index_cursor,
        0,
        hi_reg,
        P4::SeekKey(vec![leading_collation]),
    ));
    em.patch_p2(stop_addr, end_label);

    let row_skip = em.new_label();
    emit_matched_row(
        em,
        reg,
        select,
        schema,
        cursors,
        index_cursor,
        col_name,
        &limit,
        row_skip,
        end_label,
        catalog,
        sink,
    )?;

    em.place(row_skip);
    let next_addr = em.emit(Instruction::new(Opcode::IdxNext, index_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);
    Ok(true)
}

/// Compiles `WHERE col IN (v1, v2, ..., vN)` (`col` a plain column with a
/// matching index, every `vI` a literal or bind parameter) as a sequence
/// of `SeekIndexEq` point lookups — one per distinct value — each
/// chaining `IdxRowid` + `SeekRowid` to fetch the full row on a hit and
/// simply skipping to the next value on a miss (unlike the single
/// `end_label`-jumping shape of a point-lookup fast path: a miss on
/// value `vI` must still try `v(I+1)`, only reaching `end_label` once
/// every value has been tried). Duplicate literal values are compiled
/// once, so no row is ever emitted twice. Returns `Ok(false)` for `NOT
/// IN`, an empty list, any non-literal/non-param member, a non-column
/// `expr`, an unindexed column, `DISTINCT`, or any `WHERE` clause that
/// isn't a single top-level `IN (...)` — falling back to the ordinary
/// scan and the existing per-value `Eq`-loop filter
/// (`src/codegen/expr/cond.rs`, untouched).
#[allow(clippy::too_many_arguments)]
pub(super) fn try_compile_in_list_seek<F>(
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
        return Ok(false);
    }
    let Some(where_expr) = &select.where_clause else {
        return Ok(false);
    };
    let ExprKind::In {
        expr,
        list,
        negated: false,
    } = &where_expr.kind
    else {
        return Ok(false);
    };
    if list.is_empty() {
        return Ok(false);
    }
    let Some(col_name) = where_col(expr) else {
        return Ok(false);
    };
    if !list.iter().all(is_supported_operand) {
        return Ok(false);
    }
    let Some(index_position) = find_leading_index(schema, col_name) else {
        return Ok(false);
    };
    let Some(index) = schema.indexes.get(index_position) else {
        return Ok(false);
    };
    let affinity = column_affinity(schema, col_name);
    if !list
        .iter()
        .all(|v| operand_matches_column_affinity(v, affinity))
    {
        return Ok(false);
    }
    let leading_collation = index
        .columns
        .first()
        .map_or(Collation::Binary, |c| c.collation);

    // Dedup identical literal values at compile time (#606) so no row is
    // ever emitted twice; bind parameters are never considered equal to
    // each other or to a literal here (their runtime value is unknown at
    // compile time), so only exact `Literal` duplicates are collapsed.
    let mut seen_literals: Vec<&Literal> = Vec::new();
    let mut operands: Vec<&Expr> = Vec::new();
    for value in list {
        if let ExprKind::Literal(lit) = &value.kind {
            if seen_literals.contains(&lit) {
                continue;
            }
            seen_literals.push(lit);
        }
        operands.push(value);
    }

    let index_cursor = cursors.sort;
    open_index_cursor(em, index, index_cursor)?;

    let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    let limit = compile_limit_setup(em, reg, &scope, select)?;

    for operand in operands {
        let value_reg = compile_value(em, reg, &scope, operand)?;
        let next_value = em.new_label();
        let seek_addr = em.emit(Instruction::with_p4(
            Opcode::SeekIndexEq,
            index_cursor,
            0,
            value_reg,
            P4::SeekKey(vec![leading_collation]),
        ));
        em.patch_p2(seek_addr, next_value);

        let row_skip = em.new_label();
        emit_matched_row(
            em,
            reg,
            select,
            schema,
            cursors,
            index_cursor,
            col_name,
            &limit,
            row_skip,
            end_label,
            catalog,
            sink,
        )?;
        em.place(row_skip);

        em.place(next_value);
    }
    let done_addr = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
    em.patch_p2(done_addr, end_label);
    Ok(true)
}

/// Normalizes a single top-level comparison `WHERE` clause to
/// `(col_name, literal_operand, inclusive)` when it has the shape
/// `col > lit`/`col >= lit`/`lit < col`/`lit <= col` — the four spellings
/// of "the index should seek to the first entry strictly/inclusively
/// past `lit` and then walk forward with no upper bound". `col <
/// lit`/`col <= lit`/`lit > col`/`lit >= col` (the descending-bound
/// shapes) return `None`: walking those forward from a low-bound seek
/// would require a *backward* walk from the top of the index, which
/// needs an `IdxLast`/`IdxPrev` stop-check opcode this codegen doesn't
/// have yet (#654) — those shapes keep falling back to the ordinary
/// scan, unchanged from before this function existed.
fn as_forward_comparison(expr: &Expr) -> Option<(&str, &Expr, bool)> {
    let ExprKind::Binary { op, lhs, rhs } = &expr.kind else {
        return None;
    };
    match op {
        BinaryOp::Gt => Some((where_col(lhs)?, rhs.as_ref(), false)),
        BinaryOp::Ge => Some((where_col(lhs)?, rhs.as_ref(), true)),
        BinaryOp::Lt => Some((where_col(rhs)?, lhs.as_ref(), false)),
        BinaryOp::Le => Some((where_col(rhs)?, lhs.as_ref(), true)),
        _ => None,
    }
}

/// Compiles `WHERE col > lit`/`col >= lit`/`lit < col`/`lit <= col` (`col`
/// a plain column with a matching index) as a `SeekIndexGE(lit)` walk
/// with no upper bound — `col >= lit`/`lit <= col` (`inclusive`) process
/// every entry the seek lands on and after; the exclusive `>`/`<` shapes
/// additionally skip a leading run of entries equal to `lit` (duplicate
/// keys) before processing, since `SeekIndexGE`'s floor is inclusive.
/// Real sqlite3's own `EXPLAIN QUERY PLAN` collapses inclusive and
/// exclusive into the same `(col>?)` wording (confirmed empirically,
/// sqlite3 3.53.4) — [`find_range_seek_detail`] mirrors that, not a
/// `>=`-specific spelling. Returns `Ok(false)` — `em`/`reg` untouched —
/// for any shape [`as_forward_comparison`] doesn't recognize, an
/// unsupported/mismatched-affinity operand, an unindexed column, or
/// `DISTINCT`.
#[allow(clippy::too_many_arguments)]
pub(super) fn try_compile_forward_comparison_seek<F>(
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
        return Ok(false);
    }
    let Some(where_expr) = &select.where_clause else {
        return Ok(false);
    };
    let Some((col_name, operand, inclusive)) = as_forward_comparison(where_expr) else {
        return Ok(false);
    };
    if !is_supported_operand(operand) {
        return Ok(false);
    }
    let Some(index_position) = find_leading_index(schema, col_name) else {
        return Ok(false);
    };
    let Some(index) = schema.indexes.get(index_position) else {
        return Ok(false);
    };
    let affinity = column_affinity(schema, col_name);
    if !operand_matches_column_affinity(operand, affinity) {
        return Ok(false);
    }
    let leading_collation = index
        .columns
        .first()
        .map_or(Collation::Binary, |c| c.collation);

    let index_cursor = cursors.sort;
    open_index_cursor(em, index, index_cursor)?;

    let scope = Scope::single(schema, cursors.table).with_catalog(catalog.to_vec());
    let limit = compile_limit_setup(em, reg, &scope, select)?;
    let bound_reg = compile_value(em, reg, &scope, operand)?;

    let seek_addr = em.emit(Instruction::with_p4(
        Opcode::SeekIndexGE,
        index_cursor,
        0,
        bound_reg,
        P4::SeekKey(vec![leading_collation]),
    ));
    em.patch_p2(seek_addr, end_label);

    if !inclusive {
        // The seek floor is inclusive, so a run of entries equal to
        // `bound_reg` (duplicate keys) needs skipping before the walk
        // below can treat "landed here" as "strictly past the bound".
        let skip_start = em.new_label();
        em.place(skip_start);
        let past_bound = em.new_label();
        let gt_addr = em.emit(Instruction::with_p4(
            Opcode::IdxCompareGT,
            index_cursor,
            0,
            bound_reg,
            P4::SeekKey(vec![leading_collation]),
        ));
        em.patch_p2(gt_addr, past_bound);
        let skip_next_addr = em.emit(Instruction::new(Opcode::IdxNext, index_cursor, 0, 0));
        em.patch_p2(skip_next_addr, skip_start);
        let exhausted_addr = em.emit(Instruction::new(Opcode::Goto, 0, 0, 0));
        em.patch_p2(exhausted_addr, end_label);
        em.place(past_bound);
    }

    let loop_start = em.new_label();
    em.place(loop_start);
    let row_skip = em.new_label();
    emit_matched_row(
        em,
        reg,
        select,
        schema,
        cursors,
        index_cursor,
        col_name,
        &limit,
        row_skip,
        end_label,
        catalog,
        sink,
    )?;
    em.place(row_skip);
    let next_addr = em.emit(Instruction::new(Opcode::IdxNext, index_cursor, 0, 0));
    em.patch_p2(next_addr, loop_start);
    Ok(true)
}

/// The `schema.indexes` position of the index a range-seek fast path in
/// this file would pick for `where_expr`, or `None` if `where_expr`
/// doesn't match any of the recognized shapes (`BETWEEN`, `LIKE`/`GLOB`
/// prefix, `IN`, or a forward comparison) — the same
/// shape-recognition/affinity checks `try_compile_range_row_seek` itself
/// applies, factored out so a caller can inspect *which* index would be
/// scanned without actually emitting anything (`update.rs`'s #675 fix
/// uses this to decide whether the `SET` clause touches that index and a
/// two-pass ephemeral-rowid plan is still required).
pub(crate) fn range_seek_index_position(where_expr: &Expr, schema: &TableSchema) -> Option<usize> {
    match &where_expr.kind {
        ExprKind::Between {
            expr,
            lo,
            hi,
            negated: false,
        } => {
            let col_name = where_col(expr)?;
            if !is_supported_operand(lo) || !is_supported_operand(hi) {
                return None;
            }
            let index_position = find_leading_index(schema, col_name)?;
            let affinity = column_affinity(schema, col_name);
            if !operand_matches_column_affinity(lo, affinity)
                || !operand_matches_column_affinity(hi, affinity)
            {
                return None;
            }
            Some(index_position)
        }
        ExprKind::Like {
            expr,
            pattern,
            glob,
            negated: false,
            escape: None,
        } => {
            let col_name = where_col(expr)?;
            let ExprKind::Literal(Literal::Str(pattern_str)) = &pattern.kind else {
                return None;
            };
            like_literal_prefix(pattern_str, *glob)?;
            let index_position = find_leading_index(schema, col_name)?;
            if column_affinity(schema, col_name) != Affinity::Text {
                return None;
            }
            Some(index_position)
        }
        ExprKind::In {
            expr,
            list,
            negated: false,
        } => {
            if list.is_empty() || !list.iter().all(is_supported_operand) {
                return None;
            }
            let col_name = where_col(expr)?;
            let index_position = find_leading_index(schema, col_name)?;
            let affinity = column_affinity(schema, col_name);
            if !list
                .iter()
                .all(|v| operand_matches_column_affinity(v, affinity))
            {
                return None;
            }
            Some(index_position)
        }
        _ => as_forward_comparison(where_expr).and_then(|(col_name, operand, _inclusive)| {
            if !is_supported_operand(operand) {
                return None;
            }
            let index_position = find_leading_index(schema, col_name)?;
            let affinity = column_affinity(schema, col_name);
            if !operand_matches_column_affinity(operand, affinity) {
                return None;
            }
            Some(index_position)
        }),
    }
}

/// `EXPLAIN QUERY PLAN` reporting for this file's fast paths (#606's
/// acceptance criteria: `EXPLAIN QUERY PLAN` must show index usage for
/// these query shapes) — reuses the exact same shape-recognition
/// helpers (`where_col`/`is_supported_operand`/`find_leading_index`/
/// `like_literal_prefix`) the actual codegen functions above use, so
/// this report can never drift from what `compile_direct_scan` really
/// takes. `table_display` is the already-resolved display name for the
/// table (`eqp_display_name` in `eqp.rs`) — this function only ever
/// needs it for formatting.
pub(super) fn find_range_seek_detail(
    schema: &TableSchema,
    select: &Select,
    table_display: &str,
) -> Option<String> {
    let where_expr = select.where_clause.as_ref()?;
    match &where_expr.kind {
        ExprKind::Between {
            expr,
            lo,
            hi,
            negated: false,
        } => {
            let col_name = where_col(expr)?;
            if !is_supported_operand(lo) || !is_supported_operand(hi) {
                return None;
            }
            let index_position = find_leading_index(schema, col_name)?;
            let index = schema.indexes.get(index_position)?;
            let affinity = column_affinity(schema, col_name);
            if !operand_matches_column_affinity(lo, affinity)
                || !operand_matches_column_affinity(hi, affinity)
            {
                return None;
            }
            Some(format!(
                "SEARCH {table_display} USING INDEX {} ({col_name}>? AND {col_name}<?)",
                index.name
            ))
        }
        ExprKind::Like {
            expr,
            pattern,
            glob,
            negated: false,
            escape: None,
        } => {
            let col_name = where_col(expr)?;
            let ExprKind::Literal(Literal::Str(pattern_str)) = &pattern.kind else {
                return None;
            };
            like_literal_prefix(pattern_str, *glob)?;
            let index_position = find_leading_index(schema, col_name)?;
            let index = schema.indexes.get(index_position)?;
            if column_affinity(schema, col_name) != Affinity::Text {
                return None;
            }
            Some(format!(
                "SEARCH {table_display} USING INDEX {} ({col_name}>? AND {col_name}<?)",
                index.name
            ))
        }
        ExprKind::In {
            expr,
            list,
            negated: false,
        } => {
            if list.is_empty() || !list.iter().all(is_supported_operand) {
                return None;
            }
            let col_name = where_col(expr)?;
            let index_position = find_leading_index(schema, col_name)?;
            let index = schema.indexes.get(index_position)?;
            let affinity = column_affinity(schema, col_name);
            if !list
                .iter()
                .all(|v| operand_matches_column_affinity(v, affinity))
            {
                return None;
            }
            Some(format!(
                "SEARCH {table_display} USING INDEX {} ({col_name}=?)",
                index.name
            ))
        }
        _ => as_forward_comparison(where_expr).and_then(|(col_name, operand, _inclusive)| {
            if !is_supported_operand(operand) {
                return None;
            }
            let index_position = find_leading_index(schema, col_name)?;
            let index = schema.indexes.get(index_position)?;
            let affinity = column_affinity(schema, col_name);
            if !operand_matches_column_affinity(operand, affinity) {
                return None;
            }
            // Real sqlite3 collapses inclusive and exclusive into the
            // same `(col>?)` wording (see `try_compile_forward_comparison_seek`'s
            // doc) -- `_inclusive` only matters to the compiled seek's
            // dup-skip, not to this report.
            Some(format!(
                "SEARCH {table_display} USING INDEX {} ({col_name}>?)",
                index.name
            ))
        }),
    }
}
