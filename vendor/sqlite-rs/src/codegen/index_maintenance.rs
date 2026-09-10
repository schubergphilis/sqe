// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Secondary-index maintenance shared by `INSERT`/`DELETE`/`UPDATE`
//! codegen (#196): open a write cursor per index alongside the table
//! cursor, and emit the `IdxInsert`/`IdxDelete` pair for a row's index
//! entries.
//!
//! For a row whose values are only available from disk (the *old* row
//! of an `UPDATE`'s rebuild, or a row displaced by an `INSERT OR
//! REPLACE` conflict), index keys are read back from the table cursor's
//! *current* row via ordinary `Opcode::Column`/`Opcode::Rowid` (rowid
//! last, matching the on-disk index key convention
//! `btree::index::insert`/`index::delete` use) — see
//! [`emit_index_key_ops`]. For a row whose values are already sitting in
//! registers (a freshly-inserted/updated row, before it's written),
//! [`emit_index_key_ops_from_regs`] builds the same key layout via
//! `Opcode::Copy` from those registers instead, with no cursor re-seek
//! or re-read.
//!
//! `DESC` index columns are rejected (`CodegenError::Unsupported`)
//! rather than silently mis-keyed: no index b-tree comparator in this
//! codebase (#171) is aware of per-column sort direction, so a `DESC`
//! column would otherwise get built into the key as if it were
//! ascending — a plausible-looking but semantically backwards key.

use crate::codegen::expr::{column_index, emit_column_read};
use crate::codegen::select::CodegenError;
use crate::codegen::{Emitter, RegAlloc};
use crate::schema::{IndexSchema, TableSchema};
use crate::vdbe::{Instruction, Opcode, P4};

/// Validates a table's `sqlite_master.rootpage` before it's used as an
/// `OpenRead`/`OpenWrite` operand.
///
/// `rootpage` is untrusted on-disk data — a corrupt or adversarial file
/// could carry a zero, negative, or out-of-`i32`-range value for it.
/// Rather than defaulting a bad value to page 0 (the reserved header
/// page) and silently pointing the cursor at it, this rejects the
/// schema outright.
pub(crate) fn valid_table_root_page(schema: &TableSchema) -> Result<i32, CodegenError> {
    i32::try_from(schema.root_page)
        .ok()
        .filter(|p| *p > 0)
        .ok_or_else(|| CodegenError::Unsupported {
            reason: format!(
                "table {} has an invalid root page ({})",
                schema.name, schema.root_page
            ),
        })
}

/// Same validation as [`valid_table_root_page`], for an index's root page.
pub(crate) fn valid_index_root_page(index: &IndexSchema) -> Result<i32, CodegenError> {
    i32::try_from(index.root_page)
        .ok()
        .filter(|p| *p > 0)
        .ok_or_else(|| CodegenError::Unsupported {
            reason: format!(
                "index {} has an invalid root page ({})",
                index.name, index.root_page
            ),
        })
}

/// `OpenWrite`s one write cursor per index on `schema`, starting at
/// `first_cursor`, with `P5 = 1` selecting `CursorSlot::IndexWrite`
/// (#194's `OpenWrite` doc).
pub(crate) fn open_index_cursors(
    em: &mut Emitter,
    schema: &TableSchema,
    first_cursor: i32,
) -> Result<(), CodegenError> {
    for (i, index) in schema.indexes.iter().enumerate() {
        let cursor = first_cursor.saturating_add(i32::try_from(i).unwrap_or(0));
        let root_page = valid_index_root_page(index)?;
        let mut instr = Instruction::new(Opcode::OpenWrite, cursor, root_page, 0);
        instr.p5 = 1;
        em.emit(instr);
    }
    Ok(())
}

/// For every index on `schema`, reads the current row at `table_cursor`
/// into a fresh contiguous register block (index columns in declared
/// order, then rowid) and emits `opcode` (`IdxInsert` or `IdxDelete`)
/// against the matching cursor in `[first_index_cursor, ...)`.
///
/// The table cursor must already be positioned on the row whose index
/// entries are being built — callers use this both pre-`Delete` (cursor
/// already there) and post-`Insert` (after a `SeekRowid` back onto the
/// just-written row).
pub(crate) fn emit_index_key_ops(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    schema: &TableSchema,
    table_cursor: i32,
    first_index_cursor: i32,
    opcode: Opcode,
) -> Result<(), CodegenError> {
    for (i, index) in schema.indexes.iter().enumerate() {
        let index_cursor = first_index_cursor.saturating_add(i32::try_from(i).unwrap_or(0));
        let mut start = None;
        for col in &index.columns {
            if col.desc {
                // No index b-tree comparator anywhere in this codebase
                // (#171) is aware of per-column sort direction — it
                // always compares the encoded key ascending. Silently
                // building the key as if `col` were ascending would
                // write an index stock `sqlite3` (which does honor
                // `DESC`) reads back in the wrong order. Reject loudly
                // instead of writing a key that's byte-for-byte
                // plausible but semantically backwards.
                return Err(CodegenError::Unsupported {
                    reason: format!(
                        "index {} has a DESC column ({}); descending index keys aren't supported yet",
                        index.name, col.name
                    ),
                });
            }
            let col_idx =
                column_index(schema, &col.name).ok_or_else(|| CodegenError::Unsupported {
                    reason: format!(
                        "index {} references a column or expression this codegen can't resolve: {}",
                        index.name, col.name
                    ),
                })?;
            let r = reg.alloc();
            if start.is_none() {
                start = Some(r);
            }
            emit_column_read(em, schema, table_cursor, col_idx, r)?;
        }
        let rowid_reg = reg.alloc();
        if start.is_none() {
            start = Some(rowid_reg);
        }
        em.emit(Instruction::new(Opcode::Rowid, table_cursor, rowid_reg, 0));

        let count = i32::try_from(index.columns.len().saturating_add(1)).unwrap_or(0);
        em.emit(Instruction::with_p4(
            opcode,
            index_cursor,
            start.unwrap_or(rowid_reg),
            0,
            P4::Int(i64::from(count)),
        ));
    }
    Ok(())
}

/// Like [`emit_index_key_ops`], but for a row whose column values are
/// already sitting in `col_regs` (one register per `schema.columns`
/// entry, in order — the same layout `INSERT`/`UPDATE` codegen builds
/// for `MakeRecord`) and whose rowid is already in `rowid_reg`. Builds
/// each index's key via `Opcode::Copy` from those registers into a
/// fresh contiguous run instead of `Opcode::Column`/`Opcode::Rowid`
/// against a cursor — so callers don't need to `SeekRowid` back onto
/// the row first. Always emits `IdxInsert`: the only caller that needs
/// `IdxDelete` (removing a *different*, already-on-disk row's stale
/// entries) has no such register run to reuse and stays on
/// [`emit_index_key_ops`].
pub(crate) fn emit_index_key_ops_from_regs(
    em: &mut Emitter,
    reg: &mut RegAlloc,
    schema: &TableSchema,
    col_regs: &[i32],
    rowid_reg: i32,
    first_index_cursor: i32,
) -> Result<(), CodegenError> {
    for (i, index) in schema.indexes.iter().enumerate() {
        let index_cursor = first_index_cursor.saturating_add(i32::try_from(i).unwrap_or(0));
        let mut start = None;
        for col in &index.columns {
            if col.desc {
                return Err(CodegenError::Unsupported {
                    reason: format!(
                        "index {} has a DESC column ({}); descending index keys aren't supported yet",
                        index.name, col.name
                    ),
                });
            }
            let col_idx =
                column_index(schema, &col.name).ok_or_else(|| CodegenError::Unsupported {
                    reason: format!(
                        "index {} references a column or expression this codegen can't resolve: {}",
                        index.name, col.name
                    ),
                })?;
            // The rowid-alias column's own register holds NULL (readers
            // substitute the cursor's actual rowid instead — see
            // `emit_column_read`), so its live value is `rowid_reg`, not
            // `col_regs[col_idx]`.
            let src = if Some(col_idx) == schema.rowid_alias {
                rowid_reg
            } else {
                *col_regs
                    .get(col_idx)
                    .ok_or_else(|| CodegenError::Unsupported {
                        reason: format!(
                            "index {} references column {} outside the row's register run",
                            index.name, col.name
                        ),
                    })?
            };
            let r = reg.alloc();
            if start.is_none() {
                start = Some(r);
            }
            em.emit(Instruction::new(Opcode::Copy, src, r, 0));
        }
        let key_rowid_reg = reg.alloc();
        if start.is_none() {
            start = Some(key_rowid_reg);
        }
        em.emit(Instruction::new(Opcode::Copy, rowid_reg, key_rowid_reg, 0));

        let count = i32::try_from(index.columns.len().saturating_add(1)).unwrap_or(0);
        em.emit(Instruction::with_p4(
            Opcode::IdxInsert,
            index_cursor,
            start.unwrap_or(key_rowid_reg),
            0,
            P4::Int(i64::from(count)),
        ));
    }
    Ok(())
}
