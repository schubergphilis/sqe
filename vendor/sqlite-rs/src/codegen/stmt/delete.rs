// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `Delete` AST -> `Program` compilation (#210, index maintenance #196).
//! Mirrors `select.rs`'s `Init -> OpenRead -> Rewind -> [WHERE test] ->
//! Next -> Halt` scan shape, swapping `OpenRead` for `OpenWrite` and the
//! result-row emission for a per-index `IdxDelete` plus a table
//! `Delete` per matched row.
//!
//! Safe to delete mid-scan: `TableCursor`'s traversal frames are
//! snapshotted page bytes captured at descent time (`src/btree.rs`'s
//! `Frame::page`), so `Opcode::Delete` mutating the on-disk b-tree via
//! `btree::delete_row` never invalidates this cursor's own in-flight
//! `Next` traversal — unlike a cursor that re-reads live page state on
//! every step.
//!
//! #336: `WHERE rowid = <int literal|param>` (or the table's `INTEGER
//! PRIMARY KEY` rowid-alias column) compiles to `SeekRowid` instead of
//! the full scan below, mirroring `select.rs`'s `try_compile_rowid_seek`
//! (#137) exactly — same narrow recognition (a single top-level
//! equality, nothing compound), same fallback to the ordinary scan for
//! anything else.

use crate::codegen::expr::{compile_cond, compile_value};
use crate::codegen::index_maintenance::{
    emit_index_key_ops, open_index_cursors, valid_table_root_page,
};
use crate::codegen::select::{is_rowid_reference, top_level_equality_operands, CodegenError};
use crate::codegen::{CondTargets, Emitter, RegAlloc, Scope, Target};
use crate::parser::ast::{Delete, ExprKind, Literal, ParamKind};
use crate::schema::TableSchema;
use crate::vdbe::{Instruction, Opcode, Program, OPFLAG_NCHANGE};

const TABLE_CURSOR: i32 = 0;
const FIRST_INDEX_CURSOR: i32 = 1;

/// Compiles `delete` against `schema` (the resolved target table) into
/// a `Program`. `catalog = [schema]` — no cross-table subquery support
/// in the `WHERE` expression; use [`compile_delete_with_catalog`] for
/// that (#251).
pub fn compile_delete(delete: &Delete, schema: &TableSchema) -> Result<Program, CodegenError> {
    compile_delete_with_catalog(delete, schema, std::slice::from_ref(schema))
}

/// [`compile_delete`], plus `catalog` — the full table catalog, used to
/// resolve a scalar/`IN`/`EXISTS` subquery expression in the `WHERE`
/// clause when it names a table other than `schema` itself (#251).
pub fn compile_delete_with_catalog(
    delete: &Delete,
    schema: &TableSchema,
    catalog: &[TableSchema],
) -> Result<Program, CodegenError> {
    if schema.without_rowid {
        return Err(CodegenError::Unsupported {
            reason: "WITHOUT ROWID tables are not supported by DELETE codegen yet".to_string(),
        });
    }
    // #685: an on-disk `sqlite_autoindex_*` whose key columns could not
    // be recovered from the table's DDL is absent from `schema.indexes`,
    // so this codegen would neither enforce its uniqueness nor maintain
    // it — the write would report success and leave the index stale.
    // Refuse instead, per spec 010/Req 8 and spec 007/Req 1's precedent.
    if schema.unresolved_autoindex {
        return Err(CodegenError::Unsupported {
            reason: format!(
                "table {} carries an automatic index this reader could not \
                 interpret, so DELETE would corrupt it; the table is read-only",
                schema.name
            ),
        });
    }

    let mut em = Emitter::new();
    let mut reg = RegAlloc::new();

    let init_addr = em.emit(Instruction::new(Opcode::Init, 0, 0, 0));
    let body_start = em.new_label();
    em.place(body_start);
    em.patch_p2(init_addr, body_start);

    let root_page = valid_table_root_page(schema)?;
    em.emit(Instruction::new(
        Opcode::OpenWrite,
        TABLE_CURSOR,
        root_page,
        0,
    ));
    open_index_cursors(&mut em, schema, FIRST_INDEX_CURSOR)?;

    let scope = Scope::single(schema, TABLE_CURSOR).with_catalog(catalog.to_vec());
    let end_label = em.new_label();

    let rowid_seek_operand = delete
        .where_clause
        .as_ref()
        .and_then(|where_expr| top_level_equality_operands(where_expr))
        .and_then(|(lhs, rhs)| {
            if is_rowid_reference(schema, lhs) {
                Some(rhs)
            } else if is_rowid_reference(schema, rhs) {
                Some(lhs)
            } else {
                None
            }
        })
        .filter(|operand| {
            matches!(
                &operand.kind,
                ExprKind::Literal(Literal::Integer(_))
                    | ExprKind::Param(ParamKind::Anonymous | ParamKind::Numbered(_))
            )
        });

    if let Some(operand) = rowid_seek_operand {
        // #336: exactly one row can match — seek straight to it instead
        // of scanning. Jumping to `end_label` on a miss (no such rowid)
        // skips the delete entirely, same as the ordinary scan finding
        // zero matching rows.
        let value_reg = compile_value(&mut em, &mut reg, &scope, operand)?;
        let seek_addr = em.emit(Instruction::new(
            Opcode::SeekRowid,
            TABLE_CURSOR,
            0,
            value_reg,
        ));
        em.patch_p2(seek_addr, end_label);

        emit_index_key_ops(
            &mut em,
            &mut reg,
            schema,
            TABLE_CURSOR,
            FIRST_INDEX_CURSOR,
            Opcode::IdxDelete,
        )?;
        em.emit(Instruction::with_p5(
            Opcode::Delete,
            TABLE_CURSOR,
            0,
            0,
            OPFLAG_NCHANGE,
        ));

        em.place(end_label);
        em.emit(Instruction::new(Opcode::Halt, 0, 0, 0));
        return Ok(em.finish());
    }

    let rewind_addr = em.emit(Instruction::new(Opcode::Rewind, TABLE_CURSOR, 0, 0));
    em.patch_p2(rewind_addr, end_label);
    let loop_start = em.new_label();
    em.place(loop_start);

    let row_skip = em.new_label();
    if let Some(where_expr) = &delete.where_clause {
        compile_cond(
            &mut em,
            &mut reg,
            &scope,
            where_expr,
            CondTargets::null_is_false(Target::Fallthrough, Target::Jump(row_skip)),
        )?;
    }

    emit_index_key_ops(
        &mut em,
        &mut reg,
        schema,
        TABLE_CURSOR,
        FIRST_INDEX_CURSOR,
        Opcode::IdxDelete,
    )?;
    em.emit(Instruction::with_p5(
        Opcode::Delete,
        TABLE_CURSOR,
        0,
        0,
        OPFLAG_NCHANGE,
    ));

    em.place(row_skip);
    let next_addr = em.emit(Instruction::new(Opcode::Next, TABLE_CURSOR, 0, 0));
    em.patch_p2(next_addr, loop_start);

    em.place(end_label);
    em.emit(Instruction::new(Opcode::Halt, 0, 0, 0));
    Ok(em.finish())
}
