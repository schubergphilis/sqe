// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `CreateTable` AST -> `Program` compilation (#215). A single
//! `Opcode::CreateTable` instruction does the entire job procedurally at
//! exec time (allocate the root page, register the row in
//! `sqlite_master`, bump the schema cookie) — there is no per-row cursor
//! work for a DDL statement, so the usual `Init -> ... -> Halt` scan
//! shape collapses to `Init -> CreateTable -> Halt`.
//!
//! `sqlite_master.sql` gets the **verbatim** source text of the
//! statement (sliced via `create.span`), not a reconstruction from the
//! parsed `ColumnDef`s — matching stock SQLite's own storage convention
//! and what `schema::read_schema` expects to be able to round-trip.

use crate::codegen::select::CodegenError;
use crate::codegen::Emitter;
use crate::parser::ast::CreateTable;
use crate::vdbe::{Instruction, Opcode, Program, P4};

/// Compiles `CREATE TABLE` into a single `Opcode::CreateTable` instruction
/// that allocates the root page, registers the verbatim source text as the
/// `sqlite_master` row, and bumps the schema cookie at exec time.
pub fn compile_create_table(create: &CreateTable, source: &str) -> Result<Program, CodegenError> {
    let start = create.span.offset as usize;
    let end = start.saturating_add(create.span.len as usize);
    let sql = source
        .get(start..end)
        .ok_or_else(|| CodegenError::Unsupported {
            reason: "CREATE TABLE statement span out of bounds of the source text".to_string(),
        })?
        .to_string();

    let mut em = Emitter::new();
    let init_addr = em.emit(Instruction::new(Opcode::Init, 0, 0, 0));
    let body_start = em.new_label();
    em.place(body_start);
    em.patch_p2(init_addr, body_start);

    em.emit(Instruction::with_p4(
        Opcode::CreateTable,
        0,
        0,
        0,
        P4::CreateTable {
            name: create.name.clone(),
            sql,
        },
    ));
    em.emit(Instruction::new(Opcode::Halt, 0, 0, 0));
    Ok(em.finish())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::parser::error::ParseOutcome;
    use crate::vdbe::Opcode;

    #[test]
    fn compiles_to_init_create_table_halt() {
        let sql = "CREATE TABLE t(a INTEGER, b TEXT)";
        let create = match crate::parser::error::parse_create_table(sql) {
            ParseOutcome::Accepted(c) => c,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_create_table(&create, sql).unwrap();

        let opcodes: Vec<Opcode> = program.instructions.iter().map(|i| i.opcode).collect();
        assert_eq!(
            opcodes,
            vec![Opcode::Init, Opcode::CreateTable, Opcode::Halt]
        );
        match &program.instructions[1].p4 {
            P4::CreateTable { name, sql: got_sql } => {
                assert_eq!(name, "t");
                assert_eq!(got_sql, sql);
            }
            other => panic!("expected P4::CreateTable, got {other:?}"),
        }
    }
}
