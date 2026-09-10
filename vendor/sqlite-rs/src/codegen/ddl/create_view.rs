// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `CreateView` AST -> `Program` compilation (#380). Mirrors
//! `create_table.rs`'s single `Opcode::CreateView` shape exactly, minus
//! the b-tree root-page allocation `CreateTable` does — a view has no
//! b-tree of its own, so `sqlite_master.rootpage` is stored as `0`
//! (matching stock SQLite's own convention for views).
//!
//! `sqlite_master.sql` gets the **verbatim** source text of the
//! statement (sliced via `create.span`), exactly like `CreateTable` —
//! this is what `schema::read_views` re-parses on catalog load and
//! `codegen::expand_views` substitutes into a referencing `FROM`
//! clause.

use crate::codegen::select::CodegenError;
use crate::codegen::Emitter;
use crate::parser::ast::CreateView;
use crate::vdbe::{Instruction, Opcode, Program, P4};

/// Compiles `CREATE VIEW` into a single `Opcode::CreateView` instruction
/// that writes the verbatim source text into `sqlite_master.sql` with
/// `rootpage` `0` (views have no b-tree of their own).
pub fn compile_create_view(create: &CreateView, source: &str) -> Result<Program, CodegenError> {
    let start = create.span.offset as usize;
    let end = start.saturating_add(create.span.len as usize);
    let sql = source
        .get(start..end)
        .ok_or_else(|| CodegenError::Unsupported {
            reason: "CREATE VIEW statement span out of bounds of the source text".to_string(),
        })?
        .to_string();

    let mut em = Emitter::new();
    let init_addr = em.emit(Instruction::new(Opcode::Init, 0, 0, 0));
    let body_start = em.new_label();
    em.place(body_start);
    em.patch_p2(init_addr, body_start);

    em.emit(Instruction::with_p4(
        Opcode::CreateView,
        0,
        0,
        0,
        P4::CreateView {
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
    fn compiles_to_init_create_view_halt() {
        let sql = "CREATE VIEW v AS SELECT a FROM t";
        let create = match crate::parser::error::parse_create_view(sql) {
            ParseOutcome::Accepted(c) => c,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_create_view(&create, sql).unwrap();
        let ops: Vec<Opcode> = program.instructions.iter().map(|i| i.opcode).collect();
        assert_eq!(ops, vec![Opcode::Init, Opcode::CreateView, Opcode::Halt]);
    }
}
