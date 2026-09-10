// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `DropIndex` AST -> `Program` compilation (#215). A single
//! `Opcode::DropIndex` instruction frees the index's b-tree pages,
//! removes its `sqlite_master` row, and bumps the schema cookie.

use crate::codegen::select::CodegenError;
use crate::codegen::Emitter;
use crate::parser::ast::DropIndex;
use crate::vdbe::{Instruction, Opcode, Program, P4};

/// Compiles `DROP INDEX` into a single `Opcode::DropIndex` instruction that
/// frees the index's pages, removes its `sqlite_master` row, and bumps the
/// schema cookie at exec time.
pub fn compile_drop_index(di: &DropIndex, root_page: u32) -> Result<Program, CodegenError> {
    let mut em = Emitter::new();
    let init_addr = em.emit(Instruction::new(Opcode::Init, 0, 0, 0));
    let body_start = em.new_label();
    em.place(body_start);
    em.patch_p2(init_addr, body_start);

    em.emit(Instruction::with_p4(
        Opcode::DropIndex,
        0,
        0,
        0,
        P4::DropIndex {
            name: di.name.clone(),
            root_page,
        },
    ));
    em.emit(Instruction::new(Opcode::Halt, 0, 0, 0));
    Ok(em.finish())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::parser::error::{parse_drop_index, ParseOutcome};
    use crate::vdbe::Opcode;

    #[test]
    fn compiles_to_init_drop_index_halt() {
        let di = match parse_drop_index("DROP INDEX idx_t_a") {
            ParseOutcome::Accepted(d) => d,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_drop_index(&di, 3).unwrap();

        let opcodes: Vec<Opcode> = program.instructions.iter().map(|i| i.opcode).collect();
        assert_eq!(opcodes, vec![Opcode::Init, Opcode::DropIndex, Opcode::Halt]);
        match &program.instructions[1].p4 {
            P4::DropIndex { name, root_page } => {
                assert_eq!(name, "idx_t_a");
                assert_eq!(*root_page, 3);
            }
            other => panic!("expected P4::DropIndex, got {other:?}"),
        }
    }
}
