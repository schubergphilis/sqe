// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `DropTable` AST -> `Program` compilation (#215). Like `CreateTable`,
//! a single `Opcode::DropTable` instruction does the whole job at exec
//! time: free the table's b-tree pages, cascade-drop every index on it
//! (also freeing their pages), remove the `sqlite_master` row(s), and
//! bump the schema cookie once for the statement.

use crate::codegen::select::CodegenError;
use crate::codegen::Emitter;
use crate::parser::ast::DropTable;
use crate::schema::TableSchema;
use crate::vdbe::{Instruction, Opcode, Program, P4};

/// Compiles `DROP TABLE` into a single `Opcode::DropTable` instruction that
/// frees the table's pages, cascade-drops its indexes, and removes its
/// `sqlite_master` row(s) at exec time.
pub fn compile_drop_table(drop: &DropTable, schema: &TableSchema) -> Result<Program, CodegenError> {
    let indexes = schema
        .indexes
        .iter()
        .map(|idx| (idx.name.clone(), idx.root_page))
        .collect();

    let mut em = Emitter::new();
    let init_addr = em.emit(Instruction::new(Opcode::Init, 0, 0, 0));
    let body_start = em.new_label();
    em.place(body_start);
    em.patch_p2(init_addr, body_start);

    em.emit(Instruction::with_p4(
        Opcode::DropTable,
        0,
        0,
        0,
        P4::DropTable {
            name: drop.name.clone(),
            root_page: schema.root_page,
            indexes,
        },
    ));
    em.emit(Instruction::new(Opcode::Halt, 0, 0, 0));
    Ok(em.finish())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::parser::error::{parse_drop_table, ParseOutcome};
    use crate::schema::IndexSchema;
    use crate::vdbe::Opcode;

    fn schema_with_index() -> TableSchema {
        TableSchema {
            unresolved_autoindex: false,
            name: "t".to_string(),
            root_page: 2,
            columns: vec!["a".to_string()],
            without_rowid: false,
            strict: false,
            column_types: vec!["INTEGER".to_string()],
            column_collations: vec![],
            is_virtual: false,
            sql: "CREATE TABLE t(a INTEGER)".to_string(),
            indexes: vec![IndexSchema {
                name: "idx_t_a".to_string(),
                unique: false,
                columns: vec![],
                root_page: 3,
            }],
            rowid_alias: None,
        }
        .with_computed_rowid_alias()
    }

    #[test]
    fn compiles_to_init_drop_table_halt_carrying_indexes() {
        let drop = match parse_drop_table("DROP TABLE t") {
            ParseOutcome::Accepted(d) => d,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let schema = schema_with_index();
        let program = compile_drop_table(&drop, &schema).unwrap();

        let opcodes: Vec<Opcode> = program.instructions.iter().map(|i| i.opcode).collect();
        assert_eq!(opcodes, vec![Opcode::Init, Opcode::DropTable, Opcode::Halt]);
        match &program.instructions[1].p4 {
            P4::DropTable {
                name,
                root_page,
                indexes,
            } => {
                assert_eq!(name, "t");
                assert_eq!(*root_page, 2);
                assert_eq!(indexes, &vec![("idx_t_a".to_string(), 3)]);
            }
            other => panic!("expected P4::DropTable, got {other:?}"),
        }
    }
}
