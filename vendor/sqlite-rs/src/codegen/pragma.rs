// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `Pragma` AST -> `Program` compilation: `journal_mode` (#388),
//! `integrity_check`/`quick_check` (#540, #541), and `synchronous`
//! (#645). Mirrors `src/codegen/transaction.rs`'s shape: one control
//! opcode per pragma, operands carrying whatever the executor needs.

use crate::codegen::Emitter;
use crate::parser::ast::{Pragma, PragmaJournalMode, PragmaSynchronous};
use crate::vdbe::{
    Instruction, Opcode, Program, JOURNAL_MODE_DELETE, JOURNAL_MODE_WAL, SYNCHRONOUS_FULL,
    SYNCHRONOUS_NORMAL, SYNCHRONOUS_OFF, SYNCHRONOUS_QUERY,
};

/// Compiles a `PRAGMA` statement into an `Init -> <op> -> Halt` program.
/// `journal_mode` emits `SetJournalMode` (`P1` carries the target mode,
/// no result rows); `integrity_check`/`quick_check` emit
/// `IntegrityCheck` (`P1` = 1 for the `quick_check` reduced pass, 0 for
/// the full `integrity_check`), which produces a result set of `TEXT`
/// rows; `synchronous` emits `Synchronous` (`P1` carries the target
/// level, or `SYNCHRONOUS_QUERY` for the bare query form, which
/// produces a single `INTEGER` result row instead of a side effect).
pub fn compile_pragma(pragma: &Pragma) -> Program {
    let mut em = Emitter::new();
    let init_addr = em.emit(Instruction::new(Opcode::Init, 0, 0, 0));
    let body_start = em.new_label();
    em.place(body_start);
    em.patch_p2(init_addr, body_start);

    match pragma {
        Pragma::JournalMode { journal_mode, .. } => {
            let mode = match journal_mode {
                PragmaJournalMode::Wal => JOURNAL_MODE_WAL,
                PragmaJournalMode::Delete => JOURNAL_MODE_DELETE,
            };
            em.emit(Instruction::new(Opcode::SetJournalMode, mode, 0, 0));
        }
        Pragma::IntegrityCheck { quick, .. } => {
            em.emit(Instruction::new(
                Opcode::IntegrityCheck,
                i32::from(*quick),
                0,
                0,
            ));
        }
        Pragma::Synchronous { level, .. } => {
            let p1 = match level {
                None => SYNCHRONOUS_QUERY,
                Some(PragmaSynchronous::Off) => SYNCHRONOUS_OFF,
                Some(PragmaSynchronous::Normal) => SYNCHRONOUS_NORMAL,
                Some(PragmaSynchronous::Full) => SYNCHRONOUS_FULL,
            };
            em.emit(Instruction::new(Opcode::Synchronous, p1, 0, 0));
        }
    }
    em.emit(Instruction::new(Opcode::Halt, 0, 0, 0));
    em.finish()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::parser::error::{parse_pragma, ParseOutcome};

    fn opcodes(program: &Program) -> Vec<Opcode> {
        program.instructions.iter().map(|i| i.opcode).collect()
    }

    #[test]
    fn journal_mode_wal_compiles_to_init_set_journal_mode_halt() {
        let pragma = match parse_pragma("PRAGMA journal_mode = WAL") {
            ParseOutcome::Accepted(p) => p,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_pragma(&pragma);
        assert_eq!(
            opcodes(&program),
            vec![Opcode::Init, Opcode::SetJournalMode, Opcode::Halt]
        );
        assert_eq!(program.instructions[1].p1, JOURNAL_MODE_WAL);
    }

    #[test]
    fn journal_mode_delete_compiles_p1_to_delete() {
        let pragma = match parse_pragma("PRAGMA journal_mode = DELETE") {
            ParseOutcome::Accepted(p) => p,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_pragma(&pragma);
        assert_eq!(
            opcodes(&program),
            vec![Opcode::Init, Opcode::SetJournalMode, Opcode::Halt]
        );
        assert_eq!(program.instructions[1].p1, JOURNAL_MODE_DELETE);
    }

    #[test]
    fn integrity_check_compiles_p1_zero() {
        let pragma = match parse_pragma("PRAGMA integrity_check") {
            ParseOutcome::Accepted(p) => p,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_pragma(&pragma);
        assert_eq!(
            opcodes(&program),
            vec![Opcode::Init, Opcode::IntegrityCheck, Opcode::Halt]
        );
        assert_eq!(program.instructions[1].p1, 0);
    }

    #[test]
    fn quick_check_compiles_p1_one() {
        let pragma = match parse_pragma("PRAGMA quick_check") {
            ParseOutcome::Accepted(p) => p,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_pragma(&pragma);
        assert_eq!(
            opcodes(&program),
            vec![Opcode::Init, Opcode::IntegrityCheck, Opcode::Halt]
        );
        assert_eq!(program.instructions[1].p1, 1);
    }

    #[test]
    fn synchronous_query_compiles_p1_sentinel() {
        let pragma = match parse_pragma("PRAGMA synchronous") {
            ParseOutcome::Accepted(p) => p,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_pragma(&pragma);
        assert_eq!(
            opcodes(&program),
            vec![Opcode::Init, Opcode::Synchronous, Opcode::Halt]
        );
        assert_eq!(program.instructions[1].p1, SYNCHRONOUS_QUERY);
    }

    #[test]
    fn synchronous_off_compiles_p1_zero() {
        let pragma = match parse_pragma("PRAGMA synchronous = OFF") {
            ParseOutcome::Accepted(p) => p,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_pragma(&pragma);
        assert_eq!(program.instructions[1].p1, SYNCHRONOUS_OFF);
    }

    #[test]
    fn synchronous_normal_compiles_p1_one() {
        let pragma = match parse_pragma("PRAGMA synchronous = NORMAL") {
            ParseOutcome::Accepted(p) => p,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_pragma(&pragma);
        assert_eq!(program.instructions[1].p1, SYNCHRONOUS_NORMAL);
    }

    #[test]
    fn synchronous_full_via_integer_compiles_p1_two() {
        let pragma = match parse_pragma("PRAGMA synchronous = 2") {
            ParseOutcome::Accepted(p) => p,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_pragma(&pragma);
        assert_eq!(program.instructions[1].p1, SYNCHRONOUS_FULL);
    }
}
