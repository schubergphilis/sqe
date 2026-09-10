// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `Begin`/`Commit`/`Rollback` AST -> `Program` compilation (#360). Each
//! compiles to a single control opcode, exactly like the DDL statements
//! in the sibling `ddl` module: `Transaction` for `BEGIN`, `AutoCommit`
//! for `COMMIT`/`ROLLBACK` (`P2` = 1/0 respectively, stock SQLite's
//! convention). `TransactionMode` (DEFERRED/IMMEDIATE/EXCLUSIVE) is
//! carried through `Transaction`'s `P1` (#395) so
//! `crate::vdbe::control::transaction` can escalate the lock at `BEGIN`
//! time for IMMEDIATE/EXCLUSIVE rather than waiting for the first write.

use crate::codegen::Emitter;
use crate::parser::ast::{Begin, Commit, Rollback, TransactionMode};
use crate::vdbe::{
    Instruction, Opcode, Program, TRANSACTION_MODE_DEFERRED, TRANSACTION_MODE_EXCLUSIVE,
    TRANSACTION_MODE_IMMEDIATE,
};

/// Compiles `BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE]` into an
/// `Init -> Transaction -> Halt` program, carrying the transaction mode
/// through `Transaction`'s `P1`.
pub fn compile_begin(begin: &Begin) -> Program {
    let mode = match begin.mode {
        None | Some(TransactionMode::Deferred) => TRANSACTION_MODE_DEFERRED,
        Some(TransactionMode::Immediate) => TRANSACTION_MODE_IMMEDIATE,
        Some(TransactionMode::Exclusive) => TRANSACTION_MODE_EXCLUSIVE,
    };

    let mut em = Emitter::new();
    let init_addr = em.emit(Instruction::new(Opcode::Init, 0, 0, 0));
    let body_start = em.new_label();
    em.place(body_start);
    em.patch_p2(init_addr, body_start);

    em.emit(Instruction::new(Opcode::Transaction, mode, 0, 0));
    em.emit(Instruction::new(Opcode::Halt, 0, 0, 0));
    em.finish()
}

/// Compiles `COMMIT` into an `Init -> AutoCommit -> Halt` program with
/// `AutoCommit`'s `P2` set to 1.
pub fn compile_commit(_commit: &Commit) -> Program {
    compile_auto_commit(1)
}

/// Compiles `ROLLBACK` into an `Init -> AutoCommit -> Halt` program with
/// `AutoCommit`'s `P2` set to 0.
pub fn compile_rollback(_rollback: &Rollback) -> Program {
    compile_auto_commit(0)
}

fn compile_auto_commit(commit: i32) -> Program {
    let mut em = Emitter::new();
    let init_addr = em.emit(Instruction::new(Opcode::Init, 0, 0, 0));
    let body_start = em.new_label();
    em.place(body_start);
    em.patch_p2(init_addr, body_start);

    em.emit(Instruction::new(Opcode::AutoCommit, 0, commit, 0));
    em.emit(Instruction::new(Opcode::Halt, 0, 0, 0));
    em.finish()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::parser::error::{parse_begin, parse_commit, parse_rollback, ParseOutcome};

    fn opcodes(program: &Program) -> Vec<Opcode> {
        program.instructions.iter().map(|i| i.opcode).collect()
    }

    #[test]
    fn begin_compiles_to_init_transaction_halt() {
        let begin = match parse_begin("BEGIN") {
            ParseOutcome::Accepted(b) => b,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_begin(&begin);
        assert_eq!(
            opcodes(&program),
            vec![Opcode::Init, Opcode::Transaction, Opcode::Halt]
        );
        assert_eq!(program.instructions[1].p1, TRANSACTION_MODE_DEFERRED);
    }

    #[test]
    fn begin_immediate_compiles_transaction_p1_to_immediate() {
        let begin = match parse_begin("BEGIN IMMEDIATE") {
            ParseOutcome::Accepted(b) => b,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_begin(&begin);
        assert_eq!(program.instructions[1].p1, TRANSACTION_MODE_IMMEDIATE);
    }

    #[test]
    fn begin_exclusive_compiles_transaction_p1_to_exclusive() {
        let begin = match parse_begin("BEGIN EXCLUSIVE") {
            ParseOutcome::Accepted(b) => b,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_begin(&begin);
        assert_eq!(program.instructions[1].p1, TRANSACTION_MODE_EXCLUSIVE);
    }

    #[test]
    fn begin_deferred_compiles_transaction_p1_to_deferred() {
        let begin = match parse_begin("BEGIN DEFERRED") {
            ParseOutcome::Accepted(b) => b,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_begin(&begin);
        assert_eq!(program.instructions[1].p1, TRANSACTION_MODE_DEFERRED);
    }

    #[test]
    fn commit_compiles_to_auto_commit_with_p2_one() {
        let commit = match parse_commit("COMMIT") {
            ParseOutcome::Accepted(c) => c,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_commit(&commit);
        assert_eq!(
            opcodes(&program),
            vec![Opcode::Init, Opcode::AutoCommit, Opcode::Halt]
        );
        assert_eq!(program.instructions[1].p2, 1);
    }

    #[test]
    fn rollback_compiles_to_auto_commit_with_p2_zero() {
        let rollback = match parse_rollback("ROLLBACK") {
            ParseOutcome::Accepted(r) => r,
            other => panic!("expected Accepted, got {other:?}"),
        };
        let program = compile_rollback(&rollback);
        assert_eq!(
            opcodes(&program),
            vec![Opcode::Init, Opcode::AutoCommit, Opcode::Halt]
        );
        assert_eq!(program.instructions[1].p2, 0);
    }
}
