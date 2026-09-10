// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `SetJournalMode` execution (#388): switches the pager between
//! rollback-journal and WAL journal modes, matching `PRAGMA
//! journal_mode = WAL|DELETE`. Mirrors `src/vdbe/control.rs`'s
//! `transaction`/`auto_commit` shape: reaches the pager via
//! `vm.db()?.writer`, and independently checks `vm.autocommit` so a
//! mode switch mid-transaction errors with a clear message rather than
//! being silently applied.

use crate::header::{JournalMode, SynchronousMode};
use crate::integrity::run_integrity_check;
use crate::record::Value;
use crate::vdbe::exec::{ExecError, Step, Vm};
use crate::vdbe::program::Instruction;

/// `Instruction::p1` values `compile_pragma`/`set_journal_mode` (#388)
/// use to carry the target [`JournalMode`] through the
/// `SetJournalMode` opcode.
pub const JOURNAL_MODE_DELETE: i32 = 0;
/// See [`JOURNAL_MODE_DELETE`].
pub const JOURNAL_MODE_WAL: i32 = 1;

/// `Instruction::p1` values `compile_pragma`/`synchronous` (#645) use to
/// carry the target [`SynchronousMode`] (or the bare-query-form
/// sentinel) through the `Synchronous` opcode.
pub const SYNCHRONOUS_OFF: i32 = 0;
/// See [`SYNCHRONOUS_OFF`].
pub const SYNCHRONOUS_NORMAL: i32 = 1;
/// See [`SYNCHRONOUS_OFF`].
pub const SYNCHRONOUS_FULL: i32 = 2;
/// `PRAGMA synchronous` (bare, no `=`): query the current level rather
/// than set it. Distinct from every real level (`0`/`1`/`2`), never
/// mistakable for one.
pub const SYNCHRONOUS_QUERY: i32 = -1;

/// `SetJournalMode`: switches the pager's on-disk journal mode via
/// [`crate::pager::Pager::set_journal_mode`]. Errors with
/// [`ExecError::JournalModeChangeDuringTransaction`] rather than
/// silently applying (or silently no-opping) if a transaction is
/// already open (`!vm.autocommit`) — matches stock SQLite's refusal to
/// change journal mode mid-transaction; `Pager::set_journal_mode`
/// itself independently re-checks for a pending transaction, since it
/// is a public API a caller could invoke directly without going
/// through this opcode. A `Vm` with no writable database attached (a
/// read-only connection) is a no-op — nothing to switch. Reuses
/// [`ExecError::FlushFailed`]'s `#[from] PagerError` conversion for any
/// pager-level failure, the same way `control::transaction`'s
/// `begin_immediate`/`begin_exclusive` calls already do — see that
/// variant's doc comment.
pub fn set_journal_mode(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    if !vm.autocommit {
        return Err(ExecError::JournalModeChangeDuringTransaction);
    }
    let mode = if instr.p1 == JOURNAL_MODE_WAL {
        JournalMode::Wal
    } else {
        JournalMode::Legacy
    };
    if let Some(writer) = vm.db().ok().and_then(|db| db.writer.clone()) {
        writer.borrow_mut().set_journal_mode(mode)?;
    }
    Ok(Step::Next)
}

/// `Synchronous` (#645): the bare query form (`P1` equal to
/// [`SYNCHRONOUS_QUERY`]) emits the attached pager's current
/// [`SynchronousMode`] as a single `INTEGER` result row (`0`/`1`/`2`,
/// matching stock SQLite's own numeric report) via [`Vm::emit_row`] --
/// mirrors [`integrity_check`]'s row-emitting shape rather than
/// `set_journal_mode`'s side-effect-only one. A read-only `Vm` (no
/// `writer`) reports [`SynchronousMode::default`] (`Full`), since
/// there's no pager instance to have diverged from it. Otherwise sets
/// the level via [`crate::pager::Pager::set_synchronous`] -- unlike
/// [`set_journal_mode`], this never checks `vm.autocommit`: stock
/// SQLite allows changing `synchronous` at any time, including
/// mid-transaction, since it only affects fsync behavior at the next
/// commit.
pub fn synchronous(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let writer = vm.db().ok().and_then(|db| db.writer.clone());
    if instr.p1 == SYNCHRONOUS_QUERY {
        let mode = writer.map_or(SynchronousMode::default(), |writer| {
            writer.borrow().synchronous()
        });
        vm.emit_row(vec![Value::Integer(mode as i64)]);
        return Ok(Step::Next);
    }
    let mode = match instr.p1 {
        SYNCHRONOUS_OFF => SynchronousMode::Off,
        SYNCHRONOUS_NORMAL => SynchronousMode::Normal,
        _ => SynchronousMode::Full,
    };
    if let Some(writer) = writer {
        writer.borrow_mut().set_synchronous(mode);
    }
    Ok(Step::Next)
}

/// `IntegrityCheck` (#540, #541): `P1` is 1 for `quick_check`, 0 for the
/// full `integrity_check`. Runs [`run_integrity_check`] against the
/// attached database's `source`/`header` and emits one `TEXT` result row
/// per problem found (or a single `"ok"` row if none) via
/// [`Vm::emit_row`]. Works against a read-only `Vm` (see
/// [`Vm::with_db`]) since it never needs `vm.db()?.writer`.
pub fn integrity_check(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let quick = instr.p1 != 0;
    let db = vm.db()?;
    let problems = run_integrity_check(db.source.clone(), &db.header, quick);
    for problem in problems {
        vm.emit_row(vec![Value::Text(problem.into())]);
    }
    Ok(Step::Next)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::vdbe::program::Opcode;
    use crate::vdbe::Vm as VmType;
    use crate::vfs::PageSource;

    /// A one-page database, just enough for `Pager::open` to succeed —
    /// mirrors `control.rs`'s own `writable_vm` test helper.
    fn writable_vm() -> VmType {
        let page_size = 512u32;
        let mut page1 = vec![0u8; page_size as usize];
        page1[0..16].copy_from_slice(b"SQLite format 3\0");
        page1[16..18].copy_from_slice(&u16::try_from(page_size).unwrap().to_be_bytes());
        page1[18] = 1;
        page1[19] = 1;
        page1[28..32].copy_from_slice(&1u32.to_be_bytes());
        page1[56..60].copy_from_slice(&1u32.to_be_bytes());
        let mut header_bytes = [0u8; 100];
        header_bytes.copy_from_slice(&page1[..100]);
        let header = crate::header::DatabaseHeader::parse(&header_bytes).unwrap();

        let mut vfs = crate::vfs::MemoryVfs::new();
        vfs.insert("/test.db", page1);
        let pager =
            crate::pager::Pager::open(&vfs, std::path::Path::new("/test.db"), page_size).unwrap();
        VmType::with_writable_db(pager, header)
    }

    #[test]
    fn switches_journal_to_wal() {
        let mut vm = writable_vm();
        let instr = Instruction::new(Opcode::SetJournalMode, JOURNAL_MODE_WAL, 0, 0);
        assert_eq!(set_journal_mode(&mut vm, &instr).unwrap(), Step::Next);
        let writer = vm.db().unwrap().writer.clone().unwrap();
        assert_eq!(writer.borrow().read_page(1).unwrap()[18..20], [2, 2]);
    }

    #[test]
    fn errors_when_a_transaction_is_open() {
        let mut vm = writable_vm();
        vm.autocommit = false;
        let instr = Instruction::new(Opcode::SetJournalMode, JOURNAL_MODE_WAL, 0, 0);
        assert!(matches!(
            set_journal_mode(&mut vm, &instr),
            Err(ExecError::JournalModeChangeDuringTransaction)
        ));
    }

    #[test]
    fn no_writable_db_is_a_no_op() {
        let mut vm = VmType::new();
        let instr = Instruction::new(Opcode::SetJournalMode, JOURNAL_MODE_WAL, 0, 0);
        assert_eq!(set_journal_mode(&mut vm, &instr).unwrap(), Step::Next);
    }

    #[test]
    fn synchronous_defaults_to_full_on_query() {
        let mut vm = writable_vm();
        let instr = Instruction::new(Opcode::Synchronous, SYNCHRONOUS_QUERY, 0, 0);
        assert_eq!(synchronous(&mut vm, &instr).unwrap(), Step::Next);
        assert_eq!(vm.rows().to_vec(), vec![vec![Value::Integer(2)]]);
    }

    #[test]
    fn synchronous_set_then_query_round_trips() {
        let mut vm = writable_vm();
        let set_instr = Instruction::new(Opcode::Synchronous, SYNCHRONOUS_OFF, 0, 0);
        assert_eq!(synchronous(&mut vm, &set_instr).unwrap(), Step::Next);
        let writer = vm.db().unwrap().writer.clone().unwrap();
        assert_eq!(writer.borrow().synchronous(), SynchronousMode::Off);

        let query_instr = Instruction::new(Opcode::Synchronous, SYNCHRONOUS_QUERY, 0, 0);
        assert_eq!(synchronous(&mut vm, &query_instr).unwrap(), Step::Next);
        assert_eq!(vm.rows().to_vec(), vec![vec![Value::Integer(0)]]);
    }

    #[test]
    fn synchronous_normal_sets_level_one() {
        let mut vm = writable_vm();
        let instr = Instruction::new(Opcode::Synchronous, SYNCHRONOUS_NORMAL, 0, 0);
        assert_eq!(synchronous(&mut vm, &instr).unwrap(), Step::Next);
        let writer = vm.db().unwrap().writer.clone().unwrap();
        assert_eq!(writer.borrow().synchronous(), SynchronousMode::Normal);
    }

    #[test]
    fn synchronous_query_with_no_writable_db_reports_default_full() {
        let mut vm = VmType::new();
        let instr = Instruction::new(Opcode::Synchronous, SYNCHRONOUS_QUERY, 0, 0);
        assert_eq!(synchronous(&mut vm, &instr).unwrap(), Step::Next);
        assert_eq!(vm.rows().to_vec(), vec![vec![Value::Integer(2)]]);
    }

    #[test]
    fn synchronous_set_with_no_writable_db_is_a_no_op() {
        let mut vm = VmType::new();
        let instr = Instruction::new(Opcode::Synchronous, SYNCHRONOUS_OFF, 0, 0);
        assert_eq!(synchronous(&mut vm, &instr).unwrap(), Step::Next);
    }
}
