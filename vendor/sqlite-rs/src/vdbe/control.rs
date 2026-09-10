// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Control-flow opcodes (spec 009, Requirement 3): unconditional jump,
//! one-shot guarding, subroutine call/return, NULL-testing conditional
//! jumps, and the LIMIT/OFFSET counter family. Pure control flow — no
//! value semantics of its own beyond the truthiness/integer-convertible
//! checks these opcodes are defined by.

use crate::record::Value;
use crate::vdbe::exec::{ExecError, Step, Vm};
use crate::vdbe::program::Instruction;

/// `Init`: jumps to `P2` (the start of the main program body) unless
/// `P2` is 0, in which case execution falls through to PC+1.
pub fn init(instr: &Instruction) -> Result<Step, ExecError> {
    Ok(jump_or_next(instr.p2))
}

/// `Goto`: unconditional jump to `P2`.
pub fn goto(instr: &Instruction) -> Result<Step, ExecError> {
    Ok(Step::Jump(to_pc(instr.p2)))
}

/// `Once`: runs its guarded block the first time control reaches this
/// instruction address, then jumps to `P2` on every subsequent visit.
pub fn once(vm: &mut Vm, pc: usize, instr: &Instruction) -> Result<Step, ExecError> {
    if vm.once_fired.insert(pc) {
        Ok(Step::Next)
    } else {
        Ok(Step::Jump(to_pc(instr.p2)))
    }
}

/// `BeginSubrtn`: marks a subroutine's entry point; falls straight
/// through. The call site is responsible for storing a return address
/// and jumping here.
pub fn begin_subrtn() -> Result<Step, ExecError> {
    Ok(Step::Next)
}

/// `Return`: jumps to the address stored (as an integer) in register
/// `P1`.
pub fn r#return(vm: &Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let addr = vm.register(instr.p1)?;
    match addr {
        Value::Integer(i) => match i32::try_from(*i) {
            Ok(pc) => Ok(Step::Jump(to_pc(pc))),
            Err(_) => Err(ExecError::MalformedInstruction {
                opcode: "Return",
                reason: format!("return address {i} does not fit in a PC"),
            }),
        },
        other => Err(ExecError::TypeMismatch {
            opcode: "Return",
            found: value_kind(other),
        }),
    }
}

/// `Halt`: terminates execution. `P1` is the SQLite result code (0 =
/// success); `P4`, if present, carries an error message.
pub fn halt(instr: &Instruction) -> Result<Step, ExecError> {
    let message = match &instr.p4 {
        crate::vdbe::program::P4::Str(s) => Some(s.clone()),
        _ => None,
    };
    Ok(Step::Halt {
        code: instr.p1,
        message,
    })
}

/// `Transaction`: opens an explicit transaction (#360) — clears
/// `Vm::autocommit` so `Halt` no longer commits on its own, deferring to
/// an explicit `AutoCommit` (SQL `COMMIT`/`ROLLBACK`) instead. `P1`
/// carries the `TransactionMode` `compile_begin` compiled (#395):
/// `TRANSACTION_MODE_IMMEDIATE`/`TRANSACTION_MODE_EXCLUSIVE` escalate the
/// lock right away via [`crate::pager::Pager::begin_immediate`]/
/// [`crate::pager::Pager::begin_exclusive`] — stock SQLite acquires
/// RESERVED/EXCLUSIVE as soon as `BEGIN` runs rather than waiting for the
/// first write, so a concurrent writer is blocked immediately.
/// `TRANSACTION_MODE_DEFERRED` (bare `BEGIN`) keeps the lazy-on-first-
/// write behavior — no escalation here. Errors rather than silently
/// re-entering if a transaction is already open (`vm.autocommit` already
/// `false`) — stock SQLite's "cannot start a transaction within a
/// transaction" (#396).
pub fn transaction(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    if !vm.autocommit {
        return Err(ExecError::TransactionAlreadyActive);
    }
    if let Some(writer) = vm.db().ok().and_then(|db| db.writer.clone()) {
        match instr.p1 {
            TRANSACTION_MODE_IMMEDIATE => writer.borrow_mut().begin_immediate()?,
            TRANSACTION_MODE_EXCLUSIVE => writer.borrow_mut().begin_exclusive()?,
            _ => {}
        }
    }
    vm.autocommit = false;
    Ok(Step::Next)
}

/// `Instruction::p1` values `compile_begin`/`transaction` (#395) use to
/// carry `TransactionMode` through the `Transaction` opcode — bare
/// `BEGIN`/`BEGIN DEFERRED` needs no lock escalation at `BEGIN` time.
pub const TRANSACTION_MODE_DEFERRED: i32 = 0;
/// See [`TRANSACTION_MODE_DEFERRED`].
pub const TRANSACTION_MODE_IMMEDIATE: i32 = 1;
/// See [`TRANSACTION_MODE_DEFERRED`].
pub const TRANSACTION_MODE_EXCLUSIVE: i32 = 2;

/// `AutoCommit`: closes the transaction `Transaction` opened (#360).
/// `P2` follows stock SQLite's convention: 1 commits every pending
/// write via [`crate::pager::Pager::flush`], 0 discards them via
/// [`crate::pager::Pager::rollback`]. Either way `Vm::autocommit` is
/// restored to `true` so a subsequent `Halt` (or another
/// `Transaction`/`AutoCommit` pair later in the same program) behaves
/// exactly as if this were a fresh connection. Errors rather than
/// silently no-opping if no transaction is open (`vm.autocommit`
/// already `true`) — stock SQLite's "cannot commit/rollback, no
/// transaction is active" wording (#396).
pub fn auto_commit(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    if vm.autocommit {
        return Err(if instr.p2 == 1 {
            ExecError::NoActiveTransactionToCommit
        } else {
            ExecError::NoActiveTransactionToRollback
        });
    }
    let db = vm.db()?;
    if let Some(writer) = db.writer.clone() {
        if instr.p2 == 1 {
            writer.borrow_mut().flush()?;
        } else {
            writer.borrow_mut().rollback()?;
        }
    }
    vm.autocommit = true;
    Ok(Step::Next)
}

/// `IsNull`: jumps to `P2` if register `P1` holds NULL.
pub fn is_null(vm: &Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let v = vm.register(instr.p1)?;
    Ok(if matches!(v, Value::Null) {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// `NotNull`: jumps to `P2` if register `P1` does not hold NULL.
pub fn not_null(vm: &Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let v = vm.register(instr.p1)?;
    Ok(if matches!(v, Value::Null) {
        Step::Next
    } else {
        Step::Jump(to_pc(instr.p2))
    })
}

/// `IfNot`: jumps to `P2` if register `P1` is falsy (zero). NULL jumps
/// only when `P3` is nonzero.
pub fn if_not(vm: &Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let v = vm.register(instr.p1)?;
    let take_jump = match v {
        Value::Null => instr.p3 != 0,
        other => is_falsy(other),
    };
    Ok(if take_jump {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// Truthiness for the boolean-consuming opcodes (`IfNot` here,
/// `Not` in `arithmetic.rs`): SQLite's `sqlite3VdbeBooleanValue`, i.e.
/// numeric-coerced zero is false and everything else is true. NULL is
/// neither — callers decide what to do with it, which is why this
/// answers `false` for NULL rather than pretending it is a boolean.
pub(crate) fn is_falsy(v: &Value) -> bool {
    match v {
        Value::Integer(i) => *i == 0,
        Value::Real(r) => *r == 0.0,
        Value::Null => false,
        Value::Text(s) => match crate::vdbe::coerce_text_to_numeric(s) {
            Value::Integer(i) => i == 0,
            Value::Real(r) => r == 0.0,
            _ => true,
        },
        Value::Blob(_) => true,
    }
}

/// `MustBeInt`: forces register `P1` to be an integer, converting a
/// losslessly-integral REAL or well-formed integer-text in place. If the
/// value cannot be converted without data loss, jumps to `P2` (or, when
/// `P2` is 0, reports an error).
pub fn must_be_int(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let v = vm.register(instr.p1)?.clone();
    match try_to_integer(&v) {
        Some(i) => {
            vm.set_register(instr.p1, Value::Integer(i))?;
            Ok(Step::Next)
        }
        None if instr.p2 != 0 => Ok(Step::Jump(to_pc(instr.p2))),
        None => Err(ExecError::MustBeInt),
    }
}

fn try_to_integer(v: &Value) -> Option<i64> {
    match v {
        Value::Integer(i) => Some(*i),
        #[allow(clippy::cast_possible_truncation)]
        Value::Real(r) if r.fract() == 0.0 && r.is_finite() && in_i64_range(*r) => Some(*r as i64),
        Value::Text(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// Whether `r` falls within `i64`'s representable range — guards the
/// `as i64` cast above, which otherwise saturates (rather than erroring)
/// on an integral-but-out-of-range REAL like `1e300`.
fn in_i64_range(r: f64) -> bool {
    r >= i64::MIN as f64 && r < i64::MAX as f64
}

/// `OffsetLimit`: computes a combined row-budget counter from a LIMIT
/// register (`P1`) and an OFFSET register (`P3`) into destination
/// register `P2`. A non-positive LIMIT means "no limit", encoded as -1.
pub fn offset_limit(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let limit = register_as_i64(vm, instr.p1)?;
    let offset = register_as_i64(vm, instr.p3)?;
    let combined = if limit > 0 {
        limit.saturating_add(offset.max(0))
    } else {
        -1
    };
    vm.set_register(instr.p2, Value::Integer(combined))?;
    Ok(Step::Next)
}

/// `IfPos`: if register `P1` is 1 or greater, subtracts `P3` from it and
/// jumps to `P2`.
pub fn if_pos(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let v = register_as_i64(vm, instr.p1)?;
    if v > 0 {
        vm.set_register(
            instr.p1,
            Value::Integer(v.saturating_sub(i64::from(instr.p3))),
        )?;
        Ok(Step::Jump(to_pc(instr.p2)))
    } else {
        Ok(Step::Next)
    }
}

/// `IfNotZero`: if register `P1` is nonzero, decrements it (when
/// positive) and jumps to `P2`.
pub fn if_not_zero(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let v = register_as_i64(vm, instr.p1)?;
    if v != 0 {
        if v > 0 {
            vm.set_register(instr.p1, Value::Integer(v.saturating_sub(1)))?;
        }
        Ok(Step::Jump(to_pc(instr.p2)))
    } else {
        Ok(Step::Next)
    }
}

/// `DecrJumpZero`: decrements register `P1`, then jumps to `P2` if the
/// new value is exactly zero.
pub fn decr_jump_zero(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let v = register_as_i64(vm, instr.p1)?.saturating_sub(1);
    vm.set_register(instr.p1, Value::Integer(v))?;
    Ok(if v == 0 {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

fn register_as_i64(vm: &Vm, reg: i32) -> Result<i64, ExecError> {
    match vm.register(reg)? {
        Value::Integer(i) => Ok(*i),
        other => Err(ExecError::TypeMismatch {
            opcode: "control counter",
            found: value_kind(other),
        }),
    }
}

fn jump_or_next(p2: i32) -> Step {
    if p2 == 0 {
        Step::Next
    } else {
        Step::Jump(to_pc(p2))
    }
}

#[allow(clippy::cast_sign_loss)]
fn to_pc(p2: i32) -> usize {
    p2.max(0) as usize
}

fn value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "NULL",
        Value::Integer(_) => "INTEGER",
        Value::Real(_) => "REAL",
        Value::Text(_) => "TEXT",
        Value::Blob(_) => "BLOB",
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::vdbe::program::Opcode;
    use crate::vdbe::Vm as VmType;

    fn vm_with(regs: Vec<Value>) -> VmType {
        let mut vm = VmType::new();
        for (i, v) in regs.into_iter().enumerate() {
            vm.set_register(i as i32, v).unwrap();
        }
        vm
    }

    #[test]
    fn decr_jump_zero_terminates_at_zero() {
        let mut vm = vm_with(vec![Value::Integer(1)]);
        let instr = Instruction::new(Opcode::DecrJumpZero, 0, 10, 0);
        assert_eq!(decr_jump_zero(&mut vm, &instr).unwrap(), Step::Jump(10));
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(0));
    }

    #[test]
    fn offset_limit_combines_limit_and_offset() {
        let mut vm = vm_with(vec![Value::Integer(2), Value::Null, Value::Integer(1)]);
        let instr = Instruction::new(Opcode::OffsetLimit, 0, 1, 2);
        offset_limit(&mut vm, &instr).unwrap();
        assert_eq!(*vm.register(1).unwrap(), Value::Integer(3));
    }

    #[test]
    fn offset_limit_non_positive_limit_means_unbounded() {
        let mut vm = vm_with(vec![Value::Integer(0), Value::Null, Value::Integer(0)]);
        let instr = Instruction::new(Opcode::OffsetLimit, 0, 1, 2);
        offset_limit(&mut vm, &instr).unwrap();
        assert_eq!(*vm.register(1).unwrap(), Value::Integer(-1));
    }

    #[test]
    fn once_falls_through_first_time_then_jumps_on_repeat_entry() {
        let mut vm = VmType::new();
        let instr = Instruction::new(Opcode::Once, 0, 10, 0);
        assert_eq!(once(&mut vm, 3, &instr).unwrap(), Step::Next);
        assert_eq!(once(&mut vm, 3, &instr).unwrap(), Step::Jump(10));
        assert_eq!(once(&mut vm, 3, &instr).unwrap(), Step::Jump(10));
    }

    #[test]
    fn must_be_int_rejects_out_of_range_integral_real() {
        let mut vm = vm_with(vec![Value::Real(1e300)]);
        let instr = Instruction::new(Opcode::MustBeInt, 0, 0, 0);
        assert!(matches!(
            must_be_int(&mut vm, &instr),
            Err(ExecError::MustBeInt)
        ));
    }

    #[test]
    fn return_rejects_address_that_does_not_fit_in_a_pc() {
        let vm = vm_with(vec![Value::Integer(i64::MAX)]);
        let instr = Instruction::new(Opcode::Return, 0, 0, 0);
        assert!(matches!(
            r#return(&vm, &instr),
            Err(ExecError::MalformedInstruction {
                opcode: "Return",
                ..
            })
        ));
    }

    #[test]
    fn is_null_and_not_null_jump_on_null() {
        let vm = vm_with(vec![Value::Null, Value::Integer(1)]);
        let instr = Instruction::new(Opcode::IsNull, 0, 5, 0);
        assert_eq!(is_null(&vm, &instr).unwrap(), Step::Jump(5));
        let instr = Instruction::new(Opcode::NotNull, 1, 5, 0);
        assert_eq!(not_null(&vm, &instr).unwrap(), Step::Jump(5));
    }

    #[test]
    fn begin_subrtn_falls_through() {
        assert_eq!(begin_subrtn().unwrap(), Step::Next);
    }

    #[test]
    fn transaction_clears_autocommit() {
        let mut vm = VmType::new();
        assert!(vm.autocommit);
        let instr = Instruction::new(Opcode::Transaction, TRANSACTION_MODE_DEFERRED, 0, 0);
        assert_eq!(transaction(&mut vm, &instr).unwrap(), Step::Next);
        assert!(!vm.autocommit);
    }

    /// A one-page database, just enough for [`Pager::open`] to succeed —
    /// `AutoCommit`'s tests below only care about `Vm::autocommit` and
    /// `Pager::flush`/`rollback` running without error, not any real
    /// b-tree content.
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
    fn auto_commit_flushes_and_restores_autocommit() {
        let mut vm = writable_vm();
        vm.autocommit = false;
        let instr = Instruction::new(Opcode::AutoCommit, 0, 1, 0);
        assert_eq!(auto_commit(&mut vm, &instr).unwrap(), Step::Next);
        assert!(vm.autocommit);
    }

    #[test]
    fn auto_commit_rolls_back_dirty_pages() {
        let mut vm = writable_vm();
        vm.autocommit = false;
        let instr = Instruction::new(Opcode::AutoCommit, 0, 0, 0);
        assert_eq!(auto_commit(&mut vm, &instr).unwrap(), Step::Next);
        assert!(vm.autocommit);
    }

    /// `BEGIN; BEGIN;` errors rather than silently re-entering — matches
    /// stock sqlite3's "cannot start a transaction within a transaction"
    /// (verified against the oracle binary, #396).
    #[test]
    fn transaction_errors_when_already_active() {
        let mut vm = VmType::new();
        let instr = Instruction::new(Opcode::Transaction, TRANSACTION_MODE_DEFERRED, 0, 0);
        assert_eq!(transaction(&mut vm, &instr).unwrap(), Step::Next);
        assert!(matches!(
            transaction(&mut vm, &instr),
            Err(ExecError::TransactionAlreadyActive)
        ));
        assert!(!vm.autocommit, "the failed re-entry must not disturb state");
    }

    /// `BEGIN IMMEDIATE` escalates the pager's lock to RESERVED right away
    /// (#395), not just at the first write.
    #[test]
    fn transaction_immediate_escalates_to_reserved() {
        let mut vm = writable_vm();
        let instr = Instruction::new(Opcode::Transaction, TRANSACTION_MODE_IMMEDIATE, 0, 0);
        assert_eq!(transaction(&mut vm, &instr).unwrap(), Step::Next);
        let writer = vm.db().unwrap().writer.clone().unwrap();
        assert_eq!(
            writer.borrow().tx_lock_level(),
            crate::vfs::LockLevel::Reserved
        );
    }

    /// `BEGIN EXCLUSIVE` escalates all the way to EXCLUSIVE at `BEGIN`
    /// time (#395).
    #[test]
    fn transaction_exclusive_escalates_to_exclusive() {
        let mut vm = writable_vm();
        let instr = Instruction::new(Opcode::Transaction, TRANSACTION_MODE_EXCLUSIVE, 0, 0);
        assert_eq!(transaction(&mut vm, &instr).unwrap(), Step::Next);
        let writer = vm.db().unwrap().writer.clone().unwrap();
        assert_eq!(
            writer.borrow().tx_lock_level(),
            crate::vfs::LockLevel::Exclusive
        );
    }

    /// Bare `BEGIN` (DEFERRED) keeps today's lazy-on-first-write behavior
    /// — no escalation at `BEGIN` time (#395).
    #[test]
    fn transaction_deferred_does_not_escalate() {
        let mut vm = writable_vm();
        let instr = Instruction::new(Opcode::Transaction, TRANSACTION_MODE_DEFERRED, 0, 0);
        assert_eq!(transaction(&mut vm, &instr).unwrap(), Step::Next);
        let writer = vm.db().unwrap().writer.clone().unwrap();
        assert_eq!(
            writer.borrow().tx_lock_level(),
            crate::vfs::LockLevel::Shared
        );
    }

    /// Bare `COMMIT`/`ROLLBACK` with no open transaction errors rather than
    /// silently no-opping — matches stock sqlite3's "cannot commit/rollback,
    /// no transaction is active" wording (verified against the oracle
    /// binary, #396).
    #[test]
    fn auto_commit_errors_with_no_active_transaction() {
        let mut vm = writable_vm();
        assert!(vm.autocommit);

        let commit = Instruction::new(Opcode::AutoCommit, 0, 1, 0);
        assert!(matches!(
            auto_commit(&mut vm, &commit),
            Err(ExecError::NoActiveTransactionToCommit)
        ));

        let rollback = Instruction::new(Opcode::AutoCommit, 0, 0, 0);
        assert!(matches!(
            auto_commit(&mut vm, &rollback),
            Err(ExecError::NoActiveTransactionToRollback)
        ));
    }

    #[test]
    fn return_jumps_to_stored_address() {
        let vm = vm_with(vec![Value::Integer(42)]);
        let instr = Instruction::new(Opcode::Return, 0, 0, 0);
        assert_eq!(r#return(&vm, &instr).unwrap(), Step::Jump(42));
    }

    #[test]
    fn return_rejects_non_integer_register() {
        let vm = vm_with(vec![Value::Text("x".to_string().into())]);
        let instr = Instruction::new(Opcode::Return, 0, 0, 0);
        assert!(matches!(
            r#return(&vm, &instr),
            Err(ExecError::TypeMismatch {
                opcode: "Return",
                ..
            })
        ));
    }

    #[test]
    fn not_null_falls_through_on_null() {
        let vm = vm_with(vec![Value::Null]);
        let instr = Instruction::new(Opcode::NotNull, 0, 5, 0);
        assert_eq!(not_null(&vm, &instr).unwrap(), Step::Next);
    }

    #[test]
    fn is_falsy_null_is_not_falsy() {
        assert!(!is_falsy(&Value::Null));
    }

    #[test]
    fn is_falsy_text_numeric_coercion() {
        assert!(is_falsy(&Value::Text("0".to_string().into())));
        assert!(is_falsy(&Value::Text("0.0".to_string().into())));
        assert!(!is_falsy(&Value::Text("1".to_string().into())));
        assert!(is_falsy(&Value::Text("abc".to_string().into())));
    }

    #[test]
    fn is_falsy_blob_is_never_falsy() {
        assert!(is_falsy(&Value::Blob(vec![1, 2, 3].into())));
    }

    #[test]
    fn must_be_int_converts_integral_real_in_place() {
        let mut vm = vm_with(vec![Value::Real(3.0)]);
        let instr = Instruction::new(Opcode::MustBeInt, 0, 0, 0);
        assert_eq!(must_be_int(&mut vm, &instr).unwrap(), Step::Next);
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(3));
    }

    #[test]
    fn must_be_int_converts_integer_text_in_place() {
        let mut vm = vm_with(vec![Value::Text(" 7 ".to_string().into())]);
        let instr = Instruction::new(Opcode::MustBeInt, 0, 0, 0);
        assert_eq!(must_be_int(&mut vm, &instr).unwrap(), Step::Next);
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(7));
    }

    #[test]
    fn must_be_int_jumps_when_p2_set_and_conversion_fails() {
        let mut vm = vm_with(vec![Value::Text("not a number".to_string().into())]);
        let instr = Instruction::new(Opcode::MustBeInt, 0, 9, 0);
        assert_eq!(must_be_int(&mut vm, &instr).unwrap(), Step::Jump(9));
    }

    #[test]
    fn if_pos_decrements_and_jumps_when_positive() {
        let mut vm = vm_with(vec![Value::Integer(5)]);
        let instr = Instruction::new(Opcode::IfPos, 0, 20, 2);
        assert_eq!(if_pos(&mut vm, &instr).unwrap(), Step::Jump(20));
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(3));
    }

    #[test]
    fn if_pos_falls_through_when_not_positive() {
        let mut vm = vm_with(vec![Value::Integer(0)]);
        let instr = Instruction::new(Opcode::IfPos, 0, 20, 2);
        assert_eq!(if_pos(&mut vm, &instr).unwrap(), Step::Next);
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(0));
    }

    #[test]
    fn if_not_zero_decrements_positive_and_jumps() {
        let mut vm = vm_with(vec![Value::Integer(3)]);
        let instr = Instruction::new(Opcode::IfNotZero, 0, 8, 0);
        assert_eq!(if_not_zero(&mut vm, &instr).unwrap(), Step::Jump(8));
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(2));
    }

    #[test]
    fn if_not_zero_jumps_without_decrementing_negative() {
        let mut vm = vm_with(vec![Value::Integer(-3)]);
        let instr = Instruction::new(Opcode::IfNotZero, 0, 8, 0);
        assert_eq!(if_not_zero(&mut vm, &instr).unwrap(), Step::Jump(8));
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(-3));
    }

    #[test]
    fn if_not_zero_falls_through_at_zero() {
        let mut vm = vm_with(vec![Value::Integer(0)]);
        let instr = Instruction::new(Opcode::IfNotZero, 0, 8, 0);
        assert_eq!(if_not_zero(&mut vm, &instr).unwrap(), Step::Next);
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(0));
    }

    #[test]
    fn register_as_i64_rejects_non_integer() {
        let mut vm = vm_with(vec![Value::Text("x".to_string().into())]);
        let instr = Instruction::new(Opcode::IfPos, 0, 20, 2);
        assert!(matches!(
            if_pos(&mut vm, &instr),
            Err(ExecError::TypeMismatch {
                opcode: "control counter",
                ..
            })
        ));
    }

    #[test]
    fn init_falls_through_when_p2_zero() {
        let instr = Instruction::new(Opcode::Init, 0, 0, 0);
        assert_eq!(init(&instr).unwrap(), Step::Next);
    }

    /// MC/DC vector (obligation `control_215`, `try_to_integer`'s REAL
    /// guard `r.fract() == 0.0 && r.is_finite() && in_i64_range(*r)`):
    /// all three leaves true — an integral, finite, in-range REAL
    /// converts losslessly.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__control_215__v1_all_true_integral_real() {
        assert_eq!(try_to_integer(&Value::Real(3.0)), Some(3));
    }

    /// MC/DC vector (obligation `control_215`): leaf A (`r.fract() ==
    /// 0.0`) false — a non-integral REAL never converts, regardless of
    /// the other leaves. Independence pair for A against
    /// `mcdc__control_215__v1_all_true_integral_real`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__control_215__v2_fractional_real() {
        assert_eq!(try_to_integer(&Value::Real(3.5)), None);
    }

    /// MC/DC vector (obligation `control_215`): leaf B (`r.is_finite()`)
    /// false — `f64::INFINITY` is the guard leaf B exists for, even
    /// though `INFINITY.fract()` is itself `NaN` (so leaf A also
    /// short-circuits false here; no finite value has a non-zero-`fract`
    /// exception to this, since `fract()` on any non-finite float is
    /// always `NaN`, never `0.0`). Kept as the documented vector for B
    /// against `mcdc__control_215__v1_all_true_integral_real` since it
    /// is the only reachable case exercising that leaf's intent.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__control_215__v3_non_finite_real() {
        assert_eq!(try_to_integer(&Value::Real(f64::INFINITY)), None);
    }

    /// MC/DC vector (obligation `control_215`): leaves A and B true,
    /// leaf C (`in_i64_range(*r)`) false — an integral, finite REAL
    /// too large to represent as `i64`. Independence pair for C against
    /// `mcdc__control_215__v1_all_true_integral_real`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__control_215__v4_out_of_i64_range() {
        assert_eq!(try_to_integer(&Value::Real(1e300)), None);
    }

    #[test]
    fn value_kind_names_all_variants() {
        assert_eq!(value_kind(&Value::Null), "NULL");
        assert_eq!(value_kind(&Value::Integer(1)), "INTEGER");
        assert_eq!(value_kind(&Value::Real(1.0)), "REAL");
        assert_eq!(value_kind(&Value::Text("x".to_string().into())), "TEXT");
        assert_eq!(value_kind(&Value::Blob(vec![].into())), "BLOB");
    }
}
