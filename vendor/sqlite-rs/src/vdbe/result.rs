// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Result-row opcodes (spec 009, Requirement 8): literal loading
//! (`Integer`, `Int64`, `Real`, `Blob`, `Null`, `String8`, `Variable`),
//! record serialization (`MakeRecord`, reusing spec 003's on-disk record
//! encoding byte-for-byte), and row emission (`ResultRow`).

use std::sync::Arc;

use crate::record::{encode_record_into, TextEncoding, Value};
use crate::vdbe::affinity::{apply_affinity, Affinity};
use crate::vdbe::exec::{ExecError, Step, Vm};
use crate::vdbe::program::{Instruction, P4};

/// `Integer`: loads the `i64` constant `P1` into register `P2`.
pub fn integer(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    vm.set_register(instr.p2, Value::Integer(i64::from(instr.p1)))?;
    Ok(Step::Next)
}

/// `Int64` (#142): loads the `i64` constant carried in `P4` into
/// register `P2` — the 64-bit counterpart to `Integer`, whose `P1`
/// operand is `i32`-only and cannot hold a literal outside that range.
pub fn int64(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let i = match &instr.p4 {
        P4::Int(i) => *i,
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "Int64",
                reason: format!("expected an integer P4, got {other:?}"),
            })
        }
    };
    vm.set_register(instr.p2, Value::Integer(i))?;
    Ok(Step::Next)
}

/// `Real` (#142): loads the `f64` constant carried in `P4` into
/// register `P2` — a real literal loaded as an actual `Value::Real`,
/// not `String8` text relying on coercion at comparison/arithmetic time.
pub fn real(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let r = match &instr.p4 {
        P4::Real(r) => *r,
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "Real",
                reason: format!("expected a real P4, got {other:?}"),
            })
        }
    };
    vm.set_register(instr.p2, Value::Real(r))?;
    Ok(Step::Next)
}

/// `Blob` (#142): loads the byte-string constant carried in `P4` into
/// register `P2` as an actual `Value::Blob` — a blob literal, not
/// `String8` hex text relying on coercion that never actually happens
/// (BLOB affinity never converts text to blob, matching SQLite).
pub fn blob(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let bytes = match &instr.p4 {
        P4::Blob(bytes) => bytes.clone(),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "Blob",
                reason: format!("expected a blob P4, got {other:?}"),
            })
        }
    };
    vm.set_register(instr.p2, Value::Blob(bytes.into()))?;
    Ok(Step::Next)
}

/// `Null`: writes NULL into the register range `P2..=P3` (just `P2`
/// when `P3` is 0 or below `P2`). The only opcode that puts a NULL
/// into a register on purpose — without it, codegen's only NULL source
/// was "a register nobody ever wrote", which cannot express a NULL
/// that has to overwrite a live value (a three-valued comparison
/// result, a CASE with no matching branch).
pub fn null(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let last = instr.p3.max(instr.p2);
    for reg in instr.p2..=last {
        vm.set_register(reg, Value::Null)?;
    }
    Ok(Step::Next)
}

/// `Variable`: loads bound parameter `P1` (1-based, matching SQLite's
/// `sqlite3_bind_*` indexing) into register `P2`. An unbound parameter
/// (index past the end of `Vm`'s bound-value list, or no values bound
/// at all) reads as NULL — the same "unwritten register reads as NULL"
/// rule the rest of the VM follows, rather than an error.
pub fn variable(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let value = vm.param(instr.p1).cloned().unwrap_or(Value::Null);
    vm.set_register(instr.p2, value)?;
    Ok(Step::Next)
}

/// `String8`: loads the UTF-8 string constant in `P4` into register
/// `P2`.
pub fn string8(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let s = match &instr.p4 {
        P4::Str(s) => s.clone(),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "String8",
                reason: format!("expected a string P4, got {other:?}"),
            })
        }
    };
    vm.set_register(instr.p2, Value::Text(s.into()))?;
    Ok(Step::Next)
}

/// `Copy`: copies register `P1`'s value into `P2` verbatim (#208 — see
/// `Opcode::Copy`'s own doc for why `INSERT ... SELECT` needs this: a
/// `SELECT`-projected row's registers aren't already the fresh,
/// contiguous run `MakeRecord` needs once reordered/subset into the
/// target table's schema-column order).
pub fn copy(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let value = vm.register(instr.p1)?.clone();
    vm.set_register(instr.p2, value)?;
    Ok(Step::Next)
}

/// `MakeRecord`: packs the contiguous register range `[P1, P1+P2)` into
/// spec 003's record format, writing the encoded bytes (as a `Value::
/// Blob`) to register `P3`.
///
/// Affinity (#194): when `P4` is a [`P4::Affinity`] byte string, each
/// byte (SQLite's own `Affinity::to_p4_byte`/`from_p4_byte` convention)
/// is applied — via [`apply_affinity`] — to a *copy* of the
/// corresponding source register before encoding, one byte per column
/// in order; a byte string shorter than the register range leaves the
/// remaining trailing columns un-coerced (BLOB affinity's no-op, same
/// as an absent P4 leaves every column un-coerced). This mirrors
/// SQLite's own `P4_KEYINFO`/affinity-string convention without a
/// dedicated `KeyInfo` struct. Any other `P4` (including `P4::None`) is
/// the pre-#194 behavior: no affinity coercion at all.
pub fn make_record(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let count = Vm::bounded_count("MakeRecord", instr.p2)?;
    let affinities: &[u8] = match &instr.p4 {
        P4::Affinity(bytes) => bytes,
        _ => &[],
    };
    let mut values = std::mem::take(vm.make_record_values_scratch());
    values.clear();
    for i in 0..count {
        let reg = instr
            .p1
            .checked_add(i as i32)
            .ok_or(ExecError::RegisterOutOfRange {
                opcode: "MakeRecord",
                index: instr.p1,
            })?;
        let mut value = vm.register(reg)?.clone();
        if let Some(byte) = affinities.get(i) {
            apply_affinity(&mut value, Affinity::from_p4_byte(*byte));
        }
        values.push(value);
    }
    let mut scratch = std::mem::take(vm.record_scratch());
    let mut encode_scratch = std::mem::take(vm.encode_scratch());
    encode_record_into(
        &values,
        TextEncoding::Utf8,
        &mut scratch,
        &mut encode_scratch,
    );
    let payload: Arc<[u8]> = Arc::from(scratch.as_slice());
    *vm.record_scratch() = scratch;
    *vm.encode_scratch() = encode_scratch;
    *vm.make_record_values_scratch() = values;
    vm.set_register(instr.p3, Value::Blob(payload))?;
    Ok(Step::Next)
}

/// `ResultRow`: yields the contiguous register range `[P1, P1+P2)` as
/// one output row to the statement's caller.
///
/// Takes each register's value (leaving `Value::Null` behind) instead of
/// cloning it — safe because every scan loop reloads its projected
/// registers (via `Column`/`Rowid`/literal-load opcodes) before the next
/// `ResultRow` reads them again. (#465 added a `row_scratch` buffer meant
/// to amortize this `Vec`'s allocation across calls, but nothing ever
/// returned a drained row's buffer to it — every call still allocated a
/// fresh `Vec` while also paying the `mem::take` overhead. #591 removed
/// the dead field; this allocates directly.)
pub fn result_row(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let count = Vm::bounded_count("ResultRow", instr.p2)?;
    let mut row = Vec::with_capacity(count);
    for i in 0..count {
        let reg = instr
            .p1
            .checked_add(i as i32)
            .ok_or(ExecError::RegisterOutOfRange {
                opcode: "ResultRow",
                index: instr.p1,
            })?;
        row.push(vm.take_register(reg)?);
    }
    vm.emit_row(row);
    Ok(Step::Next)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::vdbe::program::Opcode;

    #[test]
    fn integer_and_string8_load_literals() {
        let mut vm = Vm::new();
        integer(&mut vm, &Instruction::new(Opcode::Integer, 42, 0, 0)).unwrap();
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(42));

        let instr = Instruction::with_p4(Opcode::String8, 0, 1, 0, P4::Str("hello".to_string()));
        string8(&mut vm, &instr).unwrap();
        assert_eq!(
            *vm.register(1).unwrap(),
            Value::Text("hello".to_string().into())
        );
    }

    #[test]
    fn int64_real_and_blob_load_typed_literals() {
        // #142: the harvested `Int64`/`Real`/`Blob` opcodes load an
        // actual typed `Value`, not `String8` text relying on coercion.
        let mut vm = Vm::new();
        let instr =
            Instruction::with_p4(Opcode::Int64, 0, 0, 0, P4::Int(9_223_372_036_854_775_807));
        int64(&mut vm, &instr).unwrap();
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(i64::MAX));

        let instr = Instruction::with_p4(Opcode::Real, 0, 1, 0, P4::Real(1.5));
        real(&mut vm, &instr).unwrap();
        assert_eq!(*vm.register(1).unwrap(), Value::Real(1.5));

        let instr = Instruction::with_p4(Opcode::Blob, 3, 2, 0, P4::Blob(vec![0x41, 0x42, 0x43]));
        blob(&mut vm, &instr).unwrap();
        assert_eq!(
            *vm.register(2).unwrap(),
            Value::Blob(vec![0x41, 0x42, 0x43].into())
        );
    }

    #[test]
    fn null_overwrites_a_live_register_and_spans_p2_to_p3() {
        let mut vm = Vm::new();
        for r in 0..3 {
            vm.set_register(r, Value::Integer(9)).unwrap();
        }
        // P3 = 0 means "just P2", not "the range 1..=0".
        null(&mut vm, &Instruction::new(Opcode::Null, 0, 1, 0)).unwrap();
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(9));
        assert_eq!(*vm.register(1).unwrap(), Value::Null);
        assert_eq!(*vm.register(2).unwrap(), Value::Integer(9));

        null(&mut vm, &Instruction::new(Opcode::Null, 0, 0, 2)).unwrap();
        for r in 0..3 {
            assert_eq!(*vm.register(r).unwrap(), Value::Null);
        }
    }

    #[test]
    fn result_row_emits_fixed_register_range() {
        let mut vm = Vm::new();
        vm.set_register(0, Value::Integer(1)).unwrap();
        vm.set_register(1, Value::Integer(2)).unwrap();
        result_row(&mut vm, &Instruction::new(Opcode::ResultRow, 0, 2, 0)).unwrap();
        assert_eq!(vm.rows(), &[vec![Value::Integer(1), Value::Integer(2)]]);
    }

    #[test]
    fn result_row_takes_registers_leaving_null_behind() {
        // #465: ResultRow moves each register's value into the emitted
        // row instead of cloning it, so the source register reads back
        // as Null afterwards — safe because scan loops always reload
        // their projected registers before the next ResultRow.
        let mut vm = Vm::new();
        vm.set_register(0, Value::Text("hello".to_string().into()))
            .unwrap();
        vm.set_register(1, Value::Integer(2)).unwrap();
        result_row(&mut vm, &Instruction::new(Opcode::ResultRow, 0, 2, 0)).unwrap();
        assert_eq!(
            vm.rows(),
            &[vec![
                Value::Text("hello".to_string().into()),
                Value::Integer(2)
            ]]
        );
        assert_eq!(*vm.register(0).unwrap(), Value::Null);
        assert_eq!(*vm.register(1).unwrap(), Value::Null);
    }

    #[test]
    fn result_row_accumulates_across_calls() {
        // #591: successive ResultRow calls each produce an independent
        // row without leaking or corrupting state from prior calls.
        let mut vm = Vm::new();
        for round in 0..3i64 {
            vm.set_register(0, Value::Integer(round)).unwrap();
            result_row(&mut vm, &Instruction::new(Opcode::ResultRow, 0, 1, 0)).unwrap();
        }
        assert_eq!(
            vm.rows(),
            &[
                vec![Value::Integer(0)],
                vec![Value::Integer(1)],
                vec![Value::Integer(2)],
            ]
        );
    }

    #[test]
    fn copy_duplicates_a_registers_value_leaving_the_source_untouched() {
        let mut vm = Vm::new();
        vm.set_register(0, Value::Text("src".to_string().into()))
            .unwrap();
        vm.set_register(1, Value::Integer(7)).unwrap();
        copy(&mut vm, &Instruction::new(Opcode::Copy, 0, 1, 0)).unwrap();
        assert_eq!(
            *vm.register(1).unwrap(),
            Value::Text("src".to_string().into())
        );
        assert_eq!(
            *vm.register(0).unwrap(),
            Value::Text("src".to_string().into())
        );
    }

    #[test]
    fn make_record_output_matches_spec_003_encoding() {
        let mut vm = Vm::new();
        vm.set_register(0, Value::Integer(42)).unwrap();
        vm.set_register(1, Value::Text("abc".to_string().into()))
            .unwrap();
        make_record(&mut vm, &Instruction::new(Opcode::MakeRecord, 0, 2, 2)).unwrap();
        let Value::Blob(payload) = vm.register(2).unwrap() else {
            panic!("expected a Blob");
        };
        let decoded = crate::record::decode_record(payload, TextEncoding::Utf8).unwrap();
        assert_eq!(
            decoded,
            vec![Value::Integer(42), Value::Text("abc".to_string().into())]
        );
    }

    #[test]
    fn make_record_applies_p4_affinity_before_encoding() {
        // #194: a text-typed register holding a numeric-looking literal
        // is coerced to INTEGER by MakeRecord's P4 affinity string
        // before encoding — mirrors an `INSERT INTO t(i) VALUES ('42')`
        // against an INTEGER column, where codegen's compiled affinity
        // string, not the literal's own type, decides the stored type.
        let mut vm = Vm::new();
        vm.set_register(0, Value::Text("42".to_string().into()))
            .unwrap();
        vm.set_register(1, Value::Text("abc".to_string().into()))
            .unwrap();
        make_record(
            &mut vm,
            &Instruction::with_p4(
                Opcode::MakeRecord,
                0,
                2,
                2,
                P4::Affinity(vec![b'D', b'B']), // INTEGER, TEXT
            ),
        )
        .unwrap();
        let Value::Blob(payload) = vm.register(2).unwrap() else {
            panic!("expected a Blob");
        };
        let decoded = crate::record::decode_record(payload, TextEncoding::Utf8).unwrap();
        assert_eq!(
            decoded,
            vec![Value::Integer(42), Value::Text("abc".to_string().into())]
        );
        // The source registers are untouched — affinity applies to a
        // copy, not the live register (mirrors the compare opcodes'
        // same rule, `exec.rs`'s `compare_jump`).
        assert_eq!(
            *vm.register(0).unwrap(),
            Value::Text("42".to_string().into())
        );
    }

    #[test]
    fn make_record_without_affinity_p4_is_unchanged_from_pre_194_behavior() {
        let mut vm = Vm::new();
        vm.set_register(0, Value::Text("42".to_string().into()))
            .unwrap();
        make_record(&mut vm, &Instruction::new(Opcode::MakeRecord, 0, 1, 1)).unwrap();
        let Value::Blob(payload) = vm.register(1).unwrap() else {
            panic!("expected a Blob");
        };
        let decoded = crate::record::decode_record(payload, TextEncoding::Utf8).unwrap();
        assert_eq!(decoded, vec![Value::Text("42".to_string().into())]);
    }
}
