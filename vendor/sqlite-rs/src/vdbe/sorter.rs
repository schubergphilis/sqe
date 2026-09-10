// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Sorter opcodes (spec 009, Requirement 9): ORDER BY as its own
//! in-memory opcode family, distinct from cursor scanning. Every
//! candidate row is buffered via `SorterInsert` during an initial scan
//! pass; `SorterSort`/`Sort` then sorts the buffer once, keyed by the
//! `P4` sort-key descriptor (`SortKeyColumn`s: per-column direction and
//! collation, applied to the record's leading columns — the row's sort
//! key is expected to be encoded first in the `MakeRecord`'d payload
//! `SorterInsert` receives, matching the oracle's own emission shape);
//! `SorterNext`/`SorterData` then iterate the sorted result exactly as
//! `Next`/`Column` iterate a table cursor, feeding an `OpenPseudo`
//! cursor (`src/vdbe/cursor.rs`) so downstream `Column` opcodes need no
//! special case for sorter-sourced rows.
//!
//! Register/cursor-slot conventions this ticket chose (see
//! `src/vdbe/cursor.rs`'s module doc for the same caveat: these are not
//! claimed to match codegen's, #91, eventual harvested operand layout):
//! - `SorterOpen(p1=cursor, p2=bound register [only if p5!=0], p4=SortKey(columns), p5=1 if bounded)`
//! - `SorterInsert(p1=cursor, p2=register holding the record blob,
//!   p3=first register of the run that record was built from, p5=1 if
//!   p3 names source registers, 0 to decode the key from the blob as
//!   before)` — the `p3`/`p5` pairing mirrors `HashAggFind`'s own
//!   `P3`: when the key columns' source registers are still live
//!   (codegen allocates `MakeRecord`'s destination register *after*
//!   that run, so they always are, for every emitter that opts in),
//!   reading them straight from registers avoids decoding the very
//!   blob this same instruction was just handed purely to recover
//!   values already in hand — see [`sorter_key_values`]. `p5=0`
//!   (every emitter that hasn't opted in yet) keeps the original
//!   decode-from-blob behavior, so this is purely additive.
//! - `SorterSort`/`Sort(p1=cursor, p2=jump target if the sorter is
//!   empty)` — mirrors `Rewind`'s "jump on empty" shape.
//! - `SorterNext(p1=cursor, p2=jump target if another row was found)` —
//!   mirrors `Next`'s "jump on success" shape.
//! - `SorterData(p1=cursor, p2=dest register)` — writes the current
//!   sorted row's raw record bytes (a `Value::Blob`) into the register,
//!   the same register `OpenPseudo`'s `P2` names.
//!
//! `SorterOpen`'s optional bound (#129): when `P5` is nonzero, `P2`
//! names a register holding the maximum number of rows the sorter ever
//! needs to keep — codegen computes it via `OffsetLimit` as `LIMIT +
//! max(OFFSET, 0)`, using that opcode's `-1`-means-unbounded convention
//! (`LIMIT -1`/no `LIMIT`) to fall back to the old unbounded behavior
//! whenever no bound is known. `SorterInsert` then maintains a bounded
//! top-K set instead of an ever-growing buffer: once at capacity, an
//! incoming row only replaces the worst-currently-kept row (by the sort
//! key) if it sorts ahead of it, and is discarded otherwise. This is
//! never a lossy approximation — the discarded rows are provably outside
//! the first `bound` positions of the final sorted output — it just
//! avoids buffering (and later sorting) rows `LIMIT` will never reach.

use std::cmp::Ordering;

#[cfg(test)]
use crate::record::decode_record;
use crate::record::{decode_record_only_into, TextEncoding, Value};
use crate::vdbe::compare::compare;
use crate::vdbe::cursor::CursorSlot;
use crate::vdbe::exec::{to_pc, ExecError, Step, Vm};
use crate::vdbe::program::{Instruction, SortKeyColumn, P4};

/// A sorter cursor's state: the sort-key descriptor, the buffered rows
/// (unsorted until `SorterSort` runs) as raw record bytes paired with
/// their already-decoded values, the current iteration position once
/// sorted, and an optional top-K bound (#129) — `None` means the
/// traditional unbounded buffer. Decoding once at insert time (rather
/// than decoding again for every top-K eviction comparison, or in a
/// separate full pass at `SorterSort` time) avoids O(N) redundant
/// decode work; keeping the bounded buffer heap-ordered (see
/// `sorter_insert`) is what keeps eviction itself at O(log bound) per
/// insert rather than O(bound) — the latter turned out to *regress*
/// performance versus the old unbounded sort whenever `bound` exceeds
/// `log2(row count)` (e.g. `bound=100` vs `log2(830_000)≈20`).
#[derive(Debug)]
pub(crate) struct SorterState {
    keys: Vec<SortKeyColumn>,
    buffer: Vec<SorterRow>,
    sorted: bool,
    pos: usize,
    bound: Option<usize>,
    /// `keys`' column indices, computed once at `SorterOpen` (#507,
    /// refined #631) — `SorterInsert` decodes only these specific
    /// columns per row, since `compare_rows` never reads any other.
    /// Deliberately not just "decode the first N columns" (#507's
    /// original scheme): a key can sit past other columns a row
    /// carries (e.g. a GROUP BY key that isn't `schema`'s first
    /// column), and decoding every column up to it just to reach one
    /// late index defeats the point of skipping columns at all.
    key_indices: Vec<usize>,
    /// Scratch record-header buffer reused by every `SorterInsert`'s key
    /// decode (#631) — passing this into
    /// `decode_record_only_into`/`parse_header_into` instead of letting
    /// each call allocate its own `Vec` avoids an allocation per row.
    header_entries: Vec<(u64, usize)>,
}

// Methods rather than free functions so the borrow of `self` elides: a free
// `fn(vm: &Vm, opcode: &'static str) -> Result<&SorterState, _>` has two input
// lifetime positions and so needs an explicit parameter, which is outside the
// qualified subset (`make check-mvl-limit`). The `&self` elision rule resolves it.
impl Vm {
    fn sorter_mut(
        &mut self,
        slot: i32,
        opcode: &'static str,
    ) -> Result<&mut SorterState, ExecError> {
        match self.cursor_mut(slot)? {
            CursorSlot::Sorter(state) => Ok(state),
            other => Err(ExecError::CursorTypeMismatch {
                opcode,
                slot,
                found: other.type_name(),
                expected: "sorter cursor",
            }),
        }
    }

    fn sorter_ref(&self, slot: i32, opcode: &'static str) -> Result<&SorterState, ExecError> {
        match self.cursor(slot)? {
            CursorSlot::Sorter(state) => Ok(state),
            other => Err(ExecError::CursorTypeMismatch {
                opcode,
                slot,
                found: other.type_name(),
                expected: "sorter cursor",
            }),
        }
    }
}

/// This row's key values read straight from `p3 + <column index>` — the
/// register run `p2`'s record was just `MakeRecord`'d from — instead of
/// decoding them back out of that same record. Both `state` (for the
/// key descriptor) and `vm.register` need only a shared borrow, so this
/// runs entirely before `SorterInsert`'s own exclusive borrow, exactly
/// as `crate::vdbe::hash_agg::hash_agg_find`'s equivalent phase does.
fn sorter_key_values_from_registers(
    vm: &Vm,
    p1: i32,
    p3: i32,
    opcode: &'static str,
) -> Result<Vec<Value>, ExecError> {
    let state = vm.sorter_ref(p1, opcode)?;
    let mut values = Vec::with_capacity(state.keys.len());
    for key in &state.keys {
        let offset = i32::try_from(key.index).map_err(|_| ExecError::RegisterRangeTooLarge {
            opcode,
            count: i32::MAX,
        })?;
        let reg = p3
            .checked_add(offset)
            .ok_or(ExecError::RegisterOutOfRange { opcode, index: p3 })?;
        values.push(vm.register(reg)?.clone());
    }
    Ok(values)
}

/// `SorterOpen`: opens an empty sorter, keyed by `P4`'s sort-key
/// descriptor, into cursor slot `P1`. A nonzero `P5` reads `P2` as this
/// sorter's top-K bound (#129): a negative value (`OffsetLimit`'s
/// unbounded sentinel) leaves it unbounded, same as `P5 == 0`.
pub fn sorter_open(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let keys = match &instr.p4 {
        P4::SortKey(keys) => keys.clone(),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "SorterOpen",
                reason: format!("expected a SortKey P4, got {other:?}"),
            })
        }
    };
    let bound = if instr.p5 == 0 {
        None
    } else {
        match vm.register(instr.p2)? {
            Value::Integer(n) if *n >= 0 => usize::try_from(*n).ok(),
            Value::Integer(_) => None,
            other => {
                return Err(ExecError::MalformedInstruction {
                    opcode: "SorterOpen",
                    reason: format!("expected an Integer bound, got {other:?}"),
                })
            }
        }
    };
    let key_indices: Vec<usize> = keys.iter().map(|k| k.index).collect();
    vm.set_cursor(
        instr.p1,
        CursorSlot::Sorter(SorterState {
            keys,
            buffer: Vec::new(),
            sorted: false,
            pos: 0,
            bound,
            key_indices,
            header_entries: Vec::new(),
        }),
    )?;
    Ok(Step::Next)
}

/// `SorterInsert`: buffers register `P2`'s record blob as one candidate
/// row on sorter `P1`. Invalidates any prior sort — the buffer is
/// re-sorted the next time `SorterSort`/`Sort` runs.
///
/// When the sorter has a top-K bound (#129) and is already at capacity,
/// this instead keeps only the better of the incoming row and the
/// worst-currently-kept row (by the sort key), discarding the other —
/// provably safe since a row that loses this comparison can never land
/// within the first `bound` positions of the final sorted output. The
/// buffer is kept as a binary max-heap (ordered so the root is always
/// the worst-kept row) specifically so this is an O(log bound)
/// operation per insert, not O(bound): a linear worst-row scan was
/// tried first and made things *slower* than the old unbounded sort
/// whenever `bound` exceeds `log2(row count)` (100 vs ~20 for an
/// 830K-row table) — more total comparisons, not fewer.
pub fn sorter_insert(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let blob = match vm.register(instr.p2)? {
        Value::Blob(b) => b.clone(),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "SorterInsert",
                reason: format!("expected a record Blob, got {other:?}"),
            })
        }
    };
    // When `p5` says `p3` names this record's own source-register run,
    // read the key columns straight out of those registers (shared
    // borrow, so this runs before `sorter_mut`'s exclusive one below) —
    // encoding into `blob` and then decoding straight back out of it
    // purely to recover values still sitting in registers is exactly
    // the overhead `HashAggFind`'s own `P3` exists to avoid; see this
    // module's doc.
    let precomputed_values = if instr.p5 == 0 {
        None
    } else {
        Some(sorter_key_values_from_registers(
            vm,
            instr.p1,
            instr.p3,
            "SorterInsert",
        )?)
    };
    let state = vm.sorter_mut(instr.p1, "SorterInsert")?;
    state.sorted = false;
    match state.bound {
        Some(0) => {}
        Some(n) if state.buffer.len() >= n => {
            let new_values = match precomputed_values {
                Some(values) => values,
                None => decode_bytes_upto(
                    "SorterInsert",
                    &blob,
                    &state.key_indices,
                    &mut state.header_entries,
                )?,
            };
            let is_better = state.buffer.first().is_some_and(|(_, worst)| {
                compare_rows(&new_values, worst, &state.keys) == Ordering::Less
            });
            if is_better {
                if let Some(slot) = state.buffer.first_mut() {
                    *slot = (blob, new_values);
                }
                heap_sift_down(&mut state.buffer, 0, &state.keys);
            }
        }
        _ => {
            let values = match precomputed_values {
                Some(values) => values,
                None => decode_bytes_upto(
                    "SorterInsert",
                    &blob,
                    &state.key_indices,
                    &mut state.header_entries,
                )?,
            };
            state.buffer.push((blob, values));
            if state.bound.is_some() {
                let last = state.buffer.len().saturating_sub(1);
                heap_sift_up(&mut state.buffer, last, &state.keys);
            }
        }
    }
    Ok(Step::Next)
}

type SorterRow = (std::sync::Arc<[u8]>, Vec<Value>);

/// Restores the max-heap property (root = worst row, per `compare_rows`)
/// after appending a new element at `buf`'s end — bubbles it up while
/// it outranks its parent. `buf`'s heap-ness is only ever needed
/// transiently during a bounded sorter's buffering phase (#129);
/// `SorterSort` re-sorts the (now small, bounded) buffer from scratch
/// afterward, so no ordering guarantee needs to survive past eviction.
fn heap_sift_up(buf: &mut [SorterRow], mut i: usize, keys: &[SortKeyColumn]) {
    while i > 0 {
        let parent = i.saturating_sub(1) / 2;
        let should_swap = match (buf.get(i), buf.get(parent)) {
            (Some((_, a)), Some((_, b))) => compare_rows(a, b, keys) == Ordering::Greater,
            _ => false,
        };
        if !should_swap {
            break;
        }
        buf.swap(i, parent);
        i = parent;
    }
}

/// Restores the max-heap property starting from `i` (normally the root)
/// after its value changed — bubbles the new value down past whichever
/// child now outranks it.
fn heap_sift_down(buf: &mut [SorterRow], mut i: usize, keys: &[SortKeyColumn]) {
    let len = buf.len();
    loop {
        let left = i.saturating_mul(2).saturating_add(1);
        let right = i.saturating_mul(2).saturating_add(2);
        let mut largest = i;
        if left < len
            && matches!(
                (buf.get(left), buf.get(largest)),
                (Some((_, a)), Some((_, b))) if compare_rows(a, b, keys) == Ordering::Greater
            )
        {
            largest = left;
        }
        if right < len
            && matches!(
                (buf.get(right), buf.get(largest)),
                (Some((_, a)), Some((_, b))) if compare_rows(a, b, keys) == Ordering::Greater
            )
        {
            largest = right;
        }
        if largest == i {
            break;
        }
        buf.swap(i, largest);
        i = largest;
    }
}

/// Only decodes the row's leading
/// `max_columns` columns (#507) — `SorterInsert`'s comparisons never
/// look past the sort key's highest column index, so decoding the rest
/// of a wide row on every insert is wasted work. The raw bytes (not this
/// partial `Vec<Value>`) are what `SorterData` ultimately hands back to
/// the query, so skipping trailing columns here changes no observable
/// output — only which columns are available for comparison, which is
/// exactly `max_columns`' job to bound correctly.
fn decode_bytes_upto(
    opcode: &'static str,
    bytes: &[u8],
    wanted: &[usize],
    header_entries: &mut Vec<(u64, usize)>,
) -> Result<Vec<Value>, ExecError> {
    decode_record_only_into(bytes, wanted, TextEncoding::Utf8, header_entries).map_err(|e| {
        ExecError::MalformedInstruction {
            opcode,
            reason: e.to_string(),
        }
    })
}

/// The sort order two already-decoded rows fall in per `keys`' per-column
/// direction/collation/NULLS placement — shared by `SorterSort`'s full
/// sort and `SorterInsert`'s bounded top-K eviction (#129), so both see
/// the exact same ordering. `a`/`b` hold exactly `keys.len()` values,
/// one per key in the same order (#631's `decode_record_only_into`
/// contract) — addressed by position here, not by `key.index` (the
/// key's original schema-column index, no longer `a`/`b`'s own layout
/// now that they're key-only rather than full-row-width).
fn compare_rows(a: &[Value], b: &[Value], keys: &[SortKeyColumn]) -> Ordering {
    for (pos, key) in keys.iter().enumerate() {
        let av = a.get(pos).unwrap_or(&Value::Null);
        let bv = b.get(pos).unwrap_or(&Value::Null);
        let ord = match (av, bv) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => {
                if key.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (_, Value::Null) => {
                if key.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            _ => {
                let ord = compare(av, bv, key.collation);
                if key.descending {
                    ord.reverse()
                } else {
                    ord
                }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// `SorterSort`/`Sort`: sorts sorter `P1`'s buffered rows in place by
/// its key descriptor's per-column direction and collation, delegating
/// the actual value comparison to the kernel (spec 009 Requirement 5).
/// Every row's values were already decoded at insert time, so this is
/// a pure comparison sort — no decoding here. Jumps to `P2` if the
/// sorter is empty (mirrors `Rewind`).
pub fn sorter_sort(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let state = vm.sorter_mut(instr.p1, "SorterSort")?;
    let keys = &state.keys;
    state
        .buffer
        .sort_by(|(_, a), (_, b)| compare_rows(a, b, keys));
    state.sorted = true;
    state.pos = 0;
    Ok(if state.buffer.is_empty() {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// `SorterNext`: advances sorter `P1` to its next sorted row, jumping to
/// `P2` (typically back to the loop body's start) if another row
/// remains — falls through once exhausted, mirroring `Next`.
pub fn sorter_next(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let state = vm.sorter_mut(instr.p1, "SorterNext")?;
    state.pos = state.pos.saturating_add(1);
    Ok(if state.pos < state.buffer.len() {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// `SorterData`: writes sorter `P1`'s current sorted row's raw record
/// bytes into register `P2` — paired with an `OpenPseudo` cursor whose
/// `P2` names the same register, so `Column` can read the row without a
/// sorter-specific case.
pub fn sorter_data(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let bytes = {
        let state = vm.sorter_ref(instr.p1, "SorterData")?;
        if !state.sorted {
            return Err(ExecError::MalformedInstruction {
                opcode: "SorterData",
                reason: "sorter has not been sorted yet (SorterSort/Sort must run first)"
                    .to_string(),
            });
        }
        state
            .buffer
            .get(state.pos)
            .ok_or(ExecError::MalformedInstruction {
                opcode: "SorterData",
                reason: "sorter cursor has no current row".to_string(),
            })?
            .0
            .clone()
    };
    vm.set_register(instr.p2, Value::Blob(bytes))?;
    Ok(Step::Next)
}

/// Small helper trait so `sorter_sort`'s comparator can read "the value
/// at this key's column index, or NULL if the row is shorter than the
/// key descriptor implies" without a separate free function shadowing
/// `Vec<Value>::get`.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::record::encode_record;
    use crate::record::Collation;
    use crate::vdbe::program::Opcode;

    fn insert_row(vm: &mut Vm, cursor: i32, values: &[Value]) {
        let blob = encode_record(values, TextEncoding::Utf8);
        vm.set_register(0, Value::Blob(blob.into())).unwrap();
        sorter_insert(vm, &Instruction::new(Opcode::SorterInsert, cursor, 0, 0)).unwrap();
    }

    fn open_sorter(vm: &mut Vm, cursor: i32, keys: Vec<SortKeyColumn>) {
        sorter_open(
            vm,
            &Instruction::with_p4(Opcode::SorterOpen, cursor, 0, 0, P4::SortKey(keys)),
        )
        .unwrap();
    }

    /// Opens a bounded (top-K) sorter: `bound` is placed in register `1`
    /// and named via `SorterOpen`'s `P2`/`P5` bound convention (#129).
    fn open_bounded_sorter(vm: &mut Vm, cursor: i32, keys: Vec<SortKeyColumn>, bound: i64) {
        vm.set_register(1, Value::Integer(bound)).unwrap();
        let mut instr = Instruction::with_p4(Opcode::SorterOpen, cursor, 1, 0, P4::SortKey(keys));
        instr.p5 = 1;
        sorter_open(vm, &instr).unwrap();
    }

    fn sorted_all(vm: &mut Vm, cursor: i32) -> Vec<Value> {
        if sorter_sort(vm, &Instruction::new(Opcode::SorterSort, cursor, 999, 0)).unwrap()
            == Step::Jump(999)
        {
            return Vec::new();
        }
        let mut seen = Vec::new();
        loop {
            sorter_data(vm, &Instruction::new(Opcode::SorterData, cursor, 5, 0)).unwrap();
            let Value::Blob(bytes) = vm.register(5).unwrap() else {
                panic!("expected a Blob");
            };
            let row = decode_record(bytes, TextEncoding::Utf8).unwrap();
            seen.push(row[0].clone());
            match sorter_next(vm, &Instruction::new(Opcode::SorterNext, cursor, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }
        seen
    }

    #[test]
    fn order_by_buffers_all_rows_before_sorting() {
        let mut vm = Vm::new();
        open_sorter(
            &mut vm,
            0,
            vec![SortKeyColumn {
                index: 0,
                descending: false,
                collation: Collation::Binary,
                nulls_first: false,
            }],
        );
        insert_row(&mut vm, 0, &[Value::Integer(30)]);
        insert_row(&mut vm, 0, &[Value::Integer(10)]);
        insert_row(&mut vm, 0, &[Value::Integer(20)]);

        let step = sorter_sort(&mut vm, &Instruction::new(Opcode::SorterSort, 0, 999, 0)).unwrap();
        assert_eq!(step, Step::Next);

        let mut seen = Vec::new();
        loop {
            sorter_data(&mut vm, &Instruction::new(Opcode::SorterData, 0, 5, 0)).unwrap();
            let Value::Blob(bytes) = vm.register(5).unwrap() else {
                panic!("expected a Blob");
            };
            let row = decode_record(bytes, TextEncoding::Utf8).unwrap();
            seen.push(row[0].clone());

            match sorter_next(&mut vm, &Instruction::new(Opcode::SorterNext, 0, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }

        assert_eq!(
            seen,
            vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)]
        );
    }

    #[test]
    fn sort_key_descriptor_drives_multi_column_order() {
        let mut vm = Vm::new();
        // 2 keys: first descending, second ascending — mirrors the
        // harvested `"k(2,-B,B)"` shape.
        open_sorter(
            &mut vm,
            0,
            vec![
                SortKeyColumn {
                    index: 0,
                    descending: true,
                    collation: Collation::Binary,
                    nulls_first: false,
                },
                SortKeyColumn {
                    index: 1,
                    descending: false,
                    collation: Collation::Binary,
                    nulls_first: false,
                },
            ],
        );
        insert_row(&mut vm, 0, &[Value::Integer(1), Value::Integer(2)]);
        insert_row(&mut vm, 0, &[Value::Integer(2), Value::Integer(1)]);
        insert_row(&mut vm, 0, &[Value::Integer(1), Value::Integer(1)]);

        sorter_sort(&mut vm, &Instruction::new(Opcode::SorterSort, 0, 999, 0)).unwrap();

        let mut seen = Vec::new();
        loop {
            sorter_data(&mut vm, &Instruction::new(Opcode::SorterData, 0, 5, 0)).unwrap();
            let Value::Blob(bytes) = vm.register(5).unwrap() else {
                panic!("expected a Blob");
            };
            seen.push(decode_record(bytes, TextEncoding::Utf8).unwrap());
            match sorter_next(&mut vm, &Instruction::new(Opcode::SorterNext, 0, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }

        assert_eq!(
            seen,
            vec![
                vec![Value::Integer(2), Value::Integer(1)],
                vec![Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(1), Value::Integer(2)],
            ]
        );
    }

    #[test]
    fn empty_sorter_jumps_past_the_loop() {
        let mut vm = Vm::new();
        open_sorter(
            &mut vm,
            0,
            vec![SortKeyColumn {
                index: 0,
                descending: false,
                collation: Collation::Binary,
                nulls_first: false,
            }],
        );
        let step = sorter_sort(&mut vm, &Instruction::new(Opcode::SorterSort, 0, 42, 0)).unwrap();
        assert_eq!(step, Step::Jump(42));
    }

    #[test]
    fn sort_alias_behaves_identically_to_sorter_sort() {
        let mut vm = Vm::new();
        open_sorter(
            &mut vm,
            0,
            vec![SortKeyColumn {
                index: 0,
                descending: false,
                collation: Collation::Binary,
                nulls_first: false,
            }],
        );
        insert_row(&mut vm, 0, &[Value::Integer(5)]);
        // Dispatched from exec.rs as `SorterSort | Sort => sorter_sort`;
        // this test exercises the same function `Sort` maps to directly.
        let step = sorter_sort(&mut vm, &Instruction::new(Opcode::Sort, 0, 999, 0)).unwrap();
        assert_eq!(step, Step::Next);
    }

    fn sorted_ints(nulls_first: bool, descending: bool) -> Vec<Value> {
        let mut vm = Vm::new();
        open_sorter(
            &mut vm,
            0,
            vec![SortKeyColumn {
                index: 0,
                descending,
                collation: Collation::Binary,
                nulls_first,
            }],
        );
        insert_row(&mut vm, 0, &[Value::Integer(5)]);
        insert_row(&mut vm, 0, &[Value::Null]);
        insert_row(&mut vm, 0, &[Value::Integer(-7)]);
        insert_row(&mut vm, 0, &[Value::Integer(0)]);
        insert_row(&mut vm, 0, &[Value::Integer(5)]);

        sorter_sort(&mut vm, &Instruction::new(Opcode::SorterSort, 0, 999, 0)).unwrap();

        let mut seen = Vec::new();
        loop {
            sorter_data(&mut vm, &Instruction::new(Opcode::SorterData, 0, 5, 0)).unwrap();
            let Value::Blob(bytes) = vm.register(5).unwrap() else {
                panic!("expected a Blob");
            };
            let row = decode_record(bytes, TextEncoding::Utf8).unwrap();
            seen.push(row[0].clone());
            match sorter_next(&mut vm, &Instruction::new(Opcode::SorterNext, 0, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }
        seen
    }

    #[test]
    fn ascending_default_places_nulls_first() {
        assert_eq!(
            sorted_ints(true, false),
            vec![
                Value::Null,
                Value::Integer(-7),
                Value::Integer(0),
                Value::Integer(5),
                Value::Integer(5),
            ]
        );
    }

    #[test]
    fn descending_default_places_nulls_last() {
        assert_eq!(
            sorted_ints(false, true),
            vec![
                Value::Integer(5),
                Value::Integer(5),
                Value::Integer(0),
                Value::Integer(-7),
                Value::Null,
            ]
        );
    }

    #[test]
    fn ascending_with_nulls_last_matches_oracle() {
        // SELECT i FROM t ORDER BY i ASC NULLS LAST (issue #140)
        assert_eq!(
            sorted_ints(false, false),
            vec![
                Value::Integer(-7),
                Value::Integer(0),
                Value::Integer(5),
                Value::Integer(5),
                Value::Null,
            ]
        );
    }

    #[test]
    fn descending_with_nulls_first_matches_oracle() {
        // SELECT i FROM t ORDER BY i DESC NULLS FIRST (issue #140)
        assert_eq!(
            sorted_ints(true, true),
            vec![
                Value::Null,
                Value::Integer(5),
                Value::Integer(5),
                Value::Integer(0),
                Value::Integer(-7),
            ]
        );
    }

    #[test]
    fn bounded_sorter_keeps_only_the_smallest_k_ascending() {
        // ORDER BY i ASC LIMIT 3 over 10..=1 descending inserts — the
        // bound must retain {1,2,3} regardless of insertion order.
        let mut vm = Vm::new();
        open_bounded_sorter(
            &mut vm,
            0,
            vec![SortKeyColumn {
                index: 0,
                descending: false,
                collation: Collation::Binary,
                nulls_first: false,
            }],
            3,
        );
        for i in (1..=10).rev() {
            insert_row(&mut vm, 0, &[Value::Integer(i)]);
        }
        assert_eq!(
            sorted_all(&mut vm, 0),
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
        );
    }

    #[test]
    fn bounded_sorter_keeps_only_the_largest_k_descending() {
        // ORDER BY i DESC LIMIT 3 — mirrors the ascending case but the
        // retained top-K are the largest values instead.
        let mut vm = Vm::new();
        open_bounded_sorter(
            &mut vm,
            0,
            vec![SortKeyColumn {
                index: 0,
                descending: true,
                collation: Collation::Binary,
                nulls_first: false,
            }],
            3,
        );
        for i in 1..=10 {
            insert_row(&mut vm, 0, &[Value::Integer(i)]);
        }
        assert_eq!(
            sorted_all(&mut vm, 0),
            vec![Value::Integer(10), Value::Integer(9), Value::Integer(8)]
        );
    }

    #[test]
    fn sort_key_past_column_zero_with_trailing_payload_columns_still_compares_correctly() {
        // #507/#631: SorterInsert now decodes only the sort key's own
        // column indices (`key_indices`, computed at SorterOpen). Rows
        // here carry a leading payload column (index 0, never read by
        // the comparator), the sort key at index 1, and a trailing
        // payload column (index 2, also never read) — this guards
        // against `key_indices` being miscomputed as "column 0" or "the
        // first N columns" instead of the key's actual index, which
        // would silently compare against the wrong column or panic on
        // an out-of-bounds index.
        let mut vm = Vm::new();
        open_bounded_sorter(
            &mut vm,
            0,
            vec![SortKeyColumn {
                index: 1,
                descending: false,
                collation: Collation::Binary,
                nulls_first: false,
            }],
            2,
        );
        insert_row(
            &mut vm,
            0,
            &[
                Value::Text("payload-a".into()),
                Value::Integer(30),
                Value::Text("trailing-a".into()),
            ],
        );
        insert_row(
            &mut vm,
            0,
            &[
                Value::Text("payload-b".into()),
                Value::Integer(10),
                Value::Text("trailing-b".into()),
            ],
        );
        insert_row(
            &mut vm,
            0,
            &[
                Value::Text("payload-c".into()),
                Value::Integer(20),
                Value::Text("trailing-c".into()),
            ],
        );

        sorter_sort(&mut vm, &Instruction::new(Opcode::SorterSort, 0, 999, 0)).unwrap();
        let mut seen = Vec::new();
        loop {
            sorter_data(&mut vm, &Instruction::new(Opcode::SorterData, 0, 5, 0)).unwrap();
            let Value::Blob(bytes) = vm.register(5).unwrap() else {
                panic!("expected a Blob");
            };
            let row = decode_record(bytes, TextEncoding::Utf8).unwrap();
            seen.push(row);
            match sorter_next(&mut vm, &Instruction::new(Opcode::SorterNext, 0, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }

        // Sort key (index 1) ascending, bound 2: keeps rows 10 and 20,
        // and every column of each surviving row — including the
        // never-compared payload columns — must still round-trip intact
        // from the full raw blob `SorterData` returns.
        assert_eq!(
            seen,
            vec![
                vec![
                    Value::Text("payload-b".into()),
                    Value::Integer(10),
                    Value::Text("trailing-b".into()),
                ],
                vec![
                    Value::Text("payload-c".into()),
                    Value::Integer(20),
                    Value::Text("trailing-c".into()),
                ],
            ]
        );
    }

    #[test]
    fn bounded_sorter_with_fewer_rows_than_bound_keeps_them_all() {
        let mut vm = Vm::new();
        open_bounded_sorter(
            &mut vm,
            0,
            vec![SortKeyColumn {
                index: 0,
                descending: false,
                collation: Collation::Binary,
                nulls_first: false,
            }],
            100,
        );
        insert_row(&mut vm, 0, &[Value::Integer(3)]);
        insert_row(&mut vm, 0, &[Value::Integer(1)]);
        insert_row(&mut vm, 0, &[Value::Integer(2)]);
        assert_eq!(
            sorted_all(&mut vm, 0),
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
        );
    }

    #[test]
    fn bound_of_zero_keeps_nothing() {
        let mut vm = Vm::new();
        open_bounded_sorter(
            &mut vm,
            0,
            vec![SortKeyColumn {
                index: 0,
                descending: false,
                collation: Collation::Binary,
                nulls_first: false,
            }],
            0,
        );
        insert_row(&mut vm, 0, &[Value::Integer(1)]);
        insert_row(&mut vm, 0, &[Value::Integer(2)]);
        assert!(sorted_all(&mut vm, 0).is_empty());
    }

    #[test]
    fn negative_bound_register_means_unbounded_offset_limit_sentinel() {
        // OffsetLimit's own convention (LIMIT -1 / no LIMIT): a negative
        // bound register falls back to the traditional unbounded sorter.
        let mut vm = Vm::new();
        open_bounded_sorter(
            &mut vm,
            0,
            vec![SortKeyColumn {
                index: 0,
                descending: false,
                collation: Collation::Binary,
                nulls_first: false,
            }],
            -1,
        );
        for i in (1..=10).rev() {
            insert_row(&mut vm, 0, &[Value::Integer(i)]);
        }
        assert_eq!(sorted_all(&mut vm, 0).len(), 10);
    }

    #[test]
    fn ties_at_the_bound_boundary_still_produce_k_rows() {
        // Duplicate keys right at the eviction boundary must not cause
        // the bounded sorter to under- or over-count.
        let mut vm = Vm::new();
        open_bounded_sorter(
            &mut vm,
            0,
            vec![SortKeyColumn {
                index: 0,
                descending: false,
                collation: Collation::Binary,
                nulls_first: false,
            }],
            2,
        );
        insert_row(&mut vm, 0, &[Value::Integer(5)]);
        insert_row(&mut vm, 0, &[Value::Integer(5)]);
        insert_row(&mut vm, 0, &[Value::Integer(5)]);
        assert_eq!(
            sorted_all(&mut vm, 0),
            vec![Value::Integer(5), Value::Integer(5)]
        );
    }
}
