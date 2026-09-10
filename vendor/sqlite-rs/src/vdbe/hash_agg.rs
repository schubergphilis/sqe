// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Hash-based `GROUP BY` aggregation (spec 009, Requirement 13, #570):
//! the O(n) alternative to `crate::vdbe::sorter`'s sort-then-group
//! strategy, which pays O(n log n) to make a group's rows adjacent
//! before folding them.
//!
//! Shaped as the same five-opcode open/insert/rewind/data/next family
//! the sorter uses, so the two strategies stay directly comparable:
//! - `HashAggOpen(p1=cursor, p4=GroupKey(columns))`
//! - `HashAggFind(p1=cursor, p2=register holding the row's record blob,
//!   p3=first register of the run that record was built from)` —
//!   locates (creating on first sight) that row's group and makes it
//!   the current one. The key is read straight out of the source
//!   registers (`p3 + column index`, the same indices `P4::GroupKey`
//!   names into the record) rather than by decoding `p2`'s blob back
//!   again: the blob is only ever *stored*, for the one row per group
//!   that is retained.
//! - `HashAggStep(p1=accumulator slot, p2=first argument register,
//!   p3=cursor, p4=AggFunc{..})` — the per-group counterpart of
//!   `AggStep`, folding into the current group rather than into the
//!   VM-wide `agg_contexts` table.
//! - `HashAggRewind(p1=cursor, p2=jump target if no group exists)` —
//!   mirrors `SorterSort`'s "jump on empty" shape.
//! - `HashAggData(p1=cursor, p2=dest register)` — mirrors `SorterData`,
//!   and additionally *installs* the current group's accumulators into
//!   `agg_contexts` so the shared `flush_group` codegen's `AggFinal`
//!   opcodes read them with no hash-specific case at all.
//! - `HashAggNext(p1=cursor, p2=jump target if a group remains)` —
//!   mirrors `SorterNext`.
//!
//! Two deliberate design points, both about *not* diverging from the
//! sort strategy's observable behavior:
//!
//! 1. **Group order.** `HashAggRewind` sorts the groups by group key
//!    before iterating, rather than handing back hash or insertion
//!    order. SQLite does not guarantee `GROUP BY` output order, but the
//!    sort strategy's happens to be key order and every existing test
//!    (and oracle diff) is written against it. Sorting K *groups* is
//!    O(K log K), not the O(n log n) sort of all n *rows* this replaces
//!    — for the low/medium-cardinality queries this path targets, K is
//!    a small fraction of n, so the asymptotic win survives intact
//!    while the output stays byte-identical.
//! 2. **Key equality.** [`push_key_bytes`] canonicalizes each key
//!    value so that two values hash to the same bucket exactly when
//!    `crate::vdbe::compare::compare` calls them equal under that
//!    column's collation, after that column's comparison affinity has
//!    been applied — the same collation and affinity the sort
//!    strategy's group-boundary `Eq` carries in its `P4::CollSeq`.
//!    Getting this wrong in the *coarse* direction merges distinct
//!    groups; in the *fine* direction it splits one group in two. The
//!    subtle case is SQLite's merged numeric class: `1` and `1.0`
//!    compare equal and so must land in the same bucket (see
//!    `exact_integer_real`). The one acknowledged gap is NaN, which
//!    `compare` reports as equal to itself but which canonicalizes here
//!    by bit pattern — SQLite cannot store a NaN (it becomes NULL on
//!    insert), so no query can reach it.

use std::collections::HashMap;
use std::sync::Arc;

use crate::record::{Collation, Value};
use crate::vdbe::affinity::{apply_affinity, Affinity};
use crate::vdbe::aggregate::AggState;
use crate::vdbe::compare::compare;
use crate::vdbe::cursor::CursorSlot;
use crate::vdbe::exec::{to_pc, ExecError, Step, Vm};
use crate::vdbe::program::{GroupKeyColumn, Instruction, P4};

/// One group's accumulated state: the row retained to answer plain
/// (non-aggregate) column references, that row's already-decoded and
/// affinity-applied key values (kept for `HashAggRewind`'s ordering
/// pass, so it never re-decodes), and one accumulator per aggregate
/// slot.
#[derive(Debug)]
struct GroupSlot {
    /// The `MakeRecord`-encoded *first* row seen for this group, handed
    /// back verbatim by `HashAggData`. First rather than last on
    /// purpose: SQLite's "arbitrary row" semantics for a plain column
    /// observably pick the group's first row, and the sort strategy —
    /// whose sort is stable, so a group's ties stay in scan order —
    /// picks the same one.
    row: Arc<[u8]>,
    /// This group's key values in key order (not record order),
    /// post-affinity, used only to order the groups at `HashAggRewind`.
    key_values: Vec<Value>,
    /// One accumulator per aggregate slot, indexed by `HashAggStep`'s
    /// `P1`. `None` for a slot this group never stepped, which
    /// `HashAggData` translates back into a cleared context slot so
    /// `AggFinal` produces its zero-row result.
    accumulators: Vec<Option<AggState>>,
}

/// A hash-aggregation cursor's state: the group-key descriptor, the
/// groups themselves, the key-bytes-to-group index, and the iteration
/// position `HashAggRewind`/`HashAggNext` maintain once frozen.
///
/// `HashMap` rather than `BTreeMap`, for the same reason
/// [`crate::vdbe::cursor::AutoIndexState`] chose one: the build phase
/// only ever does exact-key lookups, so there is nothing to give up by
/// trading O(log n) per row for O(1) amortized — and that trade is
/// precisely what makes this O(n) rather than another n-log-n plan.
/// The one place order *is* needed (iteration) gets it from `order`,
/// computed once at `HashAggRewind` over K groups instead of n rows.
#[derive(Debug)]
pub(crate) struct HashAggState {
    keys: Vec<GroupKeyColumn>,
    /// Groups in creation order; `index` and `order` both address this
    /// by position, so a group's identity is stable for the cursor's
    /// whole lifetime.
    groups: Vec<GroupSlot>,
    index: HashMap<Vec<u8>, usize>,
    /// The group `HashAggFind` most recently located, which the
    /// `HashAggStep`s that follow fold into. `None` before the first
    /// find.
    current: Option<usize>,
    /// Group positions in output order, filled in by `HashAggRewind`.
    order: Vec<usize>,
    /// Iteration position within `order`.
    pos: usize,
    frozen: bool,
    /// Scratch buffer for `HashAggFind`'s per-row key-source values,
    /// reused across rows via take/give-back instead of allocating a
    /// fresh `Vec` each call.
    values_scratch: Vec<Value>,
    /// Scratch buffer for `group_key_into`'s canonical key bytes, reused
    /// the same way. Cloned into `index` only when a row starts a new
    /// group.
    key_bytes_scratch: Vec<u8>,
    /// Scratch buffer for `group_key_into`'s decoded key values, reused
    /// the same way. Cloned into a new `GroupSlot` only when a row
    /// starts a new group.
    key_values_scratch: Vec<Value>,
    /// Scratch buffer for `HashAggStep`'s per-row aggregate arguments,
    /// reused across rows via take/give-back. Never cloned: arguments
    /// are only read by reference into `aggregate::step`.
    args_scratch: Vec<Value>,
}

// Methods rather than free functions so the borrow of `self` elides —
// same rationale as `crate::vdbe::sorter`'s `sorter_mut`/`sorter_ref`.
impl Vm {
    fn hash_agg_mut(
        &mut self,
        slot: i32,
        opcode: &'static str,
    ) -> Result<&mut HashAggState, ExecError> {
        match self.cursor_mut(slot)? {
            CursorSlot::HashAgg(state) => Ok(state),
            other => Err(ExecError::CursorTypeMismatch {
                opcode,
                slot,
                found: other.type_name(),
                expected: "hash-aggregation cursor",
            }),
        }
    }
}

/// Appends `value`'s canonical group-key bytes to `out` under
/// `collation`. Canonical means: two values produce identical byte
/// sequences exactly when [`compare`] calls them equal under the same
/// collation (see this module's doc for the NaN caveat).
///
/// Every variable-length payload is length-prefixed so a multi-column
/// key can never be split ambiguously — without it, keys
/// `('a', 'bc')` and `('ab', 'c')` would collide into one group.
fn push_key_bytes(out: &mut Vec<u8>, value: &Value, collation: Collation) {
    match value {
        Value::Null => out.push(0),
        Value::Integer(i) => {
            out.push(1);
            out.extend_from_slice(&i.to_be_bytes());
        }
        Value::Real(r) => match exact_integer_real(*r) {
            // A REAL that names an integer exactly compares equal to
            // that INTEGER, so it must encode identically to one.
            Some(i) => {
                out.push(1);
                out.extend_from_slice(&i.to_be_bytes());
            }
            None => {
                out.push(2);
                out.extend_from_slice(&r.to_bits().to_be_bytes());
            }
        },
        Value::Text(s) => {
            out.push(3);
            let folded = collate_key(s, collation);
            push_len(out, folded.len());
            out.extend_from_slice(&folded);
        }
        Value::Blob(b) => {
            out.push(4);
            push_len(out, b.len());
            out.extend_from_slice(b);
        }
    }
}

/// A text value's collation-normalized bytes: two strings that
/// [`crate::record::compare_text`] calls equal under `collation`
/// normalize to the same bytes.
fn collate_key(text: &str, collation: Collation) -> Vec<u8> {
    match collation {
        Collation::Binary => text.as_bytes().to_vec(),
        Collation::NoCase => text.as_bytes().iter().map(u8::to_ascii_lowercase).collect(),
        Collation::RTrim => text.trim_end_matches(' ').as_bytes().to_vec(),
    }
}

fn push_len(out: &mut Vec<u8>, len: usize) {
    out.extend_from_slice(&u64::try_from(len).unwrap_or(u64::MAX).to_be_bytes());
}

/// The `i64` a REAL names exactly, or `None` when it names none —
/// mirroring the equality half of `compare_int_real`'s contract
/// (`crate::vdbe::compare`): a REAL outside `i64`'s range, or with a
/// fractional part, can never compare equal to any INTEGER, so it keeps
/// its own encoding. `-0.0` folds to `0`, matching `compare`.
fn exact_integer_real(r: f64) -> Option<i64> {
    if !r.is_finite() || r.trunc() != r {
        return None;
    }
    if !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&r) {
        return None;
    }
    #[allow(clippy::cast_possible_truncation)]
    Some(r as i64)
}

/// This row's group key: the `keys`-named columns of `values`, each
/// with its comparison affinity applied, written into `bytes` (the
/// hash-map key) and `key_values` (kept for `HashAggRewind`'s
/// ordering) — both cleared and reused in place rather than allocated
/// fresh, so a caller can reuse the same pair of buffers across rows.
fn group_key_into(
    values: &[Value],
    keys: &[GroupKeyColumn],
    bytes: &mut Vec<u8>,
    key_values: &mut Vec<Value>,
) {
    bytes.clear();
    key_values.clear();
    for key in keys {
        let mut value = values.get(key.index).cloned().unwrap_or(Value::Null);
        apply_affinity(&mut value, Affinity::from_p4_byte(key.affinity));
        push_key_bytes(bytes, &value, key.collation);
        key_values.push(value);
    }
}

/// Test/non-hot-path convenience wrapper over [`group_key_into`] that
/// allocates fresh buffers — the hot path (`hash_agg_find`) reuses
/// scratch buffers directly instead.
#[cfg(test)]
fn group_key(values: &[Value], keys: &[GroupKeyColumn]) -> (Vec<u8>, Vec<Value>) {
    let mut bytes = Vec::new();
    let mut key_values = Vec::new();
    group_key_into(values, keys, &mut bytes, &mut key_values);
    (bytes, key_values)
}

/// `HashAggOpen`: opens an empty hash-aggregation table keyed by `P4`'s
/// group-key descriptor into cursor slot `P1`.
pub fn hash_agg_open(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let keys = match &instr.p4 {
        P4::GroupKey(keys) => keys.clone(),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "HashAggOpen",
                reason: format!("expected a GroupKey P4, got {other:?}"),
            })
        }
    };
    vm.set_cursor(
        instr.p1,
        CursorSlot::HashAgg(HashAggState {
            keys,
            groups: Vec::new(),
            index: HashMap::new(),
            current: None,
            order: Vec::new(),
            pos: 0,
            frozen: false,
            values_scratch: Vec::new(),
            key_bytes_scratch: Vec::new(),
            key_values_scratch: Vec::new(),
            args_scratch: Vec::new(),
        }),
    )?;
    Ok(Step::Next)
}

/// `HashAggFind`: locates register `P2`'s record's group on cursor `P1`,
/// creating it (retaining this row as the group's representative one)
/// the first time that key is seen, and makes it the group the
/// `HashAggStep`s that follow fold into.
///
/// The key values are read from `P3 + <column index>` — the register
/// run `P2`'s record was just built from — rather than by decoding that
/// record again. Encoding and re-decoding once per row purely to
/// recover values that are still sitting in registers is exactly the
/// overhead this strategy exists to avoid.
pub fn hash_agg_find(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let blob = match vm.register(instr.p2)? {
        Value::Blob(b) => b.clone(),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "HashAggFind",
                reason: format!("expected a record Blob, got {other:?}"),
            })
        }
    };
    // `group_key` addresses `values` by each key's own `index`, so the
    // keys are re-indexed positionally (0, 1, ...) alongside the values
    // gathered in that same order — the record-column index is only
    // used here to find the source register.
    let positional: Vec<GroupKeyColumn> = {
        let state = vm.hash_agg_mut(instr.p1, "HashAggFind")?;
        // A find after the table was frozen would silently break the
        // iteration `order`/`groups` agreement; codegen never emits one,
        // so treat it as the malformed program it would be rather than
        // papering over it.
        if state.frozen {
            return Err(ExecError::MalformedInstruction {
                opcode: "HashAggFind",
                reason: "hash-aggregation cursor was already rewound".to_string(),
            });
        }
        state.keys.clone()
    };
    let mut values = {
        let state = vm.hash_agg_mut(instr.p1, "HashAggFind")?;
        std::mem::take(&mut state.values_scratch)
    };
    values.clear();
    for key in &positional {
        let offset = i32::try_from(key.index).map_err(|_| ExecError::RegisterRangeTooLarge {
            opcode: "HashAggFind",
            count: i32::MAX,
        })?;
        let reg = instr
            .p3
            .checked_add(offset)
            .ok_or(ExecError::RegisterOutOfRange {
                opcode: "HashAggFind",
                index: instr.p3,
            })?;
        values.push(vm.register(reg)?.clone());
    }
    let positional: Vec<GroupKeyColumn> = positional
        .iter()
        .enumerate()
        .map(|(i, k)| GroupKeyColumn { index: i, ..*k })
        .collect();
    let state = vm.hash_agg_mut(instr.p1, "HashAggFind")?;
    let mut bytes = std::mem::take(&mut state.key_bytes_scratch);
    let mut key_values = std::mem::take(&mut state.key_values_scratch);
    group_key_into(&values, &positional, &mut bytes, &mut key_values);
    let position = match state.index.get(&bytes) {
        Some(pos) => *pos,
        None => {
            let pos = state.groups.len();
            state.groups.push(GroupSlot {
                row: blob,
                key_values: key_values.clone(),
                accumulators: Vec::new(),
            });
            state.index.insert(bytes.clone(), pos);
            pos
        }
    };
    state.current = Some(position);
    state.values_scratch = values;
    state.key_bytes_scratch = bytes;
    state.key_values_scratch = key_values;
    Ok(Step::Next)
}

/// `HashAggStep`: folds registers `P2..P2+arity` into accumulator slot
/// `P1` of cursor `P3`'s currently-located group — the per-group
/// counterpart of `AggStep`, delegating to the exact same
/// `crate::vdbe::aggregate::step` kernel so an aggregate cannot mean one
/// thing under the sort strategy and another under this one.
pub fn hash_agg_step(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let (name, arity, collation) = match &instr.p4 {
        P4::AggFunc {
            name,
            arity,
            collation,
        } => (name.clone(), *arity, *collation),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "HashAggStep",
                reason: format!("expected an AggFunc P4, got {other:?}"),
            })
        }
    };
    let mut args = {
        let state = vm.hash_agg_mut(instr.p3, "HashAggStep")?;
        std::mem::take(&mut state.args_scratch)
    };
    args.clear();
    for i in 0..arity {
        let offset = i32::try_from(i).map_err(|_| ExecError::RegisterRangeTooLarge {
            opcode: "HashAggStep",
            count: i32::try_from(arity).unwrap_or(i32::MAX),
        })?;
        let reg = instr
            .p2
            .checked_add(offset)
            .ok_or(ExecError::RegisterOutOfRange {
                opcode: "HashAggStep",
                index: instr.p2,
            })?;
        args.push(vm.register(reg)?.clone());
    }
    let slot = usize::try_from(instr.p1).map_err(|_| ExecError::MalformedInstruction {
        opcode: "HashAggStep",
        reason: format!("negative accumulator slot {}", instr.p1),
    })?;
    let state = vm.hash_agg_mut(instr.p3, "HashAggStep")?;
    let position = state.current.ok_or(ExecError::MalformedInstruction {
        opcode: "HashAggStep",
        reason: "no group located (HashAggFind must run first)".to_string(),
    })?;
    let group = state
        .groups
        .get_mut(position)
        .ok_or(ExecError::MalformedInstruction {
            opcode: "HashAggStep",
            reason: "located group no longer exists".to_string(),
        })?;
    if slot >= group.accumulators.len() {
        group
            .accumulators
            .resize_with(slot.saturating_add(1), || None);
    }
    let current = group.accumulators.get_mut(slot).and_then(Option::take);
    let updated = crate::vdbe::aggregate::step(&name, current, &args, collation).map_err(|e| {
        ExecError::MalformedInstruction {
            opcode: "HashAggStep",
            reason: e.to_string(),
        }
    })?;
    if let Some(cell) = group.accumulators.get_mut(slot) {
        *cell = Some(updated);
    }
    state.args_scratch = args;
    Ok(Step::Next)
}

/// `HashAggRewind`: freezes cursor `P1`, orders its groups by group key
/// (ascending, NULLs first, per each key column's collation — matching
/// the sort strategy's own output order, see this module's doc), and
/// positions it at the first group. Jumps to `P2` when no group was
/// ever created, mirroring `SorterSort`/`Rewind`.
pub fn hash_agg_rewind(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let state = vm.hash_agg_mut(instr.p1, "HashAggRewind")?;
    state.frozen = true;
    state.pos = 0;
    state.order = (0..state.groups.len()).collect();
    let groups = &state.groups;
    let collations: Vec<Collation> = state.keys.iter().map(|k| k.collation).collect();
    state.order.sort_by(|a, b| {
        let left = groups.get(*a).map(|g| g.key_values.as_slice());
        let right = groups.get(*b).map(|g| g.key_values.as_slice());
        compare_key_values(left.unwrap_or(&[]), right.unwrap_or(&[]), &collations)
    });
    Ok(if state.order.is_empty() {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// Orders two groups' key values column by column, ascending with
/// NULLs first — which is exactly [`compare`]'s own total order (NULL
/// ranks below every other storage class), so no separate NULL arm is
/// needed here the way `sorter::compare_rows` needs one for its
/// configurable `nulls_first`/`descending`.
fn compare_key_values(a: &[Value], b: &[Value], collations: &[Collation]) -> std::cmp::Ordering {
    for (i, collation) in collations.iter().enumerate() {
        let av = a.get(i).unwrap_or(&Value::Null);
        let bv = b.get(i).unwrap_or(&Value::Null);
        let ord = compare(av, bv, *collation);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// `HashAggData`: writes cursor `P1`'s current group's retained row into
/// register `P2` — paired with an `OpenPseudo` cursor naming that same
/// register, exactly as `SorterData` is — *and* installs that group's
/// accumulators into the VM's `AggStep`/`AggFinal` context slots, so the
/// shared `flush_group` codegen's `AggFinal` opcodes need no
/// hash-specific case at all.
pub fn hash_agg_data(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let (bytes, accumulators) = {
        let state = vm.hash_agg_mut(instr.p1, "HashAggData")?;
        if !state.frozen {
            return Err(ExecError::MalformedInstruction {
                opcode: "HashAggData",
                reason: "hash-aggregation cursor has not been rewound yet".to_string(),
            });
        }
        let position =
            state
                .order
                .get(state.pos)
                .copied()
                .ok_or(ExecError::MalformedInstruction {
                    opcode: "HashAggData",
                    reason: "hash-aggregation cursor has no current group".to_string(),
                })?;
        let group = state
            .groups
            .get(position)
            .ok_or(ExecError::MalformedInstruction {
                opcode: "HashAggData",
                reason: "current group no longer exists".to_string(),
            })?;
        (group.row.clone(), group.accumulators.clone())
    };
    for (slot, state) in accumulators.into_iter().enumerate() {
        let slot = i32::try_from(slot).map_err(|_| ExecError::RegisterRangeTooLarge {
            opcode: "HashAggData",
            count: i32::MAX,
        })?;
        match state {
            Some(value) => vm.set_agg_context(slot, value)?,
            None => vm.clear_agg_context(slot)?,
        }
    }
    vm.set_register(instr.p2, Value::Blob(bytes))?;
    Ok(Step::Next)
}

/// `HashAggNext`: advances cursor `P1` to its next group, jumping to
/// `P2` (typically back to the loop body's start) if one remains —
/// falls through once exhausted, mirroring `SorterNext`.
pub fn hash_agg_next(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let state = vm.hash_agg_mut(instr.p1, "HashAggNext")?;
    state.pos = state.pos.saturating_add(1);
    Ok(if state.pos < state.order.len() {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::record::{encode_record, TextEncoding};
    use crate::vdbe::program::Opcode;

    fn binary_key(index: usize) -> GroupKeyColumn {
        GroupKeyColumn {
            index,
            collation: Collation::Binary,
            affinity: Affinity::Blob.to_p4_byte(),
        }
    }

    fn key_bytes(value: &Value, collation: Collation) -> Vec<u8> {
        let mut out = Vec::new();
        push_key_bytes(&mut out, value, collation);
        out
    }

    /// The property the whole strategy rests on: canonical key bytes
    /// agree with `compare`'s equality, so hash grouping can never
    /// split (or merge) a group the sort strategy would not.
    #[test]
    fn integer_and_exactly_equal_real_share_one_key() {
        assert_eq!(
            key_bytes(&Value::Integer(1), Collation::Binary),
            key_bytes(&Value::Real(1.0), Collation::Binary)
        );
        assert_eq!(
            key_bytes(&Value::Integer(0), Collation::Binary),
            key_bytes(&Value::Real(-0.0), Collation::Binary)
        );
    }

    #[test]
    fn real_with_a_fraction_never_collides_with_an_integer() {
        assert_ne!(
            key_bytes(&Value::Integer(1), Collation::Binary),
            key_bytes(&Value::Real(1.5), Collation::Binary)
        );
    }

    /// `i64::MAX as f64` rounds up past the exact integer, so `compare`
    /// reports them unequal — the key encoding must agree.
    #[test]
    fn out_of_range_real_never_collides_with_an_integer() {
        assert_ne!(
            key_bytes(&Value::Integer(i64::MAX), Collation::Binary),
            key_bytes(&Value::Real(9_223_372_036_854_775_807.0), Collation::Binary)
        );
    }

    #[test]
    fn nocase_folds_text_keys_but_binary_does_not() {
        assert_eq!(
            key_bytes(&Value::Text("Abc".into()), Collation::NoCase),
            key_bytes(&Value::Text("aBC".into()), Collation::NoCase)
        );
        assert_ne!(
            key_bytes(&Value::Text("Abc".into()), Collation::Binary),
            key_bytes(&Value::Text("aBC".into()), Collation::Binary)
        );
    }

    #[test]
    fn rtrim_ignores_trailing_spaces_in_text_keys() {
        assert_eq!(
            key_bytes(&Value::Text("ab  ".into()), Collation::RTrim),
            key_bytes(&Value::Text("ab".into()), Collation::RTrim)
        );
    }

    /// Without length-prefixing, `('a','bc')` and `('ab','c')` would
    /// encode to the same bytes and merge into one group.
    #[test]
    fn multi_column_text_keys_are_unambiguous() {
        let keys = vec![binary_key(0), binary_key(1)];
        let (left, _) = group_key(&[Value::Text("a".into()), Value::Text("bc".into())], &keys);
        let (right, _) = group_key(&[Value::Text("ab".into()), Value::Text("c".into())], &keys);
        assert_ne!(left, right);
    }

    #[test]
    fn null_and_missing_columns_share_the_null_key() {
        let keys = vec![binary_key(0)];
        let (from_null, _) = group_key(&[Value::Null], &keys);
        let (from_missing, _) = group_key(&[], &keys);
        assert_eq!(from_null, from_missing);
    }

    /// Numeric affinity coerces well-formed numeric text before
    /// hashing, matching the affinity byte the sort strategy puts on
    /// its group-boundary `Eq`.
    #[test]
    fn numeric_affinity_groups_numeric_text_with_its_number() {
        let keys = vec![GroupKeyColumn {
            index: 0,
            collation: Collation::Binary,
            affinity: Affinity::Numeric.to_p4_byte(),
        }];
        let (from_text, _) = group_key(&[Value::Text("5".into())], &keys);
        let (from_int, _) = group_key(&[Value::Integer(5)], &keys);
        assert_eq!(from_text, from_int);
    }

    fn open(vm: &mut Vm, cursor: i32, keys: Vec<GroupKeyColumn>) {
        hash_agg_open(
            vm,
            &Instruction::with_p4(Opcode::HashAggOpen, cursor, 0, 0, P4::GroupKey(keys)),
        )
        .unwrap();
    }

    /// Register layout mirrors codegen's: register 0 holds the
    /// `MakeRecord`'d blob, and the run at `ROW_BASE` holds the same
    /// values it was built from (what `HashAggFind`'s `P3` names).
    const ROW_BASE: i32 = 10;

    fn fold(vm: &mut Vm, cursor: i32, row: &[Value], agg: &str, arg: Option<Value>) {
        let blob = encode_record(row, TextEncoding::Utf8);
        vm.set_register(0, Value::Blob(blob.into())).unwrap();
        for (i, value) in row.iter().enumerate() {
            let reg = ROW_BASE.saturating_add(i32::try_from(i).unwrap());
            vm.set_register(reg, value.clone()).unwrap();
        }
        hash_agg_find(
            vm,
            &Instruction::new(Opcode::HashAggFind, cursor, 0, ROW_BASE),
        )
        .unwrap();
        let arity = usize::from(arg.is_some());
        vm.set_register(1, arg.unwrap_or(Value::Null)).unwrap();
        hash_agg_step(
            vm,
            &Instruction::with_p4(
                Opcode::HashAggStep,
                0,
                1,
                cursor,
                P4::AggFunc {
                    name: agg.to_string(),
                    arity,
                    collation: Collation::Binary,
                },
            ),
        )
        .unwrap();
    }

    /// End to end over the opcode family: three rows across two groups
    /// fold independently, and iteration hands them back in key order
    /// with each group's accumulator installed for `AggFinal`.
    #[test]
    fn groups_fold_independently_and_iterate_in_key_order() {
        let mut vm = Vm::new();
        open(&mut vm, 0, vec![binary_key(0)]);
        fold(
            &mut vm,
            0,
            &[Value::Integer(2)],
            "sum",
            Some(Value::Integer(10)),
        );
        fold(
            &mut vm,
            0,
            &[Value::Integer(1)],
            "sum",
            Some(Value::Integer(3)),
        );
        fold(
            &mut vm,
            0,
            &[Value::Integer(2)],
            "sum",
            Some(Value::Integer(5)),
        );

        let step =
            hash_agg_rewind(&mut vm, &Instruction::new(Opcode::HashAggRewind, 0, 99, 0)).unwrap();
        assert_eq!(step, Step::Next);

        let mut seen = Vec::new();
        loop {
            hash_agg_data(&mut vm, &Instruction::new(Opcode::HashAggData, 0, 5, 0)).unwrap();
            let Value::Blob(bytes) = vm.register(5).unwrap() else {
                panic!("expected a Blob");
            };
            let row = crate::record::decode_record(bytes, TextEncoding::Utf8).unwrap();
            let total =
                crate::vdbe::aggregate::finalize("sum", vm.agg_context(0).unwrap()).unwrap();
            seen.push((row[0].clone(), total));
            match hash_agg_next(&mut vm, &Instruction::new(Opcode::HashAggNext, 0, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }

        assert_eq!(
            seen,
            vec![
                (Value::Integer(1), Value::Integer(3)),
                (Value::Integer(2), Value::Integer(15)),
            ]
        );
    }

    #[test]
    fn an_empty_table_jumps_past_the_loop() {
        let mut vm = Vm::new();
        open(&mut vm, 0, vec![binary_key(0)]);
        let step =
            hash_agg_rewind(&mut vm, &Instruction::new(Opcode::HashAggRewind, 0, 42, 0)).unwrap();
        assert_eq!(step, Step::Jump(42));
    }

    /// The retained row is the group's *first*, matching the sort
    /// strategy's stable-sort "arbitrary row" choice.
    #[test]
    fn the_first_row_of_a_group_is_the_one_retained() {
        let mut vm = Vm::new();
        open(&mut vm, 0, vec![binary_key(0)]);
        fold(
            &mut vm,
            0,
            &[Value::Integer(1), Value::Text("first".into())],
            "count",
            None,
        );
        fold(
            &mut vm,
            0,
            &[Value::Integer(1), Value::Text("second".into())],
            "count",
            None,
        );
        hash_agg_rewind(&mut vm, &Instruction::new(Opcode::HashAggRewind, 0, 99, 0)).unwrap();
        hash_agg_data(&mut vm, &Instruction::new(Opcode::HashAggData, 0, 5, 0)).unwrap();
        let Value::Blob(bytes) = vm.register(5).unwrap() else {
            panic!("expected a Blob");
        };
        let row = crate::record::decode_record(bytes, TextEncoding::Utf8).unwrap();
        assert_eq!(row[1], Value::Text("first".into()));
    }

    #[test]
    fn stepping_before_locating_a_group_is_rejected() {
        let mut vm = Vm::new();
        open(&mut vm, 0, vec![binary_key(0)]);
        let err = hash_agg_step(
            &mut vm,
            &Instruction::with_p4(
                Opcode::HashAggStep,
                0,
                1,
                0,
                P4::AggFunc {
                    name: "count".to_string(),
                    arity: 0,
                    collation: Collation::Binary,
                },
            ),
        );
        assert!(matches!(
            err,
            Err(ExecError::MalformedInstruction {
                opcode: "HashAggStep",
                ..
            })
        ));
    }

    #[test]
    fn reading_data_before_rewinding_is_rejected() {
        let mut vm = Vm::new();
        open(&mut vm, 0, vec![binary_key(0)]);
        fold(&mut vm, 0, &[Value::Integer(1)], "count", None);
        let err = hash_agg_data(&mut vm, &Instruction::new(Opcode::HashAggData, 0, 5, 0));
        assert!(matches!(
            err,
            Err(ExecError::MalformedInstruction {
                opcode: "HashAggData",
                ..
            })
        ));
    }
}
