// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Cursor opcodes (spec 009, Requirement 4): real table cursors over
//! V1's `TableCursor` (`OpenRead`/`Rewind`/`Last`/`Next`/`Column`/
//! `Rowid`/`SeekRowid`/`NullRow`), an in-memory ephemeral index for
//! DISTINCT (`OpenEphemeral`/`Sequence`/`Found`/`IdxInsert`/`IdxLE`/
//! `Delete`, per the epic's #87 scope decision — never the on-disk file
//! format), an in-memory ephemeral **table** (#257, `OpenEphemeral` with
//! `P5` nonzero) that materializes a subquery-in-FROM and is then scanned
//! with the same `Rewind`/`Last`/`Next`/`Column`/`Rowid`/`Insert` opcodes
//! a real table cursor uses, and a single-row pseudo-cursor (`OpenPseudo`)
//! that lets `Column` read an already-computed record (the sorter's
//! output row) without a special case. A real secondary-index read
//! cursor (`OpenRead` with `P5` nonzero) supports both a one-shot point
//! lookup (`SeekIndexEq`/`IdxRowid`, #243) and a full sequential walk
//! (`IdxRewind`/`IdxLast`/`IdxNext`/`IdxPrev`, #296) for an
//! index-ordered `ORDER BY` scan — see [`IndexReadState`]'s doc.
//!
//! Register/cursor-slot conventions used by this module's opcodes (this
//! ticket's own choice — codegen, #91, is what will actually decide
//! operand layout against the pinned oracle's `EXPLAIN` output; nothing
//! here claims byte-for-byte parity with a harvested instruction's
//! P1..P5, only with the opcode's *semantics*):
//! - `OpenRead(p1=cursor, p2=root page)`
//! - `OpenEphemeral(p1=cursor)` — key-column count isn't needed by this
//!   in-memory implementation (the whole register range passed to
//!   `Found`/`IdxInsert` *is* the key), so `P2` is unused here. `P5`
//!   nonzero (#257) opens the table-mode variant instead (see below).
//! - `OpenPseudo(p1=cursor, p2=register holding the row's record blob)`
//! - `Rewind`/`Last(p1=cursor, p2=jump target if the table is empty)`
//! - `Next(p1=cursor, p2=jump target if another row was found)` —
//!   mirrors the oracle's own `OP_Next` shape: jump back into the loop
//!   body on success, fall through to end the loop on exhaustion.
//! - `Column(p1=cursor, p2=column index, p3=dest register)`
//! - `Rowid(p1=cursor, p2=dest register)`
//! - `SeekRowid(p1=cursor, p2=jump target if not found, p3=register
//!   holding the target rowid)`
//! - `NullRow(p1=cursor)`
//! - `Sequence(p1=cursor, p2=dest register)` — also works on a table-mode
//!   ephemeral cursor (#257), handing out fresh rowids starting at `1`.
//! - `Found(p1=cursor, p2=jump target if the key is present, p3=first
//!   key register, p4=Int(key column count))`
//! - `IdxInsert(p1=cursor, p2=first key register, p4=Int(key column
//!   count))`
//! - `IdxLE(p1=cursor, p2=jump target, p3=first key register,
//!   p4=Int(key column count))` — see [`idx_le`]'s doc for this
//!   opcode's known scope limitation.
//! - `Delete(p1=cursor)` — deletes the entry `Found`/`IdxInsert` most
//!   recently probed/inserted on this cursor.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;

use crate::btree::{self, IndexCursor, TableCursor};
use crate::record::{
    decode_record, decode_record_upto, decode_serial_value, encode_record, parse_header_into,
    record_column_count, TextEncoding, Value,
};
use crate::vdbe::exec::{to_pc, ExecError, Step, Vm};
use crate::vdbe::program::{Instruction, OPFLAG_LASTROWID, OPFLAG_NCHANGE, P4};
use crate::vdbe::{compare, Collation};

/// One open cursor slot: a real table cursor, an in-memory ephemeral
/// index, a sorter (state owned by `src/vdbe/sorter.rs`, re-exported
/// here so `Vm`'s single cursor-slot table can hold all cursor kinds),
/// or a single-row pseudo-cursor.
#[derive(Debug)]
pub(crate) enum CursorSlot {
    Table(TableCursorState),
    /// A real index b-tree write cursor (#194) opened by `OpenWrite`
    /// with `P5` nonzero — `root_page` is the index (or WITHOUT ROWID
    /// table) b-tree's root page. Unlike `Table`, this slot carries no
    /// traversal position: `IdxInsert`'s real-cursor path is a
    /// stateless one-shot `insert_entry` call, so there is nothing to
    /// track between opcodes.
    IndexWrite {
        root_page: u32,
    },
    /// A real index b-tree read cursor (#243) opened by `OpenRead` with
    /// `P5` nonzero — the query-time counterpart to `IndexWrite`. Unlike
    /// `IndexWrite`, this slot carries real traversal state (#296
    /// extended it beyond #243's original one-shot `SeekIndexEq` probe
    /// to a full persisted [`IndexCursor`]) — see [`IndexReadState`]'s
    /// own doc.
    IndexRead(IndexReadState),
    Ephemeral(EphemeralState),
    /// An in-memory ephemeral **table** cursor (#257) — opened by
    /// `OpenEphemeral` with `P5` nonzero, unlike the index-mode
    /// [`CursorSlot::Ephemeral`] above (`P5` zero/default). Backs a
    /// materialized subquery-in-FROM: rows are appended via `Insert`
    /// (decoding the `MakeRecord`-encoded payload, same as a real table
    /// cursor) and then scanned with `Rewind`/`Next`/`Column`/`Rowid`
    /// exactly like [`CursorSlot::Table`], just without any on-disk
    /// b-tree backing it.
    EphemeralTable(EphemeralTableState),
    /// A transient automatic index (#545) — opened by `OpenEphemeral`
    /// with `P5 == 2`, a third mode alongside the two above. Backs a
    /// join level's own equality-key-to-rowid multi-map: `AutoIndexInsert`
    /// appends a rowid under its join key, `AutoIndexSeek`/`AutoIndexRowid`/
    /// `AutoIndexNext` then walk every rowid sharing one probed key. See
    /// [`AutoIndexState`]'s own doc for why this is a plain exact-key
    /// multi-map rather than reusing [`CursorSlot::Ephemeral`]'s
    /// single-value-per-key shape or a real index's ordered-seek shape.
    EphemeralAutoIndex(AutoIndexState),
    Pseudo {
        register: i32,
        /// Header cache for the blob currently in `register` (#631) —
        /// a pseudo cursor's row (e.g. a sorter's `SorterData` output)
        /// is typically read via several separate `Column` opcodes
        /// before the register changes again, and without this each of
        /// those calls re-walked the header and re-allocated its own
        /// entries `Vec` from scratch (`decode_column`'s `parse_header`)
        /// — same wasted-reparse shape [`RowHeaderCache`] already fixed
        /// for table cursors. Valid only while `cached_blob` still
        /// points at the same allocation as `register`'s current value.
        header_cache: RowHeaderCache,
        cached_blob: Option<Arc<[u8]>>,
    },
    Sorter(crate::vdbe::sorter::SorterState),
    /// A hash-aggregation table (#570) — opened by `HashAggOpen`, the
    /// O(n) `GROUP BY` strategy alternative to buffering every row in a
    /// [`CursorSlot::Sorter`] and sorting it. See
    /// [`crate::vdbe::hash_agg`]'s module doc for why key equality here
    /// has to agree exactly with the sort strategy's group-boundary
    /// comparison.
    HashAgg(crate::vdbe::hash_agg::HashAggState),
}

impl CursorSlot {
    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            CursorSlot::Table(_) => "table cursor",
            CursorSlot::IndexWrite { .. } => "index write cursor",
            CursorSlot::IndexRead(_) => "index read cursor",
            CursorSlot::Ephemeral(_) => "ephemeral cursor",
            CursorSlot::EphemeralTable(_) => "ephemeral table cursor",
            CursorSlot::EphemeralAutoIndex(_) => "ephemeral automatic-index cursor",
            CursorSlot::Pseudo { .. } => "pseudo cursor",
            CursorSlot::Sorter(_) => "sorter cursor",
            CursorSlot::HashAgg(_) => "hash-aggregation cursor",
        }
    }
}

/// A real cursor over `src/btree`'s table b-tree, plus the traversal
/// state `Column`/`Rowid` read from: the row `Rewind`/`Next`/`Last`/
/// `SeekRowid` most recently positioned on (`None` once exhausted), and
/// whether `NullRow` has forced this slot to read as an all-NULL row
/// (used by e.g. an outer-join-style probe that found no match — `Next`/
/// `Rewind` clear it again on the next real positioning call).
pub(crate) struct TableCursorState {
    cursor: TableCursor<Rc<dyn crate::vfs::PageSource>>,
    /// The rowid of the row `Rewind`/`Next`/`Last`/`Prev`/`SeekRowid` most
    /// recently positioned on, or `None` once exhausted/never positioned.
    /// Deliberately NOT a cached `TableRow` (#473): the payload is
    /// reassembled lazily, on demand, via `self.cursor.current_payload()`
    /// only when `Column` actually reads it — see this struct's own doc
    /// for why a cached row can't be a borrow here, and why nothing
    /// downstream needs one anyway (only "is a row positioned" and the
    /// rowid itself ever survive across opcode dispatches).
    current_rowid: Option<i64>,
    /// Memoizes the first `current_payload()` call for `current_rowid`'s
    /// row — invalidated (never left stale) by [`Self::set_current`],
    /// same lifecycle as `header_cache`. Without this, a multi-column
    /// `Column` read (the common case) called `current_payload()` once
    /// per column instead of once per row — for a 5-column `SELECT` that
    /// meant 5x the `reassemble_payload` work per row, a measured
    /// regression on the `full_scan` bench when #473 first made payload
    /// fetching lazy (caught post-merge, fixed here rather than reverting
    /// the laziness itself).
    cached_payload: Option<btree::Payload>,
    forced_null: bool,
    /// The table b-tree's root page (#194) — recorded so `Insert`/
    /// `Delete`/`NewRowid` know which b-tree to write to without a
    /// separate cursor-slot variant. Populated by both `OpenRead` and
    /// `OpenWrite`; a read-only cursor never uses it (no write opcode
    /// runs against it), so it costs nothing on the read-only path.
    root_page: u32,
    /// `current`'s parsed header (#458), computed lazily by the first
    /// `Column` read of a row and reused by every later `Column` read of
    /// the *same* row — invalidated (never left stale) by
    /// [`Self::set_current`], the only way `current` is ever reassigned.
    /// Never itself replaced with a fresh `RowHeaderCache`, so its
    /// backing `Vec` allocation is reused across every row this cursor
    /// visits rather than allocated and freed per row (see its own doc —
    /// that per-row alloc/free was a measured *regression* on the
    /// `full_scan` bench versus no caching at all before this was
    /// switched from `Option<RowHeaderCache>` to this always-present,
    /// reuse-the-allocation shape).
    header_cache: RowHeaderCache,
}

impl TableCursorState {
    /// The sole setter for `current_rowid` — always paired with
    /// invalidating `header_cache` and `cached_payload`, so neither cache
    /// can ever survive its row.
    fn set_current(&mut self, rowid: Option<i64>) {
        self.current_rowid = rowid;
        self.header_cache.invalidate();
        self.cached_payload = None;
    }

    /// Ensures `cached_payload` holds the reassembled payload for
    /// `current_rowid`'s row, returning whether a row is positioned at
    /// all (`false` if the cursor is exhausted/never positioned). Lazy
    /// (#473): the actual reassembly — including any overflow-chain
    /// walk — happens only on the first call for a given row; every
    /// later call for the *same* row is a no-op, reusing `cached_payload`
    /// instead of redoing the walk (or, for the common local/non-overflow
    /// case, redoing the cheap but still per-call `Rc` clone + range
    /// bookkeeping) once per column read — a regression caught post-merge
    /// (a 5-column `SELECT` was paying for 5x the reassembly work per row
    /// instead of 1x) and fixed by this cache.
    ///
    /// Returns whether positioned rather than the payload itself (an
    /// `Option<&Payload>` return would borrow all of `self` for as long
    /// as the reference lives, conflicting with the caller's next need
    /// for `&mut self.header_cache`) — callers read `self.cached_payload`
    /// directly afterward, a disjoint field access the borrow checker
    /// accepts.
    fn ensure_current_payload(&mut self) -> Result<bool, btree::BtreeError> {
        if self.current_rowid.is_none() {
            return Ok(false);
        }
        if self.cached_payload.is_none() {
            self.cached_payload = Some(self.cursor.current_payload()?);
        }
        Ok(true)
    }
}

impl std::fmt::Debug for TableCursorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableCursorState")
            .field("current_rowid", &self.current_rowid)
            .field("forced_null", &self.forced_null)
            .finish_non_exhaustive()
    }
}

/// A record payload's header, parsed once (#458): each column's serial
/// type paired with the byte offset of its body within the row's
/// payload. Lets repeated `Column` opcodes against the same row look up
/// an offset directly instead of re-walking the header from byte 0 every
/// time.
///
/// `valid` (rather than an `Option<RowHeaderCache>` on the owning cursor
/// state) is deliberate: a fresh `Vec` per row — the first, simpler
/// implementation of this cache — measured *slower* than no cache at all
/// on the `full_scan` bench (#458), because it traded a cheap per-column
/// header re-walk for a per-row heap allocation. Keeping one `RowHeaderCache`
/// (and its `Vec`'s capacity) alive for the cursor's whole lifetime and
/// just marking it stale on `set_current` avoids that churn — `ensure`
/// reuses the existing allocation via `Vec::clear` instead of
/// reallocating.
#[derive(Debug, Default)]
pub(crate) struct RowHeaderCache {
    entries: Vec<(u64, usize)>,
    valid: bool,
}

impl RowHeaderCache {
    fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Parses `payload`'s header into `self.entries` if not already
    /// valid for the current row; a no-op otherwise.
    fn ensure(&mut self, payload: &[u8]) -> Result<(), crate::record::RecordError> {
        if !self.valid {
            parse_header_into(payload, &mut self.entries)?;
            self.valid = true;
        }
        Ok(())
    }

    /// Column count of the header last parsed by `ensure` — callers that
    /// only need the record's trailing column (e.g. an index row's
    /// appended rowid) use this to address it without decoding the rest.
    fn column_count(&self) -> usize {
        debug_assert!(self.valid, "column_count() called before ensure()");
        self.entries.len()
    }

    fn column(
        &self,
        payload: &[u8],
        idx: usize,
        encoding: TextEncoding,
    ) -> Result<Value, crate::record::RecordError> {
        debug_assert!(self.valid, "column() called before ensure()");
        match self.entries.get(idx) {
            Some(&(serial_type, offset)) => {
                decode_serial_value(serial_type, payload, offset, encoding).map(|(v, _)| v)
            }
            None => Ok(Value::Null),
        }
    }
}

/// A real secondary-index b-tree read cursor (#243), extended by #296 to
/// also carry a persistent [`IndexCursor`] traversal position — `current`
/// is the row `SeekIndexEq`/`IdxRewind`/`IdxLast`/`IdxNext`/`IdxPrev` most
/// recently positioned on (`None` before any positioning call, on a
/// `SeekIndexEq` miss, or once a scan is exhausted), the same shape
/// `TableCursorState::current` uses for a table cursor. `IdxRowid` reads
/// the trailing rowid column out of `current`'s decoded key — for an
/// ordinary secondary index that column is always the referenced table's
/// rowid (see [`IndexRow`]'s doc); this cursor is never used against a
/// `WITHOUT ROWID` table's own storage, where that wouldn't hold.
pub(crate) struct IndexReadState {
    root_page: u32,
    cursor: IndexCursor<Rc<dyn crate::vfs::PageSource>>,
    current: Option<crate::btree::IndexRow>,
    /// Same role as [`TableCursorState::header_cache`] (#458): `current`'s
    /// parsed header, invalidated whenever `current` is reassigned.
    header_cache: RowHeaderCache,
}

impl IndexReadState {
    /// The sole setter for `current` — see
    /// [`TableCursorState::set_current`]'s doc.
    fn set_current(&mut self, row: Option<crate::btree::IndexRow>) {
        self.current = row;
        self.header_cache.invalidate();
    }
}

impl std::fmt::Debug for IndexReadState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexReadState")
            .field("root_page", &self.root_page)
            .field("current", &self.current)
            .finish_non_exhaustive()
    }
}

/// The in-memory `BTreeMap` backing DISTINCT's ephemeral index (#87):
/// entries keyed by the encoded record bytes of the probed/inserted
/// column range (spec 003's format, reused byte-for-byte — same encoder
/// `MakeRecord` uses), never touching the on-disk page format.
/// `sequence` is a monotonic counter `Sequence` hands out (independent
/// of the dedup key), and `last_key` records the key `Found`/`IdxInsert`
/// most recently touched, so a following `Delete` (per spec 009 Req 4's
/// "insert then delete the just-produced duplicate" DISTINCT dance)
/// knows which entry to remove without a separate register operand.
/// Row-count ceiling on an in-memory ephemeral table/index (#269): both
/// back a plain `Vec`/`BTreeMap` with no spill-to-disk, unlike real
/// SQLite's temp b-trees, so an unbounded subquery-in-FROM (#257) or a
/// correlated `IN (SELECT ...)` rebuilding its index per outer row
/// (`compile_in_subquery`, `src/codegen/subquery.rs`) could otherwise
/// grow memory without limit. Sized in the same order of magnitude as
/// `crate::btree::MAX_PAGES_VISITED`; not currently configurable, matching
/// this codebase's other hardcoded limits (`MAX_REGISTERS`, `MAX_STEPS`).
#[cfg(not(test))]
pub(crate) const MAX_EPHEMERAL_ROWS: usize = 1_000_000;
/// Kept small under test so the limit-exceeded regression tests don't have
/// to insert a million rows to exercise the check.
#[cfg(test)]
pub(crate) const MAX_EPHEMERAL_ROWS: usize = 8;

#[derive(Debug, Default)]
pub(crate) struct EphemeralState {
    entries: BTreeMap<Vec<u8>, Vec<Value>>,
    sequence: i64,
    last_key: Option<Vec<u8>>,
}

/// Backing store for [`CursorSlot::EphemeralAutoIndex`] (#545): a plain
/// exact-key multi-map (encoded key -> every rowid inserted under it),
/// deliberately *not* [`EphemeralState`]'s `BTreeMap<key, Vec<Value>>`
/// shape (one value per key — a later `IdxInsert` of a duplicate key
/// there silently overwrites the earlier one, correct for DISTINCT's
/// dedup but wrong for a join index, where every duplicate-key row must
/// still be found) and *not* a real index's ordered/byte-comparable
/// b-tree seek shape either (`SeekIndexEq` + walk-while-still-equal
/// `IdxNext`, #450) — a join level only ever asks "which rows share
/// this exact key", never a range, so there's no need to reproduce a
/// real index's ordering semantics or byte-order key encoding at all.
/// #547: `HashMap` rather than a `BTreeMap` for exactly that reason —
/// no caller of this map ever walks it in key order (only exact-key
/// `get`/`entry`), so there's nothing to give up by trading the
/// `BTreeMap`'s O(log n) per op for `HashMap`'s O(1) amortized, which
/// is what actually makes this a hash join (build once, probe each
/// outer row in O(1)) rather than build-once-probe-with-a-binary-search.
#[derive(Debug, Default)]
pub(crate) struct AutoIndexState {
    entries: HashMap<Vec<u8>, Vec<i64>>,
    /// The key and within-key position `AutoIndexSeek`/`AutoIndexNext`
    /// most recently positioned on, read by `AutoIndexRowid` — `None`
    /// before any seek, on a miss, or once exhausted.
    current: Option<(Vec<u8>, usize)>,
}

/// One materialized row: its rowid, plus its decoded column values.
type EphemeralRow = (i64, Vec<Value>);

/// Backing store for [`CursorSlot::EphemeralTable`] (#257): rows appended
/// in insertion order, each tagged with the rowid `Insert`'s caller
/// computed (codegen assigns sequential rowids starting at 1 — see
/// `src/codegen/subquery.rs`'s FROM-subquery materialization). `pos` is
/// the row index `Rewind`/`Last`/`Next` most recently positioned on
/// (`None` before any positioning call or once exhausted), mirroring
/// `TableCursorState::current`.
#[derive(Debug)]
pub(crate) struct EphemeralTableState {
    /// `Rc<RefCell<..>>` rather than a plain owned `Vec` (#425): a CTE
    /// referenced more than once in one statement materializes into one
    /// cursor via `OpenEphemeral`+`Insert`, then every later reference
    /// `OpenDup`s a second (third, ...) cursor sharing this same `rows`
    /// — each with its own independent `pos` (scan position), but all
    /// reading the one populated row set. By the time any `OpenDup`
    /// executes, the source cursor's own materialization has already
    /// fully run (codegen always finishes one materialization's
    /// `OpenEphemeral..Insert*` region, reaching its own end label,
    /// before compiling a later reference that might duplicate it), so
    /// there is never a concurrent writer while a dup exists to race —
    /// the `RefCell` is borrow-checked scaffolding for that invariant,
    /// not a signal that concurrent mutation is expected.
    rows: Rc<RefCell<Vec<EphemeralRow>>>,
    pos: Option<usize>,
    /// Monotonic counter `Sequence` hands out (#257) — codegen uses it to
    /// assign each materialized row a fresh rowid before `Insert`,
    /// mirroring how the index-mode [`EphemeralState::sequence`] is used
    /// for DISTINCT. Starts at `1` (rather than `0`, unlike
    /// `EphemeralState::sequence`) to match a real table's first rowid.
    /// Meaningless on an `OpenDup`-ed cursor, which never inserts.
    sequence: i64,
}

impl Default for EphemeralTableState {
    fn default() -> Self {
        Self {
            rows: Rc::new(RefCell::new(Vec::new())),
            pos: None,
            sequence: 1,
        }
    }
}

impl EphemeralTableState {
    /// Borrows the (possibly shared, #425) row set for reading. Only
    /// ever fails if some other in-progress borrow on this exact `Rc`
    /// is somehow still live — never expected given this crate's
    /// single-threaded, non-reentrant opcode dispatch (see the field
    /// doc above), but a structured `Err` rather than the panic
    /// `RefCell::borrow()` itself would give, matching this crate's
    /// no-panic-on-a-production-path convention.
    fn try_rows(
        &self,
        opcode: &'static str,
    ) -> Result<std::cell::Ref<'_, Vec<EphemeralRow>>, ExecError> {
        self.rows
            .try_borrow()
            .map_err(|_| ExecError::MalformedInstruction {
                opcode,
                reason: "ephemeral table rows already borrowed".to_string(),
            })
    }

    /// [`Self::try_rows`], mutably — only ever called by `Insert` on the
    /// one cursor still materializing (an `OpenDup`-ed cursor never
    /// inserts, so this never contends with a `try_rows` read borrow in
    /// practice).
    fn try_rows_mut(
        &self,
        opcode: &'static str,
    ) -> Result<std::cell::RefMut<'_, Vec<EphemeralRow>>, ExecError> {
        self.rows
            .try_borrow_mut()
            .map_err(|_| ExecError::MalformedInstruction {
                opcode,
                reason: "ephemeral table rows already borrowed".to_string(),
            })
    }
}

// Methods rather than free functions so the borrow of `self` elides — see the
// note on the equivalent helpers in sorter.rs.
impl Vm {
    fn table_cursor_mut(
        &mut self,
        slot: i32,
        opcode: &'static str,
    ) -> Result<&mut TableCursorState, ExecError> {
        match self.cursor_mut(slot)? {
            CursorSlot::Table(state) => Ok(state),
            other => Err(ExecError::CursorTypeMismatch {
                opcode,
                slot,
                found: other.type_name(),
                expected: "table cursor",
            }),
        }
    }

    fn ephemeral_mut(
        &mut self,
        slot: i32,
        opcode: &'static str,
    ) -> Result<&mut EphemeralState, ExecError> {
        match self.cursor_mut(slot)? {
            CursorSlot::Ephemeral(state) => Ok(state),
            other => Err(ExecError::CursorTypeMismatch {
                opcode,
                slot,
                found: other.type_name(),
                expected: "ephemeral cursor",
            }),
        }
    }

    fn auto_index_mut(
        &mut self,
        slot: i32,
        opcode: &'static str,
    ) -> Result<&mut AutoIndexState, ExecError> {
        match self.cursor_mut(slot)? {
            CursorSlot::EphemeralAutoIndex(state) => Ok(state),
            other => Err(ExecError::CursorTypeMismatch {
                opcode,
                slot,
                found: other.type_name(),
                expected: "ephemeral automatic-index cursor",
            }),
        }
    }

    fn ephemeral_table_mut(
        &mut self,
        slot: i32,
        opcode: &'static str,
    ) -> Result<&mut EphemeralTableState, ExecError> {
        match self.cursor_mut(slot)? {
            CursorSlot::EphemeralTable(state) => Ok(state),
            other => Err(ExecError::CursorTypeMismatch {
                opcode,
                slot,
                found: other.type_name(),
                expected: "ephemeral table cursor",
            }),
        }
    }
}

/// `SeekIndexEq`'s per-probe-column collation list (#500): a
/// `P4::SeekKey` names each probed column's declared `COLLATE`
/// explicitly; a plain `P4::Int(n)` (every `SeekIndexEq` emission site
/// that hasn't been taught about declared collations yet) defaults all
/// `n` columns to [`Collation::Binary`], preserving prior behavior.
/// Returns the key columns' collations for a seek/insert instruction —
/// borrowed straight out of `instr.p4` for the common `P4::SeekKey` case
/// (#591) rather than always cloning a fresh `Vec`; only the synthesized
/// all-`Binary` fallback for `P4::Int` allocates.
fn seek_key_collations<'a>(
    instr: &'a Instruction,
    opcode: &'static str,
) -> Result<std::borrow::Cow<'a, [Collation]>, ExecError> {
    match &instr.p4 {
        P4::SeekKey(collations) => Ok(std::borrow::Cow::Borrowed(collations)),
        P4::Int(n) => {
            let n = usize::try_from(*n).map_err(|_| ExecError::MalformedInstruction {
                opcode,
                reason: format!("negative key column count {n}"),
            })?;
            Ok(std::borrow::Cow::Owned(vec![Collation::Binary; n]))
        }
        other => Err(ExecError::MalformedInstruction {
            opcode,
            reason: format!("expected a SeekKey or integer P4 (key columns), got {other:?}"),
        }),
    }
}

/// Normalizes `values` for use as an ephemeral-index dedup key (#518):
/// `NoCase` case-folds text, `RTrim` strips trailing spaces, so that
/// byte-equality on the resulting encoded record matches
/// [`compare`](crate::vdbe::compare::compare)'s notion of equality under
/// that collation — without switching the `BTreeMap`-keyed dedup
/// structure itself to a `compare()`-based lookup. Non-text values, and
/// any column under `Binary`, pass through unchanged.
fn normalize_key_values(values: &[Value], collations: &[Collation]) -> Vec<Value> {
    values
        .iter()
        .zip(collations.iter())
        .map(|(v, collation)| match (v, collation) {
            (Value::Text(s), Collation::NoCase) => {
                Value::Text(Arc::from(s.to_ascii_lowercase().as_str()))
            }
            (Value::Text(s), Collation::RTrim) => Value::Text(Arc::from(s.trim_end_matches(' '))),
            _ => v.clone(),
        })
        .collect()
}

fn p4_count(instr: &Instruction, opcode: &'static str) -> Result<usize, ExecError> {
    match &instr.p4 {
        P4::Int(n) => usize::try_from(*n).map_err(|_| ExecError::MalformedInstruction {
            opcode,
            reason: format!("negative key column count {n}"),
        }),
        other => Err(ExecError::MalformedInstruction {
            opcode,
            reason: format!("expected an integer P4 (key column count), got {other:?}"),
        }),
    }
}

fn read_register_range(
    vm: &Vm,
    start: i32,
    count: usize,
    opcode: &'static str,
) -> Result<Vec<Value>, ExecError> {
    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let reg = start
            .checked_add(
                i32::try_from(i).map_err(|_| ExecError::RegisterRangeTooLarge {
                    opcode,
                    count: count as i32,
                })?,
            )
            .ok_or(ExecError::RegisterOutOfRange {
                opcode,
                index: start,
            })?;
        values.push(vm.register(reg)?.clone());
    }
    Ok(values)
}

/// `Count` (#543): counts the rows in the table/index b-tree rooted at
/// page `P1`, storing the exact result in register `P2`. Never opens a
/// cursor slot — walks the b-tree directly via [`btree::count_table_rows`],
/// mirroring SQLite's own `OP_Count`.
pub fn count(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let root_page = u32::try_from(instr.p1).map_err(|_| ExecError::MalformedInstruction {
        opcode: "Count",
        reason: format!("invalid root page {}", instr.p1),
    })?;
    let db = vm.db()?;
    let count = btree::count_table_rows(&db.source, root_page).map_err(|e| {
        ExecError::MalformedInstruction {
            opcode: "Count",
            reason: e.to_string(),
        }
    })?;
    vm.set_register(instr.p2, Value::Integer(count))?;
    Ok(Step::Next)
}

/// `OpenRead`: opens a real read cursor on `P2` (the table's root page)
/// into cursor slot `P1`, sharing the `Vm`'s attached database page
/// source (see `Vm::with_db`) with every other open `OpenRead` cursor.
/// `P5` nonzero (#243, mirroring `OpenWrite`'s own `P5` dispatch) opens a
/// [`CursorSlot::IndexRead`] instead — a real secondary-index b-tree read
/// cursor for `SeekIndexEq`, rather than a table cursor.
pub fn open_read(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let root_page = u32::try_from(instr.p2).map_err(|_| ExecError::MalformedInstruction {
        opcode: "OpenRead",
        reason: format!("invalid root page {}", instr.p2),
    })?;
    let db = vm.db()?;
    if instr.p5 != 0 {
        let usable_size = db.header.usable_page_size();
        let cursor = IndexCursor::new(Rc::clone(&db.source), usable_size, root_page);
        vm.set_cursor(
            instr.p1,
            CursorSlot::IndexRead(IndexReadState {
                root_page,
                cursor,
                current: None,
                header_cache: RowHeaderCache::default(),
            }),
        )?;
        return Ok(Step::Next);
    }
    let cursor = TableCursor::new(Rc::clone(&db.source), &db.header, root_page);
    vm.set_cursor(
        instr.p1,
        CursorSlot::Table(TableCursorState {
            cursor,
            current_rowid: None,
            cached_payload: None,
            forced_null: false,
            root_page,
            header_cache: RowHeaderCache::default(),
        }),
    )?;
    Ok(Step::Next)
}

/// `OpenWrite` (#194): opens a write-capable cursor into slot `P1` on
/// root page `P2`. `P5` selects the b-tree kind: `0` (default) opens a
/// table cursor — the same `CursorSlot::Table` `OpenRead` uses (so
/// `Rewind`/`Next`/`SeekRowid`/`Column`/`Rowid` all work unchanged on a
/// write cursor too, matching decision 6's "`Delete` reads the
/// cursor's current position" requirement); nonzero opens a
/// [`CursorSlot::IndexWrite`] for `IdxInsert`'s real (non-ephemeral)
/// path. Requires a `Vm` built via [`Vm::with_writable_db`] — errors
/// with [`ExecError::NoDatabase`] against a read-only `Vm::with_db`.
pub fn open_write(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    // Fails fast if this `Vm` has no writer, before opening the slot.
    vm.writer("OpenWrite")?;
    let root_page = u32::try_from(instr.p2).map_err(|_| ExecError::MalformedInstruction {
        opcode: "OpenWrite",
        reason: format!("invalid root page {}", instr.p2),
    })?;
    if instr.p5 != 0 {
        vm.set_cursor(instr.p1, CursorSlot::IndexWrite { root_page })?;
        return Ok(Step::Next);
    }
    let db = vm.db()?;
    let cursor = TableCursor::new(Rc::clone(&db.source), &db.header, root_page);
    vm.set_cursor(
        instr.p1,
        CursorSlot::Table(TableCursorState {
            cursor,
            current_rowid: None,
            cached_payload: None,
            forced_null: false,
            root_page,
            header_cache: RowHeaderCache::default(),
        }),
    )?;
    Ok(Step::Next)
}

/// `OpenEphemeral`: opens an empty in-memory ephemeral index (DISTINCT's
/// dedup table, #87) into cursor slot `P1`. `P5` nonzero (#257, mirroring
/// `OpenRead`/`OpenWrite`'s own table-vs-index `P5` dispatch) instead
/// opens a [`CursorSlot::EphemeralTable`] — an ephemeral table cursor for
/// a materialized subquery-in-FROM.
pub fn open_ephemeral(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    // #545: `P5 == 2` selects the transient automatic-index mode
    // (`AutoIndexState`), a third cursor flavor alongside the
    // index-mode (`P5 == 0`) and table-mode (`P5` nonzero, historically
    // just "nonzero") cursors below -- checked first so it doesn't fall
    // into the table-mode `!= 0` branch.
    if instr.p5 == 2 {
        vm.set_cursor(
            instr.p1,
            CursorSlot::EphemeralAutoIndex(AutoIndexState::default()),
        )?;
        return Ok(Step::Next);
    }
    if instr.p5 != 0 {
        vm.set_cursor(
            instr.p1,
            CursorSlot::EphemeralTable(EphemeralTableState::default()),
        )?;
        return Ok(Step::Next);
    }
    vm.set_cursor(instr.p1, CursorSlot::Ephemeral(EphemeralState::default()))?;
    Ok(Step::Next)
}

/// `OpenDup`: opens cursor `P1` as a second, independently-scanning view
/// onto the same materialized row set cursor `P2` (an ephemeral *table*
/// cursor, i.e. opened by `OpenEphemeral` with `P5` nonzero) already
/// holds (#425) — used when a `WITH`-clause CTE is referenced more than
/// once in one statement, so only the first reference pays to
/// materialize it. `P1` starts unpositioned (`pos: None`), same as a
/// fresh `OpenEphemeral`; only the row data (`rows`) is shared, via the
/// `Rc` clone.
pub fn open_dup(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let rows = match vm.cursor(instr.p2)? {
        CursorSlot::EphemeralTable(state) => Rc::clone(&state.rows),
        other => {
            return Err(ExecError::CursorTypeMismatch {
                opcode: "OpenDup",
                slot: instr.p2,
                found: other.type_name(),
                expected: "ephemeral table cursor",
            })
        }
    };
    vm.set_cursor(
        instr.p1,
        CursorSlot::EphemeralTable(EphemeralTableState {
            rows,
            pos: None,
            sequence: 1,
        }),
    )?;
    Ok(Step::Next)
}

/// `OpenPseudo`: opens a single-row pseudo-cursor into slot `P1` that
/// re-presents register `P2`'s record blob as a cursor row, so `Column`
/// needs no special case for sorter-sourced (or otherwise
/// already-computed) rows.
pub fn open_pseudo(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    vm.set_cursor(
        instr.p1,
        CursorSlot::Pseudo {
            register: instr.p2,
            header_cache: RowHeaderCache::default(),
            cached_blob: None,
        },
    )?;
    Ok(Step::Next)
}

/// `Rewind`: positions cursor `P1` at its first row, jumping to `P2` if
/// the table is empty (mirrors the oracle's own `OP_Rewind` shape). Works
/// against both a real [`CursorSlot::Table`] and (#257) an in-memory
/// [`CursorSlot::EphemeralTable`].
pub fn rewind(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let found = match vm.cursor_mut(instr.p1)? {
        CursorSlot::Table(state) => {
            state.forced_null = false;
            let row = state
                .cursor
                .first()
                .map_err(|e| ExecError::MalformedInstruction {
                    opcode: "Rewind",
                    reason: e.to_string(),
                })?;
            state.set_current(row);
            state.current_rowid.is_some()
        }
        CursorSlot::EphemeralTable(state) => {
            let len = state.try_rows("Rewind")?.len();
            state.pos = if len == 0 { None } else { Some(0) };
            state.pos.is_some()
        }
        other => {
            return Err(ExecError::CursorTypeMismatch {
                opcode: "Rewind",
                slot: instr.p1,
                found: other.type_name(),
                expected: "table or ephemeral table cursor",
            })
        }
    };
    Ok(if found {
        Step::Next
    } else {
        Step::Jump(to_pc(instr.p2))
    })
}

/// `Last`: positions cursor `P1` at its last row (highest rowid),
/// jumping to `P2` if the table is empty. Works against both a real
/// [`CursorSlot::Table`] and (#257) an in-memory
/// [`CursorSlot::EphemeralTable`].
pub fn last(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let found = match vm.cursor_mut(instr.p1)? {
        CursorSlot::Table(state) => {
            state.forced_null = false;
            let row = state
                .cursor
                .last()
                .map_err(|e| ExecError::MalformedInstruction {
                    opcode: "Last",
                    reason: e.to_string(),
                })?;
            state.set_current(row);
            state.current_rowid.is_some()
        }
        CursorSlot::EphemeralTable(state) => {
            let len = state.try_rows("Last")?.len();
            state.pos = len.checked_sub(1);
            state.pos.is_some()
        }
        other => {
            return Err(ExecError::CursorTypeMismatch {
                opcode: "Last",
                slot: instr.p1,
                found: other.type_name(),
                expected: "table or ephemeral table cursor",
            })
        }
    };
    Ok(if found {
        Step::Next
    } else {
        Step::Jump(to_pc(instr.p2))
    })
}

/// `Next`: advances cursor `P1` to the following row, jumping to `P2`
/// (typically back to the loop body's start) if another row was found —
/// falls through (ending the loop) once exhausted. Works against both a
/// real [`CursorSlot::Table`] and (#257) an in-memory
/// [`CursorSlot::EphemeralTable`].
pub fn next(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let found = match vm.cursor_mut(instr.p1)? {
        CursorSlot::Table(state) => {
            let row = state
                .cursor
                .next()
                .map_err(|e| ExecError::MalformedInstruction {
                    opcode: "Next",
                    reason: e.to_string(),
                })?;
            state.set_current(row);
            state.current_rowid.is_some()
        }
        CursorSlot::EphemeralTable(state) => {
            let next_pos = state.pos.map(|p| p.saturating_add(1)).unwrap_or(0);
            let len = state.try_rows("Next")?.len();
            state.pos = if next_pos < len { Some(next_pos) } else { None };
            state.pos.is_some()
        }
        other => {
            return Err(ExecError::CursorTypeMismatch {
                opcode: "Next",
                slot: instr.p1,
                found: other.type_name(),
                expected: "table or ephemeral table cursor",
            })
        }
    };
    Ok(if found {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// Reads column `idx` of cursor `slot`'s current row. For a table/pseudo
/// cursor this decodes only column `idx` out of the row's record payload
/// (#439) rather than the whole record, so a `WHERE`, `SET`, or
/// `SELECT`-list read pays only for the columns it actually names — a
/// row a `WHERE` filter rejects before reading later columns never has
/// those columns decoded at all.
fn read_row_column(
    vm: &mut Vm,
    slot: i32,
    idx: usize,
    opcode: &'static str,
) -> Result<Value, ExecError> {
    // A pseudo cursor's row is a register's blob, which can be read by
    // several separate `Column` opcodes before the register changes
    // again (#631) — resolve which blob it is now (an immutable borrow
    // of `vm`), then take a mutable borrow of the cursor slot to reuse
    // its header cache across those reads, same shape as the
    // cache-bearing table-cursor arm below. Ephemeral-table cursors
    // need no cache at all (a row is already stored as decoded
    // `Value`s) — handled here too against the initial immutable
    // borrow, before the mutable-borrow block starts.
    if let CursorSlot::Pseudo { register, .. } = vm.cursor(slot)? {
        let register = *register;
        let bytes = match vm.register(register)? {
            Value::Blob(bytes) => bytes.clone(),
            other => {
                return Err(ExecError::MalformedInstruction {
                    opcode,
                    reason: format!("pseudo-cursor register holds {other:?}, not a record blob"),
                })
            }
        };
        return match vm.cursor_mut(slot)? {
            CursorSlot::Pseudo {
                header_cache,
                cached_blob,
                ..
            } => {
                if !cached_blob.as_ref().is_some_and(|b| Arc::ptr_eq(b, &bytes)) {
                    header_cache.invalidate();
                    *cached_blob = Some(bytes.clone());
                }
                header_cache
                    .ensure(&bytes)
                    .map_err(|e| ExecError::MalformedInstruction {
                        opcode,
                        reason: e.to_string(),
                    })?;
                header_cache
                    .column(&bytes, idx, TextEncoding::Utf8)
                    .map_err(|e| ExecError::MalformedInstruction {
                        opcode,
                        reason: e.to_string(),
                    })
            }
            other => Err(ExecError::CursorTypeMismatch {
                opcode,
                slot,
                found: other.type_name(),
                expected: "pseudo cursor",
            }),
        };
    }

    match vm.cursor(slot)? {
        CursorSlot::EphemeralTable(state) => {
            let rows = state.try_rows(opcode)?;
            return Ok(state
                .pos
                .and_then(|p| rows.get(p))
                .ok_or_else(|| ExecError::MalformedInstruction {
                    opcode,
                    reason: "cursor has no current row".to_string(),
                })?
                .1
                .get(idx)
                .cloned()
                .unwrap_or(Value::Null));
        }
        // #494: reads back the payload `Found` last matched (or
        // `IdxInsert` just wrote) — see `idx_insert`'s doc for how a
        // stored entry's value can hold more registers than its key.
        CursorSlot::Ephemeral(state) => {
            return Ok(state
                .last_key
                .as_ref()
                .and_then(|key| state.entries.get(key))
                .ok_or_else(|| ExecError::MalformedInstruction {
                    opcode,
                    reason: "cursor has no current row".to_string(),
                })?
                .get(idx)
                .cloned()
                .unwrap_or(Value::Null));
        }
        CursorSlot::Table(_) | CursorSlot::IndexRead(_) => {}
        other => {
            return Err(ExecError::CursorTypeMismatch {
                opcode,
                slot,
                found: other.type_name(),
                expected: "table, pseudo, ephemeral table, ephemeral, or index read cursor",
            })
        }
    }

    match vm.cursor_mut(slot)? {
        CursorSlot::Table(state) => {
            if state.forced_null {
                return Ok(Value::Null);
            }
            let positioned =
                state
                    .ensure_current_payload()
                    .map_err(|e| ExecError::MalformedInstruction {
                        opcode,
                        reason: e.to_string(),
                    })?;
            if !positioned {
                return Err(ExecError::MalformedInstruction {
                    opcode,
                    reason: "cursor has no current row".to_string(),
                });
            }
            let Some(payload) = state.cached_payload.as_ref() else {
                return Err(ExecError::MalformedInstruction {
                    opcode,
                    reason: "cursor has no current row".to_string(),
                });
            };
            state
                .header_cache
                .ensure(payload)
                .map_err(|e| ExecError::MalformedInstruction {
                    opcode,
                    reason: e.to_string(),
                })?;
            state
                .header_cache
                .column(payload, idx, TextEncoding::Utf8)
                .map_err(|e| ExecError::MalformedInstruction {
                    opcode,
                    reason: e.to_string(),
                })
        }
        CursorSlot::IndexRead(state) => {
            let payload = &state
                .current
                .as_ref()
                .ok_or_else(|| ExecError::MalformedInstruction {
                    opcode,
                    reason: "cursor has no current row".to_string(),
                })?
                .payload;
            state
                .header_cache
                .ensure(payload)
                .map_err(|e| ExecError::MalformedInstruction {
                    opcode,
                    reason: e.to_string(),
                })?;
            state
                .header_cache
                .column(payload, idx, TextEncoding::Utf8)
                .map_err(|e| ExecError::MalformedInstruction {
                    opcode,
                    reason: e.to_string(),
                })
        }
        _ => unreachable!("filtered to Table/IndexRead above"),
    }
}

/// `Column`: reads column `P2` of cursor `P1`'s current row into
/// register `P3`. A `NullRow`-forced table cursor always reads as NULL,
/// regardless of `P2`. Works on index-read cursors too (#444): real
/// SQLite reuses this same opcode against an index cursor's current
/// entry rather than defining a separate index-column opcode, so
/// covering-index scans and index-only aggregates decode straight out
/// of the index's own record via this path.
///
/// Known simplification: this does not substitute the rowid-alias
/// column (`INTEGER PRIMARY KEY`, stored as NULL in the record — see
/// `src/btree.rs`'s module doc) with the cursor's actual rowid; that
/// substitution is schema-aware and belongs to codegen (#91), which
/// knows which column, if any, is the alias and can emit `Rowid` instead
/// of `Column` for it.
pub fn column(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let idx = usize::try_from(instr.p2).map_err(|_| ExecError::MalformedInstruction {
        opcode: "Column",
        reason: format!("negative column index {}", instr.p2),
    })?;
    let value = read_row_column(vm, instr.p1, idx, "Column")?;
    vm.set_register(instr.p3, value)?;
    Ok(Step::Next)
}

/// `Rowid`: writes cursor `P1`'s current rowid into register `P2` (NULL
/// if the cursor is `NullRow`-forced).
pub fn rowid(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let value = match vm.cursor(instr.p1)? {
        CursorSlot::Table(state) => {
            if state.forced_null {
                Value::Null
            } else {
                let rowid = state
                    .current_rowid
                    .ok_or_else(|| ExecError::MalformedInstruction {
                        opcode: "Rowid",
                        reason: "cursor has no current row".to_string(),
                    })?;
                Value::Integer(rowid)
            }
        }
        CursorSlot::EphemeralTable(state) => {
            let rows = state.try_rows("Rowid")?;
            let (rowid, _) = state.pos.and_then(|p| rows.get(p)).ok_or_else(|| {
                ExecError::MalformedInstruction {
                    opcode: "Rowid",
                    reason: "cursor has no current row".to_string(),
                }
            })?;
            Value::Integer(*rowid)
        }
        other => {
            return Err(ExecError::CursorTypeMismatch {
                opcode: "Rowid",
                slot: instr.p1,
                found: other.type_name(),
                expected: "table or ephemeral table cursor",
            })
        }
    };
    vm.set_register(instr.p2, value)?;
    Ok(Step::Next)
}

/// `SeekRowid`: positions cursor `P1` at the row whose rowid equals
/// register `P3`, jumping to `P2` if no such row exists.
pub fn seek_rowid(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let target = match vm.register(instr.p3)? {
        Value::Integer(i) => *i,
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "SeekRowid",
                reason: format!("target rowid register holds {other:?}, not an integer"),
            })
        }
    };
    let state = vm.table_cursor_mut(instr.p1, "SeekRowid")?;
    state.forced_null = false;
    let row = state
        .cursor
        .seek(target)
        .map_err(|e| ExecError::MalformedInstruction {
            opcode: "SeekRowid",
            reason: e.to_string(),
        })?;
    state.set_current(row);
    Ok(if state.current_rowid.is_none() {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// `SeekIndexEq` (#243): probes index-read cursor `P1` (opened by
/// `OpenRead` with `P5` nonzero) for an exact match on the `P4::Int`
/// count of key columns starting at register `P3`, jumping to `P2` on a
/// miss. On a hit, decodes the matched index row's trailing rowid column
/// and records it in the cursor slot for a following `IdxRowid` — the
/// planner's join equality-index-selection fast path chains
/// `SeekIndexEq` + `IdxRowid` + `SeekRowid` (on the table cursor) in
/// place of an unconditional `Rewind`/`Next` full scan.
///
/// Seeks the slot's own persisted traversal cursor (`state.cursor`, the
/// same one `IdxRewind`/`IdxNext`/etc. (#296) drive) rather than a
/// throwaway one, so a following `IdxNext` resumes right after the
/// matched entry — a non-unique index's duplicate-key matches (#450) are
/// then just `SeekIndexEq` + a walk-while-still-equal `IdxNext` loop,
/// same as a UNIQUE index's single match falling straight through.
pub fn seek_index_eq(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let collations = seek_key_collations(instr, "SeekIndexEq")?;
    let probe = read_register_range(vm, instr.p3, collations.len(), "SeekIndexEq")?;
    let encoding = vm.db()?.header.text_encoding;
    let state = index_read_state_mut(vm, instr.p1, "SeekIndexEq")?;
    let found =
        state
            .cursor
            .seek(&probe, encoding)
            .map_err(|e| ExecError::MalformedInstruction {
                opcode: "SeekIndexEq",
                reason: e.to_string(),
            })?;
    let matched = match &found {
        Some(row) => {
            // #591: only the probe-length prefix is ever compared, and
            // `record_column_count` (header-only, no value decoding) is
            // enough to know whether a trailing rowid column exists —
            // no need to decode the whole record via `decode_record`.
            let total_columns =
                record_column_count(&row.payload).map_err(|e| ExecError::MalformedInstruction {
                    opcode: "SeekIndexEq",
                    reason: e.to_string(),
                })?;
            total_columns > probe.len() && {
                let key = decode_record_upto(&row.payload, probe.len(), encoding).map_err(|e| {
                    ExecError::MalformedInstruction {
                        opcode: "SeekIndexEq",
                        reason: e.to_string(),
                    }
                })?;
                key.iter()
                    .zip(probe.iter())
                    .zip(collations.iter())
                    .all(|((k, p), &collation)| compare(k, p, collation).is_eq())
            }
        }
        None => false,
    };
    let current = if matched { found } else { None };
    state.set_current(current.clone());
    Ok(if current.is_none() {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// Seeks index-read cursor `P1` to the first entry whose key (built from
/// registers `P3..P3+P4`) is `>=` the probe, per `P4`'s collations.
/// Unlike [`seek_index_eq`], any landed-on row is accepted — there is no
/// exact-equality recheck, since the whole point is a range floor, not a
/// point lookup. Jumps to `P2` if the b-tree `seek()` finds no such entry
/// (the probe is greater than every key in the index).
pub fn seek_index_ge(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let collations = seek_key_collations(instr, "SeekIndexGE")?;
    let probe = read_register_range(vm, instr.p3, collations.len(), "SeekIndexGE")?;
    let encoding = vm.db()?.header.text_encoding;
    let state = index_read_state_mut(vm, instr.p1, "SeekIndexGE")?;
    let found =
        state
            .cursor
            .seek(&probe, encoding)
            .map_err(|e| ExecError::MalformedInstruction {
                opcode: "SeekIndexGE",
                reason: e.to_string(),
            })?;
    state.set_current(found.clone());
    Ok(if found.is_none() {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// Decodes index-read cursor `P1`'s current row's leading `collations.len()`
/// columns and compares them (collation-aware, per-column) against `probe`.
/// Shared by [`seek_index_eq`]'s exact-match recheck and
/// [`idx_compare_gt`]'s upper-bound stop check. Returns `None` if the
/// current row has too few columns to compare (treated as "not greater"
/// by [`idx_compare_gt`], mirroring `seek_index_eq`'s "too short = miss").
fn decode_leading_columns(
    row: &crate::btree::IndexRow,
    probe: &[Value],
    encoding: TextEncoding,
    opcode: &'static str,
) -> Result<Option<Vec<Value>>, ExecError> {
    let total_columns =
        record_column_count(&row.payload).map_err(|e| ExecError::MalformedInstruction {
            opcode,
            reason: e.to_string(),
        })?;
    if total_columns <= probe.len() {
        return Ok(None);
    }
    let key = decode_record_upto(&row.payload, probe.len(), encoding).map_err(|e| {
        ExecError::MalformedInstruction {
            opcode,
            reason: e.to_string(),
        }
    })?;
    Ok(Some(key))
}

/// Compares index-read cursor `P1`'s current entry's leading `P4` columns
/// against the key built from registers `P3..P3+P4`, jumping to `P2` if
/// the current key is strictly greater than the probe under `P4`'s
/// collations. The real-index-cursor counterpart to the ephemeral-cursor
/// `IdxLE` — used as the upper-bound stop check for `SeekIndexGE` +
/// `IdxNext` range walks (see ADR-0034). A cursor with no current row
/// (exhausted) is treated as "not greater" (falls through) since there
/// is nothing left to walk past.
pub fn idx_compare_gt(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let collations = seek_key_collations(instr, "IdxCompareGT")?;
    let probe = read_register_range(vm, instr.p3, collations.len(), "IdxCompareGT")?;
    let encoding = vm.db()?.header.text_encoding;
    let state = index_read_state_mut(vm, instr.p1, "IdxCompareGT")?;
    let current = state.current.clone();
    let is_greater = match &current {
        Some(row) => match decode_leading_columns(row, &probe, encoding, "IdxCompareGT")? {
            Some(key) => key
                .iter()
                .zip(probe.iter())
                .zip(collations.iter())
                .map(|((k, p), &collation)| compare(k, p, collation))
                .find(|o| !o.is_eq())
                .map(|o| o.is_gt())
                .unwrap_or(false),
            None => false,
        },
        None => false,
    };
    Ok(if is_greater {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// Decodes index-read cursor `P1`'s current row (an ordinary secondary
/// index entry — its decoded record's trailing column is always the
/// referenced table's rowid) into an `i64` rowid, for [`idx_rowid`] and
/// the `IdxRewind`/`IdxLast`/`IdxNext`/`IdxPrev` scan opcodes' shared
/// "have we got a row, and what's its rowid" question.
///
/// Decodes only the trailing column via `header_cache` (#591) rather than
/// the whole record — the header parse is cheap (no value decoding), and
/// only the last column's `Value` (plus one Rc for its allocation, if
/// any) ever needs to be built.
fn index_read_current_rowid(
    vm: &mut Vm,
    slot: i32,
    opcode: &'static str,
) -> Result<Option<i64>, ExecError> {
    // Type-checked (and short-circuited on "no current row") before
    // touching `vm.db()` — a cursor-type mismatch or an unpositioned
    // cursor must surface the same way whether or not a database is
    // attached to this `Vm`.
    match vm.cursor(slot)? {
        CursorSlot::IndexRead(_) => {}
        other => {
            return Err(ExecError::CursorTypeMismatch {
                opcode,
                slot,
                found: other.type_name(),
                expected: "index read cursor",
            })
        }
    }
    let encoding = vm.db()?.header.text_encoding;
    let state = match vm.cursor_mut(slot)? {
        CursorSlot::IndexRead(state) => state,
        _ => unreachable!("cursor type already checked above"),
    };
    let Some(row) = &state.current else {
        return Ok(None);
    };
    state
        .header_cache
        .ensure(&row.payload)
        .map_err(|e| ExecError::MalformedInstruction {
            opcode,
            reason: e.to_string(),
        })?;
    let last = state.header_cache.column_count().wrapping_sub(1);
    let rowid = state
        .header_cache
        .column(&row.payload, last, encoding)
        .map_err(|e| ExecError::MalformedInstruction {
            opcode,
            reason: e.to_string(),
        })?;
    match rowid {
        Value::Integer(rowid) => Ok(Some(rowid)),
        other => Err(ExecError::MalformedInstruction {
            opcode,
            reason: format!("index row's trailing rowid column is {other:?}"),
        }),
    }
}

fn index_read_state_mut<'a>(
    vm: &'a mut Vm,
    slot: i32,
    opcode: &'static str,
) -> Result<&'a mut IndexReadState, ExecError> {
    match vm.cursor_mut(slot)? {
        CursorSlot::IndexRead(state) => Ok(state),
        other => Err(ExecError::CursorTypeMismatch {
            opcode,
            slot,
            found: other.type_name(),
            expected: "index read cursor",
        }),
    }
}

/// `IdxRewind` (#296): positions index-read cursor `P1` (opened by
/// `OpenRead` with `P5` nonzero) at its first entry in ascending key
/// order, jumping to `P2` if the index is empty — the index-cursor
/// counterpart to `Rewind`, used by an index-ordered scan walking a
/// matching index forward.
pub fn idx_rewind(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let state = index_read_state_mut(vm, instr.p1, "IdxRewind")?;
    let row = state
        .cursor
        .first()
        .map_err(|e| ExecError::MalformedInstruction {
            opcode: "IdxRewind",
            reason: e.to_string(),
        })?;
    state.set_current(row);
    Ok(if state.current.is_some() {
        Step::Next
    } else {
        Step::Jump(to_pc(instr.p2))
    })
}

/// `IdxLast` (#296): positions index-read cursor `P1` at its last entry
/// (descending key order from here on), jumping to `P2` if the index is
/// empty — the index-cursor counterpart to `Last`, used by an
/// index-ordered scan walking a matching index backward (`ORDER BY ...
/// DESC` over an ascending index, or vice versa).
pub fn idx_last(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let state = index_read_state_mut(vm, instr.p1, "IdxLast")?;
    let row = state
        .cursor
        .last()
        .map_err(|e| ExecError::MalformedInstruction {
            opcode: "IdxLast",
            reason: e.to_string(),
        })?;
    state.set_current(row);
    Ok(if state.current.is_some() {
        Step::Next
    } else {
        Step::Jump(to_pc(instr.p2))
    })
}

/// `IdxNext` (#296): advances index-read cursor `P1` forward, jumping to
/// `P2` (typically back to the loop body's start) if another entry was
/// found — falls through once exhausted. Mirrors `Next`'s jump-on-found
/// shape; pairs with `IdxRewind`.
pub fn idx_next(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let state = index_read_state_mut(vm, instr.p1, "IdxNext")?;
    let row = state
        .cursor
        .next()
        .map_err(|e| ExecError::MalformedInstruction {
            opcode: "IdxNext",
            reason: e.to_string(),
        })?;
    state.set_current(row);
    Ok(if state.current.is_some() {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// `IdxPrev` (#296): advances index-read cursor `P1` backward, jumping to
/// `P2` if another entry was found — falls through once exhausted. Pairs
/// with `IdxLast`, the same way `IdxNext` pairs with `IdxRewind`.
pub fn idx_prev(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let state = index_read_state_mut(vm, instr.p1, "IdxPrev")?;
    let row = state
        .cursor
        .prev()
        .map_err(|e| ExecError::MalformedInstruction {
            opcode: "IdxPrev",
            reason: e.to_string(),
        })?;
    state.set_current(row);
    Ok(if state.current.is_some() {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// `IdxRowid` (#243): writes index-read cursor `P1`'s most recently
/// `SeekIndexEq`-matched trailing rowid into register `P2`. Errors if
/// called without a preceding successful `SeekIndexEq` on this cursor.
pub fn idx_rowid(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let rowid = index_read_current_rowid(vm, instr.p1, "IdxRowid")?.ok_or_else(|| {
        ExecError::MalformedInstruction {
            opcode: "IdxRowid",
            reason: "no current row on this index cursor (SeekIndexEq missed, or no \
                         positioning opcode was run)"
                .to_string(),
        }
    })?;
    vm.set_register(instr.p2, Value::Integer(rowid))?;
    Ok(Step::Next)
}

/// `NullRow`: forces cursor `P1` to read as an all-NULL row until its
/// next real positioning (`Rewind`/`Last`/`Next`/`SeekRowid`).
pub fn null_row(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let state = vm.table_cursor_mut(instr.p1, "NullRow")?;
    state.forced_null = true;
    state.set_current(None);
    Ok(Step::Next)
}

/// `Sequence`: writes ephemeral cursor `P1`'s next monotonic counter
/// value into register `P2` (independent of the dedup key — used to
/// allocate a synthetic rowid for an ephemeral-table row).
pub fn sequence(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let value = match vm.cursor_mut(instr.p1)? {
        CursorSlot::Ephemeral(state) => {
            let v = state.sequence;
            state.sequence = state.sequence.saturating_add(1);
            v
        }
        CursorSlot::EphemeralTable(state) => {
            let v = state.sequence;
            state.sequence = state.sequence.saturating_add(1);
            v
        }
        other => {
            return Err(ExecError::CursorTypeMismatch {
                opcode: "Sequence",
                slot: instr.p1,
                found: other.type_name(),
                expected: "ephemeral or ephemeral table cursor",
            })
        }
    };
    vm.set_register(instr.p2, Value::Integer(value))?;
    Ok(Step::Next)
}

/// `Found`: probes ephemeral cursor `P1` for the key built from `P4`
/// (`SeekKey`, a per-column collation list — a plain `Int` count treats
/// every column as `Binary`, #518) registers starting at `P3`, jumping
/// to `P2` if present. Either way, remembers the probed key as the
/// target of a following `IdxInsert`/`Delete`.
pub fn found(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let collations = seek_key_collations(instr, "Found")?;
    let values = read_register_range(vm, instr.p3, collations.len(), "Found")?;
    let key = encode_record(
        &normalize_key_values(&values, &collations),
        TextEncoding::Utf8,
    );
    let state = vm.ephemeral_mut(instr.p1, "Found")?;
    let present = state.entries.contains_key(&key);
    state.last_key = Some(key);
    Ok(if present {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// `IdxInsert`: for an ephemeral cursor (DISTINCT's dedup path), inserts
/// the key built from `P4` (`SeekKey`, a per-column collation list — a
/// plain `Int` count treats every column as `Binary`, #518) registers
/// starting at `P2` into ephemeral cursor `P1`. For a
/// real [`CursorSlot::IndexWrite`] cursor (#194, opened by `OpenWrite`
/// with `P5` nonzero), instead encodes the same register range as a
/// full index entry and writes it into the on-disk index b-tree via
/// [`btree::insert_entry`] — `Err(BtreeError::DuplicateKey)` surfaces as
/// a `MalformedInstruction` (this opcode does not model `OR IGNORE`/`OR
/// REPLACE` conflict resolution).
///
/// `P5` (#494, ephemeral cursor only — an [`CursorSlot::IndexWrite`]
/// insert ignores it): count of extra *payload-only* registers
/// immediately following the `P4`-sized key range. The stored entry
/// keys the `BTreeMap` on the key columns alone (`P4` count, unchanged
/// byte-encoding, so [`found`]'s lookup with a matching `P4` count still
/// hits) but the record VALUE holds all `P4 + P5` registers — letting a
/// caller (e.g. `codegen::subquery::memoize`'s correlated-subquery
/// cache) probe by key alone via `Found` and then read back an
/// associated payload via `Column`, instead of every stored register
/// having to double as part of the key. Defaults to `0`, which is
/// exactly today's behavior (key == whole stored record).
pub fn idx_insert(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let key_collations = seek_key_collations(instr, "IdxInsert")?;
    let key_count = key_collations.len();
    match vm.cursor(instr.p1)? {
        CursorSlot::IndexWrite { root_page } => {
            let root_page = *root_page;
            let values = read_register_range(vm, instr.p2, key_count, "IdxInsert")?;
            let pager = vm.writer("IdxInsert")?;
            let db = vm.db()?;
            let encoding = db.header.text_encoding;
            let header = db.header;
            let mut pager = pager.borrow_mut();
            btree::insert_entry(&mut pager, &header, root_page, &values, encoding).map_err(
                |e| ExecError::MalformedInstruction {
                    opcode: "IdxInsert",
                    reason: e.to_string(),
                },
            )?;
            Ok(Step::Next)
        }
        CursorSlot::Ephemeral(_) => {
            let extra = usize::from(instr.p5);
            let total =
                key_count
                    .checked_add(extra)
                    .ok_or_else(|| ExecError::RegisterRangeTooLarge {
                        opcode: "IdxInsert",
                        count: instr.p5.into(),
                    })?;
            let values = read_register_range(vm, instr.p2, total, "IdxInsert")?;
            let key_values =
                values
                    .get(..key_count)
                    .ok_or_else(|| ExecError::MalformedInstruction {
                        opcode: "IdxInsert",
                        reason: "key column count exceeds the values read".to_string(),
                    })?;
            let key = encode_record(
                &normalize_key_values(key_values, &key_collations),
                TextEncoding::Utf8,
            );
            let state = vm.ephemeral_mut(instr.p1, "IdxInsert")?;
            if !state.entries.contains_key(&key) && state.entries.len() >= MAX_EPHEMERAL_ROWS {
                return Err(ExecError::EphemeralRowLimitExceeded {
                    opcode: "IdxInsert",
                    limit: MAX_EPHEMERAL_ROWS,
                });
            }
            state.entries.insert(key.clone(), values);
            state.last_key = Some(key);
            Ok(Step::Next)
        }
        other => Err(ExecError::CursorTypeMismatch {
            opcode: "IdxInsert",
            slot: instr.p1,
            found: other.type_name(),
            expected: "ephemeral or index write cursor",
        }),
    }
}

/// `IdxDelete` (#210): for an ephemeral cursor, removes the key built
/// from `P4` (`Int`, the key column count) registers starting at `P2`
/// from ephemeral cursor `P1`. For a real [`CursorSlot::IndexWrite`]
/// cursor, encodes the same register range as an index key and removes
/// the matching entry from the on-disk index b-tree via
/// [`btree::delete_entry`] — `Err(BtreeError::KeyNotFound)`
/// surfaces as a `MalformedInstruction`. Mirrors [`idx_insert`]'s operand
/// shape and cursor-kind dispatch.
pub fn idx_delete(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let count = p4_count(instr, "IdxDelete")?;
    let values = read_register_range(vm, instr.p2, count, "IdxDelete")?;
    match vm.cursor(instr.p1)? {
        CursorSlot::IndexWrite { root_page } => {
            let root_page = *root_page;
            let pager = vm.writer("IdxDelete")?;
            let db = vm.db()?;
            let encoding = db.header.text_encoding;
            let header = db.header;
            let mut pager = pager.borrow_mut();
            btree::delete_entry(&mut pager, &header, root_page, &values, encoding).map_err(
                |e| ExecError::MalformedInstruction {
                    opcode: "IdxDelete",
                    reason: e.to_string(),
                },
            )?;
            Ok(Step::Next)
        }
        CursorSlot::Ephemeral(_) => {
            let key = encode_record(&values, TextEncoding::Utf8);
            let state = vm.ephemeral_mut(instr.p1, "IdxDelete")?;
            state.entries.remove(&key);
            if state.last_key.as_ref() == Some(&key) {
                state.last_key = None;
            }
            Ok(Step::Next)
        }
        other => Err(ExecError::CursorTypeMismatch {
            opcode: "IdxDelete",
            slot: instr.p1,
            found: other.type_name(),
            expected: "ephemeral or index write cursor",
        }),
    }
}

/// `NoConflict` (#207): jumps to `P2` when no entry in the real index
/// b-tree rooted at cursor `P1`'s `IndexWrite` root page has a key whose
/// leading columns equal the `P4` (`Int`, key column count) registers
/// starting at `P3` — i.e. "no conflicting row exists for this
/// candidate UNIQUE key", the seek+branch primitive #207's own doc
/// (`src/vdbe/cursor.rs`, this comment) identified as missing. Falls
/// through (does not jump) when a matching entry IS found, so callers
/// emit their `ON CONFLICT` handling as the fallthrough body — mirroring
/// `SeekRowid`'s "jump on absence" shape used by the rowid-PK conflict
/// check in `src/codegen/insert.rs`.
///
/// On a conflict (fallthrough), also writes the conflicting entry's
/// trailing rowid column into register `P3 + count` — one past the
/// probe range — so an `OR REPLACE` caller can `SeekRowid` the table
/// cursor onto the row being displaced without a second index lookup.
/// Callers that don't need `OR REPLACE` may leave that register
/// unallocated for anything else, but MUST NOT reuse it for the probe
/// itself.
///
/// Built on [`IndexCursor::seek`] (`src/btree/index.rs`), a linear scan
/// from the first entry (BINARY collation only, Tier 0 scope — matches
/// the cursor's own documented limitation). The probe key is just the
/// index's declared columns, without the trailing rowid every on-disk
/// entry carries; `seek` returns the first entry whose full key is not
/// less than that shorter probe (`compare_keys`' `zip` naturally treats
/// the probe as a prefix), so this checks only that returned entry's own
/// leading columns for an exact match — the trailing rowid is irrelevant
/// to uniqueness.
pub fn no_conflict(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let count = p4_count(instr, "NoConflict")?;
    let probe = read_register_range(vm, instr.p3, count, "NoConflict")?;
    let root_page = match vm.cursor(instr.p1)? {
        CursorSlot::IndexWrite { root_page } => *root_page,
        other => {
            return Err(ExecError::CursorTypeMismatch {
                opcode: "NoConflict",
                slot: instr.p1,
                found: other.type_name(),
                expected: "index write cursor",
            })
        }
    };
    let db = vm.db()?;
    let encoding = db.header.text_encoding;
    let usable_size = db.header.usable_page_size();
    let mut cursor = IndexCursor::new(Rc::clone(&db.source), usable_size, root_page);
    let found = cursor
        .seek(&probe, encoding)
        .map_err(|e| ExecError::MalformedInstruction {
            opcode: "NoConflict",
            reason: e.to_string(),
        })?;
    let conflict_key = match found {
        Some(row) => {
            let key = decode_record(&row.payload, encoding).map_err(|e| {
                ExecError::MalformedInstruction {
                    opcode: "NoConflict",
                    reason: e.to_string(),
                }
            })?;
            let matches = key.len() >= probe.len()
                && key
                    .iter()
                    .zip(probe.iter())
                    .all(|(k, p)| compare(k, p, Collation::Binary).is_eq());
            matches.then_some(key)
        }
        None => None,
    };
    match conflict_key {
        Some(key) => {
            if let Some(rowid) = key.last() {
                let dest = instr.p3.saturating_add(i32::try_from(count).map_err(|_| {
                    ExecError::RegisterRangeTooLarge {
                        opcode: "NoConflict",
                        count: count as i32,
                    }
                })?);
                vm.set_register(dest, rowid.clone())?;
            }
            Ok(Step::Next)
        }
        None => Ok(Step::Jump(to_pc(instr.p2))),
    }
}

/// `IdxLE`: jumps to `P2` if the key built from `P4` (`Int`, the key
/// column count) registers starting at `P3` is `<=` ephemeral cursor
/// `P1`'s most recently probed/inserted key (byte-order comparison of
/// the encoded key, matching a BINARY-collated index).
///
/// Known scope limitation: the harvested use of this opcode
/// (`tools/opcodes-v2.json`) ties it to an `ORDER BY ... LIMIT 1`
/// index-seek query-planner optimization this ticket does not
/// implement (the full sorter path, Requirement 9, is what V2 actually
/// executes for `ORDER BY`) — there is no harvested example with more
/// than one occurrence to derive a fuller semantics from. This
/// implementation gives `IdxLE` a well-defined, testable meaning against
/// the same ephemeral cursor `Found`/`IdxInsert` already use, rather
/// than leaving it unimplemented, but does not claim oracle-exact parity
/// for the optimization the harvest actually observed it in.
pub fn idx_le(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let count = p4_count(instr, "IdxLE")?;
    let values = read_register_range(vm, instr.p3, count, "IdxLE")?;
    let probe = encode_record(&values, TextEncoding::Utf8);
    let state = vm.ephemeral_mut(instr.p1, "IdxLE")?;
    let holds = match &state.last_key {
        Some(key) => *key <= probe,
        None => true,
    };
    Ok(if holds {
        Step::Jump(to_pc(instr.p2))
    } else {
        Step::Next
    })
}

/// `AutoIndexInsert` (#545): appends the rowid in register `P3` under
/// the key built from register `P2` (one column, per `P4::SeekKey`'s
/// declared collation, or `P4::Int(1)` for `Binary`) into automatic-
/// index cursor `P1` — unlike [`idx_insert`]'s ephemeral-index path,
/// never overwrites an existing entry under the same key, since the
/// whole point is remembering every rowid a duplicate join key maps to.
pub fn auto_index_insert(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let collations = seek_key_collations(instr, "AutoIndexInsert")?;
    let key_values = read_register_range(vm, instr.p2, collations.len(), "AutoIndexInsert")?;
    let key = encode_record(
        &normalize_key_values(&key_values, &collations),
        TextEncoding::Utf8,
    );
    let rowid = match vm.register(instr.p3)? {
        Value::Integer(i) => *i,
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "AutoIndexInsert",
                reason: format!("rowid register holds {other:?}, not an integer"),
            })
        }
    };
    let state = vm.auto_index_mut(instr.p1, "AutoIndexInsert")?;
    state.entries.entry(key).or_default().push(rowid);
    Ok(Step::Next)
}

/// `AutoIndexSeek` (#545): probes automatic-index cursor `P1` for every
/// rowid inserted under the key built from register `P3` (same operand
/// shape as [`auto_index_insert`]'s key), positioning on the first if
/// any exist and jumping to `P2` if none do (an empty key's `Vec` is
/// never actually left behind by [`auto_index_insert`], but treated the
/// same as "no entry" defensively either way).
pub fn auto_index_seek(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let collations = seek_key_collations(instr, "AutoIndexSeek")?;
    let key_values = read_register_range(vm, instr.p3, collations.len(), "AutoIndexSeek")?;
    let key = encode_record(
        &normalize_key_values(&key_values, &collations),
        TextEncoding::Utf8,
    );
    let state = vm.auto_index_mut(instr.p1, "AutoIndexSeek")?;
    let has_match = state
        .entries
        .get(&key)
        .is_some_and(|rowids| !rowids.is_empty());
    state.current = has_match.then_some((key, 0));
    Ok(if has_match {
        Step::Next
    } else {
        Step::Jump(to_pc(instr.p2))
    })
}

/// `AutoIndexRowid` (#545): writes automatic-index cursor `P1`'s
/// currently-seeked rowid into register `P2`. Errors if called without
/// a preceding successful `AutoIndexSeek`/`AutoIndexNext` on this
/// cursor.
pub fn auto_index_rowid(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let state = vm.auto_index_mut(instr.p1, "AutoIndexRowid")?;
    let (key, pos) = state
        .current
        .as_ref()
        .ok_or_else(|| ExecError::MalformedInstruction {
            opcode: "AutoIndexRowid",
            reason: "no current row on this automatic-index cursor (AutoIndexSeek missed, or no \
                 positioning opcode was run)"
                .to_string(),
        })?;
    let rowid = state
        .entries
        .get(key)
        .and_then(|rowids| rowids.get(*pos))
        .copied()
        .ok_or_else(|| ExecError::MalformedInstruction {
            opcode: "AutoIndexRowid",
            reason: "current position out of range for its key's rowid list".to_string(),
        })?;
    vm.set_register(instr.p2, Value::Integer(rowid))?;
    Ok(Step::Next)
}

/// `AutoIndexNext` (#545): advances automatic-index cursor `P1` to the
/// next rowid sharing its current key, jumping to `P2` if there was
/// one — falls through once every rowid under that key has been
/// visited. Mirrors `IdxNext`'s "jump on found" shape.
pub fn auto_index_next(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let state = vm.auto_index_mut(instr.p1, "AutoIndexNext")?;
    // #591: `take` moves the already-owned key `Vec<u8>` out of `current`
    // instead of cloning it — `current` is unconditionally overwritten
    // (to `Some((key, next_pos))` or `None`) before this function returns,
    // so nothing is lost by taking it up front.
    let Some((key, pos)) = state.current.take() else {
        return Ok(Step::Next);
    };
    let len = state.entries.get(&key).map_or(0, Vec::len);
    let next_pos = pos.saturating_add(1);
    if next_pos < len {
        state.current = Some((key, next_pos));
        Ok(Step::Jump(to_pc(instr.p2)))
    } else {
        Ok(Step::Next)
    }
}

/// `Delete`: for an ephemeral cursor (unchanged), removes cursor `P1`'s
/// most recently probed/inserted entry (per `Found`/`IdxInsert`'s
/// `last_key`) — DISTINCT's "insert then delete the just-produced
/// duplicate" path (spec 009 Requirement 4). For a real
/// [`CursorSlot::Table`] write cursor (#194), deletes the row at the
/// cursor's *current* position (whatever `Rewind`/`Next`/`SeekRowid`
/// last positioned it on) from the on-disk table b-tree via
/// [`btree::delete_row`].
pub fn delete(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    match vm.cursor(instr.p1)? {
        CursorSlot::Table(state) => {
            let rowid = state
                .current_rowid
                .ok_or_else(|| ExecError::MalformedInstruction {
                    opcode: "Delete",
                    reason: "cursor has no current row".to_string(),
                })?;
            let root_page = state.root_page;
            let pager = vm.writer("Delete")?;
            let db = vm.db()?;
            let header = db.header;
            let mut pager = pager.borrow_mut();
            btree::delete_row(&mut pager, &header, root_page, rowid).map_err(|e| {
                ExecError::MalformedInstruction {
                    opcode: "Delete",
                    reason: e.to_string(),
                }
            })?;
            drop(pager);
            // The row this cursor was positioned on is now gone —
            // clear `current` so a stray follow-up `Rowid`/`Column`
            // reads as "no row" rather than stale data.
            if let CursorSlot::Table(state) = vm.cursor_mut(instr.p1)? {
                state.set_current(None);
            }
            // Only when codegen marked this delete as the statement's row
            // change (#692). A DELETE's own `Delete` carries the flag; the
            // one an UPDATE emits before re-inserting the row does not,
            // because that pair is one changed row, not two.
            if instr.p5 & OPFLAG_NCHANGE != 0 {
                vm.record_change();
            }
            Ok(Step::Next)
        }
        CursorSlot::Ephemeral(_) => {
            let state = vm.ephemeral_mut(instr.p1, "Delete")?;
            if let Some(key) = state.last_key.take() {
                state.entries.remove(&key);
            }
            Ok(Step::Next)
        }
        other => Err(ExecError::CursorTypeMismatch {
            opcode: "Delete",
            slot: instr.p1,
            found: other.type_name(),
            expected: "ephemeral or table cursor",
        }),
    }
}

/// `Insert` (#194): inserts a row into the table b-tree cursor `P1` is
/// open on (must be a real [`CursorSlot::Table`] write cursor, opened via
/// `OpenWrite`), OR (#257) appends a row into an in-memory
/// [`CursorSlot::EphemeralTable`] (opened by `OpenEphemeral` with `P5`
/// nonzero) — used to materialize a subquery-in-FROM. Either way, `P2`
/// holds the row's rowid (an integer register) and `P3` holds the
/// already-`MakeRecord`-encoded payload blob. The real-table path
/// delegates to [`btree::insert_row`]; `OR REPLACE`/`OR IGNORE`-style
/// `P5` conflict-resolution flags are not modeled there — every insert is
/// an unconditional add, matching `insert_row`'s own contract.
pub fn insert(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let rowid = match vm.register(instr.p2)? {
        Value::Integer(i) => *i,
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "Insert",
                reason: format!("rowid register holds {other:?}, not an integer"),
            })
        }
    };
    let payload = match vm.register(instr.p3)? {
        Value::Blob(bytes) => bytes.clone(),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "Insert",
                reason: format!("record register holds {other:?}, not a blob"),
            })
        }
    };
    match vm.cursor(instr.p1)? {
        CursorSlot::EphemeralTable(_) => {
            // No real db is attached for a purely in-memory VM (e.g. the
            // ephemeral-table unit tests below) — fall back to UTF-8,
            // matching every other decode site's default.
            let encoding = vm
                .db()
                .map(|db| db.header.text_encoding)
                .unwrap_or(TextEncoding::Utf8);
            let values =
                decode_record(&payload, encoding).map_err(|e| ExecError::MalformedInstruction {
                    opcode: "Insert",
                    reason: e.to_string(),
                })?;
            let state = vm.ephemeral_table_mut(instr.p1, "Insert")?;
            let mut rows = state.try_rows_mut("Insert")?;
            if rows.len() >= MAX_EPHEMERAL_ROWS {
                return Err(ExecError::EphemeralRowLimitExceeded {
                    opcode: "Insert",
                    limit: MAX_EPHEMERAL_ROWS,
                });
            }
            rows.push((rowid, values));
            Ok(Step::Next)
        }
        _ => {
            let root_page = vm.table_cursor_mut(instr.p1, "Insert")?.root_page;
            let pager = vm.writer("Insert")?;
            let db = vm.db()?;
            let header = db.header;
            let mut pager = pager.borrow_mut();
            btree::insert_row(&mut pager, &header, root_page, rowid, &payload).map_err(|e| {
                ExecError::MalformedInstruction {
                    opcode: "Insert",
                    reason: e.to_string(),
                }
            })?;
            drop(pager);
            // See `Delete` above: the flag is codegen's call. Note the
            // `EphemeralTable` arm never reaches here, so the two-pass
            // UPDATE plan's rowid-stashing `Insert` cannot count even if
            // a future caller flagged it by mistake.
            if instr.p5 & OPFLAG_NCHANGE != 0 {
                vm.record_change();
                // Nested inside `NCHANGE`, not beside it — `vdbe.c:5803`
                // updates `db->lastRowid` within the same arm and
                // `vdbe.c:5800` asserts `LASTROWID` implies `NCHANGE`.
                // `rowid` here is the key the insert actually used, which
                // is the value `sqlite3_last_insert_rowid()` returns.
                if instr.p5 & OPFLAG_LASTROWID != 0 {
                    vm.record_last_insert_rowid(rowid);
                }
            }
            Ok(Step::Next)
        }
    }
}

/// `NewRowid` (#194): computes a fresh rowid for table cursor `P1`
/// (`max(rowid) + 1`, or `1` for an empty table — via
/// [`TableCursor::last`]) and writes it to register `P2`.
///
/// AUTOINCREMENT simplification: this VDBE layer has no schema-aware
/// way to know whether a table was declared `INTEGER PRIMARY KEY
/// AUTOINCREMENT` (that bit lives in codegen/the schema, not here), so
/// AUTOINCREMENT handling is opt-in per instruction instead: when `P5`
/// is nonzero AND `P4` carries the table's name (`P4::Str`), this also
/// consults/bumps `sqlite_sequence` via
/// [`crate::btree::ensure_sqlite_sequence_table`]/[`crate::btree::update_sequence`],
/// taking `max(sqlite_sequence.seq, TableCursor::last() rowid) + 1`
/// (matching stock SQLite: `sqlite_sequence` never regresses even after
/// the row it recorded is deleted). Without `P5`/`P4`, this opcode is
/// plain non-AUTOINCREMENT rowid allocation.
pub fn new_rowid(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let root_page = vm.table_cursor_mut(instr.p1, "NewRowid")?.root_page;
    let db = vm.db()?;
    let mut probe = TableCursor::new(Rc::clone(&db.source), &db.header, root_page);
    let max_from_table = probe
        .last()
        .map_err(|e| ExecError::MalformedInstruction {
            opcode: "NewRowid",
            reason: e.to_string(),
        })?
        .unwrap_or(0);

    let new_rowid = if instr.p5 != 0 {
        let table_name = match &instr.p4 {
            P4::Str(name) => name.clone(),
            other => {
                return Err(ExecError::MalformedInstruction {
                    opcode: "NewRowid",
                    reason: format!(
                        "AUTOINCREMENT requested (P5 nonzero) but P4 is not a table-name string, got {other:?}"
                    ),
                })
            }
        };
        let pager = vm.writer("NewRowid")?;
        let db = vm.db()?;
        let header = db.header;
        let mut pager = pager.borrow_mut();
        let seq_root = btree::ensure_sqlite_sequence_table(&mut pager, &header).map_err(|e| {
            ExecError::MalformedInstruction {
                opcode: "NewRowid",
                reason: e.to_string(),
            }
        })?;
        let mut seq_cursor = TableCursor::new(&*pager, &header, seq_root);
        let mut tracked_seq = 0i64;
        let mut row = seq_cursor
            .first_row()
            .map_err(|e| ExecError::MalformedInstruction {
                opcode: "NewRowid",
                reason: e.to_string(),
            })?;
        while let Some(r) = row {
            let values = decode_record(&r.payload, header.text_encoding).map_err(|e| {
                ExecError::MalformedInstruction {
                    opcode: "NewRowid",
                    reason: e.to_string(),
                }
            })?;
            if let (Some(Value::Text(n)), Some(Value::Integer(seq))) =
                (values.first(), values.get(1))
            {
                if *n == table_name.clone().into() {
                    tracked_seq = *seq;
                    break;
                }
            }
            row = seq_cursor
                .next_row()
                .map_err(|e| ExecError::MalformedInstruction {
                    opcode: "NewRowid",
                    reason: e.to_string(),
                })?;
        }
        let candidate = max_from_table.max(tracked_seq).saturating_add(1);
        btree::update_sequence(&mut pager, &header, &table_name, candidate).map_err(|e| {
            ExecError::MalformedInstruction {
                opcode: "NewRowid",
                reason: e.to_string(),
            }
        })?;
        candidate
    } else {
        max_from_table.saturating_add(1)
    };

    vm.set_register(instr.p2, Value::Integer(new_rowid))?;
    Ok(Step::Next)
}

/// `CreateTable` (#215): allocates a fresh table-b-tree root page,
/// registers it in `sqlite_master`, and bumps the schema cookie — the
/// whole statement in one opcode, per `codegen::create_table`'s doc.
pub fn create_table(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let (name, sql) = match &instr.p4 {
        P4::CreateTable { name, sql } => (name.clone(), sql.clone()),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "CreateTable",
                reason: format!("expected P4::CreateTable, got {other:?}"),
            })
        }
    };
    let pager = vm.writer("CreateTable")?;
    let db = vm.db()?;
    let header = db.header;
    let mut pager = pager.borrow_mut();
    let root_page = btree::create_empty_table_root(&mut pager).map_err(|e| {
        ExecError::MalformedInstruction {
            opcode: "CreateTable",
            reason: e.to_string(),
        }
    })?;
    btree::insert_master_row(
        &mut pager,
        &header,
        &btree::MasterEntry {
            kind: "table".to_string(),
            name: name.clone(),
            tbl_name: name,
            rootpage: root_page,
            sql,
        },
    )
    .map_err(|e| ExecError::MalformedInstruction {
        opcode: "CreateTable",
        reason: e.to_string(),
    })?;
    btree::bump_schema_cookie(&mut pager).map_err(|e| ExecError::MalformedInstruction {
        opcode: "CreateTable",
        reason: e.to_string(),
    })?;
    Ok(Step::Next)
}

/// `CreateView` (#380): registers a `sqlite_master` row with
/// `type = 'view'` and `rootpage = 0` — a view has no b-tree of its own,
/// so unlike [`create_table`] this never allocates a root page.
pub fn create_view(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let (name, sql) = match &instr.p4 {
        P4::CreateView { name, sql } => (name.clone(), sql.clone()),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "CreateView",
                reason: format!("expected P4::CreateView, got {other:?}"),
            })
        }
    };
    let pager = vm.writer("CreateView")?;
    let db = vm.db()?;
    let header = db.header;
    let mut pager = pager.borrow_mut();
    btree::insert_master_row(
        &mut pager,
        &header,
        &btree::MasterEntry {
            kind: "view".to_string(),
            name: name.clone(),
            tbl_name: name,
            rootpage: 0,
            sql,
        },
    )
    .map_err(|e| ExecError::MalformedInstruction {
        opcode: "CreateView",
        reason: e.to_string(),
    })?;
    btree::bump_schema_cookie(&mut pager).map_err(|e| ExecError::MalformedInstruction {
        opcode: "CreateView",
        reason: e.to_string(),
    })?;
    Ok(Step::Next)
}

/// `DropTable` (#215): frees the target table's b-tree pages plus every
/// index on it (cascading), removes the corresponding `sqlite_master`
/// rows, and bumps the schema cookie once.
pub fn drop_table(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let (name, root_page, indexes) = match &instr.p4 {
        P4::DropTable {
            name,
            root_page,
            indexes,
        } => (name.clone(), *root_page, indexes.clone()),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "DropTable",
                reason: format!("expected P4::DropTable, got {other:?}"),
            })
        }
    };
    let pager = vm.writer("DropTable")?;
    let db = vm.db()?;
    let header = db.header;
    let mut pager = pager.borrow_mut();
    for (index_name, index_root) in &indexes {
        btree::free_btree_pages(&mut pager, &header, *index_root).map_err(|e| {
            ExecError::MalformedInstruction {
                opcode: "DropTable",
                reason: e.to_string(),
            }
        })?;
        btree::delete_master_row(&mut pager, &header, index_name).map_err(|e| {
            ExecError::MalformedInstruction {
                opcode: "DropTable",
                reason: e.to_string(),
            }
        })?;
    }
    btree::free_btree_pages(&mut pager, &header, root_page).map_err(|e| {
        ExecError::MalformedInstruction {
            opcode: "DropTable",
            reason: e.to_string(),
        }
    })?;
    btree::delete_master_row(&mut pager, &header, &name).map_err(|e| {
        ExecError::MalformedInstruction {
            opcode: "DropTable",
            reason: e.to_string(),
        }
    })?;
    btree::bump_schema_cookie(&mut pager).map_err(|e| ExecError::MalformedInstruction {
        opcode: "DropTable",
        reason: e.to_string(),
    })?;
    Ok(Step::Next)
}

/// `CreateIndex` (#215): allocates a fresh index-b-tree root page,
/// populates it with one entry per pre-existing row of the target table
/// (see `btree::populate_index_from_table`), registers the index in
/// `sqlite_master`, and bumps the schema cookie.
pub fn create_index(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let (name, table_name, table_root_page, sql, column_indices) = match &instr.p4 {
        P4::CreateIndex {
            name,
            table_name,
            table_root_page,
            sql,
            column_indices,
            ..
        } => (
            name.clone(),
            table_name.clone(),
            *table_root_page,
            sql.clone(),
            column_indices.clone(),
        ),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "CreateIndex",
                reason: format!("expected P4::CreateIndex, got {other:?}"),
            })
        }
    };
    let pager = vm.writer("CreateIndex")?;
    let db = vm.db()?;
    let header = db.header;
    let mut pager = pager.borrow_mut();
    let index_root = btree::create_empty_index_root(&mut pager).map_err(|e| {
        ExecError::MalformedInstruction {
            opcode: "CreateIndex",
            reason: e.to_string(),
        }
    })?;
    btree::populate_index_from_table(
        &mut pager,
        &header,
        table_root_page,
        index_root,
        &column_indices,
    )
    .map_err(|e| ExecError::MalformedInstruction {
        opcode: "CreateIndex",
        reason: e.to_string(),
    })?;
    btree::insert_master_row(
        &mut pager,
        &header,
        &btree::MasterEntry {
            kind: "index".to_string(),
            name: name.clone(),
            tbl_name: table_name,
            rootpage: index_root,
            sql,
        },
    )
    .map_err(|e| ExecError::MalformedInstruction {
        opcode: "CreateIndex",
        reason: e.to_string(),
    })?;
    btree::bump_schema_cookie(&mut pager).map_err(|e| ExecError::MalformedInstruction {
        opcode: "CreateIndex",
        reason: e.to_string(),
    })?;
    Ok(Step::Next)
}

/// `DropIndex` (#215): frees the target index's b-tree pages, removes
/// its `sqlite_master` row, and bumps the schema cookie.
pub fn drop_index(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let (name, root_page) = match &instr.p4 {
        P4::DropIndex { name, root_page } => (name.clone(), *root_page),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "DropIndex",
                reason: format!("expected P4::DropIndex, got {other:?}"),
            })
        }
    };
    let pager = vm.writer("DropIndex")?;
    let db = vm.db()?;
    let header = db.header;
    let mut pager = pager.borrow_mut();
    btree::free_btree_pages(&mut pager, &header, root_page).map_err(|e| {
        ExecError::MalformedInstruction {
            opcode: "DropIndex",
            reason: e.to_string(),
        }
    })?;
    btree::delete_master_row(&mut pager, &header, &name).map_err(|e| {
        ExecError::MalformedInstruction {
            opcode: "DropIndex",
            reason: e.to_string(),
        }
    })?;
    btree::bump_schema_cookie(&mut pager).map_err(|e| ExecError::MalformedInstruction {
        opcode: "DropIndex",
        reason: e.to_string(),
    })?;
    Ok(Step::Next)
}

/// Counts the rows in the table b-tree rooted at `root_page` — a full
/// scan, no sampling (#461's MVP scope; see spec 011).
fn count_table_rows(
    pager: &mut crate::pager::Pager,
    header: &crate::header::DatabaseHeader,
    root_page: u32,
) -> Result<u64, ExecError> {
    let mut cursor = TableCursor::new(&*pager, header, root_page);
    let mut count = 0u64;
    let mut row = cursor
        .first()
        .map_err(|e| ExecError::MalformedInstruction {
            opcode: "Analyze",
            reason: e.to_string(),
        })?;
    while row.is_some() {
        count = count.saturating_add(1);
        row = cursor.next().map_err(|e| ExecError::MalformedInstruction {
            opcode: "Analyze",
            reason: e.to_string(),
        })?;
    }
    Ok(count)
}

/// Walks the index b-tree rooted at `root_page` and returns `(total
/// entries, avg_eq)`, where `avg_eq` is the average number of entries
/// sharing the same leading-column value — real SQLite's `sqlite_stat1`
/// semantics for a single-column index, computed here by an exact
/// full-scan pass (no sampling) counting distinct-value transitions
/// between consecutive entries in key order, rather than stock SQLite's
/// sampled estimate. `avg_eq` is `0` for an empty index.
fn count_index_entries_and_avg_eq(
    pager: &mut crate::pager::Pager,
    header: &crate::header::DatabaseHeader,
    root_page: u32,
) -> Result<(u64, u64), ExecError> {
    let usable_size = header.usable_page_size();
    let mut cursor = IndexCursor::new(&*pager, usable_size, root_page);
    let mut total = 0u64;
    let mut distinct_groups = 0u64;
    let mut prev_leading: Option<Value> = None;
    let mut row = cursor
        .first()
        .map_err(|e| ExecError::MalformedInstruction {
            opcode: "Analyze",
            reason: e.to_string(),
        })?;
    while let Some(r) = row {
        let values = decode_record(&r.payload, header.text_encoding).map_err(|e| {
            ExecError::MalformedInstruction {
                opcode: "Analyze",
                reason: e.to_string(),
            }
        })?;
        let leading = values.first().cloned();
        if prev_leading.as_ref() != leading.as_ref() {
            distinct_groups = distinct_groups.saturating_add(1);
            prev_leading = leading;
        }
        total = total.saturating_add(1);
        row = cursor.next().map_err(|e| ExecError::MalformedInstruction {
            opcode: "Analyze",
            reason: e.to_string(),
        })?;
    }
    let avg_eq = total.checked_div(distinct_groups).unwrap_or(0);
    Ok((total, avg_eq))
}

/// `ANALYZE` (#461, spec 011): populates `sqlite_stat1` for every target
/// table baked into `instr.p4` at codegen time — creating `sqlite_stat1`
/// itself on first use, replacing (not appending to) each target's prior
/// rows, matching stock SQLite's `type = table` row-count row plus one
/// `idx = <index-name>` row per index on that table.
pub fn analyze(vm: &mut Vm, instr: &Instruction) -> Result<Step, ExecError> {
    let targets = match &instr.p4 {
        P4::Analyze { targets } => targets.clone(),
        other => {
            return Err(ExecError::MalformedInstruction {
                opcode: "Analyze",
                reason: format!("expected P4::Analyze, got {other:?}"),
            })
        }
    };
    let pager = vm.writer("Analyze")?;
    let db = vm.db()?;
    let header = db.header;
    let mut pager = pager.borrow_mut();

    let stat1_root = btree::ensure_sqlite_stat1_table(&mut pager, &header).map_err(|e| {
        ExecError::MalformedInstruction {
            opcode: "Analyze",
            reason: e.to_string(),
        }
    })?;

    for target in &targets {
        btree::delete_stat1_rows_for_table(&mut pager, &header, stat1_root, &target.table_name)
            .map_err(|e| ExecError::MalformedInstruction {
                opcode: "Analyze",
                reason: e.to_string(),
            })?;

        let row_count = count_table_rows(&mut pager, &header, target.table_root_page)?;
        btree::insert_stat1_row(
            &mut pager,
            &header,
            stat1_root,
            &target.table_name,
            None,
            &row_count.to_string(),
        )
        .map_err(|e| ExecError::MalformedInstruction {
            opcode: "Analyze",
            reason: e.to_string(),
        })?;

        for index in &target.indexes {
            let (idx_rows, avg_eq) =
                count_index_entries_and_avg_eq(&mut pager, &header, index.root_page)?;
            btree::insert_stat1_row(
                &mut pager,
                &header,
                stat1_root,
                &target.table_name,
                Some(&index.index_name),
                &format!("{idx_rows} {avg_eq}"),
            )
            .map_err(|e| ExecError::MalformedInstruction {
                opcode: "Analyze",
                reason: e.to_string(),
            })?;
        }
    }

    Ok(Step::Next)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;
    use crate::header::DatabaseHeader;
    use crate::vdbe::affinity::Affinity;
    use crate::vdbe::program::Opcode;
    use crate::vfs::{UnixVfs, Vfs, VfsPageSource};
    use std::path::Path;

    fn open_vm(fixture: &str) -> Vm {
        let path = Path::new("tests/corpus/fixtures/btrees").join(fixture);
        let vfs = UnixVfs;
        let file = vfs.open_read(&path).unwrap();
        let mut header_buf = [0u8; 100];
        file.read_at(&mut header_buf, 0).unwrap();
        let header = DatabaseHeader::parse(&header_buf).unwrap();
        let source = VfsPageSource::open(&vfs, &path, header.page_size).unwrap();
        Vm::with_db(Rc::new(source), header)
    }

    /// A one-page, empty-leaf-root database (root page 1 doubling as a
    /// table b-tree root, rather than a real `sqlite_master` page — a
    /// simplification this ticket's tests share with
    /// `src/btree/insert.rs::tests::minimal_db`, whose private
    /// `write_leaf_page` helper isn't reachable from here). `page_type`
    /// is `0x0d` (`LEAF_TABLE`) or `0x0a` (`LEAF_INDEX`) — see
    /// `src/btree/index.rs`'s `LEAF_INDEX` constant.
    fn minimal_writable_db(
        page_size: u32,
        page_type: u8,
    ) -> (crate::vfs::MemoryVfs, DatabaseHeader) {
        let mut page1 = vec![0u8; page_size as usize];
        page1[0..16].copy_from_slice(b"SQLite format 3\0");
        page1[16..18].copy_from_slice(&u16::try_from(page_size).unwrap_or(1).to_be_bytes());
        page1[18] = 1;
        page1[19] = 1;
        page1[28..32].copy_from_slice(&1u32.to_be_bytes());
        page1[56..60].copy_from_slice(&1u32.to_be_bytes());

        let header_start = 100usize;
        page1[header_start] = page_type;
        page1[header_start + 1..header_start + 3].copy_from_slice(&0u16.to_be_bytes());
        page1[header_start + 3..header_start + 5].copy_from_slice(&0u16.to_be_bytes());
        let content_start = if page_size == 65536 {
            0u16
        } else {
            u16::try_from(page_size).unwrap()
        };
        page1[header_start + 5..header_start + 7].copy_from_slice(&content_start.to_be_bytes());
        page1[header_start + 7] = 0;

        let mut header_bytes = [0u8; 100];
        header_bytes.copy_from_slice(&page1[..100]);
        let header = DatabaseHeader::parse(&header_bytes).unwrap();

        let mut vfs = crate::vfs::MemoryVfs::new();
        vfs.insert("/test.db", page1);
        (vfs, header)
    }

    fn writable_vm(page_type: u8) -> Vm {
        let (vfs, header) = minimal_writable_db(512, page_type);
        let pager = crate::pager::Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        Vm::with_writable_db(pager, header)
    }

    #[test]
    fn full_scan_opens_rewinds_iterates_reads() {
        let mut vm = open_vm("table_multipage.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();

        let step = rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 999, 0)).unwrap();
        assert_eq!(step, Step::Next);

        let mut rowids = Vec::new();
        loop {
            rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 10, 0)).unwrap();
            rowids.push(vm.register(10).unwrap().clone());
            column(&mut vm, &Instruction::new(Opcode::Column, 0, 1, 11)).unwrap();

            match next(&mut vm, &Instruction::new(Opcode::Next, 0, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }

        assert_eq!(rowids.len(), 3000);
        assert_eq!(rowids[0], Value::Integer(1));
        assert_eq!(rowids[2999], Value::Integer(3000));
        assert_eq!(
            *vm.register(11).unwrap(),
            Value::Text("row number 3000".to_string().into())
        );
    }

    #[test]
    fn seek_rowid_jumps_to_p2_when_the_target_rowid_is_absent() {
        let mut vm = open_vm("table_single_page.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();
        vm.set_register(5, Value::Integer(999)).unwrap();
        let step = seek_rowid(&mut vm, &Instruction::new(Opcode::SeekRowid, 0, 42, 5)).unwrap();
        assert_eq!(step, Step::Jump(42));
    }

    #[test]
    fn seek_rowid_skips_full_scan_on_pk_equality() {
        let mut vm = open_vm("table_multipage.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();
        vm.set_register(5, Value::Integer(1500)).unwrap();
        let step = seek_rowid(&mut vm, &Instruction::new(Opcode::SeekRowid, 0, 42, 5)).unwrap();
        assert_eq!(step, Step::Next);
        rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 10, 0)).unwrap();
        assert_eq!(*vm.register(10).unwrap(), Value::Integer(1500));
    }

    #[test]
    fn null_row_forces_all_null_reads_until_repositioned() {
        let mut vm = open_vm("table_multipage.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();
        rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 999, 0)).unwrap();
        null_row(&mut vm, &Instruction::new(Opcode::NullRow, 0, 0, 0)).unwrap();

        column(&mut vm, &Instruction::new(Opcode::Column, 0, 1, 11)).unwrap();
        assert_eq!(*vm.register(11).unwrap(), Value::Null);
        rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 12, 0)).unwrap();
        assert_eq!(*vm.register(12).unwrap(), Value::Null);
    }

    #[test]
    fn distinct_probes_ephemeral_index_before_emit() {
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 1, 0)).unwrap();

        // Row "a": not found, insert, passes through.
        vm.set_register(0, Value::Text("a".to_string().into()))
            .unwrap();
        let found_a = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 99, 0, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(found_a, Step::Next);
        idx_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxInsert, 0, 0, 0, P4::Int(1)),
        )
        .unwrap();

        // Row "a" again: found, discard (the DISTINCT dedup path).
        let found_a_again = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 99, 0, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(found_a_again, Step::Jump(99));

        // Row "b": not found, insert, passes through.
        vm.set_register(0, Value::Text("b".to_string().into()))
            .unwrap();
        let found_b = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 99, 0, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(found_b, Step::Next);
    }

    #[test]
    fn distinct_treats_two_nulls_as_equal_unlike_the_eq_operator() {
        // DISTINCT's ephemeral-index dedup is exact-byte record equality,
        // not SQL's `=` — two NULL rows collapse to one here (spec 008's
        // three-valued logic says `NULL = NULL` is UNKNOWN, never true;
        // spec 009 Requirement 9's ORDER BY default NULL placement is a
        // third, independent rule again). See spec 009 Requirement 9's
        // "NULL is comparison-distinct across `=`, DISTINCT, and ORDER BY"
        // scenario (#146).
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 1, 0)).unwrap();

        // Row NULL: not found, insert, passes through.
        vm.set_register(0, Value::Null).unwrap();
        let found_first_null = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 99, 0, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(found_first_null, Step::Next);
        idx_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxInsert, 0, 0, 0, P4::Int(1)),
        )
        .unwrap();

        // Row NULL again: found, discard — NULL is equal to NULL for
        // DISTINCT's dedup, unlike `=`.
        vm.set_register(0, Value::Null).unwrap();
        let found_second_null = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 99, 0, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(found_second_null, Step::Jump(99));
    }

    #[test]
    fn sequence_hands_out_a_monotonic_counter_independent_of_the_dedup_key() {
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 1, 0)).unwrap();
        sequence(&mut vm, &Instruction::new(Opcode::Sequence, 0, 5, 0)).unwrap();
        assert_eq!(*vm.register(5).unwrap(), Value::Integer(0));
        sequence(&mut vm, &Instruction::new(Opcode::Sequence, 0, 6, 0)).unwrap();
        assert_eq!(*vm.register(6).unwrap(), Value::Integer(1));
    }

    #[test]
    fn delete_removes_the_just_probed_duplicate_row() {
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 1, 0)).unwrap();
        vm.set_register(0, Value::Text("a".to_string().into()))
            .unwrap();
        idx_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxInsert, 0, 0, 0, P4::Int(1)),
        )
        .unwrap();
        found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 99, 0, P4::Int(1)),
        )
        .unwrap();
        delete(&mut vm, &Instruction::new(Opcode::Delete, 0, 0, 0)).unwrap();

        let found_again = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 99, 0, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(found_again, Step::Next);
    }

    #[test]
    fn cursor_type_mismatch_errors_instead_of_panicking() {
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 1, 0)).unwrap();
        let err = rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 5, 0)).unwrap_err();
        assert!(matches!(err, ExecError::CursorTypeMismatch { .. }));
    }

    #[test]
    fn unopened_cursor_slot_errors_instead_of_panicking() {
        let mut vm = Vm::new();
        let err = rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 5, 0)).unwrap_err();
        assert!(matches!(err, ExecError::CursorNotOpen { slot: 0 }));
    }

    // --- #194: write-path opcodes (OpenWrite/Insert/Delete/IdxInsert/NewRowid) ---

    #[test]
    fn open_write_requires_a_writable_vm() {
        // A read-only `Vm::with_db` must reject `OpenWrite` rather than
        // silently opening a cursor that later opcodes can't actually
        // write through.
        let mut vm = open_vm("table_multipage.db");
        let err = open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 2, 0)).unwrap_err();
        assert!(matches!(err, ExecError::NoDatabase { .. }));
    }

    #[test]
    fn open_write_with_p5_set_opens_an_index_write_cursor() {
        let mut vm = writable_vm(0x0d); // LEAF_TABLE
        let mut instr = Instruction::new(Opcode::OpenWrite, 0, 7, 0);
        instr.p5 = 1;
        open_write(&mut vm, &instr).unwrap();
        assert!(matches!(
            vm.cursor(0).unwrap(),
            CursorSlot::IndexWrite { root_page: 7 }
        ));
    }

    #[test]
    fn new_rowid_starts_at_one_on_an_empty_table() {
        let mut vm = writable_vm(0x0d); // LEAF_TABLE
        open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 1, 0)).unwrap();
        new_rowid(&mut vm, &Instruction::new(Opcode::NewRowid, 0, 5, 0)).unwrap();
        assert_eq!(*vm.register(5).unwrap(), Value::Integer(1));
    }

    #[test]
    fn insert_then_read_back_round_trips_through_make_record_and_column() {
        let mut vm = writable_vm(0x0d); // LEAF_TABLE
        open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 1, 0)).unwrap();

        // NewRowid -> r0.
        new_rowid(&mut vm, &Instruction::new(Opcode::NewRowid, 0, 0, 0)).unwrap();
        assert_eq!(*vm.register(0).unwrap(), Value::Integer(1));

        // MakeRecord over r1..r3 (with INTEGER/TEXT affinity applied to
        // text-literal-but-numeric-looking input in r1) -> r3.
        vm.set_register(1, Value::Text("42".to_string().into()))
            .unwrap();
        vm.set_register(2, Value::Text("hello".to_string().into()))
            .unwrap();
        crate::vdbe::result::make_record(
            &mut vm,
            &Instruction::with_p4(
                Opcode::MakeRecord,
                1,
                2,
                3,
                P4::Affinity(vec![
                    Affinity::Integer.to_p4_byte(),
                    Affinity::Text.to_p4_byte(),
                ]),
            ),
        )
        .unwrap();
        assert!(matches!(vm.register(3).unwrap(), Value::Blob(_)));

        // Insert cursor 0, rowid r0, record r3.
        insert(&mut vm, &Instruction::new(Opcode::Insert, 0, 0, 3)).unwrap();

        // Read back through the same cursor's Rewind/Column — V1's
        // reader path (`decode_record`) must decode exactly what was
        // written, with the affinity-coerced INTEGER, not the original
        // TEXT "42".
        rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 999, 0)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 10)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 1, 11)).unwrap();
        assert_eq!(*vm.register(10).unwrap(), Value::Integer(42));
        assert_eq!(
            *vm.register(11).unwrap(),
            Value::Text("hello".to_string().into())
        );
        rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 12, 0)).unwrap();
        assert_eq!(*vm.register(12).unwrap(), Value::Integer(1));
    }

    #[test]
    fn new_rowid_after_insert_skips_past_the_max_existing_rowid() {
        let mut vm = writable_vm(0x0d);
        open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 1, 0)).unwrap();
        vm.set_register(1, Value::Integer(7)).unwrap();
        crate::vdbe::result::make_record(&mut vm, &Instruction::new(Opcode::MakeRecord, 1, 1, 2))
            .unwrap();
        vm.set_register(0, Value::Integer(5)).unwrap();
        insert(&mut vm, &Instruction::new(Opcode::Insert, 0, 0, 2)).unwrap();

        new_rowid(&mut vm, &Instruction::new(Opcode::NewRowid, 0, 9, 0)).unwrap();
        assert_eq!(*vm.register(9).unwrap(), Value::Integer(6));
    }

    #[test]
    fn delete_removes_the_row_at_the_cursors_current_position() {
        let mut vm = writable_vm(0x0d);
        open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 1, 0)).unwrap();
        vm.set_register(1, Value::Integer(99)).unwrap();
        crate::vdbe::result::make_record(&mut vm, &Instruction::new(Opcode::MakeRecord, 1, 1, 2))
            .unwrap();
        vm.set_register(0, Value::Integer(1)).unwrap();
        insert(&mut vm, &Instruction::new(Opcode::Insert, 0, 0, 2)).unwrap();

        rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 999, 0)).unwrap();
        delete(&mut vm, &Instruction::new(Opcode::Delete, 0, 0, 0)).unwrap();

        let step = rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 999, 0)).unwrap();
        assert_eq!(step, Step::Jump(999));
    }

    #[test]
    fn new_rowid_autoincrement_consults_and_bumps_sqlite_sequence() {
        let mut vm = writable_vm(0x0d);
        open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 1, 0)).unwrap();

        let mut instr = Instruction::with_p4(Opcode::NewRowid, 0, 5, 0, P4::Str("t".to_string()));
        instr.p5 = 1;
        new_rowid(&mut vm, &instr).unwrap();
        assert_eq!(*vm.register(5).unwrap(), Value::Integer(1));

        // sqlite_sequence now tracks ("t", 1); a second NewRowid call
        // (simulating a second INSERT without actually inserting a row
        // in between, which this focused test doesn't need) must not
        // regress below the tracked value.
        new_rowid(&mut vm, &instr).unwrap();
        assert_eq!(*vm.register(5).unwrap(), Value::Integer(2));
    }

    #[test]
    fn idx_insert_real_cursor_writes_an_index_entry_readable_by_index_cursor() {
        let mut vm = writable_vm(0x0a); // LEAF_INDEX
        let mut open_instr = Instruction::new(Opcode::OpenWrite, 0, 1, 0);
        open_instr.p5 = 1; // nonzero P5: open a real index write cursor
        open_write(&mut vm, &open_instr).unwrap();

        vm.set_register(0, Value::Integer(5)).unwrap();
        vm.set_register(1, Value::Text("x".to_string().into()))
            .unwrap();
        idx_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxInsert, 0, 0, 0, P4::Int(2)),
        )
        .unwrap();

        let db = vm.db().unwrap();
        let mut index_cursor =
            crate::btree::IndexCursor::new(Rc::clone(&db.source), db.header.usable_page_size(), 1);
        let row = index_cursor.first().unwrap().unwrap();
        let values = decode_record(&row.payload, TextEncoding::Utf8).unwrap();
        assert_eq!(
            values,
            vec![Value::Integer(5), Value::Text("x".to_string().into())]
        );
    }

    #[test]
    fn idx_delete_real_cursor_removes_an_index_entry() {
        let mut vm = writable_vm(0x0a); // LEAF_INDEX
        let mut open_instr = Instruction::new(Opcode::OpenWrite, 0, 1, 0);
        open_instr.p5 = 1; // nonzero P5: open a real index write cursor
        open_write(&mut vm, &open_instr).unwrap();

        vm.set_register(0, Value::Integer(5)).unwrap();
        vm.set_register(1, Value::Text("x".to_string().into()))
            .unwrap();
        idx_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxInsert, 0, 0, 0, P4::Int(2)),
        )
        .unwrap();

        idx_delete(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxDelete, 0, 0, 0, P4::Int(2)),
        )
        .unwrap();

        let db = vm.db().unwrap();
        let mut index_cursor =
            crate::btree::IndexCursor::new(Rc::clone(&db.source), db.header.usable_page_size(), 1);
        assert!(index_cursor.first().unwrap().is_none());
    }

    #[test]
    fn no_conflict_falls_through_and_reports_the_rowid_when_the_key_already_exists() {
        let mut vm = writable_vm(0x0a); // LEAF_INDEX
        let mut open_instr = Instruction::new(Opcode::OpenWrite, 0, 1, 0);
        open_instr.p5 = 1; // nonzero P5: open a real index write cursor
        open_write(&mut vm, &open_instr).unwrap();

        // One index entry: column value "v1", trailing rowid 42.
        vm.set_register(0, Value::Text("v1".to_string().into()))
            .unwrap();
        vm.set_register(1, Value::Integer(42)).unwrap();
        idx_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxInsert, 0, 0, 0, P4::Int(2)),
        )
        .unwrap();

        // Probe with just the column value (no trailing rowid) at
        // register 5, reserving register 6 for the conflicting rowid.
        vm.set_register(5, Value::Text("v1".to_string().into()))
            .unwrap();
        let step = no_conflict(
            &mut vm,
            &Instruction::with_p4(Opcode::NoConflict, 0, 999, 5, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Next, "a matching entry must not jump");
        assert_eq!(*vm.register(6).unwrap(), Value::Integer(42));
    }

    #[test]
    fn no_conflict_jumps_to_p2_when_no_matching_key_exists() {
        let mut vm = writable_vm(0x0a); // LEAF_INDEX
        let mut open_instr = Instruction::new(Opcode::OpenWrite, 0, 1, 0);
        open_instr.p5 = 1;
        open_write(&mut vm, &open_instr).unwrap();

        vm.set_register(0, Value::Text("v1".to_string().into()))
            .unwrap();
        vm.set_register(1, Value::Integer(42)).unwrap();
        idx_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxInsert, 0, 0, 0, P4::Int(2)),
        )
        .unwrap();

        vm.set_register(5, Value::Text("v2".to_string().into()))
            .unwrap();
        let step = no_conflict(
            &mut vm,
            &Instruction::with_p4(Opcode::NoConflict, 0, 999, 5, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Jump(999));
    }

    #[test]
    fn idx_delete_ephemeral_cursor_removes_the_entry() {
        let mut vm = writable_vm(0x0d);
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 0, 0)).unwrap();

        vm.set_register(0, Value::Integer(5)).unwrap();
        idx_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxInsert, 0, 0, 0, P4::Int(1)),
        )
        .unwrap();
        let found_step = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 999, 0, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(found_step, Step::Jump(999));

        idx_delete(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxDelete, 0, 0, 0, P4::Int(1)),
        )
        .unwrap();

        let step = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 999, 0, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Next);
    }

    // --- additional coverage: error branches, Last, OpenPseudo, IdxLE,
    // CreateTable/DropTable/CreateIndex/DropIndex, type_name(). ---

    #[test]
    fn cursor_slot_type_name_reports_every_variant() {
        assert_eq!(
            CursorSlot::IndexWrite { root_page: 1 }.type_name(),
            "index write cursor"
        );
        assert_eq!(
            CursorSlot::Pseudo {
                register: 0,
                header_cache: RowHeaderCache::default(),
                cached_blob: None,
            }
            .type_name(),
            "pseudo cursor"
        );
    }

    #[test]
    fn table_cursor_state_debug_reports_key_fields() {
        let mut vm = writable_vm(0x0d); // LEAF_TABLE
        open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 1, 0)).unwrap();
        let CursorSlot::Table(state) = vm.cursor(0).unwrap() else {
            panic!("expected a table cursor");
        };
        let debug = format!("{state:?}");
        assert!(debug.contains("TableCursorState"));
        assert!(debug.contains("current_rowid"));
    }

    #[test]
    fn index_read_state_debug_reports_key_fields() {
        let mut vm = writable_vm_with_index_entries(1);
        open_index_read(&mut vm, 0);
        let CursorSlot::IndexRead(state) = vm.cursor(0).unwrap() else {
            panic!("expected an index-read cursor");
        };
        let debug = format!("{state:?}");
        assert!(debug.contains("IndexReadState"));
        assert!(debug.contains("root_page"));
    }

    #[test]
    fn last_positions_at_the_highest_rowid_and_jumps_when_empty() {
        let mut vm = open_vm("table_multipage.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();
        let step = last(&mut vm, &Instruction::new(Opcode::Last, 0, 999, 0)).unwrap();
        assert_eq!(step, Step::Next);
        rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 10, 0)).unwrap();
        assert_eq!(*vm.register(10).unwrap(), Value::Integer(3000));
    }

    #[test]
    fn last_jumps_to_p2_on_an_empty_table() {
        let mut vm = writable_vm(0x0d);
        open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 1, 0)).unwrap();
        let step = last(&mut vm, &Instruction::new(Opcode::Last, 0, 999, 0)).unwrap();
        assert_eq!(step, Step::Jump(999));
    }

    #[test]
    fn ephemeral_type_mismatch_errors_instead_of_panicking() {
        let mut vm = open_vm("table_multipage.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();
        let err = sequence(&mut vm, &Instruction::new(Opcode::Sequence, 0, 5, 0)).unwrap_err();
        assert!(matches!(err, ExecError::CursorTypeMismatch { .. }));
    }

    #[test]
    fn p4_count_rejects_a_negative_int_and_a_non_int_p4() {
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 0, 0)).unwrap();
        let err = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 99, 0, P4::Int(-1)),
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));

        let err = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 99, 0, P4::Bool(true)),
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn column_reads_through_an_open_pseudo_cursor() {
        let mut vm = Vm::new();
        vm.set_register(3, Value::Integer(7)).unwrap();
        vm.set_register(4, Value::Text("hi".to_string().into()))
            .unwrap();
        crate::vdbe::result::make_record(&mut vm, &Instruction::new(Opcode::MakeRecord, 3, 2, 5))
            .unwrap();
        open_pseudo(&mut vm, &Instruction::new(Opcode::OpenPseudo, 0, 5, 0)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 10)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 1, 11)).unwrap();
        assert_eq!(*vm.register(10).unwrap(), Value::Integer(7));
        assert_eq!(
            *vm.register(11).unwrap(),
            Value::Text("hi".to_string().into())
        );
    }

    #[test]
    fn column_on_pseudo_cursor_with_non_blob_register_errors() {
        let mut vm = Vm::new();
        vm.set_register(5, Value::Integer(1)).unwrap();
        open_pseudo(&mut vm, &Instruction::new(Opcode::OpenPseudo, 0, 5, 0)).unwrap();
        let err = column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 10)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn repeated_column_reads_on_the_same_row_reuse_the_header_cache() {
        // Reads both columns of the same row twice each, in a scrambled
        // order — the second read of a column must return the same value
        // as the first (proving the cached header, populated on the very
        // first `Column` call for this row, is being reused rather than
        // silently ignored or corrupted across repeated lookups).
        let mut vm = open_vm("table_multipage.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();
        rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 999, 0)).unwrap();

        column(&mut vm, &Instruction::new(Opcode::Column, 0, 1, 10)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 11)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 1, 12)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 13)).unwrap();

        assert_eq!(*vm.register(11).unwrap(), Value::Integer(1));
        assert_eq!(*vm.register(13).unwrap(), Value::Integer(1));
        assert_eq!(
            *vm.register(10).unwrap(),
            Value::Text("row number 1".to_string().into())
        );
        assert_eq!(vm.register(10).unwrap(), vm.register(12).unwrap());
    }

    #[test]
    fn next_invalidates_the_header_cache_so_column_reads_the_new_row() {
        let mut vm = open_vm("table_multipage.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();
        rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 999, 0)).unwrap();

        // Populate row 1's header cache, then advance — if `Next` failed
        // to invalidate it, this read would wrongly reuse row 1's offsets
        // against row 2's payload.
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 10)).unwrap();
        next(&mut vm, &Instruction::new(Opcode::Next, 0, 1, 0)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 11)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 1, 12)).unwrap();

        assert_eq!(*vm.register(10).unwrap(), Value::Integer(1));
        assert_eq!(*vm.register(11).unwrap(), Value::Integer(2));
        assert_eq!(
            *vm.register(12).unwrap(),
            Value::Text("row number 2".to_string().into())
        );
    }

    #[test]
    fn seek_rowid_invalidates_the_header_cache() {
        let mut vm = open_vm("table_multipage.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();
        rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 999, 0)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 1, 10)).unwrap();

        vm.set_register(5, Value::Integer(1500)).unwrap();
        seek_rowid(&mut vm, &Instruction::new(Opcode::SeekRowid, 0, 42, 5)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 1, 11)).unwrap();

        assert_eq!(
            *vm.register(11).unwrap(),
            Value::Text("row number 1500".to_string().into())
        );
    }

    #[test]
    fn column_on_a_cursor_with_no_current_row_errors() {
        let mut vm = open_vm("table_multipage.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();
        let err = column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 10)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn column_on_an_ephemeral_cursor_with_no_current_entry_errors() {
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 0, 0)).unwrap();
        let err = column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 10)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn column_on_an_ephemeral_table_cursor_with_no_current_row_errors() {
        let mut vm = Vm::new();
        let mut open_instr = Instruction::new(Opcode::OpenEphemeral, 0, 0, 0);
        open_instr.p5 = 1; // ephemeral table, not ephemeral index
        open_ephemeral(&mut vm, &open_instr).unwrap();
        let err = column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 10)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn column_on_an_auto_index_cursor_is_a_type_mismatch() {
        let mut vm = Vm::new();
        open_auto_index(&mut vm, 0);
        let err = column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 10)).unwrap_err();
        assert!(matches!(err, ExecError::CursorTypeMismatch { .. }));
    }

    #[test]
    fn idx_insert_ephemeral_cursor_stores_extra_p5_payload_readable_via_column_after_found() {
        // #494: key = 1 register (P4::Int(1)), plus 1 extra payload
        // register (P5) that isn't part of the lookup key.
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 0, 0)).unwrap();
        vm.set_register(1, Value::Integer(42)).unwrap();
        vm.set_register(2, Value::Text("result".to_string().into()))
            .unwrap();
        idx_insert(
            &mut vm,
            &Instruction {
                opcode: Opcode::IdxInsert,
                p1: 0,
                p2: 1,
                p3: 0,
                p4: P4::Int(1),
                p5: 1,
            },
        )
        .unwrap();

        vm.set_register(3, Value::Integer(42)).unwrap();
        let step = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 99, 3, P4::Int(1)),
        )
        .unwrap();
        assert!(matches!(step, Step::Jump(pc) if pc == to_pc(99)));

        let mut out = column(&mut vm, &Instruction::new(Opcode::Column, 0, 1, 10)).unwrap();
        assert!(matches!(out, Step::Next));
        assert_eq!(
            vm.register(10).unwrap().clone(),
            Value::Text("result".to_string().into())
        );

        // A miss for a probe value never inserted stays a miss.
        vm.set_register(4, Value::Integer(7)).unwrap();
        out = found(
            &mut vm,
            &Instruction::with_p4(Opcode::Found, 0, 99, 4, P4::Int(1)),
        )
        .unwrap();
        assert!(matches!(out, Step::Next));
    }

    #[test]
    fn rowid_on_a_cursor_with_no_current_row_errors() {
        let mut vm = open_vm("table_multipage.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();
        let err = rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 10, 0)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn seek_rowid_rejects_a_non_integer_target_register() {
        let mut vm = open_vm("table_multipage.db");
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 0, 2, 0)).unwrap();
        vm.set_register(5, Value::Text("nope".to_string().into()))
            .unwrap();
        let err = seek_rowid(&mut vm, &Instruction::new(Opcode::SeekRowid, 0, 42, 5)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn insert_rejects_a_non_integer_rowid_register() {
        let mut vm = writable_vm(0x0d);
        open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 1, 0)).unwrap();
        vm.set_register(0, Value::Text("nope".to_string().into()))
            .unwrap();
        vm.set_register(3, Value::Blob(vec![].into())).unwrap();
        let err = insert(&mut vm, &Instruction::new(Opcode::Insert, 0, 0, 3)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn insert_rejects_a_non_blob_record_register() {
        let mut vm = writable_vm(0x0d);
        open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 1, 0)).unwrap();
        vm.set_register(0, Value::Integer(1)).unwrap();
        vm.set_register(3, Value::Integer(2)).unwrap();
        let err = insert(&mut vm, &Instruction::new(Opcode::Insert, 0, 0, 3)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn delete_on_a_table_cursor_with_no_current_row_errors() {
        let mut vm = writable_vm(0x0d);
        open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 1, 0)).unwrap();
        let err = delete(&mut vm, &Instruction::new(Opcode::Delete, 0, 0, 0)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn delete_on_a_pseudo_cursor_is_a_type_mismatch() {
        let mut vm = Vm::new();
        vm.set_register(0, Value::Integer(1)).unwrap();
        open_pseudo(&mut vm, &Instruction::new(Opcode::OpenPseudo, 0, 0, 0)).unwrap();
        let err = delete(&mut vm, &Instruction::new(Opcode::Delete, 0, 0, 0)).unwrap_err();
        assert!(matches!(err, ExecError::CursorTypeMismatch { .. }));
    }

    #[test]
    fn idx_insert_on_a_pseudo_cursor_is_a_type_mismatch() {
        let mut vm = Vm::new();
        vm.set_register(0, Value::Integer(1)).unwrap();
        open_pseudo(&mut vm, &Instruction::new(Opcode::OpenPseudo, 0, 0, 0)).unwrap();
        let err = idx_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxInsert, 0, 0, 0, P4::Int(1)),
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::CursorTypeMismatch { .. }));
    }

    #[test]
    fn idx_delete_on_a_pseudo_cursor_is_a_type_mismatch() {
        let mut vm = Vm::new();
        vm.set_register(0, Value::Integer(1)).unwrap();
        open_pseudo(&mut vm, &Instruction::new(Opcode::OpenPseudo, 0, 0, 0)).unwrap();
        let err = idx_delete(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxDelete, 0, 0, 0, P4::Int(1)),
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::CursorTypeMismatch { .. }));
    }

    #[test]
    fn idx_le_holds_vacuously_before_any_probe_and_tracks_the_last_key() {
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 0, 0)).unwrap();
        vm.set_register(0, Value::Integer(5)).unwrap();
        // No probe/insert yet: `last_key` is None, so IdxLE holds
        // vacuously and jumps.
        let step = idx_le(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxLE, 0, 99, 0, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Jump(99));

        idx_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxInsert, 0, 0, 0, P4::Int(1)),
        )
        .unwrap();

        // Probe with a larger key: last_key (5) <= probe (10) holds.
        vm.set_register(0, Value::Integer(10)).unwrap();
        let step = idx_le(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxLE, 0, 99, 0, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Jump(99));

        // Probe with a smaller key: last_key (5) <= probe (2) does not hold.
        vm.set_register(0, Value::Integer(2)).unwrap();
        let step = idx_le(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxLE, 0, 99, 0, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Next);
    }

    #[test]
    fn new_rowid_autoincrement_rejects_a_non_str_p4() {
        let mut vm = writable_vm(0x0d);
        open_write(&mut vm, &Instruction::new(Opcode::OpenWrite, 0, 1, 0)).unwrap();
        let mut instr = Instruction::new(Opcode::NewRowid, 0, 5, 0);
        instr.p5 = 1;
        let err = new_rowid(&mut vm, &instr).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn create_table_then_drop_table_round_trip_through_sqlite_master() {
        let mut vm = writable_vm(0x0d);
        create_table(
            &mut vm,
            &Instruction::with_p4(
                Opcode::CreateTable,
                0,
                0,
                0,
                P4::CreateTable {
                    name: "t".to_string(),
                    sql: "CREATE TABLE t (a)".to_string(),
                },
            ),
        )
        .unwrap();

        // The new table's root page is now registered in sqlite_master
        // (page 1) -- read it back to prove CreateTable actually wrote it.
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 1, 1, 0)).unwrap();
        rewind(&mut vm, &Instruction::new(Opcode::Rewind, 1, 999, 0)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 1, 0, 20)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 1, 1, 21)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 1, 3, 22)).unwrap();
        assert_eq!(
            *vm.register(20).unwrap(),
            Value::Text("table".to_string().into())
        );
        assert_eq!(
            *vm.register(21).unwrap(),
            Value::Text("t".to_string().into())
        );
        let root_page = match vm.register(22).unwrap() {
            Value::Integer(n) => u32::try_from(*n).unwrap(),
            other => panic!("expected integer rootpage, got {other:?}"),
        };

        drop_table(
            &mut vm,
            &Instruction::with_p4(
                Opcode::DropTable,
                0,
                0,
                0,
                P4::DropTable {
                    name: "t".to_string(),
                    root_page,
                    indexes: vec![],
                },
            ),
        )
        .unwrap();

        // sqlite_master is now empty again.
        let step = rewind(&mut vm, &Instruction::new(Opcode::Rewind, 1, 999, 0)).unwrap();
        assert_eq!(step, Step::Jump(999));
    }

    #[test]
    fn create_index_then_drop_index_round_trip_through_sqlite_master() {
        let mut vm = writable_vm(0x0d);
        create_table(
            &mut vm,
            &Instruction::with_p4(
                Opcode::CreateTable,
                0,
                0,
                0,
                P4::CreateTable {
                    name: "t".to_string(),
                    sql: "CREATE TABLE t (a)".to_string(),
                },
            ),
        )
        .unwrap();
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 1, 1, 0)).unwrap();
        rewind(&mut vm, &Instruction::new(Opcode::Rewind, 1, 999, 0)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 1, 3, 22)).unwrap();
        let table_root = match vm.register(22).unwrap() {
            Value::Integer(n) => u32::try_from(*n).unwrap(),
            other => panic!("expected integer rootpage, got {other:?}"),
        };

        create_index(
            &mut vm,
            &Instruction::with_p4(
                Opcode::CreateIndex,
                0,
                0,
                0,
                P4::CreateIndex {
                    name: "idx".to_string(),
                    table_name: "t".to_string(),
                    table_root_page: table_root,
                    sql: "CREATE INDEX idx ON t (a)".to_string(),
                    column_indices: vec![0],
                    unique: false,
                },
            ),
        )
        .unwrap();

        // sqlite_master now has two rows: the table and the index.
        let mut names = Vec::new();
        let step = rewind(&mut vm, &Instruction::new(Opcode::Rewind, 1, 999, 0)).unwrap();
        assert_eq!(step, Step::Next);
        let mut index_root_for_drop = None;
        loop {
            column(&mut vm, &Instruction::new(Opcode::Column, 1, 0, 30)).unwrap();
            column(&mut vm, &Instruction::new(Opcode::Column, 1, 1, 31)).unwrap();
            column(&mut vm, &Instruction::new(Opcode::Column, 1, 3, 32)).unwrap();
            names.push((
                vm.register(30).unwrap().clone(),
                vm.register(31).unwrap().clone(),
            ));
            if vm.register(30).unwrap() == &Value::Text("index".to_string().into()) {
                if let Value::Integer(n) = vm.register(32).unwrap() {
                    index_root_for_drop = Some(u32::try_from(*n).unwrap());
                }
            }
            match next(&mut vm, &Instruction::new(Opcode::Next, 1, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }
        assert_eq!(
            names,
            vec![
                (
                    Value::Text("table".to_string().into()),
                    Value::Text("t".to_string().into())
                ),
                (
                    Value::Text("index".to_string().into()),
                    Value::Text("idx".to_string().into())
                ),
            ]
        );

        drop_index(
            &mut vm,
            &Instruction::with_p4(
                Opcode::DropIndex,
                0,
                0,
                0,
                P4::DropIndex {
                    name: "idx".to_string(),
                    root_page: index_root_for_drop.unwrap(),
                },
            ),
        )
        .unwrap();

        // After DropIndex, only the table row remains.
        let mut remaining = Vec::new();
        let step = rewind(&mut vm, &Instruction::new(Opcode::Rewind, 1, 999, 0)).unwrap();
        assert_eq!(step, Step::Next);
        loop {
            column(&mut vm, &Instruction::new(Opcode::Column, 1, 1, 40)).unwrap();
            remaining.push(vm.register(40).unwrap().clone());
            match next(&mut vm, &Instruction::new(Opcode::Next, 1, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }
        assert_eq!(remaining, vec![Value::Text("t".to_string().into())]);
    }

    #[test]
    fn create_table_rejects_a_mismatched_p4() {
        let mut vm = writable_vm(0x0d);
        let err =
            create_table(&mut vm, &Instruction::new(Opcode::CreateTable, 0, 0, 0)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn drop_table_rejects_a_mismatched_p4() {
        let mut vm = writable_vm(0x0d);
        let err = drop_table(&mut vm, &Instruction::new(Opcode::DropTable, 0, 0, 0)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn create_index_rejects_a_mismatched_p4() {
        let mut vm = writable_vm(0x0d);
        let err =
            create_index(&mut vm, &Instruction::new(Opcode::CreateIndex, 0, 0, 0)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn drop_index_rejects_a_mismatched_p4() {
        let mut vm = writable_vm(0x0d);
        let err = drop_index(&mut vm, &Instruction::new(Opcode::DropIndex, 0, 0, 0)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    fn open_ephemeral_table(vm: &mut Vm, cursor: i32) {
        open_ephemeral(
            vm,
            &Instruction {
                opcode: Opcode::OpenEphemeral,
                p1: cursor,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 1,
            },
        )
        .unwrap();
    }

    fn insert_ephemeral_row(vm: &mut Vm, cursor: i32, rowid: i64, values: &[Value]) {
        for (i, v) in values.iter().enumerate() {
            vm.set_register(20i32.saturating_add(i as i32), v.clone())
                .unwrap();
        }
        crate::vdbe::result::make_record(
            vm,
            &Instruction::new(Opcode::MakeRecord, 20, values.len() as i32, 30),
        )
        .unwrap();
        vm.set_register(31, Value::Integer(rowid)).unwrap();
        insert(vm, &Instruction::new(Opcode::Insert, cursor, 31, 30)).unwrap();
    }

    #[test]
    fn ephemeral_table_insert_errors_once_row_limit_exceeded() {
        let mut vm = Vm::new();
        open_ephemeral_table(&mut vm, 0);
        for i in 0..MAX_EPHEMERAL_ROWS as i64 {
            insert_ephemeral_row(&mut vm, 0, i + 1, &[Value::Integer(i)]);
        }
        for (i, v) in [Value::Integer(999)].iter().enumerate() {
            vm.set_register(20i32.saturating_add(i as i32), v.clone())
                .unwrap();
        }
        crate::vdbe::result::make_record(&mut vm, &Instruction::new(Opcode::MakeRecord, 20, 1, 30))
            .unwrap();
        vm.set_register(31, Value::Integer(MAX_EPHEMERAL_ROWS as i64 + 1))
            .unwrap();
        let err = insert(&mut vm, &Instruction::new(Opcode::Insert, 0, 31, 30)).unwrap_err();
        assert!(matches!(
            err,
            ExecError::EphemeralRowLimitExceeded {
                opcode: "Insert",
                limit
            } if limit == MAX_EPHEMERAL_ROWS
        ));
    }

    #[test]
    fn ephemeral_index_insert_errors_once_row_limit_exceeded() {
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 0, 0)).unwrap();
        for i in 0..MAX_EPHEMERAL_ROWS as i64 {
            vm.set_register(20, Value::Integer(i)).unwrap();
            idx_insert(
                &mut vm,
                &Instruction::with_p4(Opcode::IdxInsert, 0, 20, 0, P4::Int(1)),
            )
            .unwrap();
        }
        vm.set_register(20, Value::Integer(MAX_EPHEMERAL_ROWS as i64))
            .unwrap();
        let err = idx_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxInsert, 0, 20, 0, P4::Int(1)),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ExecError::EphemeralRowLimitExceeded {
                opcode: "IdxInsert",
                limit
            } if limit == MAX_EPHEMERAL_ROWS
        ));
    }

    #[test]
    fn ephemeral_table_rewind_on_empty_cursor_jumps_to_p2() {
        let mut vm = Vm::new();
        open_ephemeral_table(&mut vm, 0);
        let step = rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 99, 0)).unwrap();
        assert_eq!(step, Step::Jump(99));
    }

    #[test]
    fn ephemeral_table_insert_then_full_scan_reads_rows_in_order() {
        let mut vm = Vm::new();
        open_ephemeral_table(&mut vm, 0);
        insert_ephemeral_row(&mut vm, 0, 1, &[Value::Integer(10)]);
        insert_ephemeral_row(&mut vm, 0, 2, &[Value::Integer(20)]);
        insert_ephemeral_row(&mut vm, 0, 3, &[Value::Integer(30)]);

        let step = rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 99, 0)).unwrap();
        assert_eq!(step, Step::Next);

        let mut seen = Vec::new();
        loop {
            rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 10, 0)).unwrap();
            column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 11)).unwrap();
            seen.push((
                vm.register(10).unwrap().clone(),
                vm.register(11).unwrap().clone(),
            ));
            match next(&mut vm, &Instruction::new(Opcode::Next, 0, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }

        assert_eq!(
            seen,
            vec![
                (Value::Integer(1), Value::Integer(10)),
                (Value::Integer(2), Value::Integer(20)),
                (Value::Integer(3), Value::Integer(30)),
            ]
        );
    }

    /// #425: `OpenDup` onto an already-materialized ephemeral table
    /// cursor scans the same rows without a second `Insert` pass, and
    /// each cursor's position is independent — rewinding the dup
    /// doesn't disturb the source cursor's own position.
    #[test]
    fn open_dup_shares_rows_with_independent_positions() {
        let mut vm = Vm::new();
        open_ephemeral_table(&mut vm, 0);
        insert_ephemeral_row(&mut vm, 0, 1, &[Value::Integer(10)]);
        insert_ephemeral_row(&mut vm, 0, 2, &[Value::Integer(20)]);

        rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 99, 0)).unwrap();
        next(&mut vm, &Instruction::new(Opcode::Next, 0, 1, 0)).unwrap();
        // Cursor 0 is now positioned on its second row (rowid 2).

        open_dup(&mut vm, &Instruction::new(Opcode::OpenDup, 1, 0, 0)).unwrap();
        // The dup starts unpositioned, independent of cursor 0's own position.
        rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 10, 0)).unwrap();
        assert_eq!(vm.register(10).unwrap().clone(), Value::Integer(2));

        let step = rewind(&mut vm, &Instruction::new(Opcode::Rewind, 1, 99, 0)).unwrap();
        assert_eq!(step, Step::Next);
        rowid(&mut vm, &Instruction::new(Opcode::Rowid, 1, 11, 0)).unwrap();
        assert_eq!(vm.register(11).unwrap().clone(), Value::Integer(1));

        // Cursor 0's position is unaffected by the dup's rewind.
        rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 12, 0)).unwrap();
        assert_eq!(vm.register(12).unwrap().clone(), Value::Integer(2));

        // The dup sees the full row set, not just what existed at dup time.
        let step = rewind(&mut vm, &Instruction::new(Opcode::Rewind, 1, 99, 0)).unwrap();
        assert_eq!(step, Step::Next);
        let mut seen = Vec::new();
        loop {
            rowid(&mut vm, &Instruction::new(Opcode::Rowid, 1, 10, 0)).unwrap();
            seen.push(vm.register(10).unwrap().clone());
            match next(&mut vm, &Instruction::new(Opcode::Next, 1, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }
        assert_eq!(seen, vec![Value::Integer(1), Value::Integer(2)]);
    }

    /// `OpenDup` onto a non-ephemeral-table cursor (e.g. a real table
    /// cursor) must error cleanly rather than panic.
    #[test]
    fn open_dup_on_a_non_ephemeral_table_cursor_is_a_type_mismatch() {
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 0, 0)).unwrap();
        let result = open_dup(&mut vm, &Instruction::new(Opcode::OpenDup, 1, 0, 0));
        assert!(matches!(result, Err(ExecError::CursorTypeMismatch { .. })));
    }

    /// A `with_db` `Vm` whose header reports `encoding` — the source is
    /// never actually read by an `EphemeralTable` insert/scan (#266), so
    /// `minimal_writable_db`'s backing memory-VFS db just needs to parse.
    fn ephemeral_vm_with_encoding(encoding: TextEncoding) -> Vm {
        let (vfs, mut header) = minimal_writable_db(512, 0x0d);
        header.text_encoding = encoding;
        let source = crate::vfs::VfsPageSource::open(&vfs, Path::new("/test.db"), 512).unwrap();
        Vm::with_db(Rc::new(source), header)
    }

    #[test]
    fn ephemeral_table_insert_decodes_using_database_text_encoding() {
        let mut vm = ephemeral_vm_with_encoding(TextEncoding::Utf16Le);
        open_ephemeral_table(&mut vm, 0);

        // Built directly with `encode_record`/`TextEncoding::Utf16Le`,
        // bypassing `MakeRecord` (which still hardcodes UTF-8 encoding,
        // a separate, wider-scope gap tracked outside #266).
        let payload = encode_record(&[Value::Text("héllo".into())], TextEncoding::Utf16Le);
        vm.set_register(30, Value::Blob(payload.into())).unwrap();
        vm.set_register(31, Value::Integer(1)).unwrap();
        insert(&mut vm, &Instruction::new(Opcode::Insert, 0, 31, 30)).unwrap();

        rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 99, 0)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 11)).unwrap();
        assert_eq!(*vm.register(11).unwrap(), Value::Text("héllo".into()));
    }

    #[test]
    fn ephemeral_table_last_positions_on_the_final_row() {
        let mut vm = Vm::new();
        open_ephemeral_table(&mut vm, 0);
        insert_ephemeral_row(&mut vm, 0, 1, &[Value::Integer(10)]);
        insert_ephemeral_row(&mut vm, 0, 2, &[Value::Integer(20)]);

        let step = last(&mut vm, &Instruction::new(Opcode::Last, 0, 99, 0)).unwrap();
        assert_eq!(step, Step::Next);
        rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 10, 0)).unwrap();
        assert_eq!(*vm.register(10).unwrap(), Value::Integer(2));
    }

    #[test]
    fn ephemeral_table_index_mode_default_still_rejects_rewind() {
        // P5 zero (the default) must keep opening the existing index-mode
        // ephemeral cursor — DISTINCT's dedup path must not regress.
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 0, 0)).unwrap();
        let err = rewind(&mut vm, &Instruction::new(Opcode::Rewind, 0, 99, 0)).unwrap_err();
        assert!(matches!(err, ExecError::CursorTypeMismatch { .. }));
    }

    #[test]
    fn ephemeral_table_rowid_with_no_current_row_errors() {
        let mut vm = Vm::new();
        open_ephemeral_table(&mut vm, 0);
        let err = rowid(&mut vm, &Instruction::new(Opcode::Rowid, 0, 10, 0)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    // --- index-read cursor: OpenRead(P5!=0)/SeekIndexEq/IdxRowid/
    // IdxRewind/IdxLast/IdxNext/IdxPrev ---

    /// Opens a real LEAF_INDEX b-tree with `n` entries `(i, i*10)` for
    /// `i` in `1..=n`, and returns a `Vm` with a write cursor open on
    /// slot 1 so the caller can `idx_insert` before switching to an
    /// index-read cursor on slot 0.
    fn writable_vm_with_index_entries(n: i64) -> Vm {
        let mut vm = writable_vm(0x0a);
        let mut open_instr = Instruction::new(Opcode::OpenWrite, 1, 1, 0);
        open_instr.p5 = 1;
        open_write(&mut vm, &open_instr).unwrap();
        for i in 1..=n {
            vm.set_register(0, Value::Integer(i)).unwrap();
            vm.set_register(1, Value::Integer(i * 10)).unwrap();
            idx_insert(
                &mut vm,
                &Instruction::with_p4(Opcode::IdxInsert, 1, 0, 0, P4::Int(2)),
            )
            .unwrap();
        }
        vm
    }

    fn open_index_read(vm: &mut Vm, slot: i32) {
        let mut open_instr = Instruction::new(Opcode::OpenRead, slot, 1, 0);
        open_instr.p5 = 1;
        open_read(vm, &open_instr).unwrap();
    }

    #[test]
    fn seek_index_eq_hits_and_idx_rowid_reads_the_trailing_rowid() {
        let mut vm = writable_vm_with_index_entries(3);
        open_index_read(&mut vm, 0);

        vm.set_register(5, Value::Integer(2)).unwrap();
        let step = seek_index_eq(
            &mut vm,
            &Instruction::with_p4(Opcode::SeekIndexEq, 0, 99, 5, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Next);
        idx_rowid(&mut vm, &Instruction::new(Opcode::IdxRowid, 0, 10, 0)).unwrap();
        assert_eq!(*vm.register(10).unwrap(), Value::Integer(20));
    }

    #[test]
    fn seek_index_eq_misses_and_jumps_to_p2() {
        let mut vm = writable_vm_with_index_entries(3);
        open_index_read(&mut vm, 0);

        vm.set_register(5, Value::Integer(999)).unwrap();
        let step = seek_index_eq(
            &mut vm,
            &Instruction::with_p4(Opcode::SeekIndexEq, 0, 99, 5, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Jump(99));
    }

    /// Distinct from `seek_index_eq_misses_and_jumps_to_p2`: probing 0
    /// (below every key: 1, 2, 3) still gives the underlying b-tree
    /// `seek` a row to land on (its ">=" floor, the lowest key, 1) —
    /// unlike probing past the end, which returns no row at all. This
    /// exercises the separate re-check (#591) that a landed-on row's
    /// probed-length prefix actually equals the probe, catching a
    /// probe that falls strictly between (or below) real keys.
    #[test]
    fn seek_index_eq_lands_on_a_row_whose_prefix_does_not_match_and_jumps_to_p2() {
        let mut vm = writable_vm_with_index_entries(3);
        open_index_read(&mut vm, 0);

        vm.set_register(5, Value::Integer(0)).unwrap();
        let step = seek_index_eq(
            &mut vm,
            &Instruction::with_p4(Opcode::SeekIndexEq, 0, 99, 5, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Jump(99));
    }

    #[test]
    fn seek_index_ge_lands_exactly_on_probe() {
        let mut vm = writable_vm_with_index_entries(3);
        open_index_read(&mut vm, 0);

        vm.set_register(5, Value::Integer(2)).unwrap();
        let step = seek_index_ge(
            &mut vm,
            &Instruction::with_p4(Opcode::SeekIndexGE, 0, 99, 5, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Next);
        idx_rowid(&mut vm, &Instruction::new(Opcode::IdxRowid, 0, 10, 0)).unwrap();
        assert_eq!(*vm.register(10).unwrap(), Value::Integer(20));
    }

    #[test]
    fn seek_index_ge_seeks_past_a_gap_to_the_next_greater_key() {
        let mut vm = writable_vm_with_index_entries(3);
        open_index_read(&mut vm, 0);

        // Keys are 1, 2, 3 — probing 0 (below every key) lands on 1;
        // there's no gap in this fixture, so probe a value strictly
        // between two keys is impossible with integer keys 1..=3, but
        // probing 0 still exercises "landed on a key > probe" rather
        // than an exact match, unlike SeekIndexEq's exact-match recheck.
        vm.set_register(5, Value::Integer(0)).unwrap();
        let step = seek_index_ge(
            &mut vm,
            &Instruction::with_p4(Opcode::SeekIndexGE, 0, 99, 5, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Next);
        idx_rowid(&mut vm, &Instruction::new(Opcode::IdxRowid, 0, 10, 0)).unwrap();
        assert_eq!(*vm.register(10).unwrap(), Value::Integer(10));
    }

    #[test]
    fn seek_index_ge_past_the_end_jumps_to_p2() {
        let mut vm = writable_vm_with_index_entries(3);
        open_index_read(&mut vm, 0);

        vm.set_register(5, Value::Integer(999)).unwrap();
        let step = seek_index_ge(
            &mut vm,
            &Instruction::with_p4(Opcode::SeekIndexGE, 0, 99, 5, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Jump(99));
    }

    #[test]
    fn idx_compare_gt_false_when_current_key_is_not_greater() {
        let mut vm = writable_vm_with_index_entries(3);
        open_index_read(&mut vm, 0);

        vm.set_register(5, Value::Integer(1)).unwrap();
        seek_index_ge(
            &mut vm,
            &Instruction::with_p4(Opcode::SeekIndexGE, 0, 99, 5, P4::Int(1)),
        )
        .unwrap();

        // current key is 1; compare against hi = 2, not greater.
        vm.set_register(6, Value::Integer(2)).unwrap();
        let step = idx_compare_gt(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxCompareGT, 0, 199, 6, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Next);
    }

    #[test]
    fn idx_compare_gt_true_when_current_key_exceeds_the_bound() {
        let mut vm = writable_vm_with_index_entries(3);
        open_index_read(&mut vm, 0);

        vm.set_register(5, Value::Integer(3)).unwrap();
        seek_index_ge(
            &mut vm,
            &Instruction::with_p4(Opcode::SeekIndexGE, 0, 99, 5, P4::Int(1)),
        )
        .unwrap();

        // current key is 3; compare against hi = 2, strictly greater.
        vm.set_register(6, Value::Integer(2)).unwrap();
        let step = idx_compare_gt(
            &mut vm,
            &Instruction::with_p4(Opcode::IdxCompareGT, 0, 199, 6, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Jump(199));
    }

    /// End-to-end range walk: `SeekIndexGE(>=2)` then `IdxNext` guarded
    /// by `IdxCompareGT(>2)` at the top of each iteration, over keys
    /// 1, 2, 3 — should visit exactly keys 2 and 3.
    #[test]
    fn seek_index_ge_and_idx_next_with_idx_compare_gt_walk_a_range() {
        let mut vm = writable_vm_with_index_entries(3);
        open_index_read(&mut vm, 0);

        vm.set_register(5, Value::Integer(2)).unwrap(); // lo
        vm.set_register(6, Value::Integer(2)).unwrap(); // hi (row 2)
        let step = seek_index_ge(
            &mut vm,
            &Instruction::with_p4(Opcode::SeekIndexGE, 0, 99, 5, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Next);

        // Update hi to 3 so both rows (2 and 3) are in range.
        vm.set_register(6, Value::Integer(3)).unwrap();

        let mut visited = vec![];
        loop {
            let stop = idx_compare_gt(
                &mut vm,
                &Instruction::with_p4(Opcode::IdxCompareGT, 0, 999, 6, P4::Int(1)),
            )
            .unwrap();
            if stop == Step::Jump(999) {
                break;
            }
            idx_rowid(&mut vm, &Instruction::new(Opcode::IdxRowid, 0, 10, 0)).unwrap();
            visited.push(vm.register(10).unwrap().clone());
            let advanced =
                idx_next(&mut vm, &Instruction::new(Opcode::IdxNext, 0, 500, 0)).unwrap();
            if advanced != Step::Jump(500) {
                break;
            }
        }
        assert_eq!(visited, vec![Value::Integer(20), Value::Integer(30)]);
    }

    #[test]
    fn idx_rowid_without_a_prior_seek_errors() {
        let mut vm = writable_vm_with_index_entries(1);
        open_index_read(&mut vm, 0);
        let err = idx_rowid(&mut vm, &Instruction::new(Opcode::IdxRowid, 0, 10, 0)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn idx_rowid_errors_when_trailing_column_is_not_an_integer() {
        let mut vm = writable_vm_with_index_entries(1);
        open_index_read(&mut vm, 0);
        let CursorSlot::IndexRead(state) = vm.cursor_mut(0).unwrap() else {
            panic!("expected an index-read cursor");
        };
        // Hand-fabricates a current row whose only (and thus trailing)
        // column isn't the integer rowid a real index entry always
        // carries there — the malformed-input path `IdxRowid` must
        // still error on, rather than a well-formed index page would
        // ever produce on its own.
        state.set_current(Some(btree::IndexRow {
            payload: btree::Payload::Owned(encode_record(
                &[Value::Text(Arc::from("not-a-rowid"))],
                TextEncoding::Utf8,
            )),
        }));
        let err = idx_rowid(&mut vm, &Instruction::new(Opcode::IdxRowid, 0, 10, 0)).unwrap_err();
        assert!(
            matches!(err, ExecError::MalformedInstruction { .. }),
            "expected a MalformedInstruction error, got: {err:?}"
        );
    }

    #[test]
    fn idx_rewind_idx_next_walk_the_index_in_ascending_order() {
        let mut vm = writable_vm_with_index_entries(3);
        open_index_read(&mut vm, 0);

        let step = idx_rewind(&mut vm, &Instruction::new(Opcode::IdxRewind, 0, 99, 0)).unwrap();
        assert_eq!(step, Step::Next);

        let mut rowids = Vec::new();
        loop {
            idx_rowid(&mut vm, &Instruction::new(Opcode::IdxRowid, 0, 10, 0)).unwrap();
            rowids.push(vm.register(10).unwrap().clone());
            match idx_next(&mut vm, &Instruction::new(Opcode::IdxNext, 0, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }
        assert_eq!(
            rowids,
            vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)]
        );
    }

    #[test]
    fn idx_last_idx_prev_walk_the_index_in_descending_order() {
        let mut vm = writable_vm_with_index_entries(3);
        open_index_read(&mut vm, 0);

        let step = idx_last(&mut vm, &Instruction::new(Opcode::IdxLast, 0, 99, 0)).unwrap();
        assert_eq!(step, Step::Next);

        let mut rowids = Vec::new();
        loop {
            idx_rowid(&mut vm, &Instruction::new(Opcode::IdxRowid, 0, 10, 0)).unwrap();
            rowids.push(vm.register(10).unwrap().clone());
            match idx_prev(&mut vm, &Instruction::new(Opcode::IdxPrev, 0, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }
        assert_eq!(
            rowids,
            vec![Value::Integer(30), Value::Integer(20), Value::Integer(10)]
        );
    }

    #[test]
    fn idx_rewind_and_idx_last_jump_to_p2_on_an_empty_index() {
        let mut vm = writable_vm(0x0a);
        open_index_read(&mut vm, 0);
        let step = idx_rewind(&mut vm, &Instruction::new(Opcode::IdxRewind, 0, 99, 0)).unwrap();
        assert_eq!(step, Step::Jump(99));
        let step = idx_last(&mut vm, &Instruction::new(Opcode::IdxLast, 0, 99, 0)).unwrap();
        assert_eq!(step, Step::Jump(99));
    }

    #[test]
    fn index_read_state_mut_type_mismatch_errors() {
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 0, 0)).unwrap();
        let err = idx_rewind(&mut vm, &Instruction::new(Opcode::IdxRewind, 0, 99, 0)).unwrap_err();
        assert!(matches!(err, ExecError::CursorTypeMismatch { .. }));
    }

    #[test]
    fn column_reads_through_an_index_read_cursor() {
        let mut vm = writable_vm_with_index_entries(1);
        open_index_read(&mut vm, 0);
        idx_rewind(&mut vm, &Instruction::new(Opcode::IdxRewind, 0, 99, 0)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 0, 0, 10)).unwrap();
        assert_eq!(*vm.register(10).unwrap(), Value::Integer(1));
    }

    // --- automatic-index cursor: OpenEphemeral(P5==2)/AutoIndexInsert/
    // AutoIndexSeek/AutoIndexRowid/AutoIndexNext ---

    fn open_auto_index(vm: &mut Vm, slot: i32) {
        open_ephemeral(
            vm,
            &Instruction {
                opcode: Opcode::OpenEphemeral,
                p1: slot,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 2,
            },
        )
        .unwrap();
    }

    #[test]
    fn auto_index_seek_finds_every_rowid_sharing_a_duplicate_key() {
        let mut vm = Vm::new();
        open_auto_index(&mut vm, 0);

        vm.set_register(0, Value::Text("k".to_string().into()))
            .unwrap();
        vm.set_register(1, Value::Integer(100)).unwrap();
        auto_index_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::AutoIndexInsert, 0, 0, 1, P4::Int(1)),
        )
        .unwrap();
        vm.set_register(1, Value::Integer(200)).unwrap();
        auto_index_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::AutoIndexInsert, 0, 0, 1, P4::Int(1)),
        )
        .unwrap();

        vm.set_register(5, Value::Text("k".to_string().into()))
            .unwrap();
        let step = auto_index_seek(
            &mut vm,
            &Instruction::with_p4(Opcode::AutoIndexSeek, 0, 99, 5, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Next);

        let mut rowids = Vec::new();
        loop {
            auto_index_rowid(&mut vm, &Instruction::new(Opcode::AutoIndexRowid, 0, 10, 0)).unwrap();
            rowids.push(vm.register(10).unwrap().clone());
            match auto_index_next(&mut vm, &Instruction::new(Opcode::AutoIndexNext, 0, 1, 0))
                .unwrap()
            {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }
        assert_eq!(rowids, vec![Value::Integer(100), Value::Integer(200)]);
    }

    #[test]
    fn auto_index_seek_jumps_to_p2_on_a_miss() {
        let mut vm = Vm::new();
        open_auto_index(&mut vm, 0);
        vm.set_register(5, Value::Text("nope".to_string().into()))
            .unwrap();
        let step = auto_index_seek(
            &mut vm,
            &Instruction::with_p4(Opcode::AutoIndexSeek, 0, 99, 5, P4::Int(1)),
        )
        .unwrap();
        assert_eq!(step, Step::Jump(99));
    }

    #[test]
    fn auto_index_rowid_without_a_prior_seek_errors() {
        let mut vm = Vm::new();
        open_auto_index(&mut vm, 0);
        let err = auto_index_rowid(&mut vm, &Instruction::new(Opcode::AutoIndexRowid, 0, 10, 0))
            .unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn auto_index_insert_rejects_a_non_integer_rowid_register() {
        let mut vm = Vm::new();
        open_auto_index(&mut vm, 0);
        vm.set_register(0, Value::Text("k".to_string().into()))
            .unwrap();
        vm.set_register(1, Value::Text("nope".to_string().into()))
            .unwrap();
        let err = auto_index_insert(
            &mut vm,
            &Instruction::with_p4(Opcode::AutoIndexInsert, 0, 0, 1, P4::Int(1)),
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn auto_index_next_with_no_current_position_is_a_no_op() {
        let mut vm = Vm::new();
        open_auto_index(&mut vm, 0);
        let step =
            auto_index_next(&mut vm, &Instruction::new(Opcode::AutoIndexNext, 0, 1, 0)).unwrap();
        assert_eq!(step, Step::Next);
    }

    #[test]
    fn auto_index_mut_type_mismatch_errors() {
        let mut vm = Vm::new();
        open_ephemeral(&mut vm, &Instruction::new(Opcode::OpenEphemeral, 0, 0, 0)).unwrap();
        let err = auto_index_seek(
            &mut vm,
            &Instruction::with_p4(Opcode::AutoIndexSeek, 0, 99, 0, P4::Int(1)),
        )
        .unwrap_err();
        assert!(matches!(err, ExecError::CursorTypeMismatch { .. }));
    }

    // --- Count / Analyze ---

    #[test]
    fn count_opcode_reports_the_exact_row_count() {
        let mut vm = open_vm("table_multipage.db");
        count(&mut vm, &Instruction::new(Opcode::Count, 2, 10, 0)).unwrap();
        assert_eq!(*vm.register(10).unwrap(), Value::Integer(3000));
    }

    #[test]
    fn count_opcode_rejects_a_negative_root_page() {
        let mut vm = open_vm("table_multipage.db");
        let err = count(&mut vm, &Instruction::new(Opcode::Count, -1, 10, 0)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }

    #[test]
    fn analyze_populates_sqlite_stat1_for_a_table_and_its_index() {
        let mut vm = writable_vm(0x0d);
        create_table(
            &mut vm,
            &Instruction::with_p4(
                Opcode::CreateTable,
                0,
                0,
                0,
                P4::CreateTable {
                    name: "t".to_string(),
                    sql: "CREATE TABLE t (a)".to_string(),
                },
            ),
        )
        .unwrap();
        open_read(&mut vm, &Instruction::new(Opcode::OpenRead, 1, 1, 0)).unwrap();
        rewind(&mut vm, &Instruction::new(Opcode::Rewind, 1, 999, 0)).unwrap();
        column(&mut vm, &Instruction::new(Opcode::Column, 1, 3, 22)).unwrap();
        let table_root = match vm.register(22).unwrap() {
            Value::Integer(n) => u32::try_from(*n).unwrap(),
            other => panic!("expected integer rootpage, got {other:?}"),
        };

        open_write(
            &mut vm,
            &Instruction::new(Opcode::OpenWrite, 2, i32::try_from(table_root).unwrap(), 0),
        )
        .unwrap();
        for i in 1..=3i64 {
            vm.set_register(3, Value::Integer(i)).unwrap();
            crate::vdbe::result::make_record(
                &mut vm,
                &Instruction::new(Opcode::MakeRecord, 3, 1, 4),
            )
            .unwrap();
            vm.set_register(5, Value::Integer(i)).unwrap();
            insert(&mut vm, &Instruction::new(Opcode::Insert, 2, 5, 4)).unwrap();
        }

        create_index(
            &mut vm,
            &Instruction::with_p4(
                Opcode::CreateIndex,
                0,
                0,
                0,
                P4::CreateIndex {
                    name: "idx".to_string(),
                    table_name: "t".to_string(),
                    table_root_page: table_root,
                    sql: "CREATE INDEX idx ON t (a)".to_string(),
                    column_indices: vec![0],
                    unique: false,
                },
            ),
        )
        .unwrap();

        // Find the freshly created index's own root page back out of
        // sqlite_master (CreateIndex only returns via the schema, not a
        // register), the same way the create_index round-trip test does.
        let mut index_root = None;
        let step = rewind(&mut vm, &Instruction::new(Opcode::Rewind, 1, 999, 0)).unwrap();
        assert_eq!(step, Step::Next);
        loop {
            column(&mut vm, &Instruction::new(Opcode::Column, 1, 0, 30)).unwrap();
            column(&mut vm, &Instruction::new(Opcode::Column, 1, 3, 31)).unwrap();
            if vm.register(30).unwrap() == &Value::Text("index".to_string().into()) {
                if let Value::Integer(n) = vm.register(31).unwrap() {
                    index_root = Some(u32::try_from(*n).unwrap());
                }
            }
            match next(&mut vm, &Instruction::new(Opcode::Next, 1, 1, 0)).unwrap() {
                Step::Jump(1) => continue,
                Step::Next => break,
                other => panic!("unexpected step {other:?}"),
            }
        }
        let index_root = index_root.unwrap();

        analyze(
            &mut vm,
            &Instruction::with_p4(
                Opcode::Analyze,
                0,
                0,
                0,
                P4::Analyze {
                    targets: vec![crate::vdbe::program::AnalyzeTarget {
                        table_name: "t".to_string(),
                        table_root_page: table_root,
                        indexes: vec![crate::vdbe::program::AnalyzeIndexTarget {
                            index_name: "idx".to_string(),
                            root_page: index_root,
                        }],
                    }],
                },
            ),
        )
        .unwrap();

        // sqlite_stat1 now holds one row for the table and one for the index.
        let db = vm.db().unwrap();
        let stat1_root = btree::ensure_sqlite_stat1_table(
            &mut vm.writer("test").unwrap().borrow_mut(),
            &db.header,
        )
        .unwrap();
        let mut stat_cursor = TableCursor::new(Rc::clone(&db.source), &db.header, stat1_root);
        let mut rows = Vec::new();
        let mut row = stat_cursor.first_row().unwrap();
        while let Some(r) = row {
            rows.push(decode_record(&r.payload, TextEncoding::Utf8).unwrap());
            row = stat_cursor.next_row().unwrap();
        }
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .any(|r| r[1] == Value::Null && r[2] == Value::Text("3".into())));
        assert!(rows
            .iter()
            .any(|r| r[1] == Value::Text("idx".into()) && r[2] == Value::Text("3 1".into())));
    }

    #[test]
    fn analyze_rejects_a_mismatched_p4() {
        let mut vm = writable_vm(0x0d);
        let err = analyze(&mut vm, &Instruction::new(Opcode::Analyze, 0, 0, 0)).unwrap_err();
        assert!(matches!(err, ExecError::MalformedInstruction { .. }));
    }
}
