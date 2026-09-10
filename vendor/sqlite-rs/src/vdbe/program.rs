// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Instruction format and the linear bytecode `Program` (spec 009,
//! Requirement 1). `Opcode` enumerates the full frozen V2 opcode set
//! (`tools/opcodes-v2.json`, 65 opcodes, oracle 3.53.4, #87/#139/#142/#137) —
//! every variant listed here, whether or not `src/vdbe/exec.rs`
//! implements it yet, so the enum stays the single source of truth for
//! "in scope for V2" across #89/#90/#91. `Variable` was added by #137
//! (bound-parameter point lookups) — the harvest now includes a
//! `WHERE id = ?1` query.

use crate::vdbe::Collation;

/// The 65 V2 opcodes, grouped by category to match
/// `tools/opcodes-v2.json`'s taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Opcode {
    // control
    /// Program entry point: jumps to `P2`, the real first instruction —
    /// mirrors SQLite's own convention of a leading `Init` before the
    /// generated body.
    Init,
    /// Unconditional jump to `P2`.
    Goto,
    /// Runs its body at most once per VDBE invocation, jumping past it
    /// on subsequent passes (used for one-time initialization).
    Once,
    /// Marks the entry point of a subroutine invoked via `Goto`/`Gosub`-
    /// style control flow, paired with `Return`.
    BeginSubrtn,
    /// Returns from a subroutine to the PC saved when it was entered.
    Return,
    /// Stops execution, ending the program (successfully or with the
    /// error carried in its operands).
    Halt,
    /// Begins a read or write transaction on the database named by `P1`.
    Transaction,
    // #360: explicit COMMIT/ROLLBACK — postdates the V2 oracle harvest
    // (V2 had no write path, so no BEGIN/COMMIT/ROLLBACK codegen
    // existed then), so excluded from `ALL` like the other V3 write
    // opcodes below, but fully dispatched and exhaustiveness-checked.
    // `P2` is stock SQLite's convention: 1 = commit, 0 = rollback.
    /// Explicit `COMMIT`/`ROLLBACK` — `P2` selects commit (1) or rollback
    /// (0), stock SQLite's own convention.
    AutoCommit,
    // #388: `PRAGMA journal_mode = WAL|DELETE` -- like `AutoCommit`
    // above, postdates the V2 oracle harvest (no PRAGMA codegen existed
    // then), so excluded from `ALL` but fully dispatched and
    // exhaustiveness-checked. `P1` carries the target mode
    // (`crate::vdbe::pragma::JOURNAL_MODE_WAL`/`JOURNAL_MODE_DELETE`).
    /// `PRAGMA journal_mode = WAL|DELETE`: `P1` carries the target mode
    /// (`crate::vdbe::pragma::JOURNAL_MODE_WAL`/`JOURNAL_MODE_DELETE`).
    SetJournalMode,
    // #540/#541: `PRAGMA integrity_check`/`quick_check` -- postdates the
    // V2 oracle harvest, so excluded from `ALL` but fully dispatched and
    // exhaustiveness-checked, like `SetJournalMode` above.
    /// `PRAGMA integrity_check`/`quick_check`: `P1` is 1 for the
    /// `quick_check` reduced pass (skips the index-vs-table cross-check),
    /// 0 for the full `integrity_check`. Emits one `TEXT` result row per
    /// problem found, or a single `"ok"` row if none.
    IntegrityCheck,
    // #645: `PRAGMA synchronous [= OFF|NORMAL|FULL]` -- postdates the V2
    // oracle harvest, so excluded from `ALL` but fully dispatched and
    // exhaustiveness-checked, like `SetJournalMode`/`IntegrityCheck`
    // above.
    /// `PRAGMA synchronous [= OFF|NORMAL|FULL]`: `P1` carries the target
    /// level (`crate::vdbe::pragma::{SYNCHRONOUS_OFF, SYNCHRONOUS_NORMAL,
    /// SYNCHRONOUS_FULL}`), or `crate::vdbe::pragma::SYNCHRONOUS_QUERY`
    /// for the bare query form, which emits the connection's current
    /// level as a single `INTEGER` result row instead of changing it.
    Synchronous,
    /// Jumps to `P2` if register `P1` is falsy (zero/false, per SQLite's
    /// truthiness rules).
    IfNot,
    /// Jumps to `P2` if register `P1` is not zero.
    IfNotZero,
    /// Jumps to `P2` if register `P1` holds a value greater than zero.
    IfPos,
    /// Decrements register `P1`; jumps to `P2` if the result is zero (or
    /// it was already zero/NULL).
    DecrJumpZero,
    /// Jumps to `P2` if register `P1` is NULL.
    IsNull,
    /// Jumps to `P2` if register `P1` is not NULL.
    NotNull,
    /// Coerces register `P1` to an integer, failing the statement if it
    /// cannot be represented as one.
    MustBeInt,
    /// Computes `LIMIT`/`OFFSET` bookkeeping from registers `P1`
    /// (limit) and `P2` (offset), storing the combined counter in `P3`.
    OffsetLimit,
    // cursor
    /// Opens cursor `P1` for read-only access to the table/index with
    /// root page `P2`.
    OpenRead,
    /// Opens cursor `P1` for read/write access to the table/index with
    /// root page `P2`.
    OpenWrite,
    /// Opens cursor `P1` on a new, empty temporary b-tree (used for
    /// scratch/ephemeral storage, e.g. `DISTINCT`/subquery materialization).
    OpenEphemeral,
    /// Opens cursor `P1` as a second view onto the *same* already-
    /// materialized ephemeral table cursor `P2` is open on (#425) —
    /// mirrors the oracle's own `OP_OpenDup`. Used when a `WITH`-clause
    /// CTE is referenced more than once in one statement: the first
    /// reference materializes it via `OpenEphemeral`+`Insert`, every
    /// later reference `OpenDup`s onto that cursor instead of
    /// re-materializing from scratch. `P1` and `P2` scan independently
    /// (separate positions) but share the same underlying row data.
    OpenDup,
    /// Opens cursor `P1` as a pseudo-cursor over a single in-memory
    /// record buffer rather than a real b-tree.
    OpenPseudo,
    /// Positions cursor `P1` at its first entry, jumping to `P2` if the
    /// table/index is empty.
    Rewind,
    /// Positions cursor `P1` at its last entry, jumping to `P2` if the
    /// table/index is empty.
    Last,
    /// Advances cursor `P1` to its next entry, jumping to `P2` if there
    /// was one (i.e. the cursor did not run off the end).
    Next,
    /// Reads column `P2` of cursor `P1`'s current row into register `P3`.
    Column,
    /// Stores cursor `P1`'s current rowid into register `P2`.
    Rowid,
    /// Seeks cursor `P1` to the row with rowid `P3`, jumping to `P2` if
    /// no such row exists.
    SeekRowid,
    /// Sets cursor `P1` to point at a synthetic NULL row (used after a
    /// failed seek so subsequent `Column` reads yield NULL).
    NullRow,
    /// Stores cursor `P1`'s next-available sequence number into register
    /// `P2` (backs `AUTOINCREMENT`-style rowid allocation).
    Sequence,
    /// Seeks cursor `P1` for an entry matching the key built from
    /// registers `P3..P3+P4`, jumping to `P2` if found.
    Found,
    /// Inserts the index entry in register `P2` into cursor `P1`'s
    /// b-tree.
    IdxInsert,
    /// Compares cursor `P1`'s current index key against the key built
    /// from registers `P3..P3+P4`, jumping to `P2` if it is `<=`.
    IdxLE,
    /// Deletes cursor `P1`'s current row.
    Delete,
    /// Inserts the record in register `P2` (keyed by rowid in `P3`) into
    /// cursor `P1`'s table b-tree.
    Insert,
    /// Generates a new, unused rowid for cursor `P1`'s table, storing it
    /// in register `P2`.
    NewRowid,
    /// Deletes cursor `P1`'s current index entry.
    IdxDelete,
    // #543: exact `COUNT(*)` shortcut — walks the b-tree rooted at page
    // `P1`, summing leaf-page cell counts, without opening a cursor or
    // decoding any row. Like #207/#243/#296 below, postdates the V2
    // oracle harvest, so excluded from `ALL` but fully dispatched and
    // exhaustiveness-checked.
    /// Counts the rows in the table/index b-tree rooted at page `P1`,
    /// storing the exact count into register `P2` (mirrors SQLite's own
    /// `OP_Count`).
    Count,
    // #207: real-index seek+branch primitive for non-rowid UNIQUE
    // constraint enforcement — like #194's other V3 write-path opcodes
    // above, this postdates the V2 oracle harvest, so it's excluded
    // from `ALL` (never harvested from a V2 `EXPLAIN`) but still fully
    // dispatched and exhaustiveness-checked.
    /// Probes cursor `P1`'s index for a conflicting key built from
    /// registers `P3..P3+P4`, jumping to `P2` if no conflict is found.
    NoConflict,
    // #243: real secondary-index read path for the planner's join
    // equality-index-selection fast path — like #207's `NoConflict`,
    // postdates the V2 oracle harvest (no query-time index seek existed
    // then), so excluded from `ALL` but fully dispatched and
    // exhaustiveness-checked. `SeekIndexEq` probes an index b-tree for an
    // exact key match (jumping P2 on miss); `IdxRowid` reads the trailing
    // rowid column off the index cursor's currently-seeked entry so
    // codegen can chain into a `SeekRowid` on the table cursor.
    /// Probes cursor `P1`'s index b-tree for an exact key match built
    /// from registers `P3..P3+P4`, jumping to `P2` on miss.
    SeekIndexEq,
    /// Reads the trailing rowid column off index cursor `P1`'s
    /// currently-seeked entry into register `P2`.
    IdxRowid,
    // #606: real-index range-seek primitives, for `BETWEEN`/`IN`/
    // `LIKE`-prefix query-planner fast paths (ADR-0034). Unlike
    // `SeekIndexEq` (exact-key-or-miss), `SeekIndexGE` accepts landing on
    // any key `>=` the probe; `IdxCompareGT` is the matching "have we
    // walked past the upper bound yet" stop check for `IdxNext`-driven
    // range walks over the same real index cursor (distinct from the
    // ephemeral-cursor `IdxLE` above). Postdates the V2 oracle harvest,
    // so excluded from `ALL` but fully dispatched and
    // exhaustiveness-checked, like `SeekIndexEq`/`IdxRowid` above.
    /// Seeks index cursor `P1` to the first entry whose key (built from
    /// registers `P3..P3+P4`) is `>=` the probe, jumping to `P2` if no
    /// such entry exists.
    SeekIndexGE,
    /// Compares index cursor `P1`'s current entry's leading `P4` columns
    /// against the key built from registers `P3..P3+P4`, jumping to `P2`
    /// if the current key is strictly greater than the probe.
    IdxCompareGT,
    // #296: index-ordered scan — walks a matching index's b-tree
    // directly (forward or backward) in place of `Rewind`/`Next` +
    // sorter opcodes, so `ORDER BY <indexed col> [DESC] LIMIT n` never
    // buffers or sorts at all. Like `SeekIndexEq`/`IdxRowid` above,
    // postdates the V2 oracle harvest, so excluded from `ALL` but fully
    // dispatched and exhaustiveness-checked. `IdxRewind`/`IdxLast`
    // mirror `Rewind`/`Last`'s "jump on empty" shape; `IdxNext`/
    // `IdxPrev` mirror `Next`'s "jump on found" shape — see ADR-0020.
    /// Positions index cursor `P1` at its first entry, jumping to `P2`
    /// if the index is empty (mirrors `Rewind`).
    IdxRewind,
    /// Positions index cursor `P1` at its last entry, jumping to `P2`
    /// if the index is empty (mirrors `Last`).
    IdxLast,
    /// Advances index cursor `P1` forward, jumping to `P2` if there was
    /// a next entry (mirrors `Next`).
    IdxNext,
    /// Advances index cursor `P1` backward, jumping to `P2` if there was
    /// a previous entry.
    IdxPrev,
    // #545: transient automatic-index primitives — a join level builds
    // one of these (opened via `OpenEphemeral` with `P5 == 2`, a third
    // mode alongside the existing index-mode/table-mode `OpenEphemeral`
    // cursors) over an otherwise-unindexed equality join column, then
    // seeks it once per outer row instead of a `Rewind`/`Next` full
    // scan. Unlike `SeekIndexEq`/`IdxNext`'s byte-order seek-then-walk
    // shape (built for a real, sorted index b-tree), this is a plain
    // exact-key multi-map (`BTreeMap<key, Vec<rowid>>`) — every join
    // only ever asks "which rows share this exact key", never a range,
    // so there is no need to reproduce a real index's ordering
    // semantics. Postdates the V2 oracle harvest, so excluded from
    // `ALL` but fully dispatched and exhaustiveness-checked, like
    // `SeekIndexEq`/`IdxRowid` above.
    /// Appends the rowid in register `P3` under the key built from
    /// register `P2` (`P4::SeekKey`, one column) into automatic-index
    /// cursor `P1`.
    AutoIndexInsert,
    /// Seeks automatic-index cursor `P1` for every rowid sharing the key
    /// built from register `P3` (`P4::SeekKey`, one column), positioning
    /// on the first if any exist, jumping to `P2` if none do.
    AutoIndexSeek,
    /// Reads automatic-index cursor `P1`'s currently-seeked rowid into
    /// register `P2`.
    AutoIndexRowid,
    /// Advances automatic-index cursor `P1` to the next rowid sharing
    /// its current key, jumping to `P2` if there was one.
    AutoIndexNext,
    // DDL (#215) — schema-mutating statements, each done procedurally in
    // one exec.rs handler rather than decomposed into cursor-driven
    // multi-instruction sequences; never harvested from a V2 oracle
    // EXPLAIN (DDL postdates V2), so excluded from `ALL` like the other
    // V3 write opcodes above.
    /// `CREATE TABLE`: registers a new `sqlite_master` row per its `P4`
    /// payload (see [`P4::CreateTable`]).
    CreateTable,
    /// `DROP TABLE`: removes the table and its indexes per its `P4`
    /// payload (see [`P4::DropTable`]).
    DropTable,
    /// `CREATE INDEX`: registers a new `sqlite_master` row and populates
    /// it from pre-existing rows per its `P4` payload (see
    /// [`P4::CreateIndex`]).
    CreateIndex,
    /// `DROP INDEX`: removes the index per its `P4` payload (see
    /// [`P4::DropIndex`]).
    DropIndex,
    /// `CreateView` (#380): registers a `sqlite_master` row with
    /// `type = 'view'` and `rootpage = 0` (a view has no b-tree of its
    /// own) — otherwise identical to `CreateTable`'s single-instruction
    /// shape, carrying its own `P4::CreateView` payload.
    CreateView,
    /// `ANALYZE` (#461, spec 011): populates `sqlite_stat1` for the
    /// table(s) named by its `P4::Analyze` payload, creating that table
    /// first if this is the first `ANALYZE` ever run — same procedural,
    /// single-instruction shape as `CreateTable`/`CreateIndex` rather
    /// than a decomposed cursor-driven scan.
    Analyze,
    // compare
    /// Jumps to `P2` if registers `P1` and `P3` are equal, per `P4`'s
    /// collation/affinity.
    Eq,
    /// Jumps to `P2` if register `P3` is `>=` register `P1`, per `P4`'s
    /// collation/affinity.
    Ge,
    /// Jumps to `P2` if register `P3` is `>` register `P1`, per `P4`'s
    /// collation/affinity.
    Gt,
    /// Jumps to `P2` if register `P3` is `<=` register `P1`, per `P4`'s
    /// collation/affinity.
    Le,
    /// Jumps to `P2` if register `P3` is `<` register `P1`, per `P4`'s
    /// collation/affinity.
    Lt,
    /// Applies REAL affinity to register `P1` in place (converts an
    /// exact integer to floating point).
    RealAffinity,
    // arithmetic
    /// Adds registers `P1` and `P2`, storing the result in `P3`.
    Add,
    /// Subtracts register `P1` from register `P2`, storing the result
    /// in `P3`.
    Subtract,
    /// Multiplies registers `P1` and `P2`, storing the result in `P3`.
    Multiply,
    /// Divides register `P2` by register `P1`, storing the result in
    /// `P3`.
    Divide,
    /// Computes register `P2` modulo register `P1`, storing the result
    /// in `P3`.
    Remainder,
    /// Stores the logical negation of register `P1` into register `P2`.
    Not,
    /// Stores the bitwise AND of registers `P1` and `P2` into `P3`.
    BitAnd,
    /// Stores the bitwise OR of registers `P1` and `P2` into `P3`.
    BitOr,
    /// Left-shifts register `P2` by register `P1`, storing the result
    /// in `P3`.
    ShiftLeft,
    /// Right-shifts register `P2` by register `P1`, storing the result
    /// in `P3`.
    ShiftRight,
    /// Stores the bitwise complement of register `P1` into register
    /// `P2`.
    BitNot,
    /// Concatenates registers `P1` and `P2` (as text), storing the
    /// result in `P3`.
    Concat,
    // #142: CAST forces an affinity via its own dedicated opcode, not
    // by misusing MustBeInt/RealAffinity.
    /// Forces register `P1` to the affinity named by `P4`, in place.
    Cast,
    // function
    /// Calls the scalar function named by `P4` with arguments in
    /// registers `P2..P2+P5`, storing the result in register `P3`.
    Function,
    // aggregate (#241) — postdates the V2 oracle harvest (no GROUP BY
    // codegen existed then), so excluded from `ALL` like the other V3
    // opcodes above, but fully dispatched and exhaustiveness-checked.
    /// Feeds one input row (registers `P2..P2+P5`) into the aggregate
    /// accumulator in register `P3`, per `P4`'s function descriptor.
    AggStep,
    /// Finalizes the aggregate accumulator in register `P1`, replacing
    /// it with the aggregate's final value per `P4`'s function
    /// descriptor.
    AggFinal,
    // result
    /// Stores the integer constant `P1` into register `P2`.
    Integer,
    /// Stores the 64-bit integer constant carried in `P4` into register
    /// `P2`.
    Int64,
    /// Stores the floating-point constant carried in `P4` into register
    /// `P2`.
    Real,
    /// Stores the blob constant carried in `P4` into register `P2`.
    Blob,
    /// Stores NULL into register `P2`.
    Null,
    /// Stores the text constant carried in `P4` into register `P2`.
    String8,
    /// Stores the value bound to parameter `P1` into register `P2`.
    Variable,
    /// Serializes registers `P1..P1+P2` into a record blob (per `P4`'s
    /// affinity string, if any), storing it in register `P3`.
    MakeRecord,
    /// Emits registers `P1..P1+P2` as one output row.
    ResultRow,
    // #208: copies register `P1`'s value into `P2` verbatim — needed
    // when `INSERT ... SELECT` re-projects a `SELECT`-scan's already-
    // populated registers into a target table's schema-column order:
    // `compile_row`'s `MakeRecord` needs one *fresh*, contiguous
    // register per column (mirroring the literal-`Expr` path's
    // `compile_value` calls, which always bump-allocate anew), not the
    // scan's original (non-contiguous, non-reorderable-in-place)
    // registers reused directly. Never harvested from a V2 oracle
    // EXPLAIN (V2 predates any write path), so excluded from `ALL`
    // like the other V3 write opcodes above.
    /// Copies register `P1`'s value into register `P2` verbatim.
    Copy,
    // sorter
    /// Opens a sorter on cursor `P1`, keyed per `P4`'s sort-key
    /// descriptor.
    SorterOpen,
    /// Inserts the record in register `P2` into sorter cursor `P1`.
    SorterInsert,
    /// Sorts sorter cursor `P1`'s buffered records and positions it at
    /// the first one, jumping to `P2` if it is empty.
    SorterSort,
    /// Advances sorter cursor `P1` to its next record, jumping to `P2`
    /// if there was one.
    SorterNext,
    /// Stores sorter cursor `P1`'s current record into register `P2`.
    SorterData,
    /// Standalone in-place sort primitive (distinct from the
    /// `SorterOpen`/`SorterInsert`/`SorterSort` cursor-driven pipeline).
    Sort,
    // hash aggregation (#570) — an O(n) alternative to the
    // `SorterOpen`/`SorterInsert`/`SorterSort` grouping pipeline above,
    // shaped deliberately as the same five-opcode open/insert/
    // rewind/data/next family so the two strategies stay comparable.
    // Postdates the V2 oracle harvest, so excluded from `ALL` like the
    // other V3+ opcodes, but fully dispatched and
    // exhaustiveness-checked. See `crate::vdbe::hash_agg`'s module doc.
    /// Opens a hash-aggregation table on cursor `P1`, keyed per `P4`'s
    /// group-key descriptor ([`P4::GroupKey`]).
    HashAggOpen,
    /// Locates (creating it on first sight) the group in hash-aggregation
    /// cursor `P1` whose key is carried by the record in register `P2`,
    /// making it the target of the `HashAggStep`s that follow.
    HashAggFind,
    /// Folds register `P2`'s value (`P4`'s arity registers starting
    /// there) into accumulator slot `P1` of hash-aggregation cursor
    /// `P3`'s currently-located group, per `P4`'s function descriptor —
    /// the per-group counterpart of [`Opcode::AggStep`].
    HashAggStep,
    /// Freezes hash-aggregation cursor `P1`, orders its groups by group
    /// key, and positions it at the first one — jumping to `P2` if no
    /// group was ever created.
    HashAggRewind,
    /// Stores hash-aggregation cursor `P1`'s current group's retained
    /// row into register `P2`, and restores that group's accumulators
    /// into the `AggStep`/`AggFinal` context slots.
    HashAggData,
    /// Advances hash-aggregation cursor `P1` to its next group, jumping
    /// to `P2` if there was one.
    HashAggNext,
}

impl Opcode {
    /// The 65 harvested variants (`tools/opcodes-v2.json`), in
    /// enum-declaration order — `tests/unit/vdbe_opcode_completeness_test.rs`
    /// checks this list against that harvest exactly, so `OpenWrite`/
    /// `Insert`/`NewRowid` (#194, the V3 write path — never harvested
    /// from a V2 oracle EXPLAIN, since V2 predates any write-path
    /// support) are deliberately excluded from `ALL`, not just missing
    /// by omission. `_exhaustive` below is the separate, unrelated
    /// guarantee that every `Opcode` variant (harvested or not) is
    /// handled by at least one match arm somewhere that matches on
    /// `Opcode` exhaustively — it does not require a variant to appear
    /// in `ALL`.
    pub const ALL: [Opcode; 68] = [
        Opcode::Init,
        Opcode::Goto,
        Opcode::Once,
        Opcode::BeginSubrtn,
        Opcode::Return,
        Opcode::Halt,
        Opcode::Transaction,
        Opcode::IfNot,
        Opcode::IfNotZero,
        Opcode::IfPos,
        Opcode::DecrJumpZero,
        Opcode::IsNull,
        Opcode::NotNull,
        Opcode::MustBeInt,
        Opcode::OffsetLimit,
        Opcode::OpenRead,
        Opcode::OpenEphemeral,
        Opcode::OpenPseudo,
        Opcode::Rewind,
        Opcode::Last,
        Opcode::Next,
        Opcode::Column,
        Opcode::Rowid,
        Opcode::SeekRowid,
        Opcode::NullRow,
        Opcode::Sequence,
        Opcode::Found,
        Opcode::IdxInsert,
        Opcode::IdxLE,
        Opcode::Delete,
        Opcode::Eq,
        Opcode::Ge,
        Opcode::Gt,
        Opcode::Le,
        Opcode::Lt,
        Opcode::RealAffinity,
        Opcode::Add,
        Opcode::Subtract,
        Opcode::Multiply,
        Opcode::Divide,
        Opcode::Remainder,
        Opcode::Not,
        Opcode::BitAnd,
        Opcode::BitOr,
        Opcode::ShiftLeft,
        Opcode::ShiftRight,
        Opcode::BitNot,
        Opcode::Concat,
        Opcode::Cast,
        Opcode::Function,
        Opcode::AggStep,
        Opcode::AggFinal,
        Opcode::Copy,
        Opcode::Integer,
        Opcode::Int64,
        Opcode::Real,
        Opcode::Blob,
        Opcode::Null,
        Opcode::String8,
        Opcode::Variable,
        Opcode::MakeRecord,
        Opcode::ResultRow,
        Opcode::SorterOpen,
        Opcode::SorterInsert,
        Opcode::SorterSort,
        Opcode::SorterNext,
        Opcode::SorterData,
        Opcode::Sort,
    ];
}

/// Unused at runtime — its only job is to fail to compile if `Opcode`
/// gains a variant that `Opcode::ALL` doesn't list.
#[allow(dead_code)]
fn _exhaustive(o: Opcode) {
    match o {
        Opcode::Init
        | Opcode::Goto
        | Opcode::Once
        | Opcode::BeginSubrtn
        | Opcode::Return
        | Opcode::Halt
        | Opcode::Transaction
        | Opcode::AutoCommit
        | Opcode::SetJournalMode
        | Opcode::IntegrityCheck
        | Opcode::Synchronous
        | Opcode::IfNot
        | Opcode::IfNotZero
        | Opcode::IfPos
        | Opcode::DecrJumpZero
        | Opcode::IsNull
        | Opcode::NotNull
        | Opcode::MustBeInt
        | Opcode::OffsetLimit
        | Opcode::OpenRead
        | Opcode::OpenWrite
        | Opcode::OpenEphemeral
        | Opcode::OpenDup
        | Opcode::OpenPseudo
        | Opcode::Rewind
        | Opcode::Last
        | Opcode::Next
        | Opcode::Column
        | Opcode::Rowid
        | Opcode::SeekRowid
        | Opcode::NullRow
        | Opcode::Sequence
        | Opcode::Found
        | Opcode::IdxInsert
        | Opcode::IdxLE
        | Opcode::Delete
        | Opcode::Insert
        | Opcode::NewRowid
        | Opcode::IdxDelete
        | Opcode::Count
        | Opcode::NoConflict
        | Opcode::SeekIndexEq
        | Opcode::IdxRowid
        | Opcode::SeekIndexGE
        | Opcode::IdxCompareGT
        | Opcode::IdxRewind
        | Opcode::IdxLast
        | Opcode::IdxNext
        | Opcode::IdxPrev
        | Opcode::AutoIndexInsert
        | Opcode::AutoIndexSeek
        | Opcode::AutoIndexRowid
        | Opcode::AutoIndexNext
        | Opcode::CreateTable
        | Opcode::CreateView
        | Opcode::DropTable
        | Opcode::CreateIndex
        | Opcode::DropIndex
        | Opcode::Analyze
        | Opcode::Eq
        | Opcode::Ge
        | Opcode::Gt
        | Opcode::Le
        | Opcode::Lt
        | Opcode::RealAffinity
        | Opcode::Add
        | Opcode::Subtract
        | Opcode::Multiply
        | Opcode::Divide
        | Opcode::Remainder
        | Opcode::Not
        | Opcode::BitAnd
        | Opcode::BitOr
        | Opcode::ShiftLeft
        | Opcode::ShiftRight
        | Opcode::BitNot
        | Opcode::Concat
        | Opcode::Cast
        | Opcode::Function
        | Opcode::AggStep
        | Opcode::AggFinal
        | Opcode::Integer
        | Opcode::Int64
        | Opcode::Real
        | Opcode::Blob
        | Opcode::Null
        | Opcode::String8
        | Opcode::Variable
        | Opcode::MakeRecord
        | Opcode::ResultRow
        | Opcode::Copy
        | Opcode::SorterOpen
        | Opcode::SorterInsert
        | Opcode::SorterSort
        | Opcode::SorterNext
        | Opcode::SorterData
        | Opcode::Sort
        | Opcode::HashAggOpen
        | Opcode::HashAggFind
        | Opcode::HashAggStep
        | Opcode::HashAggRewind
        | Opcode::HashAggData
        | Opcode::HashAggNext => {}
    }
}

/// One column of a [`SorterOpen`](Opcode::SorterOpen) sort-key
/// descriptor: sort direction plus the collation to compare under —
/// e.g. the harvested `"k(2,-B,B)"` (2 keys, first descending, both
/// BINARY) becomes `[{descending: true, ..}, {descending: false, ..}]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortKeyColumn {
    /// Index into the sorter's record payload this key column reads —
    /// not assumed to be the key's position among `SortKey`'s own
    /// `Vec` (a sort key need not be the record's leading columns).
    pub index: usize,
    /// Whether this key column sorts descending (`true`) or ascending.
    pub descending: bool,
    /// The collating sequence to compare this key column's values under.
    pub collation: Collation,
    /// Whether NULL sorts before non-NULL values for this key, per an
    /// explicit `NULLS FIRST`/`NULLS LAST` clause (or SQLite's default:
    /// NULLs first for ASC, last for DESC, when no clause is given).
    pub nulls_first: bool,
}

/// One column of a [`HashAggOpen`](Opcode::HashAggOpen) group-key
/// descriptor (#570) — the hash-aggregation counterpart of
/// [`SortKeyColumn`]. Deliberately its own struct rather than a reuse
/// of `SortKeyColumn`: a hash table has no ordering to describe (no
/// `descending`/`nulls_first`), but it *does* need the comparison
/// affinity the sort strategy carries on its group-boundary `Eq`'s
/// [`P4::CollSeq`] instead — hash equality has no such per-comparison
/// operand to hang it off, so it travels with the key descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupKeyColumn {
    /// Index into the inserted record's payload this key column reads.
    pub index: usize,
    /// The collating sequence two values of this key column are
    /// considered equal under.
    pub collation: Collation,
    /// The comparison affinity applied to this key column's values
    /// before they are compared, per SQLite's affinity codes
    /// ([`crate::vdbe::affinity::Affinity::to_p4_byte`]) — the same
    /// byte the sort strategy puts on its boundary-check `Eq`.
    pub affinity: u8,
}

/// P4's dynamic type: absent, an integer constant, a string constant
/// (or function/index descriptor), a collation-sequence-plus-affinity
/// descriptor used by the compare opcodes (e.g. `"BINARY-8"`), or a
/// sorter's per-column sort-key descriptor.
#[derive(Debug, Clone, PartialEq)]
pub enum P4 {
    /// No P4 operand.
    None,
    /// An integer constant operand.
    Int(i64),
    /// A floating-point constant operand.
    Real(f64),
    /// A blob constant operand.
    Blob(Vec<u8>),
    /// A string constant, or function/index descriptor, operand.
    Str(String),
    /// A collation-sequence-plus-affinity descriptor used by the
    /// compare opcodes (e.g. `"BINARY-8"`).
    CollSeq {
        /// The collating sequence to compare under.
        collation: Collation,
        /// The comparison affinity byte, per SQLite's affinity codes.
        affinity: u8,
    },
    /// `SeekIndexEq`'s per-key-column descriptor (#500): each probed
    /// column's declared `COLLATE` (default [`Collation::Binary`]),
    /// position-for-position with the probe registers starting at `P3`
    /// — the key-column count `seek_index_eq` used to read out of a
    /// plain `P4::Int` is just this list's length.
    SeekKey(Vec<Collation>),
    /// `AggStep`'s `"name(arity)"` descriptor plus the collation
    /// `min`/`max` compares under (#263) — `AggFinal` has no
    /// comparison to perform, so it keeps the plain `Str` descriptor.
    AggFunc {
        /// The aggregate function's name.
        name: String,
        /// The aggregate function's argument count.
        arity: usize,
        /// The collation `min`/`max` compares under.
        collation: Collation,
    },
    /// A sorter's per-column sort-key descriptor.
    SortKey(Vec<SortKeyColumn>),
    /// A hash-aggregation table's per-column group-key descriptor
    /// (#570) — [`HashAggOpen`](Opcode::HashAggOpen)'s counterpart to
    /// [`P4::SortKey`].
    GroupKey(Vec<GroupKeyColumn>),
    /// `MakeRecord`'s (#194) per-column affinity string, one
    /// [`crate::vdbe::affinity::Affinity`] byte
    /// (`Affinity::to_p4_byte`'s `'A'..='E'` convention) per source
    /// register, applied before encoding — SQLite's own
    /// `P4_KEYINFO`/affinity-string convention (`sqlite3VdbeMakeLabel`'s
    /// affinity string), modeled minimally as an owned byte string
    /// rather than a dedicated `KeyInfo` struct.
    Affinity(Vec<u8>),
    /// A boolean flag operand — used by `NewRowid` (#194) to request
    /// AUTOINCREMENT handling (checking/bumping `sqlite_sequence`)
    /// when the VDBE layer has no other place to carry that bit.
    Bool(bool),
    /// `CreateTable` (#215): the new table's name and verbatim
    /// `sqlite_master.sql` text (sliced from the original source via the
    /// AST's `span`, not reconstructed from the parsed columns).
    CreateTable {
        /// The new table's name.
        name: String,
        /// The verbatim `sqlite_master.sql` text.
        sql: String,
    },
    /// `CreateView` (#380): the new view's name and verbatim
    /// `sqlite_master.sql` text — same shape as `CreateTable`'s payload,
    /// but its own variant (rather than reusing `P4::CreateTable`) so a
    /// `Program`'s P4 operand names the DDL kind it actually came from,
    /// matching `CreateIndex`'s own dedicated variant below.
    CreateView {
        /// The new view's name.
        name: String,
        /// The verbatim `sqlite_master.sql` text.
        sql: String,
    },
    /// `DropTable` (#215): the target table's name/root page, plus every
    /// index on it (`(name, root_page)`) to cascade-drop in the same
    /// statement.
    DropTable {
        /// The target table's name.
        name: String,
        /// The target table's root page.
        root_page: u32,
        /// Every index on the table, as `(name, root_page)`, to
        /// cascade-drop in the same statement.
        indexes: Vec<(String, u32)>,
    },
    /// `CreateIndex` (#215): the new index's name, its target table's
    /// name/root page (to scan and populate entries for pre-existing
    /// rows), verbatim `sqlite_master.sql` text, the indexed columns'
    /// 0-based positions in table-column order, and the `UNIQUE` flag.
    CreateIndex {
        /// The new index's name.
        name: String,
        /// The target table's name.
        table_name: String,
        /// The target table's root page, to scan and populate entries
        /// for pre-existing rows.
        table_root_page: u32,
        /// The verbatim `sqlite_master.sql` text.
        sql: String,
        /// The indexed columns' 0-based positions in table-column order.
        column_indices: Vec<usize>,
        /// Whether the index enforces a `UNIQUE` constraint.
        unique: bool,
    },
    /// `DropIndex` (#215): the target index's name/root page.
    DropIndex {
        /// The target index's name.
        name: String,
        /// The target index's root page.
        root_page: u32,
    },
    /// `Analyze` (#461, spec 011): every table `ANALYZE` should populate
    /// stats for — baked at codegen time from the schema catalog (root
    /// pages, index names/root pages) the same way `CreateIndex`/
    /// `DropTable` bake theirs, so the exec-time handler never needs to
    /// re-resolve names against `sqlite_master`.
    Analyze {
        /// Every table (and its indexes) `ANALYZE` should populate stats for.
        targets: Vec<AnalyzeTarget>,
    },
}

/// One table `ANALYZE` (#461) populates `sqlite_stat1` for: its name and
/// table-b-tree root page, plus every index on it (name + root page) to
/// walk for index-level stats.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeTarget {
    /// The table's name.
    pub table_name: String,
    /// The table b-tree's root page.
    pub table_root_page: u32,
    /// The table's indexes, each walked for index-level stats.
    pub indexes: Vec<AnalyzeIndexTarget>,
}

/// One index `ANALYZE` (#461) walks to compute `avg_eq` for.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeIndexTarget {
    /// The index's name.
    pub index_name: String,
    /// The index b-tree's root page.
    pub root_page: u32,
}

/// A single fixed-shape bytecode instruction: an opcode tag, three
/// integer operands (`P1`/`P2`/`P3`), one dynamically-typed operand
/// (`P4`), and one flags operand (`P5`) — matching SQLite's own `Op`
/// struct shape.
#[derive(Debug, Clone, PartialEq)]
pub struct Instruction {
    /// The instruction's opcode tag.
    pub opcode: Opcode,
    /// First integer operand.
    pub p1: i32,
    /// Second integer operand.
    pub p2: i32,
    /// Third integer operand.
    pub p3: i32,
    /// Dynamically-typed fourth operand.
    pub p4: P4,
    /// Flags operand.
    pub p5: u16,
}

/// `P5` bit marking an `Insert`/`Delete` as *the* row change a statement
/// should report through the rows-changed counter (013/Req 1, #692).
///
/// Same value and same job as stock SQLite's `OPFLAG_NCHANGE`. The flag
/// exists because the opcode alone cannot tell you: one `UPDATE`ed row
/// emits a `Delete` *and* an `Insert` (`codegen/stmt/update.rs`), and the
/// two-pass range-seek plan additionally emits an `Insert` against an
/// ephemeral cursor to stash matched rowids, so counting every
/// `Insert`/`Delete` as it executes reports 3 for a plan that changed 1
/// row and 2 for the other plan of the same statement. Codegen knows
/// which mutation is the row change; the handler does not.
pub const OPFLAG_NCHANGE: u16 = 0x01;

/// `P5` bit marking the `Insert` whose rowid `Connection::last_insert_rowid`
/// should report (013/Req 1).
///
/// Same value (`0x20`) and same job as stock SQLite's `OPFLAG_LASTROWID`
/// (`sqliteInt.h:4068`, pinned 3.53.4). Two properties of the upstream
/// handler are worth stating because they are not guessable from the name,
/// and both are reproduced in `cursor::insert`:
///
/// 1. **It nests inside [`OPFLAG_NCHANGE`].** `vdbe.c:5803` updates
///    `db->lastRowid` *within* the `if( p5 & OPFLAG_NCHANGE )` arm, and
///    `vdbe.c:5800` asserts `LASTROWID` implies `NCHANGE`. So a mutation
///    that is not a counted row change never moves the rowid either.
/// 2. **An `UPDATE` does not set it.** `insert.c:2834` reads
///    `pik_flags |= (update_flags ? update_flags : OPFLAG_LASTROWID)` — the
///    flag is the *else* branch, so rewriting a row leaves the last-insert
///    rowid alone. `codegen/stmt/update.rs`'s `Insert` therefore carries
///    `OPFLAG_NCHANGE` only.
///
/// The rowid recorded is the one the insert actually used, which is why the
/// hook is here and not on `NewRowid`: for `INSERT INTO t(id, ...)` over an
/// `INTEGER PRIMARY KEY`, the rowid comes from the bound value and
/// `NewRowid` never executes.
pub const OPFLAG_LASTROWID: u16 = 0x20;

impl Instruction {
    /// Builds an instruction with `P4` absent and `P5` zero — the common
    /// case for control/arithmetic/compare opcodes that only use
    /// `P1`/`P2`/`P3`.
    pub fn new(opcode: Opcode, p1: i32, p2: i32, p3: i32) -> Self {
        Self {
            opcode,
            p1,
            p2,
            p3,
            p4: P4::None,
            p5: 0,
        }
    }

    /// Builds an instruction carrying a `P4` operand.
    pub fn with_p4(opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4) -> Self {
        Self {
            opcode,
            p1,
            p2,
            p3,
            p4,
            p5: 0,
        }
    }

    /// Builds an instruction with an explicit `P5` flags operand — the
    /// `with_p4` constructor's counterpart for the flags word. Used for
    /// [`OPFLAG_NCHANGE`]; `new`/`with_p4` both leave `p5` zero.
    pub fn with_p5(opcode: Opcode, p1: i32, p2: i32, p3: i32, p5: u16) -> Self {
        Self {
            opcode,
            p1,
            p2,
            p3,
            p4: P4::None,
            p5,
        }
    }
}

/// A linear, zero-indexed sequence of instructions. Execution starts at
/// PC 0 and advances by incrementing PC unless an instruction explicitly
/// redirects it (jump, subroutine call/return, or `Halt`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Program {
    /// The program's instructions, in execution order.
    pub instructions: Vec<Instruction>,
}

impl Program {
    /// Builds a program from its instruction sequence.
    pub fn new(instructions: Vec<Instruction>) -> Self {
        Self { instructions }
    }

    /// Whether this program is a statement whose rows-changed count is
    /// meaningful (013/Req 1, #692) — i.e. whether codegen flagged any
    /// mutation with [`OPFLAG_NCHANGE`].
    ///
    /// Deliberately *static*: it asks what the program contains, not what
    /// it executed. An `UPDATE` whose `WHERE` matches no row never runs
    /// its flagged `Insert`, but must still report a count of zero rather
    /// than "not a counting statement" — which is the distinction
    /// `sqlite3_changes()` needs in order to leave the previous count
    /// alone after a `SELECT`.
    ///
    /// Derived rather than stored so it cannot disagree with the
    /// instructions it describes; `Program` is a `Vec<Instruction>` and
    /// programs are a handful of instructions per statement.
    pub fn counts_changes(&self) -> bool {
        self.instructions.iter().any(|i| {
            i.p5 & OPFLAG_NCHANGE != 0 && matches!(i.opcode, Opcode::Insert | Opcode::Delete)
        })
    }

    /// The highest 1-based parameter index this program will read, or `0`
    /// when it has no placeholders (013/Req 3).
    ///
    /// Derived rather than stored, for the same reason
    /// [`Self::counts_changes`] is: a stored field can disagree with the
    /// instructions it describes, and [`Program::new`] is public.
    ///
    /// It also measures the right thing. This counts what the VM will
    /// *read* — `Opcode::Variable`'s `P1` — not what the compiler handed
    /// out, so `?3` alone reports 3 (matching `sqlite3_bind_parameter_count`,
    /// which returns the largest index, not the number of distinct ones)
    /// and a placeholder the optimizer folded away is not demanded.
    pub fn param_count(&self) -> usize {
        self.instructions
            .iter()
            .filter(|i| matches!(i.opcode, Opcode::Variable))
            .filter_map(|i| usize::try_from(i.p1).ok())
            .max()
            .unwrap_or(0)
    }

    /// Returns the instruction at `pc`, or `None` if `pc` is out of
    /// range.
    pub fn get(&self, pc: usize) -> Option<&Instruction> {
        self.instructions.get(pc)
    }

    /// The number of instructions in the program.
    pub fn len(&self) -> usize {
        self.instructions.len()
    }

    /// Whether the program has no instructions.
    pub fn is_empty(&self) -> bool {
        self.instructions.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn instruction_carries_typed_p4_variant() {
        // Mirrors the harvested `Ge` instruction (SELECT * FROM products
        // WHERE price >= 10 AND qty < 50): P4 is a collation-sequence
        // descriptor, not an integer or absent value.
        let ge = Instruction::with_p4(
            Opcode::Ge,
            1,
            2,
            3,
            P4::CollSeq {
                collation: Collation::Binary,
                affinity: 8,
            },
        );
        assert!(matches!(ge.p4, P4::CollSeq { .. }));
    }

    #[test]
    fn is_empty_reflects_whether_any_instructions_were_added() {
        assert!(Program::new(vec![]).is_empty());
        assert!(!Program::new(vec![Instruction::new(Opcode::Halt, 0, 0, 0)]).is_empty());
    }

    #[test]
    fn program_indexes_instructions_from_zero() {
        let program = Program::new(vec![
            Instruction::new(Opcode::Init, 0, 1, 0),
            Instruction::new(Opcode::Halt, 0, 0, 0),
        ]);
        assert_eq!(program.get(0).unwrap().opcode, Opcode::Init);
        assert_eq!(program.get(1).unwrap().opcode, Opcode::Halt);
        assert!(program.get(2).is_none());
    }
}
