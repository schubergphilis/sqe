// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Table b-tree read path (Tier 0): a read-only cursor over table b-trees
//! (page types 0x05 interior / 0x0d leaf), including overflow-chain
//! reassembly. See `.openspec/specs/006-btree/spec.md` for the page/cell
//! byte layout this module implements.
//!
//! Page-1 trap: page 1 carries the 100-byte file header before its own
//! b-tree page header, but its cell-pointer array is still relative to
//! byte 0 of the page (see `src/header.rs`'s module doc). Every page-1
//! read in this module resolves cell offsets from page start, not from
//! `header_start`.
//!
//! Rowid-alias note: a column declared exactly `INTEGER PRIMARY KEY` is
//! not stored in the record — SQLite encodes it as NULL and expects the
//! reader to substitute the cell's own rowid. This module returns the
//! record payload faithfully (NULL and all); substituting the alias
//! column is a schema-aware operation that belongs above this layer,
//! once the DDL reader (step 7) knows which column, if any, is the alias.

mod error;
mod index;
mod master;
mod schema;
mod table;

pub use error::BtreeError;
pub use index::{delete_entry, insert_entry, IndexCursor, IndexRow};
pub use master::{
    bump_schema_cookie, delete_master_row, delete_stat1_rows_for_table,
    ensure_sqlite_sequence_table, ensure_sqlite_stat1_table, insert_master_row, insert_stat1_row,
    update_sequence, MasterEntry, SQLITE_MASTER_ROOT_PAGE,
};
pub use schema::{create_empty_index_root, create_empty_table_root, populate_index_from_table};
pub use table::{delete_row, insert_row};

use std::ops::Deref;
use std::rc::Rc;

use crate::header::DatabaseHeader;
use crate::record::{decode_varint, encode_varint};
use crate::vfs::PageSource;

const LEAF_TABLE: u8 = 0x0d;
const INTERIOR_TABLE: u8 = 0x05;

/// SQLite's documented maximum size for a single value (2^31 - 1 bytes) —
/// used to reject implausible `payload_len` claims before attempting an
/// allocation.
const MAX_PAYLOAD_LEN: u64 = 2_147_483_647;

/// Sanity cap on total pages visited by a single cursor traversal or
/// overflow-chain walk, guarding against a corrupt/cyclic file causing an
/// unbounded loop.
const MAX_PAGES_VISITED: usize = 1_000_000;

/// A table row's raw record payload: a zero-copy slice into the page it
/// was read from when the payload fits entirely in the cell (no overflow
/// pages — the common case), or an owned buffer when overflow-chain
/// reassembly had to concatenate bytes from multiple pages (#467).
/// `Deref<Target = [u8]>` lets every existing `&row.payload` call site
/// (e.g. `decode_record(&row.payload, ..)`) keep working unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// Zero-copy view into a page buffer: no overflow chain, the payload
    /// fit entirely in the cell.
    Local {
        /// The page buffer the payload bytes live in.
        page: Rc<[u8]>,
        /// Byte offset of the payload's start within `page`.
        start: usize,
        /// Payload length in bytes.
        len: usize,
    },
    /// Owned buffer holding bytes reassembled from an overflow chain.
    Owned(Vec<u8>),
}

impl Deref for Payload {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            #[allow(
                clippy::indexing_slicing,
                reason = "start/len are computed from a validated in-bounds slice at construction (reassemble_payload)"
            )]
            Payload::Local { page, start, len } => &page[*start..start.saturating_add(*len)],
            Payload::Owned(bytes) => bytes,
        }
    }
}

/// One decoded table b-tree row: the SQLite rowid and its raw record
/// payload (after overflow-chain reassembly). See the module doc's
/// rowid-alias note — `payload` may encode a rowid-alias column as NULL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    /// The row's SQLite rowid.
    pub rowid: i64,
    /// The row's raw record payload.
    pub payload: Payload,
}

struct Frame {
    page_num: u32,
    is_interior: bool,
    num_cells: usize,
    cell_ptr_base: usize,
    next_cell: usize,
    rightmost: u32,
    rightmost_done: bool,
    page: Rc<[u8]>,
}

/// Everything needed to reassemble the payload of the row the cursor is
/// currently positioned at, captured cheaply (an `Rc` clone, not a copy)
/// at position time so [`TableCursor::current_payload`] can defer the
/// actual reassembly — including any overflow-chain page reads — until
/// something asks for it (#473). Mirrors real SQLite's `BtCursor`, which
/// holds a pointer into a pager-pinned page and re-fetches payload bytes
/// live off the still-positioned cursor rather than pre-decoding a row.
struct CurrentCell {
    page: Rc<[u8]>,
    page_num: u32,
    tail_start: usize,
    payload_len: u64,
}

/// Depth-first, read-only cursor over a table b-tree, yielding rows in
/// ascending rowid order.
pub struct TableCursor<P: PageSource> {
    source: P,
    usable_size: u32,
    root_page: u32,
    stack: Vec<Frame>,
    pages_visited: usize,
    positioned_reverse: bool,
    current: Option<CurrentCell>,
}

impl<P: PageSource> TableCursor<P> {
    /// Creates a cursor over the table b-tree rooted at `root_page`,
    /// unpositioned until [`Self::first`] or [`Self::last`] is called.
    pub fn new(source: P, header: &DatabaseHeader, root_page: u32) -> Self {
        TableCursor {
            source,
            usable_size: header.usable_page_size(),
            root_page,
            stack: Vec::new(),
            pages_visited: 0,
            positioned_reverse: false,
            current: None,
        }
    }

    /// Positions the cursor at the first row and returns its rowid, or
    /// `None` if the table is empty. Resets any prior traversal position.
    /// Call [`Self::current_payload`] to fetch the row's payload —
    /// reassembled lazily, only when actually needed (#473) — or use
    /// [`Self::first_row`] for the old eager (rowid + payload together)
    /// behavior.
    pub fn first(&mut self) -> Result<Option<i64>, BtreeError> {
        self.stack.clear();
        self.pages_visited = 0;
        self.positioned_reverse = false;
        self.push_page(self.root_page, false)?;
        self.advance()
    }

    /// Convenience wrapper over [`Self::first`] that also eagerly fetches
    /// the payload via [`Self::current_payload`], for callers that always
    /// want both together (the common case outside the VDBE's per-column
    /// `Column` opcode, which wants to defer payload reassembly).
    pub fn first_row(&mut self) -> Result<Option<TableRow>, BtreeError> {
        self.first()?.map_or(Ok(None), |rowid| {
            Ok(Some(TableRow {
                rowid,
                payload: self.current_payload()?,
            }))
        })
    }

    /// Advances to the next row and returns its rowid, or `None` once
    /// exhausted. Call [`Self::first`] first; calling `next` before
    /// `first` behaves as an empty cursor. See [`Self::first`]'s doc for
    /// how to fetch the payload.
    #[allow(clippy::should_implement_trait)] // deliberate cursor API (first/next/seek), not std::iter::Iterator
    pub fn next(&mut self) -> Result<Option<i64>, BtreeError> {
        self.advance()
    }

    /// Convenience wrapper over [`Self::next`] — see [`Self::first_row`].
    pub fn next_row(&mut self) -> Result<Option<TableRow>, BtreeError> {
        self.next()?.map_or(Ok(None), |rowid| {
            Ok(Some(TableRow {
                rowid,
                payload: self.current_payload()?,
            }))
        })
    }

    /// Positions the cursor at the last row (highest rowid) and returns
    /// its rowid, or `None` if the table is empty. Resets any prior
    /// traversal position. Descends the rightmost child at each interior
    /// page (highest-key subtree), then the highest-index cell of the
    /// leaf. See [`Self::first`]'s doc for how to fetch the payload.
    pub fn last(&mut self) -> Result<Option<i64>, BtreeError> {
        self.stack.clear();
        self.pages_visited = 0;
        self.positioned_reverse = true;
        self.push_page(self.root_page, true)?;
        self.advance_rev()
    }

    /// Convenience wrapper over [`Self::last`] — see [`Self::first_row`].
    pub fn last_row(&mut self) -> Result<Option<TableRow>, BtreeError> {
        self.last()?.map_or(Ok(None), |rowid| {
            Ok(Some(TableRow {
                rowid,
                payload: self.current_payload()?,
            }))
        })
    }

    /// Steps backward to the previous row (in descending rowid order),
    /// returning its rowid, or `None` once exhausted. Call [`Self::last`]
    /// first; a cursor positioned via `first`/`next` cannot be walked
    /// backward with `prev` — the two directions maintain independent
    /// stack state. See [`Self::first`]'s doc for how to fetch the
    /// payload.
    ///
    /// Calling `prev()` before any `last()` is a usage error, reported as
    /// [`BtreeError::CursorNotPositioned`] rather than silently returning
    /// `None` (empty stack), which is indistinguishable from "table
    /// exhausted." Checked in every build, not just debug ones — a
    /// misuse that only surfaces under `debug_assert` is a misuse that
    /// reaches release.
    pub fn prev(&mut self) -> Result<Option<i64>, BtreeError> {
        if !self.positioned_reverse {
            return Err(BtreeError::CursorNotPositioned {
                operation: "TableCursor::prev()",
                required: "TableCursor::last()",
            });
        }
        self.advance_rev()
    }

    /// Convenience wrapper over [`Self::prev`] — see [`Self::first_row`].
    pub fn prev_row(&mut self) -> Result<Option<TableRow>, BtreeError> {
        self.prev()?.map_or(Ok(None), |rowid| {
            Ok(Some(TableRow {
                rowid,
                payload: self.current_payload()?,
            }))
        })
    }

    /// Returns the payload of the row the cursor is currently positioned
    /// at — i.e. the row whose rowid was last returned by
    /// [`Self::first`]/[`Self::next`]/[`Self::last`]/[`Self::prev`]/
    /// [`Self::seek`] as `Some(_)`. Reassembles lazily (including walking
    /// any overflow chain) on every call rather than caching — cheap for
    /// the common local-payload case (an `Rc` clone), real work only for
    /// the rare overflow case, and only paid by callers that actually
    /// need the bytes (#473).
    pub fn current_payload(&self) -> Result<Payload, BtreeError> {
        let cur = self
            .current
            .as_ref()
            .ok_or(BtreeError::CursorNotPositioned {
                operation: "TableCursor::current_payload()",
                required: "first()/next()/last()/prev()/seek() returning Some(rowid)",
            })?;
        reassemble_payload(
            &self.source,
            self.usable_size,
            cur.page_num,
            &cur.page,
            cur.tail_start,
            cur.payload_len,
            false,
        )
    }

    /// Looks up the row with exactly `target_rowid`, independent of the
    /// `first`/`next` traversal position, and returns its rowid (i.e.
    /// `target_rowid` itself) if found. See [`Self::first`]'s doc for how
    /// to fetch the payload, or use [`Self::seek_row`].
    pub fn seek(&mut self, target_rowid: i64) -> Result<Option<i64>, BtreeError> {
        self.current = None;
        let mut page_num = self.root_page;
        // A local budget, independent of `self.pages_visited` — `seek` is a
        // standalone point lookup, not part of the `first`/`next` traversal,
        // so it must not accumulate against (or be capped by) unrelated
        // calls made earlier or later on this same long-lived cursor.
        let mut visited = 0usize;
        loop {
            visited = visited.saturating_add(1);
            if visited > MAX_PAGES_VISITED {
                return Err(BtreeError::TraversalTooLong {
                    max: MAX_PAGES_VISITED,
                });
            }
            let page = self
                .source
                .read_page(page_num)
                .map_err(|source| BtreeError::PageSource { page_num, source })?;
            let header_start = page1_header_start(page_num);
            let page_type = read_page_type(&page, header_start, page_num)?;
            let num_cells = read_num_cells(&page, header_start, page_num)?;

            match page_type {
                LEAF_TABLE => {
                    let cell_ptr_base = header_start.saturating_add(8);
                    // Leaf cells are stored in rowid-ascending order by
                    // pointer-array index (see the module's ordering
                    // invariant), so a binary search over `i` decodes only
                    // O(log n) cells instead of scanning every cell on the
                    // page — this matters a lot for repeated seeks (e.g. a
                    // join probing the same small table per outer row).
                    let mut lo = 0usize;
                    let mut hi = num_cells;
                    while lo < hi {
                        let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
                        let cell_start = read_cell_pointer(
                            &page,
                            cell_ptr_offset(cell_ptr_base, mid),
                            page_num,
                            mid,
                        )?;
                        let (rowid, payload_len, tail_start) =
                            decode_cell_head(&page, cell_start, page_num)?;
                        match rowid.cmp(&target_rowid) {
                            std::cmp::Ordering::Equal => {
                                self.current = Some(CurrentCell {
                                    page: Rc::clone(&page),
                                    page_num,
                                    tail_start,
                                    payload_len,
                                });
                                return Ok(Some(rowid));
                            }
                            std::cmp::Ordering::Less => lo = mid.saturating_add(1),
                            std::cmp::Ordering::Greater => hi = mid,
                        }
                    }
                    return Ok(None);
                }
                INTERIOR_TABLE => {
                    require_interior_header(&page, header_start, page_num)?;
                    let cell_ptr_base = header_start.saturating_add(12);
                    let rightmost = read_u32(&page, header_start.saturating_add(8), page_num)?;
                    // Interior separator keys are ascending by pointer-array
                    // index too; binary search for the leftmost `i` with
                    // `target_rowid <= key(i)` (the child to descend into),
                    // falling back to `rightmost` when none qualifies —
                    // same semantics as the old early-break linear scan.
                    let mut lo = 0usize;
                    let mut hi = num_cells;
                    let mut next_page = rightmost;
                    while lo < hi {
                        let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
                        let cell_start = read_cell_pointer(
                            &page,
                            cell_ptr_offset(cell_ptr_base, mid),
                            page_num,
                            mid,
                        )?;
                        let key_bytes = page.get(cell_start.saturating_add(4)..).ok_or({
                            BtreeError::InvalidCellPointer {
                                page_num,
                                index: mid,
                            }
                        })?;
                        let (key, _) = decode_varint(key_bytes)
                            .map_err(|source| BtreeError::InvalidCellVarint { page_num, source })?;
                        if target_rowid <= key as i64 {
                            let child = read_u32(&page, cell_start, page_num)?;
                            next_page = child;
                            hi = mid;
                        } else {
                            lo = mid.saturating_add(1);
                        }
                    }
                    page_num = next_page;
                }
                other => {
                    return Err(BtreeError::UnexpectedPageType {
                        page_num,
                        page_type: other,
                    })
                }
            }
        }
    }

    /// Convenience wrapper over [`Self::seek`] — see [`Self::first_row`].
    pub fn seek_row(&mut self, target_rowid: i64) -> Result<Option<TableRow>, BtreeError> {
        self.seek(target_rowid)?.map_or(Ok(None), |rowid| {
            Ok(Some(TableRow {
                rowid,
                payload: self.current_payload()?,
            }))
        })
    }

    fn read_page(&mut self, page_num: u32) -> Result<Rc<[u8]>, BtreeError> {
        self.pages_visited = self.pages_visited.saturating_add(1);
        if self.pages_visited > MAX_PAGES_VISITED {
            return Err(BtreeError::TraversalTooLong {
                max: MAX_PAGES_VISITED,
            });
        }
        self.source
            .read_page(page_num)
            .map_err(|source| BtreeError::PageSource { page_num, source })
    }

    /// Pushes `page_num` onto the traversal stack. `reverse` selects the
    /// initial `next_cell` cursor: `0` for forward traversal (ascending
    /// cell index), `num_cells` for backward traversal (so
    /// [`Self::advance_rev`] decrements into range before reading) —
    /// see that method's doc for how the two directions interpret
    /// `next_cell`/`rightmost_done` differently.
    fn push_page(&mut self, page_num: u32, reverse: bool) -> Result<(), BtreeError> {
        let page = self.read_page(page_num)?;
        let header_start = page1_header_start(page_num);
        let page_type = read_page_type(&page, header_start, page_num)?;
        let is_interior = match page_type {
            LEAF_TABLE => false,
            INTERIOR_TABLE => true,
            other => {
                return Err(BtreeError::UnexpectedPageType {
                    page_num,
                    page_type: other,
                })
            }
        };
        if is_interior {
            require_interior_header(&page, header_start, page_num)?;
        }
        let num_cells = read_num_cells(&page, header_start, page_num)?;
        let (cell_ptr_base, rightmost) = if is_interior {
            (
                header_start.saturating_add(12),
                read_u32(&page, header_start.saturating_add(8), page_num)?,
            )
        } else {
            (header_start.saturating_add(8), 0)
        };
        self.stack.push(Frame {
            page_num,
            is_interior,
            num_cells,
            cell_ptr_base,
            next_cell: if reverse { num_cells } else { 0 },
            rightmost,
            rightmost_done: !is_interior,
            page,
        });
        Ok(())
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "top = stack.len() - 1, computed just above from a non-empty check; always in bounds"
    )]
    fn advance(&mut self) -> Result<Option<i64>, BtreeError> {
        loop {
            let top = match self.stack.len() {
                0 => {
                    self.current = None;
                    return Ok(None);
                }
                n => n.saturating_sub(1),
            };
            let (is_interior, next_cell, num_cells, rightmost, rightmost_done) = {
                let f = &self.stack[top];
                (
                    f.is_interior,
                    f.next_cell,
                    f.num_cells,
                    f.rightmost,
                    f.rightmost_done,
                )
            };

            if !is_interior {
                if next_cell >= num_cells {
                    self.stack.pop();
                    continue;
                }
                self.stack[top].next_cell = self.stack[top].next_cell.saturating_add(1);
                return self.decode_leaf_cell(top, next_cell).map(Some);
            }

            if next_cell < num_cells {
                self.stack[top].next_cell = self.stack[top].next_cell.saturating_add(1);
                let child = self.read_interior_child(top, next_cell)?;
                self.push_page(child, false)?;
            } else if !rightmost_done {
                self.stack[top].rightmost_done = true;
                self.push_page(rightmost, false)?;
            } else {
                self.stack.pop();
            }
        }
    }

    /// The mirror-image of [`Self::advance`]: walks in descending rowid
    /// order. At an interior page, the rightmost child (highest-key
    /// subtree) is descended first, then cells in descending index down
    /// to `0` — the opposite of `advance`'s ascending-index,
    /// rightmost-last order, matching the fact that cell `i`'s child
    /// covers keys strictly between cell `i-1`'s key and cell `i`'s key
    /// while the rightmost pointer covers keys past the last cell. At a
    /// leaf, cells are visited from `num_cells - 1` down to `0`.
    /// `next_cell` here counts the number of not-yet-visited cells
    /// remaining (from the low end), so `0` means fully exhausted in
    /// both interior and leaf frames — the same terminal condition
    /// `advance`'s ascending counter reaches from the other direction.
    #[allow(
        clippy::indexing_slicing,
        reason = "top = stack.len() - 1, computed just above from a non-empty check; always in bounds"
    )]
    fn advance_rev(&mut self) -> Result<Option<i64>, BtreeError> {
        loop {
            let top = match self.stack.len() {
                0 => {
                    self.current = None;
                    return Ok(None);
                }
                n => n.saturating_sub(1),
            };
            let (is_interior, next_cell, rightmost, rightmost_done) = {
                let f = &self.stack[top];
                (f.is_interior, f.next_cell, f.rightmost, f.rightmost_done)
            };

            if !is_interior {
                if next_cell == 0 {
                    self.stack.pop();
                    continue;
                }
                let idx = next_cell.saturating_sub(1);
                self.stack[top].next_cell = idx;
                return self.decode_leaf_cell(top, idx).map(Some);
            }

            if !rightmost_done {
                self.stack[top].rightmost_done = true;
                self.push_page(rightmost, true)?;
            } else if next_cell > 0 {
                let idx = next_cell.saturating_sub(1);
                self.stack[top].next_cell = idx;
                let child = self.read_interior_child(top, idx)?;
                self.push_page(child, true)?;
            } else {
                self.stack.pop();
            }
        }
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "frame_index is always `top` from advance(), always in bounds"
    )]
    fn read_interior_child(
        &self,
        frame_index: usize,
        cell_index: usize,
    ) -> Result<u32, BtreeError> {
        let frame = &self.stack[frame_index];
        let ptr_off = cell_ptr_offset(frame.cell_ptr_base, cell_index);
        let cell_start = read_cell_pointer(&frame.page, ptr_off, frame.page_num, cell_index)?;
        read_u32(&frame.page, cell_start, frame.page_num)
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "frame_index is always `top` from advance(), always in bounds"
    )]
    fn decode_leaf_cell(
        &mut self,
        frame_index: usize,
        cell_index: usize,
    ) -> Result<i64, BtreeError> {
        let (page, page_num, ptr_off) = {
            let frame = &self.stack[frame_index];
            (
                Rc::clone(&frame.page),
                frame.page_num,
                cell_ptr_offset(frame.cell_ptr_base, cell_index),
            )
        };
        let cell_start = read_cell_pointer(&page, ptr_off, page_num, cell_index)?;
        let (rowid, payload_len, tail_start) = decode_cell_head(&page, cell_start, page_num)?;
        self.current = Some(CurrentCell {
            page,
            page_num,
            tail_start,
            payload_len,
        });
        Ok(rowid)
    }
}

/// SQLite's overflow local-size formula (fileformat2.html "Cell Payload
/// Overflow"). `min_local` is shared by every cell kind, but `max_local`
/// is NOT: table leaf cells use `usable_size - 35`, while index cells
/// (leaf AND interior — table interior cells have no payload at all) use
/// `(usable_size - 12) * 64 / 255 - 23`, a smaller threshold. Passing the
/// wrong one for an index cell computes a `local_size` that doesn't fit
/// the space SQLite actually reserved on the page — caught in practice
/// once a fixture forces an index cell's payload past the *correct*
/// (smaller) index threshold while still under the table one (#Req 7).
/// All arithmetic saturates rather than panics — a pathological
/// `usable_size` degrades to a safe (wrong but non-panicking) answer,
/// caught by the length checks around the call site instead of an
/// arithmetic panic here.
fn local_payload_size(usable_size: u32, payload_len: u64, is_index: bool) -> u64 {
    let max_local = if is_index {
        ((usable_size.saturating_sub(12) as u64).saturating_mul(64) / 255).saturating_sub(23)
    } else {
        usable_size.saturating_sub(35) as u64
    };
    if payload_len <= max_local {
        return payload_len;
    }
    let min_local =
        ((usable_size.saturating_sub(12) as u64).saturating_mul(32) / 255).saturating_sub(23);
    let denom = usable_size.saturating_sub(4).max(1) as u64;
    #[allow(
        clippy::arithmetic_side_effects,
        reason = "denom is .max(1)'d just above, so % denom never divides by zero"
    )]
    let k = min_local.saturating_add(payload_len.saturating_sub(min_local) % denom);
    if k <= max_local {
        k
    } else {
        min_local
    }
}

/// Returns the first overflow page number for a cell whose local payload
/// starts at `buf[tail_start..]`, or `None` if `payload_len` fits
/// entirely within the page (no overflow chain). Shared by the table-leaf
/// and index (leaf/interior) overflow-freeing walk in
/// `free_btree_pages_inner`.
fn first_overflow_page(
    buf: &[u8],
    tail_start: usize,
    payload_len: u64,
    usable_size: u32,
    page_num: u32,
    is_index: bool,
) -> Result<Option<u32>, BtreeError> {
    let local_size = local_payload_size(usable_size, payload_len, is_index) as usize;
    if (local_size as u64) < payload_len {
        Ok(Some(read_u32(
            buf,
            tail_start.saturating_add(local_size),
            page_num,
        )?))
    } else {
        Ok(None)
    }
}

/// Walks and frees the overflow-page chain starting at `overflow_page`,
/// the on-disk linked list SQLite uses for payloads too large to fit
/// locally. Mirrors `reassemble_payload`'s chain walk (including its
/// cycle guard) but deallocates each page instead of copying its bytes.
/// Used by `free_btree_pages_inner` (DROP TABLE/DROP INDEX) to reclaim
/// overflow pages that would otherwise leak (#215 follow-up).
fn free_overflow_chain(
    pager: &mut crate::pager::Pager,
    owner_page_num: u32,
    mut overflow_page: u32,
    visited: &mut usize,
) -> Result<(), BtreeError> {
    let mut seen = std::collections::HashSet::new();
    while overflow_page != 0 {
        *visited = visited.saturating_add(1);
        if *visited > MAX_PAGES_VISITED {
            return Err(BtreeError::TraversalTooLong {
                max: MAX_PAGES_VISITED,
            });
        }
        if !seen.insert(overflow_page) {
            return Err(BtreeError::OverflowChainCycle {
                page_num: owner_page_num,
                revisited_page: overflow_page,
            });
        }
        let page = pager
            .read_page(overflow_page)
            .map_err(|source| BtreeError::PageSource {
                page_num: overflow_page,
                source,
            })?;
        let next = read_u32(&page, 0, overflow_page)?;
        pager.deallocate_page(overflow_page)?;
        overflow_page = next;
    }
    Ok(())
}

fn reassemble_payload<P: PageSource>(
    source: &P,
    usable_size: u32,
    page_num: u32,
    page: &Rc<[u8]>,
    tail_start: usize,
    payload_len: u64,
    is_index: bool,
) -> Result<Payload, BtreeError> {
    if payload_len > MAX_PAYLOAD_LEN {
        return Err(BtreeError::PayloadTooLarge {
            page_num,
            payload_len,
        });
    }
    let cell_tail = page
        .get(tail_start..)
        .ok_or(BtreeError::PayloadTooShort { page_num })?;
    let local_size = local_payload_size(usable_size, payload_len, is_index) as usize;
    let local_bytes = cell_tail
        .get(..local_size)
        .ok_or(BtreeError::PayloadTooShort { page_num })?;
    if local_size as u64 == payload_len {
        return Ok(Payload::Local {
            page: Rc::clone(page),
            start: tail_start,
            len: local_size,
        });
    }

    let overflow_end = local_size.saturating_add(4);
    let overflow_bytes: [u8; 4] = cell_tail
        .get(local_size..overflow_end)
        .ok_or(BtreeError::PayloadTooShort { page_num })?
        .try_into()
        .map_err(|_| BtreeError::PayloadTooShort { page_num })?;
    let mut overflow_page = u32::from_be_bytes(overflow_bytes);
    // local_size < payload_len here (the local_size == payload_len case
    // returned above), so this never underflows; saturating_sub keeps the
    // lint satisfied without asserting that invariant unsafely.
    let mut remaining = payload_len.saturating_sub(local_size as u64);
    // payload_len is bounds-checked against MAX_PAYLOAD_LEN above, so
    // preallocating the full length avoids the log₂(payload/page) regrow-
    // and-recopy chain a bare to_vec() + extend_from_slice would pay (#588).
    let mut result = Vec::with_capacity(payload_len as usize);
    result.extend_from_slice(local_bytes);
    let available = usable_size.saturating_sub(4).max(1) as u64;
    let mut hops = 0usize;
    // A legitimate SQLite overflow chain never revisits a page — each
    // overflow page is freshly allocated. Tracking visited page numbers
    // catches a chain that cycles through a small number of real pages
    // immediately, rather than relying solely on MAX_PAGES_VISITED (which a
    // cycling chain could otherwise ride all the way up to, forcing up to
    // ~64GB of reads/copies out of a file only a couple of pages large).
    let mut visited_overflow_pages = std::collections::HashSet::new();

    while remaining > 0 {
        if overflow_page == 0 {
            return Err(BtreeError::OverflowChainTruncated { page_num });
        }
        if !visited_overflow_pages.insert(overflow_page) {
            return Err(BtreeError::OverflowChainCycle {
                page_num,
                revisited_page: overflow_page,
            });
        }
        hops = hops.saturating_add(1);
        if hops > MAX_PAGES_VISITED {
            return Err(BtreeError::OverflowChainTooLong {
                page_num,
                max: MAX_PAGES_VISITED,
            });
        }
        let page = source
            .read_page(overflow_page)
            .map_err(|source| BtreeError::PageSource {
                page_num: overflow_page,
                source,
            })?;
        let next = read_u32(&page, 0, overflow_page)?;
        let take = remaining.min(available) as usize;
        let chunk =
            page.get(4..4usize.saturating_add(take))
                .ok_or_else(|| BtreeError::PageTooShort {
                    page_num: overflow_page,
                    len: page.len(),
                })?;
        result.extend_from_slice(chunk);
        remaining = remaining.saturating_sub(take as u64);
        overflow_page = next;
    }
    Ok(Payload::Owned(result))
}

/// Descends from `page_num` (a table b-tree root or subtree) to the leaf
/// that should hold `rowid`, returning the ancestor interior page numbers
/// (root-to-parent order) and the target leaf page number. Shared by the
/// insert and delete write paths.
pub(super) fn find_leaf_page(
    pager: &mut crate::pager::Pager,
    mut page_num: u32,
    rowid: i64,
) -> Result<(Vec<u32>, u32), BtreeError> {
    let mut ancestors = Vec::new();
    let mut visited = 0usize;
    loop {
        visited = visited.saturating_add(1);
        if visited > MAX_PAGES_VISITED {
            return Err(BtreeError::TraversalTooLong {
                max: MAX_PAGES_VISITED,
            });
        }
        let page = pager
            .read_page(page_num)
            .map_err(|source| BtreeError::PageSource { page_num, source })?;
        let header_start = page1_header_start(page_num);
        let page_type = read_page_type(&page, header_start, page_num)?;
        if page_type == LEAF_TABLE {
            return Ok((ancestors, page_num));
        } else if page_type == INTERIOR_TABLE {
            let (entries, rightmost) = collect_interior_entries(&page, header_start, page_num)?;
            ancestors.push(page_num);
            let mut next = rightmost;
            for (child, key) in &entries {
                if rowid <= *key {
                    next = *child;
                    break;
                }
            }
            page_num = next;
        } else {
            return Err(BtreeError::UnexpectedPageType {
                page_num,
                page_type,
            });
        }
    }
}

/// Frees every page of the b-tree (table or index) rooted at `root_page`,
/// walking interior pages depth-first and freeing children before their
/// parent, finally freeing `root_page` itself. Also walks and frees each
/// leaf/interior cell's overflow-page chain (if any), so a DROP on a
/// b-tree whose rows/entries overflowed their page reclaims those pages
/// too rather than leaking them. Used by DROP TABLE/DROP INDEX (#215) to
/// reclaim the pages a dropped object's b-tree occupied.
pub(crate) fn free_btree_pages(
    pager: &mut crate::pager::Pager,
    header: &DatabaseHeader,
    root_page: u32,
) -> Result<(), BtreeError> {
    let usable_size = header.usable_page_size();
    let encoding = header.text_encoding;
    let mut visited = 0usize;
    free_btree_pages_inner(pager, root_page, usable_size, encoding, &mut visited)
}

fn free_btree_pages_inner(
    pager: &mut crate::pager::Pager,
    page_num: u32,
    usable_size: u32,
    encoding: crate::record::TextEncoding,
    visited: &mut usize,
) -> Result<(), BtreeError> {
    *visited = visited.saturating_add(1);
    if *visited > MAX_PAGES_VISITED {
        return Err(BtreeError::TraversalTooLong {
            max: MAX_PAGES_VISITED,
        });
    }
    let buf = pager
        .read_page(page_num)
        .map_err(|source| BtreeError::PageSource { page_num, source })?;
    let header_start = page1_header_start(page_num);
    let page_type = read_page_type(&buf, header_start, page_num)?;
    match page_type {
        LEAF_TABLE => {
            let num_cells = read_num_cells(&buf, header_start, page_num)?;
            let ptr_base = header_start.saturating_add(8);
            for i in 0..num_cells {
                let ptr_off = cell_ptr_offset(ptr_base, i);
                let cell_start = read_cell_pointer(&buf, ptr_off, page_num, i)?;
                let (_, payload_len, tail_start) = decode_cell_head(&buf, cell_start, page_num)?;
                if let Some(overflow_page) = first_overflow_page(
                    &buf,
                    tail_start,
                    payload_len,
                    usable_size,
                    page_num,
                    false,
                )? {
                    free_overflow_chain(pager, page_num, overflow_page, visited)?;
                }
            }
        }
        INTERIOR_TABLE => {
            let (entries, rightmost) = collect_interior_entries(&buf, header_start, page_num)?;
            for (child, _) in &entries {
                free_btree_pages_inner(pager, *child, usable_size, encoding, visited)?;
            }
            free_btree_pages_inner(pager, rightmost, usable_size, encoding, visited)?;
        }
        t if t == index::LEAF_INDEX => {
            let num_cells = read_num_cells(&buf, header_start, page_num)?;
            let ptr_base = header_start.saturating_add(8);
            for i in 0..num_cells {
                let ptr_off = cell_ptr_offset(ptr_base, i);
                let cell_start = read_cell_pointer(&buf, ptr_off, page_num, i)?;
                let (payload_len, tail_start) =
                    index::decode_payload_len(&buf, cell_start, page_num)?;
                if let Some(overflow_page) =
                    first_overflow_page(&buf, tail_start, payload_len, usable_size, page_num, true)?
                {
                    free_overflow_chain(pager, page_num, overflow_page, visited)?;
                }
            }
        }
        t if t == index::INTERIOR_INDEX => {
            let num_cells = read_num_cells(&buf, header_start, page_num)?;
            let ptr_base = header_start.saturating_add(12);
            for i in 0..num_cells {
                let ptr_off = cell_ptr_offset(ptr_base, i);
                let cell_start = read_cell_pointer(&buf, ptr_off, page_num, i)?;
                let value_start = cell_start.saturating_add(4);
                let (payload_len, tail_start) =
                    index::decode_payload_len(&buf, value_start, page_num)?;
                if let Some(overflow_page) =
                    first_overflow_page(&buf, tail_start, payload_len, usable_size, page_num, true)?
                {
                    free_overflow_chain(pager, page_num, overflow_page, visited)?;
                }
            }
            let (entries, rightmost) = index::collect_index_interior_entries(
                &*pager,
                &buf,
                header_start,
                page_num,
                usable_size,
                encoding,
            )?;
            for (child, _, _) in &entries {
                free_btree_pages_inner(pager, *child, usable_size, encoding, visited)?;
            }
            free_btree_pages_inner(pager, rightmost, usable_size, encoding, visited)?;
        }
        _ => {
            return Err(BtreeError::UnexpectedPageType {
                page_num,
                page_type,
            })
        }
    }
    Ok(pager.deallocate_page(page_num)?)
}

/// Exact row count for a table b-tree (#543), mirroring SQLite's
/// `OP_Count`/`sqlite3BtreeCount`: walks every page reachable from
/// `root_page`, summing leaf-page cell counts, without decoding any row
/// payload. Cheaper than a full row scan (no payload/overflow reads) but
/// still `O(pages)`, not `O(1)` — there is no cached table-level row count
/// in this file format, so this is the fastest *exact* count available.
pub(crate) fn count_table_rows<P: PageSource>(
    source: &P,
    root_page: u32,
) -> Result<i64, BtreeError> {
    let mut visited = 0usize;
    let mut total = 0i64;
    count_table_rows_inner(source, root_page, &mut visited, &mut total)?;
    Ok(total)
}

fn count_table_rows_inner<P: PageSource>(
    source: &P,
    page_num: u32,
    visited: &mut usize,
    total: &mut i64,
) -> Result<(), BtreeError> {
    *visited = visited.saturating_add(1);
    if *visited > MAX_PAGES_VISITED {
        return Err(BtreeError::TraversalTooLong {
            max: MAX_PAGES_VISITED,
        });
    }
    let buf = source
        .read_page(page_num)
        .map_err(|source| BtreeError::PageSource { page_num, source })?;
    let header_start = page1_header_start(page_num);
    let page_type = read_page_type(&buf, header_start, page_num)?;
    match page_type {
        LEAF_TABLE => {
            let num_cells = read_num_cells(&buf, header_start, page_num)?;
            #[allow(
                clippy::cast_possible_wrap,
                reason = "num_cells is a page's cell count, always <= u16::MAX by file-format construction"
            )]
            let num_cells = num_cells as i64;
            *total = total.saturating_add(num_cells);
        }
        INTERIOR_TABLE => {
            let (entries, rightmost) = collect_interior_entries(&buf, header_start, page_num)?;
            for (child, _) in &entries {
                count_table_rows_inner(source, *child, visited, total)?;
            }
            count_table_rows_inner(source, rightmost, visited, total)?;
        }
        _ => {
            return Err(BtreeError::UnexpectedPageType {
                page_num,
                page_type,
            })
        }
    }
    Ok(())
}

/// Reads every cell of a leaf page in on-disk (rowid-ascending) order,
/// returning each cell's rowid alongside its raw, verbatim cell bytes (so
/// splits/merges/rebuilds can move cells without re-encoding payloads or
/// re-walking overflow chains). Shared by the insert and delete write
/// paths.
pub(super) fn collect_leaf_cells(
    buf: &[u8],
    header_start: usize,
    page_num: u32,
    usable_size: u32,
) -> Result<Vec<(i64, Vec<u8>)>, BtreeError> {
    let num_cells = read_num_cells(buf, header_start, page_num)?;
    let ptr_base = header_start.saturating_add(8);
    let mut out = Vec::with_capacity(num_cells);
    for i in 0..num_cells {
        let ptr_off = cell_ptr_offset(ptr_base, i);
        let cell_start = read_cell_pointer(buf, ptr_off, page_num, i)?;
        let (rowid, payload_len, tail_start) = decode_cell_head(buf, cell_start, page_num)?;
        let local_size = local_payload_size(usable_size, payload_len, false) as usize;
        let has_overflow = (local_size as u64) < payload_len;
        let cell_end = tail_start
            .saturating_add(local_size)
            .saturating_add(if has_overflow { 4 } else { 0 });
        let cell_bytes = buf
            .get(cell_start..cell_end)
            .ok_or(BtreeError::PayloadTooShort { page_num })?
            .to_vec();
        out.push((rowid, cell_bytes));
    }
    Ok(out)
}

/// Zero-copy pre-scan of a leaf page for the insert fast path (#588):
/// returns `(insert_pos, num_cells, total_cell_bytes)` for inserting
/// `rowid` — everything the fit check and `splice_insert_cell` need —
/// without materializing any cell the way [`collect_leaf_cells`] does.
/// Errors with [`BtreeError::DuplicateRowid`] if `rowid` is already
/// present.
pub(super) fn scan_leaf_cells(
    buf: &[u8],
    header_start: usize,
    page_num: u32,
    usable_size: u32,
    rowid: i64,
) -> Result<(usize, usize, usize), BtreeError> {
    let num_cells = read_num_cells(buf, header_start, page_num)?;
    let ptr_base = header_start.saturating_add(8);
    let mut insert_pos = num_cells;
    let mut total_bytes = 0usize;
    for i in 0..num_cells {
        let ptr_off = cell_ptr_offset(ptr_base, i);
        let cell_start = read_cell_pointer(buf, ptr_off, page_num, i)?;
        let (cell_rowid, payload_len, tail_start) = decode_cell_head(buf, cell_start, page_num)?;
        if cell_rowid == rowid {
            return Err(BtreeError::DuplicateRowid { rowid });
        }
        if cell_rowid > rowid && insert_pos == num_cells {
            insert_pos = i;
        }
        let local_size = local_payload_size(usable_size, payload_len, false) as usize;
        let has_overflow = (local_size as u64) < payload_len;
        let cell_end = tail_start
            .saturating_add(local_size)
            .saturating_add(if has_overflow { 4 } else { 0 });
        if cell_end > buf.len() {
            return Err(BtreeError::PayloadTooShort { page_num });
        }
        total_bytes = total_bytes.saturating_add(cell_end.saturating_sub(cell_start));
    }
    Ok((insert_pos, num_cells, total_bytes))
}

/// Zero-copy pre-scan of a leaf page for the delete fast path (#588):
/// locates `rowid`'s cell, returning `(pos, num_cells, overflow_page)` —
/// `overflow_page` is the cell's first overflow page, or 0 if its payload
/// is entirely local — without materializing any cell. Returns `Ok(None)`
/// if `rowid` isn't on this page.
pub(super) fn find_leaf_cell(
    buf: &[u8],
    header_start: usize,
    page_num: u32,
    usable_size: u32,
    rowid: i64,
) -> Result<Option<(usize, usize, u32)>, BtreeError> {
    let num_cells = read_num_cells(buf, header_start, page_num)?;
    let ptr_base = header_start.saturating_add(8);
    for i in 0..num_cells {
        let ptr_off = cell_ptr_offset(ptr_base, i);
        let cell_start = read_cell_pointer(buf, ptr_off, page_num, i)?;
        let (cell_rowid, payload_len, tail_start) = decode_cell_head(buf, cell_start, page_num)?;
        if cell_rowid != rowid {
            continue;
        }
        let local_size = local_payload_size(usable_size, payload_len, false) as usize;
        let overflow_page = if (local_size as u64) < payload_len {
            read_u32(buf, tail_start.saturating_add(local_size), page_num)?
        } else {
            0
        };
        return Ok(Some((i, num_cells, overflow_page)));
    }
    Ok(None)
}

/// Reads every cell of an interior page, returning `(child_page, key)`
/// pairs in on-disk order plus the rightmost pointer. Shared by the insert
/// and delete write paths.
pub(super) fn collect_interior_entries(
    buf: &[u8],
    header_start: usize,
    page_num: u32,
) -> Result<(Vec<(u32, i64)>, u32), BtreeError> {
    let num_cells = read_num_cells(buf, header_start, page_num)?;
    let ptr_base = header_start.saturating_add(12);
    let mut out = Vec::with_capacity(num_cells);
    for i in 0..num_cells {
        let ptr_off = cell_ptr_offset(ptr_base, i);
        let cell_start = read_cell_pointer(buf, ptr_off, page_num, i)?;
        let child = read_u32(buf, cell_start, page_num)?;
        let rest = buf
            .get(cell_start.saturating_add(4)..)
            .ok_or(BtreeError::PayloadTooShort { page_num })?;
        let (key, _) = decode_varint(rest)
            .map_err(|source| BtreeError::InvalidCellVarint { page_num, source })?;
        out.push((child, key as i64));
    }
    let rightmost = read_u32(buf, header_start.saturating_add(8), page_num)?;
    Ok((out, rightmost))
}

/// Builds an interior table-b-tree cell: 4-byte left-child page number +
/// key (rowid) varint. Shared by the insert and delete write paths.
pub(super) fn build_interior_cell(child: u32, key: i64) -> Vec<u8> {
    let mut cell = child.to_be_bytes().to_vec();
    cell.extend(encode_varint(key as u64));
    cell
}

/// Writes `bytes` at `offset` in `buf`, or panics via an internal
/// invariant violation turned into a page-too-short error — every call
/// site here writes into a page-sized buffer at an offset the caller just
/// computed from that same buffer's length, so failure here means a bug in
/// the offset math, not bad input. Shared by the insert and delete write
/// paths.
pub(super) fn put(
    buf: &mut [u8],
    offset: usize,
    bytes: &[u8],
    page_num: u32,
) -> Result<(), BtreeError> {
    let end = offset.saturating_add(bytes.len());
    let len = buf.len();
    let slice = buf
        .get_mut(offset..end)
        .ok_or(BtreeError::PageTooShort { page_num, len })?;
    slice.copy_from_slice(bytes);
    Ok(())
}

pub(super) fn put_u8(
    buf: &mut [u8],
    offset: usize,
    value: u8,
    page_num: u32,
) -> Result<(), BtreeError> {
    put(buf, offset, &[value], page_num)
}

/// Rebuilds `buf` in place as a leaf table b-tree page holding exactly
/// `cells`, in order. Every mutation to a leaf page goes through this (or
/// [`write_interior_page`]) rather than patching bytes incrementally — see
/// `insert.rs`'s module doc's "fully rebuilds" simplification note. Shared
/// by the insert and delete write paths.
/// Drops each cell's sort key, keeping only its encoded bytes — the
/// shape [`write_leaf_page`]/[`write_interior_page`] want. Shared by
/// every leaf/interior rebuild and split site instead of each repeating
/// its own `.into_iter().map(|(_, c)| c).collect()`.
pub(super) fn cell_bytes<K>(cells: Vec<(K, Vec<u8>)>) -> Vec<Vec<u8>> {
    cells.into_iter().map(|(_, c)| c).collect()
}

pub(super) fn write_leaf_page(
    buf: &mut [u8],
    header_start: usize,
    page_num: u32,
    cells: &[Vec<u8>],
) -> Result<(), BtreeError> {
    write_page_common(buf, header_start, page_num, LEAF_TABLE, 8, cells)
}

/// As [`write_leaf_page`], but for an interior page — writes the
/// rightmost-child pointer (header bytes 8-11) after the common layout.
/// Shared by the insert and delete write paths.
pub(super) fn write_interior_page(
    buf: &mut [u8],
    header_start: usize,
    page_num: u32,
    cells: &[Vec<u8>],
    rightmost: u32,
) -> Result<(), BtreeError> {
    write_page_common(buf, header_start, page_num, INTERIOR_TABLE, 12, cells)?;
    put(
        buf,
        header_start.saturating_add(8),
        &rightmost.to_be_bytes(),
        page_num,
    )
}

pub(super) fn write_page_common(
    buf: &mut [u8],
    header_start: usize,
    page_num: u32,
    page_type: u8,
    header_len: usize,
    cells: &[Vec<u8>],
) -> Result<(), BtreeError> {
    // Only the b-tree page portion is cleared — for page 1, bytes
    // 0..header_start hold the 100-byte file header, which must survive
    // every leaf/interior rewrite of that page's b-tree content.
    let len = buf.len();
    buf.get_mut(header_start..)
        .ok_or(BtreeError::PageTooShort { page_num, len })?
        .fill(0);
    put_u8(buf, header_start, page_type, page_num)?;
    // bytes header_start+1..+3 (first freeblock) stay 0 — see module doc.
    let ptr_base = header_start.saturating_add(header_len);
    let num_cells = cells.len();

    let mut content_end = buf.len();
    let mut ptr_offsets = Vec::with_capacity(num_cells);
    for cell in cells.iter().rev() {
        content_end = content_end.saturating_sub(cell.len());
        put(buf, content_end, cell, page_num)?;
        ptr_offsets.push(content_end);
    }
    ptr_offsets.reverse();

    for (i, off) in ptr_offsets.iter().enumerate() {
        let p = ptr_base.saturating_add(i.saturating_mul(2));
        #[allow(
            clippy::cast_possible_truncation,
            reason = "off is always < page_len (<=65536), fits a u16 once the 65536-as-0 case below is applied"
        )]
        let v = if *off >= 65536 { 0u16 } else { *off as u16 };
        put(buf, p, &v.to_be_bytes(), page_num)?;
    }

    #[allow(
        clippy::cast_possible_truncation,
        reason = "num_cells is a page's cell count, always <= u16::MAX by file-format construction"
    )]
    let num_cells_u16 = num_cells as u16;
    put(
        buf,
        header_start.saturating_add(3),
        &num_cells_u16.to_be_bytes(),
        page_num,
    )?;
    #[allow(
        clippy::cast_possible_truncation,
        reason = "content_end is always < page_len (<=65536), fits a u16 once the 65536-as-0 case below is applied"
    )]
    let content_start_v: u16 = if content_end >= 65536 {
        0
    } else {
        content_end as u16
    };
    put(
        buf,
        header_start.saturating_add(5),
        &content_start_v.to_be_bytes(),
        page_num,
    )?;
    put_u8(buf, header_start.saturating_add(7), 0, page_num)
}

/// A freed byte range shorter than this can't hold a freeblock header (2
/// bytes next-offset + 2 bytes size) and is instead accounted for in the
/// page header's fragmented-free-bytes counter.
const MIN_FREEBLOCK_SIZE: usize = 4;

/// Sanity cap on freeblocks walked in one chain, guarding against a
/// corrupt/cyclic chain causing an unbounded loop.
const MAX_FREEBLOCKS: usize = 10_000;

fn read_u16(page: &[u8], offset: usize, page_num: u32) -> Result<u16, BtreeError> {
    let end = offset.saturating_add(2);
    let bytes: [u8; 2] = page
        .get(offset..end)
        .ok_or(BtreeError::PageTooShort {
            page_num,
            len: page.len(),
        })?
        .try_into()
        .map_err(|_| BtreeError::PageTooShort {
            page_num,
            len: page.len(),
        })?;
    Ok(u16::from_be_bytes(bytes))
}

fn write_u16(buf: &mut [u8], offset: usize, value: usize, page_num: u32) -> Result<(), BtreeError> {
    #[allow(
        clippy::cast_possible_truncation,
        reason = "callers only ever pass a value already known to fit (page offset/size < 65536, with the 65536-as-0 wraparound applied by the caller where relevant)"
    )]
    let v = value as u16;
    put(buf, offset, &v.to_be_bytes(), page_num)
}

/// Reads the b-tree page header's `content_start` field (bytes 5-6),
/// applying the file-format convention that a stored `0` means 65536 (the
/// only value that field can't represent directly in 16 bits).
fn read_content_start(
    page: &[u8],
    header_start: usize,
    page_num: u32,
) -> Result<usize, BtreeError> {
    let v = read_u16(page, header_start.saturating_add(5), page_num)?;
    Ok(if v == 0 { 65536 } else { v as usize })
}

fn write_content_start(
    buf: &mut [u8],
    header_start: usize,
    value: usize,
    page_num: u32,
) -> Result<(), BtreeError> {
    let stored = if value >= 65536 { 0 } else { value };
    write_u16(buf, header_start.saturating_add(5), stored, page_num)
}

fn read_first_freeblock(
    page: &[u8],
    header_start: usize,
    page_num: u32,
) -> Result<usize, BtreeError> {
    Ok(read_u16(page, header_start.saturating_add(1), page_num)? as usize)
}

fn read_fragmented_bytes(
    page: &[u8],
    header_start: usize,
    page_num: u32,
) -> Result<u8, BtreeError> {
    page.get(header_start.saturating_add(7))
        .copied()
        .ok_or(BtreeError::PageTooShort {
            page_num,
            len: page.len(),
        })
}

fn write_fragmented_bytes(
    buf: &mut [u8],
    header_start: usize,
    value: u8,
    page_num: u32,
) -> Result<(), BtreeError> {
    put_u8(buf, header_start.saturating_add(7), value, page_num)
}

/// Walks the page's freeblock chain (`fileformat2.html` "Freeblocks"),
/// returning `(offset, size)` pairs in on-disk chain order (which the
/// format requires to already be ascending-by-offset and non-overlapping).
/// Capped at [`MAX_FREEBLOCKS`] so a corrupt/cyclic chain fails cleanly
/// instead of looping forever.
fn walk_freeblocks(
    page: &[u8],
    header_start: usize,
    page_num: u32,
) -> Result<Vec<(usize, usize)>, BtreeError> {
    let mut out = Vec::new();
    let mut off = read_first_freeblock(page, header_start, page_num)?;
    let mut n = 0usize;
    while off != 0 {
        n = n.saturating_add(1);
        if n > MAX_FREEBLOCKS {
            return Err(BtreeError::TraversalTooLong {
                max: MAX_FREEBLOCKS,
            });
        }
        let next = read_u16(page, off, page_num)? as usize;
        let size = read_u16(page, off.saturating_add(2), page_num)? as usize;
        out.push((off, size));
        off = next;
    }
    Ok(out)
}

/// Writes `blocks` (offset-ascending, non-overlapping) back as the page's
/// freeblock chain: each block's own 4-byte header (next-offset + size) is
/// rewritten to point at the next block in the list (0 for the last), and
/// the page header's first-freeblock field is updated to point at the
/// first (or 0 if `blocks` is empty).
fn write_freeblock_chain(
    buf: &mut [u8],
    header_start: usize,
    page_num: u32,
    blocks: &[(usize, usize)],
) -> Result<(), BtreeError> {
    for (i, &(off, size)) in blocks.iter().enumerate() {
        let next = blocks.get(i.saturating_add(1)).map_or(0, |&(o, _)| o);
        write_u16(buf, off, next, page_num)?;
        write_u16(buf, off.saturating_add(2), size, page_num)?;
    }
    let first = blocks.first().map_or(0, |&(o, _)| o);
    write_u16(buf, header_start.saturating_add(1), first, page_num)
}

/// Inserts a newly freed `[start, start+size)` byte range into `blocks`
/// (kept offset-ascending), coalescing with an immediately adjacent
/// preceding and/or following freeblock so the chain never carries two
/// blocks that touch end-to-end (`fileformat2.html` requires freeblocks to
/// never be adjacent).
fn add_freeblock(blocks: &mut Vec<(usize, usize)>, start: usize, size: usize) {
    let pos = blocks.partition_point(|&(o, _)| o < start);
    let mut new_start = start;
    let mut new_size = size;
    let mut insert_pos = pos;

    if insert_pos > 0 {
        if let Some(&(prev_off, prev_size)) = blocks.get(insert_pos.saturating_sub(1)) {
            if prev_off.saturating_add(prev_size) == new_start {
                new_start = prev_off;
                new_size = new_size.saturating_add(prev_size);
                insert_pos = insert_pos.saturating_sub(1);
                blocks.remove(insert_pos);
            }
        }
    }
    if let Some(&(next_off, next_size)) = blocks.get(insert_pos) {
        if new_start.saturating_add(new_size) == next_off {
            new_size = new_size.saturating_add(next_size);
            blocks.remove(insert_pos);
        }
    }
    blocks.insert(insert_pos, (new_start, new_size));
}

/// Inserts `cell` at cell-pointer-array position `index` (0-based, in the
/// page's cell order) into a leaf page (table or index — both share the
/// 8-byte leaf header layout) **only** if there is enough contiguous free
/// space between the end of the cell-pointer array and `content_start` —
/// this never reuses freeblocks or reserves fragmented bytes, so it is
/// O(1) in the number of other cells on the page (just a memmove of the
/// 2-byte pointer-array entries at/after `index`, not the cell bytes
/// themselves). Returns `Ok(false)` (no mutation performed) when the gap
/// is too small; callers fall back to their existing full collect/rebuild
/// path in that case, which — being a from-scratch layout — also reclaims
/// any space sitting in freeblocks/fragmentation.
pub(super) fn splice_insert_cell(
    buf: &mut [u8],
    header_start: usize,
    page_num: u32,
    index: usize,
    cell: &[u8],
) -> Result<bool, BtreeError> {
    let num_cells = read_num_cells(buf, header_start, page_num)?;
    let ptr_base = header_start.saturating_add(8);
    let content_start = read_content_start(buf, header_start, page_num)?;
    let ptr_end = cell_ptr_offset(ptr_base, num_cells);
    let needed = cell.len().saturating_add(2);
    if content_start < ptr_end || content_start.saturating_sub(ptr_end) < needed {
        return Ok(false);
    }

    let new_content_start = content_start.saturating_sub(cell.len());
    put(buf, new_content_start, cell, page_num)?;

    for i in (index..num_cells).rev() {
        let src = cell_ptr_offset(ptr_base, i);
        let dst = cell_ptr_offset(ptr_base, i.saturating_add(1));
        let v = read_cell_pointer(buf, src, page_num, i)?;
        write_u16(buf, dst, v, page_num)?;
    }
    let new_ptr_off = cell_ptr_offset(ptr_base, index);
    write_u16(buf, new_ptr_off, new_content_start, page_num)?;
    write_content_start(buf, header_start, new_content_start, page_num)?;

    let new_num_cells = num_cells.saturating_add(1);
    write_u16(buf, header_start.saturating_add(3), new_num_cells, page_num)?;
    Ok(true)
}

/// Removes the cell at cell-pointer-array position `index` from a leaf
/// page (table or index — `has_rowid` selects which cell shape to decode:
/// a table leaf cell's head carries a payload-length varint *and* a rowid
/// varint before the payload, an index leaf cell's only the
/// payload-length varint), in place: shifts the cell-pointer array left by
/// one entry (a memmove of just the 2-byte pointer entries after `index`,
/// not the cell bytes themselves — O(1) relative to the page's other
/// cells) and returns the freed byte range to the page's free-space
/// bookkeeping per `fileformat2.html`'s "Freeblocks" format: grown into
/// `content_start` if the freed range borders it, added to the freeblock
/// chain (coalescing with neighbors) if it's at least
/// [`MIN_FREEBLOCK_SIZE`] bytes, or added to the fragmented-free-bytes
/// counter otherwise.
pub(super) fn splice_delete_cell(
    buf: &mut [u8],
    header_start: usize,
    page_num: u32,
    usable_size: u32,
    index: usize,
    has_rowid: bool,
) -> Result<(), BtreeError> {
    let num_cells = read_num_cells(buf, header_start, page_num)?;
    if index >= num_cells {
        return Err(BtreeError::Internal(
            "splice_delete_cell: index out of bounds",
        ));
    }
    let ptr_base = header_start.saturating_add(8);
    let ptr_off = cell_ptr_offset(ptr_base, index);
    let cell_start = read_cell_pointer(buf, ptr_off, page_num, index)?;
    let (payload_len, tail_start) = if has_rowid {
        let (_, payload_len, tail_start) = decode_cell_head(buf, cell_start, page_num)?;
        (payload_len, tail_start)
    } else {
        index::decode_payload_len(buf, cell_start, page_num)?
    };
    let local_size = local_payload_size(usable_size, payload_len, !has_rowid) as usize;
    let has_overflow = (local_size as u64) < payload_len;
    let cell_end = tail_start
        .saturating_add(local_size)
        .saturating_add(if has_overflow { 4 } else { 0 });
    let cell_len = cell_end.saturating_sub(cell_start);

    for i in index..num_cells.saturating_sub(1) {
        let src = cell_ptr_offset(ptr_base, i.saturating_add(1));
        let dst = cell_ptr_offset(ptr_base, i);
        let v = read_cell_pointer(buf, src, page_num, i.saturating_add(1))?;
        write_u16(buf, dst, v, page_num)?;
    }
    let new_num_cells = num_cells.saturating_sub(1);
    write_u16(buf, header_start.saturating_add(3), new_num_cells, page_num)?;

    if let Some(slice) = buf.get_mut(cell_start..cell_end) {
        slice.fill(0);
    }

    let content_start = read_content_start(buf, header_start, page_num)?;
    if cell_start == content_start {
        let mut blocks = walk_freeblocks(buf, header_start, page_num)?;
        let mut new_content_start = cell_end;
        while let Some(pos) = blocks.iter().position(|&(o, _)| o == new_content_start) {
            let (_, size) = blocks.remove(pos);
            new_content_start = new_content_start.saturating_add(size);
        }
        write_content_start(buf, header_start, new_content_start, page_num)?;
        write_freeblock_chain(buf, header_start, page_num, &blocks)?;
    } else if cell_len < MIN_FREEBLOCK_SIZE {
        let frag = read_fragmented_bytes(buf, header_start, page_num)?;
        #[allow(
            clippy::cast_possible_truncation,
            reason = "cell_len < MIN_FREEBLOCK_SIZE (4) here, always fits a u8"
        )]
        let add = cell_len as u8;
        write_fragmented_bytes(buf, header_start, frag.saturating_add(add), page_num)?;
    } else {
        let mut blocks = walk_freeblocks(buf, header_start, page_num)?;
        add_freeblock(&mut blocks, cell_start, cell_len);
        write_freeblock_chain(buf, header_start, page_num, &blocks)?;
    }
    Ok(())
}

fn page1_header_start(page_num: u32) -> usize {
    if page_num == 1 {
        100
    } else {
        0
    }
}

fn read_page_type(page: &[u8], header_start: usize, page_num: u32) -> Result<u8, BtreeError> {
    page.get(header_start)
        .copied()
        .ok_or(BtreeError::PageTooShort {
            page_num,
            len: page.len(),
        })
}

fn read_num_cells(page: &[u8], header_start: usize, page_num: u32) -> Result<usize, BtreeError> {
    let start = header_start.saturating_add(3);
    let end = header_start.saturating_add(5);
    let bytes: [u8; 2] = page
        .get(start..end)
        .ok_or(BtreeError::PageTooShort {
            page_num,
            len: page.len(),
        })?
        .try_into()
        .map_err(|_| BtreeError::PageTooShort {
            page_num,
            len: page.len(),
        })?;
    Ok(u16::from_be_bytes(bytes) as usize)
}

fn require_interior_header(
    page: &[u8],
    header_start: usize,
    page_num: u32,
) -> Result<(), BtreeError> {
    if page.len() < header_start.saturating_add(12) {
        return Err(BtreeError::PageTooShort {
            page_num,
            len: page.len(),
        });
    }
    Ok(())
}

fn read_u32(page: &[u8], offset: usize, page_num: u32) -> Result<u32, BtreeError> {
    let end = offset.saturating_add(4);
    let bytes: [u8; 4] = page
        .get(offset..end)
        .ok_or(BtreeError::PageTooShort {
            page_num,
            len: page.len(),
        })?
        .try_into()
        .map_err(|_| BtreeError::PageTooShort {
            page_num,
            len: page.len(),
        })?;
    Ok(u32::from_be_bytes(bytes))
}

#[inline]
fn read_cell_pointer(
    page: &[u8],
    ptr_off: usize,
    page_num: u32,
    cell_index: usize,
) -> Result<usize, BtreeError> {
    let end = ptr_off.saturating_add(2);
    let bytes: [u8; 2] = page
        .get(ptr_off..end)
        .ok_or(BtreeError::InvalidCellPointer {
            page_num,
            index: cell_index,
        })?
        .try_into()
        .map_err(|_| BtreeError::InvalidCellPointer {
            page_num,
            index: cell_index,
        })?;
    Ok(u16::from_be_bytes(bytes) as usize)
}

/// Cell-pointer-array byte offset for entry `i` from `base`. `i` is bounded
/// by `num_cells` (a `u16` field, max 65535); `saturating_mul`/`saturating_add`
/// keep this lint-clean without pretending the arithmetic could realistically
/// overflow.
fn cell_ptr_offset(base: usize, i: usize) -> usize {
    base.saturating_add(i.saturating_mul(2))
}

/// Decodes a leaf table-b-tree cell's head (payload-length varint + rowid
/// varint) and returns `(rowid, payload_len, tail_start)`, where
/// `tail_start` is the page offset where the payload bytes begin.
#[inline]
fn decode_cell_head(
    page: &[u8],
    cell_start: usize,
    page_num: u32,
) -> Result<(i64, u64, usize), BtreeError> {
    let cell = page
        .get(cell_start..)
        .ok_or(BtreeError::InvalidCellPointer {
            page_num,
            index: cell_start,
        })?;
    let (payload_len, n1) =
        decode_varint(cell).map_err(|source| BtreeError::InvalidCellVarint { page_num, source })?;
    let rest = cell
        .get(n1..)
        .ok_or(BtreeError::PayloadTooShort { page_num })?;
    let (rowid, n2) =
        decode_varint(rest).map_err(|source| BtreeError::InvalidCellVarint { page_num, source })?;
    Ok((
        rowid as i64,
        payload_len,
        cell_start.saturating_add(n1).saturating_add(n2),
    ))
}

/// A one-page, empty-leaf-root database: just enough header bytes for
/// `DatabaseHeader::parse` and `Pager::open` to accept it. Shared by the
/// `#[cfg(test)]` modules of `delete.rs`, `schema.rs`, `insert.rs` and
/// `master.rs`, which all built this same fixture independently before.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
pub(crate) fn test_minimal_db(
    page_size: u32,
) -> (crate::vfs::MemoryVfs, crate::header::DatabaseHeader) {
    let mut page1 = vec![0u8; page_size as usize];
    page1[0..16].copy_from_slice(b"SQLite format 3\0");
    page1[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
    page1[18] = 1;
    page1[19] = 1;
    page1[28..32].copy_from_slice(&1u32.to_be_bytes());
    page1[56..60].copy_from_slice(&1u32.to_be_bytes());
    write_leaf_page(&mut page1, 100, 1, &[]).unwrap();

    let mut header_bytes = [0u8; 100];
    header_bytes.copy_from_slice(&page1[..100]);
    let header = crate::header::DatabaseHeader::parse(&header_bytes).unwrap();

    let mut vfs = crate::vfs::MemoryVfs::new();
    vfs.insert("/test.db", page1);
    (vfs, header)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;
    use crate::header::DatabaseHeader;
    use crate::record::{decode_record, TextEncoding, Value};
    use crate::vfs::{PageError, UnixVfs, Vfs, VfsPageSource};
    use std::collections::HashMap;
    use std::path::Path;

    fn open_cursor(fixture: &str) -> TableCursor<VfsPageSource> {
        let path = Path::new("tests/corpus/fixtures/btrees").join(fixture);
        let vfs = UnixVfs;
        let file = vfs.open_read(&path).unwrap();
        let mut header_buf = [0u8; 100];
        file.read_at(&mut header_buf, 0).unwrap();
        let header = DatabaseHeader::parse(&header_buf).unwrap();
        let source = VfsPageSource::open(&vfs, &path, header.page_size).unwrap();
        TableCursor::new(source, &header, 2)
    }

    fn text(v: &Value) -> &str {
        match v {
            Value::Text(s) => s,
            other => panic!("expected text, got {other:?}"),
        }
    }

    fn int(v: &Value) -> i64 {
        match v {
            Value::Integer(i) => *i,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    fn blob(v: &Value) -> &[u8] {
        match v {
            Value::Blob(b) => b,
            other => panic!("expected blob, got {other:?}"),
        }
    }

    #[test]
    fn table_single_page_full_scan() {
        let mut cursor = open_cursor("table_single_page.db");
        let row = cursor.first_row().unwrap().unwrap();
        assert_eq!(row.rowid, 1);
        let values = decode_record(&row.payload, TextEncoding::Utf8).unwrap();
        assert_eq!(int(&values[0]), 1);
        assert_eq!(text(&values[1]), "a single leaf page");
        assert!(cursor.next_row().unwrap().is_none());
    }

    /// 006-btree Requirement 4: a plain `INTEGER PRIMARY KEY` column isn't
    /// stored in the record at all — SQLite encodes it as `NULL` and
    /// expects a higher layer to substitute the cell's own rowid. This
    /// layer must decode it faithfully as `Value::Null`, never attempt
    /// schema-aware substitution itself (it has no schema information).
    #[test]
    fn rowid_alias_column_decodes_as_null_not_substituted() {
        let mut cursor = open_cursor("select_parity.db");
        let row = cursor.first_row().unwrap().unwrap();
        assert_eq!(row.rowid, 1);
        let values = decode_record(&row.payload, TextEncoding::Utf8).unwrap();
        assert_eq!(
            values[0],
            Value::Null,
            "the rowid-alias column must decode as NULL at this layer, not the rowid"
        );
    }

    #[test]
    fn table_multipage_full_scan_matches_oracle() {
        let mut cursor = open_cursor("table_multipage.db");
        let mut rows = Vec::new();
        let mut row = cursor.first_row().unwrap();
        while let Some(r) = row {
            rows.push(r);
            row = cursor.next_row().unwrap();
        }
        assert_eq!(rows.len(), 3000);

        // Ascending rowid order, 1..=3000, no gaps or duplicates.
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(r.rowid, (i + 1) as i64);
        }

        let first = decode_record(&rows[0].payload, TextEncoding::Utf8).unwrap();
        assert_eq!(int(&first[0]), 1);
        assert_eq!(text(&first[1]), "row number 1");

        let last = decode_record(&rows[2999].payload, TextEncoding::Utf8).unwrap();
        assert_eq!(int(&last[0]), 3000);
        assert_eq!(text(&last[1]), "row number 3000");
    }

    #[test]
    fn prev_without_last_errors_rather_than_looking_exhausted() {
        // Regression guard for the precondition `prev()` documents: before
        // this was an error it was a `debug_assert`, so a release build
        // returned `None` — indistinguishable from a genuinely exhausted
        // cursor, which is the confusing outcome the check exists to prevent.
        let mut cursor = open_cursor("table_multipage.db");
        assert!(matches!(
            cursor.prev_row(),
            Err(BtreeError::CursorNotPositioned { .. })
        ));
    }

    #[test]
    fn table_single_page_last_returns_the_only_row() {
        let mut cursor = open_cursor("table_single_page.db");
        let row = cursor.last_row().unwrap().unwrap();
        assert_eq!(row.rowid, 1);
        assert!(cursor.prev_row().unwrap().is_none());
    }

    #[test]
    fn table_multipage_last_and_prev_walk_descending_rowid_order() {
        let mut cursor = open_cursor("table_multipage.db");
        let mut rows = Vec::new();
        let mut row = cursor.last_row().unwrap();
        while let Some(r) = row {
            rows.push(r);
            row = cursor.prev_row().unwrap();
        }
        assert_eq!(rows.len(), 3000);

        // Descending rowid order, 3000..=1, no gaps or duplicates.
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(r.rowid, (3000 - i) as i64);
        }

        let first = decode_record(&rows[0].payload, TextEncoding::Utf8).unwrap();
        assert_eq!(int(&first[0]), 3000);
        assert_eq!(text(&first[1]), "row number 3000");

        let last = decode_record(&rows[2999].payload, TextEncoding::Utf8).unwrap();
        assert_eq!(int(&last[0]), 1);
        assert_eq!(text(&last[1]), "row number 1");
    }

    #[test]
    fn table_multipage_last_matches_full_scans_final_row() {
        // Cross-check: last()'s single row must equal the tail of a
        // full forward scan — independent verification that reverse
        // traversal lands on the same rightmost leaf cell forward
        // traversal reaches last.
        let mut forward = open_cursor("table_multipage.db");
        let mut row = forward.first_row().unwrap();
        let mut final_forward_row = None;
        while let Some(r) = row {
            final_forward_row = Some(r.clone());
            row = forward.next_row().unwrap();
        }

        let mut backward = open_cursor("table_multipage.db");
        let last_row = backward.last_row().unwrap().unwrap();

        assert_eq!(Some(last_row), final_forward_row);
    }

    #[test]
    fn page_one_trap_sqlite_master_root_is_page_one() {
        // Page 1 carries the 100-byte file header before its own b-tree
        // page header; this reads sqlite_master (always root page 1)
        // directly, exercising the page-1 cell-pointer-array offset
        // resolution (relative to byte 0, not byte 100).
        let path = Path::new("tests/corpus/fixtures/btrees/table_single_page.db");
        let vfs = UnixVfs;
        let file = vfs.open_read(path).unwrap();
        let mut header_buf = [0u8; 100];
        file.read_at(&mut header_buf, 0).unwrap();
        let header = DatabaseHeader::parse(&header_buf).unwrap();
        let source = VfsPageSource::open(&vfs, path, header.page_size).unwrap();
        let mut cursor = TableCursor::new(source, &header, 1);

        let row = cursor.first_row().unwrap().unwrap();
        let values = decode_record(&row.payload, TextEncoding::Utf8).unwrap();
        assert_eq!(text(&values[0]), "table");
        assert_eq!(text(&values[1]), "t");
        assert_eq!(text(&values[2]), "t");
        assert_eq!(int(&values[3]), 2);
        assert_eq!(text(&values[4]), "CREATE TABLE t(a INTEGER, b TEXT)");
        assert!(cursor.next_row().unwrap().is_none());
    }

    #[test]
    fn table_multipage_seek_matches_oracle() {
        let mut cursor = open_cursor("table_multipage.db");

        let row = cursor.seek_row(1500).unwrap().unwrap();
        assert_eq!(row.rowid, 1500);
        let values = decode_record(&row.payload, TextEncoding::Utf8).unwrap();
        assert_eq!(text(&values[1]), "row number 1500");

        let first = cursor.seek_row(1).unwrap().unwrap();
        assert_eq!(first.rowid, 1);
        let last = cursor.seek_row(3000).unwrap().unwrap();
        assert_eq!(last.rowid, 3000);

        assert!(cursor.seek_row(0).unwrap().is_none());
        assert!(cursor.seek_row(3001).unwrap().is_none());
    }

    #[test]
    fn table_multipage_seek_binary_search_matches_every_rowid_and_gaps() {
        // #508: TableCursor::seek switched from a linear cell scan to binary
        // search on both leaf and interior pages. Exhaustively check every
        // rowid in range (exercises every leaf split point) plus the
        // boundary immediately below/above each hundred (exercises interior
        // separator-key edges) to guard against an off-by-one in either
        // binary search's `lo`/`hi`/`mid` bookkeeping.
        let mut cursor = open_cursor("table_multipage.db");
        for rowid in 1..=3000i64 {
            let row = cursor.seek_row(rowid).unwrap().unwrap_or_else(|| {
                panic!("expected rowid {rowid} to be found");
            });
            assert_eq!(row.rowid, rowid);
        }
        for boundary in [0i64, 3001] {
            assert!(cursor.seek_row(boundary).unwrap().is_none());
        }
    }

    #[test]
    fn table_single_page_seek_binary_search_on_small_leaf() {
        // #508: a single-leaf-page tree exercises binary search's smallest
        // cases (0, 1, and a handful of cells) without any interior page
        // involved at all.
        let mut cursor = open_cursor("table_single_page.db");
        let all = {
            let mut c = open_cursor("table_single_page.db");
            let mut rowids = Vec::new();
            let mut row = c.first_row().unwrap();
            while let Some(r) = row {
                rowids.push(r.rowid);
                row = c.next_row().unwrap();
            }
            rowids
        };
        assert!(!all.is_empty());
        for &rowid in &all {
            assert_eq!(cursor.seek_row(rowid).unwrap().unwrap().rowid, rowid);
        }
        assert!(cursor.seek_row(all[0] - 1).unwrap().is_none());
        assert!(cursor.seek_row(all[all.len() - 1] + 1).unwrap().is_none());
    }

    #[test]
    fn seek_does_not_accumulate_pages_visited_across_calls() {
        // `pages_visited` backs the `first`/`next` traversal budget; `seek`
        // must track its own local budget instead of consuming this one, or
        // a long-lived cursor doing many point lookups would eventually
        // start failing valid seeks once the cumulative total crossed
        // MAX_PAGES_VISITED.
        let mut cursor = open_cursor("table_multipage.db");
        for _ in 0..50 {
            cursor.seek_row(1500).unwrap();
        }
        assert_eq!(cursor.pages_visited, 0);
    }

    #[test]
    fn overflow_single_page_payload_is_byte_identical_to_oracle() {
        let mut cursor = open_cursor("overflow_single_page.db");
        let row = cursor.first_row().unwrap().unwrap();
        // #469: an overflow chain must reassemble into an owned copy —
        // there is no single page range to borrow from — unlike the
        // local-only case covered by
        // local_payload_borrows_from_the_page_instead_of_copying.
        assert!(
            matches!(row.payload, Payload::Owned(_)),
            "expected an Owned payload for an overflow-chain row, got a Local borrow"
        );
        let values = decode_record(&row.payload, TextEncoding::Utf8).unwrap();
        assert_eq!(int(&values[0]), 1);
        let b = blob(&values[1]);
        assert_eq!(b.len(), 6000);
        assert_eq!(
            sha256_of(b),
            "a6bedce1e512d6531cd02fe7a0b72bb64f229cdb254ec48d63308877004e620a"
        );
        assert!(cursor.next_row().unwrap().is_none());
    }

    #[test]
    fn free_btree_pages_reclaims_overflow_chain_pages() {
        // Regression test for the overflow-leak gap documented at
        // `free_btree_pages_inner`'s previous doc comment: DROP on a
        // b-tree whose rows overflowed their page must return every
        // overflow page to the freelist too, not just the leaf/interior
        // pages that make up the tree's own structure.
        let page_size = 512u32;
        let (vfs, header) = test_minimal_db(page_size);
        let mut pager = crate::pager::Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let root = schema::create_empty_table_root(&mut pager).unwrap();
        // Comfortably larger than one page, forcing a multi-page overflow
        // chain off the single row's cell.
        let payload = vec![0xABu8; 4000];
        table::insert_row(&mut pager, &header, root, 1, &payload).unwrap();

        let page_count_before = {
            let page1 = pager.get_page_mut(1).unwrap().clone();
            u32::from_be_bytes(page1[28..32].try_into().unwrap())
        };
        let freelist_before = {
            let page1 = pager.get_page_mut(1).unwrap().clone();
            u32::from_be_bytes(page1[36..40].try_into().unwrap())
        };
        assert_eq!(freelist_before, 0);

        free_btree_pages(&mut pager, &header, root).unwrap();

        let freelist_after = {
            let page1 = pager.get_page_mut(1).unwrap().clone();
            u32::from_be_bytes(page1[36..40].try_into().unwrap())
        };
        // Every page this b-tree occupies (its own root leaf plus every
        // overflow page in the chain) must come back to the freelist —
        // i.e. every page allocated after the fixture's original single
        // page, since nothing else was ever allocated in this pager.
        assert_eq!(
            freelist_after,
            page_count_before.saturating_sub(1),
            "DROP must free the root leaf AND every overflow page, not just the leaf"
        );
        assert!(
            freelist_after > 1,
            "a 4000-byte payload on a 512-byte page must span more than one overflow page"
        );
    }

    #[test]
    fn overflow_multi_page_payload_is_byte_identical_to_oracle() {
        let mut cursor = open_cursor("overflow_multi_page.db");
        let row = cursor.first_row().unwrap().unwrap();
        // #469: same Owned-variant check as the single-page overflow test
        // above, exercised here across a 14-page overflow chain instead
        // of a single overflow page.
        assert!(
            matches!(row.payload, Payload::Owned(_)),
            "expected an Owned payload for a multi-page overflow-chain row, got a Local borrow"
        );
        let values = decode_record(&row.payload, TextEncoding::Utf8).unwrap();
        assert_eq!(int(&values[0]), 1);
        let b = blob(&values[1]);
        assert_eq!(b.len(), 60000);
        assert_eq!(
            sha256_of(b),
            "0946e2eb0fb9ea7ddd935efd1922bc7d1f27101c69ce6d2f5145c7ee28f1b6ba"
        );
        assert!(cursor.next_row().unwrap().is_none());
    }

    /// SHA-256, implemented locally (no new dependency) purely to verify
    /// overflow-chain reassembly is byte-identical to the oracle without
    /// embedding 60000 bytes of expected data in the test source.
    fn sha256_of(data: &[u8]) -> String {
        const K: [u32; 64] = [
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
            0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
            0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
            0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
            0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
            0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
            0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
            0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
            0xc67178f2,
        ];
        let mut h: [u32; 8] = [
            0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
            0x5be0cd19,
        ];

        let mut msg = data.to_vec();
        let bit_len = (data.len() as u64) * 8;
        msg.push(0x80);
        while msg.len() % 64 != 56 {
            msg.push(0);
        }
        msg.extend_from_slice(&bit_len.to_be_bytes());

        for chunk in msg.chunks(64) {
            let mut w = [0u32; 64];
            for i in 0..16 {
                w[i] = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().unwrap());
            }
            for i in 16..64 {
                let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
                let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
                w[i] = w[i - 16]
                    .wrapping_add(s0)
                    .wrapping_add(w[i - 7])
                    .wrapping_add(s1);
            }

            let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
                (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
            for i in 0..64 {
                let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
                let ch = (e & f) ^ ((!e) & g);
                let temp1 = hh
                    .wrapping_add(s1)
                    .wrapping_add(ch)
                    .wrapping_add(K[i])
                    .wrapping_add(w[i]);
                let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
                let maj = (a & b) ^ (a & c) ^ (b & c);
                let temp2 = s0.wrapping_add(maj);
                hh = g;
                g = f;
                f = e;
                e = d.wrapping_add(temp1);
                d = c;
                c = b;
                b = a;
                a = temp1.wrapping_add(temp2);
            }
            h[0] = h[0].wrapping_add(a);
            h[1] = h[1].wrapping_add(b);
            h[2] = h[2].wrapping_add(c);
            h[3] = h[3].wrapping_add(d);
            h[4] = h[4].wrapping_add(e);
            h[5] = h[5].wrapping_add(f);
            h[6] = h[6].wrapping_add(g);
            h[7] = h[7].wrapping_add(hh);
        }

        h.iter().map(|x| format!("{x:08x}")).collect()
    }

    struct FakePageSource {
        pages: HashMap<u32, Vec<u8>>,
    }

    impl PageSource for FakePageSource {
        fn read_page(&self, page_num: u32) -> Result<Rc<[u8]>, PageError> {
            self.pages
                .get(&page_num)
                .map(|page| Rc::from(page.as_slice()))
                .ok_or(PageError::InvalidPageNumber)
        }
    }

    fn fake_header() -> DatabaseHeader {
        DatabaseHeader {
            page_size: 512,
            write_version: 1,
            read_version: 1,
            reserved_space: 0,
            page_count: 1,
            freelist_trunk_page: 0,
            freelist_page_count: 0,
            schema_cookie: 0,
            schema_format: 0,
            largest_root_btree_page: 0,
            text_encoding: TextEncoding::Utf8,
            user_version: 0,
            application_id: 0,
        }
    }

    #[test]
    fn unexpected_page_type_errors_not_panics() {
        let mut page = vec![0u8; 512];
        page[0] = 0xff; // not a valid table b-tree page type
        let mut pages = HashMap::new();
        pages.insert(2u32, page);
        let source = FakePageSource { pages };
        let mut cursor = TableCursor::new(source, &fake_header(), 2);

        let err = cursor.first_row().unwrap_err();
        assert!(matches!(
            err,
            BtreeError::UnexpectedPageType {
                page_num: 2,
                page_type: 0xff
            }
        ));
    }

    #[test]
    fn truncated_page_errors_not_panics() {
        let mut pages = HashMap::new();
        pages.insert(2u32, vec![0x0d, 0, 0]); // page type + 2 bytes, way short of an 8-byte header
        let source = FakePageSource { pages };
        let mut cursor = TableCursor::new(source, &fake_header(), 2);

        let err = cursor.first_row().unwrap_err();
        assert!(matches!(err, BtreeError::PageTooShort { page_num: 2, .. }));
    }

    #[test]
    fn missing_page_errors_not_panics() {
        let source = FakePageSource {
            pages: HashMap::new(),
        };
        let mut cursor = TableCursor::new(source, &fake_header(), 2);

        let err = cursor.first_row().unwrap_err();
        assert!(matches!(err, BtreeError::PageSource { page_num: 2, .. }));
    }

    #[test]
    fn overflow_chain_hitting_page_zero_early_errors_not_panics() {
        // A leaf page with one cell whose declared payload_len is larger
        // than what's actually reachable: local bytes + a next-overflow
        // pointer of 0 (chain end) while `remaining` is still nonzero.
        let mut page = vec![0u8; 512];
        page[0] = 0x0d; // leaf table
        page[3..5].copy_from_slice(&1u16.to_be_bytes()); // num_cells = 1
        let cell_ptr_off = 8usize;
        let cell_start = 16usize;
        page[cell_ptr_off..cell_ptr_off + 2].copy_from_slice(&(cell_start as u16).to_be_bytes());

        // payload_len varint = 500 (way past max_local for a 512-byte
        // usable page), rowid varint = 1, then local bytes + a 4-byte
        // overflow pointer of 0.
        let mut cell = Vec::new();
        cell.extend_from_slice(&encode_varint_for_test(500));
        cell.extend_from_slice(&encode_varint_for_test(1));
        let local_size_guess = 512usize.saturating_sub(35).min(470); // generous local region
        cell.extend(std::iter::repeat_n(0u8, local_size_guess));
        cell.extend_from_slice(&0u32.to_be_bytes()); // overflow pointer = 0 (chain end)
        page[cell_start..cell_start + cell.len()].copy_from_slice(&cell);

        let mut pages = HashMap::new();
        pages.insert(2u32, page);
        let source = FakePageSource { pages };
        let mut cursor = TableCursor::new(source, &fake_header(), 2);

        let err = cursor.first_row().unwrap_err();
        assert!(matches!(
            err,
            BtreeError::OverflowChainTruncated { page_num: 2 }
        ));
    }

    #[test]
    fn overflow_chain_cycle_errors_quickly_not_after_a_million_hops() {
        // A cell declaring a payload big enough to need several overflow
        // hops (usable_size=512: local_size=476, remaining=1524, so 3 hops
        // would be needed if the chain were legitimate), but whose sole
        // overflow page points back to itself. Without cycle detection this
        // would ride MAX_PAGES_VISITED all the way up before erroring
        // (forcing ~64GB of reads/copies out of a 2-page file at large page
        // sizes); with it, the repeat is caught on the second visit.
        let mut page2 = vec![0u8; 512];
        page2[0] = 0x0d; // leaf table
        page2[3..5].copy_from_slice(&1u16.to_be_bytes()); // num_cells = 1
        let cell_ptr_off = 8usize;
        let cell_start = 16usize;
        page2[cell_ptr_off..cell_ptr_off + 2].copy_from_slice(&(cell_start as u16).to_be_bytes());

        let mut cell = Vec::new();
        cell.extend_from_slice(&encode_varint_for_test(2000)); // payload_len
        cell.extend_from_slice(&encode_varint_for_test(1)); // rowid
        cell.extend(std::iter::repeat_n(0u8, 476)); // local_size for usable_size=512
        cell.extend_from_slice(&3u32.to_be_bytes()); // overflow pointer -> page 3
        page2[cell_start..cell_start + cell.len()].copy_from_slice(&cell);

        let mut page3 = vec![0u8; 512];
        page3[0..4].copy_from_slice(&3u32.to_be_bytes()); // self-referencing next pointer

        let mut pages = HashMap::new();
        pages.insert(2u32, page2);
        pages.insert(3u32, page3);
        let source = FakePageSource { pages };
        let mut cursor = TableCursor::new(source, &fake_header(), 2);

        let err = cursor.first_row().unwrap_err();
        assert!(matches!(
            err,
            BtreeError::OverflowChainCycle {
                page_num: 2,
                revisited_page: 3
            }
        ));
    }

    fn encode_varint_for_test(mut value: u64) -> Vec<u8> {
        // Minimal single/double-byte varint encoder sufficient for small
        // test values (this crate's real decoder handles the full 9-byte
        // form; this helper only needs to round-trip through it).
        if value < 0x80 {
            return vec![value as u8];
        }
        let mut bytes = Vec::new();
        let mut chunks = Vec::new();
        loop {
            chunks.push((value & 0x7f) as u8);
            value >>= 7;
            if value == 0 {
                break;
            }
        }
        chunks.reverse();
        for (i, c) in chunks.iter().enumerate() {
            if i + 1 == chunks.len() {
                bytes.push(*c);
            } else {
                bytes.push(c | 0x80);
            }
        }
        bytes
    }

    #[test]
    fn local_payload_size_min_local_uses_integer_division_not_modulo() {
        // usable_size=512 gives min_local=39 (correct, via `/255`) vs 167
        // (if `/255` were mutated to `%255`). payload_len=5150 is chosen so
        // the two min_local values land the `(payload_len - min_local) %
        // denom` remainder on opposite sides of a denom (508) multiple,
        // making the two paths diverge to entirely different results (70
        // vs 167) instead of coincidentally agreeing.
        assert_eq!(local_payload_size(512, 5150, false), 70);
    }

    #[test]
    fn reassemble_payload_accepts_exactly_max_payload_len() {
        let source = FakePageSource {
            pages: HashMap::new(),
        };
        let page: Rc<[u8]> = Rc::from(Vec::new().as_slice());
        let err =
            reassemble_payload(&source, 512, 2, &page, 0, MAX_PAYLOAD_LEN, false).unwrap_err();
        assert!(!matches!(err, BtreeError::PayloadTooLarge { .. }));
    }

    /// #473: `first`/`next`/etc. must position the cursor and return the
    /// rowid WITHOUT reassembling the payload — including the expensive
    /// overflow-chain case. Proven here by declaring an overflow pointer
    /// to a page that doesn't exist in the fake source: `first()` (the
    /// lean, rowid-only API) must still succeed, because it never walks
    /// the overflow chain; only `current_payload()`, called explicitly
    /// afterward, actually attempts the walk and hits the missing page.
    #[test]
    fn first_does_not_reassemble_overflow_payload_until_asked() {
        let mut page = vec![0u8; 512];
        page[0] = 0x0d; // leaf table
        page[3..5].copy_from_slice(&1u16.to_be_bytes()); // num_cells = 1
        let cell_ptr_off = 8usize;
        let cell_start = 16usize;
        page[cell_ptr_off..cell_ptr_off + 2].copy_from_slice(&(cell_start as u16).to_be_bytes());

        // payload_len varint = 5000 (forces overflow for a 512-byte usable
        // page), rowid varint = 1, then local bytes + a 4-byte overflow
        // pointer to page 99 — deliberately never inserted into `pages`.
        let mut cell = Vec::new();
        cell.extend_from_slice(&encode_varint_for_test(5000));
        cell.extend_from_slice(&encode_varint_for_test(1));
        let local_size = local_payload_size(512, 5000, false) as usize;
        cell.extend(std::iter::repeat_n(0u8, local_size));
        cell.extend_from_slice(&99u32.to_be_bytes());
        page[cell_start..cell_start.saturating_add(cell.len())].copy_from_slice(&cell);

        let mut pages = HashMap::new();
        pages.insert(2u32, page);
        let source = FakePageSource { pages };
        let mut cursor = TableCursor::new(source, &fake_header(), 2);

        assert_eq!(cursor.first().unwrap(), Some(1));
        let err = cursor.current_payload().unwrap_err();
        assert!(matches!(
            err,
            BtreeError::PageSource {
                page_num: 99,
                source: PageError::InvalidPageNumber
            }
        ));
    }

    /// #467: a row whose payload fits entirely in the local cell (no
    /// overflow) must borrow from the page's `Rc<[u8]>` rather than
    /// allocating a fresh `Vec<u8>` copy. Asserted two ways: the returned
    /// `Payload` is the `Local` (borrowed) variant, and the page's
    /// refcount goes up (the row and the still-open cursor frame share
    /// one allocation) instead of staying at 1 (which would mean a copy
    /// was made instead of a share).
    #[test]
    fn local_payload_borrows_from_the_page_instead_of_copying() {
        let cell = table_cell(1, b"hello world");
        let page = leaf_page_with_cells(512, &[cell]);
        let mut pages = HashMap::new();
        pages.insert(2u32, page);
        let source = FakePageSource { pages };
        let mut cursor = TableCursor::new(source, &fake_header(), 2);

        let row = cursor.first_row().unwrap().unwrap();
        assert_eq!(&*row.payload, b"hello world");
        match &row.payload {
            Payload::Local { page, .. } => {
                assert!(
                    Rc::strong_count(page) >= 2,
                    "expected the row to share the page's Rc, not hold the only reference"
                );
            }
            Payload::Owned(_) => panic!("expected a borrowed Local payload, got an Owned copy"),
        }
    }

    #[test]
    fn require_interior_header_rejects_page_one_byte_short() {
        let page = vec![0u8; 11];
        let err = require_interior_header(&page, 0, 2).unwrap_err();
        assert!(matches!(
            err,
            BtreeError::PageTooShort {
                page_num: 2,
                len: 11
            }
        ));
    }

    #[test]
    fn require_interior_header_accepts_page_exactly_twelve_bytes() {
        let page = vec![0u8; 12];
        assert!(require_interior_header(&page, 0, 2).is_ok());
    }

    fn leaf_page_with_cells(page_size: usize, cells: &[Vec<u8>]) -> Vec<u8> {
        let mut buf = vec![0u8; page_size];
        write_leaf_page(&mut buf, 0, 1, cells).unwrap();
        buf
    }

    /// A valid table leaf cell (payload-length varint + rowid varint +
    /// local payload bytes, no overflow) — matches what `decode_cell_head`
    /// (the `has_rowid = true` shape) expects.
    fn table_cell(rowid: i64, payload: &[u8]) -> Vec<u8> {
        let mut cell = encode_varint(payload.len() as u64);
        cell.extend(encode_varint(rowid as u64));
        cell.extend_from_slice(payload);
        cell
    }

    /// #337: a single-cell insert that fits in the gap between the
    /// cell-pointer array and `content_start` must splice in place
    /// (`Ok(true)`) rather than falling back, and the resulting page must
    /// read back correctly via the normal collect path.
    #[test]
    fn splice_insert_cell_appends_into_the_gap_when_it_fits() {
        let mut buf = leaf_page_with_cells(512, &[]);
        let cell = build_interior_cell(0, 42); // any small byte string works as a stand-in cell
        let spliced = splice_insert_cell(&mut buf, 0, 1, 0, &cell).unwrap();
        assert!(spliced);
        let cells = collect_leaf_cells(&buf, 0, 1, 512).unwrap();
        // collect_leaf_cells decodes a table-leaf cell shape; just check
        // the raw bytes landed and the header accounting is consistent.
        assert_eq!(read_num_cells(&buf, 0, 1).unwrap(), 1);
        let _ = cells; // may error decoding as a table cell; header check above is the real assertion
    }

    #[test]
    fn splice_insert_cell_declines_when_the_gap_is_too_small() {
        // A page with content_start pinned right at the end of the
        // pointer array (no gap) must refuse the fast path.
        let mut buf = vec![0u8; 32];
        put_u8(&mut buf, 0, LEAF_TABLE, 1).unwrap();
        write_content_start(&mut buf, 0, 8, 1).unwrap(); // ptr_base(8) + 0 cells == 8, zero gap
        let cell = vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let spliced = splice_insert_cell(&mut buf, 0, 1, 0, &cell).unwrap();
        assert!(!spliced, "no contiguous gap must decline the fast path");
    }

    /// #52 tagged MC/DC vector (obligation `btree_1374`, decision
    /// `content_start < ptr_end || content_start.saturating_sub(ptr_end)
    /// < needed`): leaf A (`content_start < ptr_end`) true independently
    /// flips the outcome to true regardless of leaf B — a corrupt/
    /// degenerate layout where `content_start` already sits before the
    /// end of the pointer array. Paired against
    /// `splice_insert_cell_appends_into_the_gap_when_it_fits` (both
    /// leaves false) for A's independence pair.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__btree_1489__v1_content_start_before_ptr_end() {
        let mut buf = vec![0u8; 32];
        put_u8(&mut buf, 0, LEAF_TABLE, 1).unwrap();
        write_content_start(&mut buf, 0, 4, 1).unwrap(); // ptr_base(8) + 0 cells == 8 > content_start(4)
        let cell = vec![1u8, 2, 3];
        let spliced = splice_insert_cell(&mut buf, 0, 1, 0, &cell).unwrap();
        assert!(
            !spliced,
            "content_start before ptr_end must decline regardless of gap size"
        );
    }

    /// #52 tagged MC/DC vector (obligation `btree_1374`): both leaves
    /// false — the fast path proceeds. Independence pair for leaf A
    /// against `mcdc__btree_1489__v1_content_start_before_ptr_end`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__btree_1489__v2_both_leaves_false() {
        let mut buf = leaf_page_with_cells(512, &[]);
        let cell = build_interior_cell(0, 42);
        let spliced = splice_insert_cell(&mut buf, 0, 1, 0, &cell).unwrap();
        assert!(
            spliced,
            "content_start at/after ptr_end with enough gap must splice"
        );
    }

    /// #52 tagged MC/DC vector (obligation `btree_1374`): leaf B
    /// (`content_start.saturating_sub(ptr_end) < needed`) true while A is
    /// false independently flips the outcome to true — a zero-size gap.
    /// Independence pair for leaf B against
    /// `mcdc__btree_1489__v2_both_leaves_false`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__btree_1489__v3_gap_too_small() {
        let mut buf = vec![0u8; 32];
        put_u8(&mut buf, 0, LEAF_TABLE, 1).unwrap();
        write_content_start(&mut buf, 0, 8, 1).unwrap(); // ptr_base(8) + 0 cells == 8, zero gap
        let cell = vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let spliced = splice_insert_cell(&mut buf, 0, 1, 0, &cell).unwrap();
        assert!(!spliced, "zero-size gap must decline the fast path");
    }

    /// MC/DC vector (obligation `btree_1081`, `scan_leaf_cells`'s
    /// insert-position decision `cell_rowid > rowid && insert_pos ==
    /// num_cells`): both leaves true — the first cell with a greater
    /// rowid sets `insert_pos`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__btree_1081__v1_first_greater_rowid_sets_insert_pos() {
        let buf = leaf_page_with_cells(512, &[table_cell(10, &[1, 2, 3])]);
        let (insert_pos, num_cells, _) = scan_leaf_cells(&buf, 0, 1, 512, 5).unwrap();
        assert_eq!((insert_pos, num_cells), (0, 1));
    }

    /// MC/DC vector (obligation `btree_1081`): leaf A true, leaf B
    /// (`insert_pos == num_cells`) false — a second cell also exceeding
    /// `rowid` must not overwrite the insertion point already found at
    /// the first one. Independence pair for B against
    /// `mcdc__btree_1081__v1_first_greater_rowid_sets_insert_pos`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__btree_1081__v2_later_greater_rowid_does_not_overwrite() {
        let buf = leaf_page_with_cells(
            512,
            &[table_cell(10, &[1, 2, 3]), table_cell(20, &[4, 5, 6])],
        );
        let (insert_pos, num_cells, _) = scan_leaf_cells(&buf, 0, 1, 512, 5).unwrap();
        assert_eq!((insert_pos, num_cells), (0, 2));
    }

    /// MC/DC vector (obligation `btree_1081`): leaf A false — every cell's
    /// rowid is at or below the target, so `insert_pos` stays at
    /// `num_cells` regardless of leaf B. Independence pair for A against
    /// `mcdc__btree_1081__v1_first_greater_rowid_sets_insert_pos`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__btree_1081__v3_no_greater_rowid_leaves_insert_pos_at_num_cells() {
        let buf = leaf_page_with_cells(512, &[table_cell(1, &[1, 2, 3])]);
        let (insert_pos, num_cells, _) = scan_leaf_cells(&buf, 0, 1, 512, 5).unwrap();
        assert_eq!((insert_pos, num_cells), (1, 1));
    }

    /// #337: deleting a cell that borders `content_start` must grow
    /// `content_start` past it (reclaiming the space directly into the
    /// gap) rather than creating a freeblock.
    #[test]
    fn splice_delete_cell_bordering_content_start_grows_it() {
        let cell_a = table_cell(1, &[0xAAu8; 8]);
        let cell_b = table_cell(2, &[0xBBu8; 8]);
        let mut buf = leaf_page_with_cells(512, &[cell_a.clone(), cell_b.clone()]);
        let content_start_before = read_content_start(&buf, 0, 1).unwrap();

        // `write_page_common` lays cells out back-to-front from the end
        // of the page, processing the vec in reverse — so the FIRST cell
        // in the vec ends up at the lowest offset, bordering
        // content_start.
        splice_delete_cell(&mut buf, 0, 1, 512, 0, true).unwrap();

        assert_eq!(read_num_cells(&buf, 0, 1).unwrap(), 1);
        let content_start_after = read_content_start(&buf, 0, 1).unwrap();
        assert_eq!(
            content_start_after,
            content_start_before + cell_a.len(),
            "content_start must grow by exactly the deleted cell's length"
        );
        assert_eq!(read_first_freeblock(&buf, 0, 1).unwrap(), 0);
    }

    /// #337: deleting a cell that does NOT border `content_start` (an
    /// earlier-written, higher-offset cell) must record a freeblock, not
    /// touch `content_start`.
    #[test]
    fn splice_delete_cell_not_bordering_content_start_makes_a_freeblock() {
        let cell_a = table_cell(1, &[0xAAu8; 8]);
        let cell_b = table_cell(2, &[0xBBu8; 8]);
        let mut buf = leaf_page_with_cells(512, &[cell_a.clone(), cell_b.clone()]);
        let content_start_before = read_content_start(&buf, 0, 1).unwrap();

        // Index 1 (cell_b) is the higher-offset cell, not adjacent to
        // content_start (see the sibling test's comment on layout order).
        splice_delete_cell(&mut buf, 0, 1, 512, 1, true).unwrap();

        let content_start_after = read_content_start(&buf, 0, 1).unwrap();
        assert_eq!(
            content_start_after, content_start_before,
            "content_start must be unchanged when the freed cell doesn't border it"
        );
        let blocks = walk_freeblocks(&buf, 0, 1).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].1, cell_b.len());
    }

    /// #337: two freeblocks that end up adjacent (after enough deletes)
    /// must coalesce into one rather than being tracked as separate
    /// entries.
    #[test]
    fn add_freeblock_coalesces_adjacent_ranges() {
        let mut blocks = vec![(100usize, 10usize), (130, 10)];
        // A new freed range exactly bridging the gap between the two
        // existing freeblocks must merge with both.
        add_freeblock(&mut blocks, 110, 20);
        assert_eq!(blocks, vec![(100, 40)]);
    }

    /// #337: a freed range shorter than [`MIN_FREEBLOCK_SIZE`] that does
    /// NOT border `content_start` must be accounted for as fragmentation,
    /// not a freeblock (it's too short to hold a freeblock's own 4-byte
    /// header). Sandwiching a 1-byte cell (`payload_len` varint `0`, no
    /// payload, no overflow — a valid, if degenerate, index leaf cell)
    /// between two normal cells keeps it out of the content-start-border
    /// case, which is covered separately above.
    #[test]
    fn splice_delete_cell_tiny_non_bordering_gap_becomes_fragmentation() {
        let normal_a = vec![0xCCu8; 10];
        let tiny = vec![0u8];
        let normal_b = vec![0xDDu8; 10];
        let mut buf = leaf_page_with_cells(512, &[normal_a, tiny.clone(), normal_b]);
        let content_start_before = read_content_start(&buf, 0, 1).unwrap();

        splice_delete_cell(&mut buf, 0, 1, 512, 1, false).unwrap();

        assert_eq!(
            read_content_start(&buf, 0, 1).unwrap(),
            content_start_before,
            "a non-bordering delete must never touch content_start"
        );
        assert!(
            walk_freeblocks(&buf, 0, 1).unwrap().is_empty(),
            "a sub-MIN_FREEBLOCK_SIZE gap must not become a freeblock"
        );
        assert_eq!(
            read_fragmented_bytes(&buf, 0, 1).unwrap() as usize,
            tiny.len()
        );
    }
}
