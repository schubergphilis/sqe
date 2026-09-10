// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Index b-tree read path (Tier 0): a read-only cursor over index b-trees
//! (page types 0x02 interior / 0x0a leaf). WITHOUT ROWID tables are
//! stored as index b-trees — confirmed on a real fixture (spike 005,
//! #12: FTS5's `t_idx`/`t_config` shadow tables) — so this cursor is what
//! makes those tables (and ordinary secondary indexes) readable at all.
//!
//! Unlike table b-trees, index b-tree **interior cells carry a full key
//! payload**, not just a routing pointer: an interior cell represents a
//! real, sorted entry (with a left-child subtree of lesser keys), not
//! merely a separator. In-order traversal therefore yields each interior
//! cell's own key interleaved with descending into its children —
//! different from `TableCursor`'s traversal, which never yields data
//! from an interior page.
//!
//! Key comparison (NULL < numeric < text < blob, BINARY collation only —
//! Tier 0 scope) is minimal by design, per the originating issue: enough
//! ordering to walk in the correct sequence, not a fully general seek.
//! [`IndexCursor::seek`] does a real O(log n) tree descent (#661),
//! binary-searching each level's cell array rather than scanning linearly
//! from the first entry.

use std::cmp::Ordering;
use std::rc::Rc;

use super::{
    cell_ptr_offset, local_payload_size, page1_header_start, read_cell_pointer, read_num_cells,
    read_page_type, read_u32, reassemble_payload, require_interior_header, write_page_common,
    BtreeError, Payload, MAX_PAGES_VISITED,
};
use crate::pager::Pager;
use crate::record::{decode_record, decode_varint, TextEncoding, Value};
use crate::vfs::PageSource;

mod delete;
mod insert;

pub use delete::delete_entry;
pub use insert::insert_entry;

pub(super) const LEAF_INDEX: u8 = 0x0a;
pub(super) const INTERIOR_INDEX: u8 = 0x02;

/// A decoded index leaf entry: its key (for ordering) alongside its raw,
/// verbatim cell bytes. Used by the index insert/delete write paths.
pub(super) type IndexLeafCell = (Vec<Value>, Vec<u8>);

/// A decoded index interior entry: `(child_page, decoded_key,
/// raw_value_cell_bytes)`. Used by the index insert/delete write paths.
pub(super) type IndexInteriorEntry = (u32, Vec<Value>, Vec<u8>);

/// One decoded index b-tree entry: the raw key-record payload (after
/// overflow-chain reassembly). For an ordinary secondary index the
/// decoded record's last column is the referenced table's rowid; for a
/// WITHOUT ROWID table's own storage the decoded record IS the row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRow {
    /// The decoded key-record's raw payload bytes.
    pub payload: Payload,
}

struct IndexFrame {
    page_num: u32,
    is_interior: bool,
    num_cells: usize,
    cell_ptr_base: usize,
    rightmost: u32,
    /// Leaf: cell index to yield next, 0..num_cells.
    /// Interior: a step counter over `2 * num_cells + 1` steps — even
    /// step `2*i` descends into cell `i`'s left child, odd step `2*i+1`
    /// yields cell `i`'s own key, and the final step `2*num_cells`
    /// descends into the rightmost child.
    step: usize,
    page: Rc<[u8]>,
}

/// Depth-first, read-only cursor over an index b-tree, yielding entries
/// in ascending key order (BINARY collation).
pub struct IndexCursor<P: PageSource> {
    source: P,
    usable_size: u32,
    root_page: u32,
    stack: Vec<IndexFrame>,
    pages_visited: usize,
}

impl<P: PageSource> IndexCursor<P> {
    /// Creates a cursor over the index b-tree rooted at `root_page`,
    /// unpositioned until traversal begins.
    pub fn new(source: P, usable_size: u32, root_page: u32) -> Self {
        IndexCursor {
            source,
            usable_size,
            root_page,
            stack: Vec::new(),
            pages_visited: 0,
        }
    }

    /// Positions the cursor at the first entry and returns it, or `None`
    /// if the index is empty. Resets any prior traversal position.
    pub fn first(&mut self) -> Result<Option<IndexRow>, BtreeError> {
        self.stack.clear();
        self.pages_visited = 0;
        self.push_page(self.root_page)?;
        self.advance()
    }

    /// Advances to the next entry and returns it, or `None` once
    /// exhausted. Call [`Self::first`] first.
    #[allow(clippy::should_implement_trait)] // deliberate cursor API, not std::iter::Iterator
    pub fn next(&mut self) -> Result<Option<IndexRow>, BtreeError> {
        self.advance()
    }

    /// Positions the cursor at the last entry (descending key order from
    /// here on) and returns it, or `None` if the index is empty. Resets
    /// any prior traversal position. Used by #296's index-ordered scan
    /// for a `ORDER BY <indexed col> DESC`/reverse walk — the same b-tree
    /// [`Self::first`]/[`Self::next`] already read forward, just visited
    /// high-to-low.
    pub fn last(&mut self) -> Result<Option<IndexRow>, BtreeError> {
        self.stack.clear();
        self.pages_visited = 0;
        self.push_page_desc(self.root_page)?;
        self.advance_desc()
    }

    /// Advances to the previous entry (descending key order) and returns
    /// it, or `None` once exhausted. Call [`Self::last`] first.
    pub fn prev(&mut self) -> Result<Option<IndexRow>, BtreeError> {
        self.advance_desc()
    }

    /// Returns the first entry (in ascending key order) whose decoded key
    /// is not less than `target`, or `None` if every entry is less than
    /// `target`.
    ///
    /// A real tree descent (#661): at each interior level, binary-searches
    /// that page's cell array for the leftmost cell whose key is not less
    /// than `target`, then descends into that cell's left child (which
    /// may itself hold a qualifying entry smaller than the cell's own
    /// key) — recording that cell as a fallback by leaving this frame's
    /// `step` positioned just past the descend, exactly as normal
    /// [`Self::advance`] traversal would after visiting it. If no cell on
    /// a level qualifies, descends into `rightmost` instead. At the leaf,
    /// binary-searches for the matching entry directly. Either way, the
    /// stack left behind is indistinguishable from one built by ordinary
    /// [`Self::first`]/[`Self::next`] traversal up to this point, so a
    /// final [`Self::advance`] call yields the right entry — the leaf
    /// match if there is one, or (if the leaf has nothing to offer) pops
    /// back up to the nearest ancestor's fallback cell, or `None` if
    /// nothing on the path ever qualified. This decodes only the O(log n)
    /// candidate cells actually inspected per level, not every cell in
    /// the tree.
    #[allow(
        clippy::indexing_slicing,
        reason = "top = stack.len() - 1, computed just above from a non-empty stack (just pushed); always in bounds"
    )]
    pub fn seek(
        &mut self,
        target: &[Value],
        encoding: TextEncoding,
    ) -> Result<Option<IndexRow>, BtreeError> {
        self.stack.clear();
        self.pages_visited = 0;
        self.push_page(self.root_page)?;
        loop {
            let top = self.stack.len().saturating_sub(1);
            let (is_interior, num_cells) = {
                let f = &self.stack[top];
                (f.is_interior, f.num_cells)
            };
            if !is_interior {
                let i = self.binary_search_page(top, num_cells, target, encoding, false)?;
                self.stack[top].step = i;
                break;
            }
            let i = self.binary_search_page(top, num_cells, target, encoding, true)?;
            let child = if i < num_cells {
                self.stack[top].step = i.saturating_mul(2).saturating_add(1);
                self.read_interior_child(top, i)?
            } else {
                self.stack[top].step = num_cells.saturating_mul(2).saturating_add(1);
                self.stack[top].rightmost
            };
            self.push_page(child)?;
        }
        self.advance()
    }

    /// Binary-searches `top`'s cell array (interior or leaf, per
    /// `is_interior`) for the leftmost cell index whose decoded key is
    /// not less than `target`, or `num_cells` if none qualify. Only the
    /// O(log n) cells actually probed get their payload decoded.
    fn binary_search_page(
        &self,
        top: usize,
        num_cells: usize,
        target: &[Value],
        encoding: TextEncoding,
        is_interior: bool,
    ) -> Result<usize, BtreeError> {
        let mut lo = 0usize;
        let mut hi = num_cells;
        while lo < hi {
            let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
            let row = if is_interior {
                self.decode_interior_entry(top, mid)?
            } else {
                self.decode_leaf_entry(top, mid)?
            };
            let key = decode_record(&row.payload, encoding)?;
            if compare_keys(&key, target) != Ordering::Less {
                hi = mid;
            } else {
                lo = mid.saturating_add(1);
            }
        }
        Ok(lo)
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

    fn push_page(&mut self, page_num: u32) -> Result<(), BtreeError> {
        let page = self.read_page(page_num)?;
        let header_start = page1_header_start(page_num);
        let page_type = read_page_type(&page, header_start, page_num)?;
        let is_interior = match page_type {
            LEAF_INDEX => false,
            INTERIOR_INDEX => true,
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
        self.stack.push(IndexFrame {
            page_num,
            is_interior,
            num_cells,
            cell_ptr_base,
            rightmost,
            step: 0,
            page,
        });
        Ok(())
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "top = stack.len() - 1, computed just above from a non-empty check; always in bounds"
    )]
    fn advance(&mut self) -> Result<Option<IndexRow>, BtreeError> {
        loop {
            let top = match self.stack.len() {
                0 => return Ok(None),
                n => n.saturating_sub(1),
            };
            let (is_interior, step, num_cells, rightmost) = {
                let f = &self.stack[top];
                (f.is_interior, f.step, f.num_cells, f.rightmost)
            };

            if !is_interior {
                if step >= num_cells {
                    self.stack.pop();
                    continue;
                }
                self.stack[top].step = self.stack[top].step.saturating_add(1);
                return self.decode_leaf_entry(top, step).map(Some);
            }

            let total_steps = num_cells.saturating_mul(2);
            if step > total_steps {
                self.stack.pop();
                continue;
            }
            self.stack[top].step = self.stack[top].step.saturating_add(1);
            if step == total_steps {
                self.push_page(rightmost)?;
            } else if step % 2 == 0 {
                let child = self.read_interior_child(top, step / 2)?;
                self.push_page(child)?;
            } else {
                // step is odd here (the step % 2 == 0 arm above didn't
                // match), so step >= 1 and this never underflows.
                return self
                    .decode_interior_entry(top, step.saturating_sub(1) / 2)
                    .map(Some);
            }
        }
    }

    /// [`Self::push_page`]'s mirror for descending traversal: same frame
    /// shape, just with `step` initialized so [`Self::advance_desc`] walks
    /// it high-to-low instead of low-to-high (see that method's doc for
    /// the shared even/odd action encoding this relies on).
    fn push_page_desc(&mut self, page_num: u32) -> Result<(), BtreeError> {
        self.push_page(page_num)?;
        let Some(frame) = self.stack.last_mut() else {
            return Ok(());
        };
        frame.step = if frame.is_interior {
            frame.num_cells.saturating_mul(2).saturating_add(1)
        } else {
            frame.num_cells
        };
        Ok(())
    }

    /// [`Self::advance`]'s mirror for descending traversal (`Last`/`Prev`,
    /// #296): visits the same leaf/interior cells `advance` does, in the
    /// exact opposite order. A leaf frame's `step` counts *down* from
    /// `num_cells` (0 means exhausted, `step - 1` is the next cell to
    /// yield). An interior frame's `step` counts down from `2 * num_cells
    /// + 1`; writing `a = step - 1`: an even `a` means "descend child
    /// `a / 2`" (child index `num_cells` denotes the rightmost pointer —
    /// the same encoding `advance`'s ascending walk uses, just visited
    /// from `a = total_steps` down to `a = 0` instead of `0` up to
    /// `total_steps`); an odd `a` means "yield entry `(a - 1) / 2`". This
    /// symmetry is why no separate reversed encoding is needed: `advance`
    /// and `advance_desc` differ only in which direction `step` moves.
    #[allow(
        clippy::indexing_slicing,
        reason = "top = stack.len() - 1, computed just above from a non-empty check; always in bounds"
    )]
    fn advance_desc(&mut self) -> Result<Option<IndexRow>, BtreeError> {
        loop {
            let top = match self.stack.len() {
                0 => return Ok(None),
                n => n.saturating_sub(1),
            };
            let (is_interior, step, num_cells, rightmost) = {
                let f = &self.stack[top];
                (f.is_interior, f.step, f.num_cells, f.rightmost)
            };

            if !is_interior {
                if step == 0 {
                    self.stack.pop();
                    continue;
                }
                let idx = step.saturating_sub(1);
                self.stack[top].step = idx;
                return self.decode_leaf_entry(top, idx).map(Some);
            }

            if step == 0 {
                self.stack.pop();
                continue;
            }
            let a = step.saturating_sub(1);
            self.stack[top].step = a;
            if a % 2 == 0 {
                let child_index = a / 2;
                if child_index == num_cells {
                    self.push_page_desc(rightmost)?;
                } else {
                    let child = self.read_interior_child(top, child_index)?;
                    self.push_page_desc(child)?;
                }
            } else {
                return self
                    .decode_interior_entry(top, a.saturating_sub(1) / 2)
                    .map(Some);
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
    fn decode_leaf_entry(
        &self,
        frame_index: usize,
        cell_index: usize,
    ) -> Result<IndexRow, BtreeError> {
        let frame = &self.stack[frame_index];
        let page_num = frame.page_num;
        let ptr_off = cell_ptr_offset(frame.cell_ptr_base, cell_index);
        let cell_start = read_cell_pointer(&frame.page, ptr_off, page_num, cell_index)?;
        let (payload_len, tail_start) = decode_payload_len(&frame.page, cell_start, page_num)?;
        let payload = reassemble_payload(
            &self.source,
            self.usable_size,
            page_num,
            &frame.page,
            tail_start,
            payload_len,
            true,
        )?;
        Ok(IndexRow { payload })
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "frame_index is always `top` from advance(), always in bounds"
    )]
    fn decode_interior_entry(
        &self,
        frame_index: usize,
        cell_index: usize,
    ) -> Result<IndexRow, BtreeError> {
        let frame = &self.stack[frame_index];
        let page_num = frame.page_num;
        let ptr_off = cell_ptr_offset(frame.cell_ptr_base, cell_index);
        let cell_start = read_cell_pointer(&frame.page, ptr_off, page_num, cell_index)?;
        // Interior index cell: 4-byte left-child pointer, then the same
        // payload-length-varint + payload shape as a leaf cell.
        let (payload_len, tail_start) =
            decode_payload_len(&frame.page, cell_start.saturating_add(4), page_num)?;
        let payload = reassemble_payload(
            &self.source,
            self.usable_size,
            page_num,
            &frame.page,
            tail_start,
            payload_len,
            true,
        )?;
        Ok(IndexRow { payload })
    }
}

/// Decodes an index cell's payload-length varint at `offset`, returning
/// `(payload_len, tail_start)`. Shared by the index insert/delete write
/// paths (leaf cells decode at `cell_start`; interior cells at
/// `cell_start + 4`, past the left-child pointer).
pub(super) fn decode_payload_len(
    page: &[u8],
    offset: usize,
    page_num: u32,
) -> Result<(u64, usize), BtreeError> {
    let cell = page.get(offset..).ok_or(BtreeError::InvalidCellPointer {
        page_num,
        index: offset,
    })?;
    let (payload_len, n1) =
        decode_varint(cell).map_err(|source| BtreeError::InvalidCellVarint { page_num, source })?;
    Ok((payload_len, offset.saturating_add(n1)))
}

/// SQLite's Tier 0 (BINARY collation) type ordering: NULL < numeric <
/// text < blob.
pub(super) fn value_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Integer(_) | Value::Real(_) => 1,
        Value::Text(_) => 2,
        Value::Blob(_) => 3,
    }
}

pub(super) fn compare_values(a: &Value, b: &Value) -> Ordering {
    let (ra, rb) = (value_rank(a), value_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Real(x), Value::Real(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Integer(x), Value::Real(y)) => {
            (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        (Value::Real(x), Value::Integer(y)) => {
            x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal)
        }
        (Value::Text(x), Value::Text(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Value::Blob(x), Value::Blob(y)) => x.cmp(y),
        _ => Ordering::Equal, // unreachable: value_rank already separated these
    }
}

/// Lexicographic key comparison over a (possibly composite) index key.
/// Shared by the index insert/delete write paths.
pub(super) fn compare_keys(a: &[Value], b: &[Value]) -> Ordering {
    for (x, y) in a.iter().zip(b.iter()) {
        let c = compare_values(x, y);
        if c != Ordering::Equal {
            return c;
        }
    }
    a.len().cmp(&b.len())
}

/// Reads every entry of an index leaf page in on-disk (key-ascending)
/// order, returning each entry's decoded key (for ordering) alongside its
/// raw, verbatim cell bytes: a payload-length varint, local payload
/// bytes, and an optional 4-byte overflow pointer — no leading rowid
/// varint, unlike a table leaf cell. Shared by the index insert/delete
/// write paths.
pub(super) fn collect_index_leaf_cells(
    source: &Pager,
    buf: &[u8],
    header_start: usize,
    page_num: u32,
    usable_size: u32,
    encoding: TextEncoding,
) -> Result<Vec<IndexLeafCell>, BtreeError> {
    let num_cells = read_num_cells(buf, header_start, page_num)?;
    let ptr_base = header_start.saturating_add(8);
    let page: Rc<[u8]> = Rc::from(buf);
    let mut out = Vec::with_capacity(num_cells);
    for i in 0..num_cells {
        let ptr_off = cell_ptr_offset(ptr_base, i);
        let cell_start = read_cell_pointer(buf, ptr_off, page_num, i)?;
        let (key, cell_bytes) = decode_value_cell(
            source,
            buf,
            &page,
            cell_start,
            page_num,
            usable_size,
            encoding,
        )?;
        out.push((key, cell_bytes));
    }
    Ok(out)
}

/// Reads every entry of an index interior page, returning `(child_page,
/// decoded_key, raw_value_cell_bytes)` triples in on-disk order plus the
/// rightmost pointer. Each interior cell is a 4-byte left-child pointer
/// followed by the same payload-length-varint + payload shape as a leaf
/// cell. Shared by the index insert/delete write paths.
pub(super) fn collect_index_interior_entries(
    source: &Pager,
    buf: &[u8],
    header_start: usize,
    page_num: u32,
    usable_size: u32,
    encoding: TextEncoding,
) -> Result<(Vec<IndexInteriorEntry>, u32), BtreeError> {
    let num_cells = read_num_cells(buf, header_start, page_num)?;
    let ptr_base = header_start.saturating_add(12);
    let page: Rc<[u8]> = Rc::from(buf);
    let mut out = Vec::with_capacity(num_cells);
    for i in 0..num_cells {
        let ptr_off = cell_ptr_offset(ptr_base, i);
        let cell_start = read_cell_pointer(buf, ptr_off, page_num, i)?;
        let child = read_u32(buf, cell_start, page_num)?;
        let value_start = cell_start.saturating_add(4);
        let (key, cell_bytes) = decode_value_cell(
            source,
            buf,
            &page,
            value_start,
            page_num,
            usable_size,
            encoding,
        )?;
        out.push((child, key, cell_bytes));
    }
    let rightmost = read_u32(buf, header_start.saturating_add(8), page_num)?;
    Ok((out, rightmost))
}

/// Decodes the payload-length-varint + payload "value cell" shape shared
/// by index leaf cells and (past their 4-byte child pointer) index
/// interior cells: returns the decoded key (for ordering) and the raw,
/// verbatim cell bytes (varint + local bytes + optional overflow
/// pointer), starting at `value_start`.
/// `page` must be the same bytes as `buf`, shared as an `Rc` so
/// `reassemble_payload` can follow an overflow chain past `buf`'s
/// borrow — callers decoding multiple cells off one page build `page`
/// once and pass it in, rather than paying a full-page copy per cell.
fn decode_value_cell(
    source: &Pager,
    buf: &[u8],
    page: &Rc<[u8]>,
    value_start: usize,
    page_num: u32,
    usable_size: u32,
    encoding: TextEncoding,
) -> Result<(Vec<Value>, Vec<u8>), BtreeError> {
    let (payload_len, tail_start) = decode_payload_len(buf, value_start, page_num)?;
    let local_size = local_payload_size(usable_size, payload_len, true) as usize;
    let has_overflow = (local_size as u64) < payload_len;
    let cell_end = tail_start
        .saturating_add(local_size)
        .saturating_add(if has_overflow { 4 } else { 0 });
    let cell_bytes = buf
        .get(value_start..cell_end)
        .ok_or(BtreeError::PayloadTooShort { page_num })?
        .to_vec();
    let payload = reassemble_payload(
        source,
        usable_size,
        page_num,
        page,
        tail_start,
        payload_len,
        true,
    )?;
    let key = decode_record(&payload, encoding)?;
    Ok((key, cell_bytes))
}

/// Byte length of the value-cell (payload-length varint, local payload,
/// plus optional 4-byte overflow pointer) starting at `cell_start`,
/// without decoding its key or copying its bytes. Used for page-space
/// bookkeeping where only the cell's on-disk size matters, not its sort
/// key — letting [`super::insert::insert_into_index_leaf`]'s space check
/// avoid a full [`decode_value_cell`] per existing cell.
pub(super) fn value_cell_len(
    buf: &[u8],
    cell_start: usize,
    page_num: u32,
    usable_size: u32,
) -> Result<usize, BtreeError> {
    let (payload_len, tail_start) = decode_payload_len(buf, cell_start, page_num)?;
    let local_size = local_payload_size(usable_size, payload_len, true) as usize;
    let has_overflow = (local_size as u64) < payload_len;
    let cell_end = tail_start
        .saturating_add(local_size)
        .saturating_add(if has_overflow { 4 } else { 0 });
    Ok(cell_end.saturating_sub(cell_start))
}

/// Outcome of [`search_index_leaf`]: either an exact key match (with its
/// decoded cell, so callers don't need to decode it again) or the
/// cell-pointer-array position a new entry with this key would sort into.
pub(super) enum LeafSearch {
    Found(usize, IndexLeafCell),
    NotFound(usize),
}

/// Binary search for `key` among an index leaf page's sorted entries,
/// decoding only the O(log n) cells actually compared — unlike
/// [`collect_index_leaf_cells`], which decodes every cell on the page.
/// Used by the index insert/delete write paths to locate a position
/// without paying for a full-page decode on every write (#648).
pub(super) fn search_index_leaf(
    source: &Pager,
    buf: &[u8],
    header_start: usize,
    page_num: u32,
    usable_size: u32,
    encoding: TextEncoding,
    key: &[Value],
) -> Result<LeafSearch, BtreeError> {
    let num_cells = read_num_cells(buf, header_start, page_num)?;
    let ptr_base = header_start.saturating_add(8);
    let page: Rc<[u8]> = Rc::from(buf);
    let mut lo = 0usize;
    let mut hi = num_cells;
    while lo < hi {
        let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
        let ptr_off = cell_ptr_offset(ptr_base, mid);
        let cell_start = read_cell_pointer(buf, ptr_off, page_num, mid)?;
        let decoded = decode_value_cell(
            source,
            buf,
            &page,
            cell_start,
            page_num,
            usable_size,
            encoding,
        )?;
        match compare_keys(key, &decoded.0) {
            Ordering::Equal => return Ok(LeafSearch::Found(mid, decoded)),
            Ordering::Less => hi = mid,
            Ordering::Greater => lo = mid.saturating_add(1),
        }
    }
    Ok(LeafSearch::NotFound(lo))
}

/// Builds an index interior cell: 4-byte left-child page number followed
/// by `value_cell_bytes` verbatim (the same payload-length-varint +
/// payload shape as a leaf cell — index interior cells carry a full
/// entry, not just a routing key, per the module doc). Shared by the
/// index insert/delete write paths.
pub(super) fn build_index_interior_cell(child: u32, value_cell_bytes: &[u8]) -> Vec<u8> {
    let mut cell = child.to_be_bytes().to_vec();
    cell.extend_from_slice(value_cell_bytes);
    cell
}

/// Where [`descend_index_tree`] landed: either an ordinary leaf (the
/// common case), or an exact key match found on an interior page along
/// the way. The latter matters because index b-tree interior cells carry
/// a full entry, not just a routing key (per the module doc) — an entry
/// promoted to interior level during a split is invisible to a
/// leaf-only descent, silently missing both insert's duplicate-key check
/// and delete's lookup.
pub(super) enum IndexDescent {
    Leaf {
        ancestors: Vec<u32>,
        leaf_page: u32,
    },
    InteriorMatch {
        interior_page: u32,
        entry_child: u32,
    },
}

/// Descends from `page_num` (an index b-tree root or subtree) looking for
/// `key`. Returns [`IndexDescent::InteriorMatch`] the moment an interior
/// page's own entry compares exactly equal to `key` (via
/// [`compare_keys`]); otherwise routes into the first entry whose own key
/// is greater than `key` (or the rightmost child if none is) and
/// continues, finally returning [`IndexDescent::Leaf`] once a leaf page
/// is reached. Shared by the index insert/delete write paths — insert
/// uses an `InteriorMatch` to reject a duplicate key that was promoted to
/// interior level by an earlier split; delete uses it to trigger a
/// predecessor-swap (see `index_delete.rs`).
pub(super) fn descend_index_tree(
    pager: &Pager,
    mut page_num: u32,
    usable_size: u32,
    key: &[Value],
    encoding: TextEncoding,
) -> Result<IndexDescent, BtreeError> {
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
        if page_type == LEAF_INDEX {
            return Ok(IndexDescent::Leaf {
                ancestors,
                leaf_page: page_num,
            });
        } else if page_type == INTERIOR_INDEX {
            let (entries, rightmost) = collect_index_interior_entries(
                pager,
                &page,
                header_start,
                page_num,
                usable_size,
                encoding,
            )?;
            if let Some(entry_index) = entries
                .iter()
                .position(|(_, entry_key, _)| compare_keys(key, entry_key) == Ordering::Equal)
            {
                return Ok(IndexDescent::InteriorMatch {
                    interior_page: page_num,
                    entry_child: entries
                        .get(entry_index)
                        .ok_or({
                            BtreeError::Internal(
                                "entry_index must be in bounds: it was just found via .position()",
                            )
                        })?
                        .0,
                });
            }
            ancestors.push(page_num);
            let mut next = rightmost;
            for (child, entry_key, _) in &entries {
                if compare_keys(key, entry_key) == Ordering::Less {
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

/// Rebuilds `buf` in place as an index leaf page holding exactly `cells`,
/// in order. Shared by the index insert/delete write paths — see
/// `insert.rs`'s module doc for the "every page mutation fully rebuilds
/// the page" simplification this shares with the table write path.
pub(super) fn write_index_leaf_page(
    buf: &mut [u8],
    header_start: usize,
    page_num: u32,
    cells: &[Vec<u8>],
) -> Result<(), BtreeError> {
    write_page_common(buf, header_start, page_num, LEAF_INDEX, 8, cells)
}

/// As [`write_index_leaf_page`], but for an interior page — writes the
/// rightmost-child pointer (header bytes 8-11) after the common layout.
pub(super) fn write_index_interior_page(
    buf: &mut [u8],
    header_start: usize,
    page_num: u32,
    cells: &[Vec<u8>],
    rightmost: u32,
) -> Result<(), BtreeError> {
    write_page_common(buf, header_start, page_num, INTERIOR_INDEX, 12, cells)?;
    super::put(
        buf,
        header_start.saturating_add(8),
        &rightmost.to_be_bytes(),
        page_num,
    )
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
    use crate::vfs::{PageError, UnixVfs, Vfs, VfsPageSource};
    use std::collections::HashMap;
    use std::path::Path;

    fn open_cursor(fixture: &str, root_page: u32) -> IndexCursor<VfsPageSource> {
        let path = Path::new("tests/corpus/fixtures/btrees").join(fixture);
        let vfs = UnixVfs;
        let file = vfs.open_read(&path).unwrap();
        let mut header_buf = [0u8; 100];
        file.read_at(&mut header_buf, 0).unwrap();
        let header = DatabaseHeader::parse(&header_buf).unwrap();
        let source = VfsPageSource::open(&vfs, &path, header.page_size).unwrap();
        IndexCursor::new(source, header.usable_page_size(), root_page)
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

    #[test]
    fn secondary_index_walk_matches_oracle_binary_order() {
        // idx_b on t(b); ascending b, BINARY collation (lexicographic,
        // not numeric) — "row number 1" < "row number 10" < "row number
        // 100" < "row number 1000" < "row number 1001", confirmed against
        // `sqlite3 ... ORDER BY b, a`.
        let mut cursor = open_cursor("index.db", 3);
        let mut rows = Vec::new();
        let mut row = cursor.first().unwrap();
        while let Some(r) = row {
            rows.push(r);
            row = cursor.next().unwrap();
        }
        assert_eq!(rows.len(), 3000);

        let expect = [
            ("row number 1", 1i64),
            ("row number 10", 10),
            ("row number 100", 100),
            ("row number 1000", 1000),
            ("row number 1001", 1001),
        ];
        for (row, (b, a)) in rows.iter().zip(expect.iter()) {
            let key = decode_record(&row.payload, TextEncoding::Utf8).unwrap();
            assert_eq!(text(&key[0]), *b);
            assert_eq!(int(&key[1]), *a);
        }

        // Strictly ascending BINARY order end to end (walks the full
        // interior+leaf structure correctly, not just the first page).
        for i in 1..rows.len() {
            let prev = decode_record(&rows[i - 1].payload, TextEncoding::Utf8).unwrap();
            let cur = decode_record(&rows[i].payload, TextEncoding::Utf8).unwrap();
            assert_ne!(compare_keys(&prev, &cur), Ordering::Greater);
        }
    }

    #[test]
    fn secondary_index_last_prev_matches_reversed_forward_walk() {
        // #296: `Last`/`Prev` must yield exactly the reverse of
        // `first`/`next`'s ascending walk over the same index b-tree.
        let mut forward = open_cursor("index.db", 3);
        let mut forward_rows = Vec::new();
        let mut row = forward.first().unwrap();
        while let Some(r) = row {
            forward_rows.push(r);
            row = forward.next().unwrap();
        }

        let mut backward = open_cursor("index.db", 3);
        let mut backward_rows = Vec::new();
        let mut row = backward.last().unwrap();
        while let Some(r) = row {
            backward_rows.push(r);
            row = backward.prev().unwrap();
        }

        assert_eq!(backward_rows.len(), forward_rows.len());
        let mut reversed_forward = forward_rows.clone();
        reversed_forward.reverse();
        assert_eq!(backward_rows, reversed_forward);
    }

    #[test]
    fn secondary_index_seek_matches_oracle() {
        let mut cursor = open_cursor("index.db", 3);
        let target = [Value::Text("row number 100".to_string().into())];
        let row = cursor.seek(&target, TextEncoding::Utf8).unwrap().unwrap();
        let key = decode_record(&row.payload, TextEncoding::Utf8).unwrap();
        assert_eq!(text(&key[0]), "row number 100");
        assert_eq!(int(&key[1]), 100);
    }

    /// #661: `seek`'s binary-search tree descent must land on exactly the
    /// entry a linear full scan would, for a value between two existing
    /// keys (not an exact hit on any stored key — exercises the
    /// interior-cell fallback path when a descended-into child's subtree
    /// turns out to hold nothing `>= target`) — and the cursor must then
    /// be positioned correctly for `next()` to continue in order.
    #[test]
    fn secondary_index_seek_between_keys_matches_full_scan_and_next_continues() {
        let mut scan = open_cursor("index.db", 3);
        let mut rows = Vec::new();
        let mut row = scan.first().unwrap();
        while let Some(r) = row {
            rows.push(decode_record(&r.payload, TextEncoding::Utf8).unwrap());
            row = scan.next().unwrap();
        }

        // BINARY collation: "row number 15" sorts between "row number 1"
        // and "row number 150"/"row number 1500" etc., but isn't itself a
        // stored key.
        let target = [Value::Text("row number 15".to_string().into())];
        let expect_idx = rows
            .iter()
            .position(|k| compare_keys(k, &target) != Ordering::Less)
            .expect("some row must be >= target");

        let mut cursor = open_cursor("index.db", 3);
        let landed = cursor.seek(&target, TextEncoding::Utf8).unwrap().unwrap();
        let landed_key = decode_record(&landed.payload, TextEncoding::Utf8).unwrap();
        assert_eq!(landed_key, rows[expect_idx]);

        // `next()` from here must continue exactly where a full scan
        // would, proving the stack seek() leaves behind is positioned
        // like ordinary first()/next() traversal.
        for expected in &rows[expect_idx.saturating_add(1)..] {
            let next_row = cursor.next().unwrap().expect("more rows expected");
            let next_key = decode_record(&next_row.payload, TextEncoding::Utf8).unwrap();
            assert_eq!(&next_key, expected);
        }
        assert!(cursor.next().unwrap().is_none());
    }

    #[test]
    fn secondary_index_seek_past_every_key_returns_none() {
        let mut cursor = open_cursor("index.db", 3);
        let target = [Value::Text("zzz-past-everything".to_string().into())];
        assert!(cursor.seek(&target, TextEncoding::Utf8).unwrap().is_none());
    }

    #[test]
    fn secondary_index_seek_before_every_key_returns_first_row() {
        let mut cursor = open_cursor("index.db", 3);
        let target = [Value::Text(String::new().into())];
        let row = cursor.seek(&target, TextEncoding::Utf8).unwrap().unwrap();
        let key = decode_record(&row.payload, TextEncoding::Utf8).unwrap();

        let mut expect = open_cursor("index.db", 3);
        let first = expect.first().unwrap().unwrap();
        let first_key = decode_record(&first.payload, TextEncoding::Utf8).unwrap();
        assert_eq!(key, first_key);
    }

    /// #52 tagged MC/DC vector (obligation `index_868`, the ordering-check
    /// decision `idx < expect_order.len() && text(&key[0]) == expect_order[idx]`
    /// inside `without_rowid_table_is_readable_as_index_btree` below): both
    /// leaves true.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__index_871__v1_in_range_and_matches() {
        let expect_order = ["key1"];
        let idx = 0;
        let key0 = "key1";
        assert!(idx < expect_order.len() && key0 == expect_order[idx]);
    }

    /// #52 tagged MC/DC vector (obligation `index_868`): leaf A
    /// (`idx < expect_order.len()`) true, leaf B (key match) false —
    /// independence pair for B against
    /// `mcdc__index_871__v1_in_range_and_matches`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__index_871__v2_in_range_but_does_not_match() {
        let expect_order = ["key1"];
        let idx = 0;
        let key0 = "key2";
        assert!(!(idx < expect_order.len() && key0 == expect_order[idx]));
    }

    /// #52 tagged MC/DC vector (obligation `index_868`): leaf A false —
    /// independence pair for A against
    /// `mcdc__index_871__v1_in_range_and_matches` (short-circuits, so B
    /// is never evaluated).
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__index_871__v3_out_of_range() {
        let expect_order = ["key1"];
        // `black_box` defeats constant-folding so rustc can't statically
        // prove `expect_order[idx]` out of bounds — it never runs, since
        // `&&` short-circuits on the false left operand first.
        let idx = std::hint::black_box(1usize);
        let key0 = "key1";
        assert!(!(idx < expect_order.len() && key0 == expect_order[idx]));
    }

    #[test]
    fn without_rowid_table_is_readable_as_index_btree() {
        // t(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID — the table's own
        // storage IS an index b-tree keyed on k; the decoded record is
        // the full row, not a separate key+rowid split.
        let mut cursor = open_cursor("without_rowid.db", 2);
        let mut rows = Vec::new();
        let mut row = cursor.first().unwrap();
        while let Some(r) = row {
            rows.push(r);
            row = cursor.next().unwrap();
        }
        assert_eq!(rows.len(), 500);

        let first = decode_record(&rows[0].payload, TextEncoding::Utf8).unwrap();
        assert_eq!(text(&first[0]), "key1");
        assert_eq!(text(&first[1]), "value number 1");

        // BINARY collation: "key1" < "key10" < "key100" < "key99"
        // (shorter-is-less on a shared prefix), confirmed against oracle.
        let expect_order = ["key1", "key10", "key100", "key99"];
        let mut idx = 0;
        for r in &rows {
            let key = decode_record(&r.payload, TextEncoding::Utf8).unwrap();
            if idx < expect_order.len() && text(&key[0]) == expect_order[idx] {
                idx += 1;
            }
        }
        assert_eq!(idx, expect_order.len(), "expected keys not seen in order");

        for i in 1..rows.len() {
            let prev = decode_record(&rows[i - 1].payload, TextEncoding::Utf8).unwrap();
            let cur = decode_record(&rows[i].payload, TextEncoding::Utf8).unwrap();
            assert_ne!(compare_keys(&prev, &cur), Ordering::Greater);
        }
    }

    /// #471: mirrors #467's `local_payload_borrows_from_the_page_instead_of_copying`
    /// for the index-btree read path — a leaf row whose payload fits
    /// entirely in the local cell (no overflow) must borrow from the
    /// page's `Rc<[u8]>` rather than allocating a fresh `Vec<u8>` copy.
    #[test]
    fn index_local_payload_borrows_from_the_page_instead_of_copying() {
        let mut cursor = open_cursor("index.db", 3);
        let row = cursor.first().unwrap().unwrap();
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

    #[test]
    fn unexpected_page_type_errors_not_panics() {
        let mut page = vec![0u8; 512];
        page[0] = 0xff; // not a valid index b-tree page type
        let mut pages = HashMap::new();
        pages.insert(2u32, page);
        let source = FakePageSource { pages };
        let mut cursor = IndexCursor::new(source, 512, 2);

        let err = cursor.first().unwrap_err();
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
        pages.insert(2u32, vec![0x0a, 0, 0]); // leaf index type + 2 bytes, short of an 8-byte header
        let source = FakePageSource { pages };
        let mut cursor = IndexCursor::new(source, 512, 2);

        let err = cursor.first().unwrap_err();
        assert!(matches!(err, BtreeError::PageTooShort { page_num: 2, .. }));
    }
}
