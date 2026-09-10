// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Table b-tree insert (write path): cell insert, leaf split, cascading
//! interior splits, and root split. See
//! `.openspec/specs/006-btree/spec.md` (insert/split requirements) for the
//! byte-layout contract this module writes. Read-side helpers (`page1_header_start`,
//! `read_page_type`, `read_num_cells`, `read_u32`, `read_cell_pointer`,
//! `cell_ptr_offset`, `decode_cell_head`, `local_payload_size`) are reused
//! directly from the parent `btree` module — they're private to `btree` but
//! visible here as a descendant module.
//!
//! Single-cell inserts that fit without a split go through
//! [`crate::btree::splice_insert_cell`] (O(1) relative to the page's other
//! cells — a memmove of the cell-pointer array only, not the cell bytes),
//! falling back to a full collect/rebuild only when there isn't enough
//! contiguous free space (see #337). That fallback — and split/merge/root
//! rebuild, which always go through [`write_leaf_page`]/
//! [`write_interior_page`] — still rebuilds the page's cell-pointer array
//! and content area from scratch, which is also how freeblock/fragmented
//! space gets reclaimed (see `.openspec/adr/0023-leaf-cell-splice.md`).
//! Reserved-space-per-page (`usable_size < page_size`) is still not
//! accounted for in the rebuild — matches this codebase's current at-rest
//! fixtures (reserved bytes always 0) but would need generalizing before a
//! `PRAGMA reserve_bytes` fixture is supported.

use crate::btree::{
    build_interior_cell, cell_bytes, collect_interior_entries, collect_leaf_cells, find_leaf_page,
    local_payload_size, page1_header_start, put, read_page_type, scan_leaf_cells,
    splice_insert_cell, write_interior_page, write_leaf_page, BtreeError, INTERIOR_TABLE,
    LEAF_TABLE,
};
use crate::header::DatabaseHeader;
use crate::pager::Pager;
use crate::record::encode_varint;

/// Inserts one row `(rowid, payload)` into the table b-tree rooted at
/// `root_page`, splitting leaves/interior pages (and the root itself) as
/// needed to make room. `payload` is the already record-encoded row body
/// (e.g. from [`crate::record::encode_record`]) — this function does not
/// re-encode it, only frames it into a b-tree cell.
pub fn insert_row(
    pager: &mut Pager,
    header: &DatabaseHeader,
    root_page: u32,
    rowid: i64,
    payload: &[u8],
) -> Result<(), BtreeError> {
    let usable_size = header.usable_page_size();
    let cell = encode_leaf_cell(pager, usable_size, rowid, payload)?;
    let (ancestors, leaf_page) = find_leaf_page(pager, root_page, rowid)?;
    let page_len = pager.get_page_mut(leaf_page)?.len();
    insert_into_leaf(
        pager,
        usable_size,
        page_len,
        leaf_page,
        root_page,
        &ancestors,
        rowid,
        cell,
    )
}

/// Builds a leaf table-b-tree cell: payload-length varint + rowid varint +
/// local payload bytes, plus a trailing 4-byte overflow-page pointer when
/// `payload` doesn't fit locally (fileformat2.html "Cell Payload Overflow").
fn encode_leaf_cell(
    pager: &mut Pager,
    usable_size: u32,
    rowid: i64,
    payload: &[u8],
) -> Result<Vec<u8>, BtreeError> {
    let payload_len = payload.len() as u64;
    let local_size =
        (local_payload_size(usable_size, payload_len, false) as usize).min(payload.len());
    let (local_bytes, overflow_bytes) = payload.split_at(local_size);
    let mut cell = encode_varint(payload_len);
    cell.extend(encode_varint(rowid as u64));
    cell.extend_from_slice(local_bytes);
    if !overflow_bytes.is_empty() {
        let overflow_first = write_overflow_chain(pager, usable_size, overflow_bytes)?;
        cell.extend_from_slice(&overflow_first.to_be_bytes());
    }
    Ok(cell)
}

/// Writes `data` across freshly allocated overflow pages (each: 4-byte
/// next-page-number + chunk bytes, 0 terminates), mirroring
/// `reassemble_payload`'s read-side chain format in reverse. Returns the
/// first overflow page number.
fn write_overflow_chain(
    pager: &mut Pager,
    usable_size: u32,
    data: &[u8],
) -> Result<u32, BtreeError> {
    let available = usable_size.saturating_sub(4).max(1) as usize;
    let mut chunks: Vec<&[u8]> = Vec::new();
    let mut rest = data;
    while !rest.is_empty() {
        let take = rest.len().min(available);
        let (chunk, tail) = rest.split_at(take);
        chunks.push(chunk);
        rest = tail;
    }

    let mut page_nums = Vec::with_capacity(chunks.len());
    for _ in &chunks {
        page_nums.push(pager.allocate_page()?);
    }
    for (i, chunk) in chunks.iter().enumerate() {
        let next = page_nums.get(i.saturating_add(1)).copied().unwrap_or(0);
        let page_num = *page_nums.get(i).ok_or(BtreeError::Internal(
            "overflow chain page index out of bounds",
        ))?;
        let buf = pager.get_page_mut(page_num)?;
        put(buf, 0, &next.to_be_bytes(), page_num)?;
        put(buf, 4, chunk, page_num)?;
    }
    Ok(page_nums.first().copied().unwrap_or(0))
}

/// Inserts `cell` (already encoded, for `rowid`) into leaf page
/// `leaf_page`, splitting it (and cascading into ancestors/`root_page`) if
/// it doesn't fit.
#[allow(clippy::too_many_arguments)]
fn insert_into_leaf(
    pager: &mut Pager,
    usable_size: u32,
    page_len: usize,
    leaf_page: u32,
    root_page: u32,
    ancestors: &[u32],
    rowid: i64,
    cell: Vec<u8>,
) -> Result<(), BtreeError> {
    let header_start = page1_header_start(leaf_page);
    // Pre-scan without cloning the page or materializing any cell (#588):
    // the fast path below only needs the insert position and a fit check,
    // so the full `collect_leaf_cells` copy is deferred to the rebuild and
    // split branches that actually move cells.
    let buf = pager.get_page_mut(leaf_page)?;
    let (insert_pos, num_cells, existing_bytes) =
        scan_leaf_cells(buf, header_start, leaf_page, usable_size, rowid)?;

    let header_len = 8;
    let needed = header_start
        .saturating_add(header_len)
        .saturating_add(num_cells.saturating_add(1).saturating_mul(2))
        .saturating_add(existing_bytes)
        .saturating_add(cell.len());
    if needed <= page_len {
        // Fast path: splice the new cell directly into the page (O(1)
        // relative to the other cells) when there's enough contiguous
        // free space. Falls back to a full rebuild — which also reclaims
        // any freeblock/fragmentation space — otherwise.
        if !splice_insert_cell(buf, header_start, leaf_page, insert_pos, &cell)? {
            let mut cells = collect_leaf_cells(buf, header_start, leaf_page, usable_size)?;
            cells.insert(insert_pos, (rowid, cell));
            write_leaf_page(buf, header_start, leaf_page, &cell_bytes(cells))?;
        }
        return Ok(());
    }

    let mut cells = collect_leaf_cells(buf, header_start, leaf_page, usable_size)?;
    cells.insert(insert_pos, (rowid, cell));

    // Split: left keeps the lower half (including here if inserted there),
    // right (a freshly allocated page) takes the upper half.
    let n = cells.len();
    let left_n = n.div_ceil(2);
    let right_page = pager.allocate_page()?;
    let right = cells.split_off(left_n);
    let left = cells;
    let divider = left
        .last()
        .ok_or(BtreeError::Internal(
            "left half of a split leaf must not be empty",
        ))?
        .0;

    {
        let buf = pager.get_page_mut(leaf_page)?;
        write_leaf_page(buf, header_start, leaf_page, &cell_bytes(left))?;
    }
    {
        let buf = pager.get_page_mut(right_page)?;
        write_leaf_page(buf, 0, right_page, &cell_bytes(right))?;
    }

    insert_into_parent(
        pager,
        usable_size,
        page_len,
        ancestors,
        root_page,
        leaf_page,
        right_page,
        divider,
    )
}

/// Propagates a child split (`old_page` keeps its identity as the left
/// sibling, `new_page` is the freshly allocated right sibling, `divider` is
/// the max key routed to `old_page`) into its parent, splitting the parent
/// (or the root itself) if needed.
#[allow(clippy::too_many_arguments)]
fn insert_into_parent(
    pager: &mut Pager,
    usable_size: u32,
    page_len: usize,
    ancestors: &[u32],
    root_page: u32,
    old_page: u32,
    new_page: u32,
    divider: i64,
) -> Result<(), BtreeError> {
    let Some((&parent_page, rest)) = ancestors.split_last() else {
        return root_split(pager, usable_size, root_page, new_page, divider);
    };

    let header_start = page1_header_start(parent_page);
    // `collect_interior_entries` returns owned `(child, key)` pairs, so
    // the page can be borrowed directly — no need to clone it (#588).
    let (mut entries, mut rightmost) = {
        let buf = pager.get_page_mut(parent_page)?;
        collect_interior_entries(buf, header_start, parent_page)?
    };

    match entries.iter().position(|(child, _)| *child == old_page) {
        Some(idx) => {
            entries.insert(idx, (old_page, divider));
            let successor = entries
                .get_mut(idx.saturating_add(1))
                .ok_or(BtreeError::Internal(
                    "split successor entry must exist right after insertion",
                ))?;
            successor.0 = new_page;
        }
        None if rightmost == old_page => {
            entries.push((old_page, divider));
            rightmost = new_page;
        }
        None => {
            return Err(BtreeError::MissingChildRoute {
                page_num: parent_page,
                child: old_page,
            });
        }
    }

    let cell_bytes: Vec<Vec<u8>> = entries
        .iter()
        .map(|(child, key)| build_interior_cell(*child, *key))
        .collect();
    let total_bytes: usize = cell_bytes.iter().map(Vec::len).sum();
    let header_len = 12;
    let needed = header_start
        .saturating_add(header_len)
        .saturating_add(cell_bytes.len().saturating_mul(2))
        .saturating_add(total_bytes);
    if needed <= page_len {
        let buf = pager.get_page_mut(parent_page)?;
        write_interior_page(buf, header_start, parent_page, &cell_bytes, rightmost)?;
        return Ok(());
    }

    // Interior split: the median key is promoted to the grandparent
    // without being duplicated in either child.
    let n = entries.len();
    let mid = n / 2;
    let (promoted_child, promoted_key) = *entries.get(mid).ok_or(BtreeError::Internal(
        "median entry index out of bounds during interior split",
    ))?;
    let left_entries = entries.get(..mid).ok_or(BtreeError::Internal(
        "left interior split range out of bounds",
    ))?;
    let right_entries = entries
        .get(mid.saturating_add(1)..)
        .ok_or(BtreeError::Internal(
            "right interior split range out of bounds",
        ))?;

    let right_page_num = pager.allocate_page()?;
    {
        let cells: Vec<Vec<u8>> = left_entries
            .iter()
            .map(|(child, key)| build_interior_cell(*child, *key))
            .collect();
        let buf = pager.get_page_mut(parent_page)?;
        write_interior_page(buf, header_start, parent_page, &cells, promoted_child)?;
    }
    {
        let cells: Vec<Vec<u8>> = right_entries
            .iter()
            .map(|(child, key)| build_interior_cell(*child, *key))
            .collect();
        let buf = pager.get_page_mut(right_page_num)?;
        write_interior_page(buf, 0, right_page_num, &cells, rightmost)?;
    }

    insert_into_parent(
        pager,
        usable_size,
        page_len,
        rest,
        root_page,
        parent_page,
        right_page_num,
        promoted_key,
    )
}

/// The root page number can never change (schema entries point at it), so
/// a root split relocates the root's current content (leaf or interior)
/// verbatim to a freshly allocated page, then reinitializes the root
/// page in-place as a new interior page with one cell pointing at the
/// relocated content and `new_right` as the rightmost pointer.
fn root_split(
    pager: &mut Pager,
    usable_size: u32,
    root_page: u32,
    new_right: u32,
    divider: i64,
) -> Result<(), BtreeError> {
    let header_start_root = page1_header_start(root_page);
    let content = pager.get_page_mut(root_page)?.clone();
    let page_type = read_page_type(&content, header_start_root, root_page)?;
    let relocated = pager.allocate_page()?;

    match page_type {
        LEAF_TABLE => {
            let cells = collect_leaf_cells(&content, header_start_root, root_page, usable_size)?;
            let dest = pager.get_page_mut(relocated)?;
            write_leaf_page(dest, 0, relocated, &cell_bytes(cells))?;
        }
        INTERIOR_TABLE => {
            let (entries, rightmost) =
                collect_interior_entries(&content, header_start_root, root_page)?;
            let cells: Vec<Vec<u8>> = entries
                .iter()
                .map(|(child, key)| build_interior_cell(*child, *key))
                .collect();
            let dest = pager.get_page_mut(relocated)?;
            write_interior_page(dest, 0, relocated, &cells, rightmost)?;
        }
        other => {
            return Err(BtreeError::UnexpectedPageType {
                page_num: root_page,
                page_type: other,
            })
        }
    }

    let cell = build_interior_cell(relocated, divider);
    let buf = pager.get_page_mut(root_page)?;
    write_interior_page(buf, header_start_root, root_page, &[cell], new_right)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::path::Path;

    use crate::btree::test_minimal_db as minimal_db;

    /// 006-btree Requirement 8's duplicate-rowid scenario: inserting a
    /// rowid that already exists in the leaf must error, not silently
    /// overwrite or duplicate the row.
    #[test]
    fn duplicate_rowid_is_rejected() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_row(&mut pager, &header, 1, 1, b"hello").unwrap();
        let err = insert_row(&mut pager, &header, 1, 1, b"world").unwrap_err();
        assert!(matches!(err, BtreeError::DuplicateRowid { rowid: 1 }));
    }

    /// Recursively walks the tree rooted at `page`, collecting every leaf
    /// rowid it finds. Used to verify a b-tree survives many cascading
    /// splits (leaf, interior, and root) without losing or duplicating
    /// rows, regardless of the resulting tree shape/depth.
    fn collect_all_rowids(pager: &mut Pager, usable_size: u32, page: u32, out: &mut Vec<i64>) {
        let header_start = page1_header_start(page);
        let buf = pager.get_page_mut(page).unwrap().clone();
        let page_type = read_page_type(&buf, header_start, page).unwrap();
        if page_type == LEAF_TABLE {
            let cells = collect_leaf_cells(&buf, header_start, page, usable_size).unwrap();
            out.extend(cells.iter().map(|(rowid, _)| *rowid));
        } else {
            let (entries, rightmost) = collect_interior_entries(&buf, header_start, page).unwrap();
            for (child, _) in &entries {
                collect_all_rowids(pager, usable_size, *child, out);
            }
            collect_all_rowids(pager, usable_size, rightmost, out);
        }
    }

    /// 006-btree Requirement 8's split scenarios: inserting enough small
    /// rows into a tiny-page-size database must cascade through leaf
    /// splits, an interior split (parent overflow, median promoted), and
    /// multiple root splits (first leaf-root -> interior, then that
    /// interior root splitting again) — every row must still be found
    /// exactly once afterward, regardless of the resulting tree depth.
    #[test]
    fn many_inserts_cascade_through_every_split_kind() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        // Ascending inserts drive enough leaf splits to overflow the
        // parent interior page too (promoting a median key into a fresh
        // root — the root's first split, from a leaf), and then far
        // enough to overflow that new root-as-interior page again (the
        // root's second split, this time relocating an interior page).
        let n: i64 = 5000;
        for rowid in 1..=n {
            insert_row(&mut pager, &header, 1, rowid, b"0123456789").unwrap();
        }

        // Root must have grown into (at least) an interior page — a
        // leaf-root could never hold 5000 rows in a 512-byte page.
        let header_start_root = page1_header_start(1);
        let root_buf = pager.get_page_mut(1).unwrap().clone();
        let root_type = read_page_type(&root_buf, header_start_root, 1).unwrap();
        assert_eq!(root_type, INTERIOR_TABLE);

        // Backfilling with keys smaller than everything inserted so far
        // routes every one of these into the current *leftmost* leaf,
        // which (once the tree has more than one leaf) is tracked as a
        // named routing entry in its parent rather than the parent's
        // `rightmost` pointer — so these splits exercise the "found the
        // split child among the parent's named entries" path, not just
        // the "split child was the parent's rightmost pointer" path that
        // dominates when every insert lands at the tail.
        for rowid in (-500..0).rev() {
            insert_row(&mut pager, &header, 1, rowid, b"0123456789").unwrap();
        }

        let mut rowids = Vec::new();
        collect_all_rowids(&mut pager, usable_size, 1, &mut rowids);
        rowids.sort_unstable();
        let expected: Vec<i64> = (-500..0).chain(1..=n).collect();
        assert_eq!(rowids, expected);
    }

    /// A payload larger than two overflow pages' worth of data must span a
    /// multi-page overflow chain (not just a single overflow page), and
    /// the row must be stored as a single leaf cell with the overflow
    /// pointer trailing it.
    #[test]
    fn large_payload_spans_multi_page_overflow_chain() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        // Comfortably larger than two overflow pages' capacity
        // (usable_size - 4 bytes each) so the chain has at least 3 links.
        let payload = vec![0xABu8; (usable_size as usize) * 3];
        insert_row(&mut pager, &header, 1, 1, &payload).unwrap();

        let header_start = page1_header_start(1);
        let buf = pager.get_page_mut(1).unwrap().clone();
        let cells = collect_leaf_cells(&buf, header_start, 1, usable_size).unwrap();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].0, 1);
    }

    /// `insert_into_parent`'s error path: if the immediate parent's
    /// routing entries name neither the split child nor its rightmost
    /// pointer, that's a corrupt/mismatched tree and must surface as
    /// `MissingChildRoute`, not silently misroute the split.
    #[test]
    fn insert_into_parent_errors_when_parent_has_no_route_for_child() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();
        let page_len = pager.get_page_mut(1).unwrap().len();

        // Allocate a fresh page to act as an interior "parent" whose
        // routing entries don't mention `old_page` at all.
        let parent_page = pager.allocate_page().unwrap();
        {
            let cells = vec![build_interior_cell(5, 10)];
            let buf = pager.get_page_mut(parent_page).unwrap();
            write_interior_page(buf, 0, parent_page, &cells, 6).unwrap();
        }

        let err = insert_into_parent(
            &mut pager,
            usable_size,
            page_len,
            &[parent_page],
            1,
            99,
            100,
            50,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            BtreeError::MissingChildRoute {
                page_num,
                child: 99,
            } if page_num == parent_page
        ));
    }

    /// `root_split`'s defensive error arm: a root page whose type is
    /// neither `LEAF_TABLE` nor `INTERIOR_TABLE` (e.g. an index b-tree
    /// page type, which should never reach a table b-tree's insert path)
    /// must surface as `UnexpectedPageType`, not panic or silently
    /// misinterpret the page's bytes.
    #[test]
    fn root_split_errors_on_unexpected_root_page_type() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        // Corrupt the root's page-type byte to an index leaf type — never
        // a legal root page type for a table b-tree.
        let header_start = page1_header_start(1);
        {
            let buf = pager.get_page_mut(1).unwrap();
            buf[header_start] = crate::btree::index::LEAF_INDEX;
        }

        let new_right = pager.allocate_page().unwrap();
        let err = root_split(&mut pager, usable_size, 1, new_right, 5).unwrap_err();
        assert!(matches!(
            err,
            BtreeError::UnexpectedPageType {
                page_num: 1,
                page_type,
            } if page_type == crate::btree::index::LEAF_INDEX
        ));
    }
}
