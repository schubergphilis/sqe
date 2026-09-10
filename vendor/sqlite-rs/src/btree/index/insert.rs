// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Index b-tree insert (write path): entry insert, leaf split, cascading
//! interior splits, and root split. WITHOUT ROWID tables are index
//! b-trees (see `index.rs`'s module doc) — the same `insert_entry` writes
//! both ordinary secondary indexes and WITHOUT ROWID table storage.
//!
//! Structurally this mirrors `insert.rs` (table insert), with one real
//! difference: index b-tree interior cells carry a **full entry**, not
//! just a routing key (per `index.rs`'s module doc), so index leaf split
//! behaves like `insert.rs::insert_into_parent`'s *interior*-split branch
//! — the median entry is promoted (removed) into the parent rather than
//! copied as a separator and kept in the leaf. Leaf-level and
//! interior-level splits are therefore the same shape here, unlike the
//! table write path where leaf split (copy-and-keep divider) and interior
//! split (promote-and-remove) differ.
//!
//! Position/ordering uses [`super::compare_keys`] (BINARY-collation
//! key comparison) rather than numeric rowid comparison. Shares the same
//! "every page mutation fully rebuilds the page" simplification as
//! `insert.rs`.

use crate::btree::index::{
    build_index_interior_cell, collect_index_interior_entries, collect_index_leaf_cells,
    descend_index_tree, search_index_leaf, value_cell_len, write_index_interior_page,
    write_index_leaf_page, IndexDescent, LeafSearch, INTERIOR_INDEX, LEAF_INDEX,
};
use crate::btree::{
    cell_bytes, cell_ptr_offset, local_payload_size, page1_header_start, put, read_cell_pointer,
    read_num_cells, read_page_type, splice_insert_cell, BtreeError,
};
use crate::header::DatabaseHeader;
use crate::pager::Pager;
use crate::record::{encode_record, encode_varint, TextEncoding, Value};

/// Inserts one entry (`key`, a full record — indexed columns plus the
/// referenced rowid for an ordinary secondary index, or the whole row for
/// a WITHOUT ROWID table) into the index b-tree rooted at `root_page`,
/// splitting leaves/interior pages (and the root itself) as needed.
/// Returns `Err(BtreeError::DuplicateKey)` if an entry comparing exactly
/// equal to `key` (via `compare_keys`) already exists.
pub fn insert_entry(
    pager: &mut Pager,
    header: &DatabaseHeader,
    root_page: u32,
    key: &[Value],
    encoding: TextEncoding,
) -> Result<(), BtreeError> {
    let usable_size = header.usable_page_size();
    let (ancestors, leaf_page) =
        match descend_index_tree(pager, root_page, usable_size, key, encoding)? {
            IndexDescent::Leaf {
                ancestors,
                leaf_page,
            } => (ancestors, leaf_page),
            IndexDescent::InteriorMatch { .. } => return Err(BtreeError::DuplicateKey),
        };
    // The duplicate-key check above (and the leaf-level one inside
    // `insert_into_index_leaf`) must run before any overflow pages are
    // allocated for this entry's payload — encoding the cell eagerly here
    // would leak an overflow chain on every rejected duplicate insert
    // whose key doesn't fit locally.
    let payload = encode_record(key, encoding);
    let page_len = pager.get_page_mut(leaf_page)?.len();
    insert_into_index_leaf(
        pager,
        usable_size,
        page_len,
        leaf_page,
        root_page,
        &ancestors,
        key,
        &payload,
        encoding,
    )
}

/// Builds an index leaf/interior "value cell": payload-length varint +
/// local payload bytes, plus a trailing 4-byte overflow-page pointer when
/// `payload` doesn't fit locally — the same shape as a table leaf cell
/// minus the rowid varint (index cells carry the rowid as an embedded
/// record column instead, per `index.rs`'s module doc).
fn encode_index_cell(
    pager: &mut Pager,
    usable_size: u32,
    payload: &[u8],
) -> Result<Vec<u8>, BtreeError> {
    let payload_len = payload.len() as u64;
    let local_size =
        (local_payload_size(usable_size, payload_len, true) as usize).min(payload.len());
    let (local_bytes, overflow_bytes) = payload.split_at(local_size);
    let mut cell = encode_varint(payload_len);
    cell.extend_from_slice(local_bytes);
    if !overflow_bytes.is_empty() {
        let overflow_first = write_overflow_chain(pager, usable_size, overflow_bytes)?;
        cell.extend_from_slice(&overflow_first.to_be_bytes());
    }
    Ok(cell)
}

/// Writes `data` across freshly allocated overflow pages, mirroring
/// `insert.rs::write_overflow_chain` (duplicated rather than shared: a
/// generic over two near-identical one-line-different callers isn't
/// worth the indirection here).
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

/// Inserts `cell` (already encoded, for `key`) into leaf page `leaf_page`,
/// splitting it (and cascading into ancestors/`root_page`) if it doesn't
/// fit. Unlike a table leaf split, the split's median entry is promoted
/// (removed from both halves) into the parent — see the module doc.
#[allow(clippy::too_many_arguments)]
fn insert_into_index_leaf(
    pager: &mut Pager,
    usable_size: u32,
    page_len: usize,
    leaf_page: u32,
    root_page: u32,
    ancestors: &[u32],
    key: &[Value],
    payload: &[u8],
    encoding: TextEncoding,
) -> Result<(), BtreeError> {
    let header_start = page1_header_start(leaf_page);
    let buf = pager.get_page_mut(leaf_page)?.clone();

    // Binary search for the insert position, decoding only the O(log n)
    // cells actually compared — a full `collect_index_leaf_cells` decode
    // of every cell on the page is deferred to the split/fallback paths
    // below, which are the only ones that actually need every cell's
    // contents (#648).
    let insert_pos = match search_index_leaf(
        pager,
        &buf,
        header_start,
        leaf_page,
        usable_size,
        encoding,
        key,
    )? {
        LeafSearch::Found(..) => return Err(BtreeError::DuplicateKey),
        LeafSearch::NotFound(pos) => pos,
    };
    // No duplicate found — safe to allocate overflow pages (if any) now.
    let cell = encode_index_cell(pager, usable_size, payload)?;

    let num_cells = read_num_cells(&buf, header_start, leaf_page)?;
    let ptr_base = header_start.saturating_add(8);
    let mut total_bytes = cell.len();
    for i in 0..num_cells {
        let ptr_off = cell_ptr_offset(ptr_base, i);
        let cell_start = read_cell_pointer(&buf, ptr_off, leaf_page, i)?;
        total_bytes =
            total_bytes.saturating_add(value_cell_len(&buf, cell_start, leaf_page, usable_size)?);
    }
    let header_len = 8;
    let needed = header_start
        .saturating_add(header_len)
        .saturating_add(num_cells.saturating_add(1).saturating_mul(2))
        .saturating_add(total_bytes);
    if needed <= page_len {
        // Fast path: splice directly into the page (O(1) relative to the
        // other cells) when there's enough contiguous free space; falls
        // back to a full rebuild otherwise (see #337). `buf` (the
        // pre-mutation snapshot above) still matches on-disk content here,
        // since nothing has written to the page yet.
        let spliced = {
            let page_buf = pager.get_page_mut(leaf_page)?;
            splice_insert_cell(page_buf, header_start, leaf_page, insert_pos, &cell)?
        };
        if !spliced {
            let mut cells = collect_index_leaf_cells(
                pager,
                &buf,
                header_start,
                leaf_page,
                usable_size,
                encoding,
            )?;
            cells.insert(insert_pos, (key.to_vec(), cell.clone()));
            let page_buf = pager.get_page_mut(leaf_page)?;
            write_index_leaf_page(page_buf, header_start, leaf_page, &cell_bytes(cells))?;
        }
        return Ok(());
    }

    // Split: needs every cell's contents to redistribute across the two
    // resulting pages, so the full decode is unavoidable (and correct)
    // here.
    let mut cells =
        collect_index_leaf_cells(pager, &buf, header_start, leaf_page, usable_size, encoding)?;
    cells.insert(insert_pos, (key.to_vec(), cell.clone()));

    // Split: the median entry is promoted into the parent (removed from
    // both halves); left keeps entries less than it, right (a freshly
    // allocated page) keeps entries greater.
    let n = cells.len();
    let mid = n / 2;
    let (promoted_key, promoted_bytes) = cells.get(mid).cloned().ok_or(BtreeError::Internal(
        "median entry index out of bounds during index leaf split",
    ))?;
    let right_page = pager.allocate_page()?;
    let right = cells.split_off(mid.saturating_add(1));
    cells.truncate(mid);
    let left = cells;

    {
        let buf = pager.get_page_mut(leaf_page)?;
        write_index_leaf_page(buf, header_start, leaf_page, &cell_bytes(left))?;
    }
    {
        let buf = pager.get_page_mut(right_page)?;
        write_index_leaf_page(buf, 0, right_page, &cell_bytes(right))?;
    }

    insert_into_index_parent(
        pager,
        usable_size,
        page_len,
        ancestors,
        root_page,
        leaf_page,
        right_page,
        &promoted_key,
        promoted_bytes,
        encoding,
    )
}

/// Propagates a child split (`old_page` keeps its identity as the left
/// sibling, `new_page` is the freshly allocated right sibling,
/// `promoted_key`/`promoted_bytes` is the entry promoted from the child)
/// into its parent, splitting the parent (or the root itself) if needed.
#[allow(clippy::too_many_arguments)]
fn insert_into_index_parent(
    pager: &mut Pager,
    usable_size: u32,
    page_len: usize,
    ancestors: &[u32],
    root_page: u32,
    old_page: u32,
    new_page: u32,
    promoted_key: &[Value],
    promoted_bytes: Vec<u8>,
    encoding: TextEncoding,
) -> Result<(), BtreeError> {
    let Some((&parent_page, rest)) = ancestors.split_last() else {
        return root_split(
            pager,
            usable_size,
            root_page,
            new_page,
            promoted_bytes,
            encoding,
        );
    };

    let header_start = page1_header_start(parent_page);
    let buf = pager.get_page_mut(parent_page)?.clone();
    let (mut entries, mut rightmost) = collect_index_interior_entries(
        pager,
        &buf,
        header_start,
        parent_page,
        usable_size,
        encoding,
    )?;

    match entries.iter().position(|(child, _, _)| *child == old_page) {
        Some(idx) => {
            entries.insert(idx, (old_page, promoted_key.to_vec(), promoted_bytes));
            let successor = entries
                .get_mut(idx.saturating_add(1))
                .ok_or(BtreeError::Internal(
                    "split successor entry must exist right after insertion",
                ))?;
            successor.0 = new_page;
        }
        None if rightmost == old_page => {
            entries.push((old_page, promoted_key.to_vec(), promoted_bytes));
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
        .map(|(child, _, value_bytes)| build_index_interior_cell(*child, value_bytes))
        .collect();
    let total_bytes: usize = cell_bytes.iter().map(Vec::len).sum();
    let header_len = 12;
    let needed = header_start
        .saturating_add(header_len)
        .saturating_add(cell_bytes.len().saturating_mul(2))
        .saturating_add(total_bytes);
    if needed <= page_len {
        let buf = pager.get_page_mut(parent_page)?;
        write_index_interior_page(buf, header_start, parent_page, &cell_bytes, rightmost)?;
        return Ok(());
    }

    // Interior split: same promote-and-remove shape as the leaf split.
    let n = entries.len();
    let mid = n / 2;
    let (promoted_child, promoted_key, promoted_bytes) = entries.get(mid).cloned().ok_or(
        BtreeError::Internal("median entry index out of bounds during index interior split"),
    )?;
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
            .map(|(child, _, value_bytes)| build_index_interior_cell(*child, value_bytes))
            .collect();
        let buf = pager.get_page_mut(parent_page)?;
        write_index_interior_page(buf, header_start, parent_page, &cells, promoted_child)?;
    }
    {
        let cells: Vec<Vec<u8>> = right_entries
            .iter()
            .map(|(child, _, value_bytes)| build_index_interior_cell(*child, value_bytes))
            .collect();
        let buf = pager.get_page_mut(right_page_num)?;
        write_index_interior_page(buf, 0, right_page_num, &cells, rightmost)?;
    }

    insert_into_index_parent(
        pager,
        usable_size,
        page_len,
        rest,
        root_page,
        parent_page,
        right_page_num,
        &promoted_key,
        promoted_bytes,
        encoding,
    )
}

/// The root page number can never change, so an index root split
/// relocates the root's current content (leaf or interior, verbatim) to a
/// freshly allocated page, then reinitializes the root page in-place as a
/// new interior page holding one cell (the promoted entry, routing to the
/// relocated page) and `new_right` as the rightmost pointer. Mirrors
/// `insert.rs::root_split`.
fn root_split(
    pager: &mut Pager,
    usable_size: u32,
    root_page: u32,
    new_right: u32,
    promoted_bytes: Vec<u8>,
    encoding: TextEncoding,
) -> Result<(), BtreeError> {
    let header_start_root = page1_header_start(root_page);
    let content = pager.get_page_mut(root_page)?.clone();
    let page_type = read_page_type(&content, header_start_root, root_page)?;
    let relocated = pager.allocate_page()?;

    match page_type {
        LEAF_INDEX => {
            let cells = collect_index_leaf_cells(
                pager,
                &content,
                header_start_root,
                root_page,
                usable_size,
                encoding,
            )?;
            let dest = pager.get_page_mut(relocated)?;
            write_index_leaf_page(dest, 0, relocated, &cell_bytes(cells))?;
        }
        INTERIOR_INDEX => {
            let (entries, rightmost) = collect_index_interior_entries(
                pager,
                &content,
                header_start_root,
                root_page,
                usable_size,
                encoding,
            )?;
            let cells: Vec<Vec<u8>> = entries
                .iter()
                .map(|(child, _, value_bytes)| build_index_interior_cell(*child, value_bytes))
                .collect();
            let dest = pager.get_page_mut(relocated)?;
            write_index_interior_page(dest, 0, relocated, &cells, rightmost)?;
        }
        other => {
            return Err(BtreeError::UnexpectedPageType {
                page_num: root_page,
                page_type: other,
            })
        }
    }

    let cell = build_index_interior_cell(relocated, &promoted_bytes);
    let buf = pager.get_page_mut(root_page)?;
    write_index_interior_page(buf, header_start_root, root_page, &[cell], new_right)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::vfs::{MemoryVfs, PageSource};
    use std::path::Path;

    /// A one-page, empty-leaf-root database whose root is an index leaf
    /// (`LEAF_INDEX`) instead of a table leaf.
    fn minimal_index_db(page_size: u32) -> (MemoryVfs, DatabaseHeader) {
        let mut page1 = vec![0u8; page_size as usize];
        page1[0..16].copy_from_slice(b"SQLite format 3\0");
        page1[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
        page1[18] = 1;
        page1[19] = 1;
        page1[28..32].copy_from_slice(&1u32.to_be_bytes());
        page1[56..60].copy_from_slice(&1u32.to_be_bytes());
        write_index_leaf_page(&mut page1, 100, 1, &[]).unwrap();

        let mut header_bytes = [0u8; 100];
        header_bytes.copy_from_slice(&page1[..100]);
        let header = DatabaseHeader::parse(&header_bytes).unwrap();

        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", page1);
        (vfs, header)
    }

    fn key(a: &str, rowid: i64) -> Vec<Value> {
        vec![Value::Text(a.to_string().into()), Value::Integer(rowid)]
    }

    #[test]
    fn duplicate_key_is_rejected() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_entry(&mut pager, &header, 1, &key("a", 1), TextEncoding::Utf8).unwrap();
        let err =
            insert_entry(&mut pager, &header, 1, &key("a", 1), TextEncoding::Utf8).unwrap_err();
        assert!(matches!(err, BtreeError::DuplicateKey));
    }

    #[test]
    fn rejected_duplicate_with_overflow_payload_does_not_leak_pages() {
        // Regression guard: a duplicate-key insert whose payload spills to
        // an overflow chain must not allocate that chain before the
        // duplicate check runs — otherwise every rejected retry leaks
        // pages permanently (the file only grows, never reclaimed short
        // of VACUUM).
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let big = vec![Value::Text("x".repeat(2000).into()), Value::Integer(1)];
        insert_entry(&mut pager, &header, 1, &big, TextEncoding::Utf8).unwrap();

        let raw = pager.get_page_mut(1).unwrap().clone();
        let page_count_after_first_insert =
            u32::from_be_bytes([raw[28], raw[29], raw[30], raw[31]]);

        let err = insert_entry(&mut pager, &header, 1, &big, TextEncoding::Utf8).unwrap_err();
        assert!(matches!(err, BtreeError::DuplicateKey));

        let raw = pager.get_page_mut(1).unwrap().clone();
        let page_count_after_rejected_dup =
            u32::from_be_bytes([raw[28], raw[29], raw[30], raw[31]]);
        assert_eq!(
            page_count_after_rejected_dup, page_count_after_first_insert,
            "a rejected duplicate insert must not allocate any new pages"
        );
    }

    #[test]
    fn entries_stay_in_ascending_key_order() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_entry(
            &mut pager,
            &header,
            1,
            &key("banana", 2),
            TextEncoding::Utf8,
        )
        .unwrap();
        insert_entry(&mut pager, &header, 1, &key("apple", 1), TextEncoding::Utf8).unwrap();
        insert_entry(
            &mut pager,
            &header,
            1,
            &key("cherry", 3),
            TextEncoding::Utf8,
        )
        .unwrap();

        let header_start = page1_header_start(1);
        let buf = pager.get_page_mut(1).unwrap().clone();
        let cells = collect_index_leaf_cells(
            &pager,
            &buf,
            header_start,
            1,
            header.usable_page_size(),
            TextEncoding::Utf8,
        )
        .unwrap();
        assert_eq!(cells.len(), 3);
        let texts: Vec<&str> = cells
            .iter()
            .map(|(k, _)| match &k[0] {
                Value::Text(s) => s.as_ref(),
                _ => panic!("expected text"),
            })
            .collect();
        assert_eq!(texts, vec!["apple", "banana", "cherry"]);
    }

    fn cell_for(pager: &mut Pager, usable_size: u32, encoding: TextEncoding, n: i64) -> Vec<u8> {
        let payload = encode_record(&key(&format!("k{n:04}"), n), encoding);
        encode_index_cell(pager, usable_size, &payload).unwrap()
    }

    /// Kills the `n / 2` -> `n % 2` mutant in `insert_into_index_leaf`'s
    /// split: the promoted entry must be the true median (index `n/2` of
    /// the sorted, post-insert entry list), not whatever `n % 2` happens
    /// to produce for a given entry count.
    #[test]
    fn insert_into_index_leaf_split_promotes_the_true_median_entry() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        // Pre-populate the root leaf with 4 sorted entries (k0001..k0004).
        let cells: Vec<Vec<u8>> = (1..=4)
            .map(|n| cell_for(&mut pager, usable_size, TextEncoding::Utf8, n))
            .collect();
        {
            let buf = pager.get_page_mut(1).unwrap();
            write_index_leaf_page(buf, page1_header_start(1), 1, &cells).unwrap();
        }

        // Inserting k0005 (sorts last) makes n = 5; a tiny page_len forces
        // an immediate split regardless of actual byte sizes.
        let new_payload = encode_record(&key("k0005", 5), TextEncoding::Utf8);
        insert_into_index_leaf(
            &mut pager,
            usable_size,
            1,
            1,
            1,
            &[],
            &key("k0005", 5),
            &new_payload,
            TextEncoding::Utf8,
        )
        .unwrap();

        // root(1) must now be interior: one entry (the relocated left
        // half) plus a rightmost pointer (the right half).
        let header_start = page1_header_start(1);
        let buf = pager.get_page_mut(1).unwrap().clone();
        let (entries, rightmost) = collect_index_interior_entries(
            &pager,
            &buf,
            header_start,
            1,
            usable_size,
            TextEncoding::Utf8,
        )
        .unwrap();
        assert_eq!(entries.len(), 1);
        let (relocated, promoted_key, _) = &entries[0];
        assert_eq!(
            promoted_key,
            &key("k0003", 3),
            "the true median (n=5, mid=n/2=2) is k0003"
        );

        let left_cells = collect_index_leaf_cells(
            &pager,
            &pager.read_page(*relocated).unwrap(),
            0,
            *relocated,
            usable_size,
            TextEncoding::Utf8,
        )
        .unwrap();
        assert_eq!(
            left_cells
                .iter()
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>(),
            vec![key("k0001", 1), key("k0002", 2)],
            "left half must hold exactly the entries before the true median"
        );

        let right_cells = collect_index_leaf_cells(
            &pager,
            &pager.read_page(rightmost).unwrap(),
            0,
            rightmost,
            usable_size,
            TextEncoding::Utf8,
        )
        .unwrap();
        assert_eq!(
            right_cells
                .iter()
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>(),
            vec![key("k0004", 4), key("k0005", 5)],
            "right half must hold exactly the entries after the true median"
        );
    }

    /// Kills the `n / 2` -> `n % 2` mutant in `insert_into_index_parent`'s
    /// interior split, the same way the sibling leaf-split test above
    /// covers `insert_into_index_leaf`.
    #[test]
    fn insert_into_index_parent_interior_split_promotes_the_true_median_entry() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        let parent = pager.allocate_page().unwrap();
        let child1 = pager.allocate_page().unwrap();
        let child2 = pager.allocate_page().unwrap();
        let child3 = pager.allocate_page().unwrap();
        let child4 = pager.allocate_page().unwrap();
        let old_page = pager.allocate_page().unwrap();
        let new_page = pager.allocate_page().unwrap();

        let bytes1 = cell_for(&mut pager, usable_size, TextEncoding::Utf8, 1);
        let bytes2 = cell_for(&mut pager, usable_size, TextEncoding::Utf8, 2);
        let bytes3 = cell_for(&mut pager, usable_size, TextEncoding::Utf8, 3);
        let bytes4 = cell_for(&mut pager, usable_size, TextEncoding::Utf8, 4);
        let promoted_bytes = cell_for(&mut pager, usable_size, TextEncoding::Utf8, 5);

        // parent's rightmost is old_page (not a routing entry), so
        // inserting old_page's split pushes a 5th entry and promotes
        // rightmost to new_page — same shape `delete_row`'s ancestors use.
        let cells = vec![
            build_index_interior_cell(child1, &bytes1),
            build_index_interior_cell(child2, &bytes2),
            build_index_interior_cell(child3, &bytes3),
            build_index_interior_cell(child4, &bytes4),
        ];
        {
            let buf = pager.get_page_mut(parent).unwrap();
            write_index_interior_page(buf, page1_header_start(parent), parent, &cells, old_page)
                .unwrap();
        }

        insert_into_index_parent(
            &mut pager,
            usable_size,
            1, // page_len: forces an immediate split
            &[parent],
            1,
            old_page,
            new_page,
            &key("k0005", 5),
            promoted_bytes,
            TextEncoding::Utf8,
        )
        .unwrap();

        // n = 5 (child1..child4 + pushed old_page), true median mid = 2:
        // parent keeps [child1, child2] with child3 promoted to rightmost.
        let header_start = page1_header_start(parent);
        let buf = pager.get_page_mut(parent).unwrap().clone();
        let (entries, rightmost) = collect_index_interior_entries(
            &pager,
            &buf,
            header_start,
            parent,
            usable_size,
            TextEncoding::Utf8,
        )
        .unwrap();
        assert_eq!(
            entries.iter().map(|(c, _, _)| *c).collect::<Vec<_>>(),
            vec![child1, child2],
            "parent must keep exactly the entries before the true median"
        );
        assert_eq!(
            rightmost, child3,
            "the true median's child (n=5, mid=n/2=2) must be promoted to rightmost"
        );
    }

    /// Kills the `rightmost == old_page` -> `true` mutant in
    /// `insert_into_index_parent`: when `old_page` is neither a routing
    /// entry nor `rightmost`, this must be a hard error, not a silent
    /// (incorrect) promotion.
    #[test]
    fn insert_into_index_parent_errors_when_old_page_is_not_a_child() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        let parent = pager.allocate_page().unwrap();
        let other = pager.allocate_page().unwrap();
        let rightmost = pager.allocate_page().unwrap();
        let unrelated = pager.allocate_page().unwrap();
        let new_page = pager.allocate_page().unwrap();
        let bytes_other = cell_for(&mut pager, usable_size, TextEncoding::Utf8, 1);
        let promoted_bytes = cell_for(&mut pager, usable_size, TextEncoding::Utf8, 2);

        {
            let cells = vec![build_index_interior_cell(other, &bytes_other)];
            let buf = pager.get_page_mut(parent).unwrap();
            write_index_interior_page(buf, page1_header_start(parent), parent, &cells, rightmost)
                .unwrap();
        }

        let err = insert_into_index_parent(
            &mut pager,
            usable_size,
            usable_size as usize,
            &[parent],
            1,
            unrelated,
            new_page,
            &key("k0002", 2),
            promoted_bytes,
            TextEncoding::Utf8,
        )
        .unwrap_err();
        assert!(matches!(err, BtreeError::MissingChildRoute { .. }));
    }
}
